/**
 * pi-delta-db — store and read Pi data through the Delta CLI (`delta`).
 *
 * Delta's SQLite store (`delta db ...`) is internal and read-only from outside,
 * so writes go through Delta's supported path: a private thread bound to this
 * session (`--source pi --external-id <session>`) recorded with schema-1 JSONL.
 *
 * Records are staged to a local outbox before any network call. A failed append
 * stays on disk and is retried; resending is safe because each messageId is unique
 * and Delta replaces a record that reuses one.
 */

import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { StringEnum } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

const SOURCE = "pi";
const STATE_DIR = process.env.PI_DELTA_STATE || path.join(os.homedir(), ".pi", "agent", "delta-db");
const SESSIONS_DIR = path.join(os.homedir(), ".pi", "agent", "sessions");
const MAX_CHARS = 12_000;
const TIMEOUT_MS = 120_000;
const PRUNE_AFTER_MS = 7 * 24 * 60 * 60 * 1000;

export interface Update {
	sessionUpdate: string;
	messageId?: string;
	title?: string;
	content?: { type: string; text: string }[];
	toolCallId?: string;
	name?: string;
	status?: string;
	rawInput?: unknown;
	rawOutput?: unknown;
}

interface Org {
	id: string;
	current?: boolean;
}
interface ThreadListItem {
	index?: number;
	id: string;
	name?: string;
	summary?: string;
}
interface ThreadStatus {
	producer?: { next_sequence?: number };
	url?: string;
	title?: string;
}
interface ThreadShow {
	id?: string;
	name?: string;
	transcript?: string;
	buffer?: string;
}
interface Stats {
	schema_version?: number;
	node_count?: number;
	node_compressed_bytes?: number;
	node_types?: { type: string; count: number; compressed_bytes: number }[];
	roots?: { id: string; type: string }[];
}
interface ContentBlock {
	type?: string;
	text?: string;
	thinking?: string;
	id?: string;
	name?: string;
	arguments?: unknown;
}
interface FinalMessage {
	role?: string;
	content?: string | ContentBlock[];
	toolCallId?: string;
	toolName?: string;
	isError?: boolean;
}
/** A session's Delta thread binding. `titled` keeps a continued session from renaming it. */
interface Binding {
	threadId?: string;
	titled?: boolean;
	enabled?: boolean;
}

type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

/** Parse CLI output at its boundary; unparseable text becomes undefined. */
function parseJson(text: string): JsonValue | undefined {
	if (!text.trim()) return undefined;
	try {
		return JSON.parse(text) as JsonValue;
	} catch {
		return undefined;
	}
}

/** Run `delta`, returning stdout, and turn a failed exit into an Error. */
function run(args: string[], input?: string): Promise<string> {
	return new Promise((resolve, reject) => {
		const child = spawn("delta", args, { stdio: ["pipe", "pipe", "pipe"] });
		let out = "";
		let err = "";
		const timer = setTimeout(() => child.kill("SIGKILL"), TIMEOUT_MS);
		child.stdout.on("data", (chunk) => (out += chunk));
		child.stderr.on("data", (chunk) => (err += chunk));
		child.on("error", (error) => {
			clearTimeout(timer);
			reject(new Error(error.message.includes("ENOENT") ? "`delta` was not found on PATH" : error.message));
		});
		child.on("close", (code) => {
			clearTimeout(timer);
			if (code === 0) {
				resolve(out);
				return;
			}
			// With --json the CLI prints {error:{message}} on stdout; prefer that over stderr noise.
			const reported = (parseJson(out) as { error?: { message?: string } } | undefined)?.error?.message;
			reject(new Error(reported ?? err.trim() ?? `delta ${args.slice(0, 2).join(" ")} exited ${code}`));
		});
		child.stdin.end(input);
	});
}

async function json<T>(args: string[], input?: string): Promise<T> {
	const out = await run(args, input);
	const value = parseJson(out);
	if (value === undefined) throw new Error(`delta ${args.slice(0, 2).join(" ")} returned no JSON`);
	return value as T;
}

function clip(text: string, limit = MAX_CHARS): string {
	return text.length > limit ? `${text.slice(0, limit)}\n…truncated (${text.length} chars)` : text;
}

/** A stable producer key for this Pi session; also the on-disk binding name. */
function sessionKey(ctx: ExtensionContext): string {
	const raw = ctx.sessionManager.getSessionId() || ctx.sessionManager.getSessionFile() || `cwd-${ctx.cwd}`;
	const tail = String(raw).split(/[\\/]/).pop() ?? "session";
	return tail.replace(/[^A-Za-z0-9_-]/g, "-");
}

function bindingFile(key: string): string {
	return path.join(STATE_DIR, `${key}.json`);
}

function outboxFile(key: string): string {
	return path.join(STATE_DIR, "outbox", `${key}.jsonl`);
}

function readBinding(key: string): Binding | undefined {
	try {
		const saved = parseJson(fs.readFileSync(bindingFile(key), "utf8")) as Binding | undefined;
		return saved && typeof saved === "object" && !Array.isArray(saved) ? saved : undefined;
	} catch {
		// No binding file yet: this session has never written to Delta.
		return undefined;
	}
}

function writeBinding(key: string, binding: Binding): void {
	fs.mkdirSync(STATE_DIR, { recursive: true });
	fs.writeFileSync(bindingFile(key), JSON.stringify(binding, null, 2));
}

/** `PI_DELTA_RECORD=0` forces off for this process; `/delta off` persists in the binding. */
export function recordingEnabled(key: string): boolean {
	if (process.env.PI_DELTA_RECORD === "0") return false;
	return readBinding(key)?.enabled !== false;
}

export function setRecording(key: string, enabled: boolean): void {
	writeBinding(key, { ...readBinding(key), enabled });
}

async function ensureThread(key: string): Promise<Binding> {
	const bound = readBinding(key);
	if (bound?.threadId) return bound;
	const { organizations } = await json<{ organizations?: Org[] }>(["org", "list", "--json"]);
	const org = organizations?.find((item) => item.current) ?? organizations?.[0];
	if (!org) throw new Error("No Delta organization. Run `delta auth login`, then retry.");
	const created = await json<{ thread_id: string }>([
		"thread",
		"create",
		"--source",
		SOURCE,
		"--external-id",
		key,
		"--visibility",
		"private",
		"--organization",
		org.id,
		"--json",
	]);
	const binding: Binding = { ...bound, threadId: created.thread_id };
	writeBinding(key, binding);
	return binding;
}

function statusOf(key: string, threadId: string): Promise<ThreadStatus> {
	return json<ThreadStatus>(["thread", "status", "--source", SOURCE, "--external-id", key, "--json", threadId]);
}

/** Schema-1 JSONL continuing at `first`. Exported so tests can lock the envelope. */
export function assignSequences(first: number, updates: Update[]): string {
	return updates
		.map((update, index) => JSON.stringify({ schemaVersion: 1, sequence: first + index, update }))
		.join("\n");
}

/** Append schema-1 records, continuing this producer's sequence. */
async function append(key: string, threadId: string, updates: Update[]) {
	const status = await statusOf(key, threadId);
	const first = status.producer?.next_sequence;
	if (first === undefined || !Number.isSafeInteger(first))
		throw new Error("Delta did not report the next sequence for this thread");
	await run(
		["thread", "append", "--source", SOURCE, "--external-id", key, "--acknowledge", "local", "--json", threadId],
		`${assignSequences(first, updates)}\n`,
	);
	return { count: updates.length, nextSequence: first + updates.length, url: status.url };
}

function textOf(content: string | ContentBlock[] | undefined): string {
	if (typeof content === "string") return content.trim();
	if (!Array.isArray(content)) return "";
	return content
		.filter((block) => block?.type === "text" && typeof block.text === "string")
		.map((block) => String(block.text))
		.join("\n")
		.trim();
}

function textUpdate(role: string | undefined, id: string, text: string): Update {
	return {
		sessionUpdate: role === "user" ? "user_message" : "agent_message",
		messageId: id,
		content: [{ type: "text", text: clip(text) }],
	};
}

/**
 * Turn one finalized Pi message into Delta records.
 * `id` must be unique per message: Delta replaces a record that reuses a messageId.
 */
export function updatesFromMessage(message: FinalMessage, id: string): Update[] {
	const updates: Update[] = [];
	if (message.role === "toolResult" && message.toolCallId) {
		updates.push({
			sessionUpdate: "tool_call_update",
			toolCallId: message.toolCallId,
			name: message.toolName || "tool",
			status: message.isError ? "failed" : "completed",
			rawOutput: clip(textOf(message.content) || (message.isError ? "failed" : "")),
		});
		return updates;
	}
	const content = message.content;
	if (typeof content === "string") {
		if (content.trim()) updates.push(textUpdate(message.role, `${id}:t0`, content.trim()));
		return updates;
	}
	if (!Array.isArray(content)) return updates;
	const texts: string[] = [];
	let part = 0;
	const flushText = () => {
		const text = texts.join("\n").trim();
		texts.length = 0;
		if (!text) return;
		updates.push(textUpdate(message.role, `${id}:t${part++}`, text));
	};
	for (const [index, block] of content.entries()) {
		if (block?.type === "thinking" && block.thinking?.trim()) {
			flushText();
			updates.push({
				sessionUpdate: "agent_thought",
				messageId: `${id}:thought:${index}`,
				content: [{ type: "text", text: clip(block.thinking.trim()) }],
			});
		} else if (block?.type === "toolCall" && block.id) {
			flushText();
			updates.push({
				sessionUpdate: "tool_call_update",
				toolCallId: block.id,
				name: block.name || "tool",
				status: "in_progress",
				rawInput: block.arguments ?? {},
			});
		} else if (block?.type === "text" && block.text?.trim()) {
			texts.push(block.text);
		}
	}
	flushText();
	return updates;
}

function firstLine(text: string): string {
	const line = text.split("\n").find((part) => part.trim()) ?? "";
	return line.trim().slice(0, 60);
}

/** Append records to the outbox and fsync. A crash after this still retries. */
export function stageUpdates(key: string, updates: Update[]): void {
	if (!updates.length) return;
	const file = outboxFile(key);
	fs.mkdirSync(path.dirname(file), { recursive: true });
	const fd = fs.openSync(file, "a");
	try {
		fs.writeSync(fd, `${updates.map((update) => JSON.stringify(update)).join("\n")}\n`);
		fs.fsyncSync(fd);
	} finally {
		fs.closeSync(fd);
	}
}

export function pendingUpdates(key: string): Update[] {
	let text: string;
	try {
		text = fs.readFileSync(outboxFile(key), "utf8");
	} catch {
		return [];
	}
	if (!text.trim()) return [];
	return text
		.split("\n")
		.filter(Boolean)
		.map((line) => {
			try {
				return JSON.parse(line) as Update;
			} catch {
				throw new Error(`corrupt outbox record for ${key}; sharing paused without dropping it`);
			}
		});
}

/** Send the outbox, then drop it. On failure the file stays for the next attempt. */
export async function flushPending(key: string): Promise<{ count: number; threadId?: string; url?: string; nextSequence?: number }> {
	const updates = pendingUpdates(key);
	if (!updates.length) return { count: 0 };
	const binding = await ensureThread(key);
	if (!binding.threadId) throw new Error("Delta thread was not created");
	const result = await append(key, binding.threadId, updates);
	fs.rmSync(outboxFile(key), { force: true });
	return { count: result.count, threadId: binding.threadId, url: result.url, nextSequence: result.nextSequence };
}

function sessionFileExists(sessionsDir: string, key: string): boolean {
	const stack = [sessionsDir];
	while (stack.length) {
		const dir = stack.pop();
		if (!dir) break;
		let entries: fs.Dirent[];
		try {
			entries = fs.readdirSync(dir, { withFileTypes: true });
		} catch {
			continue;
		}
		for (const entry of entries) {
			if (entry.isDirectory()) stack.push(path.join(dir, entry.name));
			else if (entry.name.endsWith(`_${key}.jsonl`)) return true;
		}
	}
	return false;
}

/** Drop bindings whose session file is gone and that have not been touched for a week. */
export function pruneBindings(currentKey: string, sessionsDir: string, now = Date.now()): string[] {
	if (!fs.existsSync(sessionsDir) || !fs.existsSync(STATE_DIR)) return [];
	let names: string[];
	try {
		names = fs.readdirSync(STATE_DIR);
	} catch {
		return [];
	}
	const removed: string[] = [];
	for (const name of names) {
		if (!name.endsWith(".json")) continue;
		const key = name.slice(0, -".json".length);
		if (key === currentKey || !/^[\w-]+$/.test(key)) continue;
		const file = path.join(STATE_DIR, name);
		if (now - fs.statSync(file).mtimeMs < PRUNE_AFTER_MS) continue;
		if (sessionFileExists(sessionsDir, key)) continue;
		fs.rmSync(file, { force: true });
		fs.rmSync(outboxFile(key), { force: true });
		removed.push(key);
	}
	return removed;
}

function summarizeList(threads: ThreadListItem[]): string {
	if (!threads.length) return "No Delta threads yet.";
	return threads
		.map((thread) => {
			const bits = [`${thread.index ?? "?"}. ${thread.name ?? "(unnamed)"}`, thread.id];
			if (thread.summary) bits.push(clip(String(thread.summary), 160));
			return bits.join(" — ");
		})
		.join("\n");
}

/** Stage a message, including the thread title the first time a user prompt arrives. */
function stageMessage(key: string, message: FinalMessage, id: string): number {
	const updates = updatesFromMessage(message, id);
	if (!updates.length) return 0;
	const binding = readBinding(key);
	if (message.role === "user" && !binding?.titled) {
		const title = firstLine(textOf(message.content));
		if (title) {
			updates.unshift({ sessionUpdate: "session_info_update", title });
			writeBinding(key, { ...binding, titled: true });
		}
	}
	stageUpdates(key, updates);
	return updates.length;
}

export default function (pi: ExtensionAPI) {
	// One writer at a time: sequence numbers come from `thread status`, so overlapping
	// appends would reuse a sequence and Delta would reject them.
	let queue: Promise<unknown> = Promise.resolve();
	function enqueue<T>(task: () => Promise<T>): Promise<T> {
		const queued = queue.then(task, task);
		queue = queued.catch(() => {});
		return queued;
	}

	let rec: { key: string; enabled: boolean; warned: boolean } | null = null;
	const sessionOf = (ctx: ExtensionContext) =>
		(rec ??= { key: sessionKey(ctx), enabled: recordingEnabled(sessionKey(ctx)), warned: false });

	async function record(key: string, message: FinalMessage, id: string): Promise<void> {
		stageMessage(key, message, id);
		await flushPending(key);
	}

	pi.on("session_start", (_event, ctx) => {
		const key = sessionKey(ctx);
		rec = { key, enabled: recordingEnabled(key), warned: false };
		pruneBindings(key, SESSIONS_DIR);
		enqueue(() => flushPending(key)).catch((error: Error) => {
			if (rec?.warned) return;
			if (rec) rec.warned = true;
			ctx.ui.notify(`Delta unreachable; queued records will retry. ${error.message}`, "warning");
		});
	});

	pi.on("message_end", (event, ctx) => {
		const current = sessionOf(ctx);
		if (!current.enabled) return;
		const message = event.message as FinalMessage;
		if (message?.role !== "user" && message?.role !== "assistant" && message?.role !== "toolResult") return;
		const id = randomUUID();
		enqueue(async () => {
			try {
				await record(current.key, message, id);
				if (current.warned) {
					current.warned = false;
					ctx.ui.notify("Delta recording resumed", "info");
				}
			} catch (error) {
				if (current.warned) return;
				current.warned = true;
				ctx.ui.notify(`Delta unreachable; this turn is queued and will retry. ${(error as Error).message}`, "warning");
			}
		});
	});

	pi.on("session_shutdown", async () => {
		// Let the tail of the queue reach Delta before the process exits. What doesn't
		// make it stays in the outbox for the next start.
		await Promise.race([queue, new Promise((resolve) => setTimeout(resolve, 8000))]);
	});

	pi.registerTool({
		name: "delta_store",
		label: "Delta Store",
		description:
			"Store text in this session's private Delta thread. Creates the thread on first use and appends a recorded message that the Delta app can open and share.",
		parameters: Type.Object({
			text: Type.String({ description: "Text to store" }),
			role: Type.Optional(
				StringEnum(["assistant", "user"] as const, { description: "Message author (default assistant)" }),
			),
			title: Type.Optional(Type.String({ description: "Set the thread title when given" })),
		}),
		async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
			const key = sessionOf(ctx).key;
			const result = await enqueue(async () => {
				const binding = readBinding(key);
				const updates: Update[] = [];
				if (params.title && !binding?.titled) {
					updates.push({ sessionUpdate: "session_info_update", title: params.title });
					writeBinding(key, { ...binding, titled: true });
				}
				updates.push(textUpdate(params.role, `${key}:${randomUUID()}`, params.text));
				stageUpdates(key, updates);
				const flushed = await flushPending(key);
				return { ...flushed, threadId: flushed.threadId };
			});
			const where = result.url ? ` ${result.url}` : "";
			return {
				content: [
					{
						type: "text",
						text: `Stored ${result.count} record(s) in Delta thread ${result.threadId} (next sequence ${result.nextSequence}).${where}`,
					},
				],
				details: { ...result },
			};
		},
	});

	pi.registerTool({
		name: "delta_read",
		label: "Delta Read",
		description:
			"Read Delta data: list threads, search thread text, or show one thread's transcript as text. Actions: list, search (needs query), show (needs thread).",
		parameters: Type.Object({
			action: StringEnum(["list", "search", "show"] as const),
			query: Type.Optional(Type.String({ description: "Keywords for search" })),
			thread: Type.Optional(Type.String({ description: "Thread id or list index for show" })),
		}),
		async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
			if (params.action === "list") {
				const threads = await json<ThreadListItem[]>(["thread", "list", "--json"]);
				return {
					content: [{ type: "text", text: summarizeList(threads) }],
					details: { action: "list", count: threads.length },
				};
			}
			if (params.action === "search") {
				if (!params.query?.trim()) throw new Error("query is required for search");
				const words = params.query.trim().split(/\s+/);
				const threads = await json<ThreadListItem[]>(["thread", "search", "-n", "20", "--json", ...words]);
				return {
					content: [{ type: "text", text: summarizeList(threads) }],
					details: { action: "search", query: params.query, count: threads.length },
				};
			}
			const bound = readBinding(sessionOf(ctx).key);
			const thread = params.thread?.trim() || bound?.threadId;
			if (!thread) throw new Error("thread is required for show (this session has no Delta thread yet)");
			const [shown] = await json<ThreadShow[]>(["thread", "show", thread, "--format", "summarize", "--json"]);
			const body = shown?.transcript ?? shown?.buffer ?? "(empty)";
			return {
				content: [{ type: "text", text: clip(`# ${shown?.name ?? thread}\n\n${body}`) }],
				details: { action: "show", threadId: shown?.id ?? thread },
			};
		},
	});

	pi.registerTool({
		name: "delta_db",
		label: "Delta DB",
		description:
			"Inspect Delta's local SQLite store (read-only): storage stats, or decode one stored root by id. Actions: stats, root (needs id).",
		parameters: Type.Object({
			action: StringEnum(["stats", "root"] as const),
			id: Type.Optional(Type.String({ description: "Root id from stats, for root" })),
		}),
		async execute(_toolCallId, params, _signal, _onUpdate) {
			if (params.action === "stats") {
				const stats = await json<Stats>(["db", "stats", "--json"]);
				const types = (stats.node_types ?? [])
					.map((item) => `${item.type}: ${item.count} nodes, ${item.compressed_bytes} B`)
					.join("\n");
				const roots = (stats.roots ?? []).map((item) => `${item.id} ${item.type}`).join("\n");
				return {
					content: [
						{
							type: "text",
							text: clip(
								`schema ${stats.schema_version}, ${stats.node_count} nodes, ${stats.node_compressed_bytes} B\n${types}\n\nroots:\n${roots}`,
							),
						},
					],
					details: { action: "stats", schemaVersion: stats.schema_version, nodes: stats.node_count },
				};
			}
			if (!params.id) throw new Error("id is required for root");
			const decoded = await json<{ type?: string }>(["db", "root", params.id, "--json"]);
			return {
				content: [{ type: "text", text: clip(JSON.stringify(decoded, null, 2)) }],
				details: { action: "root", id: params.id, type: decoded?.type },
			};
		},
	});

	pi.registerCommand("delta", {
		description: "Show or toggle this session's Delta recording",
		handler: async (args, ctx) => {
			const current = sessionOf(ctx);
			const arg = args.trim().toLowerCase();
			if (arg === "on" || arg === "off") {
				current.enabled = arg === "on";
				setRecording(current.key, current.enabled);
				ctx.ui.notify(`Delta recording ${arg}`, "info");
				return;
			}
			const binding = readBinding(current.key);
			const queued = pendingUpdates(current.key).length;
			const lines = [
				`Recording: ${current.enabled ? "on" : "off"} (/delta on|off, remembered)`,
				binding?.threadId ? `Thread: ${binding.threadId}` : "Thread: none yet",
				queued ? `Queued: ${queued}` : "Queued: 0",
			];
			if (binding?.threadId) {
				const status = await statusOf(current.key, binding.threadId);
				if (status.title) lines.push(`Title: ${status.title}`);
				if (status.url) lines.push(status.url);
			}
			ctx.ui.notify(lines.join("\n"), "info");
		},
	});
}
