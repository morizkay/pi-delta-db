/**
 * pi-delta-db — store and read Pi data through the Delta CLI (`delta`).
 *
 * Delta's SQLite store (`delta db ...`) is internal and read-only from outside,
 * so writes go through Delta's supported path: a private thread bound to this
 * session (`--source pi --external-id <session>`) recorded with schema-1 JSONL.
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
const STATE_DIR = path.join(os.homedir(), ".pi", "agent", "delta-db");
const MAX_CHARS = 12_000;
const TIMEOUT_MS = 120_000;

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
}
interface FinalMessage {
	role?: string;
	content?: string | ContentBlock[];
}
interface Update {
	sessionUpdate: string;
	messageId?: string;
	title?: string;
	content?: { type: string; text: string }[];
}

/** A session's Delta thread binding. `titled` keeps a continued session from renaming it. */
interface Binding {
	threadId: string;
	titled?: boolean;
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
	// SAFETY: Pi's session manager exposes these getters at runtime; the public
	// ExtensionContext type does not declare them, so only their presence is assumed.
	const manager = ctx.sessionManager as unknown as {
		getSessionId?: () => string;
		getSessionFile?: () => string;
	};
	const raw = manager?.getSessionId?.() ?? manager?.getSessionFile?.() ?? `cwd-${ctx.cwd}`;
	const tail = String(raw).split(/[\\/]/).pop() ?? "session";
	return tail.replace(/[^A-Za-z0-9_-]/g, "-");
}

function bindingFile(key: string): string {
	return path.join(STATE_DIR, `${key}.json`);
}

function readBinding(key: string): Binding | undefined {
	try {
		const saved = parseJson(fs.readFileSync(bindingFile(key), "utf8")) as Binding | undefined;
		return saved?.threadId ? saved : undefined;
	} catch {
		// No binding file yet: this session has never written to Delta.
		return undefined;
	}
}

function writeBinding(key: string, binding: Binding): void {
	fs.mkdirSync(STATE_DIR, { recursive: true });
	fs.writeFileSync(bindingFile(key), JSON.stringify(binding, null, 2));
}

async function ensureThread(key: string): Promise<Binding> {
	const bound = readBinding(key);
	if (bound) return bound;
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
	const binding: Binding = { threadId: created.thread_id };
	writeBinding(key, binding);
	return binding;
}

function statusOf(key: string, threadId: string): Promise<ThreadStatus> {
	return json<ThreadStatus>(["thread", "status", "--source", SOURCE, "--external-id", key, "--json", threadId]);
}

/** Append schema-1 records, continuing this producer's sequence. */
async function append(key: string, threadId: string, updates: Update[]) {
	const status = await statusOf(key, threadId);
	let sequence = status.producer?.next_sequence;
	if (!Number.isSafeInteger(sequence)) throw new Error("Delta did not report the next sequence for this thread");
	const lines = updates
		.map((update) => JSON.stringify({ schemaVersion: 1, sequence: sequence++, update }))
		.join("\n");
	await run(
		["thread", "append", "--source", SOURCE, "--external-id", key, "--acknowledge", "local", "--json", threadId],
		`${lines}\n`,
	);
	return { count: updates.length, nextSequence: sequence, url: status.url };
}

/** Text of a finalized message; only text blocks are recorded. */
// shortcut: thinking and tool calls are not recorded, add them when the transcript needs them
function messageText(message: FinalMessage): string {
	const content = message?.content;
	if (typeof content === "string") return content.trim();
	if (!Array.isArray(content)) return "";
	return content
		.filter((block) => block?.type === "text" && typeof block.text === "string")
		.map((block) => String(block.text))
		.join("\n")
		.trim();
}

function firstLine(text: string): string {
	const line = text.split("\n").find((part) => part.trim()) ?? "";
	return line.trim().slice(0, 60);
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
		(rec ??= { key: sessionKey(ctx), enabled: process.env.PI_DELTA_RECORD !== "0", warned: false });

	pi.on("session_start", (_event, ctx) => {
		rec = { key: sessionKey(ctx), enabled: process.env.PI_DELTA_RECORD !== "0", warned: false };
	});

	// Auto-record every finalized user and assistant message into this session's thread.
	pi.on("message_end", (event, ctx) => {
		const current = sessionOf(ctx);
		if (!current.enabled) return;
		const message = event.message as FinalMessage;
		if (message?.role !== "user" && message?.role !== "assistant") return;
		const text = messageText(message);
		if (!text) return;
		const wantsTitle = message.role === "user" ? firstLine(text) : undefined;
		enqueue(async () => {
			try {
				const binding = await ensureThread(current.key);
				const updates: Update[] = [];
				// A messageId must be globally unique: Delta replaces an existing record with the same id.
				if (wantsTitle && !binding.titled) {
					updates.push({ sessionUpdate: "session_info_update", title: wantsTitle });
					binding.titled = true;
					writeBinding(current.key, binding);
				}
				updates.push({
					sessionUpdate: message.role === "user" ? "user_message" : "agent_message",
					messageId: `${current.key}:${randomUUID()}`,
					content: [{ type: "text", text }],
				});
				await append(current.key, binding.threadId, updates);
			} catch (error) {
				if (current.warned) return;
				current.warned = true;
				ctx.ui.notify(`Delta recording stopped: ${(error as Error).message}`, "warning");
			}
		});
	});

	pi.on("session_shutdown", async () => {
		// Let the tail of the queue reach Delta before the process exits.
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
				const binding = await ensureThread(key);
				const updates: Update[] = [];
				if (params.title && !binding.titled) {
					updates.push({ sessionUpdate: "session_info_update", title: params.title });
					binding.titled = true;
					writeBinding(key, binding);
				}
				updates.push({
					sessionUpdate: params.role === "user" ? "user_message" : "agent_message",
					messageId: `${key}:${randomUUID()}`,
					content: [{ type: "text", text: params.text }],
				});
				return { ...(await append(key, binding.threadId, updates)), threadId: binding.threadId };
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
				ctx.ui.notify(`Delta recording ${arg}`, "info");
				return;
			}
			const threadId = readBinding(current.key);
			const lines = [
				`Recording: ${current.enabled ? "on" : "off"} (/delta on|off)`,
				threadId ? `Thread: ${threadId}` : "Thread: none yet",
			];
			if (threadId) {
				const status = await statusOf(current.key, threadId);
				if (status.title) lines.push(`Title: ${status.title}`);
				if (status.url) lines.push(status.url);
			}
			ctx.ui.notify(lines.join("\n"), "info");
		},
	});
}
