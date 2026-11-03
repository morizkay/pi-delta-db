/**
 * pi-delta-db — record a Pi session to a private Delta thread and read threads back.
 *
 * Writes go through `delta thread append` as schema-1 JSONL. Each record is numbered
 * when it is staged, and a retry sends those same bytes. Delta then returns `duplicate`
 * instead of storing a second copy.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { StringEnum } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

const SOURCE = "pi";
const PRODUCER = "Pi";
/** Contract this build speaks. Delta 0.23.0 reports min 8, max 8. */
export const CONTRACT = 8;
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
	state?: string;
	rawInput?: unknown;
	rawOutput?: unknown;
}

/** One outbox line. `sequence` is assigned at stage time and never rewritten. */
export interface StoredRecord {
	schemaVersion: 1;
	sequence: number;
	turnSequence?: number;
	update: Update;
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
interface Binding {
	threadId?: string;
	titled?: boolean;
	enabled?: boolean;
	/** Next sequence to assign. Never moved backward, and never used to renumber the outbox. */
	nextSequence?: number;
	/** Set when Delta will not accept a retry. `/delta on` clears it. */
	paused?: string;
}
interface DeltaRun {
	stdout: string;
	stderr: string;
	code: number | null;
}
interface FlushResult {
	count: number;
	threadId?: string;
	url?: string;
	nextSequence?: number;
}

type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

export class DeltaFailure extends Error {
	readonly retryable: boolean;
	readonly code?: string;
	readonly retryAfterMs?: number;
	readonly stdout: string;
	fenced = false;
	constructor(
		message: string,
		options: { retryable: boolean; code?: string; retryAfterMs?: number; stdout?: string },
	) {
		super(message);
		this.name = "DeltaFailure";
		this.retryable = options.retryable;
		this.code = options.code;
		this.retryAfterMs = options.retryAfterMs;
		this.stdout = options.stdout ?? "";
	}
}

/** `reload_required` is a published code, not the free-text reason. */
export function explain(code: string | undefined, message: string): string {
	if (code === "reload_required") {
		return `${message} Open the thread in Delta and choose Fork & Restore, or run \`delta thread discard --force\`.`;
	}
	return message;
}

/** Stop recording when this build is outside the CLI's contract range. */
export function contractProblem(min: number, max: number): string | undefined {
	if (!Number.isSafeInteger(min) || !Number.isSafeInteger(max)) {
		return "Delta did not report a contract range. Update Delta.";
	}
	if (max < CONTRACT) return `Delta CLI is too old for this plugin (contract ${max}, need ${CONTRACT}). Update Delta.`;
	if (min > CONTRACT) {
		return `This plugin is too old for this Delta CLI (needs contract ${min}, plugin speaks ${CONTRACT}). Update pi-delta-db.`;
	}
	return undefined;
}

function parseJson(text: string): JsonValue | undefined {
	if (!text.trim()) return undefined;
	try {
		return JSON.parse(text) as JsonValue;
	} catch {
		return undefined;
	}
}

function run(args: string[], input?: string): Promise<DeltaRun> {
	return new Promise((resolve, reject) => {
		const child = spawn("delta", args, { stdio: ["pipe", "pipe", "pipe"] });
		let stdout = "";
		let stderr = "";
		const timer = setTimeout(() => child.kill("SIGKILL"), TIMEOUT_MS);
		child.stdout.on("data", (chunk) => (stdout += chunk));
		child.stderr.on("data", (chunk) => (stderr += chunk));
		child.on("error", (error) => {
			clearTimeout(timer);
			const missing = error.message.includes("ENOENT");
			reject(
				new DeltaFailure(missing ? "`delta` was not found on PATH" : error.message, {
					retryable: false,
					code: missing ? "not_found" : "spawn",
				}),
			);
		});
		child.on("close", (code) => {
			clearTimeout(timer);
			resolve({ stdout, stderr, code });
		});
		child.stdin.end(input);
	});
}

function errorEnvelope(stdout: string): { message?: string; code?: string; retryable?: boolean; retry_after_ms?: number } | undefined {
	const lines = stdout.split("\n").filter((line) => line.trim());
	for (let i = lines.length - 1; i >= 0; i--) {
		const value = parseJson(lines[i]) as { error?: { message?: string; code?: string; retryable?: boolean; retry_after_ms?: number } } | undefined;
		if (value && typeof value === "object" && !Array.isArray(value) && value.error) return value.error;
	}
	return undefined;
}

function asFailure(args: string[], result: DeltaRun): DeltaFailure {
	if (result.code === null) {
		return new DeltaFailure(`delta ${args.slice(0, 2).join(" ")} timed out`, {
			retryable: true,
			code: "timeout",
			stdout: result.stdout,
		});
	}
	const error = errorEnvelope(result.stdout);
	const message = error?.message ?? (result.stderr.trim() || `delta ${args.slice(0, 2).join(" ")} exited ${result.code}`);
	return new DeltaFailure(explain(error?.code, message), {
		retryable: error?.retryable === true,
		code: error?.code,
		retryAfterMs: error?.retry_after_ms,
		stdout: result.stdout,
	});
}

async function json<T>(args: string[], input?: string): Promise<T> {
	const result = await run(args, input);
	if (result.code !== 0) throw asFailure(args, result);
	const value = parseJson(result.stdout);
	if (value === undefined) {
		throw new DeltaFailure(`delta ${args.slice(0, 2).join(" ")} returned no JSON`, { retryable: false });
	}
	return value as T;
}

let contractChecked = false;
let contractBlock: string | undefined;

async function ensureContract(): Promise<void> {
	if (contractChecked) {
		if (contractBlock) throw new DeltaFailure(contractBlock, { retryable: false, code: "contract" });
		return;
	}
	contractChecked = true;
	try {
		const report = await json<{ min?: number; max?: number }>(["thread", "contract", "--json"]);
		contractBlock = contractProblem(Number(report.min), Number(report.max));
	} catch (error) {
		contractBlock = `Could not read Delta's contract. ${(error as Error).message}`;
	}
	if (contractBlock) throw new DeltaFailure(contractBlock, { retryable: false, code: "contract" });
}

function clip(text: string, limit = MAX_CHARS): string {
	return text.length > limit ? `${text.slice(0, limit)}\n…truncated (${text.length} chars)` : text;
}

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
		return undefined;
	}
}

function writeBinding(key: string, binding: Binding): void {
	fs.mkdirSync(STATE_DIR, { recursive: true });
	const saved = { ...binding };
	if (!saved.paused) delete saved.paused;
	fs.writeFileSync(bindingFile(key), JSON.stringify(saved, null, 2));
}

export function recordingEnabled(key: string): boolean {
	if (process.env.PI_DELTA_RECORD === "0") return false;
	return readBinding(key)?.enabled !== false;
}

export function setRecording(key: string, enabled: boolean): void {
	const binding = readBinding(key) ?? {};
	writeBinding(key, enabled ? { ...binding, enabled, paused: undefined } : { ...binding, enabled });
}

async function ensureThread(key: string): Promise<Binding> {
	const bound = readBinding(key);
	if (bound?.threadId) return bound;
	const { organizations } = await json<{ organizations?: Org[] }>(["org", "list", "--json"]);
	const org = organizations?.find((item) => item.current) ?? organizations?.[0];
	if (!org) throw new DeltaFailure("No Delta organization. Run `delta auth login`, then retry.", { retryable: false });
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
	const binding: Binding = { ...readBinding(key), threadId: created.thread_id };
	writeBinding(key, binding);
	return binding;
}

function statusOf(key: string, threadId: string): Promise<ThreadStatus> {
	return json<ThreadStatus>(["thread", "status", "--source", SOURCE, "--external-id", key, "--json", threadId]);
}

function recordFor(sequence: number, update: Update, turnSequence?: number): StoredRecord {
	const record: StoredRecord = { schemaVersion: 1, sequence, update };
	const turn = update.sessionUpdate === "state_update" && update.state === "running" ? sequence : turnSequence;
	if (turn !== undefined) record.turnSequence = turn;
	return record;
}

/** Schema-1 JSONL continuing at `first`. Exported so tests can lock the envelope. */
export function assignSequences(first: number, updates: Update[], turnSequence?: number): string {
	return updates.map((update, index) => JSON.stringify(recordFor(first + index, update, turnSequence))).join("\n");
}

/**
 * How many leading receipts Delta confirmed. `recorded`, `duplicate`, and `fenced`
 * are confirmed; anything else stops the prefix so the tail keeps its sequence numbers.
 */
export function confirmedPrefix(
	stdout: string,
	threadId: string,
	records: StoredRecord[],
): { count: number; fenced: boolean } {
	let count = 0;
	let fenced = false;
	for (const line of stdout.split("\n")) {
		if (!line.trim() || count >= records.length) break;
		const receipt = parseJson(line) as { operation?: string; thread_id?: string; sequence?: number; disposition?: string } | undefined;
		const expected = records[count];
		const disposition = receipt?.disposition;
		if (
			!receipt ||
			receipt.operation !== "appended" ||
			receipt.thread_id !== threadId ||
			receipt.sequence !== expected.sequence ||
			(disposition !== "recorded" && disposition !== "duplicate" && disposition !== "fenced")
		) {
			break;
		}
		if (disposition === "fenced") fenced = true;
		count += 1;
	}
	return { count, fenced };
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

function corrupt(key: string): DeltaFailure {
	return new DeltaFailure(`corrupt outbox record for ${key}; sharing paused without dropping it`, {
		retryable: false,
		code: "corrupt",
	});
}

function readOutbox(key: string): { records: StoredRecord[]; legacy: Update[] } {
	let text: string;
	try {
		text = fs.readFileSync(outboxFile(key), "utf8");
	} catch {
		return { records: [], legacy: [] };
	}
	if (!text.trim()) return { records: [], legacy: [] };
	const records: StoredRecord[] = [];
	const legacy: Update[] = [];
	for (const line of text.split("\n")) {
		if (!line) continue;
		const value = parseJson(line);
		if (!value || typeof value !== "object" || Array.isArray(value)) throw corrupt(key);
		const row = value as unknown as StoredRecord & Update;
		if (row.schemaVersion === 1 && Number.isSafeInteger(row.sequence) && typeof row.update?.sessionUpdate === "string") {
			records.push({
				schemaVersion: 1,
				sequence: row.sequence,
				...(row.turnSequence !== undefined ? { turnSequence: row.turnSequence } : {}),
				update: row.update,
			});
			continue;
		}
		if (typeof row.sessionUpdate === "string" && row.schemaVersion === undefined) {
			legacy.push(row);
			continue;
		}
		throw corrupt(key);
	}
	if (records.length && legacy.length) throw corrupt(key);
	return { records, legacy };
}

function rewriteOutbox(key: string, records: StoredRecord[]): void {
	const file = outboxFile(key);
	if (!records.length) {
		fs.rmSync(file, { force: true });
		return;
	}
	fs.mkdirSync(path.dirname(file), { recursive: true });
	const fd = fs.openSync(file, "w");
	try {
		fs.writeSync(fd, `${records.map((record) => JSON.stringify(record)).join("\n")}\n`);
		fs.fsyncSync(fd);
	} finally {
		fs.closeSync(fd);
	}
}

function allocate(key: string, count: number): number {
	const binding = readBinding(key) ?? {};
	const first = binding.nextSequence ?? 1;
	writeBinding(key, { ...binding, nextSequence: first + count });
	return first;
}

/** Append numbered records to the outbox and fsync. Returns the first sequence. */
export function stageUpdates(key: string, updates: Update[], turnSequence?: number): number {
	if (!updates.length) return 0;
	const first = allocate(key, updates.length);
	const records = updates.map((update, index) => recordFor(first + index, update, turnSequence));
	const file = outboxFile(key);
	fs.mkdirSync(path.dirname(file), { recursive: true });
	const fd = fs.openSync(file, "a");
	try {
		fs.writeSync(fd, `${records.map((record) => JSON.stringify(record)).join("\n")}\n`);
		fs.fsyncSync(fd);
	} finally {
		fs.closeSync(fd);
	}
	return first;
}

export function pendingUpdates(key: string): StoredRecord[] {
	const loaded = readOutbox(key);
	if (!loaded.legacy.length) return loaded.records;
	const first = readBinding(key)?.nextSequence ?? 1;
	return loaded.legacy.map((update, index) => recordFor(first + index, update));
}

/** Number a pre-0.3 outbox once. Later retries resend this file, they do not ask status again. */
async function sealLegacy(key: string, updates: Update[]): Promise<StoredRecord[]> {
	const binding = readBinding(key) ?? {};
	let first = binding.nextSequence;
	if (first === undefined && binding.threadId) {
		const remote = (await statusOf(key, binding.threadId)).producer?.next_sequence;
		if (remote === undefined || !Number.isSafeInteger(remote)) {
			throw new DeltaFailure("Delta did not report the next sequence for this thread", { retryable: false, code: "sequence" });
		}
		first = remote;
	}
	first ??= 1;
	const records = updates.map((update, index) => recordFor(first + index, update));
	rewriteOutbox(key, records);
	writeBinding(key, { ...readBinding(key), nextSequence: first + updates.length });
	return records;
}

const retryAt = new Map<string, number>();

async function append(key: string, threadId: string, records: StoredRecord[]) {
	const args = ["thread", "append", "--source", SOURCE, "--external-id", key, "--acknowledge", "local", "--json", threadId];
	const result = await run(args, `${records.map((record) => JSON.stringify(record)).join("\n")}\n`);
	const prefix = confirmedPrefix(result.stdout, threadId, records);
	if (result.code !== 0) return { ...prefix, failure: asFailure(args, result) };
	if (prefix.count < records.length) {
		return {
			...prefix,
			failure: new DeltaFailure("Delta confirmed only part of the batch", { retryable: true, code: "short_read", stdout: result.stdout }),
		};
	}
	return { ...prefix, failure: undefined as DeltaFailure | undefined };
}

/** Send the outbox. Drops only the receipt-confirmed prefix; the tail keeps its sequence numbers. */
export async function flushPending(key: string): Promise<FlushResult> {
	try {
		return await flushInner(key);
	} catch (error) {
		if (error instanceof DeltaFailure && !error.retryable && error.code !== "paused" && error.code !== "contract") {
			writeBinding(key, { ...readBinding(key), paused: error.message });
		}
		throw error;
	}
}

async function flushInner(key: string): Promise<FlushResult> {
	await ensureContract();
	const binding = readBinding(key);
	if (binding?.paused) throw new DeltaFailure(binding.paused, { retryable: false, code: "paused" });
	const wait = retryAt.get(key) ?? 0;
	if (Date.now() < wait) {
		throw new DeltaFailure("Delta asked to retry later", { retryable: true, code: "retry_after", retryAfterMs: wait - Date.now() });
	}
	const loaded = readOutbox(key);
	const records = loaded.legacy.length ? await sealLegacy(key, loaded.legacy) : loaded.records;
	if (!records.length) return { count: 0 };
	const bound = await ensureThread(key);
	if (!bound.threadId) throw new DeltaFailure("Delta thread was not created", { retryable: false });
	const result = await append(key, bound.threadId, records);
	if (result.count) rewriteOutbox(key, records.slice(result.count));
	if (result.fenced) {
		const failure = result.failure ?? new DeltaFailure("This turn was taken over in Delta.", { retryable: true, code: "fenced" });
		failure.fenced = true;
		throw failure;
	}
	if (result.failure) {
		if (result.failure.retryable && result.failure.retryAfterMs) {
			retryAt.set(key, Date.now() + result.failure.retryAfterMs);
		}
		throw result.failure;
	}
	retryAt.delete(key);
	let url: string | undefined;
	try {
		url = (await statusOf(key, bound.threadId)).url;
	} catch {
		// The records are already confirmed. The URL is only for the tool reply.
	}
	return {
		count: result.count,
		threadId: bound.threadId,
		url,
		nextSequence: readBinding(key)?.nextSequence ?? records.at(-1)!.sequence + 1,
	};
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

function outboxPending(key: string): boolean {
	try {
		return fs.statSync(outboxFile(key)).size > 0;
	} catch {
		return false;
	}
}

/** Drop old bindings whose session file is gone. A non-empty outbox is unsent work, so it stays. */
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
		if (outboxPending(key)) continue;
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

function stageMessage(key: string, message: FinalMessage, id: string, turnSequence?: number): number {
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
	stageUpdates(key, updates, turnSequence);
	return updates.length;
}

export default function (pi: ExtensionAPI) {
	let queue: Promise<unknown> = Promise.resolve();
	function enqueue<T>(task: () => Promise<T>): Promise<T> {
		const queued = queue.then(task, task);
		queue = queued.catch(() => {});
		return queued;
	}

	let rec: { key: string; enabled: boolean; warned: boolean } | null = null;
	let watchChild: ChildProcess | null = null;
	let watchTimer: ReturnType<typeof setTimeout> | undefined;
	let watchFailures = 0;
	let shuttingDown = false;
	let openTurn: { key: string; turnSequence: number } | null = null;

	const sessionOf = (ctx: ExtensionContext) =>
		(rec ??= { key: sessionKey(ctx), enabled: recordingEnabled(sessionKey(ctx)), warned: false });

	function stopTurn(ctx: ExtensionContext): void {
		ctx.abort();
		ctx.ui.notify("Delta turn was taken over; stopped this turn.", "warning");
	}

	function handleError(error: unknown, ctx: ExtensionContext, current: { warned: boolean }): void {
		if (error instanceof DeltaFailure && error.fenced) {
			stopTurn(ctx);
			return;
		}
		if (current.warned) return;
		current.warned = true;
		if (error instanceof DeltaFailure && !error.retryable) {
			ctx.ui.notify(`Delta recording paused. ${error.message} Run /delta on after fixing it.`, "error");
			return;
		}
		ctx.ui.notify(`Delta unreachable; queued records will retry. ${(error as Error).message}`, "warning");
	}

	function startWatch(key: string, threadId: string, ctx: ExtensionContext): void {
		if (watchChild || shuttingDown || contractBlock) return;
		// Watch is already JSONL. `--json` makes Delta print one result and disconnect.
		const child = spawn(
			"delta",
			["thread", "watch", threadId, "--source", SOURCE, "--external-id", key, "--participate", PRODUCER],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		watchChild = child;
		let buf = "";
		child.stdout?.on("data", (chunk: Buffer) => {
			watchFailures = 0;
			buf += chunk.toString();
			let nl = buf.indexOf("\n");
			while (nl >= 0) {
				const event = parseJson(buf.slice(0, nl)) as { type?: string; status?: string } | undefined;
				buf = buf.slice(nl + 1);
				if (event?.type === "producer.turn" && event.status === "superseded") stopTurn(ctx);
				nl = buf.indexOf("\n");
			}
		});
		child.on("error", () => {});
		child.on("close", () => {
			if (watchChild === child) watchChild = null;
			if (shuttingDown) return;
			watchFailures += 1;
			if (watchFailures > 3) {
				ctx.ui.notify("Delta watch stopped; a takeover in the app will not stop this turn until restart.", "warning");
				return;
			}
			watchTimer = setTimeout(() => startWatch(key, threadId, ctx), 2000);
		});
	}

	async function flushAndWatch(key: string, ctx: ExtensionContext): Promise<void> {
		const flushed = await flushPending(key);
		const threadId = flushed.threadId ?? readBinding(key)?.threadId;
		if (threadId) startWatch(key, threadId, ctx);
	}

	pi.on("session_start", (_event, ctx) => {
		const key = sessionKey(ctx);
		rec = { key, enabled: recordingEnabled(key), warned: false };
		pruneBindings(key, SESSIONS_DIR);
		enqueue(() => flushAndWatch(key, ctx)).catch((error: unknown) => {
			if (rec) handleError(error, ctx, rec);
		});
	});

	pi.on("turn_start", (_event, ctx) => {
		const current = sessionOf(ctx);
		if (!current.enabled || contractBlock || readBinding(current.key)?.paused) return;
		const sequence = stageUpdates(current.key, [{ sessionUpdate: "state_update", state: "running" }]);
		openTurn = { key: current.key, turnSequence: sequence };
		enqueue(() => flushAndWatch(current.key, ctx)).catch((error: unknown) => handleError(error, ctx, current));
	});

	pi.on("turn_end", (_event, ctx) => {
		const current = sessionOf(ctx);
		const turn = openTurn;
		openTurn = null;
		if (!turn || turn.key !== current.key) return;
		enqueue(async () => {
			stageUpdates(current.key, [{ sessionUpdate: "state_update", state: "idle" }], turn.turnSequence);
			await flushAndWatch(current.key, ctx);
		}).catch((error: unknown) => handleError(error, ctx, current));
	});

	pi.on("message_end", (event, ctx) => {
		const current = sessionOf(ctx);
		if (!current.enabled || contractBlock) return;
		const message = event.message as FinalMessage;
		if (message?.role !== "user" && message?.role !== "assistant" && message?.role !== "toolResult") return;
		const id = randomUUID();
		const turnSequence = openTurn?.key === current.key ? openTurn.turnSequence : undefined;
		enqueue(async () => {
			try {
				stageMessage(current.key, message, id, turnSequence);
				await flushAndWatch(current.key, ctx);
				if (current.warned) {
					current.warned = false;
					ctx.ui.notify("Delta recording resumed", "info");
				}
			} catch (error) {
				handleError(error, ctx, current);
			}
		});
	});

	pi.on("session_shutdown", async () => {
		shuttingDown = true;
		if (watchTimer) clearTimeout(watchTimer);
		watchChild?.kill("SIGTERM");
		await Promise.race([queue, new Promise((resolve) => setTimeout(resolve, 8000))]);
	});

	pi.registerTool({
		name: "delta_store",
		label: "Delta Store",
		description:
			"Store text in this session's private Delta thread. Creates the thread on first use and appends a recorded message that the Delta app can open and share.",
		parameters: Type.Object({
			text: Type.String({ description: "Text to store" }),
			role: Type.Optional(StringEnum(["assistant", "user"] as const, { description: "Message author (default assistant)" })),
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
				return flushPending(key);
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
				const threads = await json<ThreadListItem[]>(["thread", "search", "-n", "20", "--json", "--", ...words]);
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

	pi.registerCommand("delta", {
		description: "Show or toggle this session's Delta recording",
		handler: async (args, ctx) => {
			const current = sessionOf(ctx);
			const arg = args.trim().toLowerCase();
			if (arg === "on" || arg === "off") {
				current.enabled = arg === "on";
				setRecording(current.key, current.enabled);
				if (current.enabled) {
					current.warned = false;
					enqueue(() => flushAndWatch(current.key, ctx)).catch((error: unknown) => handleError(error, ctx, current));
				}
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
			if (contractBlock) lines.push(contractBlock);
			if (binding?.paused) lines.push(`Paused: ${binding.paused}`);
			if (binding?.threadId && !binding.paused) {
				const status = await statusOf(current.key, binding.threadId);
				if (status.title) lines.push(`Title: ${status.title}`);
				if (status.url) lines.push(status.url);
			}
			ctx.ui.notify(lines.join("\n"), "info");
		},
	});
}
