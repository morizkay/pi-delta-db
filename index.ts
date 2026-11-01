/**
 * pi-delta-db — store and read Pi data through the Delta CLI (`delta`).
 *
 * Delta's SQLite store (`delta db ...`) is internal and read-only from outside,
 * so writes go through Delta's supported path: a private thread bound to this
 * session (`--source pi --external-id <session>`) recorded with schema-1 JSONL.
 */

import { spawn } from "node:child_process";
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

type Json = Record<string, any>;

function delta(args: string[], input?: string): Promise<any> {
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
			if (code !== 0) {
				let message = err.trim();
				try {
					message = JSON.parse(out).error?.message ?? message;
				} catch {}
				reject(new Error(message || `delta ${args.slice(0, 2).join(" ")} exited ${code}`));
				return;
			}
			try {
				resolve(JSON.parse(out));
			} catch {
				resolve(out.trim());
			}
		});
		child.stdin.end(input);
	});
}

function clip(text: string, limit = MAX_CHARS): string {
	return text.length > limit ? `${text.slice(0, limit)}\n…truncated (${text.length} chars)` : text;
}

/** A stable producer key for this Pi session; also the on-disk binding name. */
function sessionKey(ctx: ExtensionContext): string {
	const manager = ctx.sessionManager as any;
	const raw = manager?.getSessionId?.() ?? manager?.getSessionFile?.() ?? `cwd-${ctx.cwd}`;
	const tail = String(raw).split(/[\\/]/).pop() ?? "session";
	return tail.replace(/[^A-Za-z0-9_-]/g, "-");
}

function bindingFile(key: string) {
	return path.join(STATE_DIR, `${key}.json`);
}

async function ensureThread(key: string): Promise<string> {
	try {
		const saved = JSON.parse(fs.readFileSync(bindingFile(key), "utf8"));
		if (saved.threadId) return saved.threadId;
	} catch {}
	const orgs = await delta(["org", "list", "--json"]);
	const org = orgs.organizations?.find((item: Json) => item.current) ?? orgs.organizations?.[0];
	if (!org) throw new Error("No Delta organization. Run `delta auth login`, then retry.");
	const created = await delta([
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
	fs.mkdirSync(STATE_DIR, { recursive: true });
	fs.writeFileSync(bindingFile(key), JSON.stringify({ threadId: created.thread_id }, null, 2));
	return created.thread_id;
}

async function statusOf(key: string, threadId: string): Promise<Json> {
	return delta(["thread", "status", "--source", SOURCE, "--external-id", key, "--json", threadId]);
}

/** Append schema-1 records, continuing this producer's sequence. */
async function append(key: string, threadId: string, updates: Json[]) {
	const status = await statusOf(key, threadId);
	let sequence = status.producer?.next_sequence;
	if (!Number.isSafeInteger(sequence)) throw new Error("Delta did not report the next sequence for this thread");
	const lines = updates
		.map((update) => JSON.stringify({ schemaVersion: 1, sequence: sequence++, update }))
		.join("\n");
	await delta(
		["thread", "append", "--source", SOURCE, "--external-id", key, "--acknowledge", "local", "--json", threadId],
		`${lines}\n`,
	);
	return { count: updates.length, nextSequence: sequence, url: status.url as string | undefined };
}

function summarizeList(threads: Json[]): string {
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
			const key = sessionKey(ctx);
			const threadId = await ensureThread(key);
			const updates: Json[] = [];
			if (params.title) updates.push({ sessionUpdate: "session_info_update", title: params.title });
			updates.push({
				sessionUpdate: params.role === "user" ? "user_message" : "agent_message",
				messageId: `pi:${key}:${Date.now()}`,
				content: [{ type: "text", text: params.text }],
			});
			const result = await append(key, threadId, updates);
			const where = result.url ? ` ${result.url}` : "";
			return {
				content: [
					{
						type: "text",
						text: `Stored ${result.count} record(s) in Delta thread ${threadId} (next sequence ${result.nextSequence}).${where}`,
					},
				],
				details: { threadId, ...result },
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
				const threads = await delta(["thread", "list", "--json"]);
				return {
					content: [{ type: "text", text: summarizeList(threads) }],
					details: { action: "list", count: threads.length },
				};
			}
			if (params.action === "search") {
				if (!params.query?.trim()) throw new Error("query is required for search");
				const words = params.query.trim().split(/\s+/);
				const threads = await delta(["thread", "search", "-n", "20", "--json", ...words]);
				return {
					content: [{ type: "text", text: summarizeList(threads) }],
					details: { action: "search", query: params.query, count: threads.length },
				};
			}
			const key = sessionKey(ctx);
			let thread = params.thread?.trim();
			if (!thread) {
				try {
					thread = JSON.parse(fs.readFileSync(bindingFile(key), "utf8")).threadId;
				} catch {}
			}
			if (!thread) throw new Error("thread is required for show (this session has no Delta thread yet)");
			const [shown] = await delta(["thread", "show", thread, "--format", "summarize", "--json"]);
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
				const stats = await delta(["db", "stats", "--json"]);
				const types = (stats.node_types ?? [])
					.map((item: Json) => `${item.type}: ${item.count} nodes, ${item.compressed_bytes} B`)
					.join("\n");
				const roots = (stats.roots ?? []).map((item: Json) => `${item.id} ${item.type}`).join("\n");
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
			const decoded = await delta(["db", "root", params.id, "--json"]);
			return {
				content: [{ type: "text", text: clip(JSON.stringify(decoded, null, 2)) }],
				details: { action: "root", id: params.id, type: decoded?.type },
			};
		},
	});

	pi.registerCommand("delta", {
		description: "Show this session's Delta thread",
		handler: async (_args, ctx) => {
			const key = sessionKey(ctx);
			let threadId: string | undefined;
			try {
				threadId = JSON.parse(fs.readFileSync(bindingFile(key), "utf8")).threadId;
			} catch {}
			if (!threadId) {
				ctx.ui.notify("No Delta thread for this session yet — ask Pi to delta_store something.", "info");
				return;
			}
			const status = await statusOf(key, threadId);
			ctx.ui.notify(`${status.title ?? threadId}\n${status.url ?? ""}`, "info");
		},
	});
}
