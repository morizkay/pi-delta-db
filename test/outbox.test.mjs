import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-delta-state-"));
const fakeDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-delta-fake-"));
process.env.PI_DELTA_STATE = stateDir;
process.env.PI_FAKE_DELTA_DIR = fakeDir;
process.env.PATH = `${path.resolve("test/bin")}${path.delimiter}${process.env.PATH}`;

const { flushPending, pendingUpdates, pruneBindings, setRecording, recordingEnabled, stageUpdates } = await import(
	"../index.ts"
);

function log() {
	return JSON.parse(fs.readFileSync(path.join(fakeDir, "state.json"), "utf8")).log;
}

test("a failed append stays queued and a retry continues the sequence", async () => {
	stageUpdates("sess", [{ sessionUpdate: "user_message", messageId: "sess:1", content: [{ type: "text", text: "one" }] }]);
	process.env.PI_FAKE_DELTA_FAIL = "1";
	await assert.rejects(() => flushPending("sess"), /fake down/);
	assert.equal(pendingUpdates("sess").length, 1);
	delete process.env.PI_FAKE_DELTA_FAIL;

	const first = await flushPending("sess");
	assert.equal(first.count, 1);
	assert.equal(first.threadId, "thread_test");
	assert.equal(pendingUpdates("sess").length, 0);

	stageUpdates("sess", [{ sessionUpdate: "agent_message", messageId: "sess:2", content: [{ type: "text", text: "two" }] }]);
	await flushPending("sess");
	const records = log();
	assert.deepEqual(
		records.map((record) => record.sequence),
		[1, 2],
	);
	assert.deepEqual(
		records.map((record) => record.update.messageId),
		["sess:1", "sess:2"],
	);
});

test("recording off is remembered, and a week-old binding with no session file is pruned", () => {
	setRecording("sess", false);
	assert.equal(recordingEnabled("sess"), false);
	setRecording("sess", true);
	assert.equal(recordingEnabled("sess"), true);

	const sessions = fs.mkdtempSync(path.join(os.tmpdir(), "pi-sessions-"));
	fs.writeFileSync(path.join(sessions, "keep_alive.jsonl"), "");
	const old = path.join(stateDir, "dead.json");
	const young = path.join(stateDir, "young.json");
	const alive = path.join(stateDir, "alive.json");
	fs.writeFileSync(old, "{}");
	fs.writeFileSync(young, "{}");
	fs.writeFileSync(alive, "{}");
	const weekAgo = new Date(Date.now() - 8 * 24 * 60 * 60 * 1000);
	fs.utimesSync(old, weekAgo, weekAgo);
	fs.utimesSync(alive, weekAgo, weekAgo);

	const removed = pruneBindings("sess", sessions);
	assert.deepEqual(removed, ["dead"]);
	assert.equal(fs.existsSync(young), true);
	assert.equal(fs.existsSync(alive), true);
	assert.equal(fs.existsSync(path.join(stateDir, "sess.json")), true);
});
