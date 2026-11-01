import assert from "node:assert/strict";
import test from "node:test";
import { assignSequences, updatesFromMessage } from "../index.ts";

test("user and assistant text become unique message records", () => {
	const user = updatesFromMessage({ role: "user", content: "hello" }, "m1");
	const agent = updatesFromMessage({ role: "assistant", content: [{ type: "text", text: "hi" }] }, "m2");
	assert.equal(user[0].sessionUpdate, "user_message");
	assert.equal(agent[0].sessionUpdate, "agent_message");
	assert.notEqual(user[0].messageId, agent[0].messageId);
	assert.equal(user[0].content[0].text, "hello");
});

test("thinking and tool calls are recorded, and a result closes the call", () => {
	const turn = updatesFromMessage(
		{
			role: "assistant",
			content: [
				{ type: "thinking", thinking: "look first" },
				{ type: "toolCall", id: "call-1", name: "bash", arguments: { command: "ls" } },
				{ type: "text", text: "done" },
			],
		},
		"m3",
	);
	assert.deepEqual(
		turn.map((update) => update.sessionUpdate),
		["agent_thought", "tool_call_update", "agent_message"],
	);
	assert.equal(turn[1].toolCallId, "call-1");
	assert.equal(turn[1].status, "in_progress");
	assert.deepEqual(turn[1].rawInput, { command: "ls" });

	const result = updatesFromMessage(
		{ role: "toolResult", toolCallId: "call-1", toolName: "bash", content: [{ type: "text", text: "a.txt" }], isError: false },
		"m4",
	);
	assert.equal(result[0].status, "completed");
	assert.equal(result[0].rawOutput, "a.txt");
	assert.equal(
		updatesFromMessage({ role: "toolResult", toolCallId: "call-1", toolName: "bash", isError: true }, "m5")[0].status,
		"failed",
	);
});

test("sequences continue from the producer's next sequence", () => {
	const lines = assignSequences(3, [{ sessionUpdate: "user_message" }, { sessionUpdate: "agent_message" }])
		.split("\n")
		.map((line) => JSON.parse(line));
	assert.deepEqual(
		lines.map((line) => line.sequence),
		[3, 4],
	);
	assert.equal(lines[0].schemaVersion, 1);
	assert.equal(lines[1].update.sessionUpdate, "agent_message");
});
