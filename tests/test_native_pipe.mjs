import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DesktopIPC, compactState, isCapacity, orderedTurns, retryPayload, validateRetry } from "../codex_native_pipe.mjs";

const capacity = { message: "Selected model is at capacity. Please try a different model.", codexErrorInfo: "serverOverloaded" };
function state() {
  return { id: "target", hostId: "local", title: "Test task", source: "vscode", originator: "Codex Desktop",
    resumeState: "resumed", requests: [], threadRuntimeStatus: { type: "systemError" },
    turns: [{ turnId: "failed", status: "failed", error: capacity, params: { input: [] }, items: [] }] };
}

test("retry payload is empty and does not override settings or insert user prose", () => {
  assert.deepEqual(retryPayload("target"), { conversationId: "target", turnStart: {
    request: { threadId: "target", input: [], turnTrigger: "capacity_retry_automatic" },
  } });
});

test("only exact capacity errors count", () => {
  assert.equal(isCapacity(capacity), true);
  assert.equal(isCapacity({ message: "Tool output says: " + capacity.message }), false);
  assert.equal(isCapacity({ message: "Network error" }), false);
});

test("canonical tail ordering overrides entity insertion order", () => {
  const s = state();
  s.turns = [];
  s.turnHistory = { kind: "canonical", history: {
    entitiesByKey: { newer: { turnId: "new", status: "completed" }, older: { turnId: "old", status: "failed" } },
    islands: [{ newerBoundary: { status: "exhausted" }, entries: [{ value: "older" }, { value: "newer" }] }],
  } };
  assert.equal(compactState(s, "owner").latestTurn.id, "new");
  s.turnHistory.history.islands[0].newerBoundary.status = "unloaded";
  assert.throws(() => orderedTurns(s), /current history tail/);
});

test("compact snapshot does not disclose tool output or user prose", () => {
  const s = state();
  s.turns[0].items = [{ id: "i", type: "commandExecution", status: "completed", output: "secret" }];
  s.turns[0].params.input = [{ type: "text", text: "secret" }];
  assert.equal(JSON.stringify(compactState(s, "owner")).includes("secret"), false);
});

test("guards reject active, stopped, changed, approval, subagent and unknown submission", () => {
  validateRetry(compactState(state(), "owner"), "target", "failed");
  const variants = [
    s => { s.threadRuntimeStatus.type = "active"; },
    s => { s.turns[0].status = "interrupted"; },
    s => { s.turns[0].turnId = "different"; },
    s => { s.requests = [{}]; },
    s => { s.unconfirmedTurnSubmissions = [{}]; },
    s => { s.source = '{"subagent":{}}'; },
    s => { s.threadGoal = { status: "paused" }; },
  ];
  for (const change of variants) {
    const s = state(); change(s);
    assert.throws(() => validateRetry(compactState(s, "owner"), "target", "failed"));
  }
});

async function mockDesktop(t, options = {}) {
  const endpoint = process.platform === "win32" ? `\\\\.\\pipe\\capacity-retry-test-${randomUUID()}` : join(tmpdir(), `retry-${randomUUID()}.sock`);
  const requests = [];
  const sockets = new Set();
  const server = net.createServer(socket => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    let buffer = Buffer.alloc(0);
    const send = message => {
      const body = Buffer.from(JSON.stringify(message)), header = Buffer.alloc(4);
      header.writeUInt32LE(body.length);
      // Split the length prefix and body to exercise partial frame handling.
      socket.write(header.subarray(0, 2));
      socket.write(Buffer.concat([header.subarray(2), body]));
    };
    socket.on("data", data => {
      buffer = Buffer.concat([buffer, data]);
      while (buffer.length >= 4 && buffer.length >= buffer.readUInt32LE(0) + 4) {
        const length = buffer.readUInt32LE(0);
        const m = JSON.parse(buffer.subarray(4, length + 4));
        buffer = buffer.subarray(length + 4);
        requests.push(m);
        if (m.type === "request") {
          let result = {};
          if (m.method === "initialize") result = { clientId: "unique-client" };
          if (m.method === "thread-follower-start-turn") {
            if (options.disconnectOnRetry) { socket.destroy(); continue; }
            result = { result: { turn: { id: "new-turn" } } };
          }
          send({ type: "response", requestId: m.requestId, method: m.method,
            resultType: "success", handledByClientId: "desktop-owner", result });
        }
        if (m.method === "thread-stream-following-changed" && m.params.following) {
          const s = state();
          if (options.active) s.threadRuntimeStatus.type = "active";
          send({ type: "broadcast", method: "thread-stream-state-changed", version: 11, sourceClientId: "desktop-owner",
            params: { hostId: "local", conversationId: "target", change: { type: "snapshot", revision: 1, conversationState: s } } });
        }
      }
    });
  });
  await new Promise(resolve => server.listen(endpoint, resolve));
  const client = new DesktopIPC(endpoint, 2000);
  t.after(async () => {
    client.close();
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => server.close(resolve));
  });
  await client.connect();
  return { client, requests };
}

test("real framing and owner routing: final recheck then one empty start", async t => {
  const { client, requests } = await mockDesktop(t);
  const result = await client.retry("target", "failed");
  assert.equal(result.result.turnId, "new-turn");
  const writes = requests.filter(r => r.method === "thread-follower-start-turn");
  assert.equal(writes.length, 1);
  assert.equal(writes[0].targetClientId, "desktop-owner");
  assert.equal(writes[0].sourceClientId, "unique-client");
  assert.equal(writes[0].version, 2);
  assert.deepEqual(writes[0].params, retryPayload("target"));
});

test("changed live state sends no retry", async t => {
  const { client, requests } = await mockDesktop(t, { active: true });
  await assert.rejects(client.retry("target", "failed"), error => error.dispatched === false);
  assert.equal(requests.some(r => r.method === "thread-follower-start-turn"), false);
});

test("disconnect after dispatch is explicitly unknown", async t => {
  const { client } = await mockDesktop(t, { disconnectOnRetry: true });
  await assert.rejects(client.retry("target", "failed"), error => error.code === "outcome_unknown" && error.dispatched === true);
});
