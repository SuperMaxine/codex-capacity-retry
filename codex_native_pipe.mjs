/** Experimental adapter for the desktop's existing local IPC router.
 * No app-server is launched, no window is controlled, no user text is submitted.
 * Protocol verified against Codex Desktop 26.915.4065.0; not a public API.
 */
import net from "node:net";
import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";

const MAX_FRAME = 64 * 1024 * 1024;
export const DEFAULT_PIPE = "\\\\.\\pipe\\codex-ipc";

export class NativeError extends Error {
  constructor(code, message = code, dispatched = false) {
    super(message);
    this.code = code;
    this.dispatched = dispatched;
  }
}

export function isCapacity(error) {
  const message = String(error?.message ?? "").toLowerCase().replace(/\s+/g, " ").trim();
  return /^selected model is at capacity(?:[.!]|$)/.test(message);
}

export function orderedTurns(state) {
  if (state.turnHistory?.kind === "canonical") {
    const history = state.turnHistory.history;
    const tails = history?.islands?.filter(i => i.newerBoundary?.status === "exhausted");
    if (tails?.length !== 1 || !Array.isArray(tails[0].entries)) {
      throw new NativeError("unsupported_history", "Cannot identify the current history tail.");
    }
    return tails[0].entries.map(entry => {
      const turn = history.entitiesByKey?.[entry.value];
      if (!turn || typeof turn.status !== "string") {
        throw new NativeError("unsupported_history", "Unknown history entity format.");
      }
      return turn;
    });
  }
  if (state.turnHistory != null || !Array.isArray(state.turns)) {
    throw new NativeError("unsupported_history");
  }
  return state.turns;
}

export function compactState(state, ownerClientId) {
  const turns = orderedTurns(state);
  const turn = turns.at(-1);
  return {
    id: state.id, hostId: state.hostId, title: state.title,
    ownerClientId, source: state.source, originator: state.originator,
    resumeState: state.resumeState, status: state.threadRuntimeStatus,
    pendingRequests: Array.isArray(state.requests) ? state.requests.length : null,
    unconfirmedSubmissions: state.unconfirmedTurnSubmissions?.length ?? 0,
    goalStatus: state.threadGoal?.status ?? null,
    latestTurn: turn ? {
      id: turn.turnId, status: turn.status, error: turn.error,
      inputCount: turn.params?.input?.length ?? null,
      trigger: turn.params?.turnTrigger ?? null,
      model: turn.params?.model ?? null,
      startedAtMs: turn.turnStartedAtMs, durationMs: turn.durationMs,
      itemCount: turn.items?.length ?? 0,
      lastItems: (turn.items ?? []).slice(-3).map(i => ({ id: i.id, type: i.type, status: i.status })),
    } : null,
  };
}

export function validateRetry(snapshot, threadId, expectedTurn) {
  if (snapshot.id !== threadId || snapshot.hostId !== "local") throw new NativeError("task_mismatch");
  if (snapshot.originator !== "Codex Desktop" || typeof snapshot.source !== "string" ||
      snapshot.source.includes("subagent")) throw new NativeError("not_desktop_main_task");
  if (snapshot.resumeState !== "resumed" || snapshot.status?.type !== "systemError" ||
      (snapshot.status.activeFlags?.length ?? 0) !== 0) throw new NativeError("not_capacity_paused");
  if (snapshot.pendingRequests !== 0 || snapshot.unconfirmedSubmissions !== 0) {
    throw new NativeError("pending_request_or_unknown_submission");
  }
  if (snapshot.goalStatus != null && snapshot.goalStatus !== "active") throw new NativeError("goal_not_active");
  const turn = snapshot.latestTurn;
  if (!expectedTurn || turn?.id !== expectedTurn || turn.status !== "failed" || !isCapacity(turn.error)) {
    throw new NativeError("failed_turn_changed");
  }
}

export function retryPayload(threadId) {
  // Omit model/permissions/context overrides so the existing desktop manager
  // prepares the turn with its own inherited settings and writing context.
  return {
    conversationId: threadId,
    turnStart: { request: { threadId, input: [], turnTrigger: "capacity_retry_automatic" } },
  };
}

export class DesktopIPC {
  constructor(endpoint = DEFAULT_PIPE, timeoutMs = 15000) {
    this.endpoint = endpoint;
    this.timeoutMs = timeoutMs;
    this.clientId = "initializing-client";
    this.pending = new Map();
    this.snapshots = new Map();
    this.buffer = Buffer.alloc(0);
    this.socket = null;
  }

  async connect() {
    await new Promise((resolve, reject) => {
      const socket = this.socket = net.createConnection(this.endpoint);
      const timer = setTimeout(() => socket.destroy(new Error("connect timeout")), this.timeoutMs);
      socket.once("connect", () => { clearTimeout(timer); resolve(); });
      socket.on("error", () => { clearTimeout(timer); reject(new NativeError("ipc_unavailable", "Desktop IPC unavailable; keep Codex open.")); });
      socket.on("close", () => this.failAll(new NativeError("ipc_closed")));
      socket.on("data", data => {
        try { this.receive(data); }
        catch (error) { this.failAll(error); socket.destroy(); }
      });
    });
    const response = await this.request("initialize", { clientType: "capacity-retry-supervisor" }, 0);
    if (response.resultType !== "success" || !response.result?.clientId) throw new NativeError("initialize_failed");
    this.clientId = response.result.clientId;
  }

  failAll(error) {
    for (const pending of [...this.pending.values(), ...this.snapshots.values()]) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
    this.snapshots.clear();
  }

  write(message) {
    if (!this.socket || this.socket.destroyed || !this.socket.writable) throw new NativeError("ipc_closed");
    const body = Buffer.from(JSON.stringify(message));
    const header = Buffer.alloc(4);
    header.writeUInt32LE(body.length);
    this.socket.write(Buffer.concat([header, body]));
  }

  receive(data) {
    this.buffer = Buffer.concat([this.buffer, data]);
    while (this.buffer.length >= 4) {
      const length = this.buffer.readUInt32LE(0);
      if (!length || length > MAX_FRAME) throw new NativeError("frame_too_large_or_invalid");
      if (this.buffer.length < length + 4) return;
      const message = JSON.parse(this.buffer.subarray(4, length + 4).toString("utf8"));
      this.buffer = this.buffer.subarray(length + 4);
      this.handle(message);
    }
  }

  handle(message) {
    if (message.type === "response") {
      const pending = this.pending.get(message.requestId);
      if (!pending) return;
      clearTimeout(pending.timer);
      this.pending.delete(message.requestId);
      pending.resolve(message);
    } else if (message.type === "client-discovery-request") {
      // This client never claims to own tasks or handle desktop requests.
      this.write({ type: "client-discovery-response", requestId: message.requestId, response: { canHandle: false } });
    } else if (message.type === "broadcast" && message.method === "thread-stream-state-changed") {
      const params = message.params;
      const key = `${params?.hostId}:${params?.conversationId}`;
      const pending = this.snapshots.get(key);
      if (!pending || message.sourceClientId !== pending.owner || params.change?.type !== "snapshot") return;
      clearTimeout(pending.timer);
      this.snapshots.delete(key);
      try {
        if (message.version !== 11) throw new NativeError("snapshot_version_changed");
        if (params.change.conversationState?.id !== params.conversationId ||
            params.change.conversationState?.hostId !== params.hostId) throw new NativeError("task_mismatch");
        pending.resolve(compactState(params.change.conversationState, pending.owner));
      } catch (error) { pending.reject(error); }
    }
  }

  request(method, params, version, targetClientId) {
    const requestId = randomUUID();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(requestId);
        reject(new NativeError("ipc_timeout"));
      }, this.timeoutMs);
      this.pending.set(requestId, { resolve, reject, timer });
      try {
        this.write({ type: "request", requestId, sourceClientId: this.clientId,
          method, params, version, targetClientId, timeoutMs: this.timeoutMs - 1000 });
      } catch (error) {
        clearTimeout(timer);
        this.pending.delete(requestId);
        reject(error);
      }
    });
  }

  follow(threadId, owner, following) {
    this.write({ type: "broadcast", method: "thread-stream-following-changed", version: 1,
      sourceClientId: this.clientId, targetClientIds: [owner],
      params: { hostId: "local", conversationId: threadId, following } });
  }

  async snapshot(threadId) {
    const discovery = await this.request("thread-owner-discovery", { hostId: "local", conversationId: threadId }, 1);
    if (discovery.resultType !== "success" || !discovery.handledByClientId) {
      throw new NativeError("no_live_owner", "Task has no reachable desktop owner; not resumed.");
    }
    const owner = discovery.handledByClientId;
    const key = `local:${threadId}`;
    if (this.snapshots.has(key)) throw new NativeError("snapshot_already_pending");
    try {
      return await new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          this.snapshots.delete(key);
          reject(new NativeError("snapshot_timeout"));
        }, this.timeoutMs);
        this.snapshots.set(key, { owner, resolve, reject, timer });
        try { this.follow(threadId, owner, true); }
        catch (error) { clearTimeout(timer); this.snapshots.delete(key); reject(error); }
      });
    } finally {
      try { this.follow(threadId, owner, false); } catch { /* Socket close removes subscription. */ }
    }
  }

  async retry(threadId, expectedTurn) {
    const before = await this.snapshot(threadId);
    validateRetry(before, threadId, expectedTurn);
    // No await between the final guard and the write. The internal protocol
    // has no expected-failed-turn CAS, so a very small manual-action race remains.
    let response;
    try {
      response = await this.request("thread-follower-start-turn", retryPayload(threadId), 2, before.ownerClientId);
    } catch (error) {
      throw new NativeError("outcome_unknown", "Retry dispatched but result unavailable; do not resend this failed turn.", true);
    }
    if (response.resultType !== "success") {
      throw new NativeError("native_retry_error", String(response.error ?? "Unknown native error").slice(0,500), true);
    }
    return { dispatched: true, inputCount: 0, before,
      result: { status: response.result?.result?.status ?? null,
        turnId: response.result?.result?.turn?.id ?? response.result?.result?.turnId ?? null } };
  }

  close() {
    this.failAll(new NativeError("client_closed"));
    this.socket?.destroy();
  }
}

async function serve() {
  const client = new DesktopIPC();
  const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
  // Attach the iterator before connecting so an early stdin command is queued.
  const commands = lines[Symbol.asyncIterator]();
  try {
    await client.connect();
    for await (const line of commands) {
      let command;
      try {
        command = JSON.parse(line);
        let result;
        if (command.command === "info") result = { connected: true, clientId: client.clientId, protocol: "desktop-local-ipc-experimental" };
        else if (command.command === "snapshot") result = await client.snapshot(command.threadId);
        else if (command.command === "retry") result = await client.retry(command.threadId, command.expectedTurn);
        else throw new NativeError("unknown_command");
        process.stdout.write(JSON.stringify({ id: command.id, result }) + "\n");
      } catch (error) {
        process.stdout.write(JSON.stringify({ id: command?.id, error: {
          code: error.code ?? "adapter_error", message: error.message, dispatched: error.dispatched === true,
        } }) + "\n");
      }
    }
  } catch (error) {
    process.stdout.write(JSON.stringify({ fatal: { code: error.code ?? "adapter_error", message: error.message } }) + "\n");
    process.exitCode = 1;
  } finally { lines.close(); client.close(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  if (process.argv[2] !== "--stdio") {
    console.error("Internal adapter. Run codex_native_retry.py instead.");
    process.exitCode = 2;
  } else await serve();
}
