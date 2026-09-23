"""Watch local Codex Desktop capacity failures and retry with empty input.

Python 3.11+ standard library + Node.js 20+. No pip dependencies are needed.
Default: read-only dry-run. Use --execute to enable native-like retries.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import queue
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from typing import Any


class RetryError(RuntimeError):
    def __init__(self, message: str, code: str = "error", dispatched: bool = False):
        super().__init__(message)
        self.code = code
        self.dispatched = dispatched


def is_capacity(error: Any) -> bool:
    if not isinstance(error, dict):
        return False
    message = " ".join(str(error.get("message", "")).lower().split())
    return re.match(r"^selected model is at capacity(?:[.!]|$)", message) is not None


def failure_from_tail(data: bytes, *, truncated: bool = False) -> str | None:
    """Only structured terminal events count, never quoted text/tool output.

    This is a candidate filter, not authority to retry; live IPC state must agree.
    An incomplete final line can hide a later turn, so fail closed until next scan.
    """
    if not data.endswith(b"\n"):
        return None
    lines = data.splitlines()
    if truncated:
        lines = lines[1:]
    for line in reversed(lines):
        try:
            event = json.loads(line)
        except (ValueError, UnicodeDecodeError):
            return None
        if not isinstance(event, dict):
            return None
        payload = event.get("payload") or {}
        if not isinstance(payload, dict):
            continue
        kind = event.get("type")
        event_type = payload.get("type")
        if kind == "event_msg":
            if event_type == "task_complete":
                turn_id = payload.get("turn_id")
                return turn_id if isinstance(turn_id, str) and is_capacity(payload.get("error")) else None
            if event_type in {"task_started", "turn_aborted", "user_message"}:
                return None
        if kind == "turn_context" or (kind == "response_item" and payload.get("role") == "user"):
            return None
    return None


@dataclass(frozen=True)
class Candidate:
    thread_id: str
    failed_turn: str


class Catalogue:
    """Read-only index + cached bounded log tails; never alters Codex files."""
    def __init__(self, database: Path):
        self.database = database.resolve()
        self.cache: dict[str, tuple[tuple[int, int], str | None]] = {}

    def _connect(self):
        connection = sqlite3.connect(self.database.as_uri() + "?mode=ro", uri=True, timeout=3)
        connection.execute("PRAGMA query_only = ON")
        return connection

    def rows(self) -> list[tuple[str, str]]:
        connection = self._connect()
        try:
            return connection.execute(
                "SELECT id, rollout_path FROM threads "
                "WHERE archived = 0 AND originator = 'Codex Desktop' "
                "AND source NOT LIKE '%subagent%' ORDER BY updated_at ASC"
            ).fetchall()
        finally:
            connection.close()

    def still_in_scope(self, thread_id: str) -> bool:
        connection = self._connect()
        try:
            return connection.execute(
                "SELECT 1 FROM threads WHERE id = ? AND archived = 0 "
                "AND originator = 'Codex Desktop' AND source NOT LIKE '%subagent%'",
                (thread_id,),
            ).fetchone() is not None
        finally:
            connection.close()

    def scan(self, only: set[str], exclude: set[str]) -> list[Candidate]:
        result = []
        for thread_id, log_path in self.rows():
            if thread_id in exclude or (only and thread_id not in only):
                continue
            path = Path(log_path)
            try:
                stat = path.stat()
                signature = (stat.st_mtime_ns, stat.st_size)
                cached = self.cache.get(str(path))
                if cached and cached[0] == signature:
                    turn = cached[1]
                else:
                    # The terminal event is normally a small final JSON line.
                    start = max(0, stat.st_size - 1024 * 1024)
                    with path.open("rb") as stream:
                        stream.seek(start)
                        turn = failure_from_tail(stream.read(1024 * 1024), truncated=start > 0)
                    self.cache[str(path)] = (signature, turn)
                if turn:
                    result.append(Candidate(thread_id, turn))
            except OSError:
                continue  # Missing/unreadable histories are never auto-resumed.
        return result


class NativeClient:
    """Owns only the small Node IPC adapter, never a Codex execution backend."""
    def __init__(self, node: str, timeout: float = 50):
        self.timeout = timeout
        self.next_id = 0
        self.messages: queue.Queue[Any] = queue.Queue()
        self.process = subprocess.Popen(
            [node, str(Path(__file__).with_name("codex_native_pipe.mjs")), "--stdio"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, encoding="utf-8", bufsize=1,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        try:
            self.call("info")
        except BaseException:
            self.close()
            raise

    def _read(self):
        try:
            for line in self.process.stdout:
                self.messages.put(json.loads(line))
        except Exception:
            self.messages.put({"fatal": {"message": "Invalid IPC adapter output", "code": "adapter_closed"}})
        finally:
            self.messages.put({"fatal": {"message": "IPC adapter stopped", "code": "adapter_closed"}})

    def call(self, command: str, **params) -> dict[str, Any]:
        self.next_id += 1
        request_id = self.next_id
        try:
            self.process.stdin.write(json.dumps({"id": request_id, "command": command, **params}) + "\n")
            self.process.stdin.flush()
        except OSError as exc:
            raise RetryError("Adapter connection lost", "adapter_closed", command == "retry") from exc
        deadline = time.monotonic() + self.timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RetryError("Adapter timeout; do not blindly repeat retries", "adapter_timeout", command == "retry")
            try:
                response = self.messages.get(timeout=remaining)
            except queue.Empty as exc:
                raise RetryError("Adapter timeout", "adapter_timeout", command == "retry") from exc
            if response.get("fatal"):
                raise RetryError(response["fatal"]["message"], response["fatal"]["code"], command == "retry")
            if response.get("id") != request_id:
                continue
            if response.get("error"):
                error = response["error"]
                raise RetryError(error["message"], error["code"], error.get("dispatched", False))
            return response["result"]

    def snapshot(self, thread_id: str):
        return self.call("snapshot", threadId=thread_id)

    def retry(self, thread_id: str, failed_turn: str):
        return self.call("retry", threadId=thread_id, expectedTurn=failed_turn)

    def close(self):
        if self.process.stdin and not self.process.stdin.closed:
            try:
                self.process.stdin.close()
            except OSError:
                pass
        try:
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.terminate()  # Only the helper that this client created.
            self.process.wait(timeout=3)
        self.reader.join(timeout=1)
        self.process.stdout.close()


def eligible(snapshot: dict, candidate: Candidate) -> bool:
    turn = snapshot.get("latestTurn") or {}
    status = snapshot.get("status") or {}
    source = snapshot.get("source")
    return (
        snapshot.get("id") == candidate.thread_id
        and snapshot.get("hostId") == "local"
        and snapshot.get("originator") == "Codex Desktop"
        and isinstance(source, str) and "subagent" not in source
        and snapshot.get("resumeState") == "resumed"
        and status.get("type") == "systemError" and not status.get("activeFlags")
        and snapshot.get("pendingRequests") == 0
        and snapshot.get("unconfirmedSubmissions") == 0
        and snapshot.get("goalStatus") in (None, "active")
        and turn.get("id") == candidate.failed_turn and turn.get("status") == "failed"
        and is_capacity(turn.get("error"))
    )


class Journal:
    """Only metadata is stored. A reservation survives crashes and timeouts."""
    def __init__(self, path: Path | None):
        self.connection = sqlite3.connect(str(path) if path else ":memory:", timeout=5)
        self.connection.execute("PRAGMA synchronous = FULL")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS native_attempts ("
            "thread TEXT NOT NULL, failed_turn TEXT NOT NULL, seen_at REAL NOT NULL, "
            "state TEXT NOT NULL, attempted_at REAL, PRIMARY KEY(thread, failed_turn))"
        )
        self.connection.commit()

    def observe(self, candidate: Candidate, now: float) -> tuple[float, str]:
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO native_attempts VALUES (?, ?, ?, 'waiting', NULL)",
                (candidate.thread_id, candidate.failed_turn, now),
            )
        return self.connection.execute(
            "SELECT seen_at, state FROM native_attempts WHERE thread = ? AND failed_turn = ?",
            (candidate.thread_id, candidate.failed_turn),
        ).fetchone()

    def reserve(self, candidate: Candidate, now: float, limit: int) -> bool:
        with self.connection:
            count = self.connection.execute(
                "SELECT COUNT(*) FROM native_attempts WHERE attempted_at > ?", (now - 60,),
            ).fetchone()[0]
            if count >= limit:
                return False
            result = self.connection.execute(
                "UPDATE native_attempts SET state = 'reserved', attempted_at = ? "
                "WHERE thread = ? AND failed_turn = ? AND state = 'waiting'",
                (now, candidate.thread_id, candidate.failed_turn),
            )
            return result.rowcount == 1

    def mark(self, candidate: Candidate, state: str):
        with self.connection:
            self.connection.execute(
                "UPDATE native_attempts SET state = ? WHERE thread = ? AND failed_turn = ?",
                (state, candidate.thread_id, candidate.failed_turn),
            )

    def prune_waiting(self, active: set[tuple[str, str]]):
        rows = self.connection.execute(
            "SELECT thread, failed_turn FROM native_attempts WHERE state = 'waiting'"
        ).fetchall()
        with self.connection:
            self.connection.executemany(
                "DELETE FROM native_attempts WHERE thread = ? AND failed_turn = ? AND state = 'waiting'",
                [row for row in rows if row not in active],
            )

    def close(self):
        self.connection.close()


class SingleInstance:
    """OS-held lock: process exit releases it, even when a lock file remains."""
    def __init__(self, path: Path):
        self.stream = path.open("a+b")
        self.stream.seek(0, 2)
        if self.stream.tell() == 0:
            self.stream.write(b"\0")
            self.stream.flush()
        self.stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.stream.close()
            raise RetryError("Another native retry watcher is already running.") from exc

    def close(self):
        self.stream.close()


def log(event: str, **fields):
    print(json.dumps({"time": time.strftime("%Y-%m-%d %H:%M:%S"), "event": event, **fields}, ensure_ascii=False), flush=True)


class Scheduler:
    def __init__(self, client, journal: Journal, delay: float, execute: bool,
                 max_per_minute: int = 6, emit=log):
        self.client, self.journal = client, journal
        self.delay, self.execute, self.max_per_minute = delay, execute, max_per_minute
        self.emit = emit
        self.last_report: dict[tuple[str, str], str] = {}

    def report(self, candidate, event, **fields):
        key = (candidate.thread_id, candidate.failed_turn)
        if self.last_report.get(key) != event:
            self.last_report[key] = event
            self.emit(event, thread=candidate.thread_id, failed_turn=candidate.failed_turn, **fields)

    def tick(self, candidates: list[Candidate], in_scope, now=None) -> int:
        clock = now if callable(now) else time.time
        fixed = now if isinstance(now, (float, int)) else None
        get_now = (lambda: fixed) if fixed is not None else clock
        active = set()
        sent = 0
        for candidate in candidates:
            try:
                snapshot = self.client.snapshot(candidate.thread_id)
                if not eligible(snapshot, candidate):
                    self.report(candidate, "skipped_state_changed")
                    continue
                active.add((candidate.thread_id, candidate.failed_turn))
                seen_at, state = self.journal.observe(candidate, get_now())
                if state != "waiting":
                    if state != "dry_run":
                        self.report(candidate, "already_recorded", state=state)
                    continue
                remaining = max(0, seen_at + self.delay - get_now())
                if remaining:
                    self.report(candidate, "waiting", title=snapshot.get("title"), retry_in_seconds=round(remaining, 1))
                    continue
                if not self.execute:
                    self.report(candidate, "would_retry", title=snapshot.get("title"), input_count=0)
                    self.journal.mark(candidate, "dry_run")
                    continue
                if not in_scope(candidate.thread_id):
                    active.discard((candidate.thread_id, candidate.failed_turn))
                    self.report(candidate, "skipped_archived_or_excluded")
                    continue
                if not self.journal.reserve(candidate, get_now(), self.max_per_minute):
                    self.report(candidate, "rate_limited")
                    continue
                # The helper obtains another fresh live snapshot immediately
                # before dispatch. Never submit follow-up prose as a fallback.
                try:
                    result = self.client.retry(candidate.thread_id, candidate.failed_turn)
                except BaseException as exc:
                    state = "not_sent" if isinstance(exc, RetryError) and not exc.dispatched else "unknown_do_not_resend"
                    self.journal.mark(candidate, state)
                    raise
                self.journal.mark(candidate, "request_returned")
                sent += 1
                self.report(candidate, "retry_submitted", title=snapshot.get("title"), input_count=0,
                            result=result.get("result"), note="Submission is not proof of successful recovery.")
            except RetryError as exc:
                self.report(candidate, "skipped_or_error", reason=exc.code, detail=str(exc))
                if exc.code in {"ipc_closed", "adapter_closed", "adapter_timeout", "ipc_unavailable"}:
                    raise
        self.journal.prune_waiting(active)
        return sent


def positive_seconds(value: str) -> float:
    result = float(value)
    if not math.isfinite(result) or result < 1:
        raise argparse.ArgumentTypeError("must be finite and >= 1 second")
    return result


def positive_int(value: str) -> int:
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return result


def main(argv=None) -> int:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retry-delay", type=positive_seconds, default=60, help="Minimum seconds from first observing each capacity-failed turn (default: 60).")
    parser.add_argument("--poll-interval", type=positive_seconds, default=5)
    parser.add_argument("--max-per-minute", type=positive_int, default=6, help="Global retry dispatch limit; all tasks combined.")
    parser.add_argument("--execute", action="store_true", help="Enable empty-input retries; otherwise only observe.")
    parser.add_argument("--once", action="store_true", help="One scan; does not sleep until pending retries are due.")
    parser.add_argument("--run-for", type=positive_seconds, help="Stop after this many seconds (plus an in-flight IPC call).")
    parser.add_argument("--thread", action="append", default=[], help="Only this local task; repeat for multiple tasks.")
    parser.add_argument("--exclude-thread", action="append", default=[])
    parser.add_argument("--status", metavar="THREAD_ID", help="Read a live task snapshot and exit, without retrying.")
    codex_directory = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
    parser.add_argument("--database", type=Path, default=codex_directory / "state_5.sqlite")
    parser.add_argument("--state-dir", type=Path, default=Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "CodexRetrySupervisor")
    parser.add_argument("--node", default=os.environ.get("CODEX_MCP_NODE_PATH") or shutil.which("node"))
    args = parser.parse_args(argv)
    client = journal = lock = None
    try:
        if not args.node:
            raise RetryError("Node.js 20+ is required. Install Node.js or provide --node PATH.")
        client = NativeClient(args.node)
        if args.status:
            log("snapshot", snapshot=client.snapshot(args.status))
            return 0
        args.state_dir.mkdir(parents=True, exist_ok=True)
        lock = SingleInstance(args.state_dir / "native-watcher.lock")
        journal = Journal(args.state_dir / "native-attempts.sqlite3" if args.execute else None)
        catalogue = Catalogue(args.database)
        excluded = set(args.exclude_thread)
        if os.environ.get("CODEX_THREAD_ID"):
            excluded.add(os.environ["CODEX_THREAD_ID"])
        scheduler = Scheduler(client, journal, args.retry_delay, args.execute, args.max_per_minute)
        log("started", mode="execute" if args.execute else "dry_run", retry_delay=args.retry_delay,
            poll_interval=args.poll_interval, scope="local desktop main tasks with live owner",
            max_per_minute=args.max_per_minute)
        started = time.monotonic()
        first = True
        while True:
            candidates = catalogue.scan(set(args.thread), excluded)
            if first:
                log("scan", capacity_candidates=len(candidates))
                first = False
            scheduler.tick(candidates, catalogue.still_in_scope)
            remaining = None if args.run_for is None else args.run_for - (time.monotonic() - started)
            if args.once or (remaining is not None and remaining <= 0):
                break
            time.sleep(min(args.poll_interval, 60, remaining if remaining is not None else 60))
        log("stopped", reason="requested_run_complete")
        return 0
    except KeyboardInterrupt:
        log("stopped", reason="Ctrl+C; already started Codex turns are left running")
        return 0
    except (RetryError, OSError, ValueError, sqlite3.Error) as exc:
        log("error", message=str(exc), note="No follow-up-message fallback. Fix the connection/schema and restart.")
        return 1
    finally:
        if client:
            client.close()
        if journal:
            journal.close()
        if lock:
            lock.close()


if __name__ == "__main__":
    sys.exit(main())
