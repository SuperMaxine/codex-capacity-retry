import argparse
import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from codex_native_retry import (
    Candidate, Catalogue, Journal, RetryError, Scheduler, SingleInstance,
    eligible, failure_from_tail, is_capacity, positive_seconds,
)


CAPACITY = {"message": "Selected model is at capacity. Please try a different model."}


def snapshot(thread="a", turn="failed-a"):
    return {"id": thread, "hostId": "local", "title": "Test task", "source": "vscode",
            "originator": "Codex Desktop", "resumeState": "resumed", "pendingRequests": 0,
            "unconfirmedSubmissions": 0, "goalStatus": None, "status": {"type": "systemError"},
            "latestTurn": {"id": turn, "status": "failed", "error": CAPACITY.copy()}}


def event(kind, **extra):
    return {"type": "event_msg", "payload": {"type": kind, **extra}}


def lines(*events):
    return b"".join((json.dumps(e) + "\n").encode() for e in events)


class FakeClient:
    def __init__(self):
        self.states = {"a": snapshot(), "b": snapshot("b", "failed-b")}
        self.calls = []
        self.error = None

    def snapshot(self, thread):
        return copy.deepcopy(self.states[thread])

    def retry(self, thread, turn):
        self.calls.append((thread, turn))
        if self.error:
            raise self.error
        return {"dispatched": True, "inputCount": 0, "result": {"turnId": "new"}}


class TailTests(unittest.TestCase):
    def test_latest_structured_failure(self):
        data = lines(event("task_complete", turn_id="t", error=CAPACITY))
        self.assertEqual(failure_from_tail(data), "t")

    def test_quoted_capacity_is_not_error(self):
        data = lines({"type": "response_item", "payload": {"role": "assistant", "text": CAPACITY["message"]}})
        self.assertIsNone(failure_from_tail(data))

    def test_new_turn_abort_success_and_user_message_cancel_old_failure(self):
        for suffix in (event("task_started"), event("turn_aborted"), event("task_complete", turn_id="new"),
                       {"type": "turn_context", "payload": {}},
                       {"type": "response_item", "payload": {"role": "user"}}):
            with self.subTest(suffix=suffix):
                self.assertIsNone(failure_from_tail(lines(event("task_complete", turn_id="old", error=CAPACITY), suffix)))

    def test_incomplete_or_invalid_tail_is_not_retried(self):
        data = lines(event("task_complete", turn_id="old", error=CAPACITY))
        self.assertIsNone(failure_from_tail(data + b'{"type":'))
        self.assertIsNone(failure_from_tail(data + b'invalid\n'))

    def test_truncated_first_record_is_discarded(self):
        data = b"incomplete prefix\n" + lines(event("task_complete", turn_id="t", error=CAPACITY))
        self.assertEqual(failure_from_tail(data, truncated=True), "t")

    def test_other_error_or_string_does_not_qualify(self):
        self.assertFalse(is_capacity({"message": "Network failed"}))
        self.assertFalse(is_capacity(CAPACITY["message"]))
        self.assertFalse(is_capacity({"message": "Tool output quotes: " + CAPACITY["message"]}))


class GuardTests(unittest.TestCase):
    def test_valid(self):
        self.assertTrue(eligible(snapshot(), Candidate("a", "failed-a")))

    def test_skip_non_terminal_or_waiting_states(self):
        variants = [
            {"status": {"type": "active"}}, {"status": {"type": "idle"}},
            {"status": {"type": "systemError", "activeFlags": ["waitingOnApproval"]}},
            {"pendingRequests": 1}, {"pendingRequests": None}, {"unconfirmedSubmissions": 1},
            {"goalStatus": "paused"}, {"goalStatus": "complete"}, {"resumeState": "needs_resume"},
            {"hostId": "remote"}, {"originator": "CLI"}, {"source": '{"subagent":{}}'},
        ]
        for change in variants:
            with self.subTest(change=change):
                self.assertFalse(eligible(snapshot() | change, Candidate("a", "failed-a")))

    def test_latest_turn_must_match(self):
        for change in ({"id": "new"}, {"status": "interrupted"}, {"error": {"message": "network"}}):
            current = snapshot()
            current["latestTurn"].update(change)
            self.assertFalse(eligible(current, Candidate("a", "failed-a")))


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.journal = Journal(None)
        self.client = FakeClient()
        self.events = []
        self.scheduler = Scheduler(self.client, self.journal, 60, True, emit=lambda e, **f: self.events.append(e))
        self.a, self.b = Candidate("a", "failed-a"), Candidate("b", "failed-b")

    def tearDown(self):
        self.journal.close()

    def tick(self, candidates, at):
        return self.scheduler.tick(candidates, lambda _: True, at)

    def test_independent_delays(self):
        self.tick([self.a], 0)
        self.tick([self.a, self.b], 25)
        self.tick([self.a, self.b], 59)
        self.assertEqual(self.client.calls, [])
        self.tick([self.a, self.b], 60)
        self.assertEqual(self.client.calls, [("a", "failed-a")])
        self.tick([self.a, self.b], 85)
        self.assertEqual(self.client.calls, [("a", "failed-a"), ("b", "failed-b")])

    def test_new_capacity_failure_starts_new_delay(self):
        self.tick([self.a], 0)
        self.tick([self.a], 60)
        self.client.states["a"] = snapshot("a", "failed-2")
        second = Candidate("a", "failed-2")
        self.tick([second], 65)
        self.tick([second], 124)
        self.assertEqual(len(self.client.calls), 1)
        self.tick([second], 125)
        self.assertEqual(len(self.client.calls), 2)

    def test_task_that_resumed_is_not_retried(self):
        self.tick([self.a], 0)
        self.client.states["a"]["status"] = {"type": "active"}
        self.tick([self.a], 60)
        self.assertFalse(self.client.calls)

    def test_absent_candidate_resets_wait(self):
        self.tick([self.a], 0)
        self.tick([], 40)
        self.tick([self.a], 70)
        self.assertFalse(self.client.calls)

    def test_archive_recheck_before_dispatch(self):
        self.tick([self.a], 0)
        self.scheduler.tick([self.a], lambda _: False, 60)
        self.assertFalse(self.client.calls)

    def test_unknown_outcome_cannot_be_resent(self):
        self.client.error = RetryError("timeout", "outcome_unknown", True)
        self.tick([self.a], 0)
        self.tick([self.a], 60)
        self.tick([self.a], 1000)
        self.assertEqual(len(self.client.calls), 1)
        self.assertEqual(self.journal.observe(self.a, 1000)[1], "unknown_do_not_resend")

    def test_definitely_not_sent_recheck_records_no_resend(self):
        self.client.error = RetryError("changed", "failed_turn_changed", False)
        self.tick([self.a], 0)
        self.tick([self.a], 60)
        self.tick([self.a], 1000)
        self.assertEqual(self.journal.observe(self.a, 1000)[1], "not_sent")

    def test_global_rate_limit(self):
        self.scheduler.max_per_minute = 1
        self.tick([self.a, self.b], 0)
        self.tick([self.a, self.b], 60)
        self.assertEqual(len(self.client.calls), 1)
        self.tick([self.a, self.b], 120)
        self.assertEqual(len(self.client.calls), 2)

    def test_dry_run_never_calls_retry(self):
        self.scheduler.execute = False
        self.tick([self.a], 0)
        self.tick([self.a], 60)
        self.tick([self.a], 500)
        self.assertEqual(self.client.calls, [])
        self.assertIn("would_retry", self.events)

    def test_delay_parameter_is_respected(self):
        self.scheduler.delay = 123
        self.tick([self.a], 0)
        self.tick([self.a], 122)
        self.assertFalse(self.client.calls)
        self.tick([self.a], 123)
        self.assertEqual(len(self.client.calls), 1)


class PersistenceTests(unittest.TestCase):
    def test_reserved_attempt_survives_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            candidate = Candidate("a", "failed-a")
            first = Journal(path)
            first.observe(candidate, 0)
            self.assertTrue(first.reserve(candidate, 60, 6))
            first.close()
            second = Journal(path)
            try:
                self.assertEqual(second.observe(candidate, 100)[1], "reserved")
                self.assertFalse(second.reserve(candidate, 1000, 6))
            finally:
                second.close()

    def test_only_unarchived_desktop_main_tasks_are_enumerated(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "index.sqlite"
            connection = sqlite3.connect(path)
            connection.execute("CREATE TABLE threads (id, rollout_path, archived, originator, source, updated_at)")
            connection.executemany("INSERT INTO threads VALUES (?, ?, ?, ?, ?, ?)", [
                ("yes", "log", 0, "Codex Desktop", "vscode", 1),
                ("archived", "log", 1, "Codex Desktop", "vscode", 1),
                ("child", "log", 0, "Codex Desktop", '{"subagent":{}}', 1),
                ("cli", "log", 0, "CLI", "cli", 1),
            ])
            connection.commit()
            connection.close()
            catalogue = Catalogue(path)
            self.assertEqual(catalogue.rows(), [("yes", "log")])
            self.assertFalse(catalogue.still_in_scope("archived"))

    def test_os_lock_blocks_second_watcher(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lock"
            first = SingleInstance(path)
            try:
                with self.assertRaises(RetryError):
                    SingleInstance(path)
            finally:
                first.close()
            SingleInstance(path).close()

    def test_bad_delay_rejected(self):
        for value in ("0", "-1", "nan", "inf"):
            with self.assertRaises(argparse.ArgumentTypeError):
                positive_seconds(value)


if __name__ == "__main__":
    unittest.main()
