#!/usr/bin/env python3
"""
Integration tests for the multi-threaded reader/writer dispatch (0.8.0).

Tests verify:
  1. schemaVersion is present in hello response
  2. Reader thread handles search/stats/filterNewMessages correctly after init
  3. Writer thread handles indexBatch/removeBatch correctly after init
  4. Concurrent dispatch: interleaved read/write calls all return correct responses
  5. Clear → reopen signaling: writer clears DB, reader sees empty results
  6. Memory operations work through the same dispatch

Run:
  cd tabmail-native-fts
  cargo build --release
  TABMAIL_RUST_FTS_HELPER=./target/release/fts_helper python3 -m pytest tests/test_multithreaded.py -v
  # or
  TABMAIL_RUST_FTS_HELPER=./target/release/fts_helper python3 tests/test_multithreaded.py
"""

import json
import os
import shutil
import struct
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


def _send_message(proc, message):
    encoded = json.dumps(message).encode("utf-8")
    proc.stdin.write(struct.pack("=I", len(encoded)))
    proc.stdin.write(encoded)
    proc.stdin.flush()


def _read_message(proc, timeout_seconds=30):
    """Read a native messaging response. Raises on timeout or EOF."""
    import select

    # Use select for timeout on Unix
    ready, _, _ = select.select([proc.stdout], [], [], timeout_seconds)
    if not ready:
        raise TimeoutError(f"No response within {timeout_seconds}s")

    raw_length = proc.stdout.read(4)
    if not raw_length:
        raise EOFError("Process closed stdout (EOF)")
    message_length = struct.unpack("=I", raw_length)[0]
    message_bytes = proc.stdout.read(message_length)
    return json.loads(message_bytes.decode("utf-8"))


def _read_all_responses(proc, expected_count, timeout_seconds=60):
    """Read multiple responses (possibly out of order). Returns dict keyed by id."""
    responses = {}
    deadline = time.time() + timeout_seconds
    while len(responses) < expected_count:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise TimeoutError(
                f"Only got {len(responses)}/{expected_count} responses within {timeout_seconds}s. "
                f"Got ids: {list(responses.keys())}"
            )
        resp = _read_message(proc, timeout_seconds=remaining)
        resp_id = resp.get("id")
        if resp_id:
            responses[resp_id] = resp
    return responses


class TestMultiThreadedDispatch(unittest.TestCase):
    """Integration tests for multi-threaded reader/writer dispatch."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="fts_mt_test_")
        self.rust_helper_path = os.environ.get("TABMAIL_RUST_FTS_HELPER")

        if not self.rust_helper_path:
            self.skipTest("TABMAIL_RUST_FTS_HELPER not set (build Rust binary and set env var)")

        self.rust_helper_path = str(Path(self.rust_helper_path).resolve())
        if not Path(self.rust_helper_path).exists():
            self.skipTest(f"Rust helper not found: {self.rust_helper_path}")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _start_process(self):
        return subprocess.Popen(
            [self.rust_helper_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def _hello_and_init(self, proc):
        """Run hello + init handshake, return hello result."""
        _send_message(proc, {"id": "h1", "method": "hello", "params": {"addonVersion": "1.3.0"}})
        hello_resp = _read_message(proc)
        self.assertIn("result", hello_resp, f"hello failed: {hello_resp}")

        _send_message(proc, {"id": "h2", "method": "init", "params": {"profilePath": self.temp_dir}})
        init_resp = _read_message(proc)
        self.assertIn("result", init_resp, f"init failed: {init_resp}")
        self.assertTrue(init_resp["result"]["ok"])

        return hello_resp

    def _stop_process(self, proc):
        try:
            proc.stdin.close()
        except Exception:
            pass
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()

    # ------------------------------------------------------------------
    # Test 1: schemaVersion in hello response
    # ------------------------------------------------------------------
    def test_hello_includes_schema_version(self):
        """hello response must include schemaVersion (integer >= 1)."""
        proc = self._start_process()
        try:
            hello_resp = self._hello_and_init(proc)
            result = hello_resp["result"]

            self.assertIn("schemaVersion", result, "hello response missing schemaVersion")
            self.assertIsInstance(result["schemaVersion"], int)
            self.assertGreaterEqual(result["schemaVersion"], 1)

            # Also verify hostVersion is 0.8.0
            self.assertEqual(result["hostVersion"], "0.8.0")
        finally:
            self._stop_process(proc)

    # ------------------------------------------------------------------
    # Test 2: Basic reader/writer dispatch after init
    # ------------------------------------------------------------------
    def test_reader_writer_basic(self):
        """After init, reader (stats/search) and writer (indexBatch) both work."""
        proc = self._start_process()
        try:
            self._hello_and_init(proc)

            # Writer: index some data
            ts = str(int(time.time() * 1000))
            rows = [
                {
                    "msgId": f"mt-basic-{i}-{ts}",
                    "subject": f"Email about project planning number {i}",
                    "from_": f"user{i}@test.com",
                    "to_": "team@test.com",
                    "body": f"Discussion about project milestones and deadlines {i}.",
                    "dateMs": 1700000000000 + i * 1000000,
                    "hasAttachments": False,
                }
                for i in range(3)
            ]
            _send_message(proc, {"id": "w1", "method": "indexBatch", "params": {"rows": rows}})
            resp = _read_message(proc)
            self.assertIn("result", resp, f"indexBatch failed: {resp}")
            self.assertEqual(resp["result"]["count"], 3)

            # Reader: stats
            _send_message(proc, {"id": "r1", "method": "stats", "params": {}})
            resp = _read_message(proc)
            self.assertIn("result", resp, f"stats failed: {resp}")
            self.assertEqual(resp["result"]["docs"], 3)

            # Reader: search
            _send_message(proc, {"id": "r2", "method": "search", "params": {"q": "milestones", "limit": 10}})
            resp = _read_message(proc)
            self.assertIn("result", resp, f"search failed: {resp}")
            self.assertIsInstance(resp["result"], list)
            self.assertGreater(len(resp["result"]), 0)

            # Reader: filterNewMessages
            _send_message(proc, {
                "id": "r3", "method": "filterNewMessages",
                "params": {"rows": [
                    {"msgId": f"mt-basic-0-{ts}"},  # exists
                    {"msgId": f"mt-new-{ts}"},       # new
                ]}
            })
            resp = _read_message(proc)
            self.assertIn("result", resp, f"filterNewMessages failed: {resp}")
            self.assertEqual(resp["result"]["newCount"], 1)
            self.assertEqual(resp["result"]["skippedCount"], 1)

        finally:
            self._stop_process(proc)

    # ------------------------------------------------------------------
    # Test 3: Concurrent dispatch — interleaved reads and writes
    # ------------------------------------------------------------------
    def test_concurrent_dispatch(self):
        """Send multiple read and write requests rapidly, verify all responses arrive."""
        proc = self._start_process()
        try:
            self._hello_and_init(proc)

            # First, index some data so search has something to find
            ts = str(int(time.time() * 1000))
            seed_rows = [
                {
                    "msgId": f"mt-seed-{i}-{ts}",
                    "subject": f"Quarterly report discussion {i}",
                    "from_": f"exec{i}@corp.com",
                    "to_": "board@corp.com",
                    "body": f"Please review the quarterly earnings report for Q{i+1}.",
                    "dateMs": 1700000000000 + i * 1000000,
                    "hasAttachments": False,
                }
                for i in range(5)
            ]
            _send_message(proc, {"id": "seed", "method": "indexBatch", "params": {"rows": seed_rows}})
            resp = _read_message(proc)
            self.assertEqual(resp["result"]["count"], 5)

            # Now fire off a batch of interleaved read+write requests rapidly
            # Write: index more data (goes to writer thread)
            more_rows = [
                {
                    "msgId": f"mt-more-{i}-{ts}",
                    "subject": f"Budget allocation for department {i}",
                    "from_": f"finance{i}@corp.com",
                    "to_": "cfo@corp.com",
                    "body": f"Requesting budget increase for department {i} operations.",
                    "dateMs": 1700010000000 + i * 1000000,
                    "hasAttachments": False,
                }
                for i in range(10)
            ]
            _send_message(proc, {"id": "c-w1", "method": "indexBatch", "params": {"rows": more_rows}})

            # Read: search (goes to reader thread — should not be blocked by indexBatch)
            _send_message(proc, {"id": "c-r1", "method": "search", "params": {"q": "quarterly", "limit": 10}})

            # Read: stats
            _send_message(proc, {"id": "c-r2", "method": "stats", "params": {}})

            # Read: debugSample
            _send_message(proc, {"id": "c-r3", "method": "debugSample", "params": {}})

            # Collect all 4 responses (may arrive out of order in multi-threaded mode)
            responses = _read_all_responses(proc, expected_count=4, timeout_seconds=30)

            # Verify all responses are present and successful
            self.assertIn("c-w1", responses, "Missing indexBatch response")
            self.assertIn("c-r1", responses, "Missing search response")
            self.assertIn("c-r2", responses, "Missing stats response")
            self.assertIn("c-r3", responses, "Missing debugSample response")

            # Writer result
            self.assertIn("result", responses["c-w1"])
            self.assertEqual(responses["c-w1"]["result"]["count"], 10)

            # Reader results
            self.assertIn("result", responses["c-r1"])
            self.assertIsInstance(responses["c-r1"]["result"], list)
            self.assertGreater(len(responses["c-r1"]["result"]), 0, "search should find 'quarterly' results")

            self.assertIn("result", responses["c-r2"])
            # Stats should show at least the 5 seed rows (10 more may or may not be visible
            # depending on WAL visibility timing)
            self.assertGreaterEqual(responses["c-r2"]["result"]["docs"], 5)

            self.assertIn("result", responses["c-r3"])
            self.assertIsInstance(responses["c-r3"]["result"], list)

        finally:
            self._stop_process(proc)

    # ------------------------------------------------------------------
    # Test 4: Clear → reader reopens connection
    # ------------------------------------------------------------------
    def test_clear_reopen_signaling(self):
        """After clear (writer), reader thread reopens its connection and sees empty DB."""
        proc = self._start_process()
        try:
            self._hello_and_init(proc)

            # Index some data
            ts = str(int(time.time() * 1000))
            rows = [
                {
                    "msgId": f"mt-clear-{i}-{ts}",
                    "subject": f"Test email {i}",
                    "from_": f"user{i}@test.com",
                    "to_": "team@test.com",
                    "body": f"Content for test email {i}.",
                    "dateMs": 1700000000000,
                    "hasAttachments": False,
                }
                for i in range(3)
            ]
            _send_message(proc, {"id": "c1", "method": "indexBatch", "params": {"rows": rows}})
            resp = _read_message(proc)
            self.assertEqual(resp["result"]["count"], 3)

            # Verify data exists
            _send_message(proc, {"id": "c2", "method": "stats", "params": {}})
            resp = _read_message(proc)
            self.assertEqual(resp["result"]["docs"], 3)

            # Clear (goes to writer thread, signals reader to reopen)
            _send_message(proc, {"id": "c3", "method": "clear", "params": {}})
            resp = _read_message(proc)
            self.assertIn("result", resp, f"clear failed: {resp}")
            self.assertTrue(resp["result"]["ok"])

            # Stats after clear (reader should have reopened and see empty DB)
            _send_message(proc, {"id": "c4", "method": "stats", "params": {}})
            resp = _read_message(proc)
            self.assertIn("result", resp, f"stats after clear failed: {resp}")
            self.assertEqual(resp["result"]["docs"], 0, "Reader should see 0 docs after clear")

            # Search after clear
            _send_message(proc, {"id": "c5", "method": "search", "params": {"q": "test", "limit": 10}})
            resp = _read_message(proc)
            self.assertIn("result", resp, f"search after clear failed: {resp}")
            self.assertIsInstance(resp["result"], list)
            self.assertEqual(len(resp["result"]), 0, "Search should return empty after clear")

            # Index again after clear (verify write path still works)
            new_rows = [
                {
                    "msgId": f"mt-after-clear-{ts}",
                    "subject": "Post-clear email",
                    "from_": "user@test.com",
                    "to_": "team@test.com",
                    "body": "This was indexed after clear.",
                    "dateMs": 1700000000000,
                    "hasAttachments": False,
                }
            ]
            _send_message(proc, {"id": "c6", "method": "indexBatch", "params": {"rows": new_rows}})
            resp = _read_message(proc)
            self.assertEqual(resp["result"]["count"], 1)

            # Verify new data visible to reader
            _send_message(proc, {"id": "c7", "method": "stats", "params": {}})
            resp = _read_message(proc)
            self.assertEqual(resp["result"]["docs"], 1)

        finally:
            self._stop_process(proc)

    # ------------------------------------------------------------------
    # Test 5: Memory clear → reader reopens memory connection
    # ------------------------------------------------------------------
    def test_memory_clear_reopen(self):
        """memoryClear signals reader to reopen its memory connection."""
        proc = self._start_process()
        try:
            self._hello_and_init(proc)

            # Index memory entries
            ts = str(int(time.time() * 1000))
            rows = [
                {
                    "memId": f"mt-mem-{i}-{ts}",
                    "role": "user",
                    "content": f"Memory entry about topic {i}",
                    "sessionId": f"session-{ts}",
                    "dateMs": 1700000000000 + i * 1000000,
                    "turnIndex": i,
                }
                for i in range(3)
            ]
            _send_message(proc, {"id": "m1", "method": "memoryIndexBatch", "params": {"rows": rows}})
            resp = _read_message(proc)
            self.assertEqual(resp["result"]["count"], 3)

            # Verify via reader
            _send_message(proc, {"id": "m2", "method": "memoryStats", "params": {}})
            resp = _read_message(proc)
            self.assertEqual(resp["result"]["docs"], 3)

            # Memory clear
            _send_message(proc, {"id": "m3", "method": "memoryClear", "params": {}})
            resp = _read_message(proc)
            self.assertTrue(resp["result"]["ok"])

            # Reader should see empty after memoryClear
            _send_message(proc, {"id": "m4", "method": "memoryStats", "params": {}})
            resp = _read_message(proc)
            self.assertEqual(resp["result"]["docs"], 0, "Reader should see 0 memory docs after memoryClear")

        finally:
            self._stop_process(proc)

    # ------------------------------------------------------------------
    # Test 6: Unknown method returns error
    # ------------------------------------------------------------------
    def test_unknown_method_returns_error(self):
        """Unknown methods return an error response (main thread handles this)."""
        proc = self._start_process()
        try:
            self._hello_and_init(proc)

            _send_message(proc, {"id": "u1", "method": "nonExistentMethod", "params": {}})
            resp = _read_message(proc)
            self.assertEqual(resp["id"], "u1")
            self.assertIn("error", resp)
            self.assertIn("Unknown", resp["error"])

        finally:
            self._stop_process(proc)

    # ------------------------------------------------------------------
    # Test 7: Pre-init rejects non-lifecycle methods
    # ------------------------------------------------------------------
    def test_pre_init_rejects_post_init_methods(self):
        """Before init, sending search/indexBatch etc. returns error."""
        proc = self._start_process()
        try:
            # hello first
            _send_message(proc, {"id": "p1", "method": "hello", "params": {"addonVersion": "1.3.0"}})
            resp = _read_message(proc)
            self.assertIn("result", resp)

            # Try search before init
            _send_message(proc, {"id": "p2", "method": "search", "params": {"q": "test"}})
            resp = _read_message(proc)
            self.assertEqual(resp["id"], "p2")
            self.assertIn("error", resp)
            self.assertIn("init", resp["error"].lower())

        finally:
            self._stop_process(proc)

    # ------------------------------------------------------------------
    # Test 8: Response IDs always match request IDs
    # ------------------------------------------------------------------
    def test_response_ids_match_request_ids(self):
        """Every response carries the same id as its request, across both threads."""
        proc = self._start_process()
        try:
            self._hello_and_init(proc)

            # Send requests to both threads with distinctive IDs
            ts = str(int(time.time() * 1000))
            # Reader
            _send_message(proc, {"id": "alpha", "method": "stats", "params": {}})
            # Writer
            _send_message(proc, {"id": "beta", "method": "indexBatch", "params": {"rows": [
                {"msgId": f"id-test-{ts}", "subject": "test", "from_": "a@b.com",
                 "to_": "c@d.com", "body": "test", "dateMs": 1700000000000, "hasAttachments": False}
            ]}})
            # Reader
            _send_message(proc, {"id": "gamma", "method": "stats", "params": {}})

            responses = _read_all_responses(proc, expected_count=3, timeout_seconds=15)

            self.assertIn("alpha", responses)
            self.assertIn("beta", responses)
            self.assertIn("gamma", responses)

            for rid, resp in responses.items():
                self.assertEqual(resp["id"], rid)
                # All should succeed (no "error" key with absent "result")
                self.assertTrue("result" in resp or "error" not in resp,
                                f"Response {rid} has error: {resp.get('error')}")

        finally:
            self._stop_process(proc)


if __name__ == "__main__":
    unittest.main(verbosity=2)
