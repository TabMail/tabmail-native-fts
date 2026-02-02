#!/usr/bin/env python3
"""
Integration tests for the Rust native-fts host.

These tests spawn the Rust helper as a native messaging process and verify
the full workflow: hello, init, indexBatch, search, stats, clear.

How to run locally (after installing Rust toolchain):
  cd tabmail-native-fts
  cargo build --release
  TABMAIL_RUST_FTS_HELPER=./target/release/fts_helper python3 tests/run_tests.py rust

The tests will:
  - Spawn the Rust host as a native messaging process (stdin/stdout framed JSON).
  - Run a full workflow and verify results.
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


def _read_message(proc):
    raw_length = proc.stdout.read(4)
    if not raw_length:
        return None
    message_length = struct.unpack("=I", raw_length)[0]
    message_bytes = proc.stdout.read(message_length)
    return json.loads(message_bytes.decode("utf-8"))


class TestRustHelperProcess(unittest.TestCase):
    """Integration tests using the Rust helper process."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="fts_profile_rust_test_")
        self.rust_helper_path = os.environ.get("TABMAIL_RUST_FTS_HELPER")

        if not self.rust_helper_path:
            self.skipTest("TABMAIL_RUST_FTS_HELPER not set (build Rust binary and set env var)")

        self.rust_helper_path = str(Path(self.rust_helper_path).resolve())
        if not Path(self.rust_helper_path).exists():
            self.skipTest(f"Rust helper not found: {self.rust_helper_path}")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def assert_success(self, response, msg=""):
        if response is None:
            self.fail(f"{msg} - No response from helper")
        if "error" in response:
            self.fail(f"{msg} - Got error: {response['error']}")
        self.assertIn("result", response, f"{msg} - No 'result' in response: {response}")

    def test_full_workflow_rust(self):
        proc = subprocess.Popen(
            [self.rust_helper_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            # 1. Hello
            _send_message(proc, {"id": "1", "method": "hello", "params": {"addonVersion": "0.4.4"}})
            response = _read_message(proc)
            self.assertEqual(response["id"], "1")
            self.assert_success(response, "hello")
            self.assertIn("hostVersion", response["result"])

            # 2. Init with profilePath
            _send_message(
                proc,
                {"id": "2", "method": "init", "params": {"profilePath": self.temp_dir}},
            )
            response = _read_message(proc)
            self.assertEqual(response["id"], "2")
            self.assert_success(response, "init")
            self.assertTrue(response["result"]["ok"])

            # 3. Index a batch (use unique msgIds based on timestamp)
            unique_suffix = str(int(time.time() * 1000))
            _send_message(
                proc,
                {
                    "id": "3",
                    "method": "indexBatch",
                    "params": {
                        "rows": [
                            {
                                "msgId": f"test-msg-1-{unique_suffix}",
                                "subject": "Test Email About Meetings",
                                "from_": "sender@example.com",
                                "to_": "recipient@example.com",
                                "body": "Let us schedule a meeting to discuss the project.",
                                "dateMs": 1700000000000,
                                "hasAttachments": False,
                            },
                            {
                                "msgId": f"test-msg-2-{unique_suffix}",
                                "subject": "Invoice Payment",
                                "from_": "billing@vendor.com",
                                "to_": "accounts@company.com",
                                "body": "Please process the attached invoice.",
                                "dateMs": 1700001000000,
                                "hasAttachments": True,
                            },
                        ]
                    },
                },
            )
            response = _read_message(proc)
            self.assertEqual(response["id"], "3")
            self.assert_success(response, "indexBatch")
            self.assertEqual(response["result"]["count"], 2)

            # 4. Search
            _send_message(proc, {"id": "4", "method": "search", "params": {"q": "meeting", "limit": 10}})
            response = _read_message(proc)
            self.assertEqual(response["id"], "4")
            self.assert_success(response, "search meeting")
            self.assertIsInstance(response["result"], list)
            self.assertGreater(len(response["result"]), 0)
            # Verify result structure
            for key in ["uniqueId", "author", "subject", "dateMs", "hasAttachments", "snippet", "rank"]:
                self.assertIn(key, response["result"][0])

            # 5. Search with from: qualifier
            _send_message(
                proc,
                {"id": "5", "method": "search", "params": {"q": "from:billing@vendor.com", "limit": 10}},
            )
            response = _read_message(proc)
            self.assertEqual(response["id"], "5")
            self.assert_success(response, "search from:email")
            self.assertGreaterEqual(len(response["result"]), 1)
            # First result should be the billing email (FTS exact field match ranks highest)
            self.assertIn("billing@vendor.com", response["result"][0].get("author", ""))

            # 6. Stats
            _send_message(proc, {"id": "6", "method": "stats", "params": {}})
            response = _read_message(proc)
            self.assertEqual(response["id"], "6")
            self.assert_success(response, "stats")
            self.assertEqual(response["result"]["docs"], 2)

            # 7. Clear
            _send_message(proc, {"id": "7", "method": "clear", "params": {}})
            response = _read_message(proc)
            self.assertEqual(response["id"], "7")
            self.assert_success(response, "clear")
            self.assertTrue(response["result"]["ok"])

            # 8. Verify cleared
            _send_message(proc, {"id": "8", "method": "stats", "params": {}})
            response = _read_message(proc)
            self.assert_success(response, "stats after clear")
            self.assertEqual(response["result"]["docs"], 0)

        finally:
            try:
                proc.stdin.close()
            except Exception:
                pass
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()


    def test_rebuild_embeddings_batch(self):
        """Test the batch-based rebuildEmbeddings workflow:
        rebuildEmbeddingsStart → rebuildEmbeddingsBatch loop → verify via stats.
        Also tests that FTS search works during rebuild (between batches).
        """
        proc = subprocess.Popen(
            [self.rust_helper_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            # 1. Hello + Init
            _send_message(proc, {"id": "1", "method": "hello", "params": {"addonVersion": "1.2.1"}})
            response = _read_message(proc)
            self.assert_success(response, "hello")

            _send_message(proc, {"id": "2", "method": "init", "params": {"profilePath": self.temp_dir}})
            response = _read_message(proc)
            self.assert_success(response, "init")

            # 2. Index some emails
            unique_suffix = str(int(time.time() * 1000))
            rows = [
                {
                    "msgId": f"rebuild-test-{i}-{unique_suffix}",
                    "subject": f"Test Email {i} About Quarterly Reports",
                    "from_": f"user{i}@example.com",
                    "to_": "team@example.com",
                    "body": f"This is test email number {i} discussing quarterly results and forecasts.",
                    "dateMs": 1700000000000 + i * 1000000,
                    "hasAttachments": False,
                }
                for i in range(5)
            ]
            _send_message(proc, {"id": "3", "method": "indexBatch", "params": {"rows": rows}})
            response = _read_message(proc)
            self.assert_success(response, "indexBatch")
            self.assertEqual(response["result"]["count"], 5)

            # 3. Index some memory entries
            mem_rows = [
                {
                    "memId": f"mem-rebuild-{i}-{unique_suffix}",
                    "role": "user" if i % 2 == 0 else "assistant",
                    "content": f"Memory entry {i} about project planning and deadlines.",
                    "sessionId": f"session-{unique_suffix}",
                    "dateMs": 1700000000000 + i * 1000000,
                    "turnIndex": i,
                }
                for i in range(3)
            ]
            _send_message(proc, {"id": "4", "method": "memoryIndexBatch", "params": {"rows": mem_rows}})
            response = _read_message(proc)
            self.assert_success(response, "memoryIndexBatch")
            self.assertEqual(response["result"]["count"], 3)

            # 4. Verify stats show vecDocs (indexBatch creates embeddings too)
            _send_message(proc, {"id": "5", "method": "stats", "params": {}})
            response = _read_message(proc)
            self.assert_success(response, "stats before rebuild")
            self.assertEqual(response["result"]["docs"], 5)
            self.assertIn("vecDocs", response["result"])
            initial_vec_docs = response["result"]["vecDocs"]
            # indexBatch should have created embeddings already
            self.assertEqual(initial_vec_docs, 5, "indexBatch should create embeddings")

            # 5. rebuildEmbeddingsStart — clears vec tables
            _send_message(proc, {"id": "10", "method": "rebuildEmbeddingsStart", "params": {}})
            response = _read_message(proc)
            self.assert_success(response, "rebuildEmbeddingsStart")
            self.assertTrue(response["result"]["ok"])
            self.assertEqual(response["result"]["emailTotal"], 5)
            self.assertEqual(response["result"]["memoryTotal"], 3)

            # 6. Verify vec tables are now empty (start clears them)
            _send_message(proc, {"id": "11", "method": "stats", "params": {}})
            response = _read_message(proc)
            self.assert_success(response, "stats after start")
            self.assertEqual(response["result"]["docs"], 5, "FTS5 index should be intact")
            self.assertEqual(response["result"]["vecDocs"], 0, "vec should be empty after start")

            # 7. FTS search should still work (keyword search doesn't need embeddings)
            _send_message(proc, {"id": "12", "method": "search", "params": {"q": "quarterly", "limit": 10}})
            response = _read_message(proc)
            self.assert_success(response, "search during rebuild")
            self.assertGreater(len(response["result"]), 0, "FTS search should work during rebuild")

            # 8. Process email embeddings in batches (use small batch size to test loop)
            last_rowid = 0
            total_processed = 0
            total_embedded = 0
            batch_count = 0
            msg_counter = 20
            while True:
                _send_message(proc, {
                    "id": str(msg_counter),
                    "method": "rebuildEmbeddingsBatch",
                    "params": {"target": "email", "lastRowid": last_rowid, "batchSize": 2}
                })
                response = _read_message(proc)
                self.assert_success(response, f"rebuildEmbeddingsBatch email #{batch_count}")
                self.assertTrue(response["result"]["ok"])
                self.assertEqual(response["result"]["target"], "email")

                last_rowid = response["result"]["lastRowid"]
                total_processed += response["result"]["processed"]
                total_embedded += response["result"]["embedded"]
                batch_count += 1
                msg_counter += 1

                if response["result"]["done"]:
                    break

            self.assertEqual(total_processed, 5, "Should process all 5 emails")
            self.assertEqual(total_embedded, 5, "Should embed all 5 emails")
            self.assertGreaterEqual(batch_count, 3, "With batchSize=2, need at least 3 batches for 5 docs")

            # 9. Process memory embeddings in batches
            last_rowid = 0
            mem_processed = 0
            mem_embedded = 0
            while True:
                _send_message(proc, {
                    "id": str(msg_counter),
                    "method": "rebuildEmbeddingsBatch",
                    "params": {"target": "memory", "lastRowid": last_rowid, "batchSize": 2}
                })
                response = _read_message(proc)
                self.assert_success(response, "rebuildEmbeddingsBatch memory")
                self.assertEqual(response["result"]["target"], "memory")

                last_rowid = response["result"]["lastRowid"]
                mem_processed += response["result"]["processed"]
                mem_embedded += response["result"]["embedded"]
                msg_counter += 1

                if response["result"]["done"]:
                    break

            self.assertEqual(mem_processed, 3, "Should process all 3 memory entries")
            self.assertEqual(mem_embedded, 3, "Should embed all 3 memory entries")

            # 10. Verify final stats
            _send_message(proc, {"id": str(msg_counter), "method": "stats", "params": {}})
            response = _read_message(proc)
            self.assert_success(response, "stats after rebuild")
            self.assertEqual(response["result"]["docs"], 5)
            self.assertEqual(response["result"]["vecDocs"], 5, "All embeddings should be restored")
            msg_counter += 1

            _send_message(proc, {"id": str(msg_counter), "method": "memoryStats", "params": {}})
            response = _read_message(proc)
            self.assert_success(response, "memoryStats after rebuild")
            self.assertEqual(response["result"]["docs"], 3)
            self.assertEqual(response["result"]["vecDocs"], 3, "All memory embeddings should be restored")

        finally:
            try:
                proc.stdin.close()
            except Exception:
                pass
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()


if __name__ == "__main__":
    unittest.main(verbosity=2)
