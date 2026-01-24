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
            self.assertEqual(len(response["result"]), 1)

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
