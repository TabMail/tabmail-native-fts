#!/usr/bin/env python3
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""
The caller names the helper's three locations: the database folder (init
`profilePath`), the log folder (`--log-dir` argument) and the model folder
(init `modelDir`). With all three given, a full memory session leaves every
file outside them byte-identical, including a Thunderbird profile's index,
the default model folder and the default log folder. Without them the helper
keeps its defaults.

The helper runs with a temporary HOME (and APPDATA); no test downloads the
model: the default model folder holds unreadable files, and the caller's model
folder links the real cache when there is one (otherwise unreadable files too).

Run:
  TABMAIL_RUST_FTS_HELPER=./target/release/fts_helper python3 tests/run_tests.py
"""

import hashlib
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from tests.test_profile_path_cleanup import ADDON_ID, _profiles_dir
from tests.test_rust_process_parity import _read_message, _send_message

MODEL_FILES = ("model.safetensors", "tokenizer.json", "config.json")
REAL_MODEL_DIR = Path.home() / ".tabmail" / "models" / "all-MiniLM-L6-v2"
DAY_MS = 24 * 60 * 60 * 1000
# flexi_logger's name for the live file (rotated ones get numbers).
LIVE_LOG = "fts_helper.log_rCURRENT.log"


def _tree_hashes(root):
    """Path -> SHA-256 of every file under root, plus every directory (None)."""
    out = {}
    for dirpath, dirnames, filenames in os.walk(root):
        for d in dirnames:
            out[str(Path(dirpath, d).relative_to(root))] = None
        for f in filenames:
            p = Path(dirpath, f)
            out[str(p.relative_to(root))] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


def _write_unreadable_model(model_dir):
    model_dir.mkdir(parents=True)
    for name in MODEL_FILES:
        (model_dir / name).write_bytes(b"not a model")


def _has_real_model():
    return all((REAL_MODEL_DIR / name).is_file() for name in MODEL_FILES)


class TestCallerLocations(unittest.TestCase):
    def setUp(self):
        helper = os.environ.get("TABMAIL_RUST_FTS_HELPER")
        if not helper or not Path(helper).exists():
            self.skipTest("TABMAIL_RUST_FTS_HELPER not set or missing")
        self.helper = str(Path(helper).resolve())
        self.home = Path(tempfile.mkdtemp(prefix="fts_locations_home_"))
        self.caller = Path(tempfile.mkdtemp(prefix="fts_locations_caller_"))

        # A Thunderbird user's files the helper must never touch.
        profile = _profiles_dir(self.home) / "own.default"
        (profile / "extensions").mkdir(parents=True)
        (profile / "extensions" / f"{ADDON_ID}.xpi").write_bytes(b"xpi")
        tb_fts = profile / "browser-extension-data" / ADDON_ID / "tabmail_fts"
        tb_fts.mkdir(parents=True)
        (tb_fts / "fts.db").write_bytes(b"thunderbird mail index")
        (tb_fts / "memory.db").write_bytes(b"thunderbird chat memory")
        _write_unreadable_model(self.home / ".tabmail" / "models" / "all-MiniLM-L6-v2")
        logs = self.home / ".tabmail" / "logs"
        logs.mkdir(parents=True)
        (logs / LIVE_LOG).write_bytes(b"thunderbird helper log\n")

    def tearDown(self):
        shutil.rmtree(self.home, ignore_errors=True)
        shutil.rmtree(self.caller, ignore_errors=True)

    def _spawn(self, args=()):
        env = dict(os.environ)
        env["HOME"] = str(self.home)
        env["USERPROFILE"] = str(self.home)
        env["APPDATA"] = str(self.home / "AppData" / "Roaming")
        return subprocess.Popen(
            [self.helper, *args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=env,
        )

    def _session(self, args, requests):
        """Runs `requests` (no hello) and returns their responses."""
        proc = self._spawn(args)
        try:
            responses = []
            for i, (method, params) in enumerate(requests):
                _send_message(proc, {"id": f"r{i}", "method": method, "params": params})
                responses.append(_read_message(proc))
            return responses
        finally:
            proc.stdin.close()
            proc.wait(timeout=30)
            proc.stdout.close()

    def _caller_model_dir(self):
        model_dir = self.caller / "models"
        if _has_real_model():
            model_dir.symlink_to(REAL_MODEL_DIR, target_is_directory=True)
        else:
            _write_unreadable_model(model_dir)
        return model_dir

    def test_session_with_all_three_locations_leaves_everything_else_identical(self):
        before = _tree_hashes(self.home)
        db_dir = self.caller / "db" / "voice"
        log_dir = self.caller / "logs"
        model_dir = self._caller_model_dir()
        rows = [
            {
                "memId": f"turn:{i}",
                "role": "session",
                "content": f"[USER]: draft the planning note {i}\n\n[AGENT]: Here is the planning note.",
                "sessionId": "persistent_chat",
                "turnIndex": 0,
                "dateMs": 1_700_000_000_000 + i * DAY_MS,
            }
            for i in range(5)
        ]
        responses = self._session(
            ["--log-dir", str(log_dir)],
            [
                ("init", {"profilePath": str(db_dir), "modelDir": str(model_dir)}),
                ("memoryIndexBatch", {"rows": rows, "maxBytes": 1 << 30}),
                ("memorySearch", {"q": "planning"}),
                ("memoryRead", {"timestampMs": rows[2]["dateMs"]}),
                ("memoryStats", {}),
                ("memoryTrim", {"maxBytes": 1 << 30}),
                ("memoryRemoveBatch", {"ids": ["turn:0"]}),
                ("memoryClear", {}),
            ],
        )
        for response in responses:
            self.assertIn("result", response, response)
        init, indexed, _search, _read, stats, trim = (r["result"] for r in responses[:6])

        self.assertEqual(Path(init["memoryDbPath"]), db_dir / "tabmail_fts" / "memory.db")
        # The real model loads only from the caller's folder; the default one is unreadable.
        self.assertEqual(init["hasEmbeddings"], _has_real_model())
        self.assertEqual(indexed["count"], len(rows))
        self.assertTrue(indexed["withinLimit"])
        self.assertEqual(indexed["trimmed"], 0)
        self.assertEqual(stats["docs"], len(rows))
        self.assertGreater(stats["usedBytes"], 0)
        self.assertTrue(trim["withinLimit"])
        self.assertTrue((log_dir / LIVE_LOG).is_file())

        self.assertEqual(_tree_hashes(self.home), before)

    def test_without_locations_the_defaults_are_used(self):
        log = self.home / ".tabmail" / "logs" / LIVE_LOG
        log_before = log.read_bytes()
        (init,) = self._session([], [("init", {"profilePath": str(self.caller / "db" / "voice")})])
        # The default model folder's unreadable files are what it tried to load.
        self.assertFalse(init["result"]["hasEmbeddings"])
        self.assertGreater(len(log.read_bytes()), len(log_before))

    def test_memory_trim_brings_the_database_within_the_limit(self):
        db_dir = self.caller / "db" / "voice"
        rows = [
            {
                "memId": f"turn:{i}",
                "role": "session",
                "content": f"[USER]: note {i}\n\n[AGENT]: " + "quarterly planning notes " * 40,
                "sessionId": "persistent_chat",
                "turnIndex": 0,
                "dateMs": 1_700_000_000_000 + i * DAY_MS,
            }
            for i in range(400)
        ]
        init_params = {"profilePath": str(db_dir), "modelDir": str(self._caller_model_dir())}
        _, _, stats = self._session(
            ["--log-dir", str(self.caller / "logs")],
            [("init", init_params), ("memoryIndexBatch", {"rows": rows}), ("memoryStats", {})],
        )
        # Vectors are stored in chunks that free only when empty, and these
        # 400 rows share one; a limit 10% under keeps most rows either way.
        limit = stats["result"]["usedBytes"] * 9 // 10
        _, trim, after, newest = self._session(
            ["--log-dir", str(self.caller / "logs")],
            [
                ("init", init_params),
                ("memoryTrim", {"maxBytes": limit}),
                ("memoryStats", {}),
                ("memoryRead", {"timestampMs": rows[-1]["dateMs"], "toleranceMs": 60_000}),
            ],
        )
        self.assertTrue(trim["result"]["withinLimit"], trim)
        self.assertGreater(trim["result"]["trimmed"], 0)
        self.assertLessEqual(after["result"]["usedBytes"], limit)
        self.assertEqual(after["result"]["docs"], len(rows) - trim["result"]["trimmed"])
        self.assertGreater(after["result"]["docs"], 0)
        self.assertIn("result", newest, newest)
        self.assertIn("note 399", str(newest["result"]))

    def _rows(self, start, count, words):
        return [
            {
                "memId": f"turn:{i}",
                "role": "session",
                "content": f"[USER]: note {i}\n\n[AGENT]: " + "quarterly planning notes " * words,
                "sessionId": "persistent_chat",
                "turnIndex": 0,
                "dateMs": 1_700_000_000_000 + i * DAY_MS,
            }
            for i in range(start, start + count)
        ]

    def test_index_batch_with_a_limit_removes_the_oldest_and_keeps_the_new_row(self):
        db_dir = self.caller / "db" / "voice"
        init_params = {"profilePath": str(db_dir), "modelDir": str(self._caller_model_dir())}
        logs = ["--log-dir", str(self.caller / "logs")]
        old = self._rows(0, 400, 40)
        _, _, stats = self._session(
            logs, [("init", init_params), ("memoryIndexBatch", {"rows": old}), ("memoryStats", {})]
        )
        limit = stats["result"]["usedBytes"] * 9 // 10
        new = self._rows(400, 1, 40)
        _, batch, after, oldest, newest = self._session(
            logs,
            [
                ("init", init_params),
                ("memoryIndexBatch", {"rows": new, "maxBytes": limit}),
                ("memoryStats", {}),
                ("memoryRead", {"timestampMs": old[0]["dateMs"], "toleranceMs": 60_000}),
                ("memoryRead", {"timestampMs": new[0]["dateMs"], "toleranceMs": 60_000}),
            ],
        )
        result = batch["result"]
        self.assertEqual(result["count"], 1, batch)
        self.assertGreater(result["trimmed"], 0)
        self.assertTrue(result["withinLimit"])
        self.assertLessEqual(result["usedBytes"], limit)
        self.assertLessEqual(after["result"]["usedBytes"], limit)
        self.assertEqual(after["result"]["docs"], len(old) + 1 - result["trimmed"])
        self.assertGreater(after["result"]["docs"], 1)
        self.assertEqual(oldest["result"], [], oldest)
        self.assertEqual(len(newest["result"]), 1, newest)
        self.assertIn("note 400", newest["result"][0]["content"])

    def test_memory_trim_gives_the_disk_space_back(self):
        db_dir = self.caller / "db" / "voice"
        init_params = {"profilePath": str(db_dir), "modelDir": str(self._caller_model_dir())}
        logs = ["--log-dir", str(self.caller / "logs")]
        # Long exchanges, so the text outweighs the shared vector chunk and a
        # trim to half frees pages.
        rows = self._rows(0, 200, 800)
        _, _, nothing_to_do, stats = self._session(
            logs,
            [
                ("init", init_params),
                ("memoryIndexBatch", {"rows": rows}),
                ("memoryTrim", {"maxBytes": 1 << 40}),
                ("memoryStats", {}),
            ],
        )
        self.assertEqual(nothing_to_do["result"]["trimmed"], 0, nothing_to_do)
        self.assertFalse(nothing_to_do["result"]["compacted"])

        def on_disk():
            return sum(
                (db_dir / "tabmail_fts" / name).stat().st_size
                for name in ("memory.db", "memory.db-wal")
                if (db_dir / "tabmail_fts" / name).exists()
            )

        before = on_disk()
        limit = stats["result"]["usedBytes"] // 2
        _, trim = self._session(logs, [("init", init_params), ("memoryTrim", {"maxBytes": limit})])
        self.assertTrue(trim["result"]["withinLimit"], trim)
        self.assertTrue(trim["result"]["compacted"])
        after = on_disk()
        self.assertLessEqual(after, limit, f"{after} bytes on disk, limit {limit}, was {before}")

    def test_invalid_locations_and_limits_are_rejected(self):
        db_dir = self.caller / "db" / "voice"
        bad_init, init = self._session(
            ["--log-dir", str(self.caller / "logs")],
            [("init", {"profilePath": str(db_dir), "modelDir": "models"}), ("init", {"profilePath": str(db_dir)})],
        )
        self.assertIn("modelDir", bad_init["error"])
        # A rejected init opens nothing, and the next init still works.
        self.assertTrue(init["result"]["ok"])

        _, bad_batch, bad_trim, stats = self._session(
            ["--log-dir", str(self.caller / "logs")],
            [
                ("init", {"profilePath": str(db_dir)}),
                ("memoryIndexBatch", {"rows": [{"memId": "turn:1", "content": "x", "dateMs": 1}], "maxBytes": 0}),
                ("memoryTrim", {}),
                ("memoryStats", {}),
            ],
        )
        self.assertIn("error", bad_batch)
        self.assertIn("error", bad_trim)
        # A rejected limit writes nothing.
        self.assertEqual(stats["result"]["docs"], 0)

        proc = self._spawn(["--log-dir", "logs"])
        proc.stdin.close()
        self.assertNotEqual(proc.wait(timeout=30), 0)
        proc.stdout.close()


if __name__ == "__main__":
    unittest.main()
