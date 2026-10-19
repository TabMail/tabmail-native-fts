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
import json
import os
import shutil
import stat
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path

from tests.test_profile_path_cleanup import ADDON_ID, _profiles_dir
from tests.test_rust_process_parity import _read_message, _send_message

MODEL_FILES = ("model.safetensors", "tokenizer.json", "config.json")
REAL_MODEL_DIR = Path.home() / ".tabmail" / "models" / "all-MiniLM-L6-v2"
DAY_MS = 24 * 60 * 60 * 1000
# flexi_logger's name for the live file (rotated ones get numbers).
LIVE_LOG = "fts_helper.log_rCURRENT.log"
# A session that has not finished by then is stuck (a read of a FIFO blocks).
SESSION_DEADLINE_S = 60


def _tree_hashes(root):
    """Path -> SHA-256 of every file under root, every directory (None), and
    every FIFO ("fifo", never opened)."""
    out = {}
    for dirpath, dirnames, filenames in os.walk(root):
        for d in dirnames:
            out[str(Path(dirpath, d).relative_to(root))] = None
        for f in filenames:
            p = Path(dirpath, f)
            if stat.S_ISFIFO(p.lstat().st_mode):
                out[str(p.relative_to(root))] = "fifo"
            else:
                out[str(p.relative_to(root))] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


def _replace_with_fifo(path):
    """Any open of `path` now blocks until the other end opens: a read of it,
    or a write to it, stalls the helper rather than passing unseen."""
    path.unlink()
    os.mkfifo(path)


def _write_orphan(root):
    """An index an earlier add-on release left at `root` (a guessed profile)."""
    fts_dir = root / "browser-extension-data" / ADDON_ID / "tabmail_fts"
    fts_dir.mkdir(parents=True)
    (fts_dir / "fts.db").write_bytes(b"thunderbird mail index")
    (fts_dir / "memory.db").write_bytes(b"thunderbird chat memory")
    return fts_dir


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
        # The helper runs in its own folder whose `models` holds unreadable
        # files, so a relative `modelDir` resolved against it loads nothing
        # and downloads nothing.
        self.cwd = Path(tempfile.mkdtemp(prefix="fts_locations_cwd_"))
        _write_unreadable_model(self.cwd / "models")

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
        shutil.rmtree(self.cwd, ignore_errors=True)

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
            cwd=self.cwd,
        )

    def _session(self, args, requests):
        """Runs `requests` (no hello) and returns their responses. A helper
        still running after SESSION_DEADLINE_S is killed, and its missing
        responses fail the test."""
        proc = self._spawn(args)
        watchdog = threading.Timer(SESSION_DEADLINE_S, proc.kill)
        watchdog.start()
        try:
            responses = []
            for i, (method, params) in enumerate(requests):
                _send_message(proc, {"id": f"r{i}", "method": method, "params": params})
                response = _read_message(proc)
                self.assertIsNotNone(response, f"no response to {method}: the helper stalled or died")
                responses.append(response)
            return responses
        finally:
            proc.stdin.close()
            proc.wait(timeout=SESSION_DEADLINE_S)
            watchdog.cancel()
            proc.stdout.close()

    def _caller_model_dir(self):
        model_dir = self.caller / "models"
        if _has_real_model():
            model_dir.symlink_to(REAL_MODEL_DIR, target_is_directory=True)
        else:
            _write_unreadable_model(model_dir)
        return model_dir

    def test_session_with_all_three_locations_leaves_everything_else_identical(self):
        # The default model and live log become FIFOs, so a read of either (or
        # a write to the log) stalls the session instead of leaving every
        # hash unchanged.
        for name in MODEL_FILES:
            _replace_with_fifo(self.home / ".tabmail" / "models" / "all-MiniLM-L6-v2" / name)
        _replace_with_fifo(self.home / ".tabmail" / "logs" / LIVE_LOG)
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

    def test_previews_keep_a_memory_reply_under_the_native_message_cap(self):
        # Thunderbird refuses a native message over 1 MiB, and memory rows are kept whole: three
        # exchanges of 600 KB come back whole without previews, and cut, all three, with them. No
        # model, so the search is by keyword alone: every path's results go through the same cut.
        cap = 1024 * 1024
        model_dir = self.caller / "nomodel"
        _write_unreadable_model(model_dir)
        rows = [{"memId": f"turn:{i}", "role": "session",
                 "content": f"[USER]: planning {i} " + "note " * 120_000 + "\n\n[AGENT]: Done.",
                 "sessionId": "persistent_chat", "turnIndex": 0, "dateMs": int(time.time() * 1000) - (3 - i) * DAY_MS}
                for i in range(3)]
        responses = self._session(
            ["--log-dir", str(self.caller / "logs"), "--log-level", "off"],
            [
                ("init", {"profilePath": str(self.caller / "db" / "preview"), "modelDir": str(model_dir)}),
                ("memoryIndexBatch", {"rows": rows}),
                ("memorySearch", {"q": "", "ignoreDate": True, "limit": 10}),
                ("memorySearch", {"q": "", "ignoreDate": True, "limit": 10, "previewChars": 400}),
                ("memorySearch", {"q": "planning", "ignoreDate": True, "limit": 10, "previewChars": 400}),
            ],
        )
        whole, listed, found = (r["result"] for r in responses[2:5])
        self.assertGreater(len(json.dumps(whole).encode()), cap)
        self.assertEqual([r["content"] for r in whole], [r["content"] for r in reversed(rows)])
        for result in (listed, found):
            self.assertLess(len(json.dumps(result).encode()), cap // 100)
            self.assertEqual(sorted(r["memId"] for r in result), ["turn:0", "turn:1", "turn:2"])
            for r in result:
                original = next(row for row in rows if row["memId"] == r["memId"])
                self.assertEqual(r["content"], original["content"][:400])

    def test_log_level_off_writes_no_log_anywhere(self):
        _replace_with_fifo(self.home / ".tabmail" / "logs" / LIVE_LOG)
        before = _tree_hashes(self.home)
        log_dir = self.caller / "logs"
        rows = [{"memId": "turn:1", "role": "session", "content": "[USER]: the planning note\n\n[AGENT]: Done.",
                 "sessionId": "persistent_chat", "turnIndex": 0, "dateMs": 1_700_000_000_000}]
        responses = self._session(
            ["--log-dir", str(log_dir), "--log-level", "off"],
            [
                ("init", {"profilePath": str(self.caller / "db" / "voice"), "modelDir": str(self._caller_model_dir())}),
                ("memoryIndexBatch", {"rows": rows}),
                ("memorySearch", {"q": "re: planning", "ignoreDate": True}),
                ("memorySearch", {"q": "planning", "ignoreDate": True}),
                ("memoryStats", {}),
                # A refused request, so the error path is driven too.
                ("queryByDateRange", {}),
            ],
        )
        self.assertIn("result", responses[1], responses[1])
        self.assertIn("result", responses[2], responses[2])
        self.assertIn("result", responses[3], responses[3])
        self.assertEqual(responses[4]["result"]["docs"], 1)
        self.assertIn("error", responses[5], responses[5])
        # Not even a folder: the search text, refused or not, is written nowhere.
        self.assertFalse(log_dir.exists())
        self.assertEqual(_tree_hashes(self.home), before)

        # A level is still a level: at "warn" the file log is in the caller's
        # folder and leaves out info lines; at "debug" it has them.
        listing = ("memorySearch", {"q": "", "ignoreDate": True})
        db = {"profilePath": str(self.caller / "db" / "voice"), "modelDir": str(self.caller / "models")}
        self._session(["--log-dir", str(log_dir), "--log-level", "warn"], [("init", db), listing])
        self.assertTrue(log_dir.is_dir())
        logged = lambda: "".join(p.read_text(errors="replace") for p in log_dir.iterdir() if p.is_file())
        self.assertNotIn("listing all by date", logged())
        self._session(["--log-dir", str(log_dir), "--log-level", "debug"], [("init", db), listing])
        self.assertIn("listing all by date", logged())

        for args in (["--log-level", "verbose"], ["--log-level"], ["--log-level=off"]):
            proc = self._spawn(args)
            proc.stdin.close()
            self.assertNotEqual(proc.wait(timeout=30), 0, args)
            proc.stdout.close()
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

    def test_a_caller_never_moves_or_removes_an_earlier_releases_index(self):
        # The caller's database folder has the add-on's name, which the API
        # allows, and the indexes an earlier release left in ~/.tabmail and in
        # a profile without the add-on are recoverable orphans.
        orphans = [
            _write_orphan(self.home / ".tabmail"),
            _write_orphan(_profiles_dir(self.home) / "no-addon.default"),
        ]
        before = _tree_hashes(self.home)
        logs = ["--log-dir", str(self.caller / "logs")]
        model_dir = str(self._caller_model_dir())
        fresh = self.caller / "fresh" / ADDON_ID
        existing = self.caller / "existing" / ADDON_ID
        (existing / "tabmail_fts").mkdir(parents=True)
        for db_dir in (fresh, existing):
            with self.subTest(db_dir=db_dir.parent.name):
                init, stats = self._session(
                    logs, [("init", {"profilePath": str(db_dir), "modelDir": model_dir}), ("memoryStats", {})]
                )
                self.assertIn("result", init, init)
                self.assertEqual(stats["result"]["docs"], 0)
                self.assertEqual(_tree_hashes(self.home), before)
        for fts_dir in orphans:
            self.assertEqual((fts_dir / "memory.db").read_bytes(), b"thunderbird chat memory")

    def test_a_limit_that_is_not_a_positive_integer_is_refused_and_writes_nothing(self):
        db_dir = self.caller / "db" / "voice"
        row = {"memId": "turn:1", "role": "session", "content": "[USER]: hello", "sessionId": "s", "turnIndex": 0, "dateMs": 1}
        invalid = [0, -1, 1.5, 2.0, 1e30, 2**63, "100", True, False, [100], {"bytes": 100}]
        requests = [("init", {"profilePath": str(db_dir)})]
        for value in invalid:
            requests.append(("memoryIndexBatch", {"rows": [row], "maxBytes": value}))
            requests.append(("memoryTrim", {"maxBytes": value}))
        requests += [
            ("memoryTrim", {"maxBytes": None}),
            ("memoryStats", {}),
            # Without a limit (null or absent) a batch keeps no limit, as before.
            ("memoryIndexBatch", {"rows": [row], "maxBytes": None}),
            ("memoryIndexBatch", {"rows": [dict(row, memId="turn:2", dateMs=2)]}),
            ("memoryIndexBatch", {"rows": [dict(row, memId="turn:3", dateMs=3)], "maxBytes": 1 << 30}),
            ("memoryTrim", {"maxBytes": 1 << 30}),
            ("memoryStats", {}),
        ]
        responses = self._session(["--log-dir", str(self.caller / "logs")], requests)
        refused = responses[1 : 1 + 2 * len(invalid) + 1]
        for (method, params), response in zip(requests[1:], refused):
            with self.subTest(method=method, maxBytes=params["maxBytes"]):
                self.assertIn("maxBytes", response.get("error", ""), response)
        stats, null_batch, absent_batch, limited_batch, trim, after = (r["result"] for r in responses[-6:])
        self.assertEqual(stats["docs"], 0)
        self.assertEqual(null_batch["count"], 1)
        self.assertNotIn("withinLimit", null_batch)
        self.assertNotIn("withinLimit", absent_batch)
        self.assertTrue(limited_batch["withinLimit"])
        self.assertTrue(trim["withinLimit"])
        self.assertEqual(after["docs"], 3)

    def test_invalid_locations_and_limits_are_rejected(self):
        db_dir = self.caller / "db" / "voice"
        bad_values = ["models", "", 5, {}, [str(self.caller)]]
        *bad_inits, init = self._session(
            ["--log-dir", str(self.caller / "logs")],
            [("init", {"profilePath": str(db_dir), "modelDir": value}) for value in bad_values]
            + [("init", {"profilePath": str(db_dir)})],
        )
        for value, bad_init in zip(bad_values, bad_inits):
            with self.subTest(modelDir=value):
                self.assertIn("modelDir", bad_init.get("error", ""), bad_init)
        # The next init still works.
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

        before = _tree_hashes(self.home)
        # A file where the log folder's parent should be: the folder can't be made.
        blocker = self.caller / "a-file"
        blocker.write_bytes(b"in the way")
        for args in (["--log-dir", "logs"], [f"--log-dir={self.caller / 'logs'}"], ["--log-dir", str(blocker / "logs")]):
            proc = self._spawn(args)
            proc.stdin.close()
            self.assertNotEqual(proc.wait(timeout=30), 0, args)
            proc.stdout.close()
        # Refused before logging starts, and a log folder that can't be made
        # is fatal: nothing written to the default log folder.
        self.assertEqual(_tree_hashes(self.home), before)

    def test_a_rejected_init_creates_opens_and_moves_nothing(self):
        # An index an earlier release left, which an init's clean-up could move.
        orphan = _write_orphan(self.home / ".tabmail")
        before = _tree_hashes(self.home)
        db_dir = self.caller / "db" / "voice"
        bad_values = ["models", "", 5, {}, [str(self.caller)]]
        responses = self._session(
            ["--log-dir", str(self.caller / "logs")],
            [("init", {"profilePath": str(db_dir), "modelDir": value}) for value in bad_values],
        )
        for value, response in zip(bad_values, responses):
            with self.subTest(modelDir=value):
                self.assertIn("modelDir", response.get("error", ""), response)
        self.assertFalse(db_dir.exists())
        self.assertEqual(_tree_hashes(self.home), before)
        self.assertEqual((orphan / "memory.db").read_bytes(), b"thunderbird chat memory")

    def test_a_batch_whose_own_row_goes_over_the_limit_ends_within_it(self):
        db_dir = self.caller / "db" / "voice"
        init_params = {"profilePath": str(db_dir), "modelDir": str(self._caller_model_dir())}
        logs = ["--log-dir", str(self.caller / "logs")]
        _, _, stats = self._session(
            logs, [("init", init_params), ("memoryIndexBatch", {"rows": self._rows(0, 400, 40)}), ("memoryStats", {})]
        )
        limit = stats["result"]["usedBytes"] * 9 // 10
        # One new exchange (about 200 KB) far larger than the room the trim
        # leaves below the limit: trimming before it is written would end over.
        _, batch, after = self._session(
            logs,
            [
                ("init", init_params),
                ("memoryIndexBatch", {"rows": self._rows(400, 1, 8_000), "maxBytes": limit}),
                ("memoryStats", {}),
            ],
        )
        result = batch["result"]
        self.assertEqual(result["count"], 1, batch)
        self.assertTrue(result["withinLimit"], batch)
        self.assertLessEqual(after["result"]["usedBytes"], limit)
        self.assertEqual(result["usedBytes"], after["result"]["usedBytes"])

if __name__ == "__main__":
    unittest.main()
