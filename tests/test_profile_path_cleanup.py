#!/usr/bin/env python3
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""
Init with profilePath moves an index the old profile guess left where no
profile reads it into a profile without an index, removes the rest, and keeps
every index a profile does read.

The helper runs with a temporary HOME (and APPDATA), so it only ever sees
the fixture's profiles.

Run:
  TABMAIL_RUST_FTS_HELPER=./target/release/fts_helper python3 tests/run_tests.py
"""

import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from tests.test_rust_process_parity import _read_message, _send_message

ADDON_ID = "thunderbird@tabmail.ai"


def _profiles_dir(home):
    if sys.platform == "darwin":
        return home / "Library" / "Thunderbird" / "Profiles"
    if sys.platform.startswith("win"):
        return home / "AppData" / "Roaming" / "Thunderbird" / "Profiles"
    return home / ".thunderbird"


def _write_index(profile):
    fts_dir = profile / "browser-extension-data" / ADDON_ID / "tabmail_fts"
    fts_dir.mkdir(parents=True)
    (fts_dir / "fts.db").write_bytes(b"index")
    return fts_dir


def _install_addon(profile):
    (profile / "extensions").mkdir(parents=True)
    (profile / "extensions" / f"{ADDON_ID}.xpi").write_bytes(b"xpi")


class TestProfilePathCleanup(unittest.TestCase):
    def setUp(self):
        helper = os.environ.get("TABMAIL_RUST_FTS_HELPER")
        if not helper or not Path(helper).exists():
            self.skipTest("TABMAIL_RUST_FTS_HELPER not set or missing")
        self.helper = str(Path(helper).resolve())
        self.home = Path(tempfile.mkdtemp(prefix="fts_profile_cleanup_test_"))

    def tearDown(self):
        shutil.rmtree(self.home, ignore_errors=True)

    def _init(self, params, requests=()):
        """Runs hello, init and then `requests` in a helper whose HOME is the
        fixture; returns the init result and the requests' results."""
        env = dict(os.environ)
        env["HOME"] = str(self.home)
        env["USERPROFILE"] = str(self.home)
        env["APPDATA"] = str(self.home / "AppData" / "Roaming")
        proc = subprocess.Popen(
            [self.helper],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=env,
        )
        try:
            _send_message(proc, {"id": "1", "method": "hello", "params": {"addonVersion": "1.9.0"}})
            self.assertIn("result", _read_message(proc))
            results = []
            for i, (method, request_params) in enumerate([("init", params), *requests]):
                _send_message(proc, {"id": f"r{i}", "method": method, "params": request_params})
                response = _read_message(proc)
                self.assertIn("result", response, response)
                results.append(response["result"])
            return results[0] if not requests else results
        finally:
            proc.stdin.close()
            proc.wait(timeout=30)
            proc.stdout.close()

    def _link_models(self):
        real_models = Path.home() / ".tabmail" / "models"
        fallback = self.home / ".tabmail"
        fallback.mkdir(exist_ok=True)
        if real_models.is_dir():
            # Reuse the cached embedding model instead of downloading it.
            (fallback / "models").symlink_to(real_models, target_is_directory=True)
        return fallback

    def test_init_removes_only_indexes_no_profile_reads(self):
        fallback = self._link_models()
        fallback_index = _write_index(fallback)

        profiles = _profiles_dir(self.home)
        own = profiles / "own.default"
        _install_addon(own)
        crash_reports = profiles / "Crash Reports"
        crash_index = _write_index(crash_reports)
        other = profiles / "other.default"
        _install_addon(other)
        other_index = _write_index(other)

        data_dir = own / "browser-extension-data" / ADDON_ID
        # This profile already has an index, so every orphan is removed.
        (data_dir / "tabmail_fts").mkdir(parents=True)
        result = self._init({"profilePath": str(data_dir)})
        self.assertEqual(Path(result["addonDataDir"]), data_dir)

        self.assertFalse(fallback_index.exists())
        self.assertFalse(crash_index.exists())
        self.assertTrue((other_index / "fts.db").exists())
        self.assertTrue((data_dir / "tabmail_fts").is_dir())
        self.assertTrue(crash_reports.is_dir())
        self.assertTrue(fallback.is_dir())

    def test_init_moves_an_orphaned_index_into_a_profile_without_one(self):
        fallback = self._link_models()
        rows = [
            {
                "msgId": f"account1:/INBOX:msg-{i}@example.com",
                "subject": f"Quarterly planning {i}",
                "from_": "sender@example.com",
                "to_": "recipient@example.com",
                "body": "Notes from the planning meeting.",
                "dateMs": 1700000000000 + i * 1000,
                "hasAttachments": False,
            }
            for i in range(3)
        ]
        # An older helper guessed ~/.tabmail and indexed this profile's mail there.
        orphan_dir = fallback / "browser-extension-data" / ADDON_ID
        self._init({"profilePath": str(orphan_dir)}, [("indexBatch", {"rows": rows})])

        own = _profiles_dir(self.home) / "own.default"
        _install_addon(own)
        data_dir = own / "browser-extension-data" / ADDON_ID
        _, stats = self._init({"profilePath": str(data_dir)}, [("stats", {})])

        self.assertEqual(stats["docs"], len(rows))
        self.assertTrue((data_dir / "tabmail_fts" / "fts.db").exists())
        self.assertFalse((fallback / "browser-extension-data").exists())

    # Without profilePath (add-on releases before it) the helper still guesses.

    def test_guess_takes_the_most_recently_modified_profile(self):
        self._link_models()
        profiles = _profiles_dir(self.home)
        older = profiles / "older.default"
        newest = profiles / "newest.default"
        hidden = profiles / ".hidden"
        for directory in (newest, older, hidden):
            directory.mkdir(parents=True)
        now = time.time()
        os.utime(older, (now - 3600, now - 3600))
        os.utime(newest, (now - 60, now - 60))
        os.utime(hidden, (now, now))

        self.assertEqual(Path(self._init({})["tbProfile"]), newest)

    def test_guess_falls_back_without_profiles(self):
        fallback = self._link_models()
        self.assertEqual(Path(self._init({})["tbProfile"]), fallback)

    def test_guess_falls_back_with_an_empty_profiles_directory(self):
        fallback = self._link_models()
        _profiles_dir(self.home).mkdir(parents=True)
        self.assertEqual(Path(self._init({})["tbProfile"]), fallback)


if __name__ == "__main__":
    unittest.main()
