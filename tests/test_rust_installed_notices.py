#!/usr/bin/env python3
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""
The third-party notices next to an installed helper belong to the installed binary (#62).

Every installer puts `fts_helper-THIRD_PARTY_NOTICES.txt` beside the helper. The helper
carries its own notices compiled in and prints them with `--print-third-party-notices`. An
update installs the new build's notices with its binary as a pair (on Windows in the
`--apply-update` process); if the binary swap fails the old notices are put back. `hello`
only creates the file when it is missing (an install from before the notices existed,
updated by an older updater) and never replaces one. A copy under another name (a client
that bundles the helper) writes nothing.

Runs the real binary named in TABMAIL_RUST_FTS_HELPER, copied into a temporary folder, with
a temporary HOME/APPDATA. Nothing is downloaded.
"""

import json
import os
import shutil
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
REPO_NOTICES = REPO / "THIRD_PARTY_NOTICES.txt"
NOTICES_NAME = "fts_helper-THIRD_PARTY_NOTICES.txt"
EXE_NAME = "fts_helper.exe" if os.name == "nt" else "fts_helper"
SESSION_DEADLINE_S = 60


def _send(proc, message):
    encoded = json.dumps(message).encode("utf-8")
    proc.stdin.write(struct.pack("=I", len(encoded)))
    proc.stdin.write(encoded)
    proc.stdin.flush()


def _read(proc):
    raw = proc.stdout.read(4)
    if not raw:
        return None
    return json.loads(proc.stdout.read(struct.unpack("=I", raw)[0]).decode("utf-8"))


class TestInstalledNotices(unittest.TestCase):
    def setUp(self):
        helper = os.environ.get("TABMAIL_RUST_FTS_HELPER")
        if not helper or not Path(helper).exists():
            self.skipTest("TABMAIL_RUST_FTS_HELPER not set or missing")
        self.helper = Path(helper).resolve()
        self.root = Path(tempfile.mkdtemp(prefix="tm_installed_notices_"))
        self.home = self.root / "home"
        self.home.mkdir()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.cwd = self.root / "cwd"
        self.cwd.mkdir()

    def tearDown(self):
        os.chmod(self.bin, 0o755)
        shutil.rmtree(self.root, ignore_errors=True)

    def _env(self):
        env = dict(os.environ)
        env["HOME"] = str(self.home)
        env["USERPROFILE"] = str(self.home)
        env["APPDATA"] = str(self.home / "AppData" / "Roaming")
        return env

    def _install(self, name=EXE_NAME):
        exe = self.bin / name
        shutil.copy2(self.helper, exe)
        os.chmod(exe, 0o755)
        return exe

    def _hello(self, exe):
        proc = subprocess.Popen([str(exe)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, env=self._env(), cwd=self.cwd)
        try:
            _send(proc, {"id": "1", "method": "hello", "params": {"addonVersion": "test"}})
            resp = _read(proc)
            proc.stdin.close()
            proc.wait(timeout=SESSION_DEADLINE_S)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            proc.stdout.close()
        self.assertIsNotNone(resp, "no hello response")
        self.assertNotIn("error", resp)
        self.assertEqual(resp["result"]["type"], "hello-response")

    def _apply_update(self, target, staged, staged_notices=None):
        args = [str(self.helper), "--apply-update", "--target", str(target), "--staged", str(staged)]
        if staged_notices is not None:
            args += ["--staged-notices", str(staged_notices)]
        return subprocess.run(args, env=self._env(), cwd=self.cwd, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL, timeout=SESSION_DEADLINE_S).returncode

    def _notices(self):
        return (self.bin / NOTICES_NAME).read_bytes()

    # ---- what the binary carries ----

    def test_the_helper_prints_exactly_its_notices_and_touches_nothing(self):
        out = subprocess.run([str(self.helper), "--print-third-party-notices"], env=self._env(), cwd=self.cwd,
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=SESSION_DEADLINE_S)
        self.assertEqual(out.returncode, 0)
        self.assertEqual(out.stdout, REPO_NOTICES.read_bytes())
        self.assertEqual(list(self.home.iterdir()), [])
        self.assertEqual(list(self.cwd.iterdir()), [])

    # ---- the update installs the pair ----

    def test_an_applied_update_installs_the_new_binary_and_its_notices_together(self):
        target = self.bin / EXE_NAME
        target.write_bytes(b"the helper build being replaced")
        (self.bin / NOTICES_NAME).write_text("notices of the build being replaced\n")
        staged = self.root / "staged-helper"
        shutil.copy2(self.helper, staged)
        staged_notices = self.root / "staged-notices"
        staged_notices.write_bytes(REPO_NOTICES.read_bytes())
        self.assertEqual(self._apply_update(target, staged, staged_notices), 0)
        self.assertEqual(target.read_bytes(), self.helper.read_bytes())
        self.assertEqual(self._notices(), REPO_NOTICES.read_bytes())
        self.assertNotIn(NOTICES_NAME + ".backup", [p.name for p in self.bin.iterdir()])
        # The new build's first hello leaves the pair as it is.
        self._hello(target)
        self.assertEqual(self._notices(), REPO_NOTICES.read_bytes())

    def test_an_update_whose_binary_swap_fails_keeps_the_old_pair(self):
        target = self.bin / EXE_NAME
        target.write_bytes(b"the installed helper")
        old = b"notices of the installed helper\n"
        (self.bin / NOTICES_NAME).write_bytes(old)
        staged_notices = self.root / "staged-notices"
        staged_notices.write_bytes(REPO_NOTICES.read_bytes())
        self.assertNotEqual(self._apply_update(target, self.root / "no-staged-helper", staged_notices), 0)
        self.assertEqual(target.read_bytes(), b"the installed helper")
        self.assertEqual(self._notices(), old)
        self.assertEqual(sorted(p.name for p in self.bin.iterdir()), sorted([EXE_NAME, NOTICES_NAME]))

    def test_an_install_without_notices_updated_by_an_older_updater_gets_them_on_hello(self):
        # An older updater swaps only the binary (no --staged-notices); the install had none.
        target = self.bin / EXE_NAME
        target.write_bytes(b"a helper from before the notices")
        staged = self.root / "staged-helper"
        shutil.copy2(self.helper, staged)
        self.assertEqual(self._apply_update(target, staged), 0)
        self.assertFalse((self.bin / NOTICES_NAME).exists())
        self._hello(target)
        self.assertEqual(self._notices(), REPO_NOTICES.read_bytes())

    # ---- hello creates, never replaces ----

    def test_hello_never_replaces_the_notices_next_to_it(self):
        # What an older process still running after an update would do: it must not.
        installed = b"notices of the build installed here\n"
        (self.bin / NOTICES_NAME).write_bytes(installed)
        self._hello(self._install())
        self.assertEqual(self._notices(), installed)

    @unittest.skipIf(os.name == "nt", "folder permissions are POSIX modes")
    def test_notices_it_cannot_write_do_not_fail_hello(self):
        exe = self._install()
        os.chmod(self.bin, 0o555)
        self._hello(exe)
        os.chmod(self.bin, 0o755)
        self.assertEqual([p.name for p in self.bin.iterdir()], [EXE_NAME])

    def test_a_copy_under_another_name_writes_no_notices(self):
        self._hello(self._install("voice-memory"))
        self.assertEqual([p.name for p in self.bin.iterdir()], ["voice-memory"])


if __name__ == "__main__":
    unittest.main()
