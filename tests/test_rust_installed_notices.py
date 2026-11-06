#!/usr/bin/env python3
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""
The third-party notices next to an installed helper belong to the installed binary (#62).

Every installer puts `fts_helper-THIRD_PARTY_NOTICES.txt` beside the helper. The helper
carries its own notices compiled in and prints them with `--print-third-party-notices`. An
update installs the new build's notices with its binary as a pair (on Windows in the
`--apply-update` process); if the binary swap fails the old notices are put back. On `hello`
the helper makes the notices beside it its own while the file at its path is still the binary
it started from: that repairs an update applied by an older updater and one whose rollback did
not finish, and an older process still running after an update writes nothing. A copy under
another name (a client that bundles the helper) writes nothing.

Runs the real binary named in TABMAIL_RUST_FTS_HELPER, copied into a temporary folder, with
a temporary HOME/APPDATA. Updates are served from a local HTTP server and signed with a key
made for the test, which the helper is told to trust through TM_UPDATE_PUBLIC_KEYS_BASE64.
"""

import base64
import functools
import hashlib
import json
import os
import re
import shutil
import struct
import subprocess
import tempfile
import threading
import unittest
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
REPO_NOTICES = REPO / "THIRD_PARTY_NOTICES.txt"
NOTICES_NAME = "fts_helper-THIRD_PARTY_NOTICES.txt"
EXE_NAME = "fts_helper.exe" if os.name == "nt" else "fts_helper"
SESSION_DEADLINE_S = 60
MODEL_FILES = ("model.safetensors", "tokenizer.json", "config.json")
UPDATE_PLATFORM = "test-platform"
VERSION = re.search(r'^version = "([^"]+)"', (REPO / "Cargo.toml").read_text(), re.M).group(1)


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


class _HelperFolder(unittest.TestCase):
    """A temporary install folder and HOME for the real helper; no tests of its own."""

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

    def _start(self, exe, env=None):
        return subprocess.Popen([str(exe)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, env=env or self._env(), cwd=self.cwd)

    def _call(self, proc, method, params):
        _send(proc, {"id": method, "method": method, "params": params})
        resp = _read(proc)
        self.assertIsNotNone(resp, f"no {method} response")
        return resp

    def _say_hello(self, proc):
        resp = self._call(proc, "hello", {"addonVersion": "test"})
        self.assertNotIn("error", resp)
        self.assertEqual(resp["result"]["type"], "hello-response")

    def _finish(self, proc):
        try:
            proc.stdin.close()
            proc.wait(timeout=SESSION_DEADLINE_S)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            proc.stdout.close()
        return proc.returncode

    def _hello(self, exe):
        proc = self._start(exe)
        try:
            self._say_hello(proc)
        finally:
            self._finish(proc)

    def _apply_update(self, target, staged, staged_notices):
        args = [str(self.helper), "--apply-update", "--target", str(target), "--staged", str(staged),
                "--staged-notices", str(staged_notices)]
        return subprocess.run(args, env=self._env(), cwd=self.cwd, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL, timeout=SESSION_DEADLINE_S).returncode

    def _notices(self):
        return (self.bin / NOTICES_NAME).read_bytes()



class TestInstalledNotices(_HelperFolder):
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
        # An older updater (it runs its own binary) swaps in only the binary; the install had none.
        target = self.bin / EXE_NAME
        target.write_bytes(b"a helper from before the notices")
        staged = self.root / "staged-helper"
        shutil.copy2(self.helper, staged)
        os.replace(staged, target)
        self.assertFalse((self.bin / NOTICES_NAME).exists())
        self._hello(target)
        self.assertEqual(self._notices(), REPO_NOTICES.read_bytes())

    def test_an_older_updater_s_update_gets_its_notices_on_hello(self):
        # An older updater (it runs its own binary) swaps in only the binary and leaves the
        # previous build's notices.
        target = self.bin / EXE_NAME
        target.write_bytes(b"the previous helper build")
        (self.bin / NOTICES_NAME).write_text("notices of the previous helper build\n")
        staged = self.root / "staged-helper"
        shutil.copy2(self.helper, staged)
        os.replace(staged, target)
        self.assertEqual(self._notices(), b"notices of the previous helper build\n")
        self._hello(target)
        self.assertEqual(self._notices(), REPO_NOTICES.read_bytes())
        self.assertEqual(sorted(p.name for p in self.bin.iterdir()), sorted([EXE_NAME, NOTICES_NAME]))

    def test_hello_finishes_an_update_whose_rollback_did_not(self):
        # The new build's notices went in, the binary swap failed and the old notices could not
        # be put back: the installed binary's next hello makes the pair its own again.
        exe = self._install()
        (self.bin / NOTICES_NAME).write_text("notices of a build that never got installed\n")
        (self.bin / (NOTICES_NAME + ".backup")).write_bytes(REPO_NOTICES.read_bytes())
        self._hello(exe)
        self.assertEqual(self._notices(), REPO_NOTICES.read_bytes())
        self.assertEqual(sorted(p.name for p in self.bin.iterdir()), sorted([EXE_NAME, NOTICES_NAME]))

    # ---- hello writes only while it is the installed binary ----

    def test_a_process_whose_binary_was_replaced_since_it_started_writes_nothing(self):
        # An older process still running after an update must not put its notices back.
        exe = self._install()
        proc = self._start(exe)
        try:
            self._say_hello(proc)
            self.assertEqual(self._notices(), REPO_NOTICES.read_bytes())
            newer = self.root / "newer-helper"
            newer.write_bytes(b"a newer helper build")
            os.replace(newer, exe)
            (self.bin / NOTICES_NAME).write_text("notices of the newer helper build\n")
            self._say_hello(proc)
        finally:
            self._finish(proc)
        self.assertEqual(self._notices(), b"notices of the newer helper build\n")

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


class _SignedUpdates(_HelperFolder):
    """Serves the helper's own build over local HTTP, signed with a throwaway key the helper
    trusts through `TM_UPDATE_PUBLIC_KEYS_BASE64`; no tests of its own."""

    def setUp(self):
        super().setUp()
        self.keys = self.root / "keys"
        self.keys.mkdir()
        key = self.keys / "update-test-key.pem"
        subprocess.run(["openssl", "genpkey", "-algorithm", "ed25519", "-out", str(key)], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        der = subprocess.run(["openssl", "pkey", "-in", str(key), "-pubout", "-outform", "DER"], check=True,
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout
        self.key = key
        self.public_key = base64.b64encode(der[-32:]).decode("ascii")
        self.served = self.root / "served"
        self.served.mkdir()
        handler = functools.partial(_QuietHandler, directory=str(self.served))
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.http_thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.http_thread.start()
        self.exe = self._install()
        self.old_notices = b"notices of the installed helper build\n"
        (self.bin / NOTICES_NAME).write_bytes(self.old_notices)

    def tearDown(self):
        self.httpd.shutdown()
        self.http_thread.join(timeout=SESSION_DEADLINE_S)
        self.httpd.server_close()
        super().tearDown()

    def _update_env(self):
        env = self._env()
        env["TM_UPDATE_PUBLIC_KEYS_BASE64"] = self.public_key
        return env

    def _update_params(self, version):
        served = self.served / "fts_helper-download"
        shutil.copy2(self.helper, served)
        url = f"http://127.0.0.1:{self.httpd.server_address[1]}/{served.name}"
        sha = hashlib.sha256(served.read_bytes()).hexdigest()
        message = self.keys / "message"
        message.write_text(f"tabmail-native-fts|host_version={version}|platform={UPDATE_PLATFORM}|sha256={sha}|url={url}")
        signature = subprocess.run(["openssl", "pkeyutl", "-sign", "-inkey", str(self.key), "-rawin", "-in", str(message)],
                                   check=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout
        return {"targetVersion": version, "updateUrl": url, "sha256": sha, "platform": UPDATE_PLATFORM,
                "signature": base64.b64encode(signature).decode("ascii")}

    def _init(self, proc):
        models = self.root / "models"
        models.mkdir(exist_ok=True)
        for name in MODEL_FILES:
            (models / name).write_bytes(b"not a model")
        resp = self._call(proc, "init", {"profilePath": str(self.root / "profile"), "modelDir": str(models)})
        self.assertNotIn("error", resp)

    def _update(self, version, after_init):
        proc = self._start(self.exe, self._update_env())
        try:
            # No hello: it would make the notices this build's, and the served build is this
            # build, so the old notices would no longer show whether the update installed any.
            if after_init:
                self._init(proc)
            resp = self._call(proc, "updateRequest", self._update_params(version))
        finally:
            code = self._finish(proc)
        return resp, code



class TestUpdateRequestInstallsTheNotices(_SignedUpdates):
    """The `updateRequest` a client sends, before and after `init`, ends with the served
    build's binary and notices installed together, or with the installed pair untouched."""

    def _assert_installed_the_served_pair(self, resp, code):
        self.assertNotIn("error", resp)
        self.assertTrue(resp["result"]["success"])
        self.assertFalse(resp["result"]["requiresRestart"])
        self.assertEqual(code, 0)
        self.assertEqual(self.exe.read_bytes(), (self.served / "fts_helper-download").read_bytes())
        self.assertEqual(self._notices(), REPO_NOTICES.read_bytes())
        self.assertEqual([p.name for p in self.bin.iterdir() if p.name.endswith(".new")], [])

    @unittest.skipIf(os.name == "nt", "Windows applies the update in a separate process")
    def test_an_update_requested_before_init_installs_the_binary_and_its_notices(self):
        self._assert_installed_the_served_pair(*self._update(VERSION, after_init=False))

    @unittest.skipIf(os.name == "nt", "Windows applies the update in a separate process")
    def test_an_update_requested_after_init_installs_the_binary_and_its_notices(self):
        self._assert_installed_the_served_pair(*self._update(VERSION, after_init=True))

    def test_an_update_whose_build_credits_another_version_leaves_the_pair_alone(self):
        installed = self.exe.read_bytes()
        # The served build is a copy of the installed one, so bytes alone cannot show a swap.
        before = os.stat(self.exe)
        resp, code = self._update(VERSION + ".1", after_init=False)
        self.assertNotIn("result", resp)
        self.assertTrue(resp["error"])
        self.assertEqual(code, 0)
        after = os.stat(self.exe)
        self.assertEqual((after.st_ino, after.st_mtime_ns), (before.st_ino, before.st_mtime_ns))
        self.assertEqual(self.exe.read_bytes(), installed)
        self.assertEqual(self._notices(), self.old_notices)
        self.assertEqual([p.name for p in self.bin.iterdir() if p.name.endswith(".new")], [])


class TestAFailedUpdateIsAnswered(_SignedUpdates):
    """An update method that fails, before or after `init`, is answered with `{ id, error }`
    and the helper goes on serving the session (#64)."""

    def _failing_update_requests(self):
        refused = self._update_params(VERSION + ".1")
        forged = dict(self._update_params(VERSION))
        forged["signature"] = refused["signature"]
        missing = dict(self._update_params(VERSION))
        del missing["sha256"]
        return {"build crediting another version": refused, "signature over other metadata": forged,
                "missing parameter": missing}

    def _assert_answered_with_an_error(self, method, params, after_init):
        proc = self._start(self.exe, self._update_env())
        try:
            if after_init:
                self._init(proc)
            resp = self._call(proc, method, params)
            self.assertEqual(resp["id"], method)
            self.assertNotIn("result", resp)
            self.assertIsInstance(resp["error"], str)
            self.assertTrue(resp["error"])
            # The session goes on: before init it still says hello and initializes; after init
            # the reader thread still answers.
            if not after_init:
                self._say_hello(proc)
                self._init(proc)
            stats = self._call(proc, "stats", {})
            self.assertNotIn("error", stats)
        finally:
            code = self._finish(proc)
        self.assertEqual(code, 0)
        self.assertEqual(self.exe.read_bytes(), self.helper.read_bytes())

    def test_a_failed_update_request_is_answered_and_the_helper_carries_on(self):
        for failure, params in self._failing_update_requests().items():
            for after_init in (False, True):
                with self.subTest(failure=failure, after_init=after_init):
                    self._assert_answered_with_an_error("updateRequest", params, after_init)

    def test_a_failed_update_check_is_answered_and_the_helper_carries_on(self):
        for after_init in (False, True):
            with self.subTest(after_init=after_init):
                self._assert_answered_with_an_error("updateCheck", {}, after_init)


class _QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        return


if __name__ == "__main__":
    unittest.main()
