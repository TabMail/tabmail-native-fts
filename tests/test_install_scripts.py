#!/usr/bin/env python3
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""The install scripts install the binary and the third-party notices of one release: the one the
update manifest names. The "-latest" binary and notices are replaced one at a time, so two "-latest"
downloads can pair a new binary with old notices."""

import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INSTALL_SH = ROOT / "scripts" / "install.sh"
INSTALL_PS1 = ROOT / "scripts" / "install.ps1"
CDN = "https://cdn.tabmail.ai/releases/"

# Stands in for curl: serves https://cdn.tabmail.ai/releases/<path> from $FAKE_CDN/<path>. For a file
# that is not there it fails only with -f, as curl does on a 404; without -f it saves the error page
# and succeeds. Every URL asked for is logged. With $FAKE_HOLD set, a notices download waits, after
# creating $FAKE_HOLD.waiting, until $FAKE_HOLD.go exists.
FAKE_CURL = """#!/bin/sh
url="" out="" fail=""
while [ $# -gt 0 ]; do
    case "$1" in
        -o) out="$2"; shift 2 ;;
        --fail) fail=1; shift ;;
        --*) shift ;;
        -*f*) fail=1; shift ;;
        -*) shift ;;
        *) url="$1"; shift ;;
    esac
done
echo "$url" >> "$FAKE_LOG"
case "$url" in
    *-THIRD_PARTY_NOTICES.txt)
        if [ -n "$FAKE_HOLD" ]; then
            : > "$FAKE_HOLD.waiting"
            while [ ! -f "$FAKE_HOLD.go" ]; do "$SLEEP" 0.1; done
        fi ;;
esac
path="${url#%s}"
if [ "$path" = "$url" ] || [ ! -f "$FAKE_CDN/$path" ]; then
    [ -n "$fail" ] && { echo "curl: (22) 404 $url" >&2; exit 22; }
    echo "404 Not Found" > "$out"
    exit 0
fi
cp "$FAKE_CDN/$path" "$out"
""" % CDN

# Stands in for wget: as wget -O does, it empties the output file first and exits 8 on a 404.
# Every URL asked for is logged.
FAKE_WGET = """#!/bin/sh
url="" out=""
while [ $# -gt 0 ]; do
    case "$1" in
        -O) out="$2"; shift 2 ;;
        -*) shift ;;
        *) url="$1"; shift ;;
    esac
done
: > "$out"
echo "$url" >> "$FAKE_LOG"
path="${url#%s}"
[ "$path" != "$url" ] && [ -f "$FAKE_CDN/$path" ] || exit 8
cp "$FAKE_CDN/$path" "$out"
""" % CDN

# Stands in for mv, refusing to replace the installed helper, as Windows refuses while it runs.
REFUSING_MV = """#!/bin/sh
for last; do :; done
case "$last" in */fts_helper) echo "mv: $last: Operation not permitted" >&2; exit 1 ;; esac
exec "$REAL_MV" "$@"
"""

# What install.sh runs besides the download tool: only these are on PATH, so the test decides
# whether curl or wget is there.
TOOLS = ("uname", "mkdir", "sed", "tr", "chmod", "mv", "rm", "cat", "cp")


def executable(path, text):
    path.write_text(text)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


@unittest.skipIf(sys.platform not in ("darwin", "linux"), "install.sh runs on macOS and Linux")
@unittest.skipIf(sys.platform == "linux" and os.uname().machine != "x86_64", "install.sh refuses Linux other than x86_64")
class TestInstallSh(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="tabmail-install-sh-"))
        self.addCleanup(shutil.rmtree, self.tmp)
        self.home = self.tmp / "home"
        self.home.mkdir()
        self.shims = self.tmp / "shims"
        self.shims.mkdir()
        tools = self.tmp / "tools"
        tools.mkdir()
        for tool in TOOLS:
            (tools / tool).symlink_to(shutil.which(tool, path="/usr/bin:/bin"))
        self.real_mv = str(tools / "mv")
        self.path = f"{self.shims}:{tools}"
        executable(self.shims / "curl", FAKE_CURL)
        if sys.platform == "darwin":
            self.platform = "macos-universal"
            self.install_dir = self.home / "Library" / "Application Support" / "TabMail" / "native"
            self.messaging = [self.home / "Library" / "Application Support" / "Mozilla" / "NativeMessagingHosts",
                              self.home / "Library" / "Mozilla" / "NativeMessagingHosts"]
        else:
            self.platform = "linux-x86_64"
            self.install_dir = self.home / ".local" / "share" / "tabmail" / "native"
            self.messaging = [self.home / ".mozilla" / "native-messaging-hosts"]
        self.log = self.tmp / "requests.log"
        self.cdn = self.tmp / "cdn"
        self.release = self.cdn / self.platform
        self.release.mkdir(parents=True)
        # The "-latest" copies belong to a newer release whose manifest is not published yet.
        self.publish("latest", binary="#!/bin/sh\necho latest\n", notices="notices of latest\n")
        self.publish("0.12.3", binary="#!/bin/sh\necho 0.12.3\n", notices="notices of 0.12.3\n")
        self.manifest('"0.12.3"')

    def publish(self, version, binary=None, notices=None):
        if binary is not None:
            (self.release / f"fts_helper-{version}").write_text(binary)
        if notices is not None:
            (self.release / f"fts_helper-{version}-THIRD_PARTY_NOTICES.txt").write_text(notices)

    def manifest(self, version_json):
        (self.release / "update-manifest.json").write_text(
            '{\n  "latest": {\n    "version": %s,\n'
            '    "downloadUrl": "%s%s/fts_helper-0.12.3",\n    "sha256": "00"\n  }\n}\n'
            % (version_json, CDN, self.platform))

    def raw_manifest(self, text):
        (self.release / "update-manifest.json").write_text(text)

    def env(self, **extra):
        return {"HOME": str(self.home), "PATH": self.path, "FAKE_CDN": str(self.cdn), "REAL_MV": self.real_mv,
                "FAKE_LOG": str(self.log), "SLEEP": shutil.which("sleep", path="/usr/bin:/bin"), **extra}

    def install(self):
        return subprocess.run(["/bin/bash", str(INSTALL_SH)], env=self.env(), capture_output=True, text=True)

    # An install held before its notices download until `go` is created; waits until it is held.
    def held_install(self, hold):
        process = subprocess.Popen(["/bin/bash", str(INSTALL_SH)], env=self.env(FAKE_HOLD=str(hold)),
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.addCleanup(lambda: process.poll() is None and process.kill())
        for _ in range(300):
            if Path(f"{hold}.waiting").exists():
                return process
            self.assertIsNone(process.poll(), "the install ended before its notices download")
            time.sleep(0.1)
        self.fail("the install never reached its notices download")

    def release_held(self, hold, process):
        Path(f"{hold}.go").touch()
        output, _ = process.communicate(timeout=60)
        self.assertEqual(process.returncode, 0, output)

    def requested(self):
        return self.log.read_text().splitlines() if self.log.exists() else []

    def installed(self):
        return sorted(p.name for p in self.install_dir.iterdir())

    def assert_pair(self, version):
        self.assertEqual((self.install_dir / "fts_helper").read_text(), f"#!/bin/sh\necho {version}\n")
        self.assertEqual((self.install_dir / "fts_helper-THIRD_PARTY_NOTICES.txt").read_text(), f"notices of {version}\n")
        self.assertEqual(self.installed(), ["fts_helper", "fts_helper-THIRD_PARTY_NOTICES.txt"], "no download left behind")

    def test_installs_the_binary_and_notices_of_the_release_the_manifest_names(self):
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assert_pair("0.12.3")
        self.assertTrue(os.access(self.install_dir / "fts_helper", os.X_OK))
        for folder in self.messaging:
            host = json.loads((folder / "tabmail_fts.json").read_text())
            self.assertEqual(host["path"], str(self.install_dir / "fts_helper"), folder)
            self.assertEqual(host["allowed_extensions"], ["thunderbird@tabmail.ai"], folder)

    def test_installs_with_wget_when_there_is_no_curl(self):
        (self.shims / "curl").unlink()
        executable(self.shims / "wget", FAKE_WGET)
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assert_pair("0.12.3")
        self.publish("0.12.4", binary="#!/bin/sh\necho 0.12.4\n")  # its notices are missing
        self.manifest('"0.12.4"')
        self.assertNotEqual(self.install().returncode, 0)
        self.assert_pair("0.12.3")

    def test_a_failed_download_leaves_an_earlier_install_as_it_was(self):
        self.assertEqual(self.install().returncode, 0)
        for case, binary, notices in (("notices missing", "#!/bin/sh\necho 0.12.4\n", None),
                                      ("binary missing", None, "notices of 0.12.4\n")):
            with self.subTest(case):
                for name in ("fts_helper-0.12.4", "fts_helper-0.12.4-THIRD_PARTY_NOTICES.txt"):
                    (self.release / name).unlink(missing_ok=True)
                self.publish("0.12.4", binary=binary, notices=notices)
                self.manifest('"0.12.4"')
                self.assertNotEqual(self.install().returncode, 0)
                self.assert_pair("0.12.3")
        with self.subTest("manifest missing"):
            (self.release / "update-manifest.json").unlink()
            self.assertNotEqual(self.install().returncode, 0)
            self.assert_pair("0.12.3")

    def test_a_binary_that_cannot_be_replaced_leaves_the_earlier_pair(self):
        self.assertEqual(self.install().returncode, 0)
        self.publish("0.12.4", binary="#!/bin/sh\necho 0.12.4\n", notices="notices of 0.12.4\n")
        self.manifest('"0.12.4"')
        executable(self.shims / "mv", REFUSING_MV)
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Operation not permitted", result.stderr)
        self.assert_pair("0.12.3")

    def test_reads_latest_version_however_the_manifest_is_laid_out(self):
        for case, text, version in (
                ("compact", '{"latest":{"version":"0.12.3","sha256":"00"}}', "0.12.3"),
                ("another version first", '{\n  "metadata": {\n    "version": "0.12.4"\n  },\n'
                 '  "latest": {\n    "sha256": "00",\n    "version": "0.12.3"\n  }\n}\n', "0.12.3")):
            with self.subTest(case):
                self.publish("0.12.4", binary="#!/bin/sh\necho 0.12.4\n", notices="notices of 0.12.4\n")
                self.raw_manifest(text)
                result = self.install()
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assert_pair(version)
        with self.subTest("latest names none, another object does"):
            self.raw_manifest('{\n  "latest": {\n    "version": null\n  },\n'
                              '  "previous": {\n    "version": "0.12.4"\n  }\n}\n')
            self.log.unlink(missing_ok=True)
            result = self.install()
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("names no release version", result.stdout + result.stderr)
            self.assertEqual(self.requested(), [f"{CDN}{self.platform}/update-manifest.json"])
            self.assert_pair("0.12.3")

    def test_two_installers_at_once_each_put_their_own_pair_in_place(self):
        # A reads 0.12.3 and is held before its notices download. The manifest then names 0.12.4, and B
        # is held at the same point, with 0.12.4's binary downloaded. A finishes first, then B.
        self.publish("0.12.4", binary="#!/bin/sh\necho 0.12.4\n", notices="notices of 0.12.4\n")
        a = self.held_install(self.tmp / "a")
        self.manifest('"0.12.4"')
        b = self.held_install(self.tmp / "b")
        self.release_held(self.tmp / "a", a)
        self.assertEqual((self.install_dir / "fts_helper").read_text(), "#!/bin/sh\necho 0.12.3\n")
        self.assertEqual((self.install_dir / "fts_helper-THIRD_PARTY_NOTICES.txt").read_text(), "notices of 0.12.3\n")
        self.release_held(self.tmp / "b", b)
        self.assert_pair("0.12.4")

    def test_a_manifest_naming_no_version_installs_nothing(self):
        for version_json in ('""', "null", '"../fts_helper-latest"', '"0.12.3-latest"',
                             '"0.12.3/../fts_helper-latest"', '"v0.12.3"', '"0.12.3.4"', '"x0.12.3"'):
            with self.subTest(version=version_json):
                self.manifest(version_json)
                self.log.unlink(missing_ok=True)
                result = self.install()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("names no release version", result.stdout + result.stderr)
                # The CDN collapses ".." in a path, so a malformed version must reach no URL at all.
                self.assertEqual(self.requested(), [f"{CDN}{self.platform}/update-manifest.json"])
                self.assertEqual(self.installed(), [])


class TestInstallPs1(unittest.TestCase):
    # PowerShell is not on the build machines; this pins the same contract in the script's text,
    # and the script is run by hand on Windows before a change to it merges.
    def test_takes_the_binary_and_notices_of_the_release_the_manifest_names(self):
        text = INSTALL_PS1.read_text(encoding="utf-8")
        live = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
        self.assertIn('$Cdn = "https://cdn.tabmail.ai/releases/windows-x86_64"', live)
        self.assertIn('Invoke-RestMethod -Uri "$Cdn/update-manifest.json"', live)
        self.assertIn("$UpdateManifest.latest.version", live)
        self.assertNotIn("-latest", live)
        binary = re.findall(r'-Uri "\$Cdn/fts_helper-\$Version\.exe" -OutFile \$HelperDownload -UseBasicParsing', live)
        notices = re.findall(r'-Uri "\$Cdn/fts_helper-\$Version-THIRD_PARTY_NOTICES\.txt" -OutFile \$NoticesDownload -UseBasicParsing', live)
        self.assertEqual((len(binary), len(notices)), (1, 1))

    def test_installs_nothing_unless_both_downloads_succeed_and_moves_the_binary_first(self):
        live = "\n".join(line for line in INSTALL_PS1.read_text(encoding="utf-8").splitlines()
                         if not line.lstrip().startswith("#"))
        order = [live.index(step) for step in (
            '$ErrorActionPreference = "Stop"',
            "try {",
            "if ($Version -notmatch '^[0-9]+\\.[0-9]+\\.[0-9]+\\z') {",
            'throw "The update manifest names no release version."',
            "-OutFile $HelperDownload",
            "-OutFile $NoticesDownload",
            # -Force: without it Move-Item refuses an existing file, so every reinstall would fail.
            "Move-Item -Force -LiteralPath $HelperDownload -Destination $HelperPath",
            "Move-Item -Force -LiteralPath $NoticesDownload -Destination $NoticesPath",
            "} catch {",
            "Remove-Item -Force -ErrorAction SilentlyContinue -LiteralPath $HelperDownload, $NoticesDownload",
            "exit 1",
        )]
        self.assertEqual(order, sorted(order))


if __name__ == "__main__":
    unittest.main()
