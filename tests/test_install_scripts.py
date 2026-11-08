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
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INSTALL_SH = ROOT / "scripts" / "install.sh"
INSTALL_PS1 = ROOT / "scripts" / "install.ps1"
CDN = "https://cdn.tabmail.ai/releases/"

# Stands in for curl: serves https://cdn.tabmail.ai/releases/<path> from $FAKE_CDN/<path>, and
# fails (as `curl -f` does on a 404) for a file that is not there.
FAKE_CURL = """#!/bin/sh
url="" out=""
while [ $# -gt 0 ]; do
    case "$1" in
        -o) out="$2"; shift 2 ;;
        -*) shift ;;
        *) url="$1"; shift ;;
    esac
done
path="${url#%s}"
[ "$path" != "$url" ] && [ -f "$FAKE_CDN/$path" ] || { echo "curl: (22) 404 $url" >&2; exit 22; }
cp "$FAKE_CDN/$path" "$out"
""" % CDN

# Stands in for wget: as wget -O does, it empties the output file first and exits 8 on a 404.
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
TOOLS = ("uname", "mkdir", "sed", "head", "chmod", "mv", "rm", "cat", "cp")


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

    def install(self):
        env = {"HOME": str(self.home), "PATH": self.path, "FAKE_CDN": str(self.cdn), "REAL_MV": self.real_mv}
        return subprocess.run(["/bin/bash", str(INSTALL_SH)], env=env, capture_output=True, text=True)

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

    def test_a_manifest_naming_no_version_installs_nothing(self):
        for version_json in ('""', "null", '"../fts_helper-latest"', '"0.12.3-latest"',
                             '"0.12.3/../fts_helper-latest"', '"v0.12.3"', '"0.12.3.4"', '"x0.12.3"'):
            with self.subTest(version=version_json):
                self.manifest(version_json)
                result = self.install()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("names no release version", result.stdout + result.stderr)
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
        binary = re.findall(r'-Uri "\$Cdn/fts_helper-\$Version\.exe" -OutFile \$HelperDownload', live)
        notices = re.findall(r'-Uri "\$Cdn/fts_helper-\$Version-THIRD_PARTY_NOTICES\.txt" -OutFile \$NoticesDownload', live)
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
            "-LiteralPath $HelperDownload -Destination $HelperPath",
            "-LiteralPath $NoticesDownload -Destination $NoticesPath",
            "} catch {",
            "Remove-Item -Force -ErrorAction SilentlyContinue -LiteralPath $HelperDownload, $NoticesDownload",
            "exit 1",
        )]
        self.assertEqual(order, sorted(order))


if __name__ == "__main__":
    unittest.main()
