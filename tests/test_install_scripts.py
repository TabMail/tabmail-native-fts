#!/usr/bin/env python3
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""The install scripts install the binary and the third-party notices of one release: the one the
update manifest names. The "-latest" binary and notices are replaced one at a time, so two "-latest"
downloads can pair a new binary with old notices."""

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


@unittest.skipIf(sys.platform not in ("darwin", "linux"), "install.sh runs on macOS and Linux")
class TestInstallSh(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="tabmail-install-sh-"))
        self.addCleanup(shutil.rmtree, self.tmp)
        self.home = self.tmp / "home"
        self.home.mkdir()
        bin_dir = self.tmp / "bin"
        bin_dir.mkdir()
        curl = bin_dir / "curl"
        curl.write_text(FAKE_CURL)
        curl.chmod(curl.stat().st_mode | stat.S_IXUSR)
        self.path = f"{bin_dir}:/usr/bin:/bin"
        if sys.platform == "darwin":
            self.platform = "macos-universal"
            self.install_dir = self.home / "Library" / "Application Support" / "TabMail" / "native"
        else:
            self.platform = "linux-x86_64"
            self.install_dir = self.home / ".local" / "share" / "tabmail" / "native"
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
        env = {"HOME": str(self.home), "PATH": self.path, "FAKE_CDN": str(self.cdn)}
        return subprocess.run(["bash", str(INSTALL_SH)], env=env, capture_output=True, text=True)

    def installed(self):
        return sorted(p.name for p in self.install_dir.iterdir())

    def test_installs_the_binary_and_notices_of_the_release_the_manifest_names(self):
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((self.install_dir / "fts_helper").read_text(), "#!/bin/sh\necho 0.12.3\n")
        self.assertEqual((self.install_dir / "fts_helper-THIRD_PARTY_NOTICES.txt").read_text(), "notices of 0.12.3\n")
        self.assertTrue(os.access(self.install_dir / "fts_helper", os.X_OK))
        self.assertEqual(self.installed(), ["fts_helper", "fts_helper-THIRD_PARTY_NOTICES.txt"], "no download left behind")

    def test_a_failed_download_leaves_an_earlier_install_as_it_was(self):
        self.assertEqual(self.install().returncode, 0)
        self.publish("0.12.4", binary="#!/bin/sh\necho 0.12.4\n")  # its notices are missing
        self.manifest('"0.12.4"')
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual((self.install_dir / "fts_helper").read_text(), "#!/bin/sh\necho 0.12.3\n")
        self.assertEqual((self.install_dir / "fts_helper-THIRD_PARTY_NOTICES.txt").read_text(), "notices of 0.12.3\n")
        self.assertEqual(self.installed(), ["fts_helper", "fts_helper-THIRD_PARTY_NOTICES.txt"], "no download left behind")

    def test_a_manifest_naming_no_version_installs_nothing(self):
        for version_json in ('""', '"../fts_helper-latest"', "null"):
            with self.subTest(version=version_json):
                self.manifest(version_json)
                result = self.install()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("names no release version", result.stdout + result.stderr)
                self.assertEqual(self.installed(), [])


class TestInstallPs1(unittest.TestCase):
    # PowerShell is not on the build machines; this pins the same contract in the script's text.
    def test_takes_the_binary_and_notices_of_the_release_the_manifest_names(self):
        text = INSTALL_PS1.read_text(encoding="utf-8")
        live = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
        self.assertIn('Invoke-RestMethod -Uri "$Cdn/update-manifest.json"', live)
        self.assertIn("$UpdateManifest.latest.version", live)
        self.assertNotIn("latest.exe", live)
        self.assertNotIn("-latest", live)
        binary = re.findall(r'-Uri "\$Cdn/fts_helper-\$Version\.exe" -OutFile \$HelperDownload', live)
        notices = re.findall(r'-Uri "\$Cdn/fts_helper-\$Version-THIRD_PARTY_NOTICES\.txt" -OutFile \$NoticesDownload', live)
        self.assertEqual((len(binary), len(notices)), (1, 1))
        # Both downloads finish before either replaces the installed file.
        self.assertLess(live.index("-OutFile $NoticesDownload"), live.index("-Destination $NoticesPath"))
        self.assertLess(live.index("-OutFile $HelperDownload"), live.index("-Destination $NoticesPath"))


if __name__ == "__main__":
    unittest.main()
