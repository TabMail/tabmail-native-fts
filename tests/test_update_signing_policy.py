#!/usr/bin/env python3
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts" / "check-update-signing-policy.py"
OLD_KEY = "Uirza74DhxMIoj54D/XkTymObvX/SpZiG1l1g+6BADE="
NEW_KEY = "/OXegyjt64MgpTdxla2NvQvUWHf8F8IJoyPyiax5A7k="


class TestUpdateSigningPolicy(unittest.TestCase):
    def run_checker(self, version, compiled_keys, policy):
        with tempfile.TemporaryDirectory(prefix="tm-signing-policy-") as temp:
            root = Path(temp)
            (root / "scripts").mkdir()
            (root / "src").mkdir()
            (root / "release").mkdir()
            shutil.copy2(CHECKER, root / "scripts" / CHECKER.name)
            (root / "Cargo.toml").write_text(
                f'[package]\nname = "fixture"\nversion = "{version}"\n',
                encoding="utf-8",
            )
            key_lines = "\n".join(f'    "{key}",' for key in compiled_keys)
            (root / "src" / "update_signature.rs").write_text(
                "pub const UPDATE_PUBLIC_KEYS_BASE64: &[&str] = &[\n"
                f"{key_lines}\n"
                "];\n",
                encoding="utf-8",
            )
            (root / "release" / "update-signing-policy.json").write_text(
                json.dumps(policy),
                encoding="utf-8",
            )
            return subprocess.run(
                ["python3", str(root / "scripts" / CHECKER.name)],
                check=False,
                capture_output=True,
                text=True,
            )

    def test_accepts_active_only_successor_release(self):
        result = self.run_checker(
            "0.11.2",
            [NEW_KEY],
            {
                "activeManifestSigningPublicKeyBase64": NEW_KEY,
                "previousTrustedManifestSigningPublicKeysBase64": [],
                "retiredManifestSigningPublicKeys": [
                    {
                        "publicKeyBase64": OLD_KEY,
                        "firstUntrustedVersion": "0.11.2",
                    }
                ],
            },
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_accepts_old_active_plus_pending_key_in_bridge_release(self):
        result = self.run_checker(
            "0.11.1",
            [OLD_KEY, NEW_KEY],
            {
                "activeManifestSigningPublicKeyBase64": OLD_KEY,
                "bridgeReleaseVersion": "0.11.1",
                "pendingManifestSigningPublicKeyBase64": NEW_KEY,
                "previousTrustedManifestSigningPublicKeysBase64": [],
            },
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_accepts_promoted_bridge_keys_regardless_of_array_order(self):
        result = self.run_checker(
            "0.11.1",
            [OLD_KEY, NEW_KEY],
            {
                "activeManifestSigningPublicKeyBase64": NEW_KEY,
                "bridgeReleaseVersion": "0.11.1",
                "previousTrustedManifestSigningPublicKeysBase64": [OLD_KEY],
            },
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_rejects_retired_key_at_first_untrusted_version(self):
        result = self.run_checker(
            "0.11.2",
            [NEW_KEY, OLD_KEY],
            {
                "activeManifestSigningPublicKeyBase64": NEW_KEY,
                "previousTrustedManifestSigningPublicKeysBase64": [OLD_KEY],
                "retiredManifestSigningPublicKeys": [
                    {
                        "publicKeyBase64": OLD_KEY,
                        "firstUntrustedVersion": "0.11.2",
                    }
                ],
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("retired update key is still trusted", result.stderr)

    def test_rejects_transitional_key_in_release_after_bridge(self):
        result = self.run_checker(
            "0.11.2",
            [NEW_KEY, OLD_KEY],
            {
                "activeManifestSigningPublicKeyBase64": NEW_KEY,
                "bridgeReleaseVersion": "0.11.1",
                "previousTrustedManifestSigningPublicKeysBase64": [OLD_KEY],
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("survived beyond bridge release 0.11.1", result.stderr)

    def test_rejects_a_compiled_key_missing_from_policy(self):
        result = self.run_checker(
            "0.11.2",
            [NEW_KEY, OLD_KEY],
            {
                "activeManifestSigningPublicKeyBase64": NEW_KEY,
                "previousTrustedManifestSigningPublicKeysBase64": [],
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not match active + transitional policy keys", result.stderr)


if __name__ == "__main__":
    unittest.main()
