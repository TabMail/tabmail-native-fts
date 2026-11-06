#!/usr/bin/env python3
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""THIRD_PARTY_NOTICES.txt credits every crate the helper is built from, with real license texts."""

import importlib.util
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts" / "check-third-party-notices.py"
NOTICES = ROOT / "THIRD_PARTY_NOTICES.txt"

spec = importlib.util.spec_from_file_location("check_third_party_notices", CHECKER)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class TestThirdPartyNotices(unittest.TestCase):
    def test_the_repository_notices_credit_every_shipped_crate(self):
        result = subprocess.run([sys.executable, "-I", str(CHECKER), str(ROOT)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_every_release_target_is_covered(self):
        # Thunderbird ships macOS universal, Windows x64 and Linux x64; Voice adds the arm64 builds.
        self.assertEqual(set(checker.targets(ROOT)), {
            "aarch64-apple-darwin", "x86_64-apple-darwin",
            "x86_64-pc-windows-msvc", "aarch64-pc-windows-msvc",
            "x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu",
        })

    def test_a_crate_missing_from_the_notices_is_reported(self):
        notices = "- serde 1.0.217 (https://github.com/serde-rs/serde)\n"
        found = checker.problems(notices, {"serde 1.0.217", "ring 0.17.14"})
        self.assertEqual(len(found), 1)
        self.assertIn("ring 0.17.14", found[0])
        self.assertNotIn("serde", found[0])

    def test_a_crate_credited_at_another_version_is_reported(self):
        found = checker.problems("- ring 0.17.13 (https://github.com/briansmith/ring)\n",
                                 {"ring 0.17.14"})
        self.assertEqual(len(found), 1)
        self.assertIn("ring 0.17.14", found[0])

    def test_an_unfilled_license_template_is_reported(self):
        for placeholder in ("Copyright (c) <year> <copyright holders>", "Copyright (c) <year> <owner>."):
            found = checker.problems(f"- ring 0.17.14\n\n{placeholder}\n", {"ring 0.17.14"})
            self.assertEqual(len(found), 1, placeholder)
            self.assertIn("unfilled license template", found[0])

    def test_the_apache_appendix_is_not_mistaken_for_a_template(self):
        notices = "- tokenizers 0.21.4\n\n   Copyright [yyyy] [name of copyright owner]\n"
        self.assertEqual(checker.problems(notices, {"tokenizers 0.21.4"}), [])

    def test_the_notices_carry_the_components_outside_the_crate_list(self):
        text = NOTICES.read_text(encoding="utf-8")
        for needle in (
            "https://github.com/TabMail/tabmail-native-fts",  # MPL-2.0 source offer
            "Copyright (c) The Rust Project Contributors",  # the Rust standard library
            "all-MiniLM-L6-v2",  # the embedding model
            "Copyright (c) Microsoft Corporation",  # its MIT base model
            "Copyright (c) 2002-2021  K.Kosako",  # Oniguruma, compiled in by onig_sys
            "The author disclaims copyright to this source code.",  # SQLite
            "Copyright (c) 2024 Alex Garcia",  # sqlite-vec
            "Community Data License Agreement - Permissive - Version 2.0",  # webpki-roots
        ):
            self.assertIn(needle, text)

    def test_the_notices_are_plain_text(self):
        text = NOTICES.read_text(encoding="utf-8")
        for escaped in ("&quot;", "&amp;", "&lt;", "&gt;", "&#x"):
            self.assertNotIn(escaped, text)


if __name__ == "__main__":
    unittest.main()
