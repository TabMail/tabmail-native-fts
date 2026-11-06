#!/usr/bin/env python3
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""THIRD_PARTY_NOTICES.txt credits every crate the helper is built from, with real license texts."""

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts" / "check-third-party-notices.py"
GENERATOR = ROOT / "scripts" / "generate-third-party-notices.sh"
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

    def test_a_license_without_its_text_is_reported(self):
        notices = sample_notices().replace(MIT_TEXT, "")
        found = checker.problems(notices, {"serde 1.0.217"})
        self.assertEqual(len(found), 1)
        self.assertIn("no text: MIT License", found[0])
        self.assertEqual(checker.problems(sample_notices(), {"serde 1.0.217"}), [])


MIT_TEXT = "Copyright (c) 2014 The Serde Authors\n\nPermission is hereby granted, free of charge, ...\n"


def sample_notices(*crates):
    used_by = "".join(f"- {c} (https://example.com/{c.split()[0]})\n" for c in crates or ("serde 1.0.217",))
    return ("THIRD-PARTY SOFTWARE NOTICES\n" + "=" * 80 + "\n\nPART 1\n\nPART 2. RUST CRATES\n\n"
            + "=" * 80 + "\nMIT License\nUsed by:\n" + used_by + "-" * 80 + "\n\n" + MIT_TEXT)


FAKE_CARGO = """#!{python}
# A stand-in for cargo: `tree` and `about` answer from files named in the environment, and every
# call is logged.
import json, os, shutil, sys
args = sys.argv[1:]
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps(args) + "\\n")
if args[:1] == ["tree"]:
    sys.stderr.write(os.environ.get("FAKE_TREE_STDERR", ""))
    sys.stdout.write(open(os.environ["FAKE_TREE_OUT"]).read())
    sys.exit(int(os.environ.get("FAKE_TREE_RC", "0")))
if args[:2] == ["about", "--version"]:
    print("cargo-about " + os.environ["FAKE_ABOUT_VERSION"])
    sys.exit(0)
if args[:2] == ["about", "generate"]:
    shutil.copyfile(os.environ["FAKE_ABOUT_OUT"], args[args.index("-o") + 1])
    sys.exit(0)
sys.exit(101)
"""


class TestNoticeScripts(unittest.TestCase):
    """The checker and the generator, run as the release flows run them, against a stand-in cargo."""

    TARGETS = ("x86_64-unknown-linux-gnu", "aarch64-apple-darwin")

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        self.root = self.tmp / "repo"
        (self.root / "scripts").mkdir(parents=True)
        shutil.copy(CHECKER, self.root / "scripts")
        shutil.copy(GENERATOR, self.root / "scripts")
        (self.root / "about.toml").write_text(
            "accepted = [\"MIT\"]\ntargets = [\n" + "".join(f'    "{t}",\n' for t in self.TARGETS) + "]\n")
        bin_dir = self.tmp / "bin"
        bin_dir.mkdir()
        cargo = bin_dir / "cargo"
        cargo.write_text(FAKE_CARGO.replace("{python}", sys.executable))
        cargo.chmod(0o755)
        self.log = self.tmp / "cargo.log"
        self.tree = self.tmp / "tree.txt"
        self.tree.write_text("tabmail-native-fts v0.12.2 (/repo)\nserde v1.0.217\nserde v1.0.217 (*)\n")
        self.generated = self.tmp / "generated.txt"
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "HOME": str(self.tmp),
            "CARGO": str(cargo),
            "FAKE_LOG": str(self.log),
            "FAKE_TREE_OUT": str(self.tree),
            "FAKE_ABOUT_VERSION": "0.9.3",
            "FAKE_ABOUT_OUT": str(self.generated),
        }

    def run_checker(self, **env):
        return subprocess.run([sys.executable, "-I", str(self.root / "scripts" / CHECKER.name), str(self.root)],
                              capture_output=True, text=True, env={**self.env, **env})

    def run_generator(self, **env):
        return subprocess.run(["bash", str(self.root / "scripts" / GENERATOR.name)],
                              capture_output=True, text=True, env={**self.env, **env})

    def cargo_calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def write_notices(self, *crates):
        (self.root / "THIRD_PARTY_NOTICES.txt").write_text(sample_notices(*crates))

    def test_complete_notices_pass_and_every_target_is_queried(self):
        self.write_notices("tabmail-native-fts 0.12.2", "serde 1.0.217")
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("credits every shipped crate", result.stdout)
        [call] = self.cargo_calls()
        self.assertEqual(call[0], "tree")
        self.assertIn("--locked", call)
        self.assertEqual([call[i + 1] for i, a in enumerate(call) if a == "--target"], list(self.TARGETS))
        # A release builder caches only its own platform's crates; the check must be free to fetch
        # the rest (an offline query fails on a fresh cache).
        self.assertNotIn("--offline", call)

    def test_a_shipped_crate_missing_from_the_notices_fails(self):
        self.write_notices("tabmail-native-fts 0.12.2")
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        self.assertIn("does not credit: serde 1.0.217", result.stderr)

    def test_a_missing_notices_file_fails_before_cargo_runs(self):
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        self.assertIn("THIRD_PARTY_NOTICES.txt is missing", result.stderr)
        self.assertEqual(self.cargo_calls(), [])

    def test_a_cargo_failure_fails(self):
        self.write_notices("tabmail-native-fts 0.12.2", "serde 1.0.217")
        result = self.run_checker(FAKE_TREE_RC="101", FAKE_TREE_STDERR="error: failed to download")
        self.assertEqual(result.returncode, 1)
        self.assertIn("cargo tree failed", result.stderr)
        self.assertIn("failed to download", result.stderr)

    def test_an_empty_dependency_graph_fails(self):
        self.write_notices("tabmail-native-fts 0.12.2", "serde 1.0.217")
        self.tree.write_text("")
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        self.assertIn("cargo tree listed no crates", result.stderr)

    def test_an_about_toml_without_targets_fails_before_cargo_runs(self):
        self.write_notices("tabmail-native-fts 0.12.2", "serde 1.0.217")
        (self.root / "about.toml").write_text("accepted = [\"MIT\"]\n")
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        self.assertIn("names no release targets", result.stderr)
        self.assertEqual(self.cargo_calls(), [])

    def test_the_generator_writes_the_file_and_checks_it(self):
        self.generated.write_text(sample_notices("tabmail-native-fts 0.12.2", "serde 1.0.217"))
        result = self.run_generator()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.root / "THIRD_PARTY_NOTICES.txt").read_text(), self.generated.read_text())
        self.assertEqual([c[:2] for c in self.cargo_calls()], [["about", "--version"], ["about", "generate"], ["tree", "--edges"]])

    def test_the_generator_fails_when_its_output_misses_a_crate(self):
        self.generated.write_text(sample_notices("tabmail-native-fts 0.12.2"))
        result = self.run_generator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not credit: serde 1.0.217", result.stderr)

    def test_the_generator_refuses_another_cargo_about_version(self):
        self.write_notices("tabmail-native-fts 0.12.2", "serde 1.0.217")
        before = (self.root / "THIRD_PARTY_NOTICES.txt").read_text()
        self.generated.write_text("written by the wrong cargo-about")
        result = self.run_generator(FAKE_ABOUT_VERSION="0.9.4")
        self.assertEqual(result.returncode, 1)
        self.assertIn("needs cargo-about 0.9.3 (found: cargo-about 0.9.4)", result.stderr)
        self.assertEqual((self.root / "THIRD_PARTY_NOTICES.txt").read_text(), before)
        self.assertEqual([c[:2] for c in self.cargo_calls()], [["about", "--version"]])


if __name__ == "__main__":
    unittest.main()
