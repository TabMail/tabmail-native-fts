#!/usr/bin/env python3
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""
Integration tests for the Rust native-fts host.

These tests spawn the Rust helper as a native messaging process and verify
the full workflow: hello, init, indexBatch, search, stats, clear.

How to run locally (after installing Rust toolchain):
  cd tabmail-native-fts
  cargo build --release
  TABMAIL_RUST_FTS_HELPER=./target/release/fts_helper python3 tests/run_tests.py rust

The tests will:
  - Spawn the Rust host as a native messaging process (stdin/stdout framed JSON).
  - Run a full workflow and verify results.
"""

import json
import os
import shutil
import struct
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


def _send_message(proc, message):
    encoded = json.dumps(message).encode("utf-8")
    proc.stdin.write(struct.pack("=I", len(encoded)))
    proc.stdin.write(encoded)
    proc.stdin.flush()


def _read_message(proc):
    raw_length = proc.stdout.read(4)
    if not raw_length:
        return None
    message_length = struct.unpack("=I", raw_length)[0]
    message_bytes = proc.stdout.read(message_length)
    return json.loads(message_bytes.decode("utf-8"))


# Unrelated messages so a fixture's search term is rare in the index. In a
# corpus of a few messages that all contain the term, bm25 scores it near zero,
# and with embeddings on the hybrid merge drops the match below MIN_SCORE.
FILLER_COUNT = 20


def _filler_rows(prefix):
    return [
        {
            "msgId": f"{prefix}-filler-{i}",
            "subject": f"Lunch order {i}",
            "from_": f"cafe{i}@example.com",
            "to_": "office@example.com",
            "body": f"Sandwich and soup options for day {i}.",
            "dateMs": 1700000000000 + i * 1000,
            "hasAttachments": False,
        }
        for i in range(FILLER_COUNT)
    ]


class TestRustHelperProcess(unittest.TestCase):
    """Integration tests using the Rust helper process."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="fts_profile_rust_test_")
        self.rust_helper_path = os.environ.get("TABMAIL_RUST_FTS_HELPER")

        if not self.rust_helper_path:
            self.skipTest("TABMAIL_RUST_FTS_HELPER not set (build Rust binary and set env var)")

        self.rust_helper_path = str(Path(self.rust_helper_path).resolve())
        if not Path(self.rust_helper_path).exists():
            self.skipTest(f"Rust helper not found: {self.rust_helper_path}")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def assert_success(self, response, msg=""):
        if response is None:
            self.fail(f"{msg} - No response from helper")
        if "error" in response:
            self.fail(f"{msg} - Got error: {response['error']}")
        self.assertIn("result", response, f"{msg} - No 'result' in response: {response}")

    def test_full_workflow_rust(self):
        proc = subprocess.Popen(
            [self.rust_helper_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            # 1. Hello
            _send_message(proc, {"id": "1", "method": "hello", "params": {"addonVersion": "0.4.4"}})
            response = _read_message(proc)
            self.assertEqual(response["id"], "1")
            self.assert_success(response, "hello")
            self.assertIn("hostVersion", response["result"])

            # 2. Init with profilePath
            _send_message(
                proc,
                {"id": "2", "method": "init", "params": {"profilePath": self.temp_dir}},
            )
            response = _read_message(proc)
            self.assertEqual(response["id"], "2")
            self.assert_success(response, "init")
            self.assertTrue(response["result"]["ok"])

            # 3. Index a batch (use unique msgIds based on timestamp)
            unique_suffix = str(int(time.time() * 1000))
            _send_message(proc, {"id": "3-filler", "method": "indexBatch", "params": {"rows": _filler_rows(f"test-{unique_suffix}")}})
            response = _read_message(proc)
            self.assert_success(response, "filler indexBatch")
            self.assertEqual(response["result"]["count"], FILLER_COUNT)
            _send_message(
                proc,
                {
                    "id": "3",
                    "method": "indexBatch",
                    "params": {
                        "rows": [
                            {
                                "msgId": f"test-msg-1-{unique_suffix}",
                                "subject": "Test Email About Meetings",
                                "from_": "sender@example.com",
                                "to_": "recipient@example.com",
                                "body": "Let us schedule a meeting to discuss the project.",
                                "dateMs": 1700000000000,
                                "hasAttachments": False,
                            },
                            {
                                "msgId": f"test-msg-2-{unique_suffix}",
                                "subject": "Invoice Payment",
                                "from_": "billing@vendor.com",
                                "to_": "accounts@company.com",
                                "body": "Please process the attached invoice.",
                                "dateMs": 1700001000000,
                                "hasAttachments": True,
                            },
                        ]
                    },
                },
            )
            response = _read_message(proc)
            self.assertEqual(response["id"], "3")
            self.assert_success(response, "indexBatch")
            self.assertEqual(response["result"]["count"], 2)

            # 4. Search
            _send_message(proc, {"id": "4", "method": "search", "params": {"q": "meeting", "limit": 10}})
            response = _read_message(proc)
            self.assertEqual(response["id"], "4")
            self.assert_success(response, "search meeting")
            self.assertIsInstance(response["result"], list)
            self.assertGreater(len(response["result"]), 0)
            # Verify result structure
            for key in ["uniqueId", "author", "subject", "dateMs", "hasAttachments", "snippet", "rank"]:
                self.assertIn(key, response["result"][0])

            # 5. Search with from: qualifier
            _send_message(
                proc,
                {"id": "5", "method": "search", "params": {"q": "from:billing@vendor.com", "limit": 10}},
            )
            response = _read_message(proc)
            self.assertEqual(response["id"], "5")
            self.assert_success(response, "search from:email")
            self.assertGreaterEqual(len(response["result"]), 1)
            # First result should be the billing email (FTS exact field match ranks highest)
            self.assertIn("billing@vendor.com", response["result"][0].get("author", ""))

            # 6. Stats
            _send_message(proc, {"id": "6", "method": "stats", "params": {}})
            response = _read_message(proc)
            self.assertEqual(response["id"], "6")
            self.assert_success(response, "stats")
            self.assertEqual(response["result"]["docs"], 2 + FILLER_COUNT)

            # 7. Clear
            _send_message(proc, {"id": "7", "method": "clear", "params": {}})
            response = _read_message(proc)
            self.assertEqual(response["id"], "7")
            self.assert_success(response, "clear")
            self.assertTrue(response["result"]["ok"])

            # 8. Verify cleared
            _send_message(proc, {"id": "8", "method": "stats", "params": {}})
            response = _read_message(proc)
            self.assert_success(response, "stats after clear")
            self.assertEqual(response["result"]["docs"], 0)

        finally:
            try:
                proc.stdin.close()
            except Exception:
                pass
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()


    def test_memory_search_previews_with_embeddings(self):
        """With the embedding model loaded, a memory search (hybrid: keyword and
        vector) returns previews of the length asked, and whole rows without it."""
        proc = subprocess.Popen(
            [self.rust_helper_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            _send_message(proc, {"id": "1", "method": "hello", "params": {"addonVersion": "1.2.1"}})
            self.assert_success(_read_message(proc), "hello")
            _send_message(proc, {"id": "2", "method": "init", "params": {"profilePath": self.temp_dir}})
            self.assert_success(_read_message(proc), "init")

            base = int(time.time() * 1000) - 100_000
            rows = [
                {
                    "memId": f"filler-{i}",
                    "role": "session",
                    "content": f"[USER]: lunch order {i}\n\n[AGENT]: sandwich and soup for day {i}",
                    "sessionId": "persistent_chat",
                    "dateMs": base + i * 1000,
                    "turnIndex": i,
                }
                for i in range(FILLER_COUNT)
            ]
            long = "[USER]: plan the itinerary for our trip\n\n[AGENT]: " + "itinerary details " * 2000
            rows.append({"memId": "trip", "role": "session", "content": long, "sessionId": "persistent_chat", "dateMs": base + 99_000, "turnIndex": 99})
            _send_message(proc, {"id": "3", "method": "memoryIndexBatch", "params": {"rows": rows}})
            self.assert_success(_read_message(proc), "memoryIndexBatch")
            _send_message(proc, {"id": "4", "method": "memoryStats", "params": {}})
            stats = _read_message(proc)
            self.assert_success(stats, "memoryStats")
            # The model is loaded: every row has its vector, so the search below is hybrid.
            self.assertEqual(stats["result"]["vecDocs"], len(rows))

            def search(request_id, extra):
                _send_message(proc, {"id": request_id, "method": "memorySearch", "params": {"q": "itinerary", "ignoreDate": True, "limit": 10, **extra}})
                response = _read_message(proc)
                self.assert_success(response, f"memorySearch {extra}")
                return {r["memId"]: r for r in response["result"]}

            whole = search("5", {})
            cut = search("6", {"previewChars": 400})
            self.assertIn("trip", whole)
            self.assertEqual(set(cut), set(whole))
            self.assertEqual(whole["trip"]["content"], long)
            self.assertEqual(cut["trip"]["content"], long[:400])
            if whole["trip"].get("snippet") is not None:
                self.assertEqual(cut["trip"]["snippet"], whole["trip"]["snippet"][:400])
            for mem_id, row in cut.items():
                others = {k: v for k, v in row.items() if k not in ("content", "snippet")}
                self.assertEqual(others, {k: v for k, v in whole[mem_id].items() if k not in ("content", "snippet")})
        finally:
            try:
                proc.stdin.close()
            except Exception:
                pass
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()

    def test_scoped_search_with_embeddings_finds_only_the_field(self):
        """With the embedding model loaded (hybrid search), a field scopes mail search to its
        column, and memory search reads "from:" as text."""
        proc = subprocess.Popen([self.rust_helper_path], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            _send_message(proc, {"id": "1", "method": "hello", "params": {"addonVersion": "1.9.1"}})
            self.assert_success(_read_message(proc), "hello")
            _send_message(proc, {"id": "2", "method": "init", "params": {"profilePath": self.temp_dir}})
            init = _read_message(proc)
            self.assert_success(init, "init")
            if not init["result"].get("hasEmbeddings"):
                self.skipTest("embedding model not available")

            now = int(time.time() * 1000)
            filler = [dict(r, dateMs=now - (i + 10) * 60_000) for i, r in enumerate(_filler_rows("scope-filler"))]
            a = {"msgId": "scope-a", "folderId": "account:/Scope", "subject": "Quarterly numbers", "from_": "alice@example.com",
                 "to_": "team@example.com", "cc": "carol@example.com", "bcc": "dave@example.com",
                 "body": "The quarterly revenue numbers are attached.", "dateMs": now - 60_000, "hasAttachments": False}
            b = {"msgId": "scope-b", "folderId": "account:/Scope", "subject": "Notes for carol", "from_": "bob@example.com",
                 "to_": "erin@example.com", "cc": "", "bcc": "",
                 "body": "Quarterly numbers and revenue: ask carol@example.com and dave@example.com.", "dateMs": now - 120_000, "hasAttachments": False}
            _send_message(proc, {"id": "3", "method": "indexBatch", "params": {"rows": filler + [a, b]}})
            self.assert_success(_read_message(proc), "indexBatch")

            def found(q):
                _send_message(proc, {"id": q, "method": "search", "params": {"q": q, "limit": 10, "ignoreDate": True}})
                response = _read_message(proc)
                self.assert_success(response, q)
                return sorted(r["uniqueId"] for r in response["result"] if r["uniqueId"] in ("scope-a", "scope-b"))

            self.assertEqual(found("carol@example.com"), ["scope-a", "scope-b"])
            self.assertEqual(found("cc:carol@example.com"), ["scope-a"])
            self.assertEqual(found('cc:"carol@example.com" quarterly numbers'), ["scope-a"])
            self.assertEqual(found("bcc:dave@example.com revenue"), ["scope-a"])
            self.assertEqual(found("from:bob@example.com quarterly"), ["scope-b"])
            self.assertEqual(found("subject:carol"), ["scope-b"])
            self.assertEqual(found("to:erin revenue"), ["scope-b"])

            mem = [{"memId": f"scope-m{i}", "role": "session", "content": f"[USER]: lunch options {i}\n\n[AGENT]: Soup.",
                    "sessionId": "s", "turnIndex": i, "dateMs": now - (i + 10) * 60_000} for i in range(FILLER_COUNT)]
            mem.append({"memId": "scope-mem", "role": "session", "content": "[USER]: notes from: alice about the offsite\n\n[AGENT]: Saved.",
                        "sessionId": "s", "turnIndex": 99, "dateMs": now - 60_000})
            _send_message(proc, {"id": "4", "method": "memoryIndexBatch", "params": {"rows": mem}})
            self.assert_success(_read_message(proc), "memoryIndexBatch")
            _send_message(proc, {"id": "5", "method": "memorySearch", "params": {"q": "from: alice offsite", "ignoreDate": True}})
            response = _read_message(proc)
            self.assert_success(response, "memorySearch")
            self.assertIn("scope-mem", [r.get("memId") for r in response["result"]], response["result"])
        finally:
            try:
                proc.stdin.close()
            except Exception:
                pass
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()

    def test_search_sort_over_the_wire(self):
        """`search` sorts by the `sort` asked for before it takes the limit, and an unknown `sort` is
        answered with an error (the helper keeps answering)."""
        proc = subprocess.Popen([self.rust_helper_path], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            _send_message(proc, {"id": "1", "method": "hello", "params": {"addonVersion": "1.9.1"}})
            self.assert_success(_read_message(proc), "hello")
            _send_message(proc, {"id": "2", "method": "init", "params": {"profilePath": self.temp_dir}})
            self.assert_success(_read_message(proc), "init")
            now = int(time.time() * 1000)
            rows = [{"msgId": f"sort-{i}", "folderId": "account:/Sort", "subject": f"Update {i}", "from_": "alice@example.com",
                     "to_": "team@example.com", "body": f"Status of item {i}.", "dateMs": now - i * 60_000, "hasAttachments": False}
                    for i in range(10)]
            _send_message(proc, {"id": "3", "method": "indexBatch", "params": {"rows": _filler_rows("sort-filler") + rows}})
            self.assert_success(_read_message(proc), "indexBatch")

            def search(request_id, **params):
                _send_message(proc, {"id": request_id, "method": "search", "params": {"q": "from:alice@example.com", "ignoreDate": True, "limit": 3, **params}})
                response = _read_message(proc)
                self.assertEqual(response.get("id"), request_id)
                return response

            for request_id, params, expected in [
                ("default", {}, ["sort-0", "sort-1", "sort-2"]),
                ("relevance", {"sort": "relevance"}, ["sort-0", "sort-1", "sort-2"]),
                ("desc", {"sort": "date_desc"}, ["sort-0", "sort-1", "sort-2"]),
                ("asc", {"sort": "date_asc"}, ["sort-9", "sort-8", "sort-7"]),
            ]:
                response = search(request_id, **params)
                self.assert_success(response, request_id)
                self.assertEqual([r["uniqueId"] for r in response["result"]], expected, request_id)
            for request_id, sort in [("misspelt", "newest"), ("number", 1)]:
                response = search(request_id, sort=sort)
                self.assertIn("error", response, response)
                self.assertNotIn("result", response, response)
            self.assert_success(search("after", sort="date_asc"), "after an error")
        finally:
            try:
                proc.stdin.close()
            except Exception:
                pass
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()

    def _dated_memory_search_case(self, profile, outside_each_side, inside_content):
        """Indexes, with the embedding model loaded, exchanges about the itinerary (the nearest to
        the query) on both sides of a one-day range, exchanges within it (`inside_content`, one at
        each bound), and unrelated filler; returns the in-range rows, the dates, and a search
        function on that helper."""
        os.makedirs(profile, exist_ok=True)
        proc = subprocess.Popen(
            [self.rust_helper_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _send_message(proc, {"id": "1", "method": "hello", "params": {"addonVersion": "1.2.1"}})
        self.assert_success(_read_message(proc), "hello")
        _send_message(proc, {"id": "2", "method": "init", "params": {"profilePath": profile}})
        self.assert_success(_read_message(proc), "init")

        day = 86_400_000
        base = int(time.time() * 1000) - 60 * day
        start, end = base + 30 * day, base + 31 * day
        outside = [
            {"memId": f"{side}-{i}", "role": "session", "content": f"[USER]: trip itinerary {side} {i}\n\n[AGENT]: the itinerary",
             "sessionId": "persistent_chat", "dateMs": at + i * 1000, "turnIndex": i}
            for side, at in (("before", start - 2 * day), ("after", end + day))
            for i in range(outside_each_side)
        ]
        inside_dates = [start, start + 1000, start + 2000, start + 3000, start + 4000, end]
        inside = [
            {"memId": f"new-{i}", "role": "session",
             "content": inside_content.format(i=i),
             "sessionId": "persistent_chat", "dateMs": at, "turnIndex": 100 + i}
            for i, at in enumerate(inside_dates)
        ]
        # Enough unrelated exchanges that the word is rare and a keyword match scores.
        filler = [
            {"memId": f"filler-{i}", "role": "session", "content": f"[USER]: weather report {i}\n\n[AGENT]: sunny on day {i}",
             "sessionId": "persistent_chat", "dateMs": start + 100_000 + i * 1000, "turnIndex": 200 + i}
            for i in range(300)
        ]
        rows = outside + inside + filler
        _send_message(proc, {"id": "3", "method": "memoryIndexBatch", "params": {"rows": rows}})
        self.assert_success(_read_message(proc), "memoryIndexBatch")
        _send_message(proc, {"id": "4", "method": "memoryStats", "params": {}})
        stats = _read_message(proc)
        self.assert_success(stats, "memoryStats")
        # The model is loaded: every row has its vector, so the searches are hybrid.
        self.assertEqual(stats["result"]["vecDocs"], len(rows))

        ids = iter(range(5, 1000))

        def search(params):
            _send_message(proc, {"id": str(next(ids)), "method": "memorySearch", "params": {"q": "itinerary", **params}})
            response = _read_message(proc)
            self.assert_success(response, "memorySearch")
            return response["result"]

        return proc, inside, start, end, search

    def _close(self, proc):
        try:
            proc.stdin.close()
        except Exception:
            pass
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
            proc.wait()
        proc.stdout.close()
        proc.stderr.close()

    def test_memory_search_with_dates_keeps_the_matches_within_them(self):
        """With the embedding model loaded, a memory search with dates returns its
        matches within the dates, both bounds included, however many nearer exchanges
        lie before or after them."""
        # Within them, exchanges that name it among other things: nearer ones outside the dates
        # outscore them.
        proc, inside, start, end, search = self._dated_memory_search_case(
            os.path.join(self.temp_dir, "many"), 20,
            "[USER]: lunch order {i}, soup and sandwich, then the quarterly budget, and the itinerary\n\n[AGENT]: noted the lunch and budget {i}")
        try:
            found = search({"from": start, "to": end, "limit": len(inside)})
            self.assertEqual(sorted(r["memId"] for r in found), sorted(r["memId"] for r in inside))
        finally:
            self._close(proc)

    def test_memory_search_with_dates_scores_the_matches_within_them_as_without(self):
        """With the embedding model loaded, a match within the dates keeps its vector
        score, at either bound: a search with dates ranks each one as a search without
        them does."""
        # Within them, exchanges near enough the query that their vector score counts.
        proc, inside, start, end, search = self._dated_memory_search_case(
            os.path.join(self.temp_dir, "few"), 2,
            "[USER]: the trip itinerary, and lunch order {i}\n\n[AGENT]: the itinerary and lunch {i}")
        try:
            dated = {r["memId"]: r["rank"] for r in search({"from": start, "to": end, "limit": len(inside)})}
            undated = {r["memId"]: r["rank"] for r in search({"limit": 50})}
            self.assertEqual(sorted(dated), sorted(r["memId"] for r in inside))
            for mem_id, rank in dated.items():
                self.assertIn(mem_id, undated)
                self.assertAlmostEqual(rank, undated[mem_id], delta=0.01, msg=mem_id)
            # One bound alone keeps the matches on its side of it, those near the query too.
            for bound, within in (("from", lambda d: d >= start), ("to", lambda d: d <= end)):
                one_sided = search({bound: start if bound == "from" else end, "limit": 50})
                self.assertTrue(one_sided, bound)
                self.assertTrue(all(within(r["dateMs"]) for r in one_sided), (bound, [r["memId"] for r in one_sided]))
        finally:
            self._close(proc)

    def test_searches_at_the_callers_limit_are_hybrid(self):
        """With the embedding model loaded, the searches Thunderbird and TabMail Voice send
        (limit 2,000) and larger ones (4,000) still find a match by meaning alone, with dates or
        without, and with a field in the query: the helper caps the limit so the vector candidates
        it asks for stay within the 4,096 sqlite-vec answers."""
        proc = subprocess.Popen(
            [self.rust_helper_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            _send_message(proc, {"id": "1", "method": "hello", "params": {"addonVersion": "1.2.1"}})
            self.assert_success(_read_message(proc), "hello")
            _send_message(proc, {"id": "2", "method": "init", "params": {"profilePath": self.temp_dir}})
            self.assert_success(_read_message(proc), "init")

            day = 86_400_000
            now = int(time.time() * 1000)
            emails = _filler_rows("callers-limit") + [
                {"msgId": "found", "subject": "Vacation", "from_": "agent@example.com", "to_": "me@example.com",
                 "body": "Here is the vacation itinerary", "dateMs": now, "hasAttachments": False},
                {"msgId": "outside", "subject": "Vacation", "from_": "agent@example.com", "to_": "me@example.com",
                 "body": "Here is the vacation itinerary again", "dateMs": now - 10 * day, "hasAttachments": False},
                {"msgId": "outside-field", "subject": "Holiday", "from_": "agent@example.com", "to_": "me@example.com",
                 "body": "Here is the vacation itinerary", "dateMs": now - 10 * day, "hasAttachments": False},
            ]
            _send_message(proc, {"id": "3", "method": "indexBatch", "params": {"rows": emails}})
            self.assert_success(_read_message(proc), "indexBatch")
            memories = [
                {"memId": "found", "role": "session", "content": "Here is the vacation itinerary",
                 "sessionId": "persistent_chat", "dateMs": now, "turnIndex": 0},
            ]
            _send_message(proc, {"id": "4", "method": "memoryIndexBatch", "params": {"rows": memories}})
            self.assert_success(_read_message(proc), "memoryIndexBatch")
            # The model is loaded: every row has its vector, so the searches are hybrid.
            for request_id, method, count in (("5", "stats", len(emails)), ("6", "memoryStats", len(memories))):
                _send_message(proc, {"id": request_id, "method": method, "params": {}})
                stats = _read_message(proc)
                self.assert_success(stats, method)
                self.assertEqual(stats["result"]["vecDocs"], count)

            ids = iter(range(7, 1000))

            def search(method, params):
                _send_message(proc, {"id": str(next(ids)), "method": method, "params": params})
                response = _read_message(proc)
                self.assert_success(response, f"{method} {params}")
                return response["result"]

            query = "plan the vacation itinerary"
            for limit in (2000, 4000):
                undated = search("search", {"q": query, "limit": limit, "ignoreDate": True})
                self.assertEqual(sorted(r["uniqueId"] for r in undated), ["found", "outside", "outside-field"], limit)
                self.assertTrue(all(r["snippet"] == "" for r in undated), "found by meaning, not by their words")
            dated = search("search", {"q": query, "limit": 2000, "from": now - day, "to": now + day})
            self.assertEqual([r["uniqueId"] for r in dated], ["found"])
            for limit in (10, 4000):
                scoped = search("search", {"q": "subject:vacation plan the itinerary", "limit": limit, "ignoreDate": True})
                self.assertEqual(sorted(r["uniqueId"] for r in scoped), ["found", "outside"], limit)
            for limit in (2000, 4000):
                memory = search("memorySearch", {"q": query, "limit": limit, "ignoreDate": True})
                self.assertEqual([r["memId"] for r in memory], ["found"], limit)
                self.assertEqual(memory[0]["snippet"], "")
        finally:
            self._close(proc)

    def test_rebuild_embeddings_batch(self):
        """Test the batch-based rebuildEmbeddings workflow:
        rebuildEmbeddingsStart → rebuildEmbeddingsBatch loop → verify via stats.
        Also tests that FTS search works during rebuild (between batches).
        """
        proc = subprocess.Popen(
            [self.rust_helper_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            # 1. Hello + Init
            _send_message(proc, {"id": "1", "method": "hello", "params": {"addonVersion": "1.2.1"}})
            response = _read_message(proc)
            self.assert_success(response, "hello")

            _send_message(proc, {"id": "2", "method": "init", "params": {"profilePath": self.temp_dir}})
            response = _read_message(proc)
            self.assert_success(response, "init")

            # 2. Index some emails
            unique_suffix = str(int(time.time() * 1000))
            rebuild_folder_id = f"folder:embedding-rebuild:{unique_suffix}"
            rows = [
                {
                    "msgId": f"rebuild-test-{i}-{unique_suffix}",
                    "folderId": rebuild_folder_id,
                    "subject": f"Test Email {i} About Quarterly Reports",
                    "from_": f"user{i}@example.com",
                    "to_": "team@example.com",
                    "body": f"This is test email number {i} discussing quarterly results and forecasts.",
                    "dateMs": 1700000000000 + i * 1000000,
                    "hasAttachments": False,
                }
                for i in range(5)
            ]
            _send_message(proc, {"id": "3", "method": "indexBatch", "params": {"rows": rows}})
            response = _read_message(proc)
            self.assert_success(response, "indexBatch")
            self.assertEqual(response["result"]["count"], 5)

            expected_membership = sorted(row["msgId"] for row in rows)

            def assert_folder_membership(request_id, checkpoint):
                _send_message(proc, {
                    "id": request_id,
                    "method": "listFolderMembership",
                    "params": {"folderId": rebuild_folder_id, "limit": 10},
                })
                membership_response = _read_message(proc)
                self.assert_success(membership_response, checkpoint)
                self.assertEqual(
                    membership_response["result"]["msgIds"],
                    expected_membership,
                    f"folder membership changed {checkpoint}",
                )

            assert_folder_membership("3-membership", "before embedding rebuild")

            # 3. Index some memory entries
            mem_rows = [
                {
                    "memId": f"mem-rebuild-{i}-{unique_suffix}",
                    "role": "user" if i % 2 == 0 else "assistant",
                    "content": f"Memory entry {i} about project planning and deadlines.",
                    "sessionId": f"session-{unique_suffix}",
                    "dateMs": 1700000000000 + i * 1000000,
                    "turnIndex": i,
                }
                for i in range(3)
            ]
            _send_message(proc, {"id": "4", "method": "memoryIndexBatch", "params": {"rows": mem_rows}})
            response = _read_message(proc)
            self.assert_success(response, "memoryIndexBatch")
            self.assertEqual(response["result"]["count"], 3)

            # 4. Verify stats show vecDocs (indexBatch creates embeddings too)
            _send_message(proc, {"id": "5", "method": "stats", "params": {}})
            response = _read_message(proc)
            self.assert_success(response, "stats before rebuild")
            self.assertEqual(response["result"]["docs"], 5)
            self.assertIn("vecDocs", response["result"])
            initial_vec_docs = response["result"]["vecDocs"]
            # indexBatch should have created embeddings already
            self.assertEqual(initial_vec_docs, 5, "indexBatch should create embeddings")

            # 5. rebuildEmbeddingsStart — clears vec tables
            _send_message(proc, {"id": "10", "method": "rebuildEmbeddingsStart", "params": {}})
            response = _read_message(proc)
            self.assert_success(response, "rebuildEmbeddingsStart")
            self.assertTrue(response["result"]["ok"])
            self.assertEqual(response["result"]["emailTotal"], 5)
            self.assertEqual(response["result"]["memoryTotal"], 3)
            assert_folder_membership("10-membership", "after rebuildEmbeddingsStart")

            # 6. Verify vec tables are now empty (start clears them)
            _send_message(proc, {"id": "11", "method": "stats", "params": {}})
            response = _read_message(proc)
            self.assert_success(response, "stats after start")
            self.assertEqual(response["result"]["docs"], 5, "FTS5 index should be intact")
            self.assertEqual(response["result"]["vecDocs"], 0, "vec should be empty after start")

            # 7. FTS search should still work (keyword search doesn't need embeddings)
            _send_message(proc, {"id": "12", "method": "search", "params": {"q": "quarterly", "limit": 10}})
            response = _read_message(proc)
            self.assert_success(response, "search during rebuild")
            self.assertGreater(len(response["result"]), 0, "FTS search should work during rebuild")

            # 8. Process email embeddings in batches (use small batch size to test loop)
            last_rowid = 0
            total_processed = 0
            total_embedded = 0
            batch_count = 0
            msg_counter = 20
            while True:
                _send_message(proc, {
                    "id": str(msg_counter),
                    "method": "rebuildEmbeddingsBatch",
                    "params": {"target": "email", "lastRowid": last_rowid, "batchSize": 2}
                })
                response = _read_message(proc)
                self.assert_success(response, f"rebuildEmbeddingsBatch email #{batch_count}")
                self.assertTrue(response["result"]["ok"])
                self.assertEqual(response["result"]["target"], "email")

                last_rowid = response["result"]["lastRowid"]
                total_processed += response["result"]["processed"]
                total_embedded += response["result"]["embedded"]
                batch_count += 1
                msg_counter += 1

                if response["result"]["done"]:
                    break

            self.assertEqual(total_processed, 5, "Should process all 5 emails")
            self.assertEqual(total_embedded, 5, "Should embed all 5 emails")
            self.assertGreaterEqual(batch_count, 3, "With batchSize=2, need at least 3 batches for 5 docs")
            assert_folder_membership("email-batches-membership", "after email rebuildEmbeddingsBatch")

            # 9. Process memory embeddings in batches
            last_rowid = 0
            mem_processed = 0
            mem_embedded = 0
            while True:
                _send_message(proc, {
                    "id": str(msg_counter),
                    "method": "rebuildEmbeddingsBatch",
                    "params": {"target": "memory", "lastRowid": last_rowid, "batchSize": 2}
                })
                response = _read_message(proc)
                self.assert_success(response, "rebuildEmbeddingsBatch memory")
                self.assertEqual(response["result"]["target"], "memory")

                last_rowid = response["result"]["lastRowid"]
                mem_processed += response["result"]["processed"]
                mem_embedded += response["result"]["embedded"]
                msg_counter += 1

                if response["result"]["done"]:
                    break

            self.assertEqual(mem_processed, 3, "Should process all 3 memory entries")
            self.assertEqual(mem_embedded, 3, "Should embed all 3 memory entries")
            assert_folder_membership("memory-batches-membership", "after memory rebuildEmbeddingsBatch")

            # 10. Verify final stats
            _send_message(proc, {"id": str(msg_counter), "method": "stats", "params": {}})
            response = _read_message(proc)
            self.assert_success(response, "stats after rebuild")
            self.assertEqual(response["result"]["docs"], 5)
            self.assertEqual(response["result"]["vecDocs"], 5, "All embeddings should be restored")
            msg_counter += 1

            _send_message(proc, {"id": str(msg_counter), "method": "memoryStats", "params": {}})
            response = _read_message(proc)
            self.assert_success(response, "memoryStats after rebuild")
            self.assertEqual(response["result"]["docs"], 3)
            self.assertEqual(response["result"]["vecDocs"], 3, "All memory embeddings should be restored")

        finally:
            try:
                proc.stdin.close()
            except Exception:
                pass
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()


if __name__ == "__main__":
    unittest.main(verbosity=2)
