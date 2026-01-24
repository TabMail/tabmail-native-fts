#!/usr/bin/env python3
"""
Integration test for Rust host self-update route (updateRequest).

This validates:
- The host can download an update over HTTP
- SHA256 verification works
- Ed25519 signature verification works
- The host exits after a successful update so Thunderbird can reconnect (TB-as-brain model)

This uses a local HTTP server and signs the update metadata using OpenSSL + the local
private key PEM referenced by TM_UPDATE_PRIVATE_KEY_PEM_PATH in tabmail-native-fts/.dev.vars.
"""

import base64
import hashlib
import json
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
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


def _platform_key_macos():
    # Keep consistent with addon mapping (macos + arm64/x86_64)
    import platform

    arch = platform.machine()
    if arch == "arm64":
        return "macos-arm64"
    return "macos-x86_64"


def _signed_message(version: str, platform_key: str, sha256_hex: str, url: str) -> str:
    return f"tabmail-native-fts|host_version={version}|platform={platform_key}|sha256={sha256_hex}|url={url}"


def _sign_ed25519_base64(private_key_pem: str, message: str) -> str:
    # Use openssl pkeyutl -sign -rawin -in <file>
    with tempfile.NamedTemporaryFile(prefix="tm_sig_msg_", delete=True) as f_msg:
        f_msg.write(message.encode("utf-8"))
        f_msg.flush()
        with tempfile.NamedTemporaryFile(prefix="tm_sig_out_", delete=False) as f_out:
            out_path = f_out.name

        try:
            subprocess.run(
                ["openssl", "pkeyutl", "-sign", "-inkey", private_key_pem, "-rawin", "-in", f_msg.name, "-out", out_path],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            sig = Path(out_path).read_bytes()
            return base64.b64encode(sig).decode("utf-8")
        finally:
            try:
                os.unlink(out_path)
            except OSError:
                pass


class TestRustUpdateRequest(unittest.TestCase):
    def setUp(self):
        self.rust_helper_path = os.environ.get("TABMAIL_RUST_FTS_HELPER")
        if not self.rust_helper_path:
            self.skipTest("TABMAIL_RUST_FTS_HELPER not set (build Rust binary and set env var)")
        self.rust_helper_path = str(Path(self.rust_helper_path).resolve())
        if not Path(self.rust_helper_path).exists():
            self.skipTest(f"Rust helper not found: {self.rust_helper_path}")

        self.private_key_pem = os.environ.get("TM_UPDATE_PRIVATE_KEY_PEM_PATH")
        if not self.private_key_pem or not Path(self.private_key_pem).exists():
            self.skipTest("TM_UPDATE_PRIVATE_KEY_PEM_PATH not set or missing (needed to sign update metadata)")

        self.work_dir = tempfile.mkdtemp(prefix="tm_update_req_")
        self.bin_dir = Path(self.work_dir) / "bin"
        self.bin_dir.mkdir(parents=True, exist_ok=True)

        # Copy helper into a writable location so it can self-update (overwrite itself).
        self.local_helper = self.bin_dir / "fts_helper"
        shutil.copy2(self.rust_helper_path, self.local_helper)
        os.chmod(self.local_helper, 0o755)

        # We'll serve the "new" binary as the same file bytes for this test.
        self.download_file = self.bin_dir / "fts_helper-download"
        shutil.copy2(self.local_helper, self.download_file)
        os.chmod(self.download_file, 0o755)

        self.httpd = None
        self.http_thread = None
        self.base_url = None

    def tearDown(self):
        if self.httpd:
            self.httpd.shutdown()
        if self.http_thread:
            self.http_thread.join(timeout=2)
        shutil.rmtree(self.work_dir, ignore_errors=True)

    def _start_server(self):
        # Serve files from bin_dir
        os.chdir(self.bin_dir)

        class Handler(SimpleHTTPRequestHandler):
            def log_message(self, format, *args):
                # Keep test output quiet
                return

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        host, port = self.httpd.server_address
        self.base_url = f"http://{host}:{port}"
        self.http_thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.http_thread.start()

    def test_update_request_happy_path(self):
        self._start_server()

        url = f"{self.base_url}/{self.download_file.name}"
        sha256_hex = hashlib.sha256(self.download_file.read_bytes()).hexdigest()
        platform_key = _platform_key_macos()

        # targetVersion higher than current; binary bytes can be identical for this integration test.
        target_version = "0.6.999"
        msg = _signed_message(target_version, platform_key, sha256_hex, url)
        sig_b64 = _sign_ed25519_base64(self.private_key_pem, msg)

        proc = subprocess.Popen(
            [str(self.local_helper)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            _send_message(proc, {"id": "1", "method": "hello", "params": {"addonVersion": "test"}})
            hello = _read_message(proc)
            self.assertIsNotNone(hello)
            self.assertNotIn("error", hello)

            _send_message(
                proc,
                {
                    "id": "2",
                    "method": "updateRequest",
                    "params": {
                        "targetVersion": target_version,
                        "updateUrl": url,
                        "sha256": sha256_hex,
                        "platform": platform_key,
                        "signature": sig_b64,
                    },
                },
            )
            resp = _read_message(proc)
            self.assertIsNotNone(resp)
            self.assertNotIn("error", resp)
            self.assertTrue(resp["result"]["success"])

            # Host should exit shortly after responding (TB reconnect model)
            proc.stdin.close()
            proc.wait(timeout=5)
            self.assertEqual(proc.returncode, 0)

        finally:
            try:
                proc.kill()
            except Exception:
                pass

    def test_signature_rejects_wrong_platform_key(self):
        """
        Platform-specific deployment relies on including the platform key in the signed message.
        If the platform key differs, signature verification must fail.
        """
        self._start_server()

        url = f"{self.base_url}/{self.download_file.name}"
        sha256_hex = hashlib.sha256(self.download_file.read_bytes()).hexdigest()
        platform_key = _platform_key_macos()

        # Sign for a DIFFERENT platform than what we send to updateRequest.
        signed_for_platform = "windows-x86_64"
        target_version = "0.6.999"
        msg = _signed_message(target_version, signed_for_platform, sha256_hex, url)
        sig_b64 = _sign_ed25519_base64(self.private_key_pem, msg)

        proc = subprocess.Popen(
            [str(self.local_helper)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            _send_message(proc, {"id": "1", "method": "hello", "params": {"addonVersion": "test"}})
            hello = _read_message(proc)
            self.assertIsNotNone(hello)
            self.assertNotIn("error", hello)

            _send_message(
                proc,
                {
                    "id": "2",
                    "method": "updateRequest",
                    "params": {
                        "targetVersion": target_version,
                        "updateUrl": url,
                        "sha256": sha256_hex,
                        # Send the REAL platform key, but the signature was created for a different one.
                        "platform": platform_key,
                        "signature": sig_b64,
                    },
                },
            )
            resp = _read_message(proc)
            self.assertIsNotNone(resp)
            self.assertIn("error", resp)
        finally:
            try:
                proc.kill()
            except Exception:
                pass


