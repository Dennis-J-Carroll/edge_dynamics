#!/usr/bin/env python3
"""
HTTP proxy that batches JSON responses, normalizes them, compresses with zstd, and ships
batches to a downstream collector. It logs each flush event to a CSV file so that
compression metrics can be computed after the run. The dictionary path for zstd
can be specified via the DICT_PATH environment variable; otherwise it defaults to
`dict_local` in the working directory.

Usage:
    python3 proxy.py

The proxy listens on 127.0.0.1:8080 and expects a collector listening on
COLLECTOR_HOST:COLLECTOR_PORT (default 127.0.0.1:7000). It batches up to
BATCH_N messages or flushes every BATCH_MS milliseconds, whichever comes first.

Each time a batch is flushed, a line is appended to `metrics.csv` in the working
directory with the format `flush_msgs,comp_bytes`. The collector is assumed to
decompress the batches using the same dictionary and write the reconstructed
JSON lines to a file (e.g. recv.jsonl). See run_demo.sh for an example driver.
"""

import os
import socket
import socketserver
import http.client
import json
import time
import threading
import subprocess  # retained for potential future use
import zlib
from urllib.parse import urlsplit


# Configuration
# Dictionary path: can be overridden via environment variable DICT_PATH.
DICT_PATH = os.environ.get("DICT_PATH", "dict_local")
# Collector address: (host, port). It can be overridden via environment variables
# COLLECTOR_HOST and COLLECTOR_PORT.
COLLECTOR_HOST = os.environ.get("COLLECTOR_HOST", "127.0.0.1")
COLLECTOR_PORT = int(os.environ.get("COLLECTOR_PORT", "7000"))
COLLECTOR = (COLLECTOR_HOST, COLLECTOR_PORT)
# Batch parameters
BATCH_N = int(os.environ.get("BATCH_N", "100"))  # flush after this many messages
BATCH_MS = int(os.environ.get("BATCH_MS", "2000"))  # or after this many milliseconds


# Internal state
_batch = []  # list of normalized JSON strings awaiting compression
_last_flush = time.time()
_lock = threading.Lock()

# Load compression dictionary once at module import
try:
    with open(DICT_PATH, "rb") as df:
        _compression_dict = df.read()
except FileNotFoundError:
    _compression_dict = b""
    # Will be loaded at runtime if file appears later


def _flush(force: bool = False) -> None:
    """Flush pending messages if conditions are met.

    When flushing, compresses the batch using zstd with the configured
    dictionary and sends it to the collector. Also logs the flush metrics to
    `metrics.csv`.

    Args:
        force: If True, forces a flush regardless of batch size/time.
    """
    global _batch, _last_flush
    # Acquire the batch under lock to check conditions and copy messages.
    with _lock:
        if not _batch:
            return
        now = time.time()
        should_flush = force or len(_batch) >= BATCH_N or (now - _last_flush) * 1000 >= BATCH_MS
        if not should_flush:
            return
        # Make a copy of the batch and clear the original to allow new messages.
        msgs_to_flush = _batch.copy()
        _batch.clear()
        _last_flush = now

    # Prepare payload: join messages with newline and include trailing newline.
    payload = ("\n".join(msgs_to_flush) + "\n").encode("utf-8")
    # Compress using zlib with dictionary. We use raw zlib (no headers) via negative wbits.
    # Ensure the dictionary is loaded (support hot reload if file appears later)
    global _compression_dict
    if not _compression_dict and os.path.exists(DICT_PATH):
        with open(DICT_PATH, "rb") as df:
            _compression_dict = df.read()
    try:
        if _compression_dict:
            compressor = zlib.compressobj(level=9, wbits=-zlib.MAX_WBITS, zdict=_compression_dict)
        else:
            compressor = zlib.compressobj(level=9, wbits=-zlib.MAX_WBITS)
        comp_bytes = compressor.compress(payload) + compressor.flush()
    except Exception as e:
        # If compression fails, drop the batch silently and log error.
        print(f"zlib compression failed: {e}")
        return

    # Send to collector
    try:
        with socket.create_connection(COLLECTOR, timeout=2) as sock:
            sock.sendall(comp_bytes)
    except Exception as e:
        # Collector unavailable; we drop the compressed batch (lost data).
        print(f"collector unavailable ({e}); dropped batch of {len(msgs_to_flush)} msgs")
        return

    # Log metrics: write flush_msgs,comp_bytes_length to metrics.csv
    try:
        with open("metrics.csv", "a", encoding="utf-8") as mfile:
            mfile.write(f"{len(msgs_to_flush)},{len(comp_bytes)}\n")
    except Exception as e:
        print(f"failed to write metrics.csv: {e}")
        # Continue even if metrics logging fails

    # Print flush summary to stdout for interactive use
    print(f"flushed {len(msgs_to_flush)} msgs -> {len(comp_bytes)} bytes")


class TinyProxy(socketserver.StreamRequestHandler):
    """Minimal HTTP proxy handler.

    This handler accepts HTTP requests over an HTTP CONNECT-like proxy interface.
    It forwards them to the target host, sends the response back to the client,
    and captures the JSON body for batching/compression if applicable.
    """

    def handle(self) -> None:
        # Read request header until empty line (\r\n\r\n)
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = self.rfile.readline()
            if not chunk:
                break
            data += chunk
            # Protect against excessively long headers
            if len(data) > 64 * 1024:
                break
        if not data:
            return

        # Parse request line
        try:
            first_line = data.decode("iso-8859-1").split("\r\n", 1)[0]
            method, target, _ = first_line.split(" ", 2)
        except Exception:
            # Malformed request
            self.wfile.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length:0\r\n\r\n")
            return
        # Only support absolute-form requests (HTTP proxy semantics)
        if not target.startswith("http://"):
            self.wfile.write(b"HTTP/1.1 501 Not Implemented\r\nContent-Length:0\r\n\r\n")
            return
        # Split URL
        url = urlsplit(target)
        host = url.netloc
        path = url.path or "/"
        if url.query:
            path += "?" + url.query

        # Parse headers; drop Proxy-Connection and normalize Connection header
        headers = {}
        for line in data.decode("iso-8859-1").split("\r\n")[1:]:
            if not line:
                continue
            if ":" not in line:
                continue
            name, value = line.split(":", 1)
            name_lower = name.strip().lower()
            if name_lower == "proxy-connection":
                name_lower = "connection"
            headers[name_lower.title()] = value.strip()

        # Forward request to upstream (only HTTP, no HTTPS)
        try:
            conn = http.client.HTTPConnection(host, timeout=5)
            # Use headers as-is; body is None for GET requests
            conn.request(method, path, headers=headers)
            resp = conn.getresponse()
            body = resp.read()
            resp_headers = resp.getheaders()
        except Exception:
            # Upstream unavailable
            try:
                self.wfile.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length:0\r\n\r\n")
            except Exception:
                pass
            return
        finally:
            try:
                conn.close()
            except Exception:
                pass

        # Send response status and headers to client
        try:
            status_line = f"HTTP/1.1 {resp.status} {resp.reason}\r\n".encode("iso-8859-1")
            self.wfile.write(status_line)
            # Write response headers, excluding Content-Length; we'll set it ourselves
            for k, v in resp_headers:
                if k.lower() == "content-length":
                    continue
                header_line = f"{k}: {v}\r\n".encode("iso-8859-1")
                self.wfile.write(header_line)
            # Write our Content-Length header
            self.wfile.write(f"Content-Length: {len(body)}\r\n\r\n".encode("iso-8859-1"))
            # Write body
            self.wfile.write(body)
        except Exception:
            return

        # Attempt to parse JSON and normalize
        try:
            obj = json.loads(body.decode("utf-8"))
            # Remove X-Amzn-Trace-Id if present
            if isinstance(obj, dict) and isinstance(obj.get("headers"), dict):
                obj["headers"].pop("X-Amzn-Trace-Id", None)
            norm = json.dumps(obj, separators=(",", ":"))
            with _lock:
                _batch.append(norm)
        except Exception:
            # Non-JSON body; ignore
            pass

        # Attempt a conditional flush after adding this message
        _flush()


def _bg_flusher() -> None:
    """Background thread to flush batches periodically."""
    while True:
        time.sleep(max(BATCH_MS, 10) / 1000.0)
        _flush(force=False)


if __name__ == "__main__":
        print(f"Proxy listening on http://127.0.0.1:8080 (HTTP only)")
        print(f"Sending batches to {COLLECTOR} using dict {DICT_PATH}")
        # Start background flusher thread
        threading.Thread(target=_bg_flusher, daemon=True).start()
        # Start TCP server
        with socketserver.ThreadingTCPServer(("127.0.0.1", 8080), TinyProxy) as srv:
            try:
                srv.serve_forever()
            except KeyboardInterrupt:
                pass