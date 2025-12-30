#!/usr/bin/env python3
"""
up.py

Simple upstream HTTP server for testing the edge compression demo.
Returns a fake JSON response on /get endpoint, simulating a telemetry
or API service. The response includes various headers and a trace ID
that can be used to test normalization and compression.

Usage:
    python3 up.py

This will start a server on http://127.0.0.1:9000
"""

import json
import random
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


class UpstreamHandler(BaseHTTPRequestHandler):
    """Simple HTTP request handler that returns JSON responses."""

    def do_GET(self) -> None:
        """Handle GET requests."""
        if self.path == "/get":
            # Generate a response similar to httpbin.org/get
            response = {
                "args": {},
                "headers": {
                    "Accept": self.headers.get("Accept", "*/*"),
                    "Host": self.headers.get("Host", "127.0.0.1:9000"),
                    "User-Agent": self.headers.get("User-Agent", "curl/7.68.0"),
                    "X-Amzn-Trace-Id": f"Root=1-{int(time.time())}-{random.randint(10**16, 10**17-1)}",
                },
                "origin": self.client_address[0],
                "url": f"http://{self.headers.get('Host', '127.0.0.1:9000')}/get",
            }

            # Send response
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            body = json.dumps(response, indent=2).encode("utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

    def log_message(self, format: str, *args) -> None:
        """Suppress log messages for cleaner output."""
        pass


def main() -> None:
    """Start the upstream server."""
    host, port = "127.0.0.1", 9000
    server = HTTPServer((host, port), UpstreamHandler)
    print(f"Upstream server listening on http://{host}:{port}")
    print("Press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
