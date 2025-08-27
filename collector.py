#!/usr/bin/env python3
"""
Collector for the edge compression demo.

This script listens on a TCP port (default 7000) and receives batches of
compressed data from the proxy. It uses zlib to decompress each batch using
a pre-trained dictionary and appends the reconstructed JSON lines to
`recv.jsonl` in the current working directory.

The dictionary file path and port can be configured via the environment
variables `DICT_PATH`, `COLLECTOR_HOST`, and `COLLECTOR_PORT`. By default,
the dictionary file is `dict_local`, the host is `127.0.0.1`, and the port
is `7000`.

Usage:
    DICT_PATH=dict_local python3 collector.py

This script runs indefinitely until terminated (e.g. via Ctrl-C).
"""

import os
import socket
import zlib


def main() -> None:
    dict_path = os.environ.get("DICT_PATH", "dict_local")
    host = os.environ.get("COLLECTOR_HOST", "127.0.0.1")
    port = int(os.environ.get("COLLECTOR_PORT", "7000"))

    # Load dictionary for decompression
    try:
        with open(dict_path, "rb") as f:
            dictionary = f.read()
    except FileNotFoundError:
        dictionary = b""
    print(f"Collector using dictionary {dict_path!r} ({len(dictionary)} bytes)")
    print(f"Listening on {host}:{port}")

    # Create TCP server
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen()
    try:
        while True:
            conn, addr = srv.accept()
            with conn:
                data_chunks = []
                while True:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    data_chunks.append(chunk)
                if not data_chunks:
                    continue
                data = b"".join(data_chunks)
                # Decompress with dictionary
                try:
                    if dictionary:
                        dcmp = zlib.decompressobj(wbits=-zlib.MAX_WBITS, zdict=dictionary)
                    else:
                        dcmp = zlib.decompressobj(wbits=-zlib.MAX_WBITS)
                    decompressed = dcmp.decompress(data) + dcmp.flush()
                except Exception as e:
                    print(f"decompression error: {e}")
                    continue
                # Append to output file
                with open("recv.jsonl", "ab") as outf:
                    outf.write(decompressed)
    finally:
        srv.close()


if __name__ == "__main__":
    main()