#!/usr/bin/env python3
"""
train_dict.py

This script trains per‑topic compression dictionaries from sample JSONL files.
It expects a directory of topics, where each topic directory contains
JSONL files (one JSON object per line). It concatenates samples from each
topic and uses zstandard's training API to generate a small dictionary.

Usage example:
  python3 train_dict.py --samples_root samples --dict_dir dicts --size 4096

This will produce dict files at dicts/<topic>.zdict and write a
dict_index.json mapping each topic to its dictionary identifier and path.
"""

import argparse
import glob
import hashlib
import json
import os
from typing import List

import zstandard as zstd


def load_bytes(paths: List[str], max_bytes: int = 256_000) -> bytes:
    """Read up to max_bytes from a list of files, concatenating bytes with newlines."""
    buf = bytearray()
    for p in paths:
        with open(p, "rb") as f:
            data = f.read()
        buf += data + b"\n"
        if len(buf) >= max_bytes:
            break
    return bytes(buf)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train per‑topic zstandard dictionaries from samples")
    parser.add_argument("--samples_root", required=True, help="Root directory containing topic subdirectories of JSONL samples")
    parser.add_argument("--dict_dir", required=True, help="Output directory to save dictionaries and dict_index.json")
    parser.add_argument("--size", type=int, default=4096, help="Dictionary size in bytes (default: 4096)")
    args = parser.parse_args()

    os.makedirs(args.dict_dir, exist_ok=True)
    index: dict = {}
    for topic in sorted(os.listdir(args.samples_root)):
        topic_dir = os.path.join(args.samples_root, topic)
        if not os.path.isdir(topic_dir):
            continue
        sample_files = sorted(glob.glob(os.path.join(topic_dir, "*.jsonl")))
        if not sample_files:
            continue
        # Collect bytes; use 200× size guideline for training for better quality
        src = load_bytes(sample_files, max_bytes=args.size * 200)
        trainer = zstd.ZstdTrainer(src, dict_size=args.size)
        zdict = trainer.train()
        sha = hashlib.sha256(zdict).hexdigest()[:16]
        dict_path = os.path.join(args.dict_dir, f"{topic}.zdict")
        with open(dict_path, "wb") as f:
            f.write(zdict)
        dict_id = f"d:{topic}:{sha}"
        index[topic] = {"dict_id": dict_id, "path": dict_path, "size": len(zdict)}
        print(f"[dict] {topic}: {len(zdict)} bytes, id={dict_id}")
    # Write index
    index_path = os.path.join(args.dict_dir, "dict_index.json")
    with open(index_path, "w") as f:
        json.dump(index, f, separators=(",", ":"))
    print(f"[ok] wrote dict index with {len(index)} topics to {index_path}")


if __name__ == "__main__":
    main()