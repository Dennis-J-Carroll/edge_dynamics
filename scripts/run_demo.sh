#!/bin/bash
# Run the edge compression demo.
#
# This script orchestrates a complete demonstration of the edge compression
# pipeline using the local upstream (up.py), the batching proxy (proxy.py),
# and a simple netcat-based collector. It collects training data, trains a
# zstd dictionary, runs the proxy, sends a series of requests through the proxy,
# and collects both the compressed metrics and the reconstructed payload.
#
# At the end of the run, two files will be produced in the working directory:
#   - recv.jsonl  : the decompressed, normalized JSON lines collected from the proxy
#   - metrics.csv : a CSV file where each line is `flush_msgs,comp_bytes` for
#                    each batch flushed by the proxy
#   - dict_local  : the trained zstd dictionary used by the proxy
#
# After running this script, you can post-process `metrics.csv` and
# `recv.jsonl` to compute total compression ratios, bytes per message, etc.
# See the accompanying Python scripts or documentation for guidance.

set -euo pipefail

# Determine script directory and change to it
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Clean up any leftovers from previous runs
rm -f metrics.csv recv.jsonl dict_local batch_local.jsonl
rm -rf samples_local
mkdir -p samples_local

# Start the local upstream server
python3 up.py & upstream_pid=$!
sleep 1

# Collect sample responses and normalize them to build a dictionary
echo "Collecting training data..."
for i in $(seq 1 100); do
  # Fetch JSON from upstream and remove X-Amzn-Trace-Id to normalize structure
  curl -s http://127.0.0.1:9000/get \
    | jq -c 'del(.headers["X-Amzn-Trace-Id"])' \
    | tee "samples_local/$i.json" >> batch_local.jsonl
  # Ensure newlines between messages in batch file
  echo >> batch_local.jsonl
done

# Create zlib dictionary by taking the first 4096 bytes of the normalized sample file
echo "Building zlib dictionary..."
# Concatenate all sample JSON lines into a single file if not already created
# (batch_local.jsonl contains a newline after each JSON object)
# Extract the first 4096 bytes to use as a dictionary. If the file is smaller,
# use the entire file.
DICT_BYTES=4096
if [ -f batch_local.jsonl ]; then
  head -c "$DICT_BYTES" batch_local.jsonl > dict_local || cp batch_local.jsonl dict_local
else
  echo "Error: batch_local.jsonl not found for dictionary creation" >&2
  exit 1
fi

# Start collector: listens on port 7000 and writes decompressed output to recv.jsonl
echo "Starting collector..."
> recv.jsonl
# Launch the Python collector which uses zlib with dictionary
DICT_PATH=dict_local python3 collector.py & collector_pid=$!

# Give collector time to start
sleep 1

# Start proxy with environment variables; ensure metrics.csv is empty
echo "Starting proxy..."
rm -f metrics.csv
DICT_PATH=dict_local python3 proxy.py & proxy_pid=$!

# Give proxy time to start
sleep 1

# Send a burst of requests through the proxy
echo "Sending requests through proxy..."
for i in $(seq 1 100); do
  curl -s --proxy http://127.0.0.1:8080 http://127.0.0.1:9000/get > /dev/null
done

# Allow time for the proxy to flush batches
sleep 3

# Terminate processes
echo "Stopping processes..."
kill "$proxy_pid" "$collector_pid" "$upstream_pid" 2>/dev/null || true

# Give netcat time to flush buffers
sleep 1

echo "Demo complete. Results saved to recv.jsonl and metrics.csv."