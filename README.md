# Edge Dynamics

This repository contains a set of experiments, scripts and results that explore
the viability of performing **edge‑side compression** using per‑topic
dictionaries and batching.  The goal is to dramatically reduce the amount
of data transmitted from IoT devices, industrial sensors or self‑driving cars
to a central collector, without sacrificing fidelity or adding unacceptable
latency.

## Key ideas

1. **Normalize payloads** by stripping volatile fields (such as trace IDs) and
   serializing JSON with a canonical ordering.  This maximizes the overlap
   between messages.
2. **Batch multiple messages** together and flush either when a batch reaches
   a certain size (e.g. 100 messages) or a timeout (e.g. 250 ms).  Batching
   amortizes compression overhead and provides more context to exploit.
3. **Use small per‑topic dictionaries** (4–8 KB) with zstd to capture the
   repeating structure of each schema.  Each batch references its dictionary
   via a `dict_id`, allowing the collector to decompress correctly.
4. **Record metrics** at ingestion time to quantify bytes saved and
   understand the trade‑offs between latency, batch size and dictionary
   effectiveness.

## Repository layout

```
edge_dynamics/
├── README.md             This file
├── edge_agent.py         Example edge agent that batches and compresses per topic
├── collector_server.py   Collector that receives compressed frames and writes JSONL
├── train_dict.py         Script to train per‑topic dictionaries from sample data
├── proxy.py              Prototype HTTP proxy used in the initial tests
├── collector.py          Matching collector for the proxy prototype
├── run_demo.sh           Bash script to reproduce the initial HTTP demo
├── dict_local            Dictionary produced by the HTTP demo
├── report.csv            Compression ratios for the HTTP demo
├── compression_per_msg.png Chart visualising per‑message compression for the HTTP demo
├── topic_dict_report.csv  CSV comparing global vs. per‑topic dictionaries for file metadata
├── topic_dict_comparison.png Chart showing per‑topic vs. global dictionary performance
├── self_driving_bbox_compression.csv CSV summarising compression on synthetic bounding‑box metadata
├── self_driving_compression_chart.png Chart for the bounding‑box experiment
└── recv.jsonl            Example reconstructed output from the HTTP demo
```

## Summary of findings

### HTTP telemetry demo

The `edge_demo` prototype captured 50 identical HTTP JSON responses and
compressed them using zstd with and without a dictionary.  Batching the
messages and compressing them with a small dictionary reduced the payload
size by **over 99 %** (from ~12 KB down to ~38 B), proving that per‑batch
dictionary compression can practically eliminate the cost of sending
repetitive telemetry.  Per‑message gzip, by contrast, sometimes increased
size because of header overhead.

### Per‑topic dictionary demo

In the `edge_topic_demo` experiment, we simulated three different file
types (txt, csv and json) and generated 100 messages per topic.  Raw
canonical JSON averaged about 116 bytes per message.  Standard zlib
compression (no dictionary) reduced this to ~28.5 bytes per message.  A
single global dictionary shaved it slightly further, but using a small
per‑topic dictionary achieved **84 % reduction** relative to the raw
payload and about **34 %** improvement over plain zlib.  Results are
documented in `topic_dict_report.csv` and visualised in the accompanying
chart.

### Self‑driving bounding‑box demo

To test whether dictionary compression benefits self‑driving cars, the
`self_driving_test` generated synthetic bounding‑box messages for three
cameras.  Each message averaged 760–795 bytes.  Compressing with zlib
without a dictionary cut the average to ~325 bytes.  Adding a global
dictionary reduced this to ~200 bytes, and per‑topic dictionaries brought
it down further to ~187 bytes – a roughly **75 % reduction** over raw
JSON and a significant gain compared to no dictionary.  Details are
recorded in the CSV and chart provided.

### Conclusion

These experiments demonstrate that combining normalization, batching
and small per‑topic dictionaries yields dramatic reductions in
bandwidth without losing data.  For IoT devices and autonomous
vehicles that generate structured telemetry at high rates, such an
"edge dynamics" approach can cut costs, improve responsiveness and
extend battery life.  The provided scripts make it easy to adapt
this technique to your own schemas and transport protocols.