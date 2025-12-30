# File Metadata Telemetry

This example demonstrates compression of file system telemetry data.

## Message Schema

```json
{
  "file_type": "txt|csv|json|...",
  "path": "/path/to/file",
  "size": 1024,
  "checksum": "hash",
  "timestamp": 1640000000
}
```

## Typical Compression Ratios

- Raw JSON: ~120 bytes per message
- With zlib (no dict): ~28 bytes per message (76% reduction)
- With per-topic dict: ~19 bytes per message (84% reduction)

## Recommended Settings

- Dictionary size: 4 KB
- Batch size: 100 messages
- Batch timeout: 250 ms
- Compression level: 7

## Training a Dictionary

```bash
python3 train_dict.py \
  --samples_root examples/file_metadata \
  --dict_dir dicts/file_metadata \
  --size 4096
```
