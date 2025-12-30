# Examples

This directory contains example data and use cases for the edge_dynamics compression system.

## Directory Structure

- `file_metadata/` - File system telemetry examples
- `iot_sensors/` - IoT sensor data examples
- `vehicle_telemetry/` - Self-driving vehicle telemetry examples

## Usage

Each subdirectory contains:
- Sample JSONL files with example messages
- A README explaining the use case
- Configuration suggestions for optimal compression

## Running Examples

### 1. Train dictionaries from sample data

```bash
python3 train_dict.py \
  --samples_root examples/file_metadata \
  --dict_dir dicts \
  --size 4096
```

### 2. Start the collector

```bash
python3 collector_server.py
```

### 3. Run the edge agent

```bash
python3 edge_agent.py
```

Or use the synthetic data feeder included in edge_agent.py for testing.

## Adding Your Own Examples

1. Create a new subdirectory under `examples/`
2. Add sample JSONL files (one JSON object per line)
3. Document the message schema in a README
4. Train a dictionary specific to your data

For best results, provide at least 200× the dictionary size in training data
(e.g., 800 KB of samples for a 4 KB dictionary).
