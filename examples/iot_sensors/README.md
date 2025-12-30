# IoT Sensor Telemetry

This example demonstrates compression of IoT sensor data streams.

## Message Schema

```json
{
  "sensor_id": "temp-01",
  "type": "temperature|humidity|motion|...",
  "value": 22.5,
  "unit": "celsius|percent|boolean|...",
  "timestamp": 1640000000,
  "location": "room-a"
}
```

## Typical Compression Ratios

- Raw JSON: ~140 bytes per message
- With zlib (no dict): ~35 bytes per message (75% reduction)
- With per-sensor-type dict: ~22 bytes per message (84% reduction)

## Recommended Settings

- Dictionary size: 4-8 KB per sensor type
- Batch size: 50-100 messages
- Batch timeout: 100-250 ms
- Compression level: 5-7

## Use Case: Healthcare Wearables

For wearable health devices transmitting heart rate and motion data:

- Batch interval: 250 ms (meets latency SLA)
- Per-topic dictionaries for heart_rate, motion, location
- Achieved 60% payload reduction
- 40% lower cellular data costs

## Training Dictionaries

Create separate dictionaries for each sensor type:

```bash
# Group samples by sensor type first
mkdir -p samples/temperature samples/humidity samples/motion

# Train per-type dictionaries
python3 train_dict.py \
  --samples_root samples \
  --dict_dir dicts/sensors \
  --size 4096
```
