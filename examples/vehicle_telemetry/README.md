# Vehicle Telemetry (Self-Driving)

This example demonstrates compression of bounding box data from vehicle cameras.

## Message Schema

```json
{
  "camera": "front|left|right|rear",
  "frame": 1001,
  "objects": [
    {
      "type": "car|pedestrian|bicycle|...",
      "bbox": [x1, y1, x2, y2],
      "confidence": 0.95
    }
  ],
  "timestamp": 1640000000
}
```

## Typical Compression Ratios

- Raw JSON: ~780 bytes per message (with 2-3 objects)
- With zlib (no dict): ~325 bytes per message (58% reduction)
- With global dict: ~200 bytes per message (74% reduction)
- With per-camera dict: ~187 bytes per message (76% reduction)

## Recommended Settings

- Dictionary size: 8 KB per camera
- Batch size: 50-100 messages
- Batch timeout: 100 ms
- Compression level: 7

## Performance Considerations

Self-driving vehicles generate high-frequency telemetry:
- Multiple cameras at 10-30 FPS
- Each frame generates multiple object detections
- Network bandwidth is limited (4G/5G)

Per-camera dictionaries provide the best compression because:
1. Each camera has consistent object types and positions
2. Bounding boxes change incrementally between frames
3. Message structure is highly repetitive

## Training Dictionaries

```bash
# Organize samples by camera
mkdir -p samples/front samples/left samples/right

# Train per-camera dictionaries
python3 train_dict.py \
  --samples_root samples \
  --dict_dir dicts/vehicle \
  --size 8192
```

## Integration Notes

- Compress and batch before transmitting over cellular
- Use topic naming: `vehicle_id/camera_position`
- Monitor compression ratios to detect schema drift
- Retrain dictionaries periodically as routes change
