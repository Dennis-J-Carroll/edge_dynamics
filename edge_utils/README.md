# Edge Utilities

Production-ready utilities for the edge_dynamics compression system.

## Modules

### 📝 Structured Logging (`logging.py`)

JSON-formatted logging with context enrichment for better observability.

**Features:**
- JSON-structured output for log aggregation systems
- Thread-safe operation
- Automatic timestamp and level tagging
- Extra field support for contextual data

**Usage:**
```python
from edge_utils.logging import get_logger

logger = get_logger("edge_agent")
logger.info("batch_flushed", topic="sensors.temp", count=100, ratio=0.25)
```

**Output:**
```json
{"timestamp":"2024-01-15T10:30:45.123Z","level":"INFO","logger":"edge_agent","message":"batch_flushed","topic":"sensors.temp","count":100,"ratio":0.25}
```

### ⚙️ Configuration Management (`config.py`)

Type-safe configuration with Pydantic and environment variable support.

**Features:**
- Automatic validation with clear error messages
- Environment variable override (EDGE_* prefix)
- .env file support
- Type safety with IDE autocomplete
- Cached settings for performance

**Usage:**
```python
from edge_utils.config import get_settings

settings = get_settings()
print(f"Collector: {settings.collector_host}:{settings.collector_port}")
```

**Environment Variables:**
```bash
export EDGE_COLLECTOR_HOST=collector.example.com
export EDGE_COLLECTOR_PORT=7000
export EDGE_BATCH_MAX=100
export EDGE_COMPRESSION_LEVEL=7
```

### 🔒 Input Validation (`validation.py`)

Security-focused input validation to prevent common attacks.

**Features:**
- Topic name sanitization (prevents path traversal)
- Dictionary ID validation
- Message size limits (DoS prevention)
- Port number validation
- Header structure validation
- Path sanitization

**Usage:**
```python
from edge_utils.validation import InputValidator

validator = InputValidator()

# Validate topic (raises ValidationError if invalid)
topic = validator.validate_topic("sensors.temperature")

# Sanitize file paths
safe_path = validator.sanitize_path("data.jsonl", "/app/out")

# Validate message header
header = validator.validate_header(header_dict)
```

**Security Checks:**
- ✅ Alphanumeric topics only (dots, dashes, underscores allowed)
- ✅ No path traversal (`..` sequences blocked)
- ✅ Length limits enforced
- ✅ Message size limits (10 MB max per message, 100 MB max per batch)
- ✅ Port range validation (1-65535)

### 📊 Metrics Collection (`metrics.py`)

Thread-safe metrics collection with per-topic and overall aggregation.

**Features:**
- Per-topic metrics tracking
- Overall metrics aggregation
- Thread-safe operation
- Compression ratio calculation
- Throughput measurement
- Error tracking (compression & network)
- CSV export support

**Usage:**
```python
from edge_utils.metrics import MetricsCollector

metrics = MetricsCollector()

# Record a batch flush
metrics.record_batch(
    topic="sensors.temp",
    message_count=100,
    raw_bytes=5000,
    compressed_bytes=1250,
    duration_ms=15.5
)

# Get statistics
stats = metrics.get_stats()
print(f"Compression ratio: {stats['overall']['compression_ratio']:.2%}")
print(f"Throughput: {stats['overall']['throughput_mbps']:.2f} MB/s")

# Get per-topic stats
topic_stats = metrics.get_topic_stats("sensors.temp")
print(f"Topic compression: {topic_stats['compression_ratio']:.2%}")
```

**Metrics Tracked:**
- Messages processed
- Bytes in/out
- Compression ratio
- Flush count
- Average flush duration
- Throughput (MB/s)
- Messages per second
- Error counts

## Installation

The utilities are included automatically when you install edge_dynamics:

```bash
pip install -r requirements.txt
```

Dependencies:
- `pydantic>=2.0.0` - Configuration management
- `zstandard>=0.22.0` - Compression (already required)
- `ujson>=5.9.0` - Fast JSON (already required)

## Integration Examples

### Complete Edge Agent with Utilities

```python
from edge_utils import get_logger, get_settings, InputValidator, MetricsCollector

# Initialize
logger = get_logger("edge_agent")
settings = get_settings()
validator = InputValidator()
metrics = MetricsCollector()

logger.info("agent_starting",
    collector=f"{settings.collector_host}:{settings.collector_port}",
    batch_max=settings.batch_max
)

def process_message(topic: str, msg: dict):
    try:
        # Validate topic
        topic = validator.validate_topic(topic)

        # Process message...
        raw_len = len(json.dumps(msg))
        compressed = compress(msg)

        # Record metrics
        metrics.record_batch(topic, 1, raw_len, len(compressed), 5.0)

        logger.info("message_processed", topic=topic, size=raw_len)

    except ValidationError as e:
        logger.error("validation_failed", topic=topic, error=str(e))
    except Exception as e:
        logger.exception("processing_failed", topic=topic)
        metrics.record_compression_error(topic)
```

### Metrics Dashboard

```python
import time
from edge_utils.metrics import MetricsCollector

metrics = MetricsCollector()

# Collect metrics...
while True:
    stats = metrics.get_stats()

    print(f"\n=== Edge Agent Metrics ===")
    print(f"Messages: {stats['overall']['messages_processed']:,}")
    print(f"Compression: {stats['overall']['compression_ratio']:.2%}")
    print(f"Throughput: {stats['overall']['throughput_mbps']:.2f} MB/s")
    print(f"Errors: {stats['overall']['compression_errors'] + stats['overall']['network_errors']}")

    print(f"\nPer-Topic Breakdown:")
    for topic_stats in stats['topics']:
        print(f"  {topic_stats['topic']}: {topic_stats['compression_ratio']:.2%} compression")

    time.sleep(10)
```

## Testing

Tests for the utilities are located in `tests/edge_utils/`:

```bash
pytest tests/edge_utils/ -v
```

## Best Practices

### Logging

✅ **Do:** Use structured fields for searchable data
```python
logger.info("batch_sent", topic="sensors", count=100, bytes=5000)
```

❌ **Don't:** Embed data in message strings
```python
logger.info(f"Sent batch for sensors with 100 messages and 5000 bytes")
```

### Configuration

✅ **Do:** Use environment variables for deployment-specific settings
```python
settings = get_settings()  # Reads from EDGE_* environment variables
```

❌ **Don't:** Hardcode configuration in application code
```python
COLLECTOR_HOST = "hardcoded.example.com"  # Bad!
```

### Validation

✅ **Do:** Validate all external input
```python
topic = validator.validate_topic(user_input)
path = validator.sanitize_path(filename, base_dir)
```

❌ **Don't:** Trust user input
```python
file_path = os.path.join(base_dir, user_filename)  # Path traversal risk!
```

### Metrics

✅ **Do:** Record metrics consistently
```python
start = time.time()
result = compress_data(data)
duration_ms = (time.time() - start) * 1000
metrics.record_batch(topic, count, raw_len, len(result), duration_ms)
```

❌ **Don't:** Skip metrics on error paths
```python
try:
    compress_data(data)
except Exception:
    pass  # Forgot to record error!
```

## Performance

The utilities are designed for production use with minimal overhead:

- **Logging**: <0.1ms per log entry
- **Validation**: <0.01ms per topic validation
- **Metrics**: <0.01ms per record (thread-safe)
- **Configuration**: Cached (zero overhead after first load)

## Security Considerations

The validation module provides defense-in-depth:

1. **Input Sanitization**: All topics and paths are validated
2. **Size Limits**: DoS prevention via message/batch size limits
3. **Path Traversal**: Blocked via `sanitize_path()`
4. **Injection Prevention**: Pattern-based validation blocks malicious input

## License

SPDX-License-Identifier: Apache-2.0
