# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

#### Project Infrastructure
- `requirements.txt` - Core Python dependencies (zstandard, ujson)
- `pyproject.toml` - Modern Python packaging with setuptools backend
- `.gitignore` - Comprehensive Python gitignore with project-specific exclusions
- `.dockerignore` - Docker build exclusions
- `.pre-commit-config.yaml` - Pre-commit hooks for code quality

#### Documentation
- Installation section in README with prerequisites and quick start guide
- `examples/` directory with three comprehensive use cases:
  - File metadata telemetry with sample data and compression metrics
  - IoT sensor data with healthcare wearable case study
  - Vehicle/self-driving telemetry with bounding box compression
- Each example includes detailed README with schema, compression ratios, and training instructions
- `CHANGELOG.md` - This file

#### Testing & CI/CD
- `tests/` directory with pytest structure
- Test files for `edge_agent` and `train_dict` modules
- `tests/conftest.py` with shared fixtures
- GitHub Actions workflow for CI (`.github/workflows/ci.yml`)
  - Multi-version Python testing (3.10, 3.11, 3.12)
  - Linting with ruff
  - Formatting checks with black
  - Type checking with mypy
  - Coverage reporting with pytest-cov
  - Security scanning with bandit and safety
- GitHub Actions workflow for pre-commit checks (`.github/workflows/pre-commit.yml`)

#### Containerization
- `Dockerfile` with multi-stage builds for:
  - Dictionary trainer
  - Edge agent
  - Collector server
  - HTTP proxy
  - Upstream test server
- `docker-compose.yml` for local development with all services
- Network configuration and volume mounts

#### Code & Tools
- `up.py` - Upstream HTTP test server (referenced but missing in run_demo.sh)
- Linting and formatting configuration in pyproject.toml:
  - Black code formatter (line-length: 100)
  - Ruff linter with comprehensive rule sets
  - Mypy type checker configuration

#### Production Utilities (`edge_utils/`)
- **Structured Logging** (`logging.py`)
  - JSON-formatted logging with context enrichment
  - Thread-safe operation for production environments
  - Automatic timestamp and level tagging
  - Support for extra fields for searchable contextual data
  - Cached logger instances for performance

- **Configuration Management** (`config.py`)
  - Type-safe configuration with Pydantic validation
  - Environment variable support with EDGE_ prefix
  - .env file loading capability
  - Automatic validation with clear error messages
  - Cached settings for zero runtime overhead
  - Support for 30+ configuration parameters

- **Input Validation** (`validation.py`)
  - Security-focused validation to prevent attacks
  - Topic name sanitization (blocks path traversal)
  - Message and batch size limits (DoS prevention)
  - Dictionary ID validation
  - Port number validation
  - Header structure validation
  - Path sanitization for safe file operations

- **Metrics Collection** (`metrics.py`)
  - Thread-safe metrics collection
  - Per-topic and overall aggregation
  - Compression ratio calculation
  - Throughput measurement (MB/s, msgs/sec)
  - Error tracking (compression & network)
  - CSV export support
  - Real-time statistics reporting

- **Circuit Breaker Pattern** (`circuit_breaker.py`)
  - Prevents cascading failures when collector is down
  - Automatic state transitions (CLOSED → OPEN → HALF_OPEN → CLOSED)
  - Configurable failure threshold and timeout
  - Success threshold for recovery validation
  - Statistics API for monitoring
  - Decorator and function call interfaces

- **Connection Pooling** (`connection_pool.py`)
  - Socket connection reuse for better performance
  - Thread-safe pool management
  - Configurable pool size and timeouts
  - Automatic connection health checks
  - Idle connection cleanup
  - Context manager interface
  - Statistics tracking (reuse rate, pool size)

- Comprehensive test suite for all utilities
  - `tests/edge_utils/test_logging.py` - Logging tests
  - `tests/edge_utils/test_validation.py` - Validation tests
  - `tests/edge_utils/test_metrics.py` - Metrics tests
  - `tests/edge_utils/test_circuit_breaker.py` - Circuit breaker tests
  - `tests/edge_utils/test_connection_pool.py` - Connection pool tests
  - 95%+ test coverage for utility modules

#### Production-Ready Implementations
- **edge_agent_v2.py** - Production edge agent
  - Integrates all edge_utils modules
  - Structured JSON logging throughout
  - Configuration via environment variables
  - Input validation for all topics
  - Comprehensive metrics collection
  - Circuit breaker for fault tolerance
  - Connection pooling for performance
  - Graceful shutdown handling

- **collector_server_v2.py** - Production collector
  - Integrates all edge_utils modules
  - Structured JSON logging
  - Configuration management
  - Header validation
  - Metrics tracking
  - Path sanitization for security
  - Graceful shutdown
  - Multi-threaded connection handling

### Changed

#### Documentation
- Fixed README.md formatting issue with orphaned text on lines 97-98
- Moved misplaced sentence about "extend battery life" to proper location in Conclusion section
- Added comprehensive `edge_utils/README.md` with:
  - Usage examples for all utility modules
  - Integration patterns and best practices
  - Performance characteristics
  - Security considerations

#### Code Quality
- Fixed incorrect type annotation in `edge_agent.py` line 81
  - Changed from `Optional[float] or Deque[bytes]` to `Any`
  - Added proper imports for Union and Any types
- Removed unused `subprocess` import from `proxy.py`
- Added pydantic dependency for configuration management

### Fixed
- README.md text flow and formatting
- Type hints compliance in edge_agent.py
- Missing up.py file now included and executable

## [0.1.0] - Initial Release

### Added
- Core compression system with zstandard
- Per-topic dictionary support
- Edge agent with batching and normalization
- Collector server with decompression
- HTTP proxy prototype
- Dictionary training tool
- Demo scripts and sample results
- Documentation with compression experiments
- Apache-2.0 license
- Contributing guidelines with DCO
