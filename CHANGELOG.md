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

### Changed

#### Documentation
- Fixed README.md formatting issue with orphaned text on lines 97-98
- Moved misplaced sentence about "extend battery life" to proper location in Conclusion section

#### Code Quality
- Fixed incorrect type annotation in `edge_agent.py` line 81
  - Changed from `Optional[float] or Deque[bytes]` to `Any`
  - Added proper imports for Union and Any types
- Removed unused `subprocess` import from `proxy.py`

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
