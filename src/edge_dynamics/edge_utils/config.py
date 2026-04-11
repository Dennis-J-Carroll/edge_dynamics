# SPDX-License-Identifier: Apache-2.0
"""
Configuration management module for edge_dynamics.

Provides type-safe, validated configuration using Pydantic with
environment variable support.

Example:
    >>> from edge_utils.config import get_settings
    >>> settings = get_settings()
    >>> print(f"Collector: {settings.collector_host}:{settings.collector_port}")
"""

import os
from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings with environment variable support.

    All settings can be overridden via environment variables with the
    EDGE_ prefix (e.g., EDGE_COLLECTOR_HOST=localhost).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="EDGE_",
        case_sensitive=False,
    )

    # Collector settings
    collector_host: str = Field(
        default="127.0.0.1", description="Collector server hostname or IP"
    )
    collector_port: int = Field(default=7000, ge=1, le=65535, description="Collector server port")

    # Batching settings
    batch_max: int = Field(default=100, ge=1, le=10000, description="Maximum messages per batch")
    batch_ms: int = Field(default=250, ge=10, le=60000, description="Maximum batch age in milliseconds")

    # Compression settings
    compression_level: int = Field(
        default=7, ge=1, le=22, description="Zstandard compression level (1-22)"
    )
    compression_codec: str = Field(default="zstd", description="Compression codec to use")

    # Directory settings
    dict_dir: str = Field(default="./dicts", description="Directory containing compression dictionaries")
    out_dir: str = Field(default="./out", description="Output directory for decompressed data")
    metrics_file: str = Field(default="./metrics.csv", description="Path to metrics CSV file")

    # Server settings
    server_host: str = Field(default="0.0.0.0", description="Server bind address")
    server_port: int = Field(default=7000, ge=1, le=65535, description="Server bind port")

    # Proxy settings
    proxy_host: str = Field(default="127.0.0.1", description="Proxy server hostname")
    proxy_port: int = Field(default=8080, ge=1, le=65535, description="Proxy server port")

    # Dictionary training settings
    dict_size: int = Field(default=4096, ge=256, le=1048576, description="Dictionary size in bytes")
    samples_root: Optional[str] = Field(default=None, description="Root directory for sample data")

    # Performance settings
    max_workers: Optional[int] = Field(
        default=None, ge=1, le=128, description="Maximum worker threads (None = CPU count)"
    )
    connection_timeout: int = Field(default=2, ge=1, le=300, description="Socket connection timeout in seconds")
    max_memory_mb: int = Field(default=100, ge=10, le=10000, description="Maximum memory usage in MB")

    # Logging settings
    log_level: str = Field(default="INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    log_format: str = Field(default="json", description="Log format (json or text)")

    # Feature flags
    enable_metrics: bool = Field(default=True, description="Enable metrics collection")
    enable_health_check: bool = Field(default=True, description="Enable health check endpoint")

    @field_validator("compression_level")
    @classmethod
    def validate_compression_level(cls, v: int) -> int:
        """Validate compression level is within acceptable range."""
        if not 1 <= v <= 22:
            raise ValueError("Compression level must be between 1 and 22")
        return v

    @field_validator("log_level", mode="before")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Normalize and validate log level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_upper = str(v).upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Log level must be one of: {', '.join(valid_levels)}")
        return v_upper

    @field_validator("compression_codec", mode="before")
    @classmethod
    def validate_codec(cls, v: str) -> str:
        """Normalize and validate compression codec."""
        valid_codecs = {"zstd", "zlib", "none"}
        v_lower = str(v).lower()
        if v_lower not in valid_codecs:
            raise ValueError(f"Codec must be one of: {', '.join(valid_codecs)}")
        return v_lower

    @field_validator("dict_dir", "out_dir")
    @classmethod
    def create_directory_if_needed(cls, v: str) -> str:
        """Create directory if it doesn't exist."""
        if v and not os.path.exists(v):
            try:
                os.makedirs(v, exist_ok=True)
            except OSError:
                pass  # Directory creation is optional
        return v


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Returns:
        Settings object with configuration loaded from environment

    Example:
        >>> settings = get_settings()
        >>> print(settings.collector_host)
        127.0.0.1
    """
    return Settings()


def reload_settings() -> Settings:
    """
    Force reload of settings (clears cache).

    Returns:
        New Settings instance

    Example:
        >>> os.environ['EDGE_COLLECTOR_HOST'] = 'collector.example.com'
        >>> settings = reload_settings()
        >>> print(settings.collector_host)
        collector.example.com
    """
    get_settings.cache_clear()
    return get_settings()
