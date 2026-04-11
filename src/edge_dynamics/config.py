#!/usr/bin/env python3
"""
Configuration management for edge_dynamics using Pydantic.

This module provides centralized, validated configuration management
with support for environment variables and .env files.
"""

import os
from typing import Optional
from pydantic import field_validator, model_validator, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings with validation.

    Settings are loaded from environment variables and .env files.
    All values are validated to ensure they meet requirements.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="EDGE_",
        case_sensitive=False,
    )

    # Collector Configuration
    collector_host: str = Field(
        default="127.0.0.1",
        description="Collector server hostname or IP address"
    )
    collector_port: int = Field(
        default=7000,
        description="Collector server port"
    )

    # Batching Configuration
    batch_max: int = Field(
        default=100,
        description="Maximum messages per batch before flush"
    )
    batch_ms: int = Field(
        default=250,
        description="Maximum age in milliseconds before batch flush"
    )

    # Compression Configuration
    compression_level: int = Field(
        default=7,
        description="Zstandard compression level (1-22)"
    )
    dict_dir: str = Field(
        default="./dicts",
        description="Directory containing compression dictionaries"
    )

    # Output Configuration
    out_dir: str = Field(
        default="./out",
        description="Directory for output files"
    )
    metrics_file: str = Field(
        default="./metrics.csv",
        description="Path to metrics CSV file"
    )

    # Security Configuration
    auth_enabled: bool = Field(
        default=False,
        description="Enable HMAC authentication for frame signing"
    )
    auth_secret_key: Optional[str] = Field(
        default=None,
        description="Secret key for HMAC authentication (hex-encoded)"
    )
    hmac_algorithm: str = Field(
        default="sha256",
        description="HMAC hash algorithm (sha256, sha384, sha512)"
    )
    hmac_header_name: str = Field(
        default="hmac",
        description="Key name for HMAC signature in frame header"
    )

    # TLS Configuration
    tls_enabled: bool = Field(
        default=False,
        description="Enable TLS encryption for agent ↔ collector connections"
    )
    tls_cert_file: Optional[str] = Field(
        default=None,
        description="Path to TLS certificate file (PEM)"
    )
    tls_key_file: Optional[str] = Field(
        default=None,
        description="Path to TLS private key file (PEM)"
    )
    tls_ca_file: Optional[str] = Field(
        default=None,
        description="Path to CA bundle for verifying peer certificates"
    )
    tls_verify_client: bool = Field(
        default=False,
        description="Require client certificate on the collector (mutual TLS)"
    )
    tls_check_hostname: bool = Field(
        default=True,
        description="Verify server hostname matches certificate (agent-side)"
    )

    # Logging Configuration
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    log_format: str = Field(
        default="json",
        description="Log format (json or text)"
    )

    # Performance Configuration
    max_memory_mb: int = Field(
        default=100,
        description="Maximum memory usage in MB"
    )
    connection_pool_size: int = Field(
        default=10,
        description="Maximum connections in pool"
    )

    # Monitoring Configuration
    metrics_enabled: bool = Field(
        default=True,
        description="Enable metrics collection"
    )
    health_check_port: int = Field(
        default=8080,
        description="Port for health check endpoint"
    )

    # Disk Buffer Configuration
    disk_buffer_enabled: bool = Field(
        default=True,
        description="Enable persistent disk buffering"
    )
    disk_buffer_path: str = Field(
        default="./buffer.db",
        description="Path to SQLite buffer database"
    )
    disk_buffer_max_mb: int = Field(
        default=50,
        description="Maximum size of disk buffer in MB"
    )

    @field_validator("collector_port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not 1 <= v <= 65535:
            raise ValueError("Port must be between 1 and 65535")
        return v

    @field_validator("compression_level")
    @classmethod
    def validate_compression_level(cls, v: int) -> int:
        if not 1 <= v <= 22:
            raise ValueError("Compression level must be between 1 and 22")
        return v

    @field_validator("batch_max")
    @classmethod
    def validate_batch_max(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("Batch max must be positive")
        return v

    @field_validator("batch_ms")
    @classmethod
    def validate_batch_ms(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("Batch timeout must be positive")
        return v

    @field_validator("log_level", mode="before")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = str(v).upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v_upper

    @field_validator("log_format")
    @classmethod
    def validate_log_format(cls, v: str) -> str:
        if v not in ["json", "text"]:
            raise ValueError("Log format must be 'json' or 'text'")
        return v

    @field_validator("hmac_algorithm")
    @classmethod
    def validate_hmac_algorithm(cls, v: str) -> str:
        allowed = {"sha256", "sha384", "sha512"}
        if v not in allowed:
            raise ValueError(f"HMAC algorithm must be one of {allowed}")
        return v

    @field_validator("tls_cert_file", "tls_key_file", "tls_ca_file")
    @classmethod
    def validate_tls_paths(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not os.path.isfile(v):
            raise ValueError(f"TLS file path does not exist: {v}")
        return v

    @field_validator("dict_dir", "out_dir")
    @classmethod
    def validate_directory(cls, v: str) -> str:
        os.makedirs(v, exist_ok=True)
        return v

    @model_validator(mode="after")
    def validate_auth_secret_key(self) -> "Settings":
        if self.auth_enabled and not self.auth_secret_key:
            raise ValueError("Secret key is required when auth is enabled")
        return self


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """
    Get or create the global settings instance.

    Returns:
        Settings: The application settings instance
    """
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reload_settings() -> Settings:
    """
    Reload settings from environment and .env file.

    Returns:
        Settings: The reloaded settings instance
    """
    global _settings
    _settings = Settings()
    return _settings


# Example usage
if __name__ == "__main__":
    # Load settings
    config = get_settings()

    # Access configuration
    print(f"Collector: {config.collector_host}:{config.collector_port}")
    print(f"Batch size: {config.batch_max}")
    print(f"Compression level: {config.compression_level}")

    # Test validation
    try:
        # This will raise a validation error
        invalid_config = Settings(compression_level=25)
    except ValueError as e:
        print(f"Validation error (expected): {e}")
