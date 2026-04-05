#!/usr/bin/env python3
"""
security.py

Provides HMAC signing/verification and TLS context builders for
securing agent ↔ collector communication.

HMAC:
    - sign_frame(payload, key, algorithm) → hex digest string
    - verify_frame(payload, signature, key, algorithm) → bool

TLS:
    - create_client_context(settings) → ssl.SSLContext (for edge_agent)
    - create_server_context(settings) → ssl.SSLContext (for collector)
"""

import hashlib
import hmac
import ssl
from typing import Optional

from edge_dynamics.config import Settings


# ---------------------------------------------------------------------------
# HMAC helpers
# ---------------------------------------------------------------------------

def sign_frame(payload: bytes, key: str, algorithm: str = "sha256") -> str:
    """
    Compute an HMAC signature over *payload*.

    Args:
        payload:   Raw bytes to sign (typically the compressed frame body).
        key:       Hex-encoded secret key.
        algorithm: Hash algorithm name (sha256 | sha384 | sha512).

    Returns:
        Hex-encoded HMAC digest string.
    """
    key_bytes = bytes.fromhex(key)
    digest_mod = _get_hash_func(algorithm)
    return hmac.new(key_bytes, payload, digest_mod).hexdigest()


def verify_frame(
    payload: bytes, signature: str, key: str, algorithm: str = "sha256"
) -> bool:
    """
    Verify an HMAC signature over *payload*.

    Uses constant-time comparison to prevent timing attacks.

    Args:
        payload:   Raw bytes that were signed.
        signature: Hex-encoded HMAC digest to verify against.
        key:       Hex-encoded secret key.
        algorithm: Hash algorithm name (sha256 | sha384 | sha512).

    Returns:
        True if the signature is valid, False otherwise.
    """
    expected = sign_frame(payload, key, algorithm)
    return hmac.compare_digest(expected, signature)


def _get_hash_func(algorithm: str):
    """Map algorithm name → hashlib constructor."""
    mapping = {
        "sha256": hashlib.sha256,
        "sha384": hashlib.sha384,
        "sha512": hashlib.sha512,
    }
    func = mapping.get(algorithm)
    if func is None:
        raise ValueError(
            f"Unsupported HMAC algorithm '{algorithm}'; "
            f"choose from {set(mapping.keys())}"
        )
    return func


# ---------------------------------------------------------------------------
# TLS context builders
# ---------------------------------------------------------------------------

def create_client_context(settings: Settings) -> ssl.SSLContext:
    """
    Build an ``ssl.SSLContext`` suitable for the **edge agent** (client side).

    Behaviour:
        • Loads the CA bundle for server-cert verification if ``tls_ca_file``
          is provided; otherwise falls back to system defaults.
        • Optionally loads a client certificate + key for mutual TLS.
        • Respects ``tls_check_hostname``.

    Args:
        settings: Application settings instance.

    Returns:
        A configured ``ssl.SSLContext``.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    # Hostname verification
    ctx.check_hostname = settings.tls_check_hostname

    # CA bundle
    if settings.tls_ca_file:
        ctx.load_verify_locations(settings.tls_ca_file)
    else:
        ctx.load_default_certs()

    # Client certificate (for mutual TLS)
    if settings.tls_cert_file and settings.tls_key_file:
        ctx.load_cert_chain(
            certfile=settings.tls_cert_file,
            keyfile=settings.tls_key_file,
        )

    return ctx


def create_server_context(settings: Settings) -> ssl.SSLContext:
    """
    Build an ``ssl.SSLContext`` suitable for the **collector** (server side).

    Behaviour:
        • Loads the server certificate + private key.
        • Optionally requires client certificates (mutual TLS) when
          ``tls_verify_client`` is True, using ``tls_ca_file`` as the
          trust anchor.

    Args:
        settings: Application settings instance.

    Raises:
        ValueError: If ``tls_cert_file`` or ``tls_key_file`` is not set.

    Returns:
        A configured ``ssl.SSLContext``.
    """
    if not settings.tls_cert_file or not settings.tls_key_file:
        raise ValueError(
            "tls_cert_file and tls_key_file are required for the collector "
            "when TLS is enabled"
        )

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(
        certfile=settings.tls_cert_file,
        keyfile=settings.tls_key_file,
    )

    # Mutual TLS: require the agent to present a valid client certificate
    if settings.tls_verify_client:
        ctx.verify_mode = ssl.CERT_REQUIRED
        if settings.tls_ca_file:
            ctx.load_verify_locations(settings.tls_ca_file)
        else:
            ctx.load_default_certs()
    else:
        ctx.verify_mode = ssl.CERT_NONE

    return ctx
