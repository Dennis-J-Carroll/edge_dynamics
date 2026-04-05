#!/usr/bin/env python3
"""
Tests for edge_dynamics.security — HMAC signing/verification and TLS contexts.
"""

import hashlib
import hmac
import os
import ssl
import tempfile
import textwrap

import pytest

from edge_dynamics.config import Settings
from edge_dynamics.security import (
    sign_frame,
    verify_frame,
    create_client_context,
    create_server_context,
    _get_hash_func,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# A deterministic 32-byte key (hex-encoded = 64 chars)
TEST_KEY_HEX = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"
TEST_PAYLOAD = b'{"topic":"test","count":5,"raw_len":1024,"comp_len":256}'


def _make_self_signed_cert(tmp_dir: str):
    """Generate a self-signed cert + key pair using openssl CLI (if available)."""
    cert_path = os.path.join(tmp_dir, "server.crt")
    key_path = os.path.join(tmp_dir, "server.key")
    ret = os.system(
        f"openssl req -x509 -newkey rsa:2048 -keyout {key_path} "
        f"-out {cert_path} -days 1 -nodes "
        f'-subj "/CN=localhost" 2>/dev/null'
    )
    if ret != 0:
        pytest.skip("openssl CLI not available")
    return cert_path, key_path


# ---------------------------------------------------------------------------
# HMAC tests
# ---------------------------------------------------------------------------

class TestSignFrame:
    """Tests for sign_frame()."""

    def test_returns_hex_digest(self):
        sig = sign_frame(TEST_PAYLOAD, TEST_KEY_HEX)
        # SHA-256 digest is 64 hex chars
        assert len(sig) == 64
        assert all(c in "0123456789abcdef" for c in sig)

    def test_deterministic(self):
        """Same inputs always produce the same signature."""
        sig1 = sign_frame(TEST_PAYLOAD, TEST_KEY_HEX)
        sig2 = sign_frame(TEST_PAYLOAD, TEST_KEY_HEX)
        assert sig1 == sig2

    def test_different_payloads_different_sigs(self):
        sig_a = sign_frame(b"payload_a", TEST_KEY_HEX)
        sig_b = sign_frame(b"payload_b", TEST_KEY_HEX)
        assert sig_a != sig_b

    def test_different_keys_different_sigs(self):
        key2 = "ff" * 32
        sig1 = sign_frame(TEST_PAYLOAD, TEST_KEY_HEX)
        sig2 = sign_frame(TEST_PAYLOAD, key2)
        assert sig1 != sig2

    @pytest.mark.parametrize("algo,digest_len", [
        ("sha256", 64),
        ("sha384", 96),
        ("sha512", 128),
    ])
    def test_algorithm_variants(self, algo, digest_len):
        sig = sign_frame(TEST_PAYLOAD, TEST_KEY_HEX, algorithm=algo)
        assert len(sig) == digest_len

    def test_matches_stdlib_hmac(self):
        """Verify our output matches a direct hmac.new() call."""
        key_bytes = bytes.fromhex(TEST_KEY_HEX)
        expected = hmac.new(key_bytes, TEST_PAYLOAD, hashlib.sha256).hexdigest()
        assert sign_frame(TEST_PAYLOAD, TEST_KEY_HEX) == expected

    def test_unsupported_algorithm_raises(self):
        with pytest.raises(ValueError, match="Unsupported HMAC algorithm"):
            sign_frame(TEST_PAYLOAD, TEST_KEY_HEX, algorithm="md5")


class TestVerifyFrame:
    """Tests for verify_frame()."""

    def test_valid_signature(self):
        sig = sign_frame(TEST_PAYLOAD, TEST_KEY_HEX)
        assert verify_frame(TEST_PAYLOAD, sig, TEST_KEY_HEX) is True

    def test_invalid_signature(self):
        assert verify_frame(TEST_PAYLOAD, "bad" * 21 + "b", TEST_KEY_HEX) is False

    def test_tampered_payload(self):
        sig = sign_frame(TEST_PAYLOAD, TEST_KEY_HEX)
        tampered = TEST_PAYLOAD + b"x"
        assert verify_frame(tampered, sig, TEST_KEY_HEX) is False

    def test_wrong_key(self):
        sig = sign_frame(TEST_PAYLOAD, TEST_KEY_HEX)
        wrong_key = "ff" * 32
        assert verify_frame(TEST_PAYLOAD, sig, wrong_key) is False

    def test_wrong_algorithm(self):
        sig = sign_frame(TEST_PAYLOAD, TEST_KEY_HEX, algorithm="sha256")
        assert verify_frame(TEST_PAYLOAD, sig, TEST_KEY_HEX, algorithm="sha512") is False

    @pytest.mark.parametrize("algo", ["sha256", "sha384", "sha512"])
    def test_roundtrip_all_algorithms(self, algo):
        sig = sign_frame(TEST_PAYLOAD, TEST_KEY_HEX, algorithm=algo)
        assert verify_frame(TEST_PAYLOAD, sig, TEST_KEY_HEX, algorithm=algo) is True

    def test_empty_payload(self):
        sig = sign_frame(b"", TEST_KEY_HEX)
        assert verify_frame(b"", sig, TEST_KEY_HEX) is True

    def test_large_payload(self):
        big = os.urandom(1024 * 1024)  # 1 MB
        sig = sign_frame(big, TEST_KEY_HEX)
        assert verify_frame(big, sig, TEST_KEY_HEX) is True


class TestGetHashFunc:
    """Tests for _get_hash_func() helper."""

    def test_sha256(self):
        assert _get_hash_func("sha256") is hashlib.sha256

    def test_sha384(self):
        assert _get_hash_func("sha384") is hashlib.sha384

    def test_sha512(self):
        assert _get_hash_func("sha512") is hashlib.sha512

    def test_invalid(self):
        with pytest.raises(ValueError, match="Unsupported"):
            _get_hash_func("blake2b")


# ---------------------------------------------------------------------------
# TLS context tests
# ---------------------------------------------------------------------------

class TestCreateClientContext:
    """Tests for create_client_context()."""

    def test_returns_ssl_context(self):
        settings = Settings(tls_enabled=True, tls_check_hostname=False)
        ctx = create_client_context(settings)
        assert isinstance(ctx, ssl.SSLContext)

    def test_check_hostname_respected(self):
        settings = Settings(tls_enabled=True, tls_check_hostname=True)
        ctx = create_client_context(settings)
        assert ctx.check_hostname is True

        settings2 = Settings(tls_enabled=True, tls_check_hostname=False)
        ctx2 = create_client_context(settings2)
        assert ctx2.check_hostname is False

    def test_ca_file_loaded(self):
        """When tls_ca_file is provided, it should be loadable."""
        with tempfile.TemporaryDirectory() as tmp:
            cert_path, key_path = _make_self_signed_cert(tmp)
            settings = Settings(
                tls_enabled=True,
                tls_ca_file=cert_path,
                tls_check_hostname=False,
            )
            ctx = create_client_context(settings)
            assert isinstance(ctx, ssl.SSLContext)

    def test_client_cert_for_mtls(self):
        """Client cert + key should load without error."""
        with tempfile.TemporaryDirectory() as tmp:
            cert_path, key_path = _make_self_signed_cert(tmp)
            settings = Settings(
                tls_enabled=True,
                tls_cert_file=cert_path,
                tls_key_file=key_path,
                tls_check_hostname=False,
            )
            ctx = create_client_context(settings)
            assert isinstance(ctx, ssl.SSLContext)


class TestCreateServerContext:
    """Tests for create_server_context()."""

    def test_requires_cert_and_key(self):
        """Should raise if cert/key not provided."""
        settings = Settings(tls_enabled=True)
        with pytest.raises(ValueError, match="tls_cert_file and tls_key_file"):
            create_server_context(settings)

    def test_returns_ssl_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            cert_path, key_path = _make_self_signed_cert(tmp)
            settings = Settings(
                tls_enabled=True,
                tls_cert_file=cert_path,
                tls_key_file=key_path,
            )
            ctx = create_server_context(settings)
            assert isinstance(ctx, ssl.SSLContext)

    def test_verify_mode_none_by_default(self):
        """Without tls_verify_client, server should not require client certs."""
        with tempfile.TemporaryDirectory() as tmp:
            cert_path, key_path = _make_self_signed_cert(tmp)
            settings = Settings(
                tls_enabled=True,
                tls_cert_file=cert_path,
                tls_key_file=key_path,
                tls_verify_client=False,
            )
            ctx = create_server_context(settings)
            assert ctx.verify_mode == ssl.CERT_NONE

    def test_mutual_tls(self):
        """With tls_verify_client, server should require client certs."""
        with tempfile.TemporaryDirectory() as tmp:
            cert_path, key_path = _make_self_signed_cert(tmp)
            settings = Settings(
                tls_enabled=True,
                tls_cert_file=cert_path,
                tls_key_file=key_path,
                tls_ca_file=cert_path,  # self-signed = own CA
                tls_verify_client=True,
            )
            ctx = create_server_context(settings)
            assert ctx.verify_mode == ssl.CERT_REQUIRED


# ---------------------------------------------------------------------------
# Config integration tests
# ---------------------------------------------------------------------------

class TestSecurityConfig:
    """Verify Settings validation for security fields."""

    def test_defaults_disabled(self):
        s = Settings()
        assert s.auth_enabled is False
        assert s.tls_enabled is False
        assert s.hmac_algorithm == "sha256"

    def test_auth_enabled_requires_key(self):
        with pytest.raises(ValueError, match="Secret key is required"):
            Settings(auth_enabled=True, auth_secret_key=None)

    def test_auth_enabled_with_key(self):
        s = Settings(auth_enabled=True, auth_secret_key=TEST_KEY_HEX)
        assert s.auth_secret_key == TEST_KEY_HEX

    def test_invalid_hmac_algorithm(self):
        with pytest.raises(ValueError, match="HMAC algorithm"):
            Settings(hmac_algorithm="md5")

    def test_tls_cert_path_validation(self):
        with pytest.raises(ValueError, match="TLS file path does not exist"):
            Settings(tls_cert_file="/nonexistent/cert.pem")

    def test_env_override(self):
        os.environ["EDGE_AUTH_ENABLED"] = "true"
        os.environ["EDGE_AUTH_SECRET_KEY"] = TEST_KEY_HEX
        os.environ["EDGE_HMAC_ALGORITHM"] = "sha512"
        try:
            s = Settings()
            assert s.auth_enabled is True
            assert s.auth_secret_key == TEST_KEY_HEX
            assert s.hmac_algorithm == "sha512"
        finally:
            del os.environ["EDGE_AUTH_ENABLED"]
            del os.environ["EDGE_AUTH_SECRET_KEY"]
            del os.environ["EDGE_HMAC_ALGORITHM"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
