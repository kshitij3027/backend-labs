"""Tests for TLS context factories."""

import ssl
import pytest
from src.tls_context import create_server_context, create_client_context_unverified


class TestClientContextUnverified:
    def test_creates_ssl_context(self):
        ctx = create_client_context_unverified()
        assert isinstance(ctx, ssl.SSLContext)

    def test_no_hostname_check(self):
        ctx = create_client_context_unverified()
        assert ctx.check_hostname is False

    def test_no_cert_verification(self):
        ctx = create_client_context_unverified()
        assert ctx.verify_mode == ssl.CERT_NONE


class TestServerContext:
    def test_missing_cert_raises(self):
        with pytest.raises((ssl.SSLError, FileNotFoundError)):
            create_server_context("/nonexistent/cert.crt", "/nonexistent/key.key")
