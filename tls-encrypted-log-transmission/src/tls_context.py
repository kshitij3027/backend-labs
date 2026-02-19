"""SSLContext factory functions for server and client."""

import ssl


def create_server_context(cert_file: str, key_file: str) -> ssl.SSLContext:
    """Create an SSL context for the TLS server."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=cert_file, keyfile=key_file)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


def create_client_context_unverified() -> ssl.SSLContext:
    """Create an SSL context that skips certificate verification (dev use)."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx
