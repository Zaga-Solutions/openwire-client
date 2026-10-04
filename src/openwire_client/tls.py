"""TLS for ssl:// brokers, using only the standard library's ssl module (no extra packages)."""
from __future__ import annotations

import logging
import ssl
from dataclasses import dataclass
from typing import Mapping

from .url import Endpoint

log = logging.getLogger(__name__)


@dataclass
class TlsOptions:
    """How to verify the broker and, if it asks for one, present a client certificate. PEM files only:
    convert a Java keystore once with keytool/openssl (see the README).

    By default the broker's certificate and host name are verified against the system's trusted CAs."""
    ca_file: str | None = None           # extra CA certificate(s) to trust, e.g. a private CA
    cert_file: str | None = None         # client certificate (mutual TLS)
    key_file: str | None = None          # its private key; omit if the key is in cert_file
    key_password: str | None = None      # password of the key, if encrypted
    insecure: bool = False               # do not verify the broker's certificate at all (testing only)
    env_prefix: str | None = None        # set by from_env: lets error hints name the environment variables

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, prefix: str = 'OPENWIRE_TLS_') -> 'TlsOptions':
        """Reads <prefix>CA_FILE, CERT_FILE, KEY_FILE, KEY_PASSWORD and INSECURE (1/true/yes)."""
        if env is None:
            import os
            env = os.environ
        return cls(ca_file=env.get(prefix + 'CA_FILE') or None,
                   cert_file=env.get(prefix + 'CERT_FILE') or None,
                   key_file=env.get(prefix + 'KEY_FILE') or None,
                   key_password=env.get(prefix + 'KEY_PASSWORD') or None,
                   insecure=env.get(prefix + 'INSECURE', '').strip().lower() in ('1', 'true', 'yes'),
                   env_prefix=prefix)

    def name(self, option: str) -> str:
        """How the user refers to an option in a hint: the environment variable if there is one."""
        return f'{self.env_prefix}{option.upper()}' if self.env_prefix else option.lower()


def build_ssl_context(tls: TlsOptions | None = None, verify_hostname: bool = True) -> ssl.SSLContext:
    tls = tls or TlsOptions()
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    # Python 3.13+ is strict about X.509 details and rejects e.g. CA certificates without a key usage extension,
    # which many self-made and older keytool/openssl CAs lack and Java accepts. Chain, validity and host name
    # are still verified; only this extra strictness is switched off.
    context.verify_flags &= ~ssl.VERIFY_X509_STRICT
    if tls.ca_file:
        _load(tls, 'ca_file', lambda: context.load_verify_locations(cafile=tls.ca_file))
    if tls.insecure:
        log.warning('TLS verification is switched off: broker certificates are NOT verified')
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    elif not verify_hostname:
        context.check_hostname = False
    if tls.cert_file:
        _load(tls, 'cert_file', lambda: context.load_cert_chain(tls.cert_file, tls.key_file or None,
                                                                tls.key_password or None))
    return context


def _load(tls: TlsOptions, option: str, load) -> None:
    """Runs a certificate-loading call; unreadable files are reported by name (the ssl module only says
    'No such file or directory')."""
    try:
        load()
    except OSError as e:
        raise OSError(e.errno, f'cannot read {tls.name(option)} {getattr(tls, option)!r}: {e.strerror or e}') from e


def explain_connect_error(endpoint: Endpoint, message: str, tls: TlsOptions | None = None) -> str:
    """Adds a hint to the connection failures that are usually a TLS configuration mistake."""
    tls = tls or TlsOptions()
    if 'CERTIFICATE_VERIFY_FAILED' in message:
        return (f'{message} (To trust a private CA, set {tls.name("ca_file")} to its PEM file; for a name '
                f'mismatch add socket.verifyHostName=false to the URL; {tls.name("insecure")} skips '
                f'verification for testing.)')
    # With TLS 1.3 the broker's refusal of a missing client certificate arrives either as a TLS alert or,
    # depending on timing, as a dropped connection.
    dropped = ('ConnectionAbortedError', 'ConnectionResetError', 'IncompleteReadError')
    if 'BAD_CERTIFICATE' in message or 'CERTIFICATE_REQUIRED' in message or (
            endpoint.tls and any(name in message for name in dropped)):
        return (f'{message} (The broker refused the TLS connection: it may require a client certificate, set '
                f'{tls.name("cert_file")} and {tls.name("key_file")}.)')
    if endpoint.tls and ('TimeoutError' in message or 'no response' in message):
        return f'{message} (No TLS answer: is this really a TLS port? Plain brokers use tcp://.)'
    if not endpoint.tls and 'IncompleteReadError' in message:
        return f'{message} (The broker closed the connection: it may require TLS, try ssl://.)'
    return message
