"""Helpers for storing and logging source URLs safely."""

from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


SENSITIVE_QUERY_KEYS = {
    "authkey",
    "key",
    "apikey",
    "api_key",
    "token",
    "access_token",
    "sig",
    "signature",
}


def redact_sensitive_query_params(url: str | None) -> str | None:
    """Return a URL with known sensitive query parameters redacted."""

    if not url:
        return url

    parsed = urlparse(url)
    if not parsed.query:
        return url

    redacted_items = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() in SENSITIVE_QUERY_KEYS and value:
            redacted_items.append((key, "***"))
        else:
            redacted_items.append((key, value))

    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(redacted_items), parsed.fragment))
