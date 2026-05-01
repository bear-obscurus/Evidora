"""Central HTTP-politeness helper for outbound requests.

Provides a single, identifiable User-Agent string and a small factory
for ``httpx.AsyncClient`` instances pre-configured with sensible
defaults. New services should use this instead of the bare httpx
constructors so we identify ourselves consistently to upstream APIs and
RSS feeds.

Why this exists (Open-Source-Compliance audit, 2026-05-01):
  * NCBI/PubMed and OpenAlex explicitly favour identified clients
    ("polite pool") with higher rate limits or lower priority deflation.
  * Several services historically masqueraded as a desktop Safari to get
    past bot blocks (mimikama, volksbegehren, at_faktencheck_rss). RSS
    feeds and Open-Government data don't need that — a clean
    ``Evidora/1.0`` header is both more honest and gets the same data.
  * Anonymous ``python-httpx/x.y.z`` is not illegal, but is not great
    citizenship either.

Migration path: services can adopt the helper incrementally — passing
``headers=DEFAULT_HEADERS`` to existing httpx calls is enough; using
``polite_client()`` is the clean version.
"""
from __future__ import annotations

import httpx

USER_AGENT = (
    "Evidora/1.0 (+https://evidora.eu; mailto:Evidora@proton.me)"
)

DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
}


def polite_client(timeout: float = 30.0, **kwargs) -> httpx.AsyncClient:
    """Return an ``httpx.AsyncClient`` pre-configured with the polite
    User-Agent and reasonable defaults.

    Pass extra ``httpx.AsyncClient`` kwargs as needed; they win over the
    defaults if there is a conflict.
    """
    headers = dict(DEFAULT_HEADERS)
    headers.update(kwargs.pop("headers", {}) or {})
    return httpx.AsyncClient(timeout=timeout, headers=headers, **kwargs)
