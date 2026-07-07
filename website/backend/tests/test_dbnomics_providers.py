"""Contract-Test für DBnomics-Provider (Audit 2026-07-07).

Zwei Befunde des Multi-Connector-Audits:
- _PROVIDER_HINTS enthielt 'SNB' + 'WIFO' — Provider-Codes, die in der
  DBnomics-Registry NICHT existieren → der Exact-Match-Filter lieferte
  garantiert 0 Ergebnisse.
- Der Provider-Filter in _search_api verwarf ALLE Docs, wenn der genannte
  Provider nicht in den Top-10 der globalen /search lag (Normalfall) →
  IMF/WB/BIS/BOE/BOJ/INSEE-Claims bekamen still 0 DBnomics-Ergebnisse.

Dependency-light: importiert nur services.dbnomics, kein Netz.
"""
import inspect

from services.dbnomics import _PROVIDER_HINTS
import services.dbnomics as dbn


def test_no_phantom_provider_codes():
    """SNB + WIFO existieren nicht in DBnomics — dürfen nicht mehr geroutet
    werden (sonst garantierte Leer-Ergebnisse)."""
    codes = set(_PROVIDER_HINTS.values())
    assert "SNB" not in codes, "Provider 'SNB' existiert nicht in DBnomics"
    assert "WIFO" not in codes, "Provider 'WIFO' existiert nicht in DBnomics"
    # Die echten Provider bleiben erhalten
    assert {"IMF", "WB", "OECD", "BIS", "FED", "CEPII"} <= codes


def test_provider_filter_is_non_destructive():
    """Der Provider-Filter in _search_api darf nicht mehr auf [] leeren, wenn
    der Provider fehlt — er muss auf die on-topic Docs zurückfallen
    (Muster 'matched or docs')."""
    src = inspect.getsource(dbn._search_api)
    assert "matched or docs" in src, \
        "_search_api leert wieder destruktiv statt 'matched or docs' zu nutzen"
    # der alte destruktive Drop (docs = [...] direkt) darf nicht zurückkehren
    assert "docs = [d for d in docs if" not in src
