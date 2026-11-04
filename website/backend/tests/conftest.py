"""Shared fixtures for Evidora tests."""

import os
import sys

import pytest

# Allow imports from the backend root (services.*)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BACKEND_URL = os.getenv("EVIDORA_TEST_URL", "http://localhost:8000")


@pytest.fixture(autouse=True)
def _schattenprotokoll_im_tmp(tmp_path, monkeypatch):
    """Kein Test schreibt ins echte data/_wartung/strukturell_schatten.jsonl.

    Jeder Aufruf von apply_verdict_postprocessing mit einem STRUKTURELL-
    Marker haengt dort eine Zeile an (Schattenbetrieb, 4.11.2026).
    """
    try:
        import services.verdict_postprocess as vp
    except Exception:
        return
    monkeypatch.setattr(vp, "SCHATTEN_PROTOKOLL",
                        str(tmp_path / "_wartung" / "strukturell_schatten.jsonl"))


@pytest.fixture
def backend_url():
    """Base URL for the running backend."""
    return BACKEND_URL


# Reusable analysis dicts for unit tests
@pytest.fixture
def analysis_austria_renewables():
    return {
        "claim": "Österreich hat den höchsten Anteil erneuerbarer Energien in der EU",
        "category": "energy",
        "subcategory": "renewable_energy",
        "entities": ["Österreich", "erneuerbare Energien"],
        "eurostat_relevant": True,
        "eea_relevant": True,
        "pubmed_queries": [],
        "factcheck_queries": [],
        "confidence": 0.9,
    }


@pytest.fixture
def analysis_spain_unemployment():
    return {
        "claim": "Die Arbeitslosigkeit in Spanien liegt unter dem EU-Durchschnitt",
        "category": "economy",
        "subcategory": "unemployment",
        "entities": ["Spanien", "Arbeitslosigkeit"],
        "eurostat_relevant": True,
        "pubmed_queries": [],
        "factcheck_queries": [],
        "confidence": 0.9,
    }
