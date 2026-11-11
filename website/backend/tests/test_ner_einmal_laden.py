"""SpaCy wird einmal geladen — nicht bei jedem Healthcheck.

Bis 11.11.2026 lud services/ner._load_models() beide Modelle bei JEDEM
Aufruf neu; der Docstring versprach "once (lazy singleton)", ein Guard
fehlte. Der Container-Healthcheck ruft /api/health/full alle 30 s, und der
ruft _load_models(). Gemessen auf Prod: 106 Neuladungen in einer Stunde,
/api/health/full 3,5 s, und weil das Laden synchron in einem async-Endpunkt
lief, wartete jede andere Anfrage mit (/api/legal 3,0 s statt 0,003 s).

Kein echtes SpaCy in diesen Tests (die CI hat es nicht): ein gefaelschtes
Modul zaehlt die load-Aufrufe.
"""

import logging
import sys
import types
from pathlib import Path

import pytest

from services import ner

BACKEND = Path(__file__).resolve().parents[1]


class _FalschesSpacy(types.ModuleType):
    def __init__(self, ohne_lg=False, kaputt=False):
        super().__init__("spacy")
        self.aufrufe = []
        self.ohne_lg = ohne_lg
        self.kaputt = kaputt

    def load(self, name):
        self.aufrufe.append(name)
        if self.kaputt:
            raise RuntimeError("absichtlich kaputt")
        if self.ohne_lg and name == "de_core_news_lg":
            raise OSError("lg nicht installiert")
        return object()


@pytest.fixture
def frisch(monkeypatch):
    """Modul-Zustand wie nach einem Neustart: nichts geladen, nichts gescheitert."""
    monkeypatch.setattr(ner, "_nlp_de", None)
    monkeypatch.setattr(ner, "_nlp_en", None)
    monkeypatch.setattr(ner, "_load_failed", False)

    def mit(spacy):
        monkeypatch.setitem(sys.modules, "spacy", spacy)
        return spacy
    return mit


def test_der_zweite_aufruf_laedt_nichts_neu(frisch):
    spacy = frisch(_FalschesSpacy())
    for _ in range(5):              # fuenf Healthchecks hintereinander
        ner._load_models()
    assert spacy.aufrufe == ["de_core_news_lg", "en_core_web_sm"], (
        "jeder Aufruf lud die Modelle neu — das war der Healthcheck-Stau"
    )
    assert ner._nlp_de is not None and ner._nlp_en is not None


def test_auch_der_fallback_auf_sm_laedt_nur_einmal(frisch):
    spacy = frisch(_FalschesSpacy(ohne_lg=True))
    for _ in range(3):
        ner._load_models()
    assert spacy.aufrufe == ["de_core_news_lg", "de_core_news_sm", "en_core_web_sm"]


def test_ein_gescheiterter_start_wird_nicht_wiederholt(frisch):
    spacy = frisch(_FalschesSpacy(kaputt=True))
    ner._load_models()
    ner._load_models()
    assert ner._load_failed is True
    assert spacy.aufrufe == ["de_core_news_lg"]


def test_die_log_zeile_erscheint_einmal(frisch, caplog):
    """Genau diese Zeile wurde auf Prod gezaehlt: ~2.660 Mal am Tag."""
    frisch(_FalschesSpacy())
    with caplog.at_level(logging.INFO, logger="evidora"):
        for _ in range(4):
            ner._load_models()
    zeilen = [r for r in caplog.records if r.getMessage() == "SpaCy NER models loaded"]
    assert len(zeilen) == 1


def test_der_healthcheck_prueft_weiter_ueber_den_lader():
    """Der Fix darf die Pruefung nicht abschaffen: health/full ruft den Lader
    weiter auf (beim ersten Mal laedt er, danach ist es billig) und meldet
    beide Modelle."""
    q = (BACKEND / "main.py").read_text(encoding="utf-8")
    i = q.index("async def health_full")
    rumpf = q[i:i + 3000]
    assert "_ner._load_models()" in rumpf
    assert 'checks["spacy_de"] = _ner._nlp_de is not None' in rumpf
    assert 'checks["spacy_en"] = _ner._nlp_en is not None' in rumpf
