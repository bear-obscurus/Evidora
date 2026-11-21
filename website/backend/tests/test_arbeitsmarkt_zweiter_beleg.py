"""Ein zweiter Beleg im Arbeitsmarkt-Pack trägt seine eigene URL.

Live am 21.11.2026 (#322, Prod ``b5a7356``) lieferte die Reallohn-Frage
zwei Belege: „Arbeitsmarkt-Konsens“ und „EU-Kommission (DG EMPL)“. Beide
zeigten auf dieselbe WIFO-Tabelle.

Die Ursache liegt an zwei Stellen:

1. ``search_arbeitsmarkt`` reichte nur ``source_url`` und ``secondary_url``
   weiter. ``source_url_2`` las kein Dienst.
2. Der Synthesizer zeigt dem Modell pro Treffer nur das Feld ``url``
   (``secondary_url`` steht nicht in seiner Feldliste). Nur diese URLs
   überstehen den Halluzinations-Filter (``real_urls``). Hat ein Fakt eine
   einzige URL, hängt das Modell jede Quelle, die es aus dem Text kennt, an
   genau diese URL.

Behoben im Pack, nicht im Synthesizer: Ein Fakt mit ``source_url_2`` UND
``source_label_2`` ergibt einen zweiten Treffer mit eigener ``url``, eigenem
``source`` und dem Datenfeld ``source_2_feld`` als Inhalt. Den Synthesizer
habe ich nicht angefasst: ``secondary_url`` tragen 660 Fakten in 62 Dateien,
und sie dem Modell zu zeigen, verändert jede Synthese. Das wäre eine eigene
Messung.

``source_url_2`` tragen nur zwei Fakten, beide in diesem Pack. Der zweite,
``scheinselbststaendigkeit_2026``, hat als zweite URL die Startseite des
BAG, die neben dem verlinkten BAG-Urteil nichts belegt. Er bekommt bewusst
keinen ``source_label_2`` und damit keinen zweiten Treffer. Das pinnt der
Test unten.

Modellfrei: kein Netz, kein SpaCy, kein sentence-transformers.
"""

import asyncio
import json
import os
import sys

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

from services.arbeitsmarkt_pack import search_arbeitsmarkt  # noqa: E402

WIFO = "https://www.wifo.ac.at/wp-content/uploads/wtabellen/d/15.6_d_out.pdf"
EU = "https://op.europa.eu/webpub/empl/lmwd-annual-review-report-2025/chapter2/wage-developments-and-outlook.html"
CLAIM = "Sind unsere Reallöhne besser als im Rest der EU?"


def _suche(claim: str) -> list[dict]:
    return asyncio.run(search_arbeitsmarkt({"original_claim": claim}))["results"]


def _fakt(fid: str) -> dict:
    with open(os.path.join(BACKEND, "data", "arbeitsmarkt_pack.json"), encoding="utf-8") as fh:
        return next(f for f in json.load(fh)["facts"] if f["id"] == fid)


def test_eu_beleg_hat_eigene_url():
    res = _suche(CLAIM)
    eu = [r for r in res if r["url"] == EU]
    assert len(eu) == 1, [(r["source"], r["url"]) for r in res]
    assert "EU-Kommission" in eu[0]["source"]
    assert "2,7 %" in eu[0]["display_value"] and "2019" in eu[0]["display_value"]


def test_wifo_beleg_behaelt_wifo_url():
    res = _suche(CLAIM)
    wifo = [r for r in res if r["url"] == WIFO]
    assert len(wifo) == 1
    assert "EU-Kommission" not in wifo[0]["source"]


def test_eu_url_uebersteht_den_halluzinations_filter():
    """Nachgebaut wie ``synthesizer.py``: real_urls kommt nur aus ``url``."""
    real_urls = {r["url"] for r in _suche(CLAIM) if r.get("url")}
    assert EU in real_urls


def test_jede_quelle_eine_eigene_url():
    res = _suche(CLAIM)
    paare = {(r["source"], r["url"]) for r in res}
    urls = [u for _s, u in paare]
    assert len(urls) == len(set(urls)), paare


def test_zweiter_treffer_erbt_die_zuordnung():
    """Der zweite Treffer stammt aus demselben exakten Match. Er darf nicht als
    Cosine-Treffer gelten (``_matched_exact``)."""
    for r in _suche(CLAIM):
        assert r.get("_matched_exact") is True, r["source"]


def test_zweiter_treffer_steht_hinter_den_fakten():
    """Kommt mehr als ein Fakt zurück, darf der Zusatzbeleg keinen Fakt aus
    dem Per-Source-Cap (3) drängen. Darum steht er am Ende."""
    res = _suche(CLAIM)
    idx_eu = next(i for i, r in enumerate(res) if r["url"] == EU)
    assert all(r["url"] != EU for r in res[:idx_eu])
    assert idx_eu == len(res) - 1


def test_scheinselbststaendigkeit_ohne_zweiten_treffer():
    f = _fakt("scheinselbststaendigkeit_2026")
    assert f.get("source_url_2"), "Fakt hat kein source_url_2 mehr, Test anpassen"
    assert not f.get("source_label_2")
    res = _suche(f["claim_phrasings_handled"][0])
    assert all(r["url"] != f["source_url_2"] for r in res)


def test_ohne_treffer_bleibt_es_leer():
    assert _suche("Die Mondlandung war echt") == []
