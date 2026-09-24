"""E-Auto-Lebenszyklus: die Zahlen des ICCT-Berichts von 2025.

Anlass (2026-09-24), neunter Inhaltskonflikt aus #185. Der Fakt berief sich
auf eine ICCT-Veroeffentlichung, die es unter der angegebenen Adresse nicht
gibt, und nannte durchwegs andere Zahlen als der Bericht:

    "Pay-back nach ~30.000-50.000 km"        -> ICCT 2025: rund 17.000 km
    "Akku-Produktion +50-70 % CO2"           -> rund 40 % hoehere Produktion
    "BEV ~80 g/km, Diesel ~210, Benzin ~245" -> 63 / 234 / 235 g CO2e/km
    "BEV ~62 % weniger"                      -> 73 % weniger
    "Lithium-Recyclingquote 70 % bis 2031"   -> 80 % (Anhang XII Teil C)

Nachgelesen im Bericht "Life-cycle greenhouse gas emissions from passenger
cars in the European Union" (ICCT, 8./9. Juli 2025), woertlich: "these
additional emissions are more than offset after about 17,000 km of use in
the first one or two years".

Der Bericht rechnet mit 20 Jahren Nutzung und 12.000 km im Jahr, also
240.000 km — diese Annahme gehoert dazu, weil eine kuerzere Lebensdauer die
Produktionsemissionen je Kilometer rechnerisch aufblaeht.

Keine Netzabfrage in diesen Tests.
"""

import json
import re
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
DATA = BACKEND / "data"
sys.path.insert(0, str(BACKEND))

from services._struct_marker import has_false_verdict_override  # noqa: E402
from services._topic_match import substring_or_composite_match as trifft  # noqa: E402

PACK = json.loads((DATA / "energie_klima_pack.json").read_text(encoding="utf-8"))
F = next(x for x in PACK["facts"] if x.get("id") == "e_auto_akku_co2_2026")
TEXT = F["headline"] + " " + json.dumps(F["data"], ensure_ascii=False)


# --------------------------------------------------------------------------
# Die Lebenszyklus-Werte
# --------------------------------------------------------------------------

@pytest.mark.parametrize("antrieb,wert", [
    ("Benzin", "235"), ("Diesel", "234"), ("Erdgas", "203"), ("Hybrid", "188"),
    ("Wasserstoff aus Erdgas", "175"), ("Plug-in-Hybrid", "163"),
    ("Batterieauto EU-Mix", "63"), ("Batterieauto Ökostrom", "52"),
    ("Wasserstoff erneuerbar", "50"),
])
def test_lebenszyklus_werte(antrieb, wert):
    assert wert in F["data"]["icct_2025_werte"], (antrieb, wert)


def test_73_prozent_sind_nachgerechnet():
    """63 von 235 g sind 73 % weniger — die Zahl muss zur Tabelle passen."""
    assert round((1 - 63 / 235) * 100) == 73
    assert "73 %" in F["headline"]


def test_hybrid_und_plugin_liegen_bei_20_und_30_prozent():
    assert round((1 - 188 / 235) * 100) == 20
    assert round((1 - 163 / 235) * 100) == 31  # Bericht rundet auf 30
    k = F["data"]["kernsatz_fuer_synthesizer"]
    assert "20" in k and "30 %" in k


# --------------------------------------------------------------------------
# Pay-back und Produktion
# --------------------------------------------------------------------------

def test_pay_back_ist_17000_km():
    p = F["data"]["pay_back"]
    assert "17.000" in p
    assert "40 %" in p
    assert "E-Auto" in p, "der Claim spricht von E-Auto, nicht von Batterieauto"


def test_batterieproduktion_mit_quelle_und_spanne():
    b = F["data"]["batterie_produktion"]
    assert "72,8" in b and "53,4" in b and "3,9 t" in b
    assert "52" in b and "80" in b, "Spanne nach Region und Zellchemie"


def test_batterie_haelt_laenger_als_das_auto():
    h = F["data"]["batterie_haltbarkeit"]
    assert "3.000" in h and "5.000" in h
    assert "600.000" in h and "2.000.000" in h
    assert "1 %" in h


def test_lebensdauer_annahme_ist_benannt():
    """Ohne sie ist jede g/km-Zahl beliebig."""
    l = F["data"]["lebensdauer_annahme"]
    assert "20 Jahre" in l and "12.000" in l and "240.000" in l
    assert "15 Jahre" in l and "180.000" in l


# --------------------------------------------------------------------------
# Batterieverordnung
# --------------------------------------------------------------------------

@pytest.mark.parametrize("wert", ["90 %", "50 %", "95 %", "80 %", "2023/1542"])
def test_recyclingquoten(wert):
    assert wert in F["data"]["eu_batterieverordnung"], wert


def test_lithiumquote_ist_80_nicht_70():
    e = F["data"]["eu_batterieverordnung"]
    assert "80 %" in e and "Lithium" in e
    assert "70 %" not in e


# --------------------------------------------------------------------------
# Die alten Zahlen sind weg
# --------------------------------------------------------------------------

@pytest.mark.parametrize("falsch", [
    "30.000-50.000",
    "+50-70 %",
    "~80 g/km",
    "~62 % weniger",
    "120 kg CO2/kWh",
    "nach ~80.000 km",   # 180.000 enthaelt 80.000 — die Pruefung braucht den Kontext
    "70 % Lithium",
    "lifecycle-emissions-roadmap-passenger-vehicles-jul24",
])
def test_unbelegte_angaben_sind_weg(falsch):
    assert falsch not in TEXT, falsch


def test_quelle_ist_der_bericht_von_2025():
    assert "electric-cars-life-cycle-analysis-emissions-europe-jul25" in F["source_url"]
    assert "2025" in F["source_label"]
    assert F["year"] == 2025


# --------------------------------------------------------------------------
# Marker, Trigger, Datenstand
# --------------------------------------------------------------------------

def test_kein_struktureller_falsch_marker():
    """Der Fakt verneint die Gegenthese, bejaht aber ihren Ausgangspunkt
    (hoehere Produktionsemissionen) — kein Verdict-Override."""
    assert not has_false_verdict_override(F["data"]["kernsatz_fuer_synthesizer"])


@pytest.mark.parametrize("phrasing", F["claim_phrasings_handled"])
def test_phrasings_treffen(phrasing):
    assert trifft(F, phrasing.lower()), phrasing


@pytest.mark.parametrize("claim", [
    "Ist das E-Auto wirklich klimafreundlicher?",
    "Wie viel CO2 steckt in einem Autoakku?",
    "Halten E-Auto-Batterien überhaupt lange?",
])
def test_batterie_claims(claim):
    assert trifft(F, claim.lower()), claim


def test_kein_ev_token_mehr():
    """'ev' steckt normalisiert in 'oevp', 'level', 'evidenz' — der Fakt
    wurde damit auf eine Parteienfoerderungs-Frage gezogen."""
    gruppen = F["trigger_composite"]
    assert "ev" not in gruppen[0], gruppen[0]


def test_datenstand_ist_benannt():
    notiz = " ".join(F["context_notes"])
    assert "Datenstand" in notiz and re.search(r"20\d\d", notiz)


def test_prompt_felder_bleiben_unter_der_kuerzung():
    zu_lang = {k: len(v) for k, v in F["data"].items()
               if k != "kernsatz_fuer_synthesizer" and len(v) > 400}
    assert not zu_lang, zu_lang
