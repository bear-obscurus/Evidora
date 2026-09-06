"""OeNB: der Leitzins ist gestiegen, das Pack behauptete das Gegenteil.

Befund (2026-09-06, dritter Mittel-Eintrag): `oenb.json` trug den Stand
April 2026 und beschrieb einen „Senkungszyklus … danach Pause". Gegen die
EZB-SDMX-Reihe geprüft:

    Senkungen bis 2,15 % am 11.06.2025, dann ein Jahr Pause —
    und am 17.06.2026 die Wende nach OBEN auf 2,40 %.

Der Datensatz sagte 2,15 % und „sechs Sitzungen unverändert". Ein Claim wie
„Die EZB senkt weiter die Zinsen" hätte darin Bestätigung gefunden.

Zweiter Richtungsfehler in derselben Datei: die OeNB-Inflationsprognose stand
mit 2,1 % für 2026 — also Richtung EZB-Ziel. Die Juni-Prognose 2026 (OeNB
Report 2026/17 vom 12.06.2026) nennt **3,2 %**, unter dem Titel „Krieg im
Nahen und Mittleren Osten führt zu deutlich erhöhter Inflation". Der Wert
deckt sich mit WIFO/IHS aus #131 (3,2 / 3,0) — zwei unabhängige Quellen im
Bestand stimmen überein.

**Dubletten entfernt statt aufgefrischt**: EZB-Leitzins und Wechselkurse
liefert der Live-Connector `services/ecb.py` aus derselben EZB-SDMX-Reihe.
Der hier gespeicherte EUR/USD-Kurs von 1,08 lag am 2026-09-04 um 7,6 %
daneben (tatsächlich 1,1622) — bei einem täglich schwankenden Kurs ist eine
statische Kopie strukturell falsch. Die Kurse sind raus; der Leitzins bleibt
als DATIERTER Rückfall, falls die Reihe nicht erreichbar ist.

**Retrieval-Lücken**, alle drei live nachgestellt: „Die EZB senkt weiter die
Zinsen" traf nicht (der Trigger verlangte wörtlich „leitzins"), AT-Inflation
gar nicht, und „zurück zum Schilling" scheiterte an der festen Wendung
„schilling zurück". Ausserdem war `oenb` ein vierter Service mit eigenem
Prädikat ohne Schreibweisen-Normalisierung (#143/#144) — „Teuerung in
Oesterreich" traf nicht.
"""

import asyncio
import ast
import json
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
DATEN = json.loads((BACKEND / "data" / "oenb.json").read_text(encoding="utf-8"))
D = DATEN["data"]
QUELLE = (BACKEND / "services" / "oenb.py").read_text(encoding="utf-8")


def _nur_code(q: str) -> str:
    baum = ast.parse(q)
    code = q
    for k in ast.walk(baum):
        if isinstance(k, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            doc = ast.get_docstring(k, clean=False)
            if doc:
                code = code.replace(doc, "")
    return "\n".join(z for z in code.splitlines() if not z.lstrip().startswith("#"))


CODE = _nur_code(QUELLE)

from services.oenb import claim_mentions_oenb_cached, search_oenb  # noqa: E402


def _treffer(claim):
    return asyncio.run(search_oenb({"claim": claim,
                                    "original_claim": claim}))["results"]


def _haupt(claim):
    return next(x for x in _treffer(claim) if x["indicator"] == "oenb_main")


# --------------------------------------------------------------------------
# Der Leitzins ist GESTIEGEN
# --------------------------------------------------------------------------

def test_leitzins_ist_aktuell():
    assert D["ezb_leitzins_pct"] == 2.40
    assert D["ezb_einlagesatz_pct"] == 2.25
    assert D["ezb_spitzenrefinanzierungssatz_pct"] == 2.65


def test_die_letzte_aenderung_war_eine_erhoehung():
    """Der Richtungsfehler: das Pack sprach von einem Senkungszyklus."""
    text = D["ezb_letzte_aenderung"] + " " + D["ezb_zinszyklus"]
    assert "17. Juni 2026" in text
    assert "ERHÖHUNG" in text or "OBEN" in text
    assert "2,15" in text and "2,40" in text


def test_ueberholter_stand_wird_ausdruecklich_benannt():
    assert "ueberholten Stand" in D["ezb_zinszyklus"] or "überholten Stand" in D["ezb_zinszyklus"]


def test_abstand_der_saetze_stimmt():
    """Spitzenrefinanzierung liegt 25 Basispunkte über dem Hauptsatz, der
    Einlagesatz 15 darunter — eine Gegenprobe gegen Zahlendreher."""
    assert round(D["ezb_spitzenrefinanzierungssatz_pct"] - D["ezb_leitzins_pct"], 2) == 0.25
    assert round(D["ezb_leitzins_pct"] - D["ezb_einlagesatz_pct"], 2) == 0.15


# --------------------------------------------------------------------------
# Die OeNB-Prognose vom Juni 2026
# --------------------------------------------------------------------------

def test_inflationsprognose_steigt_statt_zu_fallen():
    assert D["oenb_hvpi_prognose_2026_pct"] == 3.2
    assert D["oenb_hvpi_prognose_2027_pct"] == 2.4
    assert "STEIGT" in D["richtung"]
    assert "3,2" in D["richtung"]


def test_bip_und_budget_stimmen_mit_dem_report():
    assert D["oenb_bip_prognose_2026_pct"] == 0.6
    assert D["oenb_bip_prognose_2027_pct"] == 1.1
    assert D["oenb_bip_prognose_2028_pct"] == 1.2
    assert D["oenb_budgetsaldo_2026_pct_bip"] == -4.1
    assert D["oenb_arbeitslosenquote_ams_2026_pct"] == 7.4


def test_quelle_nennt_report_und_datum():
    q = D["oenb_prognose_quelle"]
    assert "2026/17" in q and "12. Juni 2026" in q
    assert "Nahen und Mittleren Osten" in D["oenb_prognose_titel"]


def test_revision_ist_dokumentiert():
    """März 2026 sagte 2,7 %, Juni 3,2 % — die Revision selbst ist die
    Nachricht."""
    r = D["oenb_revision_maerz_auf_juni"]
    assert "2,7" in r and "3,2" in r


def test_deckt_sich_mit_wifo_ihs():
    """Zwei unabhängige Quellen im Bestand: WIFO/IHS (#131) nennt 3,2/3,0 für
    2026, die OeNB 3,2. Ein Auseinanderdriften wäre ein Befund."""
    wifo = json.loads((BACKEND / "data" / "wifo_ihs.json").read_text(encoding="utf-8"))["data"]
    assert abs(wifo["inflation_2026_pct_wifo"] - D["oenb_hvpi_prognose_2026_pct"]) <= 0.3


# --------------------------------------------------------------------------
# Dubletten zur Live-Quelle
# --------------------------------------------------------------------------

def test_wechselkurse_sind_entfernt():
    """Täglich schwankende Kurse gehören nicht in eine statische Datei — der
    Live-Connector services/ecb.py liefert sie aus derselben EZB-Reihe."""
    for feld in ("wechselkurs_eur_usd_aktuell_approx",
                 "wechselkurs_eur_chf_aktuell_approx"):
        assert feld not in D, f"{feld} ist zurück"
    assert any("1,08" in h and "1,1622" in h for h in D["wichtige_hinweise"]), \
        "der Grund der Entfernung muss belegt dastehen"


def test_leitzins_verweist_auf_die_live_quelle():
    assert "services/ecb.py" in D["ezb_quelle_live"]
    assert "Rueckfall" in D["ezb_quelle_live"] or "Rückfall" in D["ezb_quelle_live"]


# --------------------------------------------------------------------------
# Renderer
# --------------------------------------------------------------------------

def test_kein_stand_mehr_im_code_verdrahtet():
    for verdrahtet in ("April 2026", '"2026"', "aktuell_pct"):
        assert verdrahtet not in CODE, f"{verdrahtet!r} steht im Code"
    assert 'data.get("stand_iso"' in CODE


def test_ausgabe_nennt_stand_und_richtung():
    x = _haupt("Wie hoch ist der EZB-Leitzins?")
    assert "2026-09-06" in x["display_value"]
    assert "2,4 %" in x["display_value"]
    assert "ERHÖHUNG" in x["display_value"]
    assert x["year"] == "2026"


def test_keine_platzhalter():
    for claim in ("Wie hoch ist der EZB-Leitzins?", "Die Inflation in Österreich"):
        x = _haupt(claim)
        for bad in ("None", "{", "}"):
            assert bad not in x["display_value"], f"{bad!r} in der Ausgabe"


# --------------------------------------------------------------------------
# Retrieval-Lücken
# --------------------------------------------------------------------------

def test_die_drei_geschlossenen_luecken():
    for claim in ("Die EZB senkt weiter die Zinsen",       # kein Wort "Leitzins"
                  "Die Inflation in Österreich geht zurück",  # AT-Inflation
                  "Österreich sollte zurück zum Schilling"):  # Wortstellung
        assert claim_mentions_oenb_cached(claim), claim
        assert _treffer(claim), claim


def test_schilling_counter_greift_bei_freier_wortstellung():
    r = _treffer("Österreich sollte zurück zum Schilling")
    assert any(x["indicator"] == "oenb_euro_austritt_counter" for x in r)


def test_ascii_schreibweise_trifft():
    """oenb war ein vierter Service mit eigenem Prädikat ohne die
    Normalisierung aus #143/#144."""
    for claim in ("Die Teuerung in Oesterreich sinkt",
                  "Oesterreich sollte zurueck zum Schilling"):
        assert claim_mentions_oenb_cached(claim), claim


def test_kein_ueber_trigger():
    for claim in ("Die Inflation in Deutschland sinkt",
                  "Wie viele Einwohner hat Wien?",
                  "Der Schilling war eine schöne Währung",
                  "Die Zinsen für mein Sparbuch sind niedrig"):
        assert not claim_mentions_oenb_cached(claim), claim


def test_kadenz_ist_deklariert():
    assert DATEN["refresh_kadenz"] == "quartalsweise"
    assert "Interimsprognose" in DATEN["kadenz_begruendung"]
