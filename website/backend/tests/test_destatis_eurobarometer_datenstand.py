"""Destatis und Eurobarometer: sichtbarer Datenstand statt erfundener Zahlen.

Befund (2026-09-06, die letzten beiden Mittel-Einträge): Beide Packs trugen
durchgehend Werte von 2024, ohne dass irgendetwas darauf hinwies. Ein LLM, das
„Deutschland-Inflation 2024: +2,2 %" liest, kann daraus im September 2026 eine
Aussage über die Gegenwart machen.

**Destatis** — kein maschinenlesbarer Weg zu den 2025er-Jahreswerten: GENESIS
erfordert Registrierung (`helloworld/logincheck` liefert HTML statt JSON), die
Themenseiten sind JS-getrieben und geben nur Fragmente her. Statt Fragmente zu
scrapen und Zahlen zu riskieren, wurden die **offenen Eurostat-Reihen** ergänzt
— ausdrücklich getrennt und mit Messgrößen-Hinweis:

    Bevölkerung 1.1.2026   83.467.117  (gegenüber 83.577.140 am 1.1.2025)
                           → 2025 GESUNKEN, erstmals seit der Zuwanderung 2022
    HVPI 2025              2,3 %       (VPI 2024 war 2,2 %, HVPI 2024 2,5 %)
    BIP real 2025          +0,2 %      (Eurostat führt 2024 mit 0,0 %,
                                        Destatis mit -0,2 % — eine REVISION)
    Geburtenrate 2025      7,8 je 1.000 (nach 8,1)
    Lebenserwartung 2024   M 78,9 / F 83,5

Zwei davon sind echte **Messgrößen-Fallen**: VPI und HVPI sind verschiedene
Indizes (für 2024: 2,2 % gegen 2,5 %), und die Destatis-Sterbetafel 2021/23
mittelt drei Jahre, während Eurostat ein Einzeljahr ausweist. Beide Hinweise
stehen in der Headline, dem ungekürzten Kanal.

**Eurobarometer** — gar nicht aktualisierbar: Die Website ist eine
JavaScript-Anwendung ohne server-gerenderte Inhalte, die dahinterliegende API
antwortet auf alle geprüften Parameter-Varianten mit HTTP 500. Alle sechs
Fakten tragen ihren Erhebungszeitpunkt jetzt im Kopf der Headline.
"""

import json
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
DE = json.loads((BACKEND / "data" / "destatis.json").read_text(encoding="utf-8"))
EB = json.loads((BACKEND / "data" / "eurobarometer.json").read_text(encoding="utf-8"))
DE_FAKTEN = {f["topic"]: f for f in DE["facts"]}

MAX_STR = 400


# --------------------------------------------------------------------------
# Jeder Fakt sagt, wie alt er ist
# --------------------------------------------------------------------------

def test_jeder_destatis_fakt_traegt_seinen_datenstand():
    for f in DE["facts"]:
        assert "datenstand" in f["data"], f["topic"]
        assert "2024" in f["data"]["datenstand"] or "2021/23" in f["data"]["datenstand"]


def test_jeder_eurobarometer_fakt_traegt_seinen_datenstand():
    for f in EB["facts"]:
        assert "datenstand" in f["data"], f["id"]
        assert "Herbst 2024" in f["data"]["datenstand"]
        assert "NICHT enthalten" in f["data"]["datenstand"]


def test_eurobarometer_headlines_beginnen_mit_dem_stand():
    """Der Erhebungszeitpunkt gehört an den ANFANG — nicht ans Ende, wo ihn
    die Prompt-Kürzung als Erstes wegnimmt."""
    for f in EB["facts"]:
        assert f["headline"].startswith("STAND HERBST 2024"), f["id"]
        assert len(f["headline"]) <= MAX_STR


def test_alle_headlines_unter_dem_prompt_budget():
    for f in DE["facts"] + EB["facts"]:
        assert len(f["headline"]) <= MAX_STR, (f.get("id"), len(f["headline"]))


# --------------------------------------------------------------------------
# Die ergänzten Eurostat-Werte
# --------------------------------------------------------------------------

def test_bevoelkerung_ist_2025_gesunken():
    """Die Richtungsaussage: der alte Fakt sprach von Zuwanderung, tatsächlich
    ist die Bevölkerung im Jahr 2025 zurückgegangen."""
    f = DE_FAKTEN["destatis_bevoelkerung"]
    assert "83.467.117" in f["data"]["bevoelkerung_eurostat"]
    assert "GESUNKEN" in f["headline"]


def test_hvpi_und_vpi_sind_als_zwei_groessen_markiert():
    f = DE_FAKTEN["destatis_inflation"]
    assert "MESSGRÖSSE" in f["headline"]
    assert "2,2 %" in f["headline"] and "2,5 %" in f["headline"]
    assert "nicht gegeneinander gerechnet" in f["headline"]
    assert "2,3 %" in f["data"]["hvpi_eurostat"]


def test_sterbetafel_und_einzeljahr_sind_als_zwei_groessen_markiert():
    f = DE_FAKTEN["destatis_lebenserwartung"]
    assert "MESSGRÖSSE" in f["headline"]
    assert "DREI Jahre" in f["headline"] and "EINZELJAHR" in f["headline"]


def test_bip_revision_ist_als_revision_erklaert():
    """Destatis -0,2 % gegen Eurostat 0,0 % für 2024 ist eine Revision, kein
    Widerspruch — ohne den Hinweis liest der Synthesizer einen Konflikt."""
    f = DE_FAKTEN["destatis_bip_wachstum"]
    assert "REVISION" in f["data"]["bip_eurostat"]
    assert "kein Widerspruch" in f["data"]["bip_eurostat"]
    assert "+0,2 %" in f["headline"] or "+0,2 %" in f["data"]["bip_eurostat"]


def test_eurostat_wird_als_eigene_quelle_gekennzeichnet():
    """Zwei Quellen in einem Fakt sind in Ordnung — stillschweigend gemischt
    wären sie ein Befund."""
    for topic in ("destatis_bevoelkerung", "destatis_inflation",
                  "destatis_geburtenrate", "destatis_bip_wachstum",
                  "destatis_lebenserwartung"):
        felder = DE_FAKTEN[topic]["data"]
        eurostat = [v for k, v in felder.items()
                    if isinstance(v, str) and "eurostat" in k.lower()]
        assert eurostat, topic
        assert all("Eurostat-API, abgerufen 2026-09-06" in v for v in eurostat), topic
        assert all("NICHT deckungsgleich" in v for v in eurostat), topic


def test_arbeitslosigkeit_hat_keinen_erfundenen_wert():
    """Die Eurostat-Reihe war nicht abrufbar — dann steht dort auch nichts."""
    f = DE_FAKTEN["destatis_arbeitslosigkeit"]
    assert not any("eurostat" in k.lower() for k in f["data"])
    assert "nicht abrufbar" in f["data"]["datenstand"]


# --------------------------------------------------------------------------
# Was nicht ging, steht mit Grund da
# --------------------------------------------------------------------------

def test_pruefvermerke_nennen_den_grund():
    assert "GENESIS" in DE["letzte_pruefung"]
    assert "Registrierung" in DE["letzte_pruefung"]
    assert "HTTP 500" in EB["letzte_pruefung"]
    assert "JavaScript" in EB["letzte_pruefung"]


def test_quellenlabel_nennen_beide_quellen_bzw_den_stand():
    assert "Eurostat" in DE["source_label"] and "bis 2024" in DE["source_label"]
    assert "Herbst 2024" in EB["source_label"]
    assert "NICHT eingearbeitet" in EB["source_label"]


def test_kadenz_ist_deklariert():
    assert DE["refresh_kadenz"] == "jaehrlich"
    assert EB["refresh_kadenz"] == "jaehrlich"
    for pack in (DE, EB):
        assert len(pack["kadenz_begruendung"]) > 40
