"""WIFO/IHS-Konjunkturprognose: Runde 2/2026 und drei korrigierte Richtungen.

Befund (2026-09-05, aus dem Freshness-Rückstand): Die Datei trug die Runde
**1/2026 (März 2026)** — und der Service hatte diese Bezeichnung zusätzlich
FEST IM CODE. Ein reiner Daten-Refresh hätte die falsche Runde weiter
angezeigt.

Inhaltlich waren drei Aussagen nicht bloß alt, sondern richtungsverkehrt.
Die Runde 2/2026 (Juni 2026, WKO-Zusammenfassung, PDF-Text extrahiert):

  BIP 2025      alt: -0,3 % (WIFO) / -0,4 % (IHS)  ->  IST **+0,8 %**
                d. h. aus dem „dritten schwachen Jahr" wurde ein Erholungsjahr
  Inflation 26  alt: 2,6 / 2,5 (Richtung EZB-Ziel) ->  **3,2 / 3,0**, deutlich DARÜBER
  Arbeitslose   alt: 7,5 -> 7,3 (fallend)          ->  7,4 -> **7,5** (steigend)

Ein Claim wie „Österreich steckt in der Rezession" hätte mit den alten Daten
Bestätigung gefunden. Deshalb stehen die Richtungen als fertige Sätze in der
Headline, nicht nur als Zahlen im data-Dict.

Bewusst NICHT übernommen: Inflation 2025 und 2027 sowie die Eurostat-Quote —
die Runde 2/2026 weist sie nicht aus, und Zeitreihen werden nur aus EINER
Vintage gezogen (V-Dem-Lehre), nie gemischt.
"""

import asyncio
import json
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
DATEN = json.loads((BACKEND / "data" / "wifo_ihs.json").read_text(encoding="utf-8"))
D = DATEN["data"]
QUELLE = (BACKEND / "services" / "wifo_ihs.py").read_text(encoding="utf-8")

from services.wifo_ihs import search_wifo_ihs  # noqa: E402


def _ergebnis(claim="Wie entwickelt sich die Konjunktur in Österreich?"):
    r = asyncio.run(search_wifo_ihs({"claim": claim, "original_claim": claim}))
    assert r["results"], "Service liefert nichts"
    return r["results"][0]


# --------------------------------------------------------------------------
# Die Runde darf nicht mehr im Code stehen
# --------------------------------------------------------------------------

def test_prognoserunde_kommt_aus_den_daten():
    assert D["prognose_runde"].startswith("WIFO/IHS-Konjunkturprognose 2/2026")
    assert _ergebnis()["indicator_name"].startswith(D["prognose_runde"])


def test_keine_runde_mehr_im_quelltext_verdrahtet():
    """Der eigentliche Grund, warum die Datei so lange falsch blieb.

    Geprüft wird der CODE, nicht die Kommentare: Ein Kommentar, der die
    alte Runde als Historie festhält, ist erwünscht — verboten ist, dass
    eine Rundenbezeichnung in die Ausgabe wandert."""
    code = "\n".join(z for z in QUELLE.splitlines()
                     if not z.lstrip().startswith("#"))
    for verdrahtet in ("1/2026", "2/2026", "März 2026", "Juni 2026"):
        assert verdrahtet not in code, f"Runde {verdrahtet!r} steht im Code"
    assert 'data.get("prognose_runde")' in QUELLE


# --------------------------------------------------------------------------
# Die drei korrigierten Richtungen
# --------------------------------------------------------------------------

def test_2025_war_erholung_keine_rezession():
    assert D["bip_2025_ist_pct_real"] == 0.8
    assert "KEIN Rezessionsjahr" in D["konjunktur_charakterisierung"]
    assert "keine Rezession" in _ergebnis()["display_value"]


def test_inflation_liegt_ueber_dem_ezb_ziel():
    assert D["inflation_2026_pct_wifo"] == 3.2 and D["inflation_2026_pct_ihs"] == 3.0
    assert "ÜBER dem" in _ergebnis()["display_value"]
    assert "überholt" in D["inflation_charakterisierung"]


def test_arbeitslosenquote_steigt_2026():
    assert D["arbeitslosenquote_nat_2025_pct"] == 7.4
    assert D["arbeitslosenquote_nat_2026_pct"] == 7.5
    assert D["arbeitslosenquote_nat_2027_pct"] == 7.3
    assert "steigt" in _ergebnis()["display_value"]


# --------------------------------------------------------------------------
# Keine gemischten Vintages
# --------------------------------------------------------------------------

def test_keine_werte_aus_der_alten_runde():
    """Die Runde 2/2026 nennt Inflation nur für 2026. Alte 2025-/2027-Werte
    stehenzulassen hätte zwei Vintages in einem Datensatz vermischt."""
    for feld in ("inflation_2025_pct_wifo", "inflation_2025_pct_ihs",
                 "inflation_2027_pct_wifo", "inflation_2027_pct_ihs",
                 "arbeitslosenquote_eurostat_2025_pct_wifo",
                 "arbeitslosenquote_eurostat_2026_pct_wifo"):
        assert feld not in D, f"{feld} stammt aus der alten Runde"


def test_ams_definition_bleibt_erklaert():
    """Die Methodik-Warnung ist der Grund, warum die Zahl nicht mit der
    Eurostat-Quote verwechselt wird — sie darf beim Refresh nicht wegfallen."""
    t = D["ams_methodologie_caveat"]
    assert "NATIONALEN" in t and "Eurostat" in t


def test_display_value_bleibt_unter_dem_prompt_cap():
    dv = _ergebnis()["display_value"]
    assert len(dv) <= 400, f"{len(dv)} Zeichen — würde claim-zentriert gekürzt"


def test_quellen_zeigen_auf_die_neue_runde():
    assert "2-2026" in DATEN["source_url"]
    assert "2/2026" in DATEN["source_label"] and "Juni 2026" in DATEN["source_label"]
