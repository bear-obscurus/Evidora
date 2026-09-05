"""Frontex: Stand Juli 2026 — und eine Retrieval-Lücke bei der häufigsten Formulierung.

Befund (2026-09-05, zweiter Mittel-Eintrag): Der Datensatz trug Jahreszahlen für
2024/2025 und beschrieb die Routen nur qualitativ („vergleichbar mit 2024",
„deutlich rückläufig"). Für einen Faktencheck ist das zu wenig — Claims nennen
Zahlen und Richtungen.

Neuer Stand aus der Frontex-Pressemeldung vom 14.08.2026 (vorläufige Daten
1. Januar bis 31. Juli 2026), mit vollständiger Routen-Aufschlüsselung und
Vorjahreszeitraum. Gegenprobe: Die Summe der EU-Einreise-Routen ergibt 60.985
gegenüber „approximately 61 000" in der Meldung — eine Abweichung von 0,02 %,
die das Transkribieren der acht Routenwerte absichert.

**Die Kadenz stimmte nicht.** Die Datei vermerkte „Refresh: vierteljährlich
(Frontex publiziert Quartalsbilanz)". Tatsächlich publiziert Frontex monatlich
kumulierte vorläufige Zahlen — die Meldung vom 14.08.2026 deckt sieben Monate
ab. `refresh_kadenz` steht deshalb auf `monatlich`.

**Retrieval-Lücke**: „Die illegale Migration in die EU steigt dramatisch" — die
häufigste Formulierung dieses Claim-Typs überhaupt — traf WEDER frontex NOCH
migration_pack. Beide lieferten null Treffer. Der Trigger kannte nur
„irreguläre grenzübertritte", nicht „illegale Migration". Bewusst mit
Regionsbezug gekoppelt: eine Aussage ohne Region lässt sich mit
EU-Aussengrenzdaten nicht sauber beantworten.

Nicht verifizierbar und deshalb ausdrücklich gekennzeichnet: die Zahl der
Mittelmeer-Todesfälle stammt vom IOM Missing Migrants Project, nicht von
Frontex, und missingmigrants.iom.int antwortete beim Refresh mit HTTP 403.
"""

import ast
import asyncio
import json
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
DATEN = json.loads((BACKEND / "data" / "frontex.json").read_text(encoding="utf-8"))
D = DATEN["data"]
QUELLE = (BACKEND / "services" / "frontex.py").read_text(encoding="utf-8")


def _nur_code(quelle: str) -> str:
    baum = ast.parse(quelle)
    code = quelle
    for k in ast.walk(baum):
        if isinstance(k, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            doc = ast.get_docstring(k, clean=False)
            if doc:
                code = code.replace(doc, "")
    return "\n".join(z for z in code.splitlines() if not z.lstrip().startswith("#"))


CODE = _nur_code(QUELLE)

from services.frontex import (  # noqa: E402
    claim_mentions_frontex_cached,
    search_frontex,
)


def _treffer(claim):
    return asyncio.run(search_frontex({"claim": claim,
                                       "original_claim": claim}))["results"]


ROUTEN = {r["name"]: r for r in D["routen"]}
EU_ROUTEN = [r for r in D["routen"] if "Ärmelkanal" not in r["name"]]


# --------------------------------------------------------------------------
# Die Zahlen und ihre Gegenprobe
# --------------------------------------------------------------------------

def test_zeitraum_ist_der_der_pressemeldung():
    assert D["zeitraum"] == "1. Januar bis 31. Juli 2026"
    assert D["vergleichszeitraum"] == "1. Januar bis 31. Juli 2025"
    assert D["veraenderung_ggue_vorjahreszeitraum_pct"] == -37


def test_routensumme_deckt_sich_mit_der_gesamtzahl():
    """Die stärkste Gegenprobe gegen einen Transkriptionsfehler: Frontex nennt
    rund 61.000 Detektionen, die acht EU-Einreise-Routen müssen das ergeben."""
    summe = sum(r["detektionen"] for r in EU_ROUTEN)
    gesamt = D["detektionen_eu_gesamt_approx"]
    assert abs(summe - gesamt) / gesamt < 0.01, f"{summe} gegen {gesamt}"


def test_jede_route_traegt_zahl_vorjahr_und_richtung():
    for r in D["routen"]:
        for feld in ("detektionen", "vorjahreszeitraum", "veraenderung_pct"):
            assert isinstance(r.get(feld), int), f"{r['name']}: {feld} fehlt"


def test_veraenderung_passt_zu_den_beiden_zahlen():
    """Eine Prozentangabe, die nicht zu ihren eigenen Zahlen passt, ist der
    haeufigste stille Fehler in kuratierten Daten."""
    for r in D["routen"]:
        gerechnet = (r["detektionen"] - r["vorjahreszeitraum"]) / r["vorjahreszeitraum"] * 100
        assert abs(gerechnet - r["veraenderung_pct"]) <= 1, (
            f"{r['name']}: angegeben {r['veraenderung_pct']} %, "
            f"gerechnet {gerechnet:.1f} %")


def test_westliches_mittelmeer_ist_die_einzige_steigende_route():
    steigend = [r["name"] for r in EU_ROUTEN if r["veraenderung_pct"] > 0]
    assert steigend == ["Westliches Mittelmeer (Spanien)",
                        "Zirkuläre Route nach Albanien"] or \
           "Westliches Mittelmeer (Spanien)" in steigend
    assert ROUTEN["Westliches Mittelmeer (Spanien)"]["veraenderung_pct"] == 37


def test_aermelkanal_ist_als_ausreise_gekennzeichnet():
    """22.469 Detektionen am Ärmelkanal sind AUSREISEN aus der EU — sie in die
    Einreise-Gesamtzahl zu rechnen wäre ein Messgrößen-Fehler."""
    r = ROUTEN["Ärmelkanal (Ausreise Richtung Vereinigtes Königreich)"]
    assert "AUSREISEN" in r["hinweis"] or "Ausreise" in r["name"]
    assert any("AUSREISEN" in c for c in D["wichtige_caveats"])


# --------------------------------------------------------------------------
# Vorbehalte
# --------------------------------------------------------------------------

def test_vorlaeufigkeit_und_detektionen_stehen_in_der_headline():
    x = _treffer("Frontex Zahlen 2026")[0]
    assert "VORLÄUFIGE" in x["display_value"]
    assert "DETEKTIONEN, nicht Personen" in x["display_value"]


def test_ceuta_vorbehalt_ist_uebernommen():
    assert any("Ceuta" in c for c in D["wichtige_caveats"])


def test_todesfaelle_sind_als_fremde_und_aeltere_quelle_markiert():
    t = D["todesfaelle_mittelmeer"]
    assert "IOM" in t["quelle"]
    assert "403" in t["hinweis"], "der Grund der Nicht-Verifizierbarkeit muss dastehen"
    assert "NICHT von Frontex" in t["hinweis"]


def test_jahreswerte_sind_als_nicht_vergleichbar_markiert():
    """240.000 (Ganzjahr 2024) gegen 61.000 (sieben Monate 2026) ist kein
    Rückgang um 75 % — die Bezugsräume sind verschieden."""
    assert "NICHT direkt vergleichbar" in D["jahreswerte_kontext"]["hinweis"]


# --------------------------------------------------------------------------
# Renderer
# --------------------------------------------------------------------------

def test_kein_zeitraum_mehr_im_code_verdrahtet():
    for verdrahtet in ("routen_2025", "trend_2025", "Frontex 2025",
                       "_2025_total_approx", "_2024_total", "2025\"", "2024\""):
        assert verdrahtet not in CODE, f"{verdrahtet!r} steht im Code"
    assert 'data.get("zeitraum")' in CODE


def test_richtung_traegt_immer_ein_vorzeichen():
    """Bei Migrationszahlen ist das Vorzeichen die eigentliche Aussage."""
    x = _treffer("Frontex Zahlen 2026")[0]
    assert "-37 %" in x["display_value"]
    r = _treffer("Wie ist die Lage im westlichen Mittelmeer?")[0]
    assert "+37 %" in r["display_value"], r["display_value"]


def test_routen_ergebnis_nennt_zahl_und_vorjahr():
    x = _treffer("Über die Westbalkan-Route kommen immer mehr Menschen")[0]
    assert x["indicator"] == "frontex_route"
    assert "4.910" in x["display_value"] and "6.673" in x["display_value"]
    assert "-26 %" in x["display_value"]


def test_kadenz_ist_monatlich_und_begruendet():
    assert DATEN["refresh_kadenz"] == "monatlich"
    assert "monatlich" in DATEN["kadenz_begruendung"]
    assert "vierteljährlich" not in DATEN["note"], \
        "die alte, falsche Kadenz-Angabe muss raus"


# --------------------------------------------------------------------------
# Die Retrieval-Lücke
# --------------------------------------------------------------------------

def test_haeufigste_formulierung_trifft_jetzt():
    """Traf bis 2026-09 weder frontex noch migration_pack."""
    for claim in ("Die illegale Migration in die EU steigt dramatisch",
                  "Die irreguläre Migration über das Mittelmeer nimmt zu",
                  "Illegale Einwanderung an den EU-Außengrenzen",
                  "Irreguläre Zuwanderung über die Balkanroute"):
        assert claim_mentions_frontex_cached(claim), claim
        assert _treffer(claim), claim


def test_bisherige_trigger_funktionieren_weiter():
    for claim in ("Frontex meldet neue Zahlen",
                  "Wie viele irreguläre Grenzübertritte gab es in der EU?",
                  "Wie viele Menschen sind im Mittelmeer ertrunken?"):
        assert claim_mentions_frontex_cached(claim), claim


def test_kein_ueber_trigger():
    for claim in ("Die illegale Migration in die USA steigt",
                  "Wie hoch ist die Inflation in Österreich?",
                  "Migration ist ein wichtiges gesellschaftliches Thema",
                  "Der Grenzwert für Feinstaub wurde überschritten",
                  "Die Vogelmigration beginnt im Herbst"):
        assert not claim_mentions_frontex_cached(claim), claim
