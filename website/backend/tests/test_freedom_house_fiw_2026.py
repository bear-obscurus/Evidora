"""Freedom House: Ausgabe 2026 statt 2024 — und die Jahreszahl raus aus dem Code.

Befund (2026-09-05, vierter Eintrag aus dem Freshness-Rückstand): Der Cache trug
**FIW 2024** (bewertet 2023). Aktuell ist **FIW 2026** „Growing Shadow of
Autocracy" (bewertet 2025) — zwei Ausgaben übersprungen.

Die Jahreszahl steckte an drei Stellen fest im Code (`_build_display_value`
schrieb „(Freedom House FIW 2024)" als Literal, `report_year` hatte den Default
2024) **und im Dateinamen** (`data/freedom_house_2024.json`). Ein Daten-Refresh
allein hätte weiter 2024 ausgewiesen — dieselbe Klasse wie wifo_ihs (#131) und
oecd_health (#132). Datei heisst jetzt `freedom_house.json`; die Ausgabe steht
nur noch in `report_year`, und fehlt sie, wird gar kein Jahr genannt statt eines
erfundenen.

Zwei Dinge lagen zusätzlich brach:

* `world_summary` war ein **totes Datenfeld** — niemand las es. Genau diese
  Zahlen (20. Jahr Rückgang, 88 von 195 Ländern Free) beantworten Claims über
  den globalen Trend; ohne sie hat der Synthesizer nur den Punktestand EINES
  Landes.
* `description` wurde mit `text[:400]` beschnitten und endete mitten im Wort
  („erzwungenen Bevölkerungsversc"). Jetzt: ganze Sätze, und der Methodik-Caveat
  bekommt sein Budget zuerst — er ist laut den politischen Guardrails Pflicht.

Für diese Quelle gab es bis hierher **keinen einzigen Test**.

Datenherkunft: die 55 Länder wurden einzeln von
``freedomhouse.org/country/{slug}/freedom-world/2026`` gelesen (die frühere
All_data_FIW-XLSX liegt nicht mehr unter ihrem Pfad). Gegenproben: PR + CL ==
Total bei allen 55, und der im Report ausdrücklich genannte US-Verlust von
3 Punkten deckt sich mit den gelesenen Werten.
"""

import ast
import asyncio
import json
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
PFAD = BACKEND / "data" / "freedom_house.json"
DATEN = json.loads(PFAD.read_text(encoding="utf-8"))
RATINGS = DATEN["ratings"]
QUELLE = (BACKEND / "services" / "freedom_house.py").read_text(encoding="utf-8")


def _nur_code(quelle: str) -> str:
    """Quelltext ohne Kommentare und ohne Docstrings.

    Ein Kommentar oder Docstring, der die Ausgabe als Historie festhält, ist
    erwünscht — verboten ist nur, dass eine Jahreszahl in die AUSGABE wandert.
    """
    baum = ast.parse(quelle)
    code = quelle
    for knoten in ast.walk(baum):
        if isinstance(knoten, (ast.Module, ast.ClassDef,
                               ast.FunctionDef, ast.AsyncFunctionDef)):
            doc = ast.get_docstring(knoten, clean=False)
            if doc:
                code = code.replace(doc, "")
    return "\n".join(z for z in code.splitlines()
                     if not z.lstrip().startswith("#"))


CODE = _nur_code(QUELLE)

MAX_STR = 400  # synthesizer.MAX_STR

from services.freedom_house import (  # noqa: E402
    _beschreibung,
    _ganze_saetze,
    _richtungs_satz,
    search_freedom_house,
)


def _ergebnis(claim):
    r = asyncio.run(search_freedom_house({"claim": claim, "original_claim": claim}))
    assert r["results"], f"kein Treffer für {claim!r}"
    return r["results"][0]


# --------------------------------------------------------------------------
# Vintage: 2026, und nirgends verdrahtet
# --------------------------------------------------------------------------

def test_ausgabe_ist_fiw_2026():
    assert DATEN["report_year"] == 2026
    assert DATEN["covering_events_year"] == 2025
    assert "2026" in DATEN["source_label"]
    assert _ergebnis("Freedom House Österreich")["year"] == "2026"


def test_keine_jahreszahl_im_code_verdrahtet():
    """Der eigentliche Grund, warum die Quelle zwei Ausgaben zurückblieb."""
    for verdrahtet in ("FIW 2024", "FIW 2025", "FIW 2026",
                       "freedom_house_2024", "covering 2023"):
        assert verdrahtet not in CODE, f"{verdrahtet!r} steht im Code"
    assert 'data.get("report_year")' in CODE


def test_dateiname_traegt_kein_jahr():
    assert PFAD.name == "freedom_house.json"
    assert not (BACKEND / "data" / "freedom_house_2024.json").exists()


def test_ohne_report_year_wird_kein_jahr_erfunden():
    """Lieber gar keine Ausgabe nennen als die falsche behaupten."""
    assert 'data.get("report_year", 2024)' not in CODE
    assert 'data.get("report_year") or ""' in CODE


# --------------------------------------------------------------------------
# Datenintegrität der 55 Länder
# --------------------------------------------------------------------------

def test_alle_55_laender_vorhanden():
    assert len(RATINGS) == 55
    assert set(RATINGS) == set(DATEN["country_slugs"]) == set(DATEN["country_aliases"])


def test_pr_plus_cl_ergibt_das_total():
    """Die stärkste Gegenprobe gegen einen kaputten Einlese-Lauf: FIW definiert
    Total := PR + CL. Ein Tippfehler in irgendeiner Zahl fällt hier auf."""
    abweichung = [(iso, r["pr_score"], r["cl_score"], r["total_score"])
                  for iso, r in RATINGS.items()
                  if r["pr_score"] + r["cl_score"] != r["total_score"]]
    assert not abweichung, f"PR+CL != Total: {abweichung}"


def test_status_passt_zu_den_veroeffentlichten_schwellen():
    """Free 70-100, Partly Free 35-69, Not Free 0-34 (FIW-Methodik)."""
    for iso, r in RATINGS.items():
        t, st = r["total_score"], r["status"]
        erwartet = "Free" if t >= 70 else ("Partly Free" if t >= 35 else "Not Free")
        assert st == erwartet, f"{iso}: {t} Punkte, aber Status {st!r}"


def test_werte_liegen_in_den_gueltigen_bereichen():
    for iso, r in RATINGS.items():
        assert -4 <= r["pr_score"] <= 40, f"{iso} PR {r['pr_score']}"
        assert 0 <= r["cl_score"] <= 60, f"{iso} CL {r['cl_score']}"
        assert 0 <= r["total_score"] <= 100, f"{iso} Total {r['total_score']}"


def test_us_verlust_deckt_sich_mit_der_report_aussage():
    """FIW 2026 nennt ausdrücklich: 'The United States lost 3 points'."""
    us = RATINGS["USA"]
    assert us["vorjahr_score"] - us["total_score"] == 3
    assert us["total_score"] == 81 and us["status"] == "Free"


def test_vorjahreswerte_ueberall_vorhanden():
    """Ohne sie kann der Synthesizer die RICHTUNG nur raten."""
    ohne = [iso for iso, r in RATINGS.items()
            if r.get("vorjahr_score") is None or not r.get("vorjahr_status")]
    assert not ohne, f"ohne Vorjahresstand: {ohne}"


def test_keine_selbst_hergeleiteten_ratings_mehr():
    """pr_rating/cl_rating (1-7) veröffentlichen die Country-Seiten nicht.
    Sie selbst herzuleiten wäre eine eigene Klassifikation statt einer
    Quellenangabe — und der Renderer hat sie ohnehin nie gelesen."""
    for iso, r in RATINGS.items():
        assert "pr_rating" not in r and "cl_rating" not in r, iso


def test_tschechien_slug_ist_nicht_mehr_der_alte():
    """`czech-republic` liefert bei Freedom House 404 — dieser eine Slug hat
    beim Refresh als Einziger gefehlt."""
    assert DATEN["country_slugs"]["CZE"] == "czechia"
    assert "czechia" in _ergebnis("Freedom House Tschechien")["url"]


# --------------------------------------------------------------------------
# world_summary war ein totes Feld
# --------------------------------------------------------------------------

def test_world_summary_erreicht_die_beschreibung():
    w = DATEN["world_summary"]
    assert w["global_decline_years"] == 20
    assert w["free_countries"] == 88 and w["not_free_countries"] == 59
    assert w["total_countries_covered"] == 195
    text = _ergebnis("Freedom House Österreich")["description"]
    assert "20. Jahr in Folge" in text and "88 von 195" in text


def test_partly_free_ist_als_gerechnet_ausgewiesen():
    """Free und Not Free stehen wörtlich im Report, Partly Free nicht —
    die Differenz ist gerechnet und muss als solche gekennzeichnet sein."""
    w = DATEN["world_summary"]
    assert (w["free_countries"] + w["partly_free_countries"]
            + w["not_free_countries"] == w["total_countries_covered"])
    assert "gerechnet" in w["herleitung"]


def test_world_summary_key_traegt_kein_jahr():
    assert "world_summary" in DATEN and "world_summary_2024" not in DATEN


# --------------------------------------------------------------------------
# Prompt-Budget: ganze Sätze, Methodik-Caveat ist Pflicht
# --------------------------------------------------------------------------

def test_beschreibung_bricht_nicht_mitten_im_wort():
    for claim in ("Freedom House Österreich", "China Freedom House Bewertung",
                  "Laut Freedom House ist Russland nicht frei"):
        text = _ergebnis(claim)["description"]
        assert len(text) <= MAX_STR, f"{claim}: {len(text)} Zeichen"
        assert text.endswith("."), f"abgeschnitten: ...{text[-40:]!r}"


def test_methodik_caveat_kommt_immer_mit():
    """Politische Guardrails: der Methodik-Hinweis ist Pflicht, auch wenn
    andere Zusätze das Budget füllen würden."""
    for claim in ("Freedom House Österreich", "China Freedom House Bewertung"):
        text = _ergebnis(claim)["description"]
        assert "Political Rights" in text and "Civil Liberties" in text
        assert "Free 70-100" in text


def test_ganze_saetze_schneidet_an_der_satzgrenze():
    t = "Erster Satz. Zweiter Satz. Dritter Satz."
    assert _ganze_saetze(t, 100) == t
    assert _ganze_saetze(t, 25) == "Erster Satz."
    assert _ganze_saetze(t, 5) == ""


def test_alle_felder_unter_dem_prompt_budget():
    for claim in ("Freedom House Österreich", "Laut Freedom House ist Russland nicht frei"):
        r = _ergebnis(claim)
        for feld in ("indicator_name", "display_value", "description"):
            assert len(r[feld]) <= MAX_STR, f"{claim}/{feld}: {len(r[feld])}"


# --------------------------------------------------------------------------
# Richtung + negativer PR
# --------------------------------------------------------------------------

def test_richtungssatz_dekliniert_richtig():
    assert "+1 Punkt " in _richtungs_satz(
        {"total_score": 94, "vorjahr_score": 93}) + " "
    assert "-3 Punkte" in _richtungs_satz({"total_score": 81, "vorjahr_score": 84})
    assert "unverändert" in _richtungs_satz({"total_score": 9, "vorjahr_score": 9})
    assert _richtungs_satz({"total_score": 9}) == ""


def test_statuswechsel_wird_benannt():
    satz = _richtungs_satz({"total_score": 69, "status": "Partly Free",
                            "vorjahr_score": 71, "vorjahr_status": "Free"})
    assert "'Free' auf 'Partly Free'" in satz


def test_richtung_steht_in_der_ausgabe():
    assert "-3 Punkte" in _ergebnis(
        "Die USA verlieren an Freiheit laut Freedom House")["display_value"]


def test_negativer_pr_wird_nur_dort_erklaert_wo_er_auftritt():
    """China hat PR -2. Ohne Erklärung liest der Synthesizer das als Datenfehler
    — mit Erklärung bei JEDEM Land wäre es verschwendetes Prompt-Budget."""
    china = _ergebnis("China Freedom House Bewertung")
    assert "PR -2/40" in china["display_value"]
    assert "negativer PR-Wert ist methodisch vorgesehen" in china["description"]
    at = _ergebnis("Freedom House Österreich")
    assert "negativer PR-Wert" not in at["description"]


def test_beschreibung_ohne_rating_bleibt_bedienbar():
    assert "Political Rights" in _beschreibung(
        DATEN["methodology_note"], DATEN, None)


# --------------------------------------------------------------------------
# Guardrails + kein Über-Trigger
# --------------------------------------------------------------------------

def test_bewertung_wird_zugeschrieben_nicht_selbst_vorgenommen():
    """Wir zitieren Freedom-House-Scores, wir bewerten nicht selbst."""
    r = _ergebnis("Ungarn ist laut Freedom House keine Demokratie mehr")
    assert r["indicator_name"].startswith("Freedom House")
    assert "Freedom House" in r["display_value"]
    assert "Freedom House" in r["source"]


def test_kein_ueber_trigger_ohne_freiheits_bezug():
    for claim in ("Wie hoch ist die Inflation in Österreich?",
                  "Das Haus der Freiheit steht in Wien",
                  "Wer hat die Fußball-WM 2026 gewonnen?"):
        r = asyncio.run(search_freedom_house({"claim": claim,
                                              "original_claim": claim}))
        assert not r["results"], f"Über-Trigger: {claim!r}"


def test_dispatch_label_traegt_keine_jahreszahl():
    """Zweiter Namensraum, in #133 uebersehen und von der QA-Batterie am
    2026-09-06 gefunden: main.py meldet die Quelle unter einem EIGENEN Label
    in `source_coverage.names`. Dort stand "Freedom House FIW 2024", obwohl
    die Daten auf FIW 2026 standen — die Antwort nannte also eine Ausgabe,
    die sie gar nicht verwendet hat (Marker-Drift, PR #74)."""
    main_py = (BACKEND.parent / "backend" / "main.py").read_text(encoding="utf-8")
    # Auf den DISPATCH ankern, nicht auf die Import-Zeile oben.
    i = main_py.index('cached("FreedomHouse"')
    stelle = main_py[i:i + 220]
    assert 'queried_names.append("Freedom House")' in stelle, stelle[-140:]
    for jahr in ("FIW 2024", "FIW 2025", "FIW 2026"):
        assert f'queried_names.append("Freedom House {jahr}")' not in main_py
