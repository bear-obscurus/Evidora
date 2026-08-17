"""ParlGov: Kabinettswechsel ≠ Ende der Amtszeit des Staatsoberhaupts (QA50D #327).

Befund (1 von 6 Läufen, schwankend — aber die Ursache ist deterministisch):
„Emmanuel Macron regiert Frankreich noch immer." → `false@0.9` statt `true`.
Live-Summary: „Laut ParlGov wurde das Kabinett unter Emmanuel Macron
(Borne/Attal) spätestens am 21.09.2024 durch das Kabinett Barnier/Bayrou
abgelöst. Aktuell regiert Macron Frankreich daher nicht mehr."

Wurzel (lokal deterministisch reproduziert, nicht bloß vermutet): Die
ParlGov-Zeile für die Présidentielle 2022 trägt ZWEI Ämter in einem Datensatz
— ``winner`` ist das STAATSOBERHAUPT (Emmanuel Macron), ``cabinet`` die
Regierungschef-Ebene (Borne / Attal). ``_is_cabinet_superseded`` fand die
Législatives-Zeile 2024 (Kabinett Barnier / Bayrou ab 2024-09-21) und setzte
auf die GANZE Zeile — also auch auf den Präsidenten — einen harten
``STRUKTURELL FALSCH:``-Marker mit dem Wortlaut „es ist NICHT mehr die
amtierende Regierung / Präsens-Aussagen … nicht mehr zutreffend".

In semi-präsidentiellen Systemen wechseln beide Ämter unabhängig: Frankreich
wählt Präsident:in (5 Jahre) und Nationalversammlung getrennt. Der
Kabinettswechsel 2024 sagt nichts über Macrons bis 2027 laufende Amtszeit.

Neue Klasse — NICHT die Orbán/Meloni-Kante (dort ist Wikidata die Quelle und
es geht um End-Daten derselben Position).

Fix: Präsidentschaftswahl-Zeilen bekommen den harten Marker nur noch, wenn
eine SPÄTERE Präsidentschaftswahl mit ANDEREM Sieger existiert. Sonst ein
deskriptiver ``AMTS-ABGRENZUNG``-Hinweis, der den Kabinettswechsel
vollständig benennt (Kabinetts-Claims bleiben widerlegbar), aber ausdrücklich
festhält, dass daraus nichts über das Staatsoberhaupt folgt.

Dependency-light: nur JSON + Datums-/String-Logik, kein Modell, kein Netz.
"""

import asyncio
import datetime as _dt

import pytest

from services.parlgov import (
    _build_election_result,
    _cabinet_change_note_presidential,
    _is_cabinet_superseded,
    _is_presidential_election,
    _load_static_json,
    _presidency_successor,
    _winner_person,
    search_parlgov,
)

STRUCT = "STRUKTURELL FALSCH:"


def _country(code: str) -> dict:
    data = _load_static_json()
    assert data, "data/parlgov.json nicht ladbar"
    country = (data.get("countries") or {}).get(code)
    assert country, f"Land {code} fehlt in parlgov.json"
    return country


def _row(code: str, year: int, etype_contains: str = "") -> dict:
    for e in _country(code).get("elections") or []:
        if e.get("year") == year and etype_contains.lower() in (e.get("type") or "").lower():
            return e
    raise AssertionError(f"Wahl {code}/{year} ({etype_contains!r}) fehlt")


def _display(code: str, year: int, etype_contains: str = "") -> str:
    return _build_election_result(
        code, _country(code), _row(code, year, etype_contains),
    )["display_value"]


def _search(claim: str) -> list[dict]:
    res = asyncio.run(search_parlgov({"claim": claim, "original_claim": claim}))
    return res["results"]


# --------------------------------------------------------------------------
# Typ-Erkennung
# --------------------------------------------------------------------------

@pytest.mark.parametrize("etype", [
    "Présidentielle (Stichwahl)", "Presidentielle", "Präsidentschaftswahl",
    "Praesidentschaftswahl", "presidential election", "Elezioni presidenziali",
    "Elecciones presidenciales",
])
def test_praesidentschaftswahl_erkannt(etype):
    assert _is_presidential_election({"type": etype})


@pytest.mark.parametrize("etype", [
    "Bundestagswahl", "Nationalratswahl", "Législatives (Stichwahl, Neuwahl)",
    "General Election", "Elezioni politiche",
])
def test_parlamentswahl_nicht_als_praesidentschaftswahl(etype):
    assert not _is_presidential_election({"type": etype})


def test_winner_person_ohne_parteiklammer():
    assert _winner_person({"winner": "Emmanuel Macron (Renaissance)"}) == "emmanuel macron"
    assert _winner_person({"winner": "Emmanuel Macron (LREM/Renaissance)"}) == "emmanuel macron"


# --------------------------------------------------------------------------
# Der Befund: FR 2022 (Macron) darf keinen harten Marker mehr tragen
# --------------------------------------------------------------------------

def test_fr2022_kabinett_ist_weiterhin_als_abgeloest_erkannt():
    """Die Kabinetts-Logik bleibt unverändert — nur ihre WIRKUNG auf die
    Präsidenten-Zeile ändert sich. (Sonst würde der Test nur beweisen, dass
    wir die Erkennung kaputtgemacht haben.)"""
    fr = _country("FRA")
    superseded, successor = _is_cabinet_superseded(
        fr["elections"], _row("FRA", 2022, "présidentielle"), _dt.date(2026, 8, 17),
    )
    assert superseded and successor == "2024-09-21"


def test_fr2022_kein_harter_marker_auf_dem_praesidenten():
    dv = _display("FRA", 2022, "présidentielle")
    assert not dv.startswith(STRUCT), (
        "Der Kabinettswechsel Borne/Attal → Barnier/Bayrou darf keinen "
        f"STRUKTURELL-Marker auf Macron setzen:\n{dv}"
    )
    assert STRUCT not in dv


def test_fr2022_note_trennt_die_aemter_explizit():
    dv = _display("FRA", 2022, "présidentielle")
    assert "AMTS-ABGRENZUNG" in dv
    assert "Staatsoberhaupt" in dv
    # Die entscheidende Aussage: aus dem Kabinettswechsel folgt NICHT,
    # dass die Person aus dem Amt ist.
    assert "folgt NICHT" in dv
    assert "Macron" in dv


def test_fr2022_kabinettswechsel_bleibt_im_text():
    """Kabinetts-Claims müssen widerlegbar bleiben — der Wechsel darf nicht
    zusammen mit dem Marker verschwinden."""
    dv = _display("FRA", 2022, "présidentielle")
    assert "Borne / Attal" in dv
    assert "2024-09-21" in dv
    assert "nicht mehr im Amt" in dv


def test_macron_claim_liefert_keinen_marker_mehr():
    """End-to-end durch search_parlgov — der Pfad, den die Pipeline geht."""
    results = _search("Emmanuel Macron regiert Frankreich noch immer.")
    assert results, "ParlGov muss auf dem Claim weiter liefern"
    assert not any(STRUCT in str(r.get("display_value", "")) for r in results)


# --------------------------------------------------------------------------
# Gegenrichtung 1: ein echt abgelöstes Staatsoberhaupt wird weiter markiert
# --------------------------------------------------------------------------

def test_chirac2002_traegt_weiter_einen_harten_marker():
    dv = _display("FRA", 2002, "présidentielle")
    assert dv.startswith(STRUCT)
    assert "Chirac" in dv and "Sarkozy" in dv
    assert "Staatsoberhaupt" in dv


def test_praesidentschafts_nachfolger_wird_gefunden():
    fr = _country("FRA")
    succ = _presidency_successor(
        fr["elections"], _row("FRA", 2002, "présidentielle"), _dt.date(2026, 8, 17),
    )
    assert succ is not None and succ.get("year") == 2007


# --------------------------------------------------------------------------
# Gegenrichtung 2: Wiederwahl derselben Person ist kein Amtsende
# --------------------------------------------------------------------------

def test_wiederwahl_ist_kein_amtsende():
    """Macron 2017 → 2022 ist dieselbe Person. Ein Marker „abgelöst durch
    Emmanuel Macron" wäre die Wiedereintritts-Falle aus wikidata.py."""
    fr = _country("FRA")
    succ = _presidency_successor(
        fr["elections"], _row("FRA", 2017, "présidentielle"), _dt.date(2026, 8, 17),
    )
    assert succ is None
    dv = _display("FRA", 2017, "présidentielle")
    assert not dv.startswith(STRUCT)
    assert "AMTS-ABGRENZUNG" in dv


# --------------------------------------------------------------------------
# Gegenrichtung 3: parlamentarische Systeme behalten den harten Marker
# --------------------------------------------------------------------------

def test_deutsches_kabinett_behaelt_den_harten_marker():
    """In DE/AT ist der Wahlsieger die Regierungschef-Ebene — dort ist der
    Marker korrekt und muss bleiben."""
    results = _search("Regiert die Ampel-Koalition noch in Deutschland?")
    markers = [r for r in results if str(r.get("display_value", "")).startswith(STRUCT)]
    assert markers, "Abgelöstes Ampel-Kabinett muss weiter markiert werden"
    assert "Scholz" in markers[0]["display_value"]


def test_harter_kabinettsmarker_grenzt_das_staatsoberhaupt_aus():
    """Auch der unveränderte Kabinetts-Pfad sagt jetzt ausdrücklich, dass er
    nichts über Präsident:innen/Staatsoberhäupter aussagt — das ist die
    generische Hälfte des Fixes."""
    results = _search("Regiert die Ampel-Koalition noch in Deutschland?")
    dv = next(
        r["display_value"] for r in results
        if str(r.get("display_value", "")).startswith(STRUCT)
    )
    assert "AMTS-ABGRENZUNG" in dv
    assert "STAATSOBERHAUPTS" in dv.upper()


def test_parlgov_liefert_weiter_auf_echten_kabinetts_claims():
    """Muss-Treffer-Kontrolle: der Service darf nicht still verstummen."""
    for claim in (
        "Regiert die Ampel-Koalition noch in Deutschland?",
        "Wie war das Wahlergebnis der Bundestagswahl 2021 in Deutschland?",
        "Emmanuel Macron regiert Frankreich noch immer.",
    ):
        assert _search(claim), f"ParlGov liefert nichts für {claim!r}"


# --------------------------------------------------------------------------
# Der Hinweis-Text darf keinen Override auslösen
# --------------------------------------------------------------------------

def test_note_enthaelt_keinen_override_token():
    note = _cabinet_change_note_presidential(
        cname="Frankreich", winner="Emmanuel Macron (Renaissance)",
        cabinet="Borne / Attal", cabinet_start="2022-05-16",
        successor_start_iso="2024-09-21", today_iso="2026-08-17",
        base_headline="Roh-Daten",
    )
    assert STRUCT not in note
    assert "STRUKTURELL_COSINE_FALSCH:" not in note
    # "verdict" würde den Direktiven-Floor in confidence_calibration greifen
    # lassen (Gate 2) — der Hinweis ist deskriptiv, keine Direktive.
    assert "verdict" not in note.lower()
