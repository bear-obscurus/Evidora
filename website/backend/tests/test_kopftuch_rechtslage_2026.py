"""Kopftuch-Rechtslage: zwei Verbote, zwei Entscheidungen, zwei Eintraege.

Befund (2026-09-05, live gemessen): „An Österreichs Volksschulen gilt ein
Kopftuchverbot." lieferte `mostly_false@0.9` mit der Begruendung „tritt aber
erst ab 1.9.2026 sanktionswirksam in Kraft. Bis 31.8.2026 gilt es daher noch
nicht." — an einem Tag, an dem der 1.9.2026 vier Tage zurueck lag.

Die Ursache war KEINE veraltete Zahl, sondern eine aktive Fehlanweisung: Der
Fakt trug den Satz „derzeit TEILS-TEILS (Empfehlung: Verdict mixed,
Konfidenz 0.6)" und einen in die Zukunft gerichteten Stichtag. Beide
Prompt-Kanaele sagten damit das Falsche — die ungekuerzte Headline
(„Kopftuchverbot an Volksschulen — verfassungswidrig aufgehoben", das war
das Verbot von 2019) und der 400-Zeichen-Schnipsel der `description`, in dem
genau die zwei abgelaufenen Saetze standen.

Fix nach dem ParlGov-Muster (#116): Zwei verschiedene Rechtsakte gehoeren in
ZWEI Eintraege, nicht in einen.
  * G 4/2020  — das 2019er-Verbot (bis 10 Jahre) wurde 2020 aufgehoben
  * G 76/2026 — Antraege gegen das NEUE Verbot (bis 14 Jahre) am 25.6.2026
                als UNZULAESSIG zurueckgewiesen; Verbot seit 1.9.2026 in Kraft

Der zweite Punkt ist die Kompetenz-Urteil-Falle aus lessons_learned:
„zurueckgewiesen" ist NICHT „bestaetigt". Der VfGH hat die
Verfassungsmaessigkeit ausdruecklich NICHT geprueft — die Antragstellerinnen
waren zum Entscheidungszeitpunkt noch nicht aktuell betroffen.

Dependency-light: JSON + Renderer, kein Netz, kein Modell.
"""

import asyncio
import json
import re
from pathlib import Path

import pytest

from services.at_courts import search_at_courts

DATA = Path(__file__).resolve().parents[1] / "data" / "at_courts.json"
RULINGS = json.loads(DATA.read_text(encoding="utf-8"))["rulings"]

ALT = "vfgh_g_4_2020_kopftuchverbot"
NEU = "vfgh_g_76_2026_kopftuchverbot_neu"


def _r(rid):
    treffer = [x for x in RULINGS if x.get("id") == rid]
    assert treffer, f"Eintrag {rid} fehlt"
    return treffer[0]


def _suche(claim):
    return asyncio.run(search_at_courts({"claim": claim, "original_claim": claim}))["results"]


# ---------------------------------------------------------------------------
# Die Lehre aus dem Befund: keine in die Zukunft gerichteten Schalter im Fakt
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("rid", [ALT, NEU])
def test_kein_zukunfts_stichtag_im_fakt(rid):
    """Ein Fakt beschreibt den IST-Zustand. Wer ein „erst ab <Datum>" oder
    „bis <Datum> gilt" hineinschreibt, baut eine Zeitbombe: der Satz wird am
    Stichtag falsch, ohne dass irgendetwas ausschlaegt. Genau daran ist der
    Kopftuch-Fakt gescheitert."""
    blob = json.dumps(_r(rid), ensure_ascii=False)
    for muster in (r"erst ab \d{1,2}\.\d{1,2}\.20\d{2}",
                   r"noch nicht rechtswirksam",
                   r"[Ff]ür den Zeitraum bis \d"):
        assert not re.search(muster, blob), f"{rid}: Zukunfts-Stichtag {muster!r}"


@pytest.mark.parametrize("rid", [ALT, NEU])
def test_keine_maschinenlesbare_verdict_direktive(rid):
    """Die alte Fassung trug „Empfehlung: Verdict mixed, Konfidenz 0.6" —
    das wird vom Direktiven-Floor geparst und zementierte das falsche Label.
    Die Bindung steht jetzt als Klartext-Aussage, nicht als Direktive."""
    blob = json.dumps(_r(rid), ensure_ascii=False).lower()
    assert not re.search(r"verdict\s+\w+,\s*konfidenz", blob)


# ---------------------------------------------------------------------------
# Die zwei Eintraege und ihre Abgrenzung
# ---------------------------------------------------------------------------

def test_alter_eintrag_grenzt_sich_vom_neuen_verbot_ab():
    """Die Headline ist der ungekuerzte Prompt-Kanal. Sie sagte frueher nur
    „verfassungswidrig aufgehoben" — das liest sich wie „es gibt kein
    Verbot"."""
    name = _r(ALT)["case_name"]
    assert "2019" in name
    assert "1.9.2026" in name and "NICHT" in name


def test_neuer_eintrag_existiert_und_ist_ein_beschluss():
    r = _r(NEU)
    assert r["decision_type"] == "Beschluss", "Zurückweisung ist kein Erkenntnis"
    assert r["case_number"].startswith("G 76/2026")
    assert r["decided_iso"] == "2026-06-25"


def test_zurueckgewiesen_ist_nicht_bestaetigt():
    """Kompetenz-Urteil-Falle: Der VfGH hat NICHT inhaltlich geprueft."""
    r = _r(NEU)
    kern = r["kerninhalt"]
    assert "UNZULÄSSIG" in kern.upper()
    assert "INHALTLICHE" in kern.upper() and "NICHT" in kern
    assert "KEINE Bestätigung der Verfassungskonformität" in kern
    assert "verfassungskonform" in r["boulevard_falschmeldung"].lower()


def test_altersgrenze_bindet_die_bewertung():
    """Volksschule ist vollstaendig erfasst, „Schulen" allgemein nicht —
    dieselbe Messgroessen-Bindung wie bei Verbrauch/Verzehr (#321)."""
    kern = _r(NEU)["kerninhalt"]
    assert "14. GEBURTSTAG" in kern.upper()
    assert "VOLKSSCHULEN ist ZUTREFFEND" in kern
    assert "ÜBERWIEGEND zutreffend" in kern


def test_sanktionsstaffel_und_strafrahmen_belegt():
    kern = _r(NEU)["kerninhalt"]
    assert "150" in kern and "800" in kern
    assert "Ersatzfreiheitsstrafe" in kern


# ---------------------------------------------------------------------------
# Renderer + Trigger
# ---------------------------------------------------------------------------

def test_decision_type_default_bleibt_erkenntnis():
    """Bestehende Eintraege ohne das neue Feld duerfen sich nicht aendern."""
    ergebnisse = _suche("Hat der VfGH das Kopftuchverbot an Volksschulen aufgehoben?")
    alt = [x for x in ergebnisse if "G 4/2020" in x["indicator_name"]]
    assert alt and alt[0]["indicator_name"].startswith("VfGH-Erkenntnis")


def test_neuer_eintrag_rendert_als_beschluss():
    ergebnisse = _suche("Gilt an Österreichs Schulen ein Kopftuchverbot?")
    neu = [x for x in ergebnisse if "G 76/2026" in x["indicator_name"]]
    assert neu, "Der 2026-Beschluss wird nicht geliefert"
    assert neu[0]["indicator_name"].startswith("VfGH-Beschluss")


@pytest.mark.parametrize("claim", [
    "An Österreichs Volksschulen gilt ein Kopftuchverbot.",
    "Gilt an Österreichs Schulen ein Kopftuchverbot?",
    "Der VfGH hat das Kopftuchverbot bestätigt.",
    "Ist das Kopftuch an Schulen in Österreich verboten?",
])
def test_beide_eintraege_erreichen_den_claim(claim):
    """Beide Rechtsakte muessen im Prompt stehen — sonst entscheidet der
    LLM anhand des halben Bildes."""
    ids = " ".join(x["indicator_name"] for x in _suche(claim))
    assert "G 4/2020" in ids and "G 76/2026" in ids, f"nur: {ids[:160]}"


@pytest.mark.parametrize("claim", [
    "Giorgia Meloni ist Italiens Ministerpräsidentin.",
    "Der Fleischverzehr in Österreich liegt bei fast 65 Kilo pro Kopf im Jahr.",
    "In Österreich gibt es mehr als 2.000 Kilometer Autobahnen.",
    "Die Hälfte aller Lebensmittel in Österreich wird weggeworfen.",
])
def test_kein_ueber_triggern_auf_fremde_claims(claim):
    ids = " ".join(x["indicator_name"] for x in _suche(claim))
    assert "Kopftuch" not in ids, f"Kopftuch-Eintrag feuert auf: {claim!r}"
