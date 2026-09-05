"""Stichtags-Wächter: Klassifikation + Gate auf den echten Fakten.

Anlass (2026-09-05): Der Kopftuch-Fakt trug „ist aber erst ab 1.9.2026
sanktionswirksam — derzeit TEILS-TEILS (Empfehlung: Verdict mixed,
Konfidenz 0.6)". Vier Tage nach dem Stichtag lieferte die Pipeline damit
`mostly_false@0.9` auf einen zutreffenden Claim. Der ⏰-Marker im Memory
hat es nicht verhindert — er wird nur gelesen, wenn jemand danach sucht.
Und `data_freshness_check` prüft nur das DATEI-Alter, nicht Daten IM Text.

Der Wächter ist am echten Fall kalibriert: der Original-Wortlaut von vor
PR #125 steht unten als Fixture und MUSS als KRITISCH erkannt werden.
Ein Detektor, den man nicht am bekannten Positivfall geprüft hat, misst
irgendetwas.

⚠️ `test_keine_kritischen_stichtage_in_den_echten_fakten` ist bewusst
zeit-abhängig: Legt irgendwann ein eingebauter Schalter um, wird die CI
rot. Das ist die Absicht — ein Fakt, der die Pipeline aktiv falsch
instruiert, soll einen Merge blockieren. WARNUNG/INFO blockieren nicht.

Dependency-light: reine Regex-/Datums-Logik, kein Netz, kein Modell.
"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

import stichtag_check as sc  # noqa: E402

HEUTE = date(2026, 9, 5)
DATA = Path(__file__).resolve().parents[1] / "data"


def _stufen(eintrag, heute=HEUTE, vintage=24):
    return [f["stufe"] for f in sc.pruefe_eintrag(eintrag, heute, vintage)]


# ---------------------------------------------------------------------------
# Kalibrierung am echten Fall — Original-Wortlaut von vor PR #125
# ---------------------------------------------------------------------------

KOPFTUCH_VOR_DEM_FIX = {
    "id": "vfgh_g_4_2020_kopftuchverbot",
    "kerninhalt": (
        "RECHTSLAGE Stand Juli 2026: Ein NEUES Kopftuchverbot (bis 14. Geburtstag) "
        "wurde am 11.12.2025 beschlossen, ist aber erst ab 1.9.2026 sanktionswirksam "
        "— 'an Schulen gilt ein Kopftuchverbot' ist derzeit TEILS-TEILS (Empfehlung: "
        "Verdict mixed, Konfidenz 0.6). Für den Zeitraum bis 31.8.2026 gilt daher: "
        "'An Österreichs Schulen gilt ein Kopftuchverbot' ist TEILS-TEILS."
    ),
}


def test_kalibrierung_kopftuch_wird_als_kritisch_erkannt():
    funde = sc.pruefe_eintrag(KOPFTUCH_VOR_DEM_FIX, HEUTE, 24)
    kritisch = [f for f in funde if f["stufe"] == "KRITISCH"]
    assert len(kritisch) >= 2, f"nur {len(kritisch)} KRITISCH: {funde}"
    treffer = " ".join(f["fund"] for f in kritisch)
    assert "erst ab 1.9.2026" in treffer
    assert "Für den Zeitraum bis 31.8.2026" in treffer


def test_belegsatz_ueberlebt_deutsche_datumspunkte():
    """„31.8.2026" darf den Satz nicht mitten im Datum zerreißen —
    dieselbe Falle wie Tausenderpunkte in den Summary-Regexen."""
    funde = sc.pruefe_eintrag(KOPFTUCH_VOR_DEM_FIX, HEUTE, 24)
    satz = next(f["satz"] for f in funde if "Zeitraum" in f["fund"])
    assert "31.8.2026 gilt daher" in satz, satz


# ---------------------------------------------------------------------------
# Klassifikation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text", [
    "Die Regel ist aber erst ab 1.3.2026 wirksam.",
    "Für den Zeitraum bis 30.4.2026 gilt die alte Fassung.",
    "Das Gesetz ist bis 1.1.2026 noch nicht in Kraft.",
])
def test_kritisch_erkennt_umgelegte_schalter(text):
    assert "KRITISCH" in _stufen({"id": "x", "t": text})


@pytest.mark.parametrize("text", [
    "Die Impfung ist kostenfrei bis 30.6.2026.",
    "Das Programm ist befristet bis 31.12.2025.",
])
def test_warnung_erkennt_geschlossene_zeitfenster(text):
    assert "WARNUNG" in _stufen({"id": "x", "t": text})


def test_zukunft_wird_nicht_gemeldet():
    """Der Kern: nur ABGELAUFENE Stichtage sind ein Fund."""
    assert _stufen({"id": "x", "t": "gilt erst ab 1.3.2027 und ist kostenfrei bis 31.12.2027."}) == []


def test_vintage_ab_schwelle():
    alt = {"id": "x", "t": "Zahlen laut SIPRI (Stand Januar 2024)."}
    assert _stufen(alt, vintage=24) == ["INFO"]
    assert _stufen(alt, vintage=120) == [], "unterhalb der Schwelle kein Fund"


def test_hoechststand_ist_kein_vintage_marker():
    """Regressions-Pin: „HöchstSTAND Oktober 2022" ist eine historische
    Tatsache, kein Vintage-Marker — die erste Fassung meldete sie."""
    assert _stufen({"id": "x", "t": "Höchststand Oktober 2022 (10,6 %)."}) == []


def test_reihenfolge_kritisch_zuerst():
    funde = sc.scanne(DATA, HEUTE, 24)
    stufen = [f["stufe"] for f in funde]
    rang = {"KRITISCH": 0, "WARNUNG": 1, "INFO": 2}
    assert stufen == sorted(stufen, key=lambda s: rang[s])


# ---------------------------------------------------------------------------
# Das Gate auf den echten Fakten
# ---------------------------------------------------------------------------

def test_keine_kritischen_stichtage_in_den_echten_fakten():
    """⚠️ Bewusst zeit-abhängig. Wird das rot, hat ein Fakt einen
    eingebauten Schalter umgelegt und instruiert die Pipeline aktiv
    falsch — dann gehört der Fakt gefixt, nicht der Test."""
    kritisch = [f for f in sc.scanne(DATA, date.today(), 24)
                if f["stufe"] == "KRITISCH"]
    assert not kritisch, "Abgelaufene Zukunfts-Stichtage:\n" + "\n".join(
        f"  {f['pack']} / {f['id']} ({f['datum']}): {f['fund']}\n"
        f"      {f['satz'][:160]}" for f in kritisch)


def test_scan_liest_alle_container_schluessel():
    """rulings (at_courts) und facts (Packs) müssen beide erfasst sein —
    sonst prüft der Wächter die halbe Datenschicht."""
    assert "rulings" in sc.LIST_KEYS and "facts" in sc.LIST_KEYS
    ids = {f["id"] for f in sc.scanne(DATA, date(2027, 1, 1), 12)}
    assert ids, "Scan findet gar nichts — Container-Schlüssel falsch?"
