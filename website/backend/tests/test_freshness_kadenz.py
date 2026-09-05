"""Ein Schwellwert für alle Quellen passt keiner.

Befund (2026-09-05, beim Abarbeiten der Mittel-Einträge): Der Freshness-Check
prüfte jede Datei gegen dieselben 120 Tage. Das ist zwangsläufig für die einen
zu lax und für die anderen zu streng:

* `rki_surveillance` wird upstream **wöchentlich** fortgeschrieben. Bei 120 Tagen
  schlägt der Wecker erst nach 17 verpassten Ausgaben an.
* `rsf` trägt den World Press Freedom Index **2026** — die neueste Ausgabe, die
  es gibt, denn RSF publiziert einmal jährlich Anfang Mai. Der Check meldete ihn
  trotzdem als VERALTET, obwohl die Werte am 2026-09-05 Zeichen für Zeichen mit
  rsf.org übereinstimmten (Österreich Rang 19, Score 79,43). Ein Refresh hätte
  nichts geändert — es gibt nichts Neueres.

Deshalb deklariert die Datei jetzt selbst ihren Takt (`refresh_kadenz`), und der
Check leitet daraus den Schwellwert ab. Ohne Angabe bleibt es beim Standard.

**Der Denkfehler beim Bauen** — und der Grund für die zweite Regel: PR #130 hatte
die Datei-mtime zum führenden Mass gemacht, weil `fetched_at_iso` niemand
mitzog. Das Eintragen der Kadenz fasst die Datei aber an und setzt die mtime auf
0 — `frontex.json` rutschte allein dadurch von VERALTET auf „kein Alarm", obwohl
sein Inhalt unverändert 129 Tage alt war. Wer eine Kadenz deklariert,
verpflichtet sich zugleich, `fetched_at_iso` zu pflegen; für diese Dateien ist
das Feld deshalb das ehrlichere Mass. Für alle anderen bleibt die mtime führend.
"""

import json
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from tools.data_freshness_check import (  # noqa: E402
    ANLASSBEZOGEN,
    FELD_UNGEPFLEGT,
    FRISCH,
    KADENZ_MAX_AGE,
    VERALTET,
    klassifiziere,
    schwelle_fuer,
)

STANDARD = 120


# --------------------------------------------------------------------------
# Der Schwellwert kommt aus dem Takt
# --------------------------------------------------------------------------

def test_takt_bestimmt_die_schwelle():
    assert schwelle_fuer("woechentlich", STANDARD) == 21
    assert schwelle_fuer("monatlich", STANDARD) == 60
    assert schwelle_fuer("quartalsweise", STANDARD) == 150
    assert schwelle_fuer("jaehrlich", STANDARD) == 400
    assert schwelle_fuer("ereignisgetrieben", STANDARD) is None


def test_ohne_angabe_gilt_der_standard():
    assert schwelle_fuer(None, STANDARD) == STANDARD
    assert schwelle_fuer("", STANDARD) == STANDARD
    assert schwelle_fuer("phantasietakt", STANDARD) == STANDARD


def test_woechentliche_quelle_schlaegt_frueher_an():
    """126 Tage waren beim alten Mass unauffällig — bei wöchentlicher Kadenz
    sind das 18 verpasste Ausgaben."""
    assert klassifiziere(126, 126, STANDARD) == VERALTET   # Standard: knapp drüber
    assert klassifiziere(30, 30, STANDARD) == FRISCH       # ohne Takt unauffällig
    assert klassifiziere(30, 30, STANDARD, "woechentlich") == VERALTET


def test_jaehrlicher_index_loest_keinen_fehlalarm_aus():
    """Der rsf-Fall: 127 Tage nach der Mai-Ausgabe ist der Index aktuell."""
    assert klassifiziere(127, 127, STANDARD) == VERALTET
    assert klassifiziere(127, 127, STANDARD, "jaehrlich") == FRISCH
    assert klassifiziere(420, 420, STANDARD, "jaehrlich") == VERALTET


def test_ereignisgetrieben_loest_nie_einen_alters_alarm_aus():
    for alter in (0, 200, 5000):
        assert klassifiziere(alter, alter, STANDARD, "ereignisgetrieben") == ANLASSBEZOGEN


# --------------------------------------------------------------------------
# Das Mass: Feld bei deklariertem Takt, sonst mtime
# --------------------------------------------------------------------------

def test_mit_takt_entscheidet_das_feld_nicht_die_mtime():
    """Genau der Fehler, der beim Bauen passiert ist: das Eintragen der
    Kadenz setzt die mtime auf 0 und hätte die Veraltung verdeckt."""
    assert klassifiziere(129, 0, STANDARD, "monatlich") == VERALTET
    assert klassifiziere(10, 0, STANDARD, "monatlich") == FRISCH


def test_ohne_takt_bleibt_die_mtime_fuehrend():
    """Das Mass aus PR #130 gilt unverändert weiter — dort lag fetched_at_iso
    zu 61 % daneben."""
    assert klassifiziere(129, 0, STANDARD) == FELD_UNGEPFLEGT
    assert klassifiziere(129, 200, STANDARD) == VERALTET
    assert klassifiziere(0, 200, STANDARD) == VERALTET


def test_mit_takt_aber_ohne_feld_faellt_auf_die_mtime_zurueck():
    assert klassifiziere(None, 300, STANDARD, "jaehrlich") == FRISCH
    assert klassifiziere(None, 500, STANDARD, "jaehrlich") == VERALTET
    assert klassifiziere("—", 100, STANDARD, "monatlich") == VERALTET


def test_feld_ungepflegt_gibt_es_nur_ohne_takt():
    """Bei deklariertem Takt ist ein altes Feld ein echter Befund, keine
    Buchhaltung — es darf nicht in der stillen Klasse landen."""
    assert klassifiziere(129, 0, STANDARD, "monatlich") != FELD_UNGEPFLEGT
    assert klassifiziere(129, 0, STANDARD) == FELD_UNGEPFLEGT


# --------------------------------------------------------------------------
# Die Deklarationen in den echten Dateien
# --------------------------------------------------------------------------

def _deklariert():
    treffer = {}
    for p in sorted((BACKEND / "data").glob("*.json")):
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(d, dict) and d.get("refresh_kadenz"):
            treffer[p.name] = d
    return treffer


def test_jede_deklarierte_kadenz_ist_gueltig():
    for name, d in _deklariert().items():
        assert d["refresh_kadenz"] in KADENZ_MAX_AGE, \
            f"{name}: unbekannter Takt {d['refresh_kadenz']!r}"


def test_jede_kadenz_traegt_ihre_begruendung():
    """Eine Kadenz ohne Begründung ist geraten — und eine falsche Kadenz ist
    schlimmer als gar keine, weil sie den Alarm dauerhaft abschaltet."""
    for name, d in _deklariert().items():
        assert d.get("kadenz_begruendung"), f"{name}: Begründung fehlt"
        assert len(d["kadenz_begruendung"]) > 30, f"{name}: Begründung zu dünn"


def test_die_bekannten_takte_stimmen():
    dek = _deklariert()
    for datei, takt in (("rki_surveillance.json", "woechentlich"),
                        ("frontex.json", "monatlich"),
                        ("wifo_ihs.json", "quartalsweise"),
                        ("rsf.json", "jaehrlich"),
                        ("freedom_house.json", "jaehrlich"),
                        ("wahlen.json", "ereignisgetrieben")):
        assert dek.get(datei, {}).get("refresh_kadenz") == takt, datei


def test_kadenz_bricht_die_dateien_nicht():
    for name, d in _deklariert().items():
        assert "fetched_at_iso" in d or name.startswith("euvsdisinfo"), \
            f"{name}: Kadenz deklariert, aber kein fetched_at_iso zum Messen"
