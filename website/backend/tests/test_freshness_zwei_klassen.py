"""Freshness-Check trennt echten Refresh-Bedarf von ungepflegter Buchhaltung.

Befund (2026-09-05): Der Job meldete 51 Dateien als „stale" und schickte
seit dem 31.08. einen ALERT, den niemand bearbeitet hat. Beim Nachrechnen:
**31 der 51 (61 %) waren Fehlalarme.** Er mass `fetched_at_iso` — ein Feld,
das beim Bearbeiten einer Datei niemand mitzieht. `at_courts.json` galt als
129 Tage alt, obwohl es am selben Tag bearbeitet worden war (Kopftuch-Fix).

Ein Alarm, der zu 61 % danebenliegt, trainiert an, ihn zu ignorieren —
genau deshalb lag der ALERT eine Woche unbeachtet. Dieselbe Lehre wie beim
Stichtags-Wächter, nur andersherum: dort musste die Basislinie gesäubert
werden, hier war das MASS falsch.

Die echte Änderung steht in der Datei-mtime: Der Docker-Build kopiert aus
dem git-Checkout, und `git pull` setzt die mtime auf den Pull-Zeitpunkt.
Auf prod verifiziert — **86 von 87 Dateien haben mtime == Datum des letzten
Commits**; die einzige Abweichung ist `mitre_attack.json`, ein zur Laufzeit
neu geschriebener Cache (und der wird ohnehin separat geprüft).

Getestet wird die reine Klassifikation, NICHT die echten Dateien: In der CI
ist jede mtime der Checkout-Zeitpunkt und als Testgrundlage wertlos.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

import data_freshness_check as fc  # noqa: E402

MAX = 120


def test_datei_lange_nicht_angefasst_ist_veraltet():
    """Der echte Refresh-Bedarf — nur DAS soll Alarm auslösen."""
    assert fc.klassifiziere(200, 200, MAX) == fc.VERALTET


def test_feld_alt_aber_datei_frisch_ist_nur_buchhaltung():
    """Der reale at_courts-Fall: Feld 129 d, Datei am selben Tag geändert."""
    assert fc.klassifiziere(129, 0, MAX) == fc.FELD_UNGEPFLEGT


def test_beides_frisch_ist_frisch():
    assert fc.klassifiziere(10, 10, MAX) == fc.FRISCH


def test_datei_alt_schlaegt_feld_frisch():
    """Auch wenn das Feld frisch behauptet zu sein: Wenn die Datei seit
    Monaten niemand angefasst hat, ist das der stärkere Befund."""
    assert fc.klassifiziere(5, 200, MAX) == fc.VERALTET


def test_fehlendes_feld_bricht_nicht():
    """Manche Dateien tragen kein fetched_at_iso — die mtime entscheidet
    dann allein."""
    assert fc.klassifiziere(None, 200, MAX) == fc.VERALTET
    assert fc.klassifiziere(None, 5, MAX) == fc.FRISCH


@pytest.mark.parametrize("age", [119, 120])
def test_schwelle_ist_exklusiv(age):
    """Genau auf der Schwelle ist noch frisch — sonst wandert die Grenze
    je nach Rundung."""
    assert fc.klassifiziere(age, age, MAX) == fc.FRISCH


def test_ein_tag_ueber_der_schwelle_schlaegt_an():
    assert fc.klassifiziere(121, 121, MAX) == fc.VERALTET


def test_nur_die_echte_klasse_loest_alarm_aus():
    """Vertrag mit dem Cron: Webhook und Exit 1 hängen an `stale_files`,
    und dort landet ausschließlich VERALTET. Wäre FELD_UNGEPFLEGT dabei,
    pushte der Job ab Tag eins 31 bekannte Fundstellen."""
    quelle = (Path(__file__).resolve().parents[1]
              / "tools" / "data_freshness_check.py").read_text(encoding="utf-8")
    assert "if (stale_files or cache_problems) and args.alert_webhook:" in quelle
    assert "if (stale_files or cache_problems) and args.strict:" in quelle
    assert "feld_ungepflegt" in quelle
    # feld_ungepflegt darf in KEINER Alarm-Bedingung vorkommen
    for zeile in quelle.splitlines():
        if "feld_ungepflegt" in zeile and ("args.strict" in zeile
                                           or "alert_webhook" in zeile):
            pytest.fail(f"FELD_UNGEPFLEGT löst Alarm aus: {zeile.strip()}")
