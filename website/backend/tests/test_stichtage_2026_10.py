"""Die sechs Stichtags-Funde vom 18.10.2026, je gegen die Primärquelle.

Nach PR #259 (der Wächter wurde erschlagen, bevor er zählte) blieben sechs
INFO-Funde in vier Fakten. Jeder einzeln geprüft — und zwei davon waren
gar nicht veraltet, sondern nur falsch formuliert.

## Was sich geändert hat

SIPRI — Yearbook 2026, Tabelle 8A.1, Stand Januar 2026. Aus dem
Kapitel-PDF gelesen, nicht aus einer Zusammenfassung: Der Welt-Gesamtbestand
ist 12.187 (zuvor im Fakt: 12.121 aus dem Yearbook 2024). SIPRI schreibt
ausdrücklich, dass die Januar-2026-Daten alle früheren SIPRI-Zahlen
ersetzen.

EU-Sanktionen — 21 nummerierte Pakete, das 21. am 23.7.2026 (zuvor im
Fakt: 14 bis Mai 2024).

## Was NICHT veraltet war

NATO — 32 Mitgliedstaaten, unverändert seit Schwedens Beitritt am
7.3.2024. Die ZAHL war richtig; die Formulierung "Stand März 2024" machte
aus einem dauerhaften Status eine Momentaufnahme. Jetzt am Beitritt
festgemacht statt an einem Stichtag.

EVALI — 2.807 hospitalisierte Fälle und 68 Todesfälle sind die
ENDGÜLTIGEN Zahlen. Die CDC beendete die Erhebung im Februar 2020, weil
die Ursache geklärt war und die Fälle seit dem Höhepunkt im September 2019
zurückgingen. Auch hier war nicht die Zahl das Problem, sondern das
"Stand Februar 2020", das sie wie einen Zwischenstand aussehen liess.

Daraus folgt eine Unterscheidung, die jetzt in den Daten steht: Ein
"Stand"-Marker gehört an eine Zahl, die eine neuere Fassung bekommen WIRD
(SIPRI jährlich, EU-Pakete laufend). An eine abgeschlossene Erhebung oder
einen Status gehört er nicht — dort erzeugt er nur jedes Jahr denselben
Fehlalarm.

## Zwei Zählweisen-Fälle als Nebenprodukt

Beim Lesen der SIPRI-Tabelle fiel auf, dass zwei gegenläufige Aussagen
BEIDE belegbar sind: Der Gesamtbestand sinkt (12.241 → 12.187), die
militärischen Bestände steigen (9.614 → 9.745). Und der Anteil von USA
plus Russland ist knapp 86 % am Gesamtbestand, aber rund 83 % an den
militärischen Beständen. Beides steht jetzt mit seiner Bezugsgrösse im
Fakt — dieselbe Lehre wie PR #254.

Dependency-light: nur Daten, kein Netz, kein Modell.
"""
import json
import os
from datetime import date

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")


def _fakt(datei, kennung):
    with open(os.path.join(DATA, datei), encoding="utf-8") as fh:
        d = json.load(fh)
    for f in d.get("facts") or []:
        if (f.get("id") or f.get("topic")) == kennung:
            return f
    raise AssertionError(f"{kennung} nicht in {datei}")


def _text(datei, kennung):
    return json.dumps(_fakt(datei, kennung), ensure_ascii=False)


# --- SIPRI: Yearbook 2026, Tabelle 8A.1, Januar 2026 -------------------

SIPRI = ("sicherheitspolitik_pack.json", "atomwaffen_faktoide_2026")

#: Gesamtbestand je Staat aus Tabelle 8A.1, Spalte "Total inventory 2026".
SIPRI_2026 = {
    "Russland": "5.420", "USA": "5.042", "China": "620",
    "Frankreich": "370", "UK": "225", "Indien": "190",
    "Pakistan": "170", "Israel": "90", "Nordkorea": "60",
}
SIPRI_SUMMEN = {
    "Welt-Gesamtbestand": "12.187",
    "militaerische Bestaende": "9.745",
    "stationiert": "4.012",
}


@pytest.mark.parametrize("land,wert", sorted(SIPRI_2026.items()))
def test_sipri_laenderzahlen_2026(land, wert):
    assert wert in _text(*SIPRI), f"{land}: {wert} fehlt"


@pytest.mark.parametrize("name,wert", sorted(SIPRI_SUMMEN.items()))
def test_sipri_summen_2026(name, wert):
    assert wert in _text(*SIPRI), f"{name}: {wert} fehlt"


#: Felder, die AUSSAGEN tragen — im Unterschied zu den trigger_*-Feldern,
#: die nur bestimmen, WANN der Fakt antwortet.
def _inhalt(datei, kennung) -> str:
    f = _fakt(datei, kennung)
    ohne = {k: v for k, v in f.items() if not k.startswith("trigger_")}
    return json.dumps(ohne, ensure_ascii=False)


def test_die_zahlen_aus_dem_yearbook_2024_sind_aus_den_aussagen_weg():
    """SIPRI schreibt selbst, dass Januar 2026 alle früheren Zahlen
    ersetzt. Zwei Jahrgänge nebeneinander wären die Lage, in der das
    Modell sich eine aussucht.

    Geprüft werden nur die AUSSAGE-Felder. Eine erste Fassung nahm den
    ganzen Fakt und schlug auf ``trigger_composite`` an — dort steht
    "12.121" absichtlich weiter: Wer diese Zahl behauptet, soll den Fakt
    finden und die aktuelle bekommen. Ein Trigger auf eine überholte
    Zahl ist kein Fehler, sondern der Zweck.
    """
    t = _inhalt(*SIPRI)
    for alt in ("12.121", "5.580", "5.044", "9.585", "3.904"):
        assert alt not in t, f"{alt} (Yearbook 2024) steht noch in einer Aussage"


def test_die_ueberholte_zahl_bleibt_ein_trigger():
    """Die Gegenprobe zum Test oben — sonst könnte jemand die Trigger
    mit aufräumen und niemand merkt, dass der Fakt auf die alte
    Behauptung nicht mehr antwortet."""
    f = _fakt(*SIPRI)
    tokens = [x for gruppe in (f.get("trigger_composite") or []) for x in gruppe]
    assert "12.121" in tokens, "Trigger auf die alte Zahl verloren"
    assert "12.187" in tokens, "Trigger auf die neue Zahl fehlt"


def test_sipri_nennt_beide_bezugsgroessen():
    """Knapp 86 % am Gesamtbestand, rund 83 % an den militärischen
    Beständen — beide Zahlen stehen so im SIPRI-Kapitel und meinen
    Verschiedenes. Nachgerechnet: 10.462/12.187 = 85,8 %,
    8.100/9.745 = 83,1 %."""
    t = _text(*SIPRI)
    assert "86" in t and "83" in t
    assert "~90 %" not in t, "die alte Pauschal-Angabe steht noch da"


def test_sipri_nennt_die_gegenlaeufige_richtung():
    t = _text(*SIPRI)
    assert "12.241" in t, "Vorjahreswert des Gesamtbestands fehlt"
    assert "9.614" in t, "Vorjahreswert der militärischen Bestände fehlt"


def test_sipri_behaelt_einen_stand_marker():
    """Das Yearbook erscheint jährlich — in zwei Jahren SOLL der
    Wächter das hier wieder melden."""
    assert "Stand Januar 2026" in _text(*SIPRI)


def test_die_china_projektion_ist_als_fremde_zahl_markiert():
    """1.500 bis 2030 ist eine Schätzung des US-Verteidigungsministeriums,
    keine SIPRI-Zahl. Ungeprüft übernommen, aber nicht als SIPRI
    ausgegeben — nichts ersetzen, was man nicht belegen kann, und nichts
    zuschreiben, was man nicht geprüft hat."""
    t = _text(*SIPRI)
    assert "1.500" in t
    assert "keine SIPRI-Zahl" in t


# --- EU-Sanktionen: 21 Pakete, Stand Juli 2026 ------------------------

SANKT = ("welthandel_pack.json", "russland_sanktionen_wirkung_2026")


def test_eu_sanktionspakete_sind_21():
    t = _text(*SANKT)
    assert "21 nummerierte Pakete" in t
    assert "23.7.2026" in t and "23.4.2026" in t
    assert "14 Pakete" not in t, "die alte Zahl steht noch da"


def test_pakete_und_listungen_sind_getrennt():
    """Die Listungs-Runde vom 12.10.2026 hat keine Paket-Nummer. Wer
    beides vermischt, bekommt zwei ganz verschiedene Zahlen."""
    t = _text(*SANKT)
    assert "1.569" in t and "4.600" in t
    assert "Listungs" in t or "Listungen" in t


def test_sanktionen_behalten_einen_stand_marker():
    """Pakete kommen laufend dazu — der Wächter soll wieder melden."""
    assert "Stand Juli 2026" in _text(*SANKT)


# --- NATO: die Zahl war richtig, die Formulierung nicht ---------------

NATO = ("sicherheitspolitik_pack.json", "nato_mitgliedstaaten_at_diskurs_2026")


def test_nato_hat_weiterhin_32_mitglieder():
    t = _text(*NATO)
    assert "32" in t
    assert "7.3.2024" in t, "das Beitrittsdatum Schwedens fehlt"


def test_nato_traegt_keinen_stand_marker_mehr():
    """32 Staaten sind ein Status, kein Momentanwert. Ein
    "Stand"-Marker daran erzeugt jedes Jahr denselben Fehlalarm, ohne
    dass sich etwas geändert hätte."""
    assert "Stand März 2024" not in _text(*NATO)


# --- EVALI: endgültige Zahlen, abgeschlossene Erhebung ---------------

VAPE = ("substanzen_pack.json", "vape_e_zigarette_sicherheit_mythos")


def test_evali_zahlen_bleiben():
    """2.807 und 68 sind die letzten von der CDC gemeldeten Zahlen und
    damit endgültig. Sie zu ändern wäre falsch gewesen."""
    t = _text(*VAPE)
    assert "2.807" in t and "68" in t


def test_evali_sagt_dass_die_erhebung_beendet_ist():
    t = _text(*VAPE)
    assert "ENDGUELTIG" in t.upper() or "endgueltige" in t
    assert "beendete" in t or "eingestellt" in t
    assert "Februar 2020" in t, "wann die Erhebung endete, fehlt"


def test_evali_traegt_keinen_stand_marker_mehr():
    assert "Stand Februar 2020" not in _text(*VAPE)


def test_evali_nennt_die_weitere_beobachtung():
    """Ohne diesen Satz liest sich "Erhebung beendet" wie "niemand
    schaut mehr hin"."""
    t = _text(*VAPE)
    assert "Syndromic" in t or "Notaufnahme" in t


# --- Das Gesamtbild ---------------------------------------------------

def test_der_waechter_findet_nichts_mehr():
    """Zurück auf die Basislinie 0/0 aus #127/#128.

    Die Prüfung läuft über den echten Scanner auf den echten Daten —
    nicht über eine Liste erledigter IDs, die beim nächsten neuen Fund
    nichts sagt.
    """
    import sys
    sys.path.insert(0, os.path.join(BACKEND, "tools"))
    import stichtag_check as sc
    from pathlib import Path

    funde, _uebersprungen = sc.scanne(Path(DATA), date.today(), 24)
    assert not funde, (
        f"{len(funde)} Stichtags-Fund(e): "
        + ", ".join(f"{f['id']} ({f['fund']})" for f in funde[:6]))


def test_die_vier_fakten_tragen_ihr_neues_jahr():
    """``year`` geht als Vintage-Label ins Ergebnis und damit zum
    Modell. 2024 auf 2026er Zahlen wäre dieselbe Fehl-Etikettierung,
    die hier behoben wird."""
    assert _fakt(*SIPRI)["year"] == 2026
    assert _fakt(*SANKT)["year"] == 2026
