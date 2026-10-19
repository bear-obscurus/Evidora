"""Zwei Dateien mit 170 Tagen — und nur eine war wirklich fällig.

Der Freshness-Check meldete neun Dateien über ihrer Schwelle. Beim
Nachsehen stellte sich heraus, dass **acht von neun kein
``refresh_kadenz``-Feld haben** und darum gegen den 120-Tage-Standard
gemessen werden, egal in welchem Takt ihre Quelle erscheint. PR #139 hat
die Kadenz-Mechanik gebaut und auf 14 von 85 Dateien angewandt; die
übrigen 71 fallen auf den Standard.

Zwei der neun nennen ihre Kadenz sogar — aber in **Prosa**, die der
Wächter nicht liest.

## retraction_watch: Schwellenwert-Artefakt plus ein echter Zugang

Die Datei führt Rücknahme-KLASSIKER, deren Fakten sich nicht mehr ändern
(Wakefield 1998, Schön 2002, Surgisphere 2020). Ihr ``note``-Feld sagt seit
Anlage „Refresh: jährlich oder bei prominenten neuen Retractions" — bei
``jaehrlich`` (400 Tage) wären 170 Tage unauffällig.

Die Kadenz zu deklarieren, OHNE nachzusehen, wäre Stillstellen gewesen.
Nachgesehen: Im Oktober 2026 wurde ein als peer-reviewed zitiertes Papier
gegen den menschlichen Anteil am Klimawandel zurückgezogen — genau der
Desinformations-Vektor, für den diese Sammlung da ist. Aufgenommen.

DOI und Titel sind **unabhängig über Crossref geprüft**, nicht aus dem
MDPI-Nummernmuster erraten: Retraction Watch nannte den Titel nicht, die
Bandangabe (Atmosphere 14(8), 1244) legte ein DOI-Muster nahe, und ein
Muster ist kein Beleg (#235, #247).

## dach_factbook: echt fällig, und mit einem Methodenbruch

Die Datei nennt in Prosa zwei Takte, „vierteljährlich für
Asyl-Statistiken, jährlich für Sozialleistungen". Für den Wächter gilt der
KÜRZERE — bei ``quartalsweise`` (150 Tage) meldet er bei 174 Tagen weiter,
und das ist richtig: der Asyl-Teil war wirklich fällig.

Beim Nachziehen kam heraus, dass das BAMF Folgeanträge seit dem 12.06.2026
anders zählt und selbst schreibt, die Zahlen seien „nicht mehr
uneingeschränkt mit früheren Zeiträumen vergleichbar". Für die
Vergleichbarkeit wurde „Neuzugänge in das Asylsystem" eingeführt.

Daraus folgte die eigentliche Arbeit: **das DE-zu-AT-plus-CH-Verhältnis
von 3,9 ist nicht mehr berechenbar** — drei Länder, drei Zeitfenster
(DE bis September, AT bis Juli, CH bis August), drei Kategorien
(Neuzugänge / Erst- und Mehrfachantragsteller / Asylgesuche), plus der
Bruch. Es verglich schon vorher ungleiche Kategorien, nur ohne es zu
sagen; der Bruch hat es sichtbar gemacht.

Ersetzt wurde es nicht durch eine neue Zahl, sondern durch die Rechnung
mit ihren Annahmen: Monatsmittel im jeweils eigenen Fenster ergibt grob
Faktor 2,4 — mit dem Hinweis, dass Asylanträge saisonal schwanken.

## Nicht deklariert

``zaehlweisen_namen`` (#254). Der Guard macht aus Vergleichen und
Superlativen ``mixed``, wenn die Behauptung keine Zählweise nennt. Hier
geben die Zählweisen aber KEINE gegensätzlichen Antworten — Erstanträge
51.982 und Neuzugänge 55.366 zeigen in dieselbe Richtung. Eine Deklaration
würde korrekte Antworten abwerten, dieselbe Überlegung wie beim Gender Pay
Gap in #256.

Dependency-light: nur Daten, kein Netz, kein Modell.
"""
import json
import os

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")


def _datei(name):
    with open(os.path.join(DATA, name), encoding="utf-8") as fh:
        return json.load(fh)


def _fakt(name, kennung):
    for f in _datei(name).get("facts") or []:
        if (f.get("id") or f.get("topic")) == kennung:
            return f
    raise AssertionError(f"{kennung} nicht in {name}")


# --- Die Kadenz steht jetzt im FELD, nicht in Prosa -------------------

@pytest.mark.parametrize("name,kadenz", [
    ("retraction_watch.json", "jaehrlich"),
    ("dach_factbook.json", "quartalsweise"),
])
def test_die_kadenz_ist_deklariert(name, kadenz):
    d = _datei(name)
    assert d.get("refresh_kadenz") == kadenz
    assert d.get("kadenz_begruendung"), "ohne Begruendung ist es eine Behauptung"


@pytest.mark.parametrize("name", ["retraction_watch.json", "dach_factbook.json"])
def test_die_kadenz_ist_dem_waechter_bekannt(name):
    """Eine Kadenz, die ``KADENZ_MAX_AGE`` nicht kennt, faellt still auf
    den Standard zurueck — dann waere die Deklaration wirkungslos."""
    import sys
    sys.path.insert(0, os.path.join(BACKEND, "tools"))
    from data_freshness_check import KADENZ_MAX_AGE
    assert _datei(name)["refresh_kadenz"] in KADENZ_MAX_AGE


def test_dach_nimmt_den_kuerzeren_von_zwei_takten():
    """Die Datei fuehrt zwei Rhythmen. Haette sie ``jaehrlich``
    deklariert, waere der faellige Asyl-Teil stillgestellt worden."""
    import sys
    sys.path.insert(0, os.path.join(BACKEND, "tools"))
    from data_freshness_check import KADENZ_MAX_AGE
    d = _datei("dach_factbook.json")
    assert KADENZ_MAX_AGE[d["refresh_kadenz"]] < KADENZ_MAX_AGE["jaehrlich"]
    assert "KUERZERE" in d["kadenz_begruendung"]


# --- retraction_watch: der neue Eintrag -------------------------------

def test_der_klima_eintrag_ist_da():
    d = _datei("retraction_watch.json")
    e = [r for r in d["retractions"] if r["id"] == "harris_2023_atmosphere_klima"]
    assert len(e) == 1
    r = e[0]
    assert r["doi"] == "10.3390/atmos14081244"
    assert r["publication_year"] == 2023
    assert r["retraction_year"] == 2026
    assert "16.09.2026" in r["retraction_reason"]


def test_die_doi_ist_als_geprueft_vermerkt():
    """Die Bandangabe legte das DOI-Muster nahe — ein Muster ist kein
    Beleg. Steht das nicht dabei, haelt der naechste es fuer geraten."""
    p = _datei("retraction_watch.json")["letzte_pruefung"]
    assert "Crossref" in p
    assert "10.3390/atmos14081244" in p
    assert "erraten" in p or "nicht aus dem" in p


def test_der_eintrag_sagt_dass_zurueckgezogen_nichts_belegt():
    """Der Zweck der Sammlung: Ein zurueckgezogenes Papier wird weiter
    zitiert, weil die PDF-Datei auffindbar bleibt."""
    r = next(x for x in _datei("retraction_watch.json")["retractions"]
             if x["id"] == "harris_2023_atmosphere_klima")
    assert "belegt nichts mehr" in r["common_misinformation"]


def test_die_fuenf_klassiker_sind_unveraendert():
    ids = {r["id"] for r in _datei("retraction_watch.json")["retractions"]}
    for alt in ("wakefield_1998_lancet", "raoult_2020_hydroxychloroquin",
                "schoen_physik_2002", "surgisphere_2020_lancet",
                "bohannon_2015_chocolate"):
        assert alt in ids, alt
    assert len(ids) == 6


# --- dach_factbook: die neuen Zahlen und der Bruch --------------------

ASYL_DE = ("dach_factbook.json", "bamf_asyl_de_2025_2026")


def test_die_neuen_deutschen_zahlen_stehen_im_fakt():
    d = _fakt(*ASYL_DE)["data"]
    assert d["neuzugaenge_jan_sep_2026"] == 55366
    assert d["neuzugaenge_september_2026"] == 5276
    assert d["erstantraege_september_2026"] == 4217
    assert d["gesamtschutzquote_jan_sep_2026_pct"] == 36.4
    assert d["anhaengige_verfahren_ende_september_2026"] == 51234


def test_der_methodenbruch_ist_benannt():
    """Das BAMF sagt es selbst. Steht es nicht im Fakt, rechnet das
    Modell eine Zahl von vor Juni gegen eine danach."""
    t = json.dumps(_fakt(*ASYL_DE), ensure_ascii=False)
    assert "12.06.2026" in t
    assert "nicht mehr uneingeschraenkt" in t
    assert "Neuzugaenge in das Asylsystem" in t


def test_die_alten_q1_zahlen_bleiben_mit_warnung():
    """Nicht geloescht — sie sind ein historischer Stand. Aber mit dem
    Hinweis, dass sie nicht mit den Neuzugaengen zu verrechnen sind."""
    d = _fakt(*ASYL_DE)["data"]
    assert d["asylantraege_q1_2026_gesamt"] == 32833
    assert "VOR dem 12.06.2026" in d["q1_2026_hinweis"]


VERGLEICH = ("dach_factbook.json", "dach_asyl_vergleich_2026")


def test_das_einzelne_verhaeltnis_ist_ersetzt():
    d = _fakt(*VERGLEICH)["data"]
    assert "verhaeltnis_de_zu_at_plus_ch" not in d, (
        "das pauschale 3,9-fach-Verhaeltnis steht wieder da")
    assert "KEIN_EINZELNES_VERHAELTNIS" in d


def test_alle_drei_gruende_stehen_dabei():
    """Eine Sperre ohne Begruendung liest sich wie Willkuer."""
    s = _fakt(*VERGLEICH)["data"]["KEIN_EINZELNES_VERHAELTNIS"]
    assert "Zeitfenster" in s
    assert "Kategorien" in s
    assert "Methodenbruch" in s
    assert "3,9" in s, "der frueher gefuehrte Wert fehlt — dann weiss " \
                       "niemand, was ersetzt wurde"


def test_jede_laenderzahl_traegt_ihr_fenster_und_ihre_basis():
    d = _fakt(*VERGLEICH)["data"]
    assert d["de_jan_sep_2026_neuzugaenge"] == 55366
    assert d["at_jan_jul_2026_gesamt"] == 5743
    assert d["ch_jan_aug_2026_asylgesuche"] == 13545
    assert "Erstantragsteller" in d["at_jan_jul_2026_zusammensetzung"]
    assert "Jan-Sep-Summe" in d["ch_jan_aug_2026_hinweis"], (
        "dass fuer die Schweiz keine Jan-Sep-Summe veroeffentlicht war, "
        "muss dastehen — sonst sieht die Luecke aus wie Nachlaessigkeit")


def test_die_groessenordnung_nennt_ihre_annahmen():
    """Faktor 2,4 ist eine Rechnung, keine amtliche Zahl. Ohne die
    Annahmen daneben wird daraus das naechste pauschale Verhaeltnis."""
    s = _fakt(*VERGLEICH)["data"]["groessenordnung_statt_verhaeltnis"]
    assert "2,4" in s
    assert "saisonal" in s
    assert "Monatsmittel" in s


def test_das_2025er_verhaeltnis_bleibt_mit_basis_angabe():
    """2025 liegt vollstaendig vor dem Bruch — dort ist der Vergleich
    zulaessig, und das gehoert dazugesagt."""
    s = _fakt(*VERGLEICH)["data"]["verhaeltnis_de_zu_at_plus_ch_2025"]
    assert "4,7" in s
    assert "VOR dem Methodenbruch" in s


# --- Was bewusst nicht geschah ---------------------------------------

def test_keine_zaehlweisen_deklaration():
    """Der Guard aus #254 macht aus Vergleichen ``mixed``. Hier zeigen
    die Zaehlweisen in DIESELBE Richtung (Erstantraege 51.982,
    Neuzugaenge 55.366) — eine Deklaration wuerde korrekte Antworten
    abwerten. Dieselbe Ueberlegung wie beim Gender Pay Gap in #256.
    """
    import sys
    sys.path.insert(0, os.path.join(BACKEND, "services"))
    for name, kennung in (ASYL_DE, VERGLEICH):
        d = _fakt(name, kennung).get("data") or {}
        assert "zaehlweisen_namen" not in d


def test_nur_die_geprueften_fakten_tragen_das_neue_datum():
    """Neun der elf Fakten sind jaehrlich getaktet und wurden in dieser
    Runde nicht geprueft. Ihr Datum hochzuziehen waere eine Zusage, die
    niemand geprueft hat."""
    d = _datei("dach_factbook.json")
    neu = {f.get("id") for f in d["facts"]
           if f.get("fetched_at_iso") == "2026-10-19"}
    assert neu == {"bamf_asyl_de_2025_2026", "dach_asyl_vergleich_2026"}, neu
    assert "NICHT geprueft" in d["letzte_pruefung"]


# --- Der Wächter ist zufrieden ---------------------------------------

@pytest.mark.parametrize("name", ["retraction_watch.json", "dach_factbook.json"])
def test_der_freshness_check_meldet_die_datei_nicht_mehr(name):
    """Ueber das echte Werkzeug, nicht ueber eine nachgebaute Rechnung."""
    import sys
    from datetime import date
    sys.path.insert(0, os.path.join(BACKEND, "tools"))
    import data_freshness_check as fc

    d = _datei(name)
    kadenz = d["refresh_kadenz"]
    schwelle = fc.KADENZ_MAX_AGE[kadenz]
    alter = (date.today() - date.fromisoformat(d["fetched_at_iso"])).days
    assert schwelle is None or alter <= schwelle, (
        f"{name}: {alter} d gegen Schwelle {schwelle} d")
