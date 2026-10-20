"""Der Vermerk stand seit Mai da. Niemand hat ihn abgearbeitet.

``data/mipex.json`` trug seit seiner Anlage:

    audit_flag: AUDIT NEEDED — MIPEX-2020-Werte sind LLM-Approximationen
    aus dem öffentlich publizierten Ranking. Vor produktivem Einsatz mit
    offiziellem Excel-Export gegenchecken.

Gegengecheckt wurde nie, und ``mipex.py`` servierte die Zahlen seit Mai
2026 als MIPEX-Punktzahlen — in Sätzen wie „AT 46/100 Gesamt (Rang 28/52)
— Anti-Diskriminierung 78, Familiennachzug 31, Einbürgerung 13".

## Gemessen

Gegen die archivierten mipex.eu-Länderseiten der MIPEX-2020-Zeit, die die
Werte als ``data-score``-Attribute ausliefern: **103 von 198 Werten
wichen ab, 52 %.** Fehlerfrei waren nur Kanada und Deutschland.

    Ungarn     Anti-Diskriminierung  publiziert  96  im Fakt  50
    Ungarn     Bildung               publiziert   0  im Fakt  39
    Polen      Gesundheit            publiziert  27  im Fakt  65
    Griechenl. Arbeitsmarkt          publiziert  61  im Fakt  25
    Irland     Arbeitsmarkt          publiziert  22  im Fakt  56
    Österreich Anti-Diskriminierung  publiziert  53  im Fakt  78

Die GESAMTSCORES waren alle richtig — nur die Bereichswerte nicht.

## Eine Korrektur an meiner eigenen ersten Messung

Nach drei Ländern (AT, DE, SE) hatte ich berichtet, die Fehler häuften
sich bei Österreich. Das war falsch: Ich hatte zufällig zwei der nur zwei
sauberen Länder in der Stichprobe. Eine Stichprobe von drei bei 22 sagt
nichts über die Verteilung — erst der Lauf über alle zeigte die 52 %.

## Der Rang war auch nicht belegbar

„Rang 28/52" stand so im servierten Text. Die 52 war im Code
hartkodiert, das Datenfeld ``rank_total_in_index`` wurde nie gelesen. Und
der Rang widersprach den eigenen Zahlen: Australien führte 65 Punkte auf
Rang 12, Irland 64 auf Rang 11 — ein höherer Score kann keinen
schlechteren Rang haben.

Ersetzt durch ``rang_im_ausschnitt``: die Position unter den 22 erfassten
Ländern, abgeleitet aus den belegten Gesamtscores, und im Text als solche
benannt. Die Position unter allen 52 MIPEX-Ländern ließe sich mit 22
Ländern ohnehin nicht bestimmen.

## Warum das Archiv und nicht die Quelle

Die heutigen mipex.eu-Länderseiten zeigen für die acht Politikbereiche nur
noch Einstufungen („halfway favourable"), keine Zahlen. Auf ``/download``
liegen nur Methodik-Dateien, kein Scores-Export. MIPEX 2025 existiert
(Österreich dort 47 statt 46 im Gesamtscore), aber die Bereichszahlen von
2020 sind die letzten, die sich belegen lassen. Das steht jetzt im
``methodik_caveat``.

## Ein Fehler in meinem eigenen Helfer

``_laender_im_ausschnitt`` hatte in der ersten Fassung ein
``except Exception: return 0`` — und verschluckte damit ein fehlendes
``import json``. Die Anzeige hätte „Platz 19 von 0" gelautet, und niemand
hätte es bemerkt. Genau die Fehlerklasse, die PR #238 bis #242 aus diesem
Code entfernt haben, von mir neu eingebaut. Jetzt wirft er.

Dependency-light: nur Daten und der Display-Builder, kein Netz.
"""
import json
import os

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATEI = os.path.join(BACKEND, "data", "mipex.json")


def _daten():
    with open(DATEI, encoding="utf-8") as fh:
        return json.load(fh)


#: Aus den archivierten Länderseiten gelesen (``data-score``-Attribute),
#: Stand der MIPEX-2020-Ausgabe. Werkzeug: ``tools/mipex_aus_archiv.py``.
#: Nur die Fälle, die vorher falsch waren — als Beleg, dass die Korrektur
#: angekommen ist.
BELEGT = {
    "AUT": {"anti_discrimination": 53, "labour_market_mobility": 59,
            "health": 81, "permanent_residence": 50, "family_reunion": 36,
            "education": 52},
    "HUN": {"anti_discrimination": 96, "education": 0,
            "labour_market_mobility": 37, "access_to_nationality": 25},
    "POL": {"health": 27, "anti_discrimination": 63, "education": 33},
    "GRC": {"labour_market_mobility": 61, "permanent_residence": 46},
    "IRL": {"labour_market_mobility": 22, "anti_discrimination": 94},
    "AUS": {"health": 79, "labour_market_mobility": 37},
    "FIN": {"anti_discrimination": 100},
    "SWE": {"anti_discrimination": 100},
    "CHE": {"health": 83, "labour_market_mobility": 63},
}

#: Die Werte, die vorher dastanden. Keiner darf mehr vorkommen.
FRUEHER_FALSCH = {
    "AUT": {"anti_discrimination": 78, "labour_market_mobility": 45,
            "health": 73},
    "HUN": {"anti_discrimination": 50, "education": 39},
    "POL": {"health": 65},
    "GRC": {"labour_market_mobility": 25},
    "IRL": {"labour_market_mobility": 56},
    "AUS": {"health": 47},
}


@pytest.mark.parametrize("iso", sorted(BELEGT))
def test_die_belegten_werte_stehen_im_fakt(iso):
    rec = _daten()["scores"][iso]
    for feld, wert in BELEGT[iso].items():
        assert rec[feld] == wert, f"{iso}.{feld}: {rec[feld]} statt {wert}"


@pytest.mark.parametrize("iso", sorted(FRUEHER_FALSCH))
def test_die_alten_werte_sind_weg(iso):
    rec = _daten()["scores"][iso]
    for feld, falsch in FRUEHER_FALSCH[iso].items():
        assert rec[feld] != falsch, f"{iso}.{feld} steht wieder auf {falsch}"


def test_die_gesamtscores_sind_unveraendert():
    """Sie waren richtig. Ein Refresh, der sie mitaendert, hat etwas
    anderes getan als korrigiert."""
    s = _daten()["scores"]
    for iso, wert in (("AUT", 46), ("DEU", 58), ("SWE", 86), ("HUN", 43),
                      ("POL", 40), ("CAN", 80)):
        assert s[iso]["overall"] == wert, iso


def test_alle_22_laender_haben_alle_neun_werte():
    """Ein Land mit fehlendem Bereich waere stillschweigend unvollstaendig
    — der Display-Builder laesst fehlende Felder einfach weg."""
    felder = {"overall", "labour_market_mobility", "family_reunion",
              "education", "political_participation", "permanent_residence",
              "access_to_nationality", "anti_discrimination", "health"}
    s = _daten()["scores"]
    assert len(s) == 22, len(s)
    for iso, rec in s.items():
        fehlt = felder - set(rec)
        assert not fehlt, f"{iso}: {fehlt}"


# --- Der Rang ---------------------------------------------------------

def test_der_alte_index_rang_ist_weg():
    d = _daten()
    assert "rank_total_in_index" not in d
    for iso, rec in d["scores"].items():
        assert "rank" not in rec, f"{iso} traegt wieder einen rank"


def test_der_neue_rang_folgt_den_belegten_scores():
    """Der alte Rang tat das nicht: AUS 65 auf Rang 12, IRL 64 auf Rang 11.
    Ein hoeherer Score kann keinen schlechteren Rang haben."""
    s = _daten()["scores"]
    # Bei GLEICHEM Score ist die Rang-Reihenfolge frei — deshalb als
    # zweites Kriterium den Rang selbst, sonst meldet der Test einen
    # Gleichstand als Verstoss. (Erste Fassung sortierte nur nach Score
    # und schlug auf AUT/GRC bei je 46 Punkten an.)
    paare = sorted(((rec["overall"], rec["rang_im_ausschnitt"], iso)
                    for iso, rec in s.items()), key=lambda x: (-x[0], x[1]))
    vorher = 0
    for score, rang, iso in paare:
        assert rang >= vorher, (
            f"{iso}: Score {score}, Rang {rang} — niedriger als der "
            f"vorige Rang {vorher} bei hoeherem Score")
        vorher = rang


def test_die_raenge_sind_eine_luecken_lose_folge():
    s = _daten()["scores"]
    raenge = sorted(rec["rang_im_ausschnitt"] for rec in s.values())
    assert raenge == list(range(1, len(s) + 1)), raenge


def test_die_laenderzahl_steht_in_den_daten():
    """Die alte "/52" war im CODE hartkodiert und die Datenangabe wurde
    nie gelesen. Eine Zahl im Satz, die niemand nachrechnet, bleibt
    stehen, auch wenn sie nicht mehr stimmt."""
    d = _daten()
    assert d["laender_im_ausschnitt"] == len(d["scores"]) == 22


# --- Was der Dienst serviert -----------------------------------------

def _display(iso):
    import sys
    sys.path.insert(0, BACKEND)
    from services.mipex import _build_display_value
    s = _daten()["scores"]
    andere = [i for i in ("SWE", "DEU", "CHE") if i != iso][:3]
    return _build_display_value(iso, s[iso], andere, s, None)


def test_der_servierte_satz_nennt_den_ausschnitt():
    t = _display("AUT")
    assert "Platz 19 von 22" in t
    assert "nicht im vollen MIPEX-Index" in t
    assert "/52" not in t, "die hartkodierte 52 steht wieder im Satz"


def test_der_servierte_satz_traegt_die_belegten_zahlen():
    t = _display("AUT")
    assert "46/100" in t
    assert "Gesundheit 81" in t, "der alte Wert 73 wird noch serviert"
    assert "Einbürgerung 13" in t


def test_der_servierte_satz_nennt_die_ausgabe():
    assert "MIPEX 2020" in _display("DEU")


# --- Der Helfer verschluckt nichts mehr ------------------------------

def test_die_laenderzahl_wird_nicht_still_null():
    """Die erste Fassung hatte ``except Exception: return 0`` und
    verschluckte ein fehlendes ``import json``. "Platz 19 von 0" waere
    niemandem aufgefallen."""
    import sys
    sys.path.insert(0, BACKEND)
    from services.mipex import _LAENDER_IM_AUSSCHNITT, _laender_im_ausschnitt
    assert _LAENDER_IM_AUSSCHNITT == 22
    quelle = open(os.path.join(BACKEND, "services", "mipex.py"),
                  encoding="utf-8").read()
    i = quelle.index("def _laender_im_ausschnitt")
    j = quelle.index("_LAENDER_IM_AUSSCHNITT = ", i)
    # Nur den CODE pruefen, nicht die Kommentare — dort steht
    # `except Exception` als Erklaerung des alten Fehlers, und die erste
    # Fassung dieses Tests schlug auf ihrer eigenen Dokumentation an.
    code = "\n".join(z for z in quelle[i:j].splitlines()
                     if not z.lstrip().startswith("#"))
    assert "except Exception" not in code, (
        "der Helfer verschluckt wieder Fehler")
    assert "raise" in code


def test_json_ist_auf_modulebene_importiert():
    """Der konkrete Fehler, den das `except` verdeckte."""
    quelle = open(os.path.join(BACKEND, "services", "mipex.py"),
                  encoding="utf-8").read()
    assert "\nimport json\n" in quelle


# --- Die Metadaten sagen, was gilt -----------------------------------

def test_der_audit_vermerk_ist_abgearbeitet():
    a = _daten()["audit_flag"]
    assert "AUDIT NEEDED" not in a, "der Vermerk steht noch offen da"
    assert "GEPRUEFT 2026-10-20" in a
    assert "103 von 198" in a, "die Messung fehlt"
    assert "mipex_aus_archiv.py" in a, "das Werkzeug ist nicht genannt"


def test_der_caveat_nennt_die_neuere_ausgabe():
    """Die Datei behauptete, MIPEX 2020 sei die letzte Ausgabe. MIPEX
    2025 existiert — mit Oesterreich auf 47 statt 46."""
    c = _daten()["methodik_caveat"]
    assert "MIPEX 2025" in c
    assert "47 statt 46" in c
    assert "halfway favourable" in c, (
        "dass die heutigen Seiten nur Einstufungen zeigen, fehlt — dann "
        "sieht der 2020er-Stand wie Nachlaessigkeit aus")


def test_die_kadenz_ist_deklariert():
    import sys
    sys.path.insert(0, os.path.join(BACKEND, "tools"))
    from data_freshness_check import KADENZ_MAX_AGE
    d = _daten()
    assert d["refresh_kadenz"] == "jaehrlich"
    assert d["refresh_kadenz"] in KADENZ_MAX_AGE
    assert d["fetched_at_iso"] == "2026-10-20"


# --- Das Werkzeug ist wiederholbar -----------------------------------

def test_das_werkzeug_liest_eine_archivseite():
    """Ohne diesen Test ist die Korrektur eine Behauptung ueber einen
    Lauf, den niemand nachvollziehen kann."""
    import sys
    sys.path.insert(0, os.path.join(BACKEND, "tools"))
    from mipex_aus_archiv import BEREICHE, lies_seite, vergleiche
    assert len(BEREICHE) == 8
    # Eine Minimal-Seite mit der echten Attribut-Form
    import tempfile
    html = ('<div class="country-strand country-strand-education '
            'halfway-favourable-negative" data-score="52"> EDUCATION</div>'
            '<p>MIPEX Score (with Health): 46</p>')
    with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False,
                                     encoding="utf-8") as fh:
        fh.write(html)
        pfad = fh.name
    try:
        werte = lies_seite(pfad)
        assert werte == {"overall": 46, "education": 52}, werte
        assert vergleiche(werte, {"overall": 46, "education": 99}) == [
            ("education", 52, 99)]
    finally:
        os.unlink(pfad)
