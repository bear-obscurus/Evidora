"""Die fünf geparkten Inhaltskonflikte aus #185 — durch Korrektur des FAKTS.

Bei diesen fünf war nicht nur der Beleg-Link tot, sondern die AUSSAGE
unbelegt. Sie blieben deshalb geparkt: Lieber ein toter Link als eine
Quelle, die etwas anderes sagt als der Fakt. Am 29.9.2026 einzeln
nachrecherchiert — und keiner wurde durch einen Ersatzlink gelöst, sondern
alle durch Korrektur des Fakts.

## Was die Recherche ergab

1. OGH „8 ObA 70/22f (Foodora-Riders)" — **das Aktenzeichen existiert nicht.**
   Abfrage über die RIS-OGD-API: 0 Treffer, in drei Schreibweisen. Das
   Instrument wurde gegengeprüft: `Geschaeftszahl=9ObA336/89` liefert 2
   Treffer, die Abfrage funktioniert also. In Österreich regelt ein
   KOLLEKTIVVERTRAG die Fahrradbot:innen (WKO, weltweit der erste, seit
   1.1.2020) — kein Höchstgerichts-Urteil. Der KV-Teil für Fahrradboten ist
   laut WKO-Seite 2026 gekündigt.

2. ifo Schnelldienst 2023 „Wertschöpfungsanteile deutscher
   Automobilhersteller" — **echter 404**, und kein Aufsatz dieses Titels beim
   ifo auffindbar. Achtung bei der Prüfmethode: `curl` meldete HTTP 200, weil
   ifo.de eine Bot-Schutzseite („Client Challenge", 3 KB) ausliefert. Erst
   der Browser zeigte die echte 404-Seite. Ein Statuscode allein beweist
   nichts.

3. IAB-Forschungsbericht 2023 (Wapler/Brücker) „Migration und Wohnungsmarkt"
   — nicht auffindbar. Die genannten Personen forschen am IAB, aber zu
   Migration und ARBEITSMARKT.

4. DIHK „~45 % Wertschöpfung in DE als Faustregel" — Seite tot, und eine
   gesetzliche Wertschöpfungs-Quote gibt es nicht.

5. Fertilizers Europe „~60 % des EU-Stickstoffdüngers aus Russland/Belarus"
   — PDF tot, und die Zahl **widersprach den eigenen Daten des Fakts**, die
   35–40 % für Stickstoff nennen. Hier war keine Recherche nötig, nur ein
   Blick in denselben Datensatz.

## Was NICHT ersetzt wurde

Für die entfernten Zahlen wurde kein Ersatz eingesetzt. Eine unbelegte Zahl
durch eine andere unbelegte zu ersetzen wäre schlimmer als sie zu streichen.
Beispiel: Eine Websuche schlug für den Foodora-Fall „9 ObA 40/16x" vor —
dieses Aktenzeichen existiert zwar, aber nichts belegt einen Foodora-Bezug,
also wurde es nicht eingetragen. Ebenso wurde eine kursierende Zahl „60 %
freie Dienstnehmer:innen" nicht übernommen, weil die als Fundstelle
genannte AK-Seite sie nicht enthält.

Auch EuGH C-692/19 (Yodel) steht weiter im Fakt: curia und EUR-Lex waren mit
den hier verfügbaren Werkzeugen nicht abrufbar, das Zitat ist also weder
bestätigt noch widerlegt. Es wurde nicht angetastet — und das ist hier
vermerkt, damit es nicht als geprüft durchgeht.

Keine Netzabfrage in diesen Tests.
"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
DATA = BACKEND / "data"
sys.path.insert(0, str(BACKEND))


def _fakt(datei, fid):
    p = json.loads((DATA / datei).read_text(encoding="utf-8"))
    return next(f for f in p["facts"] if f.get("id") == fid)


PLATTFORM = _fakt("arbeitsmarkt_pack.json", "scheinselbststaendigkeit_2026")
MADE = _fakt("welthandel_pack.json", "made_in_germany_realitaet_2026")
DUENGER = _fakt("landwirtschaft_pack.json", "duengemittel_importabhaengigkeit_2026")
MIETE = _fakt("wohnen_pack.json", "migration_wohnungspreis_2026")
ALLE = {"plattform": PLATTFORM, "made": MADE, "duenger": DUENGER, "miete": MIETE}


KORREKTUR_MARKE = "29.9.2026"


def _nur_in_der_korrektur(fakt, wert, korrekturfeld):
    """Der entfernte Wert darf NUR noch in einer Korrektur-Notiz vorkommen.

    Die Notizen zitieren ihn absichtlich — still loeschen waere schlechter,
    weil dann niemand mehr sieht, was dort stand und warum. Geprueft wird
    deshalb nicht seine Abwesenheit, sondern dass er nirgends mehr als
    LEBENDE Behauptung steht: nicht in Ueberschrift, Quellen-Etikett oder
    URL, und in ``data`` nur in Feldern, die das Korrektur-Datum tragen.

    (Erste Fassung erlaubte ihn nur in EINEM benannten Feld. Das war zu eng:
    Auch der ``kernsatz_fuer_synthesizer`` braucht seine Notiz — er ist das
    Feld, das das Modell tatsaechlich liest. Genau dort hatte die erste
    Korrektur die erfundenen Zitate stehen lassen, und dieser Test hat es
    gefunden.)
    """
    for feld in ("headline", "source_label", "source_url", "secondary_url"):
        assert wert not in (fakt.get(feld) or ""), f"{wert} steht noch in {feld}"
    for k, v in (fakt.get("data") or {}).items():
        if KORREKTUR_MARKE in str(v):
            continue                       # Feld traegt eine Korrektur-Notiz
        assert wert not in str(v), f"{wert} steht noch in data.{k}"
    notiz = str((fakt.get("data") or {})[korrekturfeld])
    assert KORREKTUR_MARKE in notiz, f"data.{korrekturfeld} traegt kein Korrektur-Datum"
    assert wert in notiz, (
        f"{wert} fehlt in der Korrektur-Notiz — dann ist er still verschwunden")


# ---------------------------------------------------------------------------
# 1. Das nicht existente Aktenzeichen
# ---------------------------------------------------------------------------

def test_das_erfundene_aktenzeichen_steht_nur_noch_in_der_korrektur():
    _nur_in_der_korrektur(PLATTFORM, "8 ObA 70/22f", "at_ogh_8_oba_70_22")
    blob = json.dumps(PLATTFORM, ensure_ascii=False)
    for form in ("8ObA70/22f", "8oba70-22"):
        assert form not in blob, form


def test_die_korrektur_ist_im_fakt_vermerkt():
    """Nicht still loeschen: Wer die Stelle spaeter liest, soll sehen, was
    dort stand und warum es weg ist."""
    t = PLATTFORM["data"]["at_ogh_8_oba_70_22"]
    assert "existiert in der RIS-Judikatur NICHT" in t
    assert "9ObA336/89" in t, "die Gegenprobe des Instruments fehlt"


def test_oesterreich_steht_jetzt_auf_dem_kollektivvertrag():
    t = PLATTFORM["data"]["at_ogh_8_oba_70_22"]
    assert "kollektivvertrag" in t.lower()
    assert "1.1.2020" in t
    assert "GEKUENDIGT" in t or "gekuendigt" in t
    assert "wko.at" in PLATTFORM["secondary_url"]


def test_die_abgeleitete_prozentzahl_ist_mitgegangen():
    """Die '~75 % als Arbeitnehmer:innen' stuetzten sich auf das nicht
    existente Urteil — sie durften nicht stehen bleiben."""
    _nur_in_der_korrektur(PLATTFORM, "75 %", "at_plattform_landschaft_2024")
    assert "KEINE belegte Zahl" in PLATTFORM["data"]["at_plattform_landschaft_2024"]


def test_das_belegte_bag_urteil_ist_unangetastet():
    """Gegenprobe: Was geprueft und richtig ist, bleibt. Das BAG-Urteil wurde
    am 29.9.2026 gegen die Entscheidungsseite verifiziert."""
    t = PLATTFORM["data"]["bag_9_azr_102_20"]
    assert "9 AZR 102/20" in t and "1.12.2020" in t
    assert "bundesarbeitsgericht.de" in PLATTFORM["source_url"]


# ---------------------------------------------------------------------------
# 2.-4. Made in Germany: ifo-Zahlen und DIHK-Faustregel
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fort", ["30 %", "25 %", "15 %", "Tiguan", "Mecklenburg", "Hochmuth"])
def test_die_unbelegten_tiguan_zahlen_sind_weg(fort):
    assert fort not in MADE["headline"], fort


def test_die_ifo_korrektur_ist_vermerkt():
    t = MADE["data"]["ifo_tiguan_aufgliederung"]
    assert "KORREKTUR 29.9.2026" in t
    assert "404" in t


def test_die_erfundene_wertschoepfungsquote_ist_weg():
    _nur_in_der_korrektur(MADE, "45 %", "made_in_germany_recht")
    t = MADE["data"]["made_in_germany_recht"]
    assert "KEINE Prozent-Schwelle" in t
    assert "Quote existiert nicht" in t


def test_was_rechtlich_belegt_ist_bleibt():
    t = MADE["data"]["made_in_germany_recht"]
    assert "952/2013" in t and "Art. 60" in t
    assert "BGH" in t


# ---------------------------------------------------------------------------
# 5. Der Widerspruch im eigenen Datensatz
# ---------------------------------------------------------------------------

def test_die_ueberschrift_widerspricht_den_eigenen_daten_nicht_mehr():
    """Die Ueberschrift sagte '~60 % aus Russland/Belarus', das Feld
    darunter '35-40 % Stickstoff'. Dafuer brauchte es keine Recherche."""
    assert "60 %" not in DUENGER["headline"]
    assert "35-40 %" in DUENGER["headline"]
    assert "35-40 %" in DUENGER["data"]["rus_belarus_anteil_vor_2022"]


def test_die_eigenproduktions_zahl_blieb_stehen():
    """Im Kernsatz steht weiter '60 % des N-Bedarfs' — das ist die EU-EIGEN-
    produktion und war nie der Fehler. Beim Korrigieren nicht die falsche
    Stelle treffen."""
    k = DUENGER["data"]["kernsatz_fuer_synthesizer"]
    assert "60 % des N-Bedarfs" in k


# ---------------------------------------------------------------------------
# 6. Die nicht auffindbare IAB-Publikation
# ---------------------------------------------------------------------------

def test_der_nicht_auffindbare_iab_bericht_ist_weg():
    _nur_in_der_korrektur(MIETE, "10-20 %", "iab_2023_wapler")
    t = MIETE["data"]["iab_2023_wapler"]
    assert "nicht auffindbar" in t
    assert "ARBEITSMARKT" in t


def test_die_ueberschrift_nennt_keine_erfundene_zahl_mehr():
    assert "10-20 %" not in MIETE["headline"]
    assert "IAB" not in MIETE["headline"]


# ---------------------------------------------------------------------------
# Rahmen: jeder Fakt hat wieder zwei verschiedene, lebende Quellen
# ---------------------------------------------------------------------------

TOT = (
    "dihk.de/de/themen-und-positionen",
    "fertilizerseurope.com",
    "iab.de/de/publikationen",
    "ifo.de/publikationen/2023",
    "ogh.gv.at/entscheidungen-suche",
)


@pytest.mark.parametrize("name", sorted(ALLE))
def test_zwei_verschiedene_quellen(name):
    f = ALLE[name]
    assert f["source_url"] and f["secondary_url"]
    assert f["source_url"] != f["secondary_url"], name


@pytest.mark.parametrize("name", sorted(ALLE))
def test_keine_der_toten_adressen_mehr(name):
    blob = json.dumps(ALLE[name], ensure_ascii=False)
    for t in TOT:
        assert t not in blob, f"{name}: {t}"
