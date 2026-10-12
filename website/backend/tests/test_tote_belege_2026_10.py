"""Die toten Beleg-Links aus dem Lauf vom 12.10.2026.

Der Link-Wächter meldete 77 tote URLs, davon **22 in kuratierten Dateien**
— echte Belege, die ein Leser anklickt und ins Leere läuft. (Die übrigen 55
liegen in generierten Caches: MITRE-Quellenangaben und EU-Projekt-Websites,
die wir weder besitzen noch ersetzen können, siehe #243.)

Vorab geprüft, bevor irgendetwas ersetzt wurde: **alle 22 sind echt tot**,
mit Bot- UND Browser-User-Agent, durchgehend 404. Der Prüfer hat recht.

Ersetzt wurden hier **10**, und zwar nur solche, bei denen der neue Link
DIESELBE Aussage trägt:

  * **Struktur-Umzüge (7).** Zwei Ministerien haben die Domain gewechselt
    (`bmbwf.gv.at` → `bmb.gv.at`, `sozialministerium.at` → `.gv.at`), die
    Hochschulagenden sind zum BMWET gewandert, und die OeNB hat ihre
    Seitenstruktur umgebaut, das IW Köln seine Pfade vereinfacht.
    Ersetzt wurde jeweils Themenseite durch
    Themenseite — gleiche Quelle, gleicher Gegenstand, gleiche
    Beleg-Tiefe.
  * **ACSM (3 von 7).** ACSM hat seine alten Blog-Beiträge nicht migriert
    (die eigene Suche findet sie nicht, die neuen Flach-Slugs antworten
    404). Ersetzt wurde durch **genau die Primärliteratur, die im
    `source_label` des Fakts schon steht** — Bhasin 1996, Behm 2016,
    McHugh/Connolly 2003, je über PubMed. Das ist nicht nur ein Ersatz,
    sondern der bessere Beleg.

NICHT ersetzt und absichtlich offen (12) — hier fehlt der Nachweis, dass
ein Ersatz dieselbe Aussage trägt:

  * **bundesfinanzministerium.de**: liefert inzwischen site-weit 503, also
    **nicht nachweislich tot**. Die Getty-Klasse: erst wieder ansehen,
    wenn der Server antwortet.
  * **wsava.org/global-guidelines/puppy-development-guidelines/**: die
    WSAVA-Leitlinienübersicht führt **keine** Welpen-Leitlinie. Entweder
    zurückgezogen oder nie existent — und damit die Klasse aus #235. Das
    gehört untersucht, nicht ersetzt.
  * **economics.mit.edu/.../AcemogluPublishedRestrepoAERPP202405.pdf**:
    unter diesem Namen nicht auffindbar.
  * **ACSM (4 verbliebene)**: die im `source_label` genannten Arbeiten
    (u. a. „Coyle 1995", „Hall NIH 2017") liessen sich nicht eindeutig
    auflösen. Lieber ein toter Link als ein falscher Beleg.
  * **ICCT, AGES, Amnesty (ASA 01/4030/2021), IW Köln, juraforum,
    verbraucherzentrale**: brauchen je eine eigene Recherche.

Die Ansprüche aller 22 bleiben sichtbar: der Wächter führt sie weiter als
„Rückstand in Belegen" auf, und dieser Test hält fest, welche davon
erledigt sind.
"""

import json
from pathlib import Path

import pytest

DATA = Path(__file__).resolve().parents[1] / "data"

# (Datei, ersetzte tote URL, neue URL)
ERSETZT = [
    ("eter.json",
     "https://www.bmbwf.gv.at/Themen/HS-Uni/Studium/Studienstatistiken.html",
     "https://www.bmwet.gv.at/bmafjgvat/wissenschaft/hochschulsystem/"
     "hochschulstatistik-unidata.html"),
    ("inklusion_pack.json",
     "https://www.bmbwf.gv.at/Themen/schule/schulpraxis/inklusion.html",
     "https://www.bmb.gv.at/Themen/schule/schulsystem/sa/sp.html"),
    ("iqs_bildung.json",
     "https://www.bmbwf.gv.at/Themen/schule/zrp/gts.html",
     "https://www.bmb.gv.at/Themen/schule/schulsystem/gts/fakten.html"),
    ("sozialstaat_pack.json",
     "https://www.sozialministerium.at/Themen/Pflege/Pflegende-Angehoerige.html",
     "https://www.sozialministerium.gv.at/Themen/Pflege/"
     "Betreuende-und-Pflegende-Angehoerige.html"),
    ("oenb.json",
     "https://www.oenb.at/Geldpolitik/Konjunktur/"
     "wirtschaftsprognosen-fuer-oesterreich.html",
     "https://www.oenb.at/themen/wirtschaft-in-oesterreich/konjunktur.html"),
    ("housing_at.json",
     "https://www.oenb.at/Statistik.html",
     "https://www.oenb.at/daten-und-forschung/datenangebot.html"),
    ("sport_fitness_pack.json",
     "https://www.acsm.org/blog-detail/acsm-blog/2020/01/22/strength-training-women",
     "https://pubmed.ncbi.nlm.nih.gov/8637535/"),
    ("sport_fitness_pack.json",
     "https://www.acsm.org/blog-detail/acsm-certified-blog/2017/04/26/dynamic-stretching",
     "https://pubmed.ncbi.nlm.nih.gov/26642915/"),
    ("sport_fitness_pack.json",
     "https://www.acsm.org/blog-detail/acsm-certified-blog/2018/04/06/"
     "3-myths-about-muscle-soreness",
     "https://pubmed.ncbi.nlm.nih.gov/12580677/"),
    ("wohnen_pack.json",
     "https://www.iwkoeln.de/studien/themen/immobilien.html",
     "https://www.iwkoeln.de/themen/immobilien.html"),
]

# Alle 22 aus dem Lauf vom 12.10.2026 — auch die noch offenen. Keiner
# davon darf irgendwo in data/ wieder auftauchen.
TOT_12_10 = [alt for _d, alt, _n in ERSETZT] + [
    "https://economics.mit.edu/sites/default/files/2024-04/"
    "AcemogluPublishedRestrepoAERPP202405.pdf",
    "https://theicct.org/publication/"
    "real-world-energy-consumption-electric-vehicles-europe-jul24/",
    "https://wsava.org/global-guidelines/puppy-development-guidelines/",
    "https://www.acsm.org/all-blog-posts/certification-blog/acsm-certified-blog/"
    "2018/03/27/listening-to-the-pain",
    "https://www.acsm.org/blog-detail/acsm-blog/2014/06/24/"
    "exercise-fuels-low-intensity-vs-high-intensity",
    "https://www.acsm.org/blog-detail/acsm-blog/2018/03/14/lean-body-mass",
    "https://www.acsm.org/blog-detail/acsm-certified-blog/2018/10/15/"
    "progressive-overload-strength-training",
    "https://www.ages.at/themen/lebensmittel/lebensmittelsicherheit/"
    "tierwohl-und-tierschutz",
    "https://www.amnesty.org/en/documents/asa01/4030/2021/en/",
    "https://www.bundesfinanzministerium.de/Content/DE/Standardartikel/Themen/"
    "Steuern/Steuerarten/Lohnsteuer/2024-12-23-Inflationsausgleichsgesetz-FAQ.html",
    "https://www.juraforum.de/lexikon/screenshot-recht",
    "https://www.verbraucherzentrale.de/wissen/digitale-welt/datenschutz/"
    "whatsapp-chats-als-beweismittel-rechtslage-und-tipps-69466",
]

NOCH_OFFEN = [u for u in TOT_12_10 if u not in {a for _d, a, _n in ERSETZT}]


def _text(datei):
    return (DATA / datei).read_text(encoding="utf-8")


@pytest.mark.parametrize("datei,alt,neu", ERSETZT)
def test_toter_link_ist_weg(datei, alt, neu):
    assert alt not in _text(datei)


@pytest.mark.parametrize("datei,alt,neu", ERSETZT)
def test_ersatz_steht_drin(datei, alt, neu):
    assert neu in _text(datei)


def test_kein_ersetzter_link_taucht_woanders_wieder_auf():
    """Eine URL in zwei Kopien driftet — hier: ein toter Link, der an einer
    zweiten Stelle stehenbleibt und der Wächter ihn nächste Woche erneut
    meldet."""
    rueckkehrer = []
    for pfad in sorted(DATA.glob("*.json")):
        t = pfad.read_text(encoding="utf-8")
        for _d, alt, _n in ERSETZT:
            if alt in t:
                rueckkehrer.append(f"{pfad.name}: {alt}")
    assert not rueckkehrer, rueckkehrer


def test_die_offenen_sind_benannt_und_nicht_heimlich_verschwunden():
    """Was NICHT ersetzt wurde, bleibt sichtbar — sonst wäre die Lücke
    stillgelegt statt geschlossen. Jeder offene Link muss noch in einer
    Datendatei stehen (sonst stimmt die Liste nicht mehr)."""
    alle = "".join(p.read_text(encoding="utf-8")
                   for p in sorted(DATA.glob("*.json")))
    fehlend = [u for u in NOCH_OFFEN if u not in alle]
    assert not fehlend, (
        "diese Links stehen nicht mehr in data/ — Liste veraltet: " + str(fehlend))
    assert len(NOCH_OFFEN) == 12


def test_die_ersetzten_sind_erreichbare_quellen_derselben_herkunft():
    """Kein Ersatz durch irgendeine andere Quelle: Ministeriums-Seite bleibt
    Ministeriums-Seite, und wo der Beleg auf Literatur ausweicht, steht
    diese Literatur im `source_label` des Fakts."""
    erwartete_wirte = {
        "eter.json": "bmwet.gv.at",
        "inklusion_pack.json": "bmb.gv.at",
        "iqs_bildung.json": "bmb.gv.at",
        "sozialstaat_pack.json": "sozialministerium.gv.at",
        "oenb.json": "oenb.at",
        "housing_at.json": "oenb.at",
        "sport_fitness_pack.json": "pubmed.ncbi.nlm.nih.gov",
        "wohnen_pack.json": "iwkoeln.de",
    }
    for datei, _alt, neu in ERSETZT:
        assert erwartete_wirte[datei] in neu, (datei, neu)


@pytest.mark.parametrize("fakt_id,autor", [
    ("women_bulky_muscles_2026", "Bhasin"),
    ("stretching_injury_2026", "Behm"),
    ("lactic_acid_2026", "McHugh"),
])
def test_die_literatur_stand_schon_im_source_label(fakt_id, autor):
    """Der Kern der ACSM-Entscheidung: ersetzt wurde nicht durch eine neue
    Quelle, sondern durch die, auf die sich der Fakt ohnehin beruft."""
    d = json.loads(_text("sport_fitness_pack.json"))
    f = next(x for x in d["facts"] if x.get("id") == fakt_id)
    assert autor in f.get("source_label", ""), f.get("source_label")
