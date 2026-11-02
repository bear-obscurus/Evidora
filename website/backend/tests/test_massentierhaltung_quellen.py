"""Drei erfundene Quellen und eine vertauschte Zahl (2.11.2026).

Anlass war ein Nebenbefund aus #298: das ``source_label`` des
Massentierhaltungs-Fakts nannte „AGES Tierwohl-Statistik" und
„AGES-Tierwohl-Studie 2023", und beide waren auf ages.at nicht auffindbar.
Die Prüfung aller Zitate des Fakts hat mehr gefunden.

## Was nicht existiert

**AGES, dreimal unter verschiedenen Namen.** Im ``source_label`` standen
„AGES Tierwohl-Statistik" und „AGES-Tierwohl-Studie 2023", im Datenfeld
``at_tierwohl_2024`` zusätzlich „AGES Tierwohl-Bilanz 2024". Gemessen über
die vollständige Sitemap: **0 von 1.178 AGES-URLs** passen auf tierwohl,
tierschutz, tierhaltung, vollspalt oder bilanz. Gegenprobe, dass die
Sitemap repräsentativ ist: lebensmittel 131, tier/ 63, arzneimittel 35,
pflanzenschutz 9. AGES hat das Thema nicht verschoben, sondern gar nicht —
Tierwohl ist nicht ihr Auftrag.

**„Albert-Schweitzer-Stiftung Tierwohl-Index 2024"**, Quelle für eine
EU-Median-Reihe der Hofgrößen (AT 350 / DE 1.500 / NL 2.500 / DK 3.500).
**0 von 1.230 URLs** der Stiftung passen auf index, bestandsgröße oder
herdengröße; die 12 Treffer auf „ranking" sind Supermarkt- und
Veggie-Rankings. Mit der Quelle ist auch die Median-Reihe entfallen.

## Die vertauschte Zahl

Der Fakt führte „2.6 Mio Schweine, 2.5 Mio Rinder". Amtlich, Statistik
Austria PM 13 401-167/24 vom 21.8.2024:

    "Am Stichtag 1. Juni 2024 wurden laut Statistik Austria österreichweit
     1,80 Mio. Rinder und 2,52 Mio. Schweine gehalten"

**Die 2,5 Mio sind die Schweinezahl**, eingetragen auf der Rinderzeile; die
Rinderzahl fehlte ganz. Dass im selben Feld „539k Milchkühe" stand und die
Aussendung 539.400 nennt, zeigt: die Quelle lag vor, die Zuordnung ist
verrutscht. Dieselbe Klasse wie #227 — das richtige Ergebnis aus der
falschen Zuordnung. Gegengeprüft an zwei weiteren Stellen: Dez 2024
1.820.026 Rinder, Dez 2025 1.814.683, Juni 2026 1.790.211.

## Die 15-fach falsche Zahl

„7 Mio Bienen-Stöcke", zugeschrieben derselben Statistik-Austria-Quelle.
Die Viehbestand-Statistik führt **keine Bienen**; das BMLUK nennt rund
456.000 Bienenvölker. Entfernt, nicht korrigiert: der Fakt handelt von
Bestandskonzentration, und eine Zahl in zwei Kopien driftet (#134).

## Die Richtlinien waren falsch zugeordnet

Der Fakt schrieb das Käfigverbot für Legehennen den Richtlinien
2008/120/EG und 2007/43/EG zu. Beide existieren und wurden wörtlich
geprüft — aber 2008/120/EG regelt **Schweine** und 2007/43/EG
**Masthühner**. Für Legehennen gilt 1999/74/EG, die der Fakt nicht nannte;
dort steht Kapitel II Artikel 5 Absatz 2 wörtlich:

    "die Haltung in Käfigen im Sinne dieses Kapitels ab 1. Januar 2012
     untersagt"

Die Aussage des Fakts war also richtig und stand bei der falschen Norm.
1999/74/EG ist jetzt der Zweitbeleg, an die Stelle des toten AGES-Links.

## Was belegt stehen blieb

``BGBl I 2004/118`` — RIS liefert „Bundesgesetz über den Schutz der Tiere
(Tierschutzgesetz - TSchG), StF: BGBl. I Nr. 118" mit dem ELI-Pfad
``eli/bgbl/i/2004/118``. Und ``539.400 Milchkühe``, exakt wie in der
Aussendung.

## Was ersetzt statt entfernt wurde

Die Haltungs-Anteile („85 % Vollspaltboden") hingen allein an der
nicht existierenden AGES-Bilanz — und 85 % liegt über **jeder**
veröffentlichten Schätzung. Eine amtliche Quote gibt es nicht. Statt einer
erfundenen Präzision nennt der Fakt jetzt die Spannweite 58 bis 80 % mit
Erheber (VGT 2022: 69 % Vollspaltboden, 11 % Stroh; Land schafft Leben rund
60 %, Stand 2019; Bio rund 2,8 %) und sagt ausdrücklich, dass diese Werte
von Interessengruppen stammen. Für einen Faktencheck ist „es gibt dazu
keine amtliche Zahl" die brauchbarere Auskunft.

Keine Netzabfrage in diesen Tests.
"""

import json
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
PACK = BACKEND / "data" / "landwirtschaft_pack.json"
KENNUNG = "massentierhaltung_empirie_2026"


def _fakt():
    d = json.loads(PACK.read_text(encoding="utf-8"))
    treffer = [f for f in d["facts"] if (f.get("id") or f.get("topic")) == KENNUNG]
    assert len(treffer) == 1, len(treffer)
    return treffer[0]


def _blob():
    return json.dumps(_fakt(), ensure_ascii=False)


# --------------------------------------------------------------------------
# Die erfundenen Quellen dürfen nicht zurückkehren
# --------------------------------------------------------------------------

@pytest.mark.parametrize("erfunden", [
    "AGES Tierwohl-Statistik",
    "AGES-Tierwohl-Studie",
    "AGES Tierwohl-Bilanz",
    "Albert-Schweitzer-Stiftung",
])
def test_erfundene_quelle_steht_nicht_im_fakt(erfunden):
    assert erfunden not in _blob(), erfunden


def test_ages_wird_nicht_mehr_als_quelle_gefuehrt():
    """Giftprobe: "AGES" darf im Quellen-Kontext nicht mehr auftauchen.

    Das Trigger-Token " ages " bleibt erlaubt — es sorgt dafuer, dass
    Behauptungen UEBER die AGES diesen Fakt finden.
    """
    f = _fakt()
    assert "AGES" not in f["source_label"]
    for k, v in f["data"].items():
        assert "AGES" not in v, k
    for n in f["context_notes"]:
        assert "AGES" not in n, n
    # und der Trigger ist unangetastet
    assert " ages " in f["trigger_composite"][0]


def test_der_tote_ages_link_ist_weg():
    assert "ages.at" not in _blob()


# --------------------------------------------------------------------------
# Die Zahlen, gegen die Primärquelle gepinnt
# --------------------------------------------------------------------------

#: Statistik Austria, Pressemitteilung 13 401-167/24 vom 21.8.2024,
#: Stichtag 1. Juni 2024.
AMTLICH_2024 = {
    "Rinder": "1,80 Mio",
    "Schweine": "2,52 Mio",
    "Milchkühe": "539.400",
    "Mastschweine": "997.600",
    "Zuchtschweine": "212.300",
}


@pytest.mark.parametrize("was,zahl", sorted(AMTLICH_2024.items()))
def test_amtliche_zahl_steht_im_fakt(was, zahl):
    assert zahl in _blob(), (was, zahl)


def test_die_rinderzahl_ist_nicht_die_schweinezahl():
    """Der eigentliche Defekt: 2,5 Mio stand auf der Rinderzeile."""
    f = _fakt()
    for text in [f["headline"]] + list(f["data"].values()):
        assert "2,5 Mio Rinder" not in text
        assert "2.5 Mio Rinder" not in text
        assert "2,52 Mio Rinder" not in text


def test_headline_nennt_beide_tierarten_mit_der_richtigen_zahl():
    h = _fakt()["headline"]
    assert "2,52 Mio Schweine" in h
    assert "1,80 Mio Rinder" in h


def test_die_bienenzahl_ist_raus():
    blob = _blob()
    assert "Bienen-Stöcke" not in blob
    assert "7 Mio Bienen" not in blob


def test_die_unbelegte_hofgroessen_reihe_ist_raus():
    """AT 350 / DE 1.500 / NL 2.500 / DK 3.500 hatte keine Quelle."""
    blob = _blob()
    for zahl in ("DE 1.500", "NL 2.500", "DK 3.500", "Median ~350"):
        assert zahl not in blob, zahl


# --------------------------------------------------------------------------
# Die Richtlinien, jede bei ihrem Anwendungsbereich
# --------------------------------------------------------------------------

def test_jede_richtlinie_steht_bei_ihrem_gegenstand():
    feld = _fakt()["data"]["eu_tierwohl_richtlinie"]
    assert "2008/120/EG" in feld and "Schweine" in feld
    assert "2007/43/EG" in feld and "Masthühner" in feld
    assert "1999/74/EG" in feld and "Legehennen" in feld


def test_das_kaefigverbot_haengt_an_der_richtigen_richtlinie():
    """1999/74/EG Kapitel II, nicht 2007/43/EG."""
    blob = _blob()
    i = blob.find("1.1.2012")
    assert i > 0, "Verbotsdatum fehlt"
    umfeld = blob[max(0, i - 260):i + 60]
    assert "1999/74/EG" in umfeld, umfeld


def test_masthuehner_werden_nicht_als_legehennen_gefuehrt():
    feld = _fakt()["data"]["eu_tierwohl_richtlinie"]
    i = feld.find("2007/43/EG")
    assert i > 0
    assert "Legehennen" not in feld[i:i + 70]


def test_der_zweitbeleg_ist_die_legehennen_richtlinie():
    assert _fakt()["secondary_url"].endswith("CELEX:31999L0074")


def test_das_bgbl_zitat_blieb_stehen():
    """Es ist belegt: RIS nennt StF BGBl. I Nr. 118 fuer das TSchG."""
    assert "BGBl I 2004/118" in _fakt()["source_label"]


# --------------------------------------------------------------------------
# Die Quelle zeigt dorthin, wo die Zahlen stehen
# --------------------------------------------------------------------------

def test_source_url_zeigt_auf_die_jahresbestaende():
    """Die Agrarstrukturerhebung fuehrt Stand 2023, der Fakt Jahreswerte."""
    u = _fakt()["source_url"]
    assert u.endswith("viehbestand-jaehrlich")
    assert "agrarstrukturdaten" not in u


def test_die_pressemitteilung_ist_im_label_benannt():
    assert "13 401-167/24" in _fakt()["source_label"]


# --------------------------------------------------------------------------
# Wo keine amtliche Zahl existiert, sagt der Fakt das
# --------------------------------------------------------------------------

def test_die_haltungsanteile_nennen_ihren_erheber():
    feld = _fakt()["data"]["at_tierwohl_2024"]
    assert "keine amtliche Quote" in feld
    assert "VGT" in feld
    assert "58" in feld and "80" in feld


def test_die_bestandskonzentration_behauptet_keine_quote_mehr():
    feld = _fakt()["data"]["bestand_konzentration_schwein"]
    assert "keine amtliche Auswertung" in feld
    for weg in ("30 %", "50 %", "20 %", "Top-100"):
        assert weg not in feld, weg


def test_der_kernsatz_sagt_wo_die_datenluecke_ist():
    k = _fakt()["data"]["kernsatz_fuer_synthesizer"]
    assert "keine amtliche Auswertung" in k
    assert "keine amtliche Quote" in k
    assert "Interessengruppen" in k


# --------------------------------------------------------------------------
# Formvorschriften des Packs
# --------------------------------------------------------------------------

def test_datenfelder_bleiben_unter_400_zeichen():
    for k, v in _fakt()["data"].items():
        if k == "kernsatz_fuer_synthesizer":
            continue
        assert len(v) <= 400, (k, len(v))


def test_keine_rhetorische_frage_im_fakt():
    for t in [_fakt()["headline"]] + list(_fakt()["data"].values()):
        assert "?" not in t, t[:80]
