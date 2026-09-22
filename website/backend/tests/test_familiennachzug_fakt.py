"""Familiennachzug: Rechtsstand statt Stand 2023/2024.

Anlass (2026-09-22): Beim Reparieren toter Beleg-Links (#185) fiel auf, dass
die BAMF-Nachfolgeseite dem eigenen Fakt widerspricht. Gemessen an Prod
(vor dem Fix) war die Lage so:

    "… ist auf 1.000 Visa pro Monat begrenzt"        unverifiable@0.1
    "Deutschland hat … ausgesetzt"                   unverifiable@0.1
    "Deutschland hat … für alle Flüchtlinge gestoppt" false@0.9, aber mit
                                                      der aufgehobenen
                                                      1.000er-Regel begründet
    "Österreich hat den Familiennachzug gestoppt"    mostly_false@0.9
    "In Österreich gibt es eine Quote"               false@0.9
    "Asylberechtigte … ohne Limit nachholen" (AT)    mostly_true@0.9

Richtig ist: In Deutschland ist der Nachzug zu subsidiär Schutzberechtigten
seit 24.7.2025 ausgesetzt (§ 104 Abs. 14 AufenthG, befristet bis 23.7.2027);
in Österreich war er von 3.7.2025 bis 2.7.2026 per Verordnung gestoppt und
unterliegt seit August 2026 einer Quote (§ 46a NAG).

Der Fakt enthielt zudem ein *nicht wörtliches* "§ 36a-Zitat", eine erfundene
BVerfG-Bestätigung und unbelegte Visa-Zahlen. Diese Tests halten den
Rechtsstand, die Wortlaute und die Trigger fest.

Keine Netzabfrage in diesen Tests.
"""

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
DATA = BACKEND / "data"
sys.path.insert(0, str(BACKEND))

from services._struct_marker import has_false_verdict_override  # noqa: E402
from services._topic_match import substring_or_composite_match as trifft  # noqa: E402

_spec = importlib.util.spec_from_file_location("stichtag_check", BACKEND / "tools" / "stichtag_check.py")
sc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sc)


def _fakt(datei: str, fakt_id: str) -> dict:
    def such(o):
        if isinstance(o, dict):
            if o.get("id") == fakt_id:
                return o
            for v in o.values():
                if (r := such(v)) is not None:
                    return r
        elif isinstance(o, list):
            for v in o:
                if (r := such(v)) is not None:
                    return r
        return None
    f = such(json.loads((DATA / datei).read_text(encoding="utf-8")))
    assert f is not None, (datei, fakt_id)
    return f


DE = _fakt("migration_pack.json", "migration_familiennachzug_2026")
AT = _fakt("oeif_zara.json", "oeif_familiennachzug_at_2023")
DE_TEXT = json.dumps(DE, ensure_ascii=False)
AT_TEXT = json.dumps(AT, ensure_ascii=False)


# --------------------------------------------------------------------------
# Deutschland: Aussetzung seit 24.7.2025
# --------------------------------------------------------------------------

@pytest.mark.parametrize("stueck", ["24.7.2025", "23.7.2027", "§ 104 Abs. 14", "§§ 22, 23"])
def test_de_headline_nennt_die_aussetzung(stueck):
    assert stueck in DE["headline"], DE["headline"]


def test_de_kernsatz_stellt_die_aussetzung_als_geltend_dar():
    k = DE["data"]["kernsatz_fuer_synthesizer"]
    assert "ist ausgesetzt" in k
    assert "galt von August 2018 bis 23.7.2025" in k, "1.000er-Regel muss als VERGANGEN markiert sein"


def test_de_behauptet_die_1000er_regel_nicht_mehr_als_geltend():
    """Der alte Kernsatz sagte: 'ist auf 1.000 Visa pro Monat begrenzt'.
    Geprueft wird, was der Fakt AUSSAGT (headline + data) — in
    ``claim_phrasings_handled`` steht der Satz weiter, denn genau so
    behaupten es Nutzerinnen und Nutzer."""
    aussage = DE["headline"] + " " + json.dumps(DE["data"], ensure_ascii=False)
    for satz in ("ist auf 1.000 Visa pro Monat begrenzt",
                 "ist auf 1.000 nationale Visa pro Monat begrenzt",
                 "auf 1.000 Visa/Monat begrenzt;"):
        assert satz not in aussage, satz


def test_de_zitiert_paragraf_104_woertlich():
    """Wortlaut aus gesetze-im-internet.de, am 22.9.2026 geprueft."""
    assert ("'Bis zum Ablauf des 23. Juli 2027 wird ein Familiennachzug nach § 36a zu einer "
            "Person, der eine Aufenthaltserlaubnis nach § 25 Absatz 2 Satz 1 zweite Alternative "
            "erteilt worden ist, nicht gewährt. Die §§ 22 und 23 bleiben unberührt.'") in DE_TEXT


def test_de_zitiert_paragraf_36a_woertlich_statt_frei():
    """Das alte 'Zitat' stand so nicht im Gesetz."""
    assert "'Monatlich können 1 000 nationale Visa für eine Aufenthaltserlaubnis nach Absatz 1 Satz 1 und 2 erteilt werden.'" in DE_TEXT
    assert "Bei Inhabern subsidiären Schutzes wird das Familiennachzug-Verfahren begrenzt" not in DE_TEXT


def test_de_bverfg_2017_wird_korrekt_wiedergegeben():
    v = DE["data"]["vorgeschichte"]
    assert "2 BvR 1758/17" in v and "Eilantrag" in v
    assert "nicht in der Hauptsache entschieden" in v
    assert "bestätigte 2018 die Verfassungsmäßigkeit" not in DE_TEXT


def test_de_keine_unbelegten_visa_zahlen_mehr():
    for zahl in ("30.000-40.000", "~5 % der Schutzgewährungen", "zwischen 0,5 und 1,5"):
        assert zahl not in DE_TEXT, zahl


def test_de_belegte_zahlen_sind_drin():
    """Bundestag (hib): BVA-Zustimmungen + AZR-Bestand."""
    assert "11.630" in DE_TEXT and "12.000" in DE_TEXT
    assert "388.074" in DE_TEXT


def test_de_verlinkt_den_geltenden_paragrafen_und_das_bamf():
    assert DE["source_url"] == "https://www.gesetze-im-internet.de/aufenthg_2004/__104.html"
    assert DE["secondary_url"] == "https://www.bamf.de/DE/Themen/Asyl/Personengruppen/Familien/familien-node.html"


# --------------------------------------------------------------------------
# Österreich: Stopp 2025/26, Quote seit August 2026
# --------------------------------------------------------------------------

@pytest.mark.parametrize("stueck", ["3.7.2025", "2.7.2026", "§ 46a NAG", "60", "979"])
def test_at_headline_nennt_stopp_quote_und_zahlen(stueck):
    assert stueck in AT["headline"], AT["headline"]


def test_at_ist_nicht_mehr_auf_stand_2023():
    assert AT["year"] == 2026
    assert "politisch ist eine Aussetzung im Gespräch" not in AT_TEXT
    assert "Asylberechtigte haben Anspruch ohne Limit;" not in AT["headline"]


def test_at_kernsatz_ordnet_die_gaengigen_claims_ein():
    k = AT["data"]["kernsatz_fuer_synthesizer"]
    assert "traf von Juli 2025 bis Juli 2026 zu" in k
    assert "trifft nicht mehr zu" in k
    assert "gerichtlich nicht entschieden" in k, "EU-Rechtsfrage ist offen, nicht entschieden"


def test_at_nennt_die_amtlichen_zahlen():
    z = AT["data"]["amtliche_zahlen"]
    assert "979" in z and "60" in z and "401" in z and "25" in z


def test_at_verlinkt_bmi_und_parlament():
    assert AT["source_url"].startswith("https://www.ots.at/presseaussendung/OTS_20260920_OTS0002")
    assert AT["secondary_url"].startswith("https://www.parlament.gv.at/aktuelles/news/")


# --------------------------------------------------------------------------
# Inversions-Falle: beidseitige Fakten duerfen keinen FALSCH-Marker tragen
# --------------------------------------------------------------------------

@pytest.mark.parametrize("fakt,name", [(DE, "DE"), (AT, "AT")])
def test_kein_struktureller_falsch_marker(fakt, name):
    """Beide Fakten bejahen manche Claims ('… ist ausgesetzt') und verneinen
    andere. Ein STRUKTURELL-FALSCH-Prefix wuerde auch die zutreffenden
    Claims nach 'falsch' druecken — genau die Inversions-Falle.
    Der AT-Fakt trug ihn bis 2026-09-22 ueber das Wort 'VERDICT-LEITLINIE'."""
    assert not has_false_verdict_override(fakt["data"]["kernsatz_fuer_synthesizer"]), name


# --------------------------------------------------------------------------
# Trigger
# --------------------------------------------------------------------------

@pytest.mark.parametrize("phrasing", DE["claim_phrasings_handled"])
def test_de_phrasings_treffen_den_fakt(phrasing):
    assert trifft(DE, phrasing.lower()), phrasing


@pytest.mark.parametrize("phrasing", AT["claim_phrasings_handled"])
def test_at_phrasings_treffen_den_fakt(phrasing):
    assert trifft(AT, phrasing.lower()), phrasing


@pytest.mark.parametrize("claim", [
    "Der Familiennachzug für subsidiär Schutzberechtigte ist ausgesetzt",
    "Subsidiär Schutzberechtigte dürfen ihre Familie nicht mehr nachholen",
    "Deutschland stoppt den Familiennachzug",
    "Flüchtlinge dürfen ihre Familien nicht mehr nach Deutschland holen",
    "Familiennachzug: 1.000 Visa pro Monat für subsidiär Geschützte",
])
def test_de_batterie(claim):
    assert trifft(DE, claim.lower()), claim


@pytest.mark.parametrize("claim", [
    "Österreich hat den Familiennachzug gestoppt",
    "Familiennachzug in Österreich nur noch mit Quote",
    "Asylberechtigte holen ihre Familien nach Österreich",
    "Subsidiär Schutzberechtigte dürfen ihre Familie nicht mehr nachholen",
])
def test_at_batterie(claim):
    assert trifft(AT, claim.lower()), claim


def test_klar_oesterreichischer_claim_zieht_den_de_fakt_nicht():
    """Sonst mischt der Synthesizer zwei Rechtsordnungen."""
    assert not trifft(DE, "asylberechtigte können ihre familie nach at holen")
    assert trifft(AT, "asylberechtigte können ihre familie nach at holen")


# --------------------------------------------------------------------------
# Der Ablauf der Befristung muss maschinell auffallen
# --------------------------------------------------------------------------

def test_stichtags_waechter_schlaegt_nach_ablauf_an():
    assert sc.pruefe_eintrag(DE, date(2026, 9, 22), 18) == []
    funde = sc.pruefe_eintrag(DE, date(2027, 7, 24), 18)
    assert funde, "Am Tag nach dem 23.7.2027 muss der Waechter anschlagen"
