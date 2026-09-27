"""KlimaTicket: der Fakt stützt sich auf den Rechnungshof, nicht auf erfundene Zahlen.

Der tote Beleg-Link des Fakts war der Anlass, nachzusehen (#182). Beide
amtlichen Quellen widersprachen den Zahlen des Fakts:

    Fakt vorher                          Amtliche Quelle
    „~30 % Modal-Shift (WIFO 2024)"      klimaaktiv: 20 % der Fahrten; eine
                                         WIFO-Studie gab es laut eigenem
                                         Faktentext gar nicht
    „~5-7 % CO2" bzw. „-150-300 kt"      Rechnungshof 2025: 0,11 Mio. t 2024,
                                         rund 0,2 % — und nur als ERWARTUNG
    „Subvention ~150-200 Mio/Jahr"       RH: 182 Mio. EUR 2024 (KlimaTicket Ö),
                                         dazu 214 Mio. EUR regional
    Preis „1.095 EUR seit 2024"          1.400 EUR ab 2026
    Deutschlandticket „49 EUR"           63 EUR seit Jänner 2026

Vorher in Prod: „Es wurden mehr Klimatickets verkauft als erwartet" ->
unverifiable@0.1, weil der Fakt gar nicht ausloeste; „kostet mehrere hundert
Millionen" -> true@0.5 mit der Begruendung „150-200 Mio. jaehrlich".
"""

import json
from pathlib import Path

import pytest

from services._topic_match import substring_or_composite_match

DATA = Path(__file__).resolve().parents[1] / "data" / "mobilitaet_pack.json"


def _fakt():
    facts = json.loads(DATA.read_text(encoding="utf-8"))["facts"]
    seq = facts if isinstance(facts, list) else list(facts.values())
    return next(x for x in seq if x["id"] == "mobilitaet-klimaticket_bilanz_2026")


def _alles():
    return json.dumps(_fakt(), ensure_ascii=False)


def test_quelle_ist_der_rechnungshof():
    f = _fakt()
    assert "rechnungshof.gv.at" in f["source_url"]
    assert "Rechnungshof" in f["source_label"]


@pytest.mark.parametrize("zahl", ["243.754", "124.000", "520 Mio. EUR", "610 Mio. EUR",
                                  "182 Mio. EUR", "214 Mio. EUR", "396 Mio. EUR",
                                  "0,11 Mio. t", "0,2 %", "1.400 EUR", "63 EUR"])
def test_belegte_zahlen_stehen_im_fakt(zahl):
    assert zahl in _alles(), zahl


def test_klimawirkung_ist_als_erwartungswert_gekennzeichnet():
    """Der RH gibt 0,11 Mio. t als Annahme des Ministeriums wieder, nicht als
    Messung — ein Verdict darf das nicht als belegte Wirkung verkaufen."""
    assert "ERWARTUNGSWERT" in _fakt()["data"]["kernsatz_fuer_synthesizer"]


@pytest.mark.parametrize("fehler", ["WIFO 2024", "30 % Modal-Shift", "5-7 %", "150-300 kt",
                                    "-150 bis -300", "49 EUR/Monat", "1.095 EUR/Jahr (seit Oktober 2024",
                                    "empirisch widerlegt", "3-4 Mrd"])
def test_widerlegte_oder_unbelegte_aussagen_sind_weg(fehler):
    assert fehler not in _alles(), fehler


@pytest.mark.parametrize("claim", [
    "Es wurden mehr Klimatickets verkauft als erwartet",
    "Das Klimaticket kostet den Staat mehrere hundert Millionen Euro",
    "Das Klimaticket bringt nichts fürs Klima",
    "Das Klimaticket hat die CO2-Emissionen des Verkehrs in Österreich stark gesenkt",
    "Das Klimaticket Österreich kostet 2026 1.400 Euro",
    "Das Deutschlandticket kostet 49 Euro im Monat",
])
def test_die_gemessenen_claims_loesen_den_fakt_aus(claim):
    """Der erste war vorher unverifiable, weil „verkauft"/„erwartet" in der
    zweiten Trigger-Gruppe fehlten."""
    assert substring_or_composite_match(_fakt(), claim.lower()), claim


@pytest.mark.parametrize("claim", ["Das Wetter in Österreich war gut",
                                   "Der Rechnungshof prüft die Pensionen",
                                   "Klimaschutz kostet Geld"])
def test_ohne_ticket_bezug_kein_treffer(claim):
    assert not substring_or_composite_match(_fakt(), claim.lower()), claim


# --------------------------------------------------------------------------
# Die zweite Kopie: transport_at.json :: klimaticket_2024
# --------------------------------------------------------------------------

TRANSPORT = Path(__file__).resolve().parents[1] / "data" / "transport_at.json"


def _zweitfakt():
    facts = json.loads(TRANSPORT.read_text(encoding="utf-8"))["facts"]
    seq = facts if isinstance(facts, list) else list(facts.values())
    return json.dumps(next(x for x in seq if x.get("id") == "klimaticket_2024"), ensure_ascii=False)


@pytest.mark.parametrize("fehler", ["250 kt", "4,1 %", "547", "286.000", "156.000", "(1 % "])
def test_zweitfakt_ohne_unbelegte_zahlen(fehler):
    """Nach dem ersten Fix nannten die Antworten weiter „~250 kt, ca. 1 %" und
    „4,1 % Modal-Shift" — aus dieser zweiten Kopie. 547 € war zudem falsch:
    der ermäßigte Preis lag laut Rechnungshof bei 821 €."""
    assert fehler not in _zweitfakt(), fehler


@pytest.mark.parametrize("zahl", ["293.742", "118.524", "821", "0,11 Mio. t", "0,2 %"])
def test_zweitfakt_mit_rechnungshof_zahlen(zahl):
    assert zahl in _zweitfakt(), zahl


def test_kein_vergleich_mit_den_entfernten_30_prozent():
    assert "AT Klimaticket 30 %" not in DATA.read_text(encoding="utf-8")
