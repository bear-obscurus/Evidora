"""``eu_netto_zahler_2026`` feuert nur noch auf die EU-Bilanz Deutschlands.

Der Fakt (Wirtschaftspolitik-Pack) belegt Deutschlands Nettobeitrag zur EU
gegen den Binnenmarkt-Gewinn: rund 9 Mrd. Euro netto, rund 86 Mrd. Euro
Binnenmarkt-Gewinn. Sein Kernsatz traegt den STRUKTURELL-Marker
(„DIFFERENZIERT FALSCH/IRREFÜHREND").

Bis zu diesem Fix hatte ``trigger_composite`` zwei Gruppen: EU-Woerter
(" eu ", "die eu", "europa" …) und eine zweite Gruppe, die neben
Zahlungswoertern auch Allerweltswoerter und Laendernamen fuehrte
("österreich", "deutschland", "zahlen", "kosten", "mehr als", "verlust" …).
Damit reichte „Österreich" + „EU". Der Audit vom 3.11.2026 nannte es
(„Gruppe zu breit", 14 Formulierungen), E fand es am 21.11. beim Bau von #322
wieder. Gemessen vor dem Fix, exakter Pass:

    dokumentierte Phrasings ANDERER Fakten     14 von 2.653 treffen den Fakt,
                                               alle 14 Fehlzuendungen
    Claim-Texte der Messkataloge               40 von 3.517, alle 40 fremd
    alle Katalogtexte (Evidora Dokumente)      232 von 15.269, davon 4 zur
                                               EU-Bilanz Deutschlands

Inflation in Österreich ueber dem EU-Schnitt, Wahlbeteiligung im
europaeischen Vergleich, Förderschulquote in Deutschland und Europa,
„Die Zahlen zur illegalen Migration in der EU" (Zahlen ≠ zahlen) — jeder
dieser Claims bekam Deutschlands EU-Bilanz mit STRUKTURELL-Marker dazu.

DAS NEUE MUSTER
===============
Jede Regel verlangt einen Deutschland-Anker (Deutschland, Deutsche,
Bundesrepublik, BRD) UND einen Begriff der EU-Bilanz: Nettozahler, EU-Beitrag,
EU-Haushalt, Zahlmeister; „zahlt … an die EU / nach Brüssel"; „zahlt mehr
ein, als zurückkommt"; „zahlt für andere"; „die EU kostet Deutschland";
„lohnt sich die EU für Deutschland"; Binnenmarkt mit Gewinn-Wort.

Die drei laenderlosen Phrasings („EU-Mitgliedschaft kostet uns Milliarden",
„Wir sind nur Netto-Zahler in der EU", „EU-Beitrag lohnt sich nicht") stehen
nur noch WOERTLICH als Keyword. Ein laenderloses „wir" kann auch Österreich
meinen; der Fakt hat aber nur deutsche Zahlen.

Nachher: 0 von 2.653 fremden Phrasings, 0 von 3.517 Claim-Texten, 3 von
15.269 Katalogtexten — alle drei zur EU-Bilanz Deutschlands. Verloren ist
einer der vier echten Treffer, ein laenderloses „wir sind ein
Netto-Zahler-Land" (gepinnt unten).

GRENZE
======
Der Matcher ist monoton: Ein Wort mehr im Claim nimmt nie einen Treffer
weg. Wer einen der drei woertlichen Saetze in einen Österreich-Claim
einbaut („Österreichs EU-Beitrag lohnt sich nicht"), trifft den Fakt
weiter. Ohne eine Negation in der Trigger-Sprache ist das nicht trennbar.
Gepinnt als bekannter Rest.

ABDECKUNGSLUECKE
================
Für Österreichs EU-Bilanz gibt es danach keinen kuratierten Fakt mehr. Vorher
bekamen solche Claims die deutschen Zahlen, was nicht besser war.

Modellfrei: kein Netz, kein SpaCy, kein sentence-transformers.
"""

import copy
import glob
import json
import os
import re
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, BACKEND)

from services._schreibweise import normalisiere  # noqa: E402
from services._tippfehler import tippfehler_match  # noqa: E402
from services._topic_match import (  # noqa: E402
    find_matching_items,
    substring_or_composite_match,
)

PACK = os.path.join(DATA, "wirtschaftspolitik_pack.json")
FID = "eu_netto_zahler_2026"
TF = ("trigger_keywords", "trigger_composite", "trigger_all")
DE_ANKER = {"deutschland", "deutsche", "bundesrepublik", " brd "}


def _fakt() -> dict:
    with open(PACK, encoding="utf-8") as fh:
        return next(f for f in json.load(fh)["facts"] if f["id"] == FID)


def _alle_fakten() -> list[tuple[str, dict]]:
    alle = []
    for p in sorted(glob.glob(os.path.join(DATA, "*.json"))):
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
        if not isinstance(d, dict):
            continue
        for v in d.values():
            if isinstance(v, list):
                for it in v:
                    if isinstance(it, dict) and any(it.get(t) for t in TF):
                        alle.append((os.path.basename(p), it))
    return alle


FAKT = _fakt()
ALLE = _alle_fakten()


def _fid(it: dict) -> str:
    return it.get("id") or it.get("topic") or ""


def _trifft(claim: str, fakt: dict | None = None) -> bool:
    return substring_or_composite_match(fakt or FAKT, claim.lower())


def _pack_liefert(claim: str) -> list[str]:
    """Was das Pack in Produktion liefert: exakt, sonst tolerant, sonst
    englisch. ``descriptor_fn=None`` wie in services/wirtschaftspolitik_pack."""
    return [f.get("id") for f in find_matching_items(
        PACK, "facts", claim_lc=claim.lower(), full_claim=claim, descriptor_fn=None)]


def _regeln(fakt: dict) -> list[list]:
    regeln = []
    if fakt.get("trigger_composite"):
        regeln.append(fakt["trigger_composite"])
    regeln += list(fakt.get("trigger_all") or [])
    return regeln


# ---------------------------------------------------------------------------
# 1. Muss-Treffer: jede Phrasing ihr eigener Fakt
# ---------------------------------------------------------------------------

# Ausgeschrieben, nicht aus der Datei gelesen: Sonst bestuende der Test auch
# dann, wenn jemand eine Phrasing still entfernt.
MUSS = [
    # die fuenf von vorher
    "Deutschland zahlt mehr an EU als es bekommt",
    "EU-Mitgliedschaft kostet uns Milliarden",
    "Wir sind nur Netto-Zahler in der EU",
    "Deutschland zahlt für andere EU-Länder",
    "EU-Beitrag lohnt sich nicht",
    # neu: so, wie Menschen tippen
    "Zahlt Deutschland zu viel an die EU?",
    "Deutschland ist der Zahlmeister der EU",
    "Wir Deutschen überweisen jedes Jahr Milliarden nach Brüssel",
    "Kriegen wir Deutschen von der EU weniger zurück, als wir einzahlen?",
    "Lohnt sich die EU für Deutschland überhaupt?",
    "Profitiert Deutschland überhaupt vom EU-Binnenmarkt?",
    "Wie viel zahlt Deutschland netto an die EU?",
    "Wir Deutschen blechen für die ganze EU",
]


@pytest.mark.parametrize("claim", MUSS)
def test_phrasing_trifft_ihren_fakt(claim):
    assert claim in (FAKT.get("claim_phrasings_handled") or []), (
        f"Phrasing nicht dokumentiert: {claim!r}")
    assert _trifft(claim), f"{FID} trifft {claim!r} nicht"


def test_dokumentierte_phrasings_sind_genau_die_gepruften():
    """Wer eine Phrasing in der Datei ergaenzt, ergaenzt sie auch hier."""
    doku = set(FAKT.get("claim_phrasings_handled") or [])
    assert doku == set(MUSS), doku ^ set(MUSS)


def test_alltagsformulierungen_sind_dabei():
    """Ein Teil ist so formuliert, wie Menschen tippen: Alltagswort
    (Zahlmeister, blechen, Brüssel fuer die EU) und Sorgefrage."""
    alltag = [
        "Zahlt Deutschland zu viel an die EU?",                                # Sorgefrage
        "Deutschland ist der Zahlmeister der EU",                              # Alltagswort
        "Wir Deutschen überweisen jedes Jahr Milliarden nach Brüssel",         # Brüssel = EU
        "Kriegen wir Deutschen von der EU weniger zurück, als wir einzahlen?",  # Sorgefrage
        "Lohnt sich die EU für Deutschland überhaupt?",                         # Sorgefrage
        "Wir Deutschen blechen für die ganze EU",                              # Alltagswort
    ]
    assert set(alltag) <= set(MUSS)


# Nicht dokumentiert, aber dieselbe Sache in anderem Wortlaut — darunter die
# drei Katalogtexte (Faktenchecks), die der Fakt nach dem Fix noch trifft.
AUCH = [
    "Deutschland ist der größte Nettozahler der EU",
    "Deutschland sei der größte Nettozahler zum EU-Haushalt.",
    "Deutschland verschuldet sich beim IWF, um weiterhin Geld an die EU zahlen zu können.",
    "damit Deutschland nicht noch mehr Geld an die Europäische Union bezahlen müsse",
    "Die EU kostet Deutschland jedes Jahr Milliarden",
    "Was kostet Deutschland die EU-Mitgliedschaft?",
    "Profitiert Deutschland von der EU?",
    "Was hat Deutschland eigentlich von der EU?",
    "Was bringt Deutschland die EU?",
    "Der Binnenmarkt bringt jedem Deutschen 1.000 Euro im Jahr",
    "Deutschland zahlt bei der EU nur drauf",
    "Wir Deutschen finanzieren die halbe EU",
    "Deutschlands EU-Beitrag ist zu hoch",
    "Deutschland bekommt weniger EU-Gelder, als es einzahlt",
    "Warum zahlen die Deutschen so viel an die EU?",
    "Deutschland muss jedes Jahr Milliarden an die EU zahlen",
    "Deutschland und Österreich sind Nettozahler der EU",
]


@pytest.mark.parametrize("claim", AUCH)
def test_anderer_wortlaut_trifft_auch(claim):
    assert _trifft(claim), claim


# ---------------------------------------------------------------------------
# 2. Fremde Claims treffen ihn nicht mehr — gegen das ganze Pack geprueft
# ---------------------------------------------------------------------------

FREMD_OESTERREICH = [
    "Österreich zahlt mehr an die EU, als es zurückbekommt",
    "Österreich ist Nettozahler in der EU",
    "Österreichs EU-Beitrag beträgt rund drei Milliarden Euro im Jahr",
    "Wie viel zahlt Österreich an die EU?",
    "Zahlt Österreich zu viel an die EU?",
    "Wir Österreicher zahlen mehr an die EU, als wir zurückbekommen",
    "Österreich bekommt aus Brüssel mehr Förderungen, als es einzahlt",
    "Österreichs Bauern bekommen Milliarden an EU-Agrarförderung",
    "Ohne EU-Agrarförderung würden Österreichs Bauern nicht überleben",
    "Österreich ist seit 1995 Mitglied der EU",
    "Die EU-Mitgliedschaft hat Österreich Wohlstand gebracht",
    "Österreichs EU-Mitgliedschaft kostet jedes Jahr Milliarden",
    "Österreich hat vom EU-Beitritt profitiert",
    "Lohnt sich die EU für Österreich?",
    "Österreich sollte aus der EU austreten",
    # aus den Katalogen und Phrasings, die vorher feuerten
    "Die Inflation in Österreich liegt über dem EU-Durchschnitt",
    "Die Wahlbeteiligung in Österreich ist im europäischen Vergleich hoch",
    "Österreich hat die höchste Steuerlast der EU",
    "Österreich hat nicht die höchste Steuerlast in der EU",
    "Die Reallöhne in Österreich sind höher als im EU-Schnitt",
    "Österreich Rang Asyl EU",
    "Österreich macht bei EU-Verteidigung nichts mit",
]

FREMD_EU_KOSTEN = [
    "Die EU-Bürokratie kostet Milliarden",
    "Das EU-Parlament kostet die Steuerzahler zwei Milliarden Euro im Jahr",
    "Der Green Deal der EU kostet uns Billionen",
    "Sanktionen kosten EU mehr als Russland",
    "Die EU gibt ein Drittel ihres Budgets für Agrarsubventionen aus",
    "Der EU-Haushalt 2021 bis 2027 umfasst 1,2 Billionen Euro",
    "Die Ukraine-Hilfe der EU kostet uns Milliarden",
    "Die EU verschwendet unser Steuergeld",
    "EU-Beamte verdienen mehr als 20.000 Euro im Monat",
    "EU-Verteidigungsfonds hat 8 Milliarden Euro",
]

FREMD_DEUTSCHLAND = [
    "Deutschland hat eine der höchsten Förderschulquoten in Europa",
    "Die Inflation ist in Deutschland höher als in der EU",
    "Deutschland zahlt die höchsten Strompreise in Europa",
    "Deutsche zahlen mehr Steuern als der EU-Durchschnitt",
    "Die Deutschen zahlen mehr Steuern und bekommen weniger Rente als im EU-Schnitt",
    "Deutschland ist wirtschaftliches Schlusslicht in Europa",
    "Deutschland nimmt die meisten Flüchtlinge in der EU auf",
    "Die EU-Klimapolitik kostet Deutschland Milliarden",
    "Deutschland gilt als Vorbild für andere EU-Länder",
    "Der deutsche Kanzler reist nach Brüssel zum Europa-Gipfel",
    "Der deutsche Binnenmarkt schwächelt",
    "Deutsche Urlauber zahlen in Europa mit Euro",
    "Deutschland zahlt seine Schulden in Euro zurück",
    "Bayern ist der größte Nettozahler im Länderfinanzausgleich",
    # „Zahlen" als Nomen
    "Die Zahlen zur illegalen Migration in der EU",
    "Die Zahlen zur Zuwanderung aus Deutschland in die EU-Staaten",
]

FREMD = FREMD_OESTERREICH + FREMD_EU_KOSTEN + FREMD_DEUTSCHLAND


@pytest.mark.parametrize("claim", FREMD)
def test_fremder_claim_bekommt_den_fakt_nicht(claim):
    assert not _trifft(claim), f"{claim!r} trifft {FID} exakt"
    assert FID not in _pack_liefert(claim), f"{claim!r}: Pack liefert {FID}"


def test_keine_fremde_phrasing_trifft_den_fakt():
    """Alle dokumentierten Phrasings anderer Fakten, exakt ODER tolerant —
    konservativ fuer die Leck-Suche. Vorher: 14 Treffer, alle fremd."""
    fremd = [(datei, _fid(it), ph) for datei, it in ALLE if _fid(it) != FID
             for ph in it.get("claim_phrasings_handled") or []
             if _trifft(ph) or tippfehler_match(FAKT, ph.lower())]
    assert not fremd, fremd


def test_fremde_phrasings_wurden_wirklich_geprueft():
    """Sonst meldet der Test oben auf einer leeren Menge gruen."""
    n = sum(len(it.get("claim_phrasings_handled") or [])
            for _d, it in ALLE if _fid(it) != FID)
    assert n >= 2600, n


def test_gift_probe_oesterreich_im_anker():
    """Gegenprobe des Instruments: Steht „österreich" wieder im Anker, MUSS
    ein Österreich-Claim den Fakt treffen."""
    it = copy.deepcopy(FAKT)
    for regel in _regeln(it):
        for gruppe in regel:
            if "deutschland" in gruppe:
                gruppe.append("österreich")
    assert _trifft("Österreich ist Nettozahler in der EU", it)
    assert not _trifft("Österreich ist Nettozahler in der EU")


# ---------------------------------------------------------------------------
# 3. Die neuen Phrasings ziehen nur gesichtete fremde Fakten
# ---------------------------------------------------------------------------

# Beide am Thema: die EU-Mitgliedschaft (Eurobarometer) und der
# EU-Binnenmarkt (Welthandel). Die erste Zeile gab es schon vor dem Fix.
GESICHTET = {
    ("eb_eu_mitgliedschaft_2024", "EU-Mitgliedschaft kostet uns Milliarden"),
    ("eu_binnenmarkt_wirkung_2026", "Profitiert Deutschland überhaupt vom EU-Binnenmarkt?"),
}


def test_phrasings_treffen_nur_gesichtete_fremde_fakten():
    """„Deutschland ist der Zahlmeister Europas" zog ueber [deutschland] x
    [europa] den IMF-Fakt; die Phrasing heisst darum „… der EU"."""
    ueber = {(_fid(it), claim) for claim in MUSS for _d, it in ALLE
             if _fid(it) != FID and substring_or_composite_match(it, claim.lower())}
    assert ueber == GESICHTET, {"neu": ueber - GESICHTET, "weg": GESICHTET - ueber}


# ---------------------------------------------------------------------------
# 4. Englisch: der Korpus-Claim bleibt, Österreich faellt weg
# ---------------------------------------------------------------------------

def _korpus_en() -> str:
    with open(os.path.join(BACKEND, "tools", "englisch_korpus.json"), encoding="utf-8") as fh:
        eintraege = [c for c in json.load(fh)["claims"] if c["id"] == FID]
    assert len(eintraege) == 1, eintraege
    return eintraege[0]["en"]


def test_englischer_korpus_claim_erreicht_den_fakt():
    """tests/test_englisch_trigger.py zaehlt die Rueckgewinnung nur als
    Quote. Hier steht der eine Claim zu diesem Fakt einzeln: „gets back" ist
    der englische Rest, den das Glossar nicht uebersetzt."""
    claim = _korpus_en()
    assert claim == "Germany pays more into the EU than it gets back"
    assert FID in _pack_liefert(claim)


@pytest.mark.parametrize("claim", [
    "Austria pays more into the EU than it gets back",
    "Austria's tax burden is the highest in the EU",
])
def test_englischer_oesterreich_claim_bekommt_ihn_nicht(claim):
    assert FID not in _pack_liefert(claim)


# ---------------------------------------------------------------------------
# 5. Die Form der Trigger
# ---------------------------------------------------------------------------

def test_jede_regel_verlangt_deutschland():
    regeln = _regeln(FAKT)
    assert len(regeln) >= 8, len(regeln)
    for regel in regeln:
        assert any(set(g) == DE_ANKER for g in regel), regel


def test_kein_anderes_land_in_den_triggern():
    toks = [normalisiere(t) for regel in _regeln(FAKT) for g in regel for t in g]
    toks += [normalisiere(t) for t in FAKT.get("trigger_keywords") or []]
    for land in ("oesterreich", "austria", "schweiz", "frankreich", "italien"):
        assert not any(land in t for t in toks), land


def test_laenderlos_nur_woertlich():
    """Ohne Deutschland-Anker feuern nur die drei dokumentierten Saetze
    selbst — als Keyword, nicht als Gruppe."""
    kws = {normalisiere(k) for k in FAKT.get("trigger_keywords") or []}
    for satz in ("EU-Mitgliedschaft kostet uns Milliarden",
                 "Wir sind nur Netto-Zahler in der EU",
                 "EU-Beitrag lohnt sich nicht"):
        assert normalisiere(satz.lower()) in kws, satz

    # Jedes Keyword ist einer dieser Saetze, hoechstens anders getrennt
    # („Nettozahler" statt „Netto-Zahler").
    def kompakt(s):
        return normalisiere(s.lower()).replace(" ", "")

    for k in kws:
        assert any(kompakt(k) == kompakt(p) for p in MUSS), k


def test_zahlen_als_nomen_ist_kein_token():
    """„zahlen" ist auch das Nomen („Die Zahlen zur Migration …"). Blank
    darf es nicht stehen, auch nicht als Praefix „zahle"."""
    toks = {normalisiere(t).strip() for regel in _regeln(FAKT) for g in regel for t in g}
    assert "zahlen" not in toks and "zahle" not in toks


def test_kurze_tokens_sind_gebunden():
    toks = [t for regel in _regeln(FAKT) for g in regel for t in g]
    kurz = [t for t in toks if len(t.strip()) < 4 and not (t.startswith(" ") and t.endswith(" "))]
    assert not kurz, kurz


def test_pack_bleibt_exakt():
    with open(os.path.join(BACKEND, "services", "wirtschaftspolitik_pack.py"), encoding="utf-8") as fh:
        assert "descriptor_fn=None" in fh.read()


# ---------------------------------------------------------------------------
# 6. Bekannter Rest und bekannte Luecke — beidseitig gepinnt
# ---------------------------------------------------------------------------

def test_bekannter_rest_woertlicher_satz_im_oesterreich_claim():
    """Der Matcher ist monoton. Der dokumentierte Satz „EU-Beitrag lohnt sich
    nicht" steckt woertlich darin, also trifft er. Ohne Negation in der
    Trigger-Sprache nicht trennbar. Wer das schliesst, dreht die Zeile um."""
    assert _trifft("Österreichs EU-Beitrag lohnt sich nicht")


def test_bekannte_luecke_laenderloses_wir():
    """Der Preis des Fixes, am Katalog gemessen: Ein BR24-Faktencheck zur
    Europawahl zitiert sinngemaess „Wir sind ein Netto-Zahler-Land … wir
    zahlen mehr, als wir kriegen". Deutschland ist gemeint, steht aber nicht
    da. Ein laenderloses „wir" kann ebenso Österreich sein. Darum bleibt er
    draussen; wer ihn hereinholt, braucht ein anderes Signal als „wir"."""
    assert not _trifft("Wir sind ein Netto-Zahler-Land, wir zahlen mehr ein, als wir kriegen")


def test_tokens_sind_keine_regex():
    """Die Trigger sind Substrings; ein Sonderzeichen waere ein Tippfehler."""
    toks = [t for regel in _regeln(FAKT) for g in regel for t in g]
    toks += list(FAKT.get("trigger_keywords") or [])
    bad = [t for t in toks if re.search(r"[\\()\[\]*+?|^$]", t)]
    assert not bad, bad
