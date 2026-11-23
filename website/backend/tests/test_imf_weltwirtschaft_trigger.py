"""``imf_weltwirtschaft_2024`` feuert nur noch auf Claims in seinem Scope.

Der Fakt (Internationale Quellen) belegt den IMF World Economic Outlook vom
Oktober 2024: Weltwirtschaft 2024 +3,2 %, Industrie- gegen Schwellenlaender,
Deutschland 2024 bei 0 % (Stagnation, Schlusslicht im Vergleich), dazu die
Raten grosser Volkswirtschaften (USA, Japan, EU-27, FR, IT, UK, China,
Indien, Brasilien) und die Risiken des Ausblicks.

Bis zu diesem Fix hatte ``trigger_composite`` zwei Gruppen:
[imf, iwf, weltwirtschaft, …, deutschland, deutsch, wirtschaft] x
[wachstum, schrumpft, …, krise, prognose, europa, in europa]. Damit reichte
„Deutschland" + „Europa", „Wirtschaft" + „Europa" oder „deutsch" + „Krise".
Gefunden hat das H beim Bau von #325 („Deutschland ist der Zahlmeister
Europas" zog den Fakt). Gemessen vor dem Fix, exakter Pass:

    dokumentierte Phrasings ANDERER Fakten      4 von 2.663 treffen den
                                                Fakt, davon 1 fremd
                                                (Bevoelkerung) und 3 am
                                                Thema (DESTATIS-BIP)
    Claim-Texte der Messkataloge                12 von 3.286, alle 12 fremd
    alle Katalogtexte (Evidora Dokumente        84 von 48.476, davon 5 im
    + ClaimReview-Index)                        Scope, 79 Fehlzuendungen
    Texte der Katalog-PDFs                      17 von 5.484, davon 5 im
                                                Scope, 12 Fehlzuendungen

Foerderschulquote in Deutschland und Europa, der EU-Binnenmarkt („hat die
Wirtschaft um 5 Prozent wachsen lassen"), Masern in Europa, die
Weltwirtschaftskrise von 1929, die Corona-Krise, Lohnwachstum in
Deutschland, „Deutschland schrumpft" (Bevoelkerung) — jeder dieser Texte
bekam den IMF-Ausblick dazu.

DAS NEUE MUSTER
===============
Jede Regel verlangt einen Anker aus dem Scope:

    Deutschland  x Wirtschaftswort x Lage    (Composite, die Phrasing
                                              „wirtschaftliches Schlusslicht")
    Deutschland  x Rezession, Wirtschaftswachstum, Wachstumsprognose,
                   „kranke Mann Europas" …
    Deutschland  x Wachstum x Industriestaaten/-laender/-nationen, G7
    Weltwirtschaft, globale Wirtschaft, Weltkonjunktur, Welt-BIP x Lage
    globales/weltweites Wachstum x negativ, schrumpft, Prognose …
    IMF/IWF/Waehrungsfonds x Prognose, Wachstum, Rezession …
    USA, China, Japan, Indien, Brasilien, FR, IT, UK, Eurozone
                 x Wirtschaftswort x Lage    (die Laender des Fakts)

Dazu Keywords, die fuer sich im Scope liegen: World Economic Outlook,
Weltwirtschaftswachstum, weltweite/globale Rezession.

„Europa" und „EU" sind kein Anker mehr, auch nicht als Lage-Wort. Die
Lage-Woerter sind so gebunden, dass sie keine fremden Komposita treffen:
„ wachstum" (nicht Lohn-/Bevoelkerungswachstum), „ prognose" (nicht
Wahlprognose), „in der krise" statt „krise" (nicht Corona-/Klima-/
Weltwirtschaftskrise), „weltwirtschaft " (nicht Weltwirtschaftsforum),
„ wirtschaft " (nicht Landwirtschaft, Wirtschaftsfluechtlinge), „ wächst"
(nicht schWÄCHSTe), „ kollaps" (nicht Verkehrskollaps), „ flaute" (nicht
Dunkelflaute), kein „wuchs" (Nachwuchs), „kranke mann europas" statt
„kranke mann".

Nachher: 0 Fehlzuendungen in allen vier Mengen. Die Treffer im Scope
bleiben (3 Phrasings, 5 Katalogtexte, 5 PDF-Texte), dazu kommt 1 neuer im
Scope (ein PDF-Text ueber den World Economic Outlook). Aus den Katalogen
geht kein Treffer im Scope verloren.

GRENZEN
=======
Der Matcher ist monoton: Ein Wort mehr im Claim nimmt nie einen Treffer
weg. Steht „wirtschaftlich" und „Schlusslicht" im selben Satz, trifft der
Fakt, auch wenn es um Digitalisierung geht. Gepinnt als bekannter Rest.
Der Preis: Zwei Claims im Scope, die der alte Trigger traf, treffen nicht
mehr. „Europas Wirtschaft schrumpft" braucht [Wirtschaft] x [Europa], und
das ist genau die Binnenmarkt-Fehlzuendung. „Deutschland hat das schwächste
Wachstum in Europa" braucht [Deutschland] x [Europa] oder ein Rangwort. Die
Rangwort-Fassung (schwächste, geringste …) zog im Entwurf vier
Grippewellen-Texte („die schwächste der letzten vier Saisonen") und traefe
Parteien-Claims („Das Wachstum der AfD ist in Deutschland am geringsten").
Beide Claims stehen nicht in den Katalogen; gepinnt als bekannte Luecken.

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
from services._topic_match import substring_or_composite_match  # noqa: E402
from services.internationale_quellen import _claim_matches_facts  # noqa: E402

DATEI = os.path.join(DATA, "internationale_quellen.json")
FID = "imf_weltwirtschaft_2024"
TF = ("trigger_keywords", "trigger_composite", "trigger_all")


def _fakt() -> dict:
    with open(DATEI, encoding="utf-8") as fh:
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


def _dienst_liefert(claim: str) -> list[str]:
    """Was der Dienst in Produktion liefert: exakt, sonst tolerant, sonst
    englisch, ``descriptor_fn=None`` — die Funktion aus dem Dienst selbst."""
    return [f.get("id") for f in _claim_matches_facts(claim.lower(), full_claim=claim)]


def _regeln(fakt: dict) -> list[list]:
    regeln = []
    if fakt.get("trigger_composite"):
        regeln.append(fakt["trigger_composite"])
    regeln += list(fakt.get("trigger_all") or [])
    return regeln


def _tokens(fakt: dict) -> list[str]:
    toks = [t for regel in _regeln(fakt) for g in regel for t in g]
    return toks + list(fakt.get("trigger_keywords") or [])


# ---------------------------------------------------------------------------
# 1. Muss-Treffer: jede Phrasing ihr eigener Fakt
# ---------------------------------------------------------------------------

# Ausgeschrieben, nicht aus der Datei gelesen: Sonst bestuende der Test auch
# dann, wenn jemand eine Phrasing still entfernt.
MUSS = [
    # die drei von vorher
    "Die Weltwirtschaft kollabiert",
    "Deutschland ist wirtschaftliches Schlusslicht in Europa",
    "Globales Wachstum ist negativ",
    # neu: so, wie Menschen tippen
    "Geht die Weltwirtschaft den Bach runter?",
    "Ist Deutschland jetzt der kranke Mann Europas?",
    "Steckt die deutsche Wirtschaft in der Rezession?",
    "Kommt jetzt eine weltweite Rezession?",
    "Wächst die deutsche Wirtschaft heuer überhaupt noch?",
    "Der IWF sagt für Deutschland null Wachstum voraus",
    "Die Weltwirtschaft wächst 2024 um 3,2 Prozent",
    "Stimmt es, dass Deutschlands Wirtschaft schrumpft?",
]


@pytest.mark.parametrize("claim", MUSS)
def test_phrasing_trifft_ihren_fakt(claim):
    assert claim in (FAKT.get("claim_phrasings_handled") or []), (
        f"Phrasing nicht dokumentiert: {claim!r}")
    assert _trifft(claim), f"{FID} trifft {claim!r} nicht"
    assert FID in _dienst_liefert(claim)


def test_dokumentierte_phrasings_sind_genau_die_gepruften():
    """Wer eine Phrasing in der Datei ergaenzt, ergaenzt sie auch hier."""
    doku = set(FAKT.get("claim_phrasings_handled") or [])
    assert doku == set(MUSS), doku ^ set(MUSS)


def test_alltagsformulierungen_sind_dabei():
    """Ein Teil ist so formuliert, wie Menschen tippen: Alltagswort,
    oesterreichisch, Sorgefrage."""
    alltag = [
        "Geht die Weltwirtschaft den Bach runter?",               # Alltagswort, Sorgefrage
        "Ist Deutschland jetzt der kranke Mann Europas?",         # Redewendung, Sorgefrage
        "Steckt die deutsche Wirtschaft in der Rezession?",       # Sorgefrage
        "Kommt jetzt eine weltweite Rezession?",                  # Sorgefrage
        "Wächst die deutsche Wirtschaft heuer überhaupt noch?",   # oesterreichisch „heuer"
        "Der IWF sagt für Deutschland null Wachstum voraus",      # IWF statt IMF
        "Stimmt es, dass Deutschlands Wirtschaft schrumpft?",     # Sorgefrage
    ]
    assert set(alltag) <= set(MUSS)


# Nicht dokumentiert, aber dieselbe Sache in anderem Wortlaut — darunter die
# Katalogtexte, die den Fakt nach dem Fix weiter treffen, und die drei
# Phrasings des DESTATIS-BIP-Fakts (dasselbe Thema, anderer Dienst).
AUCH = [
    "Der IMF prognostiziert eine globale Rezession 2024.",
    "Globales Wirtschaftswachstum ist negativ.",
    "Deutschland liegt beim Wirtschaftswachstum weltweit auf dem letzten Platz (195 von 195).",
    "Deutschland steckt in tiefer Rezession",
    "Deutschland-Wirtschaft kollabiert",
    "BIP Deutschland schrumpft",
    "Die Weltwirtschaft schrumpft",
    "Der IWF senkt seine Wachstumsprognose für Deutschland",
    "Laut IWF wächst die Weltwirtschaft um 3,2 Prozent",
    "Der Internationale Währungsfonds erwartet für Deutschland 0,8 Prozent Wachstum",
    "Deutschland ist das Schlusslicht beim Wachstum unter den Industriestaaten, sagt der IWF",
    "Die deutsche Wirtschaft stagniert",
    "Die deutsche Wirtschaft ist 2024 nicht gewachsen, sie stagniert",
    "Die deutsche Wirtschaft erholt sich 2025",
    "Die Konjunktur in Deutschland bricht ein",
    "Die Prognose für die deutsche Wirtschaft wurde gesenkt",
    "Deutschland hat das schwächste Wirtschaftswachstum in der EU",
    "Ist Deutschland wieder der kranke Mann Europas?",
    "Die Weltwirtschaft steckt in der Krise",
    "Droht der Weltwirtschaft ein Crash?",
    "Die globale Wirtschaftslage verschlechtert sich, eine Rezession droht",
    "Chinas Wirtschaft wächst 2024 um fast 5 Prozent",
    "Die US-Wirtschaft steckt in der Rezession",
    "Indiens Wirtschaft wächst am schnellsten",
    "Die Wirtschaft der Eurozone stagniert",
    "Der World Economic Outlook sieht die Weltwirtschaft stabil",
    "Deutschland hat das geringste Wachstum der Industriestaaten",
    "Unter den Industrieländern wächst Deutschland am schwächsten",
    "Deutschland hat das schwächste Wachstum aller G7-Staaten",
]


@pytest.mark.parametrize("claim", AUCH)
def test_anderer_wortlaut_trifft_auch(claim):
    assert _trifft(claim), claim


# ---------------------------------------------------------------------------
# 2. Fremde Claims treffen ihn nicht mehr — exakt UND im ganzen Dienst
# ---------------------------------------------------------------------------

# [deutsch] x [europa] — der gemeldete Fall und seine Verwandten
FREMD_DEUTSCHLAND_EUROPA = [
    "Deutschland ist der Zahlmeister Europas",
    "Deutschland hat eine der höchsten Förderschulquoten in Europa",
    "Mein Kind geht in eine Förderschule, hat Deutschland eine der höchsten Quoten in Europa?",
    "Förderschulquote Deutschland im europäischen Vergleich",
    "Die Preise steigen in Deutschland schneller als im restlichen Europa",
    "Deutschland muss 82 Prozent seines Strombedarfs aus Europa importieren.",
    "Ist ja tatsächlich so, dass nur Deutschland ein Sonderaufnahmeprogramm in Europa macht.",
    "Anteil radikaler Muslime Deutschland Europa",
    "Die Masern-Fallzahlen in Europa nehmen zu, in Deutschland stiegen die Fälle stark",
    "Bei der Europameisterschaft 2024 gab es keine Deutschlandflaggen zu sehen",
    "Mein Freund aus Deutschland meint, Österreich ist ein Korruptionsland wie in Osteuropa",
    "Deutschland zahlt die höchsten Strompreise in Europa",
    "Deutschland nimmt die meisten Flüchtlinge in Europa auf",
    "Deutschland hat die höchste Steuerlast in Europa",
    "Die Deutsche Bahn ist die unpünktlichste Bahn Europas",
    "Deutsche Autos sind die besten in Europa",
    "Deutschland ist beim Klimaschutz Schlusslicht in Europa",
    "Deutschland ist Schlusslicht bei der Digitalisierung in Europa",
]

# [wirtschaft] x [europa / wachstum] — der EU-Binnenmarkt und andere Regionen
FREMD_WIRTSCHAFT = [
    "Der gemeinsame Markt in Europa hat die Wirtschaft um 5 Prozent wachsen lassen",
    "Hat der gemeinsame Markt in Europa die Wirtschaft um 5 Prozent wachsen lassen?",
    "EU-Binnenmarkt Wirtschaftswachstum 5 Prozent",
    "Die EU-Klimapolitik ruiniert die Wirtschaft in Europa",
    "Bayern hat im Wirtschaftswachstum stärker zugelegt als andere Bundesländer",
    "Die Wirtschaft in Wien wächst",
    "Österreichs Wirtschaft schrumpft das zweite Jahr in Folge",
    "Die österreichische Wirtschaft steckt in der Rezession",
    "Wächst die Wirtschaft in Österreich heuer überhaupt?",
    "Die Inflation in Österreich liegt über dem EU-Durchschnitt",
    "Die Landwirtschaft in Deutschland steckt in der Krise",
    "Wichtige Akteure der Coronakrise sind wirtschaftlich eng miteinander verflochten.",
    "Die Corona-Krise ist eine riesige Herausforderung für die deutsche Wirtschaft",
    "Die Klimakrise kostet die deutsche Wirtschaft Milliarden",
    "Die Energiekrise trifft Europa hart",
    "Der Euro ist in der Krise",
]

# [deutsch] x [schrumpft / wachstum / prognose] ohne Wirtschaft
FREMD_DEUTSCHLAND_SONST = [
    "Deutschland schrumpft",
    "Lohnwachstum Deutschland 2026 inflationsbereinigt",
    "Das Bevölkerungswachstum in Deutschland stagniert",
    "Das Bevölkerungswachstum in Deutschland belastet die Wirtschaft",
    "Das Lohnwachstum in der deutschen Wirtschaft ist zu niedrig",
    "Wasserstoffinfrastruktur Prognose Deutschland",
    "Wahlprognose für Deutschland: AfD bei 20 Prozent",
    "Wahlprognose Deutschland: Die Wirtschaft ist das wichtigste Thema",
    "Die AfD wächst in Deutschland schneller als in Europa",
    "Die Zahl der Wirtschaftsflüchtlinge in Deutschland wächst",
    "Migranten lassen die deutsche Wirtschaft wachsen",
    "Der deutsche Wirtschaftsminister trat zurück",
    "Deutschland ist das größte Land in Europa",
    "Die deutsche Sprache ist die meistgesprochene Muttersprache in Europa",
    "Deutschland verschuldet sich beim IWF, um weiterhin Geld an die EU zahlen zu können.",
]

# Geschichte, Davos, Personen — und die Lecks, die der Entwurf hatte
FREMD_SONST = [
    "Trump behauptete, die Weltwirtschaftskrise hätte niemals stattgefunden, "
    "wenn die USA Zölle beibehalten hätten",
    "Die Weltwirtschaftskrise 1929 führte zu Massenarbeitslosigkeit in Deutschland",
    "Die Weltwirtschaftskrise von 1929 begann in den USA",
    "Das Weltwirtschaftsforum in Davos plant den Great Reset",
    "Das Weltwirtschaftsforum hat die Corona-Krise geplant",
    "Die frühere Chefin des Internationalen Währungsfonds soll gesagt haben, "
    "alte Menschen seien ein Risiko für die Weltwirtschaft",
    "Der IWF zwingt Griechenland zu Sparprogrammen",
    "Der IWF ist eine Lobby-Organisation der USA",
    "Fachkräfte-Nachwuchs: Die deutsche Wirtschaft sucht Lehrlinge",   # „wuchs"
    "Ein kranker Mann aus Deutschland wurde im Spital abgewiesen",     # „kranke mann"
    "Wahlprognose USA: Die Wirtschaft entscheidet die Wahl",           # „prognose"
    "Die Grippewelle in Deutschland war die schwächste der letzten vier Saisonen",  # „wächst"
    "Deutschland ist unter den Industriestaaten am schwächsten bei der Digitalisierung",
    "Der IWF nennt die schwächsten Länder Afrikas",
    "Verkehrskollaps in Deutschland kostet die Wirtschaft Milliarden",  # „kollaps"
    "Dunkelflaute: Deutschland importiert Strom, die Wirtschaft leidet",  # „flaute"
    "Wohnungseinbrüche in Deutschland verursachen volkswirtschaftliche Schäden",
    "Chinas Einfluss in Afrika wächst",
    "In Italien ist die Mafia in der Krise",
    "Japan hat die älteste Bevölkerung, sie schrumpft",
    "Brexit kostet UK 4 % BIP",
    "Der Euro ist eine Weichwährung, die Eurozone zerfällt",
]

FREMD = FREMD_DEUTSCHLAND_EUROPA + FREMD_WIRTSCHAFT + FREMD_DEUTSCHLAND_SONST + FREMD_SONST


@pytest.mark.parametrize("claim", FREMD)
def test_fremder_claim_bekommt_den_fakt_nicht(claim):
    assert not _trifft(claim), f"{claim!r} trifft {FID} exakt"
    assert FID not in _dienst_liefert(claim), f"{claim!r}: Dienst liefert {FID}"


# Drei Phrasings des DESTATIS-BIP-Fakts treffen den IMF-Fakt — am Thema:
# Deutschlands Wirtschaft 2024, zwei Dienste beantworten dieselbe Frage.
# Die vierte von vorher („Deutschland schrumpft", Bevoelkerung) ist weg.
FREMD_PHRASING_AM_THEMA = {
    ("destatis_bip_2024", "Deutschland steckt in tiefer Rezession"),
    ("destatis_bip_2024", "Deutschland-Wirtschaft kollabiert"),
    ("destatis_bip_2024", "BIP Deutschland schrumpft"),
}


def test_fremde_phrasings_treffen_nur_am_thema():
    """Alle dokumentierten Phrasings anderer Fakten, exakt ODER tolerant —
    konservativ fuer die Leck-Suche. Vorher: 4 Treffer, davon 1 fremd."""
    treffer = {(_fid(it), ph) for _datei, it in ALLE if _fid(it) != FID
               for ph in it.get("claim_phrasings_handled") or []
               if _trifft(ph) or tippfehler_match(FAKT, ph.lower())}
    assert treffer == FREMD_PHRASING_AM_THEMA, {
        "neu": treffer - FREMD_PHRASING_AM_THEMA, "weg": FREMD_PHRASING_AM_THEMA - treffer}


def test_fremde_phrasings_wurden_wirklich_geprueft():
    """Sonst meldet der Test oben auf einer leeren Menge gruen."""
    n = sum(len(it.get("claim_phrasings_handled") or [])
            for _d, it in ALLE if _fid(it) != FID)
    assert n >= 2600, n


def test_gift_probe_alte_regel_zurueck():
    """Gegenprobe des Instruments: Kommt die alte Regel [deutsch] x [europa]
    als zusaetzliche Regel zurueck, MUSS der gemeldete Claim treffen."""
    it = copy.deepcopy(FAKT)
    it.setdefault("trigger_all", []).append([["deutschland", "deutsch"], ["europa"]])
    claim = "Deutschland hat eine der höchsten Förderschulquoten in Europa"
    assert _trifft(claim, it)
    assert not _trifft(claim)


# ---------------------------------------------------------------------------
# 3. Die neuen Phrasings ziehen nur gesichtete fremde Fakten
# ---------------------------------------------------------------------------

# Der DESTATIS-BIP-Fakt: dasselbe Thema (Deutschlands BIP 2024). Die erste
# Zeile gab es schon vor dem Fix.
GESICHTET = {
    ("destatis_bip_2024", "Deutschland ist wirtschaftliches Schlusslicht in Europa"),
    ("destatis_bip_2024", "Steckt die deutsche Wirtschaft in der Rezession?"),
    ("destatis_bip_2024", "Wächst die deutsche Wirtschaft heuer überhaupt noch?"),
    ("destatis_bip_2024", "Stimmt es, dass Deutschlands Wirtschaft schrumpft?"),
}


def test_phrasings_treffen_nur_gesichtete_fremde_fakten():
    ueber = {(_fid(it), claim) for claim in MUSS for _d, it in ALLE
             if _fid(it) != FID and substring_or_composite_match(it, claim.lower())}
    assert ueber == GESICHTET, {"neu": ueber - GESICHTET, "weg": GESICHTET - ueber}


# ---------------------------------------------------------------------------
# 4. Englisch: der Korpus-Claim bleibt, fremde englische Claims fallen weg
# ---------------------------------------------------------------------------

def _korpus_en() -> str:
    with open(os.path.join(BACKEND, "tools", "englisch_korpus.json"), encoding="utf-8") as fh:
        eintraege = [c for c in json.load(fh)["claims"] if c["id"] == FID]
    assert len(eintraege) == 1, eintraege
    return eintraege[0]["en"]


def test_englischer_korpus_claim_erreicht_den_fakt():
    """tests/test_englisch_trigger.py zaehlt die Rueckgewinnung nur als
    Quote. Hier steht der eine Claim zu diesem Fakt einzeln."""
    claim = _korpus_en()
    assert claim == "The world economy is collapsing"
    assert FID in _dienst_liefert(claim)


@pytest.mark.parametrize("claim", [
    # vorher Treffer ueber den englischen Pass (ClaimReview-Index)
    "Christmas is around the corner, Germany is CANCELLING their markets… and it also "
    "looks like other places in Europe are going to do the same.",
    "U.K. has the highest retirement ages, lowest pensions, lowest minimum wages and "
    "ranks worst in other economic indicators in Europe and the world",
])
def test_englischer_fremder_claim_bekommt_ihn_nicht(claim):
    assert FID not in _dienst_liefert(claim)


def test_englischer_imf_claim_erreicht_den_fakt():
    assert FID in _dienst_liefert("The IMF forecasts a global recession")


# ---------------------------------------------------------------------------
# 5. Die Form der Trigger
# ---------------------------------------------------------------------------

ANKER = {
    "deutschland": {"deutschland", "deutsche", "bundesrepublik", " brd "},
    "welt": {"weltwirtschaft ", "globale wirtschaft", "weltweite wirtschaft", "wirtschaft weltweit",
             "weltkonjunktur", "globale konjunktur", "weltweite konjunktur", "welt bip", "weltbip",
             "globale bip", "weltweite bip", "world economy", "global economy"},
    "welt_wachstum": {"globale wachstum", "weltweite wachstum", "weltwachstum"},
    "imf": {" imf ", " iwf ", "währungsfonds"},
    "land": {" usa ", " us ", "vereinigte staaten", "china", "chines", "japan", "indien",
             "indische", "brasilien", "frankreich", "französ", "italien", "großbritannien",
             "britische", " uk ", "eurozone", "euroraum"},
}


def test_jede_regel_hat_einen_scope_anker():
    regeln = _regeln(FAKT)
    assert len(regeln) == 7, len(regeln)
    for regel in regeln:
        assert sum(set(g) in ANKER.values() for g in regel) == 1, regel


def test_europa_und_eu_sind_kein_token():
    """„Europa" war die Haelfte des Lecks. Einzige Ausnahme: die Redewendung
    „kranke Mann Europas", und die steht nur neben dem Deutschland-Anker."""
    for t in _tokens(FAKT):
        n = normalisiere(t)
        if n == "kranke mann europas":
            continue
        assert "europ" not in n and n.strip() not in ("eu", "eu 27"), t
    regel = next(r for r in _regeln(FAKT) if any("kranke mann europas" in g for g in r))
    assert ANKER["deutschland"] in [set(g) for g in regel]


def test_kein_anderes_land_als_die_des_fakts():
    """Österreich hat im Fakt keine Zahl; Österreichs Wirtschaft gehoert zu
    WIFO/IHS, nicht zum IMF-Ausblick."""
    toks = [normalisiere(t) for t in _tokens(FAKT)]
    for land in ("oesterreich", "austria", "schweiz", "wien", "bayern", "tuerkei"):
        assert not any(land in t for t in toks), land


@pytest.mark.parametrize("blank,warum", [
    ("wachstum", "Lohnwachstum, Bevoelkerungswachstum: nur „ wachstum\" (Wortanfang)"),
    ("prognose", "Wahlprognose: nur „ prognose\" (Wortanfang)"),
    ("krise", "Corona-, Klima-, Weltwirtschaftskrise: nur „in der krise\" usw."),
    ("weltwirtschaft", "Weltwirtschaftsforum, -krise: nur „weltwirtschaft \" (Wortende)"),
    ("wirtschaft", "Landwirtschaft, Wirtschaftsfluechtlinge: nur „ wirtschaft \""),
    ("wuchs", "Nachwuchs"),
    ("wächst", "schWÄCHSTe, schWÄCHSTen: nur „ wächst\" (Wortanfang)"),
    ("wachsen", "erwachsen, zusammenwachsen: nur „ wachsen\""),
    ("kollaps", "Verkehrskollaps: nur „ kollaps\", „wirtschaftskollaps\""),
    ("flaute", "Dunkelflaute, Windflaute: nur „ flaute\""),
    ("kranke mann", "„ein kranker Mann\": nur „kranke mann europas\""),
    ("deutsch", "deutschsprachig, Deutschkurs: nur Deutschland/Deutsche"),
    ("einbruch", "Wohnungseinbruch: nur „bricht ein\", „konjunktureinbruch\" usw."),
    ("imf", "kurz: nur gebunden"),
    ("iwf", "kurz: nur gebunden"),
])
def test_kein_blankes_leck_token(blank, warum):
    assert blank not in {t for t in _tokens(FAKT)}, warum


def test_kurze_tokens_sind_gebunden():
    kurz = [t for t in _tokens(FAKT)
            if len(t.strip()) < 4 and not (t.startswith(" ") and t.endswith(" "))]
    assert not kurz, kurz


def test_tokens_sind_keine_regex():
    """Die Trigger sind Substrings; ein Sonderzeichen waere ein Tippfehler."""
    bad = [t for t in _tokens(FAKT) if re.search(r"[\\()\[\]*+?|^$]", t)]
    assert not bad, bad


def test_dienst_bleibt_exakt():
    with open(os.path.join(BACKEND, "services", "internationale_quellen.py"), encoding="utf-8") as fh:
        assert "descriptor_fn=None" in fh.read()


def test_andere_fakten_der_datei_unveraendert_erreichbar():
    """Die drei Nachbarn im selben Dienst behalten ihre Phrasings."""
    with open(DATEI, encoding="utf-8") as fh:
        andere = [f for f in json.load(fh)["facts"] if f["id"] != FID]
    assert {f["id"] for f in andere} == {"pew_demokratie_2024", "wmo_klimazustand_2024",
                                         "wto_welthandel_2024"}
    for f in andere:
        for ph in f.get("claim_phrasings_handled") or []:
            assert f["id"] in _dienst_liefert(ph), (f["id"], ph)


# ---------------------------------------------------------------------------
# 6. Bekannter Rest und bekannte Luecken — beidseitig gepinnt
# ---------------------------------------------------------------------------

def test_bekannter_rest_wirtschaftlich_und_schlusslicht():
    """Der Matcher ist monoton. „wirtschaftlich" + „Schlusslicht" + Deutschland
    ist genau die Phrasing des Fakts; geht es um Digitalisierung, trifft er
    trotzdem. Wer das schliesst, dreht die Zeile um."""
    assert _trifft("Wirtschaftlich gesehen ist Deutschland Schlusslicht bei der Digitalisierung")


def test_bekannter_rest_weltweites_wachstum_der_bevoelkerung():
    """„weltweites Wachstum" + „verlangsamt" ist die Form von „Globales
    Wachstum ist negativ" — auch wenn die Bevoelkerung gemeint ist."""
    assert _trifft("Das weltweite Wachstum der Bevölkerung verlangsamt sich")


@pytest.mark.parametrize("claim,warum", [
    ("Deutschland hat das schwächste Wachstum in Europa",
     "[Deutschland] x [Europa] ist das Leck; die Rangwort-Fassung traefe "
     "„Das Wachstum der AfD ist in Deutschland am geringsten\""),
    ("Europas Wirtschaft schrumpft",
     "„Europa\" + „Wirtschaft\" ist genau die Binnenmarkt-Fehlzuendung"),
])
def test_bekannte_luecke(claim, warum):
    """Der Preis des Fixes: Der alte Trigger traf beide. Wer eine Luecke
    schliesst, braucht ein Signal, das die Fehlzuendungen oben nicht
    zurueckholt."""
    assert not _trifft(claim), warum
