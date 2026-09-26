"""Kurze Trigger-Tokens in fremden Woertern — gebunden, beidseitig gepinnt.

Beim Messen des englischen Trigger-Passes (#206) fiel auf, dass deutsche
Claims auf main schon ueber-triggern. Gemessen am 2026-09-26 mit der echten
``find_matching_items`` (``descriptor_fn=None``) ueber die 2.603
dokumentierten ``claim_phrasings_handled`` von Fakten MIT Trigger-Feldern plus
1.163 Stress-Test-Claims (``tools/stress_tests``, ``stress_test_100``).
Zuordnung je Token per Ablation: dasselbe main, nur dieses Token entfernt.

    Token      Fakt                     Wirt              Phrasings  Stress
    "tum"      ETER TU Muenchen         TUMor, WachsTUM,  17         7
                                        EigenTUM, DaTUM
    "ass"      Aspirin-Konsens          wASSer            4          5
    "de "      ETER Deutschland         StudierenDE_      3          0
    "neutral"  AT-Neutralitaet (in      klimaNEUTRAL      1          3
               BEIDEN Composite-
               Gruppen = Stichwort)
    "eter-daten" ETER Europa            SmartmETER-Daten  1          1
    "ai"       KI-Bewusstsein           UkrAIne           0          1
    "ki"       KI-Jobs, KI-Bewusstsein  KInder            0 (latent, Sonden)
    "at "      ETER Oesterreich         hAT_, StaAT_      0 (latent)
    "tu"       ETER TU Muenchen         sTUdierende       0 (latent)
    "asa"      Aspirin                  nASA              0 (latent)

Branch gegen main, gleiche Messung: dienst-fremde Paare 431 -> 407 (-26),
Stress 1.272 -> 1.255 (-17), Muss-Treffer exakt 2.542 -> 2.549, **0
verloren**. Jeder weggefallene Treffer ist themenfremd; alle eigenen Treffer
der neun Fakten halten.

ZWEI TEILE
==========
1. **Matcher** (``services/_flexion.py``, WORTGRENZEN): Ein Token mit
   Rand-Leerzeichen trifft jetzt auch am Claim-Rand und vor Satzzeichen. Ohne
   das haette `" ki "` jeden Claim verloren, der mit „KI" beginnt — drei
   dokumentierte Phrasings. Allein gemessen: 0 verloren, 7 gewonnen (die
   trafen auf main nie, obwohl dokumentiert), 2 neue dienst-fremde Paare durch
   schon vorhandene gebundene Tokens am Claim-Ende.
2. **Daten**: die Tokens gebunden (`" tum "`, `" ass "`, `" de "` …); bei der
   Neutralitaet die Stichwort-Degeneration aufgeloest — Gruppe 0 gebunden
   (`" neutral"`), in Gruppe 1 statt des blanken „neutral" die praedikative
   Form („ist neutral") plus die im Fakt belegten Rechtsbegriffe
   („verfassung", „1955", „konform").

Methodik wie #205/#206: erst messen, Muss-Treffer-Kontrolle, der Sweep muss
nachweislich anschlagen koennen (Gift-Probe unten).

Dependency-light: reine Trigger-Tests, kein Netz/LLM.
"""
import copy
import glob
import json
import os

import pytest

from services._flexion import (
    ist_gebunden,
    trifft_mit_wortgrenze,
    wortgrenzen_fassung,
)
from services._schreibweise import normalisiere
from services._tippfehler import tippfehler_match
from services._topic_match import find_matching_items, substring_or_composite_match

DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "data")
TRIGGER_FELDER = ("trigger_keywords", "trigger_composite", "trigger_all")


def _fid(it: dict) -> str:
    return it.get("id") or it.get("topic") or ""


def _fakt(datei: str, fid: str) -> dict:
    with open(os.path.join(DATA, datei), encoding="utf-8") as fh:
        d = json.load(fh)
    for it in d.get("facts") or []:
        if fid in (it.get("id"), it.get("topic")):
            return it
    raise AssertionError(f"{fid} nicht in {datei}")


def _trifft_echt(datei: str, fid: str, claim: str) -> bool:
    """Die echte Pipeline-Funktion: exakter Pass, dann tippfehler-tolerant."""
    treffer = find_matching_items(os.path.join(DATA, datei), "facts",
                                  claim_lc=claim.lower(), full_claim=claim,
                                  descriptor_fn=None)
    return any(fid in (x.get("id"), x.get("topic")) for x in treffer)


# Die neun angefassten Fakten
ETER_DE = ("eter.json", "eter_overview_de_2021")
ETER_AT = ("eter.json", "eter_overview_at_2021")
ETER_TUM = ("eter.json", "eter_tu_muenchen_2021")
ETER_EU = ("eter.json", "eter_overview_eu_2021")
ASPIRIN = ("gesundheits_autoritaeten_pack.json",
           "aspirin_primaerpraevention_konsens_2026")
KHK = ("awmf.json", "awmf_nvl_khk")
KI_JOBS = ("arbeitsmarkt_pack.json", "ki_job_verdraengung_2026")
KI_BEWUSST = ("tech_ki_pack.json", "ki_bewusstsein_2026")
NEUTRAL = ("sicherheitspolitik_pack.json", "at_neutralitaet_recht_2026")
ANGEFASST = (ETER_DE, ETER_AT, ETER_TUM, ETER_EU, ASPIRIN, KHK, KI_JOBS,
             KI_BEWUSST, NEUTRAL)


# ---------------------------------------------------------------------------
# 1. Beide Richtungen: Leck zu, eigenes Thema trifft weiter
# ---------------------------------------------------------------------------
# Jede Zeile mit soll=False traf auf main (gemessen). Die soll=True-Zeilen
# trafen auf main ebenfalls — bis auf zwei, die auf main am Claim-Ende
# scheiterten („in DE?", „in AT?").

LECKS = [
    # --- "ki" / "ai" ---
    (KI_JOBS, "Die Hälfte aller Kinder in Österreich ist übergewichtig."),
    (KI_JOBS, "Kinderbetreuung ersetzt die Familie nicht."),
    (KI_JOBS, "In der Ukraine wurde die Hälfte der Kraftwerke zerstört."),
    (KI_JOBS, "Im Mai wurde die Hälfte aller Flüge gestrichen."),
    (KI_BEWUSST, "Kinder sind sich der Gefahren im Internet nicht bewusst."),
    (KI_BEWUSST, "Die Ukraine fühlt sich vom Westen im Stich gelassen."),
    (KI_BEWUSST, "Russland führt Krieg gegen Ukraine seit Februar 2022."),
    # --- "ass" / "asa" ---
    (ASPIRIN, "Jeder Mensch muss täglich 8 Gläser Wasser trinken"),
    (ASPIRIN, "Basisches Wasser ist gesund gegen Tumor"),
    (ASPIRIN, "Ionisiertes Wasser hat gesundheitliche Vorteile"),
    (ASPIRIN, "Fluorid im Trinkwasser ist gefährlich für die Gesundheit."),
    (ASPIRIN, "Die Krankenkasse zahlt die Vorsorge."),
    (ASPIRIN, "Die NASA schützt die Erde vor Asteroiden."),
    (KHK, "Nach einem Herzinfarkt soll man viel Wasser trinken."),
    # --- ETER: "de ", "studentenzahl de", "at ", "tum"/"tu", "eter-daten" ---
    (ETER_DE, "Studierende Quote AT"),
    (ETER_DE, "Lehramts-Studierende Rückgang"),
    (ETER_DE, "Studierende in Österreich zahlen keine Studiengebühren."),
    (ETER_DE, "Die Studentenzahl der Universität Wien sinkt."),
    (ETER_AT, "Deutschland hat mehr Studierende als Frankreich."),
    (ETER_TUM, "Mistelpräparate stoppen Tumor"),
    (ETER_TUM, "Globales Wachstum ist negativ"),
    (ETER_TUM, "Eigentumswohnung Preis Österreich"),
    (ETER_TUM, "Mindesthaltbarkeitsdatum bedeutet, dass Lebensmittel danach giftig sind"),
    (ETER_TUM, "Christentum ist auf dem absteigenden Ast"),
    (ETER_TUM, "Handystrahlung verursacht Gehirntumore."),
    (ETER_TUM, "Studierende in München zahlen hohe Mieten."),
    (ETER_TUM, "Die Kultur in München ist teuer."),
    (ETER_EU, "Smartmeter-Daten sind nicht personenbezogen"),
    # --- "neutral" (Stichwort-Degeneration) ---
    (NEUTRAL, "Atomstrom ist klimaneutral"),
    (NEUTRAL, "E-Fuels sind klimaneutral."),
    (NEUTRAL, "Wien will bis 2040 CO2-neutral werden."),
    (NEUTRAL, "Österreich will bis 2040 klimaneutral sein."),
    (NEUTRAL, "Die Klimaneutralität ist in der Verfassung verankert."),
    (NEUTRAL, "Die Netzneutralität ist in der EU gesetzlich verankert."),
    (NEUTRAL, "Lehrer müssen politisch neutral sein."),
]

EIGENE_THEMEN = [
    # Claim-Anfang, -Mitte, -Ende, vor Satzzeichen, mit Bindestrich
    (KI_JOBS, "KI ersetzt 47 % der Jobs"),
    (KI_JOBS, "Durch KI wird die Hälfte aller Jobs ersetzt."),
    (KI_JOBS, "Die Hälfte aller Jobs wird ersetzt durch KI."),
    (KI_JOBS, "Jobs werden ersetzt durch KI"),
    (KI_JOBS, "KI-Systeme vernichten Arbeitsplätze."),
    (KI_JOBS, "Macht (KI) Bürojobs überflüssig?"),
    (KI_JOBS, "AI ersetzt Programmierer."),
    (KI_BEWUSST, "KI hat Gefühle"),
    (KI_BEWUSST, "Hat KI Gefühle?"),
    (KI_BEWUSST, "AI ist sentient"),
    (ASPIRIN, "ASS täglich beugt Schlaganfall vor"),
    (ASPIRIN, "Tägliches ASS schützt vor Herzinfarkt."),
    (ASPIRIN, "Low-Dose-ASS zur Vorbeugung ist sinnvoll."),
    (ASPIRIN, "Ist ASS 100 gesund?"),
    (ASPIRIN, "Man sollte zur Vorbeugung täglich ASS nehmen."),
    (KHK, "ASS ist Standard bei chronischer KHK"),
    (KHK, "Nach einem Herzinfarkt ist ASS Pflicht."),
    (ETER_DE, "Wie viele Studierende gibt es in Deutschland?"),
    (ETER_DE, "Studentenzahl DE 2021"),
    (ETER_DE, "Wie viele Hochschulen gibt es in DE?"),
    (ETER_DE, "DE-Hochschulen haben 2,9 Mio. Studierende."),
    (ETER_AT, "Wie viele Studierende gibt es in Österreich?"),
    (ETER_AT, "AT-Hochschulen haben 400.000 Studenten."),
    (ETER_AT, "Wie viele Hochschulen gibt es in AT?"),
    (ETER_AT, "Studierende Quote AT"),
    (ETER_TUM, "Die TU München ist die beste Uni Deutschlands."),
    (ETER_TUM, "TUM hat 50.000 Studierende."),
    (ETER_TUM, "Wie groß ist die TUM?"),
    (ETER_TUM, "Die Technische Universität München ist die größte."),
    (ETER_EU, "ETER-Daten zeigen 2.500 Hochschulen in Europa."),
    (ETER_EU, "Laut ETER-Daten gibt es 2.500 Hochschulen."),
    (NEUTRAL, "Österreich ist neutral."),
    (NEUTRAL, "Österreich bleibt neutral."),
    (NEUTRAL, "Österreich ist seit 1955 neutral."),
    (NEUTRAL, "Die Neutralität verbietet Waffenlieferungen."),
    (NEUTRAL, "Ist die Neutralität Österreichs im Verfassungsrang?"),
    (NEUTRAL, "Österreichs Neutralität ist mit der GSVP vereinbar."),
    (NEUTRAL, "Die immerwährende Neutralität ist ein Verfassungsgesetz."),
    (NEUTRAL, "Österreich ist in keinem Militärbündnis."),
    (NEUTRAL, "Neutralität (seit 1955) verbietet Militärbasen."),
    (NEUTRAL, "Österreich ist laut Bundesverfassung ein immerwährendes neutrales Land."),
    (NEUTRAL, "Der ESSI-Beitritt verletzt eindeutig die österreichische Neutralität."),
    (NEUTRAL, "ESSI ist neutralitätskonform"),
]


@pytest.mark.parametrize("fakt,claim", LECKS)
def test_leck_geschlossen(fakt, claim):
    assert not _trifft_echt(*fakt, claim), (
        f"{fakt[1]} trifft den themenfremden Claim {claim!r}")


@pytest.mark.parametrize("fakt,claim", EIGENE_THEMEN)
def test_eigenes_thema_trifft(fakt, claim):
    assert _trifft_echt(*fakt, claim), f"{fakt[1]} verliert {claim!r}"


@pytest.mark.parametrize("fakt", ANGEFASST, ids=lambda f: f[1])
def test_dokumentierte_phrasings_treffen_exakt(fakt):
    """Muss-Treffer-Kontrolle: jede dokumentierte Phrasing der angefassten
    Fakten trifft ihren Fakt im EXAKTEN Pass (nicht nur tolerant)."""
    it = _fakt(*fakt)
    for ph in it.get("claim_phrasings_handled") or []:
        assert substring_or_composite_match(it, ph.lower()), (
            f"{fakt[1]} verliert die dokumentierte Phrasing {ph!r}")


def test_restleck_bindestrich_kompositum_bekannt():
    """Bekannter Rest: ``normalisiere`` macht aus „CO2-neutral" „co2
    neutral" — fuer den Matcher sieht das aus wie ein freistehendes
    „neutral". ALLEIN trifft es nicht mehr (die Degeneration ist weg); nur
    zusammen mit einem Rechtsbegriff der Gruppe 1. In beiden Korpora kam die
    Bindestrich-Form nicht vor (nur „klimaneutral", und das ist zu).

    Wer das schliesst, dreht diesen Test um. Das Schliessen braucht
    aufgezaehlte Adjektiv-Formen und kostet echte Neutralitaets-Claims
    („Österreich ist seit 1955 neutral") — deshalb bewusst offen."""
    assert _trifft_echt(*NEUTRAL, "Ab 2035 sind in der EU nur noch "
                                  "CO2-neutrale Neuwagen erlaubt.")
    assert not _trifft_echt(*NEUTRAL, "Wien will bis 2040 CO2-neutral werden.")


# ---------------------------------------------------------------------------
# 2. Matcher: Rand-Leerzeichen = Wortgrenze, auch am Claim-Rand
# ---------------------------------------------------------------------------

def _mw(claim: str, tok: str) -> bool:
    n = normalisiere(claim.lower())
    return trifft_mit_wortgrenze(n, wortgrenzen_fassung(n), tok)


@pytest.mark.parametrize("claim,tok,soll", [
    ("KI ersetzt Jobs", " ki ", True),              # Claim-Anfang
    ("Jobs ersetzt durch KI", " ki ", True),        # Claim-Ende
    ("Jobs ersetzt durch KI.", " ki ", True),       # vor Satzzeichen
    ("Macht (KI) Jobs überflüssig?", " ki ", True),
    ("„KI“ ersetzt Jobs", " ki ", True),
    ("KI-Systeme ersetzen Jobs", " ki ", True),     # Bindestrich
    ("Die Kinder spielen", " ki ", False),          # im Wort: nie
    ("Ski fahren ist teuer", " ki ", False),
    ("Neutralität 1955", " neutral", True),         # nur vorn gebunden
    ("Das ist klimaneutral.", " neutral", False),
    ("Studierende in Wien", " de ", False),
    ("Hochschulen in DE", " de ", True),
    # Mehrwort, vorn gebunden: die Grenze erzwingt der Flexions-Regex
    ("ETER-Daten zeigen", " eter-daten", True),
    ("Smartmeter-Daten zeigen", " eter-daten", False),
])
def test_wortgrenze_am_rand_und_vor_satzzeichen(claim, tok, soll):
    assert _mw(claim, tok) is soll


def test_ungebundene_tokens_unveraendert():
    """Ohne Rand-Leerzeichen bleibt es ein Substring — auch „ki" in
    „Kinder". Das Binden ist Sache der Daten, nicht des Matchers."""
    assert _mw("Die Kinder spielen", "ki")
    assert _mw("Das ist klimaneutral", "neutral")


def test_praefix_token_ist_nicht_gebunden():
    """Nur das ROHE Token zaehlt. „eu-" normalisiert zu „eu ", ist aber als
    Praefix gemeint („EU-Beitritt"). Als Wortende behandelt, traefe es
    zusaetzlich „neu." am Satzende."""
    assert not ist_gebunden("eu-")
    assert not _mw("Das ist neu.", "eu-")
    assert _mw("Der EU-Beitritt 1995", "eu-")
    assert ist_gebunden(" eu ") and ist_gebunden(" at") and ist_gebunden("at ")


@pytest.mark.parametrize("datei,fid,phrasing", [
    # Dokumentiert, aber auf main nie getroffen: das gebundene Token stand am
    # Claim-Rand. Allein der Matcher-Teil holt sie zurueck.
    ("ams_wifo.json", "ams_at_jugendliche_2024", "Bildung Arbeitslosigkeit AT"),
    ("ams_wifo.json", "ams_at_offene_stellen_2024", "AT-Stellenmarkt 2024"),
    ("ams_wifo.json", "ams_at_zeitreihe_2015_2024", "AT-Arbeitslosigkeit historisch"),
    ("ams_wifo.json", "ams_at_zeitreihe_2015_2024", "AT-AL-Quote 2019 vs 2024"),
    ("awmf.json", "awmf_rheumatoide_arthritis",
     "Treat-to-Target ist Standard-Strategie bei RA"),
    ("iqs_bildung.json", "nbb_2024_ganztagsschule_quote", "Wie viele Ganztagsschulen AT"),
    ("iqs_bildung.json", "nbb_2024_spf_bestand_at", "Inklusions-Anteil Bundesländer AT"),
])
def test_randtreffer_zurueck(datei, fid, phrasing):
    it = _fakt(datei, fid)
    assert phrasing in (it.get("claim_phrasings_handled") or [])
    assert substring_or_composite_match(it, phrasing.lower())


# ---------------------------------------------------------------------------
# 3. Contract: die Kurz-Tokens stehen nur noch gebunden
# ---------------------------------------------------------------------------

def _alle_tokens(it: dict):
    yield from it.get("trigger_keywords") or ()
    for grp in it.get("trigger_composite") or ():
        yield from grp
    for regel in it.get("trigger_all") or ():
        for grp in regel:
            yield from grp


VERBOTEN_BLANK = [
    (ETER_TUM, ("tum", "tu")),
    (ETER_DE, ("de ", "de", "studentenzahl de")),
    (ETER_AT, ("at ", "at")),
    (ETER_EU, ("eter-daten",)),
    (ASPIRIN, ("ass", "asa")),
    (KHK, ("ass",)),
    (KI_JOBS, ("ki", "ai")),
    (KI_BEWUSST, ("ki", "ai")),
    (NEUTRAL, ("neutral", "neutralität", "neutralitaet", "neutralitäts",
               "neutralitaets", "neutralitätsgesetz", "neutralitaetsgesetz")),
]


@pytest.mark.parametrize("fakt,verboten", VERBOTEN_BLANK,
                         ids=[f[1] for f, _ in VERBOTEN_BLANK])
def test_kurz_tokens_nur_gebunden(fakt, verboten):
    """Ein blankes Kurz-Token trifft in jedem Wort, das es enthaelt
    (TUMor, wASSer, KInder). Erlaubt ist es nur gebunden: `" tum "`."""
    tokens = set(_alle_tokens(_fakt(*fakt)))
    for tok in verboten:
        assert tok not in tokens, (
            f"{fakt[1]}: blankes Token {tok!r} — trifft in fremden Woertern")


def test_neutralitaet_keine_stichwort_degeneration():
    """Stand „neutral" in BEIDEN Composite-Gruppen, genuegte ein einziges
    Wort mit „neutral" fuer beide — die Regel war ein Stichwort, und
    „klimaneutral" erfuellte es. Jetzt darf kein Einzelwort mit „neutral"
    beide Gruppen tragen; nur die praedikative Wendung („ist neutral")."""
    it = _fakt(*NEUTRAL)
    g0, g1 = it["trigger_composite"]
    for wort in ("neutral", "klimaneutral", "co2 neutral", "neutralitaet",
                 "klimaneutralitaet", "netzneutralitaet", "neutrales"):
        assert not substring_or_composite_match(it, wort), wort
    for tok in g1:
        assert "neutral" not in normalisiere(tok) or " " in tok.strip(), (
            f"Gruppe 1 enthaelt ein Einzelwort mit neutral: {tok!r}")


# ---------------------------------------------------------------------------
# 4. Sweep ueber alle dokumentierten Phrasings — und er schlaegt nachweislich an
# ---------------------------------------------------------------------------

def _phrasings() -> list[tuple[str, str]]:
    """(Datei, Phrasing) aller Fakten MIT Trigger-Feldern."""
    out = []
    for p in sorted(glob.glob(os.path.join(DATA, "*.json"))):
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
        if not isinstance(d, dict):
            continue
        for v in d.values():
            if not isinstance(v, list):
                continue
            for it in v:
                if isinstance(it, dict) and any(it.get(t) for t in TRIGGER_FELDER):
                    for ph in it.get("claim_phrasings_handled") or []:
                        out.append((os.path.basename(p), ph))
    return out


PHRASINGS = _phrasings()


def _fremde_treffer(datei: str, it: dict) -> set[str]:
    """Dokumentierte Phrasings aus ANDEREN data-Dateien, die den Fakt treffen
    (exakt oder tolerant — konservativ fuer die Leck-Suche)."""
    return {ph for d, ph in PHRASINGS if d != datei
            and (substring_or_composite_match(it, ph.lower())
                 or tippfehler_match(it, ph.lower()))}


# Dienst-fremde Treffer, die nach dem Fix bleiben — geprueft, alle am Thema
# bis auf den letzten, der zu einer anderen Klasse gehoert.
ERLAUBT = {
    "eter_overview_at_2021": {
        "Studierende Quote AT",                        # Studierende in AT
        "Studierende Erwerbstaetigkeit Oesterreich",   # dto.
        "Tertiär-Quote Österreich",                    # tolerant: tertiaer
    },
    "awmf_nvl_khk": {
        "Statine bringen nichts gegen Herzinfarkt",    # Statine = KHK-Leitlinie
    },
    "ki_bewusstsein_2026": {
        # NICHT dieser Fix: der tolerante Pass (#205) haelt „fuehrt" fuer
        # einen Tippfehler von „fuehlt" (Abstand 1, gleicher Anlaut).
        "Kuenstliche Intelligenz fuehrt zu Massenarbeitslosigkeit",
    },
}


@pytest.mark.parametrize("fakt", ANGEFASST, ids=lambda f: f[1])
def test_sweep_keine_neuen_fremdtreffer(fakt):
    """Der Ueber-Trigger-Sweep als Gate: kommt ein themenfremder Treffer
    dazu, muss ihn jemand ansehen und hier eintragen — oder das Token
    binden."""
    it = _fakt(*fakt)
    neu = _fremde_treffer(fakt[0], it) - ERLAUBT.get(fakt[1], set())
    assert not neu, f"{fakt[1]}: neue dienst-fremde Treffer {sorted(neu)}"


def test_sweep_korpus_ist_vollstaendig():
    """Sonst misst der Sweep ins Leere — gemessen wurden 2.603 Phrasings."""
    assert len(PHRASINGS) >= 2500


@pytest.mark.parametrize("fakt,rueckbau,erwartet", [
    (ETER_TUM, [(" tum ", "tum")], "Mistelpräparate stoppen Tumor"),
    (ASPIRIN, [(" ass ", "ass")],
     "Jeder Mensch muss täglich 8 Gläser Wasser trinken"),
    (ETER_DE, [(" de ", "de ")], "Lehramts-Studierende Rückgang"),
    # Die Degeneration braucht BEIDE Seiten: blankes „neutral" in Gruppe 0
    # (sonst traegt „klimaneutral" nicht) UND in Gruppe 1.
    (NEUTRAL, [(" neutral", "neutral"), ("ist neutral", "neutral")],
     "Atomstrom ist klimaneutral"),
], ids=["tum", "ass", "de", "neutral"])
def test_sweep_schlaegt_an(fakt, rueckbau, erwartet):
    """Gift-Probe: das alte, blanke Token zurueck in eine Kopie des Fakts —
    der Sweep MUSS das bekannte Leck melden. Sonst ist er blind, und seine
    Null oben beweist nichts."""
    it = copy.deepcopy(_fakt(*fakt))
    listen = [it.get("trigger_keywords") or []] + list(it.get("trigger_composite") or [])
    for gebunden, blank in rueckbau:
        ersetzt = 0
        for lst in listen:
            for i, tok in enumerate(lst):
                if tok == gebunden:
                    lst[i] = blank
                    ersetzt += 1
        assert ersetzt, f"{gebunden!r} nicht in {fakt[1]} — Probe veraltet"
    assert erwartet in _fremde_treffer(fakt[0], it)
    assert erwartet not in _fremde_treffer(fakt[0], _fakt(*fakt))
