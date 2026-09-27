"""Parteien und Korruption: was erhoben wird — und was nicht.

Anlass (26.9.2026). Der Claim "Die FPÖ ist die korrupteste Partei
Österreichs" bekam live `unverifiable@0.15` mit der Begründung, es lägen
"keine empirischen Daten oder Studien" vor. Das Label ist richtig, die
Begründung war zu stark: Erhebungen gibt es, sie messen nur etwas anderes
als eine Rangliste.

Recherche am 26.9.2026:

    UPTS      Der Unabhängige Parteien-Transparenz-Senat verhängt Geldbußen
              nach dem Parteiengesetz; die Bescheide 2015–2026 sind beim
              BKA und im RIS veröffentlicht und betreffen elf Parteien.
    Umfrage   Unique-research für profil, 500 Befragte, ±4,4 Pp., 29.10.2022:
              48 % halten alle Parteien für korruptionsanfällig, 35 % sagen,
              die ÖVP habe ein echtes Korruptionsproblem, 8 % halten
              Korruption für medial übertrieben.
    CPI       bewertet Staaten, nicht Parteien.

Eine vergleichende Statistik, die Parteien nach dokumentierten
Korruptionsfällen reiht, existiert nicht.

Diese Suite pinnt zweierlei: die zitierten Zahlen samt Herkunft, und die
Guardrail — der Fakt darf keine EIGENE Bewertung einer Partei enthalten.
Jede Aussage, die eine Partei mit Korruption verbindet, muss im selben Satz
ihre Quelle nennen. Dazu der Guard-Fix: "partei" generisch, damit
"Welche Partei hatte die meisten Korruptionsfälle?" nicht mehr an ihm
vorbeiläuft.

Keine Netzabfrage in diesen Tests.
"""

import json
import re
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._satzgrenzen import teile_in_einheiten  # noqa: E402
from services._topic_match import (  # noqa: E402
    politik_guard_action,
    substring_or_composite_match as trifft,
)

PACK = json.loads((BACKEND / "data" / "demokratie_pack.json").read_text(encoding="utf-8"))
F = next(x for x in PACK["facts"] if x.get("id") == "parteien_korruption_datenlage_2026")
TEXT = F["headline"] + " " + json.dumps(F["data"], ensure_ascii=False)


# --------------------------------------------------------------------------
# Die Kernaussage
# --------------------------------------------------------------------------

def test_keine_rangliste_ist_die_antwort():
    assert "keine vergleichende Statistik" in F["headline"]
    k = F["data"]["kernsatz_fuer_synthesizer"]
    assert "nicht auf eine vergleichende Erhebung" in k


def test_der_unterschied_zwischen_keine_rangliste_und_keine_daten():
    """Der Zweck des Fakts: Es gibt keine Rangliste — Daten gibt es sehr
    wohl. Beides muss dastehen."""
    notiz = " ".join(F["context_notes"])
    assert "nicht, dass es keine Daten gibt" in notiz
    assert all(w in TEXT for w in ("UPTS", "Unique-research", "Verfahren"))


# --------------------------------------------------------------------------
# UPTS — amtlich, je Partei, veröffentlicht
# --------------------------------------------------------------------------

@pytest.mark.parametrize("teil", [
    "§ 10 Abs. 8", "§§ 11 bis 12a", "PartG 2012", "BGBl. I Nr. 125/2022",
    "ris.bka.gv.at/Upts",
])
def test_upts_rechtsgrundlage(teil):
    assert teil in F["data"]["upts_sanktionen"], teil


def test_upts_liste_ist_breit_und_keine_reihung():
    p = F["data"]["upts_betroffene_parteien"]
    for partei in ("ÖVP", "SPÖ", "FPÖ", "Die Grünen", "NEOS", "KPÖ"):
        assert partei in p, partei
    assert "2015 bis 2026" in p
    # Die Abgrenzung ist der Kern: Transparenzverstoss != Korruption
    assert "nicht Korruption" in p
    assert "keine" in p and "Reihung" in p


# --------------------------------------------------------------------------
# Die Umfrage — vollständig zitiert
# --------------------------------------------------------------------------

@pytest.mark.parametrize("teil", [
    "Unique-research", "profil", "500 Personen", "±4,4", "29.10.2022",
    "48 %", "35 %", "8 %",
])
def test_umfrage_wird_vollstaendig_zitiert(teil):
    assert teil in F["data"]["umfrage_wahrnehmung_2022"], teil


def test_die_grenzen_der_umfrage_stehen_dabei():
    g = F["data"]["was_umfragen_messen"]
    assert "nicht die Zahl der Vorfälle" in g
    assert "Frageformulierung" in g
    assert "500 Befragte" in g


def test_kein_cpi_wert_in_diesem_fakt():
    """Die Länder-Zahl steht in korruption_index_2026; eine zweite Kopie
    würde driften (Lehre aus #134)."""
    assert "/100" not in TEXT
    assert not re.search(r"\bRang \d", TEXT)
    assert "bewertet Staaten, nicht Parteien" in F["data"]["cpi_ist_laenderebene"]


# --------------------------------------------------------------------------
# Guardrail: der Fakt bewertet keine Partei selbst
# --------------------------------------------------------------------------

PARTEIEN = ("fpö", "spö", "övp", "neos", "grüne", "kpö", "bzö")
KORRUPTION = ("korrupt", "korruption", "bestech")
# Wörter, an denen eine Zuschreibung als ZITAT erkennbar ist.
ZITAT_MARKER = ("unique-research", "profil", "%", "upts", "bescheid", "gaben an",
                "hielten", "laut", "befragte", "parteiengesetz", "verstöße")


def test_keine_eigene_partei_bewertung():
    """Jeder Satz, der eine Partei mit Korruption verbindet, muss im selben
    Satz seine Quelle nennen. Das ist die Projektregel 'Klassifikationen nur
    zitieren, nie selbst vornehmen' — mechanisch geprüft."""
    verdaechtig = []
    for feld, text in F["data"].items():
        for satz in teile_in_einheiten(text):
            low = satz.lower()
            if any(p in low for p in PARTEIEN) and any(k in low for k in KORRUPTION):
                if not any(m in low for m in ZITAT_MARKER):
                    verdaechtig.append((feld, satz.strip()))
    assert not verdaechtig, verdaechtig


def test_der_fakt_sagt_selbst_dass_er_nicht_bewertet():
    c = F["data"]["context"]
    assert "nimmt keine Bewertung einer Partei vor" in c
    assert "ordnet keine Partei ein" in c


def test_kein_superlativ_als_eigene_aussage():
    """'die korrupteste' darf nur als zitierte Behauptung vorkommen, nie als
    Aussage des Fakts."""
    for satz in teile_in_einheiten(TEXT):
        low = satz.lower()
        if "korrupteste" in low:
            assert "'die korrupteste'" in satz or "bezeichnet" in low, satz


# --------------------------------------------------------------------------
# Der Guard: "partei" generisch
# --------------------------------------------------------------------------

@pytest.mark.parametrize("claim", [
    "Welche Partei in Österreich hatte die meisten Korruptionsfälle?",
    "Welche Partei ist die korrupteste?",
    "Alle Parteien in Österreich sind gleich korrupt",
    "Die FPÖ ist die korrupteste Partei Österreichs",
])
def test_superlativ_ohne_anker_blockt_laenderquellen(claim):
    assert politik_guard_action(claim.lower()) == "block_country_sources", claim


@pytest.mark.parametrize("claim", [
    "Die FPÖ war in die Ibiza-Affäre verwickelt",
    "Welche Parteien wurden nach dem Parteiengesetz bestraft?",
    "Die Parteienfinanzierung in Österreich ist streng geregelt",
    "Wie viele Parteien sitzen im Nationalrat?",
    "Gibt es eine Rangliste der Parteien nach Korruption?",
])
def test_ohne_superlativ_oder_mit_anker_bleibt_pass(claim):
    assert politik_guard_action(claim.lower()) == "pass", claim


# --------------------------------------------------------------------------
# Retrieval
# --------------------------------------------------------------------------

@pytest.mark.parametrize("phrasing", F["claim_phrasings_handled"])
def test_phrasings_treffen(phrasing):
    assert trifft(F, phrasing.lower()), phrasing


def test_quellen_sind_die_geprueften():
    assert "bundeskanzleramt.gv.at" in F["source_url"]
    assert "profil.at" in F["secondary_url"]


def test_prompt_felder_bleiben_unter_der_kuerzung():
    zu_lang = {k: len(v) for k, v in F["data"].items()
               if k != "kernsatz_fuer_synthesizer" and len(v) > 400}
    assert not zu_lang, zu_lang


def test_datenstand_ist_benannt():
    notiz = " ".join(F["context_notes"])
    assert "26.9.2026" in notiz and "29.10.2022" in notiz
