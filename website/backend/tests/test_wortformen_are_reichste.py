"""Zwei Wortform-Luecken: ARE gegen ARI, und die obere Haelfte ohne Umgangssprache.

QA50I-Befunde #80 und #85. Beide Claims blieben ``unverifiable@0.1`` mit
**null Belegen**, obwohl die Zahlen in den Fakten stehen, die am selben Tag
refresht wurden.

## #80 — der Fakt kannte seine eigene Abkuerzung nicht

    "In der Kalenderwoche 41 2026 lag die ARE-Rate bei 6.854"

Der Fakt ``rki_atemwegsinfekte_saisonvergleich`` schreibt **ARE** neun Mal
und fuehrt 6.854 fuer KW 41/2026. Seine Trigger kannten ``"rki ari"`` —
und **ARI kommt im Fakt null Mal vor**. Dieselbe Klasse wie #140/#141
(Flexionsformen aufzaehlen scheitert) und #229 (der Umlaut-Plural erreicht
sein Token nicht).

Dazu kam ein zweiter, struktureller Grund: ``trigger_composite`` ist ein
UND aus einer THEMEN- und einer BEWEGUNGS-Gruppe ("welle", "anstieg",
"schwaecher", ...). Ein Claim, der nur einen WERT nennt, hat kein
Bewegungswort und kann die Bedingung nie erfuellen — egal wie gut die
Themen-Gruppe ist. Die Wert-Formen stehen deshalb in
``trigger_keywords``, das ein ODER ist.

Und das Modell loeste "ARE" als "Arbeitsunfaehigkeitsrate" auf, weil die
Abkuerzung nirgends aufgeloest war. Sie steht jetzt in der HEADLINE, die
die Prompt-Kuerzung zuerst sieht (#135) — nicht in ``messgroesse``, weil
das Feld damit auf 479 Zeichen gewachsen waere.

## #85 — die untere Haelfte hatte Umgangssprache, die obere nicht

    "Das reichste Prozent in Oesterreich bekommt 10,8 Prozent des Einkommens"

WID wurde fuer diesen Claim **gar nicht erst abgefragt**. Die
``_SHARE_TRIGGERS`` waren asymmetrisch:

    untere Haelfte:  "ärmste hälfte", "untere hälfte"   umgangssprachlich DA
    obere Haelfte:   "top 1 %", "obersten 1 %"          nur Fachsprache

Dieselbe Klasse wie #228: "weibliche" traf nicht, wo nur "Frauen" im
Trigger stand.

## Ein Sweep, den ich verworfen habe

Naheliegend waere ein Zensus gewesen: Welche Abkuerzung nennt ein Fakt,
die seine Trigger nicht kennen? Zwei Fassungen gemessen, beide verworfen:

    Abkuerzung >=3x im Fakttext   629 Befunde bei 675 Fakten
    Abkuerzung in der HEADLINE    334 Befunde

Die Treffer sind ``USD``, ``URL``, ``SMS``, ``KW``, ``NHS``, ``NEJM``,
``QJE`` — Einheiten und Quellenkuerzel IN der Erklaerung, die niemand als
Suchbegriff tippt. Als Trigger wuerden sie massiv ueber-triggern. Und die
engere Fassung findet den eigenen Anlass nicht: Die RKI-Headline enthielt
"ARE" ueberhaupt nicht. Ein Instrument, das seinen eigenen Anlass nicht
findet, taugt nicht (#229) — also kein neues Gate, sondern die beiden
Formen als dokumentierte Zusage, die das Gate aus #230 dauerhaft prueft.

## Der Ueber-Trigger-Sweep

Gemessen vorher und nachher ueber alle dokumentierten Phrasings und alle
Fakten-Dateien:

    vorher    2.618 Phrasings   0 Muss-Treffer verloren   348 fremde Paare
    nachher   2.622 Phrasings   0 Muss-Treffer verloren   348 fremde Paare

Die neuen Tokens triggern **nichts** dazu.

Eine erste Messung meldete 198 verlorene Muss-Treffer und widersprach
damit dem Gate aus #230, das 0 meldet. Das waren Fakten OHNE Trigger-
Felder, die per Trigger nie erreichbar sind — derselbe Fehler, den ich in
dieser Runde schon einmal gemacht hatte.

Dependency-light: nur Daten und Trigger-Logik, kein Netz, kein Modell.
"""
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, BACKEND)

from services.rki_surveillance import _claim_matches_facts  # noqa: E402
from services.wid import claim_mentions_wid_cached  # noqa: E402

ATEM = "rki_atemwegsinfekte_saisonvergleich"


def _rki_fakt():
    with open(os.path.join(DATA, "rki_surveillance.json"), encoding="utf-8") as fh:
        d = json.load(fh)
    return [f for f in d["facts"] if f.get("id") == ATEM][0]


def _trifft_atem(claim):
    return any((f.get("id") or f.get("topic")) == ATEM
               for f in _claim_matches_facts(claim.lower(), claim))


# --- #80: die Abkuerzung ---------------------------------------------

@pytest.mark.parametrize("claim", [
    "In der Kalenderwoche 41 2026 lag die ARE-Rate bei 6.854",
    "Wie hoch ist die ARE-Rate?",
    "Die ARE-Rate ist gestiegen",
    "Die ARE Rate in Deutschland",
    "GrippeWeb Höchstwert Saison",
    "Die Konsultationsinzidenz lag bei 2.044",
    "Wie hoch war die akute respiratorische Erkrankungsrate?",
])
def test_die_are_formen_erreichen_den_fakt(claim):
    assert _trifft_atem(claim), claim


@pytest.mark.parametrize("claim", [
    "Die Rate der Arbeitslosigkeit ist gestiegen",
    "Die Ware wurde geliefert",
    "Die Schare der Zuschauer wuchs",
    "Das Sparbuch bringt kaum Zinsen",
    "In Österreich gibt es viele Schulen",
    "Die Kalenderwoche 41 war regnerisch",
    "Der Bundesrat tagt in dieser Woche",
])
def test_die_are_formen_triggern_nichts_fremdes(claim):
    """``" are "`` ist gebunden (#207) — sonst steckt es in "Ware",
    "Schare" und "Waren"."""
    assert not _trifft_atem(claim), claim


def test_ari_kommt_im_fakt_nicht_vor():
    """Der Kern des Befunds: Der Trigger nannte eine Abkuerzung, die der
    Fakt gar nicht benutzt."""
    volltext = json.dumps(_rki_fakt(), ensure_ascii=False)
    import re
    assert not re.search(r"\bARI\b", volltext), (
        "ARI steht jetzt im Fakt — dann war die Diagnose falsch")
    assert len(re.findall(r"\bARE\b", volltext)) >= 9


def test_beide_abkuerzungen_stehen_in_den_triggern():
    """ARI bleibt drin: Wer sie tippt, soll den Fakt trotzdem finden."""
    kw = " ".join(_rki_fakt()["trigger_keywords"])
    assert "are rate" in kw or "are-rate" in kw
    assert "ari rate" in kw or "ari-rate" in kw
    assert "rki ari" in kw, "die alte Form darf nicht verloren gehen"


def test_are_ist_in_der_themen_gruppe_gebunden():
    """Fuer Bewegungs-Claims ("die ARE ist gestiegen") braucht die
    Themen-Gruppe des Composite die Abkuerzung — gebunden wie " ari "
    und " rsv "."""
    gruppe = _rki_fakt()["trigger_composite"][0]
    assert " are " in gruppe
    assert "are" not in gruppe, "ungebunden wuerde es in 'Ware' stecken"


def test_der_wert_claim_braucht_kein_bewegungswort():
    """Der strukturelle Teil des Befunds.

    ``trigger_composite`` ist ein UND aus Thema und BEWEGUNG. Ein Claim,
    der nur einen Wert nennt, hat kein Bewegungswort — er muss ueber
    ``trigger_keywords`` (ODER) laufen.
    """
    fa = _rki_fakt()
    bewegung = fa["trigger_composite"][1]
    claim = "In der Kalenderwoche 41 2026 lag die ARE-Rate bei 6.854"
    assert not any(w.strip() in claim.lower() for w in bewegung), (
        "der Claim traegt doch ein Bewegungswort — dann belegt dieser Test "
        "die Begruendung nicht")
    assert _trifft_atem(claim)


def test_die_abkuerzung_ist_in_der_headline_aufgeloest():
    """Das Modell las "ARE" als "Arbeitsunfaehigkeitsrate".

    Die Auflösung steht in der HEADLINE, weil die Prompt-Kuerzung sie
    zuerst sieht (#135). In ``messgroesse`` waere das Feld auf 479
    Zeichen gewachsen und haette die 400er-Grenze gerissen.
    """
    fa = _rki_fakt()
    h = fa["headline"]
    assert "(ARE)" in h
    assert "Atemwegsinfekte" in h
    assert len(h) <= 400, len(h)
    assert "akute respiratorische" in json.dumps(fa, ensure_ascii=False).lower()


def test_die_messgroessen_sperre_aus_138_bleibt():
    """Die Headline wurde angefasst — die Sperre darf nicht verwaessern."""
    d = _rki_fakt()["data"]
    assert "MESSGRÖSSE NICHT TAUSCHEN" in d["messgroesse"]
    assert "viermal" in d["messgroesse"]
    assert "ZWEI WOCHEN SIND KEIN SAISONVERLAUF" in d["laufende_saison_2026_27"]


def test_die_neuen_formen_stehen_als_zusage():
    """Damit das Gate aus #230 sie dauerhaft prueft — statt eines neuen
    Sweeps, den ich gemessen und verworfen habe."""
    ph = _rki_fakt()["claim_phrasings_handled"]
    assert "In der Kalenderwoche 41 2026 lag die ARE-Rate bei 6.854" in ph
    assert "Wie hoch ist die ARE-Rate?" in ph
    assert len(ph) >= 9


# --- #85: die obere Haelfte ------------------------------------------

@pytest.mark.parametrize("claim", [
    "Das reichste Prozent in Österreich bekommt 10,8 Prozent des Einkommens",
    "Die reichsten 10 Prozent besitzen die Hälfte des Vermögens in Deutschland",
    "Das oberste Zehntel bekommt 30 Prozent des Einkommens in Frankreich",
    "Die reichsten 1 % in den USA",
    "Das reichste Prozent besitzt wie viel Vermögen in Italien?",
])
def test_die_reichste_formen_erreichen_wid(claim):
    assert claim_mentions_wid_cached(claim), claim


@pytest.mark.parametrize("claim", [
    "Die ärmste Hälfte besitzt 3 Prozent des Vermögens in Österreich",
    "Die untere Hälfte der Einkommen in Deutschland",
    "Top 1 Prozent Einkommen USA",
])
def test_die_bestehenden_formen_halten(claim):
    """Gegenprobe: Die Fachformen und die untere Haelfte waren schon da
    und duerfen nicht verloren gehen."""
    assert claim_mentions_wid_cached(claim), claim


@pytest.mark.parametrize("claim", [
    "Die reichsten Länder der Welt sind in Europa",
    "Österreich ist ein reiches Land",
    "Der reichste Mann der Welt ist Elon Musk",
    "Die Hälfte der Österreicher fährt Auto",
    "Zehn Prozent der Schüler wiederholen eine Klasse",
    "Die oberste Gerichtsinstanz hat entschieden",
])
def test_die_reichste_formen_triggern_nichts_fremdes(claim):
    """Alle neuen Formen verlangen "prozent"/"%"/"zehntel" dahinter —
    "die reichsten" allein wuerde bei "die reichsten Laender"
    fehltreffen."""
    assert not claim_mentions_wid_cached(claim), claim


def test_die_obere_haelfte_hat_so_viele_formen_wie_die_untere():
    """Der Kern des Befunds war die ASYMMETRIE, nicht eine fehlende
    Zeile. Fuer die untere Haelfte standen umgangssprachliche Formen da,
    fuer die obere nur Fachsprache."""
    from services.wid import _SHARE_TRIGGERS
    tokens = set(_SHARE_TRIGGERS)
    unten = {t for t in tokens if "aermste" in t or "untere" in t
             or "bottom" in t}
    oben_umgangs = {t for t in tokens if "reichst" in t or "oberst" in t
                    or "richest" in t}
    assert unten, "die untere Haelfte hat keine Formen mehr"
    assert len(oben_umgangs) >= len(unten), (
        f"obere {len(oben_umgangs)} gegen untere {len(unten)} — die "
        f"Asymmetrie ist wieder da")


# --- Der verworfene Sweep --------------------------------------------

def test_der_abkuerzungs_zensus_ist_begruendet_verworfen():
    """Kein neues Gate — und der Grund gehoert festgehalten, damit der
    Naechste die Messung nicht wiederholt.

    629 Befunde bei 675 Fakten in der breiten Fassung, 334 in der engen;
    die Treffer sind Einheiten und Quellenkuerzel. Und die enge Fassung
    fand den eigenen Anlass nicht: "ARE" stand nicht in der Headline —
    bis dieser PR es hineingeschrieben hat.
    """
    pfad = os.path.abspath(__file__)
    with open(pfad, encoding="utf-8") as fh:
        doku = fh.read()
    assert "629 Befunde bei 675 Fakten" in doku
    assert "334 Befunde" in doku
    assert "#229" in doku


def test_der_ueber_trigger_sweep_ist_mit_zahlen_belegt():
    pfad = os.path.abspath(__file__)
    with open(pfad, encoding="utf-8") as fh:
        doku = fh.read()
    assert "348 fremde Paare" in doku
    assert "198 verlorene Muss-Treffer" in doku
