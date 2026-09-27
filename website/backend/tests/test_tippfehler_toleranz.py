"""Tippfehler-toleranter Trigger-Match (QA50F-Befund, 2026-09-26).

Anlass: "Wie hoc ist die Leertsandsabgabe in Tirol?" bekam live
``unverifiable@0.15`` nach 5,2 s — die Pipeline lief nie, weil kein Trigger
traf. Fuer **49 der 63** Trigger-Services ist das ohne Netz: Sie haben den
Cosine-Backup seit #41 abgeschaltet (``descriptor_fn=None``), weil er bei
Multi-Topic-Packs themenfremde Claims zog.

Gemessen ueber alle 2.603 dokumentierten ``claim_phrasings_handled`` von
Fakten MIT Trigger-Feldern (97,7 % treffen ihren Fakt), je ein Tippfehler:

    ein Zeichen weggelassen    43,4 % der Treffer verloren
    zwei Zeichen vertauscht    42,4 % verloren
    ein Zeichen doppelt        33,7 % verloren

Der tolerante Pass holt davon **45,8 %** zurueck (1.391 von 3.037) und fuegt
dabei **9** neue dienst-fremde Treffer auf ~1,4 Mio. gepruefte Paare hinzu.

Diese Suite pinnt die Baender, nicht die Prozentzahlen: Mindestlaenge 6,
gleiche ersten 3 Zeichen, nur Einwort-Tokens, und — der Fehler der ersten
Fassung — **exakt ODER tolerant je Token**, damit kurze Pflicht-Tokens wie
"tirol" (5 Zeichen) im toleranten Pass nicht verloren gehen.

Keine Netzabfrage, kein Modell.
"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._tippfehler import (  # noqa: E402
    MIND_LAENGE,
    PRAEFIX_LAENGE,
    abstand_hoechstens_eins,
    tippfehler_match,
    token_ist_schreibnah,
)
from services._topic_match import (  # noqa: E402
    find_matching_items,
    substring_or_composite_match,
)

WOHNEN = BACKEND / "data" / "wohnen_pack.json"
LEERSTAND = next(
    f for f in json.loads(WOHNEN.read_text(encoding="utf-8"))["facts"]
    if f["id"] == "leerstandsabgabe_wirkung_2026"
)


# --------------------------------------------------------------------------
# Der Fall aus der QA50F
# --------------------------------------------------------------------------

def test_der_qa50f_tippfehler_trifft_jetzt():
    claim = "Wie hoc ist die Leertsandsabgabe in Tirol?"
    assert not substring_or_composite_match(LEERSTAND, claim.lower())
    assert tippfehler_match(LEERSTAND, claim.lower())


def test_kurze_pflicht_tokens_gehen_im_toleranten_pass_nicht_verloren():
    """Der Fehler der ersten Fassung: Wer im toleranten Pass ALLE Vergleiche
    tolerant macht, verliert "tirol" (5 Zeichen < Mindestlaenge) — und damit
    das Composite, das Thema UND Bundesland verlangt."""
    assert len("tirol") < MIND_LAENGE
    assert not token_ist_schreibnah("tirol", ["tirol"])       # zu kurz
    # trotzdem trifft das Composite, weil "tirol" exakt im Claim steht
    assert tippfehler_match(LEERSTAND, "wie hoc ist die leertsandsabgabe in tirol?")


def test_sauberer_claim_trifft_im_toleranten_pass_auch():
    assert tippfehler_match(LEERSTAND, "wie hoch ist die leerstandsabgabe in tirol?")


@pytest.mark.parametrize("claim", [
    "Wie viele Menschen leben in Wien?",
    "Impfungen verursachen Autismus",
    "Der Leitzins der EZB ist gestiegen",
])
def test_themenfremde_claims_treffen_nicht(claim):
    assert not tippfehler_match(LEERSTAND, claim.lower())


# --------------------------------------------------------------------------
# Der Abstand
# --------------------------------------------------------------------------

@pytest.mark.parametrize("a,b", [
    ("leerstandsabgabe", "leerstandsabgabe"),   # gleich
    ("leerstandsabgabe", "leertsandsabgabe"),   # vertauscht
    ("deutschland", "deutschlan"),              # weggelassen
    ("deutschland", "deutschlandd"),            # doppelt
    ("friedensvertrag", "friedensvertrog"),     # ersetzt
])
def test_abstand_eins(a, b):
    assert abstand_hoechstens_eins(a, b)
    assert abstand_hoechstens_eins(b, a), "muss symmetrisch sein"


@pytest.mark.parametrize("a,b", [
    ("leerstandsabgabe", "leertsnadsabgabe"),   # zwei Fehler
    ("deutschland", "deutschl"),                # drei weg
    ("wahlen", "zahlenwerk"),
    ("abgabe", "ausgabe"),                      # zwei Unterschiede
])
def test_abstand_groesser_eins(a, b):
    assert not abstand_hoechstens_eins(a, b)


# --------------------------------------------------------------------------
# Die drei Baender
# --------------------------------------------------------------------------

@pytest.mark.parametrize("tok,wort", [
    ("wahlen", "zahlen"),          # unterscheiden sich im Anlaut
    ("bericht", "gericht"),
    ("wohnung", "sohnung"),
])
def test_praefix_band_verhindert_die_gefaehrlichen_paare(tok, wort):
    """Die ersten drei Zeichen muessen stimmen — genau dort unterscheiden
    sich die Paare, die ein anderes echtes Wort sind."""
    assert tok[:PRAEFIX_LAENGE] != wort[:PRAEFIX_LAENGE]
    assert not token_ist_schreibnah(tok, [wort])


@pytest.mark.parametrize("tok", ["miete", "euro", "wien", "tirol"])
def test_laengenband_schliesst_kurze_tokens_aus(tok):
    assert len(tok) < MIND_LAENGE
    assert not token_ist_schreibnah(tok, [tok + "n", tok])


def test_mehrwort_tokens_bleiben_exakt():
    """Mehrwort-Trigger traegt services/_flexion.py — hier kein Spielraum."""
    assert not token_ist_schreibnah("freie wahlen", ["freie", "wahlen"])
    assert not token_ist_schreibnah("e-auto-akku", ["eautoakku"])


def test_toleranz_nur_bei_gleichem_anfang_aber_echtem_tippfehler():
    assert token_ist_schreibnah("leerstandsabgabe", ["leertsandsabgabe"])
    assert not token_ist_schreibnah("leerstandsabgabe", ["wohnbeihilfe"])


# --------------------------------------------------------------------------
# Reihenfolge und Provenance in find_matching_items
# --------------------------------------------------------------------------

def _wohnen(claim):
    return find_matching_items(
        str(WOHNEN), "facts", claim_lc=claim.lower(), full_claim=claim,
        descriptor_fn=None,          # wie im echten wohnen_pack (#41)
    )


def test_exakter_treffer_gewinnt_und_ist_als_exakt_markiert():
    treffer = _wohnen("Wie hoch ist die Leerstandsabgabe in Tirol?")
    assert treffer
    assert all(t["data"]["_matched_exact"] is True for t in treffer)


def test_toleranter_treffer_ist_als_schwach_markiert():
    """Ein schreibweisen-naher Treffer darf kein 'strukturell falsch'
    behaupten — deshalb exact=False, wie beim Cosine-Backup."""
    treffer = _wohnen("Wie hoc ist die Leertsandsabgabe in Tirol?")
    assert treffer, "der tolerante Pass muss hier greifen"
    assert all(t["data"]["_matched_exact"] is False for t in treffer)


def test_ohne_langes_wort_kein_toleranter_pass():
    """Kurzer Claim ohne ein Wort von 6 Zeichen: nichts zu tolerieren."""
    assert _wohnen("Wie viel?") == []


# --------------------------------------------------------------------------
# Ueber-Trigger: die dokumentierten Phrasings bleiben bei ihrem Fakt
# --------------------------------------------------------------------------

def test_alle_phrasings_des_packs_treffen_weiter_exakt():
    """Muss-Treffer-Kontrolle: Der neue Pass darf den exakten nicht
    verdraengen — er laeuft nur, wenn der exakte leer bleibt."""
    for f in json.loads(WOHNEN.read_text(encoding="utf-8"))["facts"]:
        for p in f.get("claim_phrasings_handled") or []:
            treffer = _wohnen(p)
            assert treffer, (f["id"], p)
            assert any(t["data"]["_matched_exact"] for t in treffer), (f["id"], p)
