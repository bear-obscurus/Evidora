"""Messgrößen-Bindung im Fleischkonsum-Fakt (QA50D #321).

Befund (0/6 deterministisch, live reproduziert 2026-08-17 auf prod 580fe0e):
Der Claim „Der Fleischverzehr in Österreich liegt bei fast 65 Kilo pro Kopf"
bekam `true@0.9`. Die Summary zitierte den Fakt VOLLSTÄNDIG KORREKT
(„Verzehr 60,1 kg, Verbrauch 64,8 kg") und schloss dann:

    „Die Behauptung von 'fast 65 kg' bezieht sich vermutlich auf den
     Verbrauch, der mit 64,8 kg sehr nah an der genannten Zahl liegt."

Der Claim nennt aber ausdrücklich den VERZEHR. Kein Override feuerte — das
rohe LLM-Verdict war schon falsch.

Wurzel: Der Fakt nennt bewusst BEIDE Messgrößen (Cluster-A-Design, weil der
Claim je nach Maßstab anders ausgeht) — aber nichts band die im Claim
GENANNTE Messgröße an den zu vergleichenden Wert. Das LLM wählte die Zahl
selbst und wählte die schmeichelhafte. Das ist die Verallgemeinerung der
Lehre „der LLM soll lesen, nicht rechnen" (Salzburg/Vorarlberg-Rang, PR #78)
auf die AUSWAHL der Messgröße: die Bindung muss als fertiger Satz im Fakt
stehen, nicht als zwei nebeneinander gestellte Zahlen.

Fix (Fakt-Ebene, kein neues Muster in der Override-Kaskade): fertiger
Bindungs-Satz an Headline-Position 1 + Punkt (3a) im kernsatz.

Was diese Suite pinnt — die Eigenschaften, die den Fix WIRKEN lassen:
  1. Die Headline bleibt unter dem 400-Zeichen-Prompt-Cap, sonst erreicht
     die Bindung den Synthesizer nur noch als Schnipsel (Prompt-Cap-Schatten,
     PR #25). Das ist die fragilste Eigenschaft: 389 von 400 Zeichen.
  2. Die Bindung selbst: 65/64,8 → Verbrauch, 60,1 → Verzehr, beides in
     EINEM Satz, vor der Trend-Aussage.
  3. Durch die ECHTE Prompt-Trunkierung gefahren: für einen Verzehrs-Claim
     muss 60,1 prompt-sichtbar sein, für einen Verbrauchs-Claim 64,8.
  4. Gegenrichtung: der Verbrauchs-Claim darf NICHT widerlegt werden und der
     kernsatz darf keinen STRUKTURELL-Override aktivieren (der würde
     `mostly_false` auf einen korrekten Claim erzwingen).
  5. Der ursprüngliche Zweck des Fakts (Trend-Claim „geht seit Jahren
     zurück") bleibt prompt-sichtbar — Muss-Treffer-Kontrolle.

Dependency-light: reine String-/Trigger-Logik, kein Modell, kein Netz.
"""

import json
import os

import pytest

from services._struct_marker import has_false_verdict_override
from services.landwirtschaft_pack import _claim_matches_facts, _data_lines
from services.synthesizer import _claim_centered_truncate, _prompt_claim_terms

FACT_ID = "fleischkonsum_at_trend_2026"
MAX_STR = 400  # synthesizer.MAX_STR — Feld-Trunkierung im Prompt

DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "landwirtschaft_pack.json",
)

CLAIM_VERZEHR = "Der Fleischverzehr in Österreich liegt bei fast 65 Kilo pro Kopf im Jahr."
CLAIM_VERBRAUCH = "Der Fleischverbrauch in Österreich liegt bei fast 65 Kilo pro Kopf im Jahr."
CLAIM_TREND = "Der Fleischkonsum in Österreich geht seit Jahren zurück."


@pytest.fixture(scope="module")
def fact():
    with open(DATA_PATH, encoding="utf-8") as fh:
        facts = json.load(fh)["facts"]
    matching = [f for f in facts if f.get("id") == FACT_ID]
    assert matching, f"Fakt {FACT_ID} fehlt im landwirtschaft_pack"
    return matching[0]


def _prompt_visible(fact: dict, claim: str) -> str:
    """Das, was der Synthesizer vom display_value dieses Fakts real sieht.

    Baut den display_value exakt wie ``search_landwirtschaft`` und fährt ihn
    durch die ECHTE Trunkierung aus synthesizer.py — nicht durch eine
    Nachbildung. (Fixtures ≠ echter Pfad, Lehrgeld QA50B.)
    """
    display = f"{fact.get('headline', '?')}. {_data_lines(fact.get('data') or {})}"
    terms = _prompt_claim_terms({"entities": []}, claim)
    if len(display) <= MAX_STR:
        return display
    return _claim_centered_truncate(display, terms, MAX_STR)


# --------------------------------------------------------------------------
# 1. Prompt-Cap: die Headline muss ungekürzt durchkommen
# --------------------------------------------------------------------------

def test_headline_bleibt_unter_dem_prompt_cap(fact):
    """indicator_name == headline wird ab 400 Zeichen claim-zentriert
    gekürzt — dann verliert der Bindungs-Satz seine Garantie."""
    assert len(fact["headline"]) <= MAX_STR, (
        f"Headline {len(fact['headline'])} Zeichen > {MAX_STR}: der "
        "Bindungs-Satz erreicht den Synthesizer nicht mehr garantiert."
    )


# --------------------------------------------------------------------------
# 2. Die Bindung selbst
# --------------------------------------------------------------------------

def test_headline_bindet_65_an_verbrauch(fact):
    head = fact["headline"]
    lower = head.lower()
    assert "65" in head and "verbrauch" in lower, "65-kg-Zuordnung fehlt"
    # 65 und die Verbrauchs-Zuordnung im selben Satz — sonst ist es wieder
    # nur eine Nebeneinanderstellung, aus der das LLM frei wählt.
    satz = next(s for s in head.split(". ") if "65" in s)
    assert "verbrauch" in satz.lower(), (
        f"'65' steht nicht im selben Satz wie die Verbrauchs-Zuordnung: {satz!r}"
    )


def test_headline_bindet_den_verzehr_an_seinen_vergleichswert(fact):
    head = fact["headline"]
    assert "60,1" in head, "Verzehrs-Wert fehlt in der Headline"
    satz = next(
        (s for s in head.split(". ") if "verzehr" in s.lower() and "60,1" in s),
        None,
    )
    assert satz, (
        "Kein Satz bindet den VERZEHR an 60,1 kg — ohne diese Bindung "
        f"bleibt die Auswahl der Messgröße beim LLM: {head!r}"
    )


def test_headline_verbietet_die_umdeutung_der_messgroesse(fact):
    """Iteration 2 (live erzwungen): Die bloße Nebeneinanderstellung beider
    Werte reichte NICHT. Mit ihr lieferte prod `mostly_true@0.9` und die
    Summary schrieb weiter „Die Behauptung bezieht sich vermutlich auf den
    Verbrauch". Das LLM braucht das ausdrückliche Verbot der wohlwollenden
    Umdeutung, nicht nur die Zuordnung."""
    head = fact["headline"]
    lower = head.lower()
    assert "nicht" in lower and "gelesen" in lower or "umgedeutet" in lower, (
        f"Kein Umdeutungs-Verbot in der Headline: {head!r}"
    )
    # Und die abgeleitete Bewertung als fertiger Satz — der LLM soll lesen,
    # nicht selbst entscheiden, ob 65 „nah genug" an 60,1 liegt.
    assert "unzutreffend" in lower or "ist falsch" in lower, (
        f"Kein fertiger Bewertungs-Satz für die Verzehrs-Lesart: {head!r}"
    )


def test_bindung_steht_vor_der_trendaussage(fact):
    """Headline-Position 1 (Prompt-Cap-Lehre): die verdict-entscheidende
    Information zuerst, die Trend-Einordnung danach."""
    head = fact["headline"]
    assert head.index("60,1") < head.index("rückläufig")


# --------------------------------------------------------------------------
# 3. Durch die echte Prompt-Trunkierung
# --------------------------------------------------------------------------

def test_verzehrsclaim_sieht_den_verzehrswert(fact):
    """DER Kern-Gegenbeweis: vor dem Fix schnitt die claim-zentrierte
    Trunkierung genau hinter 'Verbrauch 64,8 kg/Kopf (+0,9 kg ggü. […]' ab —
    60,1 war für einen Verzehrs-Claim nicht prompt-sichtbar."""
    visible = _prompt_visible(fact, CLAIM_VERZEHR)
    assert "60,1" in visible, (
        "Für einen Verzehrs-Claim ist der Verzehrs-Wert nicht prompt-sichtbar:\n"
        + visible
    )


def test_verbrauchsclaim_sieht_den_verbrauchswert(fact):
    visible = _prompt_visible(fact, CLAIM_VERBRAUCH)
    assert "64,8" in visible


def test_trendclaim_sieht_die_trendaussage(fact):
    """Muss-Treffer-Kontrolle: der ursprüngliche Zweck des Fakts darf durch
    die Umstellung nicht still verschwinden."""
    visible = _prompt_visible(fact, CLAIM_TREND)
    assert "rückläufig" in visible or "zurück" in visible


# --------------------------------------------------------------------------
# 4. Gegenrichtung: kein Kollateralschaden am korrekten Verbrauchs-Claim
# --------------------------------------------------------------------------

def test_kernsatz_aktiviert_keinen_struktur_override(fact):
    """Ein STRUKTURELL-FALSCH-Marker würde `mostly_false` auch auf den
    KORREKTEN Verbrauchs-Claim erzwingen — der Fakt ist differenzierend,
    nicht widerlegend (Zyrtec-Muster)."""
    assert not has_false_verdict_override(
        fact["data"].get("kernsatz_fuer_synthesizer", "")
    )


def test_headline_erklaert_65_nicht_pauschal_fuer_falsch(fact):
    """Der Fakt darf die 65 nicht generell verwerfen — sie ist als
    VERBRAUCHS-Zahl korrekt."""
    head = fact["headline"].lower()
    assert "64,8" in head
    assert "verbrauchs-zahl" in head or "verbrauch = 64,8" in head


# --------------------------------------------------------------------------
# 5. Trigger: beide Messgrößen-Claims erreichen den Fakt überhaupt
# --------------------------------------------------------------------------

@pytest.mark.parametrize("claim", [CLAIM_VERZEHR, CLAIM_VERBRAUCH, CLAIM_TREND])
def test_claim_trifft_den_fakt(claim):
    ids = [f.get("id") for f in _claim_matches_facts(claim.lower(), full_claim=claim)]
    assert FACT_ID in ids, f"Fakt feuert nicht auf {claim!r} — Treffer: {ids}"


def test_eigene_phrasings_treffen_weiter(fact):
    """Phrasings-Battery für genau diesen Fakt (einseitige Trigger-Fixes
    lassen 'Fakt komplett stumm' als Erfolg durchgehen)."""
    for phrasing in fact.get("claim_phrasings_handled", []):
        ids = [
            f.get("id")
            for f in _claim_matches_facts(phrasing.lower(), full_claim=phrasing)
        ]
        assert FACT_ID in ids, f"Phrasing {phrasing!r} trifft den Fakt nicht mehr"
