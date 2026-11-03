"""Eine STRUKTURELL-FALSCH-Markierung kippte richtige Antworten fremder Claims.

Gemessen am 3.11.2026 (Memo „mess_vollspalt_2026-11-03", Messung 3). Der
Claim „Die meisten Schweinehalter in Österreich haben nur ein paar Tiere"
ist richtig — 58,5 % der Halter:innen halten 1 bis 3 Schweine. Das Prod-Log,
je Anfrage mit Zeitstempel zugeordnet:

    09:19  Modell true  0,9   Override übersprungen (Tier 1, 1/10)  -> true
    09:52  Modell true  0,9   STRUKTURELL-FALSCH-Override (1/5)     -> mostly_false
    09:52  Modell true  0,95  (Umformulierung)        Override      -> mostly_false
    11:10  Modell true  0,9   Override (1/5)                        -> mostly_false

Das Modell lag 4 von 4 Mal richtig; der Wächter hat 3 davon überschrieben.
Ob ein richtiges ``true`` durchkam, hing allein am Anteil markierter Treffer
unter allen Quellen — also daran, wie viele andere Quellen zufällig
antworteten. An diesem Tag feuerte der Override in Prod viermal, viermal gegen
ein richtiges Modell-Urteil.

## Woher die Markierung kam

Nicht vom Massentierhaltungs-Fakt — der trägt seit #302 kein Token mehr.
Sondern vom **Schwester-Fakt** ``viehbestand_at_2026``: Sein Kernsatz begann
„Die Aussage 'In Österreich gibt es mehr Rinder als Schweine' ist FALSCH.",
und `` IST FALSCH.`` steht in ``FALSE_VERDICT_OVERRIDE_TOKENS``. Diese eine
Aussage ist falsch — aber die Markierung hängt am ganzen Fakt, und den zieht
jeder österreichische Schweine-Claim (Gruppe 2 enthält „österreich").

Die Markierung wirkt an **drei** Stellen:

1. **im Prompt** — „'STRUKTURELL FALSCH'-Eintrag in den Quellen → verdict =
   'false'". Auch das ``mostly_false`` zu „kleinbäuerlich strukturiert", das
   das Modell selbst lieferte, entstand unter dieser Vorgabe;
2. **in der Quellen-Rangfolge** (Tier 2, weiter oben im Prompt);
3. **im Postprocess-Override**, der ``true``/``mostly_true`` auf
   ``mostly_false @ 0,85`` zwingt, sobald der Anteil ≥ 15 % ist.

Gemessen an den **eigenen** Aussagen des Schwester-Fakts: Die falsche
(„mehr Rinder als Schweine") beantwortete das Modell ohnehin mit ``false`` —
der Override greift dort nie. Die drei richtigen („mehr Schweine als Rinder",
„rund 2,5 Millionen Schweine", „etwa 1,8 Millionen Rinder") kamen nur durch,
weil der Anteil bei 10 bis **12,5 %** lag, knapp unter 15 %. Die Markierung
gefährdete also die richtigen Zahlen ihres eigenen Fakts.

Fix: „trifft nicht zu" statt „ist FALSCH" — kein Override-Token, die
widerlegenden Zahlen (2,53 gegen 1,82 Mio) bleiben im Kernsatz und kommen im
Prompt vollständig an (Zensus 100 %).

⚠️ **Nicht behoben, dokumentiert:** Tier 2 hätte den Override überspringen
müssen, weil die Zusammenfassung „… ist **damit** korrekt" sagte. Das Muster
``behauptung.{0,120}ist (korrekt|…)`` erkennt das Füllwort nicht (die
#278-Klasse). Eine eigene Messung wert, mit Schutz gegen „ist damit **nicht**
korrekt".

## Die 96,1 % — was im Prompt stand

In 4 von 6 frischen Begründungen stand „96,1 % Kleinhaltungen" = 100 − 3,9.
Der Prompt-Zensus zeigte warum: Bei diesem Claim kamen **15 %** des Fakts an,
bei „kleinbäuerlich strukturiert" nur **7 %** — und die 13.035 Halter:innen
mit 1 bis 3 Schweinen **gar nicht**. Die Kürzung nimmt nur Einheiten, die
einen Begriff des Claims enthalten; der Fakt schrieb „AT" statt „Österreich"
(einmal im ganzen Fakt), „Halter:innen" statt „Schweinehalter", und
„kleinbäuerlich" kam nicht vor.

Zwei Fehlversuche, beide gemessen und verworfen:
* die Zahlen in Punkt (3) des Kernsatzes — wäre ebenfalls abgeschnitten worden;
* eine längere Überschrift — sie wurde in Sätze zerlegt, und es kam WENIGER
  an (739 → 642 Zeichen), sogar die 3,9 % fielen heraus.

Getragen hat ein Satz, der die Begriffe enthält, mit denen gefragt wird, und
beide Sichten in sich hat. Zensus: 13 von 13 der Batterie, 0 Verluste; dazu
ein zurückgehaltener Satz aus fünf ungesehenen Formulierungen: 5 von 5. Weil
ich den Wortlaut an meiner eigenen Batterie geschärft habe, war dieser
zurückgehaltene Satz die eigentliche Prüfung.

⚠️ Er zeigte zugleich: 2 der 5 ungesehenen Formulierungen erreichen den Fakt
über die Trigger aus #303 gar nicht. Offen, nicht hier behoben.

Keine Netzabfrage in diesen Tests.
"""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._struct_marker import (  # noqa: E402
    FALSE_VERDICT_OVERRIDE_TOKENS,
    has_false_verdict_override,
)

PACK = BACKEND / "data" / "landwirtschaft_pack.json"

_spec = importlib.util.spec_from_file_location(
    "prompt_zensus", BACKEND / "tools" / "prompt_zensus.py")
pz = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pz)


def _fakt(kennung):
    d = json.loads(PACK.read_text(encoding="utf-8"))
    t = [f for f in d["facts"] if (f.get("id") or f.get("topic")) == kennung]
    assert len(t) == 1, kennung
    return t[0]


def _im_prompt(kennung, claim):
    return pz.ankommend(pz.felder(_fakt(kennung))["display_value"], claim,
                        pz.syn.PROMPT_MAX_DISPLAY)


# --------------------------------------------------------------------------
# Punkt 1 — keine fremde Markierung mehr
# --------------------------------------------------------------------------

def test_der_schwester_fakt_traegt_kein_override_token():
    k = _fakt("viehbestand_at_2026")["data"]["kernsatz_fuer_synthesizer"]
    assert not has_false_verdict_override(k), \
        [t for t in FALSE_VERDICT_OVERRIDE_TOKENS if t in k.upper()]


def test_kein_feld_des_schwester_fakts_traegt_ein_token():
    for feld, wert in _fakt("viehbestand_at_2026")["data"].items():
        if isinstance(wert, str):
            assert not has_false_verdict_override(wert), feld


def test_der_massentierhaltungs_fakt_traegt_kein_override_token():
    for feld, wert in _fakt("massentierhaltung_empirie_2026")["data"].items():
        if isinstance(wert, str):
            assert not has_false_verdict_override(wert), feld


def test_die_widerlegung_steht_weiter_im_kernsatz():
    """Ohne Markierung muss das Modell die Aussage aus den ZAHLEN widerlegen."""
    k = _fakt("viehbestand_at_2026")["data"]["kernsatz_fuer_synthesizer"]
    assert "trifft nicht zu" in k
    assert "2,53 Mio Schweine" in k and "1,82 Mio Rinder" in k


def test_die_widerlegenden_zahlen_kommen_im_prompt_an():
    an = _im_prompt("viehbestand_at_2026",
                    "In Österreich gibt es mehr Rinder als Schweine")
    assert "2,53" in an and "1,82" in an


def test_giftprobe_das_alte_token_wuerde_markieren():
    """Sonst prueft der erste Test nichts: die alte Fassung MUSS anschlagen."""
    alt = ("Die Aussage 'In Österreich gibt es mehr Rinder als Schweine' ist "
           "FALSCH. Statistik Austria zählte …")
    assert has_false_verdict_override(alt)


# --------------------------------------------------------------------------
# Punkt 2 — die kleinen Haltungen kommen im Prompt an
# --------------------------------------------------------------------------

#: Statistik Austria, "Halter:innen von Schweinen 2023", Oesterreich-Zeile.
HALTER, SCHWEINE = 22_285, 2_651_394
EIN_BIS_DREI_H = 5_478 + 6_468 + 1_089
BIS_100_H = EIN_BIS_DREI_H + 1_607 + 721 + 916 + 727 + 625
BIS_100_S = 5_478 + 6_468 * 2 + 1_089 * 3 + 7_479 + 6_120 + 15_470 + 27_212 + 47_276


def _pct(a, b):
    return f"{100 * a / b:.1f}".replace(".", ",")


def test_die_prozentwerte_sind_aus_der_tabelle_gerechnet():
    feld = _fakt("massentierhaltung_empirie_2026")["data"]["bestand_konzentration_schwein"]
    assert _pct(EIN_BIS_DREI_H, HALTER) + " %" in feld     # 58,5 %
    assert _pct(BIS_100_H, HALTER) + " %" in feld          # 79,1 %
    assert _pct(BIS_100_S, SCHWEINE) + " %" in feld        # 4,7 %


@pytest.mark.parametrize("claim,muss", [
    ("Die meisten Schweinehalter in Österreich haben nur ein paar Tiere",
     ["58,5", "79,1", "4,7 %"]),
    ("Österreichs Schweinehaltung ist kleinbäuerlich strukturiert",
     ["79,1", "4,7 %", "nach Tieren nicht"]),
    # der zurueckgehaltene Satz — beim Formulieren NICHT verwendet
    ("Kleinbetriebe prägen die Schweinehaltung in Österreich", ["79,1", "4,7 %"]),
    ("In Österreich gibt es tausende Höfe mit nur einem oder zwei Schweinen",
     ["79,1", "4,7 %"]),
    ("Die meisten Schweine in Österreich stammen aus Großbetrieben", ["79,1", "39"]),
])
def test_kleine_haltungen_kommen_im_prompt_an(claim, muss):
    an = _im_prompt("massentierhaltung_empirie_2026", claim)
    fehlt = [m for m in muss if m not in an]
    assert not fehlt, (claim, fehlt)


def test_beide_sichten_stehen_in_einer_einheit():
    """Kommt nur ein Teil an, soll es nicht die halbe Wahrheit sein."""
    feld = _fakt("massentierhaltung_empirie_2026")["data"]["bestand_konzentration_schwein"]
    erster_satz = feld.split(". ")[0]
    assert "nach Halter:innen" in erster_satz.lower() or "Nach Halter:innen" in erster_satz
    assert "nach Tieren nicht" in erster_satz
    assert "4,7 %" in erster_satz


def test_das_feld_bleibt_unter_400_zeichen():
    feld = _fakt("massentierhaltung_empirie_2026")["data"]["bestand_konzentration_schwein"]
    assert len(feld) <= 400, len(feld)
