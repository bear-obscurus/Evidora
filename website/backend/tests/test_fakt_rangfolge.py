"""Welcher Fakt einer Quelle kommt zuerst — Inhalt statt Dateireihenfolge.

Anlass (2026-09-24, Massnahme C). Der Per-Source-Cap im Synthesizer nimmt
nur die ersten drei Ergebnisse, und seit Massnahme B bekommt das erste das
grosse Zeichenbudget. Welcher Fakt dort steht, entschied bei Static-Packs
faktisch die Reihenfolge in der JSON-Datei.

Gemessen am 23.9.2026 live: Claim "Die Leerstandsabgabe bringt gar nichts
gegen den Wohnungsmangel" zog `leerstand_umverteilung_2026` (Dateiposition
2) vor den zustaendigen `leerstandsabgabe_wirkung_2026` (Position 14) — und
brachte dessen quellenlose Empirica-Schaetzung "30.000 bis 50.000 Wohnungen
in Wien" in zwei Verdicts, obwohl der zustaendige Fakt ausdruecklich sagt,
dass es fuer Oesterreich keine Evaluierung gibt (#191).

Die Regel jetzt: innerhalb einer Quelle nach Claim-Abdeckung ordnen — wie
viele VERSCHIEDENE Claim-Terme im Inhalt des Ergebnisses stehen. Gleichstand
behaelt die bisherige Reihenfolge, die Auswahl des Rerankers bleibt also
erhalten. Ranking-Listen (Eurostat-Laendervergleiche) werden nicht umsortiert,
dort traegt die Reihenfolge selbst die Aussage.

Gemessen ueber die zehn Faelle unten: Dateireihenfolge zwei Fehlgriffe,
Abdeckung null.

Keine Netzabfrage, kein Modell.
"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
DATA = BACKEND / "data"
sys.path.insert(0, str(BACKEND))

from services._struct_marker import render_data_with_marker  # noqa: E402
from services._topic_match import substring_or_composite_match as trifft  # noqa: E402
from services.synthesizer import (  # noqa: E402
    claim_abdeckung,
    nach_claim_abdeckung,
    _prompt_claim_terms,
)

# (Claim, Datei, Fakt der zuerst stehen MUSS)
FAELLE = [
    ("Die Leerstandsabgabe bringt gar nichts gegen den Wohnungsmangel",
     "wohnen_pack.json", "leerstandsabgabe_wirkung_2026"),
    ("Die Leerstandsabgabe löst den Wohnungsmangel",
     "wohnen_pack.json", "leerstandsabgabe_wirkung_2026"),
    ("Wie hoch ist die Leerstandsabgabe in Tirol?",
     "wohnen_pack.json", "leerstandsabgabe_wirkung_2026"),
    ("Wie viele Frauen wurden 2024 in Österreich ermordet?",
     "gleichstellung_pack.json", "femizide_at_de_2026"),
    ("Unsere Lebensmittel sind voller Pestizide",
     "landwirtschaft_pack.json", "pestizid_rueckstaende_2026"),
    ("Die Agrarförderung in Österreich beträgt 7 Milliarden Euro",
     "landwirtschaft_pack.json", "agrar_subventionen_at_2026"),
    ("Die Landwirtschaft verursacht die meisten Treibhausgase",
     "landwirtschaft_pack.json", "klima_landwirtschaft_2026"),
    ("Deutschland hat den Familiennachzug ausgesetzt",
     "migration_pack.json", "migration_familiennachzug_2026"),
    ("Die Zufriedenheit mit der Demokratie in Österreich ist im freien Fall",
     "demokratie_pack.json", "at_demokratie_zufriedenheit_2026"),
    ("In Wien bekommen 140.000 Haushalte Wohnbeihilfe",
     "sozialstaat_pack.json", "wohnbeihilfe_at_2026"),
]


def _facts(obj):
    if isinstance(obj, dict):
        if "id" in obj and ("claim_phrasings_handled" in obj or "trigger_composite" in obj):
            yield obj
        for v in obj.values():
            yield from _facts(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _facts(v)


def _ergebnisse(datei: str, claim: str) -> list[dict]:
    """Die Treffer eines Pakets, als Result-Dicts wie im Pack-Service."""
    d = json.loads((DATA / datei).read_text(encoding="utf-8"))
    aus = []
    for f in _facts(d):
        if not trifft(f, claim.lower()):
            continue
        data = f.get("data") or {}
        aus.append({
            "indicator_name": f.get("headline", "?"),
            "display_value": f"{f.get('headline', '?')}. {render_data_with_marker(data)}",
            "description": (data.get("context", "") + " "
                            + " | ".join(f.get("context_notes") or [])).strip(),
            "_fakt_id": f["id"],
        })
    return aus


@pytest.mark.parametrize("claim,datei,erwartet", FAELLE,
                         ids=[f"{d.split('_')[0]}:{c[:38]}" for c, d, _ in FAELLE])
def test_zustaendiger_fakt_steht_vorne(claim, datei, erwartet):
    res = _ergebnisse(datei, claim)
    assert res, (datei, claim)
    sortiert = nach_claim_abdeckung(res, _prompt_claim_terms({}, claim))
    assert sortiert[0]["_fakt_id"] == erwartet, [r["_fakt_id"] for r in sortiert]


def test_dateireihenfolge_wuerde_danebengreifen():
    """Gegenprobe: ohne die Sortierung stimmt es in mindestens einem Fall
    nicht — sonst misst die Batterie den Unterschied gar nicht."""
    daneben = [c for c, datei, erwartet in FAELLE
               if (r := _ergebnisse(datei, c)) and r[0]["_fakt_id"] != erwartet]
    assert daneben, "Faelle ergaenzen: die Batterie trennt die Varianten nicht mehr"


def test_der_empirica_fall_von_2026_09_23():
    """Der konkrete Fall, der zwei Verdicts vergiftet hat."""
    claim = "Die Leerstandsabgabe bringt gar nichts gegen den Wohnungsmangel"
    res = _ergebnisse("wohnen_pack.json", claim)
    ids = [r["_fakt_id"] for r in res]
    assert "leerstand_umverteilung_2026" in ids and "leerstandsabgabe_wirkung_2026" in ids
    assert ids[0] == "leerstand_umverteilung_2026", "Dateireihenfolge unveraendert erwartet"
    sortiert = nach_claim_abdeckung(res, _prompt_claim_terms({}, claim))
    assert sortiert[0]["_fakt_id"] == "leerstandsabgabe_wirkung_2026"


# --------------------------------------------------------------------------
# Die Sortierung selbst
# --------------------------------------------------------------------------

def test_abdeckung_zaehlt_verschiedene_terme_nicht_haeufigkeit():
    viel = {"display_value": "Tirol Tirol Tirol Tirol"}
    breit = {"display_value": "Tirol Leerstandsabgabe"}
    terms = ["tirol", "leerstandsabgabe"]
    assert claim_abdeckung(viel, terms) == 1
    assert claim_abdeckung(breit, terms) == 2


def test_abdeckung_ist_schreibweisen_normalisiert():
    r = {"display_value": "In Österreich lagen die Ausgaben bei 265 Mio."}
    assert claim_abdeckung(r, ["oesterreich"]) == 1


def test_gleichstand_behaelt_die_reihenfolge():
    """Stabil: Was der Reranker vorne hatte, bleibt bei Gleichstand vorne."""
    a = {"display_value": "Tirol", "_fakt_id": "a"}
    b = {"display_value": "Tirol", "_fakt_id": "b"}
    aus = nach_claim_abdeckung([a, b], ["tirol"])
    assert [r["_fakt_id"] for r in aus] == ["a", "b"]


def test_ohne_terme_bleibt_alles_wie_es_war():
    a = {"display_value": "x", "_fakt_id": "a"}
    b = {"display_value": "y", "_fakt_id": "b"}
    assert nach_claim_abdeckung([a, b], []) == [a, b]


def test_einzelergebnis_bleibt_unveraendert():
    a = [{"display_value": "x"}]
    assert nach_claim_abdeckung(a, ["x"]) is a
