"""Zwei Fakten dürfen nicht dieselbe Kennung tragen.

Anlass (29.9.2026, Fund aus dem #228-Sweep). ``gender_pay_gap_2026`` lag
zweimal in den Daten — und zwar nicht als Kopie, sondern als zwei
VERSCHIEDENE Fakten:

    arbeitsmarkt_pack.json   Gender Pay Gap — Eurostat unadjusted vs.
                             adjusted, AT 18,4 %
                             (Heinze/Wolf 2010, Olivetti/Petrongolo)
    gleichstellung_pack.json Strukturelle vs. individuelle Erklärung des
                             Gender Pay Gap
                             (Eurostat 2023, DESTATIS, ILO, OECD)

Anderer Scope, andere Quellen, andere Trigger, dieselbe ID.

## Warum das zählt

Der Code liest die Kennung als ``id or topic``. Überall, wo daraus ein
Dict gebaut wird — ``{_fid(it): it for …}`` — überschreibt der zweite
Eintrag den ersten still. Kein Fehler, keine Warnung, ein verschwundener
Fakt.

Gemessen hat das bereits einmal geschadet: Ein Instrument, das die ID einer
Datei zuordnete (``wo.setdefault(id, datei)``), prüfte für diesen Fakt die
falsche Datei und meldete **vier Phantom-Verluste** — 58 statt der echten
54. Die Differenz fiel nur auf, weil ein anderes Gate mehr fand als die
frische Messung.

## Der Fix

Die Arbeitsmarkt-Kopie heißt jetzt ``gender_pay_gap_bereinigt_2026`` —
benannt nach dem, was sie von der anderen unterscheidet. Die ID geht NICHT
in die API-Antwort (die Services reichen ``topic``, ``headline``,
``display_value`` und ``source`` weiter, nicht die ID), darum ist die
Umbenennung rein intern und ändert am Verhalten nichts.

## Was bewusst NICHT geändert wurde

Beide Fakten tragen weiterhin dasselbe ``topic`` (``gender_pay_gap_konsens``).
Das ist harmlos, solange beide eine eigene ``id`` haben — ``_fid`` bevorzugt
sie. Anders als die ID landet ``topic`` aber IM Ergebnis und damit im
Prompt; es umzubenennen wäre eine Verhaltensänderung und bräuchte ihre
eigene Messung. Der zweite Fall derselben Art (``vpn_anonymitaet_mythos`` in
cybersecurity_pack und tech_ki_pack) kollidiert aus demselben Grund nicht:
Der eine Eintrag hat eine ID, der andere fällt auf sein topic zurück.

Von 665 Fakt-Einträgen haben 122 keine ``id`` und leben von ihrem
``topic`` — auch für die gilt das Gate.

Keine Netzabfrage, kein Modell.
"""

import collections
import copy
import glob
import json
import os
import sys

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, BACKEND)

TF = ("trigger_keywords", "trigger_composite", "trigger_all")


def _fid(it):
    """Dieselbe Kennung, die services/_topic_match.py benutzt."""
    return it.get("id") or it.get("topic") or ""


def _laden():
    raus = []
    for p in sorted(glob.glob(os.path.join(DATA, "*.json"))):
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if not isinstance(v, list):
                continue
            for it in v:
                if isinstance(it, dict) and any(it.get(t) for t in TF):
                    raus.append((f"{os.path.basename(p)}:{k}", it))
    return raus


ALLE = _laden()


def _kollisionen(alle):
    nach_fid = collections.defaultdict(list)
    for ort, it in alle:
        nach_fid[_fid(it)].append(ort)
    return {f: orte for f, orte in nach_fid.items() if len(orte) > 1}


# ---------------------------------------------------------------------------
# Das Gate
# ---------------------------------------------------------------------------

def test_jede_kennung_gehoert_genau_einem_fakt():
    assert not _kollisionen(ALLE), _kollisionen(ALLE)


def test_gate_schlaegt_an():
    """Gift-Probe: einen Fakt unter fremder Kennung dazulegen. Ohne sie
    beweist die Null oben nur, dass gerade niemand kollidiert."""
    ort, it = ALLE[0]
    zwilling = copy.deepcopy(ALLE[1][1])
    zwilling["id"] = _fid(it)
    zwilling.pop("topic", None)
    assert _kollisionen(ALLE + [("erfunden.json:facts", zwilling)])


def test_alle_fakten_sind_erfasst():
    """Sonst prueft das Gate eine Teilmenge und meldet trotzdem gruen."""
    assert len(ALLE) >= 660, len(ALLE)


def test_kennung_faellt_bei_fehlender_id_auf_topic_zurueck():
    """122 Eintraege haben keine 'id'. Ein Gate, das nur 'id' prueft, sieht
    sie nicht — und genau dort waere eine Kollision am unauffaelligsten."""
    ohne_id = [it for _o, it in ALLE if not it.get("id")]
    assert len(ohne_id) >= 100, len(ohne_id)
    assert all(_fid(it) for it in ohne_id), "Eintrag ohne id UND ohne topic"


# ---------------------------------------------------------------------------
# Der aufgeloeste Fall
# ---------------------------------------------------------------------------

def test_die_beiden_gender_pay_gap_fakten_sind_getrennt():
    kennungen = {_fid(it) for _o, it in ALLE
                 if "gender_pay_gap" in _fid(it)}
    assert kennungen == {"gender_pay_gap_2026", "gender_pay_gap_bereinigt_2026"}, kennungen


def test_sie_sind_wirklich_zwei_verschiedene_fakten():
    """Waeren sie inhaltsgleich, waere das Umbenennen der falsche Fix
    gewesen — dann haette einer geloescht gehoert."""
    paar = {_fid(it): it for _o, it in ALLE if "gender_pay_gap" in _fid(it)}
    a, b = paar["gender_pay_gap_2026"], paar["gender_pay_gap_bereinigt_2026"]
    assert a.get("headline") != b.get("headline")
    assert a.get("source_url") != b.get("source_url")
    assert a.get("trigger_composite") != b.get("trigger_composite")


def test_die_id_steht_nicht_in_der_antwort():
    """Begruendung dafuer, dass die Umbenennung rein intern ist: Die
    Pack-Services reichen topic/headline/display_value/source weiter, nicht
    die ID. Wer das aendert, muss die Umbenennung neu bewerten."""
    for dienst in ("arbeitsmarkt_pack.py", "gleichstellung_pack.py"):
        quelle = open(os.path.join(BACKEND, "services", dienst),
                      encoding="utf-8").read()
        block = quelle[quelle.index("results.append({"):]
        block = block[:block.index("})")]
        assert '"id"' not in block, f"{dienst} reicht die ID jetzt durch"
