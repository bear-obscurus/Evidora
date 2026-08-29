"""SPARQL-Templates liefern keine Zeilen-Dubletten (2026-08-29).

Befund (bei der Meloni-Drift #122 als Verstaerker aufgefallen): WDQS lieferte
dieselbe Zeile mehrfach. Ursachen im Query-Bau:

  * ``VALUES ?nameLabel {{ "X"@de "X"@mul "X"@en }}`` — traegt die Entitaet
    das Label in mehreren Sprachen (bei Eigennamen der Normalfall), joint
    jede Sprachvariante eine eigene Zeile. Faktor bis 3.
  * ``OPTIONAL {{ ?person wdt:P102 ?party. }}`` — mehrere Parteien
    multiplizieren zusaetzlich.

Live gemessen (politiker_amtszeit, LIMIT 20): **20 Zeilen fuer 7 distinkte
Aemter**. In Produktion steht dort ``LIMIT 5`` — die Dubletten fressen also
das Limit auf.

Das ist kein Kosmetikfall, sondern hat zwei harte Folgen:

  1. **Override-Verstaerker.** Bei „Meloni ist Ministerpraesidentin" waren
     3 der 5 Zeilen dasselbe beendete Tourismus-Ministerium. Ratio 3/9 =
     33 % riss den Tier-1-Guard (15 %) und kippte ein korrektes `true@0.9`
     auf `mostly_false@0.85`. Mit EINER Zeile waeren es ~11 % gewesen — der
     Override haette geschwiegen.
  2. **Das gefragte Amt faellt aus dem Limit.** Live gemessen fuer Orbán:
     vorher 5 Zeilen, ALLE fuenf Duplikate von „Mitglied der ungarischen
     Nationalversammlung" — das Ministerpraesidenten-Amt, nach dem der
     Claim fragt, war gar nicht dabei. Nach dem Fix ist es die erste Zeile.
     Das ist die Sunak-Falle aus dem Row-Cap-Kommentar, nur eine Ebene
     tiefer: nicht der Cap war zu klein, die Zeilen waren redundant.

Fix: ``SELECT DISTINCT`` in allen Templates, die das Mehrsprachen-VALUES
nutzen und nicht ohnehin per GROUP BY aggregieren. Live gemessen war
DISTINCT dabei sogar SCHNELLER (0,66 s statt 1,19 s).

Dieser Test pinnt die Eigenschaft als Contract — neue Templates muessen
mitziehen. Der Live-Teil (echte WDQS-Zeilen) ist nicht CI-faehig und in
PR #123 dokumentiert.
"""

import re

import pytest

from services.wikidata import _TEMPLATES

MEHRSPRACHIG = [t for t in _TEMPLATES if "@mul" in t["sparql"]]


def _hat_distinct(sparql: str) -> bool:
    return bool(re.search(r"SELECT\s+DISTINCT", sparql, re.IGNORECASE))


def _aggregiert(sparql: str) -> bool:
    return "GROUP BY" in sparql.upper()


def test_es_gibt_ueberhaupt_mehrsprachige_templates():
    """Vorbedingung — sonst prueft der Contract-Test unten nichts.
    (Die @de+@mul+@en-Umstellung kam mit PR #87, mul-Label-Migration.)"""
    assert len(MEHRSPRACHIG) >= 8


@pytest.mark.parametrize("tpl", MEHRSPRACHIG, ids=lambda t: t["name"])
def test_mehrsprachiges_values_erzwingt_dedup(tpl):
    """Wer per VALUES ueber drei Sprach-Labels joint, MUSS deduplizieren —
    sonst zaehlt das LIMIT Dubletten statt Fakten."""
    assert _hat_distinct(tpl["sparql"]) or _aggregiert(tpl["sparql"]), (
        f"{tpl['name']}: VALUES ueber @de/@mul/@en ohne SELECT DISTINCT "
        f"und ohne GROUP BY — Zeilen vervielfachen sich."
    )


def test_politiker_amtszeit_hat_distinct():
    """Der Fall, an dem der Defekt gemessen wurde."""
    tpl = next(t for t in _TEMPLATES if t["name"] == "politiker_amtszeit")
    assert _hat_distinct(tpl["sparql"])


def test_land_bevoelkerung_behaelt_distinct():
    """Regressions-Pin fuer PR #102: dort kam DISTINCT zusammen mit dem
    Staedte-Typfilter dazu und darf nicht wieder verschwinden."""
    tpl = next(t for t in _TEMPLATES if t["name"] == "land_bevoelkerung")
    assert _hat_distinct(tpl["sparql"])


def test_person_lebensdaten_aggregiert_statt_distinct():
    """Gegenprobe: dieses Template dedupliziert per GROUP BY. Der Contract
    akzeptiert beide Wege — der Test haelt fest, dass das Absicht ist."""
    tpl = next(t for t in _TEMPLATES if t["name"] == "person_lebensdaten")
    assert _aggregiert(tpl["sparql"])


@pytest.mark.parametrize("tpl", _TEMPLATES, ids=lambda t: t["name"])
def test_kein_template_hat_distinct_und_group_by(tpl):
    """Beides zusammen ist ein Zeichen fuer Copy-Paste, nicht fuer Absicht —
    und DISTINCT ist bei vorhandenem GROUP BY wirkungslos."""
    assert not (_hat_distinct(tpl["sparql"]) and _aggregiert(tpl["sparql"])), \
        f"{tpl['name']}: DISTINCT neben GROUP BY"
