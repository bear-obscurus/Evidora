"""Die Quellen-Zahlen in README und Website muessen der Realitaet entsprechen.

Anlass (2026-09-05): Die Website warb mit „75+ verifizierten Live-API-Quellen",
das README nannte an drei Stellen 120+, 190+ und 191, und die deutsche
Fassung der Quellen-Seite sagte 60+, waehrend die englische 75+ sagte — auf
derselben Seite, ueber dieselbe Liste. Real sind es 191 dispatchte Quellen,
und die betreffende 2026er-Liste hat 54 Eintraege.

Fuer einen Faktencheck-Dienst ist das kein Schoenheitsfehler: Wer Zahlen
anderer Leute prueft, muss die eigenen belegen koennen. Deshalb steht die
Zahl hier nicht als Konstante, sondern wird bei jedem CI-Lauf aus dem Code
GEZAEHLT und gegen jede Stelle geprueft, die sie behauptet.

Zaehlweise (wie in README §Inventory beschrieben): ein Dispatch-Punkt ist ein
``cached("<Label>", search_fn, ...)``-Aufruf im Fan-out von ``main.py``,
begleitet von genau einem ``queried_names.append(...)``.

Dependency-light: reine Datei-Analyse, kein Netz, kein Modell, kein Import
von main.py (das laedt SpaCy).
"""

import re
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
REPO = BACKEND.parents[1]
MAIN_PY = (BACKEND / "main.py").read_text(encoding="utf-8")
README = (REPO / "README.md").read_text(encoding="utf-8")
INDEX_HTML = (REPO / "website" / "frontend" / "index.html").read_text(encoding="utf-8")
I18N = (REPO / "website" / "frontend" / "i18n.js").read_text(encoding="utf-8")

_CACHED = re.compile(r'cached\(\s*"([^"]+)"\s*,\s*(\w+)')
_QUERIED = re.compile(r"queried_names\.append\(")


def dispatch_labels() -> list[str]:
    return [m[0] for m in _CACHED.findall(MAIN_PY)]


# ---------------------------------------------------------------------------
# Die gezaehlte Wahrheit
# ---------------------------------------------------------------------------

def test_fan_out_ist_dublettenfrei():
    """Grundlage der ganzen Zaehlung: jedes Label genau einmal."""
    labels = dispatch_labels()
    doppelt = {l for l in labels if labels.count(l) > 1}
    assert not doppelt, f"doppelte Quellen-Labels im Fan-out: {sorted(doppelt)}"


def test_cached_und_queried_names_stimmen_ueberein():
    """Jeder Dispatch meldet seine Quelle auch ans source_coverage — laufen
    die auseinander, zaehlt jede Statistik etwas anderes."""
    assert len(dispatch_labels()) == len(_QUERIED.findall(MAIN_PY))


# ---------------------------------------------------------------------------
# README
# ---------------------------------------------------------------------------

def test_readme_gesamtzahl_stimmt():
    n = len(dispatch_labels())
    m = re.search(r"\|\s*Distinct dispatched sources\s*\|\s*\*\*(\d+)\*\*", README)
    assert m, "Inventory-Tabelle im README nicht gefunden"
    assert int(m.group(1)) == n, (
        f"README-Tabelle sagt {m.group(1)}, gezaehlt sind {n}"
    )


def test_readme_kopfzeilen_nennen_dieselbe_zahl():
    """Die drei Stellen, die frueher 120+/190+/191 sagten."""
    n = len(dispatch_labels())
    assert f"**{n} scientific and institutional sources**" in README
    assert f"- **{n} source connectors**" in README


def test_readme_aufteilung_summiert_sich_auf():
    """107 + 78 + 6 = 191. Eine Aufteilung, die sich nicht aufsummiert, ist
    die eigentliche Quelle solcher Drifts."""
    n = len(dispatch_labels())
    live = int(re.search(r"\|\s*Live-API connectors\s*\|\s*\*\*(\d+)\*\*", README).group(1))
    stat = int(re.search(r"\|\s*Static-first topic services\s*\|\s*\*\*(\d+)\*\*", README).group(1))
    hyb = int(re.search(r"\|\s*Hybrid \(static core \+ live refresh\)\s*\|\s*\*\*(\d+)\*\*", README).group(1))
    assert live + stat + hyb == n, (
        f"README-Aufteilung {live}+{stat}+{hyb}={live+stat+hyb} != {n} Dispatches"
    )


def test_readme_zaehlt_die_service_dateien_richtig():
    dateien = sorted(p.name for p in (BACKEND / "services").glob("*.py"))
    m = re.search(r"\*\*(\d+)\*\* of (\d+) `services/\*\.py` \((\d+) are pipeline/helper", README)
    assert m, "Zeile 'Connector service files' im README nicht gefunden"
    connectoren, gesamt, helfer = (int(g) for g in m.groups())
    assert gesamt == len(dateien), f"README sagt {gesamt} services/*.py, real {len(dateien)}"
    assert connectoren + helfer == gesamt, f"{connectoren}+{helfer} != {gesamt}"
    assert connectoren == len(dispatch_labels())


def test_readme_pack_anzahl_stimmt():
    packs = list((BACKEND / "data").glob("*_pack.json"))
    assert f"**40 curated `*_pack.json` packs**" in README
    assert len(packs) == 40, f"real {len(packs)} *_pack.json"


# ---------------------------------------------------------------------------
# Website
# ---------------------------------------------------------------------------

def test_meta_descriptions_nennen_die_gezaehlte_zahl():
    """Drei Meta-Tags (description, og, twitter) — alle drei muessen die
    Zahl tragen, die auch im Code steht."""
    n = len(dispatch_labels())
    treffer = re.findall(r"mit (\d+) wissenschaftlichen und institutionellen Quellen", INDEX_HTML)
    assert len(treffer) == 3, f"3 Meta-Descriptions erwartet, {len(treffer)} gefunden"
    assert all(int(t) == n for t in treffer), f"Meta sagt {treffer}, gezaehlt {n}"


def test_meta_live_api_zahl_passt_zum_readme():
    live = int(re.search(r"\|\s*Live-API connectors\s*\|\s*\*\*(\d+)\*\*", README).group(1))
    treffer = re.findall(r"darunter (\d+) Live-API-Anbindungen", INDEX_HTML)
    assert len(treffer) == 3
    assert all(int(t) == live for t in treffer), f"Meta sagt {treffer} Live-API, README {live}"


def _abschnitt_li_count(ueberschrift: str) -> int:
    i = I18N.index(ueberschrift)
    rest = I18N[i + len(ueberschrift):]
    ende = min((rest.index(t) for t in ("<h3>", "<h2>") if t in rest), default=len(rest))
    return rest[:ende].count("<li>")


@pytest.mark.parametrize("ueberschrift,behauptung", [
    ("<h3>Erweiterte Live-API-Quellen (2026)</h3>",
     r"(\d+) verifizierte production-ready Live-API-Connectors"),
    ("<h3>Extended Live-API Sources (2026)</h3>",
     r"<strong>(\d+) verified production-ready live-API connectors</strong>"),
])
def test_i18n_abschnitt_zahl_entspricht_der_eigenen_liste(ueberschrift, behauptung):
    """Die Zahl im Einleitungssatz muss die Liste beschreiben, die direkt
    darunter steht — DE und EN sagten hier frueher 60+ bzw. 75+ ueber
    dieselben 54 Eintraege."""
    echt = _abschnitt_li_count(ueberschrift)
    behauptet = int(re.search(behauptung, I18N).group(1))
    assert behauptet == echt, f"{ueberschrift}: behauptet {behauptet}, gelistet {echt}"


def test_deutsche_und_englische_fassung_nennen_dieselbe_zahl():
    de = int(re.search(r"(\d+) verifizierte production-ready Live-API-Connectors", I18N).group(1))
    en = int(re.search(r"<strong>(\d+) verified production-ready live-API connectors</strong>", I18N).group(1))
    assert de == en, f"DE sagt {de}, EN sagt {en}"
