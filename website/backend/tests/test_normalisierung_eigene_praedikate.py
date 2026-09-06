"""Schreibweisen-Normalisierung für die Services mit EIGENEM Prädikat.

#143 zog die Normalisierung in den gemeinsamen Matcher `_topic_match.py`,
#144 und #145 zogen vier Services mit eigener Trigger-Logik nach. Übrig
blieben 63 weitere — sie tauchten in der ursprünglichen Messung nie auf, weil
ihnen kuratierte `claim_phrasings_handled` fehlen.

Eigene Sonde über ihre Trigger-Begriffe (ein Begriff, der allein auslöst, muss
das auch in anderer Schreibweise tun):

    Umlaut → ae/oe/ue    49 %   →   100 %
    Bindestrich          55 %   →   100 %

gemessen über 52 umgebaute Services, 155 Umlaut- und 285 Bindestrich-Begriffe.

**Der Weg dorthin ist die eigentliche Lehre.** Ein erster, blind mechanischer
Durchlauf über alle Dateien hat 47 Services STILL KAPUTT gemacht: die
Trigger-Konstanten waren normalisiert, der Claim nicht — und die Testsuite
meldete nur `wifo_ihs`, weil das als einziger dieser Services eine eigene
Abdeckung hat. 1.371 grüne Tests bei 47 defekten Services.

Der zweite Anlauf lief deshalb mit einem **Regressions-Gate pro Datei**: Vorher
wurde für jeden Service festgehalten, welche seiner Trigger-Begriffe auslösen;
nach dem Umbau musste jeder davon weiterhin auslösen, sonst wurde genau diese
Datei zurückgesetzt. Das Gate hat 7 Dateien gefangen und zurückgerollt — eine
davon (`doab`) wegen einer eigenen Fehlannahme: dort heisst die Claim-Variable
`key`, und ein pauschales Verbot auf Variablennamen mit „key" blockierte genau
die Zeile, die normalisiert werden musste.

Elf Services blieben bewusst aussen vor: neun ohne flache Trigger-Konstanten
oder ohne erkennbaren Claim-Eintritt (dort wäre der Umbau asymmetrisch
geworden), zwei weitere hat das Gate erneut zurückgerollt.
"""

import ast
import importlib
import re
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._schreibweise import norm_terme, normalisiere  # noqa: E402

UMLAUT = str.maketrans({"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"})
HAT_UMLAUT = re.compile(r"[äöüß]")


def _dienste():
    """Services mit eigenem Prädikat, die den Helfer nutzen."""
    for pfad in sorted((BACKEND / "services").glob("*.py")):
        if pfad.stem.startswith("_"):
            continue
        q = pfad.read_text(encoding="utf-8")
        if "norm_terme(" not in q or "claim_mentions_" not in q:
            continue
        try:
            mod = importlib.import_module(f"services.{pfad.stem}")
        except Exception:
            continue
        praed = next((getattr(mod, a) for a in dir(mod)
                      if a.startswith("claim_mentions_") and a.endswith("_cached")), None)
        if praed:
            yield pfad.stem, mod, praed


def _begriffe(modname):
    """Trigger-Literale aus dem QUELLTEXT, nicht aus der Laufzeit.

    Zur Laufzeit sind die Konstanten bereits normalisiert — dort steht
    „oesterreich", nicht mehr „Österreich". Die Umlaut-Variante liesse sich
    daran gar nicht mehr messen. Die Literale im Quelltext tragen die
    ursprüngliche Schreibweise.
    """
    baum = ast.parse((BACKEND / "services" / f"{modname}.py").read_text(encoding="utf-8"))
    raus = set()
    for knoten in ast.walk(baum):
        if not (isinstance(knoten, ast.Call) and isinstance(knoten.func, ast.Name)
                and knoten.func.id in ("norm_terme", "_norm_terme")):
            continue
        for arg in knoten.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                x = arg.value.strip()
                if (4 <= len(x) <= 45 and not x.startswith(("http", "\\", "^"))
                        and "(" not in x):
                    raus.add(x)
    return raus


# --------------------------------------------------------------------------
# Der gemeinsame Helfer
# --------------------------------------------------------------------------

def test_norm_terme_normalisiert_strings():
    assert norm_terme("Österreich", "BIP-Anteil") == ("oesterreich", "bip anteil")


def test_norm_terme_laesst_nicht_strings_durch():
    """Gemischte Tupel — etwa Paare aus Stichwort und Gewicht — müssen heil
    bleiben, sonst bricht der Umbau still an einer Stelle."""
    assert norm_terme(("a", 1), 42, None) == (("a", 1), 42, None)


def test_norm_terme_erhaelt_wortgrenzen_schutz():
    assert norm_terme(" eter ") == (" eter ",)


def test_helfer_steht_nur_einmal_im_projekt():
    """52 Kopien desselben Helfers waren der erste Entwurf — eine Definition
    reicht, sonst driften sie."""
    kopien = [p.stem for p in (BACKEND / "services").glob("*.py")
              if re.search(r"^def _?norm_terme\(", p.read_text(encoding="utf-8"), re.M)]
    assert kopien == ["_schreibweise"], kopien


# --------------------------------------------------------------------------
# Die Messung als Vertrag
# --------------------------------------------------------------------------

def _quote(wandler):
    ok = fehl = 0
    for _name, mod, praed in _dienste():
        for b in _begriffe(_name):
            try:
                if not praed(b):
                    continue
            except Exception:
                continue
            var = wandler(b)
            if var == b:
                continue
            try:
                if praed(var):
                    ok += 1
                else:
                    fehl += 1
            except Exception:
                fehl += 1
    return ok, fehl


def test_umlaut_umschrift_trifft():
    """Vor dem Umbau: 49 %."""
    ok, fehl = _quote(lambda b: b.translate(UMLAUT))
    assert ok + fehl >= 100, f"zu wenig Messpunkte ({ok + fehl})"
    quote = ok / (ok + fehl)
    assert quote >= 0.95, f"nur {quote:.0%} ({fehl} Ausfälle von {ok + fehl})"


def test_bindestrich_variante_trifft():
    """Vor dem Umbau: 55 %."""
    ok, fehl = _quote(lambda b: b.replace("-", " "))
    assert ok + fehl >= 200, f"zu wenig Messpunkte ({ok + fehl})"
    quote = ok / (ok + fehl)
    assert quote >= 0.95, f"nur {quote:.0%} ({fehl} Ausfälle von {ok + fehl})"


def test_genug_services_sind_umgebaut():
    """Ein Rückbau einzelner Dateien soll auffallen."""
    assert len(list(_dienste())) >= 50


# --------------------------------------------------------------------------
# Keine Regression: was vorher traf, muss weiter treffen
# --------------------------------------------------------------------------

def test_kein_service_hat_treffer_verloren():
    """Der Kern des Regressions-Gates, als dauerhafter Test. Genau diese
    Prüfung hat beim ersten Anlauf 47 still kaputte Services aufgedeckt, die
    die reguläre Suite nicht bemerkt hat."""
    verloren = {}
    for name, mod, praed in _dienste():
        fehlt = []
        for b in _begriffe(name):
            # Der Begriff selbst muss seinen eigenen Trigger ausloesen —
            # in Umlaut- UND ASCII-Schreibweise.
            try:
                if praed(b) and not praed(normalisiere(b)):
                    fehlt.append(b)
            except Exception as e:
                fehlt.append(f"<{type(e).__name__}>")
        if fehlt:
            verloren[name] = fehlt[:3]
    assert not verloren, f"Services mit verlorenen Treffern: {verloren}"


def test_symmetrie_konstanten_und_claim():
    """Die Falle des ersten Anlaufs: Konstanten normalisiert, Claim nicht.
    Wer `norm_terme` auf eine Konstante anwendet, muss auch den Claim
    normalisieren — sonst trifft ein Umlaut-Trigger keinen ASCII-Claim mehr
    UND keinen Umlaut-Claim."""
    asymmetrisch = []
    for pfad in sorted((BACKEND / "services").glob("*.py")):
        if pfad.stem.startswith("_"):
            continue
        q = pfad.read_text(encoding="utf-8")
        if not re.search(r"^_?[A-Z][A-Z0-9_]* = _?norm_terme\(", q, re.M):
            continue
        if not re.search(r"normalisiere\(\s*(claim|analysis|f\"|\(claim)", q):
            asymmetrisch.append(pfad.stem)
    assert not asymmetrisch, f"Konstanten normalisiert, Claim nicht: {asymmetrisch}"


# --------------------------------------------------------------------------
# Stichproben mit echten Claims
# --------------------------------------------------------------------------

def test_konkrete_claims_treffen_in_beiden_schreibweisen():
    from services.frontex import claim_mentions_frontex_cached
    from services.uba_klima import claim_mentions_uba_cached
    from services.wifo_ihs import claim_mentions_wifo_ihs_cached
    for praed, paar in (
        (claim_mentions_frontex_cached,
         ("Irreguläre Grenzübertritte an den EU-Außengrenzen",
          "Irregulaere Grenzuebertritte an den EU-Aussengrenzen")),
        (claim_mentions_wifo_ihs_cached,
         ("WIFO-Prognose für Österreich", "WIFO-Prognose fuer Oesterreich")),
        (claim_mentions_uba_cached,
         ("CO2-Emissionen Deutschland UBA", "CO2 Emissionen Deutschland UBA")),
    ):
        for c in paar:
            assert praed(c), c
