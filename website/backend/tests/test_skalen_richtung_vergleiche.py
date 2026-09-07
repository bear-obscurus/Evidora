"""Die Inversions-Falle bei Vergleichs-Claims.

QA50F-Befund 1: „Die Pressefreiheit in Kamerun ist schlechter als in
Südafrika" bekam `mostly_false@0.85` — obwohl die eigene Summary die Zahlen
korrekt nannte: Kamerun 40,9/100 (Rang 133), Südafrika 78,0/100 (Rang 21).
Kamerun **ist** schlechter, die Behauptung ist wahr. **Das Verdict
widersprach seiner eigenen Begründung** — der teuerste Fehlertyp, weil die
Antwort sich selbst widerlegt.

GEMESSEN MIT SPIEGEL-PAAREN
===========================
Bei einem Paar „X schlechter als Y" / „Y schlechter als X" kann genau eine
Richtung wahr sein. Gegen die Live-Instanz:

    Kamerun schlechter als Südafrika (Presse)   WAHR   -> mostly_false@0.85  ✗
    Südafrika schlechter als Kamerun (Presse)   FALSCH -> false@0.95         ✓
    Kamerun korrupter als Österreich            WAHR   -> true@0.80          ✓
    Österreich korrupter als Kamerun            FALSCH -> false@0.80         ✓

**Beide Presse-Richtungen bekamen ein Nein.** Logisch unmöglich — und
gleichzeitig der Beweis, dass es nicht am Retrieval lag: die Zahlen waren da.

DIE URSACHE STAND IN EINEM FELD
===============================
    CPI:  "CPI Cameroon (2024): 26/100 — hoch wahrgenommene Korruption"
    RSF:  "RSF Pressefreiheit Cameroon (2026): 40.9/100 — Schwierig
           (Rang 133/180)"

CPI übersetzt die Zahl in Worte; die Richtung steht da. RSF nennt eine
Kategorie und einen **Rang** — und der Rang läuft zahlenmässig GEGEN den
Score: das schlechtere Land hat die grössere Rangzahl.

Nachgemessen über neun Index-Quellen: **acht nannten ihre Richtung nicht**,
nur `transparency` tat es — und `transparency` ist die einzige, die das
Spiegel-Paar bestand. Vier der acht melden zusätzlich einen Rang.
"""

import asyncio
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._skala import richtung  # noqa: E402

MAX_STR = 400  # synthesizer.MAX_STR

# (Modul, Claim, erwartetes Stichwort im Hinweis, meldet einen Rang?)
INDEX_QUELLEN = [
    ("rsf", "Pressefreiheit in Kamerun", "Pressefreiheit", True),
    ("freedom_house", "Freedom House Bewertung fuer Ungarn", "Rechte", False),
    ("wgi", "Rechtsstaatlichkeit in Ruanda laut Weltbank", "Governance", False),
    ("vdem", "Demokratieindex fuer Ungarn laut V-Dem", "Demokratie", False),
    ("wjp_rol", "Rechtsstaatsindex fuer Oesterreich", "Rechtsstaatlichkeit", True),
    ("bti", "BTI Transformationsindex fuer Ungarn", "Transformation", True),
    ("mipex", "MIPEX Integrationsindex Oesterreich", "integrationsfreundlich", True),
]


# Die Konstante, nicht der Live-Aufruf. Ein erster Entwurf rief `search_*`
# auf — und `wgi` ist ein LIVE-Konnektor: in CI ohne Netz kam nichts zurueck,
# die Suite war rot. Ein Test, der vom Netz abhaengt, misst das Netz.
_KONSTANTE = {
    "rsf": "_SKALA_RSF", "freedom_house": "_SKALA_FH", "wgi": "_SKALA_WGI",
    "vdem": "_SKALA_VDEM", "wjp_rol": "_SKALA_WJP", "bti": "_SKALA_BTI",
    "polity5": "_SKALA_POLITY", "mipex": "_SKALA_MIPEX",
}


def _hinweis(modul):
    """Der Skalen-Hinweis, den das Modul an seine Kopfzeile haengt."""
    import importlib
    m = importlib.import_module(f"services.{modul}")
    return getattr(m, _KONSTANTE[modul])


def _quelle(modul):
    return (BACKEND / "services" / f"{modul}.py").read_text(encoding="utf-8")


def _headline_live(modul, claim):
    """Nur fuer die Netz-Tests unten."""
    import importlib
    m = importlib.import_module(f"services.{modul}")
    f = next(getattr(m, a) for a in dir(m) if a.startswith("search_"))
    r = asyncio.run(f({"claim": claim, "original_claim": claim}))
    treffer = r.get("results") or []
    assert treffer, f"{modul}: keine Ergebnisse"
    return treffer[0].get("indicator_name") or ""


# --------------------------------------------------------------------------
# Der Helfer
# --------------------------------------------------------------------------

def test_hinweis_nennt_die_richtung_in_worten():
    """„besser" allein reicht nicht — es muss stehen, WAS ein hoher Wert
    inhaltlich bedeutet."""
    h = richtung("mehr Pressefreiheit", spanne="0-100", mit_rang=True)
    assert "HOEHERER Wert = mehr Pressefreiheit" in h
    assert "Skala 0-100" in h
    assert "NIEDRIGERE Rangzahl = besser" in h


def test_rang_hinweis_nur_wo_ein_rang_steht():
    """Ein Hinweis auf Ränge, wo keiner gemeldet wird, ist Rauschen."""
    assert "Rangzahl" not in richtung("mehr Freiheit", spanne="0-100")
    assert "Rangzahl" in richtung("mehr Freiheit", spanne="0-100", mit_rang=True)


def test_leerer_inhalt_wird_abgelehnt():
    with pytest.raises(ValueError):
        richtung("")


def test_eine_einzige_formulierung_im_ganzen_projekt():
    """Neun verschiedene Umschreibungen wären neun Gelegenheiten, es anders zu
    lesen. Der Helfer steht deshalb nur einmal."""
    import re
    kopien = [p.stem for p in (BACKEND / "services").glob("*.py")
              if re.search(r"^def richtung\(", p.read_text(encoding="utf-8"), re.M)]
    assert kopien == ["_skala"], kopien


# --------------------------------------------------------------------------
# Die acht Quellen
# --------------------------------------------------------------------------

@pytest.mark.parametrize("modul,_c,stichwort,_r", INDEX_QUELLEN)
def test_index_quelle_nennt_ihre_richtung(modul, _c, stichwort, _r):
    """Vorher nannten acht von neun Quellen ihre Richtung nicht."""
    h = _hinweis(modul)
    assert "HOEHERER Wert" in h, f"{modul}: {h}"
    assert stichwort.lower() in h.lower(), f"{modul}: {h}"


@pytest.mark.parametrize("modul,_c,_s,mit_rang", INDEX_QUELLEN)
def test_rang_warnung_wo_ein_rang_gemeldet_wird(modul, _c, _s, mit_rang):
    """Score und Rang laufen gegeneinander — genau daran ist RSF gescheitert."""
    assert ("NIEDRIGERE Rangzahl" in _hinweis(modul)) == mit_rang, modul


@pytest.mark.parametrize("modul", sorted(_KONSTANTE))
def test_hinweis_haengt_wirklich_an_der_kopfzeile(modul):
    """Eine Konstante, die niemand benutzt, hilft niemandem. Geprüft wird der
    Quelltext, nicht der Live-Aufruf — sonst hängt die Suite am Netz."""
    quelle = _quelle(modul)
    name = _KONSTANTE[modul]
    assert f"{name} = " in quelle, f"{modul}: Konstante fehlt"
    # Fenster statt Einzelzeile: in `wgi.py` steht die Konstante auf der
    # FOLGEZEILE (umbrochener Ausdruck). Eine zeilenweise Prüfung hielt das
    # für „nicht verwendet" — der Test hatte recht formal, aber unrecht.
    import re as _re
    stellen = [m.start() for m in _re.finditer(r'"indicator_name"', quelle)]
    assert stellen, f"{modul}: kein indicator_name"
    assert any(name in quelle[s:s + 220] for s in stellen), (
        f"{modul}: {name} wird an keiner indicator_name-Zuweisung angehaengt")


@pytest.mark.parametrize("modul", sorted(_KONSTANTE))
def test_hinweis_passt_ins_prompt_budget(modul):
    """Die Headline geht bis 400 Zeichen ungekürzt in den Prompt (#131). Der
    Hinweis darf davon nur einen kleinen Teil beanspruchen — sonst drängt er
    die Zahlen heraus, um die es geht."""
    assert len(_hinweis(modul)) <= 100, f"{modul}: {len(_hinweis(modul))} Zeichen"


def test_transparency_bekommt_keinen_zusatz():
    """CPI hat die Richtung schon immer in Worten gesagt und das Spiegel-Paar
    als einzige bestanden — daran wird nichts geändert."""
    quelle = _quelle("transparency")
    assert "_skala" not in quelle
    assert "wahrgenommene Korruption" in quelle


@pytest.mark.skipif("CI" in __import__("os").environ,
                    reason="geht ins Netz — in CI nicht erwuenscht")
@pytest.mark.parametrize("modul,claim,_s,_r", INDEX_QUELLEN)
def test_live_kopfzeile_traegt_den_hinweis(modul, claim, _s, _r):
    """Die Gegenprobe am echten Aufruf: die Konstante muss auch wirklich in
    der ausgelieferten Kopfzeile landen, und die bleibt unter MAX_STR."""
    kopf = _headline_live(modul, claim)
    assert "HOEHERER Wert" in kopf, f"{modul}: {kopf[:90]}"
    assert len(kopf) <= MAX_STR, f"{modul}: {len(kopf)}"


# --------------------------------------------------------------------------
# Die Prompt-Schicht
# --------------------------------------------------------------------------

def test_prompt_verlangt_die_selbstpruefung():
    """Der Kern der Regel: ein Verdict, das der eigenen Summary widerspricht,
    ist immer falsch. Ohne diesen Satz bleibt es bei „lies die Skala"."""
    quelle = (BACKEND / "services" / "synthesizer.py").read_text(encoding="utf-8")
    assert "VERGLEICHS-CLAIMS" in quelle
    assert "widerspräche deiner eigenen Begründung" in quelle
    assert "korrigiere dann das Verdict, nicht die Summary" in quelle


def test_prompt_warnt_vor_der_rang_gegenrichtung():
    quelle = (BACKEND / "services" / "synthesizer.py").read_text(encoding="utf-8")
    assert "Score und Rang laufen GEGENEINANDER" in quelle
    assert "Rang 133" in quelle, "Der konkrete Fall gehört in die Regel"
