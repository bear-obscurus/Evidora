"""Jede modellfreie Testdatei steht in der CI-Liste.

Der Job ``light-unit-tests`` in ``.github/workflows/test.yml`` ruft pytest
mit einer ausdrücklichen Dateiliste auf. Was dort fehlt, läuft in der CI
nie (Regel in CLAUDE.md). Bis 20.11.2026 prüfte das niemand: Die Liste
wurde von Hand gepflegt, und ``test_unit.py`` (71 Tests: Cache, Eurostat,
PISA, Ratelimit, Verdict-Cache, Postprocess, Embedding-Cache) stand nie
darin, obwohl die Datei in der CI-Umgebung grün und ohne Netz läuft.

## Woran eine Testdatei als modellfrei gilt

Die CI installiert nur ``pytest pytest-asyncio pytest-timeout httpx numpy``.
Eine Testdatei gilt als **nicht** modellfrei, wenn ihr Quelltext (per AST,
auch funktionslokale Importe) eines davon tut:

1. Sie importiert eine Modellbibliothek: ``spacy``, ``sentence_transformers``,
   ``torch``, ``transformers``, ``huggingface_hub``.
2. Sie startet die App: ``import main`` / ``from main import …`` oder
   ``fastapi.testclient``. ``main`` lädt beim Import alle Dienste.
3. Sie geht ins Netz: Marker ``integration`` oder ``network``, ein Import
   von ``requests``, ``aiohttp``, ``urllib.request`` oder ``socket``, oder
   ein Aufruf von ``httpx.Client(…)`` / ``httpx.AsyncClient(…)`` ohne
   ``transport=`` (mit ``transport=httpx.MockTransport`` bleibt er lokal).

Alles andere gilt als modellfrei und muss in der CI-Liste stehen.

**Bewusst kein Kriterium** ist der Import von ``services.ner``,
``services._st_model`` oder ``services._reranker_backup``. Diese Module laden
SpaCy und sentence-transformers erst beim ersten Aufruf, nicht beim Import.
Drei Dateien, die heute in der CI grün laufen, importieren sie und ersetzen
das Modell durch eine Attrappe (``test_ner_einmal_laden``,
``test_st_model_revision``, ``test_unit``). Fehlt die Bibliothek, liefern die
Loader ``None``. Dass in der CI wirklich kein Modell geladen wird, garantiert
die CI selbst, weil dort keine Modellbibliothek installiert ist.

Was das Kriterium nicht erkennt (etwa ein Dienst, der ohne eigenen Client
eine Live-API ruft), kommt auf die Ausschlussliste ``AUSSCHLUSS``, mit
Begründung. Sie soll klein bleiben. Heute stehen dort ``test_sources.py``
(Live-APIs) und ``test_integration.py`` (laufendes Backend mit LLM).

Modellfrei: nur Standardbibliothek, liest zwei Dateien, kein Netz.
"""

import ast
import re
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
TESTS = BACKEND / "tests"
WORKFLOW = BACKEND.parents[1] / ".github" / "workflows" / "test.yml"
JOB = "light-unit-tests"

MODELL_BIBLIOTHEKEN = {"spacy", "sentence_transformers", "torch",
                       "transformers", "huggingface_hub"}
APP_MODULE = {"main", "fastapi.testclient"}
NETZ_MODULE = {"requests", "aiohttp", "urllib.request", "socket"}
NETZ_MARKER = {"integration", "network"}

# Dateien, die bewusst nicht in der CI laufen, mit Begründung. Nötig ist ein
# Eintrag nur, wenn das Kriterium die Datei für modellfrei hält
# (test_sources); test_integration erkennt es selbst und steht zur
# Klarheit trotzdem hier.
AUSSCHLUSS = {
    "test_integration.py": (
        "braucht ein laufendes Backend mit LLM (POST /api/check gegen "
        "EVIDORA_TEST_URL), Marker integration"),
    "test_sources.py": (
        "ruft die echten Dienste (Eurostat, EEA, EZB, Copernicus, UNHCR, "
        "PubMed, GADMO, ECDC, ClaimReview) gegen ihre Live-APIs; der "
        "Docstring sagt 'requires network'"),
}


# ---------------------------------------------------------------------------
# Kriterium
# ---------------------------------------------------------------------------

def _wurzel_treffer(name, menge):
    """'torch.nn' trifft 'torch', 'urllib.request.x' trifft 'urllib.request'."""
    return any(name == m or name.startswith(m + ".") for m in menge)


def gruende_gegen_modellfrei(quelle: str) -> list:
    """Liste der Gründe, warum der Quelltext nicht modellfrei ist (leer = frei)."""
    baum = ast.parse(quelle)
    gruende = []
    for knoten in ast.walk(baum):
        module = []
        if isinstance(knoten, ast.Import):
            module = [a.name for a in knoten.names]
        elif isinstance(knoten, ast.ImportFrom) and knoten.level == 0 and knoten.module:
            module = [knoten.module]
            module += [f"{knoten.module}.{a.name}" for a in knoten.names]
        for m in module:
            if _wurzel_treffer(m, MODELL_BIBLIOTHEKEN):
                gruende.append(f"importiert Modellbibliothek {m}")
            elif _wurzel_treffer(m, APP_MODULE):
                gruende.append(f"startet die App ({m})")
            elif _wurzel_treffer(m, NETZ_MODULE):
                gruende.append(f"Netzmodul {m}")

        # pytest.mark.integration / pytest.mark.network, als Dekorator oder pytestmark
        if (isinstance(knoten, ast.Attribute) and knoten.attr in NETZ_MARKER
                and isinstance(knoten.value, ast.Attribute)
                and knoten.value.attr == "mark"):
            gruende.append(f"Marker {knoten.attr}")

        # httpx.Client(...) / httpx.AsyncClient(...) ohne transport=
        if (isinstance(knoten, ast.Call)
                and isinstance(knoten.func, ast.Attribute)
                and knoten.func.attr in {"Client", "AsyncClient"}
                and isinstance(knoten.func.value, ast.Name)
                and knoten.func.value.id == "httpx"
                and not any(k.arg == "transport" for k in knoten.keywords)):
            gruende.append(f"echter httpx.{knoten.func.attr} ohne transport=")
    return sorted(set(gruende))


def ci_liste(text: str) -> list:
    """Testdateien, die der Job light-unit-tests an pytest übergibt."""
    jobs = re.split(r"(?m)^  (?=[A-Za-z0-9_-]+:\s*$)", text)
    job = [j for j in jobs if j.startswith(JOB + ":")]
    assert len(job) == 1, f"Job {JOB} nicht genau einmal in test.yml gefunden"
    return re.findall(r"(?m)^\s+tests/(test_[A-Za-z0-9_]+\.py)\s*\\?\s*$", job[0])


def _modellfreiheit_aller_dateien():
    return {p.name: gruende_gegen_modellfrei(p.read_text(encoding="utf-8"))
            for p in sorted(TESTS.glob("test_*.py"))}


# ---------------------------------------------------------------------------
# Die eigentliche Prüfung
# ---------------------------------------------------------------------------

def test_jede_modellfreie_datei_steht_in_der_ci_liste():
    liste = set(ci_liste(WORKFLOW.read_text(encoding="utf-8")))
    fehlend = [name for name, gruende in _modellfreiheit_aller_dateien().items()
               if not gruende and name not in liste and name not in AUSSCHLUSS]
    assert not fehlend, (
        "Modellfrei, aber nicht im CI-Job light-unit-tests "
        "(.github/workflows/test.yml) — läuft in der CI nie: "
        f"{fehlend}. Eintragen oder mit Begründung in AUSSCHLUSS.")


def test_ci_liste_nennt_nur_vorhandene_modellfreie_dateien():
    dateien = _modellfreiheit_aller_dateien()
    probleme = []
    for name in ci_liste(WORKFLOW.read_text(encoding="utf-8")):
        if name not in dateien:
            probleme.append(f"{name}: Datei gibt es nicht")
        elif dateien[name]:
            probleme.append(f"{name}: {', '.join(dateien[name])}")
    assert not probleme, probleme


def test_ci_liste_ohne_doppelte_eintraege():
    liste = ci_liste(WORKFLOW.read_text(encoding="utf-8"))
    doppelt = sorted({n for n in liste if liste.count(n) > 1})
    assert not doppelt, doppelt


def test_ausschlussliste_ist_aktuell():
    dateien = _modellfreiheit_aller_dateien()
    liste = set(ci_liste(WORKFLOW.read_text(encoding="utf-8")))
    for name, grund in AUSSCHLUSS.items():
        assert name in dateien, f"{name} steht in AUSSCHLUSS, existiert aber nicht"
        assert name not in liste, f"{name} steht in AUSSCHLUSS und in der CI-Liste"
        assert len(grund) >= 20, f"{name}: Begründung fehlt oder ist zu kurz"


def test_diese_datei_steht_selbst_in_der_ci_liste():
    assert Path(__file__).name in ci_liste(WORKFLOW.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Das Kriterium selbst
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("quelle, erwartet", [
    ("import spacy", "importiert Modellbibliothek spacy"),
    ("def f():\n    from sentence_transformers import util",
     "importiert Modellbibliothek sentence_transformers"),
    ("import torch.nn", "importiert Modellbibliothek torch.nn"),
    ("import main", "startet die App (main)"),
    ("from main import app", "startet die App (main)"),
    ("from fastapi.testclient import TestClient",
     "startet die App (fastapi.testclient)"),
    ("from fastapi import testclient", "startet die App (fastapi.testclient)"),
    ("import requests", "Netzmodul requests"),
    ("from urllib.request import urlopen", "Netzmodul urllib.request"),
    ("import pytest\npytestmark = pytest.mark.integration", "Marker integration"),
    ("import pytest\n@pytest.mark.network\ndef test_x(): pass", "Marker network"),
    ("import httpx\nasync def f():\n    async with httpx.AsyncClient(timeout=5) as c:\n        pass",
     "echter httpx.AsyncClient ohne transport="),
])
def test_kriterium_erkennt_nicht_modellfreie_dateien(quelle, erwartet):
    assert erwartet in gruende_gegen_modellfrei(quelle)


@pytest.mark.parametrize("quelle", [
    "from services import ner",                   # lädt SpaCy erst beim Aufruf
    "from services import _st_model, _reranker_backup",
    "from services.mains import x",               # nur Präfix von 'main'
    "import urllib.parse",                        # kein Netz
    "import httpx\nc = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: None))",
    "assert 'httpx.AsyncClient(' not in block",   # nur ein String
    "import pytest\npytestmark = pytest.mark.asyncio",
    "from . import spacy",                        # relativer Import, kein Paket
])
def test_kriterium_laesst_modellfreie_dateien_durch(quelle):
    assert gruende_gegen_modellfrei(quelle) == []


def test_ci_liste_liest_nur_den_job_und_keine_kommentare():
    yml = (
        "jobs:\n"
        "  light-unit-tests:\n"
        "    # tests/test_kommentar.py steht nur im Kommentar\n"
        "    steps:\n"
        "      - run: |\n"
        "          python -m pytest \\\n"
        "            tests/test_a.py \\\n"
        "            tests/test_b.py\n"
        "\n"
        "  check-new-urls:\n"
        "    steps:\n"
        "      - run: pytest \\\n"
        "            tests/test_anderer_job.py\n"
    )
    assert ci_liste(yml) == ["test_a.py", "test_b.py"]
