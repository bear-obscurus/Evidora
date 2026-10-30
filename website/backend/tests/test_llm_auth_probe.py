"""LLM-Auth-Probe: Tri-State + Cache (QA50D HOCH 2).

Am 28./29.07.2026 war Evidora ~12 h vollständig ausgefallen — Mistral
lieferte 401, jeder Claim brach im Analyze-Schritt ab — und der
Container meldete durchgehend "healthy", weil der Healthcheck
`/api/legal` prüfte (statisch, ohne LLM). Diese Probe schliesst die
Lücke; die Tests pinnen die zwei Eigenschaften, an denen sie scheitern
könnte:

  1. TRI-STATE — nur 401/403 sind ein Fehler. Timeout/Netzfehler/429/
     5xx liefern None ("unbekannt") und dürfen den Container NICHT
     unhealthy flappen lassen.
  2. CACHE — das Ergebnis (auch das negative!) hält TTL-lang, sonst
     fragt der 30-s-Healthcheck die API alle 30 s an.

Dependency-light: httpx wird gemockt, kein Netz.
"""
import asyncio
import time

import pytest

from services import ollama


class _Resp:
    def __init__(self, status_code, rumpf=None):
        self.status_code = status_code
        self._rumpf = rumpf

    def json(self):
        """Ein Rumpf, den niemand gesetzt hat, ist unlesbar — genau wie
        vor dem 30.10.2026, als die Sonde den Katalog gar nicht las."""
        if self._rumpf is None:
            raise ValueError("kein JSON")
        return self._rumpf


class _FakeClient:
    """Minimaler httpx.AsyncClient-Ersatz."""
    calls = 0

    def __init__(self, *a, **kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, url, headers=None):
        type(self).calls += 1
        if isinstance(self.behaviour, Exception):
            raise self.behaviour
        return _Resp(self.behaviour, getattr(self, "rumpf", None))


def _install(monkeypatch, behaviour, key="k" * 32, rumpf=None):
    _FakeClient.calls = 0
    cls = type("C", (_FakeClient,), {"behaviour": behaviour,
                                     "rumpf": rumpf})
    monkeypatch.setattr(ollama.httpx, "AsyncClient", cls)
    monkeypatch.setattr(ollama, "MISTRAL_API_KEY", key)
    # Cache je Test leeren
    ollama._llm_auth_cache.update({"ts": 0.0, "ok": None, "detail": ""})
    return cls


def _run(coro):
    return asyncio.run(coro)


# --- Tri-State ---

def test_200_ist_ok(monkeypatch):
    _install(monkeypatch, 200)
    r = _run(ollama.check_llm_auth())
    assert r["ok"] is True, r


@pytest.mark.parametrize("code", [401, 403])
def test_auth_fehler_ist_hart_falsch(monkeypatch, code):
    """Die Ausfallklasse vom 29.07. — muss den Container markieren."""
    _install(monkeypatch, code)
    r = _run(ollama.check_llm_auth())
    assert r["ok"] is False, r
    assert "abgelehnt" in r["detail"]


@pytest.mark.parametrize("behaviour", [
    429, 500, 502, 503,
    TimeoutError("timeout"),
    ConnectionError("connection reset"),
])
def test_transiente_fehler_sind_unbekannt_nicht_falsch(monkeypatch, behaviour):
    """Kein Flapping: ein Upstream-Schluckauf darf NICHT unhealthy machen."""
    _install(monkeypatch, behaviour)
    r = _run(ollama.check_llm_auth())
    assert r["ok"] is None, r


def test_fehlender_key_ist_hart_falsch(monkeypatch):
    _install(monkeypatch, 200, key="")
    r = _run(ollama.check_llm_auth())
    assert r["ok"] is False, r
    assert "nicht gesetzt" in r["detail"]


# --- Cache ---

def test_ergebnis_wird_gecacht(monkeypatch):
    cls = _install(monkeypatch, 200)
    _run(ollama.check_llm_auth())
    _run(ollama.check_llm_auth())
    _run(ollama.check_llm_auth())
    assert cls.calls == 1, "Probe darf pro TTL nur EINMAL raus"


def test_auch_das_negative_ergebnis_wird_gecacht(monkeypatch):
    """Sonst hämmert ein toter Key alle 30 s gegen die API."""
    cls = _install(monkeypatch, 401)
    _run(ollama.check_llm_auth())
    _run(ollama.check_llm_auth())
    assert cls.calls == 1
    assert ollama._llm_auth_cache["ok"] is False


def test_force_umgeht_den_cache(monkeypatch):
    cls = _install(monkeypatch, 200)
    _run(ollama.check_llm_auth())
    _run(ollama.check_llm_auth(force=True))
    assert cls.calls == 2


def test_cache_laeuft_nach_ttl_ab(monkeypatch):
    cls = _install(monkeypatch, 200)
    _run(ollama.check_llm_auth())
    ollama._llm_auth_cache["ts"] = time.time() - ollama.LLM_AUTH_TTL - 1
    _run(ollama.check_llm_auth())
    assert cls.calls == 2


def test_ttl_deckelt_die_upstream_last(monkeypatch):
    """30-s-Healthcheck * TTL 300 s -> hoechstens ~288 Requests/Tag."""
    assert ollama.LLM_AUTH_TTL >= 60
    assert 86400 / ollama.LLM_AUTH_TTL <= 1440


def test_probe_verbraucht_keine_tokens():
    """/v1/models statt einer Completion — Contract gegen Kosten-Drift."""
    assert ollama.MISTRAL_MODELS_URL.endswith("/v1/models")


# --- Modell-Lage (30.10.2026) ------------------------------------------
#
# Die Sonde ruft /v1/models auf und warf die LISTE weg: Sie prüfte nur
# den Status. Ein abgeschaltetes Modell wäre erst beim ersten
# gescheiterten Faktencheck aufgefallen — und die Mistral-API warnt
# nicht, `deprecation` ist bei allen 64 Modellen None.

#: Wörtlich aus dem Katalog unseres Keys, auf einen Eintrag verkürzt.
_KATALOG = {"data": [{
    "id": "mistral-medium-latest",
    "aliases": ["mistral-medium", "mistral-medium-3-5",
                "mistral-medium-2604", "magistral-medium-latest"],
    "deprecation": None, "max_context_length": 262144}]}


def test_unlesbarer_rumpf_laesst_die_sonde_in_ruhe(monkeypatch):
    """Die Tri-State-Lehre gilt weiter: Am Container-Healthcheck hängt
    `ok`, und ein Rumpf, den niemand lesen kann, ist kein Grund, den
    Dienst unhealthy zu melden."""
    _install(monkeypatch, 200, rumpf=None)
    out = _run(ollama.check_llm_auth(force=True))
    assert out["ok"] is True
    assert "modell" not in out


def test_die_modell_lage_wird_mitgeliefert(monkeypatch):
    _install(monkeypatch, 200, rumpf=_KATALOG)
    monkeypatch.setattr(ollama, "MISTRAL_MODEL", "mistral-medium-latest")
    out = _run(ollama.check_llm_auth(force=True))
    assert out["ok"] is True
    assert out["modell"]["lage"] == "ok"
    assert out["modell"]["datiert"] == "mistral-medium-2604"
    assert "2027-04-30" in out["modell"]["nachricht"]


def test_verschwundenes_modell_macht_den_dienst_nicht_unhealthy(monkeypatch):
    """BEWUSST so: Alarm schlägt der Cron-Wächter. Ein Flappen des
    Containers würde die Lage nur verschleiern."""
    _install(monkeypatch, 200, rumpf=_KATALOG)
    monkeypatch.setattr(ollama, "MISTRAL_MODEL", "gibt-es-nicht")
    out = _run(ollama.check_llm_auth(force=True))
    assert out["ok"] is True
    assert out["modell"]["lage"] == "verschwunden"


def test_der_rueckfall_auf_small_wird_benannt(monkeypatch):
    """Ohne MISTRAL_MODEL beantwortet ein anderes Modell jeden Claim."""
    monkeypatch.delenv("MISTRAL_MODEL", raising=False)
    _install(monkeypatch, 200, rumpf=_KATALOG)
    monkeypatch.setattr(ollama, "MISTRAL_MODEL", "mistral-medium-latest")
    out = _run(ollama.check_llm_auth(force=True))
    assert out["modell"]["aus_default"] is True
