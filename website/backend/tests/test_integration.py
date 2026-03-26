"""Integration tests — requires a running Evidora backend (+ LLM).

Run with:  pytest tests/test_integration.py -v --timeout=180

Set EVIDORA_TEST_URL to test against a different backend (e.g. https://evidora.eu).

NOTE: LLM verdicts are non-deterministic. Tests are split into:
  - "strict" — clear-cut claims where the verdict should be stable
  - "soft"   — borderline claims where LLM may vary; these log warnings instead of failing
"""

import json
import os
import warnings

import httpx
import pytest

BACKEND_URL = os.getenv("EVIDORA_TEST_URL", "http://localhost:8000")
VALID_VERDICTS = {"true", "mostly_true", "mixed", "mostly_false", "false", "unverifiable"}

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
]


async def _check_claim(claim: str, lang: str = "de") -> dict:
    """Send a claim to the backend and parse the SSE result."""
    async with httpx.AsyncClient(timeout=180.0) as client:
        async with client.stream(
            "POST",
            f"{BACKEND_URL}/api/check",
            json={"claim": claim, "lang": lang},
            headers={"Accept": "text/event-stream"},
        ) as resp:
            resp.raise_for_status()
            result = None
            async for line in resp.aiter_lines():
                if line.startswith("data:"):
                    data = json.loads(line[5:].strip())
                    if "verdict" in data:
                        result = data
            return result


def _assert_verdict(result, expected: tuple[str, ...], claim: str, strict: bool = True):
    """Check verdict, with soft mode for LLM-sensitive claims."""
    assert result is not None, f"No result for: {claim}"
    assert result["verdict"] in VALID_VERDICTS, f"Invalid verdict: {result['verdict']}"
    if result["verdict"] not in expected:
        msg = f"[{claim}] Expected {expected}, got: {result['verdict']}"
        if strict:
            pytest.fail(msg)
        else:
            warnings.warn(msg, stacklevel=2)


# ===================================================================
# Strict tests — clear-cut claims, stable verdicts
# ===================================================================

class TestAPIBasics:
    async def test_health_endpoint(self):
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{BACKEND_URL}/api/health")
            assert resp.status_code == 200
            assert resp.json()["status"] == "ok"

    async def test_legal_endpoint(self):
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{BACKEND_URL}/api/legal")
            assert resp.status_code == 200

    async def test_empty_claim_rejected(self):
        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{BACKEND_URL}/api/check", json={"claim": ""})
            assert resp.status_code == 400


class TestResultStructure:
    """Verify that every result has the required fields."""

    async def test_result_has_required_fields(self):
        r = await _check_claim("Impfungen verursachen Autismus")
        assert r is not None
        for key in ("verdict", "confidence", "summary", "evidence", "disclaimer"):
            assert key in r, f"Missing key: {key}"
        assert isinstance(r["confidence"], (int, float))
        assert 0.0 <= r["confidence"] <= 1.0

    async def test_source_coverage_present(self):
        r = await _check_claim("Impfungen verursachen Autismus")
        assert "source_coverage" in r
        assert r["source_coverage"]["queried"] > 0

    async def test_has_evidence(self):
        r = await _check_claim("Impfungen verursachen Autismus")
        assert len(r.get("evidence", [])) > 0


class TestVaccineAutism:
    """Clear health misinformation — should always be false."""

    async def test_verdict(self):
        r = await _check_claim("Impfungen verursachen Autismus")
        _assert_verdict(r, ("false", "mostly_false"), "Impfungen/Autismus", strict=True)


class TestHydroxychloroquin:
    """Clear medication misinformation — should always be false."""

    async def test_verdict(self):
        r = await _check_claim("Hydroxychloroquin ist ein wirksames Mittel gegen COVID-19")
        _assert_verdict(r, ("false", "mostly_false"), "Hydroxychloroquin", strict=True)


class TestGlobalTemperature:
    """Scientific consensus — should always be true."""

    async def test_verdict(self):
        r = await _check_claim("Die globale Durchschnittstemperatur ist seit 1880 um mehr als 1 Grad gestiegen")
        _assert_verdict(r, ("true", "mostly_true"), "Temperaturanstieg", strict=True)

    async def test_has_climate_source(self):
        r = await _check_claim("Die globale Durchschnittstemperatur ist seit 1880 um mehr als 1 Grad gestiegen")
        source_names = " ".join(s.get("source", "") for s in r.get("raw_sources", []))
        assert any(kw in source_names.lower() for kw in ("copernicus", "nasa", "climate"))


# ===================================================================
# Soft tests — LLM-sensitive, may vary between runs
# ===================================================================

class TestSpainUnemployment:
    """Spain unemployment above EU average — data is clear but LLM may misinterpret."""

    async def test_verdict(self):
        r = await _check_claim("Die Arbeitslosigkeit in Spanien liegt unter dem EU-Durchschnitt")
        _assert_verdict(r, ("false", "mostly_false"), "Spanien Arbeitslosigkeit", strict=False)

    async def test_has_eurostat_data(self):
        r = await _check_claim("Die Arbeitslosigkeit in Spanien liegt unter dem EU-Durchschnitt")
        source_names = " ".join(s.get("source", "") for s in r.get("raw_sources", []))
        assert "eurostat" in source_names.lower()


class TestECBRecordLow:
    """ECB rate not at record low — requires temporal reasoning."""

    async def test_verdict(self):
        r = await _check_claim("Der EZB-Leitzins ist auf einem historischen Rekordtief")
        _assert_verdict(r, ("false", "mostly_false"), "EZB Rekordtief", strict=False)


class TestSuperlativeRenewables:
    """Superlative with single-country data — should be unverifiable."""

    async def test_verdict(self):
        r = await _check_claim("Österreich hat den höchsten Anteil erneuerbarer Energien in der EU")
        _assert_verdict(r, ("unverifiable", "mixed", "mostly_true"), "AT Erneuerbare Superlativ", strict=False)


class TestCovidEradicated:
    """COVID eradicated in Europe — clearly false."""

    async def test_verdict(self):
        r = await _check_claim("COVID-19 ist in Europa ausgerottet")
        _assert_verdict(r, ("false", "mostly_false", "unverifiable"), "COVID ausgerottet", strict=True)
