"""API-level tests for individual data sources — requires network, no LLM needed."""

import pytest
import httpx

pytestmark = pytest.mark.asyncio


# ===================================================================
# Eurostat
# ===================================================================

class TestEurostat:
    async def test_unemployment_spain(self, analysis_spain_unemployment):
        from services.eurostat import search_eurostat
        result = await search_eurostat(analysis_spain_unemployment)
        assert result["source"] == "Eurostat (EU)"
        assert len(result["results"]) > 0
        # Should contain Spain data
        countries = {r.get("country", "") for r in result["results"]}
        assert any("Spain" in c or "ES" in c or "España" in c for c in countries)

    async def test_renewables_austria(self, analysis_austria_renewables):
        from services.eurostat import search_eurostat
        result = await search_eurostat(analysis_austria_renewables)
        assert result["source"] == "Eurostat (EU)"
        assert len(result["results"]) > 0
        countries = {r.get("country", "") for r in result["results"]}
        assert any("Austria" in c or "AT" in c for c in countries)

    async def test_inflation_eu(self):
        from services.eurostat import search_eurostat
        analysis = {
            "claim": "Die Inflation in der EU ist gestiegen",
            "category": "economy",
            "subcategory": "inflation",
            "entities": ["Inflation", "EU"],
            "eurostat_relevant": True,
        }
        result = await search_eurostat(analysis)
        assert len(result["results"]) > 0

    async def test_multi_country_superlative(self):
        """Superlative claims should return data for multiple EU countries."""
        from services.eurostat import search_eurostat
        analysis = {
            "claim": "Österreich hat den höchsten Anteil erneuerbarer Energien in der EU",
            "category": "energy",
            "subcategory": "renewable_energy",
            "entities": ["Österreich", "erneuerbare Energien"],
            "eurostat_relevant": True,
        }
        result = await search_eurostat(analysis)
        assert len(result["results"]) > 5, "Superlative should return multi-country ranking"
        countries = {r.get("country", r.get("geo", "")) for r in result["results"]}
        assert len(countries) > 3, f"Expected multiple countries, got: {countries}"

    async def test_non_superlative_single_country(self):
        """Non-superlative claims should return single-country data."""
        from services.eurostat import search_eurostat
        analysis = {
            "claim": "Die Arbeitslosigkeit in Spanien ist hoch",
            "category": "economy",
            "subcategory": "unemployment",
            "entities": ["Spanien", "Arbeitslosigkeit"],
            "eurostat_relevant": True,
        }
        result = await search_eurostat(analysis)
        countries = {r.get("country", "") for r in result["results"]}
        # Should only contain Spain (or EU aggregate at most)
        assert len(countries) <= 2


# ===================================================================
# EEA
# ===================================================================

class TestEEA:
    async def test_air_quality(self):
        from services.eea import search_eea
        analysis = {
            "claim": "Die Luftqualität in der EU hat sich verbessert",
            "category": "climate",
            "subcategory": "air_quality",
            "entities": ["Luftqualität", "EU"],
            "eea_relevant": True,
            "pubmed_queries": ["air quality EU improvement"],
            "factcheck_queries": ["Luftqualität EU Verbesserung"],
        }
        result = await search_eea(analysis)
        assert result["source"] == "European Environment Agency (EEA)"
        assert len(result["results"]) > 0


# ===================================================================
# ECB
# ===================================================================

class TestECB:
    async def test_interest_rate(self):
        from services.ecb import search_ecb
        analysis = {
            "claim": "Der EZB-Leitzins ist auf einem historischen Rekordtief",
            "category": "economy",
            "subcategory": "interest_rate",
            "entities": ["EZB", "Leitzins"],
            "ecb_relevant": True,
            "pubmed_queries": [],
            "factcheck_queries": [],
        }
        result = await search_ecb(analysis)
        assert result["source"] == "EZB (Europäische Zentralbank)"
        assert len(result["results"]) > 0
        # Should contain historical context
        titles = " ".join(r.get("title", "") for r in result["results"])
        assert any(word in titles.lower() for word in ["historisch", "minimum", "maximum", "historical"])


# ===================================================================
# Copernicus / NASA GISS
# ===================================================================

class TestCopernicus:
    async def test_temperature_claim(self):
        from services.copernicus import search_copernicus
        analysis = {
            "claim": "Die globale Durchschnittstemperatur ist seit 1880 um mehr als 1 Grad gestiegen",
            "category": "climate",
            "subcategory": "temperature",
            "entities": ["Durchschnittstemperatur", "1880"],
            "climate_relevant": True,
            "pubmed_queries": ["global temperature increase since 1880"],
            "factcheck_queries": [],
        }
        result = await search_copernicus(analysis)
        assert len(result["results"]) > 0
        # Should include NASA GISS data
        titles = " ".join(r.get("title", "") for r in result["results"])
        assert "temperatur" in titles.lower() or "temperature" in titles.lower()


# ===================================================================
# UNHCR
# ===================================================================

class TestUNHCR:
    async def test_refugee_data(self):
        from services.unhcr import search_unhcr
        analysis = {
            "claim": "Deutschland nimmt die meisten Flüchtlinge in Europa auf",
            "category": "migration",
            "subcategory": "refugees",
            "entities": ["Deutschland", "Flüchtlinge", "Europa"],
            "unhcr_relevant": True,
            "pubmed_queries": [],
            "factcheck_queries": [],
        }
        result = await search_unhcr(analysis)
        assert "UNHCR" in result["source"]
        # UNHCR may or may not return data depending on API availability
        assert "results" in result


# ===================================================================
# PubMed
# ===================================================================

class TestPubMed:
    async def test_vaccine_search(self):
        from services.pubmed import search_pubmed
        analysis = {
            "claim": "Impfungen verursachen Autismus",
            "category": "health",
            "pubmed_queries": ["vaccines autism causation"],
            "factcheck_queries": [],
            "entities": ["Impfungen", "Autismus"],
        }
        result = await search_pubmed(analysis)
        assert result["source"] == "PubMed"
        assert len(result["results"]) > 0


# ===================================================================
# GADMO
# ===================================================================

class TestGADMO:
    async def test_factcheck_search(self):
        from services.gadmo import search_gadmo
        analysis = {
            "claim": "Impfungen verursachen Autismus",
            "category": "health",
            "factcheck_queries": ["Impfungen Autismus"],
            "pubmed_queries": [],
            "entities": [],
        }
        result = await search_gadmo(analysis)
        assert result["source"] in ("GADMO Faktenchecks", "Europäische Faktenchecker")
        assert "results" in result


# ===================================================================
# ClaimReview
# ===================================================================

class TestECDC:
    async def test_covid_search(self):
        from services.ecdc import search_ecdc
        analysis = {
            "claim": "COVID-19 ist in Europa ausgerottet",
            "category": "health",
            "subcategory": "covid",
            "entities": ["COVID-19", "Europa"],
            "ecdc_relevant": True,
            "pubmed_queries": [],
            "factcheck_queries": [],
        }
        result = await search_ecdc(analysis)
        assert "ECDC" in result["source"]
        assert len(result["results"]) > 0
        # Should have COVID case/death data
        titles = " ".join(r.get("title", "") for r in result["results"])
        assert "COVID" in titles

    async def test_covid_country_specific(self):
        from services.ecdc import search_ecdc
        analysis = {
            "claim": "Österreich hat die höchste COVID-Impfquote",
            "category": "health",
            "subcategory": "vaccination",
            "entities": ["Österreich", "COVID-19", "Impfquote"],
            "ecdc_relevant": True,
            "pubmed_queries": [],
            "factcheck_queries": [],
        }
        result = await search_ecdc(analysis)
        assert len(result["results"]) > 0
        countries = " ".join(r.get("country", "") for r in result["results"])
        assert "Austria" in countries


class TestClaimReview:
    async def test_factcheck_search(self):
        from services.claimreview import search_claimreview
        analysis = {
            "claim": "Hydroxychloroquin ist ein wirksames Mittel gegen COVID-19",
            "category": "medication",
            "factcheck_queries": ["Hydroxychloroquin COVID-19 Wirksamkeit"],
            "pubmed_queries": [],
            "entities": [],
        }
        result = await search_claimreview(analysis)
        assert "results" in result
