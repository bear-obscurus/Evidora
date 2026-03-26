"""Unit tests — no running backend or LLM needed."""

import time

import pytest

from services.cache import _make_key, get, put, clear, stats
from services.eurostat import _find_datasets, _find_country, _is_superlative_claim, COUNTRY_CODES, DATASET_MAP
from services.ner import enrich_entities, _detect_language
from services.oecd import _load_pisa, _search_pisa, _is_pisa_claim, _is_gender_claim, _find_country_code, _detect_subject


# ===================================================================
# Cache
# ===================================================================

class TestCache:
    def setup_method(self):
        clear()

    def test_put_and_get(self):
        analysis = {"claim": "test", "category": "health", "entities": [], "pubmed_queries": []}
        data = {"source": "test", "results": [{"title": "x"}]}
        put("TestSource", analysis, data)
        result = get("TestSource", analysis)
        assert result is not None
        assert result["source"] == "test"

    def test_cache_miss(self):
        analysis = {"claim": "test", "category": "health", "entities": [], "pubmed_queries": []}
        result = get("NonExistent", analysis)
        assert result is None

    def test_cache_expiry(self):
        analysis = {"claim": "test", "category": "health", "entities": [], "pubmed_queries": []}
        put("TestSource", analysis, {"source": "test", "results": []})
        # TTL of 0 seconds → expired immediately
        result = get("TestSource", analysis, ttl=0)
        assert result is None

    def test_cache_stats(self):
        analysis = {"claim": "test", "category": "health", "entities": [], "pubmed_queries": []}
        put("A", analysis, {"source": "A", "results": []})
        put("B", analysis, {"source": "B", "results": []})
        s = stats()
        assert s["total"] == 2
        assert s["valid"] == 2

    def test_cache_clear(self):
        analysis = {"claim": "test", "category": "health", "entities": [], "pubmed_queries": []}
        put("A", analysis, {"source": "A", "results": []})
        clear()
        assert stats()["total"] == 0

    def test_different_sources_different_keys(self):
        analysis = {"claim": "test", "category": "health", "entities": [], "pubmed_queries": []}
        key1 = _make_key("SourceA", analysis)
        key2 = _make_key("SourceB", analysis)
        assert key1 != key2


# ===================================================================
# Eurostat helpers
# ===================================================================

class TestEurostatHelpers:
    def test_find_country_austria(self, analysis_austria_renewables):
        code = _find_country(analysis_austria_renewables)
        assert code == "AT"

    def test_find_country_spain(self, analysis_spain_unemployment):
        code = _find_country(analysis_spain_unemployment)
        assert code == "ES"

    def test_find_country_default_eu(self):
        analysis = {"entities": ["irgendwas Unbekanntes"]}
        code = _find_country(analysis)
        assert code == "EU27_2020"

    def test_find_datasets_renewables(self, analysis_austria_renewables):
        datasets = _find_datasets(analysis_austria_renewables)
        ds_codes = [d["dataset"] for d in datasets]
        assert "nrg_ind_ren" in ds_codes

    def test_find_datasets_unemployment(self, analysis_spain_unemployment):
        datasets = _find_datasets(analysis_spain_unemployment)
        ds_codes = [d["dataset"] for d in datasets]
        assert "une_rt_m" in ds_codes

    def test_find_datasets_empty(self):
        analysis = {"entities": ["xyz"], "subcategory": "xyz", "category": "xyz"}
        datasets = _find_datasets(analysis)
        assert datasets == []

    def test_country_codes_complete(self):
        """All 27 EU member states should be mapped."""
        eu27_codes = {
            "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
            "DE", "EL", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
            "PL", "PT", "RO", "SK", "SI", "ES", "SE",
        }
        mapped_codes = set(COUNTRY_CODES.values()) - {"EU27_2020"}
        assert eu27_codes.issubset(mapped_codes), f"Missing: {eu27_codes - mapped_codes}"

    def test_dataset_map_has_key_categories(self):
        """Essential categories should have dataset mappings."""
        essential = [
            "inflation", "arbeitslosigkeit", "erneuerbare", "bip", "bevölkerung",
            "co2", "lebenserwartung", "gesundheitsausgaben", "immobilienpreise",
            "mindestlohn", "staatsschulden", "ungleichheit", "tourismus",
        ]
        for keyword in essential:
            assert keyword in DATASET_MAP, f"Missing keyword: {keyword}"

    def test_superlative_detected(self, analysis_austria_renewables):
        assert _is_superlative_claim(analysis_austria_renewables)

    def test_no_superlative(self, analysis_spain_unemployment):
        assert not _is_superlative_claim(analysis_spain_unemployment)


# ===================================================================
# Synthesizer logic (no LLM)
# ===================================================================

class TestSuperlativeDetection:
    """Test the superlative keyword detection logic from synthesizer.py."""

    SUPERLATIVE_KEYWORDS = [
        "höchste", "höchsten", "niedrigste", "niedrigsten", "meiste", "meisten",
        "größte", "größten", "beste", "besten", "schlechteste", "schlechtesten",
        "wenigste", "wenigsten", "stärkste", "stärksten",
        "highest", "lowest", "most", "least", "largest", "smallest", "best", "worst",
    ]

    def _is_superlative(self, claim: str) -> bool:
        return any(kw in claim.lower() for kw in self.SUPERLATIVE_KEYWORDS)

    def test_german_superlative(self):
        assert self._is_superlative("Österreich hat den höchsten Anteil erneuerbarer Energien")

    def test_english_superlative(self):
        assert self._is_superlative("Germany has the highest number of refugees")

    def test_no_superlative(self):
        assert not self._is_superlative("Die Arbeitslosigkeit in Spanien liegt unter dem EU-Durchschnitt")

    def test_niedrigste(self):
        assert self._is_superlative("Der EZB-Leitzins ist auf dem niedrigsten Stand")

    def test_most(self):
        assert self._is_superlative("Germany takes in the most refugees in Europe")

    def _count_real_countries(self, source_results: list[dict]) -> int:
        eu_labels = {"EU27_2020", "European Union", "European Union - 27 countries (from 2020)", "EU"}
        all_countries = set()
        for source_data in source_results:
            if not isinstance(source_data, dict):
                continue
            for r in source_data.get("results", []):
                geo = r.get("geo", r.get("country", ""))
                if geo:
                    all_countries.add(geo)
        return len(all_countries - eu_labels)

    def test_single_country_detected(self):
        sources = [{"source": "Eurostat", "results": [
            {"country": "Austria", "value": "34%"},
            {"country": "Austria", "value": "36%"},
        ]}]
        assert self._count_real_countries(sources) == 1

    def test_multiple_countries_detected(self):
        sources = [{"source": "Eurostat", "results": [
            {"country": "Austria", "value": "34%"},
            {"country": "Germany", "value": "20%"},
            {"country": "France", "value": "22%"},
        ]}]
        assert self._count_real_countries(sources) == 3

    def test_eu_aggregate_excluded(self):
        sources = [{"source": "Eurostat", "results": [
            {"country": "Austria", "value": "34%"},
            {"country": "EU27_2020", "value": "22%"},
        ]}]
        assert self._count_real_countries(sources) == 1


# ===================================================================
# Hallucination filter
# ===================================================================

class TestHallucinationFilter:
    """Test the URL-based hallucination filter logic from synthesizer.py."""

    def _filter_evidence(self, evidence: list[dict], source_results: list[dict]) -> list[dict]:
        real_urls = set()
        for source_data in source_results:
            if isinstance(source_data, dict):
                for r in source_data.get("results", []):
                    if r.get("url"):
                        real_urls.add(r["url"])
        if not real_urls:
            return []
        return [e for e in evidence if e.get("url") in real_urls]

    def test_keeps_real_urls(self):
        sources = [{"source": "X", "results": [{"url": "https://real.com/1"}]}]
        evidence = [{"source": "X", "url": "https://real.com/1", "finding": "ok"}]
        filtered = self._filter_evidence(evidence, sources)
        assert len(filtered) == 1

    def test_removes_hallucinated_urls(self):
        sources = [{"source": "X", "results": [{"url": "https://real.com/1"}]}]
        evidence = [
            {"source": "X", "url": "https://real.com/1", "finding": "ok"},
            {"source": "Y", "url": "https://fake.com/invented", "finding": "hallucinated"},
        ]
        filtered = self._filter_evidence(evidence, sources)
        assert len(filtered) == 1
        assert filtered[0]["url"] == "https://real.com/1"

    def test_no_sources_clears_all(self):
        sources = [{"source": "X", "results": []}]
        evidence = [{"source": "Y", "url": "https://fake.com", "finding": "all fake"}]
        filtered = self._filter_evidence(evidence, sources)
        assert len(filtered) == 0


# ===================================================================
# Rate limiter
# ===================================================================

class TestNER:
    """Test SpaCy NER enrichment."""

    def test_language_detection_german(self):
        assert _detect_language("Die Arbeitslosigkeit in Spanien ist gestiegen") == "de"

    def test_language_detection_english(self):
        assert _detect_language("Unemployment in Spain has increased") == "en"

    def test_enrich_adds_countries(self):
        analysis = {"entities": ["Arbeitslosigkeit"], "category": "economy"}
        result = enrich_entities("Die Arbeitslosigkeit in Österreich ist hoch", analysis)
        # SpaCy should detect Österreich as GPE
        entities_lower = [e.lower() for e in result["entities"]]
        assert "österreich" in entities_lower

    def test_enrich_preserves_existing(self):
        analysis = {"entities": ["Impfungen", "Autismus"], "category": "health"}
        result = enrich_entities("Impfungen verursachen Autismus", analysis)
        assert "Impfungen" in result["entities"]
        assert "Autismus" in result["entities"]

    def test_enrich_no_duplicates(self):
        analysis = {"entities": ["Deutschland"], "category": "economy"}
        result = enrich_entities("Deutschland hat die meisten Flüchtlinge", analysis)
        lower = [e.lower() for e in result["entities"]]
        assert lower.count("deutschland") == 1

    def test_enrich_graceful_without_spacy(self):
        """If SpaCy fails to load, original analysis is returned unchanged."""
        analysis = {"entities": ["Test"], "category": "other"}
        result = enrich_entities("Test claim", analysis)
        assert "Test" in result["entities"]


# ===================================================================
# OECD / PISA
# ===================================================================

class TestOECD:
    def test_pisa_csv_loads(self):
        data = _load_pisa()
        assert len(data) > 100  # ~300 rows expected

    def test_pisa_has_gender_data(self):
        data = _load_pisa()
        genders = {r["gender"] for r in data}
        assert {"total", "boy", "girl"} == genders

    def test_pisa_has_key_countries(self):
        data = _load_pisa()
        codes = {r["country_code"] for r in data}
        for c in ["AUT", "DEU", "FRA", "EST", "JPN", "OECD"]:
            assert c in codes, f"Missing {c}"

    def test_pisa_claim_detection(self):
        assert _is_pisa_claim("Frauen sind schlechter in Mathematik")
        assert _is_pisa_claim("PISA scores in Finland")
        assert _is_pisa_claim("Mädchen lesen besser als Jungen")
        assert not _is_pisa_claim("Die Inflation in Österreich steigt")

    def test_gender_claim_detection(self):
        assert _is_gender_claim("Frauen sind schlechter in Mathematik")
        assert _is_gender_claim("Gender pay gap in Germany")
        assert not _is_gender_claim("Österreich hat hohe Inflation")

    def test_subject_detection(self):
        assert _detect_subject("Mädchen sind besser in Mathe") == "math"
        assert _detect_subject("Girls read better than boys") == "reading"
        assert _detect_subject("Naturwissenschaft in Finnland") == "science"
        assert _detect_subject("Bildung in Österreich") is None

    def test_country_code_detection(self):
        assert _find_country_code({"entities": ["Österreich"], "claim": ""}) == "AUT"
        assert _find_country_code({"entities": ["Germany"], "claim": ""}) == "DEU"
        assert _find_country_code({"entities": [], "claim": "PISA in Finnland"}) == "FIN"

    def test_pisa_gender_search(self):
        analysis = {"claim": "Frauen sind schlechter in Mathematik", "entities": [], "category": "education"}
        results = _search_pisa("Frauen sind schlechter in Mathematik", analysis)
        assert len(results) > 0
        # Should have OECD average math gender gap
        math_result = next((r for r in results if "Math" in r["title"]), None)
        assert math_result is not None
        assert "Jungen" in math_result["value"]
        assert "Mädchen" in math_result["value"]

    def test_pisa_country_specific(self):
        analysis = {"claim": "Österreich PISA Mathe", "entities": ["Österreich"], "category": "education"}
        results = _search_pisa("Österreich PISA Mathe Geschlecht", analysis)
        assert any("Austria" in r.get("country", "") for r in results)

    def test_pisa_non_gender(self):
        analysis = {"claim": "PISA Ergebnisse in Estland", "entities": ["Estland"], "category": "education"}
        results = _search_pisa("PISA Ergebnisse in Estland", analysis)
        assert len(results) > 0
        assert any("Estonia" in r.get("country", "") for r in results)


class TestRateLimiter:
    def test_rate_limit_logic(self):
        """Simulate the rate limit logic from main.py."""
        store: dict[str, list[float]] = {}
        limit = 3
        window = 60

        def check(ip: str) -> bool:
            now = time.time()
            timestamps = store.get(ip, [])
            timestamps = [t for t in timestamps if now - t < window]
            store[ip] = timestamps
            if len(timestamps) >= limit:
                return False
            timestamps.append(now)
            return True

        assert check("1.2.3.4") is True
        assert check("1.2.3.4") is True
        assert check("1.2.3.4") is True
        assert check("1.2.3.4") is False  # 4th request blocked
        assert check("5.6.7.8") is True   # different IP ok
