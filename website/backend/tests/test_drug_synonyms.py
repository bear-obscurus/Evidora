"""Contract-Tests für die Marken→Wirkstoff-Expansion (Audit 2026-07-07).

Befund (Voltadol): „Voltadol schädigt die Nieren" erreichte die Medizin-
Fachquellen (EMA/MedlinePlus/EuropePMC/ClinicalTrials/openFDA) nicht — die
indexieren den Wirkstoff „Diclofenac", nicht den Markennamen. Der Analyzer
ergänzt jetzt deterministisch den INN.

Dependency-light: reine String-/Dict-Logik, kein Netz, kein LLM.
"""

import pytest

from services._drug_synonyms import (
    apply_brand_expansion,
    brand_inns_in_claim,
)


def test_voltadol_maps_to_diclofenac():
    assert brand_inns_in_claim("Voltadol schädigt die Nieren") == ["Diclofenac"]


def test_case_insensitive():
    assert "Diclofenac" in brand_inns_in_claim("VOLTADOL forte gel")
    assert "Ibuprofen" in brand_inns_in_claim("hilft nurofen bei Kopfweh")


def test_word_boundary_no_substring_false_positive():
    # 'antra' als Teil eines anderen Wortes darf NICHT matchen
    assert brand_inns_in_claim("Die Antarktis schmilzt") == []
    # 'aspirin' als ganzes Wort matcht, aber nicht in 'aspirinartig'? -> \b
    assert brand_inns_in_claim("Aspirin verdünnt das Blut") == ["Acetylsalicylsäure"]


def test_multiword_brand():
    assert "Acetylsalicylsäure" in brand_inns_in_claim("Ich nehme täglich Thrombo ASS")


def test_no_brand_returns_empty():
    assert brand_inns_in_claim("Der Klimawandel ist menschengemacht") == []
    assert brand_inns_in_claim("") == []


def test_apply_expansion_adds_inn_additively():
    result = {"entities": ["Voltadol", "Nieren"], "pubmed_queries": ["Voltadol kidney damage"]}
    apply_brand_expansion("Voltadol schädigt die Nieren", result)
    # Marke bleibt erhalten, Wirkstoff kommt dazu
    assert "Voltadol" in result["entities"]
    assert "Diclofenac" in result["entities"]
    assert any("Diclofenac" == q for q in result["pubmed_queries"])


def test_apply_expansion_idempotent():
    result = {"entities": ["Diclofenac"], "pubmed_queries": ["Diclofenac"]}
    apply_brand_expansion("Voltadol Nieren", result)
    # Diclofenac schon da → keine Dublette
    assert result["entities"].count("Diclofenac") == 1
    assert result["pubmed_queries"].count("Diclofenac") == 1


def test_apply_expansion_noop_without_brand():
    result = {"entities": ["Klimawandel"], "pubmed_queries": ["climate change"]}
    before = ({*result["entities"]}, {*result["pubmed_queries"]})
    apply_brand_expansion("Der Klimawandel ist real", result)
    assert ({*result["entities"]}, {*result["pubmed_queries"]}) == before


def test_apply_expansion_handles_missing_fields():
    # kein entities/pubmed_queries-Key vorhanden → wird angelegt, kein Crash
    result = {}
    apply_brand_expansion("Voltadol Nieren", result)
    assert "Diclofenac" in result["entities"]
    assert "Diclofenac" in result["pubmed_queries"]


@pytest.mark.parametrize("brand,inn", [
    ("Mexalen", "Paracetamol"),
    ("Parkemed", "Mefenaminsäure"),
    ("Seractil", "Dexibuprofen"),
    ("Cipralex", "Escitalopram"),
    ("Pantoloc", "Pantoprazol"),
])
def test_selected_at_brands(brand, inn):
    assert inn in brand_inns_in_claim(f"Ich habe {brand} genommen")
