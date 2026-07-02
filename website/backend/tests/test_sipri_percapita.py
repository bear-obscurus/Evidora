"""Pro-Kopf-Ableitung im SIPRI-Service (#41-Data-Miss, 2026-07-02).

Der Service lieferte nur Absolutwerte + BIP-Anteil — Pro-Kopf-Claims
("Österreich liegt bei den Pro-Kopf-Rüstungsausgaben über dem
NATO-Durchschnitt") endeten als unverifiable. Diese Tests pinnen die
Ableitung (Ausgaben ÷ Bevölkerung) und den bevölkerungsgewichteten
NATO-Durchschnitt. Kein Netzwerk: der Modul-Cache wird geseedet.
"""

import time

import pytest

import services.sipri as sipri


def _seed_cache(data):
    sipri._sipri_cache = data
    sipri._sipri_cache_time = time.time()


def _synthetic_data():
    """AUT + 22 NATO-Staaten mit Ausgaben/Bevölkerung für 2024."""
    data = {
        "AUT": {2024: {"expenditure_usd": 5.9e9, "gdp_share": 1.10,
                       "govt_share": 1.5, "entity": "Austria",
                       "population": 9_100_000}},
    }
    # Deterministisch: sortiert + USA erzwungen (Set-Iterationsreihenfolge
    # ist PYTHONHASHSEED-abhängig — ohne USA kippt die Gewichts-Assertion;
    # genau so flakte der Test beim ersten Suite-Lauf).
    nato = ["USA"] + [c for c in sorted(sipri.NATO_MEMBERS) if c != "USA"]
    nato = nato[:22]
    for i, code in enumerate(nato):
        # USA dominiert absichtlich (gewichteter Schnitt)
        exp = 900e9 if code == "USA" else 20e9 + i * 1e9
        pop = 335_000_000 if code == "USA" else 20_000_000 + i * 1_000_000
        data[code] = {2024: {"expenditure_usd": exp, "gdp_share": 2.0,
                             "govt_share": 3.0, "entity": code,
                             "population": pop}}
    return data


def test_per_capita_helper():
    assert sipri._per_capita_usd(5.9e9, 9_100_000) == pytest.approx(648.35, abs=0.1)
    assert sipri._per_capita_usd(None, 9_100_000) is None
    assert sipri._per_capita_usd(5.9e9, None) is None
    assert sipri._per_capita_usd(5.9e9, 0) is None


def test_nato_avg_weighted_and_min_coverage():
    data = _synthetic_data()
    avg = sipri._nato_avg_per_capita(data, 2024)
    assert avg is not None
    avg_usd, n = avg
    assert n >= 20
    # bevölkerungsgewichtet: muss deutlich über AUT-Wert (~648) liegen,
    # weil der US-Wert (~2.686 USD/Kopf) dominiert
    assert avg_usd > 1000
    # Jahr ohne Daten -> None
    assert sipri._nato_avg_per_capita(data, 1999) is None
    # zu wenige Staaten -> None
    small = {k: v for k, v in list(data.items())[:5]}
    assert sipri._nato_avg_per_capita(small, 2024) is None


@pytest.mark.asyncio
async def test_search_sipri_per_capita_claim():
    _seed_cache(_synthetic_data())
    claim = ("Laut SIPRI-Jahrbuch 2024 liegt Österreich bei den "
             "Pro-Kopf-Rüstungsausgaben über dem NATO-Durchschnitt.")
    out = await sipri.search_sipri({"claim": claim, "ner_entities": {}})
    names = [r["indicator_name"] for r in out["results"]]
    aut = [n for n in names if n.startswith("SIPRI Austria")]
    assert aut and "USD/Kopf" in aut[0], names
    nato = [n for n in names if "NATO-Durchschnitt" in n]
    assert nato and "USD/Kopf" in nato[0], names
    assert "bevölkerungsgewichtet" in nato[0]


@pytest.mark.asyncio
async def test_search_sipri_no_nato_line_without_percapita_or_nato():
    _seed_cache(_synthetic_data())
    claim = "Die Militärausgaben von Österreich sind gestiegen."
    out = await sipri.search_sipri({"claim": claim, "ner_entities": {}})
    names = [r["indicator_name"] for r in out["results"]]
    assert not any("NATO-Durchschnitt" in n for n in names), names
