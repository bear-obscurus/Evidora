"""Transparency International — Corruption Perception Index (CPI).

Datenquelle: Transparency International (CPI, jährlich seit 1995).
Zugriff via Our World in Data (OWID) Grapher CSV, der täglich aus
den offiziellen TI-Veröffentlichungen aktualisiert wird.

Skala: 0 = höchst korrupt, 100 = keine wahrgenommene Korruption.
Abdeckung: ~180 Länder, Jahre 2012–heute (konsistente Methodik seit 2012).

Lizenz: CC BY-ND 4.0 (Transparency International).
Zitation: Transparency International, Corruption Perceptions Index {Jahr}.

GUARDRAILS (siehe project_political_guardrails.md):
- Wir zitieren CPI-Scores, berechnen keine eigenen Korruptionsbewertungen.
- Caveat zur Methodik (Wahrnehmung, nicht gemessene Korruption) ist Pflicht.
"""

import csv
import io
import logging
import time

import httpx

logger = logging.getLogger("evidora")

CPI_CSV_URL = "https://ourworldindata.org/grapher/ti-corruption-perception-index.csv"

CPI_CACHE_TTL = 86400  # 24h

# Cache structure: {iso3: {year: {score, entity}}}
_cpi_cache: dict | None = None
_cpi_cache_time: float = 0.0

# Trigger-Keywords (DE + EN)
CPI_KEYWORDS = [
    "korruption", "korrupt", "korruptionsindex",
    "corruption", "corrupt", "corruption index",
    "transparency international",
    "cpi", "corruption perception",
    "bestechung", "bribery", "bribe",
    "vetternwirtschaft", "cronyism",
    "amtsmissbrauch", "abuse of office",
    "integrität", "integrity",
]

# EU-27 Mitgliedsstaaten (ISO3) — für Durchschnitts- und Ranking-Kontext,
# wenn der Claim auf den EU-Schnitt verweist.
EU27_MEMBERS = frozenset({
    "AUT", "BEL", "BGR", "HRV", "CYP", "CZE", "DNK", "EST", "FIN", "FRA",
    "DEU", "GRC", "HUN", "IRL", "ITA", "LVA", "LTU", "LUX", "MLT", "NLD",
    "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE",
})

# Trigger-Keywords für EU-Kohorten-Vergleich (löst EU-27-Durchschnitt aus)
# 2026-04: Erweitert um komparative Formulierungen ("als in der EU", "als die
# EU", "gegenüber der EU" …), nachdem ein Testclaim "Die Korruption in der
# Ukraine ist höher als in der EU" die EU-Kohorte nicht ausgelöst hatte und
# das LLM dadurch den EU-Ø halluzinierte.
EU_COMPARISON_TRIGGERS = [
    # Durchschnitt / Mittel / Schnitt
    "eu-durchschnitt", "eu durchschnitt", "eu-schnitt", "eu schnitt",
    "eu-mittel", "eu mittel", "eu-mittelwert", "eu mittelwert",
    "europa-durchschnitt", "europa durchschnitt",
    "europa-schnitt", "europa schnitt",
    "eu average", "european average", "europe average",
    "average of the eu", "eu mean",
    # Mitgliedsstaaten
    "eu-länder", "eu länder", "eu-staaten", "eu staaten",
    "mitgliedsstaaten", "mitgliedsländer",
    "member states", "eu member",
    # Komparativ DE: "als in der EU", "als die EU", "als der EU"
    "als in der eu", "als die eu", "als der eu", "als dem eu",
    "als in europa", "als europa",
    # Komparativ DE: "gegenüber / verglichen mit"
    "gegenüber der eu", "gegenüber dem eu", "gegenüber europa",
    "verglichen mit der eu", "verglichen mit dem eu",
    "verglichen mit europa",
    "im vergleich zur eu", "im vergleich zum eu",
    "im vergleich zu europa",
    "im eu-vergleich", "im europa-vergleich",
    # Komparativ EN
    "compared to the eu", "compared with the eu",
    "vs. the eu", "vs the eu", "versus the eu",
    "than the eu", "than in the eu",
    "compared to europe", "than europe", "than in europe",
]

# Kleinere Country-Map (wir setzen auf die gleiche ISO3-Logik wie V-Dem)
COUNTRY_MAP = {
    "österreich": "AUT", "austria": "AUT",
    "deutschland": "DEU", "germany": "DEU",
    "schweiz": "CHE", "switzerland": "CHE",
    "frankreich": "FRA", "france": "FRA",
    "italien": "ITA", "italy": "ITA",
    "spanien": "ESP", "spain": "ESP",
    "niederlande": "NLD", "netherlands": "NLD",
    "belgien": "BEL", "belgium": "BEL",
    "polen": "POL", "poland": "POL",
    "tschechien": "CZE", "czech republic": "CZE", "czechia": "CZE",
    "ungarn": "HUN", "hungary": "HUN",
    "rumänien": "ROU", "romania": "ROU",
    "bulgarien": "BGR", "bulgaria": "BGR",
    "kroatien": "HRV", "croatia": "HRV",
    "slowenien": "SVN", "slovenia": "SVN",
    "slowakei": "SVK", "slovakia": "SVK",
    "dänemark": "DNK", "denmark": "DNK",
    "schweden": "SWE", "sweden": "SWE",
    "norwegen": "NOR", "norway": "NOR",
    "finnland": "FIN", "finland": "FIN",
    "portugal": "PRT",
    "griechenland": "GRC", "greece": "GRC",
    "irland": "IRL", "ireland": "IRL",
    "luxemburg": "LUX", "luxembourg": "LUX",
    "estland": "EST", "estonia": "EST",
    "lettland": "LVA", "latvia": "LVA",
    "litauen": "LTU", "lithuania": "LTU",
    "vereinigtes königreich": "GBR", "united kingdom": "GBR",
    "türkei": "TUR", "turkey": "TUR",
    "serbien": "SRB", "serbia": "SRB",
    "ukraine": "UKR",
    "russland": "RUS", "russia": "RUS",
    "belarus": "BLR",
    "usa": "USA", "vereinigte staaten": "USA", "united states": "USA",
    "china": "CHN",
    "indien": "IND", "india": "IND",
    "brasilien": "BRA", "brazil": "BRA",
    "japan": "JPN",
    "südkorea": "KOR", "south korea": "KOR",
    "iran": "IRN",
}


async def fetch_cpi(client: httpx.AsyncClient | None = None) -> dict:
    """Download and parse the CPI CSV from OWID."""
    global _cpi_cache, _cpi_cache_time

    now = time.time()
    if _cpi_cache is not None and (now - _cpi_cache_time) < CPI_CACHE_TTL:
        return _cpi_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        close_client = True

    data: dict = {}

    try:
        resp = await client.get(CPI_CSV_URL)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        for row in reader:
            code = (row.get("Code") or "").strip()
            entity = (row.get("Entity") or "").strip()
            year_raw = (row.get("Year") or "").strip()
            score_raw = (row.get("Corruption Perceptions Index") or "").strip()
            if not code or not year_raw or not score_raw:
                continue
            try:
                year = int(year_raw)
                score = float(score_raw)
            except ValueError:
                continue
            data.setdefault(code, {})[year] = {"score": score, "entity": entity}

        _cpi_cache = data
        _cpi_cache_time = now
        total_points = sum(len(y) for y in data.values())
        logger.info(f"CPI: {len(data)} countries, {total_points} country-years cached")
        return data
    finally:
        if close_client:
            await client.aclose()


def _find_countries(analysis: dict, max_n: int = 3) -> list[str]:
    """Extract ISO3 country codes from claim (NER-prioritized)."""
    ner_countries = analysis.get("ner_entities", {}).get("countries", [])
    claim = analysis.get("claim", "")
    search_terms = ner_countries + [claim]

    found: list[str] = []
    seen: set[str] = set()
    for term in search_terms:
        term_lower = term.lower()
        for name, code in COUNTRY_MAP.items():
            if name in term_lower and code not in seen:
                found.append(code)
                seen.add(code)
                if len(found) >= max_n:
                    return found
    return found


def _claim_mentions_cpi(claim: str) -> bool:
    claim_lower = claim.lower()
    return any(kw in claim_lower for kw in CPI_KEYWORDS)


def _claim_wants_eu_cohort(claim: str) -> bool:
    """Detect comparative references to the EU as a whole (avg, ranking)."""
    cl = claim.lower()
    return any(t in cl for t in EU_COMPARISON_TRIGGERS)


# Reverse-Mapping ISO3 → erste (deutsche) Bezeichnung aus COUNTRY_MAP.
# Wird in der EU-Kohorten-Description verwendet, damit der semantische
# Re-Ranker bei deutschsprachigen Claims ("Rumänien schneidet …") einen
# Wort-für-Wort-Match gegen den englischen OWID-Entity-Namen ("Romania")
# bekommt.  Ohne diesen Boost rutschte der Kohorten-Eintrag für "Rumänien"
# unter den Relevance-Threshold 0.25 und verschwand aus den Quellen.
_ISO3_TO_DE_NAME: dict[str, str] = {}
for _name, _code in COUNTRY_MAP.items():
    if _code not in _ISO3_TO_DE_NAME:
        # .title() capitalizes each word ("vereinigtes königreich" →
        # "Vereinigtes Königreich"), which is correct for all DE country
        # names in this map.  Acronyms like USA are not in EU-27 and do
        # not reach this path in cohort output.
        _ISO3_TO_DE_NAME[_code] = _name.title()


def _display_country(country_name: str, code: str) -> str:
    """Return "English (Deutsch)" if DE and EN differ, else the English name."""
    de = _ISO3_TO_DE_NAME.get(code)
    if not de or de.lower() == country_name.lower():
        return country_name
    return f"{country_name} ({de})"


def _compute_eu_cohort(data: dict) -> dict | None:
    """Compute EU-27 CPI mean, median and per-country ranking for the most
    recent year with good coverage (>=25 of 27 members).

    Returns ``None`` if the cache has too few EU-27 data points.
    """
    # Year coverage histogram across EU-27
    year_coverage: dict[int, int] = {}
    for code in EU27_MEMBERS:
        for y in data.get(code, {}).keys():
            year_coverage[y] = year_coverage.get(y, 0) + 1

    if not year_coverage:
        return None

    # Prefer the latest year with >=25 of 27 members; else the year with
    # the best coverage (fall back to latest with any data).
    eligible = [y for y, cov in year_coverage.items() if cov >= 25]
    if eligible:
        eu_year = max(eligible)
    else:
        eu_year = max(year_coverage, key=lambda y: (year_coverage[y], y))

    pairs: list[tuple[str, float]] = []
    for code in EU27_MEMBERS:
        cdata = data.get(code, {})
        entry = cdata.get(eu_year)
        if entry is None:
            continue
        score = entry.get("score")
        if score is None:
            continue
        pairs.append((code, float(score)))

    if len(pairs) < 10:
        return None

    scores = [s for _, s in pairs]
    mean = sum(scores) / len(scores)
    ordered = sorted(scores)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 0:
        median = (ordered[mid - 1] + ordered[mid]) / 2
    else:
        median = ordered[mid]

    # Rank descending (higher CPI = better = rank 1)
    by_rank = sorted(pairs, key=lambda p: p[1], reverse=True)
    rank_map = {code: i + 1 for i, (code, _) in enumerate(by_rank)}
    score_map = {code: score for code, score in pairs}

    return {
        "year": eu_year,
        "mean": mean,
        "median": median,
        "n": len(pairs),
        "rank_map": rank_map,
        "score_map": score_map,
    }


async def search_transparency(analysis: dict) -> dict:
    """Search CPI cache for corruption scores."""
    if not _claim_mentions_cpi(analysis.get("claim", "")):
        return {"source": "Transparency International", "type": "official_data", "results": []}

    data = await fetch_cpi()
    if not data:
        return {"source": "Transparency International", "type": "official_data", "results": []}

    countries = _find_countries(analysis)
    if not countries:
        countries = ["AUT", "DEU"]  # Default-Kontext

    results: list[dict] = []
    for code in countries:
        country_data = data.get(code)
        if not country_data:
            continue
        latest_year = max(country_data.keys())
        entry = country_data[latest_year]
        score = entry.get("score")
        entity = entry.get("entity", code)

        # Trend über 10 Jahre (oder wie verfügbar)
        years = sorted(country_data.keys())
        trend_note = ""
        if len(years) >= 10:
            ten_years_ago_year = latest_year - 10
            if ten_years_ago_year in country_data:
                delta = score - country_data[ten_years_ago_year]["score"]
                trend_arrow = "↑" if delta > 1 else ("↓" if delta < -1 else "→")
                trend_note = f" | Trend 10J: {trend_arrow} ({delta:+.1f} Punkte)"

        # Einordnung in Cluster (nur zur Info, nicht als Urteil)
        if score >= 80:
            cluster = "sehr gering wahrgenommene Korruption"
        elif score >= 60:
            cluster = "gering wahrgenommene Korruption"
        elif score >= 40:
            cluster = "moderat wahrgenommene Korruption"
        elif score >= 20:
            cluster = "hoch wahrgenommene Korruption"
        else:
            cluster = "sehr hoch wahrgenommene Korruption"

        results.append({
            "indicator_name": f"CPI {entity} ({latest_year}): {score:.0f}/100 — {cluster}{trend_note}",
            "indicator": "cpi_score",
            "country": code,
            "country_name": entity,
            "year": str(latest_year),
            "value": score,
            "display_value": f"{score:.0f}/100",
            "url": "https://www.transparency.org/en/cpi",
        })

    # EU-27-Kohorte anhängen, wenn der Claim auf den EU-Schnitt referenziert.
    # WICHTIG: Wir prüfen den *Original-Claim* (wie vom User eingegeben) und
    # nicht nur analysis["claim"], weil der LLM-Analyzer komparative Marker
    # wie "als die EU" / "als in der EU" beim Normalisieren entfernen kann.
    # Beobachtet beim Claim "Rumänien schneidet beim Korruptionsindex
    # schlechter als die EU ab": der normalisierte Claim verlor das "als die
    # EU" → Trigger feuerte nicht → EU-Kohorte fehlte → LLM halluzinierte.
    trigger_claim = analysis.get("original_claim") or analysis.get("claim", "")
    if results and _claim_wants_eu_cohort(trigger_claim):
        cohort = _compute_eu_cohort(data)
        if cohort:
            # Per-country Einordnung (Rang für EU-Mitglieder, nur Delta für Nicht-Mitglieder).
            # Wir verwenden den vollen Ländernamen (statt ISO3), damit der semantische
            # Re-Ranker die Zeile sauber mit dem Claim verknüpft.
            country_notes: list[str] = []
            sentence_parts: list[str] = []
            for code in countries:
                target_score = cohort["score_map"].get(code)
                if target_score is None:
                    # Ziel-Land hat keine CPI-Daten für das EU-Kohortenjahr → latest year
                    target_entry = data.get(code, {}).get(cohort["year"])
                    if target_entry is not None:
                        target_score = target_entry.get("score")
                if target_score is None:
                    continue
                # Vollständigen Ländernamen aus dem Cache holen (sonst ISO3 als Fallback).
                # Für DE-Claims zusätzlich den deutschen Namen anhängen
                # ("Romania (Rumänien)"), damit der Re-Ranker topic-match
                # verlässlicher sieht (siehe _display_country).
                country_entry = data.get(code, {})
                country_name = code
                if country_entry:
                    latest = max(country_entry.keys())
                    country_name = country_entry[latest].get("entity", code)
                display_name = _display_country(country_name, code)

                delta = target_score - cohort["mean"]
                if code in EU27_MEMBERS and code in cohort["rank_map"]:
                    if delta > 0.5:
                        pos = "über"
                    elif delta < -0.5:
                        pos = "unter"
                    else:
                        pos = "auf"
                    country_notes.append(
                        f"{display_name}: Rang {cohort['rank_map'][code]}/{cohort['n']} "
                        f"({target_score:.0f}/100, {pos} EU-Ø, Δ {delta:+.1f})"
                    )
                    sentence_parts.append(
                        f"{display_name} liegt mit einem CPI-Wert von {target_score:.0f}/100 "
                        f"{pos} dem EU-Durchschnitt von {cohort['mean']:.1f}/100 "
                        f"(Rang {cohort['rank_map'][code]} von {cohort['n']} Mitgliedstaaten)."
                    )
                else:
                    country_notes.append(
                        f"{display_name}: {target_score:.0f}/100 "
                        f"(EU-Ø {cohort['mean']:.1f}, Δ {delta:+.1f})"
                    )
                    pos = "über" if delta > 0 else "unter"
                    sentence_parts.append(
                        f"{display_name} (Nicht-EU-Land) liegt mit einem CPI-Wert von "
                        f"{target_score:.0f}/100 {pos} dem EU-Durchschnitt von "
                        f"{cohort['mean']:.1f}/100."
                    )

            rank_part = " | " + " | ".join(country_notes) if country_notes else ""
            # Natürlichsprachliche Description boosted die Cosine-Similarity im
            # Re-Ranker (sonst fällt die Zahlenzeile unter den 0.25-Threshold).
            description = (
                "Korruptionswahrnehmungs-Vergleich EU-27 (Transparency International CPI). "
                f"Durchschnittswert {cohort['year']}: {cohort['mean']:.1f} von 100 Punkten "
                f"(Median {cohort['median']:.0f}, n={cohort['n']} Mitgliedstaaten). "
                + " ".join(sentence_parts)
                + " Ranking nach CPI-Score absteigend (höherer Wert = weniger wahrgenommene Korruption)."
            )
            results.append({
                "indicator_name": (
                    f"EU-27 CPI {cohort['year']} — Korruptionswahrnehmung Durchschnitt: "
                    f"Ø {cohort['mean']:.1f}/100 (Median {cohort['median']:.0f}, "
                    f"n={cohort['n']}){rank_part}"
                ),
                "indicator": "cpi_eu_cohort",
                "country": "EU-27",
                "country_name": "European Union (27 members)",
                "year": str(cohort["year"]),
                "value": round(cohort["mean"], 1),
                "display_value": f"Ø {cohort['mean']:.1f}/100",
                "description": description,
                "url": "https://www.transparency.org/en/cpi",
            })

    if results:
        results.append({
            "indicator_name": "WICHTIGER KONTEXT: CPI misst wahrgenommene, nicht gemessene Korruption",
            "indicator": "context",
            "country": "",
            "country_name": "",
            "year": "",
            "value": "",
            "display_value": "",
            "url": "https://www.transparency.org/en/cpi/methodology",
            "description": (
                "Der Corruption Perceptions Index (CPI) von Transparency International aggregiert "
                "13 verschiedene Umfragen und Expertenbewertungen zur Wahrnehmung von Korruption im "
                "öffentlichen Sektor. Skala: 0 = höchst korrupt, 100 = keine wahrgenommene Korruption. "
                "Einschränkungen: "
                "(1) Wahrnehmung ≠ Realität — der Index misst, wie Expert:innen und Unternehmen "
                "Korruption einschätzen, nicht tatsächliche Korruptionsfälle. "
                "(2) Nur öffentlicher Sektor — private Korruption und Korruption zwischen Unternehmen "
                "sind nicht erfasst. "
                "(3) Jahresvergleiche mit Vorsicht — die Methodik seit 2012 ist stabil, aber kleine "
                "Punktänderungen (±2) liegen im Unschärfebereich. "
                "(4) Mindestzahl Quellen — pro Land müssen mindestens 3 unabhängige Quellen "
                "vorliegen; für kleinere Länder ist die Datenbasis dünner."
            ),
        })

    logger.info(f"CPI: {len(results) - (1 if results else 0)} country results, countries={countries}")
    return {"source": "Transparency International", "type": "official_data", "results": results}
