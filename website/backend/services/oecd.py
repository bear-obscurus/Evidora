"""OECD data service: PISA scores (static CSV) + SDMX live API (economy, labour, gender)."""

import csv
import io
import logging
from pathlib import Path

import httpx

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# PISA data (static CSV, ~10 KB, loaded once at startup)
# ---------------------------------------------------------------------------
_pisa_data: list[dict] | None = None

DATA_DIR = Path(__file__).parent.parent / "data"

PISA_SUBJECT_LABELS = {
    "math": "Mathematics",
    "reading": "Reading",
    "science": "Science",
}


def _load_pisa() -> list[dict]:
    global _pisa_data
    if _pisa_data is not None:
        return _pisa_data
    csv_path = DATA_DIR / "pisa_2022.csv"
    try:
        with open(csv_path, encoding="utf-8") as f:
            _pisa_data = list(csv.DictReader(f))
        logger.info(f"Loaded {len(_pisa_data)} PISA records from {csv_path}")
    except FileNotFoundError:
        logger.error(f"PISA CSV not found: {csv_path}")
        _pisa_data = []
    return _pisa_data


# Country name variants → ISO-3 codes used in PISA CSV
COUNTRY_ALIASES = {
    "österreich": "AUT", "austria": "AUT",
    "deutschland": "DEU", "germany": "DEU",
    "frankreich": "FRA", "france": "FRA",
    "italien": "ITA", "italy": "ITA",
    "spanien": "ESP", "spain": "ESP",
    "griechenland": "GRC", "greece": "GRC",
    "niederlande": "NLD", "netherlands": "NLD",
    "belgien": "BEL", "belgium": "BEL",
    "portugal": "PRT",
    "schweden": "SWE", "sweden": "SWE",
    "dänemark": "DNK", "denmark": "DNK",
    "finnland": "FIN", "finland": "FIN",
    "irland": "IRL", "ireland": "IRL",
    "polen": "POL", "poland": "POL",
    "tschechien": "CZE", "czechia": "CZE",
    "ungarn": "HUN", "hungary": "HUN",
    "rumänien": "ROU", "romania": "ROU",
    "bulgarien": "BGR", "bulgaria": "BGR",
    "kroatien": "HRV", "croatia": "HRV",
    "slowakei": "SVK", "slovakia": "SVK",
    "slowenien": "SVN", "slovenia": "SVN",
    "estland": "EST", "estonia": "EST",
    "lettland": "LVA", "latvia": "LVA",
    "litauen": "LTU", "lithuania": "LTU",
    "luxemburg": "LUX", "luxembourg": "LUX",
    "malta": "MLT",
    "zypern": "CYP", "cyprus": "CYP",
    "norwegen": "NOR", "norway": "NOR",
    "schweiz": "CHE", "switzerland": "CHE",
    "türkei": "TUR", "türkiye": "TUR",
    "vereinigtes königreich": "GBR", "united kingdom": "GBR", "uk": "GBR",
    "usa": "USA", "vereinigte staaten": "USA", "united states": "USA",
    "japan": "JPN",
    "singapur": "SGP", "singapore": "SGP",
    "südkorea": "KOR", "south korea": "KOR",
    "eu": "OECD", "oecd": "OECD",
}

# Subject detection keywords
SUBJECT_KEYWORDS = {
    "math": ["mathe", "math", "rechnen", "arithmetic", "numeracy"],
    "reading": ["lesen", "reading", "read", "literacy", "lesekompetenz"],
    "science": ["naturwissenschaft", "science", "physik", "chemie", "biologie"],
}

# ---------------------------------------------------------------------------
# OECD SDMX live API
# ---------------------------------------------------------------------------
SDMX_BASE = "https://sdmx.oecd.org/public/rest/data"
SDMX_TIMEOUT = 15.0

# Dataflows we support
OECD_DATASETS = {
    "gender_wage_gap": {
        "flow": "OECD.ELS.SAE,DSD_EARNINGS@GENDER_WAGE_GAP,",
        "label": "Gender Wage Gap",
        "keywords": ["lohnunterschied", "lohnlücke", "gehalt", "verdien", "gender pay gap",
                      "wage gap", "einkommen", "gehaltsunterschied", "equal pay",
                      "lohngleichheit", "pay gap", "gender gap gehalt"],
    },
    "employment_gender": {
        "flow": "OECD.ELS.SAE,DSD_LFS_EMP@DF_LFS_EMPSTAT_GENDER,",
        "label": "Employment by Gender",
        "keywords": ["beschäftigung", "erwerbstätig", "employment", "arbeitsmarkt",
                      "frauenanteil", "erwerbsquote", "labor market"],
    },
    "education_gender": {
        "flow": "OECD.EDU.IMEP,DSD_EAG_UOE_NON_FIN_STUD@DF_UOE_NF_SHARE_GENDER,",
        "label": "Education by Gender",
        "keywords": ["studium", "universität", "hochschule", "studieren", "absolventen",
                      "university", "graduates", "tertiary", "higher education",
                      "mint", "stem", "bildung frauen"],
    },
    "unemployment": {
        "flow": "OECD.SDD.TPS,DSD_LFS@DF_IALFS_UNE_M,",
        "label": "Arbeitslosenquote (OECD)",
        "label_en": "Unemployment Rate (OECD)",
        "keywords": ["arbeitslosigkeit", "arbeitslose", "arbeitslosenquote",
                      "jugendarbeitslosigkeit", "unemployment", "jobless",
                      "erwerbslosigkeit", "erwerbslose"],
    },
    "cpi": {
        "flow": "OECD.SDD.TPS,DSD_PRICES@DF_PRICES_ALL,",
        "label": "Verbraucherpreisindex (OECD)",
        "label_en": "Consumer Price Index (OECD)",
        "keywords": ["verbraucherpreis", "consumer price", "cpi", "teuerungsrate",
                      "preissteigerung", "preisindex", "lebenshaltungskosten",
                      "cost of living"],
    },
    "avoidable_mortality": {
        "flow": "OECD.ELS.HD,DSD_HEALTH_STAT@DF_AM,",
        "label": "Vermeidbare Sterblichkeit (OECD)",
        "label_en": "Avoidable Mortality (OECD)",
        "keywords": ["sterblichkeit", "mortality", "vermeidbare tode",
                      "avoidable death", "lebenserwartung oecd", "gesundheitssystem vergleich",
                      "health system comparison"],
    },
}


def _find_country_code(analysis: dict) -> str | None:
    """Find first country code from analysis entities."""
    codes = _find_country_codes(analysis)
    return codes[0] if codes else None


def _find_country_codes(analysis: dict) -> list[str]:
    """Find all country codes mentioned in the claim (for comparison claims)."""
    entities = analysis.get("entities", [])
    claim = analysis.get("claim", "")
    text = " ".join(entities + [claim]).lower()
    found = []
    seen = set()
    # Sort by length descending so "vereinigte staaten" matches before "vereinig"
    sorted_names = sorted(COUNTRY_ALIASES.keys(), key=len, reverse=True)
    for name in sorted_names:
        if name in text:
            code = COUNTRY_ALIASES[name]
            if code not in seen:
                found.append(code)
                seen.add(code)
    return found


def _detect_subject(claim: str) -> str | None:
    """Detect PISA subject from claim text."""
    claim_lower = claim.lower()
    for subject, keywords in SUBJECT_KEYWORDS.items():
        if any(kw in claim_lower for kw in keywords):
            return subject
    return None


def _is_pisa_claim(claim: str) -> bool:
    """Check if claim relates to education performance/scores."""
    keywords = [
        "pisa", "schüler", "schülerin", "student", "schulleistung",
        "mathe", "math", "lesen", "reading", "naturwissenschaft", "science",
        "bildung", "education", "schule", "school",
        "besser in", "schlechter in", "leistung",
        "frauen sind schlechter", "männer sind besser",
        "mädchen", "jungen", "boys", "girls",
        "bildungssystem", "education system",
    ]
    claim_lower = claim.lower()
    return any(kw in claim_lower for kw in keywords)


def _is_gender_claim(claim: str) -> bool:
    """Check if claim involves gender comparison."""
    keywords = [
        "frauen", "männer", "geschlecht", "gender", "weiblich", "männlich",
        "mädchen", "jungen", "women", "men", "female", "male",
        "boys", "girls", "sex difference",
    ]
    claim_lower = claim.lower()
    return any(kw in claim_lower for kw in keywords)


def _is_superlative_education_claim(claim: str) -> bool:
    """Check if claim makes a superlative/ranking statement about education systems."""
    keywords = [
        "beste", "bestes", "best", "schlechteste", "worst",
        "führend", "top", "ranking", "nummer eins", "number one",
        "weltbeste", "spitzenreiter", "vorne", "hinten",
    ]
    claim_lower = claim.lower()
    return any(kw in claim_lower for kw in keywords)


def _search_pisa(claim: str, analysis: dict) -> list[dict]:
    """Search PISA data for education performance claims."""
    data = _load_pisa()
    if not data:
        return []

    country_codes = _find_country_codes(analysis)
    country_code = country_codes[0] if country_codes else None
    subject = _detect_subject(claim)
    is_gender = _is_gender_claim(claim)
    results = []

    if is_gender:
        # Gender comparison: show boy vs girl scores
        # For comparison claims, check all mentioned countries
        codes_to_check = country_codes if country_codes else ["OECD"]
        subjects_to_check = [subject] if subject else ["math", "reading", "science"]
        for code in codes_to_check:
            for subj in subjects_to_check:
                rows = [r for r in data if r["country_code"] == code and r["subject"] == subj]

                boy_row = next((r for r in rows if r["gender"] == "boy"), None)
                girl_row = next((r for r in rows if r["gender"] == "girl"), None)
                total_row = next((r for r in rows if r["gender"] == "total"), None)

                if boy_row and girl_row:
                    boy_score = int(boy_row["score"])
                    girl_score = int(girl_row["score"])
                    gap = boy_score - girl_score
                    country_name = boy_row["country"]
                    subj_label = PISA_SUBJECT_LABELS.get(subj, subj)

                    if gap > 0:
                        gap_text = f"Jungen {gap} Punkte höher"
                    elif gap < 0:
                        gap_text = f"Mädchen {abs(gap)} Punkte höher"
                    else:
                        gap_text = "Kein Unterschied"

                    results.append({
                        "title": f"PISA 2022 {subj_label} — {country_name}",
                        "indicator": f"Gender Gap in {subj_label}",
                        "country": country_name,
                        "value": f"Jungen: {boy_score}, Mädchen: {girl_score} (Differenz: {gap:+d})",
                        "description": f"{gap_text}. Gesamtdurchschnitt: {total_row['score'] if total_row else 'N/A'}",
                        "year": "2022",
                        "source": "OECD PISA 2022",
                        "url": "https://www.oecd.org/en/about/programmes/pisa/pisa-2022-results.html",
                        "dataset_id": "pisa_2022",
                    })

        # Also add OECD average for context if we showed specific countries
        if country_codes and not any(c == "OECD" for c in country_codes):
            subj_for_avg = subject or "math"
            oecd_boy = next((r for r in data if r["country_code"] == "OECD" and r["subject"] == subj_for_avg and r["gender"] == "boy"), None)
            oecd_girl = next((r for r in data if r["country_code"] == "OECD" and r["subject"] == subj_for_avg and r["gender"] == "girl"), None)
            if oecd_boy and oecd_girl:
                gap = int(oecd_boy["score"]) - int(oecd_girl["score"])
                results.append({
                    "title": f"PISA 2022 {PISA_SUBJECT_LABELS.get(subj_for_avg, subj_for_avg)} — OECD-Durchschnitt",
                    "indicator": "Gender Gap (OECD average)",
                    "country": "OECD average",
                    "value": f"Jungen: {oecd_boy['score']}, Mädchen: {oecd_girl['score']} (Differenz: {gap:+d})",
                    "description": f"OECD-Durchschnitt zum Vergleich",
                    "year": "2022",
                    "source": "OECD PISA 2022",
                    "url": "https://www.oecd.org/en/about/programmes/pisa/pisa-2022-results.html",
                    "dataset_id": "pisa_2022",
                })
    else:
        # Non-gender: show country total scores
        # For comparison claims, check all mentioned countries
        codes_to_check = country_codes if country_codes else [None]
        subjects_to_check = [subject] if subject else ["math", "reading", "science"]
        for code in codes_to_check:
            for subj in subjects_to_check:
                if code:
                    rows = [r for r in data if r["country_code"] == code and r["subject"] == subj and r["gender"] == "total"]
                else:
                    rows = [r for r in data if r["country_code"] == "OECD" and r["subject"] == subj and r["gender"] == "total"]

                for row in rows:
                    results.append({
                        "title": f"PISA 2022 {PISA_SUBJECT_LABELS.get(subj, subj)} — {row['country']}",
                        "indicator": f"PISA {PISA_SUBJECT_LABELS.get(subj, subj)} Score",
                        "country": row["country"],
                        "value": row["score"],
                        "year": "2022",
                        "source": "OECD PISA 2022",
                        "url": "https://www.oecd.org/en/about/programmes/pisa/pisa-2022-results.html",
                        "dataset_id": "pisa_2022",
                    })

    # --- Add ranking for superlative claims ("bestes Bildungssystem") ---
    if _is_superlative_education_claim(claim) and results:
        ranking_subj = subject or "math"
        total_rows = sorted(
            [r for r in data if r["subject"] == ranking_subj and r["gender"] == "total"
             and r["country_code"] != "OECD"],
            key=lambda r: int(r["score"]),
            reverse=True,
        )
        if total_rows:
            ranking_text = " > ".join(
                f"{r['country']} ({r['score']})"
                for r in total_rows[:15]
            )
            results.append({
                "title": f"PISA 2022 Ranking: {PISA_SUBJECT_LABELS.get(ranking_subj, ranking_subj)} (Top 15)",
                "indicator": "PISA Score Ranking",
                "description": ranking_text,
                "year": "2022",
                "source": "OECD PISA 2022",
                "url": "https://www.oecd.org/en/about/programmes/pisa/pisa-2022-results.html",
                "dataset_id": "pisa_2022",
            })

    # --- Multi-dimensional context caveat ---
    if results:
        results.append({
            "title": "WICHTIGER KONTEXT: 'Bestes Bildungssystem' ist mehrdimensional",
            "indicator": "Methodische Einordnung",
            "description": (
                "PISA misst ausschließlich die Kompetenzen 15-Jähriger in Lesen, Mathematik und "
                "Naturwissenschaften. Ein umfassender Vergleich von Bildungssystemen erfordert "
                "zusätzliche Dimensionen, die PISA NICHT erfasst: "
                "(1) Berufsausbildung — duale Systeme (z.B. Österreich, Deutschland, Schweiz) "
                "produzieren hochqualifizierte Fachkräfte, die in PISA nicht abgebildet werden. "
                "(2) Inklusion & Chancengleichheit — Anteil sozial benachteiligter Schüler, die "
                "Basiskompetenzen erreichen; Einfluss des sozioökonomischen Hintergrunds. "
                "(3) Dropout-Rate — Anteil der Jugendlichen ohne Abschluss (EU-Durchschnitt: ~10 %). "
                "(4) Lehrerqualität & Arbeitsbedingungen — Gehalt, Ausbildungsdauer, Betreuungsverhältnis. "
                "(5) Universitäts-Rankings — Forschungsqualität (z.B. Shanghai, THE, QS). "
                "(6) Lebenslanges Lernen — Weiterbildungsbeteiligung Erwachsener. "
                "(7) Wohlbefinden — Schulstress, Lernfreude, Mobbing-Raten (PISA erhebt teils, "
                "aber dies fließt nicht in die Score-Rankings ein). "
                "Hohe PISA-Scores korrelieren nicht automatisch mit einem 'guten' Bildungssystem."
            ),
            "year": "2022",
            "source": "OECD PISA 2022, Bildung auf einen Blick (EAG)",
            "url": "https://www.oecd.org/en/about/programmes/pisa/pisa-2022-results.html",
            "dataset_id": "pisa_context",
        })

    return results


async def _search_sdmx(claim: str, analysis: dict) -> list[dict]:
    """Search OECD SDMX API for live data.

    Uses the 'all' key to avoid hardcoding dimension counts per dataset.
    Filters by REF_AREA in the response instead.
    """
    claim_lower = claim.lower()
    results = []

    # Find matching datasets
    matching_datasets = []
    for ds_id, ds_info in OECD_DATASETS.items():
        if any(kw in claim_lower for kw in ds_info["keywords"]):
            matching_datasets.append((ds_id, ds_info))

    if not matching_datasets:
        return []

    # Find countries (support comparison claims like "AT vs ES")
    country_codes = _find_country_codes(analysis)
    geo_list = country_codes if country_codes else ["OECD"]

    # Max 2 SDMX queries per request (OECD rate-limits aggressively)
    # Use c[REF_AREA] filter to get only requested countries (small response)
    geo_filter = "+".join(geo_list)

    for ds_id, ds_info in matching_datasets[:2]:
        try:
            flow = ds_info["flow"]
            url = (
                f"{SDMX_BASE}/{flow}/all"
                f"?lastNObservations=1"
                f"&dimensionAtObservation=AllDimensions"
                f"&c[REF_AREA]={geo_filter}"
            )

            async with httpx.AsyncClient(timeout=SDMX_TIMEOUT) as client:
                resp = await client.get(url, headers={
                    "Accept": "application/vnd.sdmx.data+json",
                })

            if resp.status_code == 429:
                logger.warning(f"OECD SDMX rate limited (429) for {ds_id}")
                continue
            if resp.status_code != 200:
                logger.warning(f"OECD SDMX {ds_id} returned {resp.status_code}")
                continue

            data = resp.json()
            ds = data.get("data", {}).get("dataSets", [{}])[0]
            obs = ds.get("observations", {})
            struct = data.get("data", {}).get("structures", [{}])[0]
            dims = struct.get("dimensions", {}).get("observation", [])

            # Parse dimension labels
            dim_vals = []
            for d in dims:
                vals = {}
                for i, v in enumerate(d.get("values", [])):
                    name = v.get("name", "")
                    if isinstance(name, dict):
                        name = name.get("en", str(name))
                    vals[str(i)] = name or v.get("id", "")
                dim_vals.append({"id": d.get("id", ""), "values": vals})

            # Extract observations (already filtered by country via API)
            count = 0
            for key_str, val in obs.items():
                if val[0] is None:
                    continue

                indices = key_str.split(":")
                labels = {}
                for i, idx in enumerate(indices):
                    if i < len(dim_vals):
                        labels[dim_vals[i]["id"]] = dim_vals[i]["values"].get(idx, idx)

                country_label = labels.get("REF_AREA", geo_list[0])
                measure_label = labels.get("MEASURE", labels.get("INDICATOR", ds_info["label"]))
                stat_label = labels.get("STATISTICS", labels.get("STAT_METHOD", ""))

                value = val[0]
                if isinstance(value, float):
                    value = round(value, 2)

                title = f"{ds_info['label']} — {country_label}"
                if stat_label:
                    title += f" ({stat_label})"

                results.append({
                    "title": title,
                    "indicator": measure_label,
                    "country": country_label,
                    "value": value,
                    "year": labels.get("TIME_PERIOD", "latest"),
                    "source": "OECD",
                    "url": "https://data-explorer.oecd.org/",
                    "dataset_id": ds_id,
                })

                count += 1
                if count >= 5:
                    break

        except Exception as e:
            logger.error(f"OECD SDMX error for {ds_id}: {e}")

    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _has_sdmx_keywords(claim: str) -> bool:
    """Check if claim matches any SDMX dataset keywords."""
    claim_lower = claim.lower()
    return any(
        any(kw in claim_lower for kw in ds["keywords"])
        for ds in OECD_DATASETS.values()
    )


async def search_oecd(analysis: dict) -> dict:
    """Search OECD data sources (PISA + SDMX API)."""
    claim = analysis.get("claim", "")
    results = []

    # 1. PISA data (static CSV)
    if _is_pisa_claim(claim):
        pisa_results = _search_pisa(claim, analysis)
        results.extend(pisa_results)

    # 2. OECD SDMX live API (economy, labour, gender, health)
    if _is_gender_claim(claim) or _has_sdmx_keywords(claim):
        sdmx_results = await _search_sdmx(claim, analysis)
        results.extend(sdmx_results)

    return {
        "source": "OECD",
        "results": results,
    }
