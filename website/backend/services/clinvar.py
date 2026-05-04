"""NIH NCBI ClinVar — Variant-Level Clinical Significance, REST API.

Datenbank für genetische Varianten und ihre klinische Bedeutung
(pathogen / benign / VUS). Teil von NCBI E-Utilities, ohne API-Key
nutzbar (max 3 req/sec ohne Key, 10 req/sec mit Key).

Use-Cases für Faktencheck:
- 'Glutenintoleranz/Zöliakie ist genetisch' — HLA-DQ2/DQ8 ClinVar
- 'Laktoseintoleranz ist Mutation' — LCT/MCM6 Varianten
- 'Histaminintoleranz ist DAO-Mangel' — DAO/AOC1 Varianten
- 'Fragile-X-Syndrom' — FMR1 Varianten
- 'Erblicher Brustkrebs' — BRCA1/BRCA2 Pathogenität
- '23andMe-Test' Genauigkeit-Claims

Trigger (gated, da ClinVar sehr spezialisiert):
- analysis.pubmed_queries vorhanden UND
- Claim enthält Genetik-Stichworte ODER Gen-Name-Pattern

API-Endpunkte:
- ESearch: https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=clinvar
- ESummary: https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=clinvar
"""

import logging
import re

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

# Stichworte, die eine ClinVar-Suche rechtfertigen. Großzügig in Englisch
# + Deutsch, weil viele Claims gemischt-sprachig sind.
_GENETICS_KEYWORDS = (
    "genetisch", "genetik", "erblich", "vererb", "vererbt", "vererbung",
    "mutation", "mutiert", "chromosom", "dna", "rna", "genom",
    "intoleranz", "unverträglichkeit", "zöliakie", "laktose", "gluten",
    "histamin", "fruktose", "phenylketon",
    "genetic", "hereditary", "inherited", "chromosome",
    "intolerance", "celiac", "lactose", "histamine", "fructose",
    "fragile-x", "fragile x", "huntington", "marfan", "brca",
    "alpha-1-antitrypsin", "phenylketonuria", "tay-sachs",
    "23andme", "myheritage", "gentest", "gen-test", "genetic test",
    "abstammungs", "ancestry test", "dna-test", "dna test",
)

# Generische Gen-Name-Pattern (4+ Großbuchstaben, optional Zahl).
# Beispiele: BRCA1, HLA-DQ2, MCM6, FMR1, GALT, DAO
_GENE_NAME_RE = re.compile(r"\b[A-Z]{2,}[0-9]?[A-Z0-9-]*\b")


def _claim_indicates_genetics(claim: str) -> bool:
    if not claim:
        return False
    cl = claim.lower()
    if any(kw in cl for kw in _GENETICS_KEYWORDS):
        return True
    # Spezifische Gen-Name-Pattern (mind. 1 Match mit Länge >=4)
    matches = _GENE_NAME_RE.findall(claim)
    return any(len(m) >= 4 for m in matches)


async def search_clinvar(analysis: dict) -> dict:
    """Search ClinVar for variants relevant to the claim.

    Strategy:
    1. Pre-filter: claim must mention genetics-related terms OR gene-name pattern
    2. ESearch with first pubmed_query (English) — top 5 IDs
    3. ESummary for all 5 IDs in one call
    4. Parse + return germline_classification + trait + gene
    """
    empty = {"source": "NIH ClinVar", "type": "genetic_variant", "results": []}

    claim = (analysis or {}).get("original_claim") or (analysis or {}).get("claim", "") or ""
    queries = (analysis or {}).get("pubmed_queries", []) or []

    if not queries or not _claim_indicates_genetics(claim):
        return empty

    # ClinVar All-Fields-Search ANDed strict alle Wörter — daher kurze
    # Queries bevorzugen. Try queries in order of increasing length;
    # use first that returns IDs.
    query_candidates = sorted(queries[:3], key=len)
    id_list: list[str] = []
    used_query = ""

    try:
        async with polite_client(timeout=12.0) as client:
            for q in query_candidates:
                search_params = {
                    "db": "clinvar",
                    "term": q,
                    "retmax": "5",
                    "retmode": "json",
                }
                resp = await client.get(ESEARCH_URL, params=search_params)
                resp.raise_for_status()
                search_data = resp.json()
                id_list = (
                    search_data.get("esearchresult", {}).get("idlist", []) or []
                )
                if id_list:
                    used_query = q
                    break

            if not id_list:
                logger.info(
                    f"ClinVar: 0 IDs for {len(query_candidates)} queries"
                )
                return empty

            # Step 2: ESummary — fetch details for all IDs in one call
            summary_params = {
                "db": "clinvar",
                "id": ",".join(id_list),
                "retmode": "json",
            }
            resp2 = await client.get(ESUMMARY_URL, params=summary_params)
            resp2.raise_for_status()
            summary_data = resp2.json()
    except Exception as e:
        logger.warning(f"ClinVar query failed: {e}")
        return empty

    result_block = summary_data.get("result", {}) or {}
    uids = result_block.get("uids", []) or []
    if not uids:
        return empty

    results: list[dict] = []
    for uid in uids:
        item = result_block.get(uid) or {}
        if not isinstance(item, dict):
            continue

        title = item.get("title", "") or ""
        # germline_classification: e.g. {"description": "Likely pathogenic", "trait_set": [...]}
        germline = item.get("germline_classification", {}) or {}
        clin_sig = germline.get("description", "") or "—"
        trait_set = germline.get("trait_set", []) or []
        trait_name = trait_set[0].get("trait_name", "") if trait_set else ""

        genes = item.get("genes", []) or []
        gene_symbol = genes[0].get("symbol", "") if genes else ""

        url = f"https://www.ncbi.nlm.nih.gov/clinvar/variation/{uid}/"

        # Aggregierter Display: "BRCA1: Pathogenic — Hereditary breast cancer"
        display_parts = []
        if gene_symbol:
            display_parts.append(f"Gen {gene_symbol}")
        display_parts.append(clin_sig)
        if trait_name:
            display_parts.append(f"bei {trait_name}")
        display = " — ".join(display_parts)

        results.append({
            "indicator_name": title or f"ClinVar Variation {uid}",
            "indicator": "clinvar_variant",
            "country": "USA",
            "year": "—",
            "topic": (gene_symbol or "clinvar_variant").lower(),
            "display_value": display,
            "description": trait_name or clin_sig,
            "url": url,
            "secondary_url": "",
            "source": "NIH ClinVar (National Center for Biotechnology Information, USA)",
        })

    if not results:
        return empty

    logger.info(f"ClinVar: {len(results)} variants for query '{used_query[:60]}'")

    return {
        "source": "NIH ClinVar",
        "type": "genetic_variant",
        "results": results,
    }
