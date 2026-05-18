"""AHRQ — Agency for Healthcare Research and Quality.

Quelle: AHRQ Evidence-based Practice Center (EPC) Program — US-Pendant zu
Cochrane (UK) und INAHTA. Liefert systematische Reviews, Comparative
Effectiveness Reviews und Technical Briefs zu klinischen Themen.

Zugriff (warum NCBI/PubMed statt direkter AHRQ-API):
  - SRDR+ API (https://srdrplus.ahrq.gov/api/v1/) → CloudFront 403 ohne
    Browser-UA und kein verlässlicher REST-Endpoint für externe
    Volltext-Recherche (Stand 2026-05).
  - effectivehealthcare.ahrq.gov → CloudFront blockt anonymen httpx-Traffic.
  - PubMed esearch/esummary indexiert ALLE AHRQ-EPC-Reports vollständig:
      term="Agency for Healthcare Research and Quality"[Publisher]
    Ergebnis: bookname=ahrqYYehcNNN, booktitle=<offizieller Titel>,
    publishername="Agency for Healthcare Research and Quality (US)",
    availablefromurl → ncbi.nlm.nih.gov/books/NBK… (Volltext).
  - Vorteil: nutzt die NCBI-"polite-pool"-Infrastruktur, die wir mit
    cochrane.py / biorxiv.py ohnehin schon ansprechen; gleiches optionales
    PUBMED_API_KEY/PUBMED_EMAIL.

Lizenz:
  - AHRQ EPC Reports: US Public Domain (US Government Work, 17 U.S.C. § 105)
  - PubMed-/NCBI-Metadaten: keine Restriction, korrekte Attribution genügt.

Use-Case-Trigger:
  - "AHRQ Empfehlung", "Evidence-based Practice Center"
  - "Systematic Review …", "Comparative Effectiveness Review"
  - "Wirksamkeit Medikament/Verfahren" + Gesundheitskontext
  - "EPC Report"

Cross-Cluster:
  - cochrane.py    — Cochrane SR (UK/global). Methodisch gleich, andere Org.
  - biorxiv.py     — Preprints (NICHT peer-reviewed).
  - openfda.py     — Pharmakovigilanz (FDA), keine SR.
  - clinicaltrials.py — Registrierte Trials, KEINE Synthese.
AHRQ ergänzt Cochrane bei US-Public-Health-Themen (Screening-Empfehlungen,
Comparative Effectiveness, AHRQ-konsortium-getriebene Reviews) und
liefert oft längere/aktuellere Reports als Cochrane bei US-Kontext.

Politische Guardrails: Reine wissenschaftliche Evidenzquelle.
KEINE eigene Wertung der Reviews — der Synthesizer zitiert Titel/Autoren
und verlinkt den NBK-/AHRQ-Volltext.
"""

# WIRING für main.py (NICHT in dieser Datei vornehmen):
#   from services.ahrq import search_ahrq, claim_mentions_ahrq_cached
#   if claim_mentions_ahrq_cached(claim):
#       tasks.append(cached("AHRQ", search_ahrq, analysis))
#       queried_names.append("AHRQ EPC")
#
# WIRING für services/reranker.py (Whitelist):
#   "AHRQ" und/oder "AHRQ EPC" in SOURCE_WHITELIST eintragen.
#
# WIRING für services/data_updater.py: KEIN Prefetch nötig — PubMed-Call
# ist <2 s, läuft on-demand. 24-h-In-Memory-Cache reicht.

from __future__ import annotations

import logging
import os
import time
from functools import lru_cache

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------
EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

# Optional: gleicher NCBI-Key wie cochrane.py / biorxiv.py — falls vorhanden,
# heben wir das PubMed-Rate-Limit von 3 auf 10 req/s.
PUBMED_API_KEY = os.getenv("PUBMED_API_KEY", "")
PUBMED_EMAIL = os.getenv("PUBMED_EMAIL", "")

# AHRQ-Publisher-Filter — exakt so wie er in PubMed-/Bookshelf-Metadaten
# steht. Ohne "(US)"-Klammer gefilterte Suche, weil die Klammer in einer
# Phrase-Search von PubMed nicht stabil hashed wird.
AHRQ_PUBLISHER_FILTER = '"Agency for Healthcare Research and Quality"[Publisher]'

TIMEOUT_S = 20.0
MAX_RESULTS = 5
CACHE_TTL_S = 24 * 60 * 60  # 24 h

# In-Memory-Cache: query-key → (ts, payload)
_search_cache: dict[str, tuple[float, dict]] = {}


# ---------------------------------------------------------------------------
# Trigger-Vokabular
# ---------------------------------------------------------------------------
# Explizite AHRQ-/EPC-Begriffe — sofortiger Trigger.
_AHRQ_TERMS = (
    "ahrq", "a.h.r.q.",
    "agency for healthcare research and quality",
    "evidence-based practice center", "evidence based practice center",
    "epc report", "epc-report",
    "comparative effectiveness review", "comparative effectiveness reviews",
    "comparative effectiveness research",
    "srdr+", "srdr plus", "systematic review data repository",
    "uspstf", "u.s. preventive services task force",
    "preventive services task force",
)

# Evidence-/Methoden-Begriffe — triggern NUR mit Gesundheitskontext (siehe
# `_HEALTH_CONTEXT`), sonst feuern wir auch bei Tech-/Sozial-SRs.
_EVIDENCE_TERMS = (
    "systematic review", "systematische übersicht", "systematische uebersicht",
    "meta-analyse", "metaanalyse", "meta analysis",
    "evidence-based", "evidence based", "evidenzbasiert",
    "evidenz-basiert", "evidenzbasierte medizin",
    "comparative effectiveness",
    "wirksamkeitsvergleich", "wirksamkeits-vergleich",
    # "Wirksamkeit X" als Faktencheck-Aufhänger (Task-Spec). Triggert nur in
    # Kombination mit `_HEALTH_CONTEXT`, deshalb generisch genug.
    "wirksamkeit", "wirksam gegen", "wirksam bei",
    "efficacy", "effectiveness",
)

# Gesundheits-/Klinik-Kontext, der `_EVIDENCE_TERMS` zum Volltreffer macht.
_HEALTH_CONTEXT = (
    "medikament", "medikamente", "arznei", "arzneimittel", "wirkstoff",
    "präparat", "praeparat", "medication", "medicine", "drug",
    "pharmaceutical", "therapie", "therapy", "behandlung", "treatment",
    "diagnose", "diagnostik", "diagnosis", "diagnostic",
    "screening", "vorsorge", "prävention", "praevention", "prevention",
    "krebs", "tumor", "cancer", "carcinoma",
    "diabetes", "diabetes mellitus",
    "herz-kreislauf", "kardiologie", "cardiology", "cardiovascular",
    "schlaganfall", "stroke",
    "depression", "psychiatrie", "psychiatry", "psychotherapie",
    "impfung", "impfstoff", "vaccine", "vaccination",
    "antibiotikum", "antibiotic", "antibiotika",
    "operation", "surgery", "chirurgie",
    "rehabilitation", "reha",
    "schmerz", "schmerzen", "pain",
    "demenz", "dementia", "alzheimer",
    "schwangerschaft", "pregnancy", "geburt", "neonatal",
    "kinder", "pediatric", "pädiatrie", "paediatric",
    "ältere", "geriatric", "geriatrie",
    "patient", "patientin", "patients",
    "klinisch", "clinical",
    "leitlinie", "leitlinien", "guideline", "guidelines",
)


def _claim_mentions_ahrq(claim_lc: str) -> bool:
    """Trigger-Check (interne Logik auf bereits lowercase'tem Text)."""
    # 1) Direkter AHRQ-/EPC-/USPSTF-Bezug
    if any(t in claim_lc for t in _AHRQ_TERMS):
        return True
    # 2) Evidenz-Methode + Gesundheitskontext
    has_evidence = any(t in claim_lc for t in _EVIDENCE_TERMS)
    has_health = any(t in claim_lc for t in _HEALTH_CONTEXT)
    if has_evidence and has_health:
        return True
    return False


@lru_cache(maxsize=2048)
def claim_mentions_ahrq_cached(claim: str) -> bool:
    """Public cache-fähiger Trigger-Check (LRU-Cache pro normalisiertem Claim)."""
    return _claim_mentions_ahrq((claim or "").lower())


# ---------------------------------------------------------------------------
# Query-Bau
# ---------------------------------------------------------------------------
def _build_query(analysis: dict) -> str | None:
    """Baut die PubMed-Term-Query aus den vom claim_analyzer gelieferten
    `pubmed_queries` und kombiniert sie mit dem AHRQ-Publisher-Filter.

    Falls keine pubmed_queries vorhanden sind, fallback auf den Claim-Text
    selbst (max. 60 Zeichen, normiert).
    """
    queries = [q for q in (analysis or {}).get("pubmed_queries") or []
               if isinstance(q, str) and q.strip()]
    if queries:
        topic = " OR ".join(q.strip() for q in queries[:3])
    else:
        claim = ((analysis or {}).get("claim")
                 or (analysis or {}).get("original_claim") or "").strip()
        if not claim:
            return None
        # Sehr defensiv: nur die ersten 60 Zeichen, ohne Sonderzeichen.
        topic = claim[:60]

    return f"({topic}) AND {AHRQ_PUBLISHER_FILTER}"


# ---------------------------------------------------------------------------
# HTTP-Helfer
# ---------------------------------------------------------------------------
def _pubmed_params_base() -> dict[str, str]:
    p: dict[str, str] = {"db": "pubmed", "retmode": "json"}
    if PUBMED_API_KEY:
        p["api_key"] = PUBMED_API_KEY
    if PUBMED_EMAIL:
        p["email"] = PUBMED_EMAIL
        p["tool"] = "evidora"
    return p


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _build_result_item(pmid: str, rec: dict) -> dict:
    booktitle = rec.get("booktitle") or rec.get("title") or ""
    bookname = rec.get("bookname") or ""
    pubdate = rec.get("pubdate") or rec.get("srcdate") or ""
    publisher = rec.get("publishername") or (
        "Agency for Healthcare Research and Quality (US)"
    )

    # Autoren-String (max. 3 + et al.)
    authors_list = rec.get("authors") or []
    author_names = ", ".join(
        a.get("name", "") for a in authors_list[:3]
        if isinstance(a, dict)
    )
    if len(authors_list) > 3:
        author_names += " et al."

    # Bevorzugt NCBI-Bookshelf-URL (Volltext, gehostet bei NLM); Fallback PubMed.
    url = rec.get("availablefromurl") or f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

    # Report-Number (z. B. "AHRQ 24(26)-EHC010") ist für die Synthese hilfreich.
    report_no = (rec.get("reportnumber") or "").strip()
    report_no_short = ""
    if report_no:
        # Nur erstes "Report No.: …"-Segment behalten.
        first = report_no.split("Report No.:")
        if len(first) >= 2:
            report_no_short = first[1].split("Report No.:")[0].strip()
        else:
            report_no_short = report_no

    summary_parts: list[str] = []
    if author_names:
        summary_parts.append(author_names)
    if pubdate:
        summary_parts.append(pubdate)
    summary_parts.append(publisher)
    if report_no_short:
        summary_parts.append(f"Report: {report_no_short}")
    if bookname:
        summary_parts.append(f"Bookshelf-Code: {bookname}")

    return {
        "title": booktitle or f"AHRQ EPC Report (PMID {pmid})",
        "type": "evidence_based_review",
        "url": url,
        "summary": " | ".join(summary_parts),
        "authors": author_names,
        "date": pubdate,
        "publisher": publisher,
        "bookname": bookname,
        "pmid": pmid,
    }


def _caveat_result() -> dict:
    return {
        "title": "KONTEXT: AHRQ Evidence-based Practice Center",
        "type": "context",
        "url": "https://effectivehealthcare.ahrq.gov/about/epc",
        "summary": (
            "AHRQ EPC Reports sind systematische Reviews/Comparative "
            "Effectiveness Reviews der US-Agency for Healthcare Research and "
            "Quality (Public Domain, US Government Work). "
            "Einschränkungen: "
            "(1) US-Versorgungs-/Zulassungskontext — Empfehlungen sind nicht "
            "1:1 auf AT/EU übertragbar (vgl. EMA/BASG vs. FDA). "
            "(2) Comparative-Effectiveness-Reports sind methodisch nahe an "
            "Cochrane-SRs, aber auf US-Patientenpopulationen fokussiert. "
            "(3) Für AT/EU-spezifische Wirksamkeitsfragen ergänzend Cochrane "
            "(UK/global), INAHTA (HTA-Netzwerk) oder G-BA-Methodenpapiere "
            "berücksichtigen."
        ),
    }


# ---------------------------------------------------------------------------
# Public Search
# ---------------------------------------------------------------------------
async def search_ahrq(analysis: dict) -> dict:
    """Search AHRQ EPC reports via PubMed Publisher-Filter.

    Gibt strukturierte Liste systematischer Reviews / Comparative
    Effectiveness Reports zurück. Nutzt 24-h-In-Memory-Cache.
    """
    empty: dict = {
        "source": "AHRQ",
        "type": "evidence_based_reviews",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_ahrq(matchable):
        return empty

    term = _build_query(analysis)
    if not term:
        return empty

    # Cache-Lookup
    cache_key = f"ahrq::{term}"
    now = time.time()
    cached = _search_cache.get(cache_key)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]

    try:
        async with polite_client(timeout=TIMEOUT_S) as client:
            # 1) ESEARCH — Top-PMIDs nach Relevanz.
            search_params = _pubmed_params_base()
            search_params["term"] = term
            search_params["retmax"] = str(MAX_RESULTS)
            search_params["sort"] = "relevance"
            search_resp = await client.get(
                f"{EUTILS_BASE}/esearch.fcgi", params=search_params
            )
            search_resp.raise_for_status()
            ids = (
                search_resp.json()
                .get("esearchresult", {})
                .get("idlist", [])
            )

            if not ids:
                logger.info(f"AHRQ: 0 hits for term={term[:80]}")
                _search_cache[cache_key] = (now, empty)
                return empty

            # 2) ESUMMARY — Metadaten für die Treffer.
            sum_params = _pubmed_params_base()
            sum_params["id"] = ",".join(ids)
            sum_resp = await client.get(
                f"{EUTILS_BASE}/esummary.fcgi", params=sum_params
            )
            sum_resp.raise_for_status()
            sum_data = sum_resp.json()

            items: list[dict] = []
            for pmid in ids:
                rec = sum_data.get("result", {}).get(pmid, {})
                if not isinstance(rec, dict):
                    continue
                # Filter: nur Records, die wirklich von AHRQ stammen.
                pub = (rec.get("publishername") or "").lower()
                if "agency for healthcare research and quality" not in pub:
                    # Trotz Publisher-Filter zeigt PubMed manchmal
                    # Journal-Artikel mit AHRQ-Funding — die wollen wir nicht.
                    continue
                items.append(_build_result_item(pmid, rec))

            if not items:
                logger.info(
                    f"AHRQ: {len(ids)} PMIDs but 0 AHRQ-EPC-records after filter"
                )
                _search_cache[cache_key] = (now, empty)
                return empty

            items.append(_caveat_result())
            result: dict = {
                "source": "AHRQ",
                "type": "evidence_based_reviews",
                "results": items,
            }
            logger.info(
                f"AHRQ: {len(items) - 1} EPC reports for "
                f"term={term[:80]}, api_key={'yes' if PUBMED_API_KEY else 'no'}"
            )
            _search_cache[cache_key] = (now, result)
            return result
    except Exception as e:
        logger.warning(f"AHRQ fetch failed: {type(e).__name__}: {e}")
        return empty
