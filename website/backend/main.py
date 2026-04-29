from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
import asyncio
import json
import logging
import os
import re
import time
import unicodedata

from services.claim_analyzer import analyze_claim
from services.pubmed import search_pubmed
from services.who import search_who
from services.ema import search_ema
from services.efsa import search_efsa
from services.claimreview import search_claimreview
from services.copernicus import search_copernicus
from services.eurostat import search_eurostat
from services.eea import search_eea
from services.ecdc import search_ecdc
from services.ecb import search_ecb
from services.unhcr import search_unhcr
from services.cochrane import search_cochrane
from services.gadmo import search_gadmo
from services.oecd import search_oecd, _has_sdmx_keywords
from services.euvsdisinfo import search_euvsdisinfo, _is_disinfo_claim
from services.datacommons import search_datacommons
from services.who_europe import search_who_europe
from services.openalex import search_openalex
from services.worldbank import search_worldbank
from services.europe_pmc import search_europe_pmc
from services.clinicaltrials import search_clinicaltrials
from services.semantic_scholar import search_semantic_scholar
from services.energy_safety import search_energy_safety, _is_energy_safety_claim
from services.statistik_austria import search_statistik_austria, _is_austria_context, _match_keywords, VPI_KEYWORDS, HEALTH_EXP_KEYWORDS, MORTALITY_KEYWORDS, VGR_KEYWORDS, MIGRATION_KEYWORDS, NATURALIZATION_KEYWORDS, ARBEITSMARKT_KEYWORDS, ARMUT_KEYWORDS
from services.vdem import search_vdem, _claim_mentions_vdem
from services.transparency import search_transparency, _claim_mentions_cpi
from services.rsf import search_rsf, _claim_mentions_rsf
from services.sipri import search_sipri, _claim_mentions_sipri
from services.idea import search_idea, _claim_mentions_idea
from services.parlament_at import search_parlament_at, _claim_mentions_parlament
from services.geosphere import search_geosphere, _claim_mentions_climate as _claim_mentions_geosphere_climate, _detect_cities as _geosphere_detect_cities, _claim_mentions_austria as _geosphere_mentions_austria
from services.basg import search_basg, _claim_mentions_pharma as _claim_mentions_basg_pharma, _claim_mentions_austria as _basg_mentions_austria
from services.ris import search_ris, _claim_mentions_legal as _ris_mentions_legal, _claim_mentions_austria as _ris_mentions_austria, _extract_search_terms as _ris_extract_terms, _extract_topic_law_refs as _ris_topic_refs, _extract_law_paragraph_refs as _ris_para_refs
from services.volksbegehren import search_volksbegehren, claim_mentions_volksbegehren_cached
from services.wahlen import search_wahlen, claim_mentions_wahlen_cached
from services.abstimmungen import search_abstimmungen, claim_mentions_voting_cached
from services.at_factbook import search_at_factbook, claim_mentions_factbook_cached
from services.pks import search_pks, claim_mentions_pks_cached
from services.dach_factbook import search_dach_factbook, claim_mentions_dach_factbook_cached
from services.retraction_watch import search_retraction_watch, claim_mentions_retraction_watch_cached
from services.frontex import search_frontex, claim_mentions_frontex_cached
from services.at_faktencheck_rss import search_at_faktencheck_rss, claim_mentions_at_faktencheck_rss_cached
from services.cache import get as cache_get, put as cache_put
from services.synthesizer import synthesize_results
from services.ner import enrich_entities
from services.data_updater import prefetch_all, start_background_updates, stop_background_updates

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("evidora")

app = FastAPI(title="Evidora API")

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_prefetch():
    """Prefetch external CSV data and start background refresh loop."""
    await prefetch_all()
    start_background_updates()


@app.on_event("shutdown")
async def shutdown_updater():
    stop_background_updates()


class ClaimRequest(BaseModel):
    claim: str


# --- Input validation ---
MAX_CLAIM_LENGTH = 500  # characters — claims are single statements, not essays

# Control characters and dangerous Unicode categories to strip
_UNSAFE_UNICODE_CATS = {"Cc", "Cf", "Co", "Cs"}  # control, format, private use, surrogate


def _sanitize_claim(text: str) -> str:
    """Sanitize user claim: strip control chars, normalize Unicode, limit length."""
    # Normalize Unicode (NFC = canonical composition)
    text = unicodedata.normalize("NFC", text)
    # Strip control characters and zero-width chars (keep newlines/tabs for readability)
    text = "".join(
        ch for ch in text
        if unicodedata.category(ch) not in _UNSAFE_UNICODE_CATS or ch in ("\n", "\t")
    )
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    # Enforce length limit
    return text[:MAX_CLAIM_LENGTH]


# --- Rate limiting ---
RATE_LIMIT = int(os.getenv("RATE_LIMIT", "10"))
RATE_WINDOW = int(os.getenv("RATE_WINDOW", "60"))  # seconds
_rate_store: dict[str, list[float]] = {}


def _get_client_ip(request: Request) -> str:
    """Extract real client IP, respecting X-Forwarded-For behind reverse proxy."""
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        # First IP in the chain is the original client
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _check_rate_limit(ip: str) -> bool:
    now = time.time()
    timestamps = _rate_store.get(ip, [])
    timestamps = [t for t in timestamps if now - t < RATE_WINDOW]
    _rate_store[ip] = timestamps
    if len(timestamps) >= RATE_LIMIT:
        return False
    timestamps.append(now)
    return True


@app.post("/api/check")
async def check_claim(request: Request):
    client_ip = _get_client_ip(request)
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Zu viele Anfragen. Bitte warte einen Moment.")

    body = await request.json()
    lang = body.get("lang", "de") if body.get("lang") in ("de", "en") else "de"
    raw_claim = body.get("claim", "")
    if not isinstance(raw_claim, str):
        raise HTTPException(status_code=400, detail="Invalid claim format.")
    if len(raw_claim) > MAX_CLAIM_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"Claim too long (max {MAX_CLAIM_LENGTH} characters)."
            if lang == "en" else
            f"Behauptung zu lang (max. {MAX_CLAIM_LENGTH} Zeichen).",
        )
    claim = _sanitize_claim(raw_claim)
    if not claim:
        raise HTTPException(status_code=400, detail="Claim must not be empty." if lang == "en" else "Behauptung darf nicht leer sein.")
    if len(claim) < 10 or len(claim.split()) < 2:
        raise HTTPException(status_code=400, detail="Claim too short — please enter at least 2 words." if lang == "en" else "Behauptung zu kurz — bitte mindestens 2 Wörter eingeben.")

    async def event_stream():
        # Step 1: Analyze claim with Mistral
        yield {"event": "step", "data": json.dumps({"step": "analyze"})}
        try:
            logger.info(f"Analyzing claim (category pending, {len(claim)} chars)")
            analysis = await analyze_claim(claim)
            analysis = enrich_entities(claim, analysis)
            # Preserve the user's original (sanitized) input separately from
            # the LLM-normalized claim. Services that rely on specific
            # phrasing (e.g. the EU-cohort trigger "als die EU") must check
            # the original text because the analyzer may paraphrase away
            # exactly those comparative markers.
            analysis["original_claim"] = claim
            logger.info(f"Analysis done: category={analysis.get('category')}, confidence={analysis.get('confidence')}, entities={analysis.get('entities')}")
        except ValueError as e:
            if "MISTRAL_CREDITS_EXHAUSTED" in str(e):
                logger.error("Mistral API credits exhausted")
                yield {"event": "error", "data": json.dumps({"detail": "MISTRAL_CREDITS_EXHAUSTED"})}
                return
            # Any other ValueError (e.g. "Mistral returned unparseable response")
            # MUST emit a clean error event — re-raising terminates the SSE
            # stream uncleanly and surfaces as "Error in input stream" in
            # Firefox, which is unhelpful for users.
            logger.error(f"Claim analysis ValueError: {e}", exc_info=True)
            yield {"event": "error", "data": json.dumps({"detail": "Fehler bei der Claim-Analyse. Bitte erneut versuchen." if lang == "de" else "Claim analysis failed. Please try again."})}
            return
        except Exception:
            logger.error("Claim analysis failed", exc_info=True)
            yield {"event": "error", "data": json.dumps({"detail": "Fehler bei der Claim-Analyse. Bitte erneut versuchen." if lang == "de" else "Claim analysis failed. Please try again."})}
            return

        # Step 2: Query sources in parallel (with caching)
        yield {"event": "step", "data": json.dumps({"step": "search"})}

        async def cached(source_name: str, fn, analysis: dict) -> dict:
            hit = cache_get(source_name, analysis)
            if hit is not None:
                return hit
            result = await fn(analysis)
            cache_put(source_name, analysis, result)
            return result

        # PubMed only for categories where medical/scientific literature is relevant
        pubmed_categories = {"health", "climate", "medication", "demographics", "energy", "other"}
        tasks = []
        queried_names = []
        if analysis.get("category") in pubmed_categories:
            tasks.append(cached("PubMed", search_pubmed, analysis))
            queried_names.append("PubMed")
            tasks.append(cached("Cochrane", search_cochrane, analysis))
            queried_names.append("Cochrane")
        tasks.append(cached("ClaimReview", search_claimreview, analysis))
        queried_names.append("Europäische Faktenchecker")
        tasks.append(cached("GADMO", search_gadmo, analysis))
        queried_names.append("GADMO Faktenchecks")
        tasks.append(cached("DataCommons", search_datacommons, analysis))
        queried_names.append("DataCommons ClaimReview")
        if analysis.get("who_relevant"):
            tasks.append(cached("WHO", search_who, analysis))
            queried_names.append("WHO")
        if analysis.get("ema_relevant"):
            tasks.append(cached("EMA", search_ema, analysis))
            queried_names.append("EMA")
        if analysis.get("efsa_relevant"):
            tasks.append(cached("EFSA", search_efsa, analysis))
            queried_names.append("EFSA")
        if analysis.get("climate_relevant"):
            tasks.append(cached("Copernicus", search_copernicus, analysis))
            queried_names.append("Copernicus")
        if analysis.get("eurostat_relevant"):
            tasks.append(cached("Eurostat", search_eurostat, analysis))
            queried_names.append("Eurostat (EU)")
        if analysis.get("eea_relevant"):
            tasks.append(cached("EEA", search_eea, analysis))
            queried_names.append("EEA")
        if analysis.get("ecdc_relevant"):
            tasks.append(cached("ECDC", search_ecdc, analysis))
            queried_names.append("ECDC")
        if analysis.get("ecb_relevant"):
            tasks.append(cached("ECB", search_ecb, analysis))
            queried_names.append("EZB")
        if analysis.get("unhcr_relevant"):
            tasks.append(cached("UNHCR", search_unhcr, analysis))
            queried_names.append("UNHCR")
        if analysis.get("oecd_relevant") or analysis.get("category") == "education" or _has_sdmx_keywords(claim):
            tasks.append(cached("OECD", search_oecd, analysis))
            queried_names.append("OECD")
        if analysis.get("who_europe_relevant"):
            tasks.append(cached("WHO_Europe", search_who_europe, analysis))
            queried_names.append("WHO Europe (HFA)")
        if _is_disinfo_claim(analysis):
            tasks.append(cached("EUvsDisinfo", search_euvsdisinfo, analysis))
            queried_names.append("EUvsDisinfo")
        if analysis.get("worldbank_relevant"):
            tasks.append(cached("WorldBank", search_worldbank, analysis))
            queried_names.append("World Bank")
        if _is_energy_safety_claim(analysis):
            tasks.append(cached("EnergySafety", search_energy_safety, analysis))
            queried_names.append("OWID Energy Safety")
        # Statistik Austria: Austrian VPI/inflation or health expenditure claims
        _claim_lower = claim.lower()
        if _is_austria_context(_claim_lower) and (
            _match_keywords(_claim_lower, VPI_KEYWORDS)
            or _match_keywords(_claim_lower, HEALTH_EXP_KEYWORDS)
            or _match_keywords(_claim_lower, MORTALITY_KEYWORDS)
            or _match_keywords(_claim_lower, VGR_KEYWORDS)
            or _match_keywords(_claim_lower, MIGRATION_KEYWORDS)
            or _match_keywords(_claim_lower, NATURALIZATION_KEYWORDS)
            or _match_keywords(_claim_lower, ARBEITSMARKT_KEYWORDS)
            or _match_keywords(_claim_lower, ARMUT_KEYWORDS)
        ):
            tasks.append(cached("StatistikAustria", search_statistik_austria, analysis))
            queried_names.append("Statistik Austria")
        # V-Dem: Demokratie-Qualität (liberal, elektoral, partizipativ)
        if _claim_mentions_vdem(claim):
            tasks.append(cached("V-Dem", search_vdem, analysis))
            queried_names.append("V-Dem")
        # Transparency International: Corruption Perception Index
        if _claim_mentions_cpi(claim):
            tasks.append(cached("Transparency", search_transparency, analysis))
            queried_names.append("Transparency International")
        # RSF: Pressefreiheits-Index
        if _claim_mentions_rsf(claim):
            tasks.append(cached("RSF", search_rsf, analysis))
            queried_names.append("Reporter ohne Grenzen (RSF)")
        # SIPRI: Militärausgaben (absolut, % BIP, % Staatsausgaben)
        if _claim_mentions_sipri(claim):
            tasks.append(cached("SIPRI", search_sipri, analysis))
            queried_names.append("SIPRI")
        # IDEA: Wahlbeteiligung bei Parlamentswahlen (registrierte + VAP)
        if _claim_mentions_idea(claim):
            tasks.append(cached("IDEA", search_idea, analysis))
            queried_names.append("IDEA Voter Turnout")
        # Parlament.gv.at: Aktuelle Nationalrat-Klubstärken (AT-spezifisch)
        if _claim_mentions_parlament(claim):
            tasks.append(cached("ParlamentAT", search_parlament_at, analysis))
            queried_names.append("Parlament.gv.at")
        # GeoSphere Austria: Stations-Klimadaten für AT-Städte
        # (selektives Triggering: Klima-Keyword + AT-Stadt oder Österreich-Bezug)
        if _claim_mentions_geosphere_climate(claim) and (
            _geosphere_detect_cities(claim) or _geosphere_mentions_austria(analysis)
        ):
            tasks.append(cached("GeoSphere", search_geosphere, analysis))
            queried_names.append("GeoSphere Austria")
        # BASG: AT-spezifische Arzneimittel-/Medizinprodukt-Sicherheitsmeldungen
        # (selektives Triggering: Pharma-Keyword/Drug-Entity + Österreich-Bezug)
        if _claim_mentions_basg_pharma(claim, analysis) and _basg_mentions_austria(analysis):
            tasks.append(cached("BASG", search_basg, analysis))
            queried_names.append("BASG")
        # RIS: Bundesgesetzblatt / österreichisches Bundesrecht
        # (selektives Triggering: Legal-Keyword + AT-Kontext + irgendein
        # auswertbarer Suchpfad — Suchterm, Topic-Ref oder §-Ref).
        # Bug T: Topic-Refs ohne Suchterm wurden vorher ignoriert; jetzt
        # triggert RIS auch für Claims wie "Verfassung setzt auf
        # Stärkeverhältnisse" (kein Compound, aber B-VG-Topic match).
        if (_ris_mentions_legal(claim) and _ris_mentions_austria(analysis)
                and (_ris_extract_terms(analysis)
                     or _ris_topic_refs(claim)
                     or _ris_para_refs(claim))):
            tasks.append(cached("RIS", search_ris, analysis))
            queried_names.append("RIS")
        # BMI Volksbegehren: AT-Bundes-Volksbegehren (selektives Triggering:
        # VBG-Keyword + AT-Kontext oder bekannter VBG-Name + Jahreszahl)
        if claim_mentions_volksbegehren_cached(claim):
            tasks.append(cached("Volksbegehren", search_volksbegehren, analysis))
            queried_names.append("BMI Volksbegehren")
        # BMI Wahlen: NRW + BPW + EUW Bundesergebnisse (selektives Triggering:
        # Wahltyp/-Keyword + AT-Kontext, oder Partei/Kandidat + Jahr + AT)
        if claim_mentions_wahlen_cached(claim):
            tasks.append(cached("Wahlen", search_wahlen, analysis))
            queried_names.append("BMI Wahlen")
        # Parlament Abstimmungen: NR-Beschlüsse seit 2017 mit Klub-Voting
        # (selektives Triggering: Voting-Keyword + AT-Kontext)
        if claim_mentions_voting_cached(claim):
            tasks.append(cached("Abstimmungen", search_abstimmungen, analysis))
            queried_names.append("Parlament Abstimmungen")
        # AT Factbook: kuratierte AT-Faktoide (Religion Wiener Pflichtschulen,
        # Bundesförderungen-Zeitreihe). Static-curated, manuell aktualisiert.
        if claim_mentions_factbook_cached(claim):
            tasks.append(cached("AT Factbook", search_at_factbook, analysis))
            queried_names.append("AT Factbook")
        # BKA PKS: Polizeiliche Kriminalstatistik + Lagebericht Suchtmittel
        # (statisch curated aus den jährlichen BKA-PDF-Berichten).
        if claim_mentions_pks_cached(claim):
            tasks.append(cached("BKA PKS", search_pks, analysis))
            queried_names.append("BKA Polizeiliche Kriminalstatistik")
        # DACH Factbook: kuratierte DE-/CH-Faktoide für Boulevard-Falsch-
        # meldungen aus Bild, Blick, AfD-/SVP-Aussagen (BAMF, Bürgergeld,
        # Heizung, AHV, CORRECTIV-Counter).
        if claim_mentions_dach_factbook_cached(claim):
            tasks.append(cached("DACH Factbook", search_dach_factbook, analysis))
            queried_names.append("DACH Factbook")
        # Retraction Watch: Zurückgezogene wissenschaftliche Studien
        # (Wakefield 2010 MMR/Autismus, Surgisphere 2020 Hydroxychloroquin,
        # Schön-Skandal etc.).
        if claim_mentions_retraction_watch_cached(claim):
            tasks.append(cached("Retraction Watch", search_retraction_watch, analysis))
            queried_names.append("Retraction Watch")
        # Frontex: EU-Grenzschutz-Statistiken (irreguläre Grenzübertritte,
        # Routen-Aufschlüsselung, Mittelmeer-Tote).
        if claim_mentions_frontex_cached(claim):
            tasks.append(cached("Frontex", search_frontex, analysis))
            queried_names.append("Frontex")
        # AT-Faktencheck-RSS-Aggregator (APA + Kontrast). Reranker
        # filtert thematisch (FACTCHECK_THRESHOLD).
        if claim_mentions_at_faktencheck_rss_cached(claim):
            tasks.append(cached("AT-Faktencheck-RSS", search_at_faktencheck_rss, analysis))
            queried_names.append("AT-Faktencheck-RSS")
        # OpenAlex covers all scientific disciplines — query for any claim with search terms
        if analysis.get("pubmed_queries"):
            tasks.append(cached("OpenAlex", search_openalex, analysis))
            queried_names.append("OpenAlex")
        # Europe PMC: European life science literature, same categories as PubMed
        if analysis.get("category") in pubmed_categories and analysis.get("pubmed_queries"):
            tasks.append(cached("EuropePMC", search_europe_pmc, analysis))
            queried_names.append("Europe PMC")
        # ClinicalTrials.gov: clinical studies for health/medication claims
        if analysis.get("category") in ("health", "medication") and analysis.get("pubmed_queries"):
            tasks.append(cached("ClinicalTrials", search_clinicaltrials, analysis))
            queried_names.append("ClinicalTrials.gov")
        # Semantic Scholar: AI-powered search with TLDR summaries
        if analysis.get("pubmed_queries"):
            tasks.append(cached("SemanticScholar", search_semantic_scholar, analysis))
            queried_names.append("Semantic Scholar")

        # Use asyncio.wait so completed tasks return results even if others time out
        valid_results = []
        valid_names: list[str] = []  # index-aligned with valid_results
        if tasks:
            task_objects = [asyncio.ensure_future(t) for t in tasks]
            done, pending = await asyncio.wait(task_objects, timeout=45.0)
            if pending:
                pending_names = [queried_names[i] for i, t in enumerate(task_objects) if t in pending]
                logger.warning(f"Source queries timed out after 45s: {pending_names}")
                for t in pending:
                    t.cancel()
            for i, t in enumerate(task_objects):
                if t in done:
                    try:
                        r = t.result()
                        logger.info(f"Source {i} ({queried_names[i]}) returned {len(r.get('results', []))} results")
                        valid_results.append(r)
                        valid_names.append(queried_names[i])
                    except Exception as e:
                        logger.warning(f"Source {i} ({queried_names[i]}) failed: {e}")

        # Pre-rerank counts (for logging only — the authoritative count is
        # computed AFTER synthesize_results, because the semantic re-ranker
        # inside the synthesizer filters off-topic entries and may empty
        # a source's result list entirely).
        pre_rerank_hits = sum(1 for r in valid_results if r.get("results"))

        # Step 3: Synthesize results with Mistral
        yield {"event": "step", "data": json.dumps({"step": "synthesize"})}
        try:
            logger.info(f"Synthesizing {len(valid_results)} source results")
            synthesis = await synthesize_results(claim, analysis, valid_results, lang=lang)
            logger.info(f"Synthesis verdict: {synthesis.get('verdict')}")
        except ValueError as e:
            if "MISTRAL_CREDITS_EXHAUSTED" in str(e):
                logger.error("Mistral API credits exhausted")
                yield {"event": "error", "data": json.dumps({"detail": "MISTRAL_CREDITS_EXHAUSTED"})}
                return
            # Same defensive pattern as in analyze: emit clean error event
            # rather than re-raising into the SSE generator.
            logger.error(f"Synthesis ValueError: {e}", exc_info=True)
            yield {"event": "error", "data": json.dumps({"detail": "Fehler bei der Ergebnis-Synthese. Bitte erneut versuchen." if lang == "de" else "Synthesis failed. Please try again."})}
            return
        except Exception:
            logger.error("Synthesis failed", exc_info=True)
            yield {"event": "error", "data": json.dumps({"detail": "Fehler bei der Ergebnis-Synthese. Bitte erneut versuchen." if lang == "de" else "Synthesis failed. Please try again."})}
            return

        # Post-rerank recount: the reranker mutates each source dict's
        # "results" list in place, so re-inspect after synthesis. A source
        # whose results were all filtered as off-topic should no longer
        # count as a "hit" for the frontend badge.
        sources_with_results = [r for r in valid_results if r.get("results")]
        hit_names = [valid_names[i] for i, r in enumerate(valid_results) if r.get("results")]
        post_rerank_hits = len(sources_with_results)
        if post_rerank_hits != pre_rerank_hits:
            logger.info(
                f"Source coverage after rerank: {post_rerank_hits}/{len(valid_results)} "
                f"(was {pre_rerank_hits} pre-rerank — reranker dropped "
                f"{pre_rerank_hits - post_rerank_hits} source(s) as off-topic)"
            )

        # If no source returned results, override verdict and suppress LLM opinion
        if not sources_with_results:
            synthesis["verdict"] = "unverifiable"
            synthesis["confidence"] = 0.0
            synthesis["summary"] = (
                "No sources returned relevant results. The claim cannot be verified based on available data."
                if lang == "en" else
                "Keine Quelle lieferte relevante Ergebnisse. Die Behauptung kann auf Basis der verfügbaren Daten nicht überprüft werden."
            )
            synthesis["nuance"] = None
            synthesis["evidence"] = []

        synthesis["analysis"] = analysis
        synthesis["raw_sources"] = valid_results
        synthesis["source_coverage"] = {
            "queried": len(tasks),
            "with_results": len(sources_with_results),
            "names": hit_names,
            "all_names": queried_names,
        }
        yield {"event": "result", "data": json.dumps(synthesis, ensure_ascii=False)}
        yield {"event": "done", "data": "{}"}

    return EventSourceResponse(event_stream(), ping=15)


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/legal")
async def legal():
    name = os.getenv("IMPRESSUM_NAME", "")
    email = os.getenv("IMPRESSUM_EMAIL", "")
    location = os.getenv("IMPRESSUM_LOCATION", "")
    if name and email:
        return {"configured": True, "name": name, "email": email, "location": location}
    return {"configured": False}
