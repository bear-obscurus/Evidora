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
from services.wifo_ihs import search_wifo_ihs, claim_mentions_wifo_ihs_cached
from services.oenb import search_oenb, claim_mentions_oenb_cached
from services.mimikama import search_mimikama, claim_mentions_mimikama_cached
from services.biorxiv import search_biorxiv, claim_mentions_biorxiv_cached
from services.eu_courts import search_eu_courts, claim_mentions_eu_courts_cached
from services.eu_crime import search_eu_crime, claim_mentions_eu_crime_cached
from services.energy_charts import search_energy_charts, claim_mentions_energy_charts_cached
from services.medientransparenz import search_medientransparenz, claim_mentions_medientransparenz_cached
from services.rki_surveillance import search_rki_surveillance, claim_mentions_rki_surveillance_cached
from services.education_dach import search_education, claim_mentions_education_cached
from services.at_courts import search_at_courts, claim_mentions_at_courts_cached
from services.oecd_health import search_oecd_health, claim_mentions_oecd_health_cached
from services.housing_at import search_housing, claim_mentions_housing_cached
from services.transport_at import search_transport, claim_mentions_transport_cached
from services.esoterik_pack import search_esoterik, claim_mentions_esoterik_cached
from services.geschichte_pack import search_geschichte, claim_mentions_geschichte_cached
from services.verschwoerungen_pack import search_verschwoerungen, claim_mentions_verschwoerungen_cached
from services.tech_ki_pack import search_tech_ki, claim_mentions_tech_ki_cached
from services.gesundheits_autoritaeten_pack import search_gesundheits_autoritaeten, claim_mentions_gesundheits_autoritaeten_cached
from services.destatis import search_destatis, claim_mentions_destatis_cached
from services.tier_natur_pack import search_tier_natur, claim_mentions_tier_natur_cached
from services.ernaehrungs_pack import search_ernaehrung, claim_mentions_ernaehrung_cached
from services.recht_pack import search_recht, claim_mentions_recht_cached
from services.energie_klima_pack import search_energie_klima, claim_mentions_energie_klima_cached
from services.migration_pack import search_migration, claim_mentions_migration_cached
from services.geographie_pack import search_geographie, claim_mentions_geographie_cached
from services.eurobarometer import search_eurobarometer, claim_mentions_eurobarometer_cached
from services.finanzen_pack import search_finanzen, claim_mentions_finanzen_cached
from services.bildung_pack import search_bildung, claim_mentions_bildung_cached
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
# Optional bypass for local stress tests / batch validations.
# Set EVIDORA_TEST_API_KEY in the environment (or compose) and pass it via
# the X-Evidora-Test-Key header — requests with a matching header skip the
# per-IP rate limit. The key is treated as a server-side secret; if the env
# is unset/empty, NO bypass is possible.
TEST_API_KEY = os.getenv("EVIDORA_TEST_API_KEY", "").strip()
_rate_store: dict[str, list[float]] = {}


def _get_client_ip(request: Request) -> str:
    """Extract real client IP, respecting X-Forwarded-For behind reverse proxy."""
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        # First IP in the chain is the original client
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _has_test_bypass(request: Request) -> bool:
    """True if the request carries a valid test-API-key header."""
    if not TEST_API_KEY:
        return False
    presented = request.headers.get("x-evidora-test-key", "").strip()
    # Constant-time compare to avoid timing leaks
    if not presented or len(presented) != len(TEST_API_KEY):
        return False
    diff = 0
    for a, b in zip(presented, TEST_API_KEY):
        diff |= ord(a) ^ ord(b)
    return diff == 0


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
    if not _has_test_bypass(request) and not _check_rate_limit(client_ip):
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
        # Hebel #4: Verdict-Cache mit semantischer Aehnlichkeit. Vor der
        # gesamten Pipeline (Analyzer + Datenquellen + Synthesizer)
        # pruefen, ob ein gleicher oder fast-gleicher Claim bereits
        # gecacht ist. Bei Hit: gesamte Pipeline ueberspringen,
        # Antwort in <100 ms statt 8-15 s.
        from services import verdict_cache as _vc
        cached_result = _vc.get(claim)
        if cached_result is not None:
            yield {"event": "step", "data": json.dumps({"step": "analyze"})}
            yield {"event": "step", "data": json.dumps({"step": "cache_hit"})}
            yield {"event": "result",
                   "data": json.dumps(cached_result, ensure_ascii=False)}
            yield {"event": "done", "data": "{}"}
            return

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
        # WIFO + IHS Konjunkturprognosen (Österreichs Wirtschaftsforschungs-
        # institute; BIP, Inflation, Arbeitslosigkeit, Rezession).
        if claim_mentions_wifo_ihs_cached(claim):
            tasks.append(cached("WIFO + IHS", search_wifo_ihs, analysis))
            queried_names.append("WIFO + IHS")
        # OeNB — Österreichische Nationalbank (EZB-Leitzins, Inflations-
        # Prognose, Wechselkurse, Euro-Austritts-Counter).
        if claim_mentions_oenb_cached(claim):
            tasks.append(cached("OeNB", search_oenb, analysis))
            queried_names.append("OeNB")
        # Mimikama — DACH Hoax-Faktencheck (Social-Media-Hoaxes,
        # Verschwörungs-Klassiker, KI-generierte Inhalte).
        if claim_mentions_mimikama_cached(claim):
            tasks.append(cached("Mimikama", search_mimikama, analysis))
            queried_names.append("Mimikama")
        # bioRxiv/medRxiv — Preprint-Server (frische Lebenswissenschafts-
        # Studien vor Peer-Review). Caveat im Output-Indikator.
        if claim_mentions_biorxiv_cached(claim):
            tasks.append(cached("bioRxiv/medRxiv", search_biorxiv, analysis))
            queried_names.append("bioRxiv/medRxiv")
        # EuGH (CURIA) + EGMR (HUDOC) — Schlüsselurteile, die Boulevard
        # / FPÖ-Medien regelmäßig verfälscht zitieren.
        if claim_mentions_eu_courts_cached(claim):
            tasks.append(cached("EuGH+EGMR", search_eu_courts, analysis))
            queried_names.append("EuGH+EGMR")
        # Eurostat Crime + BKA-DE/BMI-AT PKS — Tötungsdelikte EU-Vergleich,
        # Migrant-Tatverdächtigen-Anteile mit Strukturwarnungen,
        # Trend-Daten gegen 'Kriminalitätsexplosion'-Mythen.
        if claim_mentions_eu_crime_cached(claim):
            tasks.append(cached("Eurostat Crime + DACH PKS", search_eu_crime, analysis))
            queried_names.append("Eurostat Crime + DACH PKS")
        # Energy-Charts (Fraunhofer ISE) + APG — Stromproduktion +
        # Handel DACH gegen die häufigsten Energie-Boulevard-Mythen
        # (DE-Atomstrom-Mythos, EE-Anteil "nur 10 %", Dunkelflaute).
        if claim_mentions_energy_charts_cached(claim):
            tasks.append(cached("Energy-Charts", search_energy_charts, analysis))
            queried_names.append("Energy-Charts (Fraunhofer) + APG")
        # MedienTransparenz (RTR/KommAustria) — Inseraten-Volumen der
        # öffentlichen Hand + Top-Empfänger + Inseratenaffäre Kurz.
        if claim_mentions_medientransparenz_cached(claim):
            tasks.append(cached("MedienTransparenz", search_medientransparenz, analysis))
            queried_names.append("MedienTransparenz (RTR/KommAustria)")
        # RKI SurvStat — Surveillance-Eckwerte gegen Migration-bedingte
        # Krankheits-Mythen (Masern, TB) + Atemwegsinfekt-Wellen.
        if claim_mentions_rki_surveillance_cached(claim):
            tasks.append(cached("RKI SurvStat", search_rki_surveillance, analysis))
            queried_names.append("RKI SurvStat")
        # Bildung — TIMSS, PIRLS, PISA, Lehrer-Bedarf-Statistik AT;
        # gegen Boulevard-Mythen "Bildungs-Krise", "Lehrermangel",
        # "jeder dritte Volksschüler kann nicht lesen".
        if claim_mentions_education_cached(claim):
            tasks.append(cached("Bildung DACH", search_education, analysis))
            queried_names.append("Bildung (TIMSS/PIRLS/PISA + Lehrer-Bedarf)")
        # VfGH + VwGH — österreichische Höchstgerichts-Schlüsselerkenntnisse
        # (Ehe für alle, Sterbehilfe, COVID-Verordnungen, Impfpflicht,
        # ORF-Beitrag, Asyl-Drittstaat, BP-Wahl-Aufhebung 2016).
        if claim_mentions_at_courts_cached(claim):
            tasks.append(cached("VfGH+VwGH", search_at_courts, analysis))
            queried_names.append("VfGH + VwGH Schlüsselerkenntnisse")
        # OECD Health — Lebenserwartung, Spitalsbetten, Gesundheits-
        # ausgaben DACH gegen Mythen "Gesundheitssystem kollabiert".
        if claim_mentions_oecd_health_cached(claim):
            tasks.append(cached("OECD Health", search_oecd_health, analysis))
            queried_names.append("OECD Health (DACH)")
        # Wohnen Österreich — OeNB-Wohnimmobilienpreis-Index +
        # EU-SILC-Wohnkostenbelastung gegen Mythen "Wohnen wird unleistbar".
        if claim_mentions_housing_cached(claim):
            tasks.append(cached("Wohnen AT", search_housing, analysis))
            queried_names.append("Wohnen Österreich (OeNB + EU-SILC)")
        # Verkehr Österreich — ÖBB-Pünktlichkeit + Verkehrs-CO2 +
        # KlimaTicket gegen Mythen "ÖBB unzuverlässig", "KlimaTicket
        # gescheitert", "Klimakleber sollen Lkw blockieren".
        if claim_mentions_transport_cached(claim):
            tasks.append(cached("Verkehr AT", search_transport, analysis))
            queried_names.append("Verkehr Österreich (ÖBB + UBA + KlimaTicket)")
        # Esoterik / Pseudowissenschaft — kuratierte Skeptiker-Konsens-
        # Daten (GWUP, Cochrane, NHMRC, BfArM) für Themen, zu denen
        # mainstream-DBs entweder nichts oder thematisch unrelevante
        # Studien liefern (Heilsteine, Aura, Reinkarnation, Bioresonanz,
        # Astrologie, Chakren, Wünschelrute u.a.).
        if claim_mentions_esoterik_cached(claim):
            tasks.append(cached("Esoterik-Faktencheck", search_esoterik, analysis))
            queried_names.append("Esoterik-Faktencheck (GWUP + Cochrane + Skeptiker-Konsens)")
        # Geschichts-Faktencheck — kuratierte historische Konsens-Daten
        # (DÖW + USHMM + bpb + Bundesarchiv + Stiftung Berliner Mauer +
        # NIST + NASA) für Themen, zu denen mainstream-Wissenschafts-DBs
        # nichts liefern (Geschichts-Forschung ist in PubMed/Cochrane
        # nicht indiziert, Semantic Scholar findet zu DACH-spezifischen
        # Mythen oft nichts direkt Passendes). 18 Topics: NS-Mythen,
        # 20.-Jh.-Klassiker, aktuelle Geschichts-Mythen, Verschwörungs-
        # Geschichte (Mondlandung, 9/11, AIDS-CIA), sehr alte Mythen
        # (Wikinger-Hörner, Mittelalter-Erde-flach).
        if claim_mentions_geschichte_cached(claim):
            tasks.append(cached("Geschichts-Faktencheck", search_geschichte, analysis))
            queried_names.append("Geschichts-Faktencheck (DÖW + USHMM + bpb + Konsens)")
        # Verschwörungstheorien-Faktencheck — kuratierte Konsens-Daten
        # zu zeitgenössischen Verschwörungs-Narrativen mit klaren behörd-
        # lichen oder akademischen Konsens-Quellen (BVerfG, Verfassungs-
        # schutz AT/DE, ADL, IKG Wien, DÖW, IHRA Working Definition).
        # Methodische Disziplin: Wir bewerten spezifische faktische
        # Aussagen, machen keine eigenen Einstufungen, bewerten keine
        # Personen oder Bewegungen pauschal. Probe-Pack startet mit
        # 2 Topics (Reichsbürger/BRD-GmbH, Soros-EU-Steuerung).
        if claim_mentions_verschwoerungen_cached(claim):
            tasks.append(cached("Verschwoerungen-Faktencheck", search_verschwoerungen, analysis))
            queried_names.append("Verschwoerungen-Faktencheck (BVerfG + Verfassungsschutz + ADL/IKG/DÖW + IHRA)")
        # Tech-/KI-Faktencheck — kuratierte Konsens-Daten zu klassischen
        # Tech-/KI-Mythen (KI-Bewusstsein, Bitcoin-Anonymitaet, Quanten-
        # computer-Verschluesselung, Anonymisierung, VPN, Apple/Android).
        # 6 Topics; ergaenzt Wissenschafts-DBs (PubMed/SemanticScholar/
        # OpenAlex), die schon ~95 % der Tech-Mythen abdecken — Pack ist
        # vor allem Robustheits- und Konsistenz-Anker.
        if claim_mentions_tech_ki_cached(claim):
            tasks.append(cached("Tech-/KI-Faktencheck", search_tech_ki, analysis))
            queried_names.append("Tech-/KI-Faktencheck (NIST + EFF + ACM + Tech-Konsens)")
        # Gesundheits-Autoritaeten (NIH + CDC + BfR + WHO IARC) — Konsens-
        # Anker fuer klassische Gesundheits-/Lebensmittel-Mythen
        # (Impf-Autismus, Fluorid, Cannabis, rotes Fleisch, Acrylamid,
        # Mikroplastik, Glyphosat, Aspartam, BPA, Vitamin-Supplemente).
        # 10 Topics; bei Bewertungs-Uneinigkeit (Glyphosat, Aspartam) wird
        # die Uneinigkeit selbst als Faktum geliefert.
        if claim_mentions_gesundheits_autoritaeten_cached(claim):
            tasks.append(cached("Gesundheits-Autoritaeten",
                                search_gesundheits_autoritaeten, analysis))
            queried_names.append("Gesundheits-Autoritäten (NIH + CDC + BfR + WHO IARC)")
        # DESTATIS — Statistisches Bundesamt Deutschland — DE-Baseline
        # Indikatoren (Bevoelkerung, Inflation, Arbeitslos, Geburten,
        # BIP, Lebenserwartung). 6 Topics. Komplementaer zu Eurostat
        # (EU-Aggregate) und dach_factbook (Debatten-Topics).
        if claim_mentions_destatis_cached(claim):
            tasks.append(cached("DESTATIS", search_destatis, analysis))
            queried_names.append("DESTATIS — Statistisches Bundesamt Deutschland")
        # Tier-/Natur-Mythen-Pack (Goldfisch-Gedaechtnis, Stier-Rot,
        # Fledermaus-blind, Hund-s/w, Spinnen-im-Schlaf, Lemming-Suizid,
        # Hai-Angriffe, Eskimo-Schnee, Strauss-Sand, Elefant-Maus).
        # 10 Topics aus klassischer Tier-/Natur-Halbwahrheit-Repertoire.
        if claim_mentions_tier_natur_cached(claim):
            tasks.append(cached("Tier-/Natur-Mythen", search_tier_natur, analysis))
            queried_names.append("Tier-/Natur-Mythen (Smithsonian + AMNH + Britannica + Snopes)")
        # Ernaehrungs-Mythen-Pack — populaere Lebensmittel-Halbwahrheiten
        # (5-Sek-Regel, Spinat-Eisen, Eier-Cholesterin, Detox, Mikrowelle,
        # brauner Zucker, Bio-Pestizide, Kaffee-Dehydration, Milch-Schleim,
        # Karotten-Sehkraft). 10 Topics. Komplementaer zu
        # gesundheits_autoritaeten (BfR/CDC-spezifische Stoff-Risiken).
        if claim_mentions_ernaehrung_cached(claim):
            tasks.append(cached("Ernährungs-Mythen", search_ernaehrung, analysis))
            queried_names.append("Ernährungs-Mythen (DGE + Cochrane + Mayo + Harvard Chan + NHS + EFSA)")
        # Recht/Rechtsmythen-Pack — populaere rechtliche Halbwahrheiten in
        # DACH (Notwehr, Mietrecht-Schimmel, Schoenheitsreparaturen,
        # Probezeit, Screenshot-Recht, Hausrecht, Erbrecht-Pflichtteil,
        # Pause-Bezahlung, Kuendigungsschutz-Kleinbetrieb, Hund-Haftung).
        # 10 Topics. Pack ersetzt KEINE anwaltliche Beratung.
        if claim_mentions_recht_cached(claim):
            tasks.append(cached("Recht/Rechtsmythen", search_recht, analysis))
            queried_names.append("Recht/Rechtsmythen (RIS + BGBl + BGH/OGH + AK + Verbraucherzentrale)")
        # Energie-/Klima-Politik-Pack — populaere Energie- + Klimaschutz-
        # politische Halbwahrheiten (Atomkraft-CO2, China-Whataboutism,
        # E-Auto-Lifecycle, Waermepumpen-Frost, Solar-Recycling,
        # Windkraft-Voegel, Versorgungssicherheit-Erneuerbare, Diesel-
        # Skandal). 8 Topics. Quellen: IPCC + IEA + Fraunhofer + UBA + JRC.
        if claim_mentions_energie_klima_cached(claim):
            tasks.append(cached("Energie/Klima-Politik", search_energie_klima, analysis))
            queried_names.append("Energie/Klima-Politik (IPCC + IEA + Fraunhofer + UBA + JRC + EEA)")
        # Migrations-Pack — methodische Disziplin: behoerdliche Einstufungen
        # zitieren, NICHT eigene Wertungen; bei PKS Methodik-Caveats; bei
        # MIXED-Bewertungen Differenzierung explizit. 6 Topics:
        # demographic_replacement (BfV+bpb), migration_kriminalitaet
        # (PKS-Caveats), sozialmagnet_asyl (IAB-BAMF-SOEP), abschiebungen
        # (BAMF-Statistik), integration_gescheitert (IAB+OECD), asyl_arbeit
        # (§ 61 AsylG).
        if claim_mentions_migration_cached(claim):
            tasks.append(cached("Migrations-Konsens", search_migration, analysis))
            queried_names.append("Migrations-Konsens (BfV + bpb + IAB + DIW + OECD + BKA + Mediendienst)")
        # Geographie-/Reise-Mythen — populaere geo-Halbwahrheiten
        # (Bermudadreieck, Chinesische-Mauer-Mond, Everest-hoechster,
        # Toilette-Coriolis, Sahara-groesste-Wueste, Australien-giftigste-
        # Tiere, Vatikan-kleinster-Staat). 7 Topics. Politisch unauffaellig,
        # primaer unterhaltsam-bildend.
        if claim_mentions_geographie_cached(claim):
            tasks.append(cached("Geographie-Mythen", search_geographie, analysis))
            queried_names.append("Geographie-/Reise-Mythen (NASA + Lloyd's + USCG + NatGeo + CIA Factbook + UNESCO)")
        # Eurobarometer — kuratierte Eckwerte zu EU-Buerger-Einstellungen
        # (EU-Vertrauen, Top-Themen, Klimawandel-Einstellung, Demokratie-
        # Zufriedenheit, EU-Mitgliedschaft, Einwanderungs-Einstellung).
        # 6 Topics. Pack feuerte bei Aussagen mit 'die Buerger / die
        # Mehrheit der Europaeer wollen X'.
        if claim_mentions_eurobarometer_cached(claim):
            tasks.append(cached("Eurobarometer", search_eurobarometer, analysis))
            queried_names.append("Eurobarometer (Europäische Kommission + EP)")
        # Geld/Finanzen-Mythen-Pack — populaere Finanz-Halbwahrheiten
        # (Aktien-Gluecksspiel, Inflation-geheim, Zinseszins, Riester,
        # kalte Progression, Krypto-Pyramide, Negativzins). 7 Topics.
        if claim_mentions_finanzen_cached(claim):
            tasks.append(cached("Finanzen-Mythen", search_finanzen, analysis))
            queried_names.append("Finanzen-Mythen (EZB + Bundesbank + DAI + Stiftung Warentest + BaFin)")
        # Bildungs-Mythen-Pack — paedagogische + neuro-didaktische
        # Halbwahrheiten (Lernstile, Mozart-Effekt, Hirn-Haelften,
        # Multitasking, 10%-Gehirn, frueh-lernen, Hattie). 7 Topics.
        # Hohe Lehrer-Relevanz fuer User-Profil (BORG Guntramsdorf).
        if claim_mentions_bildung_cached(claim):
            tasks.append(cached("Bildungs-Mythen", search_bildung, analysis))
            queried_names.append("Bildungs-Mythen (APA + Hattie + EEF + Pashler 2008 + Nielsen 2013 + OECD ECE)")
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

        # Step 3: Synthesize results with Mistral.
        # Streaming variant: we run synthesize_results in a background task
        # with an on_chunk callback that pushes deltas onto an asyncio.Queue,
        # and emit them as 'synth_chunk' SSE events to the frontend. This
        # turns the ~11 s synthesizer wait from a frozen spinner into a live
        # progress indicator (first chunk typically arrives in <1 s).
        yield {"event": "step", "data": json.dumps({"step": "synthesize"})}
        try:
            logger.info(f"Synthesizing {len(valid_results)} source results")

            chunk_queue: asyncio.Queue = asyncio.Queue()
            DONE = object()

            async def _on_chunk(text: str) -> None:
                await chunk_queue.put(text)

            async def _synth_runner():
                try:
                    return await synthesize_results(
                        claim, analysis, valid_results, lang=lang,
                        on_chunk=_on_chunk,
                    )
                finally:
                    await chunk_queue.put(DONE)

            synth_task = asyncio.create_task(_synth_runner())

            chars_streamed = 0
            while True:
                item = await chunk_queue.get()
                if item is DONE:
                    break
                chars_streamed += len(item)
                yield {
                    "event": "synth_chunk",
                    "data": json.dumps({
                        "delta": item,
                        "total_chars": chars_streamed,
                    }, ensure_ascii=False),
                }

            synthesis = await synth_task
            logger.info(
                f"Synthesis verdict: {synthesis.get('verdict')} "
                f"(streamed {chars_streamed} chars)"
            )
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
        # Hebel #4: Verdict in den Semantic-Cache schreiben (TTL 30 Min,
        # nur wenn Confidence ≥ 0.8 + Verdict != unverifiable). Filtert
        # Stream-Loss-Artefakte aus.
        try:
            _vc.put(claim, synthesis)
        except Exception as e:
            logger.warning(f"verdict_cache.put failed (non-blocking): {e}")
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
