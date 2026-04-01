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
from services.claimreview import search_claimreview
from services.copernicus import search_copernicus
from services.eurostat import search_eurostat
from services.eea import search_eea
from services.ecdc import search_ecdc
from services.ecb import search_ecb
from services.unhcr import search_unhcr
from services.cochrane import search_cochrane
from services.gadmo import search_gadmo
from services.oecd import search_oecd
from services.euvsdisinfo import search_euvsdisinfo, _is_disinfo_claim
from services.datacommons import search_datacommons
from services.who_europe import search_who_europe
from services.openalex import search_openalex
from services.worldbank import search_worldbank
from services.energy_safety import search_energy_safety, _is_energy_safety_claim
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
            logger.info(f"Analysis done: category={analysis.get('category')}, confidence={analysis.get('confidence')}, entities={analysis.get('entities')}")
        except ValueError as e:
            if "MISTRAL_CREDITS_EXHAUSTED" in str(e):
                logger.error("Mistral API credits exhausted")
                yield {"event": "error", "data": json.dumps({"detail": "MISTRAL_CREDITS_EXHAUSTED"})}
                return
            raise
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
        if analysis.get("oecd_relevant") or analysis.get("category") == "education":
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
        # OpenAlex covers all scientific disciplines — query for any claim with search terms
        if analysis.get("pubmed_queries"):
            tasks.append(cached("OpenAlex", search_openalex, analysis))
            queried_names.append("OpenAlex")

        # Use asyncio.wait so completed tasks return results even if others time out
        valid_results = []
        sources_with_results = []
        hit_names = []
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
                        if r.get("results"):
                            sources_with_results.append(r)
                            hit_names.append(queried_names[i])
                    except Exception as e:
                        logger.warning(f"Source {i} ({queried_names[i]}) failed: {e}")

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
            raise
        except Exception:
            logger.error("Synthesis failed", exc_info=True)
            yield {"event": "error", "data": json.dumps({"detail": "Fehler bei der Ergebnis-Synthese. Bitte erneut versuchen." if lang == "de" else "Synthesis failed. Please try again."})}
            return

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
