from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
import asyncio
import json
import logging
import os
import time
import traceback

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
from services.cache import get as cache_get, put as cache_put
from services.synthesizer import synthesize_results
from services.ner import enrich_entities

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


class ClaimRequest(BaseModel):
    claim: str


# Rate limiting: max requests per IP per window
RATE_LIMIT = int(os.getenv("RATE_LIMIT", "10"))
RATE_WINDOW = int(os.getenv("RATE_WINDOW", "60"))  # seconds
_rate_store: dict[str, list[float]] = {}


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
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Zu viele Anfragen. Bitte warte einen Moment.")

    body = await request.json()
    claim = body.get("claim", "").strip()
    lang = body.get("lang", "de") if body.get("lang") in ("de", "en") else "de"
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
        except Exception as e:
            logger.error(f"Claim analysis failed: {traceback.format_exc()}")
            yield {"event": "error", "data": json.dumps({"detail": f"Fehler bei der Claim-Analyse (ist Ollama gestartet?): {e}"})}
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
        pubmed_categories = {"health", "climate", "medication", "demographics", "other"}
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

        results = await asyncio.gather(*tasks, return_exceptions=True)
        valid_results = []
        sources_with_results = []
        hit_names = []
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                logger.warning(f"Source {i} ({queried_names[i]}) failed: {r}")
            else:
                logger.info(f"Source {i} ({queried_names[i]}) returned {len(r.get('results', []))} results")
                valid_results.append(r)
                if r.get("results"):
                    sources_with_results.append(r)
                    hit_names.append(queried_names[i])

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
        except Exception as e:
            logger.error(f"Synthesis failed: {traceback.format_exc()}")
            yield {"event": "error", "data": json.dumps({"detail": f"Fehler bei der Ergebnis-Synthese: {e}"})}
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

    return EventSourceResponse(event_stream())


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
