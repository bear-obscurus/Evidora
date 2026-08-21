import asyncio
import json
import httpx
import logging
import os
import time
from typing import AsyncIterator, Callable, Awaitable

logger = logging.getLogger("evidora")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_API_URL = "https://api.mistral.ai/v1/chat/completions"
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-small-latest")
MAX_RETRIES = 3
RETRY_DELAY = 2  # Sekunden

# --- LLM-Auth-Probe (QA50D HOCH 2) -------------------------------------
# Am 28./29.07.2026 war Evidora ~12 h vollstaendig ausgefallen: der
# Mistral-Key lieferte 401, JEDER Claim brach im Analyze-Schritt ab — und
# nichts hat es gemeldet. Der Container-Healthcheck rief /api/legal auf,
# einen statischen Endpunkt ohne LLM-Beteiligung, und meldete durchgehend
# "healthy". Diese Probe schliesst genau diese Luecke.
#
# Bewusste Design-Entscheidungen:
#  - /v1/models statt einer Completion: verbraucht KEINE Tokens.
#  - Ergebnis gecacht (TTL 300 s), damit der 30-s-Healthcheck die API
#    hoechstens alle 5 Minuten beruehrt (~288 Requests/Tag).
#  - TRI-STATE: nur 401/403 gelten als Fehler (persistente Auth-/
#    Kontingent-Klasse). Timeouts, Netzfehler, 429 und 5xx liefern
#    ``None`` = "unbekannt" und duerfen den Container NICHT unhealthy
#    machen — sonst flappt er bei jedem Upstream-Schluckauf.
#  - Auch das negative Ergebnis wird gecacht, sonst wuerde ein toter Key
#    bei jedem Healthcheck erneut angefragt.
MISTRAL_MODELS_URL = "https://api.mistral.ai/v1/models"
LLM_AUTH_TTL = 300  # Sekunden
LLM_AUTH_TIMEOUT = 5.0

_llm_auth_cache: dict = {"ts": 0.0, "ok": None, "detail": "noch nicht geprüft"}


async def check_llm_auth(force: bool = False) -> dict:
    """Prüft den LLM-Auth-Pfad. Returns ``{"ok": True|False|None,
    "detail": str, "cached": bool, "age_s": int}``.

    ``ok is False`` heißt: der Key wird von Mistral abgelehnt — das ist
    die Ausfallklasse, die am 29.07. unbemerkt blieb. ``ok is None``
    heißt: nicht entscheidbar (Netz/Timeout/Rate-Limit) und ist KEIN
    Alarm-Grund.
    """
    now = time.time()
    age = now - _llm_auth_cache["ts"]
    if not force and _llm_auth_cache["ts"] and age < LLM_AUTH_TTL:
        return {"ok": _llm_auth_cache["ok"],
                "detail": _llm_auth_cache["detail"],
                "cached": True, "age_s": int(age)}

    if not MISTRAL_API_KEY:
        result = (False, "MISTRAL_API_KEY nicht gesetzt")
    else:
        try:
            async with httpx.AsyncClient(timeout=LLM_AUTH_TIMEOUT) as client:
                r = await client.get(
                    MISTRAL_MODELS_URL,
                    headers={"Authorization": f"Bearer {MISTRAL_API_KEY}"})
            if r.status_code in (401, 403):
                # Merke (QA50D): Mistral beantwortet auch einen reinen
                # KONTINGENT-Zustand mit 401 — derselbe unveraenderte Key
                # lieferte 10 Tage spaeter wieder 200. "401" heisst also
                # nicht zwingend "Key ungueltig", aber immer "Dienst
                # arbeitet gerade nicht".
                result = (False, f"HTTP {r.status_code} — Key abgelehnt "
                                 f"(ungültig ODER Kontingent erschöpft)")
            elif r.status_code == 200:
                result = (True, "HTTP 200")
            else:
                result = (None, f"HTTP {r.status_code} — transient")
        except Exception as e:
            result = (None, f"{type(e).__name__} — transient")

    _llm_auth_cache["ts"] = now
    _llm_auth_cache["ok"], _llm_auth_cache["detail"] = result
    if result[0] is False:
        logger.error(f"LLM-Auth-Probe: {result[1]} — Faktencheck-Pipeline "
                     f"ist funktionsunfähig")
    return {"ok": result[0], "detail": result[1], "cached": False, "age_s": 0}


# ---------------------------------------------------------------------------
# Token-Verbrauch (2026-08-21)
#
# Warum: Bis hierher wurde `usage` aus der Mistral-Antwort NIRGENDS
# ausgewertet — die Frage "was kostet ein Faktencheck" war nur aus dem
# Abrechnungs-Chart rueckrechenbar (~4,5 Cent/Claim, geschaetzt). Damit war
# auch unsichtbar, wie oft die Retry-Pfade in claim_analyzer/synthesizer
# feuern, obwohl jeder Retry den betreffenden Call verdoppelt.
#
# Diese Zeile ist bewusst maschinell auswertbar (feste key=value-Paare), damit
# Tagessummen ohne Zusatz-Tooling gehen:
#   docker logs evidora-backend-1 | grep llm_usage | \
#     awk -F'total_tokens=' '{s+=$2} END {print s}'
#
# Bewusst KEINE Euro-Schaetzung im Log: Preise aendern sich, ein
# hartkodierter Faktor wuerde still veralten und faktisch falsche Zahlen
# produzieren. Tokens sind die messbare Groesse, der Preis ist Politik.
USAGE_LOG_PREFIX = "llm_usage"


def _extract_usage(payload: dict | None) -> dict | None:
    """``usage``-Block aus einer Antwort/einem Stream-Chunk, oder None.

    Defensiv: Mistral liefert den Block bei Streams erst im LETZTEN Chunk,
    und der traegt ``choices: []`` — genau der Fall, den die Stream-Schleife
    vor diesem Commit uebersprungen hat.
    """
    if not isinstance(payload, dict):
        return None
    usage = payload.get("usage")
    return usage if isinstance(usage, dict) else None


def _log_usage(kind: str, model: str, usage: dict | None,
               attempt: int = 1, streamed: bool = False) -> None:
    """Eine Zeile pro LLM-Call. Enthaelt NIE Prompt-Inhalte oder Secrets —
    nur Zaehler, Modellname und den Aufruf-Typ."""
    stream_flag = "yes" if streamed else "no"
    if not usage:
        logger.info(
            f"{USAGE_LOG_PREFIX} kind={kind} model={model} attempt={attempt} "
            f"streamed={stream_flag} tokens=unbekannt"
        )
        return

    def _num(key: str) -> int:
        val = usage.get(key)
        return val if isinstance(val, int) and val >= 0 else 0

    prompt = _num("prompt_tokens")
    completion = _num("completion_tokens")
    total = _num("total_tokens") or (prompt + completion)
    logger.info(
        f"{USAGE_LOG_PREFIX} kind={kind} model={model} attempt={attempt} "
        f"streamed={stream_flag} prompt_tokens={prompt} "
        f"completion_tokens={completion} total_tokens={total}"
    )


async def _call_mistral_api(messages: list, timeout: float,
                             model: str | None = None,
                             json_mode: bool = False,
                             kind: str = "unknown",
                             attempt: int = 1) -> str:
    """Call Mistral API (EU servers, Paris).

    Determinism (Bug 1 fix):
    - ``temperature=0.0`` makes Mistral fully greedy.  At 0.1 the same
      claim could occasionally yield TRUE 100% on one run and
      UNVERIFIABLE 15% on another (observed in the 10-Kickl-claims
      verification round).  At 0.0 Mistral always picks the
      highest-probability token.
    - ``random_seed=42`` gives a stable seed when the model still
      tie-breaks (e.g. equal-probability tokens).  Same input → same
      output, reproducible across runs.

    Hebel #5 (JSON-Mode): with ``json_mode=True`` we add Mistral's
    ``response_format={"type": "json_object"}``. This makes the model
    emit syntactically-valid JSON without surrounding markdown code
    fences — saves ~3-8 tokens per response and eliminates the
    "stripped ```json prefix" parsing edge cases.  The system prompt
    must still mention "JSON" for the API to accept the request.
    """
    body: dict = {
        "model": model or MISTRAL_MODEL,
        "messages": messages,
        "temperature": 0.0,
        "max_tokens": 2048,
        "random_seed": 42,
    }
    if json_mode:
        body["response_format"] = {"type": "json_object"}
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            MISTRAL_API_URL,
            headers={
                "Authorization": f"Bearer {MISTRAL_API_KEY}",
                "Content-Type": "application/json",
            },
            json=body,
        )
        response.raise_for_status()
        # Defensive: Mistral can return 200 OK with an unexpected body shape
        # (e.g. {"error": ...} or HTML). Surface a clear error rather than
        # KeyError-bombing into the analyzer's "unparseable response" path.
        try:
            payload = response.json()
        except Exception as e:
            body_preview = response.text[:300]
            logger.error(f"Mistral 200 OK but body is not JSON: {body_preview!r}")
            raise ValueError(f"Mistral returned non-JSON 200 response: {body_preview[:120]}") from e
        if "error" in payload:
            err_msg = payload["error"]
            if isinstance(err_msg, dict):
                err_msg = err_msg.get("message", str(err_msg))
            logger.error(f"Mistral 200 OK with error field: {err_msg}")
            # Mistral sometimes returns 200 with payment/quota errors in body
            if any(s in str(err_msg).lower() for s in ("credit", "quota", "payment", "billing")):
                raise ValueError("MISTRAL_CREDITS_EXHAUSTED")
            raise ValueError(f"Mistral API error: {err_msg}")
        if "choices" not in payload or not payload["choices"]:
            logger.error(f"Mistral response missing 'choices': {str(payload)[:300]}")
            raise ValueError("Mistral returned response without 'choices' field")
        _log_usage(kind, model or MISTRAL_MODEL, _extract_usage(payload),
                   attempt=attempt, streamed=False)
        return payload["choices"][0]["message"]["content"]


async def _call_ollama(messages: list, timeout: float) -> str:
    """Call local Ollama instance."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            f"{OLLAMA_URL}/v1/chat/completions",
            json={
                "model": "mistral",
                "messages": messages,
                "temperature": 0.0,
                "max_tokens": 2048,
                "seed": 42,
            },
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]


async def chat_completion(messages: list, timeout: float = 90.0,
                          model: str | None = None,
                          json_mode: bool = False,
                          kind: str = "unknown") -> str:
    """Run a chat completion. ``model`` overrides MISTRAL_MODEL for this
    call only — used by the analyzer to optionally switch to a smaller
    model (e.g. mistral-tiny) for faster turnaround. Default keeps the
    env-var setting unchanged.

    ``json_mode`` activates Mistral's structured-output mode (Hebel #5).
    When True, the model emits a JSON object directly — no surrounding
    code fences, no preamble. Faster + more reliable parsing for the
    analyzer + synthesizer paths. System prompt must mention 'JSON' for
    Mistral to accept the request.
    """
    last_error = None
    use_cloud = bool(MISTRAL_API_KEY)

    if use_cloud:
        logger.info(f"Using Mistral API (cloud, model={model or MISTRAL_MODEL}, json_mode={json_mode})")
    else:
        logger.info("Using Ollama (local)")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if use_cloud:
                return await _call_mistral_api(
                    messages, timeout, model=model, json_mode=json_mode,
                    kind=kind, attempt=attempt,
                )
            else:
                return await _call_ollama(messages, timeout)
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                logger.warning(f"LLM attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying in {RETRY_DELAY}s...")
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error(f"LLM failed after {MAX_RETRIES} attempts: {e}")
        except httpx.HTTPStatusError as e:
            last_error = e
            logger.error(f"LLM API error: {e.response.status_code} — {e.response.text[:200]}")
            if e.response.status_code == 401:
                raise ValueError("Invalid MISTRAL_API_KEY") from e
            if e.response.status_code in (402, 429):
                # 402 = Payment Required (no credits), 429 = Rate limit / quota exceeded
                raise ValueError("MISTRAL_CREDITS_EXHAUSTED") from e
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
            else:
                raise

    raise last_error


# ---------------------------------------------------------------------------
# Streaming variant (token-by-token) — used by the synthesizer to emit
# SSE 'synth_chunk' events to the frontend so the user sees the
# generation progress instead of staring at a 10–15 s spinner.
# ---------------------------------------------------------------------------

async def _stream_mistral_api(
    messages: list, timeout: float, json_mode: bool = False,
    kind: str = "unknown", attempt: int = 1,
) -> AsyncIterator[str]:
    """Stream content tokens from Mistral's chat completion endpoint.

    Yields each non-empty ``delta.content`` string as it arrives. Same
    determinism settings as ``_call_mistral_api`` (temperature=0,
    seed=42).

    Hebel #5: ``json_mode=True`` adds response_format json_object —
    streamed deltas come without surrounding code fences.
    """
    body: dict = {
        "model": MISTRAL_MODEL,
        "messages": messages,
        "temperature": 0.0,
        "max_tokens": 2048,
        "random_seed": 42,
        "stream": True,
    }
    if json_mode:
        body["response_format"] = {"type": "json_object"}
    async with httpx.AsyncClient(timeout=timeout) as client:
        async with client.stream(
            "POST",
            MISTRAL_API_URL,
            headers={
                "Authorization": f"Bearer {MISTRAL_API_KEY}",
                "Content-Type": "application/json",
            },
            json=body,
        ) as resp:
            if resp.status_code in (402, 429):
                raise ValueError("MISTRAL_CREDITS_EXHAUSTED")
            resp.raise_for_status()
            usage_seen: dict | None = None
            async for line in resp.aiter_lines():
                if not line or not line.startswith("data: "):
                    continue
                payload = line[6:].strip()
                if payload == "[DONE]":
                    break
                try:
                    chunk = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                # WICHTIG: vor dem choices-Skip. Mistral liefert den
                # usage-Block im LETZTEN Chunk, und der traegt
                # choices: [] — die alte Schleife hat ihn verworfen.
                usage_seen = _extract_usage(chunk) or usage_seen
                choices = chunk.get("choices") or []
                if not choices:
                    continue
                delta = choices[0].get("delta") or {}
                content = delta.get("content")
                if content:
                    yield content
            _log_usage(kind, MISTRAL_MODEL, usage_seen,
                       attempt=attempt, streamed=True)


async def chat_completion_streaming(
    messages: list,
    on_chunk: Callable[[str], Awaitable[None]] | None = None,
    timeout: float = 300.0,
    json_mode: bool = False,
    kind: str = "unknown",
) -> str:
    """Streaming variant of ``chat_completion``.

    Calls Mistral with ``stream=true``. For every content delta, invokes
    ``on_chunk(text)`` (if given) and accumulates the full response.
    Returns the full content string at the end — same as
    ``chat_completion`` for downstream JSON-parsing compatibility.

    Falls back to the non-streaming Ollama path if no Mistral key is set
    (Ollama streaming would require its own implementation; for the
    local-dev case the user-experience win is smaller anyway).
    """
    use_cloud = bool(MISTRAL_API_KEY)
    if not use_cloud:
        full = await _call_ollama(messages, timeout)
        if on_chunk:
            await on_chunk(full)  # single chunk, end of stream
        return full

    parts: list[str] = []
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async for chunk in _stream_mistral_api(
                messages, timeout, json_mode=json_mode,
                kind=kind, attempt=attempt,
            ):
                parts.append(chunk)
                if on_chunk:
                    await on_chunk(chunk)
            return "".join(parts)
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            last_error = e
            parts.clear()
            if attempt < MAX_RETRIES:
                logger.warning(
                    f"Mistral streaming attempt {attempt}/{MAX_RETRIES} "
                    f"failed: {e}. Retrying in {RETRY_DELAY}s..."
                )
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error(
                    f"Mistral streaming failed after {MAX_RETRIES} attempts: {e}"
                )
                raise
        except httpx.HTTPStatusError as e:
            logger.error(
                f"Mistral streaming HTTP error: {e.response.status_code}"
            )
            if e.response.status_code == 401:
                raise ValueError("Invalid MISTRAL_API_KEY") from e
            if e.response.status_code in (402, 429):
                raise ValueError("MISTRAL_CREDITS_EXHAUSTED") from e
            raise
    raise last_error
