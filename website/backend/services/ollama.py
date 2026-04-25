import asyncio
import httpx
import logging
import os

logger = logging.getLogger("evidora")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_API_URL = "https://api.mistral.ai/v1/chat/completions"
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-small-latest")
MAX_RETRIES = 3
RETRY_DELAY = 2  # Sekunden


async def _call_mistral_api(messages: list, timeout: float) -> str:
    """Call Mistral API (EU servers, Paris)."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            MISTRAL_API_URL,
            headers={
                "Authorization": f"Bearer {MISTRAL_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": MISTRAL_MODEL,
                "messages": messages,
                "temperature": 0.1,
                "max_tokens": 2048,
            },
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
        return payload["choices"][0]["message"]["content"]


async def _call_ollama(messages: list, timeout: float) -> str:
    """Call local Ollama instance."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            f"{OLLAMA_URL}/v1/chat/completions",
            json={
                "model": "mistral",
                "messages": messages,
                "temperature": 0.1,
                "max_tokens": 2048,
            },
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]


async def chat_completion(messages: list, timeout: float = 90.0) -> str:
    last_error = None
    use_cloud = bool(MISTRAL_API_KEY)

    if use_cloud:
        logger.info("Using Mistral API (cloud)")
    else:
        logger.info("Using Ollama (local)")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if use_cloud:
                return await _call_mistral_api(messages, timeout)
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
