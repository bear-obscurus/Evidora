import asyncio
import httpx
import logging
import os

logger = logging.getLogger("evidora")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
MAX_RETRIES = 3
RETRY_DELAY = 2  # Sekunden


async def chat_completion(messages: list, timeout: float = 90.0) -> str:
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
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
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                logger.warning(f"Ollama attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying in {RETRY_DELAY}s...")
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error(f"Ollama failed after {MAX_RETRIES} attempts: {e}")

    raise last_error
