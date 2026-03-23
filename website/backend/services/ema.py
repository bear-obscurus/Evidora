import httpx
import time

MEDICINES_URL = "https://www.ema.europa.eu/sites/default/files/Medicines_output_medicines_en.json"

_cache: dict = {"data": None, "timestamp": 0}
CACHE_TTL = 3600  # 1 Stunde


async def _get_medicines(client: httpx.AsyncClient) -> list:
    now = time.time()
    if _cache["data"] is not None and (now - _cache["timestamp"]) < CACHE_TTL:
        return _cache["data"]

    resp = await client.get(MEDICINES_URL)
    resp.raise_for_status()
    _cache["data"] = resp.json()
    _cache["timestamp"] = now
    return _cache["data"]


async def search_ema(analysis: dict) -> dict:
    entities = analysis.get("entities", [])
    if not entities:
        return {"source": "EMA", "results": []}

    async with httpx.AsyncClient(timeout=30.0) as client:
        medicines = await _get_medicines(client)

        results = []
        search_terms = [e.lower() for e in entities]

        for med in medicines:
            med_name = med.get("medicineName", "").lower()
            active = med.get("activeSubstance", "").lower()

            for term in search_terms:
                if term in med_name or term in active:
                    results.append(
                        {
                            "name": med.get("medicineName", ""),
                            "active_substance": med.get("activeSubstance", ""),
                            "status": med.get("authorisationStatus", ""),
                            "category": med.get("category", ""),
                            "condition": med.get("condition", ""),
                            "url": med.get("url", ""),
                        }
                    )
                    break

            if len(results) >= 5:
                break

        return {"source": "EMA (European Medicines Agency)", "type": "official_data", "results": results}
