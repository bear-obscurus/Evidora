"""Eurostat Live-API Connector (SDMX 2.1 / JSON-stat 2.0).

Eurostat liefert offene Daten über einen REST-Endpoint:
  https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset_id}?{params}

Die Antwort kommt im JSON-stat-2.0-Format. Dieser Helper kapselt:
  - Endpoint-Aufbau mit URL-encoded Filtern
  - JSON-stat-Parser (Dimension/Index → flacher Wertvektor)
  - Cache mit 24h-TTL pro (dataset_id, filter_kombination)

Verwendung:

    from services._eurostat_live import fetch_eurostat_dataset

    data = await fetch_eurostat_dataset(
        "crim_hom_soff",
        geo=["AT", "DE", "EU27_2020"],
        time=[2023, 2024],
    )
    # → list[dict] mit {geo, time, value} pro Eurostat-Zeile

Statische Roh-Anbindung — der Caller entscheidet, was er mit den Werten
macht (refresh des JSON-Files via tools/refresh_eurostat_data.py, oder
Live-Sub-Result-Ergänzung in einem Service-Pfad).
"""

import asyncio
import logging
import time
from urllib.parse import urlencode

import httpx

logger = logging.getLogger("evidora")

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
CACHE_TTL = 24 * 3600  # 24 h

# Cache: key=(dataset_id, frozenset(filters)) → (timestamp, parsed_data)
_cache: dict[tuple, tuple[float, list[dict]]] = {}


def _cache_key(dataset_id: str, filters: dict) -> tuple:
    norm = tuple(sorted(
        (k, tuple(v) if isinstance(v, (list, tuple)) else (v,))
        for k, v in filters.items()
    ))
    return (dataset_id, norm)


def _parse_jsonstat(payload: dict) -> list[dict]:
    """Parse a JSON-stat 2.0 response into flat list[dict] rows.

    Each row has the dimension values + the numeric ``value``. Returns
    empty list on schema mismatch.
    """
    if not isinstance(payload, dict):
        return []
    dim_ids = payload.get("id") or []
    sizes = payload.get("size") or []
    dims = payload.get("dimension") or {}
    values = payload.get("value") or {}
    if not dim_ids or not sizes or not values:
        return []

    # Build per-dimension index→label-code mapping
    dim_index_to_code: list[list[str]] = []
    for dim_id in dim_ids:
        d = dims.get(dim_id) or {}
        cat = d.get("category") or {}
        index = cat.get("index") or {}
        # index can be {"code": pos} or {"code1": 0, ...}
        if isinstance(index, dict):
            ordered = sorted(index.items(), key=lambda kv: kv[1])
            dim_index_to_code.append([code for code, _ in ordered])
        elif isinstance(index, list):
            dim_index_to_code.append(list(index))
        else:
            dim_index_to_code.append([])

    rows: list[dict] = []
    # values is sparse dict: {flat_index_str: number}
    if isinstance(values, dict):
        for flat_idx_str, val in values.items():
            try:
                flat_idx = int(flat_idx_str)
            except ValueError:
                continue
            row = {}
            remaining = flat_idx
            for dim_pos in range(len(dim_ids) - 1, -1, -1):
                size = sizes[dim_pos]
                pos = remaining % size
                remaining //= size
                codes = dim_index_to_code[dim_pos]
                row[dim_ids[dim_pos]] = (codes[pos] if pos < len(codes) else None)
            row["value"] = val
            rows.append(row)
    elif isinstance(values, list):
        # dense format
        ranges = []
        rem = 1
        for s in sizes:
            ranges.append(s)
            rem *= s
        for flat_idx in range(len(values)):
            row = {}
            remaining = flat_idx
            for dim_pos in range(len(dim_ids) - 1, -1, -1):
                size = sizes[dim_pos]
                pos = remaining % size
                remaining //= size
                codes = dim_index_to_code[dim_pos]
                row[dim_ids[dim_pos]] = (codes[pos] if pos < len(codes) else None)
            row["value"] = values[flat_idx]
            rows.append(row)
    return rows


async def fetch_eurostat_dataset(
    dataset_id: str,
    *,
    client: httpx.AsyncClient | None = None,
    timeout: float = 30.0,
    **filters,
) -> list[dict]:
    """Hole einen Eurostat-Dataset-Slice via REST-API.

    Args:
        dataset_id: z.B. "crim_hom_soff"
        **filters: Eurostat-Dimension-Filter, z.B. ``geo=["AT"]``,
                   ``time=[2024]``, ``unit=["NR"]``.
                   Listen werden zu mehreren ``?param=v1&param=v2``-Eintraegen.

    Returns: list[dict] mit den ge-flatteten Zeilen, plus 'value'-Key.
    """
    # Filter normalisieren
    cleaned: dict[str, list[str]] = {}
    for k, v in filters.items():
        if isinstance(v, (list, tuple)):
            cleaned[k] = [str(x) for x in v]
        else:
            cleaned[k] = [str(v)]

    key = _cache_key(dataset_id, cleaned)
    now = time.time()
    cached = _cache.get(key)
    if cached and (now - cached[0]) < CACHE_TTL:
        logger.debug(f"eurostat_live: cache hit {dataset_id}")
        return cached[1]

    # URL bauen — Eurostat akzeptiert mehrfache ?key=val für Listen
    qs_parts = []
    for k, vs in cleaned.items():
        for v in vs:
            qs_parts.append((k, v))
    url = f"{BASE_URL}/{dataset_id}?{urlencode(qs_parts)}"

    own_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        own_client = True
    try:
        resp = await client.get(url, timeout=timeout,
                                headers={"Accept": "application/json"})
        if resp.status_code != 200:
            logger.warning(
                f"eurostat_live: {dataset_id} → HTTP {resp.status_code}"
            )
            return cached[1] if cached else []
        rows = _parse_jsonstat(resp.json())
        _cache[key] = (now, rows)
        logger.info(f"eurostat_live: fetched {dataset_id} → {len(rows)} rows")
        return rows
    except Exception as e:
        logger.warning(f"eurostat_live: {dataset_id} fetch failed: {e}")
        return cached[1] if cached else []
    finally:
        if own_client:
            await client.aclose()


def filter_rows(rows: list[dict], **constraints) -> list[dict]:
    """Convenience: filter eine Eurostat-Zeilen-Liste nach Dimension=Wert."""
    out = []
    for r in rows:
        if all(str(r.get(k)) == str(v) for k, v in constraints.items()):
            out.append(r)
    return out
