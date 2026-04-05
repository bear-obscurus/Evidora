"""Background data updater — prefetches external data on startup and refreshes periodically.

Managed sources:
- OWID COVID latest (24h refresh)
- NASA GISS temperature anomalies (7-day refresh)
- GADMO fact-check feeds + embeddings (1h refresh)
- EUvsDisinfo RSS feed + embeddings (1h refresh)
- DataCommons ClaimReview index (24h refresh)

Note: EUvsDisinfo case database (14.5K cases) is a static JSON file
shipped with the application — no download or refresh needed.
"""

import asyncio
import logging

import httpx

from services.ecdc import _fetch_owid_latest, OWID_CACHE_TTL
from services.copernicus import _fetch_nasa_giss, GISS_CACHE_TTL
from services.gadmo import prefetch_feeds, FEED_CACHE_TTL
from services.euvsdisinfo import prefetch_feed as prefetch_euvsdisinfo
from services.datacommons import update_index as update_datacommons

logger = logging.getLogger("evidora")

_background_task: asyncio.Task | None = None


async def prefetch_all():
    """Fetch all external data sources once (called at startup)."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        results = await asyncio.gather(
            _fetch_owid_latest(client),
            _fetch_nasa_giss(client),
            prefetch_feeds(),
            prefetch_euvsdisinfo(),
            update_datacommons(),
            return_exceptions=True,
        )
        names = ["OWID COVID", "NASA GISS", "GADMO Feeds", "EUvsDisinfo RSS", "DataCommons"]
        for i, name in enumerate(names):
            if isinstance(results[i], Exception):
                logger.warning(f"Startup prefetch {name} failed: {results[i]}")
            elif results[i] is not None:
                count = len(results[i]) if results[i] else 0
                logger.info(f"Startup prefetch {name}: {count} records cached")


async def _refresh_loop():
    """Periodically refresh cached data in the background."""
    # GADMO feeds refresh every hour, CSV data less frequently.
    # Each source checks its own TTL internally, so we use the shortest interval.
    interval = min(OWID_CACHE_TTL, GISS_CACHE_TTL, FEED_CACHE_TTL)
    while True:
        await asyncio.sleep(interval)
        logger.info("Background data refresh starting")
        try:
            await prefetch_all()
        except Exception as e:
            logger.error(f"Background data refresh failed: {e}")


def start_background_updates():
    """Start the background refresh loop (call once from app startup)."""
    global _background_task
    if _background_task is None or _background_task.done():
        _background_task = asyncio.create_task(_refresh_loop())
        logger.info("Background data updater started")


def stop_background_updates():
    """Cancel the background refresh loop (call from app shutdown)."""
    global _background_task
    if _background_task and not _background_task.done():
        _background_task.cancel()
        logger.info("Background data updater stopped")
