"""Background data updater — prefetches external data on startup and refreshes periodically.

Managed sources:
- OWID COVID latest (24h refresh)
- OWID measles cases + WUENIC vaccination coverage (24h refresh)
- NASA GISS temperature anomalies (7-day refresh)
- Berkeley Earth country/continent temperature anomalies via OWID (24h refresh)
- GADMO fact-check feeds + embeddings (1h refresh)
- EUvsDisinfo RSS feed + embeddings (1h refresh)
- DataCommons ClaimReview index (24h refresh)
- Statistik Austria VPI + Gesundheitsausgaben + Sterblichkeit + VGR + Arbeitsmarkt (24h refresh)
- V-Dem democracy indices (24h refresh)
- Transparency International CPI (24h refresh)
- RSF Press Freedom Index (24h refresh)
- SIPRI Military Expenditure (24h refresh)
- IDEA Voter Turnout (24h refresh)
- Parlament.gv.at Nationalrat composition (24h refresh)
- EEA / Eurostat environmental datasets (24h refresh, hot-geos prefetch)
- GeoSphere Austria klima-v2-1y annual station temperatures (24h refresh)
- BASG (Bundesamt für Sicherheit im Gesundheitswesen) news feed (1h refresh)
- BMI Volksbegehren list (24h refresh)
- BMI Wahlen — NRW + BPW + EUW Bundesergebnisse (static, no live refresh)
- Parlament Abstimmungen — NR-Voting-Records seit GP XXVI (static, manual refresh)

Note: EUvsDisinfo case database (14.5K cases) is a static JSON file
shipped with the application — no download or refresh needed.
"""

import asyncio
import logging

import httpx

from services.ecdc import _fetch_owid_latest, _fetch_measles, _fetch_vaccination, OWID_CACHE_TTL
from services.copernicus import _fetch_nasa_giss, _fetch_berkeley, GISS_CACHE_TTL, BERKELEY_CACHE_TTL
from services.gadmo import prefetch_feeds, FEED_CACHE_TTL
from services.euvsdisinfo import prefetch_feed as prefetch_euvsdisinfo
from services.datacommons import update_index as update_datacommons
from services.statistik_austria import fetch_vpi, fetch_health_expenditure, fetch_mortality, fetch_vgr, fetch_migration, fetch_naturalizations, fetch_arbeitsmarkt, fetch_armut
from services.vdem import fetch_vdem
from services.transparency import fetch_cpi
from services.rsf import fetch_rsf
from services.sipri import fetch_sipri
from services.idea import fetch_idea
from services.parlament_at import fetch_parlament_nr
from services.eea import prefetch_eea
from services.geosphere import fetch_geosphere
from services.basg import fetch_basg
from services.volksbegehren import fetch_volksbegehren
from services.wahlen import fetch_wahlen
from services.abstimmungen import fetch_abstimmungen
from services.at_factbook import fetch_at_factbook

logger = logging.getLogger("evidora")

_background_task: asyncio.Task | None = None


async def prefetch_all():
    """Fetch all external data sources once (called at startup)."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        results = await asyncio.gather(
            _fetch_owid_latest(client),
            _fetch_measles(client),
            _fetch_vaccination(client),
            _fetch_nasa_giss(client),
            _fetch_berkeley(client),
            prefetch_feeds(),
            prefetch_euvsdisinfo(),
            update_datacommons(),
            fetch_vpi(client),
            fetch_health_expenditure(client),
            fetch_mortality(client),
            fetch_vgr(client),
            fetch_migration(client),
            fetch_naturalizations(client),
            fetch_arbeitsmarkt(client),
            fetch_armut(client),
            fetch_vdem(client),
            fetch_cpi(client),
            fetch_rsf(client),
            fetch_sipri(client),
            fetch_idea(client),
            fetch_parlament_nr(client),
            prefetch_eea(client),
            fetch_geosphere(client),
            fetch_basg(client),
            fetch_volksbegehren(client),
            fetch_wahlen(client),
            fetch_abstimmungen(client),
            fetch_at_factbook(client),
            return_exceptions=True,
        )
        names = ["OWID COVID", "OWID Measles", "OWID Vaccination (WUENIC)",
                 "NASA GISS", "Berkeley Earth", "GADMO Feeds", "EUvsDisinfo RSS", "DataCommons",
                 "Statistik Austria VPI", "Statistik Austria Gesundheitsausgaben",
                 "Statistik Austria Sterblichkeit", "Statistik Austria VGR",
                 "Statistik Austria Migration", "Statistik Austria Einbürgerungen",
                 "Statistik Austria Arbeitsmarkt", "Statistik Austria EU-SILC",
                 "V-Dem", "Transparency International CPI", "RSF Press Freedom",
                 "SIPRI Military Expenditure", "IDEA Voter Turnout",
                 "Parlament.gv.at Nationalrat", "EEA / Eurostat",
                 "GeoSphere Austria", "BASG", "BMI Volksbegehren",
                 "BMI Wahlen", "Parlament Abstimmungen",
                 "AT Factbook"]
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
    interval = min(OWID_CACHE_TTL, GISS_CACHE_TTL, BERKELEY_CACHE_TTL, FEED_CACHE_TTL)
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
