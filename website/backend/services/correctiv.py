"""Correctiv — gemeinnütziges DE-Recherchezentrum, RSS-Feed.

Investigative Journalismus + Faktenchecks. IFCN-zertifiziert. Bekannt
durch Aufdeckung u.a. Cum-Ex-Skandal, Potsdam-Treffen 2024.

Komplementär zu GADMO (das Correctiv aggregiert) — direkter Connector
ist trotzdem wertvoll wegen aktuellerer Items + investigativer Stories,
die nicht alle in GADMO landen.

Trigger: alle Claims mit non-empty entities ODER queries.
"""

from services._factcheck_rss import search_factcheck_rss

FEED_URL = "https://correctiv.org/feed/"


async def search_correctiv(analysis: dict) -> dict:
    return await search_factcheck_rss(
        feed_url=FEED_URL,
        name="Correctiv",
        source_label="Correctiv (gemeinnütziges Recherchezentrum DE, IFCN-zertifiziert)",
        indicator="correctiv_factcheck_item",
        country="DE",
        analysis=analysis,
    )
