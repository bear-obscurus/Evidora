"""Full Fact — UK-basiertes unabhängiges Faktencheck-Charity, RSS-Feed.

Gegründet 2009. IFCN-zertifiziert. Schwerpunkte: UK-Politik, Statistiken
in Medien, Gesundheits-Claims, Migrations-Statistiken.

Trigger: alle Claims mit non-empty entities ODER queries.
"""

from services._factcheck_rss import search_factcheck_rss

FEED_URL = "https://fullfact.org/feed/"


async def search_full_fact(analysis: dict) -> dict:
    return await search_factcheck_rss(
        feed_url=FEED_URL,
        name="Full Fact",
        source_label="Full Fact (unabhängiges Faktencheck-Charity UK, IFCN-zertifiziert)",
        indicator="full_fact_factcheck_item",
        country="UK",
        analysis=analysis,
    )
