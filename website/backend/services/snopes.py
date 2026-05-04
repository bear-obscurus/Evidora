"""Snopes — größte EN-Faktencheck-Datenbank, RSS-Feed.

US-basiert, gegründet 1994 (David + Barbara Mikkelson). IFCN-zertifiziert
(International Fact-Checking Network). Hohe Trefferquote für viral-
verbreitete EN-Claims; oft erste Faktencheck-Quelle international.

Trigger: alle Claims mit non-empty entities ODER pubmed/factcheck-Queries
(Helper filtert clientseitig nach Entity/Query-Match).
"""

from services._factcheck_rss import search_factcheck_rss

FEED_URL = "https://www.snopes.com/feed/"


async def search_snopes(analysis: dict) -> dict:
    return await search_factcheck_rss(
        feed_url=FEED_URL,
        name="Snopes",
        source_label="Snopes (Mikkelson, IFCN-zertifiziert, USA)",
        indicator="snopes_factcheck_item",
        country="USA",
        analysis=analysis,
    )
