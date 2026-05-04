"""FactCheck.org — Annenberg Public Policy Center, University of
Pennsylvania, RSS-Feed.

US-basiert, gegründet 2003. IFCN-zertifiziert. Universitäts-getragen
(non-partisan). Schwerpunkte: US-politische Statements (Präsidenten-
Rhetorik, Kongress-Behauptungen), Wahlkampagnen-Claims, Gesundheits-
politik.

Komplementär zu Snopes: FactCheck.org-Stil ist akademisch-formaler,
mehr Transcript-Zitate, oft tiefere Quellen-Belege.

Trigger: alle Claims mit non-empty entities ODER queries.
"""

from services._factcheck_rss import search_factcheck_rss

FEED_URL = "https://www.factcheck.org/feed/"


async def search_factcheck_org(analysis: dict) -> dict:
    return await search_factcheck_rss(
        feed_url=FEED_URL,
        name="FactCheck.org",
        source_label="FactCheck.org (Annenberg Public Policy Center, Univ. Pennsylvania)",
        indicator="factcheck_org_item",
        country="USA",
        analysis=analysis,
    )
