"""Bellingcat — Open-Source-Investigative-Journalismus, RSS-Feed.

UK/global, gegründet 2014 von Eliot Higgins. Schwerpunkte: Geolocation
+ Visuals-Verifikation, Konflikt-Berichterstattung (Ukraine, Syrien,
Jemen), Cyber-Kriminalität, OSINT-Methodologie.

Komplementär zu klassischen Faktencheckern: Bellingcat investigiert oft
Geopolitik-/Visuals-Claims, die GADMO/Mimikama nicht abdecken.

Trigger: alle Claims mit non-empty entities ODER queries.
"""

from services._factcheck_rss import search_factcheck_rss

FEED_URL = "https://www.bellingcat.com/feed/"


async def search_bellingcat(analysis: dict) -> dict:
    return await search_factcheck_rss(
        feed_url=FEED_URL,
        name="Bellingcat",
        source_label="Bellingcat (Open-Source-Investigative-Journalismus, OSINT)",
        indicator="bellingcat_investigation_item",
        country="UK",
        analysis=analysis,
    )
