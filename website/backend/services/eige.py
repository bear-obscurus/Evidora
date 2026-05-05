"""EIGE (European Institute for Gender Equality) — Live-RSS-Connector.

EIGE ist die methodisch zentrale EU-Agentur für Gleichstellungs-Daten:
- Gender Equality Index (jährlich seit 2013, EU-27 + Sub-Indikatoren)
- Gender Statistics Database (DGS)
- Forschungspublikationen + Berichte
- Newsroom mit aktuellen Themen + Politik-Updates

Free RSS-Feed ohne API-Key. Komplementär zum statischen
gleichstellung_pack:
- Pack: kuratierte Konsens-Daten zu 10 Topics (Pay Gap, Femizide,
  EIGE Index, etc.) — stabile Faktenlage
- EIGE Live: aktuelle Themen aus dem Newsroom (neue EIGE-Berichte,
  EU-Direktiven-Updates, Politik-Initiativen)

Trigger: claim hat entities oder pubmed/factcheck_queries — der
RSS-Helper filtert clientseitig nach Entity/Query-Match.

Pattern-Quelle: services/_factcheck_rss.py (Snopes, Correctiv, etc.)
— gleicher Helper, neue Wrapper-Funktion. Keine eigene Trigger-
Logik, weil Helper die Filterung übernimmt.

Wiring: main.py imports + tasks.append, reranker (NICHT in
AUTHORITATIVE_INDICATORS — ist Live-Quelle, kein kuratierter
Pack-Konsens).

RSS-Feed: https://eige.europa.eu/rss.xml (redirect →
https://eige.europa.eu/newsroom/news/rss.xml)
"""

from services._factcheck_rss import search_factcheck_rss

FEED_URL = "https://eige.europa.eu/newsroom/news/rss.xml"


async def search_eige(analysis: dict) -> dict:
    return await search_factcheck_rss(
        feed_url=FEED_URL,
        name="EIGE",
        source_label="EIGE European Institute for Gender Equality (EU-Agentur Vilnius, jährlicher Gender Equality Index)",
        indicator="eige_news_item",
        country="EU",
        analysis=analysis,
    )
