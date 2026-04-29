"""AT-Faktencheck-RSS-Aggregator: APA + Kontrast.at + profil-Faktiv.

Aggregiert die drei wichtigsten österreichischen Faktencheck-Feeds zu
einer einheitlichen Quelle. Komplementär zum bestehenden GADMO-Feed-
Service, der schon eine Aggregation für DACH-Faktenchecks ist — dieser
Service liefert AT-Spezialisierung mit Direkt-Zugriff auf APA, Kontrast
und profil.

Datenquellen:
- APA-Faktencheck: https://apa.at/faktencheck/feed/ (Standard-RSS)
- Kontrast.at Faktencheck-Tag: https://kontrast.at/tag/faktencheck/feed/
- profil-Faktiv: über profil.at/feed/?tag=faktiv (allgemeiner Feed mit Faktiv-Tag)

Architektur folgt feed_aggregator-Pattern (siehe services/feeds.py):
- Parallel fetch via httpx
- Cache-Time 1h
- Reranker übernimmt die thematische Filterung
"""

import asyncio
import logging
import time
from xml.etree import ElementTree as ET

import httpx

logger = logging.getLogger("evidora")

FEED_CACHE_TTL = 3600  # 1h

FEEDS = [
    {
        "name": "APA-Faktencheck",
        "url": "https://apa.at/faktencheck/feed/",
        "country": "AT",
    },
    {
        "name": "Kontrast.at Faktencheck",
        "url": "https://kontrast.at/tag/faktencheck/feed/",
        "country": "AT",
    },
]

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.0 Safari/605.1.15"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml",
    "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
}

_cache: list[dict] | None = None
_cache_time: float = 0.0


def _parse_rss(xml_text: str, feed_meta: dict) -> list[dict]:
    """Parse RSS 2.0 feed, return list of items."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.warning(f"Failed to parse {feed_meta['name']} RSS: {e}")
        return []

    items: list[dict] = []
    for item in root.iter("item"):
        title_el = item.find("title")
        link_el = item.find("link")
        date_el = item.find("pubDate")
        desc_el = item.find("description")

        title = (title_el.text or "").strip() if title_el is not None else ""
        link = (link_el.text or "").strip() if link_el is not None else ""
        date = (date_el.text or "").strip() if date_el is not None else ""
        description = ""
        if desc_el is not None and desc_el.text:
            # Strip basic HTML/CDATA artifacts
            description = desc_el.text.strip()
            # Remove common HTML tags for clean preview
            import re as _re
            description = _re.sub(r"<[^>]+>", "", description)
            description = description[:300]

        if not title or not link:
            continue

        items.append({
            "title": title,
            "url": link,
            "date": date,
            "description": description,
            "source": feed_meta["name"],
            "country": feed_meta["country"],
        })

    return items


async def _fetch_one_feed(client: httpx.AsyncClient, feed_meta: dict) -> list[dict]:
    """Fetch one RSS feed; return parsed items or empty list on error."""
    try:
        response = await client.get(feed_meta["url"], headers=_BROWSER_HEADERS, timeout=15)
        response.raise_for_status()
        items = _parse_rss(response.text, feed_meta)
        logger.info(f"AT-Faktencheck-RSS: {feed_meta['name']} → {len(items)} items")
        return items
    except Exception as e:
        logger.warning(f"AT-Faktencheck-RSS fetch failed for {feed_meta['name']}: {e}")
        return []


async def fetch_at_faktencheck_rss(client: httpx.AsyncClient | None = None) -> list[dict]:
    """Prefetch entry-point. Returns aggregated items from all configured feeds."""
    global _cache, _cache_time

    now = time.time()
    if _cache is not None and (now - _cache_time) < FEED_CACHE_TTL:
        return _cache

    own_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=15.0)
        own_client = True

    try:
        all_items: list[dict] = []
        results = await asyncio.gather(
            *(_fetch_one_feed(client, f) for f in FEEDS),
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, list):
                all_items.extend(r)

        _cache = all_items
        _cache_time = now
        logger.info(f"AT-Faktencheck-RSS aggregated: {len(all_items)} items total")
        return all_items
    finally:
        if own_client:
            await client.aclose()


# ---------------------------------------------------------------------------
# Kuratiertes Archiv klassischer FPÖ-naher / Boulevard-Falschmeldungen
# ---------------------------------------------------------------------------
# APA, Kontrast und profil haben zu jedem dieser Themen mehrfach Faktenchecks
# publiziert; die Themen tauchen aber nicht laufend in den letzten ~20 Items
# auf. Diese Klassiker werden als authoritative Sub-Results vor dem Live-Feed
# eingespielt und gehen am Reranker (FACTCHECK_THRESHOLD = 0.55) vorbei.
_CURATED_AT_CLASSICS = (
    {
        "trigger": ("bargeldabschaffung", "bargeld abschaffung", "bargeldverbot",
                    "bargeld verboten", "bargeld wird abgeschafft",
                    "abschaffung bargeld",),
        "title": "Bargeldabschaffung in Österreich — klassischer Hoax",
        "description": ("APA-Faktencheck und Mimikama haben die wiederkehrende "
                        "Behauptung, die österreichische Regierung plane heimlich "
                        "die Abschaffung des Bargelds, mehrfach widerlegt. Das "
                        "Recht auf Bargeldzahlung ist seit 2024 sogar mit "
                        "Verfassungsrang abgesichert (BGBl I 2024/?). Die EU "
                        "verfolgt KEINE Bargeldabschaffung; Obergrenzen für "
                        "Bargeldgeschäfte (10.000 € ab 2027) sind Anti-Geldwäsche-"
                        "Maßnahmen, kein Verbot."),
        "rating": "Falsch / klassischer Boulevard-/Telegram-Hoax",
        "url": "https://apa.at/faktencheck/?s=bargeld",
    },
    {
        "trigger": ("orf-beitrag abgeschafft", "orf-gebühr abgeschafft",
                    "orf-zwangsgebühr abgeschafft", "orf abgeschafft",
                    "orf wird abgeschafft", "abschaffung orf",),
        "title": "ORF-Beitrag bzw. ORF abgeschafft — falsch",
        "description": ("Der ORF-Beitrag (Haushaltsabgabe) hat 2024 die GIS-Gebühr "
                        "abgelöst und gilt unverändert. Eine vollständige "
                        "Abschaffung des ORF oder seiner Finanzierung ist NICHT "
                        "beschlossen. Die FPÖ fordert das politisch — eine "
                        "Umsetzung wäre nur durch Gesetzesnovelle möglich."),
        "rating": "Falsch / politischer Wunsch ≠ Beschluss",
        "url": "https://apa.at/faktencheck/?s=orf+beitrag",
    },
    {
        "trigger": ("eu-pakt pension", "eu pakt pension", "eu pension kürz",
                    "eu zwingt österreich pension", "eu zwingt pension",),
        # Composite: ("pension"|"pensionen") + ("kürz"|"gekürzt"|"reduzier")
        # + ("eu"|"eu-pakt"|"brüssel"|"europäische union")
        "trigger_all": [
            (("pension", "pensionen", "altersrente"),
             ("kürz", "gekürzt", "reduzier", "senkung", "gesenkt"),
             ("eu", "eu-pakt", "eu pakt", "brüssel", "europäische union", "bruessel")),
        ],
        "title": "EU-Pakt zwingt Pensionskürzung in Österreich — falsch",
        "description": ("Pensionsanpassungen sind ausschließlich nationale "
                        "Kompetenz. Weder der EU-Asyl- und Migrationspakt noch "
                        "andere EU-Verordnungen schreiben Mitgliedstaaten "
                        "Pensionskürzungen vor. APA-Faktencheck und Kontrast "
                        "haben diesen Mythos mehrfach widerlegt. Pensionen werden "
                        "in Österreich jährlich an die Inflation angepasst "
                        "(§ 108h ASVG)."),
        "rating": "Falsch / EU hat keine Pensions-Kompetenz",
        "url": "https://apa.at/faktencheck/?s=pension+eu",
    },
    {
        "trigger": ("pflichtimpfung wieder", "pflichtimpfung kommt",
                    "pflichtimpfung kommt zurück", "neue pflichtimpfung",
                    "covid-pflichtimpfung kommt", "corona-pflichtimpfung kommt",),
        "title": "Pflichtimpfung kommt zurück — Hoax",
        "description": ("Die im Februar 2022 in Kraft getretene und im März 2022 "
                        "ausgesetzte COVID-Pflichtimpfung wurde 2023 endgültig "
                        "aufgehoben. Es gibt KEINE Pläne der österreichischen "
                        "Regierung oder der EU, eine COVID-Pflichtimpfung wieder "
                        "einzuführen. APA-Faktencheck hat diesen Mythos seit 2023 "
                        "wiederholt widerlegt."),
        "rating": "Falsch / klassischer FPÖ-naher Telegram-Hoax",
        "url": "https://apa.at/faktencheck/?s=pflichtimpfung",
    },
    {
        "trigger": ("eu zwingt österreich migrant", "eu zwingt aufnahme migrant",
                    "30.000 migranten pro jahr", "30000 migranten pro jahr",
                    "20.000 euro pro migrant", "20000 euro pro migrant",
                    "20.000 euro strafzahlung migrant", "strafzahlung migrant",),
        "title": "EU-Pakt zwingt AT zur Aufnahme tausender Migranten — irreführend",
        "description": ("Der EU-Asyl- und Migrationspakt sieht einen "
                        "Solidaritätsmechanismus vor: Mitgliedstaaten können "
                        "Asylsuchende aufnehmen ODER finanzielle Beiträge leisten "
                        "(20.000 € pro nicht aufgenommener Person). Die genaue "
                        "Quote für Österreich richtet sich nach BIP- und "
                        "Bevölkerungsanteil und liegt bei ~2.000–3.000 Personen "
                        "pro Jahr — nicht bei zehntausenden. AT hat zudem nach "
                        "Art. 58 die Solidaritätsausnahme erhalten."),
        "rating": "Irreführend / Größenordnung um Faktor 10 verzerrt",
        "url": "https://apa.at/faktencheck/?s=eu+pakt+migration",
    },
    {
        "trigger": ("euro austritt", "euro-austritt", "schilling wieder einführen",
                    "schilling zurück", "österreich schilling",
                    "österreich verlässt euro", "öxit",),
        "title": "Österreich sollte aus dem Euro austreten — wirtschaftspolitisch falsch",
        "description": ("OeNB, WIFO und IHS haben mehrfach analysiert: Ein "
                        "Euro-Austritt würde bei Österreich Vermögensverluste in "
                        "zweistelliger Milliardenhöhe verursachen, die "
                        "Importpreise massiv erhöhen und Pensionsersparnisse "
                        "entwerten. Die OeNB unterstützt den Euro-Verbleib "
                        "explizit; eine Studie 'pro Schilling' aus diesen "
                        "Institutionen existiert NICHT."),
        "rating": "Falsch / wirtschaftspolitisch widerlegt",
        "url": "https://apa.at/faktencheck/?s=euro+austritt",
    },
)


def _match_at_classics(claim_lc: str) -> list[dict]:
    """Trigger-Match: substring (any-of) ODER all-of-alternation-tuples."""
    out: list[dict] = []
    for c in _CURATED_AT_CLASSICS:
        if any(t in claim_lc for t in c.get("trigger") or ()):
            out.append(c)
            continue
        for row in c.get("trigger_all") or ():
            if all(any(tok in claim_lc for tok in alt) for alt in row):
                out.append(c)
                break
    return out


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def claim_mentions_at_faktencheck_rss_cached(claim: str) -> bool:
    """Synchronous trigger: fire on any AT-related claim with 10+ chars.

    Da die Reranker-Cosine-Similarity die thematische Filterung übernimmt,
    feuern wir breit — ähnlich wie bei GADMO. Echte Filtration durch
    Reranker-Threshold (FACTCHECK_THRESHOLD = 0.55).
    Klassiker-Trigger gehen am Reranker vorbei (authoritative).
    """
    if not claim or len(claim.strip()) < 10:
        return False
    return True


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_at_faktencheck_rss(analysis: dict) -> dict:
    """Public entrypoint. Returns aggregated AT-Faktencheck items.

    Output format compatible with reranker (ClaimReview-style).
    Curated classics werden bei Trigger-Match VOR dem Live-Feed eingespielt
    und gehen am Reranker vorbei (siehe reranker._AUTHORITATIVE_INDICATORS).
    """
    empty = {
        "source": "AT-Faktencheck-RSS",
        "type": "factcheck",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    claim_lc = claim.lower()

    authoritative_results: list[dict] = []
    for c in _match_at_classics(claim_lc):
        authoritative_results.append({
            "title": c["title"],
            "url": c["url"],
            "date": "Archiv (APA/Kontrast-Klassiker)",
            "rating": c["rating"],
            "description": c["description"],
            "source": "AT-Faktencheck-Archiv (APA/Kontrast)",
            "country": "AT",
            "indicator": "at_faktencheck_classic",
        })

    items = await fetch_at_faktencheck_rss()
    if not items and not authoritative_results:
        return empty

    rerankable = []
    for it in items:
        rerankable.append({
            "title": it["title"],
            "url": it["url"],
            "date": it["date"],
            "rating": it.get("description", "")[:200],
            "source": it["source"],
            "country": it["country"],
            "indicator": "at_faktencheck_rss",
        })

    return {
        "source": "AT-Faktencheck-RSS",
        "type": "factcheck",
        "results": authoritative_results + rerankable,
    }
