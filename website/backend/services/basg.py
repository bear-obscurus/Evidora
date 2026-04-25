"""BASG (Bundesamt für Sicherheit im Gesundheitswesen) — österreichische
Behörde für Arzneimittel- und Medizinprodukte-Sicherheit.

Datenquelle: BASG Newsfeed (RSS) — https://www.basg.gv.at/whatsnew/rss

Liefert AT-spezifische amtliche Nachrichten zu:
- Chargenrückrufe und Vertriebseinschränkungen
- Sicherheitsinformationen (DHPC) für Arzneimittel
- Pharmakovigilanz-Mustertexte (z. B. „Albendazol", „Hydroxycarbamid“)
- Medizinprodukte-Warnungen
- CHMP-Meeting-Highlights (EU-Bezug, durch BASG relayed)
- Spender-Sperren bei Fortpflanzungsmedizin

Lizenz: Inhalte des BASG sind gemeinfreie Verlautbarungen einer Behörde
(§ 7 UrhG-AT), Verwendung mit Quellennennung zulässig.

Triggering: nur bei AT-Kontext + Pharma-/Medizin-Keyword. Verhindert
Off-Topic-Treffer aus dem 50-Item-Newsfeed.

Caveat:
- BASG-Meldungen sind regulatorische/behördliche Mitteilungen, nicht
  notwendigerweise wissenschaftlicher Konsens.
- Einige Items werden von der EMA übernommen — keine eigene AT-Position.
- Der Feed mischt Major Events (Rückrufe) mit Minor Updates (FAQ-Stand).
- Aktuell zeigt der Feed nur ~50 letzte Einträge (rolling window) —
  ältere Rückrufe sind nicht über Feed abrufbar.
"""

import logging
import re
import time
from xml.etree import ElementTree

import httpx

logger = logging.getLogger("evidora")

FEED_URL = "https://www.basg.gv.at/whatsnew/rss"
CACHE_TTL = 3600  # 1h

_feed_cache: list[dict] = []
_cache_time: float = 0.0

# Pharma-/Medizin-Keywords (DE + EN), die BASG-Trigger rechtfertigen.
PHARMA_KEYWORDS = [
    # Allgemein
    "medikament", "medikamente", "arzneimittel", "arzneispezialität", "wirkstoff",
    "medication", "medicine", "drug", "pharmaceutical",
    # Sicherheits-/Rückruf-Vokabular
    "rückruf", "chargenrückruf", "rückgerufen",
    "warnung", "sicherheitswarnung", "produktwarnung",
    "verbot", "verboten", "vom markt", "marktrücknahme",
    "nebenwirkung", "nebenwirkungen", "unerwünschte arzneimittelwirkung",
    "recall", "withdrawn", "withdrawal", "safety alert", "safety warning",
    "side effect", "side effects", "adverse event", "adverse reaction",
    # Impfstoffe + Charge
    "impfstoff", "impfung", "impfdosen", "vaccine", "vaccination",
    "charge", "batch", "lot",
    # Behörde
    "basg", "ema", "chmp", "dhpc",
    # Medizinprodukte
    "medizinprodukt", "medizinprodukte", "medical device",
    # Apotheke / Vertrieb
    "apotheke", "rezeptpflichtig", "verschreibungspflichtig",
    "vertriebseinschränkung", "lieferengpass", "shortage",
    # Pharmakovigilanz
    "pharmakovigilanz", "pharmacovigilance",
]

# Ressort-Keywords im News-GUID — wir filtern auf substanzielle News-Items
NEWS_GUID_PREFIXES = ("tx_news_domain_model_news_",)

# Generische Footer-/FAQ-Items, die nie als Evidenz taugen
EXCLUDE_TITLE_TERMS = (
    "faq", "kontakt", "newsletter", "wartungsfenster", "barrierefrei",
    "nationale vorgehensweisen", "österreichisches arzneibuch",
    "zur stellungnahme",
)


def _parse_feed(xml_text: str) -> list[dict]:
    """Parse RSS feed and keep only substantive news items."""
    items: list[dict] = []
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as e:
        logger.warning(f"BASG: RSS parse error: {e}")
        return items

    for item in root.findall(".//item"):
        title_el = item.find("title")
        link_el = item.find("link")
        desc_el = item.find("description")
        guid_el = item.find("guid")
        pubdate_el = item.find("pubDate")
        if title_el is None or link_el is None or guid_el is None:
            continue

        title = (title_el.text or "").strip()
        url = (link_el.text or "").strip()
        description = (desc_el.text or "").strip() if desc_el is not None else ""
        guid = (guid_el.text or "").strip()
        pub_date = (pubdate_el.text or "").strip() if pubdate_el is not None else ""

        # Nur tx_news_domain_model_news_* — alle anderen sind Page-Stubs
        if not guid.startswith(NEWS_GUID_PREFIXES):
            continue

        # Generische Site-Items raus
        title_lower = title.lower()
        if any(term in title_lower for term in EXCLUDE_TITLE_TERMS):
            continue

        # HTML-Entities normalisieren
        description = re.sub(r"&amp;", "&", description)
        description = re.sub(r"&hellip;", "…", description)
        description = re.sub(r"&nbsp;", " ", description)

        items.append({
            "title": title,
            "url": url,
            "description": description[:400],
            "pub_date": pub_date,
            "guid": guid,
        })

    return items


async def fetch_basg(client: httpx.AsyncClient | None = None) -> list[dict]:
    """Fetch and cache the BASG news feed.

    Returns the list of relevant news items.
    """
    global _feed_cache, _cache_time

    now = time.time()
    if _feed_cache and (now - _cache_time) < CACHE_TTL:
        return _feed_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=15.0)
        close_client = True

    try:
        resp = await client.get(
            FEED_URL,
            headers={"User-Agent": "Mozilla/5.0 (Evidora fact-check service)"},
        )
        resp.raise_for_status()
        items = _parse_feed(resp.text)
        _feed_cache = items
        _cache_time = now
        logger.info(f"BASG: {len(items)} news items cached")
        return items
    except Exception as e:
        logger.warning(f"BASG: feed fetch failed: {e}")
        return _feed_cache  # stale cache is better than nothing
    finally:
        if close_client:
            await client.aclose()


def _claim_mentions_pharma(claim: str, analysis: dict | None = None) -> bool:
    """Check if claim mentions pharma/safety keywords or NER detected a drug entity."""
    claim_lower = claim.lower()
    if any(kw in claim_lower for kw in PHARMA_KEYWORDS):
        return True
    if analysis is not None:
        ner = analysis.get("ner_entities", {}) or {}
        if ner.get("drugs"):
            return True
    return False


def _claim_mentions_austria(analysis: dict) -> bool:
    """Heuristic: claim has Austria context."""
    claim_lower = analysis.get("claim", "").lower()
    if "österreich" in claim_lower or "austria" in claim_lower or "basg" in claim_lower:
        return True
    countries = analysis.get("ner_entities", {}).get("countries", [])
    return any("österreich" in c.lower() or "austria" in c.lower() for c in countries)


def _tokenize(text: str) -> set[str]:
    """Lowercase + non-alphanumeric split; keep tokens with >= 4 chars."""
    return {t for t in re.split(r"[^a-zA-ZäöüÄÖÜß0-9]+", text.lower()) if len(t) >= 4}


def _score_item(item: dict, claim_tokens: set[str], entity_tokens: set[str]) -> int:
    """Token-overlap score: claim tokens × item tokens, plus entity boost."""
    item_tokens = _tokenize(item["title"] + " " + item["description"])
    overlap_claim = claim_tokens & item_tokens
    overlap_entity = entity_tokens & item_tokens
    # Each entity-overlap is worth more than a generic claim-token overlap
    return len(overlap_claim) + 2 * len(overlap_entity)


def _gather_query_tokens(analysis: dict) -> tuple[set[str], set[str]]:
    """Return (claim_tokens, entity_tokens) for matching."""
    claim = analysis.get("claim", "")
    claim_tokens = _tokenize(claim)
    entities: list[str] = []
    ner = analysis.get("ner_entities", {}) or {}
    for key in ("drugs", "diseases", "organizations", "products", "miscellaneous"):
        for v in ner.get(key, []) or []:
            entities.append(v)
    # LLM-extracted entities (flat list)
    for v in analysis.get("entities", []) or []:
        if isinstance(v, str):
            entities.append(v)
    entity_tokens: set[str] = set()
    for e in entities:
        entity_tokens.update(_tokenize(e))
    # Stop-Tokens, die zu generisch sind
    stop = {"österreich", "austria", "deutschland", "germany", "europa", "europe",
            "frage", "behauptung", "aussage", "studie", "bericht", "jahr", "jahre"}
    claim_tokens -= stop
    entity_tokens -= stop
    return claim_tokens, entity_tokens


async def search_basg(analysis: dict) -> dict:
    """Search BASG news feed for items relevant to the claim.

    Triggers when the claim has both AT context and a pharma/safety keyword.
    Returns top 3-5 token-matched items as evidence.
    """
    claim = analysis.get("claim", "")
    if not (_claim_mentions_pharma(claim, analysis) and _claim_mentions_austria(analysis)):
        return {"source": "BASG", "type": "official_data", "results": []}

    items = await fetch_basg()
    if not items:
        return {"source": "BASG", "type": "official_data", "results": []}

    claim_tokens, entity_tokens = _gather_query_tokens(analysis)
    if not claim_tokens and not entity_tokens:
        return {"source": "BASG", "type": "official_data", "results": []}

    scored = [
        (item, _score_item(item, claim_tokens, entity_tokens))
        for item in items
    ]
    scored.sort(key=lambda p: p[1], reverse=True)

    # Require at least 1 entity-overlap OR ≥3 claim-token overlaps
    matched: list[dict] = []
    for item, score in scored[:8]:
        item_tokens = _tokenize(item["title"] + " " + item["description"])
        ent_hit = entity_tokens & item_tokens
        claim_hit = claim_tokens & item_tokens
        if ent_hit or len(claim_hit) >= 3:
            matched.append(item)
        if len(matched) >= 4:
            break

    if not matched:
        return {"source": "BASG", "type": "official_data", "results": []}

    results: list[dict] = []
    for item in matched:
        # pubDate format: "Wed, 22 Apr 2026 16:54:09 +0200"
        date_short = ""
        if item.get("pub_date"):
            m = re.match(
                r"\w+,\s+(\d{1,2})\s+(\w+)\s+(\d{4})", item["pub_date"]
            )
            if m:
                date_short = f"{m.group(3)}-{m.group(2)}-{m.group(1).zfill(2)}"
            else:
                date_short = item["pub_date"][:16]

        title = f"BASG: {item['title']}"
        if date_short:
            title = f"{title} ({date_short})"

        results.append({
            "indicator_name": title,
            "indicator": "basg_news",
            "country": "AUT",
            "country_name": "Austria",
            "year": (item.get("pub_date", "")[12:16] if item.get("pub_date") else ""),
            "value": "",
            "display_value": "",
            "url": item["url"],
            "description": item["description"] or "—",
        })

    # Caveat
    results.append({
        "indicator_name": "WICHTIGER KONTEXT: BASG-Meldungen",
        "indicator": "context",
        "country": "AUT",
        "country_name": "Austria",
        "year": "",
        "value": "",
        "display_value": "",
        "url": "https://www.basg.gv.at/marktbeobachtung/amtliche-nachrichten",
        "description": (
            "Das BASG (Bundesamt für Sicherheit im Gesundheitswesen) ist die "
            "österreichische Behörde für Arzneimittel- und Medizinprodukte-"
            "Sicherheit. Einschränkungen: "
            "(1) BASG-Meldungen sind regulatorische/behördliche Mitteilungen — "
            "Rückrufe und Warnungen erfolgen oft präventiv und sind nicht "
            "automatisch ein Beleg für tatsächliche Schadensfälle. "
            "(2) Viele Items übernimmt das BASG aus EU-Verfahren (CHMP, EMA) — "
            "in diesen Fällen ist die Quelle keine eigenständige AT-Bewertung. "
            "(3) Der RSS-Feed zeigt nur die letzten ~50 Einträge — ältere "
            "Rückrufe sind nur über das Detailregister abrufbar. "
            "(4) Wirkstoff- vs. Marken-Match: Ein Treffer auf den Wirkstoffnamen "
            "(z. B. „Hydroxycarbamid“) betrifft alle Präparate dieses Wirkstoffs, "
            "nicht zwingend ein konkret in der Behauptung genanntes Markenprodukt."
        ),
    })

    logger.info(
        f"BASG: {len(results) - 1} matched items, claim_tokens={len(claim_tokens)}, "
        f"entity_tokens={len(entity_tokens)}"
    )
    return {"source": "BASG", "type": "official_data", "results": results}
