"""Mimikama — DACH Hoax-Faktencheck-Plattform.

Spezialisiert auf Social-Media-Falschmeldungen, Verschwörungstheorien,
KI-generierte Bilder, WhatsApp-/Facebook-Hoaxes. Komplementär zu APA-
Faktencheck (das primär klassische Boulevard-Themen abdeckt) und GADMO
(generischer Aggregator).

Datenquelle: RSS-Feed https://www.mimikama.org/feed/ (stündliches Update)
Output-Format: ClaimReview-Style, kompatibel zu reranker.

Use-Case:
- Social-Media-Hoax-Erkennung
- WhatsApp-Kettenbriefe
- KI-generierte Bilder/Videos
- Verschwörungs-Klassiker (Erde flach, Mondlandung, Chemtrails)
"""

import asyncio
import logging
import time
from xml.etree import ElementTree as ET

import httpx

from services._http_polite import USER_AGENT

logger = logging.getLogger("evidora")

FEED_CACHE_TTL = 3600  # 1h
FEED_URL = "https://www.mimikama.org/feed/"
FEED_NAME = "Mimikama"

# RSS feeds are explicitly machine-readable — no need for browser
# masquerading. Polite UA identifies us consistently.
_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/rss+xml, application/xml, text/xml",
    "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
}

_cache: list[dict] | None = None
_cache_time: float = 0.0


def _parse_rss(xml_text: str) -> list[dict]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.warning(f"Failed to parse Mimikama RSS: {e}")
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
            import re as _re
            description = _re.sub(r"<[^>]+>", "", desc_el.text)
            description = description.strip()[:300]

        if not title or not link:
            continue
        items.append({
            "title": title,
            "url": link,
            "date": date,
            "description": description,
            "source": FEED_NAME,
            "country": "DACH",
        })
    return items


async def fetch_mimikama(client: httpx.AsyncClient | None = None) -> list[dict]:
    global _cache, _cache_time

    now = time.time()
    if _cache is not None and (now - _cache_time) < FEED_CACHE_TTL:
        return _cache

    own_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=15.0)
        own_client = True

    try:
        try:
            response = await client.get(FEED_URL, headers=_HEADERS, timeout=15)
            response.raise_for_status()
            items = _parse_rss(response.text)
            logger.info(f"Mimikama fetched: {len(items)} items")
        except Exception as e:
            logger.warning(f"Mimikama fetch failed: {e}")
            items = []

        _cache = items
        _cache_time = now
        return items
    finally:
        if own_client:
            await client.aclose()


# ---------------------------------------------------------------------------
# Kuratiertes Archiv klassischer Verschwörungs-Hoaxes
# ---------------------------------------------------------------------------
# Mimikama hat zu jedem dieser Themen mehrere ältere Faktenchecks. Da der RSS-
# Feed nur die letzten ~20 Posts enthält, würden Boulevard-Klassiker beim
# Reranker durchfallen, obwohl Mimikama sie längst widerlegt hat.
# Diese Klassiker werden als authoritative Sub-Results vor dem Live-Feed
# eingespielt — der Reranker (siehe reranker._AUTHORITATIVE_INDICATORS) lässt
# sie ungeprüft passieren.
_CURATED_CLASSICS = (
    {
        # Composite: ("bill gates" OR "gates") + ("chip" OR "mikrochip" OR "rfid")
        # ODER: "pandemievertrag" + ("chip" oder "rfid" oder "pflichtimpfung")
        "trigger_all": [
            (("bill gates", "gates ", "gates,", "gates."), ("chip", "mikrochip", "rfid")),
            (("pandemievertrag",), ("chip", "rfid", "pflichtimpfung")),
        ],
        "trigger": (),
        "title": "Bill Gates / RFID-Mikrochip in Impfungen — klassischer Hoax",
        "description": ("Mimikama hat die Verschwörungserzählung, Bill Gates "
                        "plane über Impfungen das Einsetzen von RFID-Mikrochips "
                        "(oft im Kontext von WHO-Pandemievertrag), seit 2020 "
                        "wiederholt widerlegt. Es gibt weder eine technische "
                        "Möglichkeit, Mikrochips über Impfnadeln zu injizieren, "
                        "noch entsprechende Aussagen von Gates oder der WHO."),
        "rating": "Falsch / klassischer Verschwörungs-Hoax (mehrfach widerlegt)",
        "url": "https://www.mimikama.org/?s=bill+gates+chip",
    },
    {
        "trigger": ("chemtrails", "chemtrail", "kondensstreifen wettermanipulation",
                    "wettermanipulation flugzeuge", "schweizer verfassung chemtrail",
                    "haarp chemtrails",),
        "title": "Chemtrails / Wettermanipulation durch Flugzeuge — Hoax",
        "description": ("Mimikama widerlegt die Chemtrails-Verschwörung seit "
                        "über zehn Jahren. Was als 'Chemtrails' bezeichnet wird, "
                        "sind Kondensstreifen aus Wasserdampf — abhängig von "
                        "Temperatur und Luftfeuchtigkeit. Es gibt KEINE Erwähnung "
                        "von Chemtrails in der Schweizer Verfassung oder einer "
                        "anderen DACH-Verfassung."),
        "rating": "Falsch / klassischer Verschwörungs-Hoax",
        "url": "https://www.mimikama.org/?s=chemtrails",
    },
    {
        "trigger": ("erde ist flach", "flache erde", "flat earth"),
        "title": "Flache Erde — klassischer Verschwörungs-Hoax",
        "description": ("Mimikama hat die 'Flache Erde'-Verschwörung mehrfach "
                        "widerlegt. Die Erde ist nachweislich annähernd kugelförmig "
                        "(Geoid); Belege reichen von Mond-Schatten bei Mondfinsternis "
                        "bis zu GPS, Satellitennavigation und ISS-Live-Bildern."),
        "rating": "Falsch / klassischer Verschwörungs-Hoax",
        "url": "https://www.mimikama.org/?s=flache+erde",
    },
    {
        "trigger": ("mondlandung fake", "moon landing fake", "apollo 11 fake",
                    "mondlandung hoax",),
        "trigger_all": [
            (("mondlandung", "moon landing", "apollo 11"),
             ("fake", "inszenier", "hoax", "studio", "kubrick", "nasa-lüge")),
        ],
        "title": "Mondlandung 1969 inszeniert — klassischer Hoax",
        "description": ("Mimikama widerlegt die Mondlandungs-Verschwörung. Es gibt "
                        "Laser-Reflektoren auf dem Mond, die seit 1969 von "
                        "verschiedenen Observatorien (auch nicht-US) gemessen werden, "
                        "von Russland mitverfolgte Telemetrie, 382 kg Mondgestein in "
                        "verschiedenen Forschungseinrichtungen weltweit."),
        "rating": "Falsch / klassischer Verschwörungs-Hoax",
        "url": "https://www.mimikama.org/?s=mondlandung",
    },
    {
        "trigger": ("5g verursacht", "5g krebs", "5g corona", "5g-strahlung gefährlich",
                    "5g impfung", "5g chip",),
        "title": "5G verursacht Krankheiten — klassischer Hoax",
        "description": ("Mimikama hat die 5G-Verschwörungserzählungen ('5G verursacht "
                        "COVID', 'aktiviert Mikrochips', 'verursacht Krebs') mehrfach "
                        "widerlegt. 5G ist nicht-ionisierende Strahlung — keine "
                        "Energieübertragung in lebende Zellen, keine DNA-Schädigung."),
        "rating": "Falsch / klassischer Verschwörungs-Hoax",
        "url": "https://www.mimikama.org/?s=5g",
    },
    {
        "trigger": ("wuhan labor freigesetzt", "wuhan labor absichtlich",
                    "covid biowaffe", "corona biowaffe", "covid laborunfall absichtlich",),
        "title": "SARS-CoV-2 absichtlich aus Wuhan-Labor freigesetzt — Hoax",
        "description": ("Während ein zoonotischer Ursprung wissenschaftlich am "
                        "wahrscheinlichsten ist und ein versehentlicher Lab-Leak "
                        "weiterhin diskutiert wird (siehe US-Geheimdienst-Reports), "
                        "ist die Behauptung einer ABSICHTLICHEN Freisetzung als "
                        "Biowaffe nicht durch Beweise gestützt und wird von Mimikama "
                        "und den meisten Faktencheckern abgelehnt."),
        "rating": "Falsch / nicht durch Beweise gestützt",
        "url": "https://www.mimikama.org/?s=wuhan+labor",
    },
    {
        "trigger": ("wasser-gedächtnis", "wasser gedächtnis", "wasser hat gedächtnis",
                    "homöopathie wirkt", "homoeopathie wirkt", "globuli wirksam",),
        "title": "Wasser-Gedächtnis / Homöopathie wirkt — Hoax",
        "description": ("Mimikama und das Skeptiker-Netzwerk haben das 'Wasser-"
                        "Gedächtnis'-Konzept (Grundlage der Homöopathie-Logik nach "
                        "Hahnemann) als pseudowissenschaftlich entlarvt. In über 200 "
                        "qualitätsgesicherten klinischen Studien zeigt Homöopathie "
                        "keine Wirkung über Placebo hinaus."),
        "rating": "Pseudowissenschaft / kein Beleg",
        "url": "https://www.mimikama.org/?s=hom%C3%B6opathie",
    },
)


def _match_classics(claim_lc: str) -> list[dict]:
    """Return all curated classics whose trigger fires on this claim.

    A classic matches if EITHER:
      - any substring in `trigger` is present, OR
      - all alternation-tuples in any of the `trigger_all` rows fire.
    """
    out: list[dict] = []
    for c in _CURATED_CLASSICS:
        if any(t in claim_lc for t in c.get("trigger") or ()):
            out.append(c)
            continue
        for row in c.get("trigger_all") or ():
            if all(any(tok in claim_lc for tok in alt) for alt in row):
                out.append(c)
                break
    return out


def claim_mentions_mimikama_cached(claim: str) -> bool:
    """Synchronous trigger: fire on any claim with 10+ chars.

    Reranker filtert thematisch via FACTCHECK_THRESHOLD = 0.55.
    Wenn ein Klassiker-Trigger zündet, wird das Item via authoritative-Pfad
    am Reranker vorbeigeleitet.
    """
    if not claim or len(claim.strip()) < 10:
        return False
    return True


async def search_mimikama(analysis: dict) -> dict:
    empty = {
        "source": "Mimikama",
        "type": "factcheck",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    claim_lc = claim.lower()

    # Authoritative classics — gehen am Reranker vorbei
    authoritative_results: list[dict] = []
    for c in _match_classics(claim_lc):
        authoritative_results.append({
            "title": c["title"],
            "url": c["url"],
            "date": "Archiv (Mimikama-Klassiker)",
            "rating": c["rating"],
            "description": c["description"],
            "source": "Mimikama (Archiv-Klassiker)",
            "country": "DACH",
            "indicator": "mimikama_classic",
        })

    items = await fetch_mimikama()
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
            "indicator": "mimikama_hoax_check",
        })

    return {
        "source": "Mimikama",
        "type": "factcheck",
        "results": authoritative_results + rerankable,
    }
