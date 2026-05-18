"""UNESCO World Heritage List — Welterbe-Stätten weltweit.

Datenquelle: UNESCO World Heritage Centre (WHC), offizielle XML-Liste
aller 1.200+ inskribierten Welterbestätten. Pro Site: Name, Land(er),
Inscription-Year, Kriterien (i)-(x), Kategorie (Cultural/Natural/Mixed),
Kurzbeschreibung, Geo-Koordinaten, Danger-Status, http_url.

API/Endpoint: https://whc.unesco.org/en/list/xml/
- Komplett-Dump als XML (~1.5 MB)
- Kein Auth erforderlich
- Update halbjaehrlich nach jeder Welterbekomitee-Sitzung (Sommer)
- Lizenz: UNESCO Open Data Policy (Public Information,
  https://en.unesco.org/open-access/terms-use-ccbysa-en)

Hinweis: Der ursprünglich geplante Opendatasoft-Endpoint
"data.unesco.org/.../whc-sites/records" existiert nicht (404). Wir
verwenden den offiziellen WHC-XML-Dump als kanonische Quelle; er ist
die Master-Liste, von der alle Drittanbieter ihre Mirrors ableiten.

Strategie: Beim ersten Lookup laden wir den XML-Dump einmal in einen
24h-In-Memory-Cache. Anchor-Detection auf den Site-Namen (DE/EN-
Aliase) plus generische "Welterbe"-Trigger. Bei Treffer: bis zu 5
strukturierte Treffer mit Inscription-Year + Kriterien + Land.

Komplementaer zu existierenden Quellen:
- europeana.py: Digitalisate konkreter Werke/Objekte.
- unesco_uis.py: globale Bildungs-Statistik (SDG-4).
- wikipedia: unstrukturierte Lead-Extracts.
- WORLD HERITAGE: kanonischer Status-Nachweis (Inskribierung,
  Kriterien, Jahr), v. a. fuer AT-Welterbe (12 Sites).

AT-Welterbe (12, Stand 2026):
- Salzburg Altstadt (1996)
- Schönbrunn (1996)
- Hallstatt-Dachstein / Salzkammergut (1997)
- Semmeringbahn (1998)
- Graz Altstadt + Eggenberg (1999)
- Wachau (2000)
- Fertö / Neusiedlersee (2001)
- Wien Innere Stadt (2001)
- Buchenurwaelder Karpaten + Europa (2007)
- Praehistorische Pfahlbauten Alpen (2011)
- Donaulimes Westabschnitt (2021)
- Great Spa Towns of Europe (Baden bei Wien) (2021)

Politische Guardrails: Pure deskriptive Inskriptions-Daten —
kein Polit-Tabu beruehrt.

# WIRING fuer main.py:
# from services.world_heritage import (
#     search_world_heritage, claim_mentions_world_heritage_cached,
# )
# if claim_mentions_world_heritage_cached(claim):
#     tasks.append(cached("UNESCO World Heritage", search_world_heritage, analysis))
#     queried_names.append("UNESCO World Heritage")
#
# WIRING fuer reranker.py (Indicator-Whitelist):
#   "world_heritage_" als Whitelist-Prefix in INDICATOR_WHITELIST_PREFIXES
#
# data_updater.py: KEIN Prefetch (Live-Only, XML-Cache 24h pro Worker)
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
import xml.etree.ElementTree as ET
from functools import lru_cache
from html.parser import HTMLParser

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

WHC_XML_URL = "https://whc.unesco.org/en/list/xml/"

HTTP_TIMEOUT_S = 25.0
MAX_RESULTS = 5
CACHE_TTL_S = 24 * 3600.0  # 24 h

# Lock fuer thread-safe XML-Initial-Fetch (Hot-Reload-Worker können parallel)
_FETCH_LOCK = asyncio.Lock()

# Geladene Site-Liste: list[dict], wird beim ersten Lookup gefüllt
_SITES_CACHE: dict[str, object] = {
    "ts": 0.0,
    "sites": [],  # list[dict]
    "by_name_lc": {},  # name_lc → site-dict (Exact-Match-Index)
    "tokens": [],  # list[tuple[set[str], dict]] für Token-Match
}


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
# Generische Welterbe-Trigger (DE/EN)
_GENERIC_HERITAGE_TERMS = (
    "welterbe",
    "welt-erbe",
    "welterbeliste",
    "welterbe-liste",
    "weltkulturerbe",
    "weltnaturerbe",
    "unesco-welterbe",
    "unesco welterbe",
    "world heritage",
    "world heritage site",
    "world heritage list",
    "wh-site",
    "whc",
)

# AT-Welterbe-Anker (Aliase, lowercase, ggf. Stadt-Kurzform)
# Reihenfolge: laengste/eindeutige Aliase zuerst, damit "wien innere stadt"
# vor "wien" matched. Match wird per Wortgrenze gefiltert.
_AT_WHL_ANCHORS: tuple[tuple[str, str], ...] = (
    # (alias_lc, canonical_site_name_search_lc)
    ("hallstatt-dachstein", "hallstatt"),
    ("hallstatt dachstein", "hallstatt"),
    ("hallstatt", "hallstatt"),
    ("salzkammergut", "hallstatt"),
    ("schloss schönbrunn", "schönbrunn"),
    ("schönbrunn", "schönbrunn"),
    ("schoenbrunn", "schönbrunn"),
    ("salzburg altstadt", "salzburg"),
    ("altstadt salzburg", "salzburg"),
    ("historisches zentrum salzburg", "salzburg"),
    ("historic centre salzburg", "salzburg"),
    ("wien innere stadt", "vienna"),
    ("wiener innere stadt", "vienna"),
    ("innere stadt wien", "vienna"),
    ("historic centre of vienna", "vienna"),
    ("historisches zentrum wien", "vienna"),
    ("historisches zentrum von wien", "vienna"),
    ("altstadt wien", "vienna"),
    ("wiener altstadt", "vienna"),
    ("semmeringbahn", "semmering"),
    ("semmering-bahn", "semmering"),
    ("semmering railway", "semmering"),
    ("wachau", "wachau"),
    ("neusiedler see", "neusiedlersee"),
    ("neusiedlersee", "neusiedlersee"),
    ("graz altstadt", "graz"),
    ("altstadt graz", "graz"),
    ("schloss eggenberg", "eggenberg"),
    ("eggenberg", "eggenberg"),
    ("buchenurwälder", "beech forests"),
    ("buchenurwaelder", "beech forests"),
    ("urwald buchen", "beech forests"),
    ("primeval beech", "beech forests"),
    ("ancient beech", "beech forests"),
    ("prähistorische pfahlbauten", "pile dwellings"),
    ("praehistorische pfahlbauten", "pile dwellings"),
    ("pfahlbauten alpen", "pile dwellings"),
    ("pfahlbauten an den alpen", "pile dwellings"),
    ("pfahlbauten um die alpen", "pile dwellings"),
    ("pfahlbauten", "pile dwellings"),
    ("pile dwellings", "pile dwellings"),
    ("donaulimes", "danube limes"),
    ("danube limes", "danube limes"),
    ("baden bei wien", "great spa towns"),
    ("great spa towns", "great spa towns"),
)

# Internationaler DE→EN-Alias-Layer für häufige Welt-Sites (kein AT-Anker,
# aber DE-Boulevard-Faktencheck-Relevanz). Wir setzen hier nur eindeutige
# DE-Begriffe, deren EN-Pendant unverwechselbar in einem Site-Namen vorkommt.
_INTL_DE_ALIASES: tuple[tuple[str, str], ...] = (
    ("pyramiden von gizeh", "giza"),
    ("pyramiden gizeh", "giza"),
    ("pyramiden gizah", "giza"),
    ("gizeh", "giza"),
    ("akropolis von athen", "acropolis"),
    ("akropolis athen", "acropolis"),
    ("akropolis", "acropolis"),
    ("kolosseum rom", "rome"),
    ("kolosseum in rom", "rome"),
    ("kolosseum", "rome"),
    ("colosseum rome", "rome"),
    ("colosseum", "rome"),
    ("petra jordanien", "petra"),
    ("machu picchu", "machu picchu"),
    ("chinesische mauer", "great wall"),
    ("große mauer china", "great wall"),
    ("grosse mauer china", "great wall"),
    ("taj mahal", "taj mahal"),
    ("angkor wat", "angkor"),
    ("angkor", "angkor"),
    ("stonehenge", "stonehenge"),
    ("kreml moskau", "kremlin"),
    ("kreml", "kremlin"),
    ("alhambra granada", "alhambra"),
    ("alhambra", "alhambra"),
    ("kölner dom", "cologne cathedral"),
    ("koelner dom", "cologne cathedral"),
    ("aachener dom", "aachen cathedral"),
    ("wattenmeer", "wadden sea"),
    ("speicherstadt hamburg", "speicherstadt"),
)


def _has_any(text: str, terms) -> bool:
    return any(t in text for t in terms)


def _claim_mentions_world_heritage(claim_lc: str) -> bool:
    """Trigger-Check (lowercase claim erwartet).

    True bei:
    - Generischer Welterbe-Term (Welterbe/world heritage/UNESCO-Welterbe …)
    - AT-Welterbe-Anker (Hallstatt, Schönbrunn, Wachau, Semmering …)
    - Composite: "UNESCO" + Site-Anker
    """
    if not claim_lc:
        return False

    if _has_any(claim_lc, _GENERIC_HERITAGE_TERMS):
        return True

    # Direkter Site-Anker (AT-Schwerpunkt + Intl-DE-Aliase)
    for alias, _canon in (*_AT_WHL_ANCHORS, *_INTL_DE_ALIASES):
        if alias in claim_lc:
            return True

    # Composite: "unesco" + generischer Stätte-Term
    if "unesco" in claim_lc:
        if _has_any(claim_lc, (
            "kulturerbe", "naturerbe",
            "altstadt", "kulturlandschaft",
            "site", "stätte", "staette",
        )):
            return True

    return False


@lru_cache(maxsize=2048)
def claim_mentions_world_heritage_cached(claim: str) -> bool:
    """LRU-gecachter Trigger-Check (Hot-Path-friendly)."""
    return _claim_mentions_world_heritage((claim or "").lower())


# ---------------------------------------------------------------------------
# XML-Loader
# ---------------------------------------------------------------------------
class _HTMLStrip(HTMLParser):
    """Minimaler HTML-Stripper fuer short_description / justification."""

    def __init__(self) -> None:
        super().__init__()
        self._buf: list[str] = []

    def handle_data(self, data: str) -> None:
        self._buf.append(data)

    def text(self) -> str:
        return " ".join("".join(self._buf).split()).strip()


def _strip_html(s: str | None) -> str:
    if not s:
        return ""
    try:
        p = _HTMLStrip()
        p.feed(s)
        return p.text()
    except Exception:
        # Defensiver Fallback: rohe Tags entfernen
        return re.sub(r"<[^>]+>", " ", s).strip()


def _trim(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 1].rstrip() + "…"


_COUNTRY_NAMES_DE: dict[str, str] = {
    "at": "Österreich", "de": "Deutschland", "ch": "Schweiz",
    "fr": "Frankreich", "it": "Italien", "es": "Spanien",
    "pt": "Portugal", "nl": "Niederlande", "be": "Belgien",
    "lu": "Luxemburg", "gb": "Vereinigtes Königreich",
    "ie": "Irland", "dk": "Dänemark", "se": "Schweden",
    "no": "Norwegen", "fi": "Finnland", "is": "Island",
    "pl": "Polen", "cz": "Tschechien", "sk": "Slowakei",
    "hu": "Ungarn", "si": "Slowenien", "hr": "Kroatien",
    "ba": "Bosnien und Herzegowina", "rs": "Serbien",
    "me": "Montenegro", "mk": "Nordmazedonien", "al": "Albanien",
    "gr": "Griechenland", "bg": "Bulgarien", "ro": "Rumänien",
    "ua": "Ukraine", "by": "Belarus", "ru": "Russland",
    "tr": "Türkei", "cy": "Zypern", "mt": "Malta",
    "ee": "Estland", "lv": "Lettland", "lt": "Litauen",
    "us": "USA", "ca": "Kanada", "mx": "Mexiko",
    "br": "Brasilien", "ar": "Argentinien", "cl": "Chile",
    "co": "Kolumbien", "pe": "Peru", "ec": "Ecuador",
    "cn": "China", "jp": "Japan", "kr": "Südkorea",
    "in": "Indien", "id": "Indonesien", "vn": "Vietnam",
    "th": "Thailand", "ph": "Philippinen", "my": "Malaysia",
    "au": "Australien", "nz": "Neuseeland",
    "eg": "Ägypten", "ma": "Marokko", "tn": "Tunesien",
    "dz": "Algerien", "ly": "Libyen", "za": "Südafrika",
    "ke": "Kenia", "tz": "Tansania", "et": "Äthiopien",
    "ng": "Nigeria", "gh": "Ghana", "sn": "Senegal",
    "ml": "Mali", "ne": "Niger", "cd": "DR Kongo",
    "ir": "Iran", "iq": "Irak", "sy": "Syrien",
    "il": "Israel", "ps": "Palästina", "jo": "Jordanien",
    "lb": "Libanon", "sa": "Saudi-Arabien", "ae": "VAE",
    "ye": "Jemen", "om": "Oman", "qa": "Katar",
    "kz": "Kasachstan", "uz": "Usbekistan", "pk": "Pakistan",
    "af": "Afghanistan", "bd": "Bangladesch", "lk": "Sri Lanka",
    "np": "Nepal", "mn": "Mongolei",
}


_WORD_SPLIT = re.compile(r"[^a-z0-9äöüß]+")


def _tokenize(s: str) -> set[str]:
    if not s:
        return set()
    s = s.lower()
    return {t for t in _WORD_SPLIT.split(s) if len(t) >= 3}


def _parse_xml(xml_bytes: bytes) -> list[dict]:
    """Parse den WHC-XML-Dump in eine Liste von Site-Dicts."""
    sites: list[dict] = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        logger.warning(f"WHC XML parse-error: {e}")
        return []
    for row in root.findall("row"):
        site_name = (row.findtext("site") or "").strip()
        if not site_name:
            continue
        iso = (row.findtext("iso_code") or "").strip().lower()
        iso_list = [c.strip() for c in iso.split(",") if c.strip()]
        states = (row.findtext("states") or "").strip()
        year = (row.findtext("date_inscribed") or "").strip()
        criteria = (row.findtext("criteria_txt") or "").strip()
        category = (row.findtext("category") or "").strip()
        region = (row.findtext("regions") or "").strip()
        danger = (row.findtext("danger") or "").strip()
        short = _strip_html(row.findtext("short_description"))
        url = (row.findtext("http_url") or "").strip()
        id_num = (row.findtext("id_number") or "").strip()
        transnational = (row.findtext("transnational") or "0").strip() == "1"

        sites.append({
            "name": site_name,
            "name_lc": site_name.lower(),
            "iso_list": iso_list,
            "states": states,
            "year": year,
            "criteria": criteria,
            "category": category,
            "region": region,
            "danger": danger,
            "short": short,
            "url": url,
            "id": id_num,
            "transnational": transnational,
            "tokens": _tokenize(site_name),
        })
    return sites


async def _ensure_sites_loaded() -> list[dict]:
    """Stelle sicher dass _SITES_CACHE aktuell ist; refetch bei Stale-Cache."""
    now = time.time()
    if _SITES_CACHE["sites"] and (now - float(_SITES_CACHE["ts"])) < CACHE_TTL_S:
        return _SITES_CACHE["sites"]  # type: ignore[return-value]

    async with _FETCH_LOCK:
        # Double-check inside lock
        now = time.time()
        if _SITES_CACHE["sites"] and (now - float(_SITES_CACHE["ts"])) < CACHE_TTL_S:
            return _SITES_CACHE["sites"]  # type: ignore[return-value]

        try:
            async with polite_client(timeout=HTTP_TIMEOUT_S) as client:
                resp = await client.get(WHC_XML_URL, follow_redirects=True)
                if resp.status_code != 200:
                    logger.warning(
                        f"WHC XML HTTP {resp.status_code} — using stale cache"
                    )
                    return _SITES_CACHE["sites"]  # type: ignore[return-value]
                xml_bytes = resp.content
        except Exception as e:
            logger.warning(f"WHC XML fetch failed: {e}")
            return _SITES_CACHE["sites"]  # type: ignore[return-value]

        sites = _parse_xml(xml_bytes)
        if not sites:
            return _SITES_CACHE["sites"]  # type: ignore[return-value]

        _SITES_CACHE["sites"] = sites
        _SITES_CACHE["ts"] = time.time()
        _SITES_CACHE["by_name_lc"] = {s["name_lc"]: s for s in sites}
        logger.info(f"UNESCO World Heritage: {len(sites)} sites geladen")
        return sites


# ---------------------------------------------------------------------------
# Site-Matching
# ---------------------------------------------------------------------------
def _wordbound_contains(haystack: str, needle: str) -> bool:
    """Wortgrenzen-Match (lockerer als \\b, akzeptiert Umlaute)."""
    if not needle or needle not in haystack:
        return False
    pattern = r"(?<![a-zäöüß0-9])" + re.escape(needle) + r"(?![a-zäöüß0-9])"
    return bool(re.search(pattern, haystack))


def _match_sites(claim_lc: str, sites: list[dict]) -> list[dict]:
    """Finde Sites zum Claim. Reihenfolge:
    1. AT-Anchor-Aliase und Intl-DE-Aliase → kanonischer Substring im name_lc.
    2. Direkter Substring-Match auf site_name_lc (≥6 Zeichen).
    3. Token-Overlap (≥2 gemeinsame Tokens).
    """
    matched: list[dict] = []
    seen_ids: set[str] = set()

    # 1) AT-Anchor-Aliase + Intl-DE-Aliase (kombiniert; AT zuerst)
    for alias, canon in (*_AT_WHL_ANCHORS, *_INTL_DE_ALIASES):
        if alias not in claim_lc:
            continue
        if not _wordbound_contains(claim_lc, alias):
            continue
        for s in sites:
            if canon in s["name_lc"]:
                sid = s["id"] or s["name_lc"]
                if sid in seen_ids:
                    continue
                seen_ids.add(sid)
                matched.append(s)
                if len(matched) >= MAX_RESULTS:
                    return matched

    # 2) Direkter Substring-Match (auf ganze Site-Namen, ohne Trigger-Wörter)
    # Wir scannen die ersten ~4 Tokens des Namens als Kandidat.
    if len(matched) < MAX_RESULTS:
        for s in sites:
            name_lc = s["name_lc"]
            sid = s["id"] or name_lc
            if sid in seen_ids:
                continue
            # Probiere: gesamter Name (falls kurz) oder erste 30 Zeichen
            # nur fuer Sites mit Namen ≥6 Zeichen
            if len(name_lc) >= 6 and len(name_lc) <= 60 and name_lc in claim_lc:
                seen_ids.add(sid)
                matched.append(s)
                if len(matched) >= MAX_RESULTS:
                    return matched

    # 3) Token-Overlap (Fallback)
    if len(matched) < MAX_RESULTS:
        claim_tokens = _tokenize(claim_lc)
        # Stoppwort-Filter: "world", "heritage", "welterbe", "unesco" sind
        # generische Trigger und sollen NICHT als Match-Tokens zaehlen.
        stopwords = {
            "world", "heritage", "welterbe", "unesco", "site", "sites",
            "list", "liste", "kulturerbe", "naturerbe", "weltkulturerbe",
            "weltnaturerbe", "stadt", "altstadt", "city", "historic",
            "historisch", "historisches", "zentrum", "centre", "center",
        }
        effective_claim_tokens = claim_tokens - stopwords
        if effective_claim_tokens:
            scored: list[tuple[int, dict]] = []
            for s in sites:
                sid = s["id"] or s["name_lc"]
                if sid in seen_ids:
                    continue
                site_tokens = s["tokens"] - stopwords
                if not site_tokens:
                    continue
                overlap = len(site_tokens & effective_claim_tokens)
                if overlap >= 2:
                    scored.append((overlap, s))
            scored.sort(key=lambda kv: kv[0], reverse=True)
            for _, s in scored:
                sid = s["id"] or s["name_lc"]
                if sid in seen_ids:
                    continue
                seen_ids.add(sid)
                matched.append(s)
                if len(matched) >= MAX_RESULTS:
                    break

    return matched


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _country_label(iso_list: list[str], states: str) -> tuple[str, str]:
    """Returns (country_code, country_name_de)."""
    if not iso_list:
        return ("—", states or "—")
    if len(iso_list) == 1:
        iso = iso_list[0]
        name = _COUNTRY_NAMES_DE.get(iso, states or iso.upper())
        return (iso.upper(), name)
    # Transnational
    names = [
        _COUNTRY_NAMES_DE.get(c, c.upper()) for c in iso_list[:3]
    ]
    suffix = ""
    if len(iso_list) > 3:
        suffix = f" u. a. (+{len(iso_list) - 3})"
    return (
        ",".join(c.upper() for c in iso_list),
        ", ".join(names) + suffix,
    )


def _build_result(site: dict) -> dict | None:
    name = site.get("name")
    if not name:
        return None
    year = site.get("year") or "—"
    criteria = site.get("criteria") or ""
    category = site.get("category") or ""
    iso_list = site.get("iso_list") or []
    states = site.get("states") or ""
    danger = site.get("danger") or ""
    short = site.get("short") or ""
    url = site.get("url") or "https://whc.unesco.org/en/list/"
    sid = site.get("id") or ""

    country_code, country_name = _country_label(iso_list, states)

    # Display: "Hallstatt-Dachstein / Salzkammergut Cultural Landscape —
    #          Welterbe seit 1997 (Kulturerbe, Kriterien (iii)(iv)), Österreich"
    bits = [name, f"— Welterbe seit {year}"]
    info_bits: list[str] = []
    if category:
        # Übersetze category in DE
        cat_de = {
            "Cultural": "Kulturerbe",
            "Natural": "Naturerbe",
            "Mixed": "Gemischtes Erbe",
        }.get(category, category)
        info_bits.append(cat_de)
    if criteria:
        info_bits.append(f"Kriterien {criteria}")
    if info_bits:
        bits.append(f"({', '.join(info_bits)})")
    if country_name and country_name != "—":
        bits.append(f"— {country_name}")
    if danger:
        bits.append("[gefährdet]")
    display_value = _trim(" ".join(bits), 300)

    desc_parts: list[str] = []
    if short:
        desc_parts.append(_trim(short, 320))
    if site.get("transnational"):
        desc_parts.append(f"Transnationale Stätte ({len(iso_list)} Staaten).")
    if danger:
        desc_parts.append("Auf der Liste des gefährdeten Welterbes.")
    description = _trim(" ".join(desc_parts) or
                        f"UNESCO-Welterbestätte ({category or 'inskribiert'}).",
                        600)

    indicator_id = f"world_heritage_{sid}" if sid else f"world_heritage_{abs(hash(name)) % 10**8}"

    return {
        "indicator_name": f"UNESCO World Heritage: {name}",
        "indicator": indicator_id,
        "country": country_code,
        "country_name": country_name,
        "year": year,
        "value": None,
        "display_value": display_value,
        "description": description,
        "url": url,
        "source": "UNESCO World Heritage Centre",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_world_heritage(analysis: dict) -> dict:
    """Live-Lookup gegen WHC-XML fuer Welterbe-Claims.

    Returns Dict mit ≤5 Welterbe-Treffern. Bei Trigger-Miss / leerem Cache /
    0 Treffern: leere results-Liste (graceful fail).

    Politische Guardrails: Pure Inskriptions-Metadaten, kein Polit-Bezug.
    """
    empty = {
        "source": "UNESCO World Heritage",
        "type": "world_heritage",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or ""
    original = analysis.get("original_claim") or claim
    if not isinstance(claim, str):
        claim = str(claim or "")
    if not isinstance(original, str):
        original = str(original or "")

    matchable = f"{original} {claim}".lower().strip()
    if not _claim_mentions_world_heritage(matchable):
        return empty

    try:
        sites = await asyncio.wait_for(
            _ensure_sites_loaded(),
            timeout=HTTP_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        logger.info("UNESCO World Heritage: Timeout beim XML-Load")
        return empty
    except Exception as e:
        logger.debug(f"UNESCO World Heritage: load-error: {e}")
        return empty

    if not sites:
        return empty

    matched = _match_sites(matchable, sites)
    if not matched:
        logger.info(
            f"UNESCO World Heritage: 0 Site-Matches fuer claim "
            f"(len={len(matchable)})"
        )
        return empty

    results: list[dict] = []
    for s in matched[:MAX_RESULTS]:
        try:
            built = _build_result(s)
        except Exception as e:
            logger.debug(f"UNESCO World Heritage: build-error: {e}")
            continue
        if built:
            results.append(built)

    logger.info(
        f"UNESCO World Heritage: {len(results)} Treffer "
        f"(matched={[s['name'][:40] for s in matched[:MAX_RESULTS]]})"
    )
    return {
        "source": "UNESCO World Heritage",
        "type": "world_heritage",
        "results": results,
    }
