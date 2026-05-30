"""GBIF Live-Connector — Tier-/Pflanzen-Vorkommen via REST API.

GBIF (Global Biodiversity Information Facility) ist die größte
Biodiversitäts-Datenbank weltweit (~2,5 Mrd Artenbeobachtungen) und
liefert für Faktencheck-Zwecke:
- Taxonomische Validierung von Tier-/Pflanzennamen (/species/match)
- Beobachtungs-Counts pro Land (z.B. AT) als Proxy für Vorkommen
- Wissenschaftliche Klassifikation (Reich / Stamm / Klasse / Familie)

Komplementär zu existierenden Quellen:
- AGES / Veterinärmedizin: Tier-Krankheiten + Statistik
- OWID: Klimadaten + globale Trends
- GBIF: konkrete Spezies-Vorkommen + Beobachtungs-Häufigkeit (NICHT
  Bestandsschätzungen — dafür wäre IUCN Red List besser)

API: https://api.gbif.org/v1/species/match?name={name}
   + https://api.gbif.org/v1/occurrence/search?country={cc}&taxon_key={key}

Free, kein API-Key nötig.

Trigger: Claim enthält Tier-/Pflanzennamen aus _DE_TO_LATIN-Liste
UND (geografischer Bezug ODER Population-Marker).

Wiring: main.py imports + tasks.append. NICHT in
AUTHORITATIVE_INDICATORS (ist Live-Quelle, kein kuratierter
Konsens-Pack).

Limitationen:
- GBIF zählt BEOBACHTUNGEN, nicht POPULATION — niemand kann aus 338
  iNaturalist-Sichtungen auf die Anzahl der Wildtiere schließen.
- iNaturalist-Bias: häufig fotografierte Tiere (Vögel, Schmetterlinge)
  haben überproportional viele Records vs. nachtaktive Tiere.
- Country-Filter ist genau (ISO 3166 Alpha-2), Region-Filter (Tirol,
  Salzburg) NICHT möglich auf GBIF-Level (nur Lat/Lon).
- Taxonomische Resolution: nur Latein + EN Vernacular-Names robust.
  DE-Namen müssen über lokale Liste DE → Latein gemappt werden.
"""

import logging
from urllib.parse import quote

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

GBIF_SPECIES_MATCH_API = "https://api.gbif.org/v1/species/match?name={name}"
GBIF_OCCURRENCE_SEARCH_API = (
    "https://api.gbif.org/v1/occurrence/search"
    "?country={cc}&taxon_key={key}&limit=0"
)

# Mapping DE → Lateinischer Name für AT-relevante Spezies.
# Erweiterbar bei Bedarf.
_DE_TO_LATIN = {
    "wolf": "Canis lupus",
    "wölfe": "Canis lupus",
    "wölfen": "Canis lupus",
    "bär": "Ursus arctos",
    "bären": "Ursus arctos",
    "braunbär": "Ursus arctos",
    "luchs": "Lynx lynx",
    "luchse": "Lynx lynx",
    "luchsen": "Lynx lynx",
    "fischotter": "Lutra lutra",
    "biber": "Castor fiber",
    "wildschwein": "Sus scrofa",
    "wildschweine": "Sus scrofa",
    "wildkatze": "Felis silvestris",
    "steinadler": "Aquila chrysaetos",
    "seeadler": "Haliaeetus albicilla",
    "bartgeier": "Gypaetus barbatus",
    "alpenmurmeltier": "Marmota marmota",
    "murmeltier": "Marmota marmota",
    "alpensteinbock": "Capra ibex",
    "steinbock": "Capra ibex",
    "gämse": "Rupicapra rupicapra",
    "gemse": "Rupicapra rupicapra",
    "rotfuchs": "Vulpes vulpes",
    "feldhamster": "Cricetus cricetus",
    "iltis": "Mustela putorius",
    "marder": "Martes martes",
    "dachs": "Meles meles",
    "wildkaninchen": "Oryctolagus cuniculus",
    "feldhase": "Lepus europaeus",
    "rothirsch": "Cervus elaphus",
    "hirsch": "Cervus elaphus",
    "reh": "Capreolus capreolus",
    "wisent": "Bison bonasus",
}

# Geo-Marker-Keywords für stärkeren Trigger.
_GEO_KEYWORDS = (
    "österreich", "austria", "tirol", "kärnten", "salzburg",
    "steiermark", "burgenland", "wien", "niederösterreich",
    "oberösterreich", "vorarlberg", "alpen", "donau", "österreichisch",
)

# Population-Marker die Bestandsfragen signalisieren.
_POPULATION_KEYWORDS = (
    "vorkommen", "population", "bestand", "sichtung", "sichtungen",
    "beobachtung", "beobachtungen", "verbreitet", "verbreitung",
    "ausgerottet", "ausgestorben", "eingewandert", "invasiv",
    "neuzuwanderer", "rückkehr", "ausgewildert", "rudel", "rudeln",
)

MAX_SPECIES = 3
TIMEOUT_S = 12.0


def _extract_species_keys(claim: str) -> list[tuple[str, str]]:
    """Extrahiere (DE-Name, Latein-Name) Paare aus Claim-Text.

    Returns Liste von max MAX_SPECIES Spezies-Paaren, dedupliziert
    auf Latein-Namen-Ebene (Wolf + Wölfe → ein Eintrag).
    """
    if not claim:
        return []
    text = claim.lower()
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for de_name, latin in _DE_TO_LATIN.items():
        if de_name in text:
            if latin.lower() in seen:
                continue
            seen.add(latin.lower())
            out.append((de_name, latin))
            if len(out) >= MAX_SPECIES:
                break
    return out


def claim_triggers_gbif(claim: str) -> bool:
    """Trigger-Check für main.py-Wiring.

    Spezies-Keyword UND (Geo-Bezug ODER Population-Marker).
    """
    if not claim:
        return False
    text = claim.lower()
    has_species = any(k in text for k in _DE_TO_LATIN)
    if not has_species:
        return False
    has_geo = any(k in text for k in _GEO_KEYWORDS)
    has_pop = any(k in text for k in _POPULATION_KEYWORDS)
    return has_geo or has_pop


def _format_count(n: int) -> str:
    """1234567 → '1.234.567' (DE-Tausender-Punkt)."""
    return f"{n:,}".replace(",", ".")


def _format_result(
    de_name: str,
    latin: str,
    match: dict,
    occurrence_count: int,
    country: str = "AT",
) -> dict:
    """Formatiere GBIF-Antwort zum Evidora-Result-Schema."""
    scientific = match.get("scientificName") or latin
    rank = match.get("rank") or "—"
    family = match.get("family") or ""
    order = match.get("order") or ""
    klass = match.get("class") or ""
    taxonomy_chain = " > ".join(
        [c for c in [klass, order, family, scientific] if c]
    )
    usage_key = match.get("usageKey")
    count_str = _format_count(occurrence_count)

    indicator_name = (
        f"GBIF: {de_name.capitalize()} ({scientific}) — "
        f"{count_str} Beobachtungen in {country}"
    )[:300]

    display_value = (
        f"{count_str} dokumentierte Beobachtungen in {country} "
        f"({scientific}, {rank}). Taxonomie: {taxonomy_chain}."
    )[:500]

    description = (
        "GBIF (Global Biodiversity Information Facility) sammelt "
        "Artenbeobachtungs-Daten weltweit (Citizen Science via "
        "iNaturalist + wissenschaftliche Surveys). Counts sind ein "
        "Proxy für Vorkommen, NICHT für Population/Bestand — dafür "
        "wäre IUCN Red List besser."
    )

    if usage_key:
        url = (
            f"https://www.gbif.org/occurrence/search?country={country}"
            f"&taxon_key={usage_key}"
        )
        secondary_url = f"https://api.gbif.org/v1/species/{usage_key}"
    else:
        url = f"https://www.gbif.org/species/search?q={quote(scientific)}"
        secondary_url = ""

    return {
        "indicator_name": indicator_name,
        "indicator": f"gbif_occurrences_{country.lower()}",
        "country": country,
        "year": "—",  # GBIF-Counts sind kumulativ, kein Jahr
        "topic": "biodiversity_occurrences",
        "display_value": display_value,
        "description": description,
        "url": url,
        "secondary_url": secondary_url,
        "source": "GBIF REST API (frei, Open Data)",
    }


async def _match_species(client, latin: str) -> dict | None:
    """GBIF /species/match — liefert usageKey + Taxonomie.

    Returns dict mit usageKey, scientificName, rank, family, order,
    class oder None bei Fail.
    """
    url = GBIF_SPECIES_MATCH_API.format(name=quote(latin))
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"GBIF species/match HTTP {resp.status_code} for '{latin}'"
            )
            return None
        data = resp.json()
        if data.get("matchType") in (None, "NONE"):
            return None
        return data
    except Exception as e:
        logger.debug(f"GBIF species/match failed for '{latin}': {e}")
        return None


async def _count_occurrences(
    client, usage_key: int, country: str = "AT"
) -> int:
    """GBIF /occurrence/search mit limit=0 — nur Count, keine Records."""
    url = GBIF_OCCURRENCE_SEARCH_API.format(cc=country, key=usage_key)
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            return 0
        data = resp.json()
        return int(data.get("count") or 0)
    except Exception as e:
        logger.debug(
            f"GBIF occurrence count failed for usageKey {usage_key}: {e}"
        )
        return 0


async def search_gbif(analysis: dict) -> dict:
    """Live-Lookup gegen GBIF REST API für Spezies-Vorkommen in AT.

    Returns Dict mit ≤MAX_SPECIES GBIF-Treffern. Trigger nur wenn
    Spezies-Keyword + (Geo-Bezug ODER Population-Marker) im Claim.
    """
    empty = {
        "source": "GBIF",
        "type": "biodiversity_occurrences",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original") or ""
    if not isinstance(claim, str):
        claim = str(claim or "")

    if not claim_triggers_gbif(claim):
        return empty

    species_pairs = _extract_species_keys(claim)
    if not species_pairs:
        return empty

    results: list[dict] = []
    async with polite_client(timeout=TIMEOUT_S) as client:
        for de_name, latin in species_pairs:
            match = await _match_species(client, latin)
            if not match:
                continue
            usage_key = match.get("usageKey")
            if not usage_key:
                continue
            count = await _count_occurrences(
                client, usage_key, country="AT"
            )
            result = _format_result(
                de_name, latin, match, count, country="AT"
            )
            results.append(result)

    if not results:
        logger.info(
            f"GBIF: 0 Treffer für Species-Pairs {species_pairs[:3]}"
        )
        return empty

    logger.info(f"GBIF: {len(results)} Treffer geliefert")
    return {
        "source": "GBIF",
        "type": "biodiversity_occurrences",
        "results": results,
    }
