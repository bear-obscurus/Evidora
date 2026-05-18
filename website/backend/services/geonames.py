"""GeoNames — Live-Connector für globale Geo-Namen + Wikipedia + PLZ.

Datenquelle: https://www.geonames.org — 11+ Mio. geografische Namen
weltweit, mit verknüpften Wikipedia-Artikeln, Postleitzahlen-Lookup,
Reverse-Geocoding. Pflegt einen redaktionell kuratierten Datenbestand
und integriert OSM-/Wikipedia-/admin-2-Quellen.

Lizenz: CC BY 4.0 (Attribution erforderlich) — Evidora-tauglich.

API: https://api.geonames.org/  bzw.  https://secure.geonames.org/
- /searchJSON?q=...                 → Volltext-Suche
- /postalCodeSearchJSON?postalcode= → PLZ-Lookup
- /findNearbyWikipediaJSON?lat=&lng=→ Wikipedia-Verknüpfung
- Username via env GEONAMES_USERNAME (kostenlos via
  https://www.geonames.org/login → E-Mail-Bestätigung erforderlich,
  danach "free web services" im Account-Profil aktivieren).
- Kontingent: 10.000 credits/Tag, 1.000/h, 1 req/s.
- Response: JSON. Bei Fehlern liefert die API
  ``{"status":{"message":..., "value":<code>}}`` statt ``geonames[]``.

Hinweis zur Hostname-Wahl:
- ``api.geonames.org`` hat ein SSL-Zertifikat mit CN=secure.geonames.org,
  d. h. HTTPS schlägt mit „SSL: no alternative certificate subject name
  matches target host name" fehl. Wir verwenden daher
  ``secure.geonames.org`` für HTTPS (offizielle SSL-Domain).

Komplementär zu nominatim.py (OSM Nominatim, ODbL):
- Nominatim: Adress-Hierarchie, Geometrie, OSM-Klassifikation. Strenges
  1-req/s-Limit, kein Account.
- GeoNames: PLZ-Lookup (postalCodeSearchJSON), Wikipedia-Anchor
  (wikipediaURL pro Treffer), administrative Hierarchie über
  admin1/admin2-Codes. Username-Pool mit 10k Tages-Credits.
- KEIN Hard-Skip wenn Nominatim auch feuert — beide Services dürfen
  parallel laufen, die Synthesizer-Logik aggregiert.

Politische Guardrails: Reine Geo-Daten, keine politischen Aussagen.
Bei "Hauptstadt-von"-Claims bleibt die Bewertung beim Synthesizer;
GeoNames liefert nur die geographische Tatsachen-Lage.

Wiring: NICHT in AUTHORITATIVE_INDICATORS — Live-Quelle.
"""

# WIRING für main.py:
# from services.geonames import search_geonames, claim_mentions_geonames_cached
# if claim_mentions_geonames_cached(claim):
#     tasks.append(cached("GeoNames", search_geonames, analysis))
#     queried_names.append("GeoNames")

from __future__ import annotations

import logging
import os
import re
import time
from urllib.parse import quote

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# secure.geonames.org liefert das passende SSL-Zertifikat; der
# „api."-Host ist HTTP-only oder schlägt mit Hostname-Mismatch fehl.
GEONAMES_BASE_URL = "https://secure.geonames.org"
GEONAMES_SEARCH_URL = f"{GEONAMES_BASE_URL}/searchJSON"
GEONAMES_POSTAL_URL = f"{GEONAMES_BASE_URL}/postalCodeSearchJSON"
GEONAMES_WIKI_URL = f"{GEONAMES_BASE_URL}/findNearbyWikipediaJSON"
GEONAMES_PAGE_TPL = "https://www.geonames.org/{geoname_id}"

# Username MUSS gesetzt sein — kein hardcodiertes „demo", weil der Demo-
# Account global rate-limited ist und in Production unbrauchbar wäre.
GEONAMES_USERNAME = (os.getenv("GEONAMES_USERNAME") or "").strip()

# Knappes Kontingent (10k Credits/Tag) → aggressiv cachen.
HTTP_TIMEOUT_S = 12.0
MAX_LOCATIONS_PER_CLAIM = 2
MAX_RESULTS_PER_LOCATION = 3
_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = 24 * 3600.0


# ---------------------------------------------------------------------------
# Trigger-Detection
# ---------------------------------------------------------------------------
#
# GeoNames feuert bei klar PLZ-/Wikipedia-bezogenen Claims sowie bei
# allgemeinen Geo-Lookup-Phrasen. Komplementär zu Nominatim:
# - Nominatim: Adress-Hierarchie + Geometrie (z. B. „liegt am Bodensee")
# - GeoNames: PLZ-Lookup + Wikipedia-Anchor + admin-Hierarchie
#
# Wir verzichten bewusst auf einen Hard-Skip wenn Nominatim auch feuert —
# beide Services liefern komplementäre Datenpunkte.

_GEONAMES_TRIGGER_TERMS: tuple[str, ...] = (
    # PLZ-Patterns
    "postleitzahl", "plz ", " plz",
    "postal code", "zip code", "zip-code",
    # Wikipedia-Verknüpfung (geo-spezifisch)
    "wikipedia-eintrag", "wikipedia eintrag",
    "wikipedia-artikel über",
    # Geo-Lookup
    "wo liegt", "wo befindet sich",
    "koordinaten von", "koordinaten der",
    "geokoordinaten", "geo-koordinaten",
    "längengrad", "breitengrad",
    # Ort-in-Land
    "stadt in ", "ort in ",
    "liegt in", "befindet sich in",
    "liegt im", "befindet sich im",
    # Generische Geo-Suche
    "ortsname", "geonames",
)

# Negative Trigger — Personen-Lebensdaten sind Wikidata-Domäne; wir
# wollen für „X wurde in Salzburg geboren" kein Geo-Lookup feuern.
_PERSON_CONTEXT_TERMS: tuple[str, ...] = (
    "geboren in", "geboren am", "geburtsdatum",
    "gestorben in", "gestorben am", "verstarb in",
)

# 4-stellige Zahl im Claim → starker AT/DE-PLZ-Indikator.
_POSTAL_4DIGIT_RE = re.compile(r"\b\d{4}\b")
# 5-stellige Zahl → DE-PLZ.
_POSTAL_5DIGIT_RE = re.compile(r"\b\d{5}\b")


def _claim_mentions_geonames(claim_lc: str) -> bool:
    """Prüft, ob der Claim einen GeoNames-Lookup rechtfertigt."""
    if not claim_lc:
        return False
    if any(t in claim_lc for t in _PERSON_CONTEXT_TERMS):
        return False
    if any(t in claim_lc for t in _GEONAMES_TRIGGER_TERMS):
        return True
    # PLZ-Komposit: 4/5-stellige Zahl + AT/DE-Kontext
    has_postal_digits = bool(
        _POSTAL_4DIGIT_RE.search(claim_lc)
        or _POSTAL_5DIGIT_RE.search(claim_lc)
    )
    if has_postal_digits:
        has_geo_ctx = any(t in claim_lc for t in (
            "postleitzahl", "plz", "österreich", "deutschland",
            "wien", "salzburg", "graz", "linz", "innsbruck",
            "berlin", "münchen", "hamburg", "köln",
        ))
        if has_geo_ctx:
            return True
    # „suchen in [Land]" Pattern für Geo-Suche
    if re.search(r"\b(?:suchen|finden) in\b[^.?!]{1,30}\b(?:österreich|deutschland|schweiz|europa)\b", claim_lc):
        return True
    return False


def claim_mentions_geonames_cached(claim: str) -> bool:
    """Stable Lower-Case-Wrapper für main.py-Wiring."""
    return _claim_mentions_geonames((claim or "").lower())


# ---------------------------------------------------------------------------
# Place / Postal-Code-Extraction
# ---------------------------------------------------------------------------

_PLACE_BLACKLIST: frozenset[str] = frozenset({
    "Postleitzahl", "PLZ", "Hauptstadt", "Stadt", "Ort", "Land",
    "Adresse", "Anschrift", "Koordinaten", "Geokoordinaten",
    "Längengrad", "Breitengrad", "Wikipedia", "GeoNames",
    "Google", "Nominatim", "OpenStreetMap",
    "Januar", "Februar", "März", "April", "Mai", "Juni",
    "Juli", "August", "September", "Oktober", "November", "Dezember",
    "Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag",
    "Samstag", "Sonntag",
})

_TOKEN_PATTERN = re.compile(
    r"\b([A-ZÄÖÜ][a-zäöüß]{2,}(?:[- ][A-ZÄÖÜ][a-zäöüß]{2,})?)\b"
)


def _extract_places(claim: str, analysis: dict) -> list[str]:
    """Liefert plausible Ortsnamen — analysis.entities + Capital-Fallback."""
    candidates: list[str] = []
    seen_lc: set[str] = set()

    def _push(name: str) -> None:
        n = (name or "").strip().strip(",. ;:!?")
        if not n or len(n) < 3:
            return
        if n in _PLACE_BLACKLIST:
            return
        key = n.lower()
        if key in seen_lc:
            return
        seen_lc.add(key)
        candidates.append(n)

    entities = (analysis or {}).get("entities", []) or []
    for ent in entities:
        if isinstance(ent, str):
            _push(ent)
        elif isinstance(ent, dict):
            v = ent.get("name") or ent.get("value") or ent.get("text")
            if isinstance(v, str):
                _push(v)

    if len(candidates) < MAX_LOCATIONS_PER_CLAIM and claim:
        for match in _TOKEN_PATTERN.finditer(claim):
            _push(match.group(1))
            if len(candidates) >= MAX_LOCATIONS_PER_CLAIM:
                break

    return candidates[:MAX_LOCATIONS_PER_CLAIM]


_AT_DE_COUNTRY_CONTEXT = {
    "österreich": "AT", "austria": "AT",
    "deutschland": "DE", "germany": "DE",
    "schweiz": "CH", "switzerland": "CH",
}


def _extract_postal_codes(claim: str) -> list[tuple[str, str | None]]:
    """Liefert ``(postalcode, country_iso2_or_None)``-Tupel aus dem Claim.

    Country wird aus dem Claim-Kontext geraten (AT/DE/CH). Wenn kein
    Hinweis: ``None`` → die API sucht global (großzügig, aber stabil).
    """
    out: list[tuple[str, str | None]] = []
    claim_lc = (claim or "").lower()
    country_hint: str | None = None
    for term, cc in _AT_DE_COUNTRY_CONTEXT.items():
        if term in claim_lc:
            country_hint = cc
            break
    seen: set[str] = set()
    # 5-stellige zuerst (DE), dann 4-stellige (AT/CH); kein Duplikat.
    for rx, default_cc in ((_POSTAL_5DIGIT_RE, "DE"), (_POSTAL_4DIGIT_RE, "AT")):
        for match in rx.finditer(claim or ""):
            code = match.group(0)
            if code in seen:
                continue
            seen.add(code)
            out.append((code, country_hint or default_cc))
            if len(out) >= MAX_LOCATIONS_PER_CLAIM:
                return out
    return out


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
_COUNTRY_ISO2_TO_ISO3 = {
    "AT": "AUT", "DE": "DEU", "CH": "CHE", "LI": "LIE", "IT": "ITA",
    "FR": "FRA", "ES": "ESP", "PT": "PRT", "NL": "NLD", "BE": "BEL",
    "LU": "LUX", "DK": "DNK", "SE": "SWE", "NO": "NOR", "FI": "FIN",
    "IS": "ISL", "IE": "IRL", "GB": "GBR", "US": "USA", "CA": "CAN",
    "MX": "MEX", "BR": "BRA", "AR": "ARG", "CZ": "CZE", "SK": "SVK",
    "PL": "POL", "HU": "HUN", "SI": "SVN", "HR": "HRV", "RS": "SRB",
    "RO": "ROU", "BG": "BGR", "GR": "GRC", "TR": "TUR", "UA": "UKR",
    "RU": "RUS", "BY": "BLR", "EE": "EST", "LV": "LVA", "LT": "LTU",
    "JP": "JPN", "CN": "CHN", "KR": "KOR", "IN": "IND", "AU": "AUS",
    "NZ": "NZL", "ZA": "ZAF",
}

_COUNTRY_ISO2_TO_NAME_DE = {
    "AT": "Österreich", "DE": "Deutschland", "CH": "Schweiz",
    "LI": "Liechtenstein", "IT": "Italien", "FR": "Frankreich",
    "ES": "Spanien", "PT": "Portugal", "NL": "Niederlande",
    "BE": "Belgien", "LU": "Luxemburg", "DK": "Dänemark",
    "SE": "Schweden", "NO": "Norwegen", "FI": "Finnland",
    "IS": "Island", "IE": "Irland", "GB": "Vereinigtes Königreich",
    "US": "Vereinigte Staaten",
}


def _format_coord(lat, lng) -> str:
    try:
        flat = float(lat)
        flng = float(lng)
    except (TypeError, ValueError):
        return ""
    ns = "N" if flat >= 0 else "S"
    ew = "E" if flng >= 0 else "W"
    return f"{abs(flat):.4f}°{ns}, {abs(flng):.4f}°{ew}"


def _build_search_result(query: str, entry: dict) -> dict | None:
    """Mappt einen /searchJSON-Eintrag auf das Evidora-Result-Schema."""
    if not isinstance(entry, dict):
        return None

    geoname_id = entry.get("geonameId")
    name = entry.get("name") or query
    cc = (entry.get("countryCode") or "").upper()
    country_name = (
        _COUNTRY_ISO2_TO_NAME_DE.get(cc)
        or entry.get("countryName")
        or "—"
    )
    iso3 = _COUNTRY_ISO2_TO_ISO3.get(cc, cc or "—")
    fcl = entry.get("fcl") or ""
    fcode = entry.get("fcode") or ""
    population = entry.get("population")
    admin1 = entry.get("adminName1") or ""

    coord_str = _format_coord(entry.get("lat"), entry.get("lng"))

    display_bits = [name]
    if admin1 and admin1.lower() != name.lower():
        display_bits.append(f", {admin1}")
    if country_name and country_name != "—":
        display_bits.append(f", {country_name}")
    if coord_str:
        display_bits.append(f" — {coord_str}")
    display_value = "".join(display_bits)

    try:
        pop_str = (
            f"{int(population):,}".replace(",", ".")
            if population not in (None, "")
            else "—"
        )
    except (TypeError, ValueError):
        pop_str = "—"

    fcl_label = {
        "P": "populated place", "A": "administrative",
        "H": "hydrographic", "T": "terrain", "S": "spot/structure",
        "L": "area", "V": "vegetation", "R": "road/railroad",
        "U": "undersea",
    }.get(fcl, fcl or "place")

    description = (
        f"GeoNames-Klassifikation: {fcl_label}"
        f"{f' ({fcode})' if fcode else ''}; "
        f"Einwohner: {pop_str}; admin1={admin1 or '—'}."
    )

    if geoname_id:
        url = GEONAMES_PAGE_TPL.format(geoname_id=geoname_id)
        indicator = f"geonames_{geoname_id}"
    else:
        url = f"https://www.geonames.org/search.html?q={quote(name)}"
        indicator = "geonames_unknown"

    return {
        "indicator_name": f"{name}{f', {country_name}' if country_name != '—' else ''}",
        "indicator": indicator,
        "country": iso3,
        "country_name": country_name,
        "year": None,
        "value": None,
        "display_value": display_value,
        "description": description,
        "url": url,
        "source": "GeoNames (CC BY 4.0)",
    }


def _build_postal_result(query_code: str, entry: dict) -> dict | None:
    """Mappt einen /postalCodeSearchJSON-Eintrag."""
    if not isinstance(entry, dict):
        return None
    place_name = entry.get("placeName") or ""
    postal = entry.get("postalCode") or query_code
    cc = (entry.get("countryCode") or "").upper()
    country_name = (
        _COUNTRY_ISO2_TO_NAME_DE.get(cc)
        or entry.get("countryName")
        or "—"
    )
    iso3 = _COUNTRY_ISO2_TO_ISO3.get(cc, cc or "—")
    admin1 = entry.get("adminName1") or ""

    coord_str = _format_coord(entry.get("lat"), entry.get("lng"))

    display = f"PLZ {postal} = {place_name}"
    if admin1:
        display += f", {admin1}"
    if country_name and country_name != "—":
        display += f", {country_name}"
    if coord_str:
        display += f" — {coord_str}"

    description = (
        f"GeoNames-PLZ-Lookup: postal={postal}, place='{place_name}', "
        f"admin1='{admin1 or '—'}', country={cc or '—'}."
    )

    return {
        "indicator_name": f"PLZ {postal}: {place_name}",
        "indicator": f"geonames_postal_{cc}_{postal}",
        "country": iso3,
        "country_name": country_name,
        "year": None,
        "value": postal,
        "display_value": display,
        "description": description,
        "url": f"https://www.geonames.org/postalcode-search.html?q={quote(postal)}&country={cc}",
        "source": "GeoNames (CC BY 4.0)",
    }


# ---------------------------------------------------------------------------
# HTTP-Fetch
# ---------------------------------------------------------------------------
def _is_api_error(payload) -> int | None:
    """Liefert den API-Error-Code, falls payload ein status-Dict ist."""
    if isinstance(payload, dict):
        status = payload.get("status")
        if isinstance(status, dict) and "value" in status:
            try:
                return int(status.get("value"))
            except (TypeError, ValueError):
                return -1
    return None


async def _fetch_search(client, query: str) -> list[dict]:
    """1 Call gegen /searchJSON → roh-Liste (max MAX_RESULTS_PER_LOCATION)."""
    params = {
        "q": query,
        "maxRows": str(MAX_RESULTS_PER_LOCATION),
        "username": GEONAMES_USERNAME,
        "type": "json",
        "style": "FULL",
    }
    try:
        resp = await client.get(GEONAMES_SEARCH_URL, params=params)
        if resp.status_code != 200:
            logger.info(
                f"GeoNames /searchJSON HTTP {resp.status_code} für "
                f"'{query[:50]}': {resp.text[:120]!r}"
            )
            return []
        data = resp.json()
        err = _is_api_error(data)
        if err is not None:
            msg = (data.get("status") or {}).get("message", "")[:160]
            logger.info(f"GeoNames /searchJSON status={err}: {msg}")
            return []
        entries = data.get("geonames") if isinstance(data, dict) else None
        if not isinstance(entries, list):
            return []
        return entries[:MAX_RESULTS_PER_LOCATION]
    except Exception as e:
        logger.info(f"GeoNames /searchJSON failed für '{query[:50]}': {e}")
        return []


async def _fetch_postal(client, code: str, country: str | None) -> list[dict]:
    """1 Call gegen /postalCodeSearchJSON."""
    params: dict[str, str] = {
        "postalcode": code,
        "maxRows": str(MAX_RESULTS_PER_LOCATION),
        "username": GEONAMES_USERNAME,
    }
    if country:
        params["country"] = country
    try:
        resp = await client.get(GEONAMES_POSTAL_URL, params=params)
        if resp.status_code != 200:
            logger.info(
                f"GeoNames /postalCodeSearchJSON HTTP {resp.status_code} "
                f"für '{code}': {resp.text[:120]!r}"
            )
            return []
        data = resp.json()
        err = _is_api_error(data)
        if err is not None:
            msg = (data.get("status") or {}).get("message", "")[:160]
            logger.info(f"GeoNames /postalCodeSearchJSON status={err}: {msg}")
            return []
        entries = data.get("postalCodes") if isinstance(data, dict) else None
        if not isinstance(entries, list):
            return []
        return entries[:MAX_RESULTS_PER_LOCATION]
    except Exception as e:
        logger.info(f"GeoNames /postalCodeSearchJSON failed für '{code}': {e}")
        return []


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_geonames(analysis: dict) -> dict:
    """Live-Lookup gegen GeoNames /searchJSON + /postalCodeSearchJSON.

    Strategie:
    1. Trigger-Check (Lower-Case-Substrings).
    2. Wenn GEONAMES_USERNAME nicht gesetzt → graceful empty Return,
       die Service-Pipeline darf das nicht zum Fehler eskalieren lassen.
    3. PLZ-Extraktion: 4/5-stellige Zahlen → /postalCodeSearchJSON.
    4. Place-Extraktion: bis zu 2 Ortsnamen → /searchJSON.
    5. Pro Lookup Top-1 übernehmen; Cache 24 h.
    """
    empty = {
        "source": "GeoNames",
        "type": "geographic_data",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_geonames(matchable):
        return empty

    if not GEONAMES_USERNAME:
        logger.info(
            "GeoNames: Trigger ok, aber GEONAMES_USERNAME nicht gesetzt — "
            "graceful empty (Account via geonames.org/login → free web "
            "services aktivieren, dann env GEONAMES_USERNAME setzen)."
        )
        return empty

    results: list[dict] = []
    now = time.time()

    # 1) PLZ-Lookups
    postal_pairs = _extract_postal_codes(original or claim)

    # 2) Place-Lookups (überspringen Wörter, die wir bereits per PLZ
    #    angefragt hätten — aber Place und PLZ sind disjunkt)
    places = _extract_places(claim or original, analysis or {})

    if not postal_pairs and not places:
        logger.info(
            "GeoNames: Trigger ok, aber weder PLZ noch Ortsname extrahiert"
        )
        return empty

    async with polite_client(timeout=HTTP_TIMEOUT_S) as client:
        # PLZ-Lookups
        for code, country in postal_pairs:
            cache_key = f"postal::{country or '*'}::{code}"
            cached = _CACHE.get(cache_key)
            if cached and (now - cached[0] < _CACHE_TTL_S):
                logger.info(f"GeoNames: Cache-Hit für PLZ {code}")
                results.extend(cached[1].get("results", [])[:1])
                continue

            raw = await _fetch_postal(client, code, country)
            built: list[dict] = []
            seen_ids: set[str] = set()
            for entry in raw:
                r = _build_postal_result(code, entry)
                if not r:
                    continue
                ind = r.get("indicator", "")
                if ind in seen_ids:
                    continue
                seen_ids.add(ind)
                built.append(r)
                if len(built) >= MAX_RESULTS_PER_LOCATION:
                    break

            if built:
                results.append(built[0])
            _CACHE[cache_key] = (now, {"results": built[:1]})

        # Place-Lookups
        for place in places:
            cache_key = f"search::{place.lower()}"
            cached = _CACHE.get(cache_key)
            if cached and (now - cached[0] < _CACHE_TTL_S):
                logger.info(f"GeoNames: Cache-Hit für '{place[:40]}'")
                results.extend(cached[1].get("results", [])[:1])
                continue

            raw = await _fetch_search(client, place)
            built: list[dict] = []
            seen_ids: set[str] = set()
            for entry in raw:
                r = _build_search_result(place, entry)
                if not r:
                    continue
                ind = r.get("indicator", "")
                if ind in seen_ids:
                    continue
                seen_ids.add(ind)
                built.append(r)
                if len(built) >= MAX_RESULTS_PER_LOCATION:
                    break

            if built:
                results.append(built[0])
            _CACHE[cache_key] = (now, {"results": built[:1]})

    # Hard-Cap: max 4 Results gesamt (2 PLZ + 2 Places).
    results = results[: (MAX_LOCATIONS_PER_CLAIM * 2)]

    if results:
        logger.info(
            f"GeoNames: {len(results)} Geo-Treffer für "
            f"postal={[p[0] for p in postal_pairs]}, places={places}"
        )

    return {
        "source": "GeoNames",
        "type": "geographic_data",
        "results": results,
    }
