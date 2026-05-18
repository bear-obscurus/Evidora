"""IUCN Red List — Globaler Gefährdungsstatus von Spezies.

Datenquelle: IUCN Red List of Threatened Species (Version 2025-2, ~157.000
bewertete Spezies). Einzige globale Quelle dieser Art; wird von CITES,
EU-Habitats-Direktive und nationalen Naturschutz-Behörden zitiert.

API-Endpoint:
  https://apiv3.iucnredlist.org/api/v3/species/{scientific_name}?token=X
  https://apiv3.iucnredlist.org/api/v3/species/category/{cat}?token=X
  https://apiv3.iucnredlist.org/api/v3/version (kein Token nötig)

Format: JSON. Token via IUCN-Antrag unter
  https://apiv3.iucnredlist.org/api/v3/token

Lizenz: IUCN-Terms (Zitation Pflicht, non-commercial OK — Evidora deckt
das ab). Quelle ist als Faktenreferenz frei nutzbar.

CLOUDFLARE-BOT-CHALLENGE (Stand 2026-05-18): apiv3.iucnredlist.org ist
hinter einem Cloudflare-Anti-Bot-Layer, der serverseitige httpx-Calls
mit HTTP 403 abweist — auch mit Browser-User-Agent. Damit ist der
Live-Pfad in der Praxis off. Wir lassen den Live-Code aktiviert (für
den Fall, dass die Challenge mit JS-fähigem Token-Pfad lokal mal
durchgeht oder IUCN den Layer lockert), priorisieren aber den
hardcodierten Fallback (30 Schlüssel-Spezies) als Default-Antwort.

IUCN-Kategorien:
  EX  = Extinct (ausgestorben)
  EW  = Extinct in the Wild (in freier Wildbahn ausgestorben)
  CR  = Critically Endangered (vom Aussterben bedroht)
  EN  = Endangered (stark gefährdet)
  VU  = Vulnerable (gefährdet)
  NT  = Near Threatened (potenziell gefährdet)
  LC  = Least Concern (nicht gefährdet)
  DD  = Data Deficient (unzureichende Datenlage)
  NE  = Not Evaluated

Trigger: "[Spezies] gefährdet", "Red List", "IUCN-Status",
"Critically Endangered [X]", "Aussterben [Tier]", "Population [Spezies]".

Politische Guardrails: Spezies-Status sind wissenschaftliche Bewertungen
ohne politische Aufladung — keine Tabu-Berührung.
"""

# WIRING für main.py:
# from services.iucn import search_iucn, claim_mentions_iucn_cached
# if claim_mentions_iucn_cached(claim):
#     tasks.append(cached("IUCN Red List", search_iucn, analysis))
#     queried_names.append("IUCN Red List")
#
# WIRING für data_updater.py (Prefetch — optional, da Fallback statisch):
#   keine Aktion nötig; live-Pfad cached selbst on-demand.
#
# WIRING für reranker (Whitelist):
#   "IUCN Red List" zur Trusted-Source-Liste hinzufügen.

from __future__ import annotations

import logging
import os
import time
from functools import lru_cache
from urllib.parse import quote

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Konstanten
# ---------------------------------------------------------------------------
IUCN_BASE = "https://apiv3.iucnredlist.org/api/v3"
IUCN_PORTAL = "https://www.iucnredlist.org/species"

CACHE_TTL = 24 * 3600  # 24 h
TIMEOUT_S = 15.0
MAX_RESULTS = 5

# Cache: key=(kind, query) → (timestamp, list[result])
_cache: dict[tuple, tuple[float, list[dict]]] = {}

# ---------------------------------------------------------------------------
# Kategorie-Labels (DE)
# ---------------------------------------------------------------------------
_CATEGORY_LABEL_DE = {
    "EX": "Ausgestorben (Extinct)",
    "EW": "In freier Wildbahn ausgestorben (Extinct in the Wild)",
    "CR": "Vom Aussterben bedroht (Critically Endangered)",
    "EN": "Stark gefährdet (Endangered)",
    "VU": "Gefährdet (Vulnerable)",
    "NT": "Potenziell gefährdet (Near Threatened)",
    "LC": "Nicht gefährdet (Least Concern)",
    "DD": "Datenlage unzureichend (Data Deficient)",
    "NE": "Nicht bewertet (Not Evaluated)",
}

_THREAT_CATEGORIES = {"EX", "EW", "CR", "EN", "VU", "NT"}

# ---------------------------------------------------------------------------
# Fallback-Liste: 30 bekannte/häufig gefragte Spezies mit IUCN-Status
# Quelle: IUCN Red List Version 2025-2 (Snapshot Stand 2026-05).
# ---------------------------------------------------------------------------
_FALLBACK_SPECIES: list[dict] = [
    # Großkatzen / Säugetiere
    {"de": "Tiger", "en": "Tiger", "sci": "Panthera tigris", "cat": "EN",
     "pop": "~3.900-5.000 Tiere weltweit (2022). Trend steigend dank Schutz-Programmen.",
     "habitat": "Asien (Indien, Russland, Südostasien)"},
    {"de": "Schneeleopard", "en": "Snow Leopard", "sci": "Panthera uncia", "cat": "VU",
     "pop": "~2.700-3.400 ausgewachsene Tiere. 2017 von EN zu VU herabgestuft.",
     "habitat": "Hochgebirge Zentralasiens"},
    {"de": "Löwe", "en": "Lion", "sci": "Panthera leo", "cat": "VU",
     "pop": "~23.000-39.000 Tiere; Trend fallend. Westafrika-Subpopulation CR.",
     "habitat": "Subsahara-Afrika; Restbestand in Indien (Gir-Wald)"},
    {"de": "Gepard", "en": "Cheetah", "sci": "Acinonyx jubatus", "cat": "VU",
     "pop": "~6.700 ausgewachsene Tiere. Asiatischer Gepard CR (<50 Tiere).",
     "habitat": "Afrika; Restbestand Iran"},
    {"de": "Eisbär", "en": "Polar Bear", "sci": "Ursus maritimus", "cat": "VU",
     "pop": "~22.000-31.000 Tiere; Trend langfristig fallend (Klimawandel/Meereis).",
     "habitat": "Arktis"},
    {"de": "Großer Panda", "en": "Giant Panda", "sci": "Ailuropoda melanoleuca", "cat": "VU",
     "pop": "~1.864 Wildtiere (2014, letzter Zensus). 2016 von EN zu VU herabgestuft.",
     "habitat": "Bambuswälder Zentralchinas"},
    {"de": "Berggorilla", "en": "Mountain Gorilla", "sci": "Gorilla beringei beringei", "cat": "EN",
     "pop": "~1.063 Tiere (2018). 2018 von CR zu EN herabgestuft — Schutzerfolg.",
     "habitat": "Virunga-Vulkane, Bwindi (DR Kongo/Ruanda/Uganda)"},
    {"de": "Orang-Utan (Borneo)", "en": "Bornean Orangutan",
     "sci": "Pongo pygmaeus", "cat": "CR",
     "pop": "~104.700; Trend stark fallend (Palmöl-Plantagen).",
     "habitat": "Borneo"},
    {"de": "Orang-Utan (Sumatra)", "en": "Sumatran Orangutan",
     "sci": "Pongo abelii", "cat": "CR",
     "pop": "~13.846; CR seit 2008.",
     "habitat": "Nord-Sumatra"},
    {"de": "Sumatra-Nashorn", "en": "Sumatran Rhinoceros",
     "sci": "Dicerorhinus sumatrensis", "cat": "CR",
     "pop": "<80 Tiere; akut vom Aussterben bedroht.",
     "habitat": "Sumatra, Borneo"},
    {"de": "Java-Nashorn", "en": "Javan Rhinoceros",
     "sci": "Rhinoceros sondaicus", "cat": "CR",
     "pop": "~76 Tiere (2024) im Ujung-Kulon-Nationalpark.",
     "habitat": "Java (Indonesien)"},
    {"de": "Spitzmaulnashorn", "en": "Black Rhinoceros",
     "sci": "Diceros bicornis", "cat": "CR",
     "pop": "~6.487 Tiere (2022); Trend leicht steigend.",
     "habitat": "Östliches/südliches Afrika"},
    {"de": "Afrikanischer Waldelefant", "en": "African Forest Elephant",
     "sci": "Loxodonta cyclotis", "cat": "CR",
     "pop": "Stark fallend; CR seit 2021 (zuvor mit Steppenelefant zusammen bewertet).",
     "habitat": "Zentralafrika"},
    {"de": "Afrikanischer Steppenelefant", "en": "African Savanna Elephant",
     "sci": "Loxodonta africana", "cat": "EN",
     "pop": "~415.000 (2016); EN seit 2021.",
     "habitat": "Afrika südlich der Sahara"},
    {"de": "Wolf (Eurasien)", "en": "Gray Wolf", "sci": "Canis lupus", "cat": "LC",
     "pop": "Global LC; nationale/regionale Subpopulationen variabel (AT: streng geschützt).",
     "habitat": "Eurasien, Nordamerika"},
    {"de": "Braunbär", "en": "Brown Bear", "sci": "Ursus arctos", "cat": "LC",
     "pop": "~110.000 weltweit; LC. Europa-Subpopulationen kleiner und geschützt.",
     "habitat": "Eurasien, Nordamerika"},
    {"de": "Luchs (Eurasisch)", "en": "Eurasian Lynx", "sci": "Lynx lynx", "cat": "LC",
     "pop": "Global LC; in Mitteleuropa wieder angesiedelt.",
     "habitat": "Europa, Sibirien"},
    {"de": "Iberischer Luchs", "en": "Iberian Lynx", "sci": "Lynx pardinus", "cat": "VU",
     "pop": "~648 ausgewachsene Tiere (2022). 2024 von EN zu VU herabgestuft — Schutzerfolg.",
     "habitat": "Iberische Halbinsel"},
    # Meerestiere
    {"de": "Blauwal", "en": "Blue Whale", "sci": "Balaenoptera musculus", "cat": "EN",
     "pop": "~5.000-15.000; Trend steigend nach Walfang-Stopp.",
     "habitat": "Weltmeere"},
    {"de": "Vaquita", "en": "Vaquita", "sci": "Phocoena sinus", "cat": "CR",
     "pop": "~10 Tiere (2023). Am stärksten bedrohte Meeressäuger-Art.",
     "habitat": "Golf von Kalifornien (Mexiko)"},
    {"de": "Atlantischer Thunfisch", "en": "Atlantic Bluefin Tuna",
     "sci": "Thunnus thynnus", "cat": "LC",
     "pop": "2021 von EN zu LC herabgestuft — Bestand erholt sich.",
     "habitat": "Atlantik, Mittelmeer"},
    {"de": "Weißer Hai", "en": "Great White Shark",
     "sci": "Carcharodon carcharias", "cat": "VU",
     "pop": "Schätzung schwierig; VU seit 1996.",
     "habitat": "Subtropische Meere"},
    {"de": "Mantarochen (Riesenmanta)", "en": "Giant Manta Ray",
     "sci": "Mobula birostris", "cat": "EN",
     "pop": "EN seit 2019. Fischerei-Druck und Beifang.",
     "habitat": "Tropische/subtropische Meere"},
    # Vögel
    {"de": "Kalifornischer Kondor", "en": "California Condor",
     "sci": "Gymnogyps californianus", "cat": "CR",
     "pop": "~561 Tiere (2023, inkl. Gefangenschaft). 1987 EX in the Wild, durch Zucht zurückgebracht.",
     "habitat": "Westliches Nordamerika"},
    {"de": "Kaiserpinguin", "en": "Emperor Penguin",
     "sci": "Aptenodytes forsteri", "cat": "NT",
     "pop": "~600.000 ausgewachsene Tiere; Trend fallend (Meereis-Verlust).",
     "habitat": "Antarktis"},
    {"de": "Bartgeier", "en": "Bearded Vulture",
     "sci": "Gypaetus barbatus", "cat": "NT",
     "pop": "Global NT; in Europa Alpen-Wiederansiedlung erfolgreich.",
     "habitat": "Gebirge Eurasiens/Afrikas"},
    # Reptilien / Amphibien
    {"de": "Lederschildkröte", "en": "Leatherback Turtle",
     "sci": "Dermochelys coriacea", "cat": "VU",
     "pop": "Subpopulationen variieren von LC bis CR.",
     "habitat": "Weltweite Ozeane"},
    {"de": "Komodowaran", "en": "Komodo Dragon",
     "sci": "Varanus komodoensis", "cat": "EN",
     "pop": "~1.380 ausgewachsene Tiere (2021). 2021 von VU zu EN heraufgestuft (Klima).",
     "habitat": "Indonesische Inseln"},
    # Insekten / Wirbellose
    {"de": "Westliche Honigbiene", "en": "Western Honey Bee",
     "sci": "Apis mellifera", "cat": "DD",
     "pop": "DD seit 2014. Wild-Subpopulationen bedroht; managed-Bienen domestiziert.",
     "habitat": "Ursprünglich Europa/Afrika, weltweit verbreitet"},
    # Pflanzen (Bonus, IUCN bewertet auch Flora)
    {"de": "Mammutbaum (Küstenmammutbaum)", "en": "Coast Redwood",
     "sci": "Sequoia sempervirens", "cat": "EN",
     "pop": "EN seit 2012; ~5 % der Ursprungsbestände erhalten.",
     "habitat": "Kalifornien/Oregon"},
]

_DE_NAME_INDEX = {sp["de"].lower(): sp for sp in _FALLBACK_SPECIES}
_EN_NAME_INDEX = {sp["en"].lower(): sp for sp in _FALLBACK_SPECIES}
_SCI_NAME_INDEX = {sp["sci"].lower(): sp for sp in _FALLBACK_SPECIES}

# Spezies-Aliase: häufige Schreibvarianten/Singular-Plural-Formen
_SPECIES_ALIASES: dict[str, str] = {
    # de-Aliase → kanonischer Eintrag (de)
    "tiger": "Tiger",
    "tigern": "Tiger",
    "tigers": "Tiger",
    "schneeleoparden": "Schneeleopard",
    "löwen": "Löwe",
    "loewe": "Löwe",
    "loewen": "Löwe",
    "geparden": "Gepard",
    "eisbären": "Eisbär",
    "eisbaer": "Eisbär",
    "eisbaeren": "Eisbär",
    "panda": "Großer Panda",
    "pandas": "Großer Panda",
    "grosser panda": "Großer Panda",
    "berggorillas": "Berggorilla",
    "gorilla": "Berggorilla",
    "gorillas": "Berggorilla",
    "orang-utan": "Orang-Utan (Borneo)",
    "orang-utans": "Orang-Utan (Borneo)",
    "orangutan": "Orang-Utan (Borneo)",
    "orangutans": "Orang-Utan (Borneo)",
    "sumatra-orang-utan": "Orang-Utan (Sumatra)",
    "sumatra-nashörner": "Sumatra-Nashorn",
    "java-nashörner": "Java-Nashorn",
    "spitzmaulnashörner": "Spitzmaulnashorn",
    "schwarzes nashorn": "Spitzmaulnashorn",
    "waldelefant": "Afrikanischer Waldelefant",
    "waldelefanten": "Afrikanischer Waldelefant",
    "steppenelefant": "Afrikanischer Steppenelefant",
    "steppenelefanten": "Afrikanischer Steppenelefant",
    "elefant": "Afrikanischer Steppenelefant",  # default to steppe
    "elefanten": "Afrikanischer Steppenelefant",
    "wölfe": "Wolf (Eurasien)",
    "wolf": "Wolf (Eurasien)",
    "wolfes": "Wolf (Eurasien)",
    "braunbären": "Braunbär",
    "braunbaer": "Braunbär",
    "braunbaeren": "Braunbär",
    "luchs": "Luchs (Eurasisch)",
    "luchse": "Luchs (Eurasisch)",
    "luchsen": "Luchs (Eurasisch)",
    "iberischer luchs": "Iberischer Luchs",
    "pardelluchs": "Iberischer Luchs",
    "blauwale": "Blauwal",
    "vaquitas": "Vaquita",
    "kalifornischer schweinswal": "Vaquita",
    "thunfisch": "Atlantischer Thunfisch",
    "weisser hai": "Weißer Hai",
    "weisshai": "Weißer Hai",
    "mantarochen": "Mantarochen (Riesenmanta)",
    "manta": "Mantarochen (Riesenmanta)",
    "kondor": "Kalifornischer Kondor",
    "kondore": "Kalifornischer Kondor",
    "kaiserpinguine": "Kaiserpinguin",
    "pinguin": "Kaiserpinguin",
    "bartgeiern": "Bartgeier",
    "lederschildkröten": "Lederschildkröte",
    "lederschildkroete": "Lederschildkröte",
    "komodowaranen": "Komodowaran",
    "komodo-drache": "Komodowaran",
    "honigbiene": "Westliche Honigbiene",
    "honigbienen": "Westliche Honigbiene",
    "biene": "Westliche Honigbiene",
    "bienen": "Westliche Honigbiene",
    "mammutbaum": "Mammutbaum (Küstenmammutbaum)",
    "mammutbäume": "Mammutbaum (Küstenmammutbaum)",
    "redwood": "Mammutbaum (Küstenmammutbaum)",
}

# ---------------------------------------------------------------------------
# Trigger-Terms
# ---------------------------------------------------------------------------
_IUCN_PRIMARY = (
    "iucn", "red list", "rote liste der iucn", "rote liste iucn",
    "iucn-status", "iucn status",
)

_THREAT_TERMS = (
    "gefährdet", "bedroht", "vom aussterben", "ausgestorben",
    "aussterben", "auszusterben", "stark gefährdet",
    "verschwindet", "verschwinden", "stirbt aus", "sterben aus",
    "endangered", "extinct", "critically endangered", "vulnerable",
    "near threatened", "least concern",
    "schutzstatus", "gefährdungsstatus", "artenschutz",
    "populationsrückgang", "bestand bedroht",
)


def _claim_mentions_iucn(claim_lc: str) -> bool:
    """Trigger-Check für IUCN-Spezies-Status-Claims.

    Strategie:
      1. Explizite IUCN/Red-List-Nennung → True.
      2. Bedrohungs-/Aussterben-Begriff + bekannter Spezies-Name → True.
      3. "Population [Spezies]" → True.
    """
    if not claim_lc:
        return False

    # Explizite IUCN-Nennung
    if any(t in claim_lc for t in _IUCN_PRIMARY):
        return True

    # Spezies-Name (de/en/sci) im Claim?
    has_species = False
    for name in _DE_NAME_INDEX:
        if name in claim_lc:
            has_species = True
            break
    if not has_species:
        for alias in _SPECIES_ALIASES:
            if alias in claim_lc:
                has_species = True
                break
    if not has_species:
        for name in _EN_NAME_INDEX:
            if name in claim_lc:
                has_species = True
                break
    if not has_species:
        for sci in _SCI_NAME_INDEX:
            if sci in claim_lc:
                has_species = True
                break

    if not has_species:
        return False

    # Bedrohungs-/Aussterbe-Vokabular
    if any(t in claim_lc for t in _THREAT_TERMS):
        return True

    # "Population [Spezies]" / "Bestand [Spezies]"
    if "population" in claim_lc or "bestand" in claim_lc:
        return True

    return False


@lru_cache(maxsize=512)
def claim_mentions_iucn_cached(claim: str) -> bool:
    """LRU-gecachter Wrapper für Trigger-Check."""
    return _claim_mentions_iucn((claim or "").lower())


# ---------------------------------------------------------------------------
# Resolver: Claim → Spezies-Liste
# ---------------------------------------------------------------------------
def _resolve_species_from_claim(claim_lc: str) -> list[dict]:
    """Identifiziere ein/mehrere Fallback-Spezies aus dem Claim-Text.

    Reihenfolge:
      1. Wissenschaftliche Namen (eindeutigster Match)
      2. DE-Namen
      3. EN-Namen
      4. Aliase
    Returns Liste eindeutiger Spezies-Dicts (de-dupliziert), max MAX_RESULTS.
    """
    found: list[dict] = []
    seen_sci: set[str] = set()

    def _push(sp: dict) -> None:
        sci = sp.get("sci", "")
        if sci and sci not in seen_sci:
            seen_sci.add(sci)
            found.append(sp)

    # 1. Wissenschaftliche Namen
    for sci, sp in _SCI_NAME_INDEX.items():
        if sci in claim_lc:
            _push(sp)

    # 2. DE-Namen — längere zuerst, um "Wolf" vor "Braunbär" zu vermeiden
    for name in sorted(_DE_NAME_INDEX.keys(), key=len, reverse=True):
        if name in claim_lc:
            _push(_DE_NAME_INDEX[name])

    # 3. EN-Namen
    for name in sorted(_EN_NAME_INDEX.keys(), key=len, reverse=True):
        if name in claim_lc:
            _push(_EN_NAME_INDEX[name])

    # 4. Aliase
    for alias in sorted(_SPECIES_ALIASES.keys(), key=len, reverse=True):
        if alias in claim_lc:
            canonical = _SPECIES_ALIASES[alias]
            sp = _DE_NAME_INDEX.get(canonical.lower())
            if sp:
                _push(sp)

    return found[:MAX_RESULTS]


# ---------------------------------------------------------------------------
# Live-API (best effort — Cloudflare-Bot-Layer macht das in Praxis off)
# ---------------------------------------------------------------------------
def _get_token() -> str | None:
    token = os.getenv("IUCN_API_KEY") or os.getenv("IUCN_TOKEN")
    return token or None


async def _fetch_iucn_species(scientific_name: str) -> dict | None:
    """Live-Lookup gegen IUCN-API. Returns dict mit category/main_common_name/
    population_trend oder None bei Fehler/403/kein Token.
    """
    token = _get_token()
    if not token:
        logger.debug("iucn: kein API-Token gesetzt — skip Live-Call")
        return None

    key = ("species", scientific_name.lower())
    now = time.time()
    cached = _cache.get(key)
    if cached and (now - cached[0]) < CACHE_TTL:
        records = cached[1]
        return records[0] if records else None

    url = f"{IUCN_BASE}/species/{quote(scientific_name)}?token={token}"
    try:
        async with polite_client(timeout=TIMEOUT_S) as client:
            resp = await client.get(url, follow_redirects=True)
            if resp.status_code == 403:
                logger.info(
                    "iucn: HTTP 403 (Cloudflare-Block) — Fallback aktiv"
                )
                _cache[key] = (now, [])
                return None
            if resp.status_code != 200:
                logger.info(
                    f"iucn: HTTP {resp.status_code} für {scientific_name}"
                )
                _cache[key] = (now, [])
                return None
            payload = resp.json()
    except Exception as e:
        logger.info(f"iucn: fetch failed für {scientific_name}: {e}")
        return None

    result = (payload or {}).get("result") if isinstance(payload, dict) else None
    if not isinstance(result, list) or not result:
        _cache[key] = (now, [])
        return None

    record = result[0] if isinstance(result[0], dict) else None
    _cache[key] = (now, [record] if record else [])
    return record


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _build_result_from_fallback(sp: dict) -> dict:
    """Konvertiere ein Fallback-Spezies-Dict in das Evidora-Result-Schema."""
    cat = sp.get("cat", "DD")
    cat_label = _CATEGORY_LABEL_DE.get(cat, cat)
    de_name = sp.get("de", "—")
    en_name = sp.get("en", "")
    sci = sp.get("sci", "")
    pop = sp.get("pop", "")
    habitat = sp.get("habitat", "")

    is_threat = cat in _THREAT_CATEGORIES

    display_value = (
        f"{de_name} ({sci}) — IUCN-Status: {cat} = {cat_label}. "
        f"{pop}".strip()
    )

    description_parts = [
        f"Englischer Name: {en_name}." if en_name else "",
        f"Verbreitung: {habitat}." if habitat else "",
        (
            "Quelle: IUCN Red List Version 2025-2. IUCN-Bewertungen sind "
            "die internationale Referenz für Gefährdungsstatus von Spezies "
            "(zitiert von CITES, EU-Habitats-Direktive und nationalen "
            "Naturschutz-Behörden)."
        ),
    ]
    if not is_threat and cat == "LC":
        description_parts.append(
            "Hinweis: 'Least Concern' bedeutet global nicht gefährdet — "
            "regionale/nationale Subpopulationen können dennoch streng "
            "geschützt sein (z.B. Wolf, Braunbär, Luchs in Mitteleuropa)."
        )

    portal_slug = sci.replace(" ", "+") if sci else quote(en_name)
    url = f"{IUCN_PORTAL}/{portal_slug}" if portal_slug else IUCN_PORTAL

    indicator_name = (
        f"IUCN Red List · {de_name} ({sci}) · Status {cat}"
    )[:300]

    return {
        "indicator_name": indicator_name,
        "indicator": f"iucn_{sci.lower().replace(' ', '_')}",
        "country": "GLOBAL",
        "country_name": "Welt",
        "year": "2025",
        "value": cat,
        "display_value": display_value,
        "description": " ".join(p for p in description_parts if p),
        "url": url,
        "source": "IUCN Red List",
    }


def _build_result_from_live(record: dict, fallback_sp: dict | None) -> dict:
    """Konvertiere einen Live-API-Record ins Evidora-Schema.

    Falls fallback_sp gesetzt, wird der DE-Name + habitat von dort genommen.
    """
    sci = record.get("scientific_name") or (fallback_sp or {}).get("sci", "")
    cat = record.get("category") or "DD"
    cat_label = _CATEGORY_LABEL_DE.get(cat, cat)
    pop_trend = record.get("population_trend") or ""
    main_common = record.get("main_common_name") or ""
    de_name = (fallback_sp or {}).get("de", main_common or sci)
    habitat = (fallback_sp or {}).get("habitat", "")

    display_value = (
        f"{de_name} ({sci}) — IUCN-Status: {cat} = {cat_label}."
    )
    if pop_trend:
        display_value += f" Populations-Trend: {pop_trend}."

    description_parts = [
        f"Englischer Name: {main_common}." if main_common else "",
        f"Verbreitung: {habitat}." if habitat else "",
        (
            "Quelle: IUCN Red List Live-API (apiv3.iucnredlist.org). "
            "IUCN-Bewertungen sind die internationale Referenz für "
            "Gefährdungsstatus von Spezies."
        ),
    ]

    portal_slug = sci.replace(" ", "+") if sci else ""
    url = f"{IUCN_PORTAL}/{portal_slug}" if portal_slug else IUCN_PORTAL

    indicator_name = (
        f"IUCN Red List · {de_name} ({sci}) · Status {cat}"
    )[:300]

    return {
        "indicator_name": indicator_name,
        "indicator": f"iucn_{sci.lower().replace(' ', '_')}",
        "country": "GLOBAL",
        "country_name": "Welt",
        "year": "2025",
        "value": cat,
        "display_value": display_value,
        "description": " ".join(p for p in description_parts if p),
        "url": url,
        "source": "IUCN Red List",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_iucn(analysis: dict) -> dict:
    """Lookup gegen IUCN Red List für Spezies-Gefährdungsstatus.

    Strategie:
      1. Trigger-Match → frühes Empty-Return.
      2. Spezies aus Claim auflösen (Fallback-Index).
      3. Pro Spezies: Live-API versuchen (nur wenn Token gesetzt + nicht
         Cloudflare-blockiert); sonst Fallback-Record.
      4. Top MAX_RESULTS Treffer ins Evidora-Format.

    Returns Dict mit ≤MAX_RESULTS Treffern oder Empty.
    """
    empty = {
        "source": "IUCN Red List",
        "type": "species_status",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_iucn(matchable):
        return empty

    species_list = _resolve_species_from_claim(matchable)
    if not species_list:
        logger.info("iucn: Trigger matched, aber keine Spezies aufgelöst")
        return empty

    results: list[dict] = []
    token_present = _get_token() is not None

    for sp in species_list:
        sci = sp.get("sci", "")
        record = None
        if token_present and sci:
            try:
                record = await _fetch_iucn_species(sci)
            except Exception as e:
                logger.debug(f"iucn: Live-Call exception für {sci}: {e}")
                record = None

        if record:
            results.append(_build_result_from_live(record, sp))
        else:
            results.append(_build_result_from_fallback(sp))

        if len(results) >= MAX_RESULTS:
            break

    if not results:
        return empty

    logger.info(
        f"iucn: {len(results)} Spezies aufgelöst "
        f"(live={'yes' if token_present else 'no'})"
    )
    return {
        "source": "IUCN Red List",
        "type": "species_status",
        "results": results,
    }
