"""EDGAR JRC — Emissions Database for Global Atmospheric Research.

Die offizielle Treibhausgas-/Luftschadstoff-Emissionsdatenbank der
Europäischen Kommission (Joint Research Centre, Ispra). EDGAR liefert
**länder- und sektor-aufgelöste** anthropogene Emissionen für CO2, CH4,
N2O, F-Gase + Luftschadstoffe — global vergleichbar, jährlich aktualisiert.

Komplementaer zu:
- ``services/uba_klima.py`` — AT-Inventur (KSG-Sektoren, UBA-Methodik,
  nur Österreich, höchste Auflösung).
- ``services/eea.py`` — EU-27-Trends (Effort-Sharing, ETS-Trennung).
- ``services/owid.py`` — kuratierte historische Vergleichsserien.
- EDGAR hier: **globale Sektor-Aufschlüsselung** + transparente
  Methodik für DE / EU-27 / China / USA / Indien-Whataboutism-Claims.
  EDGAR ist die einzige Quelle, die für *alle* Länder konsistente
  Sektor-Vergleiche liefert (UBA macht das nur für AT, Destatis nur DE).

Hybrid-Implementierung (Pack-mit-Live-Augmentation, vgl. cams.py):
  1. **15+ kuratierte Eckwerte** aus EDGAR GHG-2024-Release v9.0
     (Stichjahr 2023, publiziert Dez 2024) — AT/DE/EU-27/CN/US/IN
     Gesamt-CO2 + Sektor-Splits (Energie/Industrie/Verkehr/Gebäude/
     Landwirtschaft) + Methan-Eckwerte.
  2. **Optionaler Live-HEAD-Check** der EDGAR-Dataset-Landing-Page
     (``dataset_ghg2024``) — bestätigt, dass die Bulk-CSV-Quelle noch
     erreichbar ist, ohne den 1+ GB-Download zu starten.
  3. **Graceful-fail**: HEAD fehlerhaft → reine Pack-Antwort (Eckwerte
     bleiben gültig; HEAD ist nur Aktualitäts-Indikator).

Warum kein Live-CSV-Parse?
--------------------------
EDGAR publiziert die Bulk-Daten als **gezipptes Excel + große CSV-Pakete**
auf ``https://edgar.jrc.ec.europa.eu/dataset_ghg2024`` — kein JSON-API,
kein OData/SDMX. Ein vollständiger Live-Parse wäre ~1 GB pro Refresh
(unhöflich gegenüber JRC + unnötig für 15 Eckwerte). Die JRC-Releases
sind **jährlich** (Dezember-Release v9.0 ⇒ v10.0 Dez 2025 erwartet) —
ein kurratives Pack mit Jahres-Refresh ist die methodisch saubere
Antwort. Bei Quellen-Drift (URL-Change) protokollieren wir das via
HEAD-Check.

Lizenz: **EC public sector information** — frei nutzbar nach
Commission Decision 2011/833/EU (Re-use of Commission documents).
Attribution: "European Commission, Joint Research Centre (JRC) — EDGAR
v9.0 (2024)".

Trigger:
- "EDGAR", "JRC Emissionen", "Joint Research Centre"
- "CO2 nach Sektor [Land]" + "Sektor Treibhausgas [Land]"
- "AT-Treibhausgas-Bilanz EDGAR" / "Österreich CO2 EDGAR"
- "EU-CO2-Vergleich" / "EU-27 Emissionen"
- "China vs Deutschland CO2", "Indien USA CO2 Vergleich"
- "Methan-Emissionen [Land]" / "CH4 [Land]"

Konsens-Schutz:
- Bei Klima-Skepsis-Claims ("CO2 ist ungefährlich" / "menschengemacht
  unbelegt") wird NICHT ausgespielt — diese Fragen löst der IPCC-/EEA-/
  Skeptical-Science-Stack. EDGAR ist ein reiner Mengen-Lieferant.
- KEINE Partei-Wertung. KEINE Klimaschutz-Politik-Bewertung. Reine
  Sektor- und Länder-Mengen mit transparenter Methodik-Quelle.

Cache: 24h (Eckwerte sind jahresgranular; HEAD-Check leichtgewichtig).

------------------------------------------------------------------------
WIRING-SNIPPET (für main.py / data_updater.py / reranker.py — NICHT
automatisch eingebaut, manuell applizieren!):
------------------------------------------------------------------------
    # main.py — Imports:
    from services.edgar import (
        search_edgar,
        claim_mentions_edgar_cached,
    )

    # main.py — Task-Wiring (im pipeline-Block neben search_uba_klima):
        if claim_mentions_edgar_cached(claim):
            tasks.append(cached("EDGAR JRC", search_edgar, analysis))

    # reranker.py — _AUTHORITATIVE_INDICATORS-Tuple ergänzen:
        "edgar_anchor",
        "edgar_methodology",

    # data_updater.py — Prefetch (optional, NICHT erforderlich für v1):
    # EDGAR-Pack ist statisch + HEAD-Check ist on-demand schnell genug.
    # Kein Prefetch nötig.
------------------------------------------------------------------------
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Endpoint / Source
# ---------------------------------------------------------------------------
# EDGAR JRC Bulk-Dataset Landing-Page (HTML, kein JSON-API).
# HEAD-Check verifiziert nur, dass die Quelle noch existiert.
EDGAR_DATASET_URL = "https://edgar.jrc.ec.europa.eu/dataset_ghg2024"
EDGAR_REPORT_URL = "https://edgar.jrc.ec.europa.eu/report_2024"

EDGAR_CACHE_TTL = 86400  # 24 h
DEFAULT_TIMEOUT = 15.0

# Cache: head_ok (bool) + ts (epoch)
_head_cache: dict[str, Any] = {"ok": None, "ts": 0.0}


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_EDGAR_PRIMARY = (
    "edgar",
    "jrc emissionen", "jrc-emissionen",
    "joint research centre",
    "joint research center",
    "edgar v9", "edgar-v9", "edgar 2024", "ghg2024",
    "dataset_ghg2024",
    "emissions database for global atmospheric research",
)

# Sektor-Trigger (DE+EN). Werden mit Länder- oder Emissions-Marker
# kombiniert (composite).
_SECTOR_TERMS = (
    "sektor",
    "power industry", "energiesektor", "kraftwerk",
    "industrie", "industrial combustion",
    "transport", "verkehr", "road transport",
    "buildings", "gebäude", "haushalte", "heizung",
    "landwirtschaft", "agriculture",
    "abfall", "waste",
)

# Emissions-/THG-Marker
_THG_TERMS = (
    "co2", "co₂", "kohlendioxid", "carbon dioxide",
    "treibhausgas", "treibhausgase", "thg", "ghg",
    "klimagas", "klimagase",
    "methan", "methane", "ch4",
    "lachgas", "nitrous oxide", "n2o",
    "f-gas", "f-gase", "fluorierte",
    "emission", "emissionen", "emissions",
)

# Länder-Trigger (klein, aber breit genug für die Hauptvergleiche).
_COUNTRY_TERMS = (
    "österreich", "austria",
    "deutschland", "germany",
    "eu-27", "eu 27", "europäische union", "european union",
    "china", "chinesisch", "china-kohle",
    "usa", "u.s.a.", "vereinigte staaten", "united states",
    "indien", "india", "indisch",
    "russland", "russia",
    "frankreich", "france",
    "polen", "poland",
)

# Vergleichs-Marker für Cross-Country-Claims
_COMPARE_TERMS = (
    "vergleich", "vs", "vs.", "versus",
    "größter emittent", "groesster emittent", "top emittent",
    "pro kopf", "per capita",
)


_PRIMARY_RE = re.compile(
    r"(?i)(" + "|".join(re.escape(k) for k in _EDGAR_PRIMARY) + r")"
)


def _claim_mentions_edgar(claim_lc: str) -> bool:
    """Trigger-Logik. True wenn EDGAR-Sektor-/Länder-Vergleich plausibel.

    Erwartet lowercased Claim (Konvention wie cams.py / oenb.py).
    """
    if not claim_lc:
        return False

    # 1. Direkte EDGAR/JRC-Erwähnung
    if _PRIMARY_RE.search(claim_lc):
        return True

    # 2. Composite: Sektor + THG + Länder-Kontext (klassischer
    #    "CO2 nach Sektor [Land]"-Claim)
    has_sector = any(t in claim_lc for t in _SECTOR_TERMS)
    has_thg = any(t in claim_lc for t in _THG_TERMS)
    has_country = any(t in claim_lc for t in _COUNTRY_TERMS)
    if has_sector and has_thg and has_country:
        return True

    # 3. Composite: Cross-Country-Vergleich + THG-Marker
    #    ("China vs Deutschland CO2-Emissionen")
    has_compare = any(t in claim_lc for t in _COMPARE_TERMS)
    if has_compare and has_thg and has_country:
        return True

    # 4. EU-27 + Emissions-Marker (explizite EU-27-Aggregat-Claims)
    if ("eu-27" in claim_lc or "eu 27" in claim_lc) and has_thg:
        return True

    # 5. Composite: spezifisches Gas (Methan/CH4/Lachgas/F-Gas) + Land
    #    — diese Gase sind in EDGAR sektor-aufgelöst, im Gegensatz zu
    #    generischem "CO2/Emission" (das in vielen Quellen verbreitet ist
    #    und sonst zu false-positives führt).
    has_specific_gas = any(t in claim_lc for t in (
        "methan", "methane", "ch4",
        "lachgas", "nitrous oxide", "n2o",
        "f-gas", "f-gase", "fluorierte",
    ))
    if has_specific_gas and has_country:
        return True

    return False


def claim_mentions_edgar_cached(claim: str) -> bool:
    """Public-Trigger für main.py — nimmt rohen Claim, lowercased intern."""
    return _claim_mentions_edgar((claim or "").lower())


# ---------------------------------------------------------------------------
# Kuratierte EDGAR-Eckwerte (v9.0, Release Dez 2024, Stichjahr 2023)
# ---------------------------------------------------------------------------
# Quelle aller Werte: EDGAR v9.0 (2024) — Crippa et al., JRC PUBSY.
# Werte sind gerundet auf 0.1 Mt; CO2 ohne LULUCF; Methan in Mt CO2eq
# (GWP100 AR5). Stand 2024-12-Release.
#
# Für jeden Anker: 'topic' (Lookup-Key), 'keywords' (Trigger-Tokens),
# 'headline' (1-Zeilen-Fakt) und 'description' (Methodik + Kontext).
EDGAR_ANCHORS: list[dict[str, Any]] = [
    # ------------ Gesamt-Emissionen Länder ------------
    {
        "topic": "at_total_co2",
        "keywords": ("österreich", "austria", "at-co2", "at co2"),
        "headline": (
            "AT-CO2-Emissionen (EDGAR v9.0, Stichjahr 2023): "
            "~65 Mt CO2 fossil + Industrie-Prozesse "
            "(ohne LULUCF). Pro-Kopf-Wert ~7,1 t CO2."
        ),
        "description": (
            "EDGAR-Sektor-Split AT 2023 (gerundet, Mt CO2): "
            "Power-Industry ~9; Industrial Combustion ~10; "
            "Buildings ~12; Road Transport ~22; Other Sectors ~12. "
            "Methodik: EDGAR aggregiert IEA-Aktivitätsdaten + IPCC-2006-"
            "Tier-1/2-Emissionsfaktoren. Vergleichsbasis: UBA-OLI 2025 "
            "meldet 66,6 Mt CO2eq für 2023 — EDGAR/UBA stimmen innerhalb "
            "~2 % überein (Methodik-Unterschied: EDGAR-Stichjahr ist "
            "n-2 wegen IEA-Lag, UBA-Inventur ist n-1)."
        ),
    },
    {
        "topic": "de_total_co2",
        "keywords": ("deutschland", "germany", "de-co2"),
        "headline": (
            "DE-CO2-Emissionen (EDGAR v9.0, 2023): "
            "~675 Mt CO2 fossil + Industrie (ohne LULUCF). "
            "Pro-Kopf ~8,0 t. Größter EU-Emittent absolut."
        ),
        "description": (
            "EDGAR-Sektor-Split DE 2023 (gerundet, Mt CO2): "
            "Power-Industry ~213; Industrial Combustion ~108; "
            "Buildings ~108; Road Transport ~148; Other Sectors ~98. "
            "Rückgang seit 1990 (~1.054 Mt) v. a. durch Kraftwerks-Abbau + "
            "Industrie-Effizienz. Methodik: EDGAR JRC v9.0."
        ),
    },
    {
        "topic": "eu27_total_co2",
        "keywords": ("eu-27", "eu 27", "europäische union", "european union"),
        "headline": (
            "EU-27-CO2 (EDGAR v9.0, 2023): ~2.420 Mt CO2 fossil + "
            "Prozesse (ohne LULUCF). Anteil global ~6,7 %."
        ),
        "description": (
            "EU-27-Aggregat aus EDGAR v9.0: Rückgang seit 1990 "
            "(~3.770 Mt) um ~36 % bei +1,8 % BIP-CAGR — "
            "stärkste Treiber: Kohle-Phaseout, EE-Ausbau, "
            "Industrie-Verlagerung. Vergleich global: China ~12.700 Mt, "
            "USA ~4.700 Mt, Indien ~3.200 Mt (EDGAR 2023). "
            "EU-27-Pro-Kopf ~5,4 t CO2; deutlich unter USA (~14) "
            "und nah an China (~9)."
        ),
    },
    {
        "topic": "cn_total_co2",
        "keywords": ("china", "chinesisch", "china-kohle"),
        "headline": (
            "China-CO2 (EDGAR v9.0, 2023): ~12.700 Mt CO2 fossil + "
            "Prozesse. Größter globaler Emittent absolut (~35 %)."
        ),
        "description": (
            "Sektor-Split CN 2023: Power-Industry ~5.700; Industrial "
            "Combustion ~3.300; Road Transport ~870. Pro-Kopf ~9,0 t — "
            "höher als EU-27 (~5,4), niedriger als USA (~14). "
            "Wachstum seit 2000 (~3.500 Mt) ~+260 %. Kontext für "
            "'China-Whataboutism'-Claims: EDGAR-Daten erlauben "
            "Pro-Kopf-, Sektor- und kumulativen Vergleich (1990–2023)."
        ),
    },
    {
        "topic": "us_total_co2",
        "keywords": ("usa", "u.s.a.", "vereinigte staaten", "united states"),
        "headline": (
            "USA-CO2 (EDGAR v9.0, 2023): ~4.700 Mt CO2 fossil + "
            "Prozesse. Zweitgrößter Emittent global (~13 %)."
        ),
        "description": (
            "Sektor-Split US 2023: Power-Industry ~1.500; Road Transport "
            "~1.520; Industrial Combustion ~700; Buildings ~520. "
            "Pro-Kopf-Wert ~14 t CO2 — höchster Wert großer "
            "Industrienationen. Rückgang seit 2005 (~6.000 Mt) v. a. "
            "durch Kohle→Gas-Switch + Effizienz im Verkehrssektor."
        ),
    },
    {
        "topic": "in_total_co2",
        "keywords": ("indien", "india", "indisch"),
        "headline": (
            "Indien-CO2 (EDGAR v9.0, 2023): ~3.200 Mt CO2. Drittgrößter "
            "Emittent global (~9 %), Pro-Kopf nur ~2,2 t."
        ),
        "description": (
            "Sektor-Split IN 2023: Power-Industry ~1.450; Industrial "
            "Combustion ~620; Road Transport ~330. Pro-Kopf-Wert "
            "~2,2 t — deutlich unter EU-27 (~5,4) und Welt-Schnitt "
            "(~4,7). Wachstum seit 2000 (~1.000 Mt) ~+220 %, "
            "primär getrieben durch Kohle-Stromerzeugung + Industrialisierung."
        ),
    },
    # ------------ Sektor-Anker (Cross-Country) ------------
    {
        "topic": "sector_power",
        "keywords": (
            "power industry", "energiesektor", "kraftwerk",
            "stromerzeugung",
        ),
        "headline": (
            "EDGAR Sektor 'Power Industry' 2023: global ~14.500 Mt CO2 "
            "(~40 % aller fossilen CO2-Emissionen)."
        ),
        "description": (
            "Stromerzeugung ist der größte globale CO2-Sektor in EDGAR. "
            "Länder-Anteile (Mt CO2): CN ~5.700, USA ~1.500, IN ~1.450, "
            "EU-27 ~700, DE ~213, AT ~9. AT-Stromsektor extrem niedrig "
            "wegen Wasserkraft-Anteil (~60 %)."
        ),
    },
    {
        "topic": "sector_transport",
        "keywords": (
            "road transport", "verkehr", "transport",
            "verkehrssektor", "kfz",
        ),
        "headline": (
            "EDGAR Sektor 'Road Transport' 2023: global ~6.000 Mt CO2 "
            "(~17 % der fossilen CO2-Emissionen)."
        ),
        "description": (
            "Straßenverkehr (Pkw + Lkw) — Länder-Anteile 2023 (Mt CO2): "
            "USA ~1.520, CN ~870, EU-27 ~750, DE ~148, AT ~22. "
            "AT-Verkehrssektor stagniert seit ~1990 (+50 %), Hauptursache: "
            "Lkw-Wachstum + Tank-Tourismus (UBA-Studien 2024). EDGAR "
            "deckt Bunker-Emissionen NICHT auf nationaler Ebene ab — "
            "internationale Luft-/Schifffahrt sind separat ausgewiesen."
        ),
    },
    {
        "topic": "sector_industry",
        "keywords": (
            "industrial combustion", "industrie", "industriesektor",
            "stahl", "zement", "chemie",
        ),
        "headline": (
            "EDGAR Sektor 'Industrial Combustion' + Prozesse 2023: "
            "global ~9.500 Mt CO2 (~26 % fossil)."
        ),
        "description": (
            "Industrie-Sektor (Verbrennung + Prozess-Emissionen, ohne "
            "Strom): Länder 2023 (Mt CO2): CN ~3.300, USA ~700, "
            "IN ~620, EU-27 ~600, DE ~108, AT ~10. Stahl + Zement + "
            "Chemie sind die emissions-intensivsten Subsektoren. "
            "EDGAR trennt 'Combustion' (Brennstoff-CO2) von 'Process' "
            "(Kalzinierung, Reduktion etc.) — beide hier addiert."
        ),
    },
    {
        "topic": "sector_buildings",
        "keywords": (
            "buildings", "gebäude", "haushalte", "heizung", "raumwärme",
        ),
        "headline": (
            "EDGAR Sektor 'Buildings' (Haushalte + Dienstleistung) 2023: "
            "global ~3.000 Mt CO2 (~8 % fossil)."
        ),
        "description": (
            "Gebäude-Sektor (direkte Verbrennung: Heizöl, Gas, "
            "Fernwärme-direkt): Länder 2023 (Mt CO2): "
            "CN ~600, USA ~520, EU-27 ~500, DE ~108, AT ~12. "
            "AT-Gebäudesektor mit hohem Erdgas-Anteil — Wärmewende "
            "(Wärmepumpe + Fernwärme-Dekarbonisierung) ist KSG-Schlüssel."
        ),
    },
    {
        "topic": "sector_agriculture",
        "keywords": ("landwirtschaft", "agriculture", "agrarsektor"),
        "headline": (
            "EDGAR Landwirtschaft 2023: global ~5.700 Mt CO2eq "
            "(v. a. CH4 + N2O; nur ~10 % direkter CO2-Anteil)."
        ),
        "description": (
            "Agrar-Sektor ist NICHT CO2-, sondern CH4-/N2O-dominiert: "
            "Viehhaltung (Methan), Düngung (Lachgas), Reisanbau. "
            "Länder 2023 (Mt CO2eq, GWP100 AR5): "
            "CN ~700, IN ~640, EU-27 ~430, USA ~620, AT ~8. "
            "EDGAR-Methodik: Aktivitätsdaten aus FAOSTAT + "
            "IPCC-2006-Emissionsfaktoren. Für AT-Detail siehe UBA-OLI."
        ),
    },
    # ------------ Methan-Eckwerte ------------
    {
        "topic": "global_methane",
        "keywords": ("methan", "methane", "ch4"),
        "headline": (
            "Globale Methan-Emissionen (EDGAR v9.0, 2023): "
            "~370 Mt CH4 (~9.250 Mt CO2eq, GWP100 AR5 = 25)."
        ),
        "description": (
            "Methan-Quellen global 2023 (anthropogen, Mt CH4): "
            "Viehhaltung ~110; Reisanbau ~30; Mülldeponien ~70; "
            "Kohlebergbau ~45; Öl/Gas-Förderung ~80. EDGAR deckt "
            "ANTHROPOGENE Methan-Emissionen ab — natürliche Quellen "
            "(Feuchtgebiete, Permafrost) sind separat in den "
            "GLOBAL-CARBON-PROJECT-Inversionen erfasst. Methan trägt "
            "~17 % zur globalen Erwärmung bei (IPCC AR6)."
        ),
    },
    {
        "topic": "at_methane",
        "keywords": ("methan österreich", "ch4 österreich", "methan austria"),
        "headline": (
            "AT-Methan-Emissionen (EDGAR v9.0, 2023): "
            "~0,30 Mt CH4 (~7,5 Mt CO2eq)."
        ),
        "description": (
            "AT-CH4 dominiert von Viehhaltung (~60 %) + Mülldeponien "
            "(~20 %) + Erdgas-Leitungen (~10 %). Rückgang seit 1990 "
            "(~0,45 Mt) v. a. durch Deponie-Gas-Erfassung (Pflicht "
            "seit AT-Deponieverordnung 2008)."
        ),
    },
    # ------------ Sektor-Anker AT-Spezifisch ------------
    {
        "topic": "at_sector_split",
        "keywords": (
            "at-sektor", "österreich sektor", "at sektor", "österreich co2",
            "co2 nach sektor österreich",
        ),
        "headline": (
            "AT-Sektor-Split 2023 (EDGAR v9.0, Mt CO2): Verkehr ~22 (35 %), "
            "Gebäude ~12 (19 %), Industrie ~10 (15 %), Power ~9 (14 %), "
            "Rest ~12 (18 %)."
        ),
        "description": (
            "AT ist mit Verkehr (~35 %) der einzige große Sektor — "
            "wegen sehr niedrigem Power-Anteil (Wasserkraft-Dominanz). "
            "Vergleich DE: dort macht Power-Industry ~32 % aus. "
            "EDGAR-Werte stimmen mit UBA-OLI 2025 (siehe "
            "services/uba_klima.py) innerhalb der Methodik-Unsicherheit "
            "(~±2 %) überein."
        ),
    },
    # ------------ Pro-Kopf-Vergleich (gegen Whataboutism) ------------
    {
        "topic": "per_capita_compare",
        "keywords": (
            "pro kopf", "per capita", "pro-kopf-emission",
            "pro kopf emission",
        ),
        "headline": (
            "Pro-Kopf-CO2 2023 (EDGAR v9.0, t CO2/Person): USA ~14, "
            "DE ~8,0, AT ~7,1, CN ~9,0, EU-27 ~5,4, IN ~2,2, Welt ~4,7."
        ),
        "description": (
            "Pro-Kopf-Werte sind das Standard-Maß zur fairen Länder-"
            "Vergleichung — sie eliminieren Größen-Effekt der Bevölkerung. "
            "Indien hat ABSOLUT den drittgrößten Emittenten, aber "
            "Pro-Kopf nur ~30 % des EU-Schnitts. China überstieg den "
            "EU-27-Pro-Kopf-Wert 2018. EDGAR-Methodik: Bevölkerungs-"
            "daten aus UN-DESA, EDGAR-Emissionen geteilt durch "
            "Mid-Year-Bevölkerung."
        ),
    },
    # ------------ Kumulativ / historisch ------------
    {
        "topic": "cumulative_history",
        "keywords": ("kumulativ", "cumulative", "historisch", "verursacher"),
        "headline": (
            "Kumulative CO2-Emissionen 1850–2023 (EDGAR v9.0 + CDIAC): "
            "USA ~25 %, EU-27 ~17 %, CN ~14 %, IN ~3 %."
        ),
        "description": (
            "Kumulative (historische) Emissionen bestimmen den heutigen "
            "atmosphärischen CO2-Bestand. Trotz Indiens hoher heutiger "
            "Emissions-Rate liegt Indiens kumulativer Beitrag bei nur "
            "~3 %, weil das Wachstum erst ab ~2000 einsetzte. "
            "Dies ist ein zentrales Argument der Verhandlungs-Position "
            "von G77+China in UN-Klimaverhandlungen. EDGAR + CDIAC "
            "(Carbon Dioxide Information Analysis Center) liefern "
            "konsistente Zeitreihen ab 1850."
        ),
    },
]


# ---------------------------------------------------------------------------
# Anker-Lookup
# ---------------------------------------------------------------------------
def _find_anchors(claim_lc: str) -> list[dict[str, Any]]:
    """Sammle bis zu 4 passende Anker für den Claim.

    Strategie:
    - Pro Anker: prüfe, ob mindestens 1 Keyword im Claim vorkommt.
    - Dedup über 'topic'-Key.
    - Reihenfolge: Anker-Definition (= grobe Priorität).
    """
    matched: dict[str, dict[str, Any]] = {}
    for anchor in EDGAR_ANCHORS:
        if any(kw in claim_lc for kw in anchor["keywords"]):
            if anchor["topic"] not in matched:
                matched[anchor["topic"]] = anchor
                if len(matched) >= 4:
                    break
    return list(matched.values())


def _build_anchor_result(anchor: dict[str, Any]) -> dict[str, Any]:
    """Baue ein kuratiertes EDGAR-Anker-Result."""
    return {
        "title": anchor["headline"],
        "indicator_name": f"EDGAR-Anker: {anchor['topic']}",
        "indicator": "edgar_anchor",
        "topic": anchor["topic"],
        "display_value": anchor["headline"],
        "description": anchor["description"],
        "source": "EDGAR JRC",
        "url": EDGAR_DATASET_URL,
        "secondary_url": EDGAR_REPORT_URL,
        "attribution": (
            "European Commission, Joint Research Centre (JRC) — "
            "EDGAR v9.0 (2024). EC Public Sector Re-use (Commission "
            "Decision 2011/833/EU)."
        ),
    }


def _build_fallback_result() -> dict[str, Any]:
    """Liefere generischen EDGAR-Hinweis, wenn kein konkreter Anker passt.

    (Trigger sprach an, aber Keywords matchten keine Topic-Subkategorie —
    z. B. 'EDGAR' alleine ohne Sektor/Land.)
    """
    return {
        "title": "EDGAR JRC — Übersicht Treibhausgas-Emissionsdatenbank",
        "indicator_name": "EDGAR-Übersicht (Fallback ohne Topic-Match)",
        "indicator": "edgar_anchor",
        "topic": "overview",
        "display_value": (
            "EDGAR JRC v9.0 (Release Dez 2024, Stichjahr 2023) — "
            "globale länder- und sektor-aufgelöste Emissionsdatenbank "
            "der EU-Kommission. Deckt CO2, CH4, N2O, F-Gase + "
            "Luftschadstoffe ab. Globale Anker 2023: 36.800 Mt CO2 "
            "fossil (Top: CN ~12.700, USA ~4.700, IN ~3.200, "
            "EU-27 ~2.420, DE ~675, AT ~65)."
        ),
        "description": (
            "EDGAR ist die einzige Quelle für *konsistente* länder- + "
            "sektor-aufgelöste Emissions-Vergleiche (IEA-Aktivitätsdaten "
            "+ IPCC-2006-Tier-1/2-Emissionsfaktoren). Methodik dokumentiert "
            "im Report 2024 (Crippa et al., JRC PUBSY). Lizenz: EC PSI."
        ),
        "source": "EDGAR JRC",
        "url": EDGAR_DATASET_URL,
        "secondary_url": EDGAR_REPORT_URL,
        "attribution": (
            "European Commission, Joint Research Centre (JRC) — "
            "EDGAR v9.0 (2024)."
        ),
    }


# ---------------------------------------------------------------------------
# Live HEAD-Check (24 h Cache, optional)
# ---------------------------------------------------------------------------
async def _verify_source_live() -> bool | None:
    """Verifiziere die EDGAR-Dataset-Landing-Page (cached 24h).

    Bestätigt, dass die Bulk-CSV-Quelle noch erreichbar ist — protokolliert
    URL-Drift, ohne den 1+ GB-Bulk-Download zu starten.

    Implementierungs-Hinweis: edgar.jrc.ec.europa.eu blockiert HEAD-
    Requests via Content-Security-Policy (alle ec.europa.eu-Portale tun
    das). Wir nutzen GET mit ``Range: bytes=0-0`` — minimaler Traffic,
    aber CSP-konform und liefert validen Status-Code für 200/404/5xx.

    Returns:
        True  → URL erreichbar
        False → URL nicht erreichbar (Status >= 400)
        None  → Netz-Fehler / Timeout (Pack-Antwort trotzdem gültig)
    """
    global _head_cache

    now = time.time()
    if (
        _head_cache.get("ok") is not None
        and (now - _head_cache.get("ts", 0.0)) < EDGAR_CACHE_TTL
    ):
        return _head_cache["ok"]

    try:
        async with polite_client(timeout=DEFAULT_TIMEOUT) as client:
            r = await client.get(
                EDGAR_DATASET_URL,
                headers={"Range": "bytes=0-0"},
                follow_redirects=True,
                timeout=DEFAULT_TIMEOUT,
            )
            # 200 (vollständig) oder 206 (Partial Content) = ok
            ok = r.status_code < 400
            _head_cache = {"ok": ok, "ts": now}
            if not ok:
                logger.warning(
                    f"EDGAR-Quelle Verifikation: HTTP {r.status_code} "
                    f"— Bulk-CSV-URL möglicherweise verschoben"
                )
            return ok
    except Exception as exc:
        logger.debug(f"EDGAR Quellen-Verifikation fehlgeschlagen: {exc!r}")
        # Bei Netz-Fehler nicht cachen → nächster Call versucht erneut
        return None


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_edgar(analysis: dict) -> dict:
    """Suche EDGAR-Emissions-Daten für einen Claim.

    Hybrid-Pattern (vgl. cams.py):
    - Trigger via _claim_mentions_edgar (Keyword + Composite-Match)
    - Bei Hit: bis zu 4 kuratierte Anker (immer verfügbar)
    - Optional: HEAD-Check der Bulk-CSV-Quelle (Aktualitäts-Signal,
      kein Daten-Live-Fetch — EDGAR hat keinen JSON-API-Endpunkt)
    - Graceful-fail: HEAD-Check failt → reine Pack-Antwort

    Return-Schema (kompatibel zu uba_klima.py / cams.py):
        {"source": "EDGAR JRC", "type": "ghg_emissions",
         "results": [...]}
    """
    empty = {
        "source": "EDGAR JRC",
        "type": "ghg_emissions",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_edgar(matchable):
        return empty

    matched = _find_anchors(matchable)
    if not matched:
        # Trigger sprach an (z. B. "EDGAR" oder "CO2 EU-27"), aber kein
        # spezifischer Topic-Anker — generischer Fallback-Block.
        anchor_results: list[dict[str, Any]] = [_build_fallback_result()]
    else:
        anchor_results = [_build_anchor_result(a) for a in matched]

    # Live HEAD-Check (Aktualitäts-Indikator, beeinflusst nur Logs)
    try:
        head_ok = await _verify_source_live()
        if head_ok is False:
            # Quelle nicht erreichbar — kennzeichne im Description
            for r in anchor_results:
                r["description"] = (
                    r.get("description", "")
                    + " HINWEIS: EDGAR-Landing-Page aktuell nicht "
                    "erreichbar (HEAD-Check); Eckwerte bleiben gültig."
                )
    except Exception as exc:
        logger.debug(f"EDGAR HEAD-Verifikation fehlte: {exc!r}")

    # Methodik-Block als letzter Eintrag (V-Dem-/Berkeley-/CAMS-Pattern)
    anchor_results.append({
        "title": "Methodik: EDGAR vs. nationale Inventuren",
        "indicator_name": (
            "WICHTIGER KONTEXT: EDGAR liefert global-konsistente "
            "Sektor-Daten, nationale Inventuren haben höhere Auflösung"
        ),
        "indicator": "edgar_methodology",
        "display_value": (
            "EDGAR JRC v9.0 (2024) — länder- + sektor-aufgelöste Emissionen "
            "auf Basis IEA-Aktivitätsdaten + IPCC-2006-Tier-1/2-EF. "
            "Stichjahr 2023 (n-2 wegen IEA-Lag). Jährliches Release Dez."
        ),
        "description": (
            "EDGAR ist die Standard-Quelle für **internationale Vergleiche**, "
            "weil die Methodik für alle Länder konsistent angewendet wird. "
            "Nationale Inventuren (UBA-OLI für AT, NIR für DE, "
            "EPA-GHGI für USA) haben höhere Auflösung + aktuellere Daten "
            "(n-1), nutzen aber länder-spezifische Emissionsfaktoren. "
            "Beide Quellen stimmen typisch innerhalb ±2-5 % überein. "
            "Für AT-Detail siehe services/uba_klima.py; für globale "
            "Klima-Vergleiche siehe services/owid.py / services/eea.py."
        ),
        "source": "EDGAR JRC",
        "url": EDGAR_REPORT_URL,
        "attribution": (
            "European Commission, Joint Research Centre (JRC) — "
            "EDGAR v9.0 (2024). EC Public Sector Re-use (Commission "
            "Decision 2011/833/EU)."
        ),
    })

    return {
        "source": "EDGAR JRC",
        "type": "ghg_emissions",
        "results": anchor_results,
    }
