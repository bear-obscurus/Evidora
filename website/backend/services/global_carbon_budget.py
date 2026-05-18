"""Global Carbon Budget (GCB) — Global Carbon Project.

Quelle: Global Carbon Project (GCP) — jährlicher Carbon-Budget-Report.
Konsens-Datensatz der Klimaforschung für globale anthropogene CO2-Emissionen,
Aufteilung nach Sektoren (fossile Brennstoffe, Zement, Landnutzungs-/
Landnutzungsänderung) und natürliche Senken (Ozean, terrestrische Biosphäre,
Atmosphären-Wachstum). Wird von ~100 Forschungsinstituten getragen
(Friedlingstein et al., Earth System Science Data).

Datenzugang:
  - Primär-Publikation: https://globalcarbonbudget.org/
  - GitHub-Mirror (openclimatedata): https://github.com/openclimatedata/global-carbon-budget
  - Zenodo-DOI: Annual Carbon Budget Dataset (CC-BY 4.0)
  - Aktueller Bericht: Carbon Budget 2024 (Friedlingstein et al. 2024, ESSD)

Architektur-Hinweis — warum Pack-First mit optionalem Live-Hook:
  Der GCB-Datensatz wird einmal jährlich (typisch November/Dezember) auf
  Zenodo + GitHub veröffentlicht. Die Eckwerte ändern sich also nicht
  zwischen den Jahresreleases. Live-Abfragen auf das GitHub-CSV sind
  möglich (1-2 s), aber für einen Faktencheck mit 20-s-Budget reichen
  kuratierte Eckwerte des aktuellsten Reports. Pattern (cf. era5.py,
  ARCHITECTURE.md §3.5 Static-First):

    1. Kuratierte GCB-2024-Eckwerte als hardcoded Pack-Statements
       (globale Emissionen, fossil vs. Landnutzung, Top-5-Emittenten,
       Senken-Anteile, atmosphärischer CO2-Anstieg). Diese sind
       IPCC-/WMO-konsens und brauchen keine API.
    2. Optional: Repository-Metadaten von GitHub anhängen, um eine
       aktuelle Dataset-Referenz zu liefern. Best-effort, Graceful Fail.
    3. Wenn der Repo-Call fehlschlägt: nur Pack-Eckwerte (0 != []).

Komplementär zu:
  - era5.py / copernicus.py: Temperatur-/Wetter-Rekorde (Wirkung).
  - global_carbon_budget.py: Emissions-Bilanzen (Ursache).
  - Cluster ergibt ein vollständiges Klima-Faktencheck-Triple
    (Emissionen → Konzentration → Temperatur).

Politische Guardrails: Reine Datenquelle. KEINE Zuschreibungen
("Industrienation X ist 'schuld'") und KEINE Politik-Empfehlungen.
Die kausale + politische Einordnung übernimmt der Synthesizer mit den
üblichen IPCC-Konsens-Disclaimern. Top-Emittenten-Listen sind
faktuelle GtCO2-Werte, nicht moralische Rankings.

WIRING für main.py (NICHT in dieser Datei vornehmen):
  from services.global_carbon_budget import (
      search_global_carbon_budget,
      claim_mentions_gcb_cached,
  )
  if claim_mentions_gcb_cached(claim):
      tasks.append(cached("GlobalCarbonBudget", search_global_carbon_budget, analysis))
      queried_names.append("Global Carbon Budget")
  # Zusätzlich:
  #   - reranker-Whitelist: "Global Carbon Budget" eintragen
  #   - data_updater.py: KEIN Prefetch nötig (Pack-Eckwerte sind statisch,
  #     Repo-Call ist <2s und passiert pro Request mit 24h-Cache).
"""

from __future__ import annotations

import logging
import re
import time
from functools import lru_cache

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# Endpoints (best-effort live, Pack-Daten sind primär)
GCB_HOME_URL = "https://globalcarbonbudget.org/"
GCB_GITHUB_REPO_API = "https://api.github.com/repos/openclimatedata/global-carbon-budget"
GCB_GITHUB_REPO_HTML = "https://github.com/openclimatedata/global-carbon-budget"
GCB_ZENODO_DOI = "https://doi.org/10.18160/GCP-2024"  # Annual GCB DOI-Pattern

CACHE_TTL_S = 24 * 60 * 60  # 24 h
TIMEOUT_S = 6.0  # Repo-Metadaten-Call

# Modul-Cache für Repo-Metadaten
_repo_cache: dict[str, tuple[float, dict | None]] = {}


# ---------------------------------------------------------------------------
# Trigger — wann soll dieser Service angefragt werden?
# ---------------------------------------------------------------------------
_GCB_TERMS = (
    # Direkter Name
    "global carbon budget", "globales co2-budget", "globales co2 budget",
    "carbon budget", "co2-budget",
    "global carbon project", "globales carbon project",
    "carbon project",
    "gcb", "gcp carbon",
    # Globale Emissionen
    "globale emissionen", "global emissions",
    "globale co2-emissionen", "globale co2 emissionen",
    "globaler co2-ausstoß", "globaler co2 ausstoß",
    "globaler co2-ausstoss", "globaler co2 ausstoss",
    "weltweite co2-emissionen", "weltweite co2 emissionen",
    "weltweiter co2-ausstoß", "weltweiter co2 ausstoss",
    "global co2 emissions", "world co2 emissions",
    "globale treibhausgas-emissionen", "globale treibhausgase",
    # Fossile Emissionen
    "fossile co2-emissionen", "fossile co2 emissionen",
    "fossile emissionen weltweit", "fossile emissionen global",
    "fossil co2 emissions",
    # Spezifische Pattern (häufige Claims)
    "co2-ausstoß global", "co2 ausstoß global",
    "co2-ausstoss global", "co2 ausstoss global",
    "co2-emissionen weltweit", "co2 emissionen weltweit",
    "größter co2-emittent", "grösster co2-emittent",
    "größte co2-emittenten", "grösste co2-emittenten",
    "top co2 emittenten", "top emittenten co2",
    # Senken / Bilanz
    "co2-senken", "co2 senken", "carbon sinks",
    "landnutzungs-emissionen", "landnutzung co2", "landnutzungsänderung co2",
)

# Composite-Trigger: GtCO2-Mengenangaben oder Anteilsfragen
_QUANTITY_TOKENS = (
    "gtco2", "gt co2", "gigatonne co2", "gigatonnen co2",
    "milliarden tonnen co2", "mrd tonnen co2",
    "ppm co2", "co2 ppm",
)


def _claim_mentions_gcb(claim_lc: str) -> bool:
    """Trigger-Pre-Check für Global Carbon Budget.

    True wenn:
      1. Direkter GCB-Begriff im Claim, ODER
      2. Quantitative Emissions-Größe (GtCO2, ppm) mit Global-Kontext.
    """
    if not claim_lc:
        return False
    # 1) Direkt-Trigger
    if any(t in claim_lc for t in _GCB_TERMS):
        return True
    # 2) Composite: Mengen-Token + Global-Hinweis
    has_quantity = any(q in claim_lc for q in _QUANTITY_TOKENS)
    if has_quantity:
        has_global = any(g in claim_lc for g in (
            "global", "weltweit", "welt", "world", "menschheit",
            "atmosphäre", "atmosphaere",
        ))
        if has_global:
            return True
    return False


@lru_cache(maxsize=512)
def claim_mentions_gcb_cached(claim: str) -> bool:
    """Public-Wrapper für Trigger-Check (case-normalisiert, LRU-gecached)."""
    return _claim_mentions_gcb((claim or "").lower())


# ---------------------------------------------------------------------------
# Kuratierte GCB-2024-Eckwerte ("Pack-Fallback")
# ---------------------------------------------------------------------------
# Quellen: Friedlingstein et al. (2024), "Global Carbon Budget 2024",
#   Earth System Science Data (ESSD), DOI: 10.5194/essd-16-3795-2024.
# Werte gerundet, Bezugsjahr 2023 (jüngste vollständige Bilanz im GCB 2024)
# bzw. 2024-Projektion wo ausgewiesen.
_GCB_HIGHLIGHTS: list[dict] = [
    {
        "title": (
            "Globale CO2-Gesamtemissionen 2024: ~41.6 GtCO2 (fossil + "
            "Landnutzungsänderung), Projektion Global Carbon Budget 2024"
        ),
        "indicator_name": "Globale CO2-Gesamtemissionen 2024",
        "indicator": "gcb_total_emissions_2024",
        "year": "2024",
        "value": "~41.6 GtCO2 (Projektion)",
        "description": (
            "Das Global Carbon Project projiziert für 2024 globale anthropogene "
            "CO2-Gesamtemissionen von ~41.6 GtCO2: rund 37.4 GtCO2 aus "
            "fossilen Brennstoffen und Industrie (+0.8 % gegenüber 2023) plus "
            "~4.2 GtCO2 aus Landnutzungsänderung (v.a. Tropen-Entwaldung). "
            "Damit ist 2024 das Jahr mit den höchsten je gemessenen "
            "fossilen CO2-Emissionen. Es gibt KEIN Anzeichen eines globalen "
            "Emissions-Peaks."
        ),
        "source": "Global Carbon Project / Friedlingstein et al. 2024 (ESSD)",
        "url": GCB_HOME_URL,
    },
    {
        "title": (
            "Anteil fossiler Brennstoffe an den globalen CO2-Emissionen 2024: "
            "~90 % (37.4 von 41.6 GtCO2)"
        ),
        "indicator_name": "Fossile vs. Landnutzungs-Anteile 2024",
        "indicator": "gcb_split_fossil_lulc_2024",
        "year": "2024",
        "value": "fossil ~90 % / LULC ~10 %",
        "description": (
            "Von den projizierten ~41.6 GtCO2 globaler CO2-Emissionen 2024 "
            "entfallen rund 90 % auf fossile Brennstoffe + Zement-Produktion "
            "(~37.4 GtCO2) und ~10 % auf Landnutzungsänderung (Entwaldung, "
            "Torfland-Degradation, Bodenoxidation; ~4.2 GtCO2). Der "
            "Fossil-Anteil steigt langfristig, während Landnutzungs-Emissionen "
            "in absoluten Werten in etwa stagnieren."
        ),
        "source": "Global Carbon Project / Friedlingstein et al. 2024 (ESSD)",
        "url": GCB_HOME_URL,
    },
    {
        "title": (
            "Top-5-Emittenten 2023 (fossil + Zement): China ~31 %, USA ~13 %, "
            "Indien ~8 %, EU-27 ~6 %, Russland ~5 %"
        ),
        "indicator_name": "Top-5-Emittenten fossil 2023",
        "indicator": "gcb_top5_emitters_2023",
        "year": "2023",
        "value": "CN 31 % / US 13 % / IN 8 % / EU 6 % / RU 5 %",
        "description": (
            "GCB-2024 Rangliste für fossile CO2-Emissionen 2023 (Anteil am "
            "globalen fossilen Gesamtvolumen ~37.1 GtCO2): "
            "China ~31 % (~11.4 GtCO2), USA ~13 % (~4.9 GtCO2), "
            "Indien ~8 % (~3.1 GtCO2), EU-27 ~6 % (~2.4 GtCO2), "
            "Russland ~5 % (~1.8 GtCO2). Diese fünf decken zusammen ~63 % der "
            "globalen fossilen CO2-Emissionen ab. Die Rangfolge ist seit "
            "~2006 stabil (China überholte damals die USA)."
        ),
        "source": "Global Carbon Project 2024",
        "url": GCB_HOME_URL,
    },
    {
        "title": (
            "Pro-Kopf-CO2-Emissionen 2023 (fossil): USA ~14.3 t/Kopf, "
            "Russland ~12.7, China ~8.0, EU-27 ~5.4, Indien ~2.1, Welt ~4.6"
        ),
        "indicator_name": "Pro-Kopf-CO2-Emissionen 2023",
        "indicator": "gcb_per_capita_2023",
        "year": "2023",
        "value": "Welt ~4.6 tCO2/Kopf",
        "description": (
            "Pro-Kopf-Sicht (fossile CO2-Emissionen 2023, tCO2 pro Kopf): "
            "USA ~14.3, Russland ~12.7, China ~8.0, EU-27 ~5.4, Welt-Mittel "
            "~4.6, Indien ~2.1. Pro-Kopf ist die USA-Emission damit fast 7x "
            "höher als Indiens und ~3x höher als der Weltdurchschnitt. "
            "Wichtig: GCB-Zahlen sind territoriale Emissionen (Produktion); "
            "konsumbasierte Bilanzen können für Industrienationen 10–20 % "
            "höher liegen, weil sie Importgüter mitberücksichtigen."
        ),
        "source": "Global Carbon Project 2024",
        "url": GCB_HOME_URL,
    },
    {
        "title": (
            "Natürliche CO2-Senken 2014–2023: Ozean nimmt ~26 %, "
            "Landbiosphäre ~31 % der anthropogenen CO2-Emissionen auf, "
            "~46 % verbleiben in der Atmosphäre"
        ),
        "indicator_name": "CO2-Senken-Bilanz (Dekade 2014-2023)",
        "indicator": "gcb_sinks_decade_2014_2023",
        "year": "2014-2023",
        "value": "Ozean 26 % / Land 31 % / Atmosphäre 46 %",
        "description": (
            "Das Global Carbon Budget bilanziert über die Dekade 2014–2023 "
            "die Aufteilung der anthropogenen CO2-Emissionen: ~26 % nimmt "
            "der Ozean auf (Versauerung als Nebeneffekt), ~31 % die "
            "terrestrische Biosphäre (Wälder, Böden), und ~46 % verbleiben "
            "in der Atmosphäre — was den jährlichen ppm-Anstieg erklärt. "
            "Eine kleine 'Budget-Imbalance' (~3 %) verbleibt als statistische "
            "Unsicherheit im Bilanzschluss."
        ),
        "source": "Global Carbon Project / Friedlingstein et al. 2024 (ESSD)",
        "url": GCB_HOME_URL,
    },
    {
        "title": (
            "Atmosphärische CO2-Konzentration 2024: ~422 ppm Jahresmittel "
            "(NOAA Mauna Loa + GCB), +50 % über vorindustriellem Wert (280 ppm)"
        ),
        "indicator_name": "Atmosphärische CO2-Konzentration 2024",
        "indicator": "gcb_atmospheric_co2_2024",
        "year": "2024",
        "value": "~422 ppm",
        "description": (
            "Die globale Jahresmittel-CO2-Konzentration 2024 liegt nach "
            "NOAA-Auswertung der Mauna-Loa-Reihe + Global-Carbon-Budget-"
            "Bilanz bei ~422 ppm — rund 50 % über dem vorindustriellen Wert "
            "von ~280 ppm. Der jährliche Anstieg beträgt aktuell ~2.5 ppm/Jahr "
            "(beschleunigt von ~1 ppm/Jahr in den 1960er Jahren). Dieser "
            "Konzentrationsanstieg ist die zentrale Ursache für das "
            "anthropogene Strahlungsantrieb-Forcing."
        ),
        "source": "Global Carbon Project / NOAA GML",
        "url": "https://gml.noaa.gov/ccgg/trends/",
    },
    {
        "title": (
            "Verbleibendes 1.5°C-Carbon-Budget (50 % Wahrscheinlichkeit, "
            "ab 2024): ~235 GtCO2 — bei aktuellem Tempo (~41 GtCO2/Jahr) "
            "in unter 6 Jahren aufgebraucht"
        ),
        "indicator_name": "Verbleibendes 1.5°C-Budget ab 2024",
        "indicator": "gcb_remaining_15c_budget_2024",
        "year": "2024",
        "value": "~235 GtCO2 (≈ 6 Jahre)",
        "description": (
            "Das Global Carbon Project beziffert das verbleibende Kohlenstoff-"
            "Budget für 50 % Wahrscheinlichkeit, das 1.5°C-Ziel des Pariser "
            "Abkommens einzuhalten, mit ~235 GtCO2 ab Anfang 2024. Bei "
            "aktuellem Emissions-Tempo (~41.6 GtCO2/Jahr) wäre dieses Budget "
            "Ende 2029 / Anfang 2030 erschöpft. Für 67 % Wahrscheinlichkeit "
            "liegt das verbleibende Budget bei nur ~155 GtCO2 (~4 Jahre). "
            "Diese Größen tragen substantielle Unsicherheiten (±100 GtCO2)."
        ),
        "source": "Global Carbon Project / IPCC AR6 Synthese",
        "url": GCB_HOME_URL,
    },
    {
        "title": (
            "Kumulierte historische CO2-Emissionen 1850–2023: ~1.78 Billionen "
            "tCO2 (fossil + LULC), davon ~25 % USA, ~13 % China, ~10 % EU-27"
        ),
        "indicator_name": "Kumulierte historische Emissionen 1850-2023",
        "indicator": "gcb_cumulative_1850_2023",
        "year": "1850-2023",
        "value": "~1780 GtCO2 kumuliert",
        "description": (
            "Die GCB-2024-Bilanz beziffert die kumulierten anthropogenen "
            "CO2-Emissionen 1850–2023 mit ~1.78 Billionen tCO2 (~750 GtC). "
            "Beim historischen Anteil dominieren USA (~25 %), China (~13 %, "
            "stark steigend seit 2000), EU-27 (~10 %), Russland (~6 %), "
            "und Japan (~4 %). Dieser kumulierte Anteil ist relevant für "
            "die Klimagerechtigkeits-Debatte (Common-but-differentiated "
            "Responsibilities, CBDR-Prinzip im UNFCCC)."
        ),
        "source": "Global Carbon Project 2024",
        "url": GCB_HOME_URL,
    },
    {
        "title": (
            "Sektor-Aufteilung fossiler CO2-Emissionen 2023: Kohle ~41 %, "
            "Öl ~32 %, Gas ~21 %, Zement ~4 %, Flaring ~1 %"
        ),
        "indicator_name": "Sektor-Aufteilung fossil 2023",
        "indicator": "gcb_fuel_split_2023",
        "year": "2023",
        "value": "Kohle 41 / Öl 32 / Gas 21 / Zement 4 / Flaring 1 (%)",
        "description": (
            "Aufteilung der globalen fossilen CO2-Emissionen 2023 nach "
            "Brennstoff/Quelle: Kohle ~41 % (~15.4 GtCO2), Öl ~32 % (~11.9 "
            "GtCO2), Gas ~21 % (~7.9 GtCO2), Zement-Produktion ~4 % (~1.6 "
            "GtCO2), Flaring + sonstiges ~1 %. Kohle ist 2023 ein neues "
            "Allzeithoch erreicht (vor allem China + Indien); Gas wächst "
            "weltweit am schnellsten (~1.5 %/Jahr); Öl stagniert. "
            "Zement allein emittiert mehr CO2 als die gesamte Luftfahrt."
        ),
        "source": "Global Carbon Project 2024",
        "url": GCB_HOME_URL,
    },
    {
        "title": (
            "EU-27 CO2-Emissionen 2023: ~2.4 GtCO2 fossil — Rückgang um "
            "~8 % gegenüber 2022, ~32 % unter Wert von 1990"
        ),
        "indicator_name": "EU-27 fossile CO2-Emissionen 2023",
        "indicator": "gcb_eu27_2023",
        "year": "2023",
        "value": "~2.4 GtCO2 (–8 % vs. 2022)",
        "description": (
            "Die EU-27 verzeichnete 2023 einen außergewöhnlich starken "
            "Rückgang der fossilen CO2-Emissionen um ~8 % gegenüber 2022 "
            "(auf ~2.4 GtCO2). Treiber: deutlich geringere Kohleverstromung "
            "(milder Winter + hoher Erneuerbaren-Anteil), Industrie-"
            "Drosselung als Folge der Energiekrise 2022, und beschleunigter "
            "Ausbau der Erneuerbaren. Damit liegen die EU-Emissionen 2023 "
            "rund 32 % unter dem Stand von 1990 (Kyoto-Referenzjahr)."
        ),
        "source": "Global Carbon Project 2024 / EEA",
        "url": GCB_HOME_URL,
    },
    {
        "title": (
            "China-CO2-Emissionen 2024 (Projektion): ~11.4 GtCO2 fossil "
            "(+0.2 % vs. 2023) — Plateau-Phase angedeutet, kein Peak bestätigt"
        ),
        "indicator_name": "China fossile CO2-Emissionen 2024",
        "indicator": "gcb_china_2024",
        "year": "2024",
        "value": "~11.4 GtCO2 (Projektion)",
        "description": (
            "GCB-2024-Projektion für China: fossile CO2-Emissionen 2024 bei "
            "~11.4 GtCO2 (+0.2 %, nahezu Stagnation gegenüber 2023). "
            "China ist seit 2006 der weltgrößte Emittent und macht aktuell "
            "~31 % der globalen fossilen CO2-Emissionen aus. Ob 2023/2024 "
            "der Emissions-Peak war, ist methodisch noch offen — möglich, "
            "aber nicht bestätigt; das politische Ziel laut Xi Jinping ist "
            "ein Peak vor 2030 und Klimaneutralität bis 2060."
        ),
        "source": "Global Carbon Project 2024",
        "url": GCB_HOME_URL,
    },
    {
        "title": (
            "USA-CO2-Emissionen 2023: ~4.9 GtCO2 fossil (–3 % vs. 2022), "
            "~15 % unter Wert von 2005 (Höchststand)"
        ),
        "indicator_name": "USA fossile CO2-Emissionen 2023",
        "indicator": "gcb_usa_2023",
        "year": "2023",
        "value": "~4.9 GtCO2 (–3 % vs. 2022)",
        "description": (
            "Die USA emittierten 2023 ~4.9 GtCO2 aus fossilen Brennstoffen "
            "(–3 % vs. 2022). Hauptursache des Rückgangs: weitere Kohle-zu-"
            "Gas-Substitution + Erneuerbaren-Ausbau im Stromsektor. Die "
            "USA-Emissionen liegen 2023 rund 15 % unter dem Höchststand 2005 "
            "und damit weiterhin auf einem langfristig fallenden Pfad — "
            "allerdings nicht schnell genug für die Pariser 1.5°C-Ziele."
        ),
        "source": "Global Carbon Project 2024 / EIA",
        "url": GCB_HOME_URL,
    },
    {
        "title": (
            "Indien-CO2-Emissionen 2024 (Projektion): ~3.2 GtCO2 fossil "
            "(+4.6 % vs. 2023) — derzeit stärkster Emissions-Wachstumstreiber"
        ),
        "indicator_name": "Indien fossile CO2-Emissionen 2024",
        "indicator": "gcb_india_2024",
        "year": "2024",
        "value": "~3.2 GtCO2 (+4.6 %)",
        "description": (
            "Indien ist 2024 der stärkste Wachstumstreiber globaler "
            "CO2-Emissionen: Projektion ~3.2 GtCO2 (+4.6 % vs. 2023). "
            "Mit ~8 % am globalen Total liegt Indien an dritter Stelle. "
            "Pro Kopf bleibt Indien jedoch deutlich unter dem Weltmittel "
            "(~2.1 vs. ~4.6 tCO2/Kopf) — etwa ein Siebtel der US-Pro-Kopf-"
            "Emissionen. Wachstumstreiber: Kohleverstromung + Zement."
        ),
        "source": "Global Carbon Project 2024",
        "url": GCB_HOME_URL,
    },
]


def _gcb_methodology_row() -> dict:
    """Methodik-Disclaimer (analog era5/v-dem-Pattern): erklärt dem
    Synthesizer die Einschränkungen des GCB-Datensatzes.
    """
    return {
        "title": "Methodik: Global Carbon Budget (GCB)",
        "indicator_name": "WICHTIGER KONTEXT: GCB ist eine bilanzierende Schätzung, keine Direktmessung",
        "indicator": "gcb_methodology",
        "year": "",
        "value": "",
        "description": (
            "Der Global Carbon Budget kombiniert: "
            "(1) Aktivitätsdaten — nationale Statistiken zu Brennstoff-"
            "verbrauch und Zement-Produktion (UNFCCC, IEA, BP, EDGAR, EIA). "
            "(2) Emissions-Faktoren — IPCC-Standardwerte je Brennstoff. "
            "(3) Landnutzungs-Modelle — Bookkeeping-Modelle (BLUE, H&N, "
            "OSCAR) + Satellitendaten für Entwaldung/Wiederaufforstung. "
            "(4) Senken-Modelle — Ocean Biogeochemistry Models + Dynamic "
            "Global Vegetation Models. "
            "Einschränkungen: "
            "(a) Territoriale vs. konsumbasierte Emissionen — GCB nutzt das "
            "Produktionsprinzip (UNFCCC-Konvention). Konsumbasierte Werte "
            "sind für Importnationen typisch 10–20 % höher. "
            "(b) Landnutzungs-Anteil (~4 GtCO2) hat größere Unsicherheit "
            "(±70 %) als fossile Werte (±5 %). "
            "(c) Aktuelle Jahres-Werte (2024) sind Projektionen auf Basis "
            "Q1–Q3-Daten; Final-Werte folgen ein Jahr später. "
            "(d) Methan + N2O sind NICHT enthalten — GCB bilanziert nur CO2. "
            "Für 'Treibhausgase gesamt' braucht es CO2-Äquivalente aus "
            "IPCC-AR6 oder EDGAR."
        ),
        "source": "Friedlingstein et al. 2024 (ESSD), GCB Methodology",
        "url": "https://essd.copernicus.org/articles/16/3795/2024/",
    }


def _select_relevant_highlights(claim_lc: str) -> list[dict]:
    """Pick highlights, deren Jahr/Region zum Claim passt.

    Score: +3 für Jahres-Match, +2 für Länder-/Region-Stichwort,
           +1 für Sektor-/Senken-Token. Stabil sortiert.
    """
    if not claim_lc:
        return list(_GCB_HIGHLIGHTS)

    region_tokens = {
        "china": ("china", "cn", "volksrepublik"),
        "usa": ("usa", "us-amerika", "vereinigte staaten", "amerika"),
        "indien": ("indien", "india"),
        "eu": ("eu-27", "eu 27", "europäische union", "europaeische union"),
        "russland": ("russland", "russia"),
    }
    sector_tokens = (
        "kohle", "coal", "öl", "oel", "oil", "gas", "zement", "cement",
        "fossil", "landnutzung", "lulc", "entwaldung",
        "senke", "ozean", "biosphäre", "atmosphäre", "ppm",
        "pro kopf", "per capita", "kumuliert", "historisch", "budget",
        "1.5", "1,5",
    )

    scored: list[tuple[int, int, dict]] = []
    for idx, h in enumerate(_GCB_HIGHLIGHTS):
        score = 0
        # Jahres-Match
        for y in re.findall(r"\d{4}", h.get("year", "")):
            if y in claim_lc:
                score += 3
        # Region-Match (im Indicator-Namen)
        name_lc = h.get("indicator_name", "").lower()
        for _, aliases in region_tokens.items():
            if any(a in name_lc for a in aliases) and any(a in claim_lc for a in aliases):
                score += 2
                break
        # Sektor-/Token-Match
        for token in sector_tokens:
            if token in name_lc and token in claim_lc:
                score += 1
        scored.append((score, -idx, h))

    scored.sort(reverse=True)
    return [h for _, _, h in scored]


# ---------------------------------------------------------------------------
# Optionaler GitHub-Repo-Metadaten-Fetch (~1 s)
# ---------------------------------------------------------------------------
async def _fetch_repo_metadata() -> dict | None:
    """Hole Repo-Metadaten vom openclimatedata/global-carbon-budget Mirror.

    Cached 24h. Liefert None bei Fehler (Pack-Eckwerte sind dann der
    einzige Output — Graceful Fail).
    """
    now = time.time()
    cached = _repo_cache.get("repo")
    if cached and now - cached[0] < CACHE_TTL_S:
        return cached[1]

    try:
        async with polite_client(timeout=TIMEOUT_S) as client:
            resp = await client.get(GCB_GITHUB_REPO_API)
            resp.raise_for_status()
            metadata = resp.json()
        _repo_cache["repo"] = (now, metadata)
        return metadata
    except Exception as e:
        logger.warning(f"GCB repo metadata fetch failed: {e}")
        # Negative cache (kurz), damit wir nicht jeden Request neu probieren
        _repo_cache["repo"] = (now, None)
        return None


def _format_repo_row(metadata: dict) -> dict:
    """Bau einen Quellen-Referenz-Row aus den GitHub-Repo-Metadaten."""
    description = (metadata.get("description") or "")[:200]
    pushed_at = (metadata.get("pushed_at") or "")[:10]  # YYYY-MM-DD
    stars = metadata.get("stargazers_count")
    return {
        "title": "Global Carbon Budget — Open-Data-Mirror (openclimatedata)",
        "indicator_name": "GCB-Datensatz-Referenz (GitHub)",
        "indicator": "gcb_dataset_reference",
        "year": pushed_at[:4] if pushed_at else "",
        "value": f"letztes Update {pushed_at}" if pushed_at else "Open-Data-Mirror",
        "description": (
            f"{description} "
            f"Maschinenlesbarer CSV/Excel-Mirror des offiziellen Global Carbon "
            f"Budget. Repository aktuell mit ~{stars or '?'} Stars. "
            f"Vollständige Zeitreihen 1750–heute (LULC) bzw. 1850–heute "
            f"(fossil), nach Land + Sektor. Lizenz CC-BY 4.0."
        ).strip(),
        "source": "openclimatedata GitHub-Mirror",
        "url": GCB_GITHUB_REPO_HTML,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
async def search_global_carbon_budget(analysis: dict) -> dict:
    """Liefere GCB-relevante Eckwerte + (optional) Repo-Referenz.

    Strategie:
      1. Trigger-Check (Caller sollte ihn bereits gemacht haben).
      2. Kuratierte GCB-2024-Highlights nach Claim-Relevanz sortieren
         (Jahres-Match + Region-Match priorisiert).
      3. Optional: GitHub-Repo-Metadaten anhängen (~1 s) — wenn das Netz
         hängt, fällt der Block stumm weg (Graceful Fail).
      4. Methodik-Disclaimer als letzter Row anhängen.
    """
    empty = {
        "source": "Global Carbon Budget",
        "type": "climate_emissions",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or ""
    original = analysis.get("original_claim") or claim
    if not isinstance(claim, str):
        claim = str(claim or "")
    if not isinstance(original, str):
        original = str(original or "")
    combined_lc = f"{original} {claim}".strip().lower()

    if not _claim_mentions_gcb(combined_lc):
        return empty

    # 1) Kuratierte Highlights, nach Relevanz sortiert
    highlights = _select_relevant_highlights(combined_lc)
    # Cap: max 5 Highlights, sonst überfrachten wir den Synthesizer
    results: list[dict] = highlights[:5]

    # 2) Repo-Referenz (best-effort, Graceful Fail)
    metadata = await _fetch_repo_metadata()
    if metadata:
        try:
            results.append(_format_repo_row(metadata))
        except Exception as e:
            logger.warning(f"GCB repo row format failed: {e}")

    # 3) Methodik-Disclaimer als Hinweis-Row
    results.append(_gcb_methodology_row())

    logger.info(
        f"GlobalCarbonBudget: {len(results)} Einträge geliefert "
        f"(Highlights + Repo + Disclaimer)"
    )
    return {
        "source": "Global Carbon Budget",
        "type": "climate_emissions",
        "results": results,
        "attribution": (
            "Source: Global Carbon Project — Friedlingstein et al. (2024), "
            "'Global Carbon Budget 2024', Earth System Science Data, "
            "DOI: 10.5194/essd-16-3795-2024. Lizenz: CC-BY 4.0. "
            "Open-Data-Mirror: openclimatedata/global-carbon-budget (GitHub)."
        ),
    }
