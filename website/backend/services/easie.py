"""EASIE — European Agency Statistics on Inclusive Education (Static-First-Snapshot).

Die European Agency for Special Needs and Inclusive Education (https://www.european-agency.org)
publiziert seit 2014 alle 2 Jahre standardisierte Cross-Country-Datentabellen
zu inklusiver Bildung in 35 EU/EFTA-Ländern. Letzte finalisierte Edition:
Referenz-Schuljahr 2018/2019, publiziert 2022.

Datenformat: XLSX-Datentabellen (KEIN JSON-API). Daher Static-Snapshot-Modus
analog ``services/freedom_house.py``:

  - ``data/easie.json`` enthält 3 Kern-Indikatoren pro Land:
    * identified_sen_rate_pct  — % aller Pflichtschüler:innen mit formaler
      SEN/SPF-Feststellung (national-definierte Schwelle)
    * inclusive_enrolment_pct  — % der SEN-Schüler:innen, die ≥80 % der
      Schulwoche in einer Regelklasse unterrichtet werden
    * separate_settings_pct    — % der SEN-Schüler:innen in separaten
      Sonderschulen oder Sonderklassen

Lizenz: CC BY-NC-ND 4.0 (Standard-Lizenz der European Agency). Quote mit
voller Quellenangabe, no derivatives, non-commercial. NC-Klausel: bei
Faktencheck-Service-Einbettung wahrscheinlich legitim als 'fair use /
zitierende Verwendung', aber bei kommerzieller Nutzung im Zweifel direkt
beim Anbieter anfragen.

Komplementär zu existierenden Quellen:
  - inklusion_pack (services/inklusion_pack.py): Spezifische Mythen-Konsense
    zu Autismus / ADHS / Down-Syndrom (peer-reviewed Studien).
  - WCAG 2.2 (services/wcag22.py): Web-Accessibility-Standards.
  - destatis / oecd: Allgemeine Bildungs-Statistik DACH.
  - EASIE: Cross-Country-Vergleichsdaten zur SCHUL-Inklusion (kein Web,
    keine Spezifik-Diagnose, sondern Quoten-Indikatoren je Land).

GUARDRAILS — Politik-Tabu (siehe project_political_guardrails.md):
  - WIR ZITIEREN Quoten, WIR BEWERTEN NICHT, ob ein Land "gut" oder
    "rückständig" abschneidet.
  - WIR NEHMEN KEINE eigene normative Klassifikation vor ("progressiv",
    "fortgeschritten"). Methodik-Caveat ist Pflicht.
  - Hinweis auf unterschiedliche nationale SEN-Definitionen ist immer
    Teil der description (sonst Vergleich irreführend).

Trigger-Strategie:
  Claim enthält Inklusions-/SEN-Keyword UND mindestens ein Land-Alias
  ODER nur das EASIE-Vokabular ("inklusive bildung quote", "sen-quote
  europa", ...) — dann DACH-Default (AT/DE/CH).

Result-Schema:
  {
    "indicator_name": "EASIE 2018/19 — Österreich: 4,6% SEN, 60,8% inklusiv beschult",
    "indicator": "easie_inclusive_enrolment",
    "country": "AT",
    "year": "2018/2019",
    "topic": "inclusive_education_stats",
    "display_value": "AT 60,8% inklusiv (39,2% separat), 4,6% SEN-Identifikation — vs. DE 44,3%, IT 99,7%, SE 99,0% (EASIE 2018/19)",
    "description": "EASIE-Methodik + nationale SEN-Definition-Unterschiede + Lizenz-Hinweis (CC BY-NC-ND)",
    "url": "https://www.european-agency.org/data/easie",
    "source": "EASIE European Agency Statistics on Inclusive Education",
  }

# WIRING für main.py (NICHT in diesem Patch — manuell):
#
#   from services.easie import (
#       search_easie,
#       claim_mentions_easie_cached,
#   )
#
#   if claim_mentions_easie_cached(claim):
#       tasks.append(cached("EASIE", search_easie, analysis))
#       queried_names.append("EASIE")
#
# WIRING für services/reranker.py (Indicator-Whitelist):
#   "easie_" als Whitelist-Prefix in INDICATOR_WHITELIST_PREFIXES
#
# data_updater.py: KEIN Prefetch (reines Static-JSON via Hot-Reload-Cache).
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache

from services._static_cache import load_json_mtime_aware

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "easie.json",
)

MAX_PRIMARY_COUNTRIES = 3
MAX_DISPLAY_REFERENCE_COUNTRIES = 4

# Wenn Claim Inklusions-Keyword nennt, aber kein Land — fall back auf DACH.
_DEFAULT_COUNTRIES_FOR_DACH_CLAIMS = ("AUT", "DEU", "CHE")

# Reference-Pool für vs.-Vergleich im display_value. Auswahl: AT zuerst,
# dann DE/CH, dann je 1 Land aus jedem "Inklusions-Cluster":
#   - Vollintegration (IT, PRT, SWE)
#   - Mittelfeld (FRA, FIN)
#   - Separations-stark (BEL)
_DISPLAY_REFERENCE_COUNTRIES = (
    "AUT", "DEU", "CHE", "ITA", "PRT", "SWE", "FRA", "FIN", "BEL",
)

# ISO3 → ISO2 für kompakte display_value-Darstellung.
_ISO3_TO_ISO2 = {
    "AUT": "AT", "DEU": "DE", "CHE": "CH", "FRA": "FR", "ITA": "IT",
    "ESP": "ES", "GBR": "UK", "SWE": "SE", "NOR": "NO", "DNK": "DK",
    "FIN": "FI", "NLD": "NL", "BEL": "BE", "IRL": "IE", "ISL": "IS",
    "LUX": "LU", "MLT": "MT", "PRT": "PT", "GRC": "GR", "HUN": "HU",
    "POL": "PL", "CZE": "CZ", "SVK": "SK", "SVN": "SI", "EST": "EE",
    "LVA": "LV", "LTU": "LT", "ROU": "RO", "BGR": "BG", "HRV": "HR",
    "CYP": "CY", "SRB": "RS", "RKS": "XK",
}

# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
# Kern-Trigger: explizite Inklusions-/SEN-Statistik-Vokabeln.
_EASIE_KEYWORDS = (
    # Quelle direkt
    "easie", "european agency", "european-agency",
    # Inklusive-Bildungs-Konzept (statistik-fokus, nicht behinderten-spezifik)
    "inklusive bildung", "inklusiver unterricht", "inklusive beschulung",
    "inklusions-quote", "inklusionsquote", "inklusion in der schule",
    "inklusion schule", "inklusion in schulen", "inklusion in regelschule",
    "inklusiv beschult", "regelschul-inklusion", "regelschulinklusion",
    "inclusive education", "inclusive schooling", "inclusive enrolment",
    # 2026-05-23: Schul-/Klassen-Komposita ergänzt
    "inklusive schul", "inklusive klasse", "inklusive klassen",
    "inklusionsrate", "inklusions-rate", "inclusion rate",
    # Sonderpädagogik + SEN
    "sonderpädagogisch", "sonderpädagogischer förderbedarf", "spf",
    "sonderschule", "sonderschulen", "sonderschul-quote",
    "sonderschul-pflicht", "sonderschulpflicht",
    "sonderpädagogische förderung", "sonderpaedagogik",
    "special educational needs", "special needs education",
    "sen-quote", "sen quote", "sen rate", "sen-rate",
    "förderschule", "förderschulen", "förderschulquot", "förderquote",
    "förderschwerpunkt", "förder-schwerpunkt", "förderbedarf",
    "foerderbedarf", "foerderschule", "foerderschulen", "foerderschulquot",
    "separate beschulung", "separate setting", "separate settings",
    # Statistik-Marker (wenn mit Inklusions-Kontext)
    "isced 1", "isced 2", "isced-1", "isced-2",
    # AT/DE-spezifische Marker
    "integrationsklasse", "integrationsklassen",
    "i-klasse", "i-klassen",
    "förderzentrum", "förderzentren",
)

# Politik-sensitive Marker, die NICHT triggern dürfen (Wertungs-Claims).
# Wenn nur einer dieser Terms ohne deskriptiven Inklusions-Term vorkommt,
# unterbinden wir den Trigger.
_NORMATIVE_BLOCKLIST = (
    "rückständig", "rueckstaendig", "fortschrittlich", "progressiv",
    "vorbild-land", "vorbildland", "vorbild land",
)


@lru_cache(maxsize=2048)
def claim_mentions_easie_cached(claim: str) -> bool:
    """LRU-gecachter Trigger-Check für die main.py-Pipeline.

    True wenn:
      1. Claim enthält EASIE-/Inklusions-/SEN-Keyword (siehe _EASIE_KEYWORDS).
      2. Claim ist KEIN reiner Wertungs-/Normativ-Claim (siehe Blocklist).
    """
    if not claim:
        return False
    claim_lc = claim.lower()
    if not any(kw in claim_lc for kw in _EASIE_KEYWORDS):
        return False
    # Wenn ein normativer Marker auftaucht ohne deskriptiven Anker (Quote,
    # Prozent, Zahl, Vergleich), blocken wir. Wertungs-Aussagen wie
    # "AT ist rückständig in Inklusion" wollen wir nicht beantworten.
    has_normative = any(t in claim_lc for t in _NORMATIVE_BLOCKLIST)
    if has_normative:
        has_descriptive_anchor = any(t in claim_lc for t in (
            "%", "prozent", "quote", "anteil",
            "rate", "ratio", "zahl", "anzahl", "statistik",
            "wie hoch", "wie viele", "wie viel",
        ))
        if not has_descriptive_anchor:
            return False
    return True


def _load_data() -> dict | None:
    """Lade JSON mit Hot-Reload-Awareness (mtime-aware)."""
    return load_json_mtime_aware(STATIC_JSON_PATH)


def _detect_countries(claim_lc: str, data: dict) -> list[str]:
    """Erkenne ISO3-Codes der im Claim genannten Länder."""
    aliases = data.get("country_aliases") or {}
    found: list[str] = []
    for iso3, alias_list in aliases.items():
        for alias in alias_list:
            if not alias:
                continue
            if alias.lower() in claim_lc:
                if iso3 not in found:
                    found.append(iso3)
                break
    return found


def _fmt_pct(value) -> str:
    """Formatiere Prozent-Wert mit Komma als deutsches Dezimaltrennzeichen."""
    if value is None:
        return "—"
    try:
        return f"{float(value):.1f}".replace(".", ",") + "%"
    except (TypeError, ValueError):
        return "—"


def _country_short(iso3: str, rating: dict) -> str:
    """Kompakte Country-Zeile: 'AT 60,8% inklusiv (4,6% SEN)'."""
    iso2 = _ISO3_TO_ISO2.get(iso3, iso3[:2])
    incl = _fmt_pct(rating.get("inclusive_enrolment_pct"))
    sen = _fmt_pct(rating.get("identified_sen_rate_pct"))
    return f"{iso2} {incl} inkl. ({sen} SEN)"


def _select_display_countries(
    requested: list[str],
    country_data: dict,
    primary: str,
) -> list[str]:
    """Wähle bis zu MAX_DISPLAY_REFERENCE_COUNTRIES Länder für vs.-Vergleich.

    Strategie: erst alle aus dem Claim genannten (außer primary), dann
    Reference-Pool auffüllen.
    """
    selected: list[str] = []
    for c in requested:
        if c == primary:
            continue
        if c in country_data and c not in selected:
            selected.append(c)
        if len(selected) >= MAX_DISPLAY_REFERENCE_COUNTRIES:
            return selected
    for c in _DISPLAY_REFERENCE_COUNTRIES:
        if c == primary:
            continue
        if c in country_data and c not in selected:
            selected.append(c)
        if len(selected) >= MAX_DISPLAY_REFERENCE_COUNTRIES:
            return selected
    return selected


def _build_country_result(
    iso3: str,
    rating: dict,
    display_refs: list[str],
    country_data: dict,
    data: dict,
) -> dict | None:
    """Baue einen Result-Eintrag pro Land."""
    if not rating:
        return None
    iso2 = _ISO3_TO_ISO2.get(iso3, iso3[:2])
    name = rating.get("country_name") or iso3
    incl = _fmt_pct(rating.get("inclusive_enrolment_pct"))
    sep = _fmt_pct(rating.get("separate_settings_pct"))
    sen = _fmt_pct(rating.get("identified_sen_rate_pct"))

    head = (
        f"{iso2} {incl} inklusiv beschult ({sep} separat), "
        f"{sen} SEN-Identifikation"
    )

    # vs.-Vergleich mit anderen Ländern
    ref_parts: list[str] = []
    for ref_iso3 in display_refs:
        ref_rating = country_data.get(ref_iso3) or {}
        if ref_rating:
            ref_iso2 = _ISO3_TO_ISO2.get(ref_iso3, ref_iso3[:2])
            ref_incl = _fmt_pct(ref_rating.get("inclusive_enrolment_pct"))
            ref_parts.append(f"{ref_iso2} {ref_incl}")
    ref = " — vs. " + ", ".join(ref_parts) if ref_parts else ""

    ref_year = data.get("reference_school_year") or "2018/2019"
    display_value = f"{head}{ref} (EASIE {ref_year})"

    country_note = rating.get("note") or ""
    methodology_short = (
        data.get("methodology_note")
        or "EASIE erhebt SEN-Quoten + Inklusions-/Separations-Anteile in 35 EU/EFTA-Ländern."
    )

    # Wichtig: SEN-Definitions-Caveat in jeder description.
    sen_def_caveat = (
        "Vergleichs-Caveat: nationale SEN-Definitionen variieren stark — "
        "höhere Identifikations-Rate ≠ schlechteres System (z.B. IS, EE, "
        "FI definieren bewusst breit, um niedrigschwellige Förderung "
        "zu ermöglichen)."
    )
    license_caveat = (
        "Lizenz: CC BY-NC-ND 4.0 (European Agency). "
        "Quote mit Quellenangabe, keine Bearbeitung, nicht-kommerziell."
    )
    description_parts = [
        f"Daten EASIE-Cross-Country-Report {ref_year}.",
        methodology_short[:300],
    ]
    if country_note:
        description_parts.append(f"Länder-Hinweis: {country_note}")
    description_parts.append(sen_def_caveat)
    description_parts.append(license_caveat)
    description = " — ".join(p for p in description_parts if p)[:1200]

    indicator_name = (
        f"EASIE {ref_year} — {name}: "
        f"{incl} inklusiv beschult, {sen} SEN-Identifikation"
    )

    return {
        "indicator_name": indicator_name[:200],
        "indicator": "easie_inclusive_enrolment",
        "country": iso2,
        "country_name": name,
        "year": str(ref_year),
        "topic": "inclusive_education_stats",
        "value": rating.get("inclusive_enrolment_pct"),
        "display_value": display_value[:480],
        "description": description,
        "url": data.get("source_url") or "https://www.european-agency.org/data/easie",
        "secondary_url": data.get("secondary_url")
            or "https://www.european-agency.org/data/cross-country-reports",
        "source": data.get("source_label")
            or "EASIE European Agency Statistics on Inclusive Education",
    }


def _build_eu_average_result(data: dict) -> dict | None:
    """EU-Durchschnitt als Fallback, wenn kein Land gematcht wurde."""
    avg = data.get("eu_average") or {}
    if not avg:
        return None
    ref_year = data.get("reference_school_year") or "2018/2019"
    incl = _fmt_pct(avg.get("inclusive_enrolment_pct_avg"))
    sep = _fmt_pct(avg.get("separate_settings_pct_avg"))
    sen = _fmt_pct(avg.get("identified_sen_rate_pct_avg"))
    note = avg.get("note") or ""
    description = (
        f"Ungewichteter EU/EFTA-Durchschnitt aus EASIE-Cross-Country-Report "
        f"{ref_year}. {note} "
        "Vergleichs-Caveat: nationale SEN-Definitionen variieren stark. "
        "Lizenz: CC BY-NC-ND 4.0 (European Agency)."
    )
    return {
        "indicator_name": (
            f"EASIE {ref_year} — EU/EFTA-Durchschnitt: "
            f"{incl} inklusiv beschult"
        ),
        "indicator": "easie_eu_average",
        "country": "EU",
        "country_name": "EU/EFTA (35 Länder)",
        "year": str(ref_year),
        "topic": "inclusive_education_stats",
        "value": avg.get("inclusive_enrolment_pct_avg"),
        "display_value": (
            f"EU/EFTA-Durchschnitt {incl} inklusiv ({sep} separat), "
            f"{sen} SEN-Identifikation — Spannweite extrem groß "
            "(z.B. 44% DE bis 99% IT/PT/SE) (EASIE "
            f"{ref_year})"
        )[:480],
        "description": description[:1200],
        "url": data.get("source_url") or "https://www.european-agency.org/data/easie",
        "source": data.get("source_label")
            or "EASIE European Agency Statistics on Inclusive Education",
    }


async def search_easie(analysis: dict) -> dict:
    """Live-Lookup gegen den EASIE-Static-Cache.

    Returns Dict mit Standard-Schema:
      {
        "source": "EASIE European Agency Statistics on Inclusive Education",
        "type": "inclusive_education_stats",
        "results": [...],   # max 5 (3 primary countries + 1-2 reference/EU-avg)
      }
    """
    empty = {
        "source": "EASIE European Agency Statistics on Inclusive Education",
        "type": "inclusive_education_stats",
        "results": [],
    }

    if not analysis:
        return empty
    claim = (
        analysis.get("claim")
        or analysis.get("original_claim")
        or analysis.get("text")
        or ""
    )
    if not isinstance(claim, str):
        claim = str(claim or "")
    original = analysis.get("original_claim") or claim
    if not isinstance(original, str):
        original = str(original or "")
    combined = f"{original} {claim}".strip()
    if not combined:
        return empty
    combined_lc = combined.lower()

    if not claim_mentions_easie_cached(combined):
        return empty

    data = _load_data()
    if not data:
        logger.warning("easie: static JSON konnte nicht geladen werden")
        return empty

    country_data = data.get("country_data") or {}
    if not country_data:
        return empty

    # Country-Detection im Claim selbst
    requested = _detect_countries(combined_lc, data)

    # Plus Entity-Liste aus analysis
    entities = (analysis.get("entities") or [])
    if entities:
        ents_lc = " ".join(str(e).lower() for e in entities)
        for c in _detect_countries(ents_lc, data):
            if c not in requested:
                requested.append(c)

    # Wenn nichts erkannt → DACH-Default.
    if not requested:
        requested = list(_DEFAULT_COUNTRIES_FOR_DACH_CLAIMS)

    # Begrenze auf MAX_PRIMARY_COUNTRIES — wir liefern max. 3 Länder-Karten.
    primary_list = [c for c in requested if c in country_data][:MAX_PRIMARY_COUNTRIES]
    if not primary_list:
        # User hat zwar Inklusions-Vokabular + (vermutlich) Country-Namen
        # genannt, aber wir haben keine Daten dazu → EU-Average ausspielen.
        avg_result = _build_eu_average_result(data)
        if avg_result:
            return {**empty, "results": [avg_result]}
        return empty

    results: list[dict] = []
    for iso3 in primary_list:
        rating = country_data.get(iso3) or {}
        if not rating:
            continue
        display_refs = _select_display_countries(requested, country_data, iso3)
        row = _build_country_result(iso3, rating, display_refs, country_data, data)
        if row:
            results.append(row)

    # Ranking-Claim: wenn nach "höchste/niedrigste/vergleich/ranking"
    # gefragt wird, EU-Ranking-Benchmark mitliefern.
    is_ranking = any(t in combined_lc for t in (
        "höchste", "hoechste", "niedrigste", "ranking", "vergleich",
        "meisten", "wenigsten", "spitze", "spitzenreiter",
        "förderschulquote", "foerderschulquote", "segregation",
        "sonderschulquote", "exklusion",
    ))
    if is_ranking and len(results) < 5:
        # Ranking der separate_settings_pct: top-5 höchste
        ranked = sorted(
            [(iso3, cd.get("separate_settings_pct", 0), cd.get("country_name", iso3))
             for iso3, cd in country_data.items()
             if cd.get("separate_settings_pct") is not None],
            key=lambda x: x[1], reverse=True,
        )
        if ranked:
            ref_year = data.get("reference_school_year") or "2018/2019"
            top5 = ", ".join(
                f"{r[2]} {r[1]:.1f}%".replace(".", ",")
                for r in ranked[:5]
            )
            bottom3 = ", ".join(
                f"{r[2]} {r[1]:.1f}%".replace(".", ",")
                for r in ranked[-3:]
            )
            results.append({
                "indicator_name": f"EASIE {ref_year} — EU-Ranking Separate Settings",
                "indicator": "easie_separate_ranking",
                "country": "EU",
                "country_name": "EU/EFTA Ranking",
                "year": str(ref_year),
                "topic": "inclusive_education_stats",
                "value": None,
                "display_value": (
                    f"EASIE {ref_year} EU-Ranking Foerderschul-/Sonderschulquote "
                    f"(separate settings): Hoechste Quoten: {top5}. "
                    f"Niedrigste: {bottom3}. "
                    f"Deutschland hat mit {ranked[0][1] if ranked[0][0] == 'DEU' else [r[1] for r in ranked if r[0] == 'DEU'][0] if any(r[0] == 'DEU' for r in ranked) else '?'}% "
                    f"eine der hoechsten Separationsraten in Europa."
                )[:480],
                "description": (
                    f"Ranking basiert auf EASIE separate_settings_pct "
                    f"(Anteil SEN-Schueler in separaten Sonderschulen/Klassen). "
                    f"Vergleichs-Caveat: nationale SEN-Definitionen variieren. "
                    f"Lizenz: CC BY-NC-ND 4.0."
                ),
                "url": data.get("source_url") or "https://www.european-agency.org/data/easie",
                "source": data.get("source_label")
                    or "EASIE European Agency Statistics on Inclusive Education",
            })

    # Wenn weniger als MAX_PRIMARY_COUNTRIES Treffer und der Claim
    # generischen EU-Bezug hat → EU-Average als zusätzlicher Treffer.
    if len(results) < 5 and any(t in combined_lc for t in (
        "eu", "europa", "europe", "europäisch", "european",
        "vergleich", "durchschnitt", "average",
    )):
        avg = _build_eu_average_result(data)
        if avg:
            results.append(avg)

    if not results:
        return empty

    logger.info(
        f"easie: {len(results)} Treffer "
        f"(primary={primary_list}, requested={requested[:5]})"
    )

    return {
        "source": "EASIE European Agency Statistics on Inclusive Education",
        "type": "inclusive_education_stats",
        "results": results[:5],
    }
