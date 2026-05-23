"""ETER — European Tertiary Education Register.

Datenquelle: ETER (https://www.eter-project.com) — das einzige paneuropaeische
Mikrodaten-Register fuer Hochschulen (Higher Education Institutions, HEIs).
~3.500 Hochschulen aus 38 Laendern (EU + EWR + Westbalkan + Tuerkei + UK +
Schweiz). Jaehrlich aktualisiert. Felder pro HEI: Name (lokal + englisch),
Land/Stadt, Gruendungsjahr, Rechtsstatus, ISCED-Levels, Total students
enrolled, Academic staff (Headcount), Forschungs-Aktivitaet, PhD-Recht.

Datenzugang: ETER bietet Bulk-CSV-Download (jaehrlicher Release) plus
ein Daten-Portal (`https://www.eter-project.com/data-for-research`).
**Wir verwenden den Static-Snapshot-Pfad** — der CSV-Bulk-Download wird
periodisch (1x/Jahr nach ETER-Release) lokal als JSON serialisiert und
ueber `data/eter.json` geladen. Begruendung:
1. ETER-Daten werden jaehrlich publiziert (kein Mehrwert durch Live-Calls).
2. Bulk-CSV ist die offizielle Distributionsform; ein direkter REST-Endpunkt
   ohne Auth ist nicht garantiert stabil.
3. Snapshot ermoeglicht reproduzierbare Faktenchecks (kein Inkonsistenz-
   Risiko zwischen User-Session und ETER-Live-Stand).

Lizenz: ETER-Daten sind unter **CC BY 4.0** lizenziert
(siehe https://www.eter-project.com — Disclaimer & Terms).
Repackaging als Static-Snapshot ist erlaubt; Namensnennung erfolgt
in jedem Result-Set (`source_label` + `url`).

Komplementaer zu existierenden Quellen:
- eric.py: Bildungsforschungs-Studien (kein institutionelles Mikrodaten-
  Register).
- unesco_uis.py: SDG-4-Bildungsdaten weltweit, aber AGGREGIERT auf Landes-
  ebene — NICHT pro Hochschule.
- oecd.py: PISA / OECD-Vergleichsdaten — schulisch + tertiaer, aggregiert.
- bildung_pack.py: kuratierter DACH-Forschungsstand zu Schul-Bildungs-
  themen, KEINE Hochschul-Microdata.
- ETER: das einzige institutionelle EU-weit-vergleichbare Hochschul-
  Register.

Politische Guardrails (siehe project_political_guardrails.md):
ETER macht KEINE Rankings (das ist im Memo des ETER-Steering-Committees
explizit festgehalten) — der Service zitiert nur deskriptive Eckwerte
(Studierende, Personal, Gruendungsjahr). KEINE eigene Bewertung
"Top-Universitaet" / "Eliten-Hochschule" / "schwach forschend". Der
Synthesizer-Layer entscheidet, ob die Daten eine Aussage stuetzen
oder nicht.

# WIRING fuer main.py:
# from services.eter import search_eter, claim_mentions_eter_cached
# if claim_mentions_eter_cached(claim):
#     tasks.append(cached("ETER", search_eter, analysis))
#     queried_names.append("ETER")
"""

from __future__ import annotations

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "eter.json",
)


# ---------------------------------------------------------------------------
# Descriptor fuer Reranker-Backup
# ---------------------------------------------------------------------------
def _descriptor(f: dict) -> tuple[dict, str]:
    """Repraesentation fuer den Cosine-Backup-Fall.

    Headline + Top-2 Context-Notes liefern eine kompakte Beschreibung,
    die der Reranker semantisch matchen kann.
    """
    head = f.get("headline", "")
    scope = f.get("scope", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {scope}. {notes}"[:300])


def _claim_matches_facts(
    claim_lc: str, full_claim: str | None = None,
) -> list[dict]:
    """Substring/Composite-Match (preferred) + Reranker-Backup-Fallback."""
    return find_matching_items(
        STATIC_JSON_PATH, "facts",
        claim_lc=claim_lc, full_claim=full_claim,
        descriptor_fn=_descriptor,
    )


def claim_mentions_eter_cached(claim: str) -> bool:
    """Trigger-Check: wird ETER fuer diesen Claim aktiviert?

    Trigger-Themen (siehe data/eter.json `trigger_keywords` + `trigger_composite`):
    - Hochschul-Eigennamen (Univ. Wien, TU Wien, LMU Muenchen, ETH Zuerich, ...)
    - Hochschul-Kontext-Begriffe (Studierende, Studenten, Hochschule, Universitaet,
      Fachhochschule, Paedagogische Hochschule, Tertiaerbildung, Forschungs-Output)
    - ETER-Eigennamen (ETER, Bologna-System, European Tertiary Education Register)
    """
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


# ---------------------------------------------------------------------------
# Optionale Snapshot-Loader-Funktion (fuer data_updater + Tests)
# ---------------------------------------------------------------------------
async def fetch_eter(client=None):
    """Snapshot-Loader — gibt alle Facts zurueck (fuer data_updater-Prefetch).

    Signatur identisch zu anderen Static-Services (housing_at, education_dach).
    """
    return load_items(STATIC_JSON_PATH, "facts")


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _format_students(n) -> str:
    """Formatiert Studierendenzahlen mit deutschen Tausender-Punkten."""
    try:
        return f"{int(n):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "—"


def _build_hei_display(fact: dict) -> str:
    """Display-Value fuer einen Einzel-HEI-Fakt (Universitaet/TU/FH)."""
    d = fact.get("data") or {}
    name = d.get("name_local") or d.get("name_english") or fact.get("scope", "?")
    country = d.get("country_code") or fact.get("country", "—")
    year = d.get("students_year") or fact.get("year") or "—"
    students = _format_students(d.get("students_total"))
    staff = _format_students(d.get("academic_staff_headcount"))
    founded = d.get("foundation_year")
    legal = d.get("legal_status")

    header = f"ETER {year} ({country}) — {name}"
    parts: list[str] = []
    if students != "—":
        parts.append(f"{students} Studierende")
    if staff != "—":
        parts.append(f"{staff} wissenschaftliche Mitarbeitende")
    if founded:
        parts.append(f"gegruendet {founded}")
    if legal:
        parts.append(f"Rechtsform {legal}")

    if not parts:
        return f"{header}."
    return f"{header}: " + ", ".join(parts) + "."


def _build_overview_display(fact: dict) -> str:
    """Display-Value fuer Landes-/EU-Overview-Fakten."""
    d = fact.get("data") or {}
    country = fact.get("country", "—")
    year = fact.get("year") or "—"

    if country == "AT":
        return (
            f"ETER {year} Oesterreich: "
            f"{_format_students(d.get('hei_count_at_2021'))} Hochschulen "
            f"(davon {d.get('public_universities_at_count')} oeffentliche "
            f"Universitaeten, {d.get('fachhochschulen_at_count')} FHs, "
            f"{d.get('paedagogische_hochschulen_at_count')} Paedagogische "
            f"Hochschulen, {d.get('private_universities_at_count')} "
            f"Privatunis). Insgesamt "
            f"{_format_students(d.get('students_total_at_2021'))} "
            f"Studierende, {_format_students(d.get('academic_staff_total_at_2021'))} "
            f"wissenschaftliches Personal. "
            f"Frauenanteil Studierende: "
            f"{d.get('share_female_students_at_pct')} %, "
            f"internationale Studierende: "
            f"{d.get('share_international_students_at_pct')} %."
        )
    if country == "DE":
        return (
            f"ETER {year} Deutschland: "
            f"{_format_students(d.get('hei_count_de_2021'))} Hochschulen "
            f"(davon {d.get('public_universities_de_count')} oeffentliche "
            f"Universitaeten, {d.get('fachhochschulen_de_count')} FHs, "
            f"{d.get('private_universities_de_count')} private Hochschulen). "
            f"Insgesamt {_format_students(d.get('students_total_de_2021'))} "
            f"Studierende, {_format_students(d.get('academic_staff_total_de_2021'))} "
            f"wissenschaftliches Personal. "
            f"Frauenanteil: {d.get('share_female_students_de_pct')} %, "
            f"internationale Studierende: "
            f"{d.get('share_international_students_de_pct')} %."
        )
    if country == "EU":
        return (
            f"ETER {year} Europa-Gesamt: "
            f"{_format_students(d.get('hei_count_eu_total_2021'))} Hochschulen "
            f"in {d.get('countries_covered_2021')} Laendern. "
            f"Insgesamt "
            f"{_format_students(d.get('students_total_eu_2021'))} "
            f"Studierende und "
            f"{_format_students(d.get('academic_staff_total_eu_2021'))} "
            f"wissenschaftliches Personal. "
            f"{d.get('share_phd_awarding_pct')} % der Hochschulen "
            f"verleihen PhD-Grade; "
            f"{d.get('share_research_active_pct')} % gelten als "
            f"forschungsaktiv. Bologna-System: "
            f"{d.get('bologna_signatory_share_pct')} % aller HEIs."
        )
    return fact.get("headline", "?")


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_eter(analysis: dict) -> dict:
    """Static-Lookup gegen das ETER-Snapshot fuer Hochschul-Claims.

    Strategie:
    1. Trigger-Check via Substring/Composite (data/eter.json definiert Trigger).
    2. Falls keine direkten Treffer: Reranker-Backup (top-3 mit Cosine >= 0.45).
    3. Pro Match: einen Result-Eintrag im Evidora-Standard-Schema bauen.

    Politische Guardrails: NUR deskriptive Zahlen. KEINE Rankings,
    KEINE Bewertung. Synthesizer-Layer entscheidet.
    """
    empty = {
        "source": "ETER European Tertiary Education Register (CC-BY 4.0)",
        "type": "higher_education_register",
        "results": [],
    }

    analysis = analysis or {}
    claim = (
        analysis.get("original_claim")
        or analysis.get("claim", "")
        or ""
    )
    if not isinstance(claim, str):
        claim = str(claim or "")

    matches = _claim_matches_facts(claim.lower(), full_claim=claim)
    if not matches:
        return empty

    results: list[dict] = []
    for fact in matches:
        topic = fact.get("topic", "")
        d = fact.get("data") or {}
        url = fact.get("source_url") or "https://www.eter-project.com"
        secondary = fact.get("secondary_url")
        label = fact.get(
            "source_label",
            "ETER European Tertiary Education Register (CC BY 4.0)",
        )
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))
        country = d.get("country_code") or fact.get("country", "—")

        # Display-Text je nach Topic-Typ
        if topic in ("overview_at", "overview_de", "overview_eu"):
            display = _build_overview_display(fact)
        else:
            display = _build_hei_display(fact)

        # Indicator-Name: ETER {Country} {Year} — {Headline}
        indicator_name = (
            f"ETER {country} {year}: {fact.get('headline', topic or '?')}"
        )[:300]

        results.append({
            "indicator_name": indicator_name,
            "indicator": f"eter_{topic}" if topic else "eter_fact",
            "country": country,
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": notes_joined,
            "url": url,
            "secondary_url": secondary,
            "source": label,
        })

    # Hard-Cap: max 5 Treffer
    results = results[:5]

    logger.info(
        f"ETER: {len(results)} Treffer fuer Claim '{claim[:60]}'"
    )
    return {
        "source": "ETER European Tertiary Education Register (CC-BY 4.0)",
        "type": "higher_education_register",
        "results": results,
    }
