"""AT Factbook — kuratierte österreichische Faktoide aus offiziellen
Primärquellen, für die kein eigener Online-Service existiert oder die
Datenlage zu fragmentiert für ein generisches Statistik-API ist.

Warum dieser Service?
---------------------
Wir haben in mehreren Verifikations-Runden Claims gesehen, deren Daten
bei offiziellen AT-Stellen (Bildungsdirektion Wien, BMF Förderungs-
bericht) öffentlich verfügbar sind, aber:

- Bildungsdirektion Wien publiziert die Religions-Erhebung als PDF /
  Pressemeldung — kein API.
- BMF Förderungsbericht ist ein jährlicher PDF-Bericht an den
  Nationalrat — kein API.
- Statistik Austria ESVG-Förderquote ist ein Tabellen-Auszug aus den
  VGR — theoretisch via STATcube, aber für eine einzelne Kennzahl
  overkill.

Lösung: **Static-Curated JSON.**  Wir pflegen die für AT-politische
Debatten häufig wiederkehrenden Kennzahlen manuell in
``data/at_factbook.json``.  Refresh-Cadence: einmal pro Jahr (BMF-
Bericht erscheint Q1, Bildungsdirektions-Erhebung Q4).

v1-Themen:
- ``religion_schools_vienna`` — Religionsbekenntnisse Wiener Pflicht-
  schulen (Stichtagserhebung Bildungsdirektion).
- ``federal_subsidies_austria`` — Bundesförderungen 2019-2024
  (BMF-Förderungsbericht + Statistik Austria ESVG-Quote).

GUARDRAILS (siehe project_political_guardrails.md):
- Wir geben keine Bewertung der Daten ab (keine "zu hoch / zu niedrig").
- Wir liefern explizit den Kontext mit (z.B. "Pflichtschulen Wien" ≠
  "alle Schulen Wien" ≠ "alle Schulen Österreich"), damit die Synthese
  Behauptungen nicht mit irreführendem Geltungsbereich bestätigt.
- Bei zeitreihen-basierten Claims liefern wir stets Krisenjahre + Vor-
  Krisen-Niveau mit, damit selektive Basis-/Endjahr-Wahl auffällt.
"""

import json
import logging
import os

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "at_factbook.json",
)

_cache: dict | None = None


# ---------------------------------------------------------------------------
# AT-Kontext (geteilt mit anderen AT-Services)
# ---------------------------------------------------------------------------
_AT_CONTEXT_TERMS = (
    "österreich", "austria", "österreichisch",
    "republik österreich", "wien", "vienna",
    "bundeskanzler", "bundesregierung",
    "bmf", "bundesministerium für finanzen",
    "bildungsdirektion",
    "burgenland", "kärnten", "niederösterreich", "oberösterreich",
    "salzburg", "steiermark", "tirol", "vorarlberg",
    "fpö", "övp", "spö", "neos", "grüne",
)


def _has_at_context(claim_lc: str) -> bool:
    return any(t in claim_lc for t in _AT_CONTEXT_TERMS)


# ---------------------------------------------------------------------------
# Topic 1: Religionsbekenntnisse Wiener Pflichtschulen
# ---------------------------------------------------------------------------
# Trigger erfordert (Religions-Vokabel + Schul-Vokabel + Wien) ODER
# (eine eindeutig Religions+Schule-Kombination wie "muslimische Schüler").
_RELIGION_TERMS = (
    "religion", "religiös", "konfession", "bekenntnis", "glaube",
    "muslim", "muslimisch", "islam", "islamisch",
    "christ", "christlich", "katholisch", "katholik",
    "orthodox", "evangelisch", "protestant",
    "jüdisch", "judentum",
    "ohne bekenntnis", "konfessionslos",
)
_SCHOOL_TERMS = (
    "schule", "schul", "schüler", "schülerinnen", "schulkind",
    "pflichtschul", "volksschul", "mittelschul", "gymnasium",
    "polytechnisch", "klasse", "klassen", "schulkinder",
    "schulkindern",
)
# Wien-Kontext eng: nur "wien" oder "wiener". Nicht "wienerwald" o.ä.
_WIEN_TERMS = ("wien ", " wien", "wien.", "wien,", "wien:", "wien?",
               "wien!", "wiener", "vienna")


def _claim_mentions_religion_schools_vienna(claim_lc: str) -> bool:
    has_relig = any(t in claim_lc for t in _RELIGION_TERMS)
    has_school = any(t in claim_lc for t in _SCHOOL_TERMS)
    has_wien = any(t in claim_lc for t in _WIEN_TERMS) or claim_lc.startswith("wien")
    return has_relig and has_school and has_wien


# ---------------------------------------------------------------------------
# Topic 2: Bundesförderungen Österreich
# ---------------------------------------------------------------------------
_SUBSIDY_TERMS = (
    "förderung", "förderungen", "förderquote",
    "subvention", "subventionen",
    "zuschuss", "zuschüsse",
    "transparenzdatenbank",
    "förderungsbericht",
    "esvg", "esa-2010",
    "subsidies", "subsidy",
)
# Bundesebene-Hint: dass es um Bundesförderungen geht, nicht
# Landesförderungen / Forschungsförderung / EU-Förderung.
_FEDERAL_HINTS = (
    "bund", "bundesförderung", "bundesregierung", "bmf",
    "republik österreich", "in österreich", "österreich",
    "austria",
    "bund + länder", "alle ebenen",
)


def _claim_mentions_federal_subsidies(claim_lc: str) -> bool:
    has_subsidy = any(t in claim_lc for t in _SUBSIDY_TERMS)
    has_federal = any(t in claim_lc for t in _FEDERAL_HINTS)
    return has_subsidy and has_federal


# ---------------------------------------------------------------------------
# Topic 3: Mindestsicherung / Sozialhilfe-Höchstsätze
# ---------------------------------------------------------------------------
_SOCIAL_ASSIST_TERMS = (
    "mindestsicherung", "sozialhilfe", "ms-bezug", "bezug sozialhilfe",
    "sozialhilfeempfänger", "sozialhilfeempfaenger",
    "bedarfsorientierte mindestsicherung", "bms",
    "social assistance austria",
)


def _claim_mentions_social_assistance(claim_lc: str) -> bool:
    has_social = any(t in claim_lc for t in _SOCIAL_ASSIST_TERMS)
    if not has_social:
        return False
    # "Mindestsicherung" und "BMS" sind AT-spezifische Termini (DE hat
    # Bürgergeld, kein "Mindestsicherung"). Wenn diese explizit genannt
    # werden, zählt das selbst als AT-Kontext.
    if any(at_specific in claim_lc for at_specific in (
        "mindestsicherung", "bedarfsorientierte mindestsicherung", "bms",
        "sozialhilfe-grundsatzgesetz",
    )):
        return True
    return _has_at_context(claim_lc)


# ---------------------------------------------------------------------------
# Topic 4: Pensionsanpassung
# ---------------------------------------------------------------------------
_PENSION_TERMS = (
    "pensionserhöhung", "pensionserhoehung",
    "pensionsanpassung",
    "pensionen erhöht", "pensionen erhoeht",
    "pensionen werden", "anpassungsfaktor",
    "ausgleichszulage", "ausgleichszulagen-richtsatz",
    "mindestpension",
    "luxus-pension", "luxuspension",
    "pension increase austria", "pension adjustment austria",
)
# Composite-Pattern: "pension*" UND ("erhöh*" ODER "anpass*" ODER Prozentzahl)
# fängt Phrasings wie "Die Pensionen werden 2026 um 2,7 % erhöht" ab.
_PENSION_NOUNS = ("pension", "pensionen", "rente", "renten")
_PENSION_VERBS = ("erhöh", "erhoeh", "anpass", "steigen", "gestiegen",
                   "angehoben", "anhebung")


def _claim_mentions_pension_adjustment(claim_lc: str) -> bool:
    import re as _re
    has_pension = any(t in claim_lc for t in _PENSION_TERMS)
    has_noun = any(n in claim_lc for n in _PENSION_NOUNS)
    has_verb = any(v in claim_lc for v in _PENSION_VERBS)
    has_year = bool(_re.search(r"\b202[5-9]\b", claim_lc))
    has_pct = bool(_re.search(r"\d+(?:[,.]\d+)?\s*(?:%|prozent)", claim_lc))

    # 1. Pension-Vokabular vorhanden?
    if not has_pension and not (has_noun and has_verb):
        return False

    # 2. AT-spezifische Acronyme/Termini → automatisch AT
    if any(at_specific in claim_lc for at_specific in (
        "pensionsanpassung", "anpassungsfaktor",
        "ausgleichszulage", "ausgleichszulagen-richtsatz",
        "luxus-pension", "luxuspension",
    )):
        return True

    # 3. High-specificity policy claim (Pensionen/Renten + Verb + Jahr 2025+ + %)
    #    → assume AT (Krone-Kontext / typischer Lehrer-Use-Case)
    if has_noun and has_verb and has_year and has_pct:
        # DE-Marker als Hard-Exclude (z.B. "renten in deutschland")
        de_markers = ("deutschland", "germany", "deutsch", "berlin", "bundestag",
                       "deutsche rentenversicherung", "drv")
        if any(de in claim_lc for de in de_markers):
            return False
        return True

    # 4. Default: explizit AT-Kontext erforderlich
    return _has_at_context(claim_lc)
    # AT-spezifische Begriffe gelten selbst als AT-Kontext (DE hat
    # "Rentenanpassung" + "Eckrentner", AT hat "Pensionsanpassung" +
    # "Anpassungsfaktor" + "Ausgleichszulage").
    if any(at_specific in claim_lc for at_specific in (
        "pensionsanpassung", "anpassungsfaktor",
        "ausgleichszulage", "ausgleichszulagen-richtsatz",
        "luxus-pension", "luxuspension",
    )):
        return True
    return _has_at_context(claim_lc)


# ---------------------------------------------------------------------------
# Public trigger
# ---------------------------------------------------------------------------
def _claim_matches_any_topic(claim: str) -> list[str]:
    """Returns list of topic-ids the claim matches.  Empty list = no match."""
    if not claim:
        return []
    cl = claim.lower()
    matched: list[str] = []
    if _claim_mentions_religion_schools_vienna(cl):
        matched.append("religion_schools_vienna")
    if _claim_mentions_federal_subsidies(cl):
        matched.append("federal_subsidies_austria")
    if _claim_mentions_social_assistance(cl):
        matched.append("social_assistance_austria")
    if _claim_mentions_pension_adjustment(cl):
        matched.append("pension_adjustment_austria")
    return matched


def claim_mentions_factbook_cached(claim: str) -> bool:
    """Synchronous gate for the request hot path."""
    return bool(_claim_matches_any_topic(claim))


# ---------------------------------------------------------------------------
# Static load
# ---------------------------------------------------------------------------
def _load_static_json() -> dict | None:
    """Load the curated factbook JSON from disk (with one-shot caching)."""
    global _cache
    if _cache is not None:
        return _cache
    try:
        with open(STATIC_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "facts" not in data:
            logger.warning("at_factbook.json missing 'facts' key")
            return None
        _cache = data
        logger.info(f"AT-Factbook loaded: {len(data['facts'])} curated entries")
        return _cache
    except FileNotFoundError:
        logger.warning(f"at_factbook.json not found at {STATIC_JSON_PATH}")
        return None
    except Exception as e:
        logger.warning(f"at_factbook.json load failed: {e}")
        return None


async def fetch_at_factbook(client=None):
    """Prefetch entry-point — keeps the data_updater interface symmetric
    with the other static-first AT services.  Returns the loaded entries
    so the prefetch logger can report a count."""
    data = _load_static_json()
    if not data:
        return []
    return data.get("facts") or []


# ---------------------------------------------------------------------------
# Result builders — one per topic
# ---------------------------------------------------------------------------
def _build_religion_results(fact: dict) -> list[dict]:
    """Build result entries for Wiener Pflichtschul-Religionsstatistik."""
    data = fact.get("data") or {}
    year = fact.get("year", "")
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "Bildungsdirektion Wien"

    # Helper: format float as German percent ("41,2") — Python's default
    # uses '.' as decimal separator, but AT/DE convention is ','.
    def _de_pct(v) -> str:
        if v is None:
            return "?"
        return f"{v}".replace(".", ",")

    def _de_int(v) -> str:
        if v is None:
            return "?"
        return f"{int(v):,}".replace(",", ".")

    # Hauptzeile: Pflichtschul-Aggregat
    headline = (
        f"Wiener Pflichtschulen {year} — Religionsbekenntnisse "
        f"({_de_int(data.get('schueler_gesamt'))} Schüler:innen): "
        f"islamisch {_de_pct(data.get('islamisch_pct'))} %, "
        f"christlich {_de_pct(data.get('christlich_pct_gesamt'))} % "
        f"(davon röm.-kath. {_de_pct(data.get('roemisch_katholisch_pct'))} %, "
        f"orthodox {_de_pct(data.get('orthodox_pct'))} %, "
        f"evangelisch {_de_pct(data.get('evangelisch_pct'))} %), "
        f"ohne Bekenntnis {_de_pct(data.get('ohne_bekenntnis_pct'))} %."
    )

    description_parts = [
        f"Geltungsbereich: {fact.get('scope', 'Wiener Pflichtschulen')}.",
        "WICHTIG: Diese Zahl gilt für PFLICHTSCHULEN in WIEN — nicht für "
        "alle Wiener Schulen, nicht für alle österreichischen Schulen.",
    ]
    for note in fact.get("context_notes") or []:
        description_parts.append(note)

    main_entry = {
        "indicator_name": f"Religionsbekenntnisse Wiener Pflichtschulen {year}",
        "indicator": "factbook_religion_vienna",
        "country": "AUT",
        "country_name": "Österreich",
        "year": year,
        "value": data.get("islamisch_pct"),
        "display_value": headline,
        "description": " ".join(description_parts),
        "url": src,
        "source": label,
    }

    results = [main_entry]

    # Sub-Breakdowns als Zusatz-Einträge (Volksschulen, Mittelschulen)
    for sub in fact.get("subgroup_breakdowns") or []:
        sub_label = sub.get("label", "")
        sub_parts = []
        if sub.get("islamisch_pct") is not None:
            sub_parts.append(f"islamisch {_de_pct(sub['islamisch_pct'])} %")
        if sub.get("christlich_pct") is not None:
            sub_parts.append(f"christlich {_de_pct(sub['christlich_pct'])} %")
        if sub.get("ohne_bekenntnis_pct") is not None:
            sub_parts.append(f"ohne Bekenntnis {_de_pct(sub['ohne_bekenntnis_pct'])} %")
        sub_display = (
            f"{sub_label}: " + ", ".join(sub_parts)
            if sub_parts else sub_label
        )
        if sub.get("note"):
            sub_display += f" — {sub['note']}"
        results.append({
            "indicator_name": sub_label,
            "indicator": "factbook_religion_vienna_subgroup",
            "country": "AUT",
            "country_name": "Österreich",
            "year": year,
            "display_value": sub_display,
            "description": (
                "Untergruppen-Aufschlüsselung der Wiener Pflichtschul-"
                "Religionsstatistik. Zeigt, dass der muslimische Anteil "
                "innerhalb des Pflichtschul-Sektors variiert."
            ),
            "url": src,
            "source": label,
        })

    return results


def _build_subsidies_results(fact: dict, claim_lc: str) -> list[dict]:
    """Build result entries for Bundesförderungen-Zeitreihe."""
    yearly = fact.get("yearly_data") or []
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "BMF Förderungsbericht"
    comparisons = fact.get("comparisons") or {}

    def _de_num(v) -> str:
        if v is None:
            return "?"
        return f"{v}".replace(".", ",")

    # Hauptzeile: Aktuellster Stand + Vergleich zu Vor-Krisen-Niveau
    latest = yearly[-1] if yearly else {}
    earliest = yearly[0] if yearly else {}
    delta_2019_2024 = comparisons.get("direkt_2019_vs_2024") or {}

    headline_parts = []
    if latest.get("year") and latest.get("direkte_foerderungen_mrd_eur") is not None:
        headline_parts.append(
            f"Direkte Bundesförderungen {latest['year']}: "
            f"{_de_num(latest['direkte_foerderungen_mrd_eur'])} Mrd EUR"
        )
    if latest.get("indirekte_foerderungen_mrd_eur") is not None:
        headline_parts.append(
            f"+ {_de_num(latest['indirekte_foerderungen_mrd_eur'])} Mrd EUR indirekt "
            f"(Steuerermäßigungen)"
        )
    if latest.get("esvg_quote_pct_bip") is not None:
        headline_parts.append(
            f"ESVG-Quote {_de_num(latest['esvg_quote_pct_bip'])} % BIP"
        )
    headline = "; ".join(headline_parts) + "."

    description_parts = [
        f"Zeitreihe direkter Bundesförderungen "
        f"{earliest.get('year', '')}–{latest.get('year', '')}: " +
        " · ".join(
            f"{e['year']}: {_de_num(e.get('direkte_foerderungen_mrd_eur'))} Mrd EUR"
            f"{' (' + e['note'] + ')' if e.get('note') else ''}"
            for e in yearly
        ),
        f"Veränderung 2019→2024: nominal "
        f"+{_de_num(delta_2019_2024.get('delta_pct'))} % "
        f"({_de_num(earliest.get('direkte_foerderungen_mrd_eur'))} → "
        f"{_de_num(latest.get('direkte_foerderungen_mrd_eur'))} Mrd EUR).",
    ]
    for note in fact.get("context_notes") or []:
        description_parts.append(note)

    main_entry = {
        "indicator_name": "Direkte Bundesförderungen Österreich (BMF) — Zeitreihe 2019–2024",
        "indicator": "factbook_subsidies_at",
        "country": "AUT",
        "country_name": "Österreich",
        "year": str(latest.get("year", "")),
        "value": latest.get("direkte_foerderungen_mrd_eur"),
        "display_value": headline,
        "description": " ".join(description_parts),
        "url": src,
        "source": label,
    }

    results = [main_entry]

    # Wenn der Claim "+76 %" o.ä. enthält → den Plus-X-Check als
    # eigenen, autoritativ-bewerteten Eintrag prepend-en.
    plus_check = comparisons.get("claim_plus_76_pct_check") or {}
    has_pct_claim = any(
        s in claim_lc for s in (
            "+76", "76 prozent", "76 %", "76%", "76prozent",
            "plus 76",
        )
    )
    if plus_check and has_pct_claim:
        results.insert(0, {
            "indicator_name": "Plausibilitäts-Check: Förderungen +76 %",
            "indicator": "factbook_subsidies_check",
            "country": "AUT",
            "country_name": "Österreich",
            "year": "2019–2024",
            "display_value": (
                f"Behauptung „{plus_check.get('phrasing', '+76 %')}" +
                "“: " + plus_check.get("verdict", "")
            ),
            "description": (
                "Direkter Vergleich der Behauptung gegen die BMF-/Statistik-"
                "Austria-Daten. Nominal liegt 2019→2024 bei +117 %, real "
                "(VPI-bereinigt) bei rund +78 %. Eine genaue +76 %-Aussage "
                "lässt sich nur durch selektive Wahl von Basis- und Endjahr "
                "konstruieren."
            ),
            "url": src,
            "source": label,
        })

    return results


# ---------------------------------------------------------------------------
# Topic 3: Mindestsicherung / Sozialhilfe
# ---------------------------------------------------------------------------
def _build_social_results(fact: dict, claim_lc: str) -> list[dict]:
    """Result entries for Mindestsicherungs-Höchstsätze."""
    data = fact.get("data") or {}
    year = fact.get("year", 2026)
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "Sozialministerium"
    comparisons = fact.get("comparisons") or {}

    def _de(v) -> str:
        if v is None:
            return "?"
        return f"{v}".replace(".", ",")

    headline = (
        f"Sozialhilfe-Höchstsätze {year}: Alleinstehende max. "
        f"{_de(data.get('alleinstehende_max_eur_pro_monat'))} EUR/Monat, "
        f"Paar max. {_de(data.get('paar_max_eur_pro_monat'))} EUR, "
        f"Kinder-Zuschlag 1./2. Kind je {_de(data.get('kinder_zuschlag_eur_pro_monat_pro_kind_erstes'))} EUR, "
        f"ab 3. Kind je {_de(data.get('kinder_zuschlag_eur_pro_monat_pro_kind_ab_drittem'))} EUR."
    )

    description_parts = [
        f"Sozialhilfe-Grundsatzgesetz (BGBl I 41/2019) — Stand {year}. ",
        f"Anpassung gegenüber Vorjahr ({year-1}: "
        f"{_de(data.get('vorjahr_alleinstehende_2025_eur'))} EUR): "
        f"+{_de(data.get('anpassung_pct'))} %.",
    ]
    for note in fact.get("context_notes") or []:
        description_parts.append(note)

    main = {
        "indicator_name": f"Mindestsicherung/Sozialhilfe-Höchstsätze {year}",
        "indicator": "factbook_social_assistance_at",
        "country": "AUT",
        "country_name": "Österreich",
        "year": str(year),
        "value": data.get("alleinstehende_max_eur_pro_monat"),
        "display_value": headline,
        "description": " ".join(description_parts),
        "url": src,
        "source": label,
    }

    results: list[dict] = [main]

    # Wenn der Claim 9000 EUR / 11 Kinder erwähnt → spezifischer Check
    is_9000_check = (
        any(s in claim_lc for s in ("9000", "9.000", "9 000")) or
        any(s in claim_lc for s in ("11 kinder", "elf kinder", "syrische familie"))
    )
    plus_check = comparisons.get("claim_9000_eur_familie_11_kinder_check") or {}
    if plus_check and is_9000_check:
        results.insert(0, {
            "indicator_name": "Plausibilitäts-Check: 9.000 EUR Sozialhilfe (11 Kinder, Wien)",
            "indicator": "factbook_social_check",
            "country": "AUT",
            "country_name": "Österreich",
            "year": str(year),
            "display_value": (
                "Behauptung 'Familie mit 11 Kindern erhält 9.000 EUR Sozialhilfe in Wien': " +
                plus_check.get("verdict", "")
            ),
            "description": (
                "Rechenweg: " + plus_check.get("rechenweg", "") + " " +
                "Wien-Spezifika: " + plus_check.get("wien_aufstockung", "") + " " +
                "Diese Konstellation existiert real (Krone-Bericht 24.05.2025), "
                "ist aber ein extrem seltener Einzelfall — KEIN repräsentativer "
                "Wert für 'Sozialhilfeempfänger' im Allgemeinen."
            ),
            "url": src,
            "source": label,
        })

    return results


# ---------------------------------------------------------------------------
# Topic 4: Pensionsanpassung
# ---------------------------------------------------------------------------
def _build_pension_results(fact: dict, claim_lc: str) -> list[dict]:
    """Result entries for jährliche Pensionsanpassung."""
    data = fact.get("data") or {}
    year = fact.get("year", 2026)
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "PV.at + BMSGPK"
    comparisons = fact.get("comparisons") or {}

    def _de(v) -> str:
        if v is None:
            return "?"
        return f"{v}".replace(".", ",")

    headline = (
        f"Pensionsanpassung {year}: +{_de(data.get('anpassung_pct_normal'))} % "
        f"(Anpassungsfaktor {data.get('anpassungsfaktor')}). "
        f"Ausgleichszulagen-Richtsatz Alleinstehend: "
        f"{_de(data.get('ausgleichszulagen_richtsatz_alleinstehend_eur'))} EUR/Monat. "
        f"Pensionen ab {_de(data.get('luxus_pension_grenze_eur_pro_monat'))} EUR: "
        f"Pauschalbetrag {_de(data.get('luxus_pension_pauschal_eur'))} EUR statt voller Anpassung."
    )

    trend = comparisons.get("trend_anpassung_letzte_jahre") or []
    trend_str = " · ".join(
        f"{e['year']}: +{_de(e['pct'])} %" for e in trend
    )

    description_parts = [
        f"Gesetzliche Pensionsanpassung nach § 108h ASVG. ",
        f"Berechnungsbasis: {data.get('berechnungsbasis')}. ",
        f"Trend letzte 5 Jahre: {trend_str}. ",
        f"Reale Kaufkraft {year}: {comparisons.get('reale_kaufkraft_2026', '')}",
    ]
    for note in fact.get("context_notes") or []:
        description_parts.append(note)

    main = {
        "indicator_name": f"Pensionsanpassung Österreich {year}",
        "indicator": "factbook_pension_at",
        "country": "AUT",
        "country_name": "Österreich",
        "year": str(year),
        "value": data.get("anpassung_pct_normal"),
        "display_value": headline,
        "description": " ".join(description_parts),
        "url": src,
        "source": label,
    }
    return [main]


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_at_factbook(analysis: dict) -> dict:
    """Public entrypoint — returns matching curated facts.

    Output: ``{"source": "AT Factbook", "type": "official_data",
              "results": [...]}``
    """
    empty = {
        "source": "AT Factbook",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    matched_topics = _claim_matches_any_topic(claim)
    if not matched_topics:
        return empty

    data = _load_static_json()
    if not data:
        return empty

    facts = data.get("facts") or []
    results: list[dict] = []
    cl = claim.lower()

    for topic in matched_topics:
        for fact in facts:
            if fact.get("topic") != topic:
                continue
            if topic == "religion_schools_vienna":
                results.extend(_build_religion_results(fact))
            elif topic == "federal_subsidies_austria":
                results.extend(_build_subsidies_results(fact, cl))
            elif topic == "social_assistance_austria":
                results.extend(_build_social_results(fact, cl))
            elif topic == "pension_adjustment_austria":
                results.extend(_build_pension_results(fact, cl))

    return {
        "source": "AT Factbook",
        "type": "official_data",
        "results": results,
    }
