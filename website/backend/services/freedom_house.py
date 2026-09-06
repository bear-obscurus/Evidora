"""Freedom House Live-Connector — FIW Country-Ratings (Static-First-Cache).

Freedom House (https://freedomhouse.org) ist eine US-amerikanische NGO, die
seit 1972 jährlich ihre 'Freedom in the World' (FIW)-Studie veröffentlicht.
Sie misst politische Rechte (Political Rights, 0-40 Punkte) und Bürgerrechte
(Civil Liberties, 0-60 Punkte) pro Land. Die Summe (0-100) bestimmt den
Status: Free (70-100), Partly Free (35-69), Not Free (0-34).

Komplementär zu V-Dem:
  - V-Dem: Continuous-Indizes 0-1, methodisch via Bayesian-IRT-Aggregation
    aus Experten-Befragungen, sehr granular (470+ Sub-Indikatoren).
  - Freedom House: Aggregat-Score 0-100 mit Schwellen-Status (Free/Partly
    Free/Not Free), pragmatisch + öffentlichkeitswirksam.

Strategie: STATIC-FIRST-PRE-CACHE
=================================
Freedom House publiziert keine REST-API. Daten werden als Excel/CSV-Download
veröffentlicht (jährlich Februar/März). Wir halten einen kuratierten Subset
von ~55 Schlüssel-Ländern in ``data/freedom_house.json``:

  - DACH + EU + globale Referenz (USA, RUS, CHN, IND, BRA, ZAF, ...)
  - Osteuropa + Westbalkan + Kaukasus + Zentralasien (autoritäre Vergleichs-
    Länder)
  - Pro Land: total_score, pr_score, cl_score, status sowie der Vorjahres-
    Stand (vorjahr_score/-status) — der zeigt die Richtung, nach der Claims
    wie „X wird immer unfreier" fragen.

Die Ausgabe-Jahreszahl kommt AUSSCHLIESSLICH aus ``report_year`` in der JSON.
Weder der Dateiname noch der Code tragen sie — sonst zeigt ein reiner Daten-
Refresh weiter die alte Ausgabe an (so passiert in wifo_ihs, PR #131).

Refresh-Workflow (manuell oder per Cron):
  1. Werte je Land von ``freedomhouse.org/country/{slug}/freedom-world/{jahr}``
     lesen (Stand 2026: die frühere All_data_FIW-XLSX liegt nicht mehr unter
     ihrem alten Pfad; die Country-Seiten sind die belastbare Quelle)
  2. Gegenprobe: PR + CL muss je Land dem Total entsprechen
  3. report_year/covering_events_year + world_summary mitziehen
  4. JSON regenerieren, mtime ändert → Hot-Reload greift automatisch

Trigger:
  - Claim enthält Länder-Alias UND Demokratie-/Freiheits-/Pressefreiheits-
    Keyword
  - ODER allgemeines Freedom-House-Vokabular ("freedom-house",
    "freie länder", "demokratie-status", ...) auch ohne Land-Nennung
    → DACH-Default (AT/DE/CH).

Limitations:
  - FIW publiziert jährlich (Februar/März) — die Ausgabe bewertet immer das
    VORJAHR (FIW 2026 bewertet das Kalenderjahr 2025). Welche Ausgabe im
    Cache liegt, sagt ``report_year``/``covering_events_year``.
  - Methodik basiert auf Experten-Bewertung (Freedom-House-Analysten,
    interne + externe Reviewer). Subjektiv, aber transparent dokumentiert.
  - Manche Länder/Gebiete (z.B. Taiwan) haben Sonderstatus, sind aber im
    Static-Cache aktuell nicht enthalten.
  - Refresh einmal jährlich nötig (Februar/März). Stand 2026-09-05: FIW 2026,
    55/55 Länder direkt von den offiziellen Country-Seiten.

GUARDRAILS (siehe project_political_guardrails.md):
  - Wir zitieren Freedom-House-Scores, wir bewerten sie nicht.
  - Wir nehmen keine eigene Partei-/Politiker-Bewertung vor.
  - Caveat zur Methodik (Experten-Befragung, US-NGO) ist Pflicht.

Result-Schema:
  {
    "indicator_name": "Freedom House FIW 2026 — Russia: 12/100 (Not Free)",
    "indicator": "freedom_house_score",
    "country": "RU",
    "year": "2026",
    "topic": "freedom_house_ranking",
    "display_value": "RU 12/100 'Not Free' (PR 4/40, CL 8/60) — vs. AT 94, DE 95, SE 99, CN 9 — gegenüber der Vorausgabe -1 Punkte (Freedom House FIW 2026)",
    "description": "<methodology_note> + globale Einordnung aus world_summary",
    "url": "https://freedomhouse.org/country/russia",
    "secondary_url": "https://freedomhouse.org/report/freedom-world",
    "source": "<source_label aus der JSON>",
  }

Wiring (NICHT in dieser Datei — vom Hauptprozess manuell):
  - main.py: import + tasks.append(search_freedom_house(analysis))
  - reranker.py: Indicator-Whitelist-Marker für 'freedom_house_score' /
    'Freedom House' möglich. Live-Quelle, NICHT in AUTHORITATIVE-Pack-
    Markern (kein kuratiertes Pack, sondern Live-Static-Cache).
  - confidence_calibration.py: optional, für Boosts.
"""

from __future__ import annotations

import logging
import os
import re

from services._schreibweise import normalisiere, norm_terme
from services._static_cache import load_json_mtime_aware

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "freedom_house.json",
)

# DACH-Default-Länder, wenn Claim Freedom-House-Keyword nennt aber kein Land.
_DEFAULT_COUNTRIES_FOR_DACH_CLAIMS = ("AUT", "DEU", "CHE")

# Trigger-Keywords (DE + EN). Bei Match + Land → Treffer.
# Bei Match ohne Land → DACH-Default.
# Die ASCII-Zwillinge („buergerrechte" neben „bürgerrechte") sind hier
# entfallen: norm_terme faltet beide auf dieselbe Form. Zusammengeschriebene
# Varianten („demokratieranking") bleiben — die entstehen nicht durch
# Normalisierung, sondern sind eine eigene Schreibweise.
_FH_KEYWORDS = norm_terme(
    "freedom house", "fiw",
    "freedom in the world",
    "freedom of press",
    "pressefreiheit", "presse freiheit",
    "political rights", "politische rechte",
    "bürgerrechte", "civil liberties",
    "freie wahlen", "free elections",
    "demokratie-ranking", "demokratieranking",
    "demokratie-status", "demokratiestatus", "democracy status",
    "freie länder", "free countries",
    "unfreie länder", "not free countries",
    "partly free", "teilweise frei",
    "freiheitsindex", "freedom index",
    "country freedom rating", "länder-freiheits-rating",
    "demokratie-niveau", "demokratieniveau",
    "autoritäres regime", "authoritarian regime",
)

# Reference-Länder für display_value Multi-Country-Comparison.
# AT-Bias: AT zuerst, dann DE/CH, dann Kontrast (SE top, CN/RUS bottom).
_DISPLAY_REFERENCE_COUNTRIES = (
    "AUT", "DEU", "CHE", "SWE", "NOR", "USA", "HUN", "POL",
    "RUS", "CHN", "TUR", "BLR",
)

# Maximum Anzahl Reference-Länder im display_value.
MAX_COUNTRIES_IN_DISPLAY = 5

# Maximum Primär-Länder pro Claim.
MAX_PRIMARY_COUNTRIES = 1

# ISO3 → ISO2 Mapping (für display_value-Kompaktheit).
_ISO3_TO_ISO2 = {
    "AUT": "AT", "DEU": "DE", "CHE": "CH", "FRA": "FR", "ITA": "IT",
    "ESP": "ES", "GBR": "UK", "SWE": "SE", "NOR": "NO", "DNK": "DK",
    "FIN": "FI", "NLD": "NL", "BEL": "BE", "IRL": "IE", "GRC": "GR",
    "PRT": "PT", "USA": "US", "CAN": "CA", "AUS": "AU", "NZL": "NZ",
    "JPN": "JP", "ISR": "IL", "RUS": "RU", "CHN": "CN", "IND": "IN",
    "BRA": "BR", "ZAF": "ZA", "TUR": "TR", "HUN": "HU", "POL": "PL",
    "CZE": "CZ", "SVK": "SK", "SVN": "SI", "EST": "EE", "LVA": "LV",
    "LTU": "LT", "ROU": "RO", "BGR": "BG", "HRV": "HR", "ALB": "AL",
    "BIH": "BA", "SRB": "RS", "MKD": "MK", "MNE": "ME", "BLR": "BY",
    "UKR": "UA", "MDA": "MD", "GEO": "GE", "ARM": "AM", "AZE": "AZ",
    "KAZ": "KZ", "KGZ": "KG", "TJK": "TJ", "TKM": "TM", "UZB": "UZ",
}

# Status-Übersetzung für indicator_name.
_STATUS_DE = {
    "Free": "Free",
    "Partly Free": "Partly Free",
    "Not Free": "Not Free",
}


def _load_data() -> dict | None:
    """Lade JSON mit Hot-Reload-Awareness."""
    return load_json_mtime_aware(STATIC_JSON_PATH)


def _country_aliases(data: dict) -> dict:
    """Country-Code → Liste von DE/EN-Substring-Aliassen."""
    return data.get("country_aliases") or {}


def _detect_countries_in_claim(claim_lc: str, data: dict) -> list[str]:
    """Erkenne welche ISO3-Country-Codes der Claim erwähnt.

    Returns Liste der ISO3-Codes (jedes Land höchstens einmal).
    """
    aliases = _country_aliases(data)
    found: list[str] = []
    for iso3, alias_list in aliases.items():
        for alias in alias_list:
            if normalisiere(alias) in claim_lc:
                found.append(iso3)
                break  # nur einmal pro Land
    return found


def _has_fh_keyword(claim_lc: str) -> bool:
    """Trifft mindestens ein Freedom-House-Trigger-Keyword?"""
    return any(kw in claim_lc for kw in _FH_KEYWORDS)


def claim_mentions_freedom_house_cached(claim: str) -> bool:
    """Trigger-Pre-Check (für main.py-Pipeline-Routing).

    Returns True, wenn der Claim ein FH-Keyword enthält UND entweder
    ein Land aus den Aliassen ODER generisches Freedom-House-Vokabular
    (dann DACH-Default).
    """
    if not claim:
        return False
    # Politik-Tabu-Guard 2.0: FH misst Länder-Freedom, nicht Parteien.
    # Bewusst `claim.lower()` und NICHT `normalisiere(claim)`: die Token-Liste
    # in _topic_match ist unnormalisiert und enthält Bindestrich-Namen
    # („meinl-reisinger"). Ein gefalteter Claim macht daraus „meinl reisinger",
    # der Guard greift nicht mehr — und Länder-Quellen feuern auf eine
    # Partei-Korruptions-Aussage. Festgenagelt in
    # tests/test_freedom_house_regime_klassifikation.py.
    from services._topic_match import is_party_corruption_superlative_claim
    if is_party_corruption_superlative_claim(claim.lower()):
        return False
    data = _load_data()
    if not data:
        return False
    claim_lc = normalisiere(claim)

    if not _has_fh_keyword(claim_lc):
        return False

    # Wenn FH-Keyword + Land → trigger.
    countries_found = _detect_countries_in_claim(claim_lc, data)
    if countries_found:
        return True

    # Wenn FH-Keyword ohne Land → trigger mit DACH-Default.
    return True


async def fetch_freedom_house(client=None) -> dict:
    """On-Demand-Load der Freedom-House-Ratings aus dem Static-JSON.

    Returns das gesamte JSON-Dict (mit ratings/country_aliases/source_label/...).
    ``client`` wird ignoriert (nur für Signatur-Symmetrie mit anderen
    Live-Connectoren).
    """
    data = _load_data()
    if not data:
        return {}
    return data


def _format_country_total(iso3: str, ratings: dict) -> str:
    """Hilfs-Format: 'AT 93'."""
    iso2 = _ISO3_TO_ISO2.get(iso3, iso3[:2])
    rating = ratings.get(iso3) or {}
    total = rating.get("total_score")
    if total is None:
        return ""
    return f"{iso2} {total}"


def _select_display_countries(
    requested_countries: list[str],
    ratings: dict,
    primary: str,
) -> list[str]:
    """Wähle bis zu MAX_COUNTRIES_IN_DISPLAY Länder für den display_value.

    Strategie: Erst alle aus dem Claim genannten (außer primary, das wird
    separat dargestellt), dann auffüllen mit _DISPLAY_REFERENCE_COUNTRIES.
    """
    selected: list[str] = []
    for c in requested_countries:
        if c == primary:
            continue
        if c in ratings and c not in selected:
            selected.append(c)
        if len(selected) >= MAX_COUNTRIES_IN_DISPLAY:
            return selected
    for c in _DISPLAY_REFERENCE_COUNTRIES:
        if c == primary:
            continue
        if c in ratings and c not in selected:
            selected.append(c)
        if len(selected) >= MAX_COUNTRIES_IN_DISPLAY:
            return selected
    return selected


def _select_primary_country(
    requested_countries: list[str],
    ratings: dict,
) -> str | None:
    """Wähle das primäre Land für indicator_name + country-Feld.

    Erstes Match aus dem Claim mit verfügbaren Daten. Wenn keine Country-
    Detection erfolgt ist, fällt auf AUT zurück.
    """
    for c in requested_countries:
        if c in ratings:
            return c
    if "AUT" in ratings:
        return "AUT"
    for k in ratings:
        return k
    return None


_PROMPT_BUDGET = 400  # synthesizer.MAX_STR — darüber kürzt der Prompt
_PFLICHT_BUDGET = 250  # davon reserviert für den Methodik-Caveat


def _richtungs_satz(rating: dict) -> str:
    """'— gegenüber dem Vorjahr +1 Punkt' bzw. den Status-Wechsel.

    Claims fragen fast nie nach dem Punktestand, sondern nach der RICHTUNG
    („Österreich wird immer unfreier"). Ohne den Vorjahreswert kann der
    Synthesizer die Richtung nur raten.
    """
    jetzt, vorher = rating.get("total_score"), rating.get("vorjahr_score")
    if jetzt is None or vorher is None:
        return ""
    delta = jetzt - vorher
    if delta == 0:
        satz = " — gegenüber der Vorausgabe unverändert"
    else:
        einheit = "Punkt" if abs(delta) == 1 else "Punkte"
        satz = f" — gegenüber der Vorausgabe {delta:+d} {einheit}"
    alt_status, neu_status = rating.get("vorjahr_status"), rating.get("status")
    if alt_status and neu_status and alt_status != neu_status:
        satz += f" und von '{alt_status}' auf '{neu_status}' gewechselt"
    return satz


def _beschreibung(methodik: str, data: dict, rating: dict | None = None) -> str:
    """Methodik + globale Einordnung, zusammen unter der Prompt-Grenze.

    ``world_summary`` lag bis 2026-09 als totes Feld in der JSON: niemand las
    es. Genau diese Zahlen braucht der Synthesizer aber bei Claims über den
    globalen Trend („Demokratie ist weltweit auf dem Rückzug"), sonst hat er
    nur den Punktestand EINES Landes.
    """
    w = data.get("world_summary") or {}
    # Nur wenn es das angefragte Land betrifft: sonst liest der Synthesizer
    # ein negatives PR als Datenfehler.
    negativ_satz = ""
    if rating and isinstance(rating.get("pr_score"), int) and rating["pr_score"] < 0:
        negativ_satz = (
            "Ein negativer PR-Wert ist methodisch vorgesehen: eine Zusatzfrage "
            "zu erzwungenen Bevölkerungsverschiebungen kann Punkte abziehen."
        )
    global_satz = ""
    if w.get("global_decline_years") and w.get("free_countries"):
        global_satz = (
            f"Globale Einordnung dieser Ausgabe: {w['global_decline_years']}. "
            f"Jahr in Folge mit weltweitem Rückgang, "
            f"{w.get('countries_declined', '?')} Länder schlechter gegenüber "
            f"{w.get('countries_improved', '?')} besser; zugleich sind "
            f"{w['free_countries']} von "
            f"{w.get('total_countries_covered', '?')} Ländern 'Free'."
        )
    # Reihenfolge ist Absicht: der Methodik-Caveat ist laut den politischen
    # Guardrails PFLICHT und bekommt sein Budget zuerst. Was danach passt,
    # kommt dazu — ganze Sätze oder gar nicht.
    pflicht = _ganze_saetze(methodik, _PFLICHT_BUDGET) or methodik[:_PFLICHT_BUDGET]
    teile, rest = [pflicht], _PROMPT_BUDGET - len(pflicht)
    for zusatz in (negativ_satz, global_satz):
        if zusatz and len(zusatz) + 1 <= rest:
            teile.append(zusatz)
            rest -= len(zusatz) + 1
    return " ".join(teile)


def _ganze_saetze(text: str, budget: int) -> str:
    """Kürzt auf ganze Sätze statt mitten im Wort.

    ``text[:400]`` hat hier bis 2026-09 „erzwungenen Bevölkerungsversc"
    produziert — ein Fragment, das der Synthesizer als vollständige Aussage
    liest.
    """
    if budget <= 0:
        return ""
    if len(text) <= budget:
        return text
    behalten: list[str] = []
    laenge = 0
    for satz in re.findall(r"[^.]*\.", text):
        if laenge + len(satz) > budget:
            break
        behalten.append(satz)
        laenge += len(satz)
    return "".join(behalten).strip()


# Claims, die aus einem Freiheits-Rating eine STAATSFORM ableiten wollen.
# Bewusst eng: nur Begriffe, die eine Regime-Klassifikation behaupten.
_REGIME_BEGRIFFE = norm_terme(
    "demokratie", "demokratisch", "diktatur", "diktatorisch",
    "autokratie", "autokratisch", "autoritär",
    "regime", "staatsform", "unfreies land", "unfreier staat",
)


def _klassifikations_warnung(claim_lc: str) -> str:
    """Bei Regime-Claims: was FIW misst — und was nicht.

    Aus der QA-Batterie vom 2026-09-06. „Ungarn ist laut Freedom House keine
    Demokratie mehr" bekam true@0.9, begründet mit „Partly Free, nicht als
    Demokratie eingestuft". Der Freedom-House-Text selbst sagt das nirgends:
    er liefert 65/100 (Partly Free) und die Methodik-Schwellen. Der Sprung
    vom FREIHEITS-Rating auf eine STAATSFORM kam vom Synthesizer.

    Das berührt Guardrail 3 (keine eigene politische Klassifikation): eine
    Einstufung darf nur zitiert werden, wenn eine Quelle sie tatsächlich
    vornimmt, und die Quelle muss ausgewiesen sein. Hier wurde Freedom House
    eine Aussage zugeschrieben, die es nicht macht.

    Die Warnung hängt nur bei Regime-Claims an — sonst kostet sie bei jedem
    Länder-Claim Prompt-Budget, das die Zahlen brauchen.
    """
    if not any(t in claim_lc for t in _REGIME_BEGRIFFE):
        return ""
    return (" WICHTIG: FIW misst FREIHEITSGRADE, keine Staatsform. "
            "'Partly Free'/'Not Free' sagen NICHT, ob ein Land eine "
            "Demokratie ist — Freedom House nimmt keine Regime-"
            "Klassifikation vor. Wer beides gleichsetzt, überdehnt die "
            "Quelle.")


def _build_display_value(
    primary_iso3: str,
    primary_rating: dict,
    display_countries: list[str],
    ratings: dict,
    report_year: int | str = "",
    claim_lc: str = "",
) -> str:
    """Build 'RU 12/100 'Not Free' (PR 4/40, CL 8/60) — vs. AT 94, DE 95, ...
    (Freedom House FIW 2026)'. Die Jahreszahl kommt aus den Daten.
    """
    iso2 = _ISO3_TO_ISO2.get(primary_iso3, primary_iso3[:2])
    total = primary_rating.get("total_score", "?")
    status = primary_rating.get("status", "—")
    pr = primary_rating.get("pr_score", "?")
    cl = primary_rating.get("cl_score", "?")

    head = (
        f"{iso2} {total}/100 '{status}' "
        f"(PR {pr}/40, CL {cl}/60)"
    )

    parts: list[str] = []
    for iso3 in display_countries:
        formatted = _format_country_total(iso3, ratings)
        if formatted:
            parts.append(formatted)

    if parts:
        ref = " — vs. " + ", ".join(parts)
    else:
        ref = ""

    richtung = _richtungs_satz(primary_rating)
    marke = f" (Freedom House FIW {report_year})" if report_year else ""
    warnung = _klassifikations_warnung(claim_lc)
    return f"{head}{ref}{richtung}{marke}{warnung}"


def _country_url(iso3: str, data: dict) -> str:
    """Konstruiere die offizielle Freedom-House-Country-URL.

    Format: https://freedomhouse.org/country/{slug}
    Slug aus country_slugs (oder lower-case ISO3 als Fallback).
    """
    slugs = data.get("country_slugs") or {}
    slug = slugs.get(iso3) or iso3.lower()
    return f"https://freedomhouse.org/country/{slug}"


async def search_freedom_house(analysis: dict) -> dict:
    """Live-Lookup gegen den Freedom-House-Static-Cache für Freiheits-Claims.

    Returns Dict mit Pipeline-Standard-Schema:
      {
        "source": "Freedom House",
        "type": "freedom_rating",
        "results": [...],   # max 1 primary country (komplementär zu V-Dem)
      }
    """
    empty = {"source": "Freedom House", "type": "freedom_rating", "results": []}

    if not analysis:
        return empty
    claim = (
        analysis.get("claim")
        or analysis.get("original_claim")
        or analysis.get("text")
        or ""
    ).strip()
    if not claim:
        return empty

    data = _load_data()
    if not data:
        logger.warning("freedom_house: static JSON konnte nicht geladen werden")
        return empty

    claim_lc = normalisiere(claim)

    if not _has_fh_keyword(claim_lc):
        return empty

    # Country-Detection: Claim selbst + Entity-Liste.
    requested_countries = _detect_countries_in_claim(claim_lc, data)
    entities = (analysis.get("entities") or [])
    if entities:
        ents_lc = " ".join(str(e).lower() for e in entities)
        for c in _detect_countries_in_claim(ents_lc, data):
            if c not in requested_countries:
                requested_countries.append(c)

    # Wenn keine Land-Detection: DACH-Default.
    if not requested_countries:
        requested_countries = list(_DEFAULT_COUNTRIES_FOR_DACH_CLAIMS)

    ratings = data.get("ratings") or {}
    if not ratings:
        return empty

    source_label = data.get(
        "source_label",
        "Freedom House — Freedom in the World",
    )
    secondary_url = data.get(
        "source_url", "https://freedomhouse.org/report/freedom-world"
    )
    # KEIN Jahres-Default: fehlt die Angabe, lieber gar kein Jahr nennen als
    # eine erfundene Ausgabe behaupten.
    report_year = data.get("report_year") or ""

    # Methodik-Caveat als description.
    methodology_short = (
        data.get("methodology_note")
        or "FIW misst Political Rights (0-40) + Civil Liberties (0-60). "
           "Status: Free 70-100, Partly Free 35-69, Not Free 0-34."
    )

    results: list[dict] = []
    primary_iso3 = _select_primary_country(requested_countries, ratings)
    if primary_iso3 is None:
        return empty

    primary_rating = ratings.get(primary_iso3) or {}
    if not primary_rating:
        return empty

    iso2 = _ISO3_TO_ISO2.get(primary_iso3, primary_iso3[:2])
    total = primary_rating.get("total_score", "?")
    status = primary_rating.get("status", "—")

    # Country-Display-Name (für indicator_name): aus aliases den ersten
    # English-Alias holen, sonst ISO3.
    aliases = data.get("country_aliases", {}).get(primary_iso3) or []
    display_name = aliases[0].title() if aliases else primary_iso3
    # Versuche, einen "schöneren" englischen Namen zu finden.
    for a in aliases:
        if a in (
            "austria", "germany", "switzerland", "france", "italy",
            "spain", "united kingdom", "sweden", "norway", "denmark",
            "finland", "netherlands", "belgium", "ireland", "greece",
            "portugal", "united states", "canada", "australia",
            "new zealand", "japan", "israel", "russia", "china",
            "india", "brazil", "south africa", "turkey", "hungary",
            "poland", "czech republic", "czechia", "slovakia",
            "slovenia", "estonia", "latvia", "lithuania", "romania",
            "bulgaria", "croatia", "albania",
            "bosnia and herzegovina", "serbia", "north macedonia",
            "montenegro", "belarus", "ukraine", "moldova", "georgia",
            "armenia", "azerbaijan", "kazakhstan", "kyrgyzstan",
            "tajikistan", "turkmenistan", "uzbekistan",
        ):
            display_name = a.title()
            break

    display_countries = _select_display_countries(
        requested_countries, ratings, primary_iso3
    )

    display_value = _build_display_value(
        primary_iso3, primary_rating, display_countries, ratings, report_year,
        claim_lc,
    )

    ausgabe = f"FIW {report_year} " if report_year else ""
    indicator_name = (
        f"Freedom House {ausgabe}— "
        f"{display_name}: {total}/100 ({status})"
    )

    results.append({
        "indicator_name": indicator_name,
        "indicator": "freedom_house_score",
        "country": iso2,
        "year": str(report_year),
        "topic": "freedom_house_ranking",
        "display_value": display_value[:480],
        "description": _beschreibung(methodology_short, data, primary_rating),
        "url": _country_url(primary_iso3, data),
        "secondary_url": secondary_url,
        "source": source_label,
    })

    logger.info(
        f"freedom_house: 1 Treffer für country={primary_iso3} "
        f"(claim countries: {requested_countries[:3]})"
    )

    return {
        "source": "Freedom House",
        "type": "freedom_rating",
        "results": results,
    }
