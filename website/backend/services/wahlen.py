"""BMI Wahlen — österreichische Wahlergebnisse seit 1986/1996.

Datenquelle: Bundesministerium für Inneres (BMI), Abt. III/6
(Wahlangelegenheiten / Bundeswahlbehörde).  Pro Wahl pflegt das BMI
eine HTML-Seite unter ``/412/<Wahltyp>/<Wahltyp>_<Jahr>/start.aspx``
mit dem Bundesergebnis als HTML-Tabelle.

Abgedeckt (Bundesergebnisse, keine Bundesländer-Detail-Aufschlüsselung):

- Nationalratswahlen (NRW): 1986, 1990, 1994, 1995, 1999, 2002, 2006,
  2008, 2013, 2017, 2019, 2024
- Bundespräsidentenwahlen (BPW): 1998, 2004, 2010, 2016, 2022
  (für 2016 ist nur die Wahl-Wiederholung vom 4. Dezember 2016
  hinterlegt — der erste Wahlgang vom 24. April 2016 ist auf einer
  separaten BMI-Seite und nicht in diesem Snapshot)
- Europawahlen (EUW): 1996, 1999, 2004, 2009, 2014, 2019, 2024

WICHTIG — IP-Block durch Myra Cloud
-----------------------------------
Wie bei ``volksbegehren.py`` blockt Myra Cloud Hetzner-IPs vom
direkten BMI-Abruf.  Wir nutzen die identische Static-First-Architektur:
``data/wahlen.json`` ist Primärquelle, Online-Refresh wird best-effort
versucht, manueller Refresh via ``scripts/refresh_wahlen.py``.

GUARDRAILS (siehe project_political_guardrails.md):
- **Keine Wahlprognosen** — diese Quelle deckt nur historische,
  abgeschlossene Wahlen ab.
- **Keine Partei-/Kandidaten-Bewertung** — wir zitieren reine
  Stimmenzahlen, Prozent-Anteile und Mandate, ohne Einordnung in
  links/rechts/etc.
- **Kein Vergleich von Wahlsiegern** als „besser/schlechter" — nur
  Zahlen, keine politische Wertung.
- **Bei BPW 2016**: Caveat im Methodik-Block, dass der hinterlegte
  Wert die Wahl-Wiederholung vom 4. Dezember 2016 ist
  (VfGH-Aufhebung der Stichwahl vom 22. Mai 2016).

Lizenz: BMI-Inhalte sind als amtliche Werke gemeinfrei (§ 7 UrhG-AT).
Zitation: BMI / Bundeswahlbehörde, „<Wahltyp> <Jahr>" (abgerufen
{Abfrage-Datum}).
"""

import json
import logging
import os
import re
import time

import httpx

logger = logging.getLogger("evidora")

WAHLEN_BASE = "https://www.bmi.gv.at/412/"

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "wahlen.json",
)

WAHLEN_CACHE_TTL = 86400  # 24h

_cache: dict | None = None
_cache_time: float = 0.0


# ---------------------------------------------------------------------------
# Trigger vocabulary
# ---------------------------------------------------------------------------


# Wahl-Keyword → kanonischer Typ (für Filter beim Suchen)
WAHL_TYPE_KEYWORDS: dict[str, str] = {
    # Nationalratswahl
    "nationalratswahl": "NRW",
    "nationalratswahlen": "NRW",
    "nrw": "NRW",
    "nationalrats-wahl": "NRW",
    "nationalrat wahl": "NRW",
    "national council election": "NRW",
    # Bundespräsidentenwahl
    "bundespräsidentenwahl": "BPW",
    "bundespräsidentenwahlen": "BPW",
    "bundespräsidentschaftswahl": "BPW",
    "bpw": "BPW",
    "bundespräsident wahl": "BPW",
    "presidential election": "BPW",
    # Europawahl
    "europawahl": "EUW",
    "europawahlen": "EUW",
    "eu-wahl": "EUW",
    "eu wahl": "EUW",
    "euw": "EUW",
    "european parliament election": "EUW",
    "europäisches parlament wahl": "EUW",
}

# Generische Wahl-Keywords (kein Typ-Hinweis, nur Trigger)
GENERIC_WAHL_KEYWORDS = [
    "wahlergebnis", "wahlergebnisse",
    "stimmenanteil", "stimmenanteile",
    "wahl", "wahlen",
    "wählerstimmen", "wahlbeteiligung",
    "election result", "election results",
    "vote share",
]

# AT-Kontext (analog volksbegehren.py — strikt)
WAHL_AT_CONTEXT = [
    "österreich", "austria", "österreichisch",
    "wien", "vienna", "graz", "linz", "salzburg", "innsbruck",
    "klagenfurt", "bregenz", "eisenstadt", "st. pölten",
    "niederösterreich", "oberösterreich", "steiermark",
    "kärnten", "vorarlberg", "burgenland", "tirol",
    "nationalrat", "bundeskanzler", "bundespräsident",
    "bundesregierung",
]

# Bekannte AT-Parteien (Trigger für „FPÖ 28% bei der Wahl 2024" o.ä.)
WAHL_PARTY_KEYWORDS = [
    "övp", "spö", "fpö", "neos", "grüne", "bzö",
    "kpö", "team stronach", "liste pilz", "jetzt",
    "bierpartei", "bier partei", "mfg",
]

# Bekannte BP-Kandidat:innen (Top-Namen — vollständige Liste ist im
# data/wahlen.json kanonisch).  Nur die historisch wichtigsten
# Kandidat:innen, die häufig in Claims vorkommen.
WAHL_CANDIDATE_KEYWORDS = [
    "van der bellen", "vdb", "klestil",
    "fischer", "hofer", "khol", "hundstorfer", "griss",
    "rosenkranz", "wallentin", "wlazny", "grosz",
]


def _claim_mentions_wahl_type(claim_lc: str) -> str | None:
    """Returns 'NRW' / 'BPW' / 'EUW' if the claim explicitly names one,
    else None."""
    for kw, typ in WAHL_TYPE_KEYWORDS.items():
        if kw in claim_lc:
            return typ
    return None


def _claim_has_generic_wahl_kw(claim_lc: str) -> bool:
    return any(kw in claim_lc for kw in GENERIC_WAHL_KEYWORDS)


def _claim_has_at_context(claim_lc: str) -> bool:
    return any(kw in claim_lc for kw in WAHL_AT_CONTEXT)


def _claim_has_party_or_candidate(claim_lc: str) -> bool:
    return (any(kw in claim_lc for kw in WAHL_PARTY_KEYWORDS)
            or any(kw in claim_lc for kw in WAHL_CANDIDATE_KEYWORDS))


def _claim_mentions_wahlen(claim: str) -> bool:
    """True wenn der Claim eindeutig auf eine österreichische Wahl
    verweist.

    Trigger-Logik (jede dieser Bedingungen reicht aus):
    - AT-spezifischer Wahltyp (Nationalrat = AT-only) + irgendwas
    - AT-Partei (ÖVP/SPÖ/FPÖ/NEOS/GRÜNE/BZÖ/KPÖ/MFG) + Jahr ODER
      AT-Partei + Wahl-Keyword (Wahl/Wahlergebnis/Wahltyp)
    - BP-Kandidat (Van der Bellen, Hofer, Khol …) + Jahr
    - Genericer Wahl-Begriff + AT-Kontext + Partei/Kandidat

    AT-Parteien sind unverwechselbar (kein DE/CH-Konflikt), daher zählen
    sie als impliziter AT-Kontext.
    """
    cl = claim.lower()
    typ = _claim_mentions_wahl_type(cl)
    has_party_or_cand = _claim_has_party_or_candidate(cl)
    has_at = _claim_has_at_context(cl)
    has_year = bool(re.search(r"\b(19[89]\d|20\d{2})\b", cl))
    has_generic = _claim_has_generic_wahl_kw(cl)

    # 1) "Nationalratswahl" ist AT-only — reicht für Trigger.
    if typ == "NRW":
        return True
    # 2) "Bundespräsidentenwahl" + AT-Kontext (AT hat dieses System;
    #    DE/CH haben es nicht direkt — aber wir wollen sicherheitshalber
    #    AT-Marker oder eine AT-Partei/Kandidat.)
    if typ == "BPW" and (has_at or has_party_or_cand):
        return True
    # 3) "Europawahl" + AT-Kontext nötig (kann jedes EU-Land sein)
    if typ == "EUW" and (has_at or has_party_or_cand):
        return True
    # 4) AT-Partei/Kandidat + Jahr → impliziter AT-Wahlbezug
    #    (z.B. „SPÖ 2019" liest sich klar als NRW-Bezug)
    if has_party_or_cand and has_year:
        return True
    # 5) AT-Partei + Wahl-Keyword (auch ohne Jahr)
    if has_party_or_cand and (has_generic or typ):
        return True
    # 6) Genericer Wahl-Begriff + AT-Kontext (kein Partei-Match)
    if has_generic and has_at:
        return True
    return False


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def _load_static_json() -> list[dict] | None:
    if not os.path.exists(STATIC_JSON_PATH):
        logger.warning(
            f"BMI Wahlen: static JSON not found at {STATIC_JSON_PATH}"
        )
        return None
    try:
        with open(STATIC_JSON_PATH, encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"BMI Wahlen: static JSON load failed: {e}")
        return None
    elections = payload.get("elections") or []
    return elections or None


async def fetch_wahlen(
    client: httpx.AsyncClient | None = None,
) -> dict | None:
    """Static-first loader for BMI Wahlen.

    Online-Refresh ist hier *nicht* automatisch — die 24 Wahlen müssten
    parallel gefetcht werden, das wäre für die Hot-Path-Quote zu teuer.
    Refresh läuft manuell über ``scripts/refresh_wahlen.py``.
    """
    global _cache, _cache_time
    now = time.time()
    if _cache is not None and (now - _cache_time) < WAHLEN_CACHE_TTL:
        return _cache

    elections = _load_static_json()
    if not elections:
        logger.warning("BMI Wahlen: no elections available")
        return None

    _cache = {
        "elections": elections,
        "fetched_at": now,
        "source": "static",
    }
    _cache_time = now
    logger.info(
        f"BMI Wahlen: {len(elections)} elections cached (source=static)"
    )
    return _cache


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


_PARTY_ALIAS: dict[str, str] = {
    # Lower-case alias → kanonisches Kurzkürzel im Datensatz
    "övp": "ÖVP", "vp": "ÖVP", "volkspartei": "ÖVP",
    "spö": "SPÖ", "sp": "SPÖ", "sozialdemokraten": "SPÖ",
    "fpö": "FPÖ", "fp": "FPÖ", "freiheitliche": "FPÖ",
    "neos": "NEOS",
    "grüne": "GRÜNE", "die grünen": "GRÜNE",
    "bzö": "BZÖ",
    "kpö": "KPÖ",
    "bierpartei": "BIER", "bier": "BIER",
    "team stronach": "FRANK", "stronach": "FRANK",
    "mfg": "MFG",
    "jetzt": "JETZT", "liste pilz": "JETZT",
}


def _extract_year(claim: str) -> int | None:
    """Pull the most recent year mentioned in the claim within the
    range we cover (1986–2025)."""
    years = [int(y) for y in re.findall(r"\b(19[89]\d|20[0-2]\d)\b", claim)]
    years = [y for y in years if 1986 <= y <= 2025]
    if not years:
        return None
    # Prefer the latest year mentioned (claim is usually about one event)
    return years[-1]


def _resolve_party_short(claim_lc: str) -> str | None:
    """Find a known party alias in the claim and return its canonical
    short."""
    # Sort longer aliases first to prefer "team stronach" over "stronach"
    for alias in sorted(_PARTY_ALIAS, key=len, reverse=True):
        if alias in claim_lc:
            return _PARTY_ALIAS[alias]
    return None


def _resolve_candidate_short(claim_lc: str, results: list[dict]) -> str | None:
    """Find a known BP candidate by surname/short in the claim."""
    for r in results:
        short = (r.get("short") or "").lower()
        if short and short in claim_lc:
            return r["short"]
    # Special case: „van der bellen" (multi-word surname)
    if "van der bellen" in claim_lc or "vdb" in claim_lc:
        return "Bellen"
    return None


def _format_election_label(e: dict) -> str:
    typ = e.get("type", "")
    year = e.get("year")
    label = {
        "NRW": "Nationalratswahl",
        "BPW": "Bundespräsidentenwahl",
        "EUW": "Europawahl",
    }.get(typ, typ)
    return f"{label} {year}"


def _make_party_entry(election: dict, party: dict) -> dict:
    typ = election.get("type")
    year = election.get("year")
    label = _format_election_label(election)
    short = party.get("short", "")
    long_ = party.get("long", "")
    votes = party.get("votes")
    pct = party.get("percent")
    seats = party.get("seats")

    parts: list[str] = []
    if pct is not None:
        parts.append(f"{pct:.1f} %".replace(".", ","))
    if votes is not None:
        parts.append(f"{votes:,} Stimmen".replace(",", "."))
    if seats is not None and typ in ("NRW", "EUW"):
        parts.append(f"{seats} Mandate")
    display_value = " · ".join(parts) if parts else short

    return {
        "indicator_name": f"{label} — {short}: {long_}" if long_ != short else f"{label} — {short}",
        "indicator": f"wahl_{typ.lower()}_party",
        "country": "AUT",
        "country_name": "Österreich",
        "year": str(year) if year else "",
        "value": pct if pct is not None else "",
        "display_value": display_value,
        "description": (
            f"Bundesergebnis {label} laut offizieller BMI-Statistik."
            if long_ == short else
            f"Bundesergebnis {label} laut offizieller BMI-Statistik. "
            f"Vollständige Parteibezeichnung: {long_}."
        ),
        "url": election.get("url", WAHLEN_BASE),
    }


def _make_election_summary(election: dict, top_n: int = 3) -> dict:
    """Top-N parties of an election, suitable when the claim is generic
    ('Wahlergebnis NRW 2024')."""
    typ = election.get("type")
    year = election.get("year")
    label = _format_election_label(election)
    results = election.get("results") or []
    top = sorted(results, key=lambda r: -(r.get("percent") or 0))[:top_n]
    parts: list[str] = []
    for r in top:
        nm = r.get("short", "")
        p = r.get("percent")
        if p is not None:
            parts.append(f"{nm} {p:.1f} %".replace(".", ","))
        else:
            parts.append(nm)
    display_value = " · ".join(parts)
    return {
        "indicator_name": f"{label} — Bundesergebnis (Top {top_n})",
        "indicator": f"wahl_{typ.lower()}_summary",
        "country": "AUT",
        "country_name": "Österreich",
        "year": str(year) if year else "",
        "value": "",
        "display_value": display_value,
        "description": (
            f"Top-{top_n} Parteien/Kandidat:innen nach Stimmenanteil bei der "
            f"{label} laut offizieller BMI-Statistik. Die vollständige Liste "
            f"ist beim BMI verlinkt."
        ),
        "url": election.get("url", WAHLEN_BASE),
    }


_WAHLEN_METHODIK = (
    "Datenquelle: Bundesministerium für Inneres (BMI), Bundeswahlbehörde — "
    "die offiziellen Bundesergebnisse aller österreichischen Bundeswahlen "
    "(Nationalratswahlen, Bundespräsidentenwahlen, Europawahlen). "
    "Einschränkungen: "
    "(1) Nur Bundesergebnisse — Bundesländer- und Wahlkreis-Aufschlüsselung "
    "ist beim BMI direkt verlinkt, aber nicht in diesem Datensatz. "
    "(2) Bei der Bundespräsidentenwahl 2016 ist nur die Wahl-Wiederholung "
    "vom 4. Dezember 2016 hinterlegt — der erste Wahlgang vom 24. April 2016 "
    "und die VfGH-aufgehobene Stichwahl vom 22. Mai 2016 sind separat "
    "auf der BMI-Seite dokumentiert. "
    "(3) Wir geben keine Wahlprognosen ab — diese Quelle deckt nur "
    "historische, abgeschlossene Wahlen ab. "
    "(4) Wir nehmen keine politische Bewertung der Parteien oder "
    "Kandidat:innen vor — die Zahlen sind reine Stimmenzahlen, "
    "Prozent-Anteile und Mandate. "
    "(5) Mandatsverteilungen können sich nach der Wahl durch "
    "Klubwechsel/Austritt verändern (siehe Parlament.gv.at-Quelle für "
    "den aktuellen Stand)."
)


async def search_wahlen(analysis: dict) -> dict:
    empty = {
        "source": "BMI Wahlen",
        "type": "official_data",
        "results": [],
    }
    data = await fetch_wahlen()
    if not data:
        return empty
    elections = data.get("elections") or []
    if not elections:
        return empty
    claim = analysis.get("claim", "")
    if not _claim_mentions_wahlen(claim):
        return empty

    cl = claim.lower()
    typ = _claim_mentions_wahl_type(cl)
    year = _extract_year(claim)
    party_short = _resolve_party_short(cl)

    # Filter elections by type/year
    matched_elections: list[dict] = []
    for e in elections:
        if typ and e.get("type") != typ:
            continue
        if year and e.get("year") != year:
            continue
        matched_elections.append(e)

    # If we still have everything, keep at most the 3 most-recent
    # elections of the relevant type — claim probably refers to a
    # specific recent vote.
    if not typ and not year and not party_short:
        matched_elections = sorted(
            matched_elections, key=lambda e: -(e.get("year") or 0)
        )[:3]

    if not matched_elections:
        return empty

    results: list[dict] = []
    for election in matched_elections:
        e_results = election.get("results") or []
        if not e_results:
            continue
        # Try to find candidate (BPW) or party (NRW/EUW) match
        cand_short = None
        if election.get("type") == "BPW":
            cand_short = _resolve_candidate_short(cl, e_results)
        chosen: list[dict] = []
        if party_short:
            chosen = [r for r in e_results if r.get("short") == party_short]
        if not chosen and cand_short:
            chosen = [r for r in e_results if r.get("short") == cand_short]
        if chosen:
            for p in chosen:
                results.append(_make_party_entry(election, p))
        else:
            # No specific party/candidate match → top-3 summary
            results.append(_make_election_summary(election))

    if not results:
        return empty

    # Append methodology caveat
    results.append({
        "indicator_name": "WICHTIGER KONTEXT: Wahl-Daten",
        "indicator": "context",
        "country": "",
        "country_name": "",
        "year": "",
        "value": "",
        "display_value": "",
        "description": _WAHLEN_METHODIK,
        "url": WAHLEN_BASE,
    })

    return {
        "source": "BMI Wahlen",
        "type": "official_data",
        "results": results,
    }


# ---------------------------------------------------------------------------
# Sync gate (for main.py)
# ---------------------------------------------------------------------------


def claim_mentions_wahlen_cached(claim: str) -> bool:
    """Synchronous trigger gate (no I/O). The trigger is keyword-only;
    we don't need the JSON loaded for it.
    """
    return _claim_mentions_wahlen(claim)
