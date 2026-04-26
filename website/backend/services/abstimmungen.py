"""Parlament Voting Records — Abstimmungsverhalten der Klubs im Nationalrat.

Datenquelle: parlament.gv.at Open-Data Filter-API.  Pro Beschluss /
Verhandlungsgegenstand werden die Klubs in den Feldern ``Dafür`` /
``Dagegen`` als JSON-Listen ausgewiesen.

v1-Umfang:
- NR-Beschlüsse seit GP XXVI (2017–heute)
- DOKTYPs: RV (Regierungsvorlage), A (Antrag), BUA (Bericht und Antrag),
  BRA (Bericht)
- ~1260 Einträge mit Voting-Info, ~550 KB als JSON-Snapshot

WICHTIG — Static-First-Architektur (analog zu volksbegehren/wahlen)
parlament.gv.at antwortet von Hetzner aus zwar (kein Myra-Block), aber
der Snapshot-Ansatz ist konsistent mit den anderen AT-Quellen und
entlastet den Hot-Path. Manuelles Refresh via
``scripts/refresh_abstimmungen.py``.

GUARDRAILS (siehe project_political_guardrails.md):
- Wir geben **nur** die offiziellen Abstimmungsergebnisse wieder
  (welche Klubs dafür/dagegen waren), keine Bewertung der Inhalte
  der Gesetze und keine Bewertung des Klub-Verhaltens.
- Keine Mehrheitsanalysen ("Welche Koalition ist möglich?") — nur
  faktische Stimmenverteilung.
- Bei Behauptungen wie "FPÖ war einzige Gegenstimme" liefern wir die
  rohen Klub-Listen. Der Synthesizer/User leitet daraus die Bewertung
  ab.

Lizenz: CC BY 4.0 (Parlament Österreich), Quelle: parlament.gv.at.
"""

import json
import logging
import os
import re
import time

import httpx

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "abstimmungen.json",
)

CACHE_TTL = 86400  # 24 h — Snapshot ist Stand vom letzten Refresh

_cache: dict | None = None
_cache_time: float = 0.0


# Trigger-Vokabular: alles, was nach Klub-Abstimmungen riecht.
VOTING_KEYWORDS = [
    # Verben
    "abgestimmt", "abstimmung", "stimmten", "stimmte",
    "gestimmt", "abgelehnt", "beschlossen", "verabschiedet",
    "angenommen", "zugestimmt", "votierte", "votierten",
    # Substantive
    "abstimmungsverhalten", "fraktion", "fraktionen",
    "klub", "klubs", "klubobmann", "klubobfrau",
    "mehrheit", "minderheit", "gegenstimme", "gegenstimmen",
    "ja-stimme", "nein-stimme", "ja-stimmen", "nein-stimmen",
    "stimmenanteil",
    # Englisch
    "voted", "voting record", "parliamentary vote",
    "for the bill", "against the bill",
]

# Strikt-AT-Marker (Klubnamen sind genuin AT — kein DE/CH-Konflikt).
AT_MARKERS = [
    "österreich", "austria", "österreichisch",
    "nationalrat", "österreichisches parlament",
    "övp", "spö", "fpö", "neos", "grüne", "bzö",
    "kpö", "team stronach", "liste pilz", "jetzt", "mfg",
    "regierungsvorlage", "initiativantrag",
]

# Liste der bekannten Klub-Kürzel für die Klub-spezifische Trigger.
KNOWN_CLUBS = ("ÖVP", "SPÖ", "FPÖ", "NEOS", "GRÜNE", "BZÖ", "JETZT",
               "STRONACH")

# Stopwords für Betreff-Suche
_STOPWORDS = {
    "der", "die", "das", "und", "oder", "von", "zur", "zum", "des",
    "den", "dem", "ein", "eine", "einer", "eines", "für", "auf", "an",
    "in", "im", "mit", "ohne", "über", "unter", "bei",
    "österreich", "österreichs", "österreichisch", "österreichische",
    "österreichischen", "österreichischer",
    "bundesgesetz", "novelle", "änderung",
    "the", "and", "for", "with", "of", "on", "in", "to",
}


def _claim_mentions_voting(claim: str) -> bool:
    """True wenn der Claim auf Abstimmungsverhalten verweist."""
    cl = claim.lower()
    has_kw = any(kw in cl for kw in VOTING_KEYWORDS)
    if not has_kw:
        return False
    has_at = any(kw in cl for kw in AT_MARKERS)
    return has_at


def claim_mentions_voting_cached(claim: str) -> bool:
    """Sync trigger gate — keyword-only, no I/O."""
    return _claim_mentions_voting(claim)


def _load_static_json() -> list[dict] | None:
    if not os.path.exists(STATIC_JSON_PATH):
        logger.warning(
            f"Parlament Abstimmungen: static JSON not found at "
            f"{STATIC_JSON_PATH}"
        )
        return None
    try:
        with open(STATIC_JSON_PATH, encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"Parlament Abstimmungen: load failed: {e}")
        return None
    return payload.get("entries") or None


async def fetch_abstimmungen(
    client: httpx.AsyncClient | None = None,
) -> dict | None:
    """Static-first loader for parliament voting records."""
    global _cache, _cache_time
    now = time.time()
    if _cache is not None and (now - _cache_time) < CACHE_TTL:
        return _cache

    entries = _load_static_json()
    if not entries:
        logger.warning("Parlament Abstimmungen: no entries available")
        return None

    _cache = {
        "entries": entries,
        "fetched_at": now,
        "source": "static",
    }
    _cache_time = now
    logger.info(
        f"Parlament Abstimmungen: {len(entries)} entries cached "
        f"(source=static)"
    )
    return _cache


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


def _significant_words(text: str) -> set[str]:
    """Extract content words for word-overlap matching."""
    return {
        w for w in re.findall(r"[a-zäöüß0-9]{4,}", text.lower())
        if w not in _STOPWORDS
    }


def _extract_year(claim: str) -> int | None:
    """Extract the most recent year mentioned in the claim within
    coverage range (2017–2025)."""
    years = [int(y) for y in re.findall(r"\b(20[1-2]\d)\b", claim)]
    years = [y for y in years if 2017 <= y <= 2025]
    return years[-1] if years else None


def _extract_clubs_in_claim(claim: str) -> list[str]:
    """Return canonical club shorts mentioned in the claim."""
    cl = claim.lower()
    found: list[str] = []
    aliases = {
        "ÖVP": ("övp", "volkspartei"),
        "SPÖ": ("spö", "sozialdemokraten", "sozialdemokratisch"),
        "FPÖ": ("fpö", "freiheitliche"),
        "NEOS": ("neos",),
        "GRÜNE": ("grüne", "die grünen"),
        "BZÖ": ("bzö",),
        "JETZT": ("jetzt", "liste pilz"),
        "KPÖ": ("kpö",),
        "STRONACH": ("team stronach", "stronach"),
    }
    for short, als in aliases.items():
        if any(a in cl for a in als):
            found.append(short)
    return found


def _entry_matches_year(entry: dict, year: int | None) -> bool:
    if year is None:
        return True
    datum = entry.get("datum") or ""
    return datum.endswith(str(year))


def _score_entry(claim_words: set[str], entry: dict) -> int:
    """Word-overlap score between claim and entry betreff."""
    betreff_words = _significant_words(entry.get("betreff") or "")
    overlap = len(claim_words & betreff_words)
    return overlap


def _format_entry(entry: dict) -> dict:
    betreff = entry.get("betreff") or ""
    datum = entry.get("datum") or ""
    doktyp = entry.get("doktyp") or ""
    abst = entry.get("abstimmung_3l")
    dafuer = entry.get("dafuer") or []
    dagegen = entry.get("dagegen") or []
    text = entry.get("abstimmungstext") or ""
    url = entry.get("url") or "https://www.parlament.gv.at/"

    # display_value: kompakte Zusammenfassung
    pro = ", ".join(dafuer) if dafuer else "—"
    contra = ", ".join(dagegen) if dagegen else "—"
    status = "angenommen" if str(abst) == "1" else (
        "abgelehnt" if str(abst) == "0" else "—"
    )
    display_value = (
        f"{status} ({datum}) · Dafür: {pro} · Dagegen: {contra}"
    )

    # description: zusätzlicher Klartext-Kommentar wenn vorhanden
    descr_parts = [
        f"DOKTYP: {doktyp}" if doktyp else "",
        f"Abstimmungstext: {text}" if text else "",
    ]
    description = ". ".join(p for p in descr_parts if p)
    if not description:
        description = (
            "Klub-Abstimmungsverhalten laut Open-Data des österreichischen "
            "Parlaments."
        )

    # year-Feld für Synthesizer-Heuristik
    year = ""
    if datum and "." in datum:
        year = datum.split(".")[-1]

    return {
        "indicator_name": f"Nationalrats-Abstimmung {datum}: {betreff}",
        "indicator": "parl_voting",
        "country": "AUT",
        "country_name": "Österreich",
        "year": year,
        "value": status,
        "display_value": display_value,
        "description": description,
        "url": url,
    }


_METHODIK = (
    "Datenquelle: Parlament Österreich, Open-Data-Portal, Datensatz "
    "'Beschlüsse' (Filter-API ``data/101``). Pro Verhandlungsgegenstand "
    "werden die Klubs in 'Dafür' / 'Dagegen' als JSON-Listen ausgewiesen. "
    "Einschränkungen: "
    "(1) Nur Nationalrat — Bundesrat-Beschlüsse sind nicht abgedeckt. "
    "(2) Nur seit GP XXVI (2017) — ältere Beschlüsse sind beim Parlament "
    "vorhanden, aber nicht in diesem Snapshot. "
    "(3) DOKTYPs: Regierungsvorlagen (RV), Anträge (A), "
    "Berichte und Anträge aus Ausschüssen (BUA), Berichte (BRA). "
    "Andere Beschluss-Typen (z.B. Petitionen, Volksbegehren-Behandlungen) "
    "können fehlen. "
    "(4) Wir liefern nur die Stimmenverteilung, keine inhaltliche "
    "Bewertung der Gesetze und keine politische Einordnung des "
    "Klub-Verhaltens. "
    "(5) Bei '3. Lesung' — wenn ein Beschluss in mehreren Lesungen "
    "behandelt wird, zählt die finale 3. Lesung. Frühere Lesungs-"
    "Ergebnisse können abweichen."
)


async def search_abstimmungen(analysis: dict) -> dict:
    empty = {
        "source": "Parlament Abstimmungen",
        "type": "official_data",
        "results": [],
    }
    data = await fetch_abstimmungen()
    if not data:
        return empty
    entries = data.get("entries") or []
    if not entries:
        return empty

    claim = analysis.get("claim", "")
    if not _claim_mentions_voting(claim):
        return empty

    year = _extract_year(claim)
    clubs_in_claim = _extract_clubs_in_claim(claim)
    claim_words = _significant_words(claim)

    # 1) Year + Word-Overlap-Filter
    candidates = [e for e in entries if _entry_matches_year(e, year)]
    scored = [(e, _score_entry(claim_words, e)) for e in candidates]
    scored = [(e, s) for e, s in scored if s >= 1]
    scored.sort(key=lambda kv: -kv[1])

    # 2) Wenn der Claim einen einzelnen Klub nennt + ein generisches
    #    Wort wie "einzige" / "alleine" / "allein", filtere zusätzlich auf
    #    Beschlüsse, in denen dieser Klub *isoliert* dafür/dagegen war.
    cl = claim.lower()
    isolation_intent = any(p in cl for p in [
        "einzige", "alleinig", "alleine ", "als einzige",
        "only party", "sole opposition",
    ])

    final: list[dict] = []
    if isolation_intent and clubs_in_claim:
        target = clubs_in_claim[0]
        for e, s in scored:
            dafuer = set(e.get("dafuer") or [])
            dagegen = set(e.get("dagegen") or [])
            # Klub war einzige Gegenstimme
            if dagegen == {target}:
                final.append(e)
            # oder einzige Zustimmung
            elif dafuer == {target}:
                final.append(e)
            if len(final) >= 5:
                break

    if not final:
        # Standard: top-5 nach word-overlap
        final = [e for e, _s in scored[:5]]

    if not final:
        return empty

    results: list[dict] = [_format_entry(e) for e in final]

    # Methodik-Caveat
    results.append({
        "indicator_name": "WICHTIGER KONTEXT: Parlament-Abstimmungs-Daten",
        "indicator": "context",
        "country": "AUT",
        "country_name": "Österreich",
        "year": "",
        "value": "",
        "display_value": "",
        "description": _METHODIK,
        "url": "https://www.parlament.gv.at/recherchieren/open-data/",
    })

    return {
        "source": "Parlament Abstimmungen",
        "type": "official_data",
        "results": results,
    }
