"""Helper für Static-First-Topic-Services.

Die 10 Services aus den Cluster-A/B-Etappen (eu_courts, eu_crime,
energy_charts, medientransparenz, rki_surveillance, education_dach,
at_courts, oecd_health, housing_at, transport_at) folgen alle demselben
Pattern: Static-JSON-Load + Substring/Composite-Trigger-Match +
Reranker-Backup-Fallback. Dieses Modul kapselt die Gemeinsamkeit.

Verwendung in einem Service:

    from services._topic_match import (
        substring_or_composite_match,
        find_matching_items,
    )

    STATIC_JSON_PATH = os.path.join(..., "data", "oecd_health.json")

    def _descriptor(f: dict) -> tuple[dict, str]:
        head = f.get("headline", "")
        notes = " ".join((f.get("context_notes") or [])[:2])
        return (f, f"{head}. {notes}"[:300])

    def claim_mentions_oecd_health_cached(claim: str) -> bool:
        if not claim:
            return False
        return bool(find_matching_items(
            STATIC_JSON_PATH, "facts",
            claim_lc=claim.lower(), full_claim=claim,
            descriptor_fn=_descriptor,
        ))

`find_matching_items` returnt:
  - eine Liste der substring/composite-Treffer (wenn vorhanden), ODER
  - die Top-3 Reranker-Backup-Treffer mit Cosine ≥ threshold (0.45 default), ODER
  - eine leere Liste.

Service-spezifisch bleibt nur:
  - der `descriptor_fn` (welche Felder als Cosine-Repräsentation),
  - der `search_X`-Result-Builder (was das Display-Value pro Topic macht).
"""

import logging
import os
from typing import Callable

from services._static_cache import load_json_mtime_aware
from services._reranker_backup import best_matches as _backup_best_matches

logger = logging.getLogger("evidora")

DEFAULT_BACKUP_THRESHOLD = 0.45
DEFAULT_BACKUP_TOP_N = 3


def substring_or_composite_match(item: dict, claim_lc: str) -> bool:
    """Trifft das Item den Claim via Substring (any-of) ODER
    Composite-AND-of-OR-Tupel?

    Erwartet:
      - ``trigger_keywords``: Liste von Substring-Strings (any-of-Match)
      - ``trigger_composite``: Liste von Alternations-Listen — alle müssen
        mindestens einen Treffer haben (AND zwischen Listen, OR innerhalb)
      - ``trigger_all``: Optional, eine zweite Composite-Variante (Liste von
        Composite-Regeln, jeweils ihrerseits eine Liste von Alternations-
        Tupeln)
    """
    for kw in item.get("trigger_keywords") or ():
        if kw.lower() in claim_lc:
            return True
    composite = item.get("trigger_composite") or []
    if composite and all(
        isinstance(alt, (list, tuple)) and any(tok.lower() in claim_lc for tok in alt)
        for alt in composite
    ):
        return True
    # Optional 2. Pattern: Liste von Regeln, je AND-of-OR
    for rule in item.get("trigger_all") or ():
        if rule and all(
            isinstance(alt, (list, tuple)) and any(tok.lower() in claim_lc for tok in alt)
            for alt in rule
        ):
            return True
    return False


def find_matching_items(
    static_path: str,
    items_key: str,
    *,
    claim_lc: str,
    full_claim: str | None = None,
    descriptor_fn: Callable[[dict], tuple[dict, str]] | None = None,
    threshold: float = DEFAULT_BACKUP_THRESHOLD,
    top_n: int = DEFAULT_BACKUP_TOP_N,
) -> list[dict]:
    """Lade JSON aus ``static_path``, finde Items unter ``items_key``,
    die den Claim treffen — Substring/Composite zuerst, sonst Reranker-
    Backup-Fallback (Cosine ≥ threshold, top_n).

    Returns: Liste der gematchten Items (kann leer sein).
    """
    data = load_json_mtime_aware(static_path)
    if data is None:
        return []
    if items_key not in data:
        logger.warning(
            f"{os.path.basename(static_path)} missing '{items_key}' key"
        )
        return []
    items = data.get(items_key) or []
    matches = [it for it in items if substring_or_composite_match(it, claim_lc)]
    if matches:
        return matches
    if not full_claim or descriptor_fn is None:
        return []
    pairs = [descriptor_fn(it) for it in items]
    return _backup_best_matches(full_claim, pairs,
                                threshold=threshold, top_n=top_n)


def load_items(static_path: str, items_key: str) -> list[dict]:
    """Convenience: lade items aus dem Static-JSON. Für `fetch_X`-Funktionen."""
    data = load_json_mtime_aware(static_path)
    if data is None:
        return []
    return data.get(items_key) or []


# ===========================================================================
# Politik-Tabu-Guard (2.0-Härtung)
# ===========================================================================
#
# Pattern: Cross-Cluster-Trigger (CPI, V-Dem, Demokratie-Konsens) können
# unbeabsichtigt Politik-Tabu #1 verletzen, wenn sie auf Partei-Korruptions-
# Claims (z.B. "FPÖ hat höchste Korruption") feuern. Country-Level-Daten
# werden dann für Partei-Bewertung missbraucht — Kategorienverwechslung.
#
# Aber: Pauschal-Block ist zu strikt — empirisch-prüfbare Sub-Claims über
# konkrete Affären/Personen ("Strache Ibiza", "ÖVP-Chats 2021") müssen
# weiter triggern dürfen, weil sie methodisch valide prüfbar sind.
#
# Lösung: Differenzierte Detection. Block NUR wenn:
#   (1) Partei-Token + Korruptions-Token UND
#   (2) Normativer Superlativ/Vergleichs-Anspruch ohne empirische Skala UND
#   (3) KEIN konkreter Affären-/Personen-/Zahlen-Anker
#
# Sonst: pass — Service feuert normal.

_PARTY_TOKENS: tuple[str, ...] = (
    # AT-Parteien
    "fpö", "fpoe", "spö", "spoe", "övp", "oevp",
    "neos", "grüne ", "gruene ", "kpö", "kpoe", "bzö", "bzoe",
    "team-stronach", "team stronach",
    # DE-Parteien
    "afd", "cdu", "csu", "spd", "fdp", "linke", "die linke",
    "die grünen", "bsw", "republikaner",
    # AT-Politiker:innen (Spitzenkandidat:innen + Top-Funktionsträger)
    "kickl", "babler", "stocker", "kogler", "meinl-reisinger",
    "nehammer", "kurz", "van der bellen", "rendi-wagner",
    "strache",  # historisch relevant
)

_CORRUPTION_TOKENS: tuple[str, ...] = (
    "korruption", "korrupt", "skandal", "bestechung",
    "geldwäsche", "geldwaesche", "schmiergeld", "kickback",
    "vorteilsannahme", "untreue",
)

# Normative/Vergleichs-Superlative ohne empirische Mess-Skala
_SUPERLATIVE_TOKENS: tuple[str, ...] = (
    "höchste", "hoechste", "korrupteste", "schlimmste",
    "am korruptesten", "die meisten", "am meisten",
    "größte korruption", "groesste korruption",
    "am schlimmsten", "alle ", "jede partei", "jeder politiker",
)

# Konkrete Affären-/Personen-Anker (machen Claim faktisch-prüfbar)
_SPECIFIC_ANCHOR_TOKENS: tuple[str, ...] = (
    # Konkrete AT-Affären
    "ibiza", "casinos-affäre", "casinos affäre", "casinos-affaere",
    "övp-chats", "oevp-chats", "övp chats", "bvt-affäre", "bvt affäre",
    "inseratenaffäre", "inseratenaffaere",
    "telegram-chats", "kurz-chats",
    "buwog", "eurofighter", "hypo alpe adria", "hypo-alpe-adria",
    # DE-Affären
    "cum-ex", "cum ex", "maskenaffäre", "maskenaffaere",
    "wirecard", "amthor", "spahn-masken",
)

# Konkrete Personen-Anker — sind in PARTY_TOKENS schon enthalten,
# aber wir prüfen extra auf NICHT-Spitzenkandidaten-Ebene Affären-Akteure
_AFFAIR_PERSON_TOKENS: tuple[str, ...] = (
    "schmid", "sidlo", "blümel", "bluemel", "fellner",
    "grasser",  # BUWOG
    "amon", "pilnacek",
)

# Optional: numerische Anker (X Verfahren, Y Verurteilungen) — regex-Pattern
import re as _re
_NUMERIC_ANCHOR_RE = _re.compile(
    r"\b(\d{1,4})\s*(verfahren|verurteilung|anklage|prozess|"
    r"ermittlung|untersuchung|beschuldigt)",
    _re.IGNORECASE,
)


def politik_guard_action(claim_lc: str) -> str:
    """Politik-Tabu-Guard 2.0 (Lehrgeld 2026-05-17, FPÖ-Korruptions-Test).

    Returns:
      - "pass": Service darf normal feuern (Default, kein Politik-Tabu)
      - "block_country_sources": Service darf NICHT feuern, weil der Claim
        eine Partei-Korruptions-Aussage mit Superlativ-Anspruch ist UND
        keinen konkreten faktischen Anker hat. Country-Level-Quellen
        (CPI, WGI, V-Dem etc.) würden Kategorienfehler erzeugen.

    Wenn ein Service diese Funktion aufruft, soll er bei
    "block_country_sources" return False geben (kein Trigger).

    Hinweis: Wikipedia + Faktencheck-RSS-Services rufen die Guard NICHT auf
    — sie dürfen weiter feuern (Wikipedia-only-Cap-Pattern).
    """
    if not claim_lc:
        return "pass"

    has_party = any(tok in claim_lc for tok in _PARTY_TOKENS)
    has_corruption = any(tok in claim_lc for tok in _CORRUPTION_TOKENS)

    if not (has_party and has_corruption):
        return "pass"

    # Partei + Korruption — jetzt prüfen ob Superlativ ODER konkreter Anker
    has_superlative = any(tok in claim_lc for tok in _SUPERLATIVE_TOKENS)
    has_specific_anchor = (
        any(tok in claim_lc for tok in _SPECIFIC_ANCHOR_TOKENS)
        or any(tok in claim_lc for tok in _AFFAIR_PERSON_TOKENS)
        or bool(_NUMERIC_ANCHOR_RE.search(claim_lc))
    )

    # Superlativ-Quantifizierungs-Anspruch ohne empirischen Anker
    if has_superlative and not has_specific_anchor:
        return "block_country_sources"

    return "pass"


def is_party_corruption_superlative_claim(claim_lc: str) -> bool:
    """Convenience-Shortcut: Wahr, wenn die Guard "block_country_sources"
    zurückgeben würde. Pro Service Single-Line-Check:
        if is_party_corruption_superlative_claim(claim_lc): return False
    """
    return politik_guard_action(claim_lc) == "block_country_sources"
