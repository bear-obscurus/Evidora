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
