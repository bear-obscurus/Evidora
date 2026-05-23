"""Wikipedia "List of withdrawn drugs" als Static-JSON-Pack-Service.

Komplementär zu ``services/ema.py``: EMA-CSV deckt nur centralized-
procedure-Medikamente ab und verliert national-zugelassene Sicherheits-
Withdrawals wie Mediator (FR 2009), Vioxx (worldwide 2004 — als
Beispiel auch im Snapshot enthalten), Avandia (Europe 2010), Trasylol
(US 2008). Die Wikipedia-Liste (`en.wikipedia.org/wiki/
List_of_withdrawn_drugs`) ist die größte öffentliche, lizenzkonforme
(CC-BY-SA 4.0) Quelle für historische Marktrückzüge.

Pattern: Static-JSON-Pack-Service nach ARCHITECTURE.md §3.5, analog
``services/ema.py`` (Score-basiertes Matching) + ``services/
basg.py`` (Risiko-Items-Display).

Stichtagsbezug-Schutz: jeder Treffer aus dem Datensatz signalisiert
einen historischen Marktrückzug — Präsens-Aussagen wie "X ist
EU-zugelassen" sind ohne neuere Wiederzulassungs-Quelle nicht
zutreffend. Pattern aus lessons_learned.md (Synthesizer-Inversions-
Falle, Stichtagsbezug-Schutz) analog ``services/ema.py`` und
``services/wikidata.py`` ``_struct_marker(kind="amt")``.

Quellen-Lizenz: CC-BY-SA 4.0 verlangt Attribution + Permalink — der
Service liefert beides in ``display_value`` und ``url``.
"""

from __future__ import annotations

import json
import logging
import os
import re

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "withdrawn_drugs.json",
)

_cache: dict | None = None


def _load() -> dict:
    """Lazy-Load des statischen JSON. Re-Load nur bei expliziter
    Cache-Invalidation (kein Auto-Refresh, Wikipedia-Liste ändert sich
    selten — Monatlich-Refresh kann via data_updater erfolgen)."""
    global _cache
    if _cache is None:
        try:
            with open(STATIC_JSON_PATH, encoding="utf-8") as f:
                _cache = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            logger.warning(f"withdrawn_drugs: load failed: {e}")
            _cache = {"items": []}
    return _cache


def _word_match(term: str, text: str) -> bool:
    """Word-boundary case-insensitive substring match."""
    if not term or not text:
        return False
    return bool(
        re.search(r"\b" + re.escape(term) + r"\b", text, re.IGNORECASE)
    )


# Drug-Domain-Trigger: Claim muss ein Drug-Kontext-Wort enthalten,
# damit der Service feuert (sonst false-positive bei jedem Eigennamen
# der zufällig einem Wirkstoff ähnelt).
_DRUG_CONTEXT = re.compile(
    # Stem-based (no trailing word-boundary) damit Konjugationen +
    # Komposita matchen ("zugelassenes Schmerzmittel", "verschreibt",
    # "Marktrückzug", "Nebenwirkungen").
    r"\b("
    r"medikament|arzneimittel|wirkstoff|tablette|pille|"
    r"impfstoff|antibiotik|chemother|infus|spritz|"
    r"verschrieb|verschreib|zugelass|zulassung|"
    r"rückgezog|zurückgezog|rueckgezog|zurueckgezog|"
    r"vom markt|marktrückzug|marktrueckzug|"
    r"medic|drug|pharma|prescription|withdrawn|recall|"
    r"side effect|nebenwirkung|hepatotox|cardiotox|"
    r"schmerzmittel|antidiabetik|diabetes|antifibrinolyt"
    r")",
    re.IGNORECASE,
)


def _score_match(term: str, item: dict) -> int:
    """Score how well a term matches an entry.
    Returns 0 = no match, positive = better match.
    3 = exact INN/trade-name match (most specific)
    2 = wikilinked INN-stem match
    """
    inn = item.get("inn", "") or ""
    trade = item.get("trade_name", "") or ""
    if _word_match(term, inn) or _word_match(term, trade):
        return 3
    if len(term) >= 5 and (term.lower() in inn.lower() or term.lower() in trade.lower()):
        return 2
    return 0


def claim_mentions_withdrawn_drugs_cached(claim: str) -> bool:
    """Schneller Trigger für main.py Pipeline-Routing.

    Liefert True wenn:
    1. Claim enthält Drug-Kontext (medikament/zugelassen/etc.) — sonst
       wären zufällige Wirkstoff-Namens-Hits zu breit, und
    2. mindestens ein Datensatz-Eintrag matcht als word-boundary.
    """
    if not claim:
        return False
    if not _DRUG_CONTEXT.search(claim):
        return False
    items = _load().get("items", [])
    for it in items:
        inn = it.get("inn", "") or ""
        trade = it.get("trade_name", "") or ""
        if inn and _word_match(inn, claim):
            return True
        if trade and _word_match(trade, claim):
            return True
    return False


async def search_withdrawn_drugs(analysis: dict) -> dict:
    """Suche im Wikipedia-Withdrawn-Drugs-Snapshot nach Entity-Namen.

    Liefert pro Treffer einen ``display_value`` mit STRUKTURELL-
    FALSCH-Prefix — das signalisiert dem Synthesizer-Prompt, dass
    "X ist EU-zugelassen"-Claims gegen einen historischen Rückzug zu
    differenzieren sind.
    """
    empty = {
        "source": "Wikipedia: Liste zurückgezogener Medikamente (CC-BY-SA 4.0)",
        "type": "drug_safety_history",
        "results": [],
    }
    entities = (analysis or {}).get("entities") or []
    if not entities:
        return empty

    payload = _load()
    items = payload.get("items", [])
    if not items:
        return empty

    # Filter: skip very-short terms to avoid spurious matches.
    search_terms = [e for e in entities if isinstance(e, str) and len(e) >= 4]
    if not search_terms:
        return empty

    scored: list[tuple[int, dict]] = []
    for it in items:
        best = 0
        for t in search_terms:
            best = max(best, _score_match(t, it))
        if best > 0:
            scored.append((best, it))

    if not scored:
        return empty

    # Sort by score desc, take top 5
    scored.sort(key=lambda x: x[0], reverse=True)
    out_items: list[dict] = []
    for _, it in scored[:5]:
        inn = it.get("inn", "?")
        trade = it.get("trade_name", "")
        year = it.get("withdrawal_year") or it.get("withdrawal_year_raw") or "?"
        country = it.get("country", "?")
        reason = (it.get("reason") or "")[:240]
        title = f"{inn}" + (f" ({trade})" if trade else "")
        # Permalink zur Wikipedia-Liste (CC-BY-SA-Attribution)
        source_url = payload.get(
            "source_url",
            "https://en.wikipedia.org/wiki/List_of_withdrawn_drugs",
        )
        # STRUKTURELL-FALSCH-Prefix: historischer Rückzug ist
        # authoritative counter-evidence für "X ist zugelassen"-Claims.
        display_value = (
            f"STRUKTURELL FALSCH: {title} wurde laut Wikipedia-Liste "
            f"zurückgezogener Medikamente {year} in {country} vom Markt "
            f"genommen — Grund: {reason if reason else 'siehe Wikipedia-Quelle'}. "
            f"Präsens-Aussagen '{inn} ist (EU-/national) zugelassen' / "
            f"'wird verschrieben' sind ohne neuere Wiederzulassungs-Quelle "
            f"nicht zutreffend. Quelle: Wikipedia (CC-BY-SA 4.0)."
        )
        out_items.append({
            "indicator_name": title,
            "indicator": "withdrawn_drug",
            "country": country,
            "year": str(year),
            "topic": "withdrawn_drug_history",
            "display_value": display_value,
            "description": f"Marktrückzug {year} in {country}. Grund: {reason}",
            "url": source_url,
            "secondary_url": payload.get("license_url", ""),
            "source": "Wikipedia: Liste zurückgezogener Medikamente (CC-BY-SA 4.0)",
        })

    return {
        "source": "Wikipedia: Liste zurückgezogener Medikamente (CC-BY-SA 4.0)",
        "type": "drug_safety_history",
        "results": out_items,
    }
