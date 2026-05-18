"""W3C WCAG 2.2 — Web Content Accessibility Guidelines.

Datenquelle: W3C Recommendation "Web Content Accessibility Guidelines (WCAG) 2.2"
  Spec:        https://www.w3.org/TR/WCAG22/
  Repo:        https://github.com/w3c/wcag (Quelle aller SC-HTML-Fragmente
               unter guidelines/sc/22/*.html)
  Maschinen-JSON: https://raw.githubusercontent.com/w3c/wcag/main/guidelines/
                 act-mapping.json (W3C-eigene SC-Liste mit ACT-Rule-Mapping)

Lizenz: W3C Document License
  (https://www.w3.org/copyright/document-license/) — erlaubt Wiedergabe und
  Übersetzung unverändert/auszugsweise mit Quellenangabe. Evidora-tauglich.

Hybrid-Pack-Logik:
  1) Static-Fallback: ``data/wcag22.json`` mit allen 87 WCAG-2.2 SCs (Nr.,
     deutscher + englischer Name, Level A/AA/AAA, Prinzip, Kurz-Summary,
     EU/AT-Rechtsbezug). Wird bei Server-Start sofort geladen.
  2) Live-Augment: ``fetch_wcag22(client)`` zieht beim Startup
     act-mapping.json vom w3c/wcag-Repo (24 h-Cache). Damit verifizieren
     wir Konnektivität + reichern später optional ACT-Rule-Treffer an.
     Bei Netzwerk-Fehler bleibt der Static-Fallback aktiv.

Use-Cases:
  * "Was sagt WCAG 2.2 zu Success Criterion 1.4.3?"
  * "Ist eine Webseite AA-konform, wenn Kontrast 3:1 beträgt?"
  * "Welche neuen SCs kamen in WCAG 2.2 dazu?"
  * "Gilt WCAG 2.2 in Österreich verpflichtend?" (→ EU-RL 2016/2102 / EAA)
  * "barrierefreiheit web", "AAA-konform"

Politische Guardrails: Reine Standard-Wiedergabe. KEINE eigene Bewertung,
ob eine konkrete Seite konform ist (das wäre ein Audit-Urteil). Bei
Konformitäts-Claims liefern wir den SC-Wortlaut + Stand-Hinweis, nicht
ein Verdict.
"""

# WIRING für main.py (KEINE Aktion durch diesen Patch — bitte manuell ergänzen):
#
#   from services.wcag22 import (
#       search_wcag22,
#       claim_mentions_wcag22_cached,
#   )
#
#   if claim_mentions_wcag22_cached(claim):
#       tasks.append(cached("WCAG22", search_wcag22, analysis))
#       queried_names.append("W3C WCAG 2.2")
#
# WIRING für services/data_updater.py:
#
#   from services.wcag22 import fetch_wcag22
#   ...
#   await asyncio.gather(
#       ...
#       fetch_wcag22(client),
#   )
#
# WIRING für services/reranker.py (Whitelist der indicator-Keys):
#
#   "wcag22_sc",
#   "wcag22_overview",
#   "wcag22_legal_at_eu",

from __future__ import annotations

import json
import logging
import os
import re
import time

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Pfade & Konstanten
# ---------------------------------------------------------------------------
STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "wcag22.json",
)

# W3C-eigene maschinenlesbare Mapping-Datei. Wird via fetch_wcag22 alle 24 h
# refresht und neben dem Static-Pack im RAM gehalten. Falls Netzwerk down,
# bleibt search_wcag22 trotzdem voll funktionsfähig (Static-only).
W3C_ACT_MAPPING_URL = (
    "https://raw.githubusercontent.com/w3c/wcag/main/guidelines/act-mapping.json"
)

CACHE_TTL_S = 24 * 60 * 60  # 24h
LIVE_TIMEOUT_S = 20.0
MAX_RESULTS = 6

# Module-level caches
_static_cache: dict | None = None
_act_mapping_cache: dict | None = None
_act_mapping_cache_time: float = 0.0


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_WCAG22_TERMS = (
    # Direkt
    "wcag 2.2", "wcag2.2", "wcag-2.2", "wcag22",
    "wcag 2.1", "wcag 2.0",  # User kann Vorgängerversionen meinen
    "web content accessibility guidelines",
    "webrichtlinien", "web-content-accessibility",
    # Konformitätsstufen
    "aa-konform", "aa konform", "aaa-konform", "aaa konform",
    "level aa", "level aaa", "level a ",
    "konformitätsstufe a", "konformitätsstufe aa", "konformitätsstufe aaa",
    # Begriffe rund um Barrierefreiheit-Web
    "barrierefreiheit web", "barrierefreies web",
    "barrierefreiheit website", "barrierefreie website",
    "barrierefreie webseite", "barrierefrei im web",
    "web accessibility", "web-accessibility",
    "web zugänglichkeit", "web-zugänglichkeit", "web zugaenglichkeit",
    # EU/AT-Rechtsbezug
    "web-zugänglichkeits-gesetz", "wzg", "web zugänglichkeitsgesetz",
    "barrierefreiheitsgesetz", "bafg",
    "european accessibility act", "eaa",
    "eu accessibility directive", "eu-accessibility-directive",
    "richtlinie 2016/2102",
    "en 301 549",
    # SC-spezifische Marker
    "success criterion", "erfolgskriterium",
    "kontrastverhältnis", "kontrastverhaeltnis",
    "alt-text", "alt text", "alternativtext",
    "screenreader", "screen reader", "screen-reader",
    "tastaturbedienbar", "tastatur-bedienbar",
    "skip link", "skip-link",
    "fokus-indikator", "fokusindikator", "focus indicator",
    "aria-label", "aria label",
)

# Erkenne explizite SC-Nummern: "1.4.3", "SC 1.4.3", "Success Criterion 2.4.11"
_SC_REGEX = re.compile(
    r"(?:success\s+criterion|erfolgskriterium|sc|wcag)?\s*"
    r"\b([1-4])\.([1-9])\.(\d{1,2})\b",
    re.IGNORECASE,
)


def _claim_mentions_wcag22(claim_lc: str) -> bool:
    """Trigger-Pre-Check.

    True wenn:
      1. Direkter WCAG-/Barrierefreiheit-Web-Term im Claim.
      2. Explizite SC-Nummer (1.x.x bis 4.x.x) plus Accessibility-Kontext.
    """
    if not claim_lc:
        return False
    # 1) Direkt-Term
    if any(t in claim_lc for t in _WCAG22_TERMS):
        return True
    # 2) SC-Nummer + Web-/A11y-Kontext (sonst greift Pattern für "Artikel 1.4.3"
    #    in juristischen Texten — wir wollen nur Web-Accessibility).
    if _SC_REGEX.search(claim_lc):
        has_a11y_context = any(t in claim_lc for t in (
            "barrierefrei", "accessibility", "zugänglich", "zugaenglich",
            "screenreader", "screen reader", "alt-text", "alt text",
            "kontrast", "tastatur", "aria", "html", "website",
            "webseite", "web ", " web", "browser", "fokus", "focus",
        ))
        if has_a11y_context:
            return True
    return False


def claim_mentions_wcag22_cached(claim: str) -> bool:
    """Public-Wrapper für Trigger-Check (case-normalisiert)."""
    return _claim_mentions_wcag22((claim or "").lower())


# ---------------------------------------------------------------------------
# Static load
# ---------------------------------------------------------------------------
def _load_static_json() -> dict | None:
    global _static_cache
    if _static_cache is not None:
        return _static_cache
    try:
        with open(STATIC_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "success_criteria" not in data:
            logger.warning("wcag22.json missing 'success_criteria' key")
            return None
        _static_cache = data
        n = len(data.get("success_criteria") or [])
        logger.info(f"WCAG 2.2 data loaded: {n} Success Criteria")
        return _static_cache
    except FileNotFoundError:
        logger.warning(f"wcag22.json not found at {STATIC_JSON_PATH}")
        return None
    except Exception as e:
        logger.warning(f"wcag22.json load failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Live fetch (Hybrid-Pack-Augment)
# ---------------------------------------------------------------------------
async def fetch_wcag22(client=None) -> list[dict]:
    """Hybrid-Prefetch: lädt W3C act-mapping.json (24 h-Cache).

    Wird beim Server-Start (data_updater.prefetch_all) UND alle 24 h
    aufgerufen. Bei Netzwerk-Fehler bleibt der Static-Fallback aktiv —
    search_wcag22 funktioniert dann ohne Live-Daten weiter.

    Returns:
        Eine Liste mit dem Static-Pack-Dict, damit das Update-Pattern
        (`results = await asyncio.gather(... fetch_wcag22(client) ...)`)
        konsistent bleibt. Die Live-Augmentation passiert als Seiteneffekt
        im Modul-Cache (_act_mapping_cache).
    """
    global _act_mapping_cache, _act_mapping_cache_time

    # Static-Pack auf jeden Fall laden (idempotent).
    static = _load_static_json()

    now = time.time()
    if _act_mapping_cache is not None and (now - _act_mapping_cache_time) < CACHE_TTL_S:
        return [static] if static else []

    # Lazy import: polite_client liegt im selben Paket und muss nicht beim
    # Modul-Import ausgeführt werden.
    from services._http_polite import polite_client

    own_client = False
    if client is None:
        client = polite_client(timeout=LIVE_TIMEOUT_S)
        own_client = True

    try:
        resp = await client.get(W3C_ACT_MAPPING_URL, timeout=LIVE_TIMEOUT_S)
        if resp.status_code != 200:
            logger.info(
                f"WCAG 2.2 act-mapping fetch HTTP {resp.status_code} — "
                f"fallback to static-only"
            )
        else:
            try:
                payload = resp.json()
            except Exception as e:
                logger.info(f"WCAG 2.2 act-mapping JSON parse failed: {e}")
                payload = None
            if isinstance(payload, dict):
                _act_mapping_cache = payload
                _act_mapping_cache_time = now
                rules = payload.get("act-rules") or []
                logger.info(
                    f"WCAG 2.2 act-mapping cached: {len(rules)} ACT-Rules"
                )
    except Exception as e:
        logger.info(f"WCAG 2.2 act-mapping fetch failed (static-only ok): {e}")
    finally:
        if own_client:
            try:
                await client.aclose()
            except Exception:
                pass

    return [static] if static else []


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _principle_label(principle_id: str, principles: dict) -> str:
    return principles.get(principle_id, "—")


def _extract_explicit_sc_nums(claim: str) -> list[str]:
    """Extrahiere explizite SC-Nummern aus Claim (z. B. '1.4.3', 'SC 2.4.11')."""
    if not claim:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for m in _SC_REGEX.findall(claim):
        try:
            p, g, c = m
            key = f"{int(p)}.{int(g)}.{int(c)}"
        except (TypeError, ValueError):
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
        if len(out) >= 6:
            break
    return out


def _level_filter(claim_lc: str) -> str | None:
    """Wenn Claim explizit 'Level AA' / 'AAA-konform' nennt, filtern wir
    auf diese Konformitätsstufe."""
    if "aaa-konform" in claim_lc or "aaa konform" in claim_lc \
            or "level aaa" in claim_lc or "konformitätsstufe aaa" in claim_lc:
        return "AAA"
    if "aa-konform" in claim_lc or "aa konform" in claim_lc \
            or "level aa" in claim_lc or "konformitätsstufe aa" in claim_lc:
        return "AA"
    # 'level a' ist tricky (greift auch in "level alpha"), daher streng:
    if "level a " in claim_lc or "konformitätsstufe a " in claim_lc:
        return "A"
    return None


def _format_sc(sc: dict, principles: dict, source_url: str,
               source_label: str) -> dict | None:
    if not isinstance(sc, dict):
        return None
    num = sc.get("num")
    name = sc.get("name") or ""
    name_en = sc.get("name_en") or ""
    level = sc.get("level") or ""
    principle_id = str(sc.get("principle") or "")
    summary = (sc.get("summary") or "").strip()
    if not num:
        return None

    principle_label = _principle_label(principle_id, principles)

    display_value = (
        f"WCAG 2.2 — Success Criterion {num} '{name}' ({name_en}), "
        f"Level {level}, Prinzip {principle_id}: {principle_label}. "
        f"{summary}"
    )

    description_parts = [
        f"Original-Wortlaut der W3C-Empfehlung (verkürzt): {summary}",
        f"Konformitätsstufe: Level {level}",
        f"Prinzip {principle_id}: {principle_label}",
        "Lizenz: W3C Document License. Volltext siehe W3C-TR.",
    ]
    description = " — ".join(p for p in description_parts if p)[:1200]

    # Anker auf W3C-TR-Seite. WCAG-2.2-TR-IDs sind die englischen Namen in
    # Kebab-Case (z. B. "non-text-content"). Wir liefern den Spec-Anchor.
    anchor = _slugify(name_en) if name_en else ""
    url = f"{source_url}#{anchor}" if anchor else source_url

    return {
        "indicator_name": f"WCAG 2.2 SC {num}: {name}",
        "indicator": "wcag22_sc",
        "country": "INT",
        "country_name": "International (W3C-Standard)",
        "year": "2023",
        "value": level,
        "display_value": display_value,
        "description": description,
        "url": url,
        "source": source_label,
    }


def _slugify(text: str) -> str:
    """Sehr einfache Slug-Funktion für W3C-TR-Anker (Kebab-Case)."""
    if not text:
        return ""
    out = text.lower().strip()
    # entferne Klammer-Zusätze wie "(Minimum)"
    out = re.sub(r"\s*\([^)]*\)\s*", " ", out)
    # ersetze Nicht-Wort-Zeichen durch Bindestrich
    out = re.sub(r"[^a-z0-9]+", "-", out)
    out = out.strip("-")
    return out


def _keyword_match(sc: dict, claim_lc: str) -> int:
    """Heuristischer Score: wie gut passt ein SC zu dem Claim?

    Wir matchen claim-Tokens gegen den deutschen Namen + Summary.
    Höherer Score = bessere Relevanz; 0 = kein Treffer.
    """
    name = (sc.get("name") or "").lower()
    name_en = (sc.get("name_en") or "").lower()
    summary = (sc.get("summary") or "").lower()
    score = 0

    # Topical keywords mit Direkt-Mapping zu prominenten SCs.
    topical: tuple[tuple[tuple[str, ...], tuple[str, ...]], ...] = (
        (("kontrast", "contrast"), ("1.4.3", "1.4.6", "1.4.11")),
        (("alt-text", "alt text", "alternativtext", "alttext", "alternative text",
          "alternativ-text"),
         ("1.1.1",)),
        (("untertitel", "captions", "subtitle"), ("1.2.2", "1.2.4")),
        (("tastatur", "keyboard"), ("2.1.1", "2.1.2", "2.1.3")),
        (("fokus", "focus"), ("2.4.7", "2.4.11", "2.4.12", "2.4.13")),
        (("zielgröße", "zielgroesse", "target size", "tap target"),
         ("2.5.5", "2.5.8")),
        (("authentifizier", "authentication", "captcha", "passwort"),
         ("3.3.8", "3.3.9")),
        (("sprache der seite", "language of page", "lang attribute", "lang-attribut"),
         ("3.1.1", "3.1.2")),
        (("blink", "flash", "blitz", "epilep"), ("2.3.1", "2.3.2")),
        (("aria", "name role value", "name, rolle"), ("4.1.2", "4.1.3")),
        (("status", "live region"), ("4.1.3",)),
        (("skip link", "skip-link", "blöcke umgehen", "bloecke umgehen"),
         ("2.4.1",)),
        (("formular", "form", "label", "beschriftung"),
         ("3.3.2", "1.3.1", "4.1.2")),
        (("reflow", "umfließen", "umfliessen", "zoom", "200%"),
         ("1.4.10", "1.4.4")),
        (("redundant entry", "redundante eingab"), ("3.3.7",)),
        (("consistent help", "konsistente hilfe"), ("3.2.6",)),
        (("dragging", "ziehbewegung", "drag-and-drop", "drag and drop"),
         ("2.5.7",)),
    )
    num = sc.get("num") or ""
    for triggers, sc_nums in topical:
        if num in sc_nums and any(t in claim_lc for t in triggers):
            score += 5

    # Wortweise Heuristik (kurze Tokens werden ignoriert).
    tokens = [t for t in re.split(r"[\s,.!?:;()/]+", claim_lc) if len(t) >= 5]
    for tok in tokens:
        if tok in name or tok in name_en or tok in summary:
            score += 1
    return score


def _build_overview_result(data: dict) -> dict:
    """Generischer Übersichts-Eintrag — wird ausgeliefert, wenn der Claim
    WCAG/Barrierefreiheit ganz allgemein anspricht."""
    key_facts = data.get("key_facts") or {}
    total = key_facts.get("total_criteria", "?")
    new_in_22 = key_facts.get("new_in_2_2") or []
    obsolete = key_facts.get("obsoleted_in_2_2") or []

    display = (
        f"WCAG 2.2 (W3C-Empfehlung vom 05.10.2023, Erratum 12.12.2024): "
        f"{total} Success Criteria über 4 Prinzipien "
        f"(Wahrnehmbar, Bedienbar, Verständlich, Robust), je 3 "
        f"Konformitätsstufen (A, AA, AAA). 9 neue SCs gegenüber WCAG 2.1: "
        f"{', '.join(new_in_22)}. SC {', '.join(obsolete) or '—'} ist in "
        f"2.2 obsolet."
    )
    return {
        "indicator_name": "WCAG 2.2 — Überblick",
        "indicator": "wcag22_overview",
        "country": "INT",
        "country_name": "International (W3C-Standard)",
        "year": "2023",
        "value": str(total),
        "display_value": display,
        "description": (
            "Web Content Accessibility Guidelines 2.2 sind die aktuelle "
            "W3C-Empfehlung für barrierefreie Web-Inhalte. Aufgebaut nach "
            "den Prinzipien POUR (Perceivable, Operable, Understandable, "
            "Robust). 'A' = Mindestkonformität, 'AA' = Standard für "
            "behördliche Webauftritte in der EU (über EN 301 549), "
            "'AAA' = höchste Stufe (nicht für alle Inhalte erreichbar). "
            "Lizenz: W3C Document License."
        ),
        "url": data.get("source_url") or "https://www.w3.org/TR/WCAG22/",
        "source": data.get("source_label") or "W3C WCAG 2.2",
    }


def _build_legal_result(data: dict) -> dict:
    """Rechtsbezug AT/EU — wird zusätzlich ausgeliefert, wenn 'gesetz' /
    'verpflichtend' / EAA / WZG / BaFG im Claim auftaucht."""
    kf = data.get("key_facts") or {}
    eu_ref = kf.get("eu_legal_reference") or ""
    at_ref = kf.get("at_legal_reference") or ""
    return {
        "indicator_name": "WCAG 2.2 — Rechtsbezug EU/AT",
        "indicator": "wcag22_legal_at_eu",
        "country": "AUT",
        "country_name": "Österreich / EU",
        "year": "2025",
        "value": None,
        "display_value": (
            "Rechtlich verbindlich in EU ist EN 301 549 (referenziert "
            "aktuell WCAG 2.1 Level AA, nicht zwingend 2.2). "
            f"{eu_ref} {at_ref}".strip()
        ),
        "description": (
            f"EU: {eu_ref} || AT: {at_ref} || Disclaimer: Diese "
            "Auskunft ersetzt keine rechtliche Beratung; bei "
            "Konformitäts-Streitfragen ist eine spezialisierte Stelle "
            "(z. B. Bundes-Behindertenanwaltschaft, Behindertenanwalt) "
            "die richtige Anlaufstelle."
        ),
        "url": data.get("source_url") or "https://www.w3.org/TR/WCAG22/",
        "source": data.get("source_label") or "W3C WCAG 2.2",
    }


def _select_results(data: dict, claim_lc: str) -> list[dict]:
    scs = data.get("success_criteria") or []
    principles = data.get("principles") or {}
    src_url = data.get("source_url") or "https://www.w3.org/TR/WCAG22/"
    src_label = data.get("source_label") or "W3C WCAG 2.2"

    results: list[dict] = []
    seen_nums: set[str] = set()

    # 1) Explizite SC-Nummern aus Claim
    explicit = _extract_explicit_sc_nums(claim_lc)
    by_num = {sc.get("num"): sc for sc in scs if isinstance(sc, dict)}
    for num in explicit:
        sc = by_num.get(num)
        if not sc:
            continue
        r = _format_sc(sc, principles, src_url, src_label)
        if r and num not in seen_nums:
            seen_nums.add(num)
            results.append(r)
            if len(results) >= MAX_RESULTS:
                return results

    # 2) Level-Filter ('aa-konform' → nur Level-AA-SCs als Heuristik-Pool)
    level = _level_filter(claim_lc)
    candidate_pool = scs
    if level:
        candidate_pool = [sc for sc in scs if isinstance(sc, dict)
                          and sc.get("level") == level]

    # 3) Keyword-Heuristik über (gefilterten) Pool
    scored: list[tuple[int, dict]] = []
    for sc in candidate_pool:
        if not isinstance(sc, dict):
            continue
        if sc.get("num") in seen_nums:
            continue
        score = _keyword_match(sc, claim_lc)
        if score > 0:
            scored.append((score, sc))
    scored.sort(key=lambda kv: kv[0], reverse=True)
    for _score, sc in scored:
        num = sc.get("num")
        r = _format_sc(sc, principles, src_url, src_label)
        if r and num not in seen_nums:
            seen_nums.add(num)
            results.append(r)
            if len(results) >= MAX_RESULTS:
                break

    # 4) Generischer Überblick — wenn nichts spezifisches getroffen wurde
    if not results:
        results.append(_build_overview_result(data))

    # 5) Rechtsbezug zusätzlich, wenn Claim juristische Marker hat
    if any(t in claim_lc for t in (
        "verpflicht", "gesetz", "richtlinie", "wzg", "bafg",
        "barrierefreiheitsgesetz", "european accessibility act", "eaa",
        "en 301 549", "directive", "behindertenanwalt",
    )) and len(results) < MAX_RESULTS:
        results.append(_build_legal_result(data))

    return results[:MAX_RESULTS]


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_wcag22(analysis: dict) -> dict:
    """Static-first WCAG-2.2-Lookup mit Hybrid-Live-Augment.

    Returns dict mit ``{"source": "WCAG 2.2", "type": "accessibility_standard",
    "results": [...]}``.
    """
    empty = {
        "source": "WCAG 2.2",
        "type": "accessibility_standard",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or ""
    original = analysis.get("original_claim") or claim
    if not isinstance(claim, str):
        claim = str(claim or "")
    if not isinstance(original, str):
        original = str(original or "")
    combined = f"{original} {claim}".strip()
    combined_lc = combined.lower()

    if not _claim_mentions_wcag22(combined_lc):
        return empty

    data = _load_static_json()
    if not data:
        return empty

    results = _select_results(data, combined_lc)
    if not results:
        return empty

    logger.info(f"WCAG 2.2: {len(results)} Treffer geliefert")
    return {
        "source": "WCAG 2.2",
        "type": "accessibility_standard",
        "results": results,
    }
