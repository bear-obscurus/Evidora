"""Constitute Project Live-Connector — Verfassungstexte weltweit (UT Austin).

Die "Constitute Project"-Datenbank (Comparative Constitutions Project,
University of Texas at Austin / Hosted by ConstitutionNet) sammelt die
Volltexte praktisch aller Verfassungen weltweit (~200 in-force +
historisch). Ideal für AT-Faktencheck-Bedarf:

- "B-VG Artikel X sagt …"
- "Im deutschen Grundgesetz steht …"
- "Die US-Verfassung garantiert …"
- "Verfassung von [Land] verbietet/erlaubt …"

Komplementär zu existierenden Quellen:
- RIS (#recht_pack): nur AT-Bundesrecht, eingespiegeltes B-VG, primär.
- Wikipedia: Sekundär-Beschreibung von Verfassungen.
- Constitute: kuratierte, vergleichende Metadaten + Volltext-PDF jeder
  Verfassung weltweit, Topic-Indizes (z. B. „Constitution amendment
  procedure", „Citizenship"). Wir nutzen primär den Metadata-Endpoint
  (Versionen / Stand / Wortzahl / PDF-URL) — der Volltext-Lookup ist
  PDF und damit kein Pre-Parse-Kandidat.

API (anonym, frei, kein Key nötig):
- Base: https://www.constituteproject.org/service/
- Endpoint /constitutions       → Meta-Liste aller Verfassungen
- Endpoint /constitutions?country=Austria        → Land-Filter
- Endpoint /constitutions?cons_id=Austria_2013   → konkrete Version
- Endpoint /topics              → Topic-Taxonomie (~400 Topics)
- PDF-URL  https://www.constituteproject.org/constitution/{id}.pdf
- HTML-URL https://www.constituteproject.org/constitution/{id}
- Lizenz: Constitute-Texte sind frei (CC0-vergleichbar, "explicitly
  free to share and reuse" — siehe constituteproject.org/about).
- Rate-Limit: nicht offiziell dokumentiert; wir cachen 24 h.

Strategie:
- Trigger-Match (Verfassung/Grundgesetz/Constitution + Land oder direkt
  Constitute / B-VG-Artikel-Form).
- Politik-Tabu-Guard 2.0: Partei + Korruption + Superlativ → skip.
  (Verfassungs-Metadaten haben sonst Kategorienfehler-Risiko.)
- Country-Extraktion → /constitutions?country=… → max 3 Treffer
  (in-force + zwei historisch interessante).
- Result: Titel, Jahre (Enacted / Reinstated / Revised), Wortzahl,
  PDF-/HTML-Link, „in_force"-Flag.

Politische Guardrails: Reine Verfassungs-Metadaten + Volltext-Links.
Keine eigene normative Wertung. Bei Partei-Korruptions-Superlativen
über _topic_match.is_party_corruption_superlative_claim blockieren.

# WIRING für main.py:
# from services.constitute import (
#     search_constitute, claim_mentions_constitute_cached,
# )
# if claim_mentions_constitute_cached(claim):
#     tasks.append(cached("Constitute", search_constitute, analysis))
#     queried_names.append("Constitute")
#
# WIRING für reranker.py (Indicator-Whitelist):
#   "constitute_" als Whitelist-Prefix in INDICATOR_WHITELIST_PREFIXES
#
# data_updater.py: KEIN Prefetch (Live-Only, kein Static-Pack)
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from functools import lru_cache

from services._http_polite import polite_client
from services._topic_match import is_party_corruption_superlative_claim

logger = logging.getLogger("evidora")

CONSTITUTE_BASE = "https://www.constituteproject.org"
CONSTITUTIONS_URL = f"{CONSTITUTE_BASE}/service/constitutions"

HTTP_TIMEOUT_S = 20.0
RESULT_LIMIT = 3

# In-Memory-Cache: term → (timestamp, result-dict)
_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = 24 * 3600.0  # 24 h


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
# Explizite Quelle-Mentions
_EXPLICIT_TERMS = (
    "constitute project", "constituteproject",
    "comparative constitutions project",
)

# Generische Verfassungs-Begriffe (in Composite-Logik gebraucht)
_CONSTITUTION_TERMS = (
    "verfassung", "verfassungstext", "verfassungsartikel",
    "verfassungsbestimmung", "verfassungs­recht",
    "verfassungsrechtlich",
    "constitution", "constitutional",
    "grundgesetz", "gg artikel", "gg art.", "gg art ",
    "b-vg", "bvg artikel", "bvg art.", "bvg art ",
    "bundesverfassungsgesetz",
    "staatsgrundgesetz", "stgg",
    "us-verfassung", "amerikanische verfassung",
    "französische verfassung", "italienische verfassung",
    "schweizer bundesverfassung", "bundesverfassung schweiz",
)

# Trigger-RegEx: "B-VG Artikel 7" / "GG Art. 1" / "Article 14 US Constitution"
_ARTICLE_REF_RE = re.compile(
    r"\b(b[-\s]?vg|gg|grundgesetz|stgg|us[-\s]?verfassung|"
    r"verfassung|constitution)\s+(?:art(?:ikel|\.)?|article)\s*\d+",
    re.IGNORECASE,
)

# Länder-/Region-Hints für Composite-Trigger
_COUNTRY_HINTS = (
    "österreich", "austria", "österreichisch",
    "deutschland", "germany", "deutsch",
    "schweiz", "switzerland",
    "usa", "united states", "amerika",
    "frankreich", "france", "französisch",
    "italien", "italy", "italienisch",
    "spanien", "spain",
    "polen", "poland",
    "ungarn", "hungary",
    "türkei", "turkey",
    "ukraine",
    "russland", "russia",
    "china", "japan", "indien", "india",
    "brasilien", "brazil",
)


def _has_any(claim_lc: str, terms: tuple[str, ...]) -> bool:
    return any(t in claim_lc for t in terms)


def _claim_mentions_constitute(claim_lc: str) -> bool:
    """Trigger-Check für Constitute-Lookup.

    True bei:
    - Politik-Tabu-Guard 2.0: Partei + Korruption + Superlativ ohne
      konkreten Anker → False (skip).
    - Expliziter Mention ("Constitute Project", …)
    - Konkrete Artikel-Referenz ("B-VG Artikel 7", "GG Art. 1", …)
    - Verfassungs-Term + Länder-Hint
    - Spezial-Lemmata wie "Grundgesetz" oder "B-VG" allein (impliziert DE/AT)
    """
    if not claim_lc:
        return False

    # 0) Politik-Tabu-Guard 2.0 — Partei+Korruption+Superlativ → skip
    if is_party_corruption_superlative_claim(claim_lc):
        return False

    # 1) Explizite Quelle
    if _has_any(claim_lc, _EXPLICIT_TERMS):
        return True

    # 2) Artikel-Referenz wie "B-VG Artikel 7"
    if _ARTICLE_REF_RE.search(claim_lc):
        return True

    # 3) Spezial-Lemmata, die schon eine Verfassung benennen
    for self_country in (
        "grundgesetz", "b-vg", "bundesverfassungsgesetz",
        "staatsgrundgesetz", "stgg",
        "us-verfassung", "amerikanische verfassung",
        "schweizer bundesverfassung", "bundesverfassung schweiz",
    ):
        if self_country in claim_lc:
            return True

    # 4) Generischer Verfassungs-Term + Länder-Hint
    has_const = _has_any(claim_lc, _CONSTITUTION_TERMS)
    has_country = _has_any(claim_lc, _COUNTRY_HINTS)
    if has_const and has_country:
        return True

    return False


@lru_cache(maxsize=2048)
def claim_mentions_constitute_cached(claim: str) -> bool:
    """LRU-gecachter Trigger-Check (Hot-Path-friendly)."""
    return _claim_mentions_constitute((claim or "").lower())


# ---------------------------------------------------------------------------
# Country-Detection
# ---------------------------------------------------------------------------
# Mapping: claim-lc-Token → Constitute country-Parameter.
#
# WICHTIG: Die Constitute-API erwartet den `country_id`-Wert (mit Unter-
# strichen, ggf. `__the`-Suffix). Frühere Versionen nutzten die Anzeige-
# Variante "United States of America" (mit Leerzeichen) — das liefert
# 0 Treffer (verifiziert 2026-05-20). Korrekt: "United_States_of_America"
# bzw. "Russian_Federation__the". Single-Word-Länder (Austria, Germany,
# France, …) sind in beiden Schreibweisen identisch.
_COUNTRY_MAP: tuple[tuple[tuple[str, ...], str], ...] = (
    (("österreich", "austria", "österreichisch",
      "b-vg", "bundesverfassungsgesetz", "staatsgrundgesetz", "stgg"),
     "Austria"),
    (("deutschland", "germany", "deutsch", "grundgesetz", "gg art"),
     "Germany"),
    (("schweiz", "switzerland", "schweizer bundesverfassung",
      "bundesverfassung schweiz"),
     "Switzerland"),
    (("usa", "united states", "us-verfassung", "amerikanische verfassung",
      "us constitution", "u.s. constitution",
      "bill of rights", "first amendment", "second amendment",
      "fourteenth amendment", "fifth amendment"),
     "United_States_of_America"),
    (("frankreich", "france", "französisch"), "France"),
    (("italien", "italy", "italienisch"), "Italy"),
    (("spanien", "spain"), "Spain"),
    (("polen", "poland"), "Poland"),
    (("ungarn", "hungary"), "Hungary"),
    (("türkei", "turkey"), "Turkey"),
    (("ukraine",), "Ukraine"),
    (("russland", "russia"), "Russian_Federation__the"),
    (("china",), "China"),
    (("japan",), "Japan"),
    (("indien", "india"), "India"),
    (("brasilien", "brazil"), "Brazil"),
)


def _extract_country(claim_lc: str) -> str | None:
    """Wähle Land für Constitute-API. Fallback: Austria, wenn nur B-VG-
    Lemma im Claim ist und Default-Land Sinn ergibt."""
    if not claim_lc:
        return None
    for tokens, country in _COUNTRY_MAP:
        if _has_any(claim_lc, tokens):
            return country
    return None


# ---------------------------------------------------------------------------
# HTTP-Lookup
# ---------------------------------------------------------------------------
async def _run_constitutions_lookup(
    client, country: str
) -> list[dict] | None:
    """Holt die Verfassungs-Meta-Liste für ein Land. None bei Fehler."""
    try:
        resp = await client.get(
            CONSTITUTIONS_URL,
            params={"country": country},
            follow_redirects=True,
        )
        if resp.status_code != 200:
            logger.debug(
                f"Constitute HTTP {resp.status_code} "
                f"(body: {resp.text[:120]!r})"
            )
            return None
        data = resp.json()
        if not isinstance(data, list):
            logger.debug(f"Constitute unexpected payload: {type(data)}")
            return None
        return data
    except Exception as e:
        logger.debug(f"Constitute fetch failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _trim(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 1].rstrip() + "…"


def _safe_id_suffix(cons_id: str) -> str:
    cleaned = (cons_id or "").strip().lstrip("/").replace("/", "_")
    safe = "".join(c if c.isalnum() or c == "_" else "_" for c in cleaned)
    return safe[:80] or "unknown"


def _build_result_row(item: dict) -> dict | None:
    cons_id = (item.get("id") or "").strip()
    if not cons_id:
        return None

    title = (item.get("title") or item.get("title_short") or cons_id).strip()
    title_long = (item.get("title_long") or "").strip()
    country = (item.get("country") or item.get("country_id") or "—").strip()
    region = (item.get("region") or "").strip()
    year_enacted = item.get("year_enacted") or ""
    year_reinstated = item.get("year_reinstated") or ""
    year_revised = item.get("year_revised") or item.get("year_updated") or ""
    word_length = item.get("word_length") or 0
    in_force = bool(item.get("in_force"))
    is_historic = bool(item.get("is_historic"))
    years_amended = item.get("years_amended") or 0

    status_label = "in Kraft" if in_force else (
        "historisch" if is_historic else "nicht in Kraft"
    )

    # Display-Headline
    year_bits = []
    if year_enacted:
        year_bits.append(f"Erlassen {year_enacted}")
    if year_reinstated:
        year_bits.append(f"wiedererlassen {year_reinstated}")
    if year_revised and year_revised != year_enacted:
        year_bits.append(f"revidiert {year_revised}")
    year_str = ", ".join(year_bits) or "—"

    base_display = (
        f"{title_long or title} — {country}; {year_str}; "
        f"{status_label}; {word_length:,} Wörter; "
        f"{years_amended} Änderungs-Jahre."
    )
    # Stichtagsbezug-Schutz: historische / nicht mehr in Kraft befindliche
    # Verfassungen (z.B. Weimarer RV, DDR-Verfassung, UdSSR-Verfassung)
    # dürfen nicht als aktuell gültige Rechtsgrundlage vom Synthesizer
    # zitiert werden. STRUKTURELL-FALSCH-Marker bei is_historic / not
    # in_force. Pattern: lessons_learned.md Synthesizer-Inversions-Falle.
    if is_historic or not in_force:
        display_value = _trim(
            f"STRUKTURELL FALSCH: Die Verfassung '{title}' ({country}) "
            f"ist '{status_label}' — sie ist NICHT die aktuell gültige "
            f"Verfassung dieses Landes. Präsens-Aussagen 'X ist die "
            f"Verfassung von {country}' / 'Y gilt in {country}' sind "
            f"für diesen Verfassungstext nicht zutreffend. "
            f"Roh-Daten: {base_display}",
            500,
        )
    else:
        display_value = _trim(base_display, 320)

    description = _trim(
        f"Constitute-Project-Eintrag (Comparative Constitutions Project, "
        f"UT Austin). Region: {region or '—'}. PDF-Volltext + interaktive "
        f"Topic-Suche unter den verlinkten URLs verfügbar. Lizenz frei zur "
        f"Weiternutzung. Sekundärquelle für Verfassungs-Faktencheck — "
        f"primärer Rechtsbestand (z. B. AT B-VG) bleibt RIS/offizielles "
        f"Gesetzblatt.",
        500,
    )

    html_url = f"{CONSTITUTE_BASE}/constitution/{cons_id}"
    pdf_url = f"{CONSTITUTE_BASE}/constitution/{cons_id}.pdf"

    return {
        "indicator_name": f"Verfassung: {title}",
        "indicator": f"constitute_{_safe_id_suffix(cons_id)}",
        "country": country,
        "country_name": country,
        "year": str(year_revised or year_enacted or ""),
        "value": word_length or None,
        "display_value": display_value,
        "description": description,
        "url": html_url,
        "pdf_url": pdf_url,
        "source": "Constitute Project (UT Austin, frei)",
    }


# ---------------------------------------------------------------------------
# Kuratierte Sub-Topics (Bill of Rights & Co.)
# ---------------------------------------------------------------------------
# Konstellation: Claim erwähnt eine bekannte Verfassungs-Teilmenge (z. B.
# „Bill of Rights" = 1.–10. Amendment der US-Verfassung von 1791). Die
# Constitute-API liefert immer nur die aktuell konsolidierte Fassung, in
# der die Bill of Rights bereits als Amendments I–X eingebaut ist. Damit
# der Faktencheck den Sub-Topic-Treffer sichtbar macht, prependen wir bei
# Match eine kuratierte Referenz-Zeile (Quelle: NARA / Constitute, beide
# frei). Keine eigene normative Wertung — reine Metadaten + Quell-Link.
_SUBTOPIC_BILL_OF_RIGHTS_TERMS = (
    "bill of rights", "first amendment", "second amendment",
    "fourth amendment", "fifth amendment", "sixth amendment",
    "eighth amendment", "tenth amendment", "fourteenth amendment",
    "amendment i", "amendment ii", "amendment iv", "amendment v",
)


def _bill_of_rights_enrichment(matchable_lc: str) -> dict | None:
    """Erzeugt einen kuratierten Sub-Topic-Eintrag für die US-Bill-of-Rights,
    wenn der Claim einen einschlägigen Term enthält. Sonst None."""
    if not _has_any(matchable_lc, _SUBTOPIC_BILL_OF_RIGHTS_TERMS):
        return None
    display_value = _trim(
        "US Bill of Rights — Amendments I–X zur US-Verfassung von 1789, "
        "in Kraft seit 15.12.1791. Enthält u. a. Religions-/Rede-/Presse-"
        "/Versammlungs-Freiheit (I), Recht auf Waffenbesitz (II), Schutz "
        "vor unangemessenen Durchsuchungen (IV), Selbstbelastungs-Verbot "
        "und Due-Process (V), faires Verfahren (VI), Verbot grausamer "
        "Strafen (VIII), Vorbehalt nicht-aufgezählter Rechte beim Volk/"
        "den Staaten (IX/X). Vollständig in der konsolidierten Fassung "
        "der Constitute-Project-Version 1789 (rev. 1992) enthalten.",
        500,
    )
    description = _trim(
        "Sub-Topic-Referenz auf die ersten zehn Amendments der US-"
        "Verfassung (ratifiziert 15.12.1791). Die Constitute-API gibt "
        "die konsolidierte Fassung mit allen 27 Amendments aus — die "
        "Bill of Rights bildet darin Amendments I–X. Primärquelle (offiziell, "
        "frei): National Archives (NARA). Sekundär: Constitute Project (UT "
        "Austin, frei zur Weiternutzung).",
        500,
    )
    return {
        "indicator_name": "Verfassung: US Bill of Rights (Amendments I–X)",
        "indicator": "constitute_us_bill_of_rights",
        "country": "United States of America",
        "country_name": "United States of America",
        "year": "1791",
        "value": 10,
        "display_value": display_value,
        "description": description,
        "url": (
            "https://www.constituteproject.org/constitution/"
            "United_States_of_America_1992"
        ),
        "pdf_url": (
            "https://www.archives.gov/founding-docs/bill-of-rights-transcript"
        ),
        "source": "Constitute Project + NARA (US Bill of Rights, frei)",
    }


def _select_top_constitutions(items: list[dict]) -> list[dict]:
    """Bevorzuge in-force-Verfassung, danach 2 zeitgeschichtlich
    interessante (neueste historische)."""
    if not items:
        return []
    in_force = [x for x in items if x.get("in_force")]
    others = [x for x in items if not x.get("in_force")]

    # Historische: nach year_enacted/year_revised sortieren, neueste zuerst
    def _sort_key(x: dict) -> tuple[int, int]:
        ye = x.get("year_enacted") or "0"
        yr = x.get("year_revised") or x.get("year_updated") or ye
        try:
            return (int(yr), int(ye))
        except (TypeError, ValueError):
            return (0, 0)

    others.sort(key=_sort_key, reverse=True)

    out: list[dict] = []
    out.extend(in_force[:1])  # primary: aktuell-gültige
    for h in others:
        if len(out) >= RESULT_LIMIT:
            break
        out.append(h)
    return out[:RESULT_LIMIT]


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_constitute(analysis: dict) -> dict:
    """Live-Lookup gegen Constitute Project (Verfassungs-Metadata).

    Returns Dict mit ≤3 Verfassungs-Treffern (in-force + historisch).
    Bei Trigger-Miss / unbekanntem Land / API-Fehler: leere Liste
    (graceful fail).
    """
    empty = {
        "source": "Constitute",
        "type": "constitutional_text",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_constitute(matchable):
        return empty

    country = _extract_country(matchable)
    if not country:
        logger.info("Constitute: kein Land im Claim erkannt — skip")
        return empty

    # Cache-Key inkl. Bill-of-Rights-Suffix, damit der enrich-Pfad nicht
    # durch einen früheren Cache-Eintrag ohne Sub-Topic verdeckt wird.
    bor_suffix = (
        "+bor" if (country == "United_States_of_America"
                   and _has_any(matchable, _SUBTOPIC_BILL_OF_RIGHTS_TERMS))
        else ""
    )
    cache_key = f"cons::{country.lower()}{bor_suffix}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now - cached[0] < _CACHE_TTL_S):
        logger.info(f"Constitute: Cache-Hit für '{country}'")
        return cached[1]

    async with polite_client(timeout=HTTP_TIMEOUT_S) as client:
        try:
            items = await asyncio.wait_for(
                _run_constitutions_lookup(client, country),
                timeout=HTTP_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logger.info(f"Constitute: Timeout für '{country}'")
            return empty

    if not items:
        logger.info(f"Constitute: 0 Treffer für '{country}'")
        _CACHE[cache_key] = (now, empty)
        return empty

    selected = _select_top_constitutions(items)
    results: list[dict] = []
    seen_ids: set[str] = set()

    # Sub-Topic-Enrichment: Bill of Rights (US) — prepend, wenn passend.
    if country == "United_States_of_America":
        enrich = _bill_of_rights_enrichment(matchable)
        if enrich:
            results.append(enrich)
            seen_ids.add(enrich["indicator"])

    for item in selected:
        try:
            built = _build_result_row(item)
        except Exception as e:
            logger.debug(f"Constitute: Format-Fehler bei item: {e}")
            continue
        if not built:
            continue
        ind = built.get("indicator", "")
        if ind in seen_ids:
            continue
        seen_ids.add(ind)
        results.append(built)
        if len(results) >= RESULT_LIMIT:
            break

    out = {
        "source": "Constitute",
        "type": "constitutional_text",
        "results": results,
    }
    _CACHE[cache_key] = (now, out)
    if results:
        logger.info(
            f"Constitute: {len(results)} Treffer für '{country}'"
        )
    return out
