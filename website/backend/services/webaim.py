"""WebAIM Million — Accessibility-Audit der Top-1M-Homepages (Live + Aggregat).

Datenquelle: https://webaim.org/projects/million/
Lookup-Endpoint: https://webaim.org/projects/million/lookup?domain=<host>
Bulk: JSON + CSV unter https://webaim.org/projects/million/ (jaehrlich aktualisiert).
Lizenz: "Free for research" — WebAIM-Daten frei nutzbar mit Quellenangabe.

Engine: WAVE (Web Accessibility Versatile Evaluator, https://wave.webaim.org/).
WAVE prueft automatisch erkennbare WCAG-2-A/AA-Fehler im DOM (Kontrast,
Alt-Text, Form-Labels, Empty Links/Buttons, Sprach-Attribut etc.). WAVE
ist *kein* vollstaendiger Accessibility-Audit; manuelle Pruefung +
Screenreader-Tests bleiben noetig.

Pack-Approach (siehe ARCHITECTURE.md):
  * Aggregat-Erkenntnisse (Top-Errors, ARIA-Paradoxon, 7-Jahre-Trend)
    werden statisch eingebettet — keine externe JSON-Datei, kein
    main.py-/data_updater.py-Wiring noetig.
  * Live-Lookup gegen webaim.org/projects/million/lookup wird nur bei
    explizitem Domain-Bezug ausgeloest (Rang + Fehlerzahl + Top-Errors
    fuer eine spezifische Homepage).

Trigger-Strategie:
  1. Explizite WebAIM-/WAVE-Erwaehnung im Claim ODER
  2. Accessibility-/Barrierefreiheit-/WCAG-Term + Domain ODER
  3. Top-1M-/Million-Begriffe in Kombination mit Accessibility.

Politische Guardrails: rein technisch. Kein Ranking von Betreibern,
keine "Site X ist diskriminierend"-Bewertung. WAVE-Befunde sind
automatisierte DOM-Heuristiken — sie ersetzen keine manuelle A11Y-
Bewertung und sagen nichts ueber Tastatur-Navigation, Screenreader-
Verhalten, Cognitive Load oder Inhalts-Verstaendlichkeit aus.
"""

# WIRING fuer main.py:
# from services.webaim import search_webaim, claim_mentions_webaim_cached
# if claim_mentions_webaim_cached(claim):
#     tasks.append(cached("WebAIM Million", search_webaim, analysis))
#     queried_names.append("WebAIM Million")

from __future__ import annotations

import logging
import re
import time

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

WEBAIM_LOOKUP_URL = "https://webaim.org/projects/million/lookup"
WEBAIM_REPORT_URL = "https://webaim.org/projects/million/"

TIMEOUT_S = 15.0
MAX_DOMAINS = 2
CACHE_TTL_S = 24 * 60 * 60  # 24 h — Million-Snapshot ist jaehrlich, Cache ist grosszuegig

# Modul-Level Cache: domain_lc -> (ts, payload | None)
_lookup_cache: dict[str, tuple[float, dict | None]] = {}


# ---------------------------------------------------------------------------
# Aggregat-Erkenntnisse (WebAIM Million 2026, veroeffentlicht Feb 2026)
# ---------------------------------------------------------------------------
# Stand-Quelle: https://webaim.org/projects/million/ (jaehrlich, Feb-Snapshot).
# Bei Update einfach REPORT_YEAR + Werte aktualisieren — kein Wiring noetig.
REPORT_YEAR = 2026
REPORT_SAMPLE = "1.000.000 Homepages"

# Top-Errors 2026 (96 % aller erkannten Fehler entfallen auf diese sechs).
TOP_ERRORS_2026 = [
    ("Low contrast text (zu geringer Text-Kontrast)", 83.9),
    ("Missing alternative text for images (fehlende Bild-Alternativtexte)", 53.1),
    ("Missing form input labels (fehlende Formular-Labels)", 51.0),
    ("Empty links (Links ohne Text)", 46.3),
    ("Empty buttons (Buttons ohne Text)", 30.6),
    ("Missing document language (fehlendes <html lang>-Attribut)", 13.5),
]

AVG_ERRORS_2026 = 56.1            # Schnitt pro Homepage 2026
AVG_ERRORS_2025 = 51.0
AVG_ERRORS_2024 = 56.8
AVG_ERRORS_2023 = 50.0            # ungefaehr, Tiefpunkt
AVG_ERRORS_2019 = 60.9            # Studienbeginn

# ARIA-Paradoxon: Seiten *mit* ARIA hatten 2026 im Schnitt mehr erkannte
# Fehler (~59,1) als Seiten *ohne* ARIA (~42). WebAIM ordnet das primaer
# der Komplexitaet zu, nicht ARIA selbst (Confounder).
ARIA_PARADOX_WITH = 59.1
ARIA_PARADOX_WITHOUT = 42.0


def _aggregate_result() -> dict:
    """Statisches Aggregat-Ergebnis als Evidora-Result-Dict.

    Wird immer mitgeliefert, wenn der Trigger feuert — auch wenn keine
    spezifische Domain im Claim erkennbar war.
    """
    top_lines = "; ".join(
        f"{name}: {pct:.1f} %" for name, pct in TOP_ERRORS_2026
    )
    trend = (
        f"7-Jahres-Trend: 2019 ~{AVG_ERRORS_2019:.0f} -> 2023 ~{AVG_ERRORS_2023:.0f} "
        f"-> 2024 {AVG_ERRORS_2024:.1f} -> 2025 {AVG_ERRORS_2025:.1f} -> "
        f"2026 {AVG_ERRORS_2026:.1f} Fehler/Seite (Anstieg seit 2024)."
    )
    aria = (
        f"ARIA-Paradoxon 2026: Seiten MIT ARIA-Attributen hatten im Schnitt "
        f"{ARIA_PARADOX_WITH:.1f} erkannte Fehler, Seiten OHNE ARIA nur "
        f"{ARIA_PARADOX_WITHOUT:.1f}. WebAIM interpretiert das primaer als "
        f"Komplexitaets-Korrelation (mehr ARIA wird meist auf komplexeren "
        f"Seiten eingesetzt), nicht als Beleg gegen ARIA — falsch oder "
        f"unnoetig eingesetztes ARIA kann Barrieren aber verschaerfen."
    )
    description = (
        f"WebAIM Million {REPORT_YEAR}: Automatische WAVE-Engine-Analyse von "
        f"{REPORT_SAMPLE} (Top-1M Alexa/Tranco-Rang). Im Schnitt "
        f"{AVG_ERRORS_2026:.1f} erkennbare WCAG-2-A/AA-Fehler pro Seite. "
        f"Top-Fehlertypen (96 % aller Befunde): {top_lines}. {trend} {aria} "
        f"Wichtige Einschraenkung: WAVE prueft nur automatisch erkennbare "
        f"DOM-Defekte — manuelle Pruefung, Tastatur-Navigation und Screen"
        f"reader-Tests bleiben fuer vollstaendige Barrierefreiheits-Bewertung "
        f"unverzichtbar."
    )
    return {
        "indicator_name": f"WebAIM Million {REPORT_YEAR} — Aggregat",
        "indicator": f"webaim_million_aggregate_{REPORT_YEAR}",
        "country": "INT",
        "country_name": "Top 1M Homepages weltweit",
        "year": str(REPORT_YEAR),
        "value": AVG_ERRORS_2026,
        "display_value": (
            f"{AVG_ERRORS_2026:.1f} Fehler/Seite im Schnitt "
            f"(WAVE, {REPORT_SAMPLE}, {REPORT_YEAR})"
        ),
        "description": description[:900],
        "url": WEBAIM_REPORT_URL,
        "source": "WebAIM Million Report",
    }


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
# Konservatives Domain-Pattern (deckungsgleich mit observatory.py-Logik).
_DOMAIN_REGEX = re.compile(
    r"\b([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+)\b",
    re.IGNORECASE,
)

_NON_DOMAIN_TLDS = {
    "txt", "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx",
    "jpg", "jpeg", "png", "gif", "svg", "webp", "mp3", "mp4",
    "zip", "tar", "gz", "rar", "7z", "exe", "dmg", "iso",
    "json", "xml", "yaml", "yml", "csv", "log", "md", "html", "htm",
    "py", "js", "ts", "css", "rb", "go", "rs", "java", "cpp", "cs",
}

_DOMAIN_STOP_PATTERNS = (
    re.compile(r"^\d+\.\d+(?:\.\d+)*$"),
    re.compile(r"^v\d+\.\d+", re.IGNORECASE),
)

# Explizite WebAIM-/WAVE-Erwaehnungen.
_WEBAIM_TERMS = (
    "webaim", "web aim", "web-aim",
    "wave engine", "wave-engine",
    "wave accessibility", "wave a11y",
    "webaim million", "webaim-million",
    "million report", "million-report",
)

# Accessibility-/Barrierefreiheits-Begriffe.
_A11Y_TERMS = (
    "accessibility", "accessibility-audit", "accessibility audit",
    "a11y", "barrierefrei", "barriere-frei",
    "barrierefreiheit", "barriere-freiheit",
    "wcag", "wcag 2", "wcag 2.1", "wcag 2.2", "wcag2",
    "aria", "alt-text", "alt text", "alternativtext",
    "screenreader", "screen reader", "screen-reader",
    "kontrast-fehler", "low contrast", "low-contrast",
    "tastatur-navigation", "tastatur navigation",
    "barrierefreie website", "barrierefreie websites",
    "barrierefreie homepage", "barrierefreie homepages",
)

# Top-1M-/Million-Kontext-Begriffe (Aggregat-Anker).
_MILLION_TERMS = (
    "top 1m", "top-1m", "top 1 million", "top-1-million",
    "top million", "top-million", "1 million homepages",
    "1m homepages", "top-1000000", "top 1000000",
)


def _normalize_domain(raw: str) -> str | None:
    """Validiert + normalisiert einen Domain-Kandidaten (vgl. observatory.py)."""
    if not raw:
        return None
    d = raw.strip().lower().rstrip(".")
    # Stripping www. — WebAIM-Lookup will Domain "minus www.".
    if d.startswith("www."):
        d = d[4:]
    if len(d) < 4 or "." not in d:
        return None
    for pat in _DOMAIN_STOP_PATTERNS:
        if pat.match(d):
            return None
    tld = d.rsplit(".", 1)[-1]
    if tld in _NON_DOMAIN_TLDS:
        return None
    if not tld.isalpha():
        return None
    label = d.rsplit(".", 1)[0]
    if not any(c.isalpha() for c in label):
        return None
    return d


def _extract_domains(text: str) -> list[str]:
    if not text:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for m in _DOMAIN_REGEX.findall(text):
        d = _normalize_domain(m)
        if not d or d in seen:
            continue
        seen.add(d)
        out.append(d)
        if len(out) >= MAX_DOMAINS:
            break
    return out


def _claim_mentions_webaim(claim_lc: str) -> bool:
    """Public-Trigger-Pre-Check.

    True wenn:
      - Direkter WebAIM-/WAVE-Verweis, ODER
      - Accessibility-/WCAG-Begriff + Domain, ODER
      - Accessibility-/WCAG-Begriff + Million/Top-1M-Kontext, ODER
      - Explizite "Barrierefreiheit Top-Websites"-Phrase.
    """
    if not claim_lc:
        return False

    # 1) Direkter WebAIM-Bezug — feuert immer.
    if any(t in claim_lc for t in _WEBAIM_TERMS):
        return True

    has_a11y = any(t in claim_lc for t in _A11Y_TERMS)
    if not has_a11y:
        return False

    # 2) Accessibility + Domain.
    if _extract_domains(claim_lc):
        return True

    # 3) Accessibility + Top-1M-Kontext.
    if any(t in claim_lc for t in _MILLION_TERMS):
        return True

    # 4) "Barrierefreiheit Top-Websites" / "Top Websites Accessibility"-Phrase.
    if "top-website" in claim_lc or "top websites" in claim_lc or "top-websites" in claim_lc:
        return True

    return False


def claim_mentions_webaim_cached(claim: str) -> bool:
    """Wrapper fuer Trigger-Check — case-normalisiert."""
    return _claim_mentions_webaim((claim or "").lower())


# ---------------------------------------------------------------------------
# Live-Lookup (per-Domain HTML-Parse)
# ---------------------------------------------------------------------------
# Response-Format (Stand 2026-02-Snapshot, beobachtet via curl):
#   <h2>Results for <domain></h2>
#   <p><strong>WAVE Accessibility Rank:</strong> <big>#207,606 of 1,000,000</big>...
#   <p><strong>Popularity Rank:</strong> <big>#1,413</big> of 1,000,000</p>
#   <p><strong>Number of accessibility errors detected:</strong> <big>11</big></p>
#   <p><strong>WCAG 2 A/AA failure detected:</strong> <big>Yes</big></p>
#   <p><strong>Number of page elements:</strong> <big>1,170</big></p>
#   <p><strong>Error density:</strong> <big>0.94%</big><br>...
#   <p><strong>Top error types detected:</strong></p><ul><li>Low contrast text<li>Empty link</ul>
# Not-found-Sentinel:
#   "was not found in the WebAIM Million database"
_NOT_FOUND_SENTINEL = "was not found in the webaim million database"

_RX_RESULTS_HEADER = re.compile(
    r"<h2>\s*Results\s+for\s+([^<]+?)\s*</h2>", re.IGNORECASE
)
_RX_ACC_RANK = re.compile(
    r"WAVE\s+Accessibility\s+Rank:\s*</strong>\s*<big>#?([\d,]+)\s*of\s*([\d,]+)",
    re.IGNORECASE,
)
_RX_POP_RANK = re.compile(
    r"Popularity\s+Rank:\s*</strong>\s*<big>#?([\d,]+)\s*</big>\s*of\s*([\d,]+)",
    re.IGNORECASE,
)
_RX_NUM_ERRORS = re.compile(
    r"Number\s+of\s+accessibility\s+errors\s+detected:\s*</strong>\s*<big>([\d,]+)",
    re.IGNORECASE,
)
_RX_WCAG_FAIL = re.compile(
    r"WCAG\s*2\s*A/AA\s+failure\s+detected:\s*</strong>\s*<big>(Yes|No)",
    re.IGNORECASE,
)
_RX_PAGE_ELEMENTS = re.compile(
    r"Number\s+of\s+page\s+elements:\s*</strong>\s*<big>([\d,]+)",
    re.IGNORECASE,
)
_RX_ERR_DENSITY = re.compile(
    r"Error\s+density:\s*</strong>\s*<big>([\d.]+%?)", re.IGNORECASE
)
_RX_TOP_ERRORS_BLOCK = re.compile(
    r"Top\s+error\s+types\s+detected:\s*</strong>\s*</p>\s*<ul>(.*?)</ul>",
    re.IGNORECASE | re.DOTALL,
)
_RX_LI = re.compile(r"<li>\s*([^<]+?)\s*(?=<li|</ul|$)", re.IGNORECASE | re.DOTALL)


def _int_or_none(s: str | None) -> int | None:
    if not s:
        return None
    try:
        return int(s.replace(",", "").strip())
    except ValueError:
        return None


def _parse_lookup_html(html: str, domain: str) -> dict | None:
    """Extrahiert WebAIM-Lookup-Felder aus der HTML-Antwort.

    Gibt {"not_found": True} zurueck, wenn Domain nicht in der Studie ist.
    Gibt None zurueck, wenn die Antwort unerwartet aussieht.
    """
    if not html or not isinstance(html, str):
        return None
    html_lc = html.lower()
    if _NOT_FOUND_SENTINEL in html_lc:
        return {"not_found": True}

    m_header = _RX_RESULTS_HEADER.search(html)
    if not m_header:
        return None

    parsed: dict = {"domain": domain}
    m = _RX_ACC_RANK.search(html)
    if m:
        parsed["acc_rank"] = _int_or_none(m.group(1))
        parsed["acc_rank_total"] = _int_or_none(m.group(2))
    m = _RX_POP_RANK.search(html)
    if m:
        parsed["pop_rank"] = _int_or_none(m.group(1))
        parsed["pop_rank_total"] = _int_or_none(m.group(2))
    m = _RX_NUM_ERRORS.search(html)
    if m:
        parsed["num_errors"] = _int_or_none(m.group(1))
    m = _RX_WCAG_FAIL.search(html)
    if m:
        parsed["wcag_failure"] = m.group(1).strip().lower() == "yes"
    m = _RX_PAGE_ELEMENTS.search(html)
    if m:
        parsed["page_elements"] = _int_or_none(m.group(1))
    m = _RX_ERR_DENSITY.search(html)
    if m:
        parsed["error_density"] = m.group(1).strip()
    m = _RX_TOP_ERRORS_BLOCK.search(html)
    if m:
        block = m.group(1)
        items = [li.strip() for li in _RX_LI.findall(block) if li.strip()]
        if items:
            parsed["top_errors"] = items[:6]
    return parsed


async def _fetch_lookup(client, domain: str) -> dict | None:
    """GET WebAIM-Lookup fuer eine Domain (24 h Cache)."""
    key = domain.lower()
    cached = _lookup_cache.get(key)
    if cached and (time.time() - cached[0]) < CACHE_TTL_S:
        return cached[1]
    try:
        resp = await client.get(
            WEBAIM_LOOKUP_URL,
            params={"domain": domain},
        )
        if resp.status_code != 200:
            logger.debug(f"WebAIM HTTP {resp.status_code} for {domain}")
            _lookup_cache[key] = (time.time(), None)
            return None
        parsed = _parse_lookup_html(resp.text, domain)
        _lookup_cache[key] = (time.time(), parsed)
        return parsed
    except Exception as e:
        logger.debug(f"WebAIM fetch failed for {domain}: {e}")
        return None


def _format_lookup(parsed: dict) -> dict | None:
    """Mapping WebAIM-Lookup-Parse -> Evidora-Result-Dict."""
    if not isinstance(parsed, dict):
        return None
    domain = parsed.get("domain") or ""
    if parsed.get("not_found"):
        # Negativ-Ergebnis: explizit darstellen statt zu unterdruecken.
        return {
            "indicator_name": f"WebAIM Million Lookup: {domain}",
            "indicator": f"webaim_lookup_{domain.replace('.', '_').replace('-', '_')}",
            "country": "INT",
            "country_name": "—",
            "year": str(REPORT_YEAR),
            "value": 0,
            "display_value": f"{domain}: nicht in der WebAIM-Million-Datenbank",
            "description": (
                f"Die Domain {domain} ist nicht im aktuellen WebAIM-Million-"
                f"Snapshot ({REPORT_YEAR}) enthalten. Die Studie deckt nur die "
                f"Top-1.000.000 Homepages nach Popularitaets-Rang ab; weniger "
                f"besuchte Seiten werden nicht automatisch gescannt. Eine "
                f"manuelle WAVE-Analyse ist unter wave.webaim.org moeglich."
            ),
            "url": f"{WEBAIM_LOOKUP_URL}?domain={domain}",
            "source": "WebAIM Million Lookup",
        }

    num_errors = parsed.get("num_errors")
    acc_rank = parsed.get("acc_rank")
    acc_rank_total = parsed.get("acc_rank_total") or 1_000_000
    pop_rank = parsed.get("pop_rank")
    page_elements = parsed.get("page_elements")
    error_density = parsed.get("error_density")
    wcag_failure = parsed.get("wcag_failure")
    top_errors = parsed.get("top_errors") or []

    if num_errors is None and acc_rank is None:
        return None

    # Headline.
    headline_parts: list[str] = []
    if num_errors is not None:
        headline_parts.append(f"{num_errors} erkannte Fehler")
    if acc_rank is not None:
        headline_parts.append(
            f"WAVE-Rang #{acc_rank:,}/{acc_rank_total:,}".replace(",", ".")
        )
    headline = f"{domain}: " + ", ".join(headline_parts)

    # Beschreibung.
    desc_parts: list[str] = []
    if wcag_failure is True:
        desc_parts.append("WCAG 2 A/AA-Verstoss erkannt: ja.")
    elif wcag_failure is False:
        desc_parts.append("WCAG 2 A/AA-Verstoss erkannt: nein.")
    if page_elements is not None and error_density:
        desc_parts.append(
            f"{page_elements:,} DOM-Elemente, Fehlerdichte {error_density}."
            .replace(",", ".")
        )
    elif page_elements is not None:
        desc_parts.append(f"{page_elements:,} DOM-Elemente.".replace(",", "."))
    if pop_rank is not None:
        desc_parts.append(
            f"Popularitaets-Rang #{pop_rank:,} von 1.000.000.".replace(",", ".")
        )
    if top_errors:
        desc_parts.append(
            "Top-Fehlertypen: " + ", ".join(top_errors) + "."
        )
    desc_parts.append(
        f"Snapshot {REPORT_YEAR} der WebAIM-Million-Studie, automatische "
        f"WAVE-Engine-Pruefung (DOM-Heuristiken fuer WCAG 2 A/AA). Manuelle "
        f"Tests mit Screenreader + Tastatur-Navigation bleiben fuer eine "
        f"vollstaendige Bewertung erforderlich."
    )
    description = " ".join(desc_parts)[:900]

    safe_host = domain.replace(".", "_").replace("-", "_")
    return {
        "indicator_name": f"WebAIM Million: {domain}",
        "indicator": f"webaim_lookup_{safe_host}",
        "country": "INT",
        "country_name": "—",
        "year": str(REPORT_YEAR),
        "value": int(num_errors) if isinstance(num_errors, int) else 0,
        "display_value": headline,
        "description": description,
        "url": f"{WEBAIM_LOOKUP_URL}?domain={domain}",
        "source": "WebAIM Million Lookup",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_webaim(analysis: dict) -> dict:
    """WebAIM-Million-Lookup + Aggregat-Erkenntnisse.

    Liefert immer das Aggregat-Result (Top-Errors, ARIA-Paradoxon, Trend),
    wenn der Trigger feuert, plus optional pro Domain einen Live-Lookup-
    Eintrag.
    """
    empty = {
        "source": "WebAIM Million",
        "type": "accessibility_audit",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or ""
    original = analysis.get("original_claim") or claim
    if not isinstance(claim, str):
        claim = str(claim or "")
    if not isinstance(original, str):
        original = str(original or "")
    combined = f"{original} {claim}"
    combined_lc = combined.lower()

    if not _claim_mentions_webaim(combined_lc):
        return empty

    results: list[dict] = [_aggregate_result()]

    domains = _extract_domains(combined_lc)
    if domains:
        async with polite_client(timeout=TIMEOUT_S) as client:
            for host in domains[:MAX_DOMAINS]:
                parsed = await _fetch_lookup(client, host)
                if not parsed:
                    continue
                r = _format_lookup(parsed)
                if r:
                    results.append(r)

    logger.info(
        f"WebAIM Million: {len(results)} Resultate "
        f"(domains={domains[:MAX_DOMAINS] if domains else []})"
    )
    return {
        "source": "WebAIM Million",
        "type": "accessibility_audit",
        "results": results,
    }
