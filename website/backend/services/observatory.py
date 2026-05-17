"""Mozilla HTTP Observatory (MDN) — Web-Security-Header-Scanner (Live-API).

Datenquelle: https://developer.mozilla.org/en-US/observatory/
Backend-API: https://observatory-api.mdn.mozilla.net/api/v2/
Lizenz: MPL 2.0 (Code), Daten frei — Evidora-tauglich.

API-Endpoints (V2, Stand 2026-05):
- POST /api/v2/scan?host=<domain>     — Kurzergebnis (grade/score, ohne tests)
- POST /api/v2/analyze?host=<domain>  — Vollergebnis inkl. tests + history

Beide Endpoints sind POST mit JSON-Body (mind. "{}") und querystring-host.
WICHTIG: rescan-Parameter triggert eine neue 30+ Sek dauernde Live-Probe.
Wir verwenden ausschließlich gecachte/ältere Scans — kein Rescan-Trigger.

Trigger-Strategie:
  1. Explizite Security-Header-Begriffe ("CSP", "HSTS", "X-Frame-Options",
     "Referrer-Policy", "Permissions-Policy") + Domain im Claim, ODER
  2. Mozilla-Observatory-Erwähnung, ODER
  3. Kombination "Sicherheitsbewertung/Security-Header" + Domain.

Politische Guardrails: Pure technische Bewertung. KEINE Aussagen zu
Betreibern, keine "X ist unseriös wegen Grade F"-Bewertung. Der Grade ist
nur ein Snapshot-Indikator für HTTP-Response-Header — er sagt nichts über
TLS-Konfiguration, Backend-Sicherheit oder Datenschutz aus.
"""

# WIRING für main.py:
# from services.observatory import search_observatory, claim_mentions_observatory_cached
# if claim_mentions_observatory_cached(claim):
#     tasks.append(cached("Mozilla Observatory", search_observatory, analysis))
#     queried_names.append("Mozilla Observatory")

from __future__ import annotations

import logging
import re
import time

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

OBSERVATORY_ANALYZE_URL = "https://observatory-api.mdn.mozilla.net/api/v2/analyze"
OBSERVATORY_DETAILS_URL = "https://developer.mozilla.org/en-US/observatory/analyze"

TIMEOUT_S = 15.0
MAX_DOMAINS = 2
CACHE_TTL_S = 24 * 60 * 60  # 24 h — Scans sind teuer / langsam

# Modul-Level Cache: host_lc → (ts, payload | None)
_scan_cache: dict[str, tuple[float, dict | None]] = {}


# ---------------------------------------------------------------------------
# Trigger — Domain-Erkennung + Security-Keywords
# ---------------------------------------------------------------------------
# Konservatives Domain-Pattern: mind. eine Punkt-getrennte TLD (2+ chars).
# Vermeidet false positives wie "v1.2" oder "Datei.txt" durch Stop-Listen.
_DOMAIN_REGEX = re.compile(
    r"\b([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+)\b",
    re.IGNORECASE,
)

# TLDs/Suffixe, die KEINE Domains sind (Datei-Extensions, Versionen).
_NON_DOMAIN_TLDS = {
    "txt", "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx",
    "jpg", "jpeg", "png", "gif", "svg", "webp", "mp3", "mp4",
    "zip", "tar", "gz", "rar", "7z", "exe", "dmg", "iso",
    "json", "xml", "yaml", "yml", "csv", "log", "md", "html", "htm",
    "py", "js", "ts", "css", "rb", "go", "rs", "java", "cpp", "cs",
}

# Stop-Wörter: häufige "x.y"-Patterns, die keine Domains sind.
_DOMAIN_STOP_PATTERNS = (
    re.compile(r"^\d+\.\d+(?:\.\d+)*$"),  # Versionen 1.2.3
    re.compile(r"^v\d+\.\d+", re.IGNORECASE),  # v1.2
)

_SECURITY_HEADER_TERMS = (
    "security-header", "security header", "security-headers", "security headers",
    "sicherheits-header", "sicherheits header", "sicherheitsheader",
    "csp", "content-security-policy", "content security policy",
    "hsts", "strict-transport-security", "strict transport security",
    "x-frame-options", "x-frame", "x frame options", "frame-options",
    "referrer-policy", "referrer policy",
    "permissions-policy", "permissions policy", "feature-policy",
    "x-content-type-options", "x content type",
    "cors", "cross-origin", "cross origin",
    "subresource-integrity", "sri-hash",
)

_OBSERVATORY_TERMS = (
    "mozilla observatory", "http observatory",
    "mozilla-observatory", "http-observatory",
    "observatory.mozilla", "observatory mdn",
)

_SECURITY_GENERIC_TERMS = (
    "sicherheitsbewertung", "sicherheits-bewertung",
    "sicherheits-grade", "security-grade", "security grade",
    "sicherheits-rating", "security rating",
    "web-security", "web security",
    "https-konfiguration", "https konfiguration",
)


def _normalize_domain(raw: str) -> str | None:
    """Validiert + normalisiert einen Domain-Kandidaten.

    - lowercase
    - keine reine Zahl-Sequenz (Version)
    - TLD nicht in NON_DOMAIN_TLDS
    - mind. 4 Zeichen total
    """
    if not raw:
        return None
    d = raw.strip().lower().rstrip(".")
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
    # Mind. ein Buchstabe im Label-Teil
    label = d.rsplit(".", 1)[0]
    if not any(c.isalpha() for c in label):
        return None
    return d


def _extract_domains(text: str) -> list[str]:
    """Extrahiere bis zu MAX_DOMAINS valide Domains aus Claim-Text."""
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


def _claim_mentions_observatory(claim_lc: str) -> bool:
    """Public-Trigger-Pre-Check.

    True wenn:
      - Explizite Observatory-Erwähnung, ODER
      - Security-Header-Term + Domain im Claim, ODER
      - Generischer Security-Begriff + Domain im Claim.
    """
    if not claim_lc:
        return False
    # 1) Direkter Observatory-Verweis
    if any(t in claim_lc for t in _OBSERVATORY_TERMS):
        return bool(_extract_domains(claim_lc))
    # 2) Security-Header + Domain
    has_header_term = any(t in claim_lc for t in _SECURITY_HEADER_TERMS)
    has_domain = bool(_extract_domains(claim_lc))
    if has_header_term and has_domain:
        return True
    # 3) Generischer Security-Begriff + Domain
    has_generic = any(t in claim_lc for t in _SECURITY_GENERIC_TERMS)
    if has_generic and has_domain:
        return True
    return False


def claim_mentions_observatory_cached(claim: str) -> bool:
    """Wrapper für Trigger-Check — case-normalisiert."""
    return _claim_mentions_observatory((claim or "").lower())


# ---------------------------------------------------------------------------
# HTTP — Live-Scan-Lookup (KEIN Rescan-Trigger)
# ---------------------------------------------------------------------------
async def _fetch_scan(client, host: str) -> dict | None:
    """POST /api/v2/analyze?host=<host> — liefert vorhandenen Scan + tests.

    Triggert KEINEN Rescan; nutzt den jüngsten gecachten MDN-Scan.
    """
    key = host.lower()
    cached = _scan_cache.get(key)
    if cached and (time.time() - cached[0]) < CACHE_TTL_S:
        return cached[1]
    url = f"{OBSERVATORY_ANALYZE_URL}?host={host}"
    try:
        # Body "{}" ist Pflicht (Server lehnt leeren JSON-Content-Type ab).
        resp = await client.post(
            url,
            headers={"Content-Type": "application/json"},
            content=b"{}",
        )
        if resp.status_code != 200:
            logger.debug(
                f"Observatory HTTP {resp.status_code} for {host}"
            )
            _scan_cache[key] = (time.time(), None)
            return None
        data = resp.json()
        if not isinstance(data, dict):
            _scan_cache[key] = (time.time(), None)
            return None
        _scan_cache[key] = (time.time(), data)
        return data
    except Exception as e:
        logger.debug(f"Observatory fetch failed for {host}: {e}")
        return None


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _summarize_tests(tests: dict) -> str:
    """Kurz-Zusammenfassung der wichtigsten Header-Befunde (max ~6 Punkte)."""
    if not isinstance(tests, dict):
        return ""
    # Reihenfolge nach Wichtigkeit für die Bewertung.
    priority = (
        "content-security-policy",
        "strict-transport-security",
        "x-frame-options",
        "referrer-policy",
        "x-content-type-options",
        "subresource-integrity",
        "cookies",
        "redirection",
        "cross-origin-resource-sharing",
    )
    parts: list[str] = []
    for key in priority:
        t = tests.get(key)
        if not isinstance(t, dict):
            continue
        passed = t.get("pass")
        result = (t.get("result") or "").replace("-", " ")
        # Result-Strings sind selbstbeschreibend, z. B. "csp-not-implemented"
        if passed is True:
            marker = "OK"
        elif passed is False:
            marker = "FAIL"
        else:
            marker = "n/a"
        # Kurz-Label
        short = key.replace("-", " ")
        if result and len(result) < 50:
            parts.append(f"{short}: {marker} ({result})")
        else:
            parts.append(f"{short}: {marker}")
        if len(parts) >= 6:
            break
    return "; ".join(parts)


def _format_scan(host: str, payload: dict) -> dict | None:
    """Mapping Observatory-Response → Evidora-Result-Dict."""
    if not isinstance(payload, dict):
        return None
    scan = payload.get("scan") if isinstance(payload.get("scan"), dict) else payload
    if not isinstance(scan, dict):
        return None
    grade = scan.get("grade")
    score = scan.get("score")
    scanned_at = scan.get("scanned_at") or ""
    year = scanned_at[:4] if len(scanned_at) >= 4 and scanned_at[:4].isdigit() else "—"
    tests_failed = scan.get("tests_failed")
    tests_passed = scan.get("tests_passed")
    tests_quantity = scan.get("tests_quantity")

    if grade is None and score is None:
        return None

    grade_str = grade if isinstance(grade, str) else "?"
    score_int = int(score) if isinstance(score, (int, float)) else 0

    headline = f"{host}: Grade {grade_str} (Score {score_int}/100)"
    if isinstance(tests_passed, int) and isinstance(tests_quantity, int):
        headline += f", {tests_passed}/{tests_quantity} Tests bestanden"

    tests = payload.get("tests") if isinstance(payload.get("tests"), dict) else {}
    test_summary = _summarize_tests(tests)

    description_parts: list[str] = []
    if test_summary:
        description_parts.append(f"Top-Findings: {test_summary}.")
    if scanned_at:
        description_parts.append(f"Letzter Scan: {scanned_at[:10]}.")
    description_parts.append(
        "Quelle: Mozilla HTTP Observatory — bewertet ausschließlich HTTP-"
        "Response-Header (CSP, HSTS, X-Frame-Options, Referrer-Policy u. a.). "
        "Aussagen zur Gesamt-Sicherheit einer Website lassen sich daraus NICHT "
        "ableiten; TLS-Stärke, Backend-Härtung, Datenschutz und Patch-Stand "
        "werden NICHT gemessen. Der Grade ist eine technische Momentaufnahme."
    )
    description = " ".join(description_parts)[:900]

    safe_host = host.replace(".", "_").replace("-", "_")
    return {
        "indicator_name": f"Security Grade: {host}",
        "indicator": f"observatory_{safe_host}",
        "country": "INT",
        "country_name": "—",
        "year": year,
        "value": score_int,
        "display_value": headline,
        "description": description,
        "url": f"{OBSERVATORY_DETAILS_URL}?host={host}",
        "source": "Mozilla HTTP Observatory (MDN)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_observatory(analysis: dict) -> dict:
    """Live-Lookup gegen Mozilla HTTP Observatory.

    Extrahiert bis zu MAX_DOMAINS Domains aus dem Claim und liefert deren
    bestehende Observatory-Scans (KEIN Rescan-Trigger).
    """
    empty = {
        "source": "Mozilla Observatory",
        "type": "web_security",
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

    if not _claim_mentions_observatory(combined_lc):
        return empty

    domains = _extract_domains(combined_lc)
    if not domains:
        return empty

    results: list[dict] = []
    async with polite_client(timeout=TIMEOUT_S) as client:
        for host in domains[:MAX_DOMAINS]:
            payload = await _fetch_scan(client, host)
            if not payload:
                continue
            r = _format_scan(host, payload)
            if r:
                results.append(r)

    if not results:
        logger.info(
            f"Mozilla Observatory: 0 Treffer (domains={domains[:MAX_DOMAINS]})"
        )
        return empty

    logger.info(
        f"Mozilla Observatory: {len(results)} Scan-Resultate für "
        f"{[r['indicator_name'] for r in results]}"
    )
    return {
        "source": "Mozilla Observatory",
        "type": "web_security",
        "results": results,
    }
