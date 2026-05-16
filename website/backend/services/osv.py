"""OSV.dev — Open Source Vulnerabilities Aggregator (Live-API).

Datenquelle: https://osv.dev/ — Google-betriebener Aggregator für Open-
Source-Schwachstellen aus GitHub Advisory Database, PyPA, RustSec, GSD,
Maven, npm, Go, RubyGems, NuGet, Packagist, Hex u. a.
Lizenz: Apache 2.0 (Code), CC0/MIT-kompatibel (Daten) — Evidora-tauglich.

API:
- POST https://api.osv.dev/v1/query  — Lookup by package (+optional version)
- GET  https://api.osv.dev/v1/vulns/{id}  — Direkter Lookup nach OSV/CVE/GHSA-ID

Format: JSON, kein Auth, kein dokumentiertes Rate-Limit (99.9% Uptime).
Polite-Client-Pattern mit User-Agent wird trotzdem benutzt.

Trigger-Strategie:
  1. CVE-/GHSA-ID im Claim   → direkter Vulnerability-Lookup
  2. Bekannter Package-Name  → POST /v1/query (optional mit Version)
  3. Generische Vulnerability-Keywords ("Schwachstelle", "CVE",
     "Supply-Chain-Angriff") + Package-Match → wie 2.

Politische Guardrails: Nur Fakten zur Schwachstelle + Patch-Status.
KEINE Schuldzuweisungen an Maintainer/Companies. Synthesizer baut den
verbindlichen Disclaimer ("CVSS-Score ist eine technische Bewertung,
keine Garantie für Ausnutzbarkeit im konkreten Setup.").
"""

# WIRING für main.py:
# from services.osv import search_osv, claim_mentions_osv_cached
# if claim_mentions_osv_cached(claim):
#     tasks.append(cached("OSV.dev", search_osv, analysis))
#     queried_names.append("OSV.dev")

from __future__ import annotations

import logging
import re
import time
from urllib.parse import quote

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

OSV_QUERY_URL = "https://api.osv.dev/v1/query"
OSV_VULN_URL = "https://api.osv.dev/v1/vulns/"

TIMEOUT_S = 12.0
MAX_RESULTS = 5
CACHE_TTL_S = 24 * 60 * 60  # 24 h

# Modul-Level Caches: Key → (ts, payload)
_query_cache: dict[tuple[str, str, str], tuple[float, list[dict]]] = {}
_vuln_cache: dict[str, tuple[float, dict | None]] = {}


# ---------------------------------------------------------------------------
# Trigger — Regex + Package-Whitelist
# ---------------------------------------------------------------------------
# CVE-YYYY-NNNN(N+) — Format laut MITRE
_CVE_REGEX = re.compile(r"\bCVE-\d{4}-\d{4,7}\b", re.IGNORECASE)
# GitHub Security Advisory: GHSA-xxxx-xxxx-xxxx (4 chars × 3)
_GHSA_REGEX = re.compile(r"\bGHSA-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}\b", re.IGNORECASE)

# Bekannte Open-Source-Pakete + Standard-Ecosystem.
# Heuristik-Whitelist; kann erweitert werden.
# Maven nutzt "group:artifact"; wir bilden hier den User-sichtbaren
# Kurznamen auf den Lookup-Namen ab.
_PACKAGE_WHITELIST: dict[str, tuple[str, str]] = {
    # (claim-token-lc) -> (OSV-Package-Name, Ecosystem)
    "log4j": ("org.apache.logging.log4j:log4j-core", "Maven"),
    "log4j-core": ("org.apache.logging.log4j:log4j-core", "Maven"),
    "log4shell": ("org.apache.logging.log4j:log4j-core", "Maven"),
    "spring-core": ("org.springframework:spring-core", "Maven"),
    "spring4shell": ("org.springframework:spring-beans", "Maven"),
    "struts": ("org.apache.struts:struts2-core", "Maven"),
    "tomcat": ("org.apache.tomcat:tomcat", "Maven"),
    "jackson": ("com.fasterxml.jackson.core:jackson-databind", "Maven"),
    "openssl": ("openssl", "Debian"),
    "nginx": ("nginx", "Debian"),
    "apache": ("apache2", "Debian"),
    "curl": ("curl", "Debian"),
    "bash": ("bash", "Debian"),
    "sudo": ("sudo", "Debian"),
    # npm
    "lodash": ("lodash", "npm"),
    "express": ("express", "npm"),
    "axios": ("axios", "npm"),
    "react": ("react", "npm"),
    "next.js": ("next", "npm"),
    "nextjs": ("next", "npm"),
    "webpack": ("webpack", "npm"),
    "node-fetch": ("node-fetch", "npm"),
    # PyPI
    "django": ("django", "PyPI"),
    "flask": ("flask", "PyPI"),
    "requests": ("requests", "PyPI"),
    "numpy": ("numpy", "PyPI"),
    "pandas": ("pandas", "PyPI"),
    "pillow": ("pillow", "PyPI"),
    "cryptography": ("cryptography", "PyPI"),
    "urllib3": ("urllib3", "PyPI"),
    "fastapi": ("fastapi", "PyPI"),
    "pyyaml": ("pyyaml", "PyPI"),
    # Go
    "kubernetes": ("k8s.io/kubernetes", "Go"),
    # RubyGems
    "rails": ("rails", "RubyGems"),
    "actionpack": ("actionpack", "RubyGems"),
}

_VULN_KEYWORDS = (
    "schwachstelle", "sicherheitslücke", "sicherheitsluecke",
    "vulnerability", "exploit", "zero-day", "0-day", "zeroday",
    "supply-chain-angriff", "supply chain attack",
    "open-source-lücke", "open source vulnerability",
    "remote code execution", "rce ", " rce.", "code-injection",
    "cve-", "ghsa-",
)


def _detect_packages(claim_lc: str) -> list[tuple[str, str]]:
    """Erkenne bekannte Paket-Namen in Claim-Text.

    Returns Liste eindeutiger (osv_name, ecosystem)-Tupel. Reihenfolge
    nach Auftreten im Claim; limitiert auf 3, um API-Last zu deckeln.
    """
    found: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for token, mapping in _PACKAGE_WHITELIST.items():
        # Wortgrenzen erzwingen für kurze Tokens (vermeidet "axios" in
        # "axiosomething"; einfache substring-Suche reicht für mehrteilige
        # Tokens wie "log4j-core").
        if len(token) <= 4:
            pat = r"\b" + re.escape(token) + r"\b"
            if re.search(pat, claim_lc):
                if mapping not in seen:
                    seen.add(mapping)
                    found.append(mapping)
        else:
            if token in claim_lc:
                if mapping not in seen:
                    seen.add(mapping)
                    found.append(mapping)
        if len(found) >= 3:
            break
    return found


def _extract_cve_ghsa_ids(claim: str) -> list[str]:
    """Extrahiere CVE-/GHSA-IDs aus Claim-Text (max 3, de-dupliziert)."""
    if not claim:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for m in _CVE_REGEX.findall(claim):
        key = m.upper()
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
        if len(out) >= 3:
            return out
    for m in _GHSA_REGEX.findall(claim):
        key = m.upper()
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
        if len(out) >= 3:
            return out
    return out


_VERSION_REGEX = re.compile(r"\b(\d+\.\d+(?:\.\d+){0,2})\b")


def _extract_version(claim: str, package_token: str) -> str | None:
    """Heuristisch: erste Version-ähnliche Zahl in der Nähe des Pakets.

    Sucht innerhalb von ±40 Zeichen um den Package-Token.
    """
    if not claim or not package_token:
        return None
    lc = claim.lower()
    idx = lc.find(package_token)
    if idx < 0:
        return None
    window = claim[max(0, idx - 40): idx + len(package_token) + 40]
    m = _VERSION_REGEX.search(window)
    return m.group(1) if m else None


def _claim_mentions_osv(claim_lc: str) -> bool:
    """Public-Trigger-Pre-Check.

    True wenn:
      - CVE-/GHSA-ID im Claim, ODER
      - Bekannter Package-Name in Whitelist + Vuln-Keyword, ODER
      - Package-Name + harter Indikator wie "unsicher"/"lücke".
    """
    if not claim_lc:
        return False
    if _CVE_REGEX.search(claim_lc) or _GHSA_REGEX.search(claim_lc):
        return True
    has_package = bool(_detect_packages(claim_lc))
    if not has_package:
        return False
    has_vuln_kw = any(k in claim_lc for k in _VULN_KEYWORDS)
    if has_vuln_kw:
        return True
    # Composite: Package + "unsicher" / "kaputt" / "lücke" / "patch"
    soft_signals = (
        "unsicher", "lücke", "luecke", "patch", "angreifbar",
        "kompromittiert", "exploit", "ausgenutzt",
    )
    return any(s in claim_lc for s in soft_signals)


def claim_mentions_osv_cached(claim: str) -> bool:
    """Wrapper für Trigger-Check — case-normalisiert."""
    return _claim_mentions_osv((claim or "").lower())


# ---------------------------------------------------------------------------
# HTTP — Lookup + Query
# ---------------------------------------------------------------------------
async def _fetch_vuln_by_id(client, vuln_id: str) -> dict | None:
    """GET /v1/vulns/{id} — direkter Vulnerability-Lookup mit 24h-Cache."""
    key = vuln_id.upper()
    cached = _vuln_cache.get(key)
    if cached and (time.time() - cached[0]) < CACHE_TTL_S:
        return cached[1]
    url = f"{OSV_VULN_URL}{quote(vuln_id, safe='')}"
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code == 404:
            _vuln_cache[key] = (time.time(), None)
            return None
        if resp.status_code != 200:
            logger.debug(f"OSV vuln HTTP {resp.status_code} for {vuln_id}")
            return None
        data = resp.json()
        if not isinstance(data, dict):
            return None
        _vuln_cache[key] = (time.time(), data)
        return data
    except Exception as e:
        logger.debug(f"OSV vuln fetch failed for {vuln_id}: {e}")
        return None


async def _query_by_package(
    client,
    name: str,
    ecosystem: str,
    version: str | None = None,
) -> list[dict]:
    """POST /v1/query — Package-(+Version-)Lookup mit 24h-Cache."""
    cache_key = (name.lower(), ecosystem.lower(), (version or "").lower())
    cached = _query_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < CACHE_TTL_S:
        return cached[1]
    body: dict = {"package": {"name": name, "ecosystem": ecosystem}}
    if version:
        body["version"] = version
    try:
        resp = await client.post(OSV_QUERY_URL, json=body)
        if resp.status_code != 200:
            logger.debug(
                f"OSV query HTTP {resp.status_code} for {name} ({ecosystem})"
            )
            return []
        data = resp.json()
        vulns = data.get("vulns") if isinstance(data, dict) else None
        if not isinstance(vulns, list):
            vulns = []
        # Auf MAX_RESULTS deckeln + Cache füllen
        vulns = vulns[:MAX_RESULTS]
        _query_cache[cache_key] = (time.time(), vulns)
        return vulns
    except Exception as e:
        logger.debug(f"OSV query failed for {name} ({ecosystem}): {e}")
        return []


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _pick_cve_alias(vuln: dict) -> str:
    """Bevorzuge CVE-ID als anzeigbaren Identifier (Aliases-Feld)."""
    osv_id = vuln.get("id") or ""
    for alias in vuln.get("aliases") or []:
        if isinstance(alias, str) and alias.upper().startswith("CVE-"):
            return alias.upper()
    return osv_id


def _severity_score(vuln: dict) -> float | None:
    """Extrahiere numerischen CVSS-Score (falls vorhanden).

    OSV liefert `severity: [{type:"CVSS_V3", score:"CVSS:3.1/AV:N/..."}]`
    oder zusätzlich `database_specific.severity` als "LOW"/"MEDIUM"/"HIGH"/
    "CRITICAL". Ein numerischer Score lässt sich aus dem Vektor allein
    nicht trivial berechnen; wir mappen daher das Schweregrad-Label.
    """
    label_to_value = {
        "LOW": 3.0,
        "MODERATE": 5.5,
        "MEDIUM": 5.5,
        "HIGH": 7.5,
        "CRITICAL": 9.5,
    }
    db = vuln.get("database_specific") or {}
    label = db.get("severity")
    if isinstance(label, str) and label.upper() in label_to_value:
        return label_to_value[label.upper()]
    # Fallback: Affected-Eintrag mit Severity prüfen
    for aff in vuln.get("affected") or []:
        sev = (aff.get("database_specific") or {}).get("severity")
        if isinstance(sev, str) and sev.upper() in label_to_value:
            return label_to_value[sev.upper()]
    return None


def _affected_summary(vuln: dict) -> str:
    """Sehr kurze Zusammenfassung der betroffenen Versionen (max 2 Pakete)."""
    out: list[str] = []
    for aff in (vuln.get("affected") or [])[:2]:
        pkg = aff.get("package") or {}
        name = pkg.get("name") or "?"
        eco = pkg.get("ecosystem") or "?"
        # Versions-Bereiche (gehen oft tief; wir kürzen)
        ranges = aff.get("ranges") or []
        events_str = ""
        if ranges:
            evs = (ranges[0].get("events") or [])[:3]
            events_str = ", ".join(
                f"{k}: {v}" for ev in evs for k, v in ev.items()
            )
        out.append(f"{name} ({eco}){' ' + events_str if events_str else ''}")
    return "; ".join(out) if out else ""


def _format_vuln(vuln: dict) -> dict | None:
    """Mapping OSV-Record → Evidora-Result-Dict."""
    if not isinstance(vuln, dict):
        return None
    osv_id = vuln.get("id")
    if not osv_id:
        return None
    cve_id = _pick_cve_alias(vuln)
    summary = (vuln.get("summary") or "").strip()
    details = (vuln.get("details") or "").strip()
    published = (vuln.get("published") or "")[:10]
    year = published[:4] if len(published) >= 4 and published[:4].isdigit() else "—"

    score = _severity_score(vuln)
    affected = _affected_summary(vuln)

    # display_value: kompakte Headline
    display_bits: list[str] = []
    if summary:
        display_bits.append(summary[:240])
    elif details:
        display_bits.append(details[:240].replace("\n", " "))
    else:
        display_bits.append(f"{cve_id}: keine Kurz-Beschreibung verfügbar.")
    if score is not None:
        display_bits.append(f"Severity ≈ {score}/10")
    if published:
        display_bits.append(f"published {published}")
    display_value = " · ".join(display_bits)[:500]

    # description: ausführlicher
    description_parts: list[str] = []
    if details:
        description_parts.append(details[:600].replace("\n", " "))
    if affected:
        description_parts.append(f"Affected: {affected}")
    description_parts.append(
        "Quelle: OSV.dev (aggregiert GHSA, NVD, PyPA, RustSec u. a.). "
        "Severity-Score ist eine technische Einschätzung — die tatsächliche "
        "Ausnutzbarkeit hängt vom konkreten Deployment ab."
    )
    description = " ".join(description_parts)[:900]

    indicator_name = f"OSV {cve_id}: {summary[:120] or osv_id}"
    return {
        "indicator_name": indicator_name[:300],
        "indicator": f"osv_{osv_id.lower()}",
        "country": "GLOBAL",
        "country_name": "Open Source Global",
        "year": year,
        "value": score,
        "display_value": display_value,
        "description": description,
        "url": f"https://osv.dev/vulnerability/{osv_id}",
        "source": "OSV.dev (Open Source Vulnerabilities Aggregator)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_osv(analysis: dict) -> dict:
    """Live-Lookup gegen OSV.dev für CVE-/Package-Vulnerabilities.

    Strategie:
      1. CVE-/GHSA-ID im Claim → direkter Vulnerability-Lookup.
      2. Package-Name (+optional Version) → POST /v1/query.
      3. Nur Package-Name → Liste der bekannten Vulns (limit MAX_RESULTS).
    """
    empty = {
        "source": "OSV.dev",
        "type": "vulnerability_data",
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

    if not _claim_mentions_osv(combined_lc):
        return empty

    ids = _extract_cve_ghsa_ids(combined)
    packages = _detect_packages(combined_lc)

    if not ids and not packages:
        return empty

    results: list[dict] = []
    seen_ids: set[str] = set()

    async with polite_client(timeout=TIMEOUT_S) as client:
        # 1) Direkte ID-Lookups bevorzugt
        for vuln_id in ids:
            vuln = await _fetch_vuln_by_id(client, vuln_id)
            if not vuln:
                continue
            r = _format_vuln(vuln)
            if r and r["indicator"] not in seen_ids:
                seen_ids.add(r["indicator"])
                results.append(r)
                if len(results) >= MAX_RESULTS:
                    break

        # 2) Package-Queries (falls Platz)
        if len(results) < MAX_RESULTS:
            for (name, eco) in packages:
                if len(results) >= MAX_RESULTS:
                    break
                # Version-Hint aus Claim suchen (sucht nach dem User-Token,
                # nicht nach dem OSV-Namen — die Mapping-Tabelle bewahrt den
                # User-Token-Bezug zumindest grob über den eco-Match).
                version = None
                for token, mapping in _PACKAGE_WHITELIST.items():
                    if mapping == (name, eco):
                        version = _extract_version(combined_lc, token)
                        if version:
                            break
                vulns = await _query_by_package(client, name, eco, version)
                for vuln in vulns:
                    if len(results) >= MAX_RESULTS:
                        break
                    r = _format_vuln(vuln)
                    if r and r["indicator"] not in seen_ids:
                        seen_ids.add(r["indicator"])
                        results.append(r)

    if not results:
        logger.info(
            f"OSV.dev: 0 Treffer (ids={ids[:3]}, "
            f"packages={[p[0] for p in packages[:3]]})"
        )
        return empty

    logger.info(f"OSV.dev: {len(results)} Vulnerability-Treffer geliefert")
    return {
        "source": "OSV.dev",
        "type": "vulnerability_data",
        "results": results,
    }
