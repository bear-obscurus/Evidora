"""NIST NVD — National Vulnerability Database (Live-API).

Datenquelle: https://nvd.nist.gov/ — US-Government De-facto-Standard für
CVSS-Scoring von Software-Schwachstellen (351k+ CVE-Records). Komplementär
zu OSV.dev: OSV liefert die Vulnerability-Daten OHNE numerische CVSS-Scores
(Aggregator), NVD liefert dieselben CVEs MIT CVSS v3.1/v3.0/v2.0-Scores
+ CPE-Strings + CISA-KEV-Markierung.

Lizenz: US Government Work / Public Domain — Evidora-tauglich.

API:
- GET https://services.nvd.nist.gov/rest/json/cves/2.0?cveId=CVE-YYYY-NNNN
- GET .../cves/2.0?cpeName=cpe:2.3:a:vendor:product:version:*:*:*:*:*:*:*
- GET .../cves/2.0?keywordSearch=<term>&resultsPerPage=10
- GET .../cves/2.0?hasKev   (CISA Known-Exploited-Vulnerabilities)
- GET .../cves/2.0?cvssV3Severity=HIGH|CRITICAL

Format: JSON, API-Key optional (NVD_API_KEY-Env). Ohne Key: 50 req/30s,
mit Key: 100 req/30s. Service startet auch ohne Key — kein Block.

Trigger-Strategie (komplementär zu OSV, KEIN Hard-Skip bei OSV-Overlap):
  1. CVE-ID im Claim                 → ?cveId=...
  2. Product + Version erkennbar     → CPE-Pattern bauen, ?cpeName=...
  3. Keyword + Severity-Kontext      → ?keywordSearch=...&cvssV3Severity=...
  4. KEV-Trigger ("ausgenutzt", ...) → ?hasKev (zusätzlich)

API-Quirk (04/2026): Non-CISA-KEV-CVEs sind als "Not scheduled" gekenn-
zeichnet (nicht mehr im CVSS-Backlog) → bei vulnStatus "Awaiting Analysis"
oder "Not scheduled" Disclaimer in description rein.

Politische Guardrails: Nur Fakten zu Vulnerability + CVSS + Patch-Status +
KEV-Listung. KEINE Schuldzuweisungen an Vendors. Der Synthesizer baut den
verbindlichen Disclaimer ("CVSS ist eine technische Bewertung, keine
Garantie für Ausnutzbarkeit im konkreten Setup.").
"""

# WIRING für main.py:
# from services.nvd import search_nvd, claim_mentions_nvd_cached
# if claim_mentions_nvd_cached(claim):
#     tasks.append(cached("NIST NVD", search_nvd, analysis))
#     queried_names.append("NIST NVD")

from __future__ import annotations

import logging
import os
import re
import time
from urllib.parse import quote

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

NVD_BASE_URL = "https://services.nvd.nist.gov/rest/json/cves/2.0"

TIMEOUT_S = 15.0
MAX_RESULTS = 5
CACHE_TTL_S = 24 * 60 * 60  # 24 h

# Modul-Level Caches: Key → (ts, payload)
_cve_cache: dict[str, tuple[float, dict | None]] = {}
_query_cache: dict[tuple[str, str], tuple[float, list[dict]]] = {}


# ---------------------------------------------------------------------------
# Regex + Trigger-Whitelists
# ---------------------------------------------------------------------------
_CVE_REGEX = re.compile(r"\bCVE-\d{4}-\d{4,7}\b", re.IGNORECASE)

# Product-Token → (vendor, product) für CPE 2.3-Konstruktion.
# Bewusst konservativ — nur Produkte mit hoher CVSS-Treffer-Quote.
_PRODUCT_CPE_MAP: dict[str, tuple[str, str]] = {
    "log4j": ("apache", "log4j"),
    "log4j-core": ("apache", "log4j"),
    "log4shell": ("apache", "log4j"),
    "apache http server": ("apache", "http_server"),
    "apache httpd": ("apache", "http_server"),
    "httpd": ("apache", "http_server"),
    "tomcat": ("apache", "tomcat"),
    "struts": ("apache", "struts"),
    "openssl": ("openssl", "openssl"),
    "nginx": ("nginx", "nginx"),
    "microsoft exchange": ("microsoft", "exchange_server"),
    "exchange server": ("microsoft", "exchange_server"),
    "windows server": ("microsoft", "windows_server"),
    "windows 10": ("microsoft", "windows_10"),
    "windows 11": ("microsoft", "windows_11"),
    "openssh": ("openbsd", "openssh"),
    "sudo": ("sudo_project", "sudo"),
    "curl": ("haxx", "curl"),
    "bash": ("gnu", "bash"),
    "spring-core": ("vmware", "spring_framework"),
    "spring framework": ("vmware", "spring_framework"),
    "spring4shell": ("vmware", "spring_framework"),
    "jackson-databind": ("fasterxml", "jackson-databind"),
    "kubernetes": ("kubernetes", "kubernetes"),
    "docker": ("docker", "docker"),
    "django": ("djangoproject", "django"),
    "flask": ("palletsprojects", "flask"),
    "wordpress": ("wordpress", "wordpress"),
    "drupal": ("drupal", "drupal"),
    "joomla": ("joomla", "joomla"),
}

# Keyword-Set: löst NVD-Suche aus, wenn KEIN bekanntes Produkt erkannt wird.
_NVD_EXPLICIT_TERMS = (
    "nist", "nvd", "national vulnerability database",
    "cvss", "cvss-score", "cvss score",
    "cisa kev", "cisa-kev", "kev-liste", "kev list",
    "known exploited vulnerability", "known exploited vulnerabilities",
)

_SEVERITY_KEYWORDS = (
    "kritische schwachstelle", "kritische sicherheitslücke", "critical vulnerability",
    "high-severity", "high severity", "hochkritisch",
    "schwere lücke", "schwere sicherheitslücke",
    "kritisch verwundbar", "kritisch", "verwundbar",
)

_KEV_KEYWORDS = (
    "aktiv ausgenutzt", "aktive ausnutzung", "wird ausgenutzt",
    "actively exploited", "in the wild", "exploited in the wild",
    "kev-listung", "cisa kev",
)

# Versions-Pattern in der Nähe eines Produktnamens (z. B. "log4j 2.14.1").
_VERSION_REGEX = re.compile(r"\b(\d+\.\d+(?:\.\d+){0,2})\b")


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def _has_product_token(claim_lc: str) -> bool:
    """Sucht nach bekannten Produkt-Tokens (Wortgrenze-bewusst)."""
    for token in _PRODUCT_CPE_MAP.keys():
        if len(token) <= 4:
            if re.search(r"\b" + re.escape(token) + r"\b", claim_lc):
                return True
        else:
            if token in claim_lc:
                return True
    return False


def _claim_mentions_nvd(claim_lc: str) -> bool:
    """Trigger: CVE-ID / explizit NIST-NVD / Produkt + Severity-Kontext.

    Komplementär zu OSV — DARF parallel feuern. Reranker entscheidet.
    """
    if not claim_lc:
        return False
    # Harter Trigger: CVE-Regex oder NIST-/NVD-/CVSS-Begriffe
    if _CVE_REGEX.search(claim_lc):
        return True
    if any(t in claim_lc for t in _NVD_EXPLICIT_TERMS):
        return True
    # Composite: Produkt + Severity-/KEV-Keyword
    if _has_product_token(claim_lc):
        if any(s in claim_lc for s in _SEVERITY_KEYWORDS):
            return True
        if any(s in claim_lc for s in _KEV_KEYWORDS):
            return True
        # Composite: Produkt + Version + generischer Sec-Kontext
        if _VERSION_REGEX.search(claim_lc) and any(
            k in claim_lc for k in (
                "schwachstelle", "sicherheitslücke", "vulnerability",
                "exploit", "rce", "remote code execution", "patch",
            )
        ):
            return True
    return False


def claim_mentions_nvd_cached(claim: str) -> bool:
    """Wrapper für Trigger-Check — case-normalisiert."""
    return _claim_mentions_nvd((claim or "").lower())


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------
def _extract_cve_ids(claim: str) -> list[str]:
    """Extrahiere CVE-IDs aus Claim-Text (max 3, dedupliziert, UPPER)."""
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
            break
    return out


def _detect_product_with_version(claim_lc: str) -> list[tuple[str, str, str, str | None]]:
    """Erkenne (vendor, product, user-token, version?)-Tupel.

    Versionssuche im Fenster ±40 Zeichen um den Token. Maximal 3 Treffer.
    """
    found: list[tuple[str, str, str, str | None]] = []
    seen: set[tuple[str, str]] = set()
    for token, (vendor, product) in _PRODUCT_CPE_MAP.items():
        if (vendor, product) in seen:
            continue
        idx = -1
        if len(token) <= 4:
            m = re.search(r"\b" + re.escape(token) + r"\b", claim_lc)
            if m:
                idx = m.start()
        else:
            idx = claim_lc.find(token)
        if idx < 0:
            continue
        # Versions-Fenster
        window = claim_lc[max(0, idx - 40): idx + len(token) + 40]
        vm = _VERSION_REGEX.search(window)
        version = vm.group(1) if vm else None
        found.append((vendor, product, token, version))
        seen.add((vendor, product))
        if len(found) >= 3:
            break
    return found


def _build_cpe_name(vendor: str, product: str, version: str | None) -> str:
    """CPE 2.3-String bauen — 11 Komponenten nach `cpe:2.3:a:`."""
    v = version if version else "*"
    return f"cpe:2.3:a:{vendor}:{product}:{v}:*:*:*:*:*:*:*"


# ---------------------------------------------------------------------------
# HTTP-Layer
# ---------------------------------------------------------------------------
def _api_headers() -> dict[str, str]:
    """Optionalen NVD-API-Key als Header anhängen, wenn gesetzt."""
    headers: dict[str, str] = {}
    key = os.getenv("NVD_API_KEY")
    if key:
        headers["apiKey"] = key
    return headers


async def _fetch_cve_by_id(client, cve_id: str) -> dict | None:
    """Einzel-CVE-Lookup mit 24h-Cache. Returnt das raw `cve`-Dict oder None."""
    key = cve_id.upper()
    cached = _cve_cache.get(key)
    if cached and (time.time() - cached[0]) < CACHE_TTL_S:
        return cached[1]
    url = f"{NVD_BASE_URL}?cveId={quote(key, safe='')}"
    try:
        resp = await client.get(url, headers=_api_headers(), follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(f"NVD CVE HTTP {resp.status_code} for {cve_id}")
            _cve_cache[key] = (time.time(), None)
            return None
        data = resp.json() or {}
        vulns = data.get("vulnerabilities") or []
        if not vulns:
            _cve_cache[key] = (time.time(), None)
            return None
        cve = (vulns[0] or {}).get("cve")
        _cve_cache[key] = (time.time(), cve if isinstance(cve, dict) else None)
        return cve if isinstance(cve, dict) else None
    except Exception as e:
        logger.debug(f"NVD CVE fetch failed for {cve_id}: {e}")
        return None


async def _query_nvd(client, params: dict[str, str]) -> list[dict]:
    """Generischer NVD-Query (CPE / Keyword / KEV) mit Cache + Cap auf 10."""
    cache_key = (
        "|".join(f"{k}={v}" for k, v in sorted(params.items())),
        "nvd-query",
    )
    cached = _query_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < CACHE_TTL_S:
        return cached[1]
    # resultsPerPage default 10 — wir cappen offiziell
    q = dict(params)
    q.setdefault("resultsPerPage", "10")
    qs = "&".join(f"{k}={quote(str(v), safe=':*-,./')}" for k, v in q.items())
    url = f"{NVD_BASE_URL}?{qs}"
    try:
        resp = await client.get(url, headers=_api_headers(), follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(f"NVD query HTTP {resp.status_code} qs={qs[:120]}")
            _query_cache[cache_key] = (time.time(), [])
            return []
        data = resp.json() or {}
        vulns_raw = data.get("vulnerabilities") or []
        cves: list[dict] = []
        for entry in vulns_raw:
            c = (entry or {}).get("cve")
            if isinstance(c, dict):
                cves.append(c)
        _query_cache[cache_key] = (time.time(), cves)
        return cves
    except Exception as e:
        logger.debug(f"NVD query failed qs={qs[:120]}: {e}")
        return []


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
_SEVERITY_LABEL = (
    (9.0, "CRITICAL"),
    (7.0, "HIGH"),
    (4.0, "MEDIUM"),
    (0.1, "LOW"),
)


def _label_for_score(score: float | None) -> str:
    if score is None:
        return "UNRATED"
    for threshold, label in _SEVERITY_LABEL:
        if score >= threshold:
            return label
    return "NONE"


def _pick_cvss(cve: dict) -> tuple[float | None, str, str]:
    """Liefert (baseScore, version-string, severity-label).

    v3.1 bevorzugt, fällt zurück auf v3.0 → v2.0.
    """
    metrics = cve.get("metrics") or {}
    for key, label in (
        ("cvssMetricV31", "v3.1"),
        ("cvssMetricV30", "v3.0"),
        ("cvssMetricV2", "v2.0"),
    ):
        entries = metrics.get(key) or []
        if not entries:
            continue
        cvss = (entries[0] or {}).get("cvssData") or {}
        score = cvss.get("baseScore")
        sev = cvss.get("baseSeverity") or _label_for_score(score)
        if isinstance(score, (int, float)):
            return float(score), label, str(sev).upper()
    return None, "—", "UNRATED"


def _short_description_en(cve: dict) -> str:
    for d in cve.get("descriptions") or []:
        if d.get("lang") == "en" and d.get("value"):
            return d["value"].strip()
    descs = cve.get("descriptions") or []
    if descs and isinstance(descs[0], dict):
        return (descs[0].get("value") or "").strip()
    return ""


def _cwe_list(cve: dict) -> list[str]:
    out: list[str] = []
    for w in cve.get("weaknesses") or []:
        for d in w.get("description") or []:
            val = d.get("value")
            if isinstance(val, str) and val.startswith("CWE-") and val not in out:
                out.append(val)
    return out[:3]


def _affected_cpe_summary(cve: dict) -> str:
    """Sehr kurze Liste betroffener CPE-Strings (max 3 vendor:product)."""
    seen: set[str] = set()
    out: list[str] = []
    for cfg in cve.get("configurations") or []:
        nodes = cfg.get("nodes") or []
        for n in nodes:
            for m in n.get("cpeMatch") or []:
                cpe = m.get("criteria") or ""
                # cpe:2.3:a:<vendor>:<product>:<version>:...
                parts = cpe.split(":")
                if len(parts) >= 5:
                    vp = f"{parts[3]}:{parts[4]}"
                    if vp not in seen:
                        seen.add(vp)
                        out.append(vp)
                        if len(out) >= 3:
                            return ", ".join(out)
    return ", ".join(out)


def _is_kev(cve: dict) -> tuple[bool, str | None]:
    """CISA-KEV-Markierung erkennen (cisaExploitAdd-Datum)."""
    added = cve.get("cisaExploitAdd")
    if added:
        return True, str(added)
    return False, None


def _vuln_status_disclaimer(cve: dict) -> str:
    """Disclaimer bei 'Awaiting Analysis' / 'Not scheduled' (04/2026-Quirk)."""
    status = (cve.get("vulnStatus") or "").strip()
    if not status:
        return ""
    if status.lower() in ("awaiting analysis", "not scheduled", "received"):
        return (
            f"HINWEIS: NVD-vulnStatus = '{status}'. Seit 04/2026 markiert das "
            "NIST viele Non-CISA-KEV-Einträge als 'Not scheduled' — der "
            "CVSS-Score kann fehlen oder vorläufig sein."
        )
    return ""


def _format_cve(cve: dict, claim_lc: str | None = None) -> dict | None:
    """Mapping NVD-Record → Evidora-Result-Dict."""
    if not isinstance(cve, dict):
        return None
    cve_id = cve.get("id") or ""
    if not cve_id:
        return None
    score, cvss_ver, sev_label = _pick_cvss(cve)
    desc_en = _short_description_en(cve)
    short_desc = desc_en[:240].replace("\n", " ").strip()
    cwes = _cwe_list(cve)
    affected = _affected_cpe_summary(cve)
    is_kev, kev_date = _is_kev(cve)
    kev_name = cve.get("cisaVulnerabilityName") or ""
    published = (cve.get("published") or "")[:10]
    modified = (cve.get("lastModified") or "")[:10]
    year = published[:4] if len(published) >= 4 and published[:4].isdigit() else "—"
    status_disclaimer = _vuln_status_disclaimer(cve)

    # display_value — kompakte Headline
    score_str = f"{score:.1f}" if score is not None else "n/a"
    display_bits = [
        f"{cve_id} CVSS {cvss_ver}: {score_str} ({sev_label})",
    ]
    if short_desc:
        display_bits.append(short_desc)
    if is_kev:
        display_bits.append(f"In CISA-KEV-Liste seit {kev_date}.")
    display_value = " · ".join(display_bits)[:500]

    # description — ausführlicher
    description_parts: list[str] = []
    if cwes:
        description_parts.append(f"CWE-Klassifikation: {', '.join(cwes)}.")
    if affected:
        description_parts.append(f"Betroffene Produkte (CPE): {affected}.")
    if is_kev:
        kev_line = (
            f"CISA-KEV-Eintrag: {kev_date}"
            + (f" — '{kev_name}'" if kev_name else "")
            + "."
        )
        description_parts.append(kev_line)
    if published:
        description_parts.append(f"Veröffentlicht {published}.")
    if modified and modified != published:
        description_parts.append(f"Zuletzt modifiziert {modified}.")
    if status_disclaimer:
        description_parts.append(status_disclaimer)
    description_parts.append(
        "Quelle: NIST National Vulnerability Database. CVSS ist eine "
        "technische Bewertung des Schweregrads — die tatsächliche "
        "Ausnutzbarkeit hängt vom konkreten Deployment ab."
    )
    description = " ".join(description_parts)[:900]

    # indicator_name — KEV-Marker mitziehen, wenn vorhanden
    name_extra = ""
    if kev_name:
        name_extra = f" ({kev_name[:80]})"
    elif short_desc:
        # Erste Klammer aus Description als Kurz-Alias
        pass
    indicator_name = f"{cve_id}{name_extra}".strip()

    return {
        "indicator_name": indicator_name[:300],
        "indicator": f"nvd_{cve_id.lower()}",
        "country": "GLOBAL",
        "country_name": "Global (NVD)",
        "year": year,
        "value": score,
        "display_value": display_value,
        "description": description,
        "url": f"https://nvd.nist.gov/vuln/detail/{cve_id}",
        "source": "NIST NVD (US National Vulnerability Database)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_nvd(analysis: dict) -> dict:
    """Live-Lookup gegen NIST NVD für CVE-/Product-Vulnerabilities.

    Strategie:
      1. CVE-ID im Claim                 → ?cveId=...
      2. Product + Version erkennbar     → ?cpeName=cpe:2.3:a:vendor:...
      3. Keyword + Severity-Kontext      → ?keywordSearch=...&cvssV3Severity=HIGH
      4. KEV-Trigger                     → zusätzlich ?hasKev
    """
    empty = {
        "source": "NIST NVD",
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

    if not _claim_mentions_nvd(combined_lc):
        return empty

    cve_ids = _extract_cve_ids(combined)
    product_hits = _detect_product_with_version(combined_lc)
    is_kev_query = any(s in combined_lc for s in _KEV_KEYWORDS)
    is_severity_query = any(s in combined_lc for s in _SEVERITY_KEYWORDS)

    if not cve_ids and not product_hits and not is_kev_query and not is_severity_query:
        # Trigger feuerte zwar (z. B. "NIST" allein), aber wir haben nichts
        # Anfragbares — leerer Output ist hier korrekt.
        return empty

    results: list[dict] = []
    seen_indicators: set[str] = set()

    async with polite_client(timeout=TIMEOUT_S) as client:
        # 1) Direkte CVE-ID-Lookups (höchste Präzision)
        for cve_id in cve_ids:
            cve = await _fetch_cve_by_id(client, cve_id)
            if not cve:
                continue
            r = _format_cve(cve, combined_lc)
            if r and r["indicator"] not in seen_indicators:
                seen_indicators.add(r["indicator"])
                results.append(r)
                if len(results) >= MAX_RESULTS:
                    break

        # 2) CPE-Lookups für (vendor, product, version)
        if len(results) < MAX_RESULTS:
            for vendor, product, _tok, version in product_hits:
                if len(results) >= MAX_RESULTS:
                    break
                cpe = _build_cpe_name(vendor, product, version)
                cves = await _query_nvd(client, {"cpeName": cpe})
                # Sortiere nach CVSS-Score absteigend, damit CRITICAL zuerst kommt
                cves_sorted = sorted(
                    cves,
                    key=lambda c: (_pick_cvss(c)[0] or 0.0),
                    reverse=True,
                )
                for cve in cves_sorted:
                    if len(results) >= MAX_RESULTS:
                        break
                    r = _format_cve(cve, combined_lc)
                    if r and r["indicator"] not in seen_indicators:
                        seen_indicators.add(r["indicator"])
                        results.append(r)

        # 3) Keyword-Suche bei Severity-Kontext (nur wenn IDs+CPE noch frei)
        if len(results) < MAX_RESULTS and is_severity_query and product_hits:
            # Erstes erkanntes Produkt als Keyword nehmen
            _vendor, prod, tok, _ver = product_hits[0]
            keyword = tok if tok else prod
            params = {
                "keywordSearch": keyword,
                "cvssV3Severity": "HIGH",
                "resultsPerPage": "10",
            }
            cves = await _query_nvd(client, params)
            for cve in cves:
                if len(results) >= MAX_RESULTS:
                    break
                r = _format_cve(cve, combined_lc)
                if r and r["indicator"] not in seen_indicators:
                    seen_indicators.add(r["indicator"])
                    results.append(r)

        # 4) KEV-Query: ?hasKev — zusätzlich, wenn Claim "ausgenutzt" sagt
        if len(results) < MAX_RESULTS and is_kev_query:
            params = {"hasKev": "", "resultsPerPage": "10"}
            # Wenn wir ein Produkt-Token haben, eingrenzen
            if product_hits:
                _v, prod, tok, _vv = product_hits[0]
                params["keywordSearch"] = tok or prod
            cves = await _query_nvd(client, params)
            for cve in cves:
                if len(results) >= MAX_RESULTS:
                    break
                r = _format_cve(cve, combined_lc)
                if r and r["indicator"] not in seen_indicators:
                    seen_indicators.add(r["indicator"])
                    results.append(r)

    if not results:
        logger.info(
            f"NIST NVD: 0 Treffer (cves={cve_ids[:3]}, "
            f"products={[p[1] for p in product_hits[:3]]}, "
            f"kev={is_kev_query}, sev={is_severity_query})"
        )
        return empty

    logger.info(f"NIST NVD: {len(results)} CVE-Treffer geliefert")
    return {
        "source": "NIST NVD",
        "type": "vulnerability_data",
        "results": results,
    }
