"""Wayback-Machine Live-Connector — Internet-Archive-Snapshot-Lookup via CDX-API.

Das Internet Archive (archive.org) ist die größte öffentlich-frei zugängliche
Web-Archiv-Sammlung weltweit (~99 % aller größeren öffentlichen Sites). Für
Faktencheck-Zwecke liefert es eine seltene historische Empirie:

- "Politiker X sagte Y auf Website Z am Datum D" → Snapshot-Check direkt
- "URL X enthält den Inhalt Y" — heute 404, archiviert aber noch lesbar
  (häufig bei nachträglich gelöschten Politiker-Statements / PR-Texten)
- "Wie oft wurde URL X archiviert?" → Frequency-Indikator für Relevanz
- Disinformations-Konter: "URL X hätte das nie gesagt" → Snapshot beweist
  das Gegenteil (oder die Abwesenheit eines Snapshots)

Komplementär zu existierenden Quellen:
- Wikipedia: Enzyklopädische Definitionen
- Faktencheck-RSS (Snopes/Correctiv/...): redaktionelle Bewertungen
- GDELT: aktuelle News-Coverage
- Static-First-Packs: kuratierte Konsens-Daten
- WAYBACK: historische Original-Quellen-Snapshots (URL-spezifisch)

API: https://web.archive.org/cdx/search/cdx
+ Optional: https://archive.org/wayback/available für Spot-Checks

Free, kein Auth, polite User-Agent erbeten (~15 req/min pro IP zumutbar).

Trigger: Claim-Text enthält URL-Pattern (http://, https://, www.) ODER
analysis.entities enthält domain-ähnliche Strings.

Wiring: main.py imports + tasks.append, reranker (NICHT in
AUTHORITATIVE_INDICATORS — ist Live-Quelle).

Wayback-Limitationen:
- Private/Login-pflichtige Seiten meist NICHT archiviert
- Twitter/X seit 2023 stark eingeschränkt
- Manche dynamische Seiten zeigen "fehlerhafte" Snapshots
- Archive-Frequenz nicht konsistent — manche URLs alle 2 Wochen, andere
  1× pro Jahr
- Sehr neue URLs: erste Archivierung kann Tage dauern
- Geo-restricted URLs: Wayback umgeht das nicht zuverlässig
"""

import asyncio
import logging
import re
from urllib.parse import quote, urlparse

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# CDX-API: Snapshot-Listing für eine URL
# &output=json    → JSON-Response (erste Zeile = Header)
# &limit=10       → max 10 Snapshots zurück (sortiert nach timestamp asc)
# &filter=statuscode:200  → nur erfolgreiche Captures
WAYBACK_CDX_API = (
    "https://web.archive.org/cdx/search/cdx"
    "?url={url}&output=json&limit=10&filter=statuscode:200"
)

# URL- und Domain-Pattern (siehe README im Spec)
URL_PATTERN = re.compile(
    r'https?://(?:www\.)?[\w\-]+(?:\.[\w\-]+)+(?:/[\w\-./?%&=]*)?',
    re.IGNORECASE,
)
DOMAIN_PATTERN = re.compile(
    r'(?:^|\s)((?:www\.)?[\w\-]+\.[a-z]{2,})(?=[\s/]|$)',
    re.IGNORECASE,
)

# Skip-Liste: archive.org selbst (Rekursion) + private/local Hosts
_SKIP_HOST_SUFFIXES = (
    "archive.org",
    "web.archive.org",
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "::1",
)
_SKIP_HOST_PRIVATE_PREFIXES = (
    "10.",
    "192.168.",
    "172.16.",
    "172.17.",
    "172.18.",
    "172.19.",
    "172.20.",
    "172.21.",
    "172.22.",
    "172.23.",
    "172.24.",
    "172.25.",
    "172.26.",
    "172.27.",
    "172.28.",
    "172.29.",
    "172.30.",
    "172.31.",
)


def _is_skippable_host(host: str) -> bool:
    """True wenn der Host Wayback selbst, localhost oder eine private IP ist."""
    if not host:
        return True
    h = host.lower().strip()
    for suffix in _SKIP_HOST_SUFFIXES:
        if h == suffix or h.endswith("." + suffix):
            return True
    for prefix in _SKIP_HOST_PRIVATE_PREFIXES:
        if h.startswith(prefix):
            return True
    return False


def _normalize_url(raw: str) -> str | None:
    """Bereinige einen URL-Kandidaten und entscheide, ob er archivierbar ist.

    Akzeptiert:  https://example.com/path  und  example.com  und  www.x.de
    Returns:     vollständige URL mit Scheme oder None wenn nicht usable.
    """
    if not raw:
        return None
    raw = raw.strip().strip(",.;:)('\"")
    if not raw:
        return None
    # Domain-only-Heuristik → Scheme ergänzen
    if not raw.lower().startswith(("http://", "https://")):
        raw = "https://" + raw
    try:
        parsed = urlparse(raw)
    except Exception:
        return None
    host = (parsed.hostname or "").lower()
    if not host or "." not in host:
        return None
    if _is_skippable_host(host):
        return None
    # Ungültige TLDs (zu kurz oder nur Zahlen) ausfiltern
    tld = host.rsplit(".", 1)[-1]
    if len(tld) < 2 or tld.isdigit():
        return None
    return raw


def _extract_urls(claim: str, entities: list[str]) -> list[str]:
    """Sammle bis zu N URLs aus Claim-Text + entity-Liste.

    1. Volle URL-Pattern-Treffer aus claim (http(s)://...)
    2. Volle URL-Treffer aus entities
    3. Domain-only-Treffer aus claim als Fallback (kein Schema)
    4. Domain-only-Treffer aus entities

    Dedupliziert, max 3 URLs. Skippt archive.org/localhost/private IPs.
    """
    candidates: list[str] = []
    seen: set[str] = set()

    def _add(c: str | None) -> None:
        if not c:
            return
        norm = _normalize_url(c)
        if not norm:
            return
        # Dedup-Key: Host + Pfad ohne Trailing-Slash
        try:
            p = urlparse(norm)
            key = f"{p.hostname or ''}{(p.path or '').rstrip('/')}"
        except Exception:
            key = norm
        if key in seen:
            return
        seen.add(key)
        candidates.append(norm)

    text = claim or ""
    # 1. Volle URLs im claim
    for m in URL_PATTERN.findall(text):
        _add(m)

    # 2. Volle URLs in entities
    for e in entities or []:
        if not e:
            continue
        for m in URL_PATTERN.findall(e):
            _add(m)
        # entity selbst kann eine bare URL/Domain sein
        if e.lower().startswith(("http://", "https://")):
            _add(e)

    # 3. Domain-only-Fallback aus claim
    if len(candidates) < 3:
        for m in DOMAIN_PATTERN.findall(text):
            _add(m)

    # 4. Domain-only-Fallback aus entities
    if len(candidates) < 3:
        for e in entities or []:
            if not e:
                continue
            for m in DOMAIN_PATTERN.findall(e):
                _add(m)

    return candidates[:3]


def claim_has_url_cached(claim: str) -> bool:
    """Schneller Trigger-Pre-Check: enthält der Claim plausibel eine URL/Domain?

    Wird vom Pipeline-Trigger benutzt, um den Wayback-Aufruf nur bei
    URL-haltigen Claims zu starten. KEIN Networking, rein regex.
    """
    if not claim:
        return False
    if URL_PATTERN.search(claim):
        return True
    return bool(DOMAIN_PATTERN.search(claim))


def _format_year_month(timestamp: str) -> str:
    """Wayback-Timestamp 'yyyymmddHHMMSS' → 'yyyy-mm-dd' (oder kürzere Form)."""
    if not timestamp or len(timestamp) < 4:
        return "—"
    if len(timestamp) >= 8:
        return f"{timestamp[0:4]}-{timestamp[4:6]}-{timestamp[6:8]}"
    if len(timestamp) >= 6:
        return f"{timestamp[0:4]}-{timestamp[4:6]}"
    return timestamp[0:4]


async def _fetch_snapshots(client, target_url: str) -> list[dict] | None:
    """CDX-Query für eine URL.

    Returns Liste von dicts {timestamp, original, statuscode, mimetype}
    sortiert nach Timestamp aufsteigend, max 10 Einträge. None bei Fehler.
    [] bei "URL nie archiviert" — das ist KEIN Fehler.
    """
    cdx_url = WAYBACK_CDX_API.format(url=quote(target_url, safe=":/?&=%"))
    try:
        resp = await client.get(cdx_url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"Wayback CDX HTTP {resp.status_code} für {target_url[:60]}"
            )
            return None
        data = resp.json()
    except Exception as e:
        logger.debug(f"Wayback CDX fetch failed for {target_url[:60]}: {e}")
        return None

    # CDX gibt [] zurück, wenn nichts archiviert wurde
    if not isinstance(data, list) or len(data) < 2:
        return []

    header = data[0]
    rows = data[1:]
    # Header-Indizes lokalisieren (Reihenfolge ist meist stabil, aber paranoid)
    try:
        i_ts = header.index("timestamp")
        i_orig = header.index("original")
        i_status = header.index("statuscode")
        i_mime = header.index("mimetype") if "mimetype" in header else -1
    except (ValueError, AttributeError):
        return None

    snapshots: list[dict] = []
    for row in rows:
        if not isinstance(row, list) or len(row) <= max(i_ts, i_orig, i_status):
            continue
        snapshots.append({
            "timestamp": row[i_ts],
            "original": row[i_orig],
            "statuscode": row[i_status],
            "mimetype": row[i_mime] if i_mime >= 0 and i_mime < len(row) else "",
        })
    # CDX liefert i.d.R. zeitlich aufsteigend; defensiv noch einmal sortieren.
    snapshots.sort(key=lambda s: s["timestamp"])
    return snapshots


def _pick_representative_snapshots(snapshots: list[dict]) -> list[dict]:
    """Wähle ältesten, neuesten, ggf. mittleren Snapshot (bei Range > 1 Jahr)."""
    if not snapshots:
        return []
    if len(snapshots) == 1:
        return [snapshots[0]]
    oldest = snapshots[0]
    newest = snapshots[-1]
    picks = [oldest, newest]
    # Mittleren nur ergänzen, wenn Spanne > 1 Jahr (yyyy-Vergleich)
    try:
        if int(newest["timestamp"][:4]) - int(oldest["timestamp"][:4]) >= 2:
            mid = snapshots[len(snapshots) // 2]
            if mid["timestamp"] not in (oldest["timestamp"], newest["timestamp"]):
                picks.insert(1, mid)
    except (ValueError, KeyError):
        pass
    return picks


def _build_result(target_url: str, snapshots: list[dict]) -> dict | None:
    """Konvertiere Snapshot-Liste in Evidora-Result-Dict. None wenn 0 Snapshots."""
    if not snapshots:
        return None

    oldest = snapshots[0]
    newest = snapshots[-1]
    oldest_ymd = _format_year_month(oldest["timestamp"])
    newest_ymd = _format_year_month(newest["timestamp"])
    year_old = oldest["timestamp"][:4] if oldest["timestamp"] else "—"
    year_new = newest["timestamp"][:4] if newest["timestamp"] else "—"
    year_field = year_old if year_old == year_new else f"{year_old}-{year_new}"

    # Host für Anzeigetitel
    try:
        host = urlparse(target_url).hostname or target_url
    except Exception:
        host = target_url
    host = (host or "").lower()
    if host.startswith("www."):
        host = host[4:]

    snap_count = len(snapshots)
    indicator_name = (
        f"URL {host} archiviert ({snap_count} Snapshot"
        f"{'s' if snap_count != 1 else ''} seit {_format_year_month(oldest['timestamp'])[:7]})"
    )

    # Wayback-Wildcard-Browse-URL (Kalenderansicht aller Snapshots)
    wayback_browse = (
        f"https://web.archive.org/web/{year_new}*/{target_url}"
    )
    # Direkt-Link auf neuesten Snapshot
    wayback_newest = (
        f"https://web.archive.org/web/{newest['timestamp']}/{target_url}"
    )

    display = (
        f"{target_url} archiviert: {snap_count} Snapshot"
        f"{'s' if snap_count != 1 else ''}, "
        f"ältester {oldest_ymd}, neuester {newest_ymd}. "
        f"Wayback-Zugriff: {wayback_browse}"
    )[:500]

    description = (
        "Internet-Archive-Snapshots der URL über Zeit. "
        "Verwendung: 'auf URL X stand am Datum Y aber Z' empirisch "
        "überprüfbar."
    )

    return {
        "indicator_name": indicator_name,
        "indicator": "wayback_archive",
        "country": "—",
        "year": year_field,
        "topic": "wayback_url_history",
        "display_value": display,
        "description": description,
        "url": wayback_browse,
        "secondary_url": wayback_newest,
        "source": (
            "Internet Archive — Wayback Machine CDX API (frei, gemeinnützig)"
        ),
    }


async def _query_one_url(client, target_url: str) -> dict | None:
    """Hole Snapshots für eine URL und baue ein Result-Dict — None bei 0 Hits."""
    snapshots = await _fetch_snapshots(client, target_url)
    if snapshots is None:
        # Echter Fehler — silently skippen
        return None
    if not snapshots:
        # 0 Snapshots — URL existiert evtl., wurde aber nie archiviert
        logger.debug(f"Wayback: 0 Snapshots für {target_url[:60]}")
        return None
    # Repräsentative Auswahl wird aktuell NICHT direkt im Output genutzt
    # (wir reporten Aggregat-Statistik), aber sie könnte für die Frontend-
    # Liste später interessant werden.
    _ = _pick_representative_snapshots(snapshots)
    return _build_result(target_url, snapshots)


async def search_wayback(analysis: dict) -> dict:
    """Live-Lookup gegen Wayback-Machine-CDX-API für URLs/Domains im Claim.

    Trigger-Pfad: Pipeline ruft die Funktion nur, wenn der Claim eine URL
    enthält (siehe ``claim_has_url_cached``). Der Funktion werden trotzdem
    Edge-Cases gegönnt (leere/none-Eingaben, 0 URLs).

    Returns Dict mit ≤3 URL-History-Treffern.
    """
    empty = {"source": "Wayback Machine", "type": "url_archive", "results": []}

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original_claim") or analysis.get("text") or ""
    entities = analysis.get("entities", []) or []

    urls = _extract_urls(claim, entities)
    if not urls:
        return empty

    async with polite_client(timeout=15.0) as client:
        tasks = [_query_one_url(client, u) for u in urls]
        results_raw = await asyncio.gather(*tasks, return_exceptions=True)

    results: list[dict] = []
    seen_urls: set[str] = set()
    for r in results_raw:
        if isinstance(r, Exception) or not r:
            continue
        key = r.get("secondary_url") or r.get("url") or ""
        if key in seen_urls:
            continue
        seen_urls.add(key)
        results.append(r)

    if not results:
        logger.info(
            f"Wayback: 0 Snapshot-Treffer für URLs "
            f"{[u[:50] for u in urls]}"
        )
        return empty

    logger.info(
        f"Wayback: {len(results)} URL-History-Treffer geliefert "
        f"(geprüft: {len(urls)} URLs)"
    )
    return {
        "source": "Wayback Machine",
        "type": "url_archive",
        "results": results,
    }
