#!/usr/bin/env python3
"""URL-Repair-Helper für broken URLs in ``data/*.json``.

Nimmt eine Liste broken URLs und liefert Reparatur-Vorschläge anhand:
1. Wayback CDX API — gibt es einen Snapshot der toten URL?
2. Domain-Hauptseite — funktioniert die Root-Domain?
3. DOI-Suche bei wissenschaftlichen Domains (BMJ, Nature, Science) —
   Crossref API.

Output ist eine strukturierte Liste pro URL, die manuelle Validierung
unterstützt. Ist KEIN Auto-Patcher — die finale Entscheidung über die
Replacement-URL bleibt manuell, weil URL-Reparatur immer
fact-check-ähnliche Sorgfalt verlangt.

Usage:
  # 1. Vorschläge für alle Tier-1-broken-URLs sammeln:
  python3 tools/repair_urls.py --from /tmp/url_check_v2.json

  # 2. Vorschläge für eine einzelne URL:
  python3 tools/repair_urls.py --url https://www.adl.org/resources/backgrounder/quantifying-hate-soros
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from typing import Any
from urllib.parse import urlparse

import httpx

USER_AGENT = "Evidora/1.0 (+https://evidora.eu; URL repair tool)"
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.0 Safari/605.1.15"
)


async def wayback_lookup(client: httpx.AsyncClient, url: str) -> str | None:
    """Use the Wayback CDX API to find the most recent snapshot."""
    try:
        cdx_url = "http://web.archive.org/cdx/search/cdx"
        params = {
            "url": url,
            "output": "json",
            "limit": "-5",  # 5 most recent, may include non-200
        }
        r = await client.get(cdx_url, params=params, timeout=15.0)
        if r.status_code != 200:
            return None
        rows = r.json()
        if len(rows) < 2:
            return None
        # rows[0] is header, rows[1+] are entries
        last = rows[-1]
        timestamp = last[1]
        original = last[2]
        return f"https://web.archive.org/web/{timestamp}/{original}"
    except Exception:
        return None


async def check_status(client: httpx.AsyncClient, url: str) -> int | None:
    """Quick check with Browser-UA."""
    for method in ("HEAD", "GET"):
        try:
            r = await client.request(method, url, follow_redirects=True,
                                     timeout=10.0,
                                     headers={"User-Agent": BROWSER_UA})
            return r.status_code
        except Exception:
            continue
    return None


async def domain_root(client: httpx.AsyncClient, url: str) -> tuple[str, int | None]:
    """Test if the domain root is reachable."""
    p = urlparse(url)
    root = f"{p.scheme}://{p.netloc}/"
    status = await check_status(client, root)
    return root, status


async def doi_lookup_for_journal(client: httpx.AsyncClient, url: str) -> str | None:
    """Heuristic DOI extraction for BMJ, Nature, Science URLs."""
    # BMJ pattern: bmj.com/content/<volume>/<issue>/<page>
    m = re.match(r"https?://(?:www\.)?bmj\.com/content/(\d+)(?:/(\d+))?/(\d+)", url)
    if m:
        vol, issue, page = m.group(1), m.group(2), m.group(3)
        doi = f"10.1136/bmj.{vol}.{issue}.{page}" if issue else f"10.1136/bmj.{vol}.{page}"
        candidate = f"https://doi.org/{doi}"
        status = await check_status(client, candidate)
        return candidate if status and status < 400 else None
    # Cell pattern: cell.com/.../fulltext/...
    # Could add more journal patterns here
    return None


async def repair_one(client: httpx.AsyncClient, url: str) -> dict[str, Any]:
    """Generate repair suggestions for one broken URL."""
    suggestions: list[dict] = []

    # 1. Wayback snapshot
    way = await wayback_lookup(client, url)
    if way:
        way_status = await check_status(client, way)
        suggestions.append({
            "type": "wayback",
            "url": way,
            "status": way_status,
        })

    # 2. DOI for journal URLs
    doi = await doi_lookup_for_journal(client, url)
    if doi:
        suggestions.append({"type": "doi", "url": doi, "status": 200})

    # 3. Domain root
    root, root_status = await domain_root(client, url)
    suggestions.append({
        "type": "domain_root",
        "url": root,
        "status": root_status,
    })

    return {
        "broken_url": url,
        "domain": urlparse(url).netloc,
        "suggestions": suggestions,
    }


async def main_async(args: argparse.Namespace) -> int:
    if args.url:
        urls = [args.url]
    elif args.from_file:
        with open(args.from_file) as f:
            data = json.load(f)
        urls = []
        for tier in ("tier1", "tier2_sample", "tier3_sample"):
            urls.extend(
                r["url"] for r in data.get(tier, [])
                if r["status"] is None or r["status"] >= 400
            )
        urls = sorted(set(urls))
    else:
        sys.exit("either --url or --from is required")

    print(f"[repair] checking {len(urls)} URLs ...", file=sys.stderr)

    sem = asyncio.Semaphore(8)
    headers = {"User-Agent": USER_AGENT}

    async def bound(client, url):
        async with sem:
            return await repair_one(client, url)

    async with httpx.AsyncClient(headers=headers) as client:
        results = await asyncio.gather(*[bound(client, u) for u in urls])

    if args.out:
        with open(args.out, "w") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"[repair] saved -> {args.out}", file=sys.stderr)

    # Summary
    for r in results:
        print(f"\n=== {r['broken_url']}")
        for s in r["suggestions"]:
            marker = "OK" if s.get("status") and s["status"] < 400 else "??"
            print(f"  [{marker}] {s['type']:12s} {s['url']}")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="URL repair suggestions")
    ap.add_argument("--url", help="Single URL to repair")
    ap.add_argument("--from", dest="from_file",
                    help="JSON file with check_urls.py results — use all 4xx URLs")
    ap.add_argument("--out", help="Write suggestions to JSON file")
    args = ap.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
