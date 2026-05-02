#!/usr/bin/env python3
"""URL-Health-Check für **neu hinzugekommene** URLs in einem PR.

Im Gegensatz zu ``check_urls.py`` (vollständiger Audit aller URLs),
analysiert dieses Tool den ``git diff`` zwischen einem PR-Branch und
einem Base-Branch (typisch ``main``), extrahiert nur die NEU
hinzugekommenen URLs in ``data/*.json``-Dateien und prüft diese.

Use-Case: CI-Gate. Wenn ein PR neue URLs einführt, müssen sie
funktionieren. Alte URLs werden vom periodischen Voll-Audit
(``check_urls.py``, Cron-Job) gehandhabt — dieses Tool soll PRs nicht
unnötig blockieren wegen Tot-Links, die schon vor dem PR existierten.

Usage:
  # Lokal vor dem PR:
  python3 tools/check_new_urls.py --base main --head HEAD

  # In CI (GitHub Actions):
  python3 tools/check_new_urls.py --base origin/main --head HEAD
"""
from __future__ import annotations

import argparse
import asyncio
import re
import subprocess
import sys
from typing import Any

import httpx

USER_AGENT = "Evidora/1.0 (+https://evidora.eu; URL CI gate)"
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.0 Safari/605.1.15"
)
URL_RE = re.compile(r'https?://[^\s"<>\)]+')
TRAILING_TRIM = ".,;:!?)]}\"'"


def clean_url(url: str) -> str:
    while url and url[-1] in TRAILING_TRIM:
        url = url[:-1]
    return url


def get_diff_added_lines(base: str, head: str, path_glob: str) -> list[str]:
    """Run git diff between base..head, return only added lines (start with +)
    from files matching path_glob."""
    try:
        cmd = ["git", "diff", f"{base}...{head}", "--", path_glob]
        out = subprocess.check_output(cmd, encoding="utf-8")
    except subprocess.CalledProcessError as e:
        sys.exit(f"git diff failed: {e}")
    added: list[str] = []
    for line in out.splitlines():
        if line.startswith("+") and not line.startswith("+++"):
            added.append(line[1:])  # strip the leading +
    return added


def extract_new_urls(added_lines: list[str]) -> set[str]:
    urls: set[str] = set()
    for line in added_lines:
        for u in URL_RE.findall(line):
            cleaned = clean_url(u)
            if cleaned:
                urls.add(cleaned)
    return urls


def get_existing_urls(base: str, path_glob: str) -> set[str]:
    """All URLs that exist in the base branch — these are pre-existing,
    not introduced by the PR."""
    try:
        # ls-files at base, then concat content of matching files
        ls_cmd = ["git", "ls-tree", "-r", "--name-only", base, "--", path_glob]
        files = subprocess.check_output(ls_cmd, encoding="utf-8").splitlines()
    except subprocess.CalledProcessError:
        return set()
    urls: set[str] = set()
    for f in files:
        try:
            content = subprocess.check_output(
                ["git", "show", f"{base}:{f}"], encoding="utf-8"
            )
        except subprocess.CalledProcessError:
            continue
        for u in URL_RE.findall(content):
            cleaned = clean_url(u)
            if cleaned:
                urls.add(cleaned)
    return urls


async def check_one(client: httpx.AsyncClient, sem: asyncio.Semaphore,
                    url: str) -> dict[str, Any]:
    async with sem:
        # Stage 1: HEAD with polite UA
        try:
            r = await client.head(url, follow_redirects=True, timeout=10.0)
            if r.status_code < 400:
                return {"url": url, "status": r.status_code, "method": "HEAD"}
        except Exception:
            pass
        # Stage 2: GET with polite UA
        try:
            r = await client.get(url, follow_redirects=True, timeout=10.0)
            if r.status_code < 400:
                return {"url": url, "status": r.status_code, "method": "GET"}
            polite_status = r.status_code
        except Exception:
            polite_status = None
        # Stage 3: GET with Browser-UA fallback (filters UA-blocks)
        try:
            r = await client.get(url, follow_redirects=True, timeout=15.0,
                                 headers={"User-Agent": BROWSER_UA})
            return {"url": url, "status": r.status_code,
                    "method": "GET-browser-ua",
                    "polite_status": polite_status}
        except Exception as e:
            return {"url": url, "status": None,
                    "error": f"{type(e).__name__}: {e}",
                    "method": "GET-browser-ua",
                    "polite_status": polite_status}


async def main_async(args: argparse.Namespace) -> int:
    # Find new URLs introduced by the PR
    added_lines = get_diff_added_lines(args.base, args.head, "data/*.json")
    new_urls = extract_new_urls(added_lines)
    existing = get_existing_urls(args.base, "data/*.json")
    actually_new = new_urls - existing

    if not actually_new:
        print("[check-new-urls] No new URLs introduced in this diff.")
        return 0

    print(f"[check-new-urls] Found {len(actually_new)} new URLs to check.")

    sem = asyncio.Semaphore(args.concurrency)
    headers = {"User-Agent": USER_AGENT}
    async with httpx.AsyncClient(headers=headers) as client:
        results = await asyncio.gather(
            *[check_one(client, sem, u) for u in sorted(actually_new)]
        )

    # Klassifizierung — wir unterscheiden:
    #  OK       2xx/3xx — alles gut
    #  DEAD     404/410 — echt tot, blockiert PR
    #  BLOCKED  403/Cloudflare/Bot-Filter — Browser-OK, nur Warnung
    #  TIMEOUT  Connection-Error/Timeout — transient, nur Warnung
    #  OTHER    sonstige 4xx/5xx — vorsichtig: Warnung
    ok, dead, blocked, transient, other = [], [], [], [], []
    for r in results:
        s = r["status"]
        if s is None:
            transient.append(r)
        elif s < 400:
            ok.append(r)
        elif s in (404, 410):
            dead.append(r)
        elif s == 403:
            blocked.append(r)
        else:
            other.append(r)

    print(f"\n=== Results ({len(results)} new URLs) ===")
    print(f"  OK:       {len(ok)}")
    if dead:
        print(f"  DEAD:     {len(dead)}  (404/410 — blocking)")
    if blocked:
        print(f"  BLOCKED:  {len(blocked)}  (403 — likely Bot-Filter, warn only)")
    if transient:
        print(f"  TIMEOUT:  {len(transient)}  (transient, warn only)")
    if other:
        print(f"  OTHER:    {len(other)}  (other 4xx/5xx, warn only)")

    if blocked or transient or other:
        print("\nWarnings (URLs to spot-check manually):")
        for r in blocked + transient + other:
            marker = r["status"] if r["status"] is not None else r.get("error", "?")[:40]
            print(f"  [{marker!s:6}] {r['url']}")

    if dead:
        print("\nDEAD URLs introduced by this PR (4xx with browser-UA):")
        for r in dead:
            print(f"  [{r['status']}] {r['url']}")
        print()
        print("These URLs are tot. Please fix them before merging — see")
        print("CONTRIBUTING.md §URL-Stability for the replacement hierarchy.")
        return 1

    print("\nGate passed (no DEAD URLs).")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Check that new URLs introduced in a PR work.",
    )
    ap.add_argument("--base", default="origin/main",
                    help="Base reference (default: origin/main)")
    ap.add_argument("--head", default="HEAD",
                    help="PR head reference (default: HEAD)")
    ap.add_argument("--concurrency", type=int, default=10)
    args = ap.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
