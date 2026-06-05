#!/usr/bin/env python3
"""Run a stress test against an Evidora backend.

Reads a claim set from a JSON file (see ``tools/stress_tests/*.json``),
fires each claim against the backend, and reports verdict-match,
source-match, evidence count and per-claim duration.

With ``--check-urls``, also validates all evidence URLs returned by the
backend (HEAD with GET fallback, browser-UA retry for 4xx).

Usage:
  python3 tools/stress_test.py --claims tools/stress_tests/esoterik.json
  python3 tools/stress_test.py --claims my_set.json --url http://localhost:8000
                               --concurrency 4 --out /tmp/results.json
  python3 tools/stress_test.py --claims tools/stress_tests/comprehensive_100_v2.json \
                               --check-urls --out /tmp/results_with_urls.json

Environment:
  EVIDORA_TEST_API_KEY   bypass the per-IP rate limit (X-Evidora-Test-Key
                         header). Optional but recommended for parallel
                         runs against the public deployment.

Methodology: see ARCHITECTURE.md §4.4 and memory/stress_test_method.md
(four measurement points: verdict-match, source-match, trigger gaps,
hot-reload mini-test).

Toleranz: ``false``/``mostly_false`` are mutually accepted, analog
``true``/``mostly_true``.

Exit code: 0 if verdict-match >= threshold (default 18/20 i.e. 0.9),
1 otherwise. CI- and cron-job-friendly.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from collections import Counter
from typing import Any

import httpx

# ---------------------------------------------------------------------------
# URL-Check constants
# ---------------------------------------------------------------------------

POLITE_UA = "Evidora/1.0 (+https://evidora.eu; URL health-check)"
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.0 Safari/605.1.15"
)
TRAILING_TRIM = ".,;:!?)]}\"'"


def _clean_url(url: str) -> str:
    while url and url[-1] in TRAILING_TRIM:
        url = url[:-1]
    return url


# ---------------------------------------------------------------------------
# Claim-set loading
# ---------------------------------------------------------------------------

def load_claim_set(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if "claims" not in data:
        raise ValueError(f"{path}: no 'claims' key")
    return data


# ---------------------------------------------------------------------------
# Single-claim runner
# ---------------------------------------------------------------------------

async def run_one(client: httpx.AsyncClient, claim: dict, *,
                  url: str, api_key: str | None) -> dict:
    t0 = time.time()
    out: dict[str, Any] = {
        "id": claim.get("id"),
        "category": claim.get("category", ""),
        "claim": claim["claim"],
        "expected_verdicts": claim.get("expected_verdicts", []),
        "expected_source": claim.get("expected_source"),
        "verdict": None,
        "confidence": None,
        "evidence_count": 0,
        "sources_with_results": [],
        "evidence_urls": [],
        "error": None,
        "duration_s": None,
    }
    headers: dict[str, str] = {"Accept": "text/event-stream"}
    if api_key:
        headers["X-Evidora-Test-Key"] = api_key
    try:
        async with client.stream(
            "POST", f"{url}/api/check",
            json={"claim": claim["claim"]},
            headers=headers,
            timeout=httpx.Timeout(connect=10, read=300, write=10, pool=10),
        ) as resp:
            if resp.status_code != 200:
                out["error"] = f"HTTP {resp.status_code}"
                return out
            event = None
            async for line in resp.aiter_lines():
                if line.startswith("event:"):
                    event = line.split(":", 1)[1].strip()
                elif line.startswith("data:") and event == "result":
                    try:
                        d = json.loads(line.split(":", 1)[1].strip())
                        out["verdict"] = d.get("verdict")
                        out["confidence"] = d.get("confidence")
                        evidence = d.get("evidence") or []
                        out["evidence_count"] = len(evidence)
                        # Extract evidence URLs
                        for ev in evidence:
                            u = ev.get("url")
                            if u:
                                out["evidence_urls"].append(_clean_url(u))
                        cov = d.get("source_coverage") or {}
                        out["sources_with_results"] = cov.get("names", [])
                    except Exception as e:
                        out["error"] = f"parse: {e}"
                elif event == "done":
                    break
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    out["duration_s"] = round(time.time() - t0, 1)
    return out


# ---------------------------------------------------------------------------
# Verdict / source matching
# ---------------------------------------------------------------------------

def verdict_matches(verdict: str | None, expected: list[str]) -> bool:
    """Apply the false<->mostly_false / true<->mostly_true tolerance."""
    if not expected:
        return True  # no expectation set
    if verdict in expected:
        return True
    pairs = [{"false", "mostly_false"}, {"true", "mostly_true"}]
    for pair in pairs:
        if verdict in pair and any(e in pair for e in expected):
            return True
    return False


def source_matches(
    sources: list[str], expected: str | list[str] | None,
) -> bool | None:
    if not expected:
        return None
    if isinstance(expected, str):
        candidates = [c.strip() for c in expected.split("|") if c.strip()]
    elif isinstance(expected, list):
        candidates = [str(c).strip() for c in expected if str(c).strip()]
    else:
        return None
    if not candidates:
        return None
    sources_lc = [s.lower() for s in sources]
    return any(
        any(c.lower() in s for s in sources_lc)
        for c in candidates
    )


# ---------------------------------------------------------------------------
# URL health checking
# ---------------------------------------------------------------------------

async def _check_one_url(client: httpx.AsyncClient,
                         sem: asyncio.Semaphore,
                         url: str) -> dict[str, Any]:
    """Check a single URL: HEAD -> GET -> GET with browser UA."""
    async with sem:
        # Stage 1: HEAD with polite UA
        try:
            r = await client.head(url, follow_redirects=True, timeout=10.0)
            if r.status_code < 400:
                return {"url": url, "status": r.status_code,
                        "final_url": str(r.url), "method": "HEAD", "ok": True}
        except Exception:
            pass

        # Stage 2: GET with polite UA
        try:
            r = await client.get(url, follow_redirects=True, timeout=10.0)
            if r.status_code < 400:
                return {"url": url, "status": r.status_code,
                        "final_url": str(r.url), "method": "GET", "ok": True}
            polite_status = r.status_code
        except Exception:
            polite_status = None

        # Stage 3: GET with browser UA fallback
        try:
            r = await client.get(url, follow_redirects=True, timeout=15.0,
                                 headers={"User-Agent": BROWSER_UA})
            ok = r.status_code < 400
            return {"url": url, "status": r.status_code,
                    "final_url": str(r.url), "method": "GET-browser-ua",
                    "polite_status": polite_status, "ok": ok}
        except Exception as e:
            return {"url": url, "status": None,
                    "error": f"{type(e).__name__}: {e}",
                    "method": "GET-browser-ua",
                    "polite_status": polite_status, "ok": False}


async def check_urls_batch(urls: list[str],
                           concurrency: int = 15) -> list[dict[str, Any]]:
    """Batch-check a list of URLs. Returns per-URL results."""
    unique = sorted(set(urls))
    if not unique:
        return []
    sem = asyncio.Semaphore(concurrency)
    headers = {"User-Agent": POLITE_UA}
    async with httpx.AsyncClient(headers=headers, http2=False) as client:
        tasks = [_check_one_url(client, sem, u) for u in unique]
        return await asyncio.gather(*tasks)


def _url_category(status: int | None) -> str:
    if status is None:
        return "ERROR"
    if 200 <= status < 300:
        return "OK"
    if 300 <= status < 400:
        return "REDIRECT"
    return f"{status}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main_async(args: argparse.Namespace) -> int:
    claim_set = load_claim_set(args.claims)
    name = claim_set.get("name", os.path.basename(args.claims))
    claims = claim_set["claims"]
    print(f"=== Stress test: {name} ({len(claims)} claims) ===")
    print(f"    backend: {args.url}")
    print(f"    concurrency: {args.concurrency}")
    if args.check_urls:
        print(f"    URL-check: enabled (concurrency {args.url_concurrency})")
    print()

    # ---- Phase 1: fire claims ----
    sem = asyncio.Semaphore(args.concurrency)
    async with httpx.AsyncClient() as client:
        async def bound(claim):
            async with sem:
                r = await run_one(client, claim, url=args.url,
                                  api_key=args.api_key)
                v_ok = verdict_matches(r["verdict"], r["expected_verdicts"])
                s_ok = source_matches(r["sources_with_results"],
                                      r["expected_source"])
                v_marker = "OK" if v_ok else "MISS"
                if s_ok is None:
                    s_marker = "--"
                elif s_ok:
                    s_marker = "OK"
                else:
                    s_marker = "MISS"
                n_urls = len(r["evidence_urls"])
                print(f"  [#{r['id']:>3}] V{v_marker:4s} S{s_marker:4s} "
                      f"{r['verdict']!s:13s} conf={r['confidence']!s:5s} "
                      f"ev={r['evidence_count']:>2d} "
                      f"urls={n_urls:<3d} "
                      f"({r['duration_s']!s}s)  {r['category']:18s}",
                      flush=True)
                return r
        results = await asyncio.gather(*[bound(c) for c in claims])

    # ---- Phase 1 Summary ----
    n = len(results)
    v_match = sum(1 for r in results
                  if verdict_matches(r["verdict"], r["expected_verdicts"]))
    expected_with_src = [r for r in results if r["expected_source"]]
    s_match = sum(1 for r in expected_with_src
                  if source_matches(r["sources_with_results"],
                                    r["expected_source"]))
    errors = [r for r in results if r["error"]]

    print()
    print(f"=== Verdict Summary ===")
    print(f"Verdict-Match: {v_match}/{n}  ({v_match/n:.0%})")
    if expected_with_src:
        print(f"Source-Match (where expected): "
              f"{s_match}/{len(expected_with_src)}  "
              f"({s_match/len(expected_with_src):.0%})")
    if errors:
        print(f"Errors: {len(errors)}")
        for e in errors:
            print(f"  #{e['id']}: {e['error']}")

    # Collect all evidence URLs
    all_urls: list[str] = []
    url_to_claims: dict[str, list[int]] = {}
    for r in results:
        for u in r.get("evidence_urls", []):
            all_urls.append(u)
            url_to_claims.setdefault(u, []).append(r["id"])

    unique_urls = sorted(set(all_urls))
    print(f"\nEvidence-URLs: {len(all_urls)} total, {len(unique_urls)} unique")

    # Per-category URL count
    cat_url_counts: dict[str, int] = {}
    for r in results:
        cat = r.get("category", "?")
        cat_url_counts[cat] = cat_url_counts.get(cat, 0) + len(r.get("evidence_urls", []))

    # ---- Phase 2: URL health check ----
    url_results: list[dict] = []
    if args.check_urls and unique_urls:
        print(f"\n=== URL Health Check ({len(unique_urls)} unique URLs) ===")
        url_results = await check_urls_batch(
            unique_urls, concurrency=args.url_concurrency)

        # Build lookup
        url_status: dict[str, dict] = {r["url"]: r for r in url_results}

        ok_count = sum(1 for r in url_results if r.get("ok"))
        broken = [r for r in url_results if not r.get("ok")]

        print(f"\nURL-Health: {ok_count}/{len(url_results)}  "
              f"({ok_count/len(url_results):.0%} OK)")

        if broken:
            # Group by status
            status_groups: dict[str, list[dict]] = {}
            for b in broken:
                cat = _url_category(b.get("status"))
                status_groups.setdefault(cat, []).append(b)

            print(f"\nBroken URLs ({len(broken)}):")
            for cat in sorted(status_groups.keys()):
                items = status_groups[cat]
                print(f"\n  [{cat}] ({len(items)} URLs):")
                for item in items[:15]:
                    claims_str = ",".join(f"#{c}" for c in
                                         url_to_claims.get(item["url"], []))
                    url_short = item["url"][:90]
                    print(f"    {url_short}")
                    print(f"      -> claims: {claims_str}")
                if len(items) > 15:
                    print(f"    ... and {len(items) - 15} more")

        # Annotate per-claim URL results
        for r in results:
            claim_url_results = []
            for u in r.get("evidence_urls", []):
                ur = url_status.get(u, {})
                claim_url_results.append({
                    "url": u,
                    "status": ur.get("status"),
                    "ok": ur.get("ok", False),
                })
            r["url_check_results"] = claim_url_results
            r["urls_ok"] = sum(1 for x in claim_url_results if x["ok"])
            r["urls_broken"] = sum(1 for x in claim_url_results if not x["ok"])

        # Per-claim URL health summary
        claims_with_broken = [r for r in results if r.get("urls_broken", 0) > 0]
        if claims_with_broken:
            print(f"\nClaims mit broken URLs ({len(claims_with_broken)}):")
            for r in claims_with_broken:
                broken_urls = [x["url"][:70] for x in r["url_check_results"]
                               if not x["ok"]]
                print(f"  #{r['id']:>3} {r['category']:18s} "
                      f"({r['urls_broken']} broken / "
                      f"{len(r['evidence_urls'])} total)")
                for bu in broken_urls[:3]:
                    print(f"        {bu}")

    # ---- Save results ----
    if args.out:
        out_data = {
            "name": name,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "backend": args.url,
            "results": results,
            "verdict_match": f"{v_match}/{n}",
            "verdict_match_pct": round(v_match / n * 100, 1),
        }
        if url_results:
            ok_count = sum(1 for r in url_results if r.get("ok"))
            out_data["url_check"] = {
                "total_unique": len(unique_urls),
                "ok": ok_count,
                "broken": len(unique_urls) - ok_count,
                "ok_pct": round(ok_count / len(unique_urls) * 100, 1)
                          if unique_urls else 0,
                "details": url_results,
            }
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out_data, f, ensure_ascii=False, indent=2)
        print(f"\nSaved -> {args.out}")

    # ---- Final summary line ----
    print(f"\n{'='*60}")
    print(f"VERDICT: {v_match}/{n} ({v_match/n:.0%})")
    if url_results:
        ok_count = sum(1 for r in url_results if r.get("ok"))
        print(f"URLS:    {ok_count}/{len(url_results)} "
              f"({ok_count/len(url_results):.0%})")
    print(f"{'='*60}")

    # Exit code by threshold
    threshold_pct = args.threshold
    if v_match / n < threshold_pct:
        print(f"\nFAIL: verdict-match {v_match}/{n} below threshold "
              f"{threshold_pct:.0%}")
        return 1
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Run a stress test against an Evidora backend.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--claims", required=True,
                    help="Path to a JSON claim set "
                         "(see tools/stress_tests/*.json)")
    ap.add_argument("--url", default=os.getenv("EVIDORA_URL",
                                                "https://evidora.eu"),
                    help="Backend URL (default: $EVIDORA_URL or evidora.eu)")
    ap.add_argument("--api-key",
                    default=os.getenv("EVIDORA_TEST_API_KEY"),
                    help="Bypass-key for the per-IP rate limit "
                         "(default: $EVIDORA_TEST_API_KEY)")
    ap.add_argument("--concurrency", type=int, default=4,
                    help="How many claims to fire in parallel (default: 4)")
    ap.add_argument("--out", default=None,
                    help="Optional path to write the full result JSON")
    ap.add_argument("--threshold", type=float, default=0.9,
                    help="Pass threshold for verdict-match "
                         "(default: 0.9 = 18/20)")
    ap.add_argument("--check-urls", action="store_true", default=False,
                    help="Also check all evidence URLs for broken links")
    ap.add_argument("--url-concurrency", type=int, default=15,
                    help="Concurrency for URL health checks (default: 15)")
    args = ap.parse_args()

    if not os.path.isfile(args.claims):
        sys.exit(f"claims file not found: {args.claims}")

    try:
        rc = asyncio.run(main_async(args))
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc)


if __name__ == "__main__":
    main()
