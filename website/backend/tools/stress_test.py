#!/usr/bin/env python3
"""Run a stress test against an Evidora backend.

Reads a claim set from a JSON file (see ``tools/stress_tests/*.json``),
fires each claim against the backend, and reports verdict-match,
source-match, evidence count and per-claim duration.

Usage:
  python3 tools/stress_test.py --claims tools/stress_tests/esoterik.json
  python3 tools/stress_test.py --claims my_set.json --url http://localhost:8000
                               --concurrency 4 --out /tmp/results.json

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
import sys
import time
from typing import Any

import httpx


def load_claim_set(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if "claims" not in data:
        raise ValueError(f"{path}: no 'claims' key")
    return data


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
                        out["evidence_count"] = len(d.get("evidence") or [])
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


def source_matches(sources: list[str], expected: str | None) -> bool | None:
    if not expected:
        return None  # n/a, no expectation
    return any(expected in s for s in sources)


async def main_async(args: argparse.Namespace) -> int:
    claim_set = load_claim_set(args.claims)
    name = claim_set.get("name", os.path.basename(args.claims))
    claims = claim_set["claims"]
    print(f"=== Stress test: {name} ({len(claims)} claims) ===")
    print(f"    backend: {args.url}")
    print(f"    concurrency: {args.concurrency}")
    print()

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
                print(f"  [#{r['id']:>3}] V{v_marker:4s} S{s_marker:4s} "
                      f"{r['verdict']!s:13s} conf={r['confidence']!s:5s} "
                      f"ev={r['evidence_count']:>2d} "
                      f"({r['duration_s']!s}s)  {r['category']:18s}",
                      flush=True)
                return r
        results = await asyncio.gather(*[bound(c) for c in claims])

    # Summary
    n = len(results)
    v_match = sum(1 for r in results
                  if verdict_matches(r["verdict"], r["expected_verdicts"]))
    expected_with_src = [r for r in results if r["expected_source"]]
    s_match = sum(1 for r in expected_with_src
                  if source_matches(r["sources_with_results"], r["expected_source"]))
    errors = [r for r in results if r["error"]]

    print()
    print(f"=== Summary ===")
    print(f"Verdict-Match: {v_match}/{n}  ({v_match/n:.0%})")
    if expected_with_src:
        print(f"Source-Match (where expected): "
              f"{s_match}/{len(expected_with_src)}  "
              f"({s_match/len(expected_with_src):.0%})")
    if errors:
        print(f"Errors: {len(errors)}")
        for e in errors:
            print(f"  #{e['id']}: {e['error']}")

    if args.out:
        json.dump({"name": name, "results": results},
                  open(args.out, "w"), ensure_ascii=False, indent=2)
        print(f"\nSaved -> {args.out}")

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
