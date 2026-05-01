#!/usr/bin/env python3
"""Profile the per-stage latency of an Evidora pipeline.

Sequentially fires a small set of cross-domain claims, timestamps each
SSE event client-side, and reports per-stage durations:

  setup    = T(analyze)    - T0
  analyze  = T(search)     - T(analyze)    -- LLM call #1 (claim analyzer)
  search   = T(synthesize) - T(search)     -- all sources fan-out, parallel
  synth    = T(result)     - T(synthesize) -- LLM call #2 (synthesizer)
  done     = T(done)       - T(result)
  total    = T(done)       - T0

Why client-side timestamps: SSE-Latency from server to client is
negligible compared to pipeline stages (verified: <50 ms vs. 2-15 s
per stage).

Usage:
  python3 tools/profile_latency.py
  python3 tools/profile_latency.py --url http://localhost:8000

Background context: the 2026-05-01 profiling run found ~73 % of total
latency in the synthesizer LLM call. See ARCHITECTURE.md for stage
overview, or memory/data_sources_roadmap.md (private) for optimization
roadmap.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time

import httpx


# Default cross-domain probe set. One claim per major source-class so
# the median actually represents real-world traffic, not just one path.
DEFAULT_CLAIMS = [
    ("Wissenschaft (PubMed-getrieben)",
     "Impfungen verursachen Autismus."),
    ("Wirtschaft (Eurostat live)",
     "Die Inflation in Oesterreich liegt unter dem EU-Schnitt."),
    ("AT Static-First (Wohnen)",
     "Wohnpreise in Wien sind seit 2010 mehr als verdoppelt."),
    ("Esoterik (Static-First)",
     "Heilsteine wie Rosenquarz haben heilende Energien."),
    ("Klima (mehrere Live-APIs)",
     "CO2-Emissionen Oesterreichs sind seit 2010 gesunken."),
    ("Verkehr (AT Static-First)",
     "Die OeBB ist puenktlicher als die Deutsche Bahn."),
]


async def profile_one(client, label, claim, *, url, api_key):
    timings = {}
    t0 = time.time()
    headers = {"Accept": "text/event-stream"}
    if api_key:
        headers["X-Evidora-Test-Key"] = api_key
    try:
        async with client.stream(
            "POST", f"{url}/api/check",
            json={"claim": claim},
            headers=headers,
            timeout=httpx.Timeout(connect=10, read=300, write=10, pool=10),
        ) as resp:
            cur = None
            async for line in resp.aiter_lines():
                if line.startswith("event:"):
                    cur = line.split(":", 1)[1].strip()
                elif line.startswith("data:") and cur in ("step", "result", "done"):
                    elapsed = time.time() - t0
                    if cur == "step":
                        try:
                            d = json.loads(line.split(":", 1)[1].strip())
                            timings[d.get("step")] = elapsed
                        except Exception:
                            pass
                    else:
                        timings[cur] = elapsed
    except Exception as e:
        return {"label": label, "error": str(e), "timings": timings}
    return {"label": label, "claim": claim, "timings": timings}


def stages_of(t):
    s = {}
    if "analyze" in t:                       s["setup"]   = t["analyze"]
    if "search" in t and "analyze" in t:     s["analyze"] = t["search"] - t["analyze"]
    if "synthesize" in t and "search" in t:  s["search"]  = t["synthesize"] - t["search"]
    if "result" in t and "synthesize" in t:  s["synth"]   = t["result"] - t["synthesize"]
    if "done" in t and "result" in t:        s["done"]    = t["done"] - t["result"]
    if "done" in t:                          s["total"]   = t["done"]
    return s


async def main_async(args):
    if args.claims:
        with open(args.claims) as f:
            data = json.load(f)
        items = [(c.get("category", c.get("id", "?")), c["claim"])
                 for c in data["claims"][:args.max_claims]]
    else:
        items = DEFAULT_CLAIMS

    all_results = []
    async with httpx.AsyncClient() as client:
        for label, claim in items:
            print(f"\n--- {label} ---")
            print(f"    {claim}")
            r = await profile_one(client, label, claim,
                                  url=args.url, api_key=args.api_key)
            if r.get("error"):
                print(f"    ERROR: {r['error']}")
                continue
            s = stages_of(r["timings"])
            r["stages"] = s
            print(f"    setup={s.get('setup',0):.1f}s  "
                  f"analyze={s.get('analyze',0):.1f}s  "
                  f"search={s.get('search',0):.1f}s  "
                  f"synth={s.get('synth',0):.1f}s  "
                  f"done={s.get('done',0):.2f}s  "
                  f"TOTAL={s.get('total',0):.1f}s")
            all_results.append(r)

    print(f"\n=== Aggregate (median across {len(all_results)} claims) ===")
    for stage in ("setup", "analyze", "search", "synth", "done", "total"):
        vals = [r["stages"].get(stage, 0) for r in all_results
                if "stages" in r and stage in r["stages"]]
        if not vals:
            continue
        med = statistics.median(vals)
        print(f"  {stage:8s}  median={med:5.1f}s   "
              f"min={min(vals):4.1f}s  max={max(vals):4.1f}s")

    if args.out:
        json.dump(all_results, open(args.out, "w"),
                  ensure_ascii=False, indent=2)
        print(f"\nSaved -> {args.out}")
    return 0


def main():
    ap = argparse.ArgumentParser(description="Profile per-stage latency.")
    ap.add_argument("--url", default=os.getenv("EVIDORA_URL",
                                                "https://evidora.eu"))
    ap.add_argument("--api-key", default=os.getenv("EVIDORA_TEST_API_KEY"))
    ap.add_argument("--claims", default=None,
                    help="Optional claim-set JSON to use instead of "
                         "the default cross-domain probe")
    ap.add_argument("--max-claims", type=int, default=10)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
