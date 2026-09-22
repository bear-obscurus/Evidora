#!/usr/bin/env python3
"""URL-Health-Check für die in ``data/*.json`` referenzierten Quellen-Links.

Drei-Tier-Strategie:
  Tier 1 — JEDE Datendatei mit Links (ausser Tier 2/3), jede Woche neu
           bestimmt. Bis 2026-09-22 eine feste Liste von 21 Dateien;
           61 weitere wurden nie geprueft. Pflicht-Check.
  Tier 2 — halb-kuratierte Cache-Files (abstimmungen, volksbegehren,
           wahlen) — strukturierte parlament.gv.at/BMI-URLs. Sample 5 %.
  Tier 3 — externe Cache-Dumps (claimreview_index, euvsdisinfo_db) —
           ~25k URLs, upstream-controlled. Sample 0.5 %.

Plus optional ``--live N``: feuert N Claims aus den Stress-Test-Sets
gegen den Backend, sammelt URLs aus Verdicts/Evidence, checkt sie auch.
Erfasst dynamisch generierte URLs (PubMed-DOIs, Eurostat-Datasets,
Curia-Links etc.).

Usage:
  python3 tools/check_urls.py --tier 1
  python3 tools/check_urls.py --tier all --out /tmp/url_check.json
  python3 tools/check_urls.py --tier 1 --live 30 --backend https://evidora.eu

Methodik: parallel HEAD-Request mit GET-Fallback (manche Server lehnen
HEAD ab), Polite-User-Agent (analog services/_http_polite.py),
follow_redirects, 10 s Timeout. Status-Codes:
  2xx       OK
  3xx       Redirect (Ziel sollte erreichbar sein, schaue final URL)
  4xx       broken (404 = tot, 403 = blockiert, etc.)
  5xx       Server-Error (kann transient sein)
  None      Connection-Error / Timeout
"""
from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import os
import random
import re
import sys
from collections import Counter
from typing import Any

import httpx

# ---------------------------------------------------------------------------
# Tier-Definitionen
# ---------------------------------------------------------------------------

# Tier 1 ist KEINE feste Liste mehr. Bis 2026-09-22 stand hier eine von
# 21 Dateien — alles, was danach dazukam, wurde nie geprueft: 61 von 87
# Datendateien mit 1.052 Links, darunter iqs_bildung, substanzen_pack und
# mobilitaet_pack, deren tote Links in Prod Belege kosteten. Der Vollcheck
# fand 261 tote Links (21 %) — fast alle ausserhalb der Liste. Dieselbe
# Klasse wie der Ausschluss-Filter aus #147: eine Liste, die beim Wachsen
# nicht mitwaechst, schweigt still.
#
# Tier 1 = jede Datendatei mit mindestens einem Link, ausser den beiden
# Stichproben-Stufen unten.

TIER_2_FILES = ["abstimmungen.json", "volksbegehren.json", "wahlen.json"]
TIER_3_FILES = ["claimreview_index.json", "euvsdisinfo_db.json"]

TIER_2_SAMPLE_RATIO = 0.05   # 5 %

# Liste bekannter toter Links: nur NEU verrottete Links loesen Alarm aus.
# Ohne sie wuerde der Waechter jede Woche dieselben 250 Links melden — und
# ein Waechter, der immer schreit, wird abgeschaltet (#128).
BEKANNT_TOT_PFAD = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "url_bekannt_tot.json")
TIER_3_SAMPLE_RATIO = 0.005  # 0.5 %

USER_AGENT = "Evidora/1.0 (+https://evidora.eu; mailto:Evidora@proton.me; URL health-check)"
# Browser-UA-Fallback: viele Server filtern Bot-UAs auf 403/400/405/etc.
# Bei 4xx-Antworten retryen wir mit Browser-UA, um "echt tot" von "Server-
# filtert-uns" zu trennen. Das verbessert die Aussagekraft des Audits.
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.0 Safari/605.1.15"
)

URL_RE = re.compile(r'https?://[^\s"<>\)]+')

# Trim trailing punctuation that's commonly attached to URLs in prose
TRAILING_TRIM = ".,;:!?)]}\"'"


# ---------------------------------------------------------------------------
# URL-Extraktion
# ---------------------------------------------------------------------------

def clean_url(url: str) -> str:
    while url and url[-1] in TRAILING_TRIM:
        url = url[:-1]
    return url


def extract_urls(file_path: str) -> set[str]:
    try:
        with open(file_path, encoding="utf-8") as f:
            text = f.read()
    except FileNotFoundError:
        return set()
    raw = URL_RE.findall(text)
    return {clean_url(u) for u in raw if clean_url(u)}


def kuratierte_dateien(data_dir: str) -> list[str]:
    """Jede Datendatei mit mindestens einem Link — ausser den Stichproben-
    Stufen. Wird bei jedem Lauf neu bestimmt, damit eine neue Datei nicht
    wieder aus der Pruefung faellt."""
    ausgenommen = set(TIER_2_FILES) | set(TIER_3_FILES)
    namen = []
    for name in sorted(os.listdir(data_dir)):
        if not name.endswith(".json") or name in ausgenommen:
            continue
        if extract_urls(os.path.join(data_dir, name)):
            namen.append(name)
    return namen


def herkunft(data_dir: str) -> dict[str, list[str]]:
    """URL -> Dateien, in denen sie steht (fuer Meldung und Liste)."""
    out: dict[str, list[str]] = {}
    for name in kuratierte_dateien(data_dir):
        for u in extract_urls(os.path.join(data_dir, name)):
            out.setdefault(u, []).append(name)
    return out


def collect_urls(tier: str, data_dir: str, seed: int = 42) -> dict[str, set[str]]:
    """Return dict tier_label -> set of URLs to check."""
    random.seed(seed)
    out: dict[str, set[str]] = {}

    if tier in ("1", "all"):
        urls = set()
        for f in kuratierte_dateien(data_dir):
            urls.update(extract_urls(os.path.join(data_dir, f)))
        out["tier1"] = urls

    if tier in ("2", "all"):
        urls = set()
        for f in TIER_2_FILES:
            file_urls = list(extract_urls(os.path.join(data_dir, f)))
            n = max(1, int(len(file_urls) * TIER_2_SAMPLE_RATIO))
            sample = random.sample(file_urls, min(n, len(file_urls)))
            urls.update(sample)
        out["tier2_sample"] = urls

    if tier in ("3", "all"):
        urls = set()
        for f in TIER_3_FILES:
            file_urls = list(extract_urls(os.path.join(data_dir, f)))
            n = max(1, int(len(file_urls) * TIER_3_SAMPLE_RATIO))
            sample = random.sample(file_urls, min(n, len(file_urls)))
            urls.update(sample)
        out["tier3_sample"] = urls

    return out


# ---------------------------------------------------------------------------
# Live-URL-Sammlung (optional)
# ---------------------------------------------------------------------------

LIVE_CLAIM_FILES = [
    "tools/stress_tests/esoterik.json",
    "tools/stress_tests/geschichte.json",
    "tools/stress_tests/verschwoerungen.json",
    "tools/stress_tests/fitness.json",
    "tools/stress_tests/lehrer.json",
]


async def fetch_live_urls(backend: str, api_key: str | None,
                          n_claims: int) -> set[str]:
    """Fire N claims at the backend, collect URLs from evidence."""
    all_claims: list[dict] = []
    for f in LIVE_CLAIM_FILES:
        try:
            with open(f, encoding="utf-8") as fh:
                data = json.load(fh)
        except FileNotFoundError:
            continue
        all_claims.extend(data.get("claims", []))

    if not all_claims:
        return set()

    random.seed(42)
    sample = random.sample(all_claims, min(n_claims, len(all_claims)))

    headers = {"Accept": "text/event-stream", "User-Agent": USER_AGENT}
    if api_key:
        headers["X-Evidora-Test-Key"] = api_key

    urls: set[str] = set()
    sem = asyncio.Semaphore(2)

    async def fire_one(client: httpx.AsyncClient, claim: dict):
        async with sem:
            try:
                async with client.stream(
                    "POST", f"{backend}/api/check",
                    json={"claim": claim["claim"]},
                    headers=headers,
                    timeout=httpx.Timeout(connect=10, read=300, write=10, pool=10),
                ) as r:
                    cur = None
                    async for line in r.aiter_lines():
                        if line.startswith("event:"):
                            cur = line.split(":", 1)[1].strip()
                        elif line.startswith("data:") and cur == "result":
                            try:
                                d = json.loads(line.split(":", 1)[1].strip())
                                for ev in d.get("evidence") or []:
                                    if ev.get("url"):
                                        urls.add(clean_url(ev["url"]))
                            except Exception:
                                pass
                        elif cur == "done":
                            break
            except Exception as e:
                print(f"  live-fetch error for #{claim.get('id')}: {e}",
                      file=sys.stderr)

    print(f"[live] firing {len(sample)} claims at {backend} ...")
    async with httpx.AsyncClient() as client:
        await asyncio.gather(*[fire_one(client, c) for c in sample])
    print(f"[live] collected {len(urls)} unique URLs from evidence")
    return urls


# ---------------------------------------------------------------------------
# URL-Check
# ---------------------------------------------------------------------------

async def check_one(client: httpx.AsyncClient, sem: asyncio.Semaphore,
                    url: str) -> dict[str, Any]:
    async with sem:
        # Stage 1: HEAD with polite UA
        try:
            r = await client.head(url, follow_redirects=True, timeout=10.0)
            if r.status_code < 400:
                return {"url": url, "status": r.status_code,
                        "final_url": str(r.url), "method": "HEAD"}
        except Exception:
            pass

        # Stage 2: GET with polite UA (some servers don't allow HEAD)
        try:
            r = await client.get(url, follow_redirects=True, timeout=10.0)
            if r.status_code < 400:
                return {"url": url, "status": r.status_code,
                        "final_url": str(r.url), "method": "GET"}
            polite_status = r.status_code
        except Exception as e:
            polite_status = None

        # Stage 3: GET with Browser-UA fallback (filters out UA-blocks)
        try:
            r = await client.get(url, follow_redirects=True, timeout=15.0,
                                 headers={"User-Agent": BROWSER_UA})
            return {"url": url, "status": r.status_code,
                    "final_url": str(r.url), "method": "GET-browser-ua",
                    "polite_status": polite_status}
        except Exception as e:
            return {"url": url, "status": None,
                    "error": f"{type(e).__name__}: {e}",
                    "method": "GET-browser-ua",
                    "polite_status": polite_status}


async def check_urls(urls: set[str], concurrency: int = 20) -> list[dict[str, Any]]:
    sem = asyncio.Semaphore(concurrency)
    headers = {"User-Agent": USER_AGENT}
    async with httpx.AsyncClient(headers=headers, http2=False) as client:
        tasks = [check_one(client, sem, u) for u in sorted(urls)]
        return await asyncio.gather(*tasks)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

# Nur diese Befunde sagen: der Link ist tot. Ein 403 ist eine Sperre, ein
# Timeout oder 5xx eine Stoerung — beides sagt etwas ueber den Abruf, nicht
# ueber die Seite (#171). Eine Domain, die nicht mehr aufloest, ist dagegen tot.
_DNS_TOT = ("nodename nor servname", "name or service not known",
            "getaddrinfo failed", "no address associated")


def ist_tot(r: dict) -> bool:
    status = r.get("status")
    if status in (404, 410):
        return True
    if status is None:
        fehler = str(r.get("error") or "").lower()
        return any(s in fehler for s in _DNS_TOT)
    return False


def lade_bekannt(pfad: str) -> dict[str, dict]:
    try:
        with open(pfad, encoding="utf-8") as f:
            return json.load(f).get("tot") or {}
    except FileNotFoundError:
        return {}


def vergleiche(tot: set[str], bekannt: dict) -> tuple[list[str], list[str], list[str]]:
    """(neu tot, weiterhin bekannt tot, bekannt aber nicht mehr tot/gefunden)."""
    neu = sorted(tot - set(bekannt))
    weiter = sorted(tot & set(bekannt))
    erledigt = sorted(set(bekannt) - tot)
    return neu, weiter, erledigt


def post_alert(webhook: str, title: str, message: str) -> None:
    """ntfy-Push — gleiche Mechanik wie quellen_lebenszeichen."""
    if not webhook:
        print("WARN: kein EVIDORA_ALERT_WEBHOOK gesetzt — kein Push", file=sys.stderr)
        return
    import urllib.request
    try:
        req = urllib.request.Request(
            webhook, data=message.encode("utf-8"),
            headers={"Title": title, "Priority": "high", "Tags": "link"})
        urllib.request.urlopen(req, timeout=10)
        print("Alert gesendet.")
    except Exception as e:  # noqa: BLE001
        print(f"WARN: Alert-Push fehlgeschlagen: {e}", file=sys.stderr)


def categorize(status: int | None) -> str:
    if status is None:
        return "ERROR"
    if 200 <= status < 300:
        return "OK"
    if 300 <= status < 400:
        return "REDIRECT"
    if 400 <= status < 500:
        return f"4xx ({status})"
    if 500 <= status < 600:
        return f"5xx ({status})"
    return "OTHER"


def report(results_by_tier: dict[str, list[dict]]) -> None:
    print("\n=== URL Health Check ===")
    for tier, results in results_by_tier.items():
        n = len(results)
        if n == 0:
            continue
        cats = Counter(categorize(r["status"]) for r in results)
        ok = cats.get("OK", 0)
        print(f"\n{tier}: {n} URLs")
        for cat, count in sorted(cats.items()):
            pct = count / n * 100
            print(f"  {cat:12s}  {count:4d}  ({pct:.1f} %)")

        # List broken URLs
        broken = [r for r in results
                  if r["status"] is None or (r["status"] is not None and r["status"] >= 400)]
        if broken:
            print(f"\n  Broken / Error in {tier} ({len(broken)}):")
            for r in broken[:30]:  # limit output
                marker = r["status"] if r["status"] else r.get("error", "?")[:40]
                print(f"    [{marker}] {r['url']}")
            if len(broken) > 30:
                print(f"    ... and {len(broken) - 30} more (see --out file)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

async def main_async(args: argparse.Namespace) -> int:
    data_dir = args.data_dir
    results_by_tier: dict[str, list[dict]] = {}

    # Static URLs from data/*.json
    tiers = collect_urls(args.tier, data_dir)
    for tier_label, urls in tiers.items():
        if not urls:
            continue
        print(f"[{tier_label}] checking {len(urls)} URLs ...")
        results = await check_urls(urls, concurrency=args.concurrency)
        results_by_tier[tier_label] = results

    # Live URLs (optional)
    if args.live > 0:
        live_urls = await fetch_live_urls(args.backend,
                                           args.api_key or os.getenv("EVIDORA_TEST_API_KEY"),
                                           args.live)
        if live_urls:
            print(f"[live] checking {len(live_urls)} URLs ...")
            results_by_tier["live"] = await check_urls(live_urls,
                                                       concurrency=args.concurrency)

    report(results_by_tier)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(results_by_tier, f, ensure_ascii=False, indent=2)
        print(f"\nFull results -> {args.out}")

    # Tot heisst 404/410 oder erloschene Domain. Frueher war der Exit-Code bei
    # JEDEM Status >= 400 gesetzt, auch bei den ~100 Sperren (403) — der Lauf
    # war damit immer "rot", schrieb in ein Log, das niemand las, und
    # alarmierte nie.
    tot = {r["url"] for results in results_by_tier.values() for r in results if ist_tot(r)}
    bekannt = lade_bekannt(args.bekannt)
    neu, weiter, erledigt = vergleiche(tot, bekannt)
    print(f"\n=== Tote Links: {len(tot)} (neu {len(neu)}, bekannt {len(weiter)}; "
          f"{len(erledigt)} bekannte nicht mehr tot/gefunden)")
    orte = herkunft(data_dir)
    for u in neu[:30]:
        print(f"  NEU TOT  {', '.join(orte.get(u, ['live']))[:40]:<40} {u}")
    if args.schreibe_bekannt:
        heute = datetime.date.today().isoformat()
        eintraege = {u: {"dateien": orte.get(u, ["live"]),
                         "seit": (bekannt.get(u) or {}).get("seit", heute)}
                     for u in sorted(tot)}
        with open(args.bekannt, "w", encoding="utf-8") as f:
            json.dump({"hinweis": "Bekannte tote Beleg-Links. Nur NEUE tote Links "
                                  "alarmieren. Reparierte Links hier entfernen "
                                  "(check_urls.py --schreibe-bekannt).",
                       "tot": eintraege}, f, ensure_ascii=False, indent=1)
            f.write("\n")
        print(f"Liste bekannter toter Links geschrieben: {args.bekannt} ({len(eintraege)})")
        return 0
    if neu:
        zeilen = [f"{', '.join(orte.get(u, ['live']))}: {u}" for u in neu[:10]]
        post_alert(args.alert_webhook,
                   f"Evidora: {len(neu)} neue tote Beleg-Links",
                   "\n".join(zeilen))
        return 1
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Check URL health across data/*.json + optional live evidence URLs.")
    ap.add_argument("--tier", choices=["1", "2", "3", "all"], default="1",
                    help="Which tier(s) to check (default: 1)")
    ap.add_argument("--live", type=int, default=0,
                    help="Optionally fire N claims and check evidence URLs (default: 0)")
    ap.add_argument("--backend", default=os.getenv("EVIDORA_URL", "https://evidora.eu"))
    ap.add_argument("--api-key", default=None)
    ap.add_argument("--concurrency", type=int, default=20)
    ap.add_argument("--out", default=None,
                    help="Write full result JSON to this path")
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--bekannt", default=BEKANNT_TOT_PFAD,
                    help="Liste bekannter toter Links (nur neue alarmieren)")
    ap.add_argument("--schreibe-bekannt", action="store_true",
                    help="Liste bekannter toter Links aus diesem Lauf neu schreiben")
    ap.add_argument("--alert-webhook", default=os.getenv("EVIDORA_ALERT_WEBHOOK", ""))
    args = ap.parse_args()

    try:
        rc = asyncio.run(main_async(args))
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc)


if __name__ == "__main__":
    main()
