#!/usr/bin/env python3
"""Weekly Phrasing-Stress-Check — soll als Cron auf prod laufen.

Pattern: 20 kanonische Test-Claims (jeweils mit erwarteter Quelle),
gegen evidora.eu mit Bypass-Key. Wenn Verdict-Match unter 18/20 oder
Source-Match unter 18/20 fällt, wird ein Alert in den Backend-Log
geschrieben (oder optional via Webhook gesendet).

Zweck: Trigger-Erosion früh erkennen — wenn z.B. Mistral seine
Reformulationen ändert, oder neue Datenquellen Trigger-Konflikte
erzeugen, wird das nach spätestens einer Woche sichtbar.

Aufruf:
  python3 tools/weekly_phrasing_check.py [--alert-webhook URL] [--api-key KEY]

Cron-Eintrag (auf prod, in burrito-crontab):
  # jeden Sonntag 03:00
  0 3 * * 0 cd /opt/Evidora/website/backend && \
    EVIDORA_TEST_API_KEY=<key> python3 tools/weekly_phrasing_check.py \
    >> /var/log/evidora_phrasing_check.log 2>&1

Schwellwerte:
  - VERDICT_THRESHOLD = 18 / 20 (90 %)
  - SOURCE_THRESHOLD  = 18 / 20 (90 %)
"""
import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime

import httpx

# ---------------------------------------------------------------------------
# Test-Claim-Suite — kanonisch, eine pro Quelle/Topic.
# Bei jeder neuen Quelle wird hier ein Eintrag ergänzt.
# ---------------------------------------------------------------------------
CLAIMS = [
    # EuGH/EGMR
    ("Schrems II macht alle US-Cloud-Dienste in Europa illegal.",
     ["false", "mostly_false"], "EuGH+EGMR"),
    ("Das Klimaseniorinnen-Urteil hat keinen rechtlichen Effekt.",
     ["false", "mostly_false"], "EuGH+EGMR"),
    # Eurostat Crime
    ("Migranten verursachen 60 Prozent der Kriminalität in Österreich.",
     ["false", "mostly_false"], "Eurostat Crime"),
    ("Deutschland hat die niedrigste Mordrate aller EU-Staaten.",
     ["false", "mostly_false"], "Eurostat Crime"),
    # Energy-Charts
    ("Deutschland kauft seit Atomausstieg billigen Atomstrom aus Frankreich.",
     ["false", "mostly_false", "misleading"], "Energy-Charts"),
    ("Erneuerbare decken nur 10 Prozent des deutschen Stromverbrauchs.",
     ["false", "mostly_false"], "Energy-Charts"),
    # MedienTransparenz
    ("Die Krone bekommt am meisten Inserate von der öffentlichen Hand.",
     ["true", "mostly_true"], "MedienTransparenz"),
    ("In der Inseratenaffäre Kurz wurden Beinschab-Studien gefälscht.",
     ["true", "mostly_true"], "MedienTransparenz"),
    # RKI SurvStat
    ("Die Masern-Welle in Deutschland kommt von Asylsuchenden.",
     ["false", "mostly_false"], "RKI SurvStat"),
    ("In Deutschland herrscht eine Tuberkulose-Epidemie wegen Migration.",
     ["false", "mostly_false"], "RKI SurvStat"),
    # Bildung
    ("In Österreich gibt es einen massiven flächendeckenden Lehrermangel.",
     ["false", "mostly_false"], "Bildung"),
    ("Jeder dritte österreichische Volksschüler kann nicht richtig lesen.",
     ["false", "mostly_false"], "Bildung"),
    # VfGH/VwGH
    ("Der VfGH hat die COVID-Lockdowns als illegal erklärt.",
     ["false", "mostly_false"], "VfGH"),
    ("Die Bundespräsidenten-Stichwahl 2016 wurde manipuliert.",
     ["false", "mostly_false"], "VfGH"),
    # OECD Health
    ("Die Lebenserwartung in Österreich sinkt seit Jahren.",
     ["false", "mostly_false"], "OECD Health"),
    ("Das österreichische Gesundheitssystem kollabiert wegen Bettenmangel.",
     ["false", "mostly_false"], "OECD Health"),
    # Wohnen
    ("Wohnen ist in Österreich seit 2010 unleistbar geworden.",
     ["true", "mostly_true"], "Wohnen"),
    # Verkehr
    ("Die ÖBB sind unzuverlässig — die Pünktlichkeit ist katastrophal.",
     ["false", "mostly_false"], "Verkehr"),
    ("Das KlimaTicket ist gescheitert.",
     ["false", "mostly_false"], "Verkehr"),
    # Statistik Austria Bevölkerung
    ("In Wien ist mehr als jeder dritte Einwohner Ausländer.",
     ["true", "mostly_true"], "AT Factbook"),
]

VERDICT_THRESHOLD = 18  # von 20
SOURCE_THRESHOLD = 18   # von 20


async def run_one(client: httpx.AsyncClient, claim: str, expected_verdicts: list,
                  expected_src: str, api_key: str) -> dict:
    out = {"claim": claim, "expected_src": expected_src,
           "expected_verdicts": expected_verdicts,
           "verdict": None, "confidence": None,
           "sources_with_results": [], "error": None}
    try:
        async with client.stream(
            "POST", "https://evidora.eu/api/check",
            json={"claim": claim},
            headers={"Accept": "text/event-stream",
                     "X-Evidora-Test-Key": api_key},
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
                        cov = d.get("source_coverage") or {}
                        out["sources_with_results"] = cov.get("names", [])
                    except Exception as e:
                        out["error"] = f"parse: {e}"
                elif event == "done":
                    break
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    return out


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key",
        default=os.environ.get("EVIDORA_TEST_API_KEY", ""),
        help="Bypass-Key (default: EVIDORA_TEST_API_KEY env)")
    parser.add_argument("--alert-webhook",
        default=os.environ.get("EVIDORA_ALERT_WEBHOOK", ""),
        help="Optional URL for alert payload (POST JSON)")
    parser.add_argument("--strict", action="store_true",
        help="Exit code 1 wenn Schwellwert unterschritten (für Cron-Failure)")
    args = parser.parse_args()

    if not args.api_key:
        print("ERROR: kein API-Key (--api-key oder EVIDORA_TEST_API_KEY env)")
        sys.exit(2)

    timestamp = datetime.utcnow().isoformat() + "Z"
    print(f"=== Evidora weekly phrasing check {timestamp} ===")
    sem = asyncio.Semaphore(3)

    async def bound(claim, ev, es):
        async with sem:
            r = await run_one(client, claim, ev, es, args.api_key)
            v_ok = r["verdict"] in r["expected_verdicts"]
            s_ok = any(r["expected_src"] in s for s in r["sources_with_results"])
            print(f"  {('✓' if v_ok else 'X')}V {('✓' if s_ok else 'X')}S "
                  f"{r['verdict']!s:13s} {r['expected_src']:18s}  "
                  f"{claim[:80]}")
            return r

    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(
            *(bound(c, ev, es) for c, ev, es in CLAIMS)
        )

    v_match = sum(1 for r in results if r["verdict"] in r["expected_verdicts"])
    s_match = sum(1 for r in results
                  if any(r["expected_src"] in s for s in r["sources_with_results"]))
    n = len(CLAIMS)

    print()
    print(f"Result: Verdict-Match {v_match}/{n} (threshold {VERDICT_THRESHOLD})")
    print(f"        Source-Match  {s_match}/{n} (threshold {SOURCE_THRESHOLD})")

    drift_alert = (v_match < VERDICT_THRESHOLD or s_match < SOURCE_THRESHOLD)
    if drift_alert:
        msg = (f"⚠ DRIFT ALERT: Verdict={v_match}/{n}, Source={s_match}/{n} "
               f"(thresholds {VERDICT_THRESHOLD}/{SOURCE_THRESHOLD})")
        print(msg)
        # Failures-Log
        print("Failed claims:")
        for r in results:
            v_ok = r["verdict"] in r["expected_verdicts"]
            s_ok = any(r["expected_src"] in s for s in r["sources_with_results"])
            if not (v_ok and s_ok):
                print(f"  - [{r['verdict']!s} / {r['expected_src']!s}] {r['claim'][:90]}")
                if not v_ok:
                    print(f"      verdict={r['verdict']!s} expected={r['expected_verdicts']!s}")
                if not s_ok:
                    print(f"      sources={r['sources_with_results']!s}")
        if args.alert_webhook:
            try:
                async with httpx.AsyncClient() as c:
                    await c.post(args.alert_webhook,
                                 json={"timestamp": timestamp,
                                       "verdict_match": v_match,
                                       "source_match": s_match,
                                       "n_claims": n,
                                       "message": msg})
                print(f"  alert webhook posted to {args.alert_webhook}")
            except Exception as e:
                print(f"  alert webhook failed: {e}")
        if args.strict:
            sys.exit(1)
    else:
        print("OK — alle Schwellwerte erfüllt.")


if __name__ == "__main__":
    asyncio.run(main())
