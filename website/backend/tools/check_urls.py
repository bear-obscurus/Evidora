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

# Generierte Caches tragen FREMDE Referenzen: MITRE-ATT&CK-Quellenangaben,
# Projekt-Websites aus der CORDIS-Datenbank. Wenn die sterben, ist das die
# Sache von MITRE bzw. der EU — wir koennen sie nicht ersetzen, und sie
# gehoeren deshalb nicht in denselben Topf wie unsere eigenen Belege.
# Gemessen 2026-10-12: von 76 toten Links lagen 42 in mitre_attack, 14 in
# cordis_projects_slim und nur 20 in kuratierten Dateien. Drei Viertel des
# Alarms waren also nichts, woran jemand arbeiten kann — und die 20, an
# denen man arbeiten koennte, gingen darin unter.
GENERIERTE_CACHES = frozenset({
    "cordis_projects_slim.json", "mitre_attack.json", "claimreview_index.json",
})

# Maschinen-Gedaechtnis: was war beim LETZTEN Lauf tot? Daraus wird die
# Veraenderung berechnet. Bewusst GETRENNT von url_bekannt_tot.json, das
# eine von Hand gepflegte Parkliste ist ("angesehen, bleibt vorerst").
# Beides zu vermischen war der Fehler: die Parkliste wird nur mit
# --schreibe-bekannt fortgeschrieben, der Cron tut das nicht, also meldete
# der Waechter JEDE Woche dieselben 60+ Links als "neu". Am 12.10.2026
# waren 27 der 30 angezeigten wortgleich die der Vorwoche.
#
# Die Datei liegt unter data/, weil /app/tools im Container read-only ist —
# aber in einem UNTERVERZEICHNIS, denn `kuratierte_dateien` listet data/
# und wuerde sie sonst als Quelle von URLs einlesen. Genau das ist mir am
# 12.10. mit einer --out-Datei passiert: der Pruefer fuehrte sich selbst
# als Fundort auf.
STAND_UNTERORDNER = "_wartung"
STAND_DATEI = "url_tot_stand.json"
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
        # Punkt und Unterstrich sind Arbeitsdateien, keine Daten. Am
        # 12.10.2026 habe ich `--out` versehentlich nach data/ zeigen
        # lassen; `herkunft()` las die Ergebnisdatei danach ein und der
        # Pruefer fuehrte SICH SELBST als Fundort der toten Links auf.
        # Ein Werkzeug, das in das Verzeichnis schreibt, das es liest,
        # braucht diese Grenze.
        if name.startswith((".", "_")):
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


def bekannt_eintraege(tot: set[str], bekannt: dict, orte: dict, heute: str) -> dict:
    """Neue Liste bekannter toter Links. Handnotizen (z. B. "grund", warum
    ein Link nicht ersetzbar ist) ueberleben das Neuschreiben."""
    return {u: {**(bekannt.get(u) or {}),
                "dateien": orte.get(u, ["live"]),
                "seit": (bekannt.get(u) or {}).get("seit", heute)}
            for u in sorted(tot)}


def vergleiche(tot: set[str], bekannt: dict) -> tuple[list[str], list[str], list[str]]:
    """(neu tot, weiterhin bekannt tot, bekannt aber nicht mehr tot/gefunden)."""
    neu = sorted(tot - set(bekannt))
    weiter = sorted(tot & set(bekannt))
    erledigt = sorted(set(bekannt) - tot)
    return neu, weiter, erledigt


def stand_pfad(data_dir: str) -> str:
    return os.path.join(data_dir, STAND_UNTERORDNER, STAND_DATEI)


def lade_stand(pfad: str) -> tuple[set[str], str | None]:
    """(tote URLs beim letzten Lauf, Datum). Ohne Datei: (leer, None)."""
    try:
        with open(pfad, encoding="utf-8") as f:
            d = json.load(f)
        return set(d.get("tot") or []), d.get("stand")
    except (FileNotFoundError, json.JSONDecodeError):
        return set(), None


def schreibe_stand(pfad: str, tot: set[str], heute: str) -> None:
    os.makedirs(os.path.dirname(pfad) or ".", exist_ok=True)
    with open(pfad, "w", encoding="utf-8") as f:
        json.dump({"hinweis": "Maschinen-Gedaechtnis fuer check_urls.py: was "
                              "war beim letzten Lauf tot. NICHT von Hand "
                              "pflegen — dafuer ist url_bekannt_tot.json da.",
                   "stand": heute, "tot": sorted(tot)}, f,
                  ensure_ascii=False, indent=1)
        f.write("\n")


def aus_generiertem_cache(url: str, orte: dict) -> bool:
    """Steht die URL AUSSCHLIESSLICH in generierten Caches?

    Ausschliesslich, nicht irgendwo: steht dieselbe URL auch in einem
    kuratierten Pack, ist sie unser Beleg und damit unsere Aufgabe.
    """
    dateien = set(orte.get(url) or [])
    return bool(dateien) and dateien <= set(GENERIERTE_CACHES)


def teile_nach_herkunft(urls, orte: dict) -> tuple[list[str], list[str]]:
    """(unsere Belege, Fremdreferenzen aus generierten Caches)."""
    eigen, fremd = [], []
    for u in urls:
        (fremd if aus_generiertem_cache(u, orte) else eigen).append(u)
    return eigen, fremd


def alarm_text(neu_tot, wieder_da, tot, orte, stand_datum) -> str:
    """Der Push nennt die VERAENDERUNG zuerst, den Bestand danach.

    Vorher stand im Titel die Zahl aller nicht-geparkten toten Links — am
    12.10.2026 waren das 64, von denen 27 der 30 angezeigten schon in der
    Vorwoche gemeldet worden waren. Ein Waechter, der jede Woche dasselbe
    ruft, wird abgeschaltet (#128, #236).
    """
    neu_eigen, neu_fremd = teile_nach_herkunft(neu_tot, orte)
    best_eigen, best_fremd = teile_nach_herkunft(sorted(tot), orte)
    seit = f" seit {stand_datum}" if stand_datum else ""
    zeilen = [
        f"{len(neu_tot)} neu tot{seit}: "
        f"{len(neu_eigen)} in Belegen, {len(neu_fremd)} in generierten Caches",
        f"Bestand: {len(tot)} tot — {len(best_eigen)} in Belegen, "
        f"{len(best_fremd)} in generierten Caches",
    ]
    if wieder_da:
        zeilen.append(f"{len(wieder_da)} wieder erreichbar")
    zeilen.append("")
    for u in (neu_eigen + neu_fremd)[:10]:
        zeilen.append(f"{', '.join(orte.get(u) or ['live'])}: {u}")
    return "\n".join(zeilen)[:3800]


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
    orte = herkunft(data_dir)
    heute = datetime.date.today().isoformat()

    # Veraenderung gegen den LETZTEN LAUF — nicht gegen die handgepflegte
    # Parkliste. Die wird nur mit --schreibe-bekannt fortgeschrieben, und
    # der Cron tut das nicht; deshalb meldete dieser Waechter jede Woche
    # dieselben 60+ Links als "neu".
    stand = args.stand or stand_pfad(data_dir)
    vorher, stand_datum = lade_stand(stand)
    neu_tot = sorted(tot - vorher) if stand_datum else []
    # "Wieder erreichbar" nur, wo diesmal tatsaechlich nachgesehen wurde.
    # Am 12.10.2026 meldete der Waechter die zehn Links, die #244 ERSETZT
    # hatte, als "wieder erreichbar" — `vorher - tot` verwechselt "lebt
    # wieder" mit "steht nicht mehr in den Daten". Fuer die Frage "muss
    # jemand etwas tun?" ist das der Unterschied zwischen einer guten
    # Nachricht und einer erledigten Aufgabe.
    geprueft = {r["url"] for ergebnisse in results_by_tier.values()
                for r in ergebnisse}
    wieder_da = sorted((vorher - tot) & geprueft)
    nicht_mehr_geprueft = sorted((vorher - tot) - geprueft)

    eigen, fremd = teile_nach_herkunft(sorted(tot), orte)
    print(f"\n=== Tote Links: {len(tot)} — {len(eigen)} in Belegen, "
          f"{len(fremd)} in generierten Caches (MITRE/CORDIS)")
    print(f"    Parkliste: {len(weiter)} geparkt, {len(neu)} ungeparkt, "
          f"{len(erledigt)} geparkte nicht mehr tot")
    if stand_datum:
        print(f"    Veraenderung seit {stand_datum}: {len(neu_tot)} neu tot, "
              f"{len(wieder_da)} wieder erreichbar, "
              f"{len(nicht_mehr_geprueft)} nicht mehr geprueft")
    else:
        print("    Keine Stand-Datei — dieser Lauf legt die Basislinie an, "
              "kein Alarm.")

    for u in neu_tot:
        print(f"  NEU TOT  {', '.join(orte.get(u) or ['live'])[:40]:<40} {u}")
    for u in wieder_da:
        print(f"  WIEDER DA {', '.join(orte.get(u) or ['live'])[:39]:<39} {u}")
    for u in nicht_mehr_geprueft:
        # Meist: aus den Daten entfernt (ersetzt oder geloescht). Bei
        # Live-URLs kann es auch heissen, dass diesmal andere Claims
        # gezogen wurden — deshalb die zurueckhaltende Formulierung.
        print(f"  NICHT MEHR GEPRUEFT{'':<30} {u}")
    # Der Rueckstand, an dem jemand arbeiten KANN: unsere eigenen Belege.
    offen_eigen = [u for u in eigen if u not in neu_tot]
    if offen_eigen:
        print(f"\n--- Rueckstand in Belegen ({len(offen_eigen)}), "
              f"die ersten 20:")
        for u in offen_eigen[:20]:
            print(f"  TOT  {', '.join(orte.get(u) or ['live'])[:40]:<40} {u}")

    if args.schreibe_bekannt:
        eintraege = bekannt_eintraege(tot, bekannt, orte, heute)
        with open(args.bekannt, "w", encoding="utf-8") as f:
            json.dump({"hinweis": "Bekannte tote Beleg-Links. Nur NEUE tote Links "
                                  "alarmieren. Reparierte Links hier entfernen "
                                  "(check_urls.py --schreibe-bekannt).",
                       "tot": eintraege}, f, ensure_ascii=False, indent=1)
            f.write("\n")
        print(f"Liste bekannter toter Links geschrieben: {args.bekannt} ({len(eintraege)})")

    schreibe_stand(stand, tot, heute)

    if neu_tot:
        post_alert(args.alert_webhook,
                   f"Evidora: {len(neu_tot)} neu tote Beleg-Links",
                   alarm_text(neu_tot, wieder_da, tot, orte, stand_datum))
        return 2
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
    ap.add_argument("--stand", default=None,
                    help="Maschinen-Gedaechtnis (Default: "
                         "<data-dir>/_wartung/url_tot_stand.json). Daraus "
                         "wird die VERAENDERUNG seit dem letzten Lauf "
                         "berechnet — nicht aus der Parkliste.")
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
