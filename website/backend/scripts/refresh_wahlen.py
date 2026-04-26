#!/usr/bin/env python3
"""Refresh the static BMI Wahlen snapshot.

Same Myra-Cloud problem as the Volksbegehren list: bmi.gv.at blocks
Hetzner IPs, so we ship a pre-parsed JSON snapshot in
``data/wahlen.json`` and refresh it locally from a non-blocked IP
(home network etc.) whenever a new Wahl is held.

Covered elections (Bundesergebnis only — Bundesländer-Detail is left
out to keep the snapshot small and the search fast):

- Nationalratswahlen (NRW): 1986, 1990, 1994, 1995, 1999, 2002, 2006,
  2008, 2013, 2017, 2019, 2024
- Bundespräsidentenwahlen (BPW): 1998, 2004, 2010, 2016, 2022
  (1. + 2. Wahlgang werden separat geführt, falls vorhanden)
- Europawahlen (EUW): 1996, 1999, 2004, 2009, 2014, 2019, 2024

Output schema (per election):
    {
      "type": "NRW" | "BPW" | "EUW",
      "year": int,
      "round": int | None,    # only for BPW
      "url": str,
      "results": [
        {"short": "ÖVP", "long": "...", "votes": int, "percent": float, "seats": int|None}
      ]
    }

Usage (from backend/):
    python3 scripts/refresh_wahlen.py
"""

import asyncio
import html as htmllib
import json
import os
import re
import sys
import time

import httpx

HERE = os.path.dirname(os.path.abspath(__file__))
BACKEND_ROOT = os.path.dirname(HERE)
DATA_PATH = os.path.join(BACKEND_ROOT, "data", "wahlen.json")

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.0 Safari/605.1.15"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
    "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
}

NRW_YEARS = [1986, 1990, 1994, 1995, 1999, 2002, 2006, 2008,
             2013, 2017, 2019, 2024]
BPW_YEARS = [1998, 2004, 2010, 2016, 2022]
EUW_YEARS = [1996, 1999, 2004, 2009, 2014, 2019, 2024]


def _strip(s: str) -> str:
    s = re.sub(r"<br\s*/?>", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = htmllib.unescape(s).replace("\xa0", " ")
    return re.sub(r"\s+", " ", s).strip()


def _parse_int(s: str) -> int | None:
    if not s:
        return None
    cleaned = re.sub(r"[^0-9]", "", s)
    return int(cleaned) if cleaned else None


def _parse_pct(s: str) -> float | None:
    if not s:
        return None
    s = s.replace("%", "").strip()
    m = re.search(r"-?\d+(?:[,.]\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", "."))
    except ValueError:
        return None


def _find_party_table(html: str) -> tuple[str, list[list[str]]] | None:
    """Find the first table that has a party-results shape:
    Parteibezeichnung | Kurzbezeichnung | Stimmen | Prozent[e] | (Mandate)
    Returns (raw_table, list of row-cell-lists)."""
    party_markers = ("ÖVP", "FPÖ", "SPÖ", "GRÜNE", "NEOS",
                     "BZÖ", "TS", "FRANK", "BIER", "MFG")
    tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.DOTALL)
    for t in tables:
        rows_html = re.findall(r"<tr[^>]*>(.*?)</tr>", t, re.DOTALL)
        if len(rows_html) < 3:
            continue
        rows = []
        for r in rows_html:
            cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", r, re.DOTALL)
            rows.append([_strip(c) for c in cells])
        if not rows:
            continue
        joined = " ".join(c for row in rows for c in row)
        if not any(p in joined for p in party_markers):
            continue
        header = " ".join(rows[0]).lower()
        # Accept either: explicit "%" anywhere, or a "prozent[e]" header.
        if "%" in joined or "prozent" in header:
            return t, rows
    return None


def parse_party_table(html: str) -> list[dict]:
    """Parse NRW + EUW pages — uniform shape:
    Parteibezeichnung | Kurzbezeichnung | Stimmen | Prozent[e] | Mandate?

    Returns list of dicts with short, long, votes, percent, seats.
    """
    found = _find_party_table(html)
    if not found:
        return []
    _, rows = found

    # First row is header.  Older pages use 4 cols (no Mandate); newer
    # use 5 cols.  Skip rows that don't match the result shape.
    results = []
    for row in rows[1:]:
        if len(row) < 4:
            continue
        long_name, short, votes_s, pct_s = row[0], row[1], row[2], row[3]
        seats_s = row[4] if len(row) >= 5 else None

        if not short or not votes_s:
            continue
        # Skip "summary" rows like "Gesamt", "Wahlberechtigte" etc.
        if any(stop in short.lower() for stop in
               ["gesamt", "summe", "wahlbe", "abgegeb", "ungültig"]):
            continue
        votes = _parse_int(votes_s)
        pct = _parse_pct(pct_s)
        if votes is None or pct is None:
            continue
        seats = _parse_int(seats_s) if seats_s else None
        results.append({
            "short": short,
            "long": long_name,
            "votes": votes,
            "percent": pct,
            "seats": seats,
        })
    return results


def _candidate_short(full: str) -> str:
    """Extract the family name (last word) from a full candidate name,
    stripping academic titles (Dr., Mag., Ing., Prof.)."""
    cleaned = re.sub(
        r"^(Dr\.|Ing\.|Mag\.|Mr\.|Mrs\.|Prof\.|FH-Prof\.)\s+", "", full
    )
    parts = cleaned.split()
    return parts[-1] if parts else full


def parse_bpw_bundesergebnis(html: str) -> list[dict]:
    """Parse BPW pages — two shapes are seen in the wild:

    Shape A (BPW 2004+): candidates as columns, Bundesländer as rows,
    last row is "Gesamt" (Bundesergebnis).

    Shape B (BPW 1998): candidates as rows directly with
    "Kandidat | Stimmen | %" — same as the NRW party-table layout.

    Returns list of dicts with short=candidate-last-name,
    long=full name, votes, percent.
    """
    tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.DOTALL)
    for t in tables:
        rows_html = re.findall(r"<tr[^>]*>(.*?)</tr>", t, re.DOTALL)
        if len(rows_html) < 3:
            continue
        rows = []
        for r in rows_html:
            cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", r, re.DOTALL)
            rows.append([_strip(c) for c in cells])
        if not rows or not rows[0]:
            continue
        header_first = rows[0][0].lower()

        # Shape A — Bundesländer-Matrix
        if header_first == "bundesland":
            candidates = rows[0][1:]
            if not candidates:
                continue
            gesamt = None
            for row in rows:
                if row and row[0].lower() in ("gesamt", "österreich"):
                    gesamt = row
                    break
            if not gesamt or len(gesamt) < 1 + 2 * len(candidates):
                continue
            results = []
            for i, cand in enumerate(candidates):
                v = _parse_int(gesamt[1 + 2 * i])
                p = _parse_pct(gesamt[2 + 2 * i])
                if v is None or p is None:
                    continue
                results.append({
                    "short": _candidate_short(cand),
                    "long": cand,
                    "votes": v,
                    "percent": p,
                    "seats": None,
                })
            if results:
                return results

        # Shape B — Kandidat | Stimmen | %  (candidate-as-row)
        if (("kandidat" in header_first
             or "kandidatin" in header_first
             or "name" in header_first)
                and len(rows[0]) >= 3):
            results = []
            for row in rows[1:]:
                if len(row) < 3:
                    continue
                cand = row[0]
                if not cand:
                    continue
                if any(stop in cand.lower() for stop in
                       ["gesamt", "summe", "wahlbe"]):
                    continue
                v = _parse_int(row[1])
                p = _parse_pct(row[2])
                if v is None or p is None:
                    continue
                results.append({
                    "short": _candidate_short(cand),
                    "long": cand,
                    "votes": v,
                    "percent": p,
                    "seats": None,
                })
            if results:
                return results
    return []


async def fetch_one(client: httpx.AsyncClient, kind: str, year: int,
                    round_: int | None = None) -> dict | None:
    """Fetch and parse a single Wahl page."""
    if kind == "NRW":
        url = (f"https://www.bmi.gv.at/412/Nationalratswahlen/"
               f"Nationalratswahl_{year}/start.aspx")
        parser = parse_party_table
    elif kind == "EUW":
        url = (f"https://www.bmi.gv.at/412/Europawahlen/"
               f"Europawahl_{year}/start.aspx")
        parser = parse_party_table
    elif kind == "BPW":
        url = (f"https://www.bmi.gv.at/412/Bundespraesidentenwahlen/"
               f"Bundespraesidentenwahl_{year}/start.aspx")
        parser = parse_bpw_bundesergebnis
    else:
        return None

    try:
        r = await client.get(url, follow_redirects=False)
    except httpx.HTTPError as e:
        print(f"  ERROR {kind} {year}: {e}", file=sys.stderr)
        return None
    if r.status_code != 200:
        print(f"  SKIP {kind} {year}: HTTP {r.status_code}", file=sys.stderr)
        return None
    results = parser(r.text)
    if not results:
        print(f"  SKIP {kind} {year}: parser returned 0 entries",
              file=sys.stderr)
        return None
    out = {
        "type": kind,
        "year": year,
        "url": url,
        "results": results,
    }
    if round_ is not None:
        out["round"] = round_
    print(f"  ✓ {kind} {year}: {len(results)} entries")
    return out


async def main() -> int:
    print(f"Refreshing BMI Wahlen → {DATA_PATH}")
    elections: list[dict] = []
    async with httpx.AsyncClient(timeout=30.0,
                                 headers=BROWSER_HEADERS) as client:
        for year in NRW_YEARS:
            e = await fetch_one(client, "NRW", year)
            if e:
                elections.append(e)
        for year in BPW_YEARS:
            e = await fetch_one(client, "BPW", year)
            if e:
                elections.append(e)
        for year in EUW_YEARS:
            e = await fetch_one(client, "EUW", year)
            if e:
                elections.append(e)

    if not elections:
        print("ERROR: no elections parsed", file=sys.stderr)
        return 1

    payload = {
        "elections": elections,
        "fetched_at_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "source_root": "https://www.bmi.gv.at/412/",
        "note": (
            "Generated by scripts/refresh_wahlen.py — primary source for "
            "production because BMI/Myra Cloud blocks Hetzner IPs."
        ),
    }
    os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"\nWrote {len(elections)} elections to {DATA_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
