#!/usr/bin/env python3
"""Refresh-Script für Drug-Safety-Datensätze.

Lädt beide Quellen neu und speichert nach data/:
1. Wikipedia EN "List of withdrawn drugs" → data/withdrawn_drugs.json
2. EMA Referrals XLSX → data/ema_referrals.json

Aufruf:
    python3 tools/refresh_drug_data.py [--wd] [--ema] [--all]

Default: --all. Nutzungs-Empfehlung: monatlich via Hetzner-Cron
oder ad-hoc bei Bedarf.

Lizenz:
- Wikipedia (CC-BY-SA 4.0): Source-URL + Permalink in JSON-Header
- EMA Referrals (EU PSI / CC-BY 4.0): Source-URL in JSON-Header
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import urllib.request
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from services._atomic import atomic_write_json  # noqa: E402

LOG = logging.getLogger("refresh_drug_data")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

REPO_ROOT = Path(__file__).resolve().parent.parent  # backend/
DATA_DIR = REPO_ROOT / "data"

# --- Wikipedia withdrawn drugs ---------------------------------------------
WD_PAGE_TITLE = "List_of_withdrawn_drugs"
WD_API_URL = (
    "https://en.wikipedia.org/w/api.php"
    "?action=parse&page=" + WD_PAGE_TITLE +
    "&prop=wikitext&format=json&formatversion=2"
)
WD_PAGE_URL = f"https://en.wikipedia.org/wiki/{WD_PAGE_TITLE}"
WD_OUT = DATA_DIR / "withdrawn_drugs.json"


def _clean_wikilink(s: str) -> str:
    s = re.sub(r"\[\[([^|\]]+)\|([^\]]+)\]\]", r"\2", s)
    s = re.sub(r"\[\[([^\]]+)\]\]", r"\1", s)
    return s


def _strip_refs(s: str) -> str:
    s = re.sub(r"<ref[^>]*?/>", "", s, flags=re.DOTALL)
    s = re.sub(r"<ref[^>]*>.*?</ref>", "", s, flags=re.DOTALL)
    return s


def _strip_templates(s: str) -> str:
    prev = None
    while prev != s:
        prev = s
        s = re.sub(r"\{\{[^{}]*\}\}", "", s)
    return s


def _clean_cell(s: str) -> str:
    s = _strip_refs(s)
    s = _strip_templates(s)
    s = _clean_wikilink(s)
    s = re.sub(r"<[^>]+>", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r'^class="[^"]*"\|', "", s).strip()
    return s


def refresh_withdrawn_drugs() -> int:
    LOG.info("Fetching Wikipedia EN List of withdrawn drugs ...")
    req = urllib.request.Request(
        WD_API_URL, headers={"User-Agent": "evidora-refresh/1.0"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    wikitext = payload.get("parse", {}).get("wikitext", "")
    m = re.search(r'\{\| class="wikitable[^"]*".*?\|\}', wikitext, re.DOTALL)
    if not m:
        LOG.error("No wikitable found in Wikipedia page")
        return 1
    table = m.group(0)
    rows_raw = table.split("|-")

    items: list[dict] = []
    for row in rows_raw[1:]:
        cells = [c for c in row.split("\n|") if c.strip()]
        if len(cells) < 3:
            continue
        drug = _clean_cell(cells[0].lstrip("|").strip())
        year = _clean_cell(cells[1].lstrip("|").strip())
        country = _clean_cell(cells[2].lstrip("|").strip())
        reason = _clean_cell(cells[3].lstrip("|").strip()) if len(cells) > 3 else ""
        if not drug:
            continue
        ym = re.search(r"(\d{4})", year)
        year_int = int(ym.group(1)) if ym else None
        paren = re.search(r"\(([^)]+)\)", drug)
        trade = paren.group(1).strip() if paren else ""
        inn_part = drug.split("(", 1)[0].strip().rstrip(" -").strip()
        inn_part = inn_part.split(" - ")[0].strip()
        items.append({
            "inn": inn_part,
            "trade_name": trade,
            "withdrawal_year": year_int,
            "withdrawal_year_raw": year,
            "country": country,
            "reason": reason[:300],
        })

    output = {
        "source": "Wikipedia (EN): List of withdrawn drugs",
        "source_url": WD_PAGE_URL,
        "license": "CC-BY-SA 4.0",
        "license_url": "https://creativecommons.org/licenses/by-sa/4.0/",
        "fetched_at_iso": date.today().isoformat(),
        "note": (
            "Statischer Snapshot der Wikipedia-Tabelle. Ergänzt das "
            "EMA-CSV (centralized procedure) um historische "
            "Marktrückzüge wie Vioxx (2004 worldwide), Avandia "
            "(2010 EU), Trasylol (2008 US), Mediator (2009 FR)."
        ),
        "items": items,
    }
    atomic_write_json(WD_OUT, output, ensure_ascii=False, indent=2)
    LOG.info(f"withdrawn_drugs: wrote {len(items)} items → {WD_OUT}")
    return 0


# --- EMA Referrals XLSX ----------------------------------------------------
EMA_REFERRALS_URL = (
    "https://www.ema.europa.eu/system/files/documents/other/"
    "medicines_output_referrals_en.xlsx"
)
EMA_OVERVIEW_URL = "https://www.ema.europa.eu/en/medicines/human/referrals"
EMA_OUT = DATA_DIR / "ema_referrals.json"


def refresh_ema_referrals() -> int:
    try:
        import openpyxl  # type: ignore
    except ImportError:
        LOG.error("openpyxl missing — install via pip install openpyxl")
        return 1
    LOG.info("Fetching EMA Referrals XLSX ...")
    req = urllib.request.Request(
        EMA_REFERRALS_URL, headers={"User-Agent": "evidora-refresh/1.0"}
    )
    tmp_path = Path("/tmp/ema_referrals.xlsx")
    with urllib.request.urlopen(req, timeout=60) as resp:
        tmp_path.write_bytes(resp.read())

    wb = openpyxl.load_workbook(tmp_path, read_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    # Find header row dynamically: first row with "Category" + "Referral name"
    hdr_idx = None
    for i, r in enumerate(rows[:20]):
        if r and any(c == "Category" for c in r if c) and any(
            c and "Referral name" in str(c) for c in r
        ):
            hdr_idx = i
            break
    if hdr_idx is None:
        LOG.error("Header row not found in EMA XLSX")
        return 1
    hdr = rows[hdr_idx]
    cols = {str(h).strip(): i for i, h in enumerate(hdr) if h}
    items: list[dict] = []

    def g(row, name):
        i = cols.get(name)
        if i is None:
            return ""
        v = row[i]
        return str(v).strip() if v else ""

    for row in rows[hdr_idx + 1:]:
        if not row or not (row[1] if len(row) > 1 else None):
            continue
        item = {
            "category": g(row, "Category"),
            "referral_name": g(row, "Referral name").rstrip(", ").strip(),
            "inn": g(
                row,
                "International non-proprietary name (INN) / common name",
            ),
            "status": g(row, "Status of referral"),
            "safety_referral": g(row, "Safety referrals").lower() == "yes",
            "referral_type": g(row, "Referral type").strip(),
            "associated_names": g(row, "Associated name"),
            "reference_number": g(row, "Reference number"),
            "authorisation_model": g(row, "Decision making model"),
        }
        if item["inn"] or item["referral_name"]:
            items.append(item)

    output = {
        "source": "EMA Referrals (Art. 20/30/31/107i)",
        "source_url": EMA_OVERVIEW_URL,
        "xlsx_url": EMA_REFERRALS_URL,
        "license": "EU PSI / CC-BY 4.0",
        "fetched_at_iso": date.today().isoformat(),
        "note": (
            "EMA-Referrals: Sicherheits- (Art. 31, 107i) und technische "
            "(Art. 20, 30) Bewertungs-Verfahren. KEIN harter STRUKTURELL-"
            "Marker, weil XLSX kein Outcome-Detail liefert "
            "(Maintained / Restriction / Withdrawal nicht direkt aus "
            "'European Commission final decision' ableitbar). Caveat-"
            "Counter-Evidence im display_value."
        ),
        "items": items,
    }
    atomic_write_json(EMA_OUT, output, ensure_ascii=False, indent=2)
    LOG.info(f"ema_referrals: wrote {len(items)} items → {EMA_OUT}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wd", action="store_true", help="Refresh Wikipedia withdrawn drugs")
    parser.add_argument("--ema", action="store_true", help="Refresh EMA Referrals XLSX")
    parser.add_argument("--all", action="store_true", help="Refresh both (default)")
    args = parser.parse_args()
    if not (args.wd or args.ema or args.all):
        args.all = True
    rc = 0
    if args.wd or args.all:
        rc = refresh_withdrawn_drugs() or rc
    if args.ema or args.all:
        rc = refresh_ema_referrals() or rc
    return rc


if __name__ == "__main__":
    sys.exit(main())
