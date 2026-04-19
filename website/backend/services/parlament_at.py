"""Parlament.gv.at (Österreichisches Parlament) — Nationalrat-Stammdaten.

Datenquelle: Parlament Österreich, Open Data-Portal
(https://www.parlament.gv.at/recherchieren/open-data/).
Offizielle JSON-API des Parlaments — keine Anmeldung nötig.

v1-Umfang (bewusst klein gehalten):
- Aktuelle Klubstärken (Mandatsverteilung) im Nationalrat
- Summe aller Mandate (zur Bestätigung der 183 Sitze)
- Einfache Mehrheitsrechnung (wie viele Sitze für 50% + 1)

Warum so schmal?  Das Parlament publiziert ~25 Datensätze (Abstimmungen,
Ausschüsse, Anfragen, Petitionen …).  Ein umfassendes Faktencheck-Mapping
aller dieser Datensätze wäre ein Projekt für sich und würde politische
Wertungsfragen aufwerfen (z.B. Abstimmungs-Interpretation).  v1 beschränkt
sich auf die *konstitutionelle Bestandsaufnahme* — wer sitzt aktuell im
Nationalrat, wie stark ist welcher Klub — als reine Faktenquelle.

Lizenz: CC BY 4.0 (Parlament Österreich), Quelle: parlament.gv.at.
Zitation: Parlament Österreich, Open-Data-Portal (abgerufen
{Abfrage-Datum}).

GUARDRAILS (siehe project_political_guardrails.md):
- Wir zitieren Mandatsverteilungen, bewerten keine Parteien.
- Wir ordnen keine Parteien auf einer Links-Rechts-Skala ein.
- Keine Koalitions-Prognosen.
- Mehrheitsrechnung ist rein arithmetisch (50% + 1 von 183 = 92).
"""

import json
import logging
import re
import time

import httpx

logger = logging.getLogger("evidora")

# Offizieller JSON-POST-Endpunkt für "Aktuelle Abgeordnete zum NR"
# (Wird im Open-Data-Portal als kanonische Liste geführt.)
# Parameter M=M&W=W erzwingt die Rückgabe beider Geschlechter — ohne diese
# Flags antwortet die API mit "Mann und/oder Frau muss ausgewählt sein!".
PARLAMENT_NR_URL = (
    "https://www.parlament.gv.at/Filter/api/json/post"
    "?jsMode=EVAL&FBEZ=WFW_002&listeId=10002&showAll=true&M=M&W=W"
)

PARLAMENT_CACHE_TTL = 86400  # 24h
NR_TOTAL_SEATS = 183
NR_MAJORITY = NR_TOTAL_SEATS // 2 + 1  # 92

# Cache structure: {klubs: {klub_name: count}, total: int, fetched_at: float}
_cache: dict | None = None
_cache_time: float = 0.0

# Keywords, die den Parlament-Lookup auslösen (DE-only, AT-spezifisch)
PARLAMENT_KEYWORDS = [
    # Nationalrat / Parlament
    "nationalrat", "hohes haus",
    # AT-Parlament explizit (wird auch vom AT-Kontext-Check bestätigt)
    "österreichisches parlament", "österreichischen parlament",
    "österr. parlament", "parlament österreich",
    "parlament in österreich",
    # Klubs / Fraktion
    "klubstärke", "klubstärken", "klubobmann", "klubobfrau",
    "klubobleute",
    # Mandate / Sitze
    "mandat", "mandate", "mandatsverteilung",
    "sitzverteilung", "sitze im parlament", "sitze im nationalrat",
    "nationalrats-mandate", "nationalratsmandate",
    # Mehrheiten
    "mehrheit im nationalrat", "mehrheit im parlament",
    "regierungsmehrheit", "zweidrittelmehrheit",
    # Klubnamen (nur wirksam in Kombination mit AT-Kontext via
    # _claim_mentions_parlament, damit "FPÖ" in Bayern-Claim nicht triggert)
    "fpö-klub", "övp-klub", "spö-klub", "grünen-klub", "neos-klub",
    # 27./28. Gesetzgebungsperiode
    "gesetzgebungsperiode", "legislaturperiode im nationalrat",
    "xxvii. gp", "xxviii. gp", "27. gp", "28. gp",
]

# Österreich-Kontext: diese Keywords im Claim, zusammen mit einem der
# obigen Parlament-Keywords, aktivieren den Lookup.  Reines "FPÖ" oder
# "SPÖ" allein reicht nicht — z.B. könnten sie in einem Claim über
# Bayern/Europa auftauchen.
#
# WICHTIG: Nur mehrstelliges, unverwechselbares Vokabular.  Kurzformen
# wie "at" matchen als Substring in "hat"/"bundesstaat"/"parlament" und
# erzeugen False-Positives — siehe Regression-Test im Smoke-Run.
AT_CONTEXT_KEYWORDS = [
    "österreich", "austria",
    "wien", "vienna", "graz", "linz", "salzburg", "innsbruck",
    "bundeskanzler", "bundespräsident", "nationalrat",
    "bundesrat", "klubobmann", "klubobfrau",
    # Explizit Stadt- und Landesnamen, damit „Kärnten ändert das …"
    # auch ohne "Österreich" als AT-Kontext zählt.
    "niederösterreich", "oberösterreich", "steiermark",
    "kärnten", "vorarlberg", "burgenland", "tirol",
]


def _strip_klub_html(html_fragment: str | None) -> str:
    """Extract the party short name from HTML like
    ``<span title="NEOS Parlamentsklub" class="...">NEOS</span>``.

    Falls back to the raw string if no tags are present.
    """
    if not html_fragment:
        return ""
    m = re.search(r">([^<]+)<", html_fragment)
    return (m.group(1) if m else html_fragment).strip()


async def fetch_parlament_nr(
    client: httpx.AsyncClient | None = None,
) -> dict | None:
    """Fetch current Nationalrat composition.

    Returns dict {klubs: {name: count}, total: int, fetched_at: float} or
    None on failure.
    """
    global _cache, _cache_time

    now = time.time()
    if _cache is not None and (now - _cache_time) < PARLAMENT_CACHE_TTL:
        return _cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        close_client = True

    try:
        # Die API erwartet POST, auch wenn die Filter via Query-String
        # übergeben werden.  Body ist ein leeres JSON-Objekt.
        resp = await client.post(
            PARLAMENT_NR_URL,
            json={},
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as e:
        logger.warning(f"Parlament.gv.at: fetch failed: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.warning(f"Parlament.gv.at: invalid JSON: {e}")
        return None
    finally:
        if close_client:
            await client.aclose()

    rows = data.get("rows") or []
    header = data.get("header") or []
    if not rows or not header:
        logger.warning("Parlament.gv.at: empty response")
        return None

    # Robuste Spalten-Zuordnung: finde Index der Sort_FR-Spalte (stabiler
    # Klub-Kurzname ohne HTML), fallback auf HTML-Spalte "Klub".
    sort_fr_idx = next(
        (i for i, h in enumerate(header) if h.get("label") == "Sort_FR"),
        None,
    )
    klub_html_idx = next(
        (i for i, h in enumerate(header) if h.get("label") == "Klub"),
        1,
    )

    klubs: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, list):
            continue
        klub = None
        if sort_fr_idx is not None and sort_fr_idx < len(row):
            klub = (row[sort_fr_idx] or "").strip()
        if not klub and klub_html_idx < len(row):
            klub = _strip_klub_html(row[klub_html_idx])
        if not klub:
            continue
        klubs[klub] = klubs.get(klub, 0) + 1

    total = sum(klubs.values())
    _cache = {
        "klubs": klubs,
        "total": total,
        "fetched_at": now,
    }
    _cache_time = now
    logger.info(
        f"Parlament.gv.at: {total} NR-Abgeordnete, "
        f"Klubs={dict(sorted(klubs.items(), key=lambda kv: -kv[1]))}"
    )
    return _cache


def _claim_mentions_parlament(claim: str) -> bool:
    """True if the claim concerns the Austrian Nationalrat composition.

    Requires both a parlament-specific keyword and an Austria-context
    keyword, to avoid false positives (e.g. "FPÖ" mentioned in a Bavaria
    claim).  The kw ``nationalrat`` itself counts as AT-context.
    """
    cl = claim.lower()
    has_kw = any(kw in cl for kw in PARLAMENT_KEYWORDS)
    if not has_kw:
        return False
    has_at = any(kw in cl for kw in AT_CONTEXT_KEYWORDS)
    return has_at


async def search_parlament_at(analysis: dict) -> dict:
    """Return current Nationalrat composition if the claim is AT-specific."""
    claim = analysis.get("claim", "")
    if not _claim_mentions_parlament(claim):
        return {
            "source": "Parlament.gv.at",
            "type": "official_data",
            "results": [],
        }

    data = await fetch_parlament_nr()
    if not data:
        return {
            "source": "Parlament.gv.at",
            "type": "official_data",
            "results": [],
        }

    klubs = data["klubs"]
    total = data["total"]

    if not klubs or total == 0:
        return {
            "source": "Parlament.gv.at",
            "type": "official_data",
            "results": [],
        }

    # Sortiere nach Mandatszahl absteigend
    ranked = sorted(klubs.items(), key=lambda kv: -kv[1])

    # Hauptzeile: Gesamtkomposition
    composition = ", ".join(f"{name} {count}" for name, count in ranked)
    largest_name, largest_count = ranked[0]
    has_majority = largest_count >= NR_MAJORITY

    if has_majority:
        majority_note = (
            f"{largest_name} hält mit {largest_count} von {total} Mandaten "
            f"bereits die absolute Mehrheit (≥ {NR_MAJORITY})."
        )
    else:
        shortfall = NR_MAJORITY - largest_count
        majority_note = (
            f"Die stärkste Fraktion ({largest_name}, {largest_count} Mandate) "
            f"braucht {shortfall} weitere Mandate für eine absolute Mehrheit "
            f"({NR_MAJORITY} von {total})."
        )

    results: list[dict] = [
        {
            "indicator_name": f"Nationalrat — Aktuelle Klubstärken ({total} Sitze)",
            "indicator": "parlament_nr_composition",
            "country": "AUT",
            "country_name": "Österreich",
            "year": str(time.localtime().tm_year),
            "value": total,
            "display_value": composition,
            "description": majority_note,
            "url": "https://www.parlament.gv.at/wer/nationalrat/",
        }
    ]

    # Pro-Klub-Detailzeile für maximale Transparenz im Synthesizer
    for name, count in ranked:
        share = 100.0 * count / total if total else 0.0
        results.append({
            "indicator_name": f"Klub {name}: {count} Mandate ({share:.1f} %)",
            "indicator": "parlament_nr_klub",
            "country": "AUT",
            "country_name": "Österreich",
            "year": str(time.localtime().tm_year),
            "value": count,
            "display_value": f"{count} / {total}",
            "url": "https://www.parlament.gv.at/wer/nationalrat/",
        })

    # Methodik-Caveat
    results.append({
        "indicator_name": "WICHTIGER KONTEXT: Aktuelle Momentaufnahme",
        "indicator": "context",
        "country": "",
        "country_name": "",
        "year": "",
        "value": "",
        "display_value": "",
        "url": "https://www.parlament.gv.at/recherchieren/open-data/",
        "description": (
            "Die Zahlen geben die aktuelle Mandatsverteilung im Nationalrat wieder und "
            "werden täglich aus dem Open-Data-Portal des Parlaments abgerufen. "
            "Einschränkungen: "
            "(1) Momentaufnahme — Mandate können durch Austritt, Klubwechsel oder Nachrücken "
            "in der laufenden Gesetzgebungsperiode variieren. "
            "(2) Wahltermine — die Zahlen nach einer Nationalratswahl können für wenige Tage "
            "vom Wahlergebnis abweichen, bis die neuen Abgeordneten angelobt sind. "
            "(3) Mehrheitsrechnung — 92 Mandate sind arithmetische Grundmehrheit. "
            "Zwei-Drittel- (122) und Verfassungs-Mehrheiten ergeben sich entsprechend. "
            "(4) Wir geben keine Einschätzung zu Koalitionen oder Parteipositionen ab — "
            "die Zahlen sind reine Sitzarithmetik."
        ),
    })

    return {
        "source": "Parlament.gv.at",
        "type": "official_data",
        "results": results,
    }
