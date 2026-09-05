"""Frontex — EU-Grenzschutz-Statistiken (irreguläre Grenzübertritte).

Datenquelle: Frontex Migratory Map + Pressemeldungen. Kuratiertes JSON
in ``data/frontex.json``, weil die Live-API zwar existiert (data.europa.eu)
aber das Frontex-Dashboard primär als HTML-Karte ausliefert. Die Pressemeldungen
liefern die wichtigsten Aggregate (Total, Routen-Aufschlüsselung, Top-Herkunfts-
länder).

Use-Case:
- "irreguläre Grenzübertritte 2025 sind um X gesunken/gestiegen"
- "Westbalkan-Route geschlossen / aktiv"
- "Mittelmeer-Tote 2025"
- "Frontex-Statistik" / "EU-Außengrenzen"
"""

import json
import logging
import os

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "frontex.json",
)

_cache: dict | None = None


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_FRONTEX_TERMS = (
    "frontex", "grenzschutz eu", "eu-grenzschutz",
    "eu-außengrenzen", "eu außengrenzen",
    "irreguläre grenzübertritte", "irregular border crossings",
    "illegale grenzübertritte",
    "westbalkan-route", "westbalkanroute", "balkan-route",
    "mittelmeer-route", "mittelmeerroute",
    # Adjektiv-Flexion mit aufnehmen: "im westlichEN Mittelmeer" traf sonst
    # nicht (gleiche Falle wie "europaeischEN Parlament", QA50D).
    "zentrales mittelmeer", "östliches mittelmeer", "westliches mittelmeer",
    "zentralen mittelmeer", "östlichen mittelmeer", "westlichen mittelmeer",
    "oestliches mittelmeer", "oestlichen mittelmeer",
    "kanaren-route", "westafrika-route", "westafrikaroute",
    "ärmelkanal migration", "channel crossings",
    "mittelmeer tote", "tote mittelmeer", "tote im mittelmeer",
    "im mittelmeer gestorben", "menschen gestorben mittelmeer",
    "ertrunken mittelmeer", "mittelmeer ertrunken",
    "fluchtroute eu", "fluchtrouten",
)


def _claim_mentions_frontex(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _FRONTEX_TERMS)
    if has_term:
        return True
    # Composite: 'grenzübertritt' + EU-Bezug
    has_grenzubert = any(t in claim_lc for t in (
        "grenzübertritt", "grenzübertrit", "border crossing",
        "border-crossing",
    ))
    has_eu = any(t in claim_lc for t in (
        "eu", "europa", "europäische union", "european union",
    ))
    if has_grenzubert and has_eu:
        return True
    # Composite: 'illegale/irregulaere Migration' + EU-/Grenz-Bezug.
    # Die haeufigste Formulierung ueberhaupt traf bis 2026-09 WEDER frontex
    # NOCH migration_pack — beide lieferten fuer "Die illegale Migration in
    # die EU steigt dramatisch" null Treffer. Bewusst mit Regionsbezug
    # gekoppelt: eine Aussage ohne Region ("die illegale Migration steigt")
    # laesst sich mit EU-Aussengrenzdaten nicht sauber beantworten.
    has_migration = any(t in claim_lc for t in (
        "illegale migration", "illegaler migration",
        "irreguläre migration", "irregulaere migration", "irregulärer migration",
        "illegale einwanderung", "illegale zuwanderung",
        "irreguläre einwanderung", "irreguläre zuwanderung",
        "illegal migration", "irregular migration",
    ))
    has_grenzbezug = has_eu or any(t in claim_lc for t in (
        "außengrenze", "aussengrenze", "grenze", "mittelmeer", "route",
    ))
    if has_migration and has_grenzbezug:
        return True
    # Composite: Mittelmeer + Tote/Tod
    has_mittelmeer = "mittelmeer" in claim_lc
    has_tot = any(t in claim_lc for t in (
        "tote", "gestorben", "tod ", "tods", "ertrunken",
    ))
    if has_mittelmeer and has_tot:
        return True
    return False


def claim_mentions_frontex_cached(claim: str) -> bool:
    return _claim_mentions_frontex((claim or "").lower())


# ---------------------------------------------------------------------------
# Static load
# ---------------------------------------------------------------------------
def _load_static_json() -> dict | None:
    global _cache
    if _cache is not None:
        return _cache
    try:
        with open(STATIC_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "data" not in data:
            logger.warning("frontex.json missing 'data' key")
            return None
        _cache = data
        logger.info("Frontex data loaded: 1 dataset")
        return _cache
    except FileNotFoundError:
        logger.warning(f"frontex.json not found at {STATIC_JSON_PATH}")
        return None
    except Exception as e:
        logger.warning(f"frontex.json load failed: {e}")
        return None


async def fetch_frontex(client=None):
    data = _load_static_json()
    if not data:
        return []
    return [data] if data else []


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _de_int(v):
    if v is None:
        return "?"
    try:
        return f"{int(v):,}".replace(",", ".")
    except Exception:
        return str(v)


def _zeichen(v) -> str:
    """'-37 %' bzw. '+37 %' — das Vorzeichen ist bei Migrationszahlen die
    eigentliche Aussage und darf nicht verlorengehen."""
    try:
        return f"{int(v):+d} %"
    except (TypeError, ValueError):
        return "?"


def _routen_zeile(r: dict) -> str:
    teil = (f"{r.get('name')}: {_de_int(r.get('detektionen'))} "
            f"({_zeichen(r.get('veraenderung_pct'))} gegenüber "
            f"{_de_int(r.get('vorjahreszeitraum'))})")
    if r.get("hinweis"):
        teil += f" — {r['hinweis']}"
    return teil


def _build_results(fact: dict, claim_lc: str) -> list[dict]:
    """Baut Haupt- und Routen-Ergebnis.

    Zeitraum und Routen kommen AUSSCHLIESSLICH aus den Daten. Bis 2026-09
    standen "2025", "2024" und die Feldnamen ``routen_2025``/``trend_2025``
    fest im Code — ein Daten-Refresh haette weiter den alten Zeitraum
    beschriftet (gleiche Klasse wie wifo_ihs #131 und rki #138).
    """
    data = fact.get("data") or {}
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "Frontex"
    zeitraum = data.get("zeitraum") or ""
    vergleich = data.get("vergleichszeitraum") or "dem Vorjahreszeitraum"

    results: list[dict] = []
    routen = data.get("routen") or []

    headline = (
        f"Frontex, {zeitraum}: rund "
        f"{_de_int(data.get('detektionen_eu_gesamt_approx'))} Detektionen "
        f"irregulärer Grenzübertritte an den EU-Aussengrenzen, "
        f"{_zeichen(data.get('veraenderung_ggue_vorjahreszeitraum_pct'))} "
        f"gegenüber {vergleich}. Es sind VORLÄUFIGE Zahlen und DETEKTIONEN, "
        f"nicht Personen."
    )

    description_parts: list[str] = []
    if routen:
        description_parts.append(
            "Routen im Zeitraum: " + " · ".join(_routen_zeile(r) for r in routen))
    kontext = data.get("jahreswerte_kontext") or {}
    if kontext.get("2025_gesamt_approx"):
        description_parts.append(
            f"Ganzjahres-Kontext: 2024 rund "
            f"{_de_int(kontext.get('2024_gesamt'))} Detektionen "
            f"({_zeichen(kontext.get('2024_veraenderung_pct'))}), 2025 rund "
            f"{_de_int(kontext.get('2025_gesamt_approx'))} "
            f"({_zeichen(kontext.get('2025_veraenderung_pct'))}). "
            f"{kontext.get('hinweis', '')}")
    tote = data.get("todesfaelle_mittelmeer") or {}
    if tote.get("wert_2025_min"):
        description_parts.append(
            f"Mittelmeer-Todesfälle mindestens "
            f"{_de_int(tote.get('wert_2025_min'))} ({tote.get('quelle')}). "
            f"{tote.get('hinweis', '')}")
    for caveat in data.get("wichtige_caveats") or []:
        description_parts.append(caveat)

    results.append({
        "indicator_name": headline,
        "indicator": "frontex_main",
        "country": "EU",
        "country_name": "Europäische Union",
        "year": zeitraum,
        "value": data.get("detektionen_eu_gesamt_approx"),
        "display_value": headline,
        "description": " ".join(p for p in description_parts if p).strip(),
        "url": src,
        "source": label,
    })

    # Spezial-Eintrag wenn Claim eine spezifische Route nennt.
    route_triggers = [
        ("westbalkan", "westbalkan"),
        ("balkan", "westbalkan"),
        ("westafrika", "westafrikanisch"),
        ("kanaren", "westafrikanisch"),
        # Beide Adjektiv-Formen: "im westlichEN Mittelmeer" ist die
        # natuerlichere Formulierung als "westlichES Mittelmeer".
        ("zentrales mittelmeer", "zentrales mittelmeer"),
        ("zentralen mittelmeer", "zentrales mittelmeer"),
        ("östliches mittelmeer", "östliches mittelmeer"),
        ("östlichen mittelmeer", "östliches mittelmeer"),
        ("oestliches mittelmeer", "östliches mittelmeer"),
        ("oestlichen mittelmeer", "östliches mittelmeer"),
        ("kreta", "östliches mittelmeer"),
        ("westliches mittelmeer", "westliches mittelmeer"),
        ("westlichen mittelmeer", "westliches mittelmeer"),
        ("ärmelkanal", "ärmelkanal"),
        ("aermelkanal", "ärmelkanal"),
        ("channel", "ärmelkanal"),
        ("landgrenze", "östliche landgrenze"),
        ("albanien", "albanien"),
    ]
    for trigger_kw, route_substr in route_triggers:
        if trigger_kw in claim_lc:
            for r in routen:
                if route_substr in r["name"].lower():
                    results.insert(0, {
                        "indicator_name": f"Frontex {r['name']}, {zeitraum}",
                        "indicator": "frontex_route",
                        "country": "EU", "country_name": "Europäische Union",
                        "year": zeitraum,
                        "display_value": f"Frontex, {zeitraum} — {_routen_zeile(r)}.",
                        "description": (
                            "Frontex-Routen-Aufschlüsselung, vorläufige Daten. "
                            "WICHTIG: Detektionen sind keine Personen — eine "
                            "Person kann mehrfach gezählt werden."
                        ),
                        "url": src, "source": label,
                    })
                    break
            break

    return results


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_frontex(analysis: dict) -> dict:
    empty = {
        "source": "Frontex",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_frontex(matchable):
        return empty

    data = _load_static_json()
    if not data:
        return empty

    return {
        "source": "Frontex",
        "type": "official_data",
        "results": _build_results(data, matchable),
    }
