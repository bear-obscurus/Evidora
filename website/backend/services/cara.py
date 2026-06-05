"""CARA — Center for Applied Research in the Apostolate (Georgetown).

Quelle: CARA (Center for Applied Research in the Apostolate) an der
Georgetown University, Washington D.C. — seit 1964 das fuehrende
sozialwissenschaftliche Forschungszentrum zur katholischen Kirche.
Liefert quantitative Statistik zu US-Katholiken, weltweiten Katholiken,
Pfarreien, Berufungen (Priester, Diakone, Ordensleute, Seminaristen),
Sakramenten (Taufen, Trauungen) und Messbesuch.

Komplementaere Hauptquelle: Vatikan Annuarium Statisticum Ecclesiae
(jaehrlicher statistischer Bilanzbericht des Heiligen Stuhls,
Stato della Citta del Vaticano / Libreria Editrice Vaticana). CARA's
Frequently Requested Church Statistics ziehen ihre weltweiten Eckwerte
direkt aus dem Annuarium; daher zitieren wir beide nebeneinander.

Zugriffsmuster (Pack-Approach):
  - CARA publiziert die meisten Daten als kuratierte HTML-Tabellen
    ("Frequently Requested Church Statistics", aktualisiert jaehrlich)
    sowie als Special Reports und Press Releases.
  - Es gibt KEINE offene JSON-/REST-API. Detail-Studien sind
    z. T. kostenpflichtig (CARA Catholic Polls, NCEA-Reports).
  - "Frequently Requested Church Statistics" und "CARA Special Reports"
    sind aber frei abrufbar und werden hier als kuratierte Eckwert-
    Tabelle eingebettet (Stand jeweils notiert).
  - polite_client steht bereit fuer optionale Live-Fetches der CARA-
    Quick-Facts-Seite (HEAD/GET) zur URL-Lebenszeit-Verifikation.

Lizenz / Zitierhinweis:
  - CARA Frequently Requested Statistics: frei verfuegbar, Zitierhinweis
    "Source: CARA at Georgetown University" obligatorisch.
  - CARA Special Reports: frei einsehbar (PDF), Zitierhinweis erforderlich.
  - Annuarium Statisticum Ecclesiae: Libreria Editrice Vaticana ©
    Heiliger Stuhl; statistische Eckwerte gelten als Facts und sind
    nicht-urheberrechtlich-schutzfaehig (rein numerisch).

Use-Case-Trigger:
  - "CARA Catholic", "CARA Georgetown", "Center for Applied Research"
  - "Katholiken weltweit", "Katholiken global", "Anzahl Katholiken"
  - "Vatikan-Statistik", "Vatikan Statistik", "Statistik Heiliger Stuhl"
  - "Annuarium Statisticum Ecclesiae"
  - "katholische Kirche [Land] Statistik" + Land-Kontext
  - "Pfarreien weltweit", "Priester weltweit", "Seminaristen"
  - "Messbesuch", "Sonntagsmesse Besucherzahlen"

Komplementaer zu:
  - religionsgemeinschaften_pack.py — qualitative Religions-Mythen-
    Klaerungen (Vatikan-Vermoegen-Mythos, Missbrauchs-Aufarbeitung,
    Religion-Gewalt-Korrelation). CARA liefert die *quantitative*
    Saeule (Mitglieder, Pfarreien, Berufungen).
  - destatis.json / statistik_austria — DACH-Konfessions-Statistik
    (DESTATIS misst Kirchenmitgliedschaft, CARA misst weltweite
    katholische Population).
  - pew_research / world_values_survey — Glaubenszugehoerigkeits-
    Surveys; CARA ergaenzt mit klassischer Kirchen-Verwaltungsstatistik.

Politische Guardrails (project_political_guardrails.md):
  - KEINE theologischen Wertungen ("Kirche schrumpft = schlecht/gut").
  - KEINE Prognosen ueber Konversionen oder Glaubensverluste.
  - Pure deskriptive Mitglieder-/Pfarrei-/Berufungs-Statistik aus
    Vatikan-Eigen-Reporting (Annuarium) und CARA-Aggregaten.
  - Konfessions-Statistik ist KEIN Polit-Thema im engeren Sinne —
    keines der 4 Tabus wird beruehrt (keine Partei-Bewertung,
    keine Wahl-Prognose, keine eigene Links/Rechts-Klassifikation,
    keine normativen Aussagen).
"""

# WIRING fuer main.py (NICHT in dieser Datei vornehmen):
#   from services.cara import search_cara, claim_mentions_cara_cached
#   if claim_mentions_cara_cached(claim):
#       tasks.append(cached("CARA", search_cara, analysis))
#       queried_names.append("CARA Georgetown")
#
# WIRING fuer services/reranker.py (Whitelist):
#   "CARA" und/oder "CARA Georgetown" in SOURCE_WHITELIST eintragen;
#   "cara_" als Whitelist-Prefix in INDICATOR_WHITELIST_PREFIXES.
#
# WIRING fuer services/data_updater.py: KEIN Prefetch noetig — die
# kuratierten Eckwerte sind embedded; 24-h-In-Memory-Cache reicht.

from __future__ import annotations

import logging
import time
from functools import lru_cache

from services._http_polite import polite_client  # noqa: F401  (held for future live-verify)

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------
CARA_BASE_URL = "https://cara.georgetown.edu/"
CARA_FAQ_URL = "https://cara.georgetown.edu/frequently-requested-church-statistics"
VATICAN_ANNUARIUM_URL = (
    "https://press.vatican.va/content/salastampa/en/bollettino/pubblico/"
)

TIMEOUT_S = 15.0
MAX_RESULTS = 5
CACHE_TTL_S = 24 * 60 * 60  # 24 h

# In-Memory-Cache: cache-key (claim_lc gehasht) -> (ts, payload)
_search_cache: dict[str, tuple[float, dict]] = {}


# ---------------------------------------------------------------------------
# Trigger-Vokabular
# ---------------------------------------------------------------------------
_CARA_TERMS = (
    # Direkter Quellen-Bezug
    "cara catholic", "cara georgetown",
    "center for applied research in the apostolate",
    "apostolate georgetown",
    # Vatikan-Statistik-Bezeichnung
    "annuarium statisticum ecclesiae", "annuarium statisticum",
    "annuario pontificio statistico", "statistik des heiligen stuhls",
    "vatikan-statistik", "vatikan statistik", "vatican statistics",
    "vatican statistical yearbook", "statistical yearbook of the church",
    "heiliger stuhl statistik", "heiligen stuhl statistik",
    "heiliger-stuhl-statistik", "holy see statistics",
    # Weltweite Katholiken-Eckwerte
    "katholiken weltweit", "katholiken global",
    "katholische kirche weltweit", "katholische kirche global",
    "anzahl der katholiken", "anzahl katholiken",
    "wie viele katholiken", "wieviele katholiken",
    "catholics worldwide", "catholics globally", "number of catholics",
    # Pfarreien / Priester / Seminaristen — generisch weltweit
    "pfarreien weltweit", "katholische pfarreien weltweit",
    "priester weltweit", "katholische priester weltweit",
    "seminaristen weltweit", "katholische seminaristen",
    "berufungen katholisch", "katholische berufungen",
    # Messbesuch
    "messbesuch katholisch", "sonntagsmesse besucher",
    "sonntagsmesse besucherzahlen", "katholiken kirchgang",
    "mass attendance catholic",
)


# Quantifier-Composite-Vokabular (Zahl-/Mengen-Indikatoren)
_QUANTIFIER_TERMS = (
    " mio", " mio.", " mrd", " mrd.",
    "million", "millionen", "milliard", "milliarden",
    "billion", "billions",
    "prozent", " % ", "% ", " %", "percent",
    "anzahl", "zahl der", "zahl an ",
    " mitglieder", " gläubige", " glaeubige",
    "bevölkerung", "bevoelkerung", "population",
)

# Katholik-Token (fuer Quantifier-Composite)
_CATHOLIC_TOKENS = (
    "katholik", "katholiken",  # Substantiv-Plural inkl. Beugung
    "katholisch", "katholische", "katholischen",
    "römisch-katholisch", "roemisch-katholisch",
    "catholic", "catholics",
)


def _has_any(text: str, terms) -> bool:
    return any(t in text for t in terms)


def _claim_mentions_cara(claim_lc: str) -> bool:
    """Trigger-Check (lowercase claim erwartet).

    True bei:
      - Direkter CARA-/Annuarium-/Vatikan-Statistik-Term.
      - Composite: "katholisch" + (weltweit | global | pfarrei | priester |
        seminarist | messbesuch | mitgliederzahl).
      - Composite: ("katholische kirche" | "römisch-katholisch") +
        explizite Statistik-/Zahl-Begriffe (statistik | anzahl |
        zahl der | mitglieder | gläubige).
    """
    if not claim_lc:
        return False

    if _has_any(claim_lc, _CARA_TERMS):
        return True

    # Composite-Trigger
    has_cath = _has_any(claim_lc, (
        "katholisch", "katholische", "katholischen", "katholiken",
        "römisch-katholisch", "roemisch-katholisch",
        "roman catholic", "catholic church",
    ))
    if has_cath:
        if _has_any(claim_lc, (
            "weltweit", "global", "world", "worldwide",
            "pfarrei", "pfarreien", "parish", "parishes",
            "priester", "priest", "priests",
            "diakon", "diakone", "deacon", "deacons",
            "seminarist", "seminaristen", "seminarian",
            "berufung", "berufungen", "vocation", "vocations",
            "ordensleute", "ordensbruder", "ordensschwester",
            "religious order", "religious sister", "religious brother",
            "messbesuch", "kirchgang", "sonntagsmesse",
            "mitgliederzahl", "anzahl gläubige",
        )):
            return True

    # Composite-Trigger: ("katholische kirche" / "römisch-katholisch") +
    # ein generisches Land + Statistik-Begriff. Wir prueffen pauschal auf
    # "statistik", "anzahl", "zahl der", "mitglieder" — die Land-
    # Spezifikation uebernimmt das Synthesizer-Routing.
    if has_cath:
        if _has_any(claim_lc, (
            " statistik", "-statistik",
            "anzahl der", "anzahl an ",
            "zahl der ", " mitglieder ", " gläubige", " glaeubige",
        )):
            return True

    # Quantifier-Composite: Zahl-/Mengen-Begriff + Katholik-Token
    # ("1,39 Mrd Katholiken", "17 Prozent Katholiken", "Anzahl Katholiken").
    if _has_any(claim_lc, _CATHOLIC_TOKENS) and _has_any(
        claim_lc, _QUANTIFIER_TERMS
    ):
        return True

    # Denomination-Vergleichs-Composite: Katholik + Denomination +
    # Vergleichswort ("mehr Katholiken als Baptisten", "größte
    # Religionsgemeinschaft", "meisten Gläubigen").
    _DENOMINATION_TOKENS = (
        "baptist", "protestant", "evangelisch", "evangelikal",
        "methodist", "lutheran", "lutherisch", "anglikan",
        "denomination", "religionsgemeinschaft", "konfession",
        "kirche in den usa", "kirche in amerika",
    )
    _COMPARISON_TOKENS = (
        " mehr ", " weniger ", "größte", "groesste", "kleinste",
        "meisten", "wenigsten", "ranking", "größer als", "grösser als",
        "groesser als", "kleiner als",
    )
    if _has_any(claim_lc, _CATHOLIC_TOKENS) and (
        _has_any(claim_lc, _DENOMINATION_TOKENS)
        or _has_any(claim_lc, _COMPARISON_TOKENS)
    ):
        return True

    # Vatikan / Heiliger Stuhl als impliziter Trigger ZUSAMMEN mit
    # Katholik-Bezug oder Statistik-Begriff.
    if _has_any(claim_lc, (
        "vatikan", "vatican", "heiliger stuhl", "heiligen stuhl",
        "holy see",
    )):
        if _has_any(claim_lc, _CATHOLIC_TOKENS) or _has_any(claim_lc, (
            " statistik", "-statistik", "annuarium",
            "anzahl", "zahl der", "kirchen-statistik",
        )):
            return True

    return False


@lru_cache(maxsize=2048)
def claim_mentions_cara_cached(claim: str) -> bool:
    """LRU-gecachter Trigger-Check (Hot-Path-friendly)."""
    return _claim_mentions_cara((claim or "").lower())


# ---------------------------------------------------------------------------
# Kuratierte Eckwerte (Pack-Approach)
# ---------------------------------------------------------------------------
# Quelle: CARA "Frequently Requested Church Statistics" + Vatikan
# Annuarium Statisticum Ecclesiae (jeweiliger Stand-Jahr s. Eintrag).
# Wir embeddeen die KONSENS-Eckwerte direkt; Detail-Tabellen sind in
# CARA-Special-Reports verfuegbar (verlinkt).

_CARA_FACTS: tuple[dict, ...] = (
    {
        "key": "cara_global_catholics",
        "scope": "global",
        "year": "2022",
        "value_num": 1_390_000_000,
        "headline": (
            "Weltweite Katholiken (Vatikan Annuarium Statisticum "
            "Ecclesiae, Stand 2022): rund 1,39 Milliarden. CARA "
            "fasst diesen Eckwert in seinen 'Frequently Requested "
            "Church Statistics' zusammen."
        ),
        "description": (
            "Quelle ist die jaehrliche zentrale Bilanzstatistik des "
            "Heiligen Stuhls (Annuarium Statisticum Ecclesiae, "
            "Libreria Editrice Vaticana). CARA aggregiert sie fuer "
            "den englischsprachigen Raum. Die globale katholische "
            "Population waechst absolut, der Anteil an der "
            "Weltbevoelkerung liegt seit Jahren stabil bei rund 17,7 %."
        ),
        "url": CARA_FAQ_URL,
    },
    {
        "key": "cara_global_parishes",
        "scope": "global",
        "year": "2022",
        "value_num": 221_700,
        "headline": (
            "Katholische Pfarreien weltweit (Annuarium 2022): rund "
            "221.700. Davon Europa ~52.500, Amerika ~67.500, "
            "Afrika ~14.000, Asien ~38.000, Ozeanien ~2.500."
        ),
        "description": (
            "Pfarreien-Bestand ist seit 2010 leicht ruecklaeufig in "
            "Europa, deutlich wachsend in Afrika und Asien. CARA "
            "weist auf strukturelle Pfarrei-Zusammenlegungen in "
            "DE/AT/US hin (gleicher Globalwert, weniger Lokal-"
            "Pfarreien)."
        ),
        "url": CARA_FAQ_URL,
    },
    {
        "key": "cara_global_priests",
        "scope": "global",
        "year": "2022",
        "value_num": 407_872,
        "headline": (
            "Katholische Priester weltweit (Annuarium 2022): "
            "rund 407.900 (Diozesan- + Ordenspriester). Die Zahl "
            "ist seit 2014 leicht ruecklaeufig (-1,5 %); Wachstum "
            "in Afrika/Asien, Rueckgang in Europa/Amerika."
        ),
        "description": (
            "Aufschluesselung Annuarium 2022: Diozesanpriester "
            "~280.000, Ordenspriester ~127.900. CARA-Trend-"
            "Analysen zeigen: Afrika +3,2 % p. a., Asien +1,1 %, "
            "Europa -1,7 % p. a. Die Gesamtzahl folgt damit dem "
            "regional gegenlaeufigen Trend."
        ),
        "url": CARA_FAQ_URL,
    },
    {
        "key": "cara_global_seminarians",
        "scope": "global",
        "year": "2022",
        "value_num": 108_481,
        "headline": (
            "Katholische Seminaristen weltweit (Annuarium 2022): "
            "rund 108.500. Hoechststand 2011 bei rund 120.600; "
            "seitdem leichter Rueckgang weltweit, getrieben durch "
            "Rueckgang in Europa/Amerika; Afrika und Asien stabil "
            "bis wachsend."
        ),
        "description": (
            "CARA-Special-Report zu Vocations: Seminaristen-Zahlen "
            "sind der Frueh-Indikator fuer Priester-Nachwuchs in "
            "8-10 Jahren. Regionaler Trend: Afrika rund 32.000 "
            "(+), Amerika rund 32.000 (-), Asien rund 32.000 (+), "
            "Europa rund 12.500 (--), Ozeanien rund 1.000 (stabil)."
        ),
        "url": CARA_FAQ_URL,
    },
    {
        "key": "cara_global_religious",
        "scope": "global",
        "year": "2022",
        "value_num": 608_958,
        "headline": (
            "Ordensleute (professed religious) weltweit "
            "(Annuarium 2022): rund 609.000 — davon rund 599.200 "
            "Ordensschwestern und rund 49.700 Ordensbrueder/-priester "
            "(letztere ueberlappen mit Priester-Zahl)."
        ),
        "description": (
            "Ordensschwestern sind seit 1970 (rund 1,0 Mio) deutlich "
            "ruecklaeufig — primaer durch Demographie in Europa und "
            "Nordamerika. CARA weist auf wachsende Kongregationen in "
            "Afrika und Asien hin. Zahl bezieht sich auf 'professed' "
            "religious (mit ewigen Geluebden), nicht auf Novizinnen."
        ),
        "url": CARA_FAQ_URL,
    },
    {
        "key": "cara_us_denomination_ranking",
        "scope": "us",
        "year": "2023",
        "value_num": None,
        "headline": (
            "US-Religionsgemeinschaften nach Mitgliederzahl (CARA / "
            "Pew / U.S. Religion Census 2020): Roemisch-katholische "
            "Kirche ~73 Mio ist die GROESSTE einzelne Denomination "
            "in den USA. Dahinter: Southern Baptist Convention ~13 Mio, "
            "United Methodist Church ~5,7 Mio, National Baptist Convention "
            "~5 Mio, Church of God in Christ ~4,7 Mio, Assemblies of God "
            "~3 Mio. Evangelikale Protestanten sind als GRUPPE (~25 %) "
            "groesser, bestehen aber aus Dutzenden eigenstaendiger "
            "Denominationen."
        ),
        "description": (
            "Die katholische Kirche ist die groesste EINZELNE "
            "Religionsgemeinschaft der USA — nicht die groesste "
            "konfessionelle Tradition (das waere protestantisch "
            "insgesamt ~40 %). CARA-FAQ unterscheidet 'denomination' "
            "vs. 'tradition'. U.S. Religion Census 2020 listet 236 "
            "christliche Denominationen in den USA."
        ),
        "url": CARA_FAQ_URL,
    },
    {
        "key": "cara_us_catholics",
        "scope": "us",
        "year": "2023",
        "value_num": 73_000_000,
        "headline": (
            "US-Katholiken (CARA-Schaetzung 2023): rund 73 Millionen "
            "(ca. 22 % der US-Bevoelkerung). Damit groesste einzelne "
            "Religionsgemeinschaft in den USA."
        ),
        "description": (
            "CARA verwendet Self-Identification (z. B. Pew-Surveys, "
            "GSS, CCD) plus Pfarrei-Listen. Mass Attendance ('weekly "
            "Mass attendance'): nur rund 17-25 % der US-Katholiken "
            "regelmaessig sonntags (CARA 2023). Sakramentsdaten: "
            "Taufen ~615.000 p. a. (Trend abnehmend), "
            "Trauungen ~135.000 p. a. (deutlich abnehmend)."
        ),
        "url": CARA_FAQ_URL,
    },
    {
        "key": "cara_mass_attendance_us",
        "scope": "us",
        "year": "2023",
        "value_num": None,
        "headline": (
            "Mass-Attendance USA (CARA 2023): nur rund 17 % der "
            "US-Katholiken besuchen woechentlich die Messe; weitere "
            "rund 22 % mind. einmal im Monat. 1955 lag der woechentliche "
            "Messbesuch noch bei rund 75 %."
        ),
        "description": (
            "CARA misst Self-Reported Attendance per Survey (CCD, "
            "GSS). Real-Counts an Sonntagen (z. B. Diozesan-Zaehlungen) "
            "liegen typischerweise rund 5-10 Prozentpunkte unter den "
            "Self-Reports. Trend: gleichlaufender Rueckgang in DACH "
            "(AT-Kirchgangsstatistik der Katholischen Kirche "
            "Oesterreich: ca. 7-9 % der Katholiken sonntags 2023)."
        ),
        "url": CARA_FAQ_URL,
    },
    {
        "key": "cara_regional_distribution",
        "scope": "global",
        "year": "2022",
        "value_num": None,
        "headline": (
            "Regionale Verteilung der weltweit 1,39 Mrd Katholiken "
            "(Annuarium 2022): Amerika rund 48 % (Lateinamerika "
            "Schwerpunkt), Europa rund 21 %, Afrika rund 19 %, "
            "Asien rund 11 %, Ozeanien rund 1 %."
        ),
        "description": (
            "Wachstums-Schwerpunkt seit 2000: Afrika (+115 %), "
            "Asien (+44 %). Europa-Anteil sinkt strukturell durch "
            "Demographie + Saekularisierung. CARA weist regelmaessig "
            "auf das 'Demographic-Center-of-Gravity-Shift' der "
            "Welt-Kirche Richtung Globaler Sueden hin."
        ),
        "url": CARA_FAQ_URL,
    },
)


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _de_num(v) -> str:
    if v is None:
        return "?"
    try:
        n = int(v)
    except (TypeError, ValueError):
        return str(v)
    # DE-Tausendertrenner mit Punkt
    return f"{n:,}".replace(",", ".")


def _build_result(fact: dict) -> dict:
    key = fact.get("key") or "cara_fact"
    headline = fact.get("headline") or ""
    description = fact.get("description") or ""
    url = fact.get("url") or CARA_FAQ_URL
    scope = fact.get("scope") or "global"
    year = fact.get("year") or "—"
    value_num = fact.get("value_num")

    country_code = "—"
    country_name = "weltweit"
    if scope == "us":
        country_code = "USA"
        country_name = "Vereinigte Staaten"
    elif scope == "global":
        country_code = "WLD"
        country_name = "weltweit"

    return {
        "indicator_name": f"CARA Catholic Statistics: {key}",
        "indicator": key,
        "country": country_code,
        "country_name": country_name,
        "year": year,
        "value": value_num,
        "display_value": (
            f"{headline} (Stand {year}, Quelle: CARA / "
            f"Annuarium Statisticum Ecclesiae)."
            if year and year != "—"
            else f"{headline} (Quelle: CARA / Annuarium Statisticum Ecclesiae)."
        ),
        "description": description,
        "url": url,
        "source": "CARA (Center for Applied Research in the Apostolate)",
    }


def _select_facts(claim_lc: str) -> list[dict]:
    """Waehle die relevantesten Eckwerte fuer den Claim aus."""
    selected: list[dict] = []

    has_us = _has_any(claim_lc, (
        "usa", "u.s.a.", "us-amerika", "united states",
        "vereinigte staaten", " us ", " us-", "us-katholik",
    ))
    has_global = _has_any(claim_lc, (
        "weltweit", "global", "world", "worldwide",
        "annuarium", "vatikan-statistik", "vatikan statistik",
    ))
    has_priests = _has_any(claim_lc, (
        "priester", "priest", "priests",
    ))
    has_seminarian = _has_any(claim_lc, (
        "seminarist", "seminaristen", "seminarian", "berufung",
        "vocation", "ordensnachwuchs",
    ))
    has_religious = _has_any(claim_lc, (
        "ordensleute", "ordensschwester", "ordensschwestern",
        "ordensbrueder", "ordensbrueder", "religious sister",
        "religious brother", "religious order", "professed religious",
    ))
    has_parish = _has_any(claim_lc, (
        "pfarrei", "pfarreien", "parish", "parishes",
    ))
    has_mass = _has_any(claim_lc, (
        "messbesuch", "kirchgang", "sonntagsmesse", "mass attendance",
        "wochenmesse",
    ))

    for fact in _CARA_FACTS:
        key = fact.get("key", "")
        scope = fact.get("scope", "global")

        # USA-spezifisch: nur wenn USA explizit oder Mass-Attendance erwaehnt
        if scope == "us":
            if has_us or has_mass:
                selected.append(fact)
            continue

        # Globale Eckwerte
        if key == "cara_global_priests" and has_priests:
            selected.append(fact)
            continue
        if key == "cara_global_seminarians" and has_seminarian:
            selected.append(fact)
            continue
        if key == "cara_global_religious" and has_religious:
            selected.append(fact)
            continue
        if key == "cara_global_parishes" and has_parish:
            selected.append(fact)
            continue
        if key == "cara_regional_distribution" and (
            has_global or _has_any(claim_lc, (
                "afrika", "asien", "europa", "lateinamerika", "amerika",
                "verteilung", "anteil", "kontinent",
            ))
        ):
            selected.append(fact)
            continue
        # Global Catholics als Default-Headline immer, wenn nichts
        # spezifischeres gefunden wurde.
        if key == "cara_global_catholics":
            selected.append(fact)

    # Deduplicate (preserve order), cap at MAX_RESULTS
    seen: set[str] = set()
    out: list[dict] = []
    for f in selected:
        k = f.get("key", "")
        if k in seen:
            continue
        seen.add(k)
        out.append(f)
        if len(out) >= MAX_RESULTS:
            break

    # Fallback: wenn nichts gematcht hat (sollte nicht passieren, da
    # Trigger schon gefiltert hat), liefere wenigstens den
    # Global-Catholics-Headline.
    if not out:
        for f in _CARA_FACTS:
            if f.get("key") == "cara_global_catholics":
                out.append(f)
                break

    return out


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_cara(analysis: dict) -> dict:
    """Lookup gegen kuratierte CARA-/Annuarium-Eckwerte.

    Returns Dict mit <= MAX_RESULTS Treffern. Bei Trigger-Miss: leere
    results-Liste (graceful fail).

    Politische Guardrails: Pure deskriptive Konfessions-Statistik,
    keines der 4 Polit-Tabus beruehrt.
    """
    empty = {
        "source": "CARA",
        "type": "catholic_statistics",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or ""
    original = analysis.get("original_claim") or claim
    if not isinstance(claim, str):
        claim = str(claim or "")
    if not isinstance(original, str):
        original = str(original or "")

    matchable = f"{original} {claim}".lower().strip()
    if not _claim_mentions_cara(matchable):
        return empty

    # 24-h-In-Memory-Cache: Schluessel ist gehashter claim_lc
    cache_key = matchable
    now = time.time()
    cached = _search_cache.get(cache_key)
    if cached is not None:
        ts, payload = cached
        if (now - ts) < CACHE_TTL_S:
            return payload

    try:
        facts = _select_facts(matchable)
    except Exception as e:
        logger.debug(f"CARA: select-error: {e}")
        return empty

    if not facts:
        _search_cache[cache_key] = (now, empty)
        return empty

    results: list[dict] = []
    for f in facts[:MAX_RESULTS]:
        try:
            built = _build_result(f)
        except Exception as e:
            logger.debug(f"CARA: build-error: {e}")
            continue
        if built:
            results.append(built)

    payload = {
        "source": "CARA",
        "type": "catholic_statistics",
        "results": results,
    }
    _search_cache[cache_key] = (now, payload)
    logger.info(
        f"CARA: {len(results)} Eckwert-Treffer "
        f"(keys={[r['indicator'] for r in results]})"
    )
    return payload
