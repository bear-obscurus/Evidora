"""DACH Factbook — kuratierte deutsche und schweizerische Faktoide für
Boulevardpresse-/Politik-Behauptungen aus DE und CH.

Komplementär zum österreichischen `at_factbook` — gleiche Static-First-
Architektur, aber explizit für DE-/CH-Themen, die Evidora-User in
Faktenchecks abfragen.

Themenschwerpunkte v1:
- BAMF Asylstatistik Deutschland (2025-2026, Q1-Bilanz)
- Bürgergeld Deutschland (Regelsätze + Falschmeldungs-Counter)
- Heizungsgesetz Deutschland (GEG-Reform 2026 + Bild-Falschmeldung Counter)
- Schweizer AHV / Ergänzungsleistungen / Frauen-Rentenalter
- CORRECTIV-Faktencheck-Counter:
  * Meeresspiegel Venedig (AfD Klauß)
  * Asylbewerber Gesundheitsleistungen (AfD Schattner)
  * Klima-Skeptizismus 'Frost = kein Klimawandel'

Bewusste Architektur-Entscheidung: Separater Service statt at_factbook-
Erweiterung, weil at_factbook bereits 17 Topics + 2k LOC hat und die
DE-/CH-Daten thematisch klar abgegrenzt sind.

GUARDRAILS:
- Wir kennzeichnen Quellen-Land im Faktoid (DE, CH).
- Wir verwenden CORRECTIV/dpa/Faktencheck-Konsens für DE-spezifische
  Falschmeldungs-Counter.
- AT-Themen bleiben weiter im at_factbook.
"""

import json
import logging
import os

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "dach_factbook.json",
)

_cache: dict | None = None


# ---------------------------------------------------------------------------
# DACH-Kontext
# ---------------------------------------------------------------------------
_DE_CONTEXT_TERMS = (
    "deutschland", "germany", "deutsch ", "deutsche",
    "berlin", "münchen", "hamburg", "köln", "frankfurt",
    "bamf", "bürgergeld", "buergergeld", "habeck", "scholz",
    "bundestag", "bundesregierung deutschland",
    "afd", "spd", "cdu", "csu", "grüne deutschland", "fdp",
)
_CH_CONTEXT_TERMS = (
    "schweiz", "switzerland", "schweizer", "swiss",
    "zürich", "bern", "basel", "genf", "lausanne",
    "ahv", "iv-rente", "iv rente", "ergänzungsleistungen",
    "bundesrat schweiz", "schweizer bundesrat",
    "svp", "sp schweiz", "fdp schweiz", "mitte",
)


def _has_de_context(claim_lc: str) -> bool:
    return any(t in claim_lc for t in _DE_CONTEXT_TERMS)


def _has_ch_context(claim_lc: str) -> bool:
    return any(t in claim_lc for t in _CH_CONTEXT_TERMS)


# ---------------------------------------------------------------------------
# Topic-Trigger
# ---------------------------------------------------------------------------
_BAMF_TERMS = (
    "asylanträge deutschland", "asyl deutschland",
    "bamf", "asylantrag deutschland",
    "erstanträge deutschland", "asylerstantrag deutschland",
    "geflüchtete deutschland", "asylsuchende deutschland",
    "168.543", "168543", "22.491", "22491",
    "afghanistan syrien türkei",
)


def _claim_mentions_bamf(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _BAMF_TERMS)
    if has_term:
        return True
    has_asyl = any(t in claim_lc for t in ("asyl", "asylum"))
    if has_asyl and _has_de_context(claim_lc):
        return True
    return False


_BUERGERGELD_TERMS = (
    "bürgergeld", "buergergeld",
    "hartz iv", "hartz 4", "hartz-iv",
    "regelsatz",
    "grundsicherungsgeld",
    "563 euro", "1.012 euro",
    "bürgergeld vs", "bürgergeld mehr als",
    "niedriglohn",
)


def _claim_mentions_buergergeld(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _BUERGERGELD_TERMS)
    if has_term:
        return True
    return False


_HEIZUNG_TERMS = (
    "heizungsgesetz",
    "gebäudeenergiegesetz", "geg",
    "gebäudemodernisierungsgesetz",
    "habeck heizung", "habeck wohn",
    "65-prozent-regel", "65 prozent regel",
    "wohn-hammer", "heizhammer",
    "1 billion heizung", "billion heizung",
)


def _claim_mentions_heizung(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _HEIZUNG_TERMS)
    if has_term:
        return True
    return False


_AHV_TERMS = (
    "ahv ", " ahv", "13. ahv", "13 ahv",
    "ahv-rente", "ahv rente",
    "ergänzungsleistungen schweiz", "ergaenzungsleistungen schweiz",
    "schweizer altersvorsorge",
    "frauen rentenalter schweiz", "rentenalter 65 schweiz",
    "iv-schlupfloch", "ahv-schlupfloch",
)


def _claim_mentions_ahv(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _AHV_TERMS)
    if has_term:
        return True
    has_pension = any(t in claim_lc for t in (
        "rente", "rentenalter", "altersvorsorge", "pensionsalter",
        "frauen 65", "bis 65 arbeiten", "frauen bis 65",
    ))
    if has_pension and _has_ch_context(claim_lc):
        return True
    # Composite: 'frauen' + 'schweiz' + ('65' oder 'rentenalter' oder 'arbeiten')
    has_frauen = "frauen" in claim_lc
    has_age = any(t in claim_lc for t in (
        "65", "64", "rentenalter", "pensionsalter",
        "arbeiten", "anhebung",
    ))
    if has_frauen and has_age and _has_ch_context(claim_lc):
        return True
    return False


_VENEDIG_TERMS = (
    "meeresspiegel venedig",
    "venedig 1500 jahre", "venedig 1.500 jahre",
    "klauß meeresspiegel", "klauss meeresspiegel",
    "venedig unverändert",
)


def _claim_mentions_venedig(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _VENEDIG_TERMS)
    if has_term:
        return True
    has_venedig = "venedig" in claim_lc or "venice" in claim_lc
    has_klima = any(t in claim_lc for t in (
        "meeresspiegel", "sea level", "klimawandel", "climate change",
        "1500 jahr", "1.500 jahr", "unverändert",
    ))
    return has_venedig and has_klima


_KLIMASKEPSIS_TERMS = (
    "klimawandel ist nicht", "klimawandel nicht real",
    "klimawandel schwindel", "klimawandel lüge",
    "klimawandel erfunden", "klimawandel panikmache",
    "kein klimawandel weil",
    "kalter winter klima", "frost klima",
    "klimawandel war früher genauso",
    "klimawandel pausiert", "klimawandel-pause",
    "globale temperatur steigt nicht",
    "seit 1998 nicht mehr",
    "hiatus klima", "klima-hiatus",
    "erwärmung pausiert", "global warming hiatus",
    # Skeptical Science Top-Argumente
    "erwärmung kommt von der sonne", "sonne erwärmung",
    "klima hat sich schon immer geändert",
    "co2 ist nur ein spurengas", "spurengas co2",
    "kein wissenschaftlicher konsens klima",
    "hockeystick gefälscht", "hockey-stick gefälscht",
    "klimamodelle sind falsch", "klimamodelle ungenau",
    "antarktis eis wächst", "antarktis eis nimmt zu",
    "mittelalterliche warmzeit", "warmzeit wärmer",
    "co2 folgt temperatur", "wasserdampf treibhausgas",
)


def _claim_mentions_klimaskepsis_basic(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _KLIMASKEPSIS_TERMS)
    if has_term:
        return True
    # Composite: 'klimawandel' + ('nicht' oder 'frost' oder 'kalter winter' oder 'erfunden')
    has_klima = any(t in claim_lc for t in (
        "klimawandel", "klima-wandel", "global warming",
        "globale temperatur", "globale erwärmung",
        "kein klimawandel",
    ))
    has_skeptik = any(t in claim_lc for t in (
        "nicht real", "schwindel", "erfunden", "panikmache",
        "frost", "kalter winter", "kalter monat",
        "war früher", "schon immer",
        "pausiert", "pause", "steigt nicht mehr",
        "seit 1998", "hiatus",
    ))
    if has_klima and has_skeptik:
        return True
    # Skeptical-Science-Argumente: Hockeystick + Antarktis-Eis als
    # eigenständige Klima-Skeptiker-Marker (auch ohne 'klimawandel'-Wort)
    if "hockeystick" in claim_lc or "hockey-stick" in claim_lc:
        return True
    if "antarktis" in claim_lc and "eis" in claim_lc and (
        "wächst" in claim_lc or "nimmt zu" in claim_lc or "zunimmt" in claim_lc
    ):
        return True
    if "mittelalterliche warmzeit" in claim_lc:
        return True
    if "co2 folgt temperatur" in claim_lc or (
        "co2" in claim_lc and "folgt" in claim_lc and "temperatur" in claim_lc
    ):
        return True
    return False


_DACH_ASYL_VERGLEICH_TERMS = (
    "asylanträge dach", "asyl deutschland österreich schweiz",
    "deutschland asyl höher",
    "asyl at + ch", "asyl at und ch",
    "deutschland mehr asyl",
    "asyl dach vergleich",
)


def _claim_mentions_dach_asyl(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _DACH_ASYL_VERGLEICH_TERMS)
    if has_term:
        return True
    # Composite: 'asyl' + 2 von 3 DACH-Ländern + 'höher' / 'zusammen'
    has_asyl = any(t in claim_lc for t in ("asyl", "asylantrag", "asylanträge"))
    if not has_asyl:
        return False
    de_mentioned = any(t in claim_lc for t in ("deutschland", "germany"))
    at_mentioned = "österreich" in claim_lc or "austria" in claim_lc
    ch_mentioned = "schweiz" in claim_lc or "switzerland" in claim_lc
    has_compare = any(t in claim_lc for t in (
        "höher als", "mehr als", "zusammen", "summe", "kombiniert",
        "im vergleich",
    ))
    if (de_mentioned + at_mentioned + ch_mentioned) >= 2 and has_compare:
        return True
    return False


_DACH_PENSIONS_TERMS = (
    "dach pension", "dach rente",
    "pensionsantrittsalter dach",
    "rentenalter dach", "pensionsalter dach",
    "alle drei länder rente",
    "alle drei dach", "alle dach länder",
    "österreich deutschland schweiz pension",
    "österreich deutschland schweiz rente",
)


def _claim_mentions_dach_pensions(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _DACH_PENSIONS_TERMS)
    if has_term:
        return True
    has_pension = any(t in claim_lc for t in (
        "pension", "rente", "rentenalter", "pensionsalter",
        "antrittsalter",
    ))
    if not has_pension:
        return False
    de_mentioned = any(t in claim_lc for t in ("deutschland", "germany"))
    at_mentioned = "österreich" in claim_lc or "austria" in claim_lc
    ch_mentioned = "schweiz" in claim_lc or "switzerland" in claim_lc
    has_dach = any(t in claim_lc for t in ("dach", "drei länder", "alle drei"))
    if has_dach or (de_mentioned + at_mentioned + ch_mentioned) >= 2:
        return True
    return False


_EU_BESCHLUESSE_TERMS = (
    "eu ai act", "ai act", "eu-ki-verordnung", "ki-verordnung eu",
    "eu-taxonomie", "eu taxonomie",
    "atomstrom eu-taxonomie", "atomstrom nachhaltig",
    "csddd", "lieferketten-richtlinie", "eu-lieferketten",
    "lieferkettengesetz eu",
    "recht auf reparatur", "eu-reparatur",
    "eu-plastiksteuer", "plastiksteuer eu",
    "eu-methan", "methan-verordnung",
    "eu-renaturierung", "renaturierungsverordnung",
    "nature restoration law",
    "eu-verordnung 2024", "eu-richtlinie 2024",
)


def _claim_mentions_eu_beschluesse(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _EU_BESCHLUESSE_TERMS)
    if has_term:
        return True
    # Composite: 'EU' + Verordnung/Richtlinie + 2024/2025/2026
    has_eu = any(t in claim_lc for t in ("eu-verordnung", "eu-richtlinie",
                                          "europäische verordnung",
                                          "europäische richtlinie"))
    if has_eu:
        return True
    return False


_DACH_HPV_TERMS = (
    "hpv-impfrate", "hpv impfrate",
    "hpv-impfquote", "hpv impfquote",
    "impfrate dach",
    "hpv österreich deutschland",
    "hpv schweiz österreich",
)


def _claim_mentions_dach_hpv(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _DACH_HPV_TERMS)
    if has_term:
        return True
    has_hpv = "hpv" in claim_lc
    if not has_hpv:
        return False
    de_mentioned = any(t in claim_lc for t in ("deutschland", "germany"))
    at_mentioned = "österreich" in claim_lc or "austria" in claim_lc
    ch_mentioned = "schweiz" in claim_lc or "switzerland" in claim_lc
    has_compare = any(t in claim_lc for t in (
        "deutlich vor", "vor deutschland", "vor schweiz",
        "höher als", "vergleich",
    ))
    if (de_mentioned + at_mentioned + ch_mentioned) >= 2 and has_compare:
        return True
    return False


_ASYLBEWERBER_GESUNDHEIT_TERMS = (
    "asylbewerber gesundheitsleistungen",
    "asylbewerber krankenversicherung",
    "asylsuchende sofort versichert",
    "asylblg", "asylbewerberleistungsgesetz",
    "asylbewerber alle leistungen",
    "asylbewerber sofort gesundheit",
    "schattner asylbewerber",
)


def _claim_mentions_asylbewerber_gesundheit(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _ASYLBEWERBER_GESUNDHEIT_TERMS)
    if has_term:
        return True
    # Composite: 'asylbewerber' / 'asylsuchend' + 'gesundheit'/'krankenversicherung' + DE-Kontext
    has_asyl = any(t in claim_lc for t in ("asylbewerber", "asylsuchende", "asylsuchender"))
    has_med = any(t in claim_lc for t in (
        "gesundheitsleistung", "krankenversicherung", "arzt", "behandlung",
        "medizinisch", "krankenkasse",
    ))
    if has_asyl and has_med and _has_de_context(claim_lc):
        return True
    return False


# ---------------------------------------------------------------------------
# Public trigger
# ---------------------------------------------------------------------------
def _claim_matches_any_topic(claim: str) -> list[str]:
    if not claim:
        return []
    cl = claim.lower()
    matched: list[str] = []
    if _claim_mentions_bamf(cl):
        matched.append("bamf_asyl_de")
    if _claim_mentions_buergergeld(cl):
        matched.append("buergergeld_de")
    if _claim_mentions_heizung(cl):
        matched.append("heizungsgesetz_de")
    if _claim_mentions_ahv(cl):
        matched.append("ahv_ch")
    if _claim_mentions_venedig(cl):
        matched.append("afd_venedig_meeresspiegel_counter")
    if _claim_mentions_klimaskepsis_basic(cl):
        matched.append("klimaskepsis_counter")
    if _claim_mentions_asylbewerber_gesundheit(cl):
        matched.append("asylbewerber_gesundheit_de_counter")
    if _claim_mentions_dach_asyl(cl):
        matched.append("dach_asyl_vergleich")
    if _claim_mentions_dach_pensions(cl):
        matched.append("dach_pensions_vergleich")
    if _claim_mentions_dach_hpv(cl):
        matched.append("dach_hpv_impfraten")
    if _claim_mentions_eu_beschluesse(cl):
        matched.append("eu_schluessel_beschluesse")
    return matched


def claim_mentions_dach_factbook_cached(claim: str) -> bool:
    return bool(_claim_matches_any_topic(claim))


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
        if not isinstance(data, dict) or "facts" not in data:
            logger.warning("dach_factbook.json missing 'facts' key")
            return None
        _cache = data
        logger.info(f"DACH-Factbook loaded: {len(data['facts'])} curated entries")
        return _cache
    except FileNotFoundError:
        logger.warning(f"dach_factbook.json not found at {STATIC_JSON_PATH}")
        return None
    except Exception as e:
        logger.warning(f"dach_factbook.json load failed: {e}")
        return None


async def fetch_dach_factbook(client=None):
    data = _load_static_json()
    if not data:
        return []
    return data.get("facts") or []


# ---------------------------------------------------------------------------
# Helper-Formatierer
# ---------------------------------------------------------------------------
def _de_int(v) -> str:
    if v is None:
        return "?"
    try:
        return f"{int(v):,}".replace(",", ".")
    except Exception:
        return str(v)


def _de_pct(v) -> str:
    if v is None:
        return "?"
    return f"{v}".replace(".", ",")


# ---------------------------------------------------------------------------
# Result-Builder pro Topic
# ---------------------------------------------------------------------------
def _build_bamf_results(fact: dict, claim_lc: str) -> list[dict]:
    data = fact.get("data") or {}
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "BAMF"
    top3 = data.get("top_3_herkunftslaender_q1_2026") or []
    top3_str = ", ".join(f"{e['land']} ({_de_int(e['antraege'])})" for e in top3)

    headline = (
        f"BAMF Asylstatistik DE: 2025 = {_de_int(data.get('asylantraege_gesamt_2025'))} Anträge "
        f"(2024: {_de_int(data.get('asylantraege_gesamt_2024'))}, 2023: "
        f"{_de_int(data.get('asylantraege_gesamt_2023'))}). Q1 2026: "
        f"{_de_int(data.get('asylantraege_q1_2026_erstantraege'))} Erstanträge "
        f"+ {_de_int(data.get('asylantraege_q1_2026_folgeantraege'))} Folgeanträge "
        f"= {_de_int(data.get('asylantraege_q1_2026_gesamt'))} gesamt. "
        f"Erstanträge -{_de_pct(abs(data.get('rueckgang_q1_yoy_pct') or 0))} % YoY. "
        f"Top-3 Herkunft Q1 2026: {top3_str}."
    )

    description_parts = []
    for note in fact.get("context_notes") or []:
        description_parts.append(note)

    return [{
        "indicator_name": "BAMF Asylstatistik Deutschland 2025-2026",
        "indicator": "dach_bamf_asyl",
        "country": "DEU", "country_name": "Deutschland",
        "year": "2025-2026",
        "value": data.get("asylantraege_gesamt_2025"),
        "display_value": headline,
        "description": " ".join(description_parts),
        "url": src, "source": label,
    }]


def _build_buergergeld_results(fact: dict, claim_lc: str) -> list[dict]:
    data = fact.get("data") or {}
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "DGB / ifo Institut"
    vergleich = data.get("buergergeld_vs_niedriglohn_2026") or {}

    results: list[dict] = []

    # Hauptzeile: Regelsätze
    results.append({
        "indicator_name": "Bürgergeld Regelsätze Deutschland 2026",
        "indicator": "dach_buergergeld",
        "country": "DEU", "country_name": "Deutschland",
        "year": "2026",
        "value": data.get("regelsatz_alleinstehend_eur_2026"),
        "display_value": (
            f"Bürgergeld-Regelsatz Deutschland 2026: Alleinstehend "
            f"{data.get('regelsatz_alleinstehend_eur_2026')} EUR/Monat, "
            f"Paar {data.get('regelsatz_paar_eur_2026')} EUR/Monat, "
            f"Kinder {data.get('kindersatz_min_eur')}–{data.get('kindersatz_max_eur')} EUR. "
            f"Eine Behauptung von '563 EUR Bürgergeld' ist faktisch korrekt."
        ),
        "description": " ".join(fact.get("context_notes") or []),
        "url": src, "source": label,
    })

    # Wenn Claim "Bürgergeld vs. Niedriglohn" ansprcht → Spezial-Counter
    if any(s in claim_lc for s in (
        "mehr als niedriglohn", "mehr als arbeit",
        "lohnt sich nicht", "niedriglohnempfänger",
        "vollzeit weniger", "arbeit lohnt nicht",
    )):
        results.insert(0, {
            "indicator_name": "Bürgergeld vs. Niedriglohn — direkter Counter",
            "indicator": "dach_buergergeld_counter",
            "country": "DEU", "country_name": "Deutschland",
            "year": "2026",
            "display_value": (
                f"STRUKTURELL FALSCH (Bild-Mythos): "
                f"Vollzeit-Niedriglohn 38h netto = "
                f"{vergleich.get('vollzeit_niedriglohn_38h_netto_eur_pro_monat')} EUR/Monat, "
                f"Bürgergeld inkl. Wohngeld = "
                f"{vergleich.get('buergergeld_alleinstehend_inkl_wohngeld_eur_pro_monat')} EUR/Monat. "
                f"Differenz = +{vergleich.get('differenz_eur_pro_monat')} EUR pro Monat zugunsten Niedriglohn. "
                f"VERDICT-EMPFEHLUNG: 'false' mit Confidence 0.9. "
                f"Quelle: ifo-Institut + DGB-Berechnung."
            ),
            "description": (
                "Diese Behauptung ist eine der häufigsten Falschmeldungen "
                "in der deutschen Boulevardpresse (Bild). DGB-Berechnung "
                "und ifo-Statement widerlegen sie eindeutig: ein Vollzeit-"
                "Niedriglohnempfänger erhält rund 520 EUR netto pro Monat "
                "MEHR als ein Bürgergeld-Empfänger inklusive Wohngeld."
            ),
            "url": src, "source": label,
        })

    return results


def _build_heizung_results(fact: dict, claim_lc: str) -> list[dict]:
    data = fact.get("data") or {}
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "CORRECTIV / RWI"

    results: list[dict] = []

    # Default-Eintrag
    results.append({
        "indicator_name": "Heizungsgesetz Deutschland 2026",
        "indicator": "dach_heizungsgesetz",
        "country": "DEU", "country_name": "Deutschland",
        "year": "2026",
        "display_value": (
            f"Deutsches Heizungsgesetz: Reform-Eckpunkte "
            f"{data.get('reform_eckpunkte_beschluss')}. "
            f"65-Prozent-Regel ABGESCHAFFT. Umbenennung in '"
            f"{data.get('offizieller_name')}'. "
            f"Tatsächliche Kostenschätzung der Bundesregierung bis 2050: "
            f"~{data.get('tatsaechliche_kostenschaetzung_de_approx_mrd_eur')} Mrd EUR — "
            f"NICHT 1.000 Mrd EUR (Bild-Falschmeldung)."
        ),
        "description": " ".join(fact.get("context_notes") or []),
        "url": src, "source": label,
    })

    # Counter-Eintrag wenn '1 Billion' / 'Habecks Wohn-Hammer'
    if any(s in claim_lc for s in (
        "1 billion", "1.000 milliarden", "1000 milliarden",
        "wohn-hammer", "wohnhammer", "habeck", "billion euro",
    )):
        results.insert(0, {
            "indicator_name": "Bild-Falschmeldung '1 Billion Heizungsgesetz' — direkter Counter",
            "indicator": "dach_heizung_counter",
            "country": "DEU", "country_name": "Deutschland",
            "year": "2026",
            "display_value": (
                "STRUKTURELL FALSCH (Bild-Schlagzeile 2024 / verbreitet 2024-2026): "
                "'Habecks Heizungsgesetz kostet 1 Billion Euro / 1.000 Milliarden Euro'. "
                "Das RWI und der zitierte Forscher haben sich dezidiert von der "
                "zugeschriebenen Berechnung DISTANZIERT — die 1-Billion-Zahl ist "
                "NICHT vom RWI berechnet. Tatsächliche Kostenschätzung der "
                "Bundesregierung über 25 Jahre (2024-2050): ca. 200 Mrd EUR. "
                "VERDICT-EMPFEHLUNG: 'false' mit Confidence 0.9."
            ),
            "description": (
                "Quelle: CORRECTIV-Faktencheck Heizungsreform 02/2026 + taz-"
                "Artikel 'Habeck kostet uns keine Billion'. Die Bild hat "
                "die Zahl ohne Rückfrage beim RWI verbreitet."
            ),
            "url": src, "source": label,
        })

    return results


def _build_ahv_results(fact: dict, claim_lc: str) -> list[dict]:
    data = fact.get("data") or {}
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "Schweizer Bundesrat"
    el = data.get("ergaenzungsleistungen_ohne_iv_ahv_anspruch_ende_2024") or {}
    migration_studie = data.get("migration_entlastet_ahv_studie_2023") or {}

    results: list[dict] = []

    # Hauptzeile
    results.append({
        "indicator_name": "Schweizer AHV-Reform und Ergänzungsleistungen 2026",
        "indicator": "dach_ch_ahv",
        "country": "CHE", "country_name": "Schweiz",
        "year": "2026",
        "display_value": (
            f"Schweizer AHV 2026: "
            f"13. AHV-Rente Erstauszahlung {data.get('dreizehnte_ahv_rente_erstauszahlung')} "
            f"(Referendum {data.get('dreizehnte_ahv_rente_referendum')}). "
            f"Frauen-Rentenalter steigt seit {data.get('frauen_rentenalter_anhebung_seit')} "
            f"schrittweise um {data.get('frauen_rentenalter_anhebung_pro_jahr_monate')} "
            f"Monate/Jahr auf 65 (vollständig {data.get('frauen_rentenalter_voll_implementiert')})."
        ),
        "description": " ".join(fact.get("context_notes") or []),
        "url": src, "source": label,
    })

    # Spezial: Ergänzungsleistungen-Topic
    if any(s in claim_lc for s in (
        "ergänzungsleistungen", "ergaenzungsleistungen",
        "1.420", "1420", "el ohne",
        "ahv-schlupfloch", "schlupfloch",
    )):
        results.insert(0, {
            "indicator_name": "Schweiz Ergänzungsleistungen ohne IV/AHV-Anspruch (Ende 2024)",
            "indicator": "dach_ch_el_counter",
            "country": "CHE", "country_name": "Schweiz",
            "year": "2024",
            "display_value": (
                f"Schweizer Bundesrat 2025: {el.get('anzahl_personen')} ausländische "
                f"Personen erhielten Ende 2024 Ergänzungsleistungen ohne IV/AHV-"
                f"Grundanspruch — davon {el.get('davon_eu_efta')} aus EU/EFTA-Staaten. "
                f"Gesamt-Summe: {el.get('gesamt_summe_chf_mio')} Millionen CHF. "
                f"Quelle: {el.get('quelle')}."
            ),
            "description": (
                "Die Behauptung von 1.420 Personen ist faktisch korrekt — "
                "Bundesrat-Antwort auf SVP-Anfrage von Pascal Schmid. "
                "Verdict 'true' mit Confidence 0.9."
            ),
            "url": src, "source": label,
        })

    # Spezial: Frauen-65-Anhebung
    if any(s in claim_lc for s in (
        "frauen 65", "rentenalter 65", "frauen rentenalter",
        "frauen bis 65", "frauen pensionsalter",
        "ahv-reform 65", "ahv reform frauen",
        "drei monate pro jahr", "3 monate pro jahr",
    )):
        results.insert(0, {
            "indicator_name": "Schweiz Frauen-Rentenalter-Anhebung 65 (Sub-Eintrag)",
            "indicator": "dach_ch_frauen_65",
            "country": "CHE", "country_name": "Schweiz",
            "year": "2025-2028",
            "display_value": (
                f"Schweizer AHV-Reform: Frauen-Rentenalter steigt seit "
                f"{data.get('frauen_rentenalter_anhebung_seit')} schrittweise "
                f"um {data.get('frauen_rentenalter_anhebung_pro_jahr_monate')} "
                f"Monate pro Jahr — vollständig auf "
                f"{data.get('frauen_rentenalter_neu')} Jahre ab "
                f"{data.get('frauen_rentenalter_voll_implementiert')}. "
                f"Eine Behauptung 'Frauen müssen ab 2028 bis 65 arbeiten — "
                f"Anhebung seit 2025, 3 Monate/Jahr' ist faktisch korrekt — "
                f"verdict 'true' @ 0.95."
            ),
            "description": (
                "AHV-Reform 2022 vom Schweizer Stimmvolk angenommen. "
                "Übergangsregelung: 1961 = +3 Monate (= 64 + 3), 1962 = +6 "
                "Monate, ..., 1964 ff. = volles Rentenalter 65. Damit ist "
                "ab Jahrgang 1964 (also 2028) die Anhebung vollständig "
                "implementiert."
            ),
            "url": src, "source": label,
        })

    # Spezial: Migration-AHV
    if any(s in claim_lc for s in ("migration ahv", "migranten ahv",
                                     "zuwanderung ahv", "ausländer ahv")):
        results.insert(0, {
            "indicator_name": "Migration und AHV-Schweiz — BSV-Studie 2023",
            "indicator": "dach_ch_migration_ahv",
            "country": "CHE", "country_name": "Schweiz",
            "year": "2023",
            "display_value": (
                f"BSV-Studie 2023: {migration_studie.get('befund')}. "
                f"Eine Behauptung 'Migration entlastet die AHV langfristig' ist "
                f"mostly_true mit Nuance — der Effekt ist real, aber moderat über "
                f"30 Jahre und reicht nicht für die strukturelle Finanzierungslücke."
            ),
            "description": migration_studie.get("quelle", ""),
            "url": src, "source": label,
        })

    return results


def _build_venedig_counter_results(fact: dict, claim_lc: str) -> list[dict]:
    data = fact.get("data") or {}
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "IPCC AR6 + ISPRA"

    headline = (
        f"STRUKTURELL FALSCH (AfD-Klauß): 'Meeresspiegel Venedig 1.500 Jahre unverändert'. "
        f"Globaler Anstieg laut IPCC AR6: ~{data.get('ipcc_ar6_globaler_anstieg_1901_2018_cm')} cm "
        f"zwischen 1901 und 2018. "
        f"Venedig-Pegel-Anstieg seit 1872: ~{data.get('venedig_relativer_anstieg_seit_1872_cm')} cm "
        f"(ISPRA Italien). Acqua-alta-Häufigkeit seit 1900: "
        f"+{data.get('venedig_acqua_alta_haeufigkeit_anstieg_seit_1900_pct')} %."
    )

    return [{
        "indicator_name": "Meeresspiegel Venedig — IPCC + ISPRA-Counter",
        "indicator": "dach_venedig_counter",
        "country": "ITA", "country_name": "Italien",
        "year": "1901-2018",
        "display_value": headline,
        "description": " ".join(fact.get("context_notes") or []),
        "url": src, "source": label,
    }]


def _build_klimaskepsis_counter_results(fact: dict, claim_lc: str) -> list[dict]:
    data = fact.get("data") or {}
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "WMO + Copernicus"

    results: list[dict] = []

    # Skeptical Science Top-Argumente: Trigger-Mapping
    # Single-keyword-Trigger (Substring) ODER Composite-Trigger
    # (mehrere Keywords müssen ALLE in claim_lc enthalten sein).
    skeptical_args = data.get("skeptical_science_top_arguments") or []
    arg_triggers = [
        # (matcher_func, arg_id)
        (lambda c: "sonne" in c and ("erwärmung" in c or "klimawandel" in c),
         "sun_caused_warming"),
        (lambda c: ("klima" in c or "klimawandel" in c) and ("schon immer" in c or "früher genauso" in c),
         "climate_always_changed"),
        (lambda c: "spurengas" in c or "0,04 prozent" in c or "0.04 prozent" in c,
         "co2_only_trace_gas"),
        (lambda c: "konsens" in c and ("klima" in c or "wissenschaft" in c),
         "no_consensus"),
        (lambda c: "hockeystick" in c or "hockey-stick" in c or "hockey stick" in c,
         "hockey_stick_fake"),
        (lambda c: ("klimamodell" in c) and ("falsch" in c or "ungenau" in c
                                              or "übertreib" in c),
         "models_wrong"),
        (lambda c: "antarktis" in c and ("eis" in c) and ("wächst" in c
                                                          or "nimmt zu" in c
                                                          or "zunimmt" in c),
         "antarctic_ice_growing"),
        (lambda c: "mittelalterliche warmzeit" in c or "warmzeit" in c and "wärmer" in c,
         "medieval_warm_period"),
        (lambda c: "co2 folgt" in c or ("co2" in c and "folgt temperatur" in c),
         "co2_lags_temperature"),
        (lambda c: "wasserdampf" in c and ("wichtiger" in c or "treibhausgas" in c),
         "water_vapor_more_important"),
    ]
    matched_args = []
    matched_ids = set()
    for matcher, arg_id in arg_triggers:
        try:
            if matcher(claim_lc) and arg_id not in matched_ids:
                for arg in skeptical_args:
                    if arg.get("id") == arg_id:
                        matched_args.append(arg)
                        matched_ids.add(arg_id)
                        break
        except Exception:
            continue
    for arg in matched_args:
        results.append({
            "indicator_name": f"Skeptical-Science-Counter: {arg.get('id', '')}",
            "indicator": "skeptical_science_counter",
            "country": "WLD", "country_name": "Welt (Klimawissenschaft)",
            "year": "2024",
            "display_value": (
                f"STRUKTURELL FALSCH (Klima-Skeptiker-Argument): "
                f"'{arg.get('behauptung', '')}'. "
                f"Counter: {arg.get('counter', '')} "
                f"VERDICT-EMPFEHLUNG: 'false' mit Confidence 0.9-0.95."
            ),
            "description": f"Quelle: {arg.get('quelle', '')}",
            "url": "https://skepticalscience.com/argument.php",
            "source": "Skeptical Science + IPCC AR6",
        })

    # Hiatus-Special wenn Claim '1998' / 'pausiert' / 'hiatus' nennt
    hiatus = data.get("hiatus_mythos_1998") or {}
    if any(s in claim_lc for s in (
        "1998", "pausiert", "pause", "hiatus",
        "steigt nicht mehr", "stagniert",
    )):
        results.append({
            "indicator_name": "Klima-Hiatus 1998 — direkter Counter",
            "indicator": "dach_klimaskepsis_counter",
            "country": "WLD", "country_name": "Welt",
            "year": "1998-2024",
            "display_value": (
                f"STRUKTURELL FALSCH (Klima-Hiatus-Mythos): "
                f"'{hiatus.get('behauptung', 'Globale Temperatur steigt seit 1998 nicht mehr')}'. "
                f"Cherry-Picking — 1998 war ein extremes El-Niño-Jahr (Anomalie). "
                f"Trend seit 1998 = +{hiatus.get('trend_seit_1998_celsius_pro_dekade')} °C/Dekade "
                f"(NASA GISS). 12 der 13 wärmsten Jahre seit Messbeginn liegen NACH 1998. "
                f"IPCC AR6 (2023): 'Es gibt keinen Beleg für eine globale Erwärmungspause.' "
                f"VERDICT-EMPFEHLUNG: 'false' mit Confidence 0.9–0.95."
            ),
            "description": (
                f"{hiatus.get('befund_nach_2010', '')} "
                f"Quellen: Karl et al. 2015 (Science), Hausfather et al. 2017 "
                f"(Science Advances), IPCC AR6 (2023)."
            ),
            "url": src, "source": label,
        })

    # Standard Wetter-vs-Klima-Eintrag
    headline = (
        f"STRUKTURELL FALSCH (Klima-Skeptiker-Klassiker): "
        f"'Klimawandel nicht real wegen kaltem Winter / Frost'. "
        f"Wetter ≠ Klima — WMO-Definition: Klima ist Statistik des Wetters über "
        f"min. 30 Jahre (Referenz {data.get('wmo_referenzperiode')}). "
        f"Globale Anomalie 2024: +{data.get('globale_temperatur_anomalie_2024_celsius')} °C. "
        f"Alle 10 wärmsten Jahre seit 1880 liegen NACH 2014."
    )

    results.append({
        "indicator_name": "Klima-Skeptizismus — Wetter-vs-Klima-Counter",
        "indicator": "dach_klimaskepsis_counter",
        "country": "WLD", "country_name": "Welt",
        "year": "2024",
        "display_value": headline,
        "description": " ".join(fact.get("context_notes") or []),
        "url": src, "source": label,
    })

    return results


def _build_asylbewerber_gesundheit_counter_results(fact: dict, claim_lc: str) -> list[dict]:
    data = fact.get("data") or {}
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "AsylbLG + CORRECTIV"
    kosten = data.get("haufigste_kostenpunkte_pro_kopf_jahr_eur_approx") or {}

    headline = (
        f"STRUKTURELL FALSCH (AfD-Schattner 04/2026, CORRECTIV widerlegt): "
        f"'Asylbewerber bekommen sofort alle Gesundheitsleistungen.' "
        f"Tatsächlich gewährt § 4 AsylbLG nur EINGESCHRÄNKTE Leistungen "
        f"(akut, Schmerzen, Schwangerschaft) im ersten Jahr. "
        f"Voller GKV-Zugang erst {data.get('voller_gkv_zugang_ab')}. "
        f"Pro-Kopf-Kosten: ~{kosten.get('akute_behandlungen')} EUR/Jahr "
        f"(GKV-Durchschnitt: {kosten.get('vergleich_gkv_durchschnitt')} EUR)."
    )

    return [{
        "indicator_name": "Asylbewerberleistungsgesetz Deutschland — CORRECTIV-Counter",
        "indicator": "dach_asylbLG_counter",
        "country": "DEU", "country_name": "Deutschland",
        "year": "2026",
        "display_value": headline,
        "description": " ".join(fact.get("context_notes") or []),
        "url": src, "source": label,
    }]


# ---------------------------------------------------------------------------
# DACH-Vergleichs-Topics
# ---------------------------------------------------------------------------
def _build_dach_asyl_vergleich_results(fact: dict, claim_lc: str) -> list[dict]:
    data = fact.get("data") or {}
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "BAMF + BMI + SEM"

    headline = (
        f"DACH-Asyl-Vergleich Q1 2026: DE {_de_int(data.get('antraege_de_q1_2026_gesamt'))}, "
        f"AT {_de_int(data.get('antraege_at_q1_2026_gesamt'))}, "
        f"CH ~{_de_int(data.get('antraege_ch_q1_2026_gesamt_approx'))}. "
        f"AT + CH zusammen = {_de_int(data.get('antraege_at_plus_ch_q1_2026_summe'))}. "
        f"DE ist rund 3,9-mal höher als AT + CH zusammen → Behauptung "
        f"'DE höher als AT + CH zusammen' ist FAKTISCH KORREKT."
    )

    return [{
        "indicator_name": "DACH-Asyl-Vergleich Q1 2026",
        "indicator": "dach_asyl_vergleich",
        "country": "DEU/AUT/CHE", "country_name": "DACH-Region",
        "year": "Q1 2026",
        "value": data.get("antraege_de_q1_2026_gesamt"),
        "display_value": headline,
        "description": " ".join(fact.get("context_notes") or []),
        "url": src, "source": label,
    }]


def _build_dach_pensions_vergleich_results(fact: dict, claim_lc: str) -> list[dict]:
    data = fact.get("data") or {}
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "BMF AT + DRV DE + BSV CH"

    headline = (
        f"DACH-Pensionsalter-Reformen 2024-2026: "
        f"AT Korridorpension {data.get('at_korridorpension_alt_jahre')}→"
        f"{data.get('at_korridorpension_neu_jahre')} Jahre ab "
        f"{data.get('at_korridorpension_in_kraft_ab')}; "
        f"CH Frauen {data.get('ch_frauen_alt_jahre')}→"
        f"{data.get('ch_frauen_neu_jahre')} Jahre ab "
        f"{data.get('ch_frauen_in_kraft_ab')}; "
        f"DE laufende Anhebung Regelaltersgrenze "
        f"{data.get('de_regelaltersgrenze_2024')}→"
        f"{data.get('de_regelaltersgrenze_2031')} (bereits 2007 beschlossen, "
        f"2024-2026 wirksam aber kein neuer Reform-Akt). "
        f"VERDICT-EMPFEHLUNG: 'mostly_true' für DACH-Erhöhungs-Behauptungen."
    )

    return [{
        "indicator_name": "DACH-Pensionsalter-Reformen 2024-2026",
        "indicator": "dach_pensions_vergleich",
        "country": "DEU/AUT/CHE", "country_name": "DACH-Region",
        "year": "2024-2026",
        "display_value": headline,
        "description": " ".join(fact.get("context_notes") or []),
        "url": src, "source": label,
    }]


def _build_eu_beschluesse_results(fact: dict, claim_lc: str) -> list[dict]:
    data = fact.get("data") or {}
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "EUR-Lex / EU-Kommission"
    beschluesse = data.get("beschluesse") or []

    results: list[dict] = []

    # Versuch, ein spezifisches Beschluss-Topic zu identifizieren
    matched_beschluesse = []
    for b in beschluesse:
        bid = b.get("id", "").lower()
        bname = b.get("name", "").lower()
        # Match auf id oder name-Substring
        for trigger_kw in (
            ("ai act", "eu_ai_act"),
            ("ki-verordnung", "eu_ai_act"),
            ("taxonomie", "eu_taxonomie"),
            ("atomstrom", "eu_taxonomie"),
            ("csddd", "eu_csrd"),
            ("lieferketten", "eu_csrd"),
            ("recht auf reparatur", "eu_recht"),
            ("plastiksteuer", "eu_plastik"),
            ("methan", "eu_methan"),
            ("renaturierung", "eu_nature"),
            ("nature restoration", "eu_nature"),
            ("asyl- und migrations", "eu_asyl"),
        ):
            if trigger_kw[0] in claim_lc and trigger_kw[1] in bid:
                matched_beschluesse.append(b)
                break

    # Wenn ein spezifischer Beschluss erkannt → Detail-Eintrag
    for b in matched_beschluesse:
        results.append({
            "indicator_name": f"EU: {b['name']}",
            "indicator": "eu_beschluss_detail",
            "country": "EU", "country_name": "Europäische Union",
            "year": b.get("in_kraft", ""),
            "display_value": (
                f"EU-Rechtsakt: {b['name']} ({b.get('verordnung', '')}). "
                f"In Kraft: {b.get('in_kraft', '?')}. "
                f"{('Vollanwendung ab: ' + b['vollanwendung_ab'] + '. ') if b.get('vollanwendung_ab') else ''}"
                f"Kerninhalt: {b.get('kerninhalt', '')}"
            ),
            "description": " ".join(fact.get("context_notes") or []),
            "url": src, "source": label,
        })

    # Übersichts-Eintrag immer dazu
    overview_text = (
        f"EU-Schlüsselbeschlüsse 2024-2026 ({len(beschluesse)} dokumentierte): "
        + ", ".join(f"{b['name']} ({b.get('in_kraft', '?')})" for b in beschluesse[:5])
        + (f" und {len(beschluesse) - 5} weitere." if len(beschluesse) > 5 else "")
    )
    results.append({
        "indicator_name": "EU-Schlüsselbeschlüsse 2024-2026 (Übersicht)",
        "indicator": "eu_beschluesse_overview",
        "country": "EU", "country_name": "Europäische Union",
        "year": "2024-2026",
        "display_value": overview_text,
        "description": " ".join(fact.get("context_notes") or []),
        "url": src, "source": label,
    })

    return results


def _build_dach_hpv_results(fact: dict, claim_lc: str) -> list[dict]:
    data = fact.get("data") or {}
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "WHO-WUENIC"

    ranking = data.get("ranking_dach_2024") or []
    rank_str = ", ".join(f"{e['land']} {e['rate_pct']} %" for e in ranking)

    headline = (
        f"HPV-Impfraten DACH 2024 (WHO-WUENIC, 2-Dosen-Schema 15j Mädchen): "
        f"{rank_str}. {data.get('ranking_kommentar', '')}"
    )

    return [{
        "indicator_name": "HPV-Impfraten DACH-Region 2024",
        "indicator": "dach_hpv_impfraten",
        "country": "DEU/AUT/CHE", "country_name": "DACH-Region",
        "year": "2024",
        "value": data.get("at_hpv_2dose_pct_15j_maedchen_2024"),
        "display_value": headline,
        "description": " ".join(fact.get("context_notes") or []),
        "url": src, "source": label,
    }]


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_dach_factbook(analysis: dict) -> dict:
    empty = {
        "source": "DACH Factbook",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()
    matched_topics = _claim_matches_any_topic(matchable)
    if not matched_topics:
        return empty

    data = _load_static_json()
    if not data:
        return empty

    facts = data.get("facts") or []
    results: list[dict] = []

    for topic in matched_topics:
        for fact in facts:
            if fact.get("topic") != topic:
                continue
            if topic == "bamf_asyl_de":
                results.extend(_build_bamf_results(fact, matchable))
            elif topic == "buergergeld_de":
                results.extend(_build_buergergeld_results(fact, matchable))
            elif topic == "heizungsgesetz_de":
                results.extend(_build_heizung_results(fact, matchable))
            elif topic == "ahv_ch":
                results.extend(_build_ahv_results(fact, matchable))
            elif topic == "afd_venedig_meeresspiegel_counter":
                results.extend(_build_venedig_counter_results(fact, matchable))
            elif topic == "klimaskepsis_counter":
                results.extend(_build_klimaskepsis_counter_results(fact, matchable))
            elif topic == "asylbewerber_gesundheit_de_counter":
                results.extend(_build_asylbewerber_gesundheit_counter_results(fact, matchable))
            elif topic == "dach_asyl_vergleich":
                results.extend(_build_dach_asyl_vergleich_results(fact, matchable))
            elif topic == "dach_pensions_vergleich":
                results.extend(_build_dach_pensions_vergleich_results(fact, matchable))
            elif topic == "dach_hpv_impfraten":
                results.extend(_build_dach_hpv_results(fact, matchable))
            elif topic == "eu_schluessel_beschluesse":
                results.extend(_build_eu_beschluesse_results(fact, matchable))

    return {
        "source": "DACH Factbook",
        "type": "official_data",
        "results": results,
    }
