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
    has_pension = any(t in claim_lc for t in ("rente", "rentenalter", "altersvorsorge"))
    if has_pension and _has_ch_context(claim_lc):
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
)


def _claim_mentions_klimaskepsis_basic(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _KLIMASKEPSIS_TERMS)
    if has_term:
        return True
    # Composite: 'klimawandel' + ('nicht' oder 'frost' oder 'kalter winter' oder 'erfunden')
    has_klima = any(t in claim_lc for t in ("klimawandel", "klima-wandel", "global warming"))
    has_skeptik = any(t in claim_lc for t in (
        "nicht real", "schwindel", "erfunden", "panikmache",
        "frost", "kalter winter", "kalter monat",
        "war früher", "schon immer",
    ))
    return has_klima and has_skeptik


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

    headline = (
        f"STRUKTURELL FALSCH (Klima-Skeptiker-Klassiker): "
        f"'Klimawandel nicht real wegen kaltem Winter / Frost'. "
        f"Wetter ≠ Klima — WMO-Definition: Klima ist Statistik des Wetters über "
        f"min. 30 Jahre (Referenz {data.get('wmo_referenzperiode')}). "
        f"Globale Anomalie 2024: +{data.get('globale_temperatur_anomalie_2024_celsius')} °C. "
        f"Alle 10 wärmsten Jahre seit 1880 liegen NACH 2014."
    )

    return [{
        "indicator_name": "Klima-Skeptizismus — Wetter-vs-Klima-Counter",
        "indicator": "dach_klimaskepsis_counter",
        "country": "WLD", "country_name": "Welt",
        "year": "2024",
        "display_value": headline,
        "description": " ".join(fact.get("context_notes") or []),
        "url": src, "source": label,
    }]


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

    return {
        "source": "DACH Factbook",
        "type": "official_data",
        "results": results,
    }
