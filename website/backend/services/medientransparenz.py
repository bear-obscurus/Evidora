"""MedienTransparenz.gv.at (RTR / KommAustria) — Inserate-Meldungen
der öffentlichen Hand in Österreich nach §§ 2 + 4 MedKF-TG.

Datenquelle: Static-curated JSON in data/medientransparenz.json — basierend
auf den jährlichen RTR-Veröffentlichungen. Live-CSV-Pfad
(rtr.at/medien/was_wir_tun/medientransparenz) wäre quartalsweise refreshbar,
aber für die Boulevard-Use-Cases ('wer kriegt am meisten Inserate?',
'Inseratenaffäre Kurz') reicht eine jährliche Aktualisierung.

Pattern: Trigger-Match → Topic-spezifische Result-Builder mit den
Top-Empfänger / Top-Auftraggeber-Listen + WKStA-Kontext.
"""

import logging
import os

from services._static_cache import load_json_mtime_aware
from services._reranker_backup import best_matches as _backup_best_matches

logger = logging.getLogger("evidora")


def _fact_with_descriptor(f: dict) -> tuple[dict, str]:
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {notes}"[:300])

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "medientransparenz.json",
)


def _load_static_json() -> dict | None:
    data = load_json_mtime_aware(STATIC_JSON_PATH)
    if data is None:
        return None
    if "facts" not in data:
        logger.warning("medientransparenz.json missing 'facts' key")
        return None
    return data


def _fact_matches(fact: dict, claim_lc: str) -> bool:
    for kw in fact.get("trigger_keywords") or ():
        if kw.lower() in claim_lc:
            return True
    composite = fact.get("trigger_composite") or []
    if composite and all(
        isinstance(alt, (list, tuple)) and any(tok in claim_lc for tok in alt)
        for alt in composite
    ):
        return True
    return False


def _claim_matches_facts(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    data = _load_static_json()
    if not data:
        return []
    facts = data.get("facts") or []
    matches = [f for f in facts if _fact_matches(f, claim_lc)]
    if matches:
        return matches
    if not full_claim:
        return []
    items = [_fact_with_descriptor(f) for f in facts]
    return _backup_best_matches(full_claim, items, threshold=0.45, top_n=3)


def claim_mentions_medientransparenz_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_medientransparenz(client=None):
    data = _load_static_json()
    if not data:
        return []
    return data.get("facts") or []


async def search_medientransparenz(analysis: dict) -> dict:
    empty = {
        "source": "MedienTransparenz (RTR/KommAustria)",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("original_claim") or (analysis or {}).get("claim", "") or ""
    matches = _claim_matches_facts(claim.lower(), full_claim=claim)
    if not matches:
        return empty

    def _de(v):
        try:
            return f"{int(round(float(v))):,}".replace(",", ".")
        except Exception:
            return str(v)

    results: list[dict] = []

    def _emit(*, name: str, year: str, display: str, description: str,
              url: str, source: str, topic: str = "",
              value=None, value_unit: str = ""):
        """Append one evidence-tauglichen Sub-Result. Jeder Eintrag ist
        eine eigenständige Behauptung, die der Synthesizer 1:1 als
        evidence übernehmen kann.

        Optional: ``value`` + ``value_unit`` strukturieren die primäre
        numerische Aussage (z.B. der Spitzenreiter-Wert), damit der
        Synthesizer keine Aggregat-Halluzination machen muss."""
        out = {
            "indicator_name": name,
            "indicator": "medientransparenz_fact",
            "country": "AT",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description.strip(" |").strip(),
            "url": url,
            "source": source,
        }
        if value is not None:
            out["value"] = value
            if value_unit:
                out["unit"] = value_unit
        results.append(out)

    for fact in matches:
        topic = fact.get("topic", "")
        d = fact.get("data") or {}
        url = fact.get("source_url", "")
        label = fact.get("source_label", "RTR / KommAustria")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        if topic == "medientransparenz_overview":
            # Sub-Result A: Gesamtvolumen + Trend (für "explodiert"-/Trend-Claims)
            _emit(
                topic=topic,
                name="Inserate-Volumen öffentliche Hand AT 2024 (Gesamt + Trend)",
                year=year,
                display=(
                    f"Inserate-Volumen der öffentlichen Hand in Österreich 2024: "
                    f"{d.get('gesamtvolumen_2024_mio_eur')} Mio. € "
                    f"(2017: {d.get('gesamtvolumen_2017_mio_eur')} Mio. €, "
                    f"+30 % über 7 Jahre — entspricht Inflation)."
                ),
                description=notes_joined,
                url=url, source=label,
            )
            # Sub-Result B: Top-Empfänger (Boulevard-Konzentration belegen)
            top10 = d.get("top_10_empfaenger_2024") or []
            if top10:
                top5 = top10[:5]
                top5_str = " · ".join(
                    f"#{e['rang']} {e['medium']} ({e['betrag_mio_eur']} Mio.)"
                    for e in top5
                )
                # Boulevard-Aggregat
                boulevard_keys = ("Krone", "Heute", "Österreich", "oe24")
                boulevard_sum = sum(
                    e["betrag_mio_eur"] for e in top10
                    if any(k in e["medium"] for k in boulevard_keys)
                )
                seriose_keys = ("Standard", "Presse", "Salzburger Nachrichten", "OÖ Nachrichten")
                seriose_sum = sum(
                    e["betrag_mio_eur"] for e in top10
                    if any(k in e["medium"] for k in seriose_keys)
                )
                spitzenreiter = top5[0]
                _emit(
                    topic=topic,
                    name=(f"Spitzenreiter Inserate-Empfänger AT 2024: "
                          f"{spitzenreiter['medium']}"),
                    year=year,
                    value=spitzenreiter["betrag_mio_eur"],
                    value_unit="Mio. EUR",
                    display=(
                        f"Spitzenreiter der Inserate-Empfänger 2024: "
                        f"{spitzenreiter['medium']} mit "
                        f"{spitzenreiter['betrag_mio_eur']} Mio. € — "
                        f"das ist der größte Einzelposten unter allen "
                        f"Medien, die öffentliche Inserate erhalten haben "
                        f"(Rang #1 von 10). "
                        f"Vollständige Top-5: {top5_str}. "
                        f"Boulevard-Trio (Krone + Heute + Österreich/oe24) "
                        f"erhielt zusammen {boulevard_sum:.1f} Mio. €; "
                        f"seriöse Tageszeitungen (Standard + Presse + SN + OÖN) "
                        f"zusammen {seriose_sum:.1f} Mio. €. "
                        f"Boulevard ist überproportional bedacht."
                    ),
                    description=notes_joined,
                    url=url, source=label,
                )
            # Sub-Result C: Top-Auftraggeber (z. B. Stadt Wien)
            top5_auftrag = d.get("top_5_auftraggeber_2024") or []
            if top5_auftrag:
                ranking = " · ".join(
                    f"#{e['rang']} {e['auftraggeber']} ({e['betrag_mio_eur']} Mio.)"
                    for e in top5_auftrag
                )
                wien = next(
                    (e for e in top5_auftrag if "Wien" in e.get("auftraggeber", "")),
                    None,
                )
                bund = next(
                    (e for e in top5_auftrag
                     if "Bundesministerien" in e.get("auftraggeber", "")
                     or "Bundesregierung" in e.get("auftraggeber", "")),
                    None,
                )
                summary_parts = []
                if wien:
                    summary_parts.append(
                        f"Stadt Wien: {wien['betrag_mio_eur']} Mio. € "
                        f"(Rang #{wien['rang']}, größter Einzel-Auftraggeber)"
                    )
                if bund:
                    summary_parts.append(
                        f"Bundesministerien gesamt: {bund['betrag_mio_eur']} Mio. € "
                        f"(Rang #{bund['rang']}) — NICHT mit dem Gesamtvolumen "
                        f"der öffentlichen Hand verwechseln"
                    )
                summary = " ; ".join(summary_parts)
                _emit(
                    topic=topic,
                    name="Top-5 Inserate-Auftraggeber AT 2024",
                    year=year,
                    display=(
                        f"Top-5-Auftraggeber 2024: {ranking}. {summary}"
                    ),
                    description=(
                        "Wichtig: Die Bundesregierung (Bundesministerien zusammen) "
                        "ist NICHT der einzige öffentliche Auftraggeber — Stadt Wien "
                        "und ÖBB liegen einzeln darüber bzw. ähnlich hoch. "
                        "Aussagen über 'die Bundesregierung gibt X Millionen aus' "
                        "beziehen sich nur auf den Bundesministerien-Anteil, nicht "
                        "auf das gesamte Inserate-Volumen der öffentlichen Hand."
                    ),
                    url=url, source=label,
                )

        elif topic == "medientransparenz_kurz_aera":
            # Sub-Result A: BKA-Verlauf
            _emit(
                topic=topic,
                name="BKA-Inserate-Verlauf 2017–2021 (Kurz-Ära)",
                year="2017-2021",
                display=(
                    f"Bundeskanzleramt-Inserate: 2017 = "
                    f"{d.get('bundeskanzleramt_inserate_2017_mio_eur')} Mio. €, "
                    f"2019 = {d.get('bundeskanzleramt_inserate_2019_mio_eur')} Mio. €, "
                    f"2021 = {d.get('bundeskanzleramt_inserate_2021_mio_eur')} Mio. € "
                    f"— Sechsfachung in vier Jahren (+509 %)."
                ),
                description=d.get("kontext_pilnacek_wkstaat_ermittlung", ""),
                url=url, source=label,
            )
            # Sub-Result B: BMF-Verlauf
            _emit(
                topic=topic,
                name="BMF-Inserate-Verlauf 2019–2021 (Kurz-Ära)",
                year="2019-2021",
                display=(
                    f"Finanzministerium-Inserate: 2019 = "
                    f"{d.get('bmf_inserate_2019_mio_eur')} Mio. €, 2021 = "
                    f"{d.get('bmf_inserate_2021_mio_eur')} Mio. € (+89 %)."
                ),
                description=notes_joined,
                url=url, source=label,
            )
            # Sub-Result C: BMLRT-Verlauf + WKStA-Kontext
            _emit(
                topic=topic,
                name="BMLRT-Inserate-Verlauf 2019–2021 + WKStA-Anklage",
                year="2019-2024",
                display=(
                    f"Landwirtschafts-/Regionen-Ministerium-Inserate: "
                    f"2019 = {d.get('bmlrt_inserate_2019_mio_eur')} Mio. €, "
                    f"2021 = {d.get('bmlrt_inserate_2021_mio_eur')} Mio. € (+264 %). "
                    f"WKStA-Anklage gegen Sebastian Kurz, Thomas Schmid u. a. "
                    f"seit Herbst 2024 — Verfahren laufend, Unschuldsvermutung gilt."
                ),
                description=d.get("kontext_pilnacek_wkstaat_ermittlung", ""),
                url=url, source=label,
            )
        else:
            _emit(
                topic=topic,
                name=fact.get("headline", "?"),
                year=year,
                display=fact.get("headline", "?"),
                description=notes_joined,
                url=url, source=label,
            )

    return {
        "source": "MedienTransparenz (RTR/KommAustria)",
        "type": "official_data",
        "results": results,
    }
