"""OECD Health Statistics + Eurostat hlth_* — Gesundheitssystem-Eckwerte
DACH gegen die häufigsten Boulevard-Mythen ('Gesundheitssystem
kollabiert', 'Lebenserwartung sinkt', 'zu wenig Spitalsbetten').
"""

import logging
import os

from services._topic_match import (
    find_matching_items,
    load_items,
)
from services._fmt import de_int, de_num

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "oecd_health.json",
)



def _fuege(*bausteine, d: dict) -> str:
    """Setzt den Anzeigetext aus Bausteinen zusammen und laesst jeden Baustein
    weg, dessen Datenfelder fehlen.

    Grund: Bis 2026-09 hat dieser Renderer Feldnamen fest verdrahtet und beim
    Vintage-Wechsel eine TypeError-Arithmetik auf None ausgeloest (gleiche
    Klasse wie in services/wifo_ihs.py). Ein Datensatz ohne Feld darf hoechstens
    einen Satz kosten, nicht die ganze Quelle.
    """
    teile = [text for text, felder in bausteine
             if all(d.get(f) is not None for f in felder)]
    return " ".join(teile)


def _descriptor(f: dict) -> tuple[dict, str]:
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {notes}"[:300])


def _claim_matches_facts(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    return find_matching_items(
        STATIC_JSON_PATH, "facts",
        claim_lc=claim_lc, full_claim=full_claim,
        descriptor_fn=_descriptor,
    )


def claim_mentions_oecd_health_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_oecd_health(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


async def search_oecd_health(analysis: dict) -> dict:
    empty = {
        "source": "OECD Health (DACH)",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("original_claim") or (analysis or {}).get("claim", "") or ""
    matches = _claim_matches_facts(claim.lower(), full_claim=claim)
    if not matches:
        return empty

    results: list[dict] = []
    for fact in matches:
        topic = fact.get("topic", "")
        d = fact.get("data") or {}
        url = fact.get("source_url", "")
        label = fact.get("source_label", "OECD")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        if topic == "lebenserwartung":
            display = _fuege(
                (f"Lebenserwartung bei Geburt in Österreich {year}: "
                 f"{de_num(d.get('lebenserwartung_at_' + year))} Jahre.",
                 ["lebenserwartung_at_" + year]),
                (f"Das sind rund {d.get('abstand_zum_eu_schnitt_monate')} Monate "
                 f"ÜBER dem EU-Schnitt; {year} wurde erstmals seit der Pandemie "
                 f"der frühere Höchststand wieder übertroffen.",
                 ["abstand_zum_eu_schnitt_monate"]),
                (f"Frauen {de_num(d.get('lebenserwartung_at_frauen_' + year))} Jahre, "
                 f"{de_num(d.get('geschlechter_luecke_jahre_' + year))} Jahre mehr "
                 f"als Männer.",
                 ["lebenserwartung_at_frauen_" + year,
                  "geschlechter_luecke_jahre_" + year]),
                d=d,
            )
        elif topic == "spitalsbetten":
            display = _fuege(
                (f"Spitalsbetten je 1.000 Einwohner in Österreich {year}: "
                 f"{de_num(d.get('spitalsbetten_pro_1000_at_' + year))} — einer der "
                 f"höchsten Werte der EU.",
                 ["spitalsbetten_pro_1000_at_" + year]),
                (f"Seit 2017 um {d.get('spitalsbetten_rueckgang_2017_' + year + '_pct')} % "
                 f"GESUNKEN (bewusster Abbau zugunsten ambulanter Versorgung).",
                 ["spitalsbetten_rueckgang_2017_" + year + "_pct"]),
                (f"Die stationären Aufenthalte gingen im selben Zeitraum um "
                 f"{d.get('stationaere_aufenthalte_rueckgang_2017_' + year + '_pct')} % "
                 f"zurück.",
                 ["stationaere_aufenthalte_rueckgang_2017_" + year + "_pct"]),
                (f"Ärztedichte {de_num(d.get('aerzte_pro_1000_at_' + year))} je 1.000 "
                 f"gegenüber {de_num(d.get('aerzte_pro_1000_eu_avg_' + year))} im "
                 f"EU-Schnitt.",
                 ["aerzte_pro_1000_at_" + year, "aerzte_pro_1000_eu_avg_" + year]),
                d=d,
            )
        elif topic == "gesundheitsausgaben":
            display = _fuege(
                (f"Gesundheitsausgaben Österreich {year}: "
                 f"{de_num(d.get('gesundheitsausgaben_at_pct_bip_' + year))} % des BIP, "
                 f"{de_num(d.get('abstand_eu_schnitt_pp'))} Prozentpunkte über dem "
                 f"EU-Schnitt.",
                 ["gesundheitsausgaben_at_pct_bip_" + year, "abstand_eu_schnitt_pp"]),
                (f"Kaufkraftbereinigt {de_int(d.get('pro_kopf_kkp_eur_at_' + year))} € "
                 f"pro Kopf gegenüber "
                 f"{de_int(d.get('pro_kopf_kkp_eur_eu_avg_' + year))} € im EU-Schnitt.",
                 ["pro_kopf_kkp_eur_at_" + year, "pro_kopf_kkp_eur_eu_avg_" + year]),
                (f"Öffentlicher Anteil "
                 f"{de_num(d.get('oeffentlicher_anteil_at_pct_' + year))} % "
                 f"(EU-Schnitt {de_num(d.get('oeffentlicher_anteil_eu_avg_pct_' + year))} %), "
                 f"Selbstbehalte {de_num(d.get('oop_anteil_at_pct_' + year))} % "
                 f"(EU {de_num(d.get('oop_anteil_eu_avg_pct_' + year))} %).",
                 ["oeffentlicher_anteil_at_pct_" + year,
                  "oeffentlicher_anteil_eu_avg_pct_" + year,
                  "oop_anteil_at_pct_" + year, "oop_anteil_eu_avg_pct_" + year]),
                d=d,
            )
        elif topic == "kinder_adipositas":
            display = _fuege(
                (f"Kinder + Jugendliche Übergewicht/Adipositas (DACH-Vergleich): "
                 f"AT (6–15 J.) = "
                 f"{de_num(d.get('uebergewicht_at_kinder_6_15_pct_' + year))} % "
                 f"übergewichtig, davon "
                 f"{de_num(d.get('adipositas_at_kinder_6_15_pct_' + year))} % adipös.",
                 ["uebergewicht_at_kinder_6_15_pct_" + year,
                  "adipositas_at_kinder_6_15_pct_" + year]),
                (f"Trend AT 2014→{year}: "
                 f"{de_num(d.get('uebergewicht_at_kinder_2014_pct'))} % → "
                 f"{de_num(d.get('uebergewicht_at_kinder_6_15_pct_' + year))} %.",
                 ["uebergewicht_at_kinder_2014_pct",
                  "uebergewicht_at_kinder_6_15_pct_" + year]),
                (f"DE (KIGGS, 3–17 J.): "
                 f"{de_num(d.get('uebergewicht_de_kinder_3_17_pct_' + year))} % / "
                 f"{de_num(d.get('adipositas_de_kinder_3_17_pct_' + year))} %.",
                 ["uebergewicht_de_kinder_3_17_pct_" + year,
                  "adipositas_de_kinder_3_17_pct_" + year]),
                (f"OECD-Schnitt Übergewicht: "
                 f"{de_num(d.get('uebergewicht_oecd_avg_kinder_pct_' + year))} %.",
                 ["uebergewicht_oecd_avg_kinder_pct_" + year]),
                d=d,
            )
        else:
            display = fact.get("headline", "?")

        for zusatz in ("messgroesse", "vergleichsbasis"):
            wert = d.get(zusatz)
            if wert:
                display = f"{display} {wert}"
        description = (d.get("context", "") + " " + notes_joined)

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "oecd_health_fact",
            "country": "AT/DE/CH/EU",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description.strip(" |").strip(),
            "url": url,
            "source": label,
        })

    return {
        "source": "OECD Health (DACH)",
        "type": "official_data",
        "results": results,
    }
