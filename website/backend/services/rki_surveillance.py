"""RKI SurvStat 2.0 — Robert Koch-Institut Surveillance-Eckwerte für die
häufigsten Boulevard-Themen rund um meldepflichtige Krankheiten in DE.

Datenquelle: Static-curated JSON in data/rki_surveillance.json. Live-Pfad
über survstat.rki.de wäre via SOAP-Endpoint möglich, ist aber komplex
und nur quartalsweise notwendig — für die wichtigsten Use-Cases (Masern-
Welle 2024, TB-Migration-Mythos, COVID-vs.-Grippe-Winter 2024/25)
reicht eine kuratierte Sammlung mit jährlicher Aktualisierung.

Pattern: Trigger-Match → Topic-spezifischer Result-Builder mit
Strukturkontext (Inzidenz-Vergleich historisch, Migrations-Anteil
mit Erklärung, Peak-Vergleich mit Vor-Pandemie-Niveau).
"""

import logging
import os

from services._topic_match import find_matching_items, load_items
from services._fmt import de_int, de_num

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "rki_surveillance.json",
)


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


def claim_mentions_rki_surveillance_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_rki_surveillance(client=None):
    return load_items(STATIC_JSON_PATH, "facts")



def _fuege(*bausteine, d: dict) -> str:
    """Setzt den Anzeigetext aus Bausteinen zusammen und laesst jeden Baustein
    weg, dessen Datenfelder fehlen (gleiche Bauform wie services/oecd_health.py).
    """
    return " ".join(text for text, felder in bausteine
                    if all(d.get(f) is not None for f in felder))


def _zuwachs(vorher, nachher) -> str:
    """'+706 %' stand bis 2026-09 als Literal im Code — beim naechsten
    Daten-Refresh haette es nicht mehr zu den Zahlen gepasst."""
    try:
        v, n = float(vorher), float(nachher)
        if v <= 0:
            return ""
        return f"{(n - v) / v * 100:+.0f} % gegenueber dem Vorjahr".replace(
            "gegenueber", "gegenüber")
    except (TypeError, ValueError):
        return ""


def _saisonreihe(d: dict, praefix: str) -> str:
    """'2022/23 11.204, 2023/24 10.434, …' aus allen Feldern mit dem Praefix.

    Die Saisonen stehen damit in den DATEN, nicht im Code — eine neue Saison
    ergaenzt man in der JSON, ohne den Renderer anzufassen.
    """
    teile = []
    for k in sorted(k for k in d if k.startswith(praefix)):
        rest = k[len(praefix):]
        if not (len(rest) == 7 and rest[4] == "_"):
            continue                      # z. B. '..._woche_2025_26'
        saison = f"20{rest[2:4]}/{rest[5:7]}"
        wert = d[k]
        teile.append(f"{saison} "
                     f"{de_int(wert) if float(wert).is_integer() else de_num(wert)}")
    return ", ".join(teile)


async def search_rki_surveillance(analysis: dict) -> dict:
    empty = {
        "source": "RKI SurvStat (Surveillance)",
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
        label = fact.get("source_label", "RKI SurvStat")
        notes = fact.get("context_notes") or []

        if topic == "rki_masern":
            display = _fuege(
                (f"Masern in Deutschland: 2023 = "
                 f"{de_int(d.get('rki_masern_faelle_2023'))} Fälle, 2024 = "
                 f"{de_int(d.get('rki_masern_faelle_2024'))} Fälle "
                 f"({_zuwachs(d.get('rki_masern_faelle_2023'), d.get('rki_masern_faelle_2024'))}).",
                 ["rki_masern_faelle_2023", "rki_masern_faelle_2024"]),
                (f"Zweitimpfquote mit 24 Monaten = "
                 f"{de_num(d.get('impfquote_de_masern_kinder_24m_pct_2024'))} % "
                 f"(WHO-Schwelle für Herdimmunität: "
                 f"{de_num(d.get('impfquote_who_herdimmunitaet_pct'))} %).",
                 ["impfquote_de_masern_kinder_24m_pct_2024",
                  "impfquote_who_herdimmunitaet_pct"]),
                (str(d.get("datenstand")), ["datenstand"]),
                d=d,
            )
        elif topic == "rki_tuberkulose":
            display = _fuege(
                (f"Tuberkulose in Deutschland 2024: "
                 f"{de_int(d.get('rki_tb_faelle_2024'))} Fälle, Inzidenz "
                 f"{de_num(d.get('rki_tb_inzidenz_pro_100k_2024'))} je 100.000.",
                 ["rki_tb_faelle_2024", "rki_tb_inzidenz_pro_100k_2024"]),
                (f"Zum Vergleich 1980 = {de_num(d.get('rki_tb_inzidenz_pro_100k_1980'))}, "
                 f"1995 = {de_num(d.get('rki_tb_inzidenz_pro_100k_1995'))} je 100.000.",
                 ["rki_tb_inzidenz_pro_100k_1980", "rki_tb_inzidenz_pro_100k_1995"]),
                (f"Anteil im Ausland Geborener: "
                 f"{de_num(d.get('anteil_im_ausland_geboren_pct_2024'))} % — wenig "
                 f"Übertragung innerhalb Deutschlands, viele Fälle werden bei "
                 f"der Einreise diagnostiziert.",
                 ["anteil_im_ausland_geboren_pct_2024"]),
                (str(d.get("datenstand")), ["datenstand"]),
                d=d,
            )
        elif topic == "rki_atemwegsinfekte":
            display = _fuege(
                (f"Atemwegsinfekte Deutschland, Höchstwert der GrippeWeb-ARE-"
                 f"Inzidenz je 100.000: "
                 f"{_saisonreihe(d, 'grippeweb_are_peak_je_100k_')}.",
                 ["grippeweb_are_peak_je_100k_2025_26"]),
                (f"Vor-Pandemie-Vergleich 2018/19: "
                 f"{de_int(d.get('grippeweb_are_peak_vor_pandemie_2018_19'))}.",
                 ["grippeweb_are_peak_vor_pandemie_2018_19"]),
                (f"SARI-Hospitalisierungsinzidenz je 100.000: "
                 f"{_saisonreihe(d, 'sari_hospitalisierung_peak_je_100k_')}.",
                 ["sari_hospitalisierung_peak_je_100k_2025_26"]),
                (f"COVID-Anteil an den ARE-Konsultationen in der Höchstwoche: "
                 f"{de_num(d.get('covid_anteil_an_are_konsultationen_peakwoche_2025_26_pct'))} %.",
                 ["covid_anteil_an_are_konsultationen_peakwoche_2025_26_pct"]),
                # Die ausführliche Messgrößen-Warnung steht in der Headline —
                # dem Kanal, der die 400-Zeichen-Kürzung ungekürzt passiert.
                # Sie hier zu wiederholen würde den display_value darüber
                # heben und die Warnung wäre das Erste, was wegfällt.
                d=d,
            )
        else:
            display = fact.get("headline", "?")

        description = (d.get("context", "") + " "
                       + d.get("context_quelle", "")).strip()

        if notes:
            description = (description + " ").strip() + " | " + " | ".join(notes)

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "rki_surveillance_fact",
            "country": "DE",
            "year": str(fact.get("year", "")),
            "topic": topic,
            "display_value": display,
            "description": description.strip(" |").strip(),
            "url": url,
            "source": label,
        })

    return {
        "source": "RKI SurvStat (Surveillance)",
        "type": "official_data",
        "results": results,
    }
