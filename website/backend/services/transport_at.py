"""ÖBB Pünktlichkeit + Verkehrs-CO2 Umweltbundesamt + KlimaTicket-Stats —
Verkehrs-Eckwerte gegen Boulevard-Mythen ('ÖBB unzuverlässig',
'Klimakleber sollen Lkw blockieren', 'KlimaTicket ist gescheitert')."""

import logging
import os

from services._topic_match import find_matching_items, load_items
from services._notizen import prompt_notizen
from services._fmt import de_int

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "transport_at.json",
)


def _descriptor(f: dict) -> tuple[dict, str]:
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {notes}"[:300])


def _claim_matches_facts(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    return find_matching_items(
        STATIC_JSON_PATH, "facts",
        claim_lc=claim_lc, full_claim=full_claim,
        # Cosine-Backup deaktiviert (QA100 2026-07-27, analog #41):
        # gemessen lieferte der Backup auf 100 breit gestreute Claims
        # 172 von 176 Treffern — durchweg themenfremd (Olympia,
        # Glasfaser, Weltbank) — und verdrängte im Prompt den passenden
        # Inhalt. Eigene claim_phrasings-Abdeckung vorher auf 100 %
        # gehoben, Über-Trigger-Kontrolle: +1 (thematisch korrekter)
        # Exakt-Treffer auf denselben 100 Claims.
        descriptor_fn=None,
    )


def claim_mentions_transport_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_transport(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


async def search_transport(analysis: dict) -> dict:
    empty = {
        "source": "Verkehr Österreich (ÖBB + UBA + KlimaTicket)",
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
        label = fact.get("source_label", "ÖBB / UBA / Eurostat")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(prompt_notizen(notes))
        year = str(fact.get("year", ""))

        if topic == "oebb_puenktlichkeit":
            display = (
                f"ÖBB-Pünktlichkeit 2024 (≤5 Min Verspätung): "
                f"Nahverkehr {d.get('puenktlichkeit_oebb_nahverkehr_2024_pct')} %, "
                f"Fernverkehr {d.get('puenktlichkeit_oebb_fernverkehr_2024_pct')} %. "
                f"DACH-Vergleich Fernverkehr: SBB "
                f"{d.get('puenktlichkeit_sbb_fernverkehr_2024_pct')} %, DB nur "
                f"{d.get('puenktlichkeit_db_fernverkehr_2024_pct')} %. "
                f"Fahrgäste 2024: {d.get('fahrgaeste_oebb_2024_mio')} Mio "
                f"(2014: {d.get('fahrgaeste_oebb_2014_mio')} Mio, +11 %)."
            )
            description = d.get("context", "") + " " + notes_joined
        elif topic == "verkehr_co2":
            display = (
                f"Verkehrs-CO2 Österreich 2024: "
                f"{d.get('co2_verkehr_at_2024_mio_t')} Mio. t "
                f"(1990: {d.get('co2_verkehr_at_1990_mio_t')} Mio. t; "
                f"2005-Peak: {d.get('co2_verkehr_at_2005_peak_mio_t')} Mio. t). "
                f"Verkehr = {d.get('anteil_verkehr_an_thg_at_pct_2024')} % der "
                f"AT-Treibhausgase. Pkw verursachen "
                f"{d.get('anteil_pkw_an_verkehrs_co2_pct_2024')} % der "
                f"Verkehrs-CO2, Lkw {d.get('anteil_lkw_an_verkehrs_co2_pct_2024')} %, "
                f"Flugverkehr {d.get('anteil_flugverkehr_at_emissionen_pct_2024')} %. "
                f"Modal-Split AT: {d.get('modal_split_pkw_at_pct')} % Pkw, "
                f"{d.get('modal_split_oeffi_at_pct')} % ÖV, "
                f"{d.get('modal_split_rad_fuss_at_pct')} % Rad/Fuß."
            )
            description = d.get("context", "") + " " + notes_joined
        elif topic == "klimaticket":
            # Bis zum 20.10.2026 las dieser Zweig SECHS Felder, die es in
            # transport_at.json nicht gibt: klimaticket_inhaber_2024,
            # _inhaber_2022_start, _kosten_2024_eur, _kosten_jugend_2024_eur,
            # modal_shift_pkw_zu_oeffi_pct_geschaetzt und
            # co2_einsparung_geschaetzt_2024_kt. Live ging damit heraus:
            #
            #   "KlimaTicket Österreich: ? Inhaber:innen 2024 (Start 10/2021:
            #    ?, +83 %). Vollpreis ? €/Jahr; Jugend/Senior ? €.
            #    Geschätzter Modal-Shift None % ...; CO2-Einsparung ~None kt"
            #
            # Vier Fragezeichen und zwei woertliche None — waehrend der Fakt
            # belastbare Rechnungshof-Zahlen fuehrt (Reihe BUND 2025/38).
            # Der Zweig war gegen eine aeltere Datengestalt geschrieben und
            # wurde beim Daten-Refresh nicht mitgezogen. Gefunden von
            # tools/feldzugriffe_pruefen.py nach dem QA50I-Lauf.
            #
            # Die Steigerung wird GERECHNET, nicht gefuehrt: Das alte
            # "+83 %" stand als Literal da und war falsch (118.524 -> 293.742
            # sind +148 %).
            jahre = d.get("klimaticket_oe_jahresaequivalente") or {}
            geordnet = sorted((str(k), v) for k, v in jahre.items()
                              if isinstance(v, (int, float)))
            teile = ["KlimaTicket Österreich (Jahresäquivalente, Rechnungshof)"]
            if geordnet:
                (erstes_j, erstes_w), (letztes_j, letztes_w) = (
                    geordnet[0], geordnet[-1])
                verlauf = ", ".join(f"{j}: {de_int(w)}" for j, w in geordnet)
                teile.append(f"{verlauf}")
                if erstes_w:
                    wachstum = round((letztes_w / erstes_w - 1) * 100)
                    teile.append(f"{erstes_j} bis {letztes_j}: +{wachstum} %")
            prognose = d.get("klimaticket_oe_prognose_2023")
            if isinstance(prognose, (int, float)) and geordnet:
                ist_2023 = jahre.get("2023")
                if isinstance(ist_2023, (int, float)):
                    teile.append(
                        f"Prognose für 2023 war {de_int(prognose)}, "
                        f"tatsächlich {de_int(ist_2023)}")
            preise = []
            for feld, text in (
                    ("preis_bis_2024_eur", "Vollpreis bis 2024"),
                    ("preis_ab_2026_eur", "ab 2026"),
                    ("preis_ermaessigt_bis_2024_eur", "ermäßigt bis 2024"),
                    ("preis_ermaessigt_ab_2026_eur", "ermäßigt ab 2026")):
                wert = d.get(feld)
                if isinstance(wert, (int, float)):
                    preise.append(f"{text} {de_int(wert)} €")
            if preise:
                teile.append("; ".join(preise))
            co2 = d.get("co2_reduktion_erwartet_2024_mio_t")
            anteil = d.get("co2_anteil_verkehrsemissionen_pct")
            if isinstance(co2, (int, float)):
                satz = f"erwartete CO2-Reduktion {co2} Mio. t"
                if isinstance(anteil, (int, float)):
                    satz += f" ({anteil} % der Verkehrsemissionen)"
                teile.append(satz)
            display = ". ".join(teile) + "."
            description = d.get("context", "") + " " + notes_joined
        else:
            display = fact.get("headline", "?")
            description = notes_joined

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "transport_at_fact",
            "country": "AT",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description.strip(" |").strip(),
            "url": url,
            "source": label,
        })

    return {
        "source": "Verkehr Österreich (ÖBB + UBA + KlimaTicket)",
        "type": "official_data",
        "results": results,
    }
