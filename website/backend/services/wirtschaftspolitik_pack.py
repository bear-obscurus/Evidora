"""Wirtschaftspolitik-Pack — kuratierte Konsens-Daten zu wirtschafts-
politischen Mythen + Halbwahrheiten in DACH.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: empirisch falsifizierbare Wirtschaftspolitik-
Mythen (Steuer-Selbstfinanzierung, Mindestlohn-Beschäftigung,
Schuldenbremse-Diskurs, Geldmenge-Inflation, Migration-Sozialsystem,
EU-Netto-Zahler, Vermögenssteuer-Kapitalflucht, Bahn-Privatisierung,
Hartz-IV-Faulenzer, Inflation-Verursacher, Renten-Zusammenbruch,
Erbschaftssteuer-Mittelstand, DE-Steuern-höchste).

Topics (13):
  - steuersenkung_selbstfinanzierung_konsens (Laffer-These — CBO 2018
    + Reagan/Bush-Daten widerlegen Selbstfinanzierungs-Quote 100 %)
  - mindestlohn_beschaeftigung_konsens (Card/Krueger 1994 + Cengiz
    2019 + IAB DE 2015-24 — keine signifikanten Job-Verluste bei
    moderaten Erhöhungen)
  - schuldenbremse_schwarze_null_konsens (GENUINER FORSCHUNGSSTREIT —
    Pack präsentiert PRO Sachverständigenrat-Mehrheit + IFO Fuest
    UND CONTRA DIW Fratzscher + Blanchard ohne Wertung)
  - geldmenge_inflation_konsens (EZB QE 2015-22 ohne Inflation,
    Inflations-Schub 2022 ~70 % Supply-Side Russland-Krieg)
  - migration_sozialsystem_konsens (IAB Brücker + OECD + Bundesbank —
    netto-fiskalischer Effekt langfristig positiv bei Integration)
  - eu_netto_zahler_konsens (DE 9 Mrd Netto-Saldo vs. 86 Mrd Binnen-
    markt-Gewinn = Faktor 9 — Mythos durch Bertelsmann/Felbermayr
    widerlegt)
  - vermoegenssteuer_kapitalflucht_konsens (Norwegen seit 1882 +
    Schweiz Brülhart 2022 + Frankreich ISF — Mobility-Effekt
    quantitativ klein 0,1-3 %)
  - privatisierung_bahn_effizienz_konsens (UK 1993-97 → Network Rail
    Re-Verstaatlichung 2014 + Cambridge Studien — empirisch kein
    Effizienz-Gewinn)
  - hartz_iv_faulenzer_konsens (BA-Statistik 2024 + IAB — 30 %
    Aufstocker + 19 % Erwerbsunfähige + 15 % Erziehende = 64 % der
    Empfänger juristisch nicht 'arbeits-pflichtig')
  - inflation_staat_verursacher_konsens (EZB Lane + IWF + Bundesbank
    Dekomposition 2022/23 — ~70 % Supply-Side, ~10 % Geld-/Fiskal-
    Politik)
  - renten_zusammenbruch_konsens (DRV Modell-Rechnungen 2024-2050 —
    System schrumpft + wird teurer, aber NICHT vor Zusammenbruch)
  - erbschaftssteuer_mittelstand_konsens (DESTATIS 2023 — nur 13 %
    der Erbschaften erreichen Schwelle, ~2 % zahlen substantiell;
    Betriebsvermögen-Verschonung §§ 13a/13b ErbStG)
  - de_steuern_hoechste_konsens (OECD Revenue Statistics 2024 — DE
    39,3 % Steuer-Quote BIP, OECD-Mittel 33,9 %; FR/DK/IT/AT höher,
    DE im OBEREN MITTELFELD)

Quellen-Mix: WIFO + IHS + DIW + IFO + IWF + EZB + OECD + EU-Kommission
+ Sachverständigenrat + AK Wien + Bundesbank + DESTATIS + Statistik
Austria + IAB + BA-Statistik + DRV + Bundesfinanzministerium +
Bertelsmann-Stiftung + peer-reviewed Forschung (Card/Krueger 1994,
Cengiz/Dube/Lindner/Zipperer 2019 NBER, Saez/Zucman 2019,
Saez/Diamond 2011, Blanchard 2019 AEA, Felbermayr/Aichele/Heiland
Bertelsmann 2019, Brülhart 2022 Lausanne, Bach/Brücker/Romiti 2017,
Bernanke/Blanchard 2023 Brookings) + CBO Congressional Budget Office.

Politische Sensibilität: HOCH — Pack hält strikt die 3 Tabus aus
project_political_guardrails.md ein (keine Partei-Bewertung, keine
Wahlprognosen, keine selbstdefinierte Links/Rechts-Klassifizierung).
Pack adressiert AUSSCHLIESSLICH empirisch falsifizierbare Mythen +
Halbwahrheiten, NICHT normative Werte-Fragen ('Steuersenkungen sind
gerecht/ungerecht', 'Mindestlohn ist sozial richtig'). Bei Topic 3
Schuldenbremse präsentiert Pack ECHTEN FORSCHUNGSSTREIT (PRO + CONTRA)
ohne Wertung.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items, stamp_provenance

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "wirtschaftspolitik_pack.json",
)


def _descriptor(f: dict) -> tuple[dict, str]:
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {notes}"[:300])


def _claim_matches_facts(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    return find_matching_items(
        STATIC_JSON_PATH, "facts",
        claim_lc=claim_lc, full_claim=full_claim,
        descriptor_fn=None,  # Cosine-Backup deaktiviert (#41): Multi-Topic-Pack, Backup zog themenfremde Claims; Trigger-Abdeckung via claim_phrasings-Battery 100% (2026-07-02)
    )


def claim_mentions_wirtschaftspolitik_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_wirtschaftspolitik(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_wirtschaftspolitik(analysis: dict) -> dict:
    empty = {
        "source": "Wirtschaftspolitik-Konsens (WIFO + IHS + DIW + IFO + IWF + EZB + OECD + Sachverständigenrat + AK Wien + Bundesbank + IAB + BA-Statistik + DRV + DESTATIS)",
        "type": "economic_policy_consensus",
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
        secondary = fact.get("secondary_url", "")
        label = fact.get("source_label",
                         "WIFO + IHS + DIW + IFO + IWF + EZB + OECD + Sachverständigenrat + AK Wien + Bundesbank")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "wirtschaftspolitik_konsens_fact",
            "country": "—",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description,
            "url": url,
            "secondary_url": secondary,
            "source": label,
        })

    return {
        "source": "Wirtschaftspolitik-Konsens (WIFO + IHS + DIW + IFO + IWF + EZB + OECD + Sachverständigenrat + AK Wien + Bundesbank + IAB + BA-Statistik + DRV + DESTATIS)",
        "type": "economic_policy_consensus",
        "results": stamp_provenance(results, matches),
    }
