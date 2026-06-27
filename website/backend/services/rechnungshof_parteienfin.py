"""Rechnungshof Parteienfinanzierung AT — Static-First-Topic-Service mit
kuratiertem Snapshot 2026-05-24 zur österreichischen Parteienfinanzierung
(Bundes-Parteienförderung + Klubförderung + Parteiakademien + Wahlkampfkosten
+ Spenden-Schwellen + Sanktionen + Rechtsgrundlagen).

Hintergrund: Der österreichische Rechnungshof veröffentlicht zwar
Parteispenden-Listen + Rechenschaftsberichte, hat aber KEINE API + KEIN
CSV-Export — alle Primärdaten liegen als jährliche PDFs vor (siehe
hard_to_implement.md). Dieser Pack ist ein Quick-Win-Sekundär-Spiegel,
der Parlament + Bundeskanzleramt + JUSLINE (PartG-Volltext) als
strukturierte Sekundär-Quellen aggregiert.

Datenquellen-Stack:
  - Parteiengesetz 2012 (PartG, BGBl I Nr 56/2012, Stand 2024 inkl.
    Novellen BGBl I 38/2019 + BGBl I 56/2022) — JUSLINE-Volltext
  - Parteien-Förderungsgesetz 2012 (PartFörG)
  - Bundeskanzleramt — Parteien- und Parteiakademienförderung (Vollzug)
  - Rechnungshof Österreich — Kontrolle der Parteien + Parteispenden-
    Listen 2024 + Nationalratswahl-2024-Kontrolle
  - Parlament Österreich — Fachinfos Rechtswissenschaft ('Was ist neu
    im Bereich der Parteienfinanzierung?' + 'Wie sind die Gründung und
    Finanzierung von Parteien geregelt?')
  - ORF.at + meinbezirk.at + Tiroler Tageszeitung als Berichterstatt-
    ungs-Sekundärbelege (für Auszahlungs-Aggregate 2024)

Pack-Snapshot 2026-05-24 enthält 11 Kern-Facts:
  - parteiengesetz_partg_2012_rechtsgrundlage (PartG + PartFörG + UPTS-
    Zuständigkeit)
  - bundes_parteienfoerderung_2024_konsens (37,2 Mio EUR + Aufschlüsselung
    ÖVP/SPÖ/FPÖ/Grüne/NEOS — deskriptive Vollzugs-Daten BKA)
  - partg_par3_pro_wahlberechtigtem_rahmen (3,10-11 EUR-Rahmen aus § 3 PartG)
  - wahlkampfkostengrenze_nationalratswahl_konsens (8,66 Mio EUR NR-Wahl
    2024 + Valorisierung-Hintergrund)
  - spenden_meldeschwelle_partg_par6 (165 EUR Meldung + 540 EUR Veröffentl.
    + 7.500 EUR Einzelspende-Cap + 750.000 EUR Gesamt-Cap)
  - sanktionen_partg_par10 (UPTS-Geldbußen-Schema: 3-fach unzulässige
    Spende / bis 50.000 Partei / bis 50.000 Manager / bis 15.000 Spender)
  - rechenschaftsbericht_pflicht_partg_par8 (jährliche Bericht-Pflicht
    + 30.9.-Frist + Wirtschaftsprüfer-Testat)
  - rechnungshof_kontrolle_parteien (formale Rechnungshof-Rolle)
  - klubfoerderung_kfg_2024 (28,6 Mio EUR Klub-Topf — KFG-Rechtsgrundlage)
  - parteiakademien_partakadg_2024 (12 Mio EUR Akademien-Topf + 5 Akademien)
  - parteienfin_total_2024_kontext (ca. 273 Mio EUR Aggregat-Schätzung
    Bund + Länder + Klub + Akademien + EU-Wahl-Sonder)

Komplementär zu existierenden Quellen:
  - services/medientransparenz.py (KommAustria-Medien-Transparenz —
    überlappt NICHT, dort geht es um Medien-Kooperationen) — keine
    Doppelung.
  - Wikipedia-Cap-Pattern aus confidence_calibration.py greift bei
    normativen Politik-Klassifikatoren — dieser Pack ist deskriptiv-
    statistisch, kein normativer Klassifikator-Pack.

GUARDRAILS — Politik-Tabu (siehe project_political_guardrails.md):
  - Pack hält strikt die 4 Tabus ein:
    * Keine Partei-Bewertung (Förderhöhen werden deskriptiv aufgelistet,
      KEINE Wertung 'kassiert zu viel' / 'angemessen' / 'zu wenig')
    * Keine Prognose (nur normierte + ausgezahlte Beträge, keine
      Vorhersage zukünftiger Förder-Höhen)
    * Keine eigene normative Klassifikation (Empfänger-Parteien werden
      neutral genannt — keine Links/Rechts-Einordnung)
    * Bei Spenden-Daten: NUR Schwellen-Werte aus § 6 PartG zitiert,
      KEINE spekulativen Spender-Listen oder Personen-Daten. Für
      konkrete Spender-Recherche ist Primärquelle rechnungshof.gv.at.
  - KEIN STRUKTURELL-FALSCH-Marker — Pack ist deskriptiv-only, hat
    keine Mythen-Widerlegungs-Logik.

Verdict-Strategie:
  - "Die SPÖ bekam 2024 X EUR öffentliche Mittel" → true/false basierend
    auf konkreter Zahl-Plausibilität gegen den Pack.
  - "ÖVP wird subventioniert" → mixed (politische Wertungsfrage, formaler
    Tatbestand korrekt aber 'subventioniert' impliziert Wertung).
  - "Parteienförderung in AT ist zu hoch/niedrig" → mixed (Wertungsfrage,
    kein faktischer Verdict möglich).

Static-First-Topic-Pattern: substring/composite-Match (siehe
services/_topic_match.py) mit Reranker-Backup-Fallback.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items, stamp_provenance

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "rechnungshof_parteienfin.json",
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


def claim_mentions_rechnungshof_parteienfin_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_rechnungshof_parteienfin(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Der Pack ist überwiegend deskriptiv (keine FALSE-Verdict-Overrides
    geplant — Politik-Tabu verbietet Bewertungs-Marker bei Parteien-
    Daten), aber wir nutzen denselben Helper für konsistentes Rendering.
    Falls zukünftige Updates einen normativen Mythos hinzufügen
    (z.B. eindeutig falsche Zahlen-Behauptungen), greift der Helper
    automatisch.
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_rechnungshof_parteienfin(analysis: dict) -> dict:
    empty = {
        "source": "Rechnungshof Österreich (Parteispenden + Rechenschaftsberichts-Veröffentlichung) + Bundeskanzleramt (PartFörG-Vollzug) + Parlament Österreich (Fachinfos) + PartG/PartFörG 2012",
        "type": "parteienfinanzierung_at",
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
        label = fact.get(
            "source_label",
            "Rechnungshof Österreich + Bundeskanzleramt + Parlament Österreich + PartG 2012",
        )
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "rechnungshof_parteienfin_fact",
            "country": "AT",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description,
            "url": url,
            "secondary_url": secondary,
            "source": label,
        })

    return {
        "source": "Rechnungshof Österreich (Parteispenden + Rechenschaftsberichts-Veröffentlichung) + Bundeskanzleramt (PartFörG-Vollzug) + Parlament Österreich (Fachinfos) + PartG/PartFörG 2012",
        "type": "parteienfinanzierung_at",
        "results": stamp_provenance(results, matches),
    }
