"""Verkehrssicherheit-Pack — kuratierte Konsens-Daten zu klassischen
Verkehrs-Mythen.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: physikalisch + statistisch belegbare Verkehrs-
sicherheits-Konsens-Aussagen (WHO/OECD-IRTAD/BASt/ADAC/KFV-gestützt).

Topics (10):
  - tempo_30_kindersicherheit_konsens (TRUE — WHO + ETSC, Pasanen-
    Kurve 50→30 km/h reduziert Mortalität 50-80%)
  - helmpflicht_radfahrer_kontroverse_mythos (NUANCED — Cochrane:
    individueller Helm-Schutz ja, populationsweite Pflicht-Effekte
    komplex; Olivier 2017)
  - kindersitz_alter_groesse_konsens (TRUE — ECE R129 i-Size,
    NHTSA: 71% Säuglings-Mortalitäts-Reduktion)
  - tagfahrlicht_unfall_reduktion_mythos (TRUE — Elvik 2009 review,
    5-15% Reduktion Tag-Unfälle)
  - promille_grenzen_alkohol_kontext (TRUE — Borkenstein 1964 +
    Compton 2015 NHTSA, dosis-abhängiges Risiko)
  - mobile_telefon_freisprech_konsens (TRUE — Strayer 2003 Univ
    Utah: hands-free ≈ handheld in kognitiver Beeinträchtigung)
  - rote_ampel_kein_verkehr_mythos (FALSE — IIHS, NHTSA: 1.000
    USA-Verkehrstote/Jahr durch Rotlicht-Verstöße)
  - mued_fahren_promille_aequivalent_mythos (TRUE — Williamson
    2000 Occup Environ Med: 24h ohne Schlaf ≈ 1,0 Promille)
  - bremsweg_geschwindigkeit_quadratisch_konsens (TRUE — Physik
    E=½mv², ADAC-Tabellen, StVO-Faustformel)
  - winterreifen_pflicht_konsens (TRUE — ADAC + ÖAMTC: Bremsweg-
    Reduktion 30-50% auf Schnee/Eis; DE § 2 Abs. 3a StVO + AT § 102
    Abs. 8a KFG situative Pflicht)

Quellen-Mix: WHO Speed Management 2008, OECD-IRTAD, ETSC European
Transport Safety Council, BASt Bundesanstalt für Straßenwesen, ADAC
+ ÖAMTC + TCS Reifen-/Kindersitz-Tests, KFV Kuratorium für Verkehrs-
sicherheit AT, IIHS Insurance Institute for Highway Safety, NHTSA
National Highway Traffic Safety Administration, AAA Foundation,
Cochrane Reviews, EU-Verordnung 1004/2008 (DRL), ECE R129 i-Size,
Borkenstein 1964 Grand Rapids Studie, Compton 2015 NHTSA, Williamson
2000 Occup Environ Med, Dawson 1997 Nature, Strayer 2003 Univ Utah,
Pasanen 1992 Helsinki Univ Tech, Elvik 2009 review, Retting 2003
NHTSA, Olivier 2017 Int J Epidemiol.

Politische Sensibilität: sehr niedrig — alle physikalisch-statistisch
belegt. Hohe Lebensalltag-Relevanz für Verkehrs-Teilnehmer:innen.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "verkehrssicherheit_pack.json",
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


def claim_mentions_verkehrssicherheit_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_verkehrssicherheit(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    parts: list[str] = []
    for key, val in d.items():
        if key == "context":
            continue
        if isinstance(val, str) and val.strip():
            label = key.replace("_", " ").strip()
            parts.append(f"{label.capitalize()}: {val}")
    return " | ".join(parts)


async def search_verkehrssicherheit(analysis: dict) -> dict:
    empty = {
        "source": "Verkehrssicherheit-Konsens (WHO + OECD-IRTAD + BASt + ADAC + KFV + IIHS + NHTSA)",
        "type": "road_safety_consensus",
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
        label = fact.get("source_label", "WHO / OECD-IRTAD / BASt / ADAC / KFV / IIHS / NHTSA")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "verkehrssicherheit_konsens_fact",
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
        "source": "Verkehrssicherheit-Konsens (WHO + OECD-IRTAD + BASt + ADAC + KFV + IIHS + NHTSA)",
        "type": "road_safety_consensus",
        "results": results,
    }
