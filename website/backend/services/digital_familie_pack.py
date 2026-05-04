"""Digital-Familie-Pack — kuratierte Konsens-Daten zu Bildschirmzeit-,
Social-Media-, Online-Mythen für Kinder/Jugendliche.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: medizinisch-/entwicklungspsychologisch-statistische
Bildungs-Konsens-Aussagen (peer-reviewed, AAP/APA/Orben-Oxford/EU Kids
Online-gestützt). Bewusst RAUS: Schul-Smartphone-Verbots-Debatten
(Politik-Frage), spezifische Datenschutz-Fragen (separater Diskurs).

Topics (10):
  - bildschirmzeit_hirnentwicklung_mythos (NUANCED — ABCD Study,
    Orben 2019 Nat Hum Behav SCA d=0,05)
  - tiktok_algorithmus_dumm_mythos (NUANCED — Maza 2023 JAMA Pediatr,
    Verdrängungs-Hypothese)
  - smartphone_macht_dumm_mythos (NUANCED — Ward 2017 Brain Drain
    nicht konsistent reproduzierbar; Bowman 2021, Ruiz Pardo 2023)
  - kinderschutz_filter_wirksamkeit_mythos (FALSE — Filter 30-40%
    umgangen; EU Kids Online; BzKJ Triple-A Empfehlung)
  - online_spiele_aggression_mythos (FALSE — APA 2020 Resolution
    revidiert; Hilgard 2017, Drummond 2020 longitudinal)
  - gen_z_iphone_depression_mythos (NUANCED — Twenge iGen vs.
    Orben/Vuorre Replikations-Probleme; multifaktoriell)
  - cyberbullying_mehr_als_offline_mythos (FALSE — Olweus 2018,
    Modecki 2014 Meta; klassisches Bullying häufiger)
  - social_media_addiction_mythos (FALSE — DSM-5-TR/ICD-11 keine
    eigene Diagnose; Internet Gaming Disorder spezifisch ICD-11)
  - frueh_lesen_lernen_smartphone_mythos (FALSE — Hattie d=0,65
    Vorlesen vs. d=0,1 Apps allein; EEF, Dialogic Reading)
  - kinder_smartphone_alter_mythos (NUANCED — keine empirische
    14-Schwelle; AAP Family Media Plan; gestaffelte Einführung)

Quellen-Mix: AAP Council on Communications and Media, APA Task Force
on Violent Media 2020, Orben & Przybylski Oxford Internet Institute,
Adolescent Brain Cognitive Development (ABCD) Study NIH, EU Kids Online
LSE Livingstone, Common Sense Media, BzKJ (Bundeszentrale für Kinder-
und Jugendmedienschutz DE), jugendschutz.net, Hattie Visible Learning,
EEF (Education Endowment Foundation UK), Cochrane Reviews, NICE digital
media guidance, NIMH, DSM-5-TR (APA), ICD-11 (WHO), Olweus Aggression
Research Center, Hinduja-Patchin Cyberbullying Research Center, Dan
Olweus Bullying Prevention Program (OBPP).

Politische Sensibilität: niedrig bis mittel (Twenge-iGen-Debatte,
Schul-Smartphone-Verbote politisch heikel — wir bleiben streng bei
peer-reviewed Daten + Replikations-Status). Hohe Lehrer-Relevanz
für Schul-/Erziehungs-Kontext AT/DE.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "digital_familie_pack.json",
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


def claim_mentions_digital_familie_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_digital_familie(client=None):
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


async def search_digital_familie(analysis: dict) -> dict:
    empty = {
        "source": "Digital-Familie-Konsens (AAP + APA + Orben Oxford + ABCD Study + EU Kids Online + BzKJ)",
        "type": "digital_family_consensus",
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
        label = fact.get("source_label", "AAP / APA / Orben Oxford / ABCD Study / EU Kids Online / BzKJ")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "digital_familie_konsens_fact",
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
        "source": "Digital-Familie-Konsens (AAP + APA + Orben Oxford + ABCD Study + EU Kids Online + BzKJ)",
        "type": "digital_family_consensus",
        "results": results,
    }
