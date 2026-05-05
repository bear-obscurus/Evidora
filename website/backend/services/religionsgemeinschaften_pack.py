"""Religionsgemeinschaften-Pack — kuratierte Konsens-Daten zu
Religions-Mythen + Sekten + religiöser Gewalt + DACH-Religionsrecht.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: empirisch-deskriptive Konsens-Aussagen zu
Christentum/Islam/Judentum/Buddhismus/Hinduismus + Sekten/Kulten +
Vatikan-Finanzen + Kirchensteuer/Kirchenbeitrag + Religion-Gewalt-
Korrelation + Missbrauchs-Skandalen + Religionsunterricht.

Topics (14):
  - christen_aussterben_dach_konsens (DACH-Säkularisierung real,
    globaler Christen-Anteil wächst absolut)
  - islam_mehrheit_radikal_mythos (Pew-Daten + Verfassungsschutz —
    Mehrheit lehnt Gewalt ab, Salafismus quantitativ klein)
  - sharia_imminent_europa_mythos (EGMR Refah Partisi 2003 + Art. 4
    GG — Sharia rechtlich unmöglich in Europa)
  - antisemitismus_verschwoerung_konsens (ADL Global 100 + IHRA
    Definition + RIAS-Statistiken)
  - buddhismus_friedlich_mythos (Myanmar-Genozid + Sri Lanka +
    Wirathu — buddhistische Praxis nicht inhärent friedlich)
  - hinduismus_kasten_mythos (Pew India 2021 + NCRB — Kastensystem
    rechtlich abgeschafft 1950, faktisch fortbestehend)
  - vatikan_vermoegen_mythos (IOR Bilanz 5,3 Mrd vs. Apple 3,5 Bio —
    Vatikan ist NICHT reichste Institution)
  - kirchensteuer_kirchenbeitrag_konsens (DE staatlich erhoben ~12,8
    Mrd, AT kirchlich erhoben ~530 Mio, CH kantonal — KEIN Steuergeld)
  - scientology_sekten_status_dach_konsens (Bayern + 14 Bundesländer
    Verfassungsschutz seit 1997, BAG 1997 — keine Religion)
  - zeugen_jehovas_blut_konsens (BGH-Rechtsprechung — Familiengericht
    überschreibt Eltern-Willen bei lebensbedrohlicher Kinder-Indikation)
  - destruktive_kulte_kennzeichen_konsens (BITE-Modell Hassan + ICSA-
    Charakteristika + Beispiele Aum/Heaven Gate/Universelles Leben)
  - religion_gewalt_korrelation_konsens (Phillips Axelrod 2007 — nur
    7 % der Kriege primär religiös; säkulare Ideologien 20. Jh.
    quantitativ tödlicher)
  - kirchen_paedophilie_aufarbeitung_konsens (MHG-Studie 2018 +
    John-Jay-Report 2004 + Klasnic-Kommission AT — 4-7 % der
    Geistlichen betroffen, dokumentierte Vertuschung)
  - religionsunterricht_dach_schule_konsens (Art. 7 GG + Konkordat
    1933 + B-VG Art. 17 — verfassungsgarantiert mit Ethik-Alternative)

Quellen-Mix: Pew Research Center, BfV/Verfassungsschutz DE/AT/CH,
ADL Anti-Defamation League, IHRA International Holocaust Remembrance
Alliance, RIAS Bundesverband, RAND Corporation, UCDP Uppsala Conflict
Data Program, OHCHR UN Fact-Finding Missions, NCRB India,
Vatikan-Finanzbericht, IOR Bilanz, DBK + EKD + Erzdiözesen DACH,
DESTATIS + Statistik Austria + BFS Konfessions-Statistik, BMI Bayern
+ Hamburger Verfassungsschutz + AT Bundesstelle für Sektenfragen,
ICSA International Cultic Studies Association, MHG-Studie 2018,
John-Jay-Report 2004, Pennsylvania Grand Jury Report 2018,
Klasnic-Kommission AT, CIASE Sauvé-Kommission Frankreich 2021,
peer-reviewed Forschung (Hassan BITE, Singer Cults, Juergensmeyer
Terror in the Mind of God, Pinker Better Angels, Phillips Axelrod
Encyclopedia of Wars, Stanley Tambiah, Michael Jerryson Buddhist
Warfare, Brian Daizen Victoria Zen at War).

Politische Sensibilität: HOCH — Pack hält strikt die 3 Tabus aus
project_political_guardrails.md ein (keine Partei-Bewertung, keine
Wahlprognosen, keine selbstdefinierte Links/Rechts-Klassifizierung).
Pack distanziert sich von theologischen Wertungen + Religion-pro-
oder-contra-Positionen, präsentiert nur empirische Studien-Lage,
Behörden-Bewertungen und Konsens-Daten.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "religionsgemeinschaften_pack.json",
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


def claim_mentions_religionsgemeinschaften_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_religionsgemeinschaften(client=None):
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


async def search_religionsgemeinschaften(analysis: dict) -> dict:
    empty = {
        "source": "Religionsgemeinschaften-Konsens (Pew + BfV + ADL + IHRA + RAND + UCDP + Vatikan-Finanzbericht + DESTATIS + Statistik Austria + Sektenstellen)",
        "type": "religion_consensus",
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
                         "Pew + BfV + ADL + IHRA + RAND + UCDP + Vatikan-Finanzbericht + DESTATIS + Statistik Austria + Sektenstellen")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "religionsgemeinschaften_konsens_fact",
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
        "source": "Religionsgemeinschaften-Konsens (Pew + BfV + ADL + IHRA + RAND + UCDP + Vatikan-Finanzbericht + DESTATIS + Statistik Austria + Sektenstellen)",
        "type": "religion_consensus",
        "results": results,
    }
