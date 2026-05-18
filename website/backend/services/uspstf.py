"""USPSTF — U.S. Preventive Services Task Force Recommendations API.

Datenquelle: https://www.uspreventiveservicestaskforce.org/ (AHRQ/HHS).
Der USPSTF ist ein unabhängiges Expertenpanel (16 freiwillige Mitglieder),
das US-Empfehlungen zu Screening, Beratung und präventiver Medikation auf
Basis von Evidenz-Reviews erstellt. ~120 aktive Empfehlungen.

Lizenz: US Public Domain (US Government Work, AHRQ). Vendor-Auflage:
„Any vendors presenting USPSTF work must reproduce the text verbatim,
without modification, and cite the source appropriately." — wir kürzen
keine Empfehlungstexte und zeigen Source-Link prominent.

API:
- Endpoint: https://data.uspreventiveservicestaskforce.org/api/json?key=<KEY>
- API-Key: OPTIONAL via env `USPSTF_API_KEY`. Seit 2021-03-01 wird ein
  Key benötigt, um die echten Empfehlungstexte zu erhalten; ohne Key
  liefert die API nur einen Platzhalter-Warnhinweis. Free Request via
  https://www.uspreventiveservicestaskforce.org/apps/api.jsp (E-Mail an
  uspstfpda@ahrq.gov).
- Format: JSON (ein einziger Dump mit allen ~120 Empfehlungen).
- Rate-Limits: nicht dokumentiert; wir cachen den Dump 24h und indexieren
  lokal — pro Tag also nur 1 echter Upstream-Call.

USPSTF-Grade-Skala (für Output verbatim wiederzugeben):
  A — Strong recommendation FOR (high certainty of substantial benefit)
  B — Moderate recommendation FOR (moderate-to-high certainty)
  C — Selective recommendation (small net benefit, individualisieren)
  D — Recommend AGAINST (no benefit / harm ≥ benefit)
  I — Insufficient evidence (kein Urteil möglich)

Hybrid-Pattern: Bei kleinem Corpus (~120 Empfehlungen) holen wir den
gesamten Dump einmalig, indexieren lokal nach Topic-Keywords (DE+EN),
und matchen Claims keyword-basiert. Damit ist die Service-Latenz ab dem
2. Hit unter ~5ms.

Politische / methodische Caveats:
- USPSTF ist eine US-Empfehlung — nicht 1:1 auf AT/EU übertragbar. AT-
  Gesundheitsministerium, ÖGGG, ÖGGH etc. weichen teils ab (z. B. Brust-
  krebs-Screening: USPSTF ab 40, AT ab 45 im organisierten Programm).
- USPSTF bewertet primär US-population-attributable benefit/harm —
  Inzidenz/Prävalenz können in EU/AT anders aussehen.
- Cross-Cluster: cochrane.py (systematic reviews, Evidenzbasis) +
  openfda.py (US-Drug-Daten, Pharmakovigilanz) + ema.py / basg.py (EU/AT-
  Pendants für Medikamente). USPSTF ist KEIN Ersatz für AT-Leitlinien.
"""

# WIRING für main.py (Vorschlag — KEIN Auto-Patch):
# from services.uspstf import search_uspstf, claim_mentions_uspstf_cached
# if claim_mentions_uspstf_cached(claim):
#     tasks.append(cached("USPSTF", search_uspstf, analysis))
#     queried_names.append("USPSTF")
#
# WIRING für services/reranker.py (Whitelist, falls vorhanden):
# "USPSTF" in SOURCE_WHITELIST
#
# WIRING für services/data_updater.py: nicht nötig — Service prefetcht den
# Dump on-demand in eigenem 24h-Cache; ein separater Prefetch-Job wäre
# zwar möglich (z. B. nightly), ist aber kein Muss.

from __future__ import annotations

import logging
import os
import re
import time
from functools import lru_cache

import httpx

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------
USPSTF_API_URL = "https://data.uspreventiveservicestaskforce.org/api/json"
USPSTF_API_KEY = os.getenv("USPSTF_API_KEY", "").strip()

TIMEOUT_S = 25.0
MAX_RESULTS = 5
CACHE_TTL_S = 24 * 60 * 60  # 24h

# Public canonical URLs (für Source-Links wenn API-Key fehlt)
USPSTF_HOMEPAGE = "https://www.uspreventiveservicestaskforce.org/"
USPSTF_GRADE_DEFS = (
    "https://www.uspreventiveservicestaskforce.org/uspstf/about-uspstf/"
    "methods-and-processes/grade-definitions"
)
USPSTF_TOPICS_INDEX = (
    "https://www.uspreventiveservicestaskforce.org/uspstf/topic_search_results"
)
USPSTF_API_REQUEST_INFO = (
    "https://www.uspreventiveservicestaskforce.org/apps/api.jsp"
)

# Modul-Caches
_dump_cache: tuple[float, dict | None] | None = None    # (ts, payload)
_query_cache: dict[str, tuple[float, list[dict]]] = {}  # cache_key → results


# ---------------------------------------------------------------------------
# Trigger-Vokabular (DE+EN)
# ---------------------------------------------------------------------------
_USPSTF_EXPLICIT_TERMS = (
    "uspstf",
    "u.s. preventive services task force",
    "us preventive services task force",
    "preventive services task force",
    "us-präventiv-task-force", "us präventiv task force",
    "ahrq-empfehlung", "ahrq empfehlung",
)

# Allgemeine Prävention-/Screening-Begriffe (DE+EN). Triggern NUR in
# Kombination mit einem Gesundheits-/Topic-Token.
_PREVENTION_TERMS = (
    "screening", "screenings", "screening-empfehlung", "screening empfehlung",
    "vorsorge", "vorsorgeuntersuchung", "vorsorge-untersuchung",
    "früherkennung", "frueherkennung",
    "prävention", "praevention", "präventiv", "praeventiv",
    "prevention", "preventive", "preventative",
    "supplementierung", "supplementation", "supplement",
    "prophylaxe", "prophylactic",
    "empfehlung mammographie", "empfehlung psa",
)

# Inhärent prävention-/screening-bezogene Test-/Maßnahmen-Namen, die ALLEIN
# (ohne extra "Screening"-/"Vorsorge"-Wort) eine USPSTF-Frage signalisieren,
# WENN der Claim eine Empfehlungs-/Sinnhaftigkeits-/Alters-Frage stellt.
_INHERENT_SCREENING_TOKENS = (
    "mammographie", "mammografie", "mammogram", "mammography",
    "psa-test", "psa test",
    "koloskopie", "colonoscopy",
    "pap-test", "pap test", "pap smear",
    "low-dose ct", "ldct",
    "hpv-test", "hpv test",
)

# Kontext-Heuristiken: "ist X sinnvoll", "ab welchem Alter", "wer braucht X",
# "empfohlen", "recommended". Im Verbund mit _INHERENT_SCREENING_TOKENS
# reicht das als Trigger.
_RECOMMENDATION_CONTEXT = (
    "sinnvoll", "empfohlen", "empfehlen", "empfehlung",
    "ab welchem alter", "ab wann", "wer braucht", "wer sollte",
    "soll man", "soll ich", "should i", "should one",
    "recommended", "recommend", "recommendation", "advised",
    "lohnt sich", "nützt", "nutzt", "nutzen", "benefit",
    "harm", "schaden", "übertherapie", "uebertherapie",
)

# Bekannte Topic-Tokens (DE+EN), die mit Prevention/Screening eine
# USPSTF-Frage ergeben. Wird auch fürs lokale Matching auf den Dump
# wiederverwendet.
_TOPIC_KEYWORDS: dict[str, list[str]] = {
    # Cancer-Screening
    "breast_cancer": [
        "brustkrebs", "mammakarzinom", "mammographie", "mammografie",
        "breast cancer", "mammogram", "mammography",
    ],
    "cervical_cancer": [
        "gebärmutterhalskrebs", "gebaermutterhalskrebs", "zervixkarzinom",
        "pap-test", "pap test", "hpv-test", "hpv test",
        "cervical cancer", "cervical screening",
    ],
    "colorectal_cancer": [
        "darmkrebs", "kolonkarzinom", "kolorektales karzinom",
        "darmkrebs-screening", "koloskopie", "colonoscopy",
        "colorectal cancer", "colon cancer",
    ],
    "lung_cancer": [
        "lungenkrebs", "bronchialkarzinom",
        "lung cancer", "low-dose ct", "ldct",
    ],
    "prostate_cancer": [
        "prostatakrebs", "prostatakarzinom", "psa-test", "psa test",
        "prostate cancer", "psa", "prostate-specific antigen",
    ],
    "skin_cancer": [
        "hautkrebs", "melanom", "melanoma",
        "skin cancer",
    ],
    "pancreatic_cancer": [
        "bauchspeicheldrüsenkrebs", "pankreaskarzinom",
        "pancreatic cancer",
    ],
    "ovarian_cancer": [
        "eierstockkrebs", "ovarialkarzinom",
        "ovarian cancer",
    ],
    "testicular_cancer": [
        "hodenkrebs", "testicular cancer",
    ],
    "thyroid_cancer": [
        "schilddrüsenkrebs", "schilddruesenkrebs",
        "thyroid cancer",
    ],
    "oral_cancer": [
        "mundhöhlenkrebs", "mundhoehlenkrebs",
        "oral cancer",
    ],
    "bladder_cancer": [
        "blasenkrebs", "bladder cancer",
    ],

    # Cardiovascular & metabolic
    "statin_prevention": [
        "statin", "statine", "statin prävention", "statin prevention",
        "statin use", "statin therapy",
    ],
    "aspirin_prevention": [
        "aspirin", "aspirin prävention", "acetylsalicylsäure prophylaxe",
        "aspirin use", "aspirin prevention",
    ],
    "hypertension": [
        "bluthochdruck", "hypertonie", "hypertension",
        "high blood pressure", "blood pressure screening",
    ],
    "lipid_screening": [
        "cholesterin", "cholesterol", "lipid", "lipide", "lipid screening",
        "blutfettwerte",
    ],
    "diabetes_screening": [
        "diabetes screening", "diabetes-screening", "prädiabetes",
        "praediabetes", "prediabetes", "type 2 diabetes screening",
    ],
    "obesity": [
        "adipositas", "übergewicht", "uebergewicht",
        "obesity", "overweight",
    ],
    "abdominal_aortic_aneurysm": [
        "bauchaortenaneurysma", "abdominal aortic aneurysm", "aaa",
    ],
    "atrial_fibrillation": [
        "vorhofflimmern", "atrial fibrillation",
    ],

    # Infectious diseases
    "hiv": ["hiv", "hiv-screening", "hiv screening", "hiv test"],
    "hepatitis_b": ["hepatitis b", "hbv", "hepatitis-b"],
    "hepatitis_c": ["hepatitis c", "hcv", "hepatitis-c"],
    "syphilis": ["syphilis"],
    "chlamydia_gonorrhea": [
        "chlamydien", "chlamydia", "gonorrhö", "gonorrhoe", "gonorrhea",
    ],
    "tuberculosis": [
        "tuberkulose", "latente tuberkulose", "latent tuberculosis", "ltbi",
    ],

    # Mental health
    "depression_screening": [
        "depression", "depressions-screening", "depression screening",
        "depressive störung",
    ],
    "anxiety_screening": [
        "angststörung", "angststoerung", "anxiety", "anxiety screening",
    ],
    "suicide_risk": [
        "suizidrisiko", "suicide risk", "suicide screening",
    ],
    "alcohol_use": [
        "alkoholkonsum", "alkohol screening", "alcohol use", "alcohol screening",
        "unhealthy alcohol use", "alcohol misuse",
    ],
    "tobacco_use": [
        "rauchentwöhnung", "rauchentwoehnung", "tabakentwöhnung",
        "tobacco cessation", "smoking cessation", "tobacco use",
    ],
    "illicit_drug_use": [
        "drogenkonsum", "illicit drug use", "unhealthy drug use",
    ],
    "intimate_partner_violence": [
        "häusliche gewalt", "haeusliche gewalt",
        "intimate partner violence", "ipv",
    ],

    # Nutrition / supplements
    "vitamin_d": [
        "vitamin d", "vitamin-d", "vitamin-d-supplement",
        "vitamin d supplementation",
    ],
    "vitamin_supplements": [
        "multivitamin", "vitaminpräparat", "vitaminpraeparat",
        "vitamin supplement", "vitamin supplements",
    ],
    "folic_acid": [
        "folsäure", "folsaeure", "folic acid",
    ],
    "calcium_vitamin_d": [
        "kalzium", "calcium", "calcium vitamin d",
    ],

    # Pediatric & obstetric
    "preeclampsia": [
        "präeklampsie", "praeeklampsie", "preeclampsia",
    ],
    "gestational_diabetes": [
        "schwangerschaftsdiabetes", "gestational diabetes",
    ],
    "perinatal_depression": [
        "wochenbettdepression", "perinatal depression",
        "postpartum depression",
    ],
    "rh_incompatibility": [
        "rhesus", "rh incompatibility",
    ],
    "lead_screening": [
        "blei-screening", "lead screening", "lead poisoning",
    ],
    "autism_screening": [
        "autismus screening", "autism screening",
    ],

    # Vision / hearing / osteo
    "osteoporosis_screening": [
        "osteoporose", "osteoporose-screening", "knochendichte",
        "osteoporosis", "osteoporosis screening", "bone density",
    ],
    "vision_children": [
        "sehscreening kinder", "vision screening", "amblyopia",
    ],
    "hearing_loss": [
        "hörscreening", "hoerscreening", "hearing screening",
        "hearing loss",
    ],

    # Misc
    "falls_prevention": [
        "sturzprävention", "sturzpraevention", "falls prevention",
        "falls in older adults",
    ],
    "dental_caries": [
        "kariesprävention", "kariespraevention", "dental caries",
        "fluoride varnish", "fluoride supplementation",
    ],
}


def _claim_mentions_uspstf(claim_lc: str) -> bool:
    """Trigger-Check auf bereits lowercase'tem Text.

    Strategie:
      1) Direkter USPSTF-Name → harter Hit.
      2) Prevention-Begriff + Health-Topic → Composite-Hit.
      3) Inhärenter Screening-Test (Mammographie, PSA-Test, Koloskopie, …)
         + Empfehlungs-/Sinnhaftigkeits-Kontext → Composite-Hit.
    """
    if not claim_lc:
        return False
    if any(t in claim_lc for t in _USPSTF_EXPLICIT_TERMS):
        return True

    has_prevention = any(t in claim_lc for t in _PREVENTION_TERMS)
    if has_prevention:
        for tokens in _TOPIC_KEYWORDS.values():
            for tok in tokens:
                if tok in claim_lc:
                    return True

    # 3) Inhärenter Screening-Test-Name + Empfehlungs-Kontext
    has_inherent = any(t in claim_lc for t in _INHERENT_SCREENING_TOKENS)
    if has_inherent and any(t in claim_lc for t in _RECOMMENDATION_CONTEXT):
        return True

    return False


@lru_cache(maxsize=2048)
def claim_mentions_uspstf_cached(claim: str) -> bool:
    """Public cache-fähiger Trigger-Check (LRU pro normalisiertem Claim)."""
    return _claim_mentions_uspstf((claim or "").lower())


# ---------------------------------------------------------------------------
# Topic-Extraktion aus Claim
# ---------------------------------------------------------------------------
def _detect_topics(claim_lc: str) -> list[str]:
    """Ermittle relevante Topic-Keys aus dem Claim (max 3)."""
    hits: list[str] = []
    for topic, tokens in _TOPIC_KEYWORDS.items():
        if any(t in claim_lc for t in tokens):
            hits.append(topic)
            if len(hits) >= 3:
                break
    return hits


def _topic_search_terms(topics: list[str]) -> list[str]:
    """Hole alle englischen Such-Terms zu den gefundenen Topics für das
    lokale Matching auf den USPSTF-Dump (der englisch indexiert ist)."""
    en_terms: list[str] = []
    for t in topics:
        for tok in _TOPIC_KEYWORDS.get(t, []):
            # Nur englische / ASCII-only Token in Dump-Matching nehmen
            if all(ord(c) < 128 for c in tok) and len(tok) >= 3:
                en_terms.append(tok.lower())
    return en_terms


# ---------------------------------------------------------------------------
# HTTP-Layer
# ---------------------------------------------------------------------------
async def _fetch_dump(client: httpx.AsyncClient) -> dict | None:
    """Hole den vollständigen USPSTF-JSON-Dump (mit 24h-Cache)."""
    global _dump_cache
    now = time.time()
    if _dump_cache and (now - _dump_cache[0]) < CACHE_TTL_S:
        return _dump_cache[1]

    params: dict[str, str] = {}
    if USPSTF_API_KEY:
        params["key"] = USPSTF_API_KEY
    try:
        resp = await client.get(USPSTF_API_URL, params=params, follow_redirects=True)
        if resp.status_code != 200:
            logger.info(f"USPSTF dump HTTP {resp.status_code}")
            _dump_cache = (now, None)
            return None
        data = resp.json()
        if not isinstance(data, dict):
            _dump_cache = (now, None)
            return None
        _dump_cache = (now, data)
        spec_count = len(data.get("specificRecommendations") or [])
        gen_count = len(data.get("generalRecommendations") or {})
        logger.info(
            f"USPSTF dump loaded: {spec_count} specific / {gen_count} general "
            f"recommendations (api_key={'yes' if USPSTF_API_KEY else 'no'})"
        )
        return data
    except Exception as e:
        logger.warning(f"USPSTF dump fetch failed: {type(e).__name__}: {e}")
        _dump_cache = (now, None)
        return None


def _is_placeholder(text: str) -> bool:
    """Erkenne den API-Key-Warnungs-Platzhalter, der ohne Key überall steht."""
    if not text or not isinstance(text, str):
        return False
    return "requires an API key" in text or text.startswith("! Starting")


def _has_real_data(dump: dict) -> bool:
    """Heuristik: Hat der Dump echte Daten oder nur den No-Key-Platzhalter?"""
    specs = dump.get("specificRecommendations") or []
    if not specs:
        return False
    # Wenn das erste spec ein Platzhalter ist, ist der gesamte Dump leer
    first = specs[0] if isinstance(specs[0], dict) else {}
    if _is_placeholder(first.get("title", "")) or _is_placeholder(
        first.get("text", "")
    ):
        return False
    return True


# ---------------------------------------------------------------------------
# Indexierung + Matching
# ---------------------------------------------------------------------------
_GRADE_DESCRIPTION = {
    "A": ("Empfehlung FÜR (stark): hohe Sicherheit, dass der Netto-Nutzen "
          "substantiell ist."),
    "B": ("Empfehlung FÜR (moderat): mittlere bis hohe Sicherheit, dass "
          "der Netto-Nutzen moderat bis substantiell ist."),
    "C": ("Selektive Empfehlung: kleiner Netto-Nutzen — individuelle "
          "Patientensituation entscheidet."),
    "D": ("Empfehlung GEGEN: kein Nutzen / Schaden ≥ Nutzen."),
    "I": ("Unzureichende Evidenz: aktueller Forschungsstand erlaubt keine "
          "Empfehlung FÜR oder GEGEN."),
}


def _normalize_grade(raw: object) -> str:
    """Mappe Grade-Field auf A/B/C/D/I (oder ''-empty)."""
    if not raw:
        return ""
    g = str(raw).strip().upper()
    # Manche Einträge: 'A or B', 'I statement' etc.
    for candidate in ("A", "B", "C", "D", "I"):
        if g.startswith(candidate):
            return candidate
    return ""


def _flatten_recommendations(dump: dict) -> list[dict]:
    """Bringe specific + general Recommendations in eine flache Liste.

    Felder pro Eintrag (best-effort, abhängig vom Upstream-Schema):
      title, grade, text, rationale, riskName, gender, ageRange, topic, url
    """
    out: list[dict] = []

    # general: dict {id: {topic, title, rationale, clinical, ...}}
    general = dump.get("generalRecommendations") or {}
    if isinstance(general, dict):
        for gid, g in general.items():
            if not isinstance(g, dict):
                continue
            title = (g.get("title") or g.get("topic") or "").strip()
            if not title or _is_placeholder(title):
                continue
            out.append({
                "_id": f"gen:{gid}",
                "title": title,
                "topic": (g.get("topic") or "").strip(),
                "keywords": (g.get("keywords") or "").strip(),
                "rationale": (g.get("rationale") or "").strip(),
                "clinical": (g.get("clinical") or "").strip(),
                "pubDate": (g.get("pubDate") or "").strip(),
                "uspstfAlias": (g.get("uspstfAlias") or "").strip(),
                "topicYear": g.get("topicYear"),
                "specific_ids": g.get("specific") or [],
                "_type": "general",
            })

    # specific: list of {id, title, grade, text, rationale, gender, ageRange, ...}
    specific = dump.get("specificRecommendations") or []
    if isinstance(specific, list):
        for s in specific:
            if not isinstance(s, dict):
                continue
            title = (s.get("title") or "").strip()
            text = (s.get("text") or "").strip()
            if (not title and not text) or _is_placeholder(title):
                continue
            grade = _normalize_grade(s.get("grade"))
            out.append({
                "_id": f"spec:{s.get('id')}",
                "title": title,
                "grade": grade,
                "grade_raw": str(s.get("grade") or "").strip(),
                "text": text,
                "rationale": (s.get("rationale") or "").strip(),
                "riskName": (s.get("riskName") or "").strip(),
                "gender": (s.get("gender") or s.get("sex") or "").strip(),
                "ageRange": s.get("ageRange") or [],
                "servFreq": (s.get("servFreq") or "").strip(),
                "general_ref": s.get("general"),
                "_type": "specific",
            })

    return out


def _score_record(rec: dict, search_terms: list[str]) -> int:
    """Sehr einfaches Term-Frequency-Scoring. Höher = relevanter."""
    if not search_terms:
        return 0
    hay = " ".join([
        rec.get("title", ""),
        rec.get("topic", ""),
        rec.get("keywords", ""),
        rec.get("text", ""),
        rec.get("rationale", ""),
        rec.get("uspstfAlias", ""),
    ]).lower()
    if not hay:
        return 0
    score = 0
    for t in search_terms:
        if t in hay:
            # Title-Match besonders stark gewichten
            if t in rec.get("title", "").lower():
                score += 5
            else:
                score += 2
    return score


def _build_result(rec: dict) -> dict:
    """Wandle einen Recommendations-Record in ein Evidora-Result um."""
    grade = rec.get("grade") or ""
    grade_label = _GRADE_DESCRIPTION.get(grade, "")
    title = rec.get("title") or rec.get("topic") or "USPSTF Recommendation"
    text = rec.get("text") or rec.get("rationale") or ""
    risk = rec.get("riskName") or ""
    gender = rec.get("gender") or ""
    age = rec.get("ageRange") or []
    age_str = ""
    if isinstance(age, list) and len(age) == 2:
        try:
            lo, hi = int(age[0]), int(age[1])
            if (lo, hi) != (0, 100):
                age_str = f"{lo}–{hi} Jahre"
        except Exception:
            age_str = ""

    # Headline: "USPSTF Grade B: Mammography for women 50-74"
    headline_bits = ["USPSTF"]
    if grade:
        headline_bits.append(f"Grade {grade}")
    headline_bits.append(title)
    display = ": ".join([headline_bits[0], " — ".join(headline_bits[1:])]) \
        if len(headline_bits) > 1 else headline_bits[0]
    if len(display) > 300:
        display = display[:297] + "..."

    # Vollständige Beschreibung (USPSTF-Verbatim-Auflage einhalten — wir
    # kürzen den Empfehlungstext NICHT bzw. nur am hinteren Ende mit
    # explizitem '…'-Marker, und verweisen prominent auf Original-URL).
    description_parts: list[str] = []
    if grade and grade_label:
        description_parts.append(f"Grade {grade}: {grade_label}")
    if text:
        description_parts.append(f"Empfehlung (Original-Text): „{text}\"")
    if rec.get("rationale"):
        rationale = rec["rationale"]
        if len(rationale) > 400:
            rationale = rationale[:397] + "..."
        description_parts.append(f"Rationale: {rationale}")
    pop_bits = []
    if gender:
        pop_bits.append(gender)
    if age_str:
        pop_bits.append(age_str)
    if risk:
        pop_bits.append(f"Risiko: {risk}")
    if pop_bits:
        description_parts.append(f"Population: {', '.join(pop_bits)}.")
    if rec.get("servFreq"):
        description_parts.append(f"Frequenz: {rec['servFreq']}.")
    if rec.get("pubDate"):
        description_parts.append(f"Publiziert: {rec['pubDate']}.")
    description_parts.append(
        "Quelle: U.S. Preventive Services Task Force (AHRQ). "
        "Hinweis: US-Empfehlung — nicht 1:1 auf Österreich/EU übertragbar; "
        "AT-Leitlinien (z. B. ÖGGG für Mammographie ab 45) können abweichen."
    )

    # Topic-Detail-URL aus Alias bauen, falls vorhanden
    alias = rec.get("uspstfAlias") or ""
    if alias:
        topic_url = (
            f"https://www.uspreventiveservicestaskforce.org/uspstf/"
            f"recommendation/{alias}"
        )
    else:
        topic_url = USPSTF_TOPICS_INDEX

    return {
        "title": display,
        "type": "preventive_recommendation",
        "grade": grade,
        "topic": rec.get("topic") or "",
        "url": topic_url,
        "summary": " · ".join(description_parts),
        "population": ", ".join(pop_bits) if pop_bits else "",
        "source": "USPSTF (U.S. Preventive Services Task Force)",
    }


# ---------------------------------------------------------------------------
# Result-Caveats
# ---------------------------------------------------------------------------
def _caveat_result() -> dict:
    return {
        "title": "WICHTIGER KONTEXT: USPSTF-Empfehlungen",
        "type": "context",
        "url": USPSTF_GRADE_DEFS,
        "summary": (
            "Der USPSTF (U.S. Preventive Services Task Force) ist ein "
            "unabhängiges Expertenpanel der US-Regierung (AHRQ/HHS) und "
            "publiziert Prävention-Empfehlungen für primärmedizinische "
            "Versorgung in den USA. Skala: "
            "A=für (stark), B=für (moderat), C=selektiv, D=gegen, "
            "I=unzureichende Evidenz. "
            "Einschränkungen: (1) US-Empfehlungen sind NICHT automatisch "
            "deckungsgleich mit AT/EU-Leitlinien — z. B. Brustkrebs-"
            "Screening: USPSTF empfiehlt ab 40 (Grade B), AT-organisiertes "
            "Programm startet ab 45. (2) USPSTF bewertet primär den "
            "Netto-Nutzen in der US-Allgemeinbevölkerung; Risiko-Subgruppen "
            "werden separat ausgewiesen. (3) Grade I bedeutet NICHT 'tut "
            "es nicht', sondern 'aktuell keine ausreichende Evidenz für "
            "Pro/Kontra'. (4) USPSTF-Empfehlungen werden alle 5–7 Jahre "
            "überarbeitet — Stand der Evidenz kann sich ändern."
        ),
    }


def _no_api_key_result() -> dict:
    """Fallback-Result, wenn kein API-Key gesetzt ist (Dump ist dann ein
    Platzhalter)."""
    return {
        "title": (
            "USPSTF-Empfehlungen verfügbar (API-Key für Detailtexte "
            "erforderlich)"
        ),
        "type": "preventive_recommendations_meta",
        "url": USPSTF_TOPICS_INDEX,
        "summary": (
            "Die U.S. Preventive Services Task Force (USPSTF, AHRQ/HHS) "
            "publiziert ~120 Prävention-Empfehlungen mit Grade-A/B/C/D/I-"
            "Bewertung. Die Empfehlungstexte sind im Web öffentlich, der "
            "JSON-Endpoint benötigt seit 2021-03-01 einen kostenlosen API-"
            "Key (Anfrage via uspstfpda@ahrq.gov, siehe "
            f"{USPSTF_API_REQUEST_INFO}). "
            "Grade-Skala: A=für (stark), B=für (moderat), C=selektiv, "
            "D=gegen, I=unzureichende Evidenz. "
            "Hinweis: USPSTF ist eine US-Empfehlung — AT-Leitlinien können "
            "abweichen (ÖGGG, ÖGGH, BMSGPK-Vorsorge-Untersuchung). "
            "Für Detail-Recherche siehe direkt: " + USPSTF_TOPICS_INDEX
        ),
        "source": "USPSTF (U.S. Preventive Services Task Force)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_uspstf(analysis: dict) -> dict:
    """Search USPSTF Prevention Recommendations for the given claim.

    Returns matching A/B/C/D/I-graded recommendations or, if no API-Key
    is configured, a meta-result pointing to the public web index.
    """
    empty = {
        "source": "USPSTF",
        "type": "preventive_recommendations",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    if not isinstance(claim, str):
        claim = str(claim or "")
    if not isinstance(original, str):
        original = str(original or "")
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_uspstf(matchable):
        return empty

    topics = _detect_topics(matchable)

    # Cache-Lookup
    cache_key = (
        f"uspstf::"
        f"{','.join(sorted(topics))}::"
        f"key={'1' if USPSTF_API_KEY else '0'}"
    )
    now = time.time()
    cached = _query_cache.get(cache_key)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return {
            "source": "USPSTF",
            "type": "preventive_recommendations",
            "results": cached[1],
        }

    async with polite_client(timeout=TIMEOUT_S) as client:
        dump = await _fetch_dump(client)

    # Ohne API-Key (oder leerer Dump) → meta-result + caveat
    if not dump or not _has_real_data(dump):
        results = [_no_api_key_result(), _caveat_result()]
        _query_cache[cache_key] = (now, results)
        logger.info(
            f"USPSTF: meta-result (api_key={'yes' if USPSTF_API_KEY else 'no'}, "
            f"dump={'ok' if dump else 'fail'}, topics={topics})"
        )
        return {
            "source": "USPSTF",
            "type": "preventive_recommendations",
            "results": results,
        }

    # Mit echten Daten → indexieren + scoren
    flat = _flatten_recommendations(dump)
    search_terms = _topic_search_terms(topics)

    scored: list[tuple[int, dict]] = []
    for rec in flat:
        score = _score_record(rec, search_terms)
        if score > 0:
            scored.append((score, rec))

    scored.sort(key=lambda x: x[0], reverse=True)
    top = [rec for _s, rec in scored[:MAX_RESULTS]]

    if not top:
        logger.info(
            f"USPSTF: 0 matches in dump (topics={topics}, "
            f"search_terms={search_terms[:5]})"
        )
        results = [_caveat_result()]
    else:
        results = [_build_result(r) for r in top]
        results.append(_caveat_result())
        logger.info(
            f"USPSTF: {len(top)} matches (topics={topics}, "
            f"top_score={scored[0][0]})"
        )

    _query_cache[cache_key] = (now, results)
    return {
        "source": "USPSTF",
        "type": "preventive_recommendations",
        "results": results,
    }
