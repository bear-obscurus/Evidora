"""openFDA — US Food and Drug Administration open-data API.

Datenquelle: https://open.fda.gov/ — Public-Domain-Datensätze der FDA
zu Arzneimitteln, Medizinprodukten, Nahrungsmitteln und Tabakprodukten.
Lizenz: Public Domain (US Government Work, 17 U.S.C. § 105) —
Evidora-/CC-tauglich.

Genutzte Endpunkte (4 von 14 verfügbaren):
- /drug/event.json       — FAERS (FDA Adverse Event Reporting System):
                           Berichte zu unerwünschten Arzneimittelwirkungen.
- /drug/label.json       — Strukturierte Beipackzettel (SPL):
                           Wirkstoff, Indikation, Warnhinweise.
- /drug/enforcement.json — Recalls/Rückrufe von Arzneimitteln (Class I/II/III).
- /device/enforcement.json — Medizinprodukt-Rückrufe.

Authentifizierung: optional. Ohne API-Key sind 240 Requests/Minute und
1.000 Requests/Tag pro IP erlaubt; mit Key (`OPENFDA_API_KEY` ENV) sind es
240/Min und 120.000/Tag. Wir nutzen den Key, wenn vorhanden.

Politische / methodische Caveats:
- FAERS-Berichte sind FREIWILLIGE Meldungen — sie zeigen keine Kausalität.
  Ein hoher Bericht-Count beweist NICHT, dass das Medikament die Wirkung
  ausgelöst hat. Reporting-Bias (neue/populäre Medikamente werden mehr
  gemeldet), confounders (Co-Medikation, Grunderkrankung) und das fehlende
  Nenner-Problem (keine Verschreibungs-Population als Bezugsgröße).
- US-Daten sind nicht 1:1 auf EU/AT übertragbar — Zulassung, Indikation
  und Pharmakovigilanz-Standards unterscheiden sich (FDA vs. EMA vs. BASG).
- Recalls sind klassifiziert (I = lebensgefährlich, III = unwahrscheinlich
  Schaden) — wir geben Class mit aus.

Cross-Cluster:
- ema.py  — EU-Pendant (Zulassungsdatenbank).
- basg.py — AT-Pendant (Rückrufe via RSS-Feed).
openFDA = US-Standard; bei AT/EU-Claims ergänzt openFDA, ersetzt aber nicht.
"""

# WIRING für main.py:
# from services.openfda import search_openfda, claim_mentions_openfda_cached
# if claim_mentions_openfda_cached(claim):
#     tasks.append(cached("openFDA", search_openfda, analysis))
#     queried_names.append("openFDA")
#
# WIRING für services/reranker.py (Whitelist):
# "openFDA" in SOURCE_WHITELIST
#
# WIRING für services/data_updater.py: nicht nötig (Live-API, kein
# Static-JSON-Prefetch).

from __future__ import annotations

import logging
import os
import re
import time
from functools import lru_cache
from urllib.parse import quote

import httpx

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------
BASE_URL = "https://api.fda.gov"
OPENFDA_API_KEY = os.getenv("OPENFDA_API_KEY", "")

TIMEOUT_S = 20.0
MAX_RESULTS_PER_ENDPOINT = 3
CACHE_TTL_S = 24 * 60 * 60  # 24 h

# In-Memory-Cache: search-key → (ts, payload)
_search_cache: dict[str, tuple[float, dict]] = {}


# ---------------------------------------------------------------------------
# Trigger-Vokabular
# ---------------------------------------------------------------------------
# Pharmakovigilanz-/FDA-spezifisches Vokabular (DE + EN)
_OPENFDA_TERMS = (
    # Direkte FDA-Referenzen
    "openfda", "open fda", "fda",
    "fda-warnung", "fda warnung", "fda warning",
    "fda-recall", "fda recall",
    "fda-zulassung", "fda zulassung", "fda approval",
    # Pharmakovigilanz
    "faers", "pharmakovigilanz", "pharmacovigilance",
    "adverse event", "adverse events", "adverse drug reaction",
    "unerwünschte arzneimittelwirkung", "unerwuenschte arzneimittelwirkung",
    "nebenwirkung", "nebenwirkungen", "side effect", "side effects",
    # Recalls / Rückrufe
    "drug recall", "medikamenten-rückruf", "medikamentenrückruf",
    "arzneimittelrückruf", "arzneimittel-rückruf",
    "device recall", "medical device recall",
    "medizinprodukt-rückruf", "medizinprodukterückruf",
    # Labels / Beipackzettel
    "drug label", "drug-label", "beipackzettel", "fachinformation",
    "indications and usage",
)

# Medizinprodukt-Schlüsselbegriffe (DE+EN) — direkter Device-Bezug, der einen
# Recall-Lookup im /device/enforcement.json-Endpoint rechtfertigt. Wird zusätzlich
# zur Trigger-Logik als Routing-Signal verwendet (Drug- vs. Device-Endpoint).
_DEVICE_TERMS = (
    # Generisch
    "device", "medical device", "medizinprodukt", "medizinprodukte",
    "implant", "implants", "implantat", "implantate",
    # Konkrete Geräte-Klassen
    "insulin pump", "insulin pumps", "insulinpumpe", "insulinpumpen",
    "pacemaker", "pacemakers", "herzschrittmacher", "schrittmacher",
    "defibrillator", "defibrillators", "icd",
    "stent", "stents",
    "catheter", "catheters", "katheter",
    "ventilator", "ventilators", "beatmungsgerät", "beatmungsgeraet",
    "infusion pump", "infusion pumps", "infusionspumpe", "infusionspumpen",
    "hip implant", "knee implant", "hüftimplantat", "hueftimplantat",
    "knieimplantat", "breast implant", "brustimplantat",
    "glucose monitor", "glucose monitors", "blutzuckermessgerät",
    "blutzuckermessgeraet", "cgm",
)

# Symptom-/Reaction-Pattern (DE+EN) — wenn diese ZUSAMMEN mit einem
# Medikamenten-Namen auftreten, ist FAERS sinnvoll.
_SYMPTOM_TERMS = (
    "übelkeit", "uebelkeit", "nausea",
    "erbrechen", "vomiting",
    "kopfschmerz", "headache",
    "ausschlag", "rash", "hautausschlag",
    "schwindel", "dizziness",
    "leberschaden", "liver damage", "hepatotoxicity",
    "nierenschaden", "renal failure",
    "todesfall", "todesfälle", "death", "deaths", "fatal",
    "anaphylaxie", "anaphylaxis",
    "thrombose", "thrombosis",
    "schlaganfall", "stroke",
    "herzinfarkt", "heart attack", "myocardial infarction",
)

# Pharma-Kontext-Terms (Drogen-/Medikament-Domäne)
_PHARMA_CONTEXT = (
    "medikament", "medikamente", "arzneimittel", "wirkstoff", "präparat",
    "praeparat", "medication", "medicine", "drug", "pharmaceutical",
    "tablette", "tabletten", "kapsel", "injektion",
    "impfstoff", "impfung", "vaccine", "vaccination",
)


def _claim_mentions_openfda(claim_lc: str) -> bool:
    """Trigger-Check (interne Logik auf bereits lowercase'tem Text)."""
    # 1) Direkter FDA-/openFDA-Bezug
    if any(t in claim_lc for t in _OPENFDA_TERMS):
        return True
    # 2) Symptom + Pharma-Kontext → FAERS-Use-Case
    has_symptom = any(t in claim_lc for t in _SYMPTOM_TERMS)
    has_pharma = any(t in claim_lc for t in _PHARMA_CONTEXT)
    if has_symptom and has_pharma:
        return True
    # 3) Device-Schlüsselbegriff + Recall/Warning-Kontext → Device-Recall-Use-Case
    has_device = any(t in claim_lc for t in _DEVICE_TERMS)
    has_recall_ctx = any(
        t in claim_lc for t in (
            "recall", "rückruf", "rueckruf", "withdrawal", "warning",
            "warnung", "alert", "fda",
        )
    )
    if has_device and has_recall_ctx:
        return True
    return False


@lru_cache(maxsize=2048)
def claim_mentions_openfda_cached(claim: str) -> bool:
    """Public cache-fähiger Trigger-Check (LRU-Cache pro normalisiertem Claim)."""
    return _claim_mentions_openfda((claim or "").lower())


# ---------------------------------------------------------------------------
# Drug-Name-Extraktion
# ---------------------------------------------------------------------------
# Sehr generische Wörter, die wir NIEMALS als Drug-Name an openFDA schicken
# (würde Tausende irrelevanter Treffer liefern).
_STOP_TOKENS = {
    "fda", "drug", "drugs", "medication", "medications", "medicine",
    "medicines", "patient", "patients", "doctor", "doctors", "hospital",
    "hospitals", "study", "studies", "report", "reports", "year", "years",
    "side", "effect", "effects", "adverse", "event", "events", "recall",
    "recalls", "label", "labels", "warning", "warnings", "death", "deaths",
    "medikament", "medikamente", "arzneimittel", "wirkstoff", "nebenwirkung",
    "nebenwirkungen", "rückruf", "warnung", "studie", "studien", "bericht",
    "berichte", "jahr", "jahre", "patient", "patientin", "arzt", "ärztin",
    "spital", "spitäler", "österreich", "austria", "deutschland", "europa",
    "vaccine", "vaccination", "impfung", "impfstoff",
    "with", "from", "about", "this", "that", "these", "those", "have",
    "been", "were", "they", "their", "into", "after", "before", "during",
}


def _extract_drug_names(analysis: dict) -> list[str]:
    """Extrahiere mögliche Medikamenten-Namen aus NER und LLM-Entities.

    Bevorzugt NER-`drugs`-Liste, fällt zurück auf flache `entities`-Liste,
    extrahiert im Notfall Kandidaten aus dem Claim selbst.
    """
    candidates: list[str] = []
    ner = (analysis.get("ner_entities") or {})
    for name in (ner.get("drugs") or []):
        if name and isinstance(name, str):
            candidates.append(name.strip())

    if not candidates:
        # Fallback: flache LLM-Entities
        for e in analysis.get("entities") or []:
            if isinstance(e, str) and len(e) >= 4:
                candidates.append(e.strip())

    # Filter: keine Stop-Tokens, keine Zahlen-only, mindestens 4 Zeichen
    cleaned: list[str] = []
    seen: set[str] = set()
    for c in candidates:
        c_lc = c.lower()
        if c_lc in _STOP_TOKENS:
            continue
        if len(c) < 4:
            continue
        if c_lc.isdigit():
            continue
        # Erstes Token darf nicht reines Stopwort sein
        first = c_lc.split()[0] if c_lc.split() else c_lc
        if first in _STOP_TOKENS:
            continue
        if c_lc in seen:
            continue
        seen.add(c_lc)
        cleaned.append(c)

    return cleaned[:3]  # max. 3 Drug-Namen pro Claim


def _extract_device_terms(claim_lc: str) -> list[str]:
    """Extrahiere Medizinprodukt-Begriffe für /device/enforcement.json.

    Liefert englische Begriffe (openFDA-Index ist englisch) und mappt
    deutsche Synonyme entsprechend.
    """
    de_to_en = {
        "insulinpumpe": "insulin pump",
        "insulinpumpen": "insulin pumps",
        "herzschrittmacher": "pacemaker",
        "schrittmacher": "pacemaker",
        "katheter": "catheter",
        "beatmungsgerät": "ventilator",
        "beatmungsgeraet": "ventilator",
        "infusionspumpe": "infusion pump",
        "infusionspumpen": "infusion pumps",
        "hüftimplantat": "hip implant",
        "hueftimplantat": "hip implant",
        "knieimplantat": "knee implant",
        "brustimplantat": "breast implant",
        "blutzuckermessgerät": "glucose monitor",
        "blutzuckermessgeraet": "glucose monitor",
        "implantat": "implant",
        "implantate": "implant",
        "medizinprodukt": "device",
        "medizinprodukte": "device",
    }
    found: list[str] = []
    for de, en in de_to_en.items():
        if de in claim_lc and en not in found:
            found.append(en)
    # Englische Begriffe direkt aus Vokabular abgleichen (länger zuerst, damit
    # "insulin pumps" vor "pump" gefunden wird)
    english_terms = sorted(
        (t for t in _DEVICE_TERMS if t.isascii() and " " in t or t in (
            "pacemaker", "pacemakers", "defibrillator", "defibrillators",
            "stent", "stents", "catheter", "catheters", "ventilator",
            "ventilators", "implant", "implants", "icd", "cgm",
        )),
        key=len,
        reverse=True,
    )
    for en in english_terms:
        if en in claim_lc and en not in found:
            # Vermeide Doppel-Matches (z. B. "insulin pump" + "insulin pumps")
            if not any(en in existing or existing in en for existing in found):
                found.append(en)
    return found[:3]


def _extract_symptoms(claim_lc: str) -> list[str]:
    """Extrahiere Symptom-Begriffe für FAERS-Reaction-Filter (englische
    MedDRA-Terms, da openFDA englisch indexiert)."""
    de_to_en = {
        "übelkeit": "nausea", "uebelkeit": "nausea",
        "erbrechen": "vomiting",
        "kopfschmerz": "headache",
        "ausschlag": "rash", "hautausschlag": "rash",
        "schwindel": "dizziness",
        "leberschaden": "hepatotoxicity",
        "nierenschaden": "renal failure",
        "todesfall": "death", "todesfälle": "death",
        "anaphylaxie": "anaphylactic reaction",
        "thrombose": "thrombosis",
        "schlaganfall": "stroke",
        "herzinfarkt": "myocardial infarction",
    }
    found: list[str] = []
    for de, en in de_to_en.items():
        if de in claim_lc and en not in found:
            found.append(en)
    for en in ("nausea", "vomiting", "headache", "rash", "dizziness",
               "stroke", "thrombosis", "anaphylaxis"):
        if en in claim_lc and en not in found:
            found.append(en)
    return found[:2]


# ---------------------------------------------------------------------------
# HTTP-Helfer
# ---------------------------------------------------------------------------
def _build_url(endpoint: str, search: str, limit: int) -> str:
    """Baue openFDA-URL mit optionalem API-Key."""
    url = f"{BASE_URL}{endpoint}?search={quote(search)}&limit={limit}"
    if OPENFDA_API_KEY:
        url += f"&api_key={OPENFDA_API_KEY}"
    return url


async def _fetch_json(client: httpx.AsyncClient, url: str) -> dict | None:
    """GET + JSON-parse mit klarer Fehlerbehandlung.

    openFDA gibt 404 wenn es schlicht keine Treffer gibt — das ist KEIN
    Fehler, sondern ein leeres Suchergebnis.
    """
    try:
        resp = await client.get(url)
        if resp.status_code == 404:
            return {"results": []}
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        logger.info(f"openFDA HTTP {e.response.status_code} for {url[:100]}")
        return None
    except Exception as e:
        logger.warning(f"openFDA fetch failed: {type(e).__name__}: {e}")
        return None


# ---------------------------------------------------------------------------
# Endpoint-Loader
# ---------------------------------------------------------------------------
async def _fetch_faers(
    client: httpx.AsyncClient, drug: str, symptoms: list[str]
) -> list[dict]:
    """FAERS (Adverse Events) — Top-Berichte für einen Drug-Namen."""
    search_parts = [f'patient.drug.medicinalproduct:"{drug}"']
    if symptoms:
        sym = "+".join(f'"{s}"' for s in symptoms)
        search_parts.append(f"patient.reaction.reactionmeddrapt:({sym})")
    url = _build_url("/drug/event.json", "+AND+".join(search_parts),
                     MAX_RESULTS_PER_ENDPOINT)

    data = await _fetch_json(client, url)
    if not data:
        return []

    total = (data.get("meta", {}).get("results", {}) or {}).get("total", 0)
    items: list[dict] = []

    # Aggregierte Top-Line (Gesamtzahl FAERS-Berichte)
    if total:
        reactions: list[str] = []
        for r in data.get("results", [])[:3]:
            for reac in (r.get("patient", {}).get("reaction") or [])[:2]:
                t = reac.get("reactionmeddrapt")
                if t and t not in reactions:
                    reactions.append(t)
        items.append({
            "title": f"openFDA FAERS: {total:,} Berichte zu {drug}",
            "type": "faers_summary",
            "url": (
                "https://open.fda.gov/data/faers/ "
                "(API: api.fda.gov/drug/event.json)"
            ),
            "summary": (
                f"Insgesamt {total:,} FAERS-Berichte (freiwillige Meldungen, "
                f"keine Kausalitätsbeweise) zu „{drug}“"
                + (f" mit Reaktionen u. a.: {', '.join(reactions[:5])}."
                   if reactions else ".")
            ),
            "drug": drug,
            "total_reports": total,
        })

    # Einzelne Beispiel-Berichte
    for r in (data.get("results") or [])[:MAX_RESULTS_PER_ENDPOINT]:
        report_id = r.get("safetyreportid", "?")
        country = r.get("primarysourcecountry", "?")
        recv = r.get("receivedate", "")
        recv_short = (
            f"{recv[:4]}-{recv[4:6]}-{recv[6:8]}" if len(recv) == 8 else recv
        )
        reactions = []
        for reac in (r.get("patient", {}).get("reaction") or [])[:3]:
            t = reac.get("reactionmeddrapt")
            if t:
                reactions.append(t)
        items.append({
            "title": (
                f"FAERS-Bericht #{report_id} ({country}, {recv_short})"
            ),
            "type": "faers_report",
            "url": (
                f"https://api.fda.gov/drug/event.json?"
                f"search=safetyreportid:{report_id}"
            ),
            "summary": (
                f"Reaktion(en): {', '.join(reactions) if reactions else '?'}. "
                f"Land: {country}. Eingang: {recv_short}."
            ),
            "drug": drug,
        })

    return items


async def _fetch_drug_label(client: httpx.AsyncClient, drug: str) -> list[dict]:
    """Drug Label — strukturierte Beipackzettel-Daten."""
    search = (
        f'(openfda.brand_name:"{drug}"+'
        f'openfda.generic_name:"{drug}"+'
        f'openfda.substance_name:"{drug}")'
    )
    url = _build_url("/drug/label.json", search, 1)
    data = await _fetch_json(client, url)
    if not data or not data.get("results"):
        return []

    label = data["results"][0]
    openfda_meta = label.get("openfda", {}) or {}
    brand = (openfda_meta.get("brand_name") or [""])[0]
    generic = (openfda_meta.get("generic_name") or [""])[0]
    manufacturer = (openfda_meta.get("manufacturer_name") or [""])[0]

    warnings = (label.get("warnings") or label.get("boxed_warning")
                or [""])[0][:400]
    indications = (label.get("indications_and_usage") or [""])[0][:300]

    return [{
        "title": (
            f"FDA-Drug-Label: {brand or drug}"
            + (f" (generic: {generic})" if generic and generic != brand else "")
        ),
        "type": "drug_label",
        "url": (
            f"https://labels.fda.gov/getlabels?search={quote(brand or drug)}"
        ),
        "summary": (
            (f"Indikation: {indications}. " if indications else "")
            + (f"Warnhinweise: {warnings}" if warnings else "")
        ).strip() or "Strukturierter Beipackzettel verfügbar.",
        "drug": drug,
        "brand_name": brand,
        "generic_name": generic,
        "manufacturer": manufacturer,
    }]


async def _fetch_recalls(client: httpx.AsyncClient, drug: str) -> list[dict]:
    """Drug-Recalls (Enforcement)."""
    search = f'product_description:"{drug}"'
    url = _build_url("/drug/enforcement.json", search, MAX_RESULTS_PER_ENDPOINT)
    data = await _fetch_json(client, url)
    if not data or not data.get("results"):
        return []

    items: list[dict] = []
    total = (data.get("meta", {}).get("results", {}) or {}).get("total", 0)
    for r in data["results"][:MAX_RESULTS_PER_ENDPOINT]:
        classification = r.get("classification", "?")
        reason = (r.get("reason_for_recall") or "?")[:200]
        firm = r.get("recalling_firm", "?")
        date_raw = r.get("recall_initiation_date", "")
        date_short = (
            f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
            if len(date_raw) == 8 else date_raw
        )
        status = r.get("status", "?")
        items.append({
            "title": (
                f"FDA-Drug-Recall {classification}: {drug} "
                f"({firm}, {date_short})"
            ),
            "type": "drug_recall",
            "url": (
                "https://www.fda.gov/safety/recalls-market-withdrawals-"
                "safety-alerts"
            ),
            "summary": (
                f"Rückrufgrund: {reason}. Status: {status}. "
                f"Klassifizierung: {classification} "
                f"(I=lebensgefährlich, II=möglicher Schaden, "
                f"III=unwahrscheinlich Schaden)."
            ),
            "drug": drug,
            "classification": classification,
            "total_matching_recalls": total,
        })
    return items


async def _fetch_device_recalls(
    client: httpx.AsyncClient, term: str
) -> list[dict]:
    """Medical-Device-Recalls — getriggert nur bei explizitem Device-Bezug."""
    search = f'product_description:"{term}"'
    url = _build_url("/device/enforcement.json", search, 2)
    data = await _fetch_json(client, url)
    if not data or not data.get("results"):
        return []

    items: list[dict] = []
    for r in data["results"][:2]:
        classification = r.get("classification", "?")
        reason = (r.get("reason_for_recall") or "?")[:200]
        firm = r.get("recalling_firm", "?")
        date_raw = r.get("recall_initiation_date", "")
        date_short = (
            f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
            if len(date_raw) == 8 else date_raw
        )
        items.append({
            "title": (
                f"FDA-Device-Recall {classification}: {term} "
                f"({firm}, {date_short})"
            ),
            "type": "device_recall",
            "url": "https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfRES/res.cfm",
            "summary": (
                f"Rückrufgrund: {reason}. "
                f"Klassifizierung: {classification}."
            ),
            "device": term,
        })
    return items


# ---------------------------------------------------------------------------
# Result-Caveat
# ---------------------------------------------------------------------------
def _caveat_result() -> dict:
    return {
        "title": "WICHTIGER KONTEXT: openFDA-Daten",
        "type": "context",
        "url": "https://open.fda.gov/about/",
        "summary": (
            "openFDA stellt Daten der US-FDA bereit (Public Domain). "
            "Einschränkungen: "
            "(1) FAERS-Berichte sind FREIWILLIGE Meldungen — sie zeigen "
            "Assoziation, nicht Kausalität. Reporting-Bias (populäre/neue "
            "Medikamente werden häufiger gemeldet) und fehlende "
            "Nenner-Population machen Raten irreführend. "
            "(2) US-Zulassungs- und Pharmakovigilanz-Standards weichen von "
            "EU (EMA) und AT (BASG) ab — Befund nicht 1:1 übertragbar. "
            "(3) Recall-Klassen: I = ernste Gesundheitsgefahr, "
            "II = möglicher Schaden, III = Etiketten-/Verpackungsmängel. "
            "(4) Bei US-spezifischen Marken/Generika kann das gleiche "
            "Präparat in der EU unter anderem Namen oder gar nicht "
            "zugelassen sein."
        ),
    }


# ---------------------------------------------------------------------------
# Public Search
# ---------------------------------------------------------------------------
async def search_openfda(analysis: dict) -> dict:
    """Search openFDA for adverse events, labels and recalls.

    Triggert nur bei pharmakovigilanz-/FDA-spezifischen Claims. Ohne
    extrahierbaren Drug-Namen wird kein Live-Call abgesetzt.
    """
    empty = {"source": "openFDA", "type": "pharmacovigilance", "results": []}

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_openfda(matchable):
        return empty

    drugs = _extract_drug_names(analysis)
    devices = _extract_device_terms(matchable)

    if not drugs and not devices:
        logger.info(
            "openFDA: trigger matched but no drug or device terms extracted"
        )
        return empty

    # Cache-Lookup (Devices und Drugs separat keyen)
    cache_key = (
        "openfda::"
        + ",".join(sorted(d.lower() for d in drugs))
        + "::dev="
        + ",".join(sorted(d.lower() for d in devices))
    )
    now = time.time()
    cached = _search_cache.get(cache_key)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]

    symptoms = _extract_symptoms(matchable)

    all_results: list[dict] = []

    async with polite_client(timeout=TIMEOUT_S) as client:
        # Drug-Pfad: FAERS + Label + Drug-Recalls
        for drug in drugs:
            faers = await _fetch_faers(client, drug, symptoms)
            all_results.extend(faers)

            label = await _fetch_drug_label(client, drug)
            all_results.extend(label)

            recalls = await _fetch_recalls(client, drug)
            all_results.extend(recalls)

        # Device-Pfad: /device/enforcement.json
        for device in devices:
            devs = await _fetch_device_recalls(client, device)
            all_results.extend(devs)

    if not all_results:
        logger.info(
            f"openFDA: drugs={drugs} devices={devices} symptoms={symptoms} "
            f"→ 0 hits"
        )
        result = empty
    else:
        # Caveat anhängen
        all_results.append(_caveat_result())
        result = {
            "source": "openFDA",
            "type": "pharmacovigilance",
            "results": all_results,
        }
        logger.info(
            f"openFDA: {len(all_results) - 1} matches, drugs={drugs}, "
            f"devices={devices}, symptoms={symptoms}, "
            f"api_key={'yes' if OPENFDA_API_KEY else 'no'}"
        )

    _search_cache[cache_key] = (now, result)
    return result
