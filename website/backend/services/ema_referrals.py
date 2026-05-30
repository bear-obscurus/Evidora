"""EMA Article-31/20/30/107i-Referrals als Static-JSON-Pack-Service.

Ergänzt ``services/ema.py`` (EMA-Medicines-CSV) und ``services/
withdrawn_drugs.py`` (Wikipedia-Liste) um EU-weite Sicherheits-
Bewertungs-Verfahren VOR oder OHNE Marktrückzug. Beispiele:
- Valproat-in-Schwangerschaft (Art-31 2014, 2018) — Restriktion ohne
  Withdrawal
- Topiramat-Schwangerschaft (Art-31 2023) — Restriktion
- Pseudoephedrin (Art-31 2023) — PRAC-Empfehlung
- Diane-35 (Art-107 2013), Hydroxyzin-QT (Art-31 2015)

Datenquelle: EMA-Referrals-XLSX (offizielles 2×-täglich-Snapshot,
EU PSI / CC-BY 4.0). Snapshot wird via data_updater wöchentlich
refresh-bar (Zukunfts-Cron). Stand initialer Import: 2023-12-05,
573 Referrals.

Schema in data/ema_referrals.json:
- category (Human/Veterinary)
- referral_name (Wirkstoff- oder Produktname mit Komma-Suffix)
- inn (International non-proprietary name)
- status (z.B. "European Commission final decision",
  "Procedure started", "Under evaluation",
  "Recommendation provided by PRAC")
- safety_referral (bool: ist es ein PRAC-Sicherheits-Verfahren?)
- referral_type (z.B. "Article 31 referrals", "Article 20 procedures",
  "Article 30 referrals", "Article 107i")
- associated_names (Brand-Namen, semikolon-separiert)
- reference_number (z.B. "EMEA/H/A-31/1526")
- authorisation_model (Centrally / Nationally / both)

Pattern: Static-JSON-Pack-Service nach ARCHITECTURE.md §3.5, analog
withdrawn_drugs.py. Wichtiger Unterschied: KEIN harter STRUKTURELL-
FALSCH-Marker, weil XLSX kein Outcome-Detail liefert ("European
Commission final decision" kann sowohl Withdrawal als auch
Maintained-with-Restrictions sein). Stattdessen Caveat-Display-Value
("X wurde von EMA Art-31 evaluiert, Status: ..."), den der
Synthesizer als Counter-Evidence für "X ist sicher"-Claims nutzen
kann, ohne in eine Inversions-Falle bei Maintained-Verfahren zu
laufen.
"""

from __future__ import annotations

import json
import logging
import os
import re

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "ema_referrals.json",
)

_cache: dict | None = None


def _load() -> dict:
    global _cache
    if _cache is None:
        try:
            with open(STATIC_JSON_PATH, encoding="utf-8") as f:
                _cache = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            logger.warning(f"ema_referrals: load failed: {e}")
            _cache = {"items": []}
    return _cache


def _word_match(term: str, text: str) -> bool:
    if not term or not text:
        return False
    return bool(
        re.search(r"\b" + re.escape(term) + r"\b", text, re.IGNORECASE)
    )


def _split_inn_list(inn_field: str) -> list[str]:
    """EMA-INN-Felder enthalten oft kommasepariert mehrere Synonym-INN
    (z.B. "sodium valproate, valproate magnesium, ..."). Wir splitten
    und liefern einzelne Sub-INN, die für word-boundary-Match nutzbar
    sind."""
    if not inn_field:
        return []
    parts = [p.strip() for p in re.split(r"[,;]", inn_field)]
    return [p for p in parts if p]


def _inn_variants(inn: str) -> list[str]:
    """Englische INN-Stammformen + deutsche Adaption.

    Beispiele:
    - "topiramate" → ["topiramate", "topiramat"]
    - "sodium valproate" → ["sodium valproate", "valproate", "valproat"]
    - "valproic acid" → ["valproic acid", "valproic"]

    Vorsichtig: nur final-"e" / final-"a"-Variante (häufige
    englisch-deutsch-Adaption für Wirkstoff-Namen) — keine fuzzy-
    matches, sonst false positives.
    """
    out = [inn]
    # final "e" → "" (z.B. topiramate→topiramat, valproate→valproat)
    if inn.endswith("e") and len(inn) >= 5:
        out.append(inn[:-1])
    # Letzter Token (falls multi-word "sodium valproate" → "valproate")
    tokens = inn.split()
    if len(tokens) > 1:
        last = tokens[-1]
        if last not in out:
            out.append(last)
        if last.endswith("e") and len(last) >= 5 and last[:-1] not in out:
            out.append(last[:-1])
    return out


def _matches_inn_field(claim: str, inn_field: str) -> bool:
    """True wenn irgendein Sub-INN (oder Variante) als word-boundary
    im Claim auftaucht."""
    for sub in _split_inn_list(inn_field):
        for variant in _inn_variants(sub):
            if len(variant) >= 5 and _word_match(variant, claim):
                return True
    return False


# Drug-/Pharma-Kontext-Trigger (analog withdrawn_drugs aber breiter,
# weil Referrals auch bei nicht-zurückgezogenen Wirkstoffen feuern
# sollen). Stem-based ohne trailing word-boundary.
_PHARMA_CONTEXT = re.compile(
    r"\b("
    r"medikament|arzneimittel|wirkstoff|tablette|pille|"
    r"impfstoff|antibiotik|chemother|infus|spritz|"
    r"verschrieb|verschreib|zugelass|zulassung|"
    r"verträg|verträglich|sicher|sicherheit|nebenwirkung|"
    r"schwanger|stillen|stillzeit|stillende|"
    r"medic|drug|pharma|prescription|safety|side effect|"
    r"hepatotox|cardiotox|teratogen|"
    r"schmerzmittel|antidepressivum|antidepressiva|"
    r"diabetes|krebs|antibiotic|antibiotika|hormon|"
    r"qt-zeit|qt-verlängerung|qt-verlaeng|"
    r"erkältung|erkaeltung|migräne|migraene|"
    r"kopfschmerz|schlafstörung|schlafstoer|"
    r"abhängig|abhaeng|sucht|"
    r"indikation|kontraindikation|risiko"
    r")",
    re.IGNORECASE,
)


def claim_mentions_ema_referrals_cached(claim: str) -> bool:
    """Trigger-Check für EMA-Referrals-Lookup.

    True wenn:
    1. Claim enthält Pharma-/Sicherheits-Kontext-Term, und
    2. mindestens ein INN/referral_name/associated_name matcht
       als word-boundary.
    """
    if not claim:
        return False
    if not _PHARMA_CONTEXT.search(claim):
        return False
    items = _load().get("items", [])
    for it in items:
        inn = (it.get("inn") or "").strip()
        if inn and _matches_inn_field(claim, inn):
            return True
        ref_name = (it.get("referral_name") or "").strip().rstrip(",").rstrip()
        ref_name_short = re.sub(
            r"-?containing medicinal products$", "", ref_name, flags=re.IGNORECASE
        ).strip()
        if ref_name_short and _matches_inn_field(claim, ref_name_short):
            return True
        # Associated names (brand names)
        assoc = it.get("associated_names") or ""
        if assoc and _matches_inn_field(claim, assoc):
            return True
    return False


_STATUS_DESCRIPTION = {
    "Procedure started": "Verfahren gestartet",
    "Under evaluation": "in laufender Bewertung",
    "Recommendation provided by Pharmacovigilance Risk Assessment Committee":
        "PRAC-Empfehlung vorgelegt",
    "Opinion provided by Committee for Medicinal Products for Human Use":
        "CHMP-Stellungnahme vorgelegt",
    "CMDh final position": "CMDh-Endposition (national koordiniert)",
    "European Commission final decision":
        "Endgültige EU-Kommissions-Entscheidung",
}


def _build_display(it: dict) -> str:
    """Caveat-orientierter Display-Value (KEIN STRUKTURELL-Marker,
    weil Outcome-Detail nicht aus XLSX ableitbar)."""
    inn = (it.get("inn") or "—").strip()
    ref_type = (it.get("referral_type") or "").strip()
    status = (it.get("status") or "").strip()
    status_de = _STATUS_DESCRIPTION.get(status, status)
    safety = it.get("safety_referral")
    safety_marker = " [Sicherheits-Referral]" if safety else ""
    ref_num = (it.get("reference_number") or "").strip()
    body = (
        f"EMA-Bewertungs-Verfahren für '{inn}' (Typ: {ref_type}{safety_marker}, "
        f"Stand: {status_de}). Aktenzeichen: {ref_num}. "
        f"Hinweis: Das Vorliegen eines EMA-Referrals bedeutet, dass für "
        f"diesen Wirkstoff EU-weit ein Sicherheits- oder Bewertungs-"
        f"Verfahren durchgeführt wurde oder läuft. Outcome (Maintained / "
        f"Restriction / Withdrawal) ist über die EMA-Verfahrens-Detailseite "
        f"einzusehen — Verdict-Caveat statt pauschaler Sicherheits-Zusage."
    )
    return body


def _is_meta_referral_claim(claim: str) -> bool:
    """Erkennt Claims ueber die Gesamtzahl der EMA-Referral-Verfahren,
    z.B. 'Die EMA hat ueber 500 Ueberpruefungsverfahren durchgefuehrt'."""
    cl = claim.lower()
    has_ema = "ema" in cl or "europäische arzneimittel" in cl or "european medicines" in cl
    has_quant = any(t in cl for t in (
        "referral", "überprüfung", "ueberpruefung",
        "verfahren", "bewertungsverfahren", "sicherheitsverfahren",
        "procedures", "evaluations",
    ))
    has_number = bool(re.search(r"\b\d{2,}\b", cl))
    return has_ema and (has_quant or has_number)


def _build_meta_result() -> dict:
    """Liefert einen Meta-Fact ueber die Gesamtzahl aller EMA-Referrals."""
    items = _load().get("items", [])
    total = len(items)
    safety_count = sum(1 for it in items if it.get("safety_referral"))
    art31 = sum(1 for it in items if "Article 31" in (it.get("referral_type") or ""))
    art20 = sum(1 for it in items if "Article 20" in (it.get("referral_type") or ""))
    art30 = sum(1 for it in items if "Article 30" in (it.get("referral_type") or ""))
    art107i = sum(1 for it in items if "107i" in (it.get("referral_type") or ""))
    payload = _load()
    return {
        "indicator_name": "EMA Referrals — Gesamtbestand",
        "indicator": "ema_referrals_meta",
        "country": "EU",
        "year": payload.get("snapshot_date", "2023"),
        "topic": "ema_drug_safety_referral",
        "display_value": (
            f"EMA-Referral-Verfahren Gesamtbestand: {total} Verfahren "
            f"(Stand Snapshot {payload.get('snapshot_date', '?')}). "
            f"Davon {safety_count} Sicherheits-Referrals. "
            f"Aufschluesselung: Art. 31: {art31}, Art. 20: {art20}, "
            f"Art. 30: {art30}, Art. 107i: {art107i}. "
            f"Die EMA fuehrt regelmaessig Bewertungs-Verfahren fuer "
            f"zugelassene Arzneimittel durch — sowohl bei Sicherheits-"
            f"bedenken (Art. 31, 107i) als auch bei technischen Fragen "
            f"(Art. 20, 30). Outcomes reichen von 'Maintained without "
            f"changes' bis 'Suspended/Withdrawn'."
        ),
        "description": (
            f"Quelle: EMA-Referrals-XLSX (EU PSI / CC-BY 4.0). "
            f"Snapshot vom {payload.get('snapshot_date', '?')}. "
            f"Insgesamt {total} Verfahren seit EMA-Gruendung."
        ),
        "url": payload.get("source_url",
                           "https://www.ema.europa.eu/en/medicines/human/referrals"),
        "source": "EMA Referrals (Art. 20/30/31/107i, CC-BY 4.0)",
    }


async def search_ema_referrals(analysis: dict) -> dict:
    empty = {
        "source": "EMA Referrals (Art. 20/30/31/107i, CC-BY 4.0)",
        "type": "drug_safety_referral",
        "results": [],
    }

    # Meta-Claim-Check: quantitative Claims ueber die Gesamtzahl
    original_claim = (analysis or {}).get("original_claim", "")
    if _is_meta_referral_claim(original_claim):
        return {
            "source": "EMA Referrals (Art. 20/30/31/107i, CC-BY 4.0)",
            "type": "drug_safety_referral",
            "results": [_build_meta_result()],
        }

    entities = (analysis or {}).get("entities") or []
    if not entities:
        return empty
    items = _load().get("items", [])
    if not items:
        return empty
    search_terms = [e for e in entities if isinstance(e, str) and len(e) >= 4]
    if not search_terms:
        return empty

    scored: list[tuple[int, dict]] = []
    for it in items:
        inn = (it.get("inn") or "").strip()
        ref_name = (it.get("referral_name") or "").rstrip(", ").strip()
        ref_name_short = re.sub(
            r"-?containing medicinal products$", "", ref_name, flags=re.IGNORECASE
        ).strip()
        assoc = it.get("associated_names") or ""
        best = 0
        for t in search_terms:
            # Match Claim-Term gegen INN-Liste / ref_name / assoc
            if inn and _matches_inn_field(t, inn):
                best = max(best, 3)
            elif ref_name_short and _matches_inn_field(t, ref_name_short):
                best = max(best, 3)
            elif assoc and _matches_inn_field(t, assoc):
                best = max(best, 2)
        if best > 0:
            scored.append((best, it))

    if not scored:
        return empty
    scored.sort(key=lambda x: x[0], reverse=True)

    payload = _load()
    out_items: list[dict] = []
    for _, it in scored[:5]:
        inn = (it.get("inn") or "?").strip()
        ref_type = (it.get("referral_type") or "").strip()
        status = (it.get("status") or "").strip()
        ref_num = (it.get("reference_number") or "").strip()
        display = _build_display(it)
        out_items.append({
            "indicator_name": f"EMA Referral: {inn} ({ref_type})",
            "indicator": "ema_referral",
            "country": "EU",
            "year": "—",
            "topic": "ema_drug_safety_referral",
            "display_value": display,
            "description": (
                f"EMA Referral-Verfahren {ref_num}. Status: {status}. "
                f"Sicherheits-Bewertung: "
                f"{'ja' if it.get('safety_referral') else 'nein'}."
            ),
            "url": payload.get(
                "source_url",
                "https://www.ema.europa.eu/en/medicines/human/referrals",
            ),
            "secondary_url": payload.get("xlsx_url", ""),
            "source": "EMA Referrals (Art. 20/30/31/107i, CC-BY 4.0)",
        })

    return {
        "source": "EMA Referrals (Art. 20/30/31/107i, CC-BY 4.0)",
        "type": "drug_safety_referral",
        "results": out_items,
    }
