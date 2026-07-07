"""Marken→Wirkstoff-Expansion für Medizin-Claims (Audit 2026-07-07).

Befund (Voltadol): Ein Claim „Voltadol schädigt die Nieren" erreichte die
medizinischen Fachquellen (EMA, MedlinePlus, Europe PMC, ClinicalTrials,
openFDA) NICHT — sie liefern zum Markennamen „Voltadol" nichts, nur zum
Wirkstoff „Diclofenac". Der LLM-Analyzer expandiert Marken unzuverlässig.

Dieses Modul mappt gängige (v. a. österreichische/DACH) Markennamen
deterministisch auf ihren INN-Wirkstoff und wird im claim_analyzer nach der
Normalisierung eingehängt: der Wirkstoff wird zu entities UND pubmed_queries
ergänzt (nie ersetzt — die Marke bleibt erhalten). Damit feuern ALLE
Medizin-Connectoren, die diese Felder lesen, gemeinsam.

Kuratierungs-Regeln (Korrektheit > Vollständigkeit):
- NUR eindeutige Mono-Wirkstoff-Marken. Kombipräparate (z. B. Thomapyrin)
  sind ausgelassen — eine falsche Wirkstoff-Zuordnung wäre schädlicher als
  eine fehlende Expansion.
- Match auf Wortgrenzen (kein Substring), case-insensitiv.
- Erweiterbar; bei Unsicherheit lieber NICHT aufnehmen.
"""

import re

# Markenname (lowercase) → INN-Wirkstoff (deutsch/englisch-nah, wie ihn die
# Fachquellen indexieren). Fokus: AT-OTC + häufige Rx im DACH-Raum.
_BRAND_TO_INN: dict[str, str] = {
    # NSAR / Analgetika
    "voltadol": "Diclofenac",
    "voltaren": "Diclofenac",
    "diclobene": "Diclofenac",
    "deflamat": "Diclofenac",
    "mexalen": "Paracetamol",
    "ben-u-ron": "Paracetamol",
    "benuron": "Paracetamol",
    "nurofen": "Ibuprofen",
    "brufen": "Ibuprofen",
    "ibumetin": "Ibuprofen",
    "dolgit": "Ibuprofen",
    "seractil": "Dexibuprofen",
    "parkemed": "Mefenaminsäure",
    "novalgin": "Metamizol",
    "novaminsulfon": "Metamizol",
    "buscopan": "Butylscopolamin",
    # Thrombozytenaggregationshemmer / Antikoagulanzien
    "thrombo ass": "Acetylsalicylsäure",
    "herz ass": "Acetylsalicylsäure",
    "aspirin": "Acetylsalicylsäure",
    "marcoumar": "Phenprocoumon",
    "xarelto": "Rivaroxaban",
    "eliquis": "Apixaban",
    # Protonenpumpenhemmer
    "pantoloc": "Pantoprazol",
    "nexium": "Esomeprazol",
    "losec": "Omeprazol",
    "antramups": "Omeprazol",
    # Psychopharmaka / Benzodiazepine
    "cipralex": "Escitalopram",
    "zoloft": "Sertralin",
    "seroxat": "Paroxetin",
    "xanor": "Alprazolam",
    "praxiten": "Oxazepam",
    # Kardiovaskulär / Metabolisch
    "concor": "Bisoprolol",
    "sortis": "Atorvastatin",
    "euthyrox": "Levothyroxin",
    "thyrex": "Levothyroxin",
    # Muskelrelaxans
    "sirdalud": "Tizanidin",
}

# Vorkompilierte Wortgrenzen-Pattern pro Marke (Mehrwort-Marken erlaubt).
_BRAND_PATTERNS = [
    (re.compile(r"\b" + re.escape(brand) + r"\b", re.IGNORECASE), inn)
    for brand, inn in _BRAND_TO_INN.items()
]


def brand_inns_in_claim(claim: str) -> list[str]:
    """Alle INN-Wirkstoffe, deren Marke als ganzes Wort im Claim vorkommt.
    Reihenfolge stabil (Mapping-Reihenfolge), ohne Duplikate."""
    if not claim:
        return []
    found: list[str] = []
    seen: set[str] = set()
    for pat, inn in _BRAND_PATTERNS:
        if inn not in seen and pat.search(claim):
            seen.add(inn)
            found.append(inn)
    return found


def apply_brand_expansion(claim: str, result: dict) -> dict:
    """Ergänzt result['entities'] + result['pubmed_queries'] um die
    Wirkstoffe der im Claim genannten Marken (in-place, idempotent).

    Nie ersetzend — der Markenname bleibt als Entity erhalten; der Wirkstoff
    kommt additiv dazu, damit sowohl Marken- als auch Wirkstoff-Treffer in den
    Fachquellen gefunden werden. No-op, wenn keine bekannte Marke vorkommt."""
    inns = brand_inns_in_claim(claim)
    if not inns:
        return result

    entities = result.get("entities")
    if not isinstance(entities, list):
        entities = []
    ent_lc = {e.lower() for e in entities if isinstance(e, str)}
    for inn in inns:
        if inn.lower() not in ent_lc:
            entities.append(inn)
            ent_lc.add(inn.lower())
    result["entities"] = entities

    queries = result.get("pubmed_queries")
    if not isinstance(queries, list):
        queries = []
    q_lc = {q.lower() for q in queries if isinstance(q, str)}
    for inn in inns:
        if inn.lower() not in q_lc:
            queries.append(inn)
            q_lc.add(inn.lower())
    result["pubmed_queries"] = queries

    return result
