"""Verdict-Cache mit semantischer Ähnlichkeit (Hebel #4 der Latenz-
Optimierung).

Während `services/cache.py` einzelne Datenquellen-Resultate cached
(per source × analysis), cached `verdict_cache.py` das KOMPLETTE
synthetische Verdict-Dict (verdict, confidence, summary, evidence,
source_coverage, ...) auf Claim-Ebene — und zusätzlich semantisch:
selbst leichte Umformulierungen ('Ist Spinat eisenreich?' vs 'Hat
Spinat viel Eisen?') treffen denselben Cache-Eintrag, sofern Cosine-
Ähnlichkeit ≥ Threshold ist.

Latenz-Effekt: bei Cache-Hit kann die gesamte Pipeline (Analyzer +
30+ Datenquellen + Synthesizer) übersprungen werden — Antwort
in <100 ms statt 8-15 s.

Konservative Defaults:
  - Exact-Match-Cache: TTL 30 Min, identisch zu services/cache.py
  - Semantic-Cache: Threshold 0.92 (sehr hoch — fast identische Claims)
  - Nur Verdicts mit Confidence ≥ 0.8 werden in den Semantic-Cache
    gepackt (vermeidet, dass schwache 'unverifiable'-Verdicts
    semantische Treffer kontaminieren)
  - Hot-Reload-aware: data_version-Bump invalidiert alle Cache-Einträge

Sicherheit gegen False-Positives:
  - Threshold 0.92 ist konservativ. Studienlage zu Sentence-BERT-
    Embeddings: bei 0.95+ praktisch identisch, 0.90+ Bedeutungs-Kern
    gleich, <0.85 Themen-ähnlich aber unterschiedlich.
  - Cache wird per (claim_text, data_version) verschlüsselt — bei
    Datendaten-Änderung automatisch invalidiert.
  - Cache-Hit wird im Log markiert mit Cosine + Original-Claim, damit
    bei Nutzer-Beschwerden nachvollziehbar ist, welcher Claim den Hit
    geliefert hat.
"""

import logging
import re
import time
from typing import Optional

# numpy wird nur im semantischen Pfad (np.dot) gebraucht und lazy in get()
# importiert. So bleibt das Modul auch ohne ML-Stack importierbar — die
# reine Guard-Logik (_polarity_mismatch) ist dadurch ohne numpy/torch
# unit-testbar.
from services._reranker_backup import _get_model as _get_st_model
from services._static_cache import get_data_version

logger = logging.getLogger("evidora")

# In-Memory-Stores: claim_lc -> (timestamp, embedding, result_dict, data_version)
_verdict_store: dict[str, tuple[float, "np.ndarray | None", dict, str]] = {}

DEFAULT_TTL = 1800  # 30 Min — identisch zu services/cache.py
SEMANTIC_THRESHOLD = 0.92  # Cosine-Ähnlichkeit für Cache-Hit
MIN_CONFIDENCE_FOR_CACHE = 0.8  # nur sicher genug Verdicts cachen
MAX_STORE_SIZE = 500  # FIFO-Limit, damit Memory nicht unbegrenzt wächst


def _normalize(claim: str) -> str:
    """Trim + lowercase claim für Exact-Match-Lookup."""
    return claim.strip().lower()


# --- Negations-/Polaritäts-Guard für den semantischen Cache ---------------
# MiniLM-Embeddings sind NEGATIONS-BLIND: "Spinat ist eisenreich" und
# "Spinat ist NICHT eisenreich" liegen auf paraphrase-multilingual-MiniLM
# häufig > 0.92 Cosine, haben aber gegenteilige Bedeutung. Ohne diesen
# Guard würde der semantische Cache für einen negierten Claim das
# invertierte Verdict des Gegenteils ausliefern — die gefährlichste
# Fehlerklasse, weil sie mit Confidence ≥ 0.8 daherkommt und im Log nur
# als harmloser "SEMANTIC HIT" sichtbar ist.
_NEGATION_TOKENS = frozenset({
    # Deutsch
    "nicht", "kein", "keine", "keinen", "keiner", "keinem", "keines",
    "nie", "niemals", "nichts", "niemand", "ohne", "weder", "kaum",
    # Englisch
    "not", "no", "never", "none", "without", "neither", "nor",
})

_NUMBER_RE = re.compile(r"\d[\d.,]*")

# --- Richtungs-Blindheit bei Vergleichs-Claims (QA100, 2026-07-27) --------
# Satz-Embeddings kodieren die ARGUMENT-REIHENFOLGE praktisch nicht: die
# Log-Belege aus der Live-Verifikation zeigten
#   cos=0.961  'Windkraft liefert mehr Strom als Photovoltaik'
#          ->  'photovoltaik liefert mehr strom als windkraft'
#   cos=0.976  'Kernkraft klimaschädlicher als Kohle'
#          ->  'kohle klimaschädlicher als kernkraft'
#   cos=0.967  'Kohle klimaFREUNDLICHER als Kernkraft'
#          ->  'kohle klimaSCHÄDLICHER als kernkraft'
# Alle drei liegen weit über SEMANTIC_THRESHOLD und sind bedeutungs-
# gegenteilig. Der Negations-Guard greift nicht: es gibt keine Negation,
# und die Zahlen sind identisch bzw. fehlen. Zwei zusätzliche Prüfungen
# schließen die Lücke — vertauschte Vergleichs-Operanden und getauschte
# Richtungswörter.

# Funktionswörter, die nach "als" kein Vergleichs-Operand sein können.
_ALS_STOP = frozenset({
    "der", "die", "das", "den", "dem", "des", "ein", "eine", "einen",
    "einem", "einer", "eines", "in", "im", "bei", "beim", "von", "vom",
    "zu", "zum", "zur", "auf", "für", "fuer", "mit", "und", "oder",
    "ist", "sind", "war", "waren", "wird", "werden", "es", "man", "sich",
    "ja", "wohl", "doch", "noch", "auch", "the", "a", "an", "of", "in",
})

# Richtungs-Antonyme als Wortstämme (Substring-Match auf Tokens, damit
# Komposita und Flexion mitgehen: "klimaSCHÄDLICHer", "preisGÜNSTIGer").
_POLARITY_ANTONYMS = (
    ("schädlich", "freundlich"), ("schaedlich", "freundlich"),
    ("gefährlich", "sicher"), ("gefaehrlich", "sicher"),
    ("höher", "niedriger"), ("hoeher", "niedriger"),
    ("höher", "geringer"), ("hoeher", "geringer"),
    ("mehr", "weniger"),
    ("größer", "kleiner"), ("groesser", "kleiner"),
    ("teurer", "billiger"), ("teurer", "günstiger"),
    ("besser", "schlechter"),
    ("schneller", "langsamer"),
    ("stärker", "schwächer"), ("staerker", "schwaecher"),
    ("steigt", "sinkt"), ("anstieg", "rückgang"), ("anstieg", "rueckgang"),
    ("zunahme", "abnahme"), ("mehrheit", "minderheit"),
    ("more", "less"), ("higher", "lower"), ("increase", "decrease"),
    # QA50D (2026-08-08) im Container reproduziert: bei diesen Paaren
    # schwieg der Guard, waehrend die Kontrollen (Operanden-Tausch,
    # schaedlich/freundlich) korrekt griffen. Produktionswirkung: wer die
    # Gegenfrage stellt, bekommt das gegenteilige Verdict mit voller
    # Konfidenz, ohne dass die Pipeline laeuft.
    # `_operands_swapped` fing sie nicht auf, weil auf beiden Seiten
    # dasselbe Vergleichsobjekt steht ("... als Maenner").
    ("häufiger", "seltener"), ("haeufiger", "seltener"),
    ("öfter", "seltener"), ("oefter", "seltener"),
    ("wärmer", "kälter"), ("waermer", "kaelter"),
    ("länger", "kürzer"), ("laenger", "kuerzer"),
    ("älter", "jünger"), ("aelter", "juenger"),
    ("früher", "später"), ("frueher", "spaeter"),
    ("reicher", "ärmer"), ("reicher", "aermer"),
    ("dicker", "dünner"), ("dicker", "duenner"),
    ("leichter", "schwerer"),
    ("gesünder", "ungesünder"), ("gesuender", "ungesuender"),
)


def _negation_present(tokens: set[str]) -> bool:
    return bool(tokens & _NEGATION_TOKENS)


def _has_stem(tokens: set[str], stem: str) -> bool:
    """Stamm in irgendeinem Token — aber NICHT, wenn ihm das negierende
    Präfix 'un' unmittelbar vorausgeht ('unsicher' ist kein 'sicher',
    'ungefährlich' kein 'gefährlich')."""
    for tok in tokens:
        idx = tok.find(stem)
        if idx < 0:
            continue
        if idx >= 2 and tok[idx - 2:idx] == "un":
            continue
        return True
    return False


def _comparison_operands(text: str):
    """(Tokens vor 'als', erster inhaltstragender Begriff nach 'als')
    für Vergleichs-Claims — sonst None."""
    m = re.search(r"\bals\b", text.lower())
    if not m:
        return None
    pre = set(re.findall(r"\w+", text[:m.start()].lower()))
    for w in re.findall(r"\w+", text[m.end():].lower()):
        if len(w) >= 3 and w not in _ALS_STOP:
            return pre, w
    return None


def _operands_swapped(a: str, b: str) -> bool:
    """True, wenn beide Claims dieselben zwei Dinge vergleichen, aber in
    VERTAUSCHTER Rolle ('A mehr als B' vs. 'B mehr als A'). Das Vergleichs-
    Objekt der einen Seite steht dann auf der anderen Seite vor dem 'als'
    — und umgekehrt."""
    oa, ob = _comparison_operands(a), _comparison_operands(b)
    if not oa or not ob:
        return False
    pre_a, obj_a = oa
    pre_b, obj_b = ob
    return (obj_a != obj_b and obj_a in pre_b and obj_b in pre_a)


def _direction_flipped(ta: set[str], tb: set[str]) -> bool:
    """True, wenn die eine Seite ein Richtungswort trägt und die andere
    genau dessen Gegenteil (und keine Seite beide) — 'klimafreundlicher'
    vs. 'klimaschädlicher', 'mehr' vs. 'weniger'. Verlangt bewusst die
    Anwesenheit BEIDER Pole (je einer pro Seite): ein einseitig fehlendes
    Richtungswort ist bloß eine Umformulierung, kein Gegenteil."""
    for x, y in _POLARITY_ANTONYMS:
        ax, ay = _has_stem(ta, x), _has_stem(ta, y)
        bx, by = _has_stem(tb, x), _has_stem(tb, y)
        if (ax and not ay and by and not bx) or (ay and not ax and bx and not by):
            return True
    return False


def _extract_numbers(text: str) -> set[str]:
    """Signifikante Zahlen aus dem Claim (Tausender-Punkte/Dezimal-Kommata
    entfernt). 'über 1.000' → {'1000'}, 'PISA 2018' → {'2018'}."""
    out: set[str] = set()
    for raw in _NUMBER_RE.findall(text):
        cleaned = raw.replace(".", "").replace(",", "")
        if cleaned.isdigit():
            out.add(cleaned)
    return out


def _polarity_mismatch(a: str, b: str) -> bool:
    """Heuristik gegen die Negations-Blindheit des semantischen Caches.

    Returns True, wenn a und b wahrscheinlich GEGENTEILIGE Aussagen sind:
      (1) eine Negation steht nur auf EINER der beiden Seiten, ODER
      (2) beide nennen Zahlen, die disjunkt sind (z.B. Jahr 2018 vs. 2022,
          'über 1000' vs. 'über 2000'), ODER
      (3) die Vergleichs-Operanden sind VERTAUSCHT ('A mehr als B' vs.
          'B mehr als A'), ODER
      (4) ein Richtungswort ist durch sein Gegenteil ersetzt
          ('klimafreundlicher' vs. 'klimaschädlicher').

    (3) und (4) kamen 2026-07-27 dazu, nachdem die Live-Verifikation der
    QA100-Fixes zeigte, dass Satz-Embeddings die Argument-Reihenfolge
    kaum kodieren (cos 0.96-0.98 zwischen gegenteiligen Vergleichs-
    Claims) — die Fehlerklasse trifft ausgerechnet die Claims, für die
    der L4-Layer eigene Muster hat.

    Bewusst konservativ in die SICHERE Richtung: ein False-Positive bewirkt
    nur einen Cache-Miss (die volle Pipeline läuft und liefert ein korrektes
    Ergebnis), während ein False-Negative das invertierte Verdict liefern
    würde — genau der Bug, den dieser Guard schließt.
    """
    ta = set(re.findall(r"\w+", a.lower()))
    tb = set(re.findall(r"\w+", b.lower()))
    if _negation_present(ta) != _negation_present(tb):
        return True
    na, nb = _extract_numbers(a), _extract_numbers(b)
    if na and nb and na.isdisjoint(nb):
        return True
    if _operands_swapped(a, b):
        return True
    if _direction_flipped(ta, tb):
        return True
    return False


def _purge_expired() -> None:
    """Entfernt abgelaufene Einträge."""
    now = time.time()
    expired = [k for k, (ts, *_) in _verdict_store.items()
               if now - ts > DEFAULT_TTL]
    for k in expired:
        del _verdict_store[k]


def _enforce_size_limit() -> None:
    """FIFO-Eviction wenn Store über MAX_STORE_SIZE."""
    if len(_verdict_store) <= MAX_STORE_SIZE:
        return
    # Älteste Einträge zuerst rausschmeißen
    items = sorted(_verdict_store.items(), key=lambda kv: kv[1][0])
    overflow = len(items) - MAX_STORE_SIZE
    for k, _ in items[:overflow]:
        del _verdict_store[k]


def _embed(text: str) -> Optional["np.ndarray"]:
    """Berechnet Sentence-BERT-Embedding (multilingual MiniLM-L12-v2).
    Returns None wenn Modell nicht verfügbar."""
    model = _get_st_model()
    if model is None:
        return None
    try:
        emb = model.encode(text, convert_to_numpy=True, normalize_embeddings=True)
        return emb
    except Exception as e:
        logger.warning(f"verdict_cache: embedding failed: {e}")
        return None


def get(claim: str) -> Optional[dict]:
    """Liefert ein gecachtes Verdict für ``claim``, falls verfügbar.

    Probiert in Reihenfolge:
      1. Exact-Match (Trim + lowercase) — sehr schnell, ohne Embedding
      2. Semantic-Match (Cosine ≥ SEMANTIC_THRESHOLD) — ~30 ms

    Returns das gecachte result-Dict oder None.
    """
    if not claim:
        return None
    _purge_expired()
    current_dv = get_data_version()

    # 1) Exact match
    norm = _normalize(claim)
    entry = _verdict_store.get(norm)
    if entry is not None:
        ts, emb, result, dv = entry
        if dv == current_dv and time.time() - ts <= DEFAULT_TTL:
            logger.info(f"verdict_cache: EXACT HIT for {claim[:60]!r}")
            return _annotate_hit(result, "exact", 1.0, claim)

    # 2) Semantic match
    if not _verdict_store:
        return None
    query_emb = _embed(claim)
    if query_emb is None:
        return None  # st-Modell nicht verfügbar — semantic-Pfad deaktiviert

    import numpy as np  # lazy: nur der semantische Pfad braucht numpy

    best_score = 0.0
    best_key = None
    best_result = None
    for key, (ts, emb, result, dv) in _verdict_store.items():
        if emb is None or dv != current_dv:
            continue
        if time.time() - ts > DEFAULT_TTL:
            continue
        # Negations-/Polaritäts-Guard: MiniLM ist negations-blind, ein
        # semantischer Treffer auf eine GEGENTEILIGE Aussage würde das
        # invertierte Verdict liefern. Solche Kandidaten überspringen —
        # der Claim läuft dann durch die volle Pipeline (sicher).
        if _polarity_mismatch(claim, key):
            continue
        # Cosine: beide normalisiert -> Skalarprodukt
        score = float(np.dot(query_emb, emb))
        if score > best_score:
            best_score = score
            best_key = key
            best_result = result

    if best_score >= SEMANTIC_THRESHOLD and best_result is not None:
        logger.info(
            f"verdict_cache: SEMANTIC HIT cos={best_score:.3f} "
            f"for {claim[:60]!r} -> matched {best_key[:60]!r}"
        )
        return _annotate_hit(best_result, "semantic", best_score, claim,
                             matched_claim=best_key)
    return None


def _annotate_hit(result: dict, hit_type: str, score: float,
                   claim: str, matched_claim: str | None = None) -> dict:
    """Annotiert das Cache-Hit-Result mit Metadata für Debugging.
    Mutiert das Dict NICHT — gibt eine Kopie zurück."""
    annotated = dict(result)
    annotated["_cache_hit"] = {
        "type": hit_type,
        "score": round(score, 4),
        "matched_claim": matched_claim,
    }
    return annotated


def put(claim: str, result: dict) -> None:
    """Speichert ein Verdict-Result für späteren Cache-Lookup.

    Cache-Filter:
      - Confidence muss ≥ MIN_CONFIDENCE_FOR_CACHE sein
      - Verdict darf NICHT 'unverifiable' sein (Stream-Loss-Artefakte
        + low-info Verdicts werden nicht gecached)
      - Result muss gültiges JSON-Dict mit 'verdict' + 'confidence' sein
    """
    if not claim or not result:
        return
    verdict = result.get("verdict")
    confidence = result.get("confidence")
    if not verdict or verdict == "unverifiable":
        return
    if confidence is None or confidence < MIN_CONFIDENCE_FOR_CACHE:
        return

    norm = _normalize(claim)
    emb = _embed(claim)
    dv = get_data_version()
    _verdict_store[norm] = (time.time(), emb, result, dv)
    _enforce_size_limit()
    logger.info(
        f"verdict_cache: STORED {claim[:60]!r} "
        f"(verdict={verdict}, conf={confidence}, store_size={len(_verdict_store)})"
    )


def clear() -> None:
    """Vollständige Cache-Leerung (z.B. für Tests)."""
    _verdict_store.clear()


def stats() -> dict:
    """Cache-Stats für Diagnose."""
    now = time.time()
    valid = sum(1 for ts, *_ in _verdict_store.values()
                if now - ts <= DEFAULT_TTL)
    with_emb = sum(1 for _, emb, *_ in _verdict_store.values()
                   if emb is not None)
    return {
        "total": len(_verdict_store),
        "valid_unexpired": valid,
        "with_embedding": with_emb,
        "max_size": MAX_STORE_SIZE,
        "ttl_seconds": DEFAULT_TTL,
        "semantic_threshold": SEMANTIC_THRESHOLD,
    }
