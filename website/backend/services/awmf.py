"""AWMF — Arbeitsgemeinschaft der Wissenschaftlichen Medizinischen
Fachgesellschaften e.V. — Leitlinien-Register.

Datenquelle: AWMF Leitlinienregister (https://register.awmf.org) — das
zentrale deutsche Leitlinien-Verzeichnis fuer Medizin (~800 Leitlinien
von 180+ Fachgesellschaften). Die Leitlinien sind nach S-Klassifikation
geordnet:

  S1 = Handlungsempfehlung von Experten (niedrigster Evidenzgrad)
  S2k = formaler Konsens (konsensbasiert, ohne systematische Recherche)
  S2e = evidenzbasiert (systematische Recherche, ohne Konsens-Prozess)
  S3  = evidenz- UND konsensbasiert (hoechster Grad, Goldstandard)

Datenzugang: AWMF stellt das Register oeffentlich ohne Login bereit;
einzelne Leitlinien-PDFs sind frei abrufbar. Wir verwenden den
**Static-Snapshot-Pfad** — eine kuratierte Auswahl der wichtigsten
S3- und S2-Leitlinien zu Volkskrankheiten wird als JSON gepflegt und
quartalsweise aktualisiert (Begruendung: Leitlinien werden alle 3-5
Jahre revidiert, ein Snapshot ist also stabil; ein REST-Endpunkt ist
nicht offiziell dokumentiert).

Lizenz-Hinweis: Leitlinien-Texte unterliegen den Urheberrechten der
publizierenden Fachgesellschaften. Wir zitieren ausschliesslich
bibliographische Metadaten (Register-Nr, Fachgesellschaft, Jahr, URL)
und Kern-Empfehlungssaetze nach wissenschaftlicher Standardpraxis.
AWMF-Register selbst ist oeffentliches Verzeichnis ohne Login-Sperre.

Komplementaer zu existierenden Quellen:
- cochrane.py: PubMed-Live-Lookup fuer Cochrane-Reviews — komplementaer
  zur deutschen Leitlinien-Sicht.
- mental_health_pack.py: psychiatrische Mythen mit DGPPN/NIMH/Cochrane —
  AWMF ergaenzt mit formalen Leitlinien-Empfehlungen.
- gesundheits_autoritaeten_pack.py: institutionelle Gesundheits-
  Autoritaeten (RKI/BMG/ECDC) — AWMF ergaenzt mit Behandlungs-Standards.
- AWMF: deutsches Leitlinien-Register, formale Therapie-Empfehlungen.

Politische Guardrails (siehe project_political_guardrails.md):
AWMF-Leitlinien sind medizinisch-wissenschaftlich (keine Partei-
Politik). ABER: bei kontroversen medizin-ethischen Themen
(Cannabis-Verordnung, Adipositas-Chirurgie, Sterbehilfe-Debatte,
Schwangerschaftsabbruch) bleibt der Service strikt deskriptiv —
zitiert nur "die Leitlinie sagt X" und nicht "X ist gut/richtig".
Der Synthesizer-Layer entscheidet, ob die Daten eine Aussage stuetzen
oder nicht.

# WIRING fuer main.py (NICHT in diesem PR ausgefuehrt):
# from services.awmf import search_awmf, claim_mentions_awmf_cached
# if claim_mentions_awmf_cached(claim):
#     tasks.append(cached("AWMF", search_awmf, analysis))
#     queried_names.append("AWMF")
"""

from __future__ import annotations

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "awmf.json",
)


# ---------------------------------------------------------------------------
# Descriptor fuer Reranker-Backup (Cosine-Fallback)
# ---------------------------------------------------------------------------
def _descriptor(f: dict) -> tuple[dict, str]:
    """Repraesentation fuer den Cosine-Backup-Fall.

    Headline + Kern-Empfehlung + Top-1-Context-Note liefern eine
    kompakte Beschreibung fuer den Reranker-Backup-Pfad.
    """
    head = f.get("headline", "")
    d = f.get("data") or {}
    kern = d.get("kernempfehlung", "") or ""
    notes = " ".join((f.get("context_notes") or [])[:1])
    return (f, f"{head}. {kern[:200]}. {notes}"[:400])


def _claim_matches_facts(
    claim_lc: str, full_claim: str | None = None,
) -> list[dict]:
    """Substring/Composite-Match (preferred) + Reranker-Backup-Fallback."""
    return find_matching_items(
        STATIC_JSON_PATH, "facts",
        claim_lc=claim_lc, full_claim=full_claim,
        descriptor_fn=_descriptor,
    )


def claim_mentions_awmf_cached(claim: str) -> bool:
    """Trigger-Check: wird AWMF fuer diesen Claim aktiviert?

    Trigger-Themen (siehe data/awmf.json `trigger_keywords` +
    `trigger_composite`):
    - Krankheits-Keywords (diabetes, hypertonie, depression, asthma,
      copd, demenz, migraene, parkinson, schlaganfall, herzinsuffizienz,
      adipositas, schizophrenie, adhs, kreuzschmerz, mammakarzinom,
      darmkrebs, prostatakrebs, lungenkrebs, covid-19, long-covid,
      epilepsie, multiple sklerose, osteoporose, palliativ, niereninsuff,
      schilddruese, neurodermitis, rheumatoide arthritis, suizid).
    - Leitlinien-Bezug (leitlinie, S3-Leitlinie, S2k-Leitlinie, NVL,
      Nationale VersorgungsLeitlinie, AWMF) zusammen mit Krankheit oder
      Therapie/Empfehlung.
    """
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


# ---------------------------------------------------------------------------
# Optionale Snapshot-Loader-Funktion (fuer data_updater + Tests)
# ---------------------------------------------------------------------------
async def fetch_awmf(client=None):
    """Snapshot-Loader — gibt alle Facts zurueck (fuer data_updater-Prefetch).

    Signatur identisch zu anderen Static-Services (eter, mental_health_pack
    etc.).
    """
    return load_items(STATIC_JSON_PATH, "facts")


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _build_display(fact: dict) -> str:
    """Display-Value fuer einen Leitlinien-Eintrag.

    Format: "<S-Level>-Leitlinie <Scope> (<Fachgesellschaft>, Stand <Jahr>):
    <Kern-Empfehlung>."
    """
    d = fact.get("data") or {}
    s_level = d.get("s_level", "S?")
    scope = fact.get("scope", "")
    fachgesellschaft = d.get("fachgesellschaft", "")
    last_revision = d.get("last_revision") or fact.get("year") or "—"
    kern = d.get("kernempfehlung", "") or ""

    header = f"{s_level}-Leitlinie {scope}"
    if fachgesellschaft:
        # Erste Fachgesellschaft fuer kompakte Anzeige
        first_fg = fachgesellschaft.split("+")[0].strip()
        header += f" ({first_fg}, Stand {last_revision})"
    else:
        header += f" (Stand {last_revision})"

    if kern:
        return f"{header}: {kern[:500]}"
    return f"{header}."


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_awmf(analysis: dict) -> dict:
    """Static-Lookup gegen das AWMF-Snapshot fuer medizinische Claims.

    Strategie:
    1. Trigger-Check via Substring/Composite (data/awmf.json definiert
       Trigger-Keywords pro Leitlinie).
    2. Falls keine direkten Treffer: Reranker-Backup (top-3 mit
       Cosine >= 0.45).
    3. Pro Match: einen Result-Eintrag im Evidora-Standard-Schema bauen.

    Politische Guardrails: bei kontroversen medizinisch-ethischen Themen
    bleibt der Service deskriptiv (zitiert nur Leitlinien-Position,
    kein eigenes "gut/schlecht"-Urteil). Synthesizer-Layer entscheidet.
    """
    empty = {
        "source": "AWMF Leitlinienregister (S3-Leitlinien)",
        "type": "medical_guideline",
        "results": [],
    }

    analysis = analysis or {}
    claim = (
        analysis.get("original_claim")
        or analysis.get("claim", "")
        or ""
    )
    if not isinstance(claim, str):
        claim = str(claim or "")

    matches = _claim_matches_facts(claim.lower(), full_claim=claim)
    if not matches:
        return empty

    results: list[dict] = []
    for fact in matches:
        topic = fact.get("topic", "")
        d = fact.get("data") or {}
        register_nummer = d.get("awmf_register_nummer", "")
        s_level = d.get("s_level", "")
        fachgesellschaft = d.get("fachgesellschaft", "")
        scope = fact.get("scope", "")
        last_revision = (
            d.get("last_revision") or fact.get("year") or ""
        )
        url = fact.get("source_url") or (
            f"https://register.awmf.org/de/leitlinien/detail/"
            f"{register_nummer}"
            if register_nummer else "https://register.awmf.org"
        )
        secondary = fact.get("secondary_url")
        label = fact.get(
            "source_label",
            "AWMF Leitlinienregister",
        )
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)

        display = _build_display(fact)

        # Indicator-Name: "{S-Level}-Leitlinie {Scope-Kurz}"
        indicator_name = (
            f"{s_level}-Leitlinie {scope}" if scope and s_level
            else fact.get("headline", "AWMF-Leitlinie")
        )[:300]

        results.append({
            "indicator_name": indicator_name,
            "indicator": f"awmf_{topic}" if topic else "awmf_leitlinie",
            "country": fact.get("country", "DE"),
            "year": str(last_revision),
            "topic": topic,
            "display_value": display,
            "description": notes_joined,
            "url": url,
            "secondary_url": secondary,
            "source": fachgesellschaft or label,
        })

    # Hard-Cap: max 5 Treffer
    results = results[:5]

    logger.info(
        f"AWMF: {len(results)} Treffer fuer Claim '{claim[:60]}'"
    )
    return {
        "source": "AWMF Leitlinienregister (S3-Leitlinien)",
        "type": "medical_guideline",
        "results": results,
    }
