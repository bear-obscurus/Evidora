"""ÖIF (Österreichischer Integrationsfonds) + ZARA (Zivilcourage und Anti-
Rassismus-Arbeit) — kuratierter Static-First-Topic-Service zu Migration /
Integration / Anti-Diskriminierung in Österreich.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Quellen-Mix (zwei methodisch sehr unterschiedliche NGO/Quasi-NGO-Quellen):
- **ÖIF** (Österreichischer Integrationsfonds) — BMI-nahe NGO, gibt die
  offiziellen Integrationsberichte heraus. Statistik-orientiert, geringes
  Bias-Risiko; primär aggregierter Konsens aus Statistik Austria, BMI,
  Eurostat, OECD. Deskriptive Aussagen zur Migration/Integration AT.
- **ZARA** (Zivilcourage und Anti-Rassismus-Arbeit) — NGO mit klarer
  Anti-Rassismus-Position. Vorfalls-Statistiken sind **selbstmeldungs-
  basiert** (nicht repräsentativ, nicht Gerichtsstatistik). Bei jedem
  ZARA-Fact ist der methodische Caveat explizit im
  ``kernsatz_fuer_synthesizer`` angelegt — KEINE harten STRUKTURELL-
  Marker, sondern nuanciert.

Politische Guardrails (project_political_guardrails.md):
- **Keine Partei-Bewertung**: Pack klassifiziert KEINE politische Bewegung
  als 'integrations-feindlich' oder 'pro-Migration'. Reform-Debatten zu
  Familiennachzug, Wertekurs, Mindestsicherung werden NICHT bewertet.
- **Keine Prognosen**: keine Vorhersagen zur Integrationsentwicklung oder
  Migrations-Trends.
- **Keine eigene Links/Rechts-Klassifikation**: Pack klassifiziert weder
  ZARA noch ÖIF normativ — zitiert nur die Selbstauskunft + methodische
  Status der Organisation.
- **Wikipedia-only-Cap bei normativen Termen**: Pack triggert NICHT auf
  pauschal-normative Aussagen ('AT ist rassistisch'); fokussiert auf
  empirische Aussagen (Zahlen, Rechtsstatus, Indikator-Struktur).

ZARA-spezifisch: bei jedem ZARA-fact ist der Disclaimer
"ZARA-Zahl ist Selbstmeldungs-basiert (nicht repräsentativ), kein
Gerichtsstatistik. Bei Vergleichen mit BMI-/Polizeistat-Daten ist
methodische Differenz zu beachten." nuanciert im kernsatz_fuer_synthesizer
verankert — KEIN harter STRUKTURELL-Marker (anders als bei
verschwoerungen_pack), weil die Aussage NICHT "false" ist, sondern eine
**methodisch differenzierte Zitation** erfordert.

Topic-Auswahl (20 facts, Stand 2026-05-23):
- 15 ÖIF-facts (deskriptiv-statistisch, geringes Bias-Risiko):
  asylanträge, migrationssaldo, einbürgerung, sprachkurse,
  integrationsmonitor, migrationshintergrund, arbeitsmarkt-integration,
  pisa-migration, eu-drittstaat-struktur, subsidiärer schutz,
  sozialleistungen-migration, familiennachzug, anerkennung-qualifikation,
  wertekurs, grundversorgung.
- 5 ZARA-facts (NGO-Quelle mit Methodik-Disclaimer):
  rassismus report, hate speech online, arbeitswelt-diskriminierung,
  rechtsschutz-beratung, zivilcourage-struktur.

Komplementär zu:
- statistik_austria.py (amtliche Asyl-/Einbürgerungs-/Migrations-Salden)
- migration_pack.py (Migrations-Mythos-Cluster wie 'Großer Austausch',
  'Sozialmagnet', 'Demografie-Arbeitsmarkt')
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "oeif_zara.json",
)


def _descriptor(f: dict) -> tuple[dict, str]:
    """Descriptor for the cosine-similarity backup trigger."""
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {notes}"[:300])


def _claim_matches_facts(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    return find_matching_items(
        STATIC_JSON_PATH, "facts",
        claim_lc=claim_lc, full_claim=full_claim,
        descriptor_fn=_descriptor,
    )


def claim_mentions_oeif_zara_cached(claim: str) -> bool:
    """Trigger-Detection: Migrations-/Integrations-/Rassismus-Themen +
    AT-Kontext aus dem Static-Pack.

    Substring/Composite + Reranker-Backup-Fallback laufen zentral über
    services/_topic_match.find_matching_items.
    """
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_oeif_zara(client=None):
    """Lade die statischen Facts (für data_updater + Tests)."""
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict zu Synthesizer-tauglichem Text.

    WICHTIG — abweichend von verschwoerungen_pack / esoterik_pack:
    Wir verwenden NICHT den STRUKTURELL-FALSCH-Marker. ÖIF/ZARA-facts
    formulieren VERDICT-LEITLINIEN (nicht harte 'IST FALSCH'-Aussagen),
    die der Synthesizer kontextsensitiv abwägen soll. Bei ZARA-facts
    ist der Methodik-Disclaimer (Selbstmeldung, nicht repräsentativ)
    nuanciert verankert — eine harte 'STRUKTURELL FALSCH'-Marker-
    Aktivierung wäre hier irreführend (die Zitation der ZARA-Zahl IST
    methodisch valide, nur die Interpretation als 'amtliche Statistik'
    wäre falsch).

    Stattdessen geben wir die data-Felder zeilenweise gerendert weiter
    und überlassen dem Synthesizer-Prompt die VERDICT-Leitlinien-
    Auswertung aus dem ``kernsatz_fuer_synthesizer``.
    """
    parts: list[str] = []
    skip_keys = ("context",)
    for key, val in d.items():
        if key in skip_keys:
            continue
        if isinstance(val, str) and val.strip():
            label = key.replace("_", " ").strip()
            parts.append(f"{label.capitalize()}: {val}")
    return " | ".join(parts)


async def search_oeif_zara(analysis: dict) -> dict:
    """Top-5-Treffer aus dem Static-Pack für den Claim.

    Returns einen reranker-tauglichen Result-Block mit type-Tag
    ``oeif_zara_konsens`` — vom Reranker als authoritative Whitelist
    behandelbar (im main.py-Wiring).
    """
    empty = {
        "source": "ÖIF Integrationsbericht + ZARA Rassismus Report — AT Migration/Integration",
        "type": "oeif_zara_konsens",
        "results": [],
    }

    claim = (analysis or {}).get("original_claim") or (analysis or {}).get("claim", "") or ""
    matches = _claim_matches_facts(claim.lower(), full_claim=claim)
    if not matches:
        return empty

    # Top-5 (find_matching_items returnt ohnehin Substring/Composite
    # ODER Top-3 Reranker-Fallback; falls Substring viele Treffer findet,
    # behalten wir konservativ die ersten 5).
    matches = matches[:5]

    results: list[dict] = []
    for fact in matches:
        topic = fact.get("topic", "")
        d = fact.get("data") or {}
        url = fact.get("source_url", "")
        secondary = fact.get("secondary_url", "")
        label = fact.get(
            "source_label",
            "ÖIF Integrationsbericht / ZARA Rassismus Report",
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
            "indicator": "oeif_zara_konsens_fact",
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
        "source": "ÖIF Integrationsbericht + ZARA Rassismus Report — AT Migration/Integration",
        "type": "oeif_zara_konsens",
        "results": results,
    }
