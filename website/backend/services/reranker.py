"""Semantic re-ranking of source results using Sentence Transformers.

Uses paraphrase-multilingual-MiniLM-L12-v2 for cross-language support (DE/EN).
Loads model lazily on first use (~120 MB download, ~250 MB RAM).
If sentence-transformers is not installed, falls back to no-op (results unchanged).
"""

import logging

logger = logging.getLogger("evidora")

_model = None
_available = None


def _load_model():
    """Lazy-load the sentence transformer model."""
    global _model, _available
    if _available is not None:
        return _available

    try:
        from sentence_transformers import SentenceTransformer

        _model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
        _available = True
        logger.info("Sentence Transformer model loaded (paraphrase-multilingual-MiniLM-L12-v2)")
        return True
    except ImportError:
        _available = False
        logger.info("sentence-transformers not installed — semantic re-ranking disabled")
        return False
    except Exception as e:
        _available = False
        logger.warning(f"Failed to load Sentence Transformer: {e}")
        return False


def _result_text(result: dict) -> str:
    """Extract searchable text from a result entry."""
    parts = []
    for key in ("title", "name", "indicator_name", "description", "journal",
                 "rating", "claim", "tldr", "authors"):
        val = result.get(key)
        if val:
            parts.append(str(val))
    return " ".join(parts)


# Bug V: Indicator types that mark *authoritative* direct-links curated
# by the source service itself (topic-mapping in RIS, methodology blocks
# everywhere).  These should never be filtered by the semantic reranker
# — their relevance is pre-validated by the trigger logic, and their
# descriptions are deliberately generic ("Direktlink zur konsolidierten
# Fassung") so they cannot reliably embed-match arbitrary claim wording.
_AUTHORITATIVE_INDICATORS = (
    "ris_geltende_fassung",       # B-VG, ABGB, … direct-links via §-ref
    "ris_geltende_fassung_topic", # B-VG, ORF-BeitrG, … via topic mapping
    "context",                    # methodology caveats
    "vbg_count_total",            # VBG aggregate ("109 since 1964")
    "vbg_top_anzahl",             # VBG ranking-summary
    "vbg_top_beteiligung",
    "vbg_top_neueste",
    "vbg_top_älteste",
    "wahl_nrw_ranking",           # Wahl ranking-summary
    "wahl_bpw_ranking",
    "wahl_euw_ranking",
    "factbook_religion_vienna",          # AT-Factbook curated entries
    "factbook_religion_vienna_subgroup",
    "factbook_wien_schule_sprache",
    "factbook_wien_schule_staatsbuerger",
    "factbook_subsidies_at",
    "factbook_subsidies_check",
    "factbook_social_assistance_at",
    "factbook_social_check",
    "factbook_pension_at",
    "factbook_pension_mindestpension",
    "factbook_health_blocked",
    "factbook_asyl_quartal",
    "factbook_asyl_familienzu",
    "factbook_citizenship_at",
    "factbook_asyl_ranking",
    "factbook_sparpaket_at",
    "factbook_sparpaket_verteidigung",
    "factbook_sparpaket_pendlereuro",
    "factbook_sparpaket_korridor",
    "factbook_sparpaket_familie",
    "factbook_energy_tariffs_at",
    "factbook_klimaticket",
    "factbook_stromsozialtarif",
    "factbook_gasnetzgebuehren",
    "factbook_naturalized_equal",
    "factbook_health_mis",
    "factbook_vegan_counter",
    "factbook_labor_shortage_at",
    "factbook_heat_pumps_at",
    "factbook_food_inflation",
    "factbook_eu_pakt_solidaritaet",
    "factbook_eu_pakt_aussengrenz",
    "factbook_eu_austritt_counter",
    "factbook_eu_pakt_general",
    "factbook_bmf_mwst_2026",
    "factbook_bmf_kalte_progression",
    "factbook_bmf_overview",
    "pks_overall",                       # BKA PKS-Hauptbericht
    "pks_drug_crime",                    # BKA Lagebericht Suchtmittel
    "pks_drug_crime_vienna",
    "pks_youth_drug_check",
    "pks_youth_general",
    "pks_plausibility",
    "pks_wien_bezirke",
    "pks_aufklaerungsquote",
    "pks_abschiebung_straftaeter",
    "Methodologie-Vergleich AMS-vs-ILO",  # Statistik Austria caveat
    # DACH-Factbook
    "dach_bamf_asyl",
    "dach_buergergeld",
    "dach_buergergeld_counter",
    "dach_heizungsgesetz",
    "dach_heizung_counter",
    "dach_ch_ahv",
    "dach_ch_el_counter",
    "dach_ch_migration_ahv",
    "dach_venedig_counter",
    "dach_klimaskepsis_counter",
    "skeptical_science_counter",
    "biorxiv_preprint",
    "mimikama_classic",                  # Mimikama-Archiv-Klassiker
    "at_faktencheck_classic",            # APA/Kontrast-Archiv-Klassiker
    "eu_courts_ruling",                  # EuGH/EGMR Schlüsselurteile
    "eu_crime_fact",                     # Eurostat Crime + DACH PKS Eckwerte
    "energy_charts_fact",                # Energy-Charts/APG Stromhandel + EE-Anteil
    "medientransparenz_fact",            # RTR/KommAustria Inserate-§2-Daten
    "rki_surveillance_fact",             # RKI SurvStat Surveillance-Eckwerte
    "biorxiv_classic",                   # bioRxiv Anti-Vax-/COVID-Klassiker
    "education_dach_fact",               # TIMSS/PIRLS/PISA/Lehrer-Bedarf DACH
    "at_courts_ruling",                  # VfGH/VwGH Schlüsselerkenntnisse
    "oecd_health_fact",                  # OECD Health DACH-Eckwerte
    "housing_at_fact",                   # Wohnungsmarkt AT (OeNB + EU-SILC)
    "transport_at_fact",                 # Verkehr AT (ÖBB + UBA + KlimaTicket)
    "esoterik_skeptic_fact",             # Esoterik-Pack (GWUP + Cochrane + Skeptiker)
    "geschichte_konsens_fact",           # Geschichts-Pack (DÖW + USHMM + bpb)
    "verschwoerungen_konsens_fact",      # Verschwörungen-Pack (BVerfG + VS + ADL/IKG/DÖW)
    "tech_ki_konsens_fact",              # Tech-/KI-Pack (NIST + EFF + ACM)
    "gesundheits_autoritaeten_fact",     # Gesundheits-Autoritäten (NIH + CDC + BfR + IARC)
    "destatis_de_fact",                  # DESTATIS DE-Baseline (Bevölkerung, BIP, Inflation, etc.)
    "tier_natur_konsens_fact",           # Tier-/Natur-Pack (Smithsonian + AMNH + Britannica + Snopes)
    "dach_asylbLG_counter",
    "dach_ch_frauen_65",
    "dach_asyl_vergleich",
    "dach_pensions_vergleich",
    "dach_hpv_impfraten",
    "eu_beschluss_detail",
    "eu_beschluesse_overview",
    "retraction_watch_classic",
    "frontex_main",
    "frontex_route",
    "wifo_ihs_main",
    "oenb_main",
    "oenb_euro_austritt_counter",
)


"""Minimum cosine-similarity score.  Results below this threshold are
considered off-topic and removed before the synthesizer ever sees them."""
RELEVANCE_THRESHOLD = 0.25

# Fact-checker databases return short, generic titles that inflate
# cosine-similarity scores (e.g. "gefährlich" matches everything
# containing "gefährlich").  A stricter threshold is needed.
# 2026-04: raised from 0.45 → 0.55 after observing off-topic dpa results
# passing at 0.47–0.53 for an unrelated CPI claim. Genuine topical matches
# typically score ≥ 0.65 on this multilingual model, so 0.55 cleanly
# separates real hits from shared-vocabulary noise ("Italien", "EU").
FACTCHECK_THRESHOLD = 0.55
_FACTCHECK_SOURCES = {"GADMO", "DataCommons", "ClaimReview", "Faktenchecker", "Fact Check",
                       "AT-Faktencheck-RSS", "Mimikama"}

# Academic databases (OpenAlex, Semantic Scholar) return papers whose titles
# often share keywords with the claim without being topically relevant
# ("ESG Reporting", "Green Growth", "Food Self-Sufficiency" matched a
# Ukraine-corruption claim at 0.27–0.38).  A stricter threshold drops these
# off-topic papers while keeping genuinely relevant work (≥ 0.45 typically).
# 2026-04: introduced at 0.40 after Ukraine CPI test surfaced noise.
ACADEMIC_THRESHOLD = 0.40
_ACADEMIC_SOURCES = {"OpenAlex", "Semantic Scholar"}

# Bug B (energy safety): For claims explicitly about energy-source
# mortality / safety comparisons ("deaths per TWh", "Tote pro TWh",
# "Mortalitätsrate Energieträger"), the dedicated OWID Energy Safety
# source already provides authoritative, peer-reviewed numbers (Sovacool
# et al. via OWID).  Academic databases on the same query return many
# adjacent-but-not-relevant papers (carbon-capture optimization,
# coal-to-hydrogen, permanent-magnet drives) that share vocabulary
# without addressing the mortality comparison.  When the claim matches
# the energy-safety pattern, raise the academic threshold so only the
# tightest matches survive.
_ENERGY_SAFETY_PATTERNS = (
    "tote pro twh", "tote/twh", "todesfälle pro twh",
    "mortalität pro twh", "mortalitätsrate",
    "deaths per twh", "deaths/twh",
    "fatalities per twh", "death rate.*energy",
    "mortality.*energy", "energy.*mortality",
    "pro twh", "per twh",
)
ACADEMIC_THRESHOLD_ENERGY_SAFETY = 0.55


def rerank_results(claim: str, source_results: list) -> list:
    """Re-rank results within each source by semantic similarity to the claim.

    Results whose similarity score falls below ``RELEVANCE_THRESHOLD`` are
    removed entirely so the synthesizer does not present off-topic evidence.

    Args:
        claim: The original claim text.
        source_results: List of source dicts with "results" lists.

    Returns:
        The same list with results re-ordered by relevance (most similar first)
        and off-topic entries removed.
    """
    if not _load_model():
        return source_results

    try:
        from sentence_transformers import util
        import re as _re

        claim_embedding = _model.encode(claim, convert_to_tensor=True)
        total_removed = 0
        # Bug B: detect energy-safety / deaths-per-TWh claims once, then
        # use a stricter academic threshold for them (OWID has the
        # authoritative numbers, academic DBs offer adjacent noise).
        claim_lc = (claim or "").lower()
        is_energy_safety_claim = any(
            _re.search(p, claim_lc) for p in _ENERGY_SAFETY_PATTERNS
        )

        for source_data in source_results:
            if not isinstance(source_data, dict):
                continue
            results = source_data.get("results", [])
            if not results:
                continue

            # Bug V: Separate authoritative entries from regular ones.
            # Authoritative entries (curated direct-links, methodology
            # blocks, ranking summaries) bypass the reranker — they were
            # already validated by the trigger logic and their texts are
            # deliberately generic.
            authoritative: list[dict] = []
            rerankable: list[dict] = []
            for r in results:
                if r.get("indicator") in _AUTHORITATIVE_INDICATORS:
                    authoritative.append(r)
                else:
                    rerankable.append(r)

            if not rerankable:
                # All entries were authoritative — keep them all, no rerank
                source_data["results"] = authoritative
                continue

            texts = [_result_text(r) for r in rerankable]
            result_embeddings = _model.encode(texts, convert_to_tensor=True)
            scores = util.cos_sim(claim_embedding, result_embeddings)[0]

            # Sort rerankable results by similarity score (descending)
            scored = sorted(zip(rerankable, scores.tolist()), key=lambda x: x[1], reverse=True)

            # Use stricter thresholds for sources prone to keyword-noise.
            # Fact-checkers have very short headlines that inflate cosine
            # scores; academic titles share generic keywords with off-topic
            # papers (ESG, green growth, etc.).  See threshold constants.
            source_name = source_data.get("source", "Unknown")
            is_factcheck = any(fc in source_name for fc in _FACTCHECK_SOURCES)
            is_academic = any(ac in source_name for ac in _ACADEMIC_SOURCES)
            if is_factcheck:
                threshold = FACTCHECK_THRESHOLD
            elif is_academic:
                # Bug B: tighten threshold for energy-safety claims —
                # OWID Energy Safety is authoritative, academic results
                # are typically adjacent noise.
                threshold = (ACADEMIC_THRESHOLD_ENERGY_SAFETY
                             if is_energy_safety_claim else ACADEMIC_THRESHOLD)
            else:
                threshold = RELEVANCE_THRESHOLD

            # Filter out off-topic results below threshold
            before_count = len(scored)
            scored = [(r, s) for r, s in scored if s >= threshold]
            removed = before_count - len(scored)
            total_removed += removed

            # Authoritative entries always stay — prepend them so they
            # appear first in the results list.
            source_data["results"] = authoritative + [r for r, _ in scored]

            top_score = scored[0][1] if scored else 0
            if removed:
                logger.info(
                    f"Reranked {source_name}: kept {len(scored)}/{before_count} results "
                    f"(removed {removed} below threshold {threshold}, top score: {top_score:.3f})"
                )
            else:
                logger.debug(f"Reranked {len(results)} results for {source_name} (top score: {top_score:.3f})")

        if total_removed:
            logger.info(f"Relevance filter: removed {total_removed} off-topic results total")

        return source_results

    except Exception as e:
        logger.warning(f"Semantic re-ranking failed: {e}")
        return source_results
