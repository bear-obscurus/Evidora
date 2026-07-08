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
    """Bind the module-global ``_model`` to the shared SentenceTransformer.

    The model itself lives in ``services._st_model`` so that the reranker and
    the backup-trigger/verdict-cache path share ONE instance instead of each
    loading its own (~250 MB saved). ``_model`` stays a module attribute here
    because several services import it directly
    (``from services.reranker import _load_model, _model``).
    """
    global _model, _available
    if _available is not None:
        return _available

    from services._st_model import get_model

    _model = get_model()
    _available = _model is not None
    if _available:
        logger.info("Reranker using shared Sentence Transformer instance")
    else:
        logger.info("sentence-transformers unavailable — semantic re-ranking disabled")
    return _available


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
    "factbook_volkskanzler",
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
    "ernaehrung_konsens_fact",           # Ernährungs-Pack (DGE + Cochrane + Mayo + Harvard + NHS + EFSA)
    "recht_konsens_fact",                # Recht-/Rechtsmythen-Pack (RIS + BGBl + BGH/OGH + AK)
    "energie_klima_konsens_fact",        # Energie/Klima-Politik-Pack (IPCC + IEA + Fraunhofer)
    "migration_konsens_fact",            # Migrations-Pack (BfV + bpb + IAB + DIW + OECD + BKA)
    "geographie_konsens_fact",           # Geographie/Reise-Pack (NASA + Lloyd's + NatGeo + CIA Factbook)
    "eurobarometer_fact",                # Eurobarometer (Europäische Kommission + EP)
    "finanzen_konsens_fact",             # Finanzen-Pack (EZB + Bundesbank + DAI + StiftungW + BaFin)
    "bildung_konsens_fact",              # Bildungs-Pack (APA + Hattie + EEF + Pashler + Nielsen)
    "internationale_quellen_fact",       # Pew + WMO + IMF + WTO (globale Perspektive)
    "sport_fitness_konsens_fact",        # Sport/Fitness-Pack (ACSM + Cochrane + ISSN + NSCA)
    "kunst_kultur_konsens_fact",         # Kunst/Kultur-Pack (Smithsonian + Harvard + Folger + UNESCO)
    "geschichts_mythen2_konsens_fact",   # Geschichts-Mythen-2-Pack (Stanford SEP + Britannica + Royal Society + Princeton)
    "reproduktion_konsens_fact",         # Reproduktions-Pack (NEJM Wilcox + Cochrane + ACOG + AUA + NAMS + NIH + WHO)
    "onkologie_konsens_fact",            # Onkologie-Pack (NCI + DKFZ + Cochrane + Cancer Research UK + USPSTF + IARC)
    "mental_health_konsens_fact",        # Mental-Health-Pack (DGPPN + NIMH + Cochrane + APA + NICE + WHO)
    "substanzen_konsens_fact",           # Substanzen-Pack (NIDA + EMCDDA + EFSA + WHO + Cochrane + FDA)
    "digital_familie_konsens_fact",      # Digital-Familie-Pack (AAP + APA + Orben Oxford + ABCD Study + EU Kids Online)
    "geldanlage_konsens_fact",           # Geldanlage-Pack (BaFin + FCA + SEC + ESMA + Stiftung Warentest)
    "alltags_mythen_konsens_fact",       # Alltags-Mythen-Pack (NHS + AAO + AASM + Mayo Clinic + NIH + Cochrane)
    "verkehrssicherheit_konsens_fact",   # Verkehrssicherheit-Pack (WHO + OECD-IRTAD + BASt + ADAC + IIHS + NHTSA)
    "tierhaltung_konsens_fact",          # Tierhaltung-Pack (ÖTK + Bundestierärztekammer + WSAVA + AAFCO + FAO + EFSA)
    "cybersecurity_konsens_fact",        # Cybersecurity-Pack (NIST + BSI + ENISA + EFF + Mozilla + NCSC)
    "lebensmittel_konsens_fact",         # Lebensmittel-Sicherheit-Pack (BfR + EFSA + FDA + RKI + AAP + ÖLMB)
    "gleichstellung_konsens_fact",       # Gleichstellung-Pack (EIGE + Eurostat + OECD + FRA + BMI/BKA + Statistik Austria + DESTATIS)
    "religionsgemeinschaften_konsens_fact",  # Religionsgemeinschaften-Pack (Pew + BfV + ADL + IHRA + RAND + UCDP + Sektenstellen)
    "wirtschaftspolitik_konsens_fact",       # Wirtschaftspolitik-Pack (WIFO + IHS + DIW + IFO + IWF + EZB + OECD + Sachverständigenrat + AK Wien + Bundesbank + IAB)
    "wohnen_konsens_fact",                   # Wohnen-Pack (DESTATIS + Statistik Austria + BMWSB + Wien-Wohnen + DIW + IFO + Pestel + IW Köln + Eurostat + Empirica + GBV)
    "arbeitsmarkt_konsens_fact",             # Arbeitsmarkt-Pack (AMS + WIFO + IHS + IAB + DESTATIS + Statistik Austria + AK Wien + DGB + OECD + Eurostat + Card/Krueger + Cengiz QJE + Bloom Stanford + Lalive/Zweimüller)
    "mobilitaet_konsens_fact",               # Mobilität-Pack (ADAC + ICCT + ÖBB + DB + UBA + Helmholtz + BMK + IIHS + BASt + Fraunhofer ISE + Agora Verkehrswende + VDV + FÖS + BAG + ASFINAG + BNetzA)
    "datenschutz_konsens_fact",              # Datenschutz-Pack (EuGH + BVerfG + AT-VfGH + Datenschutz-Behörde AT + Bitkom + NOYB + LfM + BSI + Citizen Lab + Amnesty Tech + EFF + Mozilla)
    "sozialstaat_konsens_fact",              # Sozialstaat-Pack AT-Spezial (Statistik Austria + BMSGPK + WIFO + IHS + AK Wien + AMS + PVA + AT-VfGH + OECD + Eurostat ESSPROS + Bertelsmann + Lalive/Zweimüller AER 2009)
    "demokratie_konsens_fact",               # Demokratie-Pack (V-Dem + Freedom House + Transparency CPI + RSF + IDEA + BMI + Statistik Austria + AT-VfGH + Eurobarometer + Bertelsmann + Duverger + Lijphart)
    "landwirtschaft_konsens_fact",           # Landwirtschaft-Pack (AGES + EFSA + IARC + WHO + BMVL + BOKU + Statistik Austria + FAO + IPES-Food + Wagenigen UR + Seufert 2012 Nature + Ponisio 2015 + Smith 2019 NatComm)
    "welthandel_konsens_fact",               # Welthandel-Pack (Statistik Austria + WIFO + IFO + Bank of England + LSE + Weltbank + IEA + BNEF + WTO + UNCTAD + Piketty + Saez + Krugman + Cecchini-Bericht)
    "inklusion_konsens_fact",                # Inklusion-Pack (DSM-5 + ICD-11 + UN-BRK 2006 + WHO ICF 2001 + CDC + DKHWB + Cochrane + Hattie 2009/2023 + Flynn 1987 + Heckman NBER + Aron 1996 + Greven 2019 + Lovaas 1987 + Dawson 2010 + TEACCH + ASAN + Statistik Austria + BMSGPK + AT BGStG + EU EAA + ÖAR)
    "sicherheitspolitik_konsens_fact",       # Sicherheitspolitik-Pack (AT BVG 1955 + AT-VfGH + B-VG + UN-Resolutionen 68/262 + ES-11/4 + ICJ + EuGH + BVerwG + NATO Strategic Concept 2022 + EU PESCO + EU Strategic Compass + EU Hybrid Toolbox + SIPRI + RAND + DSN + BMLV + BMI + Hybrid CoE Helsinki)
    "medlineplus_health_topic",          # NIH MedlinePlus Live (National Library of Medicine, US)
    "cdc_newsroom_item",                 # CDC Newsroom Live (Centers for Disease Control, US)
    "cdc_open_data_dataset",             # CDC Open Data Live (Socrata-Catalog data.cdc.gov, US)
    "gdelt_gkg_article",                 # GDELT v2 Global Knowledge Graph Live (BigQuery, 100k+ News-Quellen weltweit)
    "wikipedia_article",                 # Wikipedia Live (REST API, DE-first + EN-Fallback)
    "owid_indicator",                    # OurWorldInData Live (CC-BY 4.0, ~31 Indikatoren Klima/Health/Economy/Demographics)
    "vdem_index",                        # V-Dem Live (University of Gothenburg, Varieties of Democracy v16, 11 Indizes)
    "wayback_archive",                   # Internet Archive Wayback Machine CDX (URL-Archiv-Snapshots)
    "crossref_paper",                    # Crossref REST API (DOI-Resolution + Paper-Metadata, polite-pool)
    "openaq_measurement",                # OpenAQ v3 Live (Air Quality Sensor-Daten, CC-BY 4.0, requires API-key)
    "wikidata_fact",                     # Wikidata SPARQL (CC0, 10 strukturierte Templates: Person/Politik/Land/Org/Werk/Geographie)
    "freedom_house_score",               # Freedom House FIW 2024 Static-First (55 Länder × 6 Indikatoren, komplementär zu V-Dem)
    "arxiv_preprint",                    # arXiv Live (Cornell-Hosting, Atom-XML, ~2 Mio Preprints, NICHT peer-reviewed)
    "uncomtrade_trade",                  # UN Comtrade Live (M49-Codes + HS-Klassifikation, bilaterale Handelsflüsse)
    "eige_news_item",                    # EIGE Live (European Institute for Gender Equality, Vilnius)
    "clinvar_variant",                   # NIH ClinVar Live (NCBI Genetic Variants, US)
    "snopes_factcheck_item",             # Snopes Live (US, IFCN-zertifiziert)
    "correctiv_factcheck_item",          # Correctiv Live (DE-Recherchezentrum, IFCN)
    "full_fact_factcheck_item",          # Full Fact Live (UK-Charity, IFCN)
    "bellingcat_investigation_item",     # Bellingcat Live (UK/global, OSINT)
    "factcheck_org_item",                # FactCheck.org Live (Annenberg Univ. Pennsylvania)
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

# Static-first pack results that entered via the cosine-BACKUP fallback
# (provenance ``_matched_exact == False``) are a weak signal — no exact
# substring/composite trigger fired, the pack was only "maybe relevant" by
# semantic proximity. On Austria-flavoured claims that pulls thematically
# distant packs (Rechnungshof/Sozialstaat/Mobilität on a SIPRI claim) via
# shared "Österreich + Ausgaben" vocabulary. Such backup matches must clear
# a stricter bar than the lax RELEVANCE_THRESHOLD. Exact-trigger matches and
# untagged results are unaffected (provenance is the discriminator, not the
# score — same principle as the STRUKTURELL provenance gate, 0bb0f48).
BACKUP_THRESHOLD = 0.55

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

            # Filter out off-topic results below threshold. Cosine-backup
            # pack matches (provenance _matched_exact == False) only entered
            # because no exact trigger fired, so they must clear the stricter
            # BACKUP_THRESHOLD. Exact-trigger / untagged results keep the
            # source threshold (safe default → no change for unmigrated packs).
            before_count = len(scored)

            def _eff_threshold(r):
                if (threshold < BACKUP_THRESHOLD
                        and r.get("_matched_exact") is False):
                    return BACKUP_THRESHOLD
                return threshold

            scored = [(r, s) for r, s in scored if s >= _eff_threshold(r)]
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
                # 2026-05-24 Debug-Verbose: bisher debug-only, jetzt info,
                # damit wir bei Defense-in-Depth-Audit den Pfad sehen,
                # ob ein Source wirklich durch den Reranker geht.
                # Bei Verdacht auf Reranker-Skip einer Source kann man
                # diesen Log nutzen, um zu verifizieren ob die Source
                # die Reranker-Routine erreicht.
                logger.info(
                    f"Reranked {source_name}: kept {len(results)}/{len(results)} "
                    f"(no removal, top score: {top_score:.3f}, threshold: {threshold})"
                )

        if total_removed:
            logger.info(f"Relevance filter: removed {total_removed} off-topic results total")

        return source_results

    except Exception as e:
        logger.warning(f"Semantic re-ranking failed: {e}")
        return source_results


# Sentinel-Prefixe gespiegelt aus services/_struct_marker.py (dort die
# Quelle der Wahrheit). Hier dupliziert, um einen Import-Zyklus
# reranker↔_struct_marker zu vermeiden.
_STRUCT_EXACT = "STRUKTURELL FALSCH:"
_STRUCT_COSINE = "STRUKTURELL_COSINE_FALSCH:"


def resolve_struct_marker_provenance(source_results: list) -> list:
    """Bug #47 Wurzel-Fix (Safe-Subset) — Cosine-Kontamination entschärfen.

    Pack-Services emittieren STRUKTURELL-Marker, die via schwachem Cosine-
    Backup (statt exaktem Trigger) gematcht wurden, mit dem auflösbaren
    Prefix ``STRUKTURELL_COSINE_FALSCH:``. Dieser Pass löst sie auf:

      • Existiert IRGENDWO ein exakter Anker (``STRUKTURELL FALSCH:``) für
        denselben Claim → die Cosine-Marker sind mit hoher Sicherheit
        themenfremde Kontamination (per find_matching_items über geteilte
        AT-/DE-Vokabeln reingezogen) → zu Klartext degradieren (Prefix weg,
        Inhalt bleibt als schwache Info erhalten).
      • KEIN exakter Anker → der Cosine-Marker ist das einzige strukturelle
        Signal (z.B. adjazente Medizin-Mythen bei „Kurkuma heilt Krebs") →
        zu normalem ``STRUKTURELL FALSCH:`` restaurieren (Verhalten wie vor
        dem Fix, keine Regression).

    Läuft NACH rerank_results, VOR dem Prompt-Build — Prompt UND
    Postprocess-Override sehen damit nur die aufgelösten Marker. Mutiert
    display_values in place.
    """
    has_exact = False
    cosine_results: list = []
    for sd in source_results:
        if not isinstance(sd, dict):
            continue
        for r in sd.get("results", []):
            dv = r.get("display_value", "")
            if not isinstance(dv, str):
                continue
            if _STRUCT_EXACT in dv:
                has_exact = True
            if _STRUCT_COSINE in dv:
                cosine_results.append(r)

    if not cosine_results:
        return source_results

    if has_exact:
        for r in cosine_results:
            r["display_value"] = (r["display_value"]
                                  .replace(_STRUCT_COSINE + " ", "")
                                  .replace(_STRUCT_COSINE, ""))
        logger.info(
            f"STRUKTURELL provenance: exakter Anker vorhanden → "
            f"{len(cosine_results)} Cosine-Backup-Marker zu Klartext "
            f"degradiert (Bug #47 Kontaminations-Filter)."
        )
    else:
        for r in cosine_results:
            r["display_value"] = r["display_value"].replace(
                _STRUCT_COSINE, _STRUCT_EXACT)
        logger.debug(
            f"STRUKTURELL provenance: kein exakter Anker → "
            f"{len(cosine_results)} Cosine-Marker zu normalem "
            f"STRUKTURELL FALSCH: restauriert."
        )
    return source_results
