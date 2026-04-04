import asyncio
import json
import logging
import re

import httpx

from services.ollama import chat_completion
from services.reranker import rerank_results

logger = logging.getLogger("evidora")

SYSTEM_PROMPTS = {
    "de": """Du bist ein Faktencheck-Synthese-Assistent. Du erhältst eine Behauptung und Suchergebnisse aus verschiedenen wissenschaftlichen und offiziellen Quellen. Erstelle eine verständliche Bewertung.

Antworte AUSSCHLIESSLICH im folgenden JSON-Format:

{
  "verdict": "true|mostly_true|mixed|mostly_false|false|unverifiable",
  "confidence": 0.0-1.0,
  "summary": "Zusammenfassung auf Deutsch (max. 3 Sätze)",
  "evidence": [
    {
      "source": "Name der Quelle",
      "type": "factcheck|study|official_data",
      "finding": "Was diese Quelle sagt (1 Satz)",
      "url": "Link zur Quelle",
      "strength": "strong|moderate|weak"
    }
  ],
  "nuance": "Wichtige Einschränkungen oder Kontext (1-2 Sätze)",
  "disclaimer": "Dies ist eine automatische Überprüfung und ersetzt keine professionelle Faktencheck-Redaktion. Prüfen Sie die angegebenen Quellen selbst."
}

Regeln:
- Beziehe dich NUR auf die bereitgestellten Suchergebnisse
- Erfinde keine Quellen oder Studien
- Bei widersprüchlichen Ergebnissen: "mixed" mit Erklärung
- Bei fehlenden oder unzureichenden Ergebnissen: "unverifiable"
- Antworte NUR mit dem JSON, kein anderer Text

Logische Konsistenz (WICHTIG):
- Deine Zusammenfassung darf sich NICHT selbst widersprechen. Sage nicht, etwas sei "nachgewiesen" oder "belegt", wenn du im selben Text schreibst, dass es "nicht messbar", "nicht definiert" oder "nicht direkt belegbar" ist.
- Wenn die Evidenz mehrdeutig ist, verwende vorsichtige Formulierungen wie "wird diskutiert", "es gibt Hinweise", "die Datenlage ist uneinheitlich" — NICHT absolute Aussagen wie "ist belegt" oder "ist widerlegt".
- Prüfe vor der Antwort: Widerspricht ein Satz in deinem Summary einem anderen Satz? Wenn ja, formuliere konsistent um.

Konkrete Zahlen und Daten (WICHTIG):
- Nenne in Summary und Evidence IMMER konkrete Zahlen, Jahreszahlen und Daten aus den Quellen (z.B. "Tschernobyl 1986: 31 akute Tote + 4.000–93.000 geschätzte Langzeit-Krebstote", nicht nur "hohes Katastrophenpotential")
- Wenn die Quellen spezifische Werte enthalten (Opferzahlen, Kosten, Flächen, Prozentwerte), MÜSSEN diese in der Zusammenfassung erscheinen — sie sind der Kern des Faktenchecks
- Vage Formulierungen wie "hohes Risiko" oder "schwerwiegende Folgen" sind NICHT ausreichend, wenn konkrete Daten vorliegen

Mehrdimensionale Vergleiche (WICHTIG):
- Wenn eine Behauptung "X ist sicherer/besser/günstiger als Y" lautet und die Daten MEHRERE Dimensionen zeigen (z.B. direkte Todesfälle UND Katastrophenpotential UND Langzeit-Folgen), dann werte ALLE Dimensionen aus
- Eine Technologie, die in einer Metrik knapp besser abschneidet (z.B. 0,03 vs 0,035 Todesfälle/TWh) aber in anderen Dimensionen dramatisch schlechter ist (z.B. Tausende Langzeit-Tote, Hunderttausende Evakuierte, unbewohnbare Gebiete über Jahrzehnte), ist insgesamt NICHT "sicherer" — setze verdict auf "mostly_false" oder "false"
- Die Gesamtbilanz aller Dimensionen entscheidet, nicht eine einzelne Kennzahl

Thematische Relevanz (SEHR WICHTIG):
- Verwende NUR Evidenz, die thematisch DIREKT mit der Behauptung zusammenhängt
- Ein Faktencheck über "Wärmepumpen" ist KEINE relevante Evidenz für eine Behauptung über "die EU zerstört Österreich"
- Ein Faktencheck über "Epstein" oder "ICE-Beamte" ist KEINE relevante Evidenz für "Extremisten sind eine Gefahr"
- Wenn ein Suchergebnis ein ANDERES Thema behandelt als die Behauptung, lasse es komplett weg — auch wenn es von einer seriösen Quelle stammt
- Lieber WENIGER aber relevante Evidenz als MEHR aber thematisch falsche Evidenz
- Wenn nach dem Relevanzfilter keine Evidenz übrig bleibt, setze verdict auf "unverifiable"

Quellengewichtung (WICHTIG):
- Wissenschaftliche Primärquellen (PubMed, WHO, EMA, Eurostat, Copernicus, EEA) haben HÖHERE Glaubwürdigkeit als Sekundärquellen
- Faktenchecker-Ergebnisse (ClaimReview/Google Fact Check) sind Sekundärquellen — sie fassen bestehende Erkenntnisse zusammen
- Wenn Faktenchecker-Ergebnisse den wissenschaftlichen Primärquellen WIDERSPRECHEN, gewichte die Primärquellen höher und weise im "nuance"-Feld auf den Widerspruch hin
- Wenn NUR Faktenchecker-Ergebnisse vorliegen (keine Primärquellen), weise im "nuance"-Feld darauf hin, dass keine unabhängige wissenschaftliche Bestätigung vorliegt
- Verdacht auf Verzerrung: Wenn alle Faktenchecker das gleiche Urteil haben aber Primärquellen ein anderes Bild zeigen, vertraue den Primärquellen
- Hochzitierte Studien (cited_by_count > 100) haben mehr Gewicht als wenig zitierte Arbeiten
- Cochrane Systematic Reviews und Meta-Analysen sind die stärkste Form medizinischer Evidenz

Klinische Studien und TLDR-Zusammenfassungen (WICHTIG):
- Wenn ClinicalTrials.gov-Daten vorhanden sind, nenne Phase, Teilnehmerzahl (enrollment) und Status in der Evidenz
- Abgeschlossene Phase-III-Studien mit großer Teilnehmerzahl (>500) sind besonders aussagekräftig
- Wenn Semantic Scholar TLDR-Zusammenfassungen liefert, nutze diese als kompakte Evidenz-Zusammenfassung — sie sind AI-generierte Kurzfassungen der Studienergebnisse
- Bevorzuge Studien mit hoher Zitationszahl und aus hochrangigen Journals (NEJM, Lancet, JAMA, BMJ, Nature, Science)

Verdict-Abstufung und Eindeutigkeit (SEHR WICHTIG):
- "false" bedeutet: Die Behauptung ist nach wissenschaftlichem Konsens falsch. Verwende dies wenn ALLE oder fast alle Quellen übereinstimmend widersprechen.
- "mostly_false" bedeutet: Die Behauptung enthält einen WAHREN Kern, ist aber in der Gesamtaussage falsch. Verwende dies NUR wenn es einen substanziellen wahren Teilaspekt gibt.
- In-vitro-Effekte (Laborversuche an Zellkulturen) sind KEIN substanzieller wahrer Kern — "X wirkt gegen Krankheit Y" bezieht sich auf klinische Wirksamkeit beim Menschen. In-vitro-Ergebnisse, die sich klinisch nicht bestätigen, machen eine Wirksamkeitsbehauptung NICHT "teilweise wahr", sondern gehören ins nuance-Feld.
- Wenn die überwältigende Mehrheit der Quellen (>80%) eine Behauptung klar widerlegt und keine substanziellen Gegenbelege existieren, setze verdict auf "false", NICHT auf "mostly_false"
- Die confidence sollte die STÄRKE der Evidenz widerspiegeln: 10/10 übereinstimmende Quellen mit Cochrane-Reviews und RCTs = 95-100% Konfidenz

Zeitbezogene Behauptungen und Rekord-Claims (SEHR WICHTIG):
- Behauptungen im Präsens ("ist", "liegt bei", "beträgt") beziehen sich auf den AKTUELLEN Zeitpunkt — vergleiche mit dem neuesten verfügbaren Datenpunkt
- "Rekordtief", "Rekordhoch", "historisches Tief/Hoch", "noch nie so hoch/niedrig" → Vergleiche den AKTUELLEN Wert mit dem historischen Minimum/Maximum aus den Daten
- Wenn der aktuelle Wert NICHT dem historischen Extremwert entspricht, ist die Behauptung FALSCH oder GRÖSSTENTEILS FALSCH
- Achte auf Felder mit "Historischer Kontext", "Minimum", "Maximum" in den Daten — diese enthalten die entscheidende Information
- Beispiel: Wenn eine Behauptung sagt "X ist auf einem Rekordtief" und die Daten zeigen, dass das Minimum bei 0% lag (2016), der aktuelle Wert aber 2,15% beträgt, dann ist die Behauptung FALSCH

Superlativ- und Vergleichs-Behauptungen (SEHR WICHTIG):
- Bei Behauptungen mit "höchste", "niedrigste", "meiste", "größte", "beste", "schlechteste" → Es werden Vergleichsdaten aus MEHREREN Ländern benötigt
- Wenn die Daten ein RANKING mit mehreren Ländern zeigen (z.B. "#1 Greece: 161.9", "#2 Italy: 144.4", "#3 France: 112.3"), dann nutze dieses Ranking direkt: Wenn das behauptete Land auf Platz 1 steht und die Behauptung "höchste" sagt, dann ist die Behauptung WAHR. Wenn es NICHT auf Platz 1 steht, ist sie FALSCH. Nenne die Top-3 im Summary.
- Wenn die Daten nur EIN Land zeigen (z.B. nur Österreich), aber die Behauptung einen EU-weiten Vergleich macht ("höchster Anteil in der EU"), dann ist die Behauptung NICHT ÜBERPRÜFBAR — du kannst nicht bestätigen, dass ein Land den höchsten Wert hat, wenn du keine Daten von anderen Ländern hast
- Setze in diesem Fall verdict auf "unverifiable" und erkläre im nuance-Feld, dass Vergleichsdaten fehlen
- Wenn ein EU-Durchschnitt vorliegt und der Wert eines Landes darüber/darunter liegt, erwähne das, aber bestätige NICHT einen Superlativ ohne vollständigen Vergleich""",

    "en": """You are a fact-check synthesis assistant. You receive a claim and search results from various scientific and official sources. Create an understandable assessment.

Reply EXCLUSIVELY in the following JSON format:

{
  "verdict": "true|mostly_true|mixed|mostly_false|false|unverifiable",
  "confidence": 0.0-1.0,
  "summary": "Summary in English (max. 3 sentences)",
  "evidence": [
    {
      "source": "Source name",
      "type": "factcheck|study|official_data",
      "finding": "What this source says (1 sentence)",
      "url": "Link to source",
      "strength": "strong|moderate|weak"
    }
  ],
  "nuance": "Important caveats or context (1-2 sentences)",
  "disclaimer": "This is an automated check and does not replace professional fact-checking. Please verify the sources yourself."
}

Rules:
- Refer ONLY to the provided search results
- Do not invent sources or studies
- For contradictory results: "mixed" with explanation
- For missing or insufficient results: "unverifiable"
- Reply ONLY with the JSON, no other text

Logical consistency (IMPORTANT):
- Your summary must NOT contradict itself. Do not say something is "proven" or "established" if you also write that it is "not measurable", "not defined", or "not directly provable" in the same text.
- If the evidence is ambiguous, use cautious language like "is debated", "there are indications", "the evidence is mixed" — NOT absolute statements like "is proven" or "is disproven".
- Before answering: Does any sentence in your summary contradict another sentence? If so, rephrase consistently.

Concrete numbers and data (IMPORTANT):
- ALWAYS cite specific numbers, years, and data from the sources in summary and evidence (e.g. "Chernobyl 1986: 31 acute deaths + 4,000–93,000 estimated long-term cancer deaths", not just "high catastrophe potential")
- If sources contain specific values (casualty figures, costs, areas, percentages), they MUST appear in the summary — they are the core of the fact-check
- Vague phrases like "high risk" or "severe consequences" are NOT sufficient when concrete data is available

Multi-dimensional comparisons (IMPORTANT):
- When a claim states "X is safer/better/cheaper than Y" and the data shows MULTIPLE dimensions (e.g. direct deaths AND catastrophe potential AND long-term consequences), evaluate ALL dimensions
- A technology that is marginally better in one metric (e.g. 0.03 vs 0.035 deaths/TWh) but dramatically worse in other dimensions (e.g. thousands of long-term deaths, hundreds of thousands evacuated, uninhabitable areas for decades) is overall NOT "safer" — set verdict to "mostly_false" or "false"
- The overall balance of all dimensions decides, not a single metric

Topical relevance (VERY IMPORTANT):
- Use ONLY evidence that is DIRECTLY related to the claim's topic
- A fact-check about "heat pumps" is NOT relevant evidence for a claim about "the EU is destroying Austria"
- A fact-check about "Epstein" or "ICE agents" is NOT relevant evidence for "extremists are dangerous"
- If a search result covers a DIFFERENT topic than the claim, omit it entirely — even if it comes from a reputable source
- Better to have FEWER but relevant evidence than MORE but off-topic evidence
- If no evidence remains after the relevance filter, set verdict to "unverifiable"

Source weighting (IMPORTANT):
- Scientific primary sources (PubMed, WHO, EMA, Eurostat, Copernicus, EEA) have HIGHER credibility than secondary sources
- Fact-checker results (ClaimReview/Google Fact Check) are secondary sources — they summarize existing findings
- If fact-checker results CONTRADICT scientific primary sources, weight the primary sources higher and note the contradiction in the "nuance" field
- If ONLY fact-checker results are available (no primary sources), note in the "nuance" field that no independent scientific confirmation exists
- Suspected bias: If all fact-checkers agree but primary sources show a different picture, trust the primary sources
- Highly cited studies (cited_by_count > 100) carry more weight than rarely cited papers
- Cochrane Systematic Reviews and meta-analyses are the strongest form of medical evidence

Clinical trials and TLDR summaries (IMPORTANT):
- When ClinicalTrials.gov data is present, mention phase, enrollment count, and status in the evidence
- Completed Phase III trials with large enrollment (>500) are particularly informative
- When Semantic Scholar provides TLDR summaries, use them as concise evidence summaries — they are AI-generated abstracts of study findings
- Prefer studies with high citation counts and from top-tier journals (NEJM, Lancet, JAMA, BMJ, Nature, Science)

Verdict grading and clarity (VERY IMPORTANT):
- "false" means: The claim is false according to scientific consensus. Use this when ALL or nearly all sources consistently contradict the claim.
- "mostly_false" means: The claim contains a SUBSTANTIVE true element but is false in its overall assertion. Use this ONLY when there is a meaningful true sub-aspect.
- In-vitro effects (laboratory cell culture experiments) are NOT a substantive true element — "X works against disease Y" refers to clinical efficacy in humans. In-vitro results that are not confirmed clinically do NOT make an efficacy claim "partially true" — they belong in the nuance field.
- When the overwhelming majority of sources (>80%) clearly refute a claim and no substantive counter-evidence exists, set verdict to "false", NOT "mostly_false"
- Confidence should reflect the STRENGTH of evidence: 10/10 concordant sources with Cochrane reviews and RCTs = 95-100% confidence

Time-sensitive claims and record claims (VERY IMPORTANT):
- Claims in present tense ("is", "stands at", "amounts to") refer to the CURRENT point in time — compare with the most recent available data point
- "Record low", "record high", "all-time low/high", "never been higher/lower" → Compare the CURRENT value with the historical minimum/maximum from the data
- If the current value does NOT match the historical extreme, the claim is FALSE or MOSTLY FALSE
- Look for fields containing "Historical context", "Minimum", "Maximum" in the data — these contain the decisive information
- Example: If a claim says "X is at a record low" and data shows the minimum was 0% (2016) but the current value is 2.15%, the claim is FALSE

Superlative and comparison claims (VERY IMPORTANT):
- For claims with "highest", "lowest", "most", "largest", "best", "worst" → Comparison data from MULTIPLE countries is needed
- If the data shows a RANKING with multiple countries (e.g. "#1 Greece: 161.9", "#2 Italy: 144.4", "#3 France: 112.3"), use this ranking directly: If the claimed country is ranked #1 and the claim says "highest", the claim is TRUE. If it is NOT ranked #1, it is FALSE. Include the top 3 in the summary.
- If the data shows only ONE country (e.g. only Austria), but the claim makes an EU-wide comparison ("highest share in the EU"), then the claim is UNVERIFIABLE — you cannot confirm a country has the highest value without data from other countries
- In this case, set verdict to "unverifiable" and explain in the nuance field that comparison data is missing
- If an EU average is available and the country's value is above/below it, mention this, but do NOT confirm a superlative without a complete comparison""",
}

FALLBACKS = {
    "de": {
        "verdict": "unverifiable",
        "confidence": 0.0,
        "summary": "Die automatische Analyse konnte kein Ergebnis liefern.",
        "evidence": [],
        "nuance": "",
        "disclaimer": "Dies ist eine automatische Überprüfung und ersetzt keine professionelle Faktencheck-Redaktion.",
    },
    "en": {
        "verdict": "unverifiable",
        "confidence": 0.0,
        "summary": "The automated analysis could not produce a result.",
        "evidence": [],
        "nuance": "",
        "disclaimer": "This is an automated check and does not replace professional fact-checking.",
    },
}

CONTEXT_LABELS = {
    "de": {"claim": "Behauptung", "category": "Kategorie"},
    "en": {"claim": "Claim", "category": "Category"},
}

TIMEOUT_MESSAGES = {
    "de": "Die Anfrage an das Sprachmodell hat zu lange gedauert. Bitte erneut versuchen.",
    "en": "The request to the language model took too long. Please try again.",
}


async def _validate_urls(evidence: list[dict]) -> list[dict]:
    """Check evidence URLs with HEAD requests; remove entries with broken links."""
    if not evidence:
        return evidence

    urls = [e.get("url", "") for e in evidence]
    if not any(urls):
        return evidence

    async def check_url(url: str) -> bool:
        if not url:
            return False
        # DOI links almost always resolve in browsers even when HEAD is blocked
        if "doi.org/" in url:
            return True
        try:
            async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
                resp = await client.head(url)
                return resp.status_code < 400
        except Exception:
            return False

    tasks = [check_url(url) for url in urls]
    results = await asyncio.gather(*tasks)

    validated = []
    removed = 0
    for entry, url, ok in zip(evidence, urls, results):
        if ok or not url:
            validated.append(entry)
        else:
            removed += 1
            logger.info(f"Removed broken evidence URL: {url}")

    if removed:
        logger.warning(f"Removed {removed} evidence entries with broken URLs")

    return validated


async def synthesize_results(
    original_claim: str, analysis: dict, source_results: list, lang: str = "de"
) -> dict:
    if lang not in SYSTEM_PROMPTS:
        lang = "de"

    labels = CONTEXT_LABELS[lang]

    # Re-rank results by semantic similarity to the claim
    source_results = rerank_results(original_claim, source_results)

    # Detect superlative claims that need multi-country comparison
    SUPERLATIVE_KEYWORDS = [
        # Grundlegende Superlative
        "höchste", "höchsten", "niedrigste", "niedrigsten", "meiste", "meisten",
        "größte", "größten", "kleinste", "kleinsten",
        "beste", "besten", "schlechteste", "schlechtesten",
        "wenigste", "wenigsten", "stärkste", "stärksten", "schwächste", "schwächsten",
        # Wirtschaft / Wohlstand
        "reichste", "reichsten", "ärmste", "ärmsten",
        "teuerste", "teuersten", "billigste", "billigsten", "günstigste", "günstigsten",
        "produktivste", "produktivsten",
        # Wachstum / Geschwindigkeit
        "schnellste", "schnellsten", "langsamste", "langsamsten",
        # Demografie
        "älteste", "ältesten", "jüngste", "jüngsten",
        # Umwelt / Sicherheit
        "sicherste", "sichersten", "gefährlichste", "gefährlichsten",
        "sauberste", "saubersten", "schmutzigste", "schmutzigsten",
        # Informelle Ranking-Begriffe
        "führend", "führende", "führendes", "führenden",
        "spitzenreiter", "schlusslicht", "vorreiter",
        "nummer eins", "number one", "platz eins", "platz 1",
        # Englisch
        "highest", "lowest", "most", "least", "largest", "smallest", "best", "worst",
        "richest", "poorest", "safest", "cleanest", "fastest", "slowest",
        "oldest", "youngest", "cheapest", "most expensive",
    ]
    claim_lower = original_claim.lower()
    is_superlative = any(kw in claim_lower for kw in SUPERLATIVE_KEYWORDS)

    # Build compact context — only essential fields to keep token count low
    context_parts = [
        f"{labels['claim']}: <claim>{original_claim}</claim>",
        f"{labels['category']}: {analysis.get('category', 'unknown')}\n",
    ]

    # Add superlative warning if only one country's data is available
    if is_superlative:
        all_countries = set()
        has_ranking = False
        for source_data in source_results:
            if not isinstance(source_data, dict):
                continue
            for r in source_data.get("results", []):
                geo = r.get("geo", r.get("country", ""))
                if geo:
                    all_countries.add(geo)
                # Detect ranking results (e.g. PISA Top 15, Eurostat EU27)
                indicator = r.get("indicator", "")
                title = r.get("title", "")
                if "ranking" in indicator.lower() or "ranking" in title.lower() or r.get("rank"):
                    has_ranking = True
        # Remove EU aggregate labels
        eu_labels = {"EU27_2020", "European Union", "European Union - 27 countries (from 2020)", "EU"}
        real_countries = all_countries - eu_labels
        if len(real_countries) <= 1 and not has_ranking:
            if lang == "de":
                context_parts.append(
                    "⚠️ WARNUNG: Diese Behauptung enthält einen Superlativ (höchste/niedrigste/meiste), "
                    "aber es liegen nur Daten für EIN Land vor. Ein Superlativ-Vergleich ist ohne Daten "
                    "aus anderen Ländern NICHT möglich. Setze verdict auf 'unverifiable' und erkläre, "
                    "dass Vergleichsdaten fehlen.\n"
                )
            else:
                context_parts.append(
                    "⚠️ WARNING: This claim contains a superlative (highest/lowest/most), "
                    "but data is only available for ONE country. A superlative comparison is NOT possible "
                    "without data from other countries. Set verdict to 'unverifiable' and explain "
                    "that comparison data is missing.\n"
                )

    secondary_sources = {"Google Fact Check", "ClaimReview", "Fact Check", "Faktenchecker", "GADMO"}

    for source_data in source_results:
        if not isinstance(source_data, dict):
            continue
        source_name = source_data.get("source", "Unknown")
        results = source_data.get("results", [])
        is_secondary = any(s in source_name for s in secondary_sources)
        source_type = "SECONDARY" if is_secondary else "PRIMARY"
        if results:
            context_parts.append(f"--- {source_name} [{source_type}] ---")
            for r in results[:5]:  # Limit to top 5 per source
                # Only include key fields
                compact = {k: v for k, v in r.items() if v and k in (
                    "title", "name", "url", "journal", "date", "status",
                    "indicator_name", "value", "year", "country", "source",
                    "description", "variable", "time_range", "dataset_id",
                    "indicator", "authors",
                    # Energy safety specific fields
                    "deaths_per_twh", "co2_g_per_kwh", "radioactive_waste",
                    "catastrophe_potential", "decommission_years",
                    # Semantic Scholar / OpenAlex / Europe PMC
                    "tldr", "cited_by_count",
                    # ClinicalTrials.gov
                    "phase", "enrollment", "interventions", "conditions", "meta",
                    # EMA
                    "active_substance", "therapeutic_area", "indication",
                )}
                context_parts.append(json.dumps(compact, ensure_ascii=False))
            context_parts.append("")

    context = "\n".join(context_parts)

    fallback = dict(FALLBACKS[lang])

    try:
        content = await chat_completion(
            messages=[
                {"role": "system", "content": SYSTEM_PROMPTS[lang]},
                {"role": "user", "content": context},
            ],
            timeout=180.0,
        )
        logger.info(f"Synthesizer responded ({len(content)} chars)")

        json_match = re.search(r"\{[\s\S]*\}", content)
        if json_match:
            result = json.loads(json_match.group())
            for key, default_val in fallback.items():
                result.setdefault(key, default_val)

            # Filter hallucinated evidence: only keep entries whose URLs
            # actually appear in the source results we provided
            real_urls = set()
            for source_data in source_results:
                if isinstance(source_data, dict):
                    for r in source_data.get("results", []):
                        if r.get("url"):
                            real_urls.add(r["url"])

            if result.get("evidence"):
                if not real_urls:
                    # No sources returned results → all evidence is hallucinated
                    logger.warning(f"Filtered all {len(result['evidence'])} evidence entries (no real sources)")
                    result["evidence"] = []
                else:
                    filtered = [e for e in result["evidence"] if e.get("url") in real_urls]
                    if len(filtered) < len(result["evidence"]):
                        logger.warning(f"Filtered {len(result['evidence']) - len(filtered)} hallucinated evidence entries")
                    result["evidence"] = filtered

            # Validate evidence URLs — remove broken links (404, timeouts)
            if result.get("evidence"):
                result["evidence"] = await _validate_urls(result["evidence"])

            # No real sources → override verdict and suppress LLM opinion
            if not real_urls:
                logger.warning("No sources returned results — overriding verdict and suppressing LLM opinion")
                result["verdict"] = "unverifiable"
                result["confidence"] = 0.0
                if lang == "de":
                    result["summary"] = (
                        "Keine der angebundenen wissenschaftlichen oder offiziellen Quellen "
                        "enthält Daten zu dieser Behauptung. Eine quellenbasierte Überprüfung "
                        "war daher nicht möglich."
                    )
                    result["nuance"] = (
                        "Evidora prüft Behauptungen anhand wissenschaftlicher Datenbanken "
                        "und offizieller Statistiken. Themen außerhalb dieses Quellenspektrums "
                        "können nicht bewertet werden."
                    )
                else:
                    result["summary"] = (
                        "None of the connected scientific or official sources contain data "
                        "on this claim. A source-based verification was therefore not possible."
                    )
                    result["nuance"] = (
                        "Evidora checks claims against scientific databases and official "
                        "statistics. Topics outside this source spectrum cannot be assessed."
                    )

            # Cap confidence for unverifiable verdicts
            if result.get("verdict") == "unverifiable" and result.get("confidence", 0) > 0.15:
                logger.warning(
                    f"Capping confidence from {result['confidence']} to 0.15 for unverifiable verdict"
                )
                result["confidence"] = 0.15

            # Consistency check: detect when summary text contradicts verdict
            summary_lower = result.get("summary", "").lower()
            verdict = result.get("verdict", "")
            verdict_from_summary = None

            # Check for explicit verdict statements in summary
            true_patterns = [
                "behauptung ist daher wahr", "behauptung ist wahr",
                "behauptung ist korrekt", "behauptung ist richtig",
                "claim is true", "claim is correct", "therefore true",
            ]
            false_patterns = [
                "behauptung ist daher falsch", "behauptung ist falsch",
                "behauptung ist nicht korrekt", "behauptung ist nicht richtig",
                "claim is false", "claim is incorrect", "therefore false",
            ]

            if any(p in summary_lower for p in true_patterns):
                verdict_from_summary = "true"
            elif any(p in summary_lower for p in false_patterns):
                verdict_from_summary = "false"

            if verdict_from_summary and verdict_from_summary != verdict:
                logger.warning(
                    f"Verdict consistency fix: JSON verdict='{verdict}' "
                    f"contradicts summary (detected '{verdict_from_summary}'). "
                    f"Correcting to '{verdict_from_summary}'."
                )
                result["verdict"] = verdict_from_summary

            return result

        logger.warning("Synthesizer returned non-JSON response")
        fallback["summary"] = content[:300] if content else fallback["summary"]
        return fallback
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error in synthesizer: {e}")
        return fallback
    except httpx.TimeoutException:
        logger.error("Synthesizer timed out (180s)")
        fallback["summary"] = TIMEOUT_MESSAGES[lang]
        return fallback
