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

Quellengewichtung (WICHTIG):
- Wissenschaftliche Primärquellen (PubMed, WHO, EMA, Eurostat, Copernicus, EEA) haben HÖHERE Glaubwürdigkeit als Sekundärquellen
- Faktenchecker-Ergebnisse (ClaimReview/Google Fact Check) sind Sekundärquellen — sie fassen bestehende Erkenntnisse zusammen
- Wenn Faktenchecker-Ergebnisse den wissenschaftlichen Primärquellen WIDERSPRECHEN, gewichte die Primärquellen höher und weise im "nuance"-Feld auf den Widerspruch hin
- Wenn NUR Faktenchecker-Ergebnisse vorliegen (keine Primärquellen), weise im "nuance"-Feld darauf hin, dass keine unabhängige wissenschaftliche Bestätigung vorliegt
- Verdacht auf Verzerrung: Wenn alle Faktenchecker das gleiche Urteil haben aber Primärquellen ein anderes Bild zeigen, vertraue den Primärquellen

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

Source weighting (IMPORTANT):
- Scientific primary sources (PubMed, WHO, EMA, Eurostat, Copernicus, EEA) have HIGHER credibility than secondary sources
- Fact-checker results (ClaimReview/Google Fact Check) are secondary sources — they summarize existing findings
- If fact-checker results CONTRADICT scientific primary sources, weight the primary sources higher and note the contradiction in the "nuance" field
- If ONLY fact-checker results are available (no primary sources), note in the "nuance" field that no independent scientific confirmation exists
- Suspected bias: If all fact-checkers agree but primary sources show a different picture, trust the primary sources

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
        "höchste", "höchsten", "niedrigste", "niedrigsten", "meiste", "meisten",
        "größte", "größten", "beste", "besten", "schlechteste", "schlechtesten",
        "wenigste", "wenigsten", "stärkste", "stärksten",
        "highest", "lowest", "most", "least", "largest", "smallest", "best", "worst",
    ]
    claim_lower = original_claim.lower()
    is_superlative = any(kw in claim_lower for kw in SUPERLATIVE_KEYWORDS)

    # Build compact context — only essential fields to keep token count low
    context_parts = [
        f"{labels['claim']}: {original_claim}",
        f"{labels['category']}: {analysis.get('category', 'unknown')}\n",
    ]

    # Add superlative warning if only one country's data is available
    if is_superlative:
        all_countries = set()
        for source_data in source_results:
            if not isinstance(source_data, dict):
                continue
            for r in source_data.get("results", []):
                geo = r.get("geo", r.get("country", ""))
                if geo:
                    all_countries.add(geo)
        # Remove EU aggregate labels
        eu_labels = {"EU27_2020", "European Union", "European Union - 27 countries (from 2020)", "EU"}
        real_countries = all_countries - eu_labels
        if len(real_countries) <= 1:
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
            for r in results[:3]:  # Limit to top 3 per source
                # Only include key fields
                compact = {k: v for k, v in r.items() if v and k in (
                    "title", "name", "url", "journal", "date", "status",
                    "indicator_name", "value", "year", "country", "source",
                    "description", "variable", "time_range", "dataset_id",
                    "indicator",
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

            # No real sources → override verdict to unverifiable
            if not real_urls:
                logger.warning("No sources returned results — overriding verdict to unverifiable")
                result["verdict"] = "unverifiable"
                result["confidence"] = 0.0

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
