"""SpaCy NER enrichment and keyword extraction for claim analysis.

Runs deterministic Named Entity Recognition and noun-chunk-based keyword
extraction on the claim text. Results are merged into the Mistral-produced
analysis dict to improve downstream source queries.

German model: de_core_news_lg (large, ~500 MB, word vectors for better accuracy)
English model: en_core_web_sm (small, fallback for English claims)
"""

import logging

logger = logging.getLogger("evidora")

# Lazy-loaded SpaCy models
_nlp_de = None
_nlp_en = None
_load_failed = False

# Stopwords to filter out of extracted keywords (common but meaningless for search)
_DE_STOPWORDS = {
    "der", "die", "das", "ein", "eine", "und", "oder", "aber", "dass",
    "ist", "sind", "hat", "haben", "wird", "werden", "kann", "können",
    "es", "sie", "er", "wir", "man", "ich", "sich", "nicht", "kein",
    "mehr", "viel", "sehr", "alle", "nur", "noch", "schon", "auch",
    "so", "da", "hier", "dort", "wenn", "weil", "ob", "wie", "was",
    "wer", "wo", "wann", "warum", "diesem", "dieser", "diese", "dieses",
    "jeder", "jede", "jedes", "andere", "anderer", "anderen",
    "prozent", "jahr", "jahre", "jahren", "mal", "laut", "neue",
    "etwa", "rund", "seit", "über", "nach", "vor", "zum", "zur",
}

_EN_STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "that", "this", "is", "are",
    "was", "were", "has", "have", "had", "be", "been", "being",
    "it", "they", "he", "she", "we", "you", "not", "no", "all",
    "some", "any", "more", "most", "very", "also", "just", "only",
    "than", "then", "so", "if", "when", "where", "how", "what",
    "who", "which", "about", "percent", "year", "years", "new",
}


def _load_models():
    """Load SpaCy models once (lazy singleton). Falls back to sm if lg not available."""
    global _nlp_de, _nlp_en, _load_failed
    if _load_failed:
        return
    try:
        import spacy
        # Prefer large model for better NER, fall back to small
        try:
            _nlp_de = spacy.load("de_core_news_lg")
            logger.info("SpaCy DE model: de_core_news_lg")
        except OSError:
            _nlp_de = spacy.load("de_core_news_sm")
            logger.info("SpaCy DE model: de_core_news_sm (lg not available)")
        _nlp_en = spacy.load("en_core_web_sm")
        logger.info("SpaCy NER models loaded")
    except Exception as e:
        _load_failed = True
        logger.warning(f"SpaCy models not available, NER enrichment disabled: {e}")


def _detect_language(text: str) -> str:
    """Simple language detection based on common German words."""
    german_indicators = [
        " der ", " die ", " das ", " ist ", " hat ", " und ", " von ",
        " den ", " dem ", " ein ", " eine ", " nicht ", " sind ", " für ",
        " mit ", " auf ", " wird ", " dass ", " sich ",
    ]
    text_lower = f" {text.lower()} "
    german_count = sum(1 for word in german_indicators if word in text_lower)
    return "de" if german_count >= 2 else "en"


def _extract_keywords(doc, lang: str) -> list[str]:
    """Extract meaningful keywords from SpaCy noun chunks.

    Filters out stopwords and very short/generic chunks, keeping only
    substantive phrases that are useful as search terms.
    """
    stopwords = _DE_STOPWORDS if lang == "de" else _EN_STOPWORDS
    keywords = []
    seen = set()

    for chunk in doc.noun_chunks:
        # Clean: keep only nouns, adjectives, proper nouns
        tokens = [t for t in chunk if t.pos_ in ("NOUN", "PROPN", "ADJ") and not t.is_stop]
        if not tokens:
            continue

        # Limit to max 3 tokens per keyword
        tokens = tokens[:3]

        # Build keyword from remaining tokens
        keyword = " ".join(t.text for t in tokens).strip()
        keyword_lower = keyword.lower()

        # Filter criteria
        if len(keyword) < 3:
            continue
        if keyword_lower in stopwords:
            continue
        if keyword_lower in seen:
            continue

        seen.add(keyword_lower)
        keywords.append(keyword)

    return keywords


def enrich_entities(claim_text: str, analysis: dict) -> dict:
    """Enrich analysis with SpaCy NER entities and extracted keywords.

    Adds deterministic entity extraction and keyword extraction on top of
    the LLM's output. Merges into analysis["entities"], adds
    analysis["ner_entities"] with typed entity groups, and supplements
    analysis["factcheck_queries"] with extracted keywords.

    Gracefully returns the original analysis if SpaCy is not available.
    """
    if _nlp_de is None and not _load_failed:
        _load_models()

    if _nlp_de is None or _nlp_en is None:
        return analysis

    lang = _detect_language(claim_text)
    nlp = _nlp_de if lang == "de" else _nlp_en
    doc = nlp(claim_text)

    # --- NER ---
    ner_entities = {
        "countries": [],
        "dates": [],
        "organizations": [],
        "numbers": [],
        "persons": [],
    }

    for ent in doc.ents:
        label = ent.label_
        text = ent.text.strip()
        if not text:
            continue

        if label in ("GPE", "LOC"):
            ner_entities["countries"].append(text)
        elif label in ("DATE",):
            ner_entities["dates"].append(text)
        elif label in ("ORG",):
            ner_entities["organizations"].append(text)
        elif label in ("CARDINAL", "MONEY", "QUANTITY", "PERCENT"):
            ner_entities["numbers"].append(text)
        elif label in ("PER", "PERSON"):
            ner_entities["persons"].append(text)

    # Deduplicate within each group
    for key in ner_entities:
        ner_entities[key] = list(dict.fromkeys(ner_entities[key]))

    # Merge into existing entities (deduplicate)
    existing = set(e.lower() for e in analysis.get("entities", []))
    new_entities = list(analysis.get("entities", []))
    all_ner = (
        ner_entities["countries"]
        + ner_entities["organizations"]
        + ner_entities["dates"]
    )
    for entity in all_ner:
        if entity.lower() not in existing:
            new_entities.append(entity)
            existing.add(entity.lower())

    analysis["entities"] = new_entities
    analysis["ner_entities"] = ner_entities

    # --- Keyword extraction ---
    keywords = _extract_keywords(doc, lang)
    if keywords:
        existing_queries = set(q.lower() for q in analysis.get("factcheck_queries", []))
        new_queries = list(analysis.get("factcheck_queries", []))
        for kw in keywords:
            if kw.lower() not in existing_queries:
                new_queries.append(kw)
                existing_queries.add(kw.lower())
        analysis["factcheck_queries"] = new_queries
        analysis["spacy_keywords"] = keywords

    logger.info(
        f"NER ({lang}): {len(ner_entities['countries'])} countries, "
        f"{len(ner_entities['dates'])} dates, "
        f"{len(ner_entities['organizations'])} orgs, "
        f"{len(keywords)} keywords extracted"
    )

    return analysis
