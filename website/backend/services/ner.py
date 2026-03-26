"""SpaCy NER enrichment for claim analysis.

Runs deterministic Named Entity Recognition on the claim text and merges
detected entities (countries, dates, organizations, numbers) into the
Mistral-produced analysis dict.
"""

import logging

logger = logging.getLogger("evidora")

# Lazy-loaded SpaCy models
_nlp_de = None
_nlp_en = None
_load_failed = False


def _load_models():
    """Load SpaCy models once (lazy singleton)."""
    global _nlp_de, _nlp_en, _load_failed
    if _load_failed:
        return
    try:
        import spacy
        _nlp_de = spacy.load("de_core_news_sm")
        _nlp_en = spacy.load("en_core_web_sm")
        logger.info("SpaCy NER models loaded (de + en)")
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


def enrich_entities(claim_text: str, analysis: dict) -> dict:
    """Enrich analysis with SpaCy NER entities.

    Adds deterministic entity extraction on top of the LLM's output.
    Merges into analysis["entities"] and adds analysis["ner_entities"]
    with typed entity groups.

    Gracefully returns the original analysis if SpaCy is not available.
    """
    if _nlp_de is None and not _load_failed:
        _load_models()

    if _nlp_de is None or _nlp_en is None:
        return analysis

    lang = _detect_language(claim_text)
    nlp = _nlp_de if lang == "de" else _nlp_en
    doc = nlp(claim_text)

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

    logger.info(
        f"NER ({lang}): {len(ner_entities['countries'])} countries, "
        f"{len(ner_entities['dates'])} dates, "
        f"{len(ner_entities['organizations'])} orgs"
    )

    return analysis
