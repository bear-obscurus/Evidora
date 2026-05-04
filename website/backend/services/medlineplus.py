"""NIH MedlinePlus Web Service — National Library of Medicine
Patientenaufklärung Health Topics.

Free REST API ohne API-Key. Komplementär zu PubMed/Europe PMC:
- Patienten-Edukation (nicht Forschungs-Literatur)
- Multi-Krankheits-Themen-Datenbank
- Englische Quellen, hohe Vertrauenswürdigkeit (NLM ist NIH-Bibliothek)

API-Doku: https://www.nlm.nih.gov/medlineplus/web_service.html

Usage:
- Trigger: gleicher Pfad wie PubMed (analysis.pubmed_queries non-empty)
- Query-Sprache: Englisch (MedlinePlus DB ist englisch)
- Response: XML (parse via xml.etree, ohne externe Deps)

Wiring: main.py imports + tasks.append, reranker (NICHT in
AUTHORITATIVE_INDICATORS — ist Live-Quelle, nicht kuratierte Pack).
"""

import asyncio
import logging
import re
from xml.etree import ElementTree as ET

import httpx

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

BASE_URL = "https://wsearch.nlm.nih.gov/ws/query"

# Strip MedlinePlus highlight tags <span class="qt0">term</span> from text
_HIGHLIGHT_TAG_RE = re.compile(r'<span class="qt[0-9]+">|</span>')
# Strip generic HTML tags from snippets
_HTML_TAG_RE = re.compile(r'<[^>]+>')


def _strip_html(text: str) -> str:
    if not text:
        return ""
    text = _HIGHLIGHT_TAG_RE.sub("", text)
    text = _HTML_TAG_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


async def _medlineplus_single_query(
    client: httpx.AsyncClient, query: str, db: str = "healthTopics", retmax: int = 8
) -> list[dict]:
    """Run a single MedlinePlus Web Service query."""
    params = {
        "db": db,
        "term": query,
        "retmax": str(retmax),
        "rettype": "brief",
    }
    try:
        resp = await client.get(BASE_URL, params=params)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        documents = []
        for doc in root.findall(".//document"):
            url = doc.attrib.get("url", "")
            entry: dict[str, str] = {"url": url}
            for c in doc.findall("content"):
                name = c.attrib.get("name", "")
                if name in {"title", "snippet", "altTitle", "groupName", "FullSummary"}:
                    if name == "groupName":
                        # collect multi-valued
                        existing = entry.get("groupName", "")
                        val = (c.text or "").strip()
                        entry["groupName"] = (
                            f"{existing}; {val}" if existing else val
                        )
                    else:
                        # take first instance for non-multi keys
                        if name not in entry:
                            entry[name] = (c.text or "").strip()
            documents.append(entry)
        return documents
    except Exception as e:
        logger.warning(f"MedlinePlus query failed for '{query}': {e}")
        return []


async def search_medlineplus(analysis: dict) -> dict:
    """Search MedlinePlus health topics for the given analysis.

    Triggered same path as PubMed — only when analysis has English
    pubmed_queries (i.e. claim is health-related).
    """
    queries = analysis.get("pubmed_queries", []) or []
    entities = analysis.get("entities", []) or []
    if not queries:
        return {"source": "NIH MedlinePlus", "type": "health_topics", "results": []}

    # Run up to 3 queries in parallel
    async with polite_client(timeout=15.0) as client:
        tasks = [_medlineplus_single_query(client, q) for q in queries[:3]]
        all_results = await asyncio.gather(*tasks)

    # Flatten + dedupe by URL
    seen: set[str] = set()
    documents: list[dict] = []
    for batch in all_results:
        for doc in batch:
            url = doc.get("url", "")
            if url and url not in seen:
                seen.add(url)
                documents.append(doc)

    if not documents:
        logger.info(f"MedlinePlus: 0 results from {len(queries[:3])} queries")
        return {"source": "NIH MedlinePlus", "type": "health_topics", "results": []}

    logger.info(
        f"MedlinePlus: {len(documents)} unique health topics "
        f"from {len(queries[:3])} queries"
    )

    results: list[dict] = []
    for doc in documents:
        title = _strip_html(doc.get("title", ""))
        snippet = _strip_html(doc.get("snippet", ""))
        alt_title = _strip_html(doc.get("altTitle", ""))
        group_name = _strip_html(doc.get("groupName", ""))
        url = doc.get("url", "")

        # Lightweight entity overlap filter — bei Entitäten Vergleich auf
        # title/altTitle (snippet ist oft generisch). Wenn keine Entities
        # angegeben sind, alles durchlassen.
        if entities:
            haystack = f"{title} {alt_title}".lower()
            if not any(
                e.lower() in haystack for e in entities if len(e) >= 3
            ):
                # noch zweite Chance via query-words
                qwords = set()
                for q in queries[:3]:
                    qwords.update(w.lower() for w in q.split() if len(w) >= 4)
                if not any(w in haystack for w in qwords):
                    continue

        # Keep snippet under 500 chars for synthesizer
        snippet_short = (snippet[:500] + "…") if len(snippet) > 500 else snippet
        display = title
        if alt_title and alt_title.lower() != title.lower():
            display += f" ({alt_title})"

        results.append({
            "indicator_name": display,
            "indicator": "medlineplus_health_topic",
            "country": "USA",
            "year": "—",
            "topic": title.lower().replace(" ", "_")[:50],
            "display_value": snippet_short,
            "description": group_name or "",
            "url": url,
            "secondary_url": "",
            "source": "NIH MedlinePlus (National Library of Medicine, USA)",
        })

    # Limit to top 5 hits
    results = results[:5]

    return {
        "source": "NIH MedlinePlus",
        "type": "health_topics",
        "results": results,
    }
