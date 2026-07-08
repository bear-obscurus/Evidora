"""Gemeinsamer Helper für internationale Fact-Check-RSS-Connectoren.

Die folgenden Services delegieren an diesen Helper:
- services/snopes.py (US, EN — größte EN-Faktencheck-DB)
- services/correctiv.py (DE — investigative + Faktenchecks)
- services/full_fact.py (UK, EN)
- services/bellingcat.py (UK/global, EN — OSINT)
- services/factcheck_org.py (US, EN — Annenberg)

Pattern stammt aus services/cdc_newsroom.py und services/mimikama.py.

Ohne API-Key, alle nutzen polite_client(timeout=15s) mit korrektem
User-Agent. XML-Parsing via stdlib xml.etree (kein feedparser nötig).
"""

import logging
import re
from datetime import datetime
from xml.etree import ElementTree as ET

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", _HTML_TAG_RE.sub(" ", text)).strip()


def _parse_rss_year(pub_date: str) -> str:
    """RFC-822 pubDate → year string. Returns '—' on failure."""
    if not pub_date:
        return "—"
    try:
        # RSS pubDate: "Mon, 04 May 2026 02:00:00 +0000"
        dt = datetime.strptime(pub_date[:25].strip(), "%a, %d %b %Y %H:%M:%S")
        return str(dt.year)
    except Exception:
        return "—"


def _word_hit(term: str, text_lc: str) -> bool:
    """Wortgrenzen-Match statt Substring (Audit 2026-07-08). Verhindert
    Fehltreffer wie 'Kinder' ⊂ 'Kinderhandel' / 'Kindern' — genau die, die
    im Prod-Keyword-Fallback themenfremde Feed-Items durchließen. ``term``
    bereits lowercase; Mehrwort-Terme werden als ganze Sequenz gematcht."""
    return re.search(r"\b" + re.escape(term) + r"\b", text_lc) is not None


def _entity_or_query_match(text: str, entities: list[str], queries: list[str]) -> bool:
    """Strenger Match (Pattern aus europe_pmc.py), jetzt WORTGRENZEN-basiert:

    EITHER:
      (a) mindestens eine Entity (>=3 chars, multi-word möglich) kommt als
          GANZES WORT im Text vor — single hit reicht, weil NER-Entities
          spezifisch sind ('Mozart', 'Salmonella', 'Vasektomie')
      OR:
      (b) mindestens 2 verschiedene Query-Wörter (>=4 chars) als ganze Wörter
          — verhindert Single-Word-False-Positives bei generischen Begriffen

    Falls beide Listen leer → False.
    """
    text_lc = text.lower()

    # (a) Entity-Match — single hit reicht, Wortgrenze statt Substring
    entity_terms = [e.lower() for e in entities if len(e) >= 3]
    if any(_word_hit(e, text_lc) for e in entity_terms):
        return True

    # (b) Query-Word-Match — mindestens 2 verschiedene Wörter (Wortgrenze)
    query_words: set[str] = set()
    for q in queries[:4]:
        for w in q.split():
            if len(w) >= 4:
                query_words.add(w.lower())
    if not query_words:
        return False
    hits = sum(1 for w in query_words if _word_hit(w, text_lc))
    return hits >= 2


# ---------------------------------------------------------------------------
# Claim-Relevanz-Filter für Live-Feed-Items (Audit 2026-07-07)
# ---------------------------------------------------------------------------
# Der nachgelagerte Reranker (FACTCHECK_THRESHOLD) filtert NUR den
# Synthesizer-Prompt. SSE-Stream, PDF-Export und die "X von Y Quellen
# lieferten Ergebnisse"-Zählung sehen die rohen Service-Results. Feed-
# Services, die ihren ganzen Feed zurückgeben (Mimikama, AT-Faktencheck-RSS),
# kippten dadurch 20 themenfremde Items in jeden Export. Dieser Filter läuft
# deshalb IN den Services, bevor Ergebnisse den Stream erreichen.

# Embedding-Cache pro Item-URL — Feeds rotieren langsam (1h-TTL), das
# Encoding von ~40 Kurztexten pro Request wäre sonst doppelte Arbeit
# zusätzlich zum Reranker. Bounded, FIFO-Eviction.
_ITEM_EMB_CACHE: dict[str, object] = {}
_ITEM_EMB_CACHE_MAX = 512

_FALLBACK_FACTCHECK_THRESHOLD = 0.55


def _item_text(item: dict) -> str:
    return f"{item.get('title', '')} {item.get('description', '')}".strip()


def _has_entity_overlap(item: dict, entities: list[str]) -> bool:
    """GADMO-Pattern: mind. eine NER-Entity muss im Item-Text vorkommen.
    Ohne Entities keine Anforderung (dann trägt der Cosine-Threshold allein).
    Wortgrenzen-Match (Audit 2026-07-08) statt Substring."""
    if not entities:
        return True
    text_lc = _item_text(item).lower()
    return any(_word_hit(e.lower(), text_lc) for e in entities if len(e) >= 3)


# Einmalige Warnung, wenn der semantische Filter-Pfad im Prod ausfällt —
# der Keyword-Fallback ist gröber; ein stiller Ausfall (Audit 2026-07-08:
# toter Reranker durch Xet-Bake) soll künftig im Log sichtbar sein.
_semantic_unavailable_warned = False


def _warn_semantic_unavailable(reason: str) -> None:
    global _semantic_unavailable_warned
    if not _semantic_unavailable_warned:
        logger.warning(
            f"feed-claim-filter: semantischer Pfad NICHT verfügbar "
            f"({reason}) — Keyword-Fallback aktiv (gröber)."
        )
        _semantic_unavailable_warned = True


def filter_items_for_claim(
    claim: str,
    items: list[dict],
    *,
    entities: list[str],
    queries: list[str],
    top_k: int = 5,
) -> list[dict]:
    """Filtert Live-Feed-Items auf Claim-Relevanz VOR Stream/Export/Zählung.

    Semantischer Pfad: geteilte MiniLM-Instanz, Cosine >= FACTCHECK_THRESHOLD
    des Rerankers + Entity-Overlap (identische Messlatte wie die Prompt-
    Filterung — was hier durchkommt, käme auch dort durch). Ist das Modell
    nicht verfügbar (z. B. CI), greift der strenge Keyword-Fallback
    ``_entity_or_query_match`` — niemals Durchwinken des ganzen Feeds.
    """
    if not items or not claim:
        return []

    try:
        from services import _st_model

        model = _st_model.get_model()
        if model is None:
            _warn_semantic_unavailable("SentenceTransformer nicht geladen")
        if model is not None:
            from sentence_transformers import util

            from services._reranker_backup import _encode_claim_cached

            try:
                from services.reranker import FACTCHECK_THRESHOLD as _thr
            except Exception:
                _thr = _FALLBACK_FACTCHECK_THRESHOLD

            claim_emb = _encode_claim_cached(model, claim)

            # nur uncachte Items encodieren
            to_encode = [
                it for it in items
                if (it.get("url") or _item_text(it)) not in _ITEM_EMB_CACHE
            ]
            if to_encode:
                embs = model.encode(
                    [_item_text(it) for it in to_encode], convert_to_tensor=True
                )
                for it, emb in zip(to_encode, embs):
                    key = it.get("url") or _item_text(it)
                    _ITEM_EMB_CACHE[key] = emb
                    if len(_ITEM_EMB_CACHE) > _ITEM_EMB_CACHE_MAX:
                        _ITEM_EMB_CACHE.pop(next(iter(_ITEM_EMB_CACHE)))

            scored = []
            for it in items:
                emb = _ITEM_EMB_CACHE.get(it.get("url") or _item_text(it))
                if emb is None:
                    continue
                score = float(util.cos_sim(claim_emb, emb)[0][0])
                scored.append((it, score))
            scored.sort(key=lambda x: x[1], reverse=True)

            kept = [
                it for it, score in scored
                if score >= _thr and _has_entity_overlap(it, entities)
            ][:top_k]
            if scored:
                logger.info(
                    f"feed-claim-filter: top {scored[0][1]:.3f}, "
                    f"kept {len(kept)}/{len(items)}"
                )
            return kept
    except Exception as e:
        _warn_semantic_unavailable(f"Exception: {e}")

    # Keyword-Fallback (CI / Modell nicht verfügbar): strenger Match
    # statt Feed-Dump — Entity-Treffer (Wortgrenze) ODER >=2 Query-Wörter.
    kept = [
        it for it in items
        if _entity_or_query_match(_item_text(it).lower(), entities, queries)
    ]
    return kept[:top_k]


async def search_factcheck_rss(
    *,
    feed_url: str,
    name: str,
    source_label: str,
    indicator: str,
    country: str,
    analysis: dict,
    max_results: int = 5,
) -> dict:
    """Generic factcheck-RSS search.

    Args:
        feed_url: RSS-Feed-URL (https://...)
        name: kurzer Service-Name (z.B. "Snopes", "Correctiv")
        source_label: vollständiger Quellen-Label für UI/Synthesizer
        indicator: reranker-Indicator-Name (z.B. "snopes_factcheck_item")
        country: ISO-/Sigel-Country-Tag (z.B. "USA", "DE", "UK")
        analysis: Standard analysis dict
        max_results: Top-N Treffer

    Returns:
        Standard {source, type, results} dict.
    """
    empty = {"source": name, "type": "factcheck", "results": []}

    entities = (analysis or {}).get("entities", []) or []
    queries: list[str] = []
    queries.extend((analysis or {}).get("pubmed_queries", []) or [])
    queries.extend((analysis or {}).get("factcheck_queries", []) or [])

    if not entities and not queries:
        return empty

    try:
        async with polite_client(timeout=15.0) as client:
            resp = await client.get(feed_url, follow_redirects=True)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
    except Exception as e:
        logger.warning(f"{name} RSS fetch failed: {e}")
        return empty

    items: list[dict] = []
    for item in root.findall(".//item"):
        title = _strip_html((item.findtext("title") or "").strip())
        desc = _strip_html((item.findtext("description") or "").strip())
        link = (item.findtext("link") or "").strip()
        pub_date = (item.findtext("pubDate") or "").strip()
        if not title:
            continue
        items.append({
            "title": title,
            "description": desc,
            "link": link,
            "pubDate": pub_date,
        })

    if not items:
        logger.info(f"{name}: 0 RSS items parsed")
        return empty

    matched: list[dict] = []
    for it in items:
        text = f"{it['title']} {it['description']}".lower()
        if _entity_or_query_match(text, entities, queries):
            matched.append(it)

    if not matched:
        logger.info(
            f"{name}: 0/{len(items)} matched on entities/queries"
        )
        return empty

    matched = matched[:max_results]
    logger.info(f"{name}: {len(matched)}/{len(items)} items matched")

    results: list[dict] = []
    for it in matched:
        year = _parse_rss_year(it.get("pubDate", ""))
        desc = it.get("description") or ""
        desc_short = (desc[:400] + "…") if len(desc) > 400 else desc

        results.append({
            "indicator_name": it["title"],
            "indicator": indicator,
            "country": country,
            "year": year,
            "topic": f"{name.lower()}_factcheck_item",
            "display_value": desc_short,
            "description": it.get("pubDate", ""),
            "url": it["link"],
            "secondary_url": "",
            "source": source_label,
        })

    return {
        "source": name,
        "type": "factcheck",
        "results": results,
    }
