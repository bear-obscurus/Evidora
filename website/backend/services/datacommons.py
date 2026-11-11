"""DataCommons ClaimReview — lokaler Index historischer Faktenchecks.

Lädt den DataCommons ClaimReview Feed (CC BY 4.0, ~93.000 Einträge, täglich
aktualisiert) und filtert deutschsprachige/europäische Einträge heraus.
Die gefilterten Einträge werden lokal gespeichert und per MiniLM semantisch
durchsuchbar gemacht.

Quelle: https://datacommons.org/factcheck/download
Feed: https://storage.googleapis.com/datacommons-feeds/claimreview/latest/data.json

## Warum der Neuaufbau im Hintergrund laeuft (11.11.2026)

Bis dahin rechnete ``update_index`` die Embeddings aller 11.071 Eintraege
mit ``_model.encode`` SYNCHRON in der Event-Loop — beim Start (``await
prefetch_all()``, uvicorn nahm erst danach Verbindungen an) und taeglich in
der Stundenschleife nach Ablauf der 24 h. Gemessen an 8 von 8 Neuaufbauten:
4,0–8,1 min lang beantwortete der Dienst NICHTS, nicht einmal den
Healthcheck. Der Start bestand zu 93 % aus diesem Embedding.

Jetzt:
1. Die Embeddings liegen in ``data/claimreview_embeddings.npz`` (gemountet,
   gitignored) — je Eintrag ein Hash des Textes, dazu die Modellkennung.
   Ein Neuaufbau rechnet nur neue oder geaenderte Eintraege.
2. Gerechnet wird in einem Thread und stueckweise; die Encode-Sperre in
   ``_st_model`` haelt die Tokenizer der anderen Dienste heraus.
3. ``update_index`` kehrt sofort zurueck: Index und gespeicherte Embeddings
   werden geladen, Download und Abgleich laufen als Hintergrund-Aufgabe.
4. Fehlen die Embeddings, gibt es nur die Schlagwortsuche — nie mehr ein
   Einbetten aller Eintraege pro Anfrage.

Messung und Kriterien: ``Evidora Dokumente/start_prefetch_2026-11-11/``.
Tests: ``tests/test_datacommons_hintergrund.py``.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import tempfile
import time
from pathlib import Path

import httpx
from services._http_polite import polite_client
from services._atomic import atomic_write_json

logger = logging.getLogger("evidora")

FEED_URL = "https://storage.googleapis.com/datacommons-feeds/claimreview/latest/data.json"
DATA_DIR = Path(__file__).parent.parent / "data"
LOCAL_INDEX = DATA_DIR / "claimreview_index.json"
# Bewusst KEIN .json: data_freshness_check und stichtag_check laufen ueber
# alle data/*.json und wuerden eine Hash-Liste als Befund melden.
EMB_SPEICHER = DATA_DIR / "claimreview_embeddings.npz"
# So viele Texte je encode-Aufruf. Zwischen zwei Stuecken ist die
# Encode-Sperre frei; eine Anfrage wartet hoechstens ein Stueck (~1 s).
EMB_STUECK = 32

# In-memory cache. Index und Embeddings werden immer GEMEINSAM getauscht
# (ohne await dazwischen), damit Zeile i des Tensors zu Eintrag i gehoert.
_index: list[dict] | None = None
_embeddings = None
_index_ts: float = 0
INDEX_TTL = 86400  # 24 hours
_neuaufbau_task: asyncio.Task | None = None

# German-language fact-checkers and European orgs to keep
DE_EU_CHECKERS = {
    "correctiv", "dpa", "mimikama", "apa.at", "faktencheck", "faktenfinder",
    "br.de", "tagesschau", "spiegel", "volksverpetzer", "futurezone",
    "derstandard", "kurier.at", "orf.at", "dw.com", "zdf.de", "ndr.de",
    "mdr.de", "swr.de", "wdr.de", "sueddeutsche", "faz.net",
    "afp.com", "reuters.com", "france24", "liberation", "lemonde",
    "elpais", "repubblica", "nieuwscheckers", "logically", "fullfact",
    "polygraph", "eufactcheck", "euractiv",
}

# Languages to keep
KEEP_LANGUAGES = {"de", "german", "deutsch"}

# Characters that indicate German text
_DE_CHARS = re.compile(r'[äöüßÄÖÜ]')
_DE_WORDS = re.compile(r'\b(und|der|die|das|ist|nicht|ein|eine|dass|wird|hat|sind|für|mit|auch|nach|wie|nur|noch|aber)\b', re.I)


def _is_german_text(text: str) -> bool:
    """Heuristic check if text is German."""
    if not text or len(text) < 10:
        return False
    if _DE_CHARS.search(text):
        return True
    # Check for German words (need at least 2 matches in short text)
    matches = _DE_WORDS.findall(text)
    return len(matches) >= 2


def _is_relevant_entry(entry: dict) -> bool:
    """Check if a ClaimReview entry is from a German/European source."""
    # Check author URL/name
    author = entry.get("author", {})
    author_url = (author.get("url") or "").lower()
    author_name = (author.get("name") or "").lower()

    for checker in DE_EU_CHECKERS:
        if checker in author_url or checker in author_name:
            return True

    # Check language field
    lang = (entry.get("inLanguage") or "").lower()
    if lang in KEEP_LANGUAGES:
        return True

    # Check if claim text is German
    claim = entry.get("claimReviewed", "")
    if _is_german_text(claim):
        return True

    # Check rating text for German
    rating = entry.get("reviewRating", {})
    rating_text = rating.get("alternateName", "")
    if _is_german_text(rating_text):
        return True

    return False


def _extract_entry(raw: dict) -> dict | None:
    """Extract relevant fields from a raw ClaimReview JSON-LD object."""
    claim = raw.get("claimReviewed", "").strip()
    if not claim or len(claim) < 10:
        return None

    author = raw.get("author", {})
    rating = raw.get("reviewRating", {})
    item_reviewed = raw.get("itemReviewed", {})

    # Get the review URL (fact-check article)
    url = raw.get("url", "")

    # Get rating text
    rating_text = rating.get("alternateName", "") or rating.get("bestRating", "")

    return {
        "claim": claim[:500],
        "url": url,
        "source": author.get("name", ""),
        "source_url": author.get("url", ""),
        "rating": rating_text[:200],
        "date": raw.get("datePublished", ""),
    }


async def _download_and_filter() -> list[dict]:
    """Stream-download the DataCommons feed, filter for DE/EU entries."""
    try:
        async with polite_client(timeout=120.0) as client:
            # Stream the large JSON file
            async with client.stream("GET", FEED_URL) as resp:
                resp.raise_for_status()

                chunks = []
                async for chunk in resp.aiter_bytes(chunk_size=1_000_000):
                    chunks.append(chunk)

        # Dekodieren, Parsen und Filtern im Thread: das sind Sekunden reiner
        # CPU-Arbeit an 200 MB, die vorher in der Event-Loop liefen.
        return await asyncio.to_thread(_parse_and_filter, chunks)
    except Exception as e:
        logger.error(f"DataCommons download failed: {e}")
        return []


def _parse_and_filter(chunks: list[bytes]) -> list[dict]:
    """Feed-Bytes -> gefilterte DE/EU-Eintraege (laeuft im Thread)."""
    entries = []

    try:
        raw = b"".join(chunks).decode("utf-8", errors="replace")
        chunks.clear()

        logger.info(f"DataCommons: downloaded {len(raw) // 1_000_000} MB")

        # Parse the DataFeed JSON — handle encoding errors gracefully
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            # Fix common issues: invalid unicode escapes
            raw_fixed = re.sub(r'\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u)', '', raw)
            data = json.loads(raw_fixed)

        elements = data.get("dataFeedElement", [])

        for element in elements:
            items = element.get("item") or []
            if isinstance(items, dict):
                items = [items]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if item.get("@type") != "ClaimReview":
                    continue
                if not _is_relevant_entry(item):
                    continue
                entry = _extract_entry(item)
                if entry:
                    entries.append(entry)

        # Deduplicate by claim text
        seen = set()
        unique = []
        for e in entries:
            key = e["claim"].lower()[:100]
            if key not in seen:
                seen.add(key)
                unique.append(e)
        entries = unique

        logger.info(f"DataCommons: {len(entries)} DE/EU entries extracted from {len(elements)} feed elements")

    except Exception as e:
        logger.error(f"DataCommons parse failed: {e}")

    return entries


# ---------------------------------------------------------------------------
# Embeddings: Zwischenspeicher + stueckweises Rechnen im Thread
# ---------------------------------------------------------------------------

def _modell_kennung() -> str:
    """Name@Revision des geteilten Modells — ein Wechsel macht den Speicher ungueltig."""
    from services import _st_model
    return f"{_st_model._MODEL_NAME}@{_st_model._MODEL_REVISION}"


def _texte(entries: list[dict]) -> list[str]:
    return [f"{e['claim']} {e['rating']}" for e in entries]


def _text_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def _lade_speicher(kennung: str) -> dict:
    """Hash -> Vektor aus ``EMB_SPEICHER``; leer, wenn fehlend, kaputt oder anderes Modell."""
    import numpy as np
    if not EMB_SPEICHER.exists():
        return {}
    try:
        with np.load(EMB_SPEICHER, allow_pickle=False) as z:
            if str(z["modell"]) != kennung:
                logger.info("DataCommons: Embedding-Speicher gehoert zu einem anderen Modell — wird neu gerechnet")
                return {}
            matrix, hashes = z["matrix"], z["hashes"]
            if matrix.ndim != 2 or len(hashes) != matrix.shape[0]:
                logger.warning("DataCommons: Embedding-Speicher inkonsistent — wird neu gerechnet")
                return {}
            return {str(h): matrix[i] for i, h in enumerate(hashes)}
    except Exception as e:
        logger.warning(f"DataCommons: Embedding-Speicher unlesbar ({e}) — wird neu gerechnet")
        return {}


def _schreibe_speicher(kennung: str, hashes: list[str], matrix) -> None:
    """Atomar: erst eine Temp-Datei im selben Verzeichnis, dann os.replace."""
    import numpy as np
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=DATA_DIR, prefix=".claimreview_embeddings.", suffix=".npz.tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            np.savez(f, matrix=matrix, hashes=np.array(hashes, dtype="<U16"), modell=np.array(kennung))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, EMB_SPEICHER)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _rechne_stueckweise(model, texte: list[str]):
    """Laeuft im Thread. Stueckweise, damit die Encode-Sperre zwischendurch frei ist."""
    import numpy as np
    teile = []
    for i in range(0, len(texte), EMB_STUECK):
        teil = model.encode(texte[i:i + EMB_STUECK], batch_size=EMB_STUECK,
                            convert_to_numpy=True, show_progress_bar=False)
        teile.append(np.asarray(teil, dtype=np.float32))
    return np.vstack(teile)


async def _embeddings_fuer(entries: list[dict], model):
    """Matrix (float32, Zeile i = Eintrag i). Rechnet nur, was nicht im Speicher liegt."""
    import numpy as np
    kennung = _modell_kennung()
    texte = _texte(entries)
    hashes = [_text_hash(t) for t in texte]
    speicher = await asyncio.to_thread(_lade_speicher, kennung)
    fehlend = sorted({i for i, h in enumerate(hashes) if h not in speicher})
    neu = {}
    if fehlend:
        vektoren = await asyncio.to_thread(_rechne_stueckweise, model, [texte[i] for i in fehlend])
        neu = {hashes[i]: vektoren[k] for k, i in enumerate(fehlend)}
    matrix = np.vstack([speicher[h] if h in speicher else neu[h] for h in hashes]).astype(np.float32)
    if fehlend or set(speicher) != set(hashes):
        try:
            await asyncio.to_thread(_schreibe_speicher, kennung, hashes, matrix)
        except Exception as e:
            logger.warning(f"DataCommons: Embedding-Speicher nicht geschrieben: {e}")
    logger.info(
        f"DataCommons: embeddings ready for {len(entries)} entries "
        f"({len(fehlend)} neu gerechnet, {len(entries) - len(fehlend)} aus dem Speicher)"
    )
    return matrix


def _als_tensor(matrix):
    """torch-Tensor ohne Kopie, damit cos_sim pro Anfrage nichts umwandelt."""
    try:
        import torch
        return torch.from_numpy(matrix)
    except ImportError:
        return matrix


def _geteiltes_modell():
    from services import reranker
    if not reranker._load_model():
        return None
    return reranker._model


# ---------------------------------------------------------------------------
# Index: sofort laden, im Hintergrund erneuern
# ---------------------------------------------------------------------------

def _load_local_index():
    """Lokalen Index laden und — wenn der Speicher ihn vollstaendig abdeckt —
    die Embeddings dazu. Schnell (keine Modell-Rechnung); fuer Start und Anfrage."""
    global _index, _index_ts, _embeddings
    if _index is not None:
        return
    if not LOCAL_INDEX.exists():
        return
    try:
        with open(LOCAL_INDEX) as f:
            entries = json.load(f)
        ts = LOCAL_INDEX.stat().st_mtime
    except Exception as e:
        logger.warning(f"DataCommons: failed to load local index: {e}")
        return
    emb = None
    try:
        speicher = _lade_speicher(_modell_kennung())
        hashes = [_text_hash(t) for t in _texte(entries)]
        if speicher and all(h in speicher for h in hashes):
            import numpy as np
            emb = _als_tensor(np.vstack([speicher[h] for h in hashes]).astype(np.float32))
    except Exception as e:
        logger.warning(f"DataCommons: Embeddings aus dem Speicher nicht geladen: {e}")
    # gemeinsam tauschen
    _index, _embeddings, _index_ts = entries, emb, ts
    logger.info(
        f"DataCommons: loaded {len(entries)} entries from local cache "
        f"(embeddings {'aus dem Speicher' if emb is not None else 'fehlen noch'})"
    )


async def _neuaufbau(download: bool) -> None:
    """Hintergrund-Aufgabe: ggf. Feed holen, Embeddings abgleichen, gemeinsam tauschen."""
    global _index, _embeddings, _index_ts
    try:
        entries = None
        if download:
            entries = await _download_and_filter()
            if entries:
                try:
                    await asyncio.to_thread(atomic_write_json, LOCAL_INDEX, entries, ensure_ascii=False)
                    logger.info(f"DataCommons: saved {len(entries)} entries to {LOCAL_INDEX}")
                except Exception as e:
                    logger.warning(f"DataCommons: failed to save local index: {e}")
        if not entries:
            entries = _index
        if not entries:
            logger.warning("DataCommons: no entries available")
            return
        emb = None
        model = _geteiltes_modell()
        if model is not None:
            emb = _als_tensor(await _embeddings_fuer(entries, model))
        # Index und Embeddings gemeinsam tauschen — kein await dazwischen.
        # Ein reiner Embedding-Abgleich (download=False) laesst die Frist
        # stehen; nach einem Download-Versuch beginnt sie neu (wie bisher:
        # auch ein gescheiterter Download wird erst nach 24 h wiederholt).
        neue_ts = time.time() if download else _index_ts
        _index, _embeddings, _index_ts = entries, emb, neue_ts
    except Exception as e:
        logger.warning(f"DataCommons rebuild failed: {e}")


async def update_index():
    """Start und Stundenschleife: Bestand laden, Neuaufbau ggf. im Hintergrund starten.

    Kehrt SOFORT zurueck. Vorher wartete der Start ~7 min auf das Embedding,
    und taeglich stand der Dienst dafuer still (siehe Modul-Docstring).
    """
    global _neuaufbau_task
    _load_local_index()
    if _neuaufbau_task is not None and not _neuaufbau_task.done():
        return
    frisch = _index is not None and time.time() - _index_ts < INDEX_TTL
    if frisch and _embeddings is not None:
        return
    _neuaufbau_task = asyncio.create_task(_neuaufbau(download=not frisch))


def _has_entity_overlap(entry: dict, entities: list[str]) -> bool:
    """Check if any entity appears in the entry's claim or rating text."""
    if not entities:
        return True  # No entities to check — accept all
    text = f"{entry['claim']} {entry['rating']}".lower()
    return any(e.lower() in text for e in entities if len(e) >= 3)


def _semantic_search(claim: str, entries: list[dict], entities: list[str] | None = None, top_k: int = 3) -> list[dict]:
    """Search entries by semantic similarity with entity overlap requirement.

    Requires that results both score above the similarity threshold AND
    contain at least one entity from the claim analysis.  This prevents
    generic words (e.g. "gefährlich") from pulling in unrelated fact-checks.
    """
    # Nur mit FERTIGEN Embeddings zum selben Index. Vorher wurden sonst ALLE
    # Eintraege pro Anfrage neu eingebettet (11.071 Texte, Minuten) — mit dem
    # Neuaufbau im Hintergrund waere genau dieser Zustand nach jedem Start
    # eingetreten. Ohne Embeddings: None -> Schlagwortsuche.
    idx, emb = _index, _embeddings
    if emb is None or idx is None or entries is not idx or len(emb) != len(idx):
        return None
    try:
        model = _geteiltes_modell()
        if model is None:
            return None

        from sentence_transformers import util

        claim_embedding = model.encode(claim, convert_to_tensor=True)
        entry_embeddings = emb

        scores = util.cos_sim(claim_embedding, entry_embeddings)[0]
        scored = sorted(zip(entries, scores.tolist()), key=lambda x: x[1], reverse=True)

        # Require both semantic similarity AND entity overlap.
        # 2026-05-24: Threshold von 0.35 → 0.55 (= FACTCHECK_THRESHOLD aus
        # services/reranker.py). Vorher kamen themenfremde Treffer durch,
        # die nur ein Entity teilten ('Migration' / 'Österreich') — z.B.
        # 'Wien Hochburg der Kriminalität' bei Asyl-Claim oder 'BR24
        # Wahlarena-Faktencheck' bei Migrations-Claim (User-Report
        # 2026-05-24). Top-K von 5 auf 3 reduziert, damit nur die
        # relevantesten Treffer durchkommen.
        kept = [
            e for e, score in scored[:top_k * 3]
            if score > 0.55 and _has_entity_overlap(e, entities or [])
        ][:top_k]

        if scored:
            top_score = scored[0][1]
            logger.info(
                f"DataCommons semantic: top score {top_score:.3f}, "
                f"kept {len(kept)}/{min(top_k * 3, len(scored))} "
                f"(entities: {entities})"
            )
        return kept

    except Exception as e:
        logger.debug(f"DataCommons semantic search failed: {e}")
        return None


def _keyword_search(entries: list[dict], keywords: list[str]) -> list[dict]:
    """Fallback keyword search."""
    matched = []
    for entry in entries:
        text = f"{entry['claim']} {entry['rating']}".lower()
        if any(kw.lower() in text for kw in keywords):
            matched.append(entry)
    return matched[:5]


async def search_datacommons(analysis: dict) -> dict:
    """Search the local DataCommons ClaimReview index."""
    global _index, _index_ts

    claim = analysis.get("claim", "")
    entities = analysis.get("entities", [])
    factcheck_queries = analysis.get("factcheck_queries", [])
    keywords = entities + factcheck_queries

    if not claim and not keywords:
        return {"source": "DataCommons ClaimReview", "type": "factcheck", "results": []}

    # Ensure index is loaded
    if _index is None:
        _load_local_index()

    if not _index:
        return {"source": "DataCommons ClaimReview", "type": "factcheck", "results": []}

    # Semantic search first, keyword fallback
    matched = None
    if claim:
        matched = _semantic_search(claim, _index, entities=entities)

    if matched is None:
        matched = _keyword_search(_index, keywords)

    results = []
    for entry in matched[:5]:
        title = entry["claim"][:150]
        if entry["rating"]:
            title = f"{entry['source']}: {title}"

        results.append({
            "title": title,
            "url": entry["url"],
            "source": entry["source"] or "DataCommons",
            "rating": entry["rating"],
            "date": entry["date"],
        })

    return {
        "source": "DataCommons ClaimReview",
        "type": "factcheck",
        "results": results,
    }
