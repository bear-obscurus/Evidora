"""Eine einzige, geteilte SentenceTransformer-Instanz.

Sowohl der Top-Level-Reranker (``reranker.py``) als auch der Backup-Trigger-/
Verdict-Cache-Pfad (``_reranker_backup.py`` → auch von ``verdict_cache.py``
genutzt) verwenden dasselbe Modell ``paraphrase-multilingual-MiniLM-L12-v2``.

Bisher lud JEDES der beiden Module seine EIGENE Instanz (~250 MB pro Instanz),
das Modell lag also doppelt im RAM. Dieses Modul hält die EINE geteilte
Instanz, damit das Modell genau einmal geladen wird. Lazy + idempotent:
der erste Aufruf lädt, alle weiteren geben dieselbe Instanz zurück.

Gibt ``None`` zurück, wenn sentence-transformers nicht installiert ist oder
der Load fehlschlägt — die Aufrufer degradieren dann sauber (Reranker wird
no-op, Backup-Trigger/Verdict-Cache deaktivieren ihren semantischen Pfad).
"""

import logging

logger = logging.getLogger("evidora")

_MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"

_model = None
_unavailable = False


def get_model():
    """Lazy-load und Rückgabe der geteilten SentenceTransformer-Instanz
    (oder ``None``, wenn nicht verfügbar)."""
    global _model, _unavailable
    if _model is not None:
        return _model
    if _unavailable:
        return None
    try:
        from sentence_transformers import SentenceTransformer

        _model = SentenceTransformer(_MODEL_NAME)
        logger.info(f"Shared SentenceTransformer loaded once ({_MODEL_NAME})")
        return _model
    except ImportError:
        _unavailable = True
        logger.info("sentence-transformers not installed — semantic features disabled")
        return None
    except Exception as e:
        _unavailable = True
        logger.warning(f"Failed to load shared SentenceTransformer: {e}")
        return None
