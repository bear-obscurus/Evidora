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
# Revision MUSS mit dem Docker-Model-Bake übereinstimmen (Dockerfile +
# requirements-Pin). Ohne den Pin lud get_model() den 'main'-Ref — im
# Offline-Image (HF_HUB_OFFLINE=1) ist aber NUR dieser Snapshot materialisiert.
# Folge (Audit 2026-07-08): Bake lud revision=e8f8…, Runtime lud 'main' →
# „couldn't find them in the cached files" → „semantic re-ranking disabled",
# obwohl das Modell im Image lag. Beide Pfade jetzt auf dieselbe Revision.
_MODEL_REVISION = "e8f8c211226b894fcb81acc59f3b34ba3efd5f42"

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

        _model = SentenceTransformer(_MODEL_NAME, revision=_MODEL_REVISION)
        logger.info(
            f"Shared SentenceTransformer loaded once "
            f"({_MODEL_NAME}@{_MODEL_REVISION[:8]})"
        )
        return _model
    except ImportError:
        _unavailable = True
        logger.info("sentence-transformers not installed — semantic features disabled")
        return None
    except Exception as e:
        _unavailable = True
        logger.warning(f"Failed to load shared SentenceTransformer: {e}")
        return None
