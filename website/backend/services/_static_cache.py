"""Hot-Reload-fähiges JSON-Caching für Static-First-Services.

Pattern: Bei jedem Aufruf wird die mtime der Datei geprüft. Wenn sie
neuer ist als die zuletzt gecachte mtime, wird die Datei frisch
gelesen und der Cache aktualisiert. Wenn sie unverändert ist, wird
der Cache zurückgegeben.

Erspart bei Edits an data/*.json einen Backend-Restart.

Verwendung in einem Service:

    from services._static_cache import load_json_mtime_aware
    STATIC_JSON_PATH = ...
    def _load_static_json():
        return load_json_mtime_aware(STATIC_JSON_PATH)

Der Helper ist thread- bzw. async-sicher genug für unseren Use-Case
(GIL + reine CPU-Operations beim Laden; bei Concurrent-Reads kann
ggf. einmal doppelt geladen werden, was harmlos ist).
"""

import json
import logging
import os

logger = logging.getLogger("evidora")

# (mtime, data, last_log_at) — last_log_at unterdrückt gleiche
# Reload-Logs in schneller Folge.
_caches: dict[str, tuple[float, dict, float]] = {}


def load_json_mtime_aware(path: str) -> dict | None:
    """Load JSON from disk, hot-reloading on mtime change.

    Returns the parsed dict on success, None on failure (file missing,
    parse error etc.). On hot-reload (mtime advanced), emits an INFO log.
    """
    try:
        current_mtime = os.path.getmtime(path)
    except FileNotFoundError:
        # First-time miss — try to load (may fail with FileNotFoundError below)
        current_mtime = -1.0
    except Exception as e:
        logger.warning(f"static_cache: stat({path}) failed: {e}")
        return None

    cached = _caches.get(path)
    if cached is not None and cached[0] == current_mtime and current_mtime >= 0:
        return cached[1]

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        logger.warning(f"static_cache: file not found at {path}")
        return None
    except json.JSONDecodeError as e:
        logger.warning(f"static_cache: JSON parse failed for {path}: {e}")
        # Keep prior cache on parse failure to avoid a corrupted edit
        # killing the service mid-flight.
        return cached[1] if cached else None
    except Exception as e:
        logger.warning(f"static_cache: load failed for {path}: {e}")
        return cached[1] if cached else None

    if cached is not None:
        logger.info(
            f"static_cache: hot-reloaded {os.path.basename(path)} "
            f"(mtime {cached[0]} → {current_mtime})"
        )

    _caches[path] = (current_mtime, data, 0.0)
    return data


def invalidate(path: str | None = None) -> None:
    """Manually invalidate the cache for a path (or all if None)."""
    if path is None:
        _caches.clear()
    else:
        _caches.pop(path, None)
