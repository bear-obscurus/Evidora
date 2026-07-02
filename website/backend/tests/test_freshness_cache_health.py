"""Pinnt check_generated_caches aus tools/data_freshness_check.py.

Hintergrund (2026-07-02): Der CORDIS-Quartals-Refresh lief nach einer
Upstream-Format-Umstellung STILL auf 0 Records — niemand merkte es, weil
Exit-Codes nur im Cron-Logfile landeten und der Freshness-Check generierte
Caches gar nicht prüfte. Diese Tests sichern die neue Health-Prüfung
(Existenz / Mindestgröße / Höchstalter). Kein Netzwerk, tmp_path-basiert.
"""

import importlib.util
import os
import time
from pathlib import Path

_TOOL = Path(__file__).resolve().parent.parent / "tools" / "data_freshness_check.py"
_spec = importlib.util.spec_from_file_location("data_freshness_check", _TOOL)
dfc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dfc)


def _make_cache(tmp_path, name, size_bytes, age_days=0):
    p = tmp_path / name
    p.write_bytes(b"x" * size_bytes)
    if age_days:
        old = time.time() - age_days * 86400
        os.utime(p, (old, old))
    return p


def test_healthy_caches_no_problems(tmp_path):
    _make_cache(tmp_path, "cordis_projects_slim.json", 60_000_000, age_days=10)
    _make_cache(tmp_path, "claimreview_index.json", 5_000_000)
    assert dfc.check_generated_caches(tmp_path) == []


def test_missing_cache_flagged(tmp_path):
    _make_cache(tmp_path, "claimreview_index.json", 5_000_000)
    problems = dfc.check_generated_caches(tmp_path)
    assert len(problems) == 1
    assert "cordis_projects_slim.json" in problems[0]
    assert "FEHLT" in problems[0]


def test_too_small_cache_flagged(tmp_path):
    # Der reale Fehlerfall: Refresh schrieb (fast) nichts
    _make_cache(tmp_path, "cordis_projects_slim.json", 1_000, age_days=1)
    _make_cache(tmp_path, "claimreview_index.json", 5_000_000)
    problems = dfc.check_generated_caches(tmp_path)
    assert len(problems) == 1
    assert "MB Minimum" in problems[0]


def test_too_old_cache_flagged(tmp_path):
    _make_cache(tmp_path, "cordis_projects_slim.json", 60_000_000, age_days=150)
    _make_cache(tmp_path, "claimreview_index.json", 5_000_000)
    problems = dfc.check_generated_caches(tmp_path)
    assert len(problems) == 1
    assert "Refresh-Cron" in problems[0]


def test_age_ignored_when_none(tmp_path):
    # claimreview_index hat max_age_days=None — Alter darf nicht alarmieren
    _make_cache(tmp_path, "cordis_projects_slim.json", 60_000_000, age_days=5)
    _make_cache(tmp_path, "claimreview_index.json", 5_000_000, age_days=400)
    assert dfc.check_generated_caches(tmp_path) == []
