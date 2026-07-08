"""Contract-Test: der ST-Modell-Revision-Pin ist konsistent (Audit 2026-07-08).

Befund: services._st_model.get_model() lud OHNE revision (= 'main'), während
der Docker-Model-Bake das Modell mit revision='e8f8…' ins Offline-Image
backte. Im Offline-Container war nur der e8f8-Snapshot materialisiert → der
Runtime-Load fand 'main' nicht → „semantic re-ranking disabled". Reranker +
Feed-Filter liefen prod-weit tot.

Dieser Test pinnt, dass der Code-Revision-Pin und die Dockerfile-Bake-Revision
übereinstimmen — verhindert künftige stille Drift.

Dependency-light: liest nur die Konstante + das Dockerfile, kein Netz/Modell.
"""

import os
import re


def test_st_model_has_pinned_revision():
    from services import _st_model
    rev = _st_model._MODEL_REVISION
    assert isinstance(rev, str) and re.fullmatch(r"[0-9a-f]{40}", rev), \
        f"_MODEL_REVISION muss ein 40-stelliger Commit-Hash sein, war: {rev!r}"


def test_code_revision_matches_dockerfile_bake():
    from services import _st_model
    dockerfile = os.path.join(os.path.dirname(__file__), "..", "Dockerfile")
    text = open(dockerfile, encoding="utf-8").read()
    # Alle im Dockerfile referenzierten revisions müssen dem Code-Pin gleichen.
    revs = set(re.findall(r"revision='([0-9a-f]{40})'", text))
    assert revs, "Dockerfile referenziert keine revision im Model-Bake"
    assert revs == {_st_model._MODEL_REVISION}, (
        f"Revision-Drift! Code-Pin {_st_model._MODEL_REVISION} vs. "
        f"Dockerfile {revs} — Bake und Runtime-Ladepfad müssen dieselbe "
        f"Revision nutzen (sonst 'semantic re-ranking disabled' im Prod)."
    )


def test_build_gate_uses_real_runtime_path():
    """Das Build-Gate muss den ECHTEN Ladepfad (_st_model.get_model) testen,
    nicht einen parallelen direkten SentenceTransformer-Call (Lehre aus #54)."""
    dockerfile = os.path.join(os.path.dirname(__file__), "..", "Dockerfile")
    text = open(dockerfile, encoding="utf-8").read()
    assert "from services._st_model import get_model" in text, \
        "BUILD-GATE testet nicht den echten _st_model.get_model()-Runtime-Pfad"
