FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
# torch exakt gepinnt (2026-07-02-Prod-Stand; +cpu-Build kommt vom Index)
RUN pip install --no-cache-dir torch==2.12.1 --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir -r requirements.txt
# SpaCy-Modelle als gepinnte Wheels (spacy download zieht sonst still das
# jeweils neueste kompatible Modell — NER-Verhalten soll reproduzierbar sein)
RUN pip install --no-cache-dir \
    https://github.com/explosion/spacy-models/releases/download/de_core_news_lg-3.8.0/de_core_news_lg-3.8.0-py3-none-any.whl \
    https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.8.0/en_core_web_sm-3.8.0-py3-none-any.whl

# H4e (2026-07-02): non-root User. uid 1000 = Deploy-User "burrito" auf prod,
# damit hat der Container native Schreibrechte auf den ./backend/data-Mount
# (und neue Cache-Dateien landen burrito-owned auf dem Host) — das frühere
# cap_add: DAC_OVERRIDE ist damit obsolet. Port 8000 > 1024 braucht kein root.
RUN useradd --create-home --uid 1000 --user-group evidora

# SentenceTransformer-Modell ins Image backen statt Runtime-Download:
# (a) non-root braucht sonst einen beschreibbaren HF-Cache, (b) jeder
# frische Container lud bisher ~1 GB nach /root/.cache — Start wird
# schneller und unabhängig von huggingface.co-Verfügbarkeit.
ENV HF_HOME=/opt/hf-cache \
    SENTENCE_TRANSFORMERS_HOME=/opt/hf-cache
# HF_HUB_DISABLE_XET=1 beim Bake ERZWINGT echte Blob-Downloads statt Xet-
# Pointer. Ohne das lud der neuere huggingface_hub das Modell via Xet-
# Backend nur als Pointer in den Cache — die zur LAUFZEIT (HF_HUB_OFFLINE=1)
# nicht auflösbar sind. Folge (Audit 2026-07-08): Startup-Log "Failed to
# load shared SentenceTransformer ... couldn't find them in the cached files"
# -> "semantic re-ranking disabled". Der semantische Reranker + der Feed-
# Claim-Filter liefen prod-weit degradiert (Keyword-Fallback), themenfremde
# Feed-Items kamen durch.
RUN mkdir -p /opt/hf-cache \
 && HF_HUB_DISABLE_XET=1 python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2', revision='e8f8c211226b894fcb81acc59f3b34ba3efd5f42')" \
 && chown -R evidora:evidora /opt/hf-cache

COPY --chown=evidora:evidora . .

# BUILD-GATE (NACH COPY, damit der ECHTE Runtime-Ladepfad getestet wird):
# services._st_model.get_model() offline laden + encode. Das erste Build-Gate
# testete versehentlich einen ANDEREN Pfad (direkter SentenceTransformer-Call
# mit revision) als der Runtime (_st_model, der 'main' lud) — es gab grünes
# Licht, obwohl der Reranker im Prod tot war (Audit 2026-07-08, Lehre aus #54:
# ein Guard muss den ECHTEN Pfad testen). Jetzt exakt der Laufzeit-Code.
RUN HF_HUB_OFFLINE=1 python -c "from services._st_model import get_model; m = get_model(); assert m is not None, 'ECHTER Ladepfad _st_model.get_model() lieferte None — Reranker waere im Prod tot'; v = m.encode('Offline-Ladecheck'); assert v is not None and len(v) > 0, 'encode fehlgeschlagen'; print('BUILD-GATE ok: _st_model.get_model() offline ladbar + encode funktioniert')"

USER evidora

# H4d-Ergänzungen (Runtime-only, bewusst NACH dem Model-Bake-Layer):
# - HF_HUB_OFFLINE: Modell kommt aus dem Image — kein Remote-Ping beim Load
#   (unter read_only ohnehin nicht beschreibbar, Warnung entfällt).
# - PYTHONDONTWRITEBYTECODE: keine __pycache__-Schreibversuche auf dem
#   read-only Root-FS.
ENV HF_HUB_OFFLINE=1 \
    PYTHONDONTWRITEBYTECODE=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
