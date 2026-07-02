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
RUN mkdir -p /opt/hf-cache \
 && python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2', revision='e8f8c211226b894fcb81acc59f3b34ba3efd5f42')" \
 && chown -R evidora:evidora /opt/hf-cache

COPY --chown=evidora:evidora . .

USER evidora

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
