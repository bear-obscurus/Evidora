FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir -r requirements.txt
RUN python -m spacy download de_core_news_lg
RUN python -m spacy download en_core_web_sm

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
 && python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')" \
 && chown -R evidora:evidora /opt/hf-cache

COPY --chown=evidora:evidora . .

USER evidora

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
