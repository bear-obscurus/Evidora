# Evidora

*Evid(ence) + ora(re) — Evidence speaks for itself*

A European fact-checking service against misinformation — powered by a local LLM (Mistral 7B via Ollama) or optionally the Mistral Cloud API (EU servers).

Evidora automatically verifies claims against scientific and institutional sources such as PubMed, OpenAlex, Cochrane, WHO, WHO Europe, EMA, ECDC, Copernicus, Eurostat, ECB, UNHCR, EEA, OECD, World Bank, DataCommons, EUvsDisinfo, and European fact-checkers.

**Live Demo:** [https://evidora.eu](https://evidora.eu)

> ⚠️ This project is under active development. The online version uses the Mistral Cloud API (EU servers, Paris) for AI analysis.

## Features

- **Local or Cloud LLM** — Run locally via Ollama (Mistral 7B) or use the Mistral API (EU servers, Paris) for cloud deployment
- **19 data sources** — Scientific databases, systematic reviews, official EU/UN/OECD statistics, climate data, disease surveillance, disinformation databases, and fact-checkers
- **Cross-validation** — Primary sources (PubMed, WHO, Eurostat) are weighted higher than secondary sources (fact-checkers)
- **Multi-country ranking** — Superlative claims ("highest", "most") automatically query all EU-27 countries for a full ranking
- **Multi-dimensional context** — Prevents one-metric verdicts by injecting methodological caveats (energy safety: 7 dimensions; PISA: 7 education dimensions; CO₂: territorial vs. consumption-based; migration: asylum vs. total; GDP: welfare vs. output)
- **Hallucination filtering** — Evidence URLs are verified against actual source results
- **Input hardening** — Unicode normalization, control character stripping, OData injection prevention, 500-character claim limit
- **Prompt injection defense** — User claims are wrapped in XML delimiters with explicit LLM instructions to ignore embedded commands
- **GDPR-compliant** — No cookies, no tracking, anonymized logs
- **Bilingual** — Full German/English interface (DE/EN toggle)
- **Accessible** — ARIA labels, keyboard navigation, skip links, semantic landmarks
- **SpaCy NER** — Named entity recognition enriches claim analysis with deterministic GPE/DATE/ORG entities (German lg + English sm models)
- **OECD PISA + SDMX** — PISA 2022 scores (static CSV, 35 countries, by gender) + live OECD API for gender wage gap, education, employment
- **Semantic reranking** — Sentence Transformers (multilingual MiniLM) rerank source results by relevance
- **Search history** — Last 10 checks stored locally (localStorage), no server storage
- **PDF export** — Save fact-check results as PDF
- **Share button** — Copy result link to clipboard (with fallback for HTTP/older browsers)
- **Claim writing tips** — Built-in guidance for formulating precise, checkable claims
- **Background data updates** — Static datasets (PISA, OWID COVID) are automatically refreshed

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose)
- [Ollama](https://ollama.com/) with the `mistral` model (not needed if using Mistral API):
  ```bash
  ollama pull mistral
  ```
- **~6 GB RAM** available for Mistral 7B inference (local mode only)
- Ports **3000** (frontend) and **8000** (backend) must be free

## Quickstart

### 0. Configure Ollama for Docker access

By default, Ollama only listens on `localhost`. Docker containers need network access, so you must configure Ollama to listen on all interfaces:

**Linux (systemd):**
```bash
sudo systemctl edit ollama
```
Add the following:
```ini
[Service]
Environment="OLLAMA_HOST=0.0.0.0:11434"
```
Then restart:
```bash
sudo systemctl daemon-reload
sudo systemctl restart ollama
```

**macOS:** Ollama Desktop already listens on all interfaces — no changes needed.

**Windows:** Set the environment variable `OLLAMA_HOST=0.0.0.0:11434` in System Settings → Environment Variables, then restart Ollama.

### 1. Clone the repository

```bash
git clone https://github.com/bear-obscurus/Evidora.git
cd Evidora
```

### 2. Configure API keys

Copy the template and add your own keys:

```bash
cp website/.env.example website/.env
```

Open `website/.env` and replace the placeholders:

| Variable | Description | Get a key |
|---|---|---|
| `OLLAMA_URL` | URL to your Ollama instance (default works for Docker) | — |
| `PUBMED_API_KEY` | For PubMed queries (optional but recommended) | [NCBI API Key](https://ncbiinsights.ncbi.nlm.nih.gov/2017/11/02/new-api-keys-for-the-e-utilities/) |
| `PUBMED_EMAIL` | Contact email for PubMed API (recommended by NCBI) | — |
| `GOOGLE_FACTCHECK_API_KEY` | For ClaimReview search via European fact-checkers | [Google Fact Check Tools](https://console.cloud.google.com/apis/library/factchecktools.googleapis.com) |
| `CDS_API_KEY` | For Copernicus climate data (catalogue works without) | [Copernicus CDS](https://cds.climate.copernicus.eu/how-to-api) |
| `MISTRAL_API_KEY` | Use Mistral Cloud API instead of local Ollama (optional) | [Mistral Console](https://console.mistral.ai/api-keys) |
| `MISTRAL_MODEL` | Cloud model to use (default: `mistral-small-latest`) | — |
| `IMPRESSUM_NAME` | Your name for the legal notice | — |
| `IMPRESSUM_EMAIL` | Contact email (displayed as text, no mailto link) | — |
| `IMPRESSUM_LOCATION` | Your location for the legal notice | — |
| `RATE_LIMIT` | Max requests per window per IP (default: `10`) | — |
| `RATE_WINDOW` | Rate limit window in seconds (default: `60`) | — |

> **Note:** PubMed works without an API key but with lower rate limits (3 instead of 10 requests/second). Without `GOOGLE_FACTCHECK_API_KEY`, the fact-checker search is skipped. If `MISTRAL_API_KEY` is set, Evidora uses the Mistral Cloud API (EU servers, Paris) instead of a local Ollama instance — no GPU required.

### 3. Start

```bash
cd website
docker compose up -d --build
```

The app will be available at:
- **Frontend:** [http://localhost:3000](http://localhost:3000)
- **Backend API:** [http://localhost:8000](http://localhost:8000)

> **Important:** The `.env` file is required. If you skip step 2, the backend will fail to connect to Ollama.
>
> **First startup:** The backend downloads SpaCy models (~500 MB), a multilingual Sentence Transformer (~120 MB), and prefetches DataCommons index data (~197 MB). The healthcheck allows up to 3 minutes for this process.

### 4. Stop

```bash
docker compose down
```

## Data Sources

| Source | Type | Coverage | Status |
|---|---|---|---|
| PubMed | Biomedical studies | Health, medicine, biology | ✅ Active |
| Cochrane Reviews | Systematic reviews (via PubMed) | Highest level of medical evidence | ✅ Active |
| WHO GHO | Health indicators | Global health statistics | ✅ Active |
| WHO Europe (HFA) | Health for All Gateway | 39 indicators, 66 European countries | ✅ Active |
| EMA | Drug approvals (EU) | Pharmaceuticals, vaccines | ✅ Active |
| ECDC (via OWID) | Infectious diseases | COVID-19 cases, deaths, vaccinations (cached CSV) | ✅ Active |
| Copernicus CDS | Climate data (ERA5, CAMS) | Temperature, emissions, satellite | ✅ Active |
| Eurostat | EU statistics | Economy, migration, energy, CO₂, housing, debt, wages, inequality, tourism | ✅ Active |
| EEA (via Eurostat) | Environmental data | GHG emissions, air pollutants, renewables, waste | ✅ Active |
| ECB | Central bank data | Interest rates, exchange rates, money supply | ✅ Active |
| UNHCR | Refugee statistics | Refugee populations, asylum applications | ✅ Active |
| OECD | Education & gender equality | PISA 2022 scores (35 countries, by gender), gender wage gap, employment | ✅ Active |
| GADMO Faktenchecks | German-language fact-checks | APA, Correctiv (DACH region) | ✅ Active |
| DataCommons | ClaimReview aggregator | Global fact-checker results via knowledge graph | ✅ Active |
| World Bank | Development indicators | GDP, poverty, unemployment, inflation, CO₂, education, military, Gini | ✅ Active |
| OWID Energy Safety | Multi-dimensional energy profiles | 9 sources × 7 dimensions: deaths/TWh, CO₂, land use, waste, catastrophe risk, decommissioning, capacity factor | ✅ Active |
| OpenAlex | Scholarly works (250M+) | All disciplines: physics, social science, economics, engineering, etc. | ✅ Active |
| EUvsDisinfo | Disinformation database | Pro-Kremlin disinformation cases (EEA East StratCom) | ✅ Active |
| Google Fact Check API | ClaimReview markup | European fact-checkers (EFCSN) | ✅ Active |

## Security

- **Input sanitization** — Unicode NFC normalization, control character stripping, whitespace collapsing, 500-character limit
- **OData injection prevention** — User input is sanitized before inclusion in OData filter queries (WHO API)
- **Prompt injection defense** — Claims are wrapped in `<claim>` XML delimiters; LLM system prompts explicitly instruct to ignore embedded instructions
- **Rate limiting** — Configurable per-IP rate limiting (default: 10 requests/60s), respects `X-Forwarded-For` behind reverse proxy
- **No traceback leaking** — Error responses return generic messages; full tracebacks are logged server-side only
- **GDPR-compliant** — No cookies, no tracking, no personal data stored

## Project Structure

```
Evidora/
├── website/
│   ├── .env                 # Your API keys (not committed!)
│   ├── .env.example         # Template for .env
│   ├── docker-compose.yml
│   ├── backend/
│   │   ├── main.py          # FastAPI entry point
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── pytest.ini        # Test configuration
│   │   ├── tests/            # Test suite (unit, source API, integration)
│   │   ├── data/             # Static datasets
│   │   │   └── pisa_2022.csv # OECD PISA scores by country & gender
│   │   └── services/        # Data source modules
│   │       ├── claim_analyzer.py  # LLM-based claim analysis
│   │       ├── ollama.py          # Ollama/Mistral API client
│   │       ├── ner.py             # SpaCy NER enrichment (de_lg + en_sm)
│   │       ├── pubmed.py          # PubMed (biomedical studies)
│   │       ├── who.py             # WHO GHO (health indicators)
│   │       ├── who_europe.py      # WHO Europe HFA Gateway (39 indicators)
│   │       ├── ema.py             # EMA (drug approvals)
│   │       ├── claimreview.py     # Google Fact Check API
│   │       ├── copernicus.py      # Copernicus CDS (climate)
│   │       ├── eurostat.py        # Eurostat (EU statistics)
│   │       ├── eea.py             # EEA (environment, via Eurostat API)
│   │       ├── ecdc.py            # ECDC (COVID via OWID)
│   │       ├── cochrane.py        # Cochrane systematic reviews
│   │       ├── ecb.py             # ECB (interest rates, exchange rates)
│   │       ├── unhcr.py           # UNHCR (refugee statistics)
│   │       ├── oecd.py            # OECD (PISA + SDMX live API)
│   │       ├── gadmo.py           # GADMO fact-checks (APA, Correctiv)
│   │       ├── datacommons.py     # DataCommons ClaimReview aggregator
│   │       ├── euvsdisinfo.py     # EUvsDisinfo (disinformation DB)
│   │       ├── energy_safety.py    # OWID energy safety (deaths per TWh)
│   │       ├── openalex.py        # OpenAlex (250M+ scholarly works)
│   │       ├── worldbank.py       # World Bank (global development indicators)
│   │       ├── data_updater.py    # Background CSV/data refresh
│   │       ├── cache.py           # In-memory response cache
│   │       ├── reranker.py        # Sentence Transformers reranking
│   │       └── synthesizer.py     # LLM synthesis via Ollama/Mistral
│   └── frontend/
│       ├── nginx.conf       # Nginx reverse proxy config
│       ├── index.html
│       ├── style.css
│       ├── app.js
│       ├── i18n.js          # DE/EN translations
│       └── favicon.svg
├── .gitignore
└── LICENSE                  # MIT License
```

## How It Works

1. **Claim Analysis** — The LLM analyzes the input claim (wrapped in `<claim>` delimiters for injection safety), extracts keywords, determines the category, and generates optimized search queries
2. **NER Enrichment** — SpaCy (de_core_news_lg + en_core_web_sm) adds deterministic GPE/DATE/ORG entities to supplement LLM extraction
3. **Source Querying** — Relevant sources are queried in parallel based on the claim's category (e.g., health → PubMed + Cochrane + WHO + WHO Europe + EMA + ECDC; migration → Eurostat + UNHCR; economy → Eurostat + ECB; education/gender → OECD PISA + SDMX)
4. **Semantic Reranking** — Sentence Transformers (multilingual MiniLM) rerank results by semantic similarity to the original claim
5. **Cross-Validation** — Results from primary sources (scientific databases) are weighted higher than secondary sources (fact-checkers)
6. **Synthesis** — The LLM evaluates all evidence and produces a verdict (true/mostly true/mixed/mostly false/false/unverifiable) with confidence score
7. **Multi-Dimensional Context** — Data sources with one-metric bias risk (energy safety, PISA, CO₂, migration, GDP) automatically inject methodological caveats so the LLM cannot produce a misleading single-dimension verdict
8. **Claim Guards** — Superlative claims ("highest", "most") require multi-country data; record claims ("all-time low") are checked against historical min/max; present-tense claims are compared to the latest data point
9. **Verdict Consistency** — Post-processor detects when the LLM summary contradicts the verdict field and auto-corrects
10. **Hallucination Filter** — All evidence URLs are verified against actual source results; fabricated references are removed
11. **Caching** — API responses are cached in-memory (30 min TTL) to reduce load and speed up repeated queries

## Tech Stack

- **Backend:** Python, FastAPI, SSE streaming
- **Frontend:** Vanilla JS, CSS (no frameworks)
- **LLM:** Mistral 7B via Ollama (local) or Mistral API (cloud, EU servers)
- **NLP:** SpaCy (de_core_news_lg + en_core_web_sm) for named entity recognition
- **ML:** Sentence Transformers (multilingual MiniLM) for semantic reranking
- **Deployment:** Docker Compose (backend + nginx)

## Testing

```bash
cd website/backend

# Unit tests (no backend needed, instant)
python -m pytest tests/test_unit.py -v

# Source API tests (needs network, no LLM)
python -m pytest tests/test_sources.py -v --timeout=60

# Integration tests (needs running backend + LLM)
python -m pytest tests/test_integration.py -v --timeout=180

# All tests
python -m pytest -v --timeout=180
```

## Troubleshooting

| Problem | Solution |
|---|---|
| `Connection refused` to Ollama | Make sure Ollama is running and listening on `0.0.0.0:11434` (see step 0) |
| `host.docker.internal` not resolving (Linux) | Requires Docker 20.10+. The `extra_hosts` entry in docker-compose.yml handles this automatically. |
| Port 3000/8000 already in use | Stop the conflicting service, or change ports in `docker-compose.yml` (e.g., `"3001:80"`) |
| Backend crashes on startup | Check that `website/.env` exists (`cp .env.example .env`) |
| Backend unhealthy on first start | First startup downloads models (~800 MB total). The healthcheck allows 3 minutes — wait and retry. |
| LLM responses are slow | Mistral 7B needs ~6 GB RAM. Close other memory-heavy applications. |
| `Rate limit exceeded` (429) | Default: 10 requests per 60 seconds per IP. Adjust via `RATE_LIMIT` and `RATE_WINDOW` in `.env` |
| `API credits exhausted` | Mistral Cloud API has no remaining credits. Top up at [Mistral Console](https://console.mistral.ai/) |

## License

[MIT](LICENSE)
