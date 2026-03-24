# Evidora

A European fact-checking service against misinformation — powered by a local LLM (Mistral 7B via Ollama) or optionally the Mistral Cloud API (EU servers).

Evidora automatically verifies claims against scientific and institutional sources such as PubMed, Cochrane, WHO, EMA, ECDC, Copernicus, Eurostat, EEA, and European fact-checkers.

## Features

- **Local or Cloud LLM** — Run locally via Ollama (Mistral 7B) or use the Mistral API (EU servers, Paris) for cloud deployment
- **12 European data sources** — Scientific databases, systematic reviews, official EU statistics, climate data, disease surveillance, and fact-checkers
- **Cross-validation** — Primary sources (PubMed, WHO, Eurostat) are weighted higher than secondary sources (fact-checkers)
- **Hallucination filtering** — Evidence URLs are verified against actual source results
- **GDPR-compliant** — No cookies, no tracking, anonymized logs
- **Bilingual** — Full German/English interface (DE/EN toggle)
- **Accessible** — ARIA labels, keyboard navigation, skip links, semantic landmarks

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

### 4. Stop

```bash
docker compose down
```

## Data Sources

| Source | Type | Coverage | Status |
|---|---|---|---|
| PubMed | Biomedical studies | Health, medicine, biology | ✅ Active |
| WHO GHO | Health indicators | Global health statistics | ✅ Active |
| EMA | Drug approvals (EU) | Pharmaceuticals, vaccines | ✅ Active |
| Google Fact Check API | ClaimReview markup | European fact-checkers (EFCSN) | ✅ Active |
| Copernicus CDS | Climate data (ERA5, CAMS) | Temperature, emissions, satellite | ✅ Active |
| Eurostat | EU statistics | Economy, migration, energy | ✅ Active |
| EEA | Environmental data | Air quality, emissions, biodiversity | ✅ Active |
| ECDC | Infectious diseases | Epidemiological surveillance | ✅ Active |
| Cochrane Reviews | Systematic reviews (via PubMed) | Highest level of medical evidence | ✅ Active |
| GADMO Faktenchecks | German-language fact-checks | APA, Correctiv (DACH region) | ✅ Active |

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
│   │   └── services/        # Data source modules
│   │       ├── claim_analyzer.py  # LLM-based claim analysis
│   │       ├── pubmed.py          # PubMed (biomedical studies)
│   │       ├── who.py             # WHO (health indicators)
│   │       ├── ema.py             # EMA (drug approvals)
│   │       ├── claimreview.py     # Google Fact Check API
│   │       ├── copernicus.py      # Copernicus CDS (climate)
│   │       ├── eurostat.py        # Eurostat (EU statistics)
│   │       ├── eea.py             # EEA (environment)
│   │       ├── ecdc.py            # ECDC (infectious diseases)
│   │       ├── cochrane.py        # Cochrane systematic reviews
│   │       ├── gadmo.py           # GADMO fact-checks (APA, Correctiv)
│   │       ├── cache.py           # In-memory response cache
│   │       └── synthesizer.py     # LLM synthesis via Ollama
│   └── frontend/
│       ├── index.html
│       ├── style.css
│       ├── app.js
│       ├── i18n.js          # DE/EN translations
│       └── favicon.svg
└── LICENSE                  # MIT License
```

## How It Works

1. **Claim Analysis** — The LLM analyzes the input claim, extracts keywords, determines the category, and generates optimized search queries
2. **Source Querying** — Relevant sources are queried in parallel based on the claim's category (e.g., health claims → PubMed + WHO + EMA)
3. **Cross-Validation** — Results from primary sources (scientific databases) are weighted higher than secondary sources (fact-checkers)
4. **Synthesis** — The LLM evaluates all evidence and produces a verdict (true/mostly true/mixed/mostly false/false/unverifiable) with confidence score
5. **Hallucination Filter** — All evidence URLs are verified against actual source results; fabricated references are removed
6. **Caching** — API responses are cached in-memory (30 min TTL) to reduce load and speed up repeated queries

## Tech Stack

- **Backend:** Python, FastAPI, SSE streaming
- **Frontend:** Vanilla JS, CSS (no frameworks)
- **LLM:** Mistral 7B via Ollama (local inference)
- **Deployment:** Docker Compose (backend + nginx)

## Troubleshooting

| Problem | Solution |
|---|---|
| `Connection refused` to Ollama | Make sure Ollama is running and listening on `0.0.0.0:11434` (see step 0) |
| `host.docker.internal` not resolving (Linux) | Requires Docker 20.10+. The `extra_hosts` entry in docker-compose.yml handles this automatically. |
| Port 3000/8000 already in use | Stop the conflicting service, or change ports in `docker-compose.yml` (e.g., `"3001:80"`) |
| Backend crashes on startup | Check that `website/.env` exists (`cp .env.example .env`) |
| LLM responses are slow | Mistral 7B needs ~6 GB RAM. Close other memory-heavy applications. |
| `Rate limit exceeded` (429) | Default: 10 requests per 60 seconds per IP. Adjust via `RATE_LIMIT` and `RATE_WINDOW` in `.env` |

## License

[MIT](LICENSE)
