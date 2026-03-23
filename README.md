# Evidora

A European fact-checking service against misinformation — powered by a local LLM (Mistral 7B via Ollama).

Evidora automatically verifies claims against scientific and institutional sources such as PubMed, WHO, EMA, Copernicus, Eurostat, and European fact-checkers (EFCSN members).

## Features

- **Local LLM** — All analysis runs on your machine via Ollama (Mistral 7B), no data leaves your system
- **8 European data sources** — Scientific databases, official EU statistics, and climate data
- **Cross-validation** — Primary sources (PubMed, WHO, Eurostat) are weighted higher than secondary sources (fact-checkers)
- **Hallucination filtering** — Evidence URLs are verified against actual source results
- **GDPR-compliant** — No cookies, no tracking, anonymized logs
- **Bilingual** — Full German/English interface (DE/EN toggle)
- **Accessible** — ARIA labels, keyboard navigation, skip links, semantic landmarks

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose)
- [Ollama](https://ollama.com/) with the `mistral` model:
  ```bash
  ollama pull mistral
  ```

## Quickstart

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

> **Note:** PubMed works without an API key but with lower rate limits (3 instead of 10 requests/second). Without `GOOGLE_FACTCHECK_API_KEY`, the fact-checker search is skipped.

### 3. Start

```bash
cd website
docker compose up -d --build
```

The app will be available at:
- **Frontend:** [http://localhost:3000](http://localhost:3000)
- **Backend API:** [http://localhost:8000](http://localhost:8000)

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
| ECDC | Infectious diseases | Epidemiological surveillance | 🔜 Planned |

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

## Tech Stack

- **Backend:** Python, FastAPI, SSE streaming
- **Frontend:** Vanilla JS, CSS (no frameworks)
- **LLM:** Mistral 7B via Ollama (local inference)
- **Deployment:** Docker Compose (backend + nginx)

## License

[MIT](LICENSE)
