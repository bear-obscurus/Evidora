# Evidora

*Evid(ence) + ora(re) — Evidence speaks for itself*

A European fact-checking service against misinformation — powered by a local LLM (Mistral 7B via Ollama) or optionally the Mistral Cloud API (EU servers).

Evidora automatically verifies claims against **50+ scientific and institutional sources** spanning research databases, EU/UN/OECD statistics, climate data, disease surveillance, courts, parliaments, electoral records, fact-checker databases, and curated topic packs for Austria and the DACH region.

**Live Demo:** [https://evidora.eu](https://evidora.eu)

**Quality assurance:** 8 structured stress-tests run since project start, **160+ claims**, **0 false-positives, 0 false-negatives** (state 2026-04-30). See [ARCHITECTURE.md §4.4](ARCHITECTURE.md) for the methodology.

> ⚠️ This project is under active development. The online version uses the Mistral Cloud API (EU servers, Paris) for AI analysis.

## Features

- **Local or Cloud LLM** — Run locally via Ollama (Mistral 7B) or use the Mistral API (EU servers, Paris) for cloud deployment
- **50+ data sources** — Scientific databases, systematic reviews, clinical trials, official EU/UN/OECD/Austrian statistics, climate data, disease surveillance, court rulings, parliamentary records, electoral data, disinformation databases, and curated topic packs (see table below)
- **Static-first topic services** — Curated facts (`data/*.json`) with substring/composite triggers and a cosine-similarity backup, so well-known claims hit deterministic answers without an extra API roundtrip. ~25 topic services cover Austrian/DACH-specific questions (housing, education, transport, courts, electoral data, etc.). See [ARCHITECTURE.md §3](ARCHITECTURE.md)
- **Hot-reload of static data** — Edits to `data/*.json` go live without a backend restart (mtime-aware cache + verdict-cache version-suffix)
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
- **Semantic reranking** — Sentence Transformers (multilingual MiniLM) rerank source results by relevance, plus a backup-trigger fallback when curated triggers miss a phrasing
- **Search history** — Last 10 checks stored locally (localStorage), no server storage
- **PDF export** — Save fact-check results as PDF
- **Share button** — Copy result link to clipboard (with fallback for HTTP/older browsers)
- **Claim writing tips** — Built-in guidance for formulating precise, checkable claims
- **Background data updates** — Static datasets (PISA, OWID COVID, Statistik Austria) are automatically refreshed; two cron-jobs on production guard against phrasing drift and stale data

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
| `S2_API_KEY` | Semantic Scholar API key (optional, higher rate limits) | [Semantic Scholar API](https://www.semanticscholar.org/product/api#api-key) |
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

Sources are grouped by domain. Each lives in its own service module
(`backend/services/`). The fan-out chooses sources per claim category
during analysis. For implementation patterns (live-API vs. static-first
topic vs. hybrid), see [ARCHITECTURE.md §2](ARCHITECTURE.md).

| Domain | Sources |
|---|---|
| **Science & medicine** | PubMed, Cochrane (via PubMed), Europe PMC, OpenAlex, Semantic Scholar, ClinicalTrials.gov, Retraction Watch, bioRxiv / medRxiv |
| **Climate & environment** | NASA GISS, Berkeley Earth, Copernicus CDS, EEA (via Eurostat), GeoSphere AT, Skeptical Science, Energy-Charts |
| **Economy & finance** | Eurostat, OECD (PISA + SDMX), World Bank, ECB, Statistik Austria, WIFO + IHS forecasts, OeNB |
| **Politics & democracy** | V-Dem, Transparency International, RSF, SIPRI, IDEA, Parlament.gv.at, BMI Wahlen + Volksbegehren, MedienTransparenz (KommAustria) |
| **Justice & courts** | RIS (Austrian legal information), EuGH + EGMR (EU & ECHR rulings), VfGH + VwGH (Austrian constitutional + administrative courts) |
| **Health & disease surveillance** | WHO GHO, WHO Europe (HFA Gateway), ECDC (via OWID), EMA, EFSA, RKI SurvStat, OECD Health, BASG |
| **Migration** | UNHCR, Frontex |
| **Energy** | OWID Energy Safety (9 sources × 7 dimensions), Energy-Charts |
| **Fact-checker databases** | GADMO (APA + Correctiv), Google Fact Check API (EFCSN), DataCommons ClaimReview, EUvsDisinfo, Mimikama, AT-Faktencheck-RSS |
| **Austria-specific topic packs** | AT Factbook (18 topics), DACH Factbook (11 topics), BKA PKS (police crime statistics), Bildung DACH, Wohnen AT, Verkehr AT, AT Courts |

Each curated topic pack is a `data/*.json` file with substring +
composite triggers. The pattern is documented in
[ARCHITECTURE.md §3](ARCHITECTURE.md). To contribute a new topic,
follow the anatomy in §3.5.

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
│   ├── .env                       # Your API keys (not committed!)
│   ├── .env.example
│   ├── docker-compose.yml
│   ├── backend/
│   │   ├── main.py                # FastAPI entry point + SSE pipeline
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── pytest.ini
│   │   ├── tests/                 # Unit, source API, integration tests
│   │   ├── data/                  # Static datasets for static-first
│   │   │   │                      #   topic services (~25 JSON packs)
│   │   │   ├── pisa_2022.csv      # OECD PISA scores
│   │   │   ├── housing_at.json    # OeNB + Statistik Austria wohnen
│   │   │   ├── education_dach.json
│   │   │   ├── eu_courts.json
│   │   │   ├── …                  # +20 more topic packs
│   │   ├── services/
│   │   │   ├── _topic_match.py    # Shared trigger logic (10 services)
│   │   │   ├── _reranker_backup.py # Cosine-similarity fallback trigger
│   │   │   ├── _static_cache.py   # mtime-aware hot-reload for data/*.json
│   │   │   ├── _eurostat_live.py  # Generic Eurostat SDMX connector
│   │   │   ├── claim_analyzer.py  # LLM claim analysis
│   │   │   ├── ner.py             # SpaCy NER (GPE/DATE/ORG)
│   │   │   ├── reranker.py        # Sentence-Transformers reranking
│   │   │   ├── synthesizer.py     # LLM synthesis + verdict-consistency
│   │   │   ├── cache.py           # In-memory response cache
│   │   │   ├── ollama.py          # Ollama/Mistral API client
│   │   │   ├── data_updater.py    # Background data refresh
│   │   │   ├── pubmed.py · who.py · ema.py · efsa.py · ecdc.py
│   │   │   ├── eurostat.py · eea.py · ecb.py · unhcr.py · oecd.py
│   │   │   ├── openalex.py · europe_pmc.py · semantic_scholar.py
│   │   │   ├── clinicaltrials.py · cochrane.py · worldbank.py
│   │   │   ├── statistik_austria.py · biorxiv.py · retraction_watch.py
│   │   │   ├── basg.py · oenb.py · geosphere.py · copernicus.py
│   │   │   ├── gadmo.py · claimreview.py · datacommons.py
│   │   │   ├── euvsdisinfo.py · mimikama.py · at_faktencheck_rss.py
│   │   │   ├── eu_courts.py · eu_crime.py · at_courts.py · ris.py
│   │   │   ├── housing_at.py · transport_at.py · education_dach.py
│   │   │   ├── oecd_health.py · rki_surveillance.py · energy_safety.py
│   │   │   ├── energy_charts.py · medientransparenz.py · frontex.py
│   │   │   ├── wahlen.py · volksbegehren.py · abstimmungen.py
│   │   │   ├── parlament_at.py · transparency.py · vdem.py · rsf.py
│   │   │   ├── sipri.py · idea.py · pks.py · wifo_ihs.py
│   │   │   └── at_factbook.py · dach_factbook.py
│   │   └── tools/
│   │       ├── weekly_phrasing_check.py   # Cron: Sun 03:00
│   │       ├── data_freshness_check.py    # Cron: Mon 04:00
│   │       └── refresh_eurostat_crime.py  # Live-API refresh showcase
│   └── frontend/
│       ├── nginx.conf
│       ├── index.html · style.css · app.js
│       ├── i18n.js                # DE/EN translations
│       └── favicon.svg
├── ARCHITECTURE.md                # Developer-facing architecture doc
├── README.md                      # This file
├── .gitignore
└── LICENSE                        # MIT License
```

## How It Works

1. **Claim Analysis** — The LLM analyzes the input claim (wrapped in `<claim>` delimiters for injection safety), extracts keywords, determines the category, and generates optimized search queries
2. **NER Enrichment** — SpaCy (de_core_news_lg + en_core_web_sm) adds deterministic GPE/DATE/ORG entities to supplement LLM extraction
3. **Source Querying** — Relevant sources are queried in parallel based on the claim's category (e.g., health → PubMed + Cochrane + Europe PMC + ClinicalTrials.gov + Semantic Scholar + WHO + WHO Europe + EMA + ECDC; migration → Eurostat + UNHCR; economy → Eurostat + ECB; education/gender → OECD PISA + SDMX; Austrian claims → Statistik Austria VPI/health/mortality)
4. **Semantic Reranking** — Sentence Transformers (multilingual MiniLM) rerank results by semantic similarity to the original claim
5. **Cross-Validation** — Results from primary sources (scientific databases) are weighted higher than secondary sources (fact-checkers)
6. **Synthesis** — The LLM evaluates all evidence and produces a verdict (true/mostly true/mixed/mostly false/false/unverifiable) with confidence score
7. **Multi-Dimensional Context** — Data sources with one-metric bias risk (energy safety, PISA, CO₂, migration, GDP) automatically inject methodological caveats so the LLM cannot produce a misleading single-dimension verdict
8. **Claim Guards** — Superlative claims ("highest", "most") require multi-country data; record claims ("all-time low") are checked against historical min/max; present-tense claims are compared to the latest data point
9. **Verdict Consistency** — Post-processor detects when the LLM summary contradicts the verdict field and auto-corrects
10. **Hallucination Filter** — All evidence URLs are verified against actual source results; fabricated references are removed
11. **Caching** — API responses are cached in-memory (30 min TTL) to reduce load and speed up repeated queries

## Tech Stack

- **Backend:** Python 3.14, FastAPI, SSE streaming
- **Frontend:** Vanilla JS, CSS (no frameworks)
- **LLM:** Mistral 7B via Ollama (local) or Mistral Cloud API (EU servers, Paris)
- **NLP:** SpaCy (de_core_news_lg + en_core_web_sm) for named entity recognition
- **ML:** Sentence Transformers (multilingual MiniLM) for semantic reranking *and* a backup-trigger fallback for static-first topic services
- **Deployment:** Docker Compose (backend + nginx) on a single Hetzner host

## Documentation

- **[ARCHITECTURE.md](ARCHITECTURE.md)** — How the pipeline works, how
  to add a new data source, the static-first topic-service pattern,
  hot-reload, the cron-jobs that catch phrasing drift and stale data,
  and the stress-test methodology
- **[CONTRIBUTING.md](CONTRIBUTING.md)** — Branching workflow, commit
  style, testing requirements, how to add a new data source step by
  step, what NOT to commit
- **[README.md](README.md)** — This file (installation + features)

## Stress-testing

In addition to the unit tests in `website/backend/tests/`, Evidora ships
a stress-test runner that fires curated claim sets at the backend and
reports verdict-match plus expected-source coverage:

```bash
cd website/backend

# Run an existing claim set against the live site
python3 tools/stress_test.py --claims tools/stress_tests/esoterik.json

# Or against a local backend
python3 tools/stress_test.py --claims tools/stress_tests/lehrer.json \
    --url http://localhost:8000 --concurrency 2
```

Bundled claim sets live under `tools/stress_tests/`. Methodology
(four measurement points: verdict-match, source-match, trigger gaps,
hot-reload) is documented in [ARCHITECTURE.md §4.4](ARCHITECTURE.md).
Cumulative result across 10 stress tests, 200 curated claims:
**0 false-positives, 0 false-negatives** (state 2026-05-01).

A latency profiler complements this:

```bash
python3 tools/profile_latency.py
```

reports per-stage durations (analyze / search / synth) so the
bottleneck is always known.

## Testing

```bash
cd website/backend

# Unit tests (no backend needed, instant)
python3 -m pytest tests/test_unit.py tests/test_topic_match.py -v

# Source API tests (needs network, no LLM)
python3 -m pytest tests/test_sources.py -v --timeout=60

# Integration tests (needs running backend + LLM)
python3 -m pytest tests/test_integration.py -v --timeout=180

# All tests except integration
python3 -m pytest tests/ --ignore=tests/test_integration.py
```

### Stress-test methodology

In addition to unit/integration tests, Evidora uses **structured 20-claim
stress-tests** to catch trigger gaps and verdict regressions. Each test
measures verdict-match, expected-source coverage, trigger gaps, and live
hot-reload behavior. The full methodology lives in
`memory/stress_test_method.md` and [ARCHITECTURE.md §4.4](ARCHITECTURE.md);
cumulative balance across 8 tests: **160+ claims, 0 false-positives,
0 false-negatives** (state 2026-04-30).

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
