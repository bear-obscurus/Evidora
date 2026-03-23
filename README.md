# Evidora
European fact-checking against fake news

# Evidora

Europäischer Faktencheck-Service gegen Fake-News — powered by einem lokalen LLM (Mistral 7B via Ollama).

Evidora prüft Behauptungen automatisch gegen wissenschaftliche und institutionelle Quellen wie PubMed, WHO, EMA, Copernicus, Eurostat und europäische Faktenchecker (EFCSN-Mitglieder).

## Voraussetzungen

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (inkl. Docker Compose)
- [Ollama](https://ollama.com/) mit dem Modell `mistral`:
  ```bash
  ollama pull mistral
  ```

## Schnellstart

### 1. Repository klonen

```bash
git clone <repository-url>
cd Evidora
```

### 2. API-Keys konfigurieren

Kopiere die Vorlage und trage deine eigenen Keys ein:

```bash
cp website/.env.example website/.env
```

Öffne `website/.env` und ersetze die Platzhalter:

| Variable | Beschreibung | Key beantragen |
|---|---|---|
| `OLLAMA_URL` | URL zu deiner Ollama-Instanz (Standard passt für Docker) | — |
| `PUBMED_API_KEY` | Für PubMed-Abfragen (optional, aber empfohlen) | [NCBI API Key](https://ncbiinsights.ncbi.nlm.nih.gov/2017/11/02/new-api-keys-for-the-e-utilities/) |
| `GOOGLE_FACTCHECK_API_KEY` | Für ClaimReview-Suche bei europäischen Faktencheckern | [Google Fact Check Tools](https://console.cloud.google.com/apis/library/factchecktools.googleapis.com) |
| `CDS_API_KEY` | Für Copernicus-Klimadaten (Katalog funktioniert auch ohne) | [Copernicus CDS](https://cds.climate.copernicus.eu/how-to-api) |

> **Hinweis:** Ohne `PUBMED_API_KEY` funktioniert PubMed trotzdem, aber mit niedrigerem Rate-Limit (3 statt 10 Requests/Sekunde). Ohne `GOOGLE_FACTCHECK_API_KEY` wird die Faktenchecker-Suche übersprungen.

### 3. Starten

```bash
cd website
docker compose up -d --build
```

Die App läuft dann unter:
- **Frontend:** [http://localhost:3000](http://localhost:3000)
- **Backend-API:** [http://localhost:8000](http://localhost:8000)

### 4. Stoppen

```bash
docker compose down
```

## Projektstruktur

```
Evidora/
├── website/
│   ├── .env                 # Deine API-Keys (nicht committen!)
│   ├── .env.example         # Vorlage für .env
│   ├── docker-compose.yml
│   ├── backend/
│   │   ├── main.py          # FastAPI-Einstiegspunkt
│   │   └── services/        # Datenquellen-Module
│   │       ├── pubmed.py    # PubMed (biomedizinische Studien)
│   │       ├── who.py       # WHO (Gesundheitsindikatoren)
│   │       ├── ema.py       # EMA (Medikamentenzulassungen)
│   │       ├── claimreview.py  # EFCSN-Faktenchecker
│   │       ├── copernicus.py   # Copernicus CDS (Klimadaten)
│   │       ├── eurostat.py     # Eurostat (EU-Statistiken)
│   │       ├── eea.py          # EEA (Umweltdaten)
│   │       └── synthesizer.py  # LLM-Synthese via Ollama
│   └── frontend/
│       ├── index.html
│       ├── style.css
│       └── app.js
└── mind/                    # Entwicklungs-Notizen
```

## Datenquellen

| Quelle | Typ | Status |
|---|---|---|
| PubMed | Biomedizinische Studien | ✅ Aktiv |
| WHO GHO | Gesundheitsindikatoren | ✅ Aktiv |
| EMA | Medikamentenzulassungen (EU) | ✅ Aktiv |
| EFCSN-Faktenchecker | ClaimReview-Markup | ✅ Aktiv |
| Copernicus CDS | Klimadaten (ERA5, CAMS, Satellit) | ✅ Aktiv |
| Eurostat | EU-Statistiken (Wirtschaft, Migration, Energie, …) | ✅ Aktiv |
| ECDC | Infektionskrankheiten | 🔜 Geplant |
| EEA | Umweltdaten (Luft, Emissionen, Biodiversität) | ✅ Aktiv |

## Lizenz

Privates Projekt
