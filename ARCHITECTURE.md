# Evidora — Architecture

This document describes the internal architecture of Evidora for developers
who want to understand the codebase, contribute new data sources, or fork
the project. For installation and end-user documentation, see
[README.md](README.md).

---

## 1. Big picture

```
                    ┌─────────────────────────┐
   user claim  ───► │  FastAPI /verify (SSE)  │
                    └──────────┬──────────────┘
                               │
                ┌──────────────┴───────────────┐
                ▼                              ▼
        ┌──────────────┐             ┌──────────────┐
        │  Analyzer    │             │  NER (SpaCy) │
        │  (LLM)       │             │  GPE/DATE/ORG│
        └──────┬───────┘             └──────┬───────┘
               └────────────┬────────────────┘
                            ▼
              ┌─────────────────────────────┐
              │   Source fan-out (async)    │
              │  ─────────────────────────  │
              │  ~30 live-API services      │
              │  ~25 static-first topics    │
              └─────────────┬───────────────┘
                            ▼
              ┌─────────────────────────────┐
              │   Reranker  (MiniLM-L12)    │
              │   + authoritative whitelist │
              └─────────────┬───────────────┘
                            ▼
              ┌─────────────────────────────┐
              │   Synthesizer (Mistral)     │
              │   JSON verdict + evidence   │
              └─────────────┬───────────────┘
                            ▼
              ┌─────────────────────────────┐
              │   Hallucination filter      │
              │   verdict-consistency check │
              └─────────────┬───────────────┘
                            ▼
                  SSE events to frontend
```

The whole pipeline streams via Server-Sent Events. Each stage emits
progress events so the frontend can show a live timeline.

---

## 2. Three service patterns

Sources fall into three categories, each with its own anatomy.

### 2.1 Live-API services

Stateless wrappers around external APIs. Examples: `pubmed.py`,
`eurostat.py`, `who.py`, `worldbank.py`, `openalex.py`,
`europe_pmc.py`.

Pattern:
1. Build query from analysis (entities, category, claim text).
2. Async HTTP call(s).
3. Parse + normalize into the common result schema.
4. Cache via `cache.py` (30 min in-memory TTL).

These are the "classic" sources from the early phase of the project.
Their bottleneck is upstream latency; their failure mode is API outages
and rate limits.

### 2.2 Static-first topic services

Curated facts shipped as `data/*.json`, queried via deterministic
trigger logic with a semantic-similarity fallback. Examples:
`eu_courts.py`, `eu_crime.py`, `energy_charts.py`,
`medientransparenz.py`, `rki_surveillance.py`, `education_dach.py`,
`at_courts.py`, `oecd_health.py`, `housing_at.py`, `transport_at.py`,
plus the topic-cluster files (`at_factbook.py`, `dach_factbook.py`,
`pks.py`, `frontex.py`, `wahlen.py`, etc.).

The logic for these services is shared in `services/_topic_match.py`
(see §3). Each service file contains only the bits that vary per topic
(JSON path, descriptor function for the reranker fallback, result
shape).

These are the bulk of new sources added during the
v1/v2/Cluster A/B/C roadmap etappes.

### 2.3 Hybrid services

A static dataset for the slow-moving core, plus a live API for fresh
deltas. Currently: `oecd.py` (PISA 2022 CSV + OECD SDMX live for
gender wage gap, employment, etc.).

---

## 3. Static-first topic service anatomy

This is the pattern most likely to be reused. Adding a new topic
typically requires ~50 LOC of service code plus a JSON file.

### 3.1 JSON schema

```jsonc
{
  "schema_version": 1,
  "fetched_at_iso": "2026-04-29",
  "source_label": "OeNB Wohnimmobilienpreis-Index + Statistik Austria",
  "facts": [
    {
      "id": "oenb_wohnpreise_2024",
      "topic": "wohnpreise_at",
      "year": 2024,
      "headline": "Entwicklung der Wohnimmobilienpreise in Österreich",
      "data": {
        "wohnimmo_index_at_2024": 207.1,
        "context": "Free-text narrative for the synthesizer."
      },
      "context_notes": [
        "Methodological caveats — surfaced into the LLM prompt."
      ],
      "claim_phrasings_handled": [
        "Wohnen ist unleistbar geworden",
        "Wohnpreise explodieren Österreich"
      ],
      "trigger_keywords": [
        "wohnimmobilienpreise", "miete wien", "wohnen unleistbar"
      ],
      "trigger_composite": [
        ["wohn", "miete", "immobil"],
        ["preis", "kosten", "explod", "anstieg", "gestiegen"],
        ["österreich", "wien", "graz", "salzburg"]
      ],
      "source_url": "https://www.oenb.at/...",
      "secondary_url": "https://www.statistik.at/..."
    }
  ]
}
```

### 3.2 The trigger mechanic

Three trigger fields, evaluated against the lower-cased claim:

| Field | Semantic | Match rule |
|---|---|---|
| `trigger_keywords` | flat list of stems | **any-of** substring match (`kw.lower() in claim_lc`) |
| `trigger_composite` | list of alternation lists | **AND of OR**: every list must contribute at least one substring |
| `trigger_all` *(optional)* | list of composite rules | **OR of (AND of OR)**: any rule may satisfy the whole match |

A fact wins if **any** of these three patterns fire.

Both composite paths defensively lower-case tokens before comparing
(`tok.lower() in claim_lc`), so a stray `"Wohn"` in JSON does not
silently fail to match — the test
`test_composite_token_uppercase_still_matches` pins this contract.

### 3.3 The reranker-backup fallback

If neither substring nor composite trigger fires, but the service was
plausibly relevant, `_topic_match.find_matching_items` falls back to a
cosine-similarity match between the claim and a per-fact descriptor
string, using the multilingual MiniLM model already loaded for the
top-level reranker. Threshold: **0.45** (configurable per call).
Implementation: `services/_reranker_backup.py`.

This catches phrasings that are semantically clear but lexically
distant from the curated triggers — the typical "long-tail" failure
mode of substring-only systems.

### 3.4 The shared helper

`services/_topic_match.py` exposes:

```python
substring_or_composite_match(item, claim_lc) -> bool
find_matching_items(static_path, items_key, *,
                    claim_lc, full_claim, descriptor_fn,
                    threshold=0.45, top_n=3) -> list[dict]
load_items(static_path, items_key) -> list[dict]
```

Migrating the 10 cluster-A/B services to this helper removed 218 net
LOC and consolidated a previously duplicated pattern. Unit tests in
`tests/test_topic_match.py` cover the substring, composite,
`trigger_all`, backup-fallback, file-not-found and hot-reload paths
(36 tests, all green).

### 3.5 Anatomy of a new topic service

To add a new topic (`my_topic`):

1. **Create `data/my_topic.json`** with the schema in §3.1.
2. **Create `services/my_topic.py`**:

   ```python
   import os
   from services._topic_match import find_matching_items, load_items

   STATIC_JSON_PATH = os.path.join(
       os.path.dirname(__file__), "..", "data", "my_topic.json"
   )

   def _descriptor(fact: dict) -> tuple[dict, str]:
       head = fact.get("headline", "")
       notes = " ".join((fact.get("context_notes") or [])[:2])
       return (fact, f"{head}. {notes}"[:300])

   def claim_mentions_my_topic_cached(claim: str) -> bool:
       if not claim:
           return False
       return bool(find_matching_items(
           STATIC_JSON_PATH, "facts",
           claim_lc=claim.lower(), full_claim=claim,
           descriptor_fn=_descriptor,
       ))

   async def fetch_my_topic(analysis: dict) -> dict:
       claim = analysis.get("claim", "")
       items = find_matching_items(
           STATIC_JSON_PATH, "facts",
           claim_lc=claim.lower(), full_claim=claim,
           descriptor_fn=_descriptor,
       )
       results = [_format_result(it) for it in items]
       return {"source": "MyTopic", "results": results}
   ```

3. **Wire in `main.py`**:
   - Add the source to the fan-out dispatch table.
   - Register in the rate-limit / source-coverage list.

4. **Whitelist in the reranker** (`services/reranker.py`) so the
   source is treated as authoritative for relevant categories.

5. **Add a prefetch in `data_updater.py`** if the JSON has a refresh
   strategy (cron-job, mtime-driven, etc.).

6. **Add to `tests/test_sources.py`** as a smoke test (one or two
   matching claims).

7. **Run a 20-claim stress test** following the methodology in
   `memory/stress_test_method.md`.

---

## 4. Operations

### 4.1 Hot-reload of static JSON

`services/_static_cache.py` exposes `load_json_mtime_aware(path)`:
each call `os.stat`s the file. If the mtime advanced, the file is
re-read and a global `_data_version` counter increments. The verdict
cache (`services/cache.py`) includes `_data_version` in its cache
key, so any JSON edit transparently invalidates all stale verdicts —
no backend restart needed.

This is the foundation of the prod-edit workflow: ssh into the
server, edit `data/*.json` in-place, the next claim hits the new
content. Verified in stress-test methodology step 4
(`memory/stress_test_method.md`).

### 4.2 Cron-jobs on production

Two cron-jobs run on the Hetzner host:

| Time | Job | Purpose |
|---|---|---|
| Sun 03:00 | `tools/weekly_phrasing_check.py` | Re-runs a 20-claim phrasing-drift battery; emails maintainer if pass-rate < 18/20 |
| Mon 04:00 | `tools/data_freshness_check.py` | Walks `data/*.json`, alerts if any `fetched_at_iso` > 120 days old |

Both jobs go through `run_evidora_tool.sh`, which loads the prod env
file and the bypass API key. See `memory/deployment_hetzner.md`.

### 4.3 Bypass key for stress tests

`EVIDORA_TEST_API_KEY` (env) + `X-Evidora-Test-Key` (HTTP header)
disables the per-IP rate limit (default 10/min). Used by stress-test
scripts to fire 4 claims in parallel without throttling.

### 4.4 Stress-test methodology

Standardized 20-claim battery, four measurement points:

1. **Verdict match** — synthesizer verdict against expected
   (`true`/`false`/`mostly_false`/`misleading`/`unverifiable`),
   target ≥ 18/20.
2. **Source match** — expected primary source in
   `source_coverage.names`, target ≥ 18/20.
3. **Trigger-gap log** — every miss documented with which trigger
   should have fired and which token was missing.
4. **Hot-reload mini-test** — edit a `data/*.json` field in-place via
   ssh + sed, fire the same claim, expect the new value in the
   response without docker restart.

Eight stress tests have been run since the project started; cumulative
balance: **160+ claims, 0 false-positives, 0 false-negatives**.

Full methodology in `memory/stress_test_method.md`.

### 4.5 Trigger-wordform audit

A recurring failure mode is asymmetric substring matching: a token
`"drittel"` matches the noun forms `Drittel` / `Drittels` but **not**
the phrase `"jeder dritte"`. After such a miss surfaced in the
2026-04-30 Lehrer stress test, a heuristic audit script catches three
gap families:

- **Fractions**: `drittel` ↔ `jeder dritte`, `ein drittel`.
- **Verb ↔ noun**: `anstieg` ↔ `steigt`, `gestiegen`.
- **Verb conjugation**: `kollap` ↔ `kollabiert` (extra letter).

A sweep on commit `ec61f9e` fixed 16 gaps across 5 JSON files
(rki_surveillance, housing_at, education_dach, oecd_health,
energy_charts). Re-run after every new topic is added.

---

## 5. Robustness layers

In rough order of when they were added during the Cluster A/B/C
etappes:

| Layer | File | What it prevents |
|---|---|---|
| Reranker-backup trigger | `_reranker_backup.py` | Lexically distant claims missing curated triggers |
| Hot-reload static cache | `_static_cache.py` | Restart-induced downtime on data edits |
| Verdict-cache version-suffix | `cache.py` | Stale verdicts after JSON edits |
| Hallucination URL filter | `synthesizer.py` (post-process) | Made-up evidence URLs passing through |
| Verdict-consistency post-check | `synthesizer.py` | LLM summary contradicting its own verdict field |
| Synthesizer no-aggregate-invention | prompt block | LLM inventing percentages from descriptive prose |
| Multi-dimensional context injection | per-source data files | One-metric verdicts on multi-dim topics (energy, PISA, CO₂) |
| Prompt-injection delimiters | `claim_analyzer.py` | User claims wrapped in `<claim>` XML to neutralize embedded instructions |
| Input hardening | `main.py` | Unicode tricks, control chars, OData injection |
| Per-IP rate limit | `main.py` | Abuse, with bypass-key for stress-testing |

---

## 6. Pointers

| Topic | File / location |
|---|---|
| Detailed roadmap with all etappes | `memory/data_sources_roadmap.md` |
| Stress-test methodology | `memory/stress_test_method.md` |
| Deployment + cron docs | `memory/deployment_hetzner.md` |
| Political guard-rails (no party rating, etc.) | `memory/project_political_guardrails.md` |
| Sources tried and rejected | `memory/hard_to_implement.md` |
| One-page synthesis (re-entry) | `memory/stand_2026_04_30_synthese.md` |
