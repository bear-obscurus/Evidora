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
              │  107 live-API connectors    │
              │  78 static-first services   │
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

The diagram is simplified. Two stages sit between the reranker and the
synthesizer prompt: a STRUKTURELL **provenance gate**
(`resolve_struct_marker_provenance`, neutralizes off-topic cosine-
contamination markers before they reach the LLM) and a **fan-out prompt
budget** (`_budget_prompt_sources`, caps how many sources enter the prompt
on broad claims while keeping `source_coverage` complete). The
"hallucination filter / verdict-consistency check" box is now a dedicated,
unit-tested module — `verdict_postprocess.apply_verdict_postprocessing`,
the override cascade described in §5. A semantic verdict cache
(`verdict_cache.py`, with a negation guard) can short-circuit the whole
pipeline for near-identical repeat claims.

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
top-level reranker. The descriptor-match threshold **0.45** lives in
`_topic_match.py` (`DEFAULT_BACKUP_THRESHOLD`, passed into
`_backup_best_matches`); `services/_reranker_backup.py` provides the
underlying cosine engine (`best_matches`, own default 0.65 for the
boolean `claim_might_be_about` callers).

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

> **Prerequisite (restored 2026-07-02):** hot-reload only works because
> `docker-compose.yml` mounts `./backend/data` into the container. If
> that mount is ever removed, the data files are baked into the image
> at build time — host-side edits and `git pull` then never reach the
> running container, and every `data/*.json` change silently requires
> `docker compose up -d --build backend`. The mount also makes
> generated caches (`cordis_projects_slim.json`,
> `claimreview_index.json`) and the cron refresh tools survive rebuilds.

#### Deploy decision table

The hot-reload behavior interacts with the deployment workflow. After
a change, exactly one of three actions is needed:

| What changed | Deploy command | Why |
|---|---|---|
| `data/*.json` | nothing — picked up on next request | mtime-aware cache + verdict-cache version-suffix |
| `services/*.py`, `main.py`, `requirements.txt` | `docker compose up -d --build backend` | needs a new image; `COPY` runs only at build time |
| Container env / runtime flags only | `docker compose restart backend` | re-launches the same image with the new env |

Common failure mode: choosing `restart` after a code change. The
container relaunches cleanly, no error appears, but the new module is
nowhere — the image still holds the old `COPY`-ed files. Verification
trick:

```bash
ssh <host> 'docker exec evidora-backend-1 ls /app/services/<new_file>.py'
```

If the new file isn't there, the build was skipped and you're running
the old image. `up -d --build` fixes it.

> **On prod, don't decide by hand:** `website/deploy.sh` codifies this
> table — it pulls, derives the right action from the changed files
> (build / recreate / hot-reload-nothing, incl. a mount sanity-check for
> data-only changes), waits for healthy, and then *proves* the deploy:
> container StartedAt must reset, changed backend files must be
> hash-identical host vs. container, `/api/legal` must return 200.
> Failures push an ntfy alert. `--dry-run` shows the decision only.

### 4.2 Cron-jobs on production

Seven cron-jobs run on the Hetzner host (verified 2026-07-06):

| Time | Job | Purpose |
|---|---|---|
| Sun 03:00 | `tools/weekly_phrasing_check.py` | Re-runs a 20-claim phrasing-drift battery; pushes an ntfy alert if verdict- or source-match < 18/20 |
| Mon 04:00 | `tools/data_freshness_check.py --max-age-days 120 --strict` | Walks `data/*.json` (`fetched_at_iso` > 120 d) + health-checks the generated caches; ntfy alert on problems |
| Mon 04:00 | `tools/check_urls.py --live 30` | URL health check over the Tier-1 source links |
| daily 02:30 | `evidora-restic-backup.sh` | restic backup (`.env` + `data/`) to the Hetzner volume |
| Sun 03:30 | `evidora-restic-prune.sh` | restic retention (keep-daily 7 / keep-weekly 4) |
| monthly 1st 03:00 | `docker builder/image/container prune` | Reclaims Docker build cache and dangling images |
| quarterly 1st 03:00 | `tools/refresh_cordis.py` | Rebuilds the CORDIS slim cache (generated, untracked) |

Tool jobs go through `run_evidora_tool.sh`, which loads the prod env
file and the bypass API key; failures append to `ALERTS.log` and push
ntfy. See `memory/deployment_hetzner.md`.

**No cron may write a git-tracked file.** Since `data/` is host-mounted,
an in-container write to a tracked file dirties the server worktree and
blocks every following deploy (`deploy.sh` dirty-guard). Curated files
with refreshable values (`eu_crime.json`, `vdem_indicators.json`) are
refreshed manually — run the matching `tools/refresh_*.py` locally,
commit, deploy; the freshness cron nags via ntfy when they age past
120 d. The former quarterly `refresh_eurostat_crime.py` cron was removed
for exactly this reason (2026-07-06).

### 4.3 Bypass key for stress tests

`EVIDORA_TEST_API_KEY` (env) + `X-Evidora-Test-Key` (HTTP header)
disables the per-IP rate limit (default 10/min). Used by stress-test
scripts to fire 4 claims in parallel without throttling.

### 4.4 Stress-test methodology

Standardized 20-claim battery, four measurement points:

1. **Verdict match** — synthesizer verdict against expected
   (`true`/`mostly_true`/`mixed`/`mostly_false`/`false`/`unverifiable` —
   the only labels the pipeline can emit), target ≥ 18/20.
2. **Source match** — expected primary source in
   `source_coverage.names`, target ≥ 18/20.
3. **Trigger-gap log** — every miss documented with which trigger
   should have fired and which token was missing.
4. **Hot-reload mini-test** — edit a `data/*.json` field in-place via
   ssh + sed, fire the same claim, expect the new value in the
   response without docker restart.

58+ structured stress-test PDFs have been run since the project started;
cumulative **1100+ curated claims**, aggregate verdict-match consistently
above 90 %. Latest systematic run: 100-claim stress test across 15
vulnerability clusters, 96.9 % (93/96) after three fix-sprints.

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
| Prompt-injection delimiters (analyzer) | `claim_analyzer.py` | User claims wrapped in `<claim>` XML to neutralize embedded instructions |
| Prompt-injection hardening (synthesizer) | `synthesizer.py` (`_harden_claim_for_prompt` + `_CLAIM_GUARD`) | The claim also enters the *synthesizer* prompt; a `</claim>` breakout could inject a fake `STRUKTURELL FALSCH:` source block and force a verdict. Neutralizes claim tags / marker literals / `[PRIMARY]` blocks in the user claim + guard instruction. Tests: `tests/test_prompt_injection.py` |
| Input hardening | `main.py` | Unicode tricks, control chars, OData injection |
| Per-IP rate limit | `main.py` + `ratelimit.py` | Abuse (bypass-key for stress-testing). Keyed on `X-Real-IP` (set un-spoofably by the trusted host-nginx); the client-controlled leftmost `X-Forwarded-For` is deliberately ignored. Bounded store with eviction. Tests: `tests/test_ratelimit.py` |
| Atomic cache/JSON writes | `_atomic.py` (`atomic_write_json` via tempfile + `os.replace` + fsync) | No truncated files on a write abort — used by the generated caches (datacommons/cordis/mitre) and the curated refresh tools |
| Verdict post-processing module | `verdict_postprocess.py` | The ~820-line override cascade (STRUKTURELL override + relevance guards, Wikipedia-normative-term, 4-tier consistency check, factual-content patterns, AMS/ILO + electoral-forecast guards) extracted out of `synthesize_results` so the execution order is explicit and each override is unit-testable (`tests/test_verdict_postprocess_golden.py`, `tests/test_unit.py::TestVerdictPostprocessing`) |
| STRUKTURELL provenance gate | `_topic_match.py` + `_struct_marker.py` + `reranker.py` | Off-topic cosine-contamination: packs matched only via the cosine backup (not an exact trigger) emit a resolvable `STRUKTURELL_COSINE_FALSCH:` marker; `resolve_struct_marker_provenance` (after rerank, before prompt) degrades it to plaintext when an exact anchor exists, else restores it. The categorical discriminator is match **provenance**, not a cosine threshold (which does not separate). Tests: `tests/test_struct_provenance.py` |
| Semantic-cache negation guard | `verdict_cache.py` | Inverted cached verdicts: MiniLM is negation-blind (cos ≥ 0.92 for "X" vs "not X"), so `_polarity_mismatch` skips a semantic cache hit when negation appears on only one side or the claims cite disjoint numbers |
| Degraded-analysis confidence cap | `verdict_postprocess.py` (`apply_analysis_fallback_cap`) | Silent high-confidence verdicts when the analyzer fell back to raw-claim-only analysis (`_fallback`): caps confidence + adds a transparency caveat |
| Fan-out prompt budget | `synthesizer.py` (`_budget_prompt_sources`) | Oversized prompts on broad claims (20–28 sources): only the top-N sources go into the prompt (authoritative + STRUKTURELL always kept), `source_coverage`/`raw_sources` stay complete |

**Performance (not robustness, but pipeline-relevant):** the claim embedding
is memoized per request (`_reranker_backup._encode_claim_cached`, bounded LRU)
instead of being recomputed for every static-first trigger, and the MiniLM
model lives in a single shared instance (`_st_model.py`) used by both the
reranker and the backup-trigger/verdict-cache path.

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
