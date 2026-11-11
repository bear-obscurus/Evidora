# Evidora — Architecture

This document describes the internal architecture of Evidora for developers
who want to understand the codebase, contribute new data sources, or fork
the project. For installation and end-user documentation, see
[README.md](README.md).

---

## 1. Big picture

```
                    ┌─────────────────────────┐
   user claim  ───► │ FastAPI /api/check (SSE)│
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

Curated facts shipped as `data/*.json` (675 facts in 63 files, 477 of
them in the 40 `*_pack.json` packs), queried via deterministic trigger
logic. A semantic-similarity fallback exists but is switched off for all
packs (§3.3). Examples:
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

Packs match the user's **original wording** (`analysis["original_claim"]`),
not the analyzer's paraphrase — the analyzer may drop exactly the markers a
trigger needs ("als die EU", "kaum noch").

Three trigger fields, evaluated against the lower-cased claim:

| Field | Semantic | Match rule |
|---|---|---|
| `trigger_keywords` | flat list of stems | **any-of** substring match (`kw.lower() in claim_lc`) |
| `trigger_composite` | list of alternation lists | **AND of OR**: every list must contribute at least one substring |
| `trigger_all` *(optional)* | list of composite rules | **OR of (AND of OR)**: any rule may satisfy the whole match |

A fact wins if **any** of these three patterns fire.

"Substring" is no longer a plain `in` test. Claim and trigger both pass
through `_schreibweise.normalisiere` (umlaut folding, unified separators —
"Oesterreich" = "Österreich"); short tokens are bound to word edges (#207 —
"ei" inside "Österreich" once fired an egg fact on welfare claims);
multi-word terms are inflection-tolerant (#167), and
umlaut plurals reach their singular token (#229). Tokens are lower-cased
defensively, so a stray `"Wohn"` in JSON still matches
(`test_composite_token_uppercase_still_matches`).

`find_matching_items` runs up to four passes and stops at the first that
returns anything:

1. **exact** — the trigger rules above;
2. **typo-tolerant** (`_tippfehler.py`, #205) — only if the exact pass is
   empty; recovered 45.8 % of hits lost to a single typo;
3. **English glossary** (`_englisch.py`, #206) — only for English claims;
   curated German terms are written next to the English words and the same
   rules run again;
4. **cosine backup** (§3.3) — only where the service passes a
   `descriptor_fn`.

Passes 2–4 tag their hits `exact=False`. A STRUKTURELL marker on such a
hit is emitted in resolvable form (`STRUKTURELL_COSINE_FALSCH:`) and the
provenance gate (§5) degrades it to plain text whenever another fact
anchors the claim exactly; without an exact anchor it is restored.

### 3.3 The reranker-backup fallback (off for all packs)

`_topic_match.find_matching_items` can fall back to a cosine-similarity
match between the claim and a per-fact descriptor string, using the
multilingual MiniLM model already loaded for the top-level reranker
(threshold **0.45**, `DEFAULT_BACKUP_THRESHOLD`; the boolean
`claim_might_be_about` callers in `_reranker_backup.py` use 0.65).

**It is switched off (`descriptor_fn=None`) in 50 services, including
every curated pack, since 2026-07-02.** The measurement behind that
decision: an off-topic SIPRI claim scored *higher* on several packs than
legitimate claims did (welthandel 0.594 vs. 0.483) — no threshold
separates "same broad topic" from "same claim". Pulled-in facts carried
STRUKTURELL markers about other topics into the prompt and flipped
verdicts. The trigger coverage of every pack's own
`claim_phrasings_handled` was raised to 100 % instead and is pinned by
`tests/test_konsens_exact_only.py`. Twelve services still use the
fallback.

The price is recall on human phrasing. The 2026-11-05 phrasing-variant
battery (§4.4) measured it: 67 % of colloquial variants still reach the
fact the clean phrasing reaches; irony (29 %), everyday words (34 %) and
Austrian colloquial (53 %) fall through. Simply also matching the
analyzer's paraphrase raised recall to 80 % but more than tripled
off-topic marker hits (1.6 % → 5.0 % of variants) — so the next step is a
relevance decision by the LLM, not a wider net.

### 3.4 The shared helper

`services/_topic_match.py` exposes:

```python
substring_or_composite_match(item, claim_lc) -> bool
find_matching_items(static_path, items_key, *,
                    claim_lc, full_claim, descriptor_fn,
                    threshold=0.45, top_n=3) -> list[dict]
load_items(static_path, items_key) -> list[dict]
```

All static-first services use this helper. `tests/test_topic_match.py`
covers the substring, composite, `trigger_all`, backup-fallback,
file-not-found and hot-reload paths; `tests/test_konsens_exact_only.py`
pins, for the consensus and myth packs it lists, that each pack's own
`claim_phrasings_handled` keep hitting their fact and that the packs stay
exact-only.

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

   def claim_mentions_my_topic_cached(claim: str) -> bool:
       if not claim:
           return False
       return bool(find_matching_items(
           STATIC_JSON_PATH, "facts",
           claim_lc=claim.lower(), full_claim=claim,
           descriptor_fn=None,  # exact-only, see §3.3
       ))

   async def fetch_my_topic(analysis: dict) -> dict:
       # Match the user's own wording, not the analyzer's paraphrase (§3.2).
       claim = analysis.get("original_claim") or analysis.get("claim", "")
       items = find_matching_items(
           STATIC_JSON_PATH, "facts",
           claim_lc=claim.lower(), full_claim=claim,
           descriptor_fn=None,
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
   matching claims), and register an exact-only pack in
   `tests/test_konsens_exact_only.py` so its phrasings stay covered.

7. **Write the phrasings the way people type.** Every entry in
   `claim_phrasings_handled` must hit its own fact. Add colloquial
   variants on purpose — everyday words, Austrian idiom, a worried
   question — because those are where routing loses facts today (§3.3).

8. **Run a 20-claim stress test** following §4.4.

---

## 4. Operations

### 4.1 Hot-reload of static JSON

`services/_static_cache.py` exposes `load_json_mtime_aware(path)`:
each call `os.stat`s the file. If the mtime advanced, the file is
re-read and a global `_data_version` counter increments. The verdict
cache (`services/cache.py`) includes `_data_version` in its cache
key, so any JSON edit transparently invalidates all stale verdicts —
no backend restart needed.

This is why a data-only change needs no rebuild: commit it, run
`deploy.sh`, and the next claim hits the new content. Do **not** edit
`data/*.json` on the server by hand — the edit dirties the server
worktree and `deploy.sh` refuses every following deploy until it is
cleaned up (dirty guard; see §4.2 "No cron may write a git-tracked
file"). The hot-reload itself is verified in stress-test step 4 (§4.4).

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

> **On prod, don't decide by hand:** `website/deploy.sh` is the only
> way to deploy. Do not run `git pull` on the server first: the script
> pulls itself and derives its action from what the pull brings — after a
> manual pull it sees nothing new and skips the build. It derives the right action from the changed files
> (build / recreate / hot-reload-nothing, incl. a mount sanity-check for
> data-only changes), waits for healthy, and then *proves* the deploy:
> container StartedAt must reset, changed backend files must be
> hash-identical host vs. container, `/api/legal` must return 200.
> Failures push an ntfy alert. `--dry-run` shows the decision only.

### 4.2 Cron-jobs on production

Eleven cron-jobs run on the Hetzner host. The canonical copy lives in
`website/crontab.prod` and is installed with `crontab crontab.prod` —
before 2026-10-18 the list existed only on the server, and PR #249
silently broke two of its lines (see below):

| Time | Job | Purpose |
|---|---|---|
| Sun 03:00 | `tools/weekly_phrasing_check.py` | Re-runs a 20-claim phrasing-drift battery; pushes an ntfy alert if verdict- or source-match < 18/20 |
| Mon 04:00 | `tools/data_freshness_check.py --max-age-days 120 --strict` | Walks `data/*.json` (`fetched_at_iso` > 120 d) + health-checks the generated caches; ntfy alert on problems |
| Mon 04:00 | `tools/check_urls.py --live 30` | URL health check over the Tier-1 source links |
| daily 02:30 | `evidora-restic-backup.sh` | restic backup (`.env` + `data/`) to the Hetzner volume |
| Sun 03:30 | `evidora-restic-prune.sh` | restic retention (keep-daily 7 / keep-weekly 4) |
| monthly 1st 03:00 | `docker builder/image/container prune` | Reclaims Docker build cache and dangling images |
| quarterly 1st 03:00 | `tools/refresh_cordis.py --force` | Rebuilds the CORDIS slim cache (generated, untracked) |
| daily 05:00 | `tools/canary_check.py` | Fires one real claim through the whole pipeline against `http://backend:8000`; the 2026-07-29 outage went unnoticed for twelve hours because no cron touched `/api/check` |
| daily 05:30 | `tools/quellen_lebenszeichen.py` | Probes every source for a sign of life; exit 2 = finding |
| daily 06:00 | `tools/stichtag_check.py --strict` | Finds facts whose built-in cut-off date has passed — complements the freshness check, which only looks at file age |
| daily 06:30 | `tools/modell_wachhund.py` | Checks that the answering model is still in Mistral's catalog and when it is retired (#289) — the API's `deprecation` field was `None` for all 64 models, so the e-mail of 2026-10-30 was the only warning; `mistral-medium-2604` retires 2027-04-30 |

Tool jobs go through `run_evidora_tool.sh`, which runs the tool in a
separate one-shot container (`--profile wartung`) with the prod env file
and its own memory limit. No cron line names a compose service any more:
that knowledge lives in the wrapper alone, because PR #249 renamed the
service and the quarterly CORDIS line kept calling the old name
(`no such service: cordis-refresh`, dormant until 2027-01-01).

**Exit-code contract** (lesson from #236, reinforced 2026-10-18): a tool
separates *I have a finding* from *I could not run*. On a finding the tool
has already sent its own ntfy push, so the cron only logs and must not
send a second one — `canary_check.py` returns 1 (backend answers but the
pipeline yields nothing) or 3 (backend unreachable at the configured
address), `quellen_lebenszeichen.py` returns 2. A blanket `|| { … }`
branch conflates both cases: it made the canary alert read
"Backend-Container down?" for six days while the container was healthy.
`tests/test_cron_aufrufe.py` gates this.

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
disables the per-client rate limit (default 10/min). Used by stress-test
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

58+ structured stress-test PDFs were run up to July 2026; cumulative
**1100+ curated claims**, aggregate verdict-match consistently above
90 % (100-claim stress test across 15 vulnerability clusters: 96.9 % =
93/96 after three fix-sprints).

Since July the main instrument is the **transfer QA**: claims written the
way users phrase them, expectations fixed and reviewed *before* the run,
every miss re-run to separate bugs from model variance, a fresh cache for
every claim. Latest: QA50I (2026-10-20), 87/98 = 88 % raw.

Two rules from these runs:
- **Measure first, with the decision rule written down before the
  data exists.** A threshold chosen after seeing the numbers proves
  nothing.
- **A single run is not a finding.** Verdicts vary between runs; a
  difference counts only once it repeats (three runs).

**Phrasing-variant battery (since 2026-11-05).** It measures *routing*,
not verdicts. The clean phrasing of 100 random curated facts and 30
live-source claims is rewritten ten ways each, 1,300 variants in all:
first person, worried question, hearsay, everyday word, Austrian
colloquial, phone typing, keyword fragment, conspiracy framing, irony,
and a longer text. The metric is how often the fact or source switch
that the clean phrasing reaches is still reached. The analysis stage runs
once in the one-shot container; the matching runs locally and costs
nothing. Baseline: 67 % for facts, 53 % for live switches. The same
battery is the yardstick for analyzer prompt changes and for the model
switch before 2027-04-30.

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
| Reranker-backup trigger | `_reranker_backup.py` | Lexically distant claims missing curated triggers — off for all packs since 2026-07-02, still on in 12 services (§3.3) |
| Typo-tolerant + English trigger passes | `_tippfehler.py`, `_englisch.py` (#205/#206) | One typo or an English claim losing the curated fact; both run only when the exact pass is empty (§3.2) |
| Hot-reload static cache | `_static_cache.py` | Restart-induced downtime on data edits |
| Verdict-cache version-suffix | `cache.py` | Stale verdicts after JSON edits |
| Hallucination URL filter | `synthesizer.py` (post-process) | Made-up evidence URLs passing through |
| Verdict-consistency post-check | `synthesizer.py` | LLM summary contradicting its own verdict field |
| Synthesizer no-aggregate-invention | prompt block | LLM inventing percentages from descriptive prose |
| Multi-dimensional context injection | per-source data files | One-metric verdicts on multi-dim topics (energy, PISA, CO₂) |
| Prompt-injection delimiters (analyzer) | `claim_analyzer.py` | User claims wrapped in `<claim>` XML to neutralize embedded instructions |
| Prompt-injection hardening (synthesizer) | `synthesizer.py` (`_harden_claim_for_prompt` + `_CLAIM_GUARD`) | The claim also enters the *synthesizer* prompt; a `</claim>` breakout could inject a fake `STRUKTURELL FALSCH:` source block and force a verdict. Neutralizes claim tags / marker literals / `[PRIMARY]` blocks in the user claim + guard instruction. Tests: `tests/test_prompt_injection.py` |
| Input hardening | `main.py` | Unicode tricks, control chars, OData injection |
| Per-client rate limit | `main.py` + `ratelimit.py` | Abuse (bypass-key for stress-testing). Keyed on `X-Real-IP` (set un-spoofably by the trusted host-nginx); the client-controlled leftmost `X-Forwarded-For` is deliberately ignored. Since #310 the table holds an HMAC-SHA256 of the address with a per-process `os.urandom` key, never the address; a background loop drops every entry at most 90 s after the last request. Tests: `tests/test_ratelimit.py`, `tests/test_ratelimit_ohne_ip.py` |
| Log redaction | `_log_redaktion.py` (+ `merke_anfrage` in `main.py`) | Claim text in logs (#306/#307). The words of the claim and of every field derived from it (entities, search queries) are registered per request and blacked out (`‹…›`) in every log line of every logger for 15 minutes — URLs decoded first, inflections checked both ways, field names before `=` and the `llm_usage` lines kept. Tests: `tests/test_log_redaktion.py` |
| File log without IPs | `_log_datei.py`, `frontend/nginx.conf`, `host_logs_ohne_ip.sh` | Logs lost on every deploy and IPs in access logs (#308). Backend log rotates at midnight UTC, 30 days kept; nginx uses `log_format ohne_ip` and logs the path without query. Retention numbers are pinned to the privacy statement by `tests/test_datenschutz_exakt.py` |
| STRUKTURELL shadow mode | `verdict_postprocess.py` (`STRUKTURELL_OVERRIDE`, default `schatten`) | The override never helped (0 of 69 myth runs needed it) and overturned correct verdicts five times (#305). It still decides: every time a marker meets a `true`/`mostly_true` verdict, one line goes to `data/_wartung/strukturell_schatten.jsonl` (date, tier decision incl. "would fire", verdict, confidence, topics — no claim text), and the verdict stays as the model set it. `aktiv` restores the old behavior. Tests: `tests/test_strukturell_schattenbetrieb.py` |
| Atomic cache/JSON writes | `_atomic.py` (`atomic_write_json` via tempfile + `os.replace` + fsync) | No truncated files on a write abort — used by the generated caches (datacommons/cordis/mitre) and the curated refresh tools |
| Verdict post-processing module | `verdict_postprocess.py` | The ~820-line override cascade (STRUKTURELL override — in shadow mode, see above — + relevance guards, Wikipedia-normative-term, 4-tier consistency check, factual-content patterns, AMS/ILO + electoral-forecast guards) extracted out of `synthesize_results` so the execution order is explicit and each override is unit-testable (`tests/test_verdict_postprocess_golden.py`, `tests/test_unit.py::TestVerdictPostprocessing`) |
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

## 6. Privacy and political invariants

Two sets of rules hold for every change, whatever it touches.

**Privacy.** The privacy statement on evidora.eu promises that claims and
IP addresses are not stored. Code must keep that promise literally:

- No claim text and no IP address goes into a file, a log or a protocol.
  New log lines run through `_log_redaktion.py` automatically; anything
  written outside the logging system must not contain either.
- A change to what is logged or how long it is kept changes the privacy
  statement (`frontend/i18n.js`, DE and EN). `tests/test_datenschutz_exakt.py`
  binds every number in that text to its constant, so the two cannot drift
  apart silently.
- Before adding any persistent protocol, read the privacy statement
  first.

**Politics.** Evidora checks facts about politics; it does not rate
politics:

- no party scores or indices of its own;
- no election forecasts;
- no left/right classification of its own;
- no `true`/`false` on a politically loaded label when Wikipedia or
  Wikidata are the only sources.

Established academic indices such as V-Dem and Freedom House are cited
instead.

## 7. Pointers

| Topic | File / location |
|---|---|
| Deploy logic | `website/deploy.sh` (`--dry-run` shows the decision) |
| Production crontab | `website/crontab.prod`, wrapper `website/run_evidora_tool.sh` |
| Contributor workflow | [CONTRIBUTING.md](CONTRIBUTING.md) |
| CI checks | `.github/workflows/test.yml` (`light-unit-tests`, `check-new-urls`) |
| Stress-test claim sets | `website/backend/tools/stress_tests/` |
| Retrieval census (which source answered what) | `website/backend/tools/retrieval_census.py` |

The maintainer keeps the sprint history, QA reports and roadmap notes
outside the repository; the durable conclusions from them are folded into
this document.
