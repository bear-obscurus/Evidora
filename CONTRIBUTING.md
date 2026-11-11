# Contributing to Evidora

Thanks for considering a contribution. This document is the practical
companion to [README.md](README.md) (installation) and
[ARCHITECTURE.md](ARCHITECTURE.md) (how the pipeline works) — it
focuses on the *workflow*: how to develop, test, and submit changes.

## Quick orientation

If you're new, read in this order:

1. **[README.md](README.md)** — what Evidora is, how to run it locally
2. **[ARCHITECTURE.md](ARCHITECTURE.md)** — pipeline, service patterns,
   operations
3. **[ARCHITECTURE.md §3.5](ARCHITECTURE.md)** — anatomy of adding a new
   data source (the most likely first contribution)

## Development workflow

### Branching

- `main` is always deployable.
- Open a feature branch for any change: `feat/<short-name>`,
  `fix/<short-name>`, `docs/<short-name>`.
- Open a Pull Request against `main`. Direct pushes to `main` are
  **blocked by branch protection** (since 2026-07-02, enforced for
  admins too) — both CI checks (unit suites + URL gate) must pass
  before a PR can be merged. A red check now actually blocks the
  merge instead of just warning.
- Squash-merge — required linear history keeps `main` clean.

### Commit message style

We use a lightweight scope-prefixed style. The recent log shows the
pattern:

```
fix(datenschutz): das Rate-Limit haelt keine IP mehr, und nichts laenger als 90 Sekunden (#310)
fix(strukturell): der Override half nie und schadete fuenfmal — jetzt im Schatten (#305)
feat(retrieval): tippfehler-toleranter Trigger-Pass — 45,8 % der verlorenen Treffer zurück (#205)
docs(architecture): deploy decision table — restart vs --build vs nothing
```

- **Scope in parentheses**, then a subject that says what is *true now*
  ("the rate limit holds no IP any more"), not which function changed.
  The PR number goes at the end.
- **Body** explains the *why* and any non-obvious *what* — concrete,
  measured numbers welcome (e.g. "45.8 % of lost hits recovered",
  "16 gaps in 5 files"). Say what was measured and what was not.
- German or English are both fine; pick one and stick with it for the
  whole commit.

### Files NOT to commit

The repo's `.gitignore` already covers `*.env`, `__pycache__`, IDE
files, OS junk, and `.claude/` (private memory). A few more that should
**not** be committed manually:

- `website/backend/test_results*.json` — local pytest output dumps
- `website/backend/data/claimreview_index.json` — generated cache
  (~5 MB), rebuilt at backend startup
- `website/backend/test_claims.py` — historical local debug script
- `website/backend/data/_wartung/` — runtime protocols written by the
  backend (already ignored)
- anything with personal data: contracts, invoices, user e-mails, real
  user claims

If you see them as `untracked`, leave them alone.

## Testing

### Unit tests (mandatory before PR)

CI runs `light-unit-tests`: an explicit list of 183 model-free test
files in `.github/workflows/test.yml` — no SpaCy, no network, a few
seconds. A red check blocks the merge. Run the same files locally before
you push. The quickest check for trigger work:

```bash
cd website/backend
python3 -m pytest tests/test_topic_match.py tests/test_konsens_exact_only.py -v
```

`tests/test_unit.py` needs the SpaCy models and therefore does not run in
CI; run it locally if you touched the analyzer or the NER path. If you
add a new test file, add it to the list in `test.yml`, otherwise CI never
runs it.

If you touched `services/_topic_match.py` or any static-first topic
service, run the whole topic-match suite — it pins the substring,
composite, `trigger_all`, typo-tolerant, backup-fallback and hot-reload
paths.

### Source API tests (recommended)

These hit external APIs and are slower (~30-60 s) but catch regressions
in source-specific code:

```bash
python3 -m pytest tests/test_sources.py -v --timeout=60
```

### Integration tests (optional, needs running backend)

```bash
docker compose up -d --build
python3 -m pytest tests/test_integration.py -v --timeout=180
```

### Stress test (when you change trigger logic or add a source)

If your change affects what the matcher catches, run a stress test
against your local backend:

```bash
python3 tools/stress_test.py --claims tools/stress_tests/esoterik.json \
    --url http://localhost:8000 --concurrency 2
```

Pass criterion: `verdict-match >= 18/20` (default threshold). Methodology
is documented in [ARCHITECTURE.md §4.4](ARCHITECTURE.md). If your change
drops the rate below the threshold, the script exits non-zero — fix
before submitting the PR.

If you add a *new* topic, also create a claim set under
`tools/stress_tests/<your-topic>.json` (see existing files for the
schema) and run it. The PR description should include the result.

## URL stability for source links

Every Topic-Pack `source_url` and `secondary_url` is shown to end users
in the Verdict output — broken links damage credibility. We follow this
**Stabilitäts-Hierarchie** when picking URLs:

| Priorität | Methode | Wann |
|---|---|---|
| 1 | **DOI** (`https://doi.org/10.xxxx/yyyy`) | für wissenschaftliche Studien — Crossref garantiert Permanenz |
| 2 | **PubMed PMID** (`https://pubmed.ncbi.nlm.nih.gov/12345/`) | für medizinische Studien — NIH-Garantie |
| 3 | **Aktuelle Original-URL** (Topic-Übersicht oder spezifischer Pfad) | wenn Site stabile Topic-URLs hat |
| 4 | **Wikipedia-Artikel** als kurierte Alternative | wenn Original nicht erreichbar oder Cloudflare-blockt |
| 5 | **Domain-Hauptseite** als Notfall-Fallback | nur wenn alles andere ausfällt |

Vor jedem Commit, der `data/*.json` ändert, **bitte URLs prüfen**:

```bash
cd website/backend

# Voll-Audit aller URLs (langsam, ~3 min):
python3 tools/check_urls.py --tier 1

# Nur die URLs prüfen, die in deinem Branch neu hinzugekommen sind
# (schnell, das ist auch was die CI prüft):
python3 tools/check_new_urls.py --base origin/main --head HEAD
```

Das CI-Gate `check-new-urls` läuft bei jedem PR und schlägt fehl, wenn
neu hinzugekommene URLs **404/410** zurückgeben (echt tot). 403er
(Bot-Filter), Timeouts (transient) und ähnliche Edge-Cases werden als
**Warnung** ausgegeben, blockieren aber nicht — diese URLs sind oft im
User-Browser funktional.

Bei toten URLs nutze `tools/repair_urls.py` für Wayback-/DOI-/Domain-
Vorschläge:

```bash
python3 tools/repair_urls.py --url https://www.example.com/dead-page
```

## Adding a new data source

The static-first topic-service pattern is the most common contribution
shape. The full anatomy is in
[ARCHITECTURE.md §3.5](ARCHITECTURE.md), but the short version:

1. Create `data/<your_topic>.json` with the schema in §3.1
2. Create `services/<your_topic>.py` from the template in §3.5 — exact-only
   (`descriptor_fn=None`), matching the user's `original_claim`
3. Wire into `main.py` (add to imports + fan-out)
4. Whitelist the `indicator` in `services/reranker.py`
5. Every phrasing in `claim_phrasings_handled` must hit its own fact —
   and write some of them the way people actually type (everyday words,
   Austrian idiom, a worried question). That is where routing loses facts
   today ([ARCHITECTURE.md §3.3](ARCHITECTURE.md)).
6. Check that your triggers do **not** fire on unrelated claims — a short
   generic token in a composite rule ("ei", "sicher") is the classic
   cause of a fact showing up where it does not belong
7. Add a claim set under `tools/stress_tests/` and run the stress test
8. Update this paragraph if your new source needs an API key

## Rules that apply to every change

**Privacy.** No claim text and no IP address may end up in a file, a log
or a protocol — the privacy statement promises exactly that. Logging goes
through the redaction filter automatically; if you change what is logged
or how long it is kept, update the privacy statement in
`frontend/i18n.js` (DE and EN) in the same PR. `tests/test_datenschutz_exakt.py`
fails if a number in the text no longer matches the code.

**Politics.** No party scores or indices of our own, no election
forecasts, no left/right classification of our own, and no `true`/`false`
on a politically loaded label when Wikipedia/Wikidata are the only
sources. Details: [ARCHITECTURE.md §6](ARCHITECTURE.md).

**Measure first.** For anything that changes verdicts or routing, write
the expected result and the decision rule down *before* you run the
measurement, and treat a single run as an observation, not a finding.

## Deploy considerations

You typically don't deploy as a contributor — the maintainer deploys
after the merge, exclusively with `website/deploy.sh`. For local
testing, the [decision table in ARCHITECTURE.md §4.1](ARCHITECTURE.md)
is the rule of thumb:

| Change | What to run |
|---|---|
| `data/*.json` | nothing — hot-reload picks it up |
| `services/*.py`, `main.py`, `requirements.txt` | `docker compose up -d --build backend` |
| Container env / runtime flags only | `docker compose restart backend` |

## What if you want to add something bigger

Bigger architectural changes (new pipeline stage, new LLM provider,
schema migrations) should start with an issue first. We can discuss the
shape before code lands.

## Code of conduct

Be considerate. The project is built by a teacher in their free time.
Constructive criticism is welcome; entitled demands are not.

## License

By contributing, you agree that your contributions will be licensed
under the project's [MIT License](LICENSE).
