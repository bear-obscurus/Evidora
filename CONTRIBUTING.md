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
  discouraged.
- Squash-merge or rebase-merge — keep the history clean.

### Commit message style

We use a lightweight scope-prefixed style. The recent log shows the
pattern:

```
feat(esoterik-pack): 15 Skeptiker-Konsens-Topics als Static-First-Service
fix(triggers): 16 Wortform-Lücken in 5 JSON-Trigger-Listen geschlossen
test(topic_match): 36 unit-Tests + defensive lower-cased composite tokens
docs(architecture): deploy decision table — restart vs --build vs nothing
refactor(services): 10 Static-Topic-Services auf _topic_match-Helper umgestellt
```

- **First line ≤ 80 chars**, present tense, scope in parentheses.
- **Body** explains the *why* and any non-obvious *what* — concrete
  numbers welcome (e.g. "−218 LOC", "16 gaps in 5 files").
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

If you see them as `untracked`, leave them alone.

## Testing

### Unit tests (mandatory before PR)

These have no network or LLM dependency and run in <10 s:

```bash
cd website/backend
python3 -m pytest tests/test_unit.py tests/test_topic_match.py -v
```

If you touched `services/_topic_match.py` or any static-first topic
service, run the whole topic-match suite — it pins both substring,
composite, `trigger_all`, backup-fallback, and hot-reload paths.

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
2. Create `services/<your_topic>.py` from the template in §3.5
3. Wire into `main.py` (add to imports + fan-out)
4. Whitelist the `indicator` in `services/reranker.py`
5. Smoke-test triggers locally
6. Add a claim set under `tools/stress_tests/` and run the stress test
7. Update this paragraph if your new source needs an API key

## Deploy considerations

You typically don't deploy as a contributor — that happens when a PR is
merged. But for local testing, the [decision table in
ARCHITECTURE.md §4.1](ARCHITECTURE.md) is the rule of thumb:

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
