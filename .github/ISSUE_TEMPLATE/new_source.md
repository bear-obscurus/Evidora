---
name: New data source proposal
about: Propose a new data source or topic pack
title: 'Source: '
labels: enhancement, source-proposal
---

## Source

- **Name**: <e.g. "INSEE — French national statistics">
- **URL / API**: <link to API docs or RSS feed>
- **License / terms of use**: <free? attribution? rate limits?>
- **API key needed?**: yes / no — if yes, where to get one

## Why this source

What kind of claims would it help fact-check that the current ~50
sources don't cover well? Concrete examples welcome.

## Service pattern

- [ ] Live-API service (every claim hits the upstream)
- [ ] Static-first topic service (curated facts in `data/<topic>.json`,
      see ARCHITECTURE.md §3)
- [ ] Hybrid (static core + live deltas)

## Sample claims for a stress test

If accepted, a `tools/stress_tests/<source>.json` should accompany the
service. List 5–20 claim ideas here so we can sanity-check coverage.

## Owner

Are you proposing **and** implementing, or proposing only?
