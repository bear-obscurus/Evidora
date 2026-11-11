<!--
Thanks for the contribution! Please fill out the relevant sections.
For trivial changes (typos, doc fixes), most sections can be omitted.
-->

## What this changes

<!-- One-paragraph summary. What problem does this solve? -->

## Type of change

- [ ] New data source / topic pack
- [ ] Bug fix
- [ ] Refactor / cleanup
- [ ] Documentation
- [ ] Test
- [ ] Other:

## Testing

- [ ] The `light-unit-tests` files pass locally (list in
      `.github/workflows/test.yml`); a new test file is added to that list
- [ ] `pytest tests/test_unit.py` passes locally (if you have SpaCy installed)
- [ ] If trigger logic changed: ran `tools/stress_test.py` against a local
      backend and verdict-match ≥ 18/20
- [ ] If triggers were added: they do not fire on unrelated claims

Stress-test result (paste the summary if applicable):

```
Verdict-Match: __/20
Source-Match: __/__
```

## Related issues / context

<!-- Link to issues, prior PRs, ARCHITECTURE.md sections. -->

## Checklist

- [ ] Code follows the patterns in [ARCHITECTURE.md](../ARCHITECTURE.md)
- [ ] No new files committed that should be `.gitignore`d
  (test_results*.json, claimreview_index.json, .env)
- [ ] Commit message follows the `scope(area): subject` style
- [ ] No claim text or IP address is written to a file, log or protocol;
      if logging or retention changed, the privacy statement in
      `frontend/i18n.js` is updated in this PR (DE + EN)
- [ ] No party scores, election forecasts or own left/right labels
      ([ARCHITECTURE.md §6](../ARCHITECTURE.md))
