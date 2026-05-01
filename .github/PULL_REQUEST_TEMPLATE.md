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

- [ ] `pytest tests/test_topic_match.py` passes locally
- [ ] `pytest tests/test_unit.py` passes locally (if you have SpaCy installed)
- [ ] If trigger logic changed: ran `tools/stress_test.py` against a local
      backend and verdict-match ≥ 18/20

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
