"""Unit tests for ``services._topic_match`` — the central helper for the
10 static-first topic services (eu_courts, eu_crime, energy_charts,
medientransparenz, rki_surveillance, education_dach, at_courts,
oecd_health, housing_at, transport_at).

Covers:
- ``substring_or_composite_match``: substring (any-of), composite
  (AND-of-OR), and the optional ``trigger_all`` (list of composite rules,
  OR between rules) trigger-pattern.
- ``find_matching_items``: orchestrates static-JSON-load + match-pass +
  reranker-backup-fallback (``_backup_best_matches`` mocked to keep the
  unit test free of sentence-transformers).
- ``load_items``: convenience-loader with empty/missing fallback.

Why this matters: 10 services delegate to this helper after the
2026-04-30 refactor (net −218 LOC); a regression here breaks every
static-first topic at once.
"""

import json

import pytest

from services import _topic_match
from services._topic_match import (
    substring_or_composite_match,
    find_matching_items,
    load_items,
)


# ---------------------------------------------------------------------------
# Cache-Reset zwischen Tests — _static_cache._caches ist module-level
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clear_static_cache():
    from services import _static_cache
    _static_cache.invalidate()
    yield
    _static_cache.invalidate()


def _write_json(tmp_path, data, name="data.json"):
    path = tmp_path / name
    path.write_text(json.dumps(data), encoding="utf-8")
    return str(path)


# ===================================================================
# substring_or_composite_match
# ===================================================================

class TestSubstringMatch:
    """Substring-Pfad: any-of über trigger_keywords."""

    def test_empty_item_returns_false(self):
        assert substring_or_composite_match({}, "irgendein claim") is False

    def test_single_keyword_match(self):
        item = {"trigger_keywords": ["drittel"]}
        assert substring_or_composite_match(item, "ein drittel der menschen") is True

    def test_no_keyword_match(self):
        item = {"trigger_keywords": ["impfung", "kollabiert"]}
        assert substring_or_composite_match(item, "die wirtschaft wächst") is False

    def test_any_of_multiple_keywords(self):
        item = {"trigger_keywords": ["drittel", "viertel", "fünftel"]}
        # only one needs to match
        assert substring_or_composite_match(item, "ein viertel der bevölkerung") is True

    def test_keyword_is_lowercased_before_match(self):
        # Real-world: trigger lists sometimes use mixed casing for readability.
        # Code calls kw.lower() before comparing to claim_lc.
        item = {"trigger_keywords": ["Drittel", "ANSTIEG"]}
        assert substring_or_composite_match(item, "ein drittel der bevölkerung") is True
        assert substring_or_composite_match(item, "der anstieg ist hoch") is True

    def test_empty_claim_returns_false(self):
        item = {"trigger_keywords": ["drittel"]}
        assert substring_or_composite_match(item, "") is False

    def test_substring_inside_word(self):
        # "explod" is a stem — should match "explodieren", "explodiert", etc.
        # That's the whole point of substring-matching word stems.
        item = {"trigger_keywords": ["explod"]}
        assert substring_or_composite_match(item, "die preise explodieren") is True
        assert substring_or_composite_match(item, "preisexplosion in wien") is False  # 'explos' ≠ 'explod'
        assert substring_or_composite_match(item, "wien-preise sind explodiert") is True


class TestCompositeMatch:
    """Composite-Pfad: AND-of-OR über trigger_composite."""

    def test_full_composite_match(self):
        item = {"trigger_composite": [
            ["wohn", "miete"],
            ["preis", "explosion"],
            ["wien", "österreich"],
        ]}
        assert substring_or_composite_match(item, "die wohnpreise in wien explodieren") is True

    def test_one_list_misses_breaks_and(self):
        item = {"trigger_composite": [
            ["wohn", "miete"],
            ["preis", "explosion"],
            ["wien", "österreich"],
        ]}
        # missing "preis"/"explosion" segment → AND fails
        assert substring_or_composite_match(item, "die mieten in wien stagnieren") is False

    def test_no_composite_token_match(self):
        item = {"trigger_composite": [["wohn"], ["preis"]]}
        assert substring_or_composite_match(item, "der käse ist teuer") is False

    def test_empty_alt_list_kills_and(self):
        # leere alt-Liste in composite kann nichts matchen → AND ist False
        item = {"trigger_composite": [["wohn"], []]}
        assert substring_or_composite_match(item, "die wohnpreise") is False

    def test_empty_composite_no_match(self):
        item = {"trigger_composite": []}
        # leere composite-Liste matcht nicht (kein implizites "match-all")
        assert substring_or_composite_match(item, "irgendwas") is False

    def test_composite_token_uppercase_still_matches(self):
        # Defensiv: composite-Tokens werden via tok.lower() verglichen,
        # damit ein versehentliches "Wohn" / "PREIS" in der JSON-Config
        # nicht still ins Leere läuft (analog zu trigger_keywords).
        item = {"trigger_composite": [["Wohn"], ["PREIS"]]}
        assert substring_or_composite_match(item, "wohnpreise sind hoch") is True


class TestSubstringPlusComposite:
    """Zusammenspiel: Substring zündet vor Composite."""

    def test_substring_wins_when_composite_misses(self):
        item = {
            "trigger_keywords": ["explod"],
            "trigger_composite": [["foo"], ["bar"]],
        }
        assert substring_or_composite_match(item, "die preise explodieren") is True

    def test_composite_wins_when_substring_misses(self):
        item = {
            "trigger_keywords": ["completelydifferent"],
            "trigger_composite": [["wohn"], ["preis"]],
        }
        assert substring_or_composite_match(item, "wohnpreise in wien") is True

    def test_both_miss_returns_false(self):
        item = {
            "trigger_keywords": ["foo"],
            "trigger_composite": [["bar"], ["baz"]],
        }
        assert substring_or_composite_match(item, "irgendwas anderes") is False


class TestTriggerAll:
    """trigger_all-Pfad: Liste von Composite-Regeln, OR zwischen Regeln."""

    def test_first_rule_matches(self):
        item = {"trigger_all": [
            [["wohn"], ["preis"]],          # rule 1
            [["miete"], ["explosion"]],     # rule 2
        ]}
        assert substring_or_composite_match(item, "wohnpreise sind hoch") is True

    def test_second_rule_matches(self):
        item = {"trigger_all": [
            [["wohn"], ["preis"]],
            [["miete"], ["explosion"]],
        ]}
        assert substring_or_composite_match(item, "miete-explosion in wien") is True

    def test_no_rule_matches(self):
        item = {"trigger_all": [
            [["wohn"], ["preis"]],
            [["miete"], ["explosion"]],
        ]}
        assert substring_or_composite_match(item, "der wein ist gut") is False

    def test_partial_rule_does_not_satisfy(self):
        # Rule 1 needs 'wohn' AND 'preis'; only 'wohn' matches → rule 1 false
        # Rule 2 needs 'miete' AND 'explosion'; neither → rule 2 false
        item = {"trigger_all": [
            [["wohn"], ["preis"]],
            [["miete"], ["explosion"]],
        ]}
        assert substring_or_composite_match(item, "die wohnung ist klein") is False

    def test_empty_trigger_all_no_match(self):
        item = {"trigger_all": []}
        assert substring_or_composite_match(item, "irgendwas") is False

    def test_trigger_all_token_uppercase_still_matches(self):
        # Same defensive lower-casing as composite-pfad.
        item = {"trigger_all": [
            [["WOHN"], ["Preis"]],
        ]}
        assert substring_or_composite_match(item, "wohnpreise sind hoch") is True


# ===================================================================
# find_matching_items
# ===================================================================

class TestFindMatchingItems:
    def test_missing_file_returns_empty(self, tmp_path):
        path = str(tmp_path / "nonexistent.json")
        result = find_matching_items(path, "facts", claim_lc="claim", full_claim="claim")
        assert result == []

    def test_missing_items_key_returns_empty(self, tmp_path):
        path = _write_json(tmp_path, {"other_key": []})
        result = find_matching_items(path, "facts", claim_lc="claim", full_claim="claim")
        assert result == []

    def test_null_items_returns_empty(self, tmp_path):
        # JSON: {"facts": null} — defensiv (items_key existiert, value ist null)
        path = _write_json(tmp_path, {"facts": None})
        result = find_matching_items(path, "facts", claim_lc="claim", full_claim="claim")
        assert result == []

    def test_substring_match_returns_items(self, tmp_path):
        data = {"facts": [
            {"id": "a", "trigger_keywords": ["drittel"]},
            {"id": "b", "trigger_keywords": ["bahn"]},
        ]}
        path = _write_json(tmp_path, data)
        result = find_matching_items(
            path, "facts",
            claim_lc="ein drittel der bevölkerung",
            full_claim="Ein Drittel der Bevölkerung",
        )
        assert len(result) == 1
        assert result[0]["id"] == "a"

    def test_multiple_items_match(self, tmp_path):
        data = {"facts": [
            {"id": "a", "trigger_keywords": ["drittel"]},
            {"id": "b", "trigger_keywords": ["drittel", "bahn"]},
            {"id": "c", "trigger_keywords": ["impfung"]},
        ]}
        path = _write_json(tmp_path, data)
        result = find_matching_items(
            path, "facts",
            claim_lc="ein drittel der bahn-passagiere",
            full_claim="Ein Drittel der Bahn-Passagiere",
        )
        ids = sorted(r["id"] for r in result)
        assert ids == ["a", "b"]

    def test_no_match_no_descriptor_skips_backup(self, tmp_path, monkeypatch):
        """Without descriptor_fn the backup-pfad must not be invoked."""
        called = {"count": 0}
        monkeypatch.setattr(
            _topic_match, "_backup_best_matches",
            lambda *a, **k: (called.__setitem__("count", called["count"] + 1) or [])
        )
        data = {"facts": [{"id": "a", "trigger_keywords": ["drittel"]}]}
        path = _write_json(tmp_path, data)
        result = find_matching_items(
            path, "facts",
            claim_lc="der himmel ist blau",
            full_claim="Der Himmel ist blau",
            # descriptor_fn=None default → backup must skip
        )
        assert called["count"] == 0
        assert result == []

    def test_no_match_with_descriptor_invokes_backup(self, tmp_path, monkeypatch):
        """When substring/composite all miss AND descriptor_fn given,
        reranker-backup is invoked with the right args and its result returned."""
        captured = {}

        def fake_backup(claim, pairs, threshold, top_n):
            captured["claim"] = claim
            captured["pairs"] = pairs
            captured["threshold"] = threshold
            captured["top_n"] = top_n
            return [pairs[0][0]]

        monkeypatch.setattr(_topic_match, "_backup_best_matches", fake_backup)

        data = {"facts": [
            {"id": "a", "trigger_keywords": ["foo"]},
            {"id": "b", "trigger_keywords": ["bar"]},
        ]}
        path = _write_json(tmp_path, data)

        result = find_matching_items(
            path, "facts",
            claim_lc="kein match",
            full_claim="Kein Match möglich",
            descriptor_fn=lambda f: (f, f["id"]),
            threshold=0.42,
            top_n=2,
        )

        assert captured["claim"] == "Kein Match möglich"
        assert captured["threshold"] == 0.42
        assert captured["top_n"] == 2
        assert len(captured["pairs"]) == 2
        # Returned list is the items, not the (item, descriptor) tuples
        assert result == [{"id": "a", "trigger_keywords": ["foo"]}]

    def test_substring_match_skips_backup(self, tmp_path, monkeypatch):
        """When substring matches, reranker-backup must NOT be called —
        backup is only the fallback path."""
        called = {"count": 0}

        def fake_backup(*args, **kwargs):
            called["count"] += 1
            return []

        monkeypatch.setattr(_topic_match, "_backup_best_matches", fake_backup)

        data = {"facts": [{"id": "a", "trigger_keywords": ["drittel"]}]}
        path = _write_json(tmp_path, data)

        result = find_matching_items(
            path, "facts",
            claim_lc="ein drittel",
            full_claim="Ein Drittel",
            descriptor_fn=lambda f: (f, f["id"]),
        )
        assert called["count"] == 0
        assert len(result) == 1

    def test_default_threshold_and_top_n(self, tmp_path, monkeypatch):
        """Defaults: threshold=0.45, top_n=3 (from module constants)."""
        captured = {}

        def fake_backup(claim, pairs, threshold, top_n):
            captured["threshold"] = threshold
            captured["top_n"] = top_n
            return []

        monkeypatch.setattr(_topic_match, "_backup_best_matches", fake_backup)

        data = {"facts": [{"id": "a", "trigger_keywords": ["foo"]}]}
        path = _write_json(tmp_path, data)

        find_matching_items(
            path, "facts",
            claim_lc="kein match",
            full_claim="Kein Match",
            descriptor_fn=lambda f: (f, f["id"]),
            # no threshold / top_n given → use defaults
        )
        assert captured["threshold"] == _topic_match.DEFAULT_BACKUP_THRESHOLD
        assert captured["top_n"] == _topic_match.DEFAULT_BACKUP_TOP_N


# ===================================================================
# load_items
# ===================================================================

class TestLoadItems:
    def test_missing_file_returns_empty(self, tmp_path):
        path = str(tmp_path / "missing.json")
        assert load_items(path, "facts") == []

    def test_missing_key_returns_empty(self, tmp_path):
        path = _write_json(tmp_path, {"other_key": []})
        assert load_items(path, "facts") == []

    def test_null_value_returns_empty(self, tmp_path):
        path = _write_json(tmp_path, {"facts": None})
        assert load_items(path, "facts") == []

    def test_returns_items(self, tmp_path):
        data = {"facts": [{"id": "a"}, {"id": "b"}]}
        path = _write_json(tmp_path, data)
        items = load_items(path, "facts")
        assert len(items) == 2
        assert items[0]["id"] == "a"
        assert items[1]["id"] == "b"

    def test_hot_reload_picks_up_changes(self, tmp_path):
        """load_json_mtime_aware re-reads on mtime change — load_items inherits."""
        import time
        path = _write_json(tmp_path, {"facts": [{"id": "v1"}]})
        first = load_items(path, "facts")
        assert first[0]["id"] == "v1"

        # Stand-alone tmp_path-mtime advance: filesystem mtime is at least 1s
        # resolution on some platforms; sleep briefly + write again.
        time.sleep(1.05)
        _write_json(tmp_path, {"facts": [{"id": "v2"}]})
        second = load_items(path, "facts")
        assert second[0]["id"] == "v2"
