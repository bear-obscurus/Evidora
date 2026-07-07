"""Contract-Tests für den Feed-Claim-Relevanz-Filter (Audit 2026-07-07).

Befund: Mimikama und AT-Faktencheck-RSS gaben ihren KOMPLETTEN Feed
(je 20 neueste Items — Eisbärin Tonja, Aldi-Bons, FIFA …) als results
zurück und verließen sich auf den Reranker. Der filtert aber nur den
Synthesizer-Prompt — SSE-Stream, PDF-Export und die "X von Y Quellen
lieferten Ergebnisse"-Zählung zeigten die rohen Feed-Dumps.

Diese Tests pinnen: (a) der Keyword-Fallback des Filters winkt NIE den
ganzen Feed durch, (b) beide Services liefern für themenfremde Claims
0 Ergebnisse, (c) kuratierte Klassiker (authoritative) bleiben erhalten.

Dependency-light: kein Netz, kein sentence-transformers (in CI läuft
gezielt der Keyword-Fallback-Pfad — genau der muss streng sein).
"""

import time

import services.at_faktencheck_rss as at_rss
import services.mimikama as mimikama
from services._factcheck_rss import filter_items_for_claim

# ---------------------------------------------------------------------------
# Canned-Feed: 1 thematisch passendes Item + 4 typische Feed-Dump-Items
# (reale Titel aus den Export-PDFs vom 2026-07-07)
# ---------------------------------------------------------------------------
_FEED = [
    {
        "title": "Faktencheck: Impfung gegen Masern schützt nachweislich",
        "url": "https://example.org/masern-impfung",
        "date": "Mon, 06 Jul 2026 10:00:00 +0000",
        "description": "Die Masern-Impfung bietet laut Studien hohen Schutz.",
        "source": "APA-Faktencheck",
        "country": "AT",
    },
    {
        "title": "Berliner Eisbärin Tonja lebt – Fake-Account erfindet Zoo-Drama",
        "url": "https://example.org/eisbaerin",
        "date": "Mon, 06 Jul 2026 07:22:36 +0000",
        "description": "Ein Fake-Account verbreitet ein erfundenes Zoo-Drama.",
        "source": "Mimikama",
        "country": "DACH",
    },
    {
        "title": "Aldi-Kassenbons 1998/2026: Gefälschter Preisvergleich",
        "url": "https://example.org/aldi-bons",
        "date": "Thu, 02 Jul 2026 12:52:22 +0000",
        "description": "Der virale Preisvergleich ist manipuliert.",
        "source": "Mimikama",
        "country": "DACH",
    },
    {
        "title": "FIFA suspendiert Schiedsrichter nach Spiel Deutschland – Paraguay? Nein.",
        "url": "https://example.org/fifa",
        "date": "Wed, 01 Jul 2026 11:31:42 +0000",
        "description": "Die Behauptung über eine Suspendierung ist frei erfunden.",
        "source": "Mimikama",
        "country": "DACH",
    },
    {
        "title": "Haaland erschrickt nicht wirklich vor seinem Spiegelbild",
        "url": "https://example.org/haaland",
        "date": "Sat, 04 Jul 2026 10:54:08 +0000",
        "description": "Das virale Video ist ein bearbeiteter Werbeclip.",
        "source": "Mimikama",
        "country": "DACH",
    },
]

_MASERN_ANALYSIS = {
    "claim": "Die Masern-Impfung schützt nicht vor Masern",
    "entities": ["Masern"],
    "factcheck_queries": ["Masern Impfung Schutz"],
}

_OFFTOPIC_ANALYSIS = {
    "claim": "Herbert Kickl hat sich als Volkskanzler bezeichnen lassen",
    "entities": ["Herbert Kickl", "Volkskanzler"],
    "factcheck_queries": ["Kickl Volkskanzler Bezeichnung"],
}


def _prime_feed_caches():
    """Beide Feed-Caches mit dem Canned-Feed füllen — kein Netz-Zugriff."""
    mimikama._cache = list(_FEED)
    mimikama._cache_time = time.time()
    at_rss._cache = list(_FEED)
    at_rss._cache_time = time.time()


# ---------------------------------------------------------------------------
# Filter-Helper direkt
# ---------------------------------------------------------------------------
def test_filter_keeps_only_claim_relevant_items():
    kept = filter_items_for_claim(
        _MASERN_ANALYSIS["claim"], _FEED,
        entities=_MASERN_ANALYSIS["entities"],
        queries=_MASERN_ANALYSIS["factcheck_queries"],
    )
    titles = [it["title"] for it in kept]
    assert any("Masern" in t for t in titles), "on-topic Item muss überleben"
    assert all("Eisbärin" not in t and "FIFA" not in t and "Aldi" not in t
               and "Haaland" not in t for t in titles), \
        f"Feed-Dump-Items dürfen nicht durchkommen: {titles}"


def test_filter_returns_empty_for_unrelated_claim():
    kept = filter_items_for_claim(
        _OFFTOPIC_ANALYSIS["claim"], _FEED,
        entities=_OFFTOPIC_ANALYSIS["entities"],
        queries=_OFFTOPIC_ANALYSIS["factcheck_queries"],
    )
    assert kept == [], \
        f"themenfremder Claim darf KEINE Feed-Items durchlassen: {kept}"


def test_filter_never_passes_whole_feed():
    """Regression gegen den Feed-Dump: selbst generische Claims dürfen
    nicht den kompletten Feed zurückbekommen."""
    kept = filter_items_for_claim(
        "Das ist eine ganz allgemeine Behauptung über die Welt", _FEED,
        entities=[], queries=[],
    )
    assert len(kept) < len(_FEED), "ganzer Feed durchgewinkt"


# ---------------------------------------------------------------------------
# Service-Ebene: Mimikama
# ---------------------------------------------------------------------------
async def test_mimikama_no_dump_for_unrelated_claim():
    _prime_feed_caches()
    res = await mimikama.search_mimikama(_OFFTOPIC_ANALYSIS)
    assert res["results"] == [], \
        f"Mimikama darf für themenfremde Claims keinen Feed-Dump liefern: " \
        f"{[r.get('title') for r in res['results']]}"


async def test_mimikama_keeps_matching_item():
    _prime_feed_caches()
    res = await mimikama.search_mimikama(_MASERN_ANALYSIS)
    titles = [r["title"] for r in res["results"]]
    assert any("Masern" in t for t in titles)
    assert all("Eisbärin" not in t for t in titles)


async def test_mimikama_classics_survive_filter():
    """Kuratierte Klassiker sind authoritative und müssen den Filter
    unabhängig vom Live-Feed überleben."""
    _prime_feed_caches()
    res = await mimikama.search_mimikama({
        "claim": "Bill Gates lässt über Impfungen Mikrochips einsetzen",
        "entities": ["Bill Gates"],
        "factcheck_queries": ["Bill Gates Mikrochip Impfung"],
    })
    indicators = [r.get("indicator") for r in res["results"]]
    assert "mimikama_classic" in indicators, \
        "authoritative Klassiker fehlt nach Filter-Einbau"


# ---------------------------------------------------------------------------
# Service-Ebene: AT-Faktencheck-RSS
# ---------------------------------------------------------------------------
async def test_at_rss_no_dump_for_unrelated_claim():
    _prime_feed_caches()
    res = await at_rss.search_at_faktencheck_rss(_OFFTOPIC_ANALYSIS)
    assert res["results"] == [], \
        f"AT-Faktencheck-RSS darf keinen Feed-Dump liefern: " \
        f"{[r.get('title') for r in res['results']]}"


async def test_at_rss_keeps_matching_item():
    _prime_feed_caches()
    res = await at_rss.search_at_faktencheck_rss(_MASERN_ANALYSIS)
    titles = [r["title"] for r in res["results"]]
    assert any("Masern" in t for t in titles)


async def test_at_rss_classics_survive_filter():
    _prime_feed_caches()
    res = await at_rss.search_at_faktencheck_rss({
        "claim": "Die Regierung plant heimlich die Bargeldabschaffung",
        "entities": [],
        "factcheck_queries": ["Bargeldabschaffung Österreich"],
    })
    indicators = [r.get("indicator") for r in res["results"]]
    assert "at_faktencheck_classic" in indicators, \
        "authoritative AT-Klassiker fehlt nach Filter-Einbau"
