"""Trigger sollen an Schreibweisen nicht scheitern.

Befund (2026-09-06, gemessen über 85 Services und 2.656 dokumentierte
`claim_phrasings_handled`): **371 (14 %) trafen nicht mehr**, sobald man
dieselbe Aussage in einer anderen, völlig üblichen Schreibweise eingibt.

    Bindestrich → Leerzeichen    75 % Trefferquote   198 Ausfälle
    Umlaut → ae/oe/ue            88 %                127 Ausfälle
    Leerzeichen → Bindestrich    92 %                 66 Ausfälle

Echte Fälle aus der Messung: „Oesterreich", „Linkshaender", „kuerzere
Lebenserwartung", „Massen Ueberwachung", „Soros NGOs", „SGLT2-Hemmer". Für
einen österreichischen Dienst ist die ASCII-Umschrift keine Randerscheinung.

**Vorher zuvor mechanisch geprüft und VERWORFEN**: die Hypothese, die Trigger
seien gegen Umformulierung brüchig. Frageform, Negation und Füllwörter
treffen zu 100 % — Substring-Matching stört sich nicht an zusätzlichen
Wörtern. Die 6 scheinbaren Flexions-Ausfälle waren grammatisch falsches
Deutsch („Die Chinesischen Mauer"), also Generator-Unsinn. Dort lohnt keine
Arbeit; das Problem sind Schreibweisen, nicht Grammatik.

Bisher wurde das von Hand aufgezählt, wo jemand daran dachte — `"fpö",
"fpoe"`, `"spö", "spoe"`, `"geldwäsche", "geldwaesche"`, 32 solcher
Doppel-Einträge allein in `_topic_match.py`. Dasselbe Muster wie die
aufgezählten Flexionsformen bei Frontex (#141): Wer Varianten aufzählt,
vergisst welche.

**Der Fehler beim Bauen**, den der Über-Trigger-Sweep gefangen hat: Die erste
Fassung von `normalisiere()` rief `.strip()` auf. Der Trigger `" eter "` trägt
seine Leerzeichen aber als WORTGRENZEN-SCHUTZ für das Kürzel ETER — ohne sie
steckt „eter" in „KilomETER", und ein Claim über Autobahn-Kilometer feuerte
den Hochschul-Datensatz. Rand-Leerzeichen bleiben deshalb erhalten.
"""

import json
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._schreibweise import enthaelt, normalisiere  # noqa: E402
from services._topic_match import substring_or_composite_match  # noqa: E402


# --------------------------------------------------------------------------
# Die Normalisierung selbst
# --------------------------------------------------------------------------

def test_umlaute_werden_gefaltet():
    assert normalisiere("Österreich") == "oesterreich"
    assert normalisiere("Linkshänder") == "linkshaender"
    assert normalisiere("Straße") == "strasse"
    assert normalisiere("Über") == "ueber"


def test_trennzeichen_werden_vereinheitlicht():
    for variante in ("Brustkrebs-Früherkennung", "Brustkrebs Früherkennung",
                     "Brustkrebs–Früherkennung", "Brustkrebs_Früherkennung"):
        assert normalisiere(variante) == "brustkrebs frueherkennung", variante


def test_mehrfache_leerzeichen_werden_zusammengefasst():
    assert normalisiere("BIP   Anteil") == "bip anteil"
    assert normalisiere("BIP - Anteil") == "bip anteil"


def test_rand_leerzeichen_bleiben_erhalten():
    """Der Fehler, den der Über-Trigger-Sweep gefangen hat: `" eter "` schützt
    das Kürzel ETER vor Treffern in „KilomETER". Ein strip() macht daraus
    `"eter"` — und ein Claim über Autobahn-Kilometer feuert den
    Hochschul-Datensatz."""
    assert normalisiere(" ETER ") == " eter "
    assert not enthaelt("2.000 Kilometer Autobahnen", " eter ")
    assert enthaelt("Die ETER Datenbank", " eter ")


def test_normalisierung_ist_idempotent():
    for t in ("Österreich", "Brustkrebs-Früherkennung", " ETER ", ""):
        assert normalisiere(normalisiere(t)) == normalisiere(t), t


def test_leerer_text_faellt_weich():
    assert normalisiere("") == "" and normalisiere(None or "") == ""
    assert not enthaelt("irgendein Claim", "")


# --------------------------------------------------------------------------
# Die konkreten Fälle aus der Messung
# --------------------------------------------------------------------------

def test_die_gemessenen_ausfaelle_treffen_jetzt():
    for claim, trigger in (
        # Umlaut-ASCII — der häufigste Fall bei österreichischen Nutzern
        ("Foerderungen Oesterreich sind gestiegen", "förderungen österreich"),
        ("Linkshaender leben kuerzer", "linkshänder"),
        ("Das Spike-Protein bleibt im Koerper", "körper"),
        ("Drogen Delikte Oesterreich", "österreich"),
        # Bindestrich gegen Leerzeichen
        ("Massen Überwachung in Österreich", "massen-überwachung"),
        ("Soros NGOs steuern die Politik", "soros-ngos"),
        ("BIP Anteil Gesundheit", "bip-anteil"),
        ("Brustkrebs Früherkennung Österreich", "brustkrebs-früherkennung"),
        # und die Gegenrichtung
        ("Massen-Überwachung", "massen überwachung"),
    ):
        assert enthaelt(claim, trigger), f"{claim!r} enthält {trigger!r} nicht"


def test_beide_seiten_werden_normalisiert():
    """Nicht nur der Claim — sonst scheitert ein Trigger mit Umlaut an einem
    ASCII-Claim und umgekehrt."""
    assert enthaelt("oesterreich", "Österreich")
    assert enthaelt("Österreich", "oesterreich")


# --------------------------------------------------------------------------
# Im gemeinsamen Matcher
# --------------------------------------------------------------------------

def test_matcher_nutzt_die_normalisierung():
    item = {"trigger_keywords": ["brustkrebs-früherkennung"]}
    assert substring_or_composite_match(item, "brustkrebs frueherkennung wien")
    assert substring_or_composite_match(item, "Brustkrebs-Früherkennung".lower())


def test_composite_nutzt_die_normalisierung():
    item = {"trigger_composite": [["österreich"], ["förderung"]]}
    assert substring_or_composite_match(item, "foerderungen in oesterreich")
    assert not substring_or_composite_match(item, "foerderungen in deutschland")


def test_matcher_triggert_weiterhin_nicht_ueber():
    """Die Normalisierung führt Zeichenketten zusammen — sie darf keine
    neuen Treffer erzeugen."""
    item = {"trigger_keywords": [" eter "]}
    assert not substring_or_composite_match(
        item, "in österreich gibt es 2.000 kilometer autobahnen")


# --------------------------------------------------------------------------
# Die Messung als Vertrag
# --------------------------------------------------------------------------

def _phrasings_mit_variante(wandler):
    """Alle dokumentierten Phrasings, die ihren eigenen Fakt treffen, in der
    gewandelten Schreibweise erneut geprüft."""
    ok = fehl = 0
    for pfad in sorted((BACKEND / "data").glob("*.json")):
        try:
            d = json.loads(pfad.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(d, dict):
            continue
        for f in (d.get("facts") or d.get("items") or d.get("entries") or []):
            if not isinstance(f, dict):
                continue
            for p in (f.get("claim_phrasings_handled") or []):
                if not substring_or_composite_match(f, p.lower()):
                    continue
                var = wandler(p)
                if var == p:
                    continue
                if substring_or_composite_match(f, var.lower()):
                    ok += 1
                else:
                    fehl += 1
    return ok, fehl


UML = str.maketrans({"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss",
                     "Ä": "Ae", "Ö": "Oe", "Ü": "Ue"})


def test_umlaut_umschrift_trifft_praktisch_immer():
    """Vor der Normalisierung: 88 %. Der Vertrag hält den Gewinn fest."""
    ok, fehl = _phrasings_mit_variante(lambda p: p.translate(UML))
    quote = ok / (ok + fehl) if ok + fehl else 1.0
    assert quote >= 0.95, f"nur {quote:.0%} ({fehl} Ausfälle von {ok+fehl})"


def test_bindestrich_variante_trifft_praktisch_immer():
    """Vor der Normalisierung: 75 %."""
    ok, fehl = _phrasings_mit_variante(lambda p: p.replace("-", " "))
    quote = ok / (ok + fehl) if ok + fehl else 1.0
    assert quote >= 0.95, f"nur {quote:.0%} ({fehl} Ausfälle von {ok+fehl})"


def test_keine_doppel_aufzaehlung_mehr_noetig():
    """`"fpö", "fpoe"` nebeneinander ist seit der Normalisierung redundant.
    Der Test verbietet sie nicht — Bestand darf bleiben —, hält aber fest,
    dass beide Schreibweisen ohne die Aufzählung treffen."""
    for umlaut, ascii_ in (("fpö", "fpoe"), ("spö", "spoe"),
                           ("geldwäsche", "geldwaesche")):
        assert normalisiere(umlaut) == normalisiere(ascii_), (umlaut, ascii_)


# --------------------------------------------------------------------------
# Services mit EIGENEM Praedikat (laufen nicht ueber _topic_match)
# --------------------------------------------------------------------------

def test_eigene_praedikate_verstehen_ascii_umschrift():
    """at_factbook, dach_factbook und pks haben eigene Trigger-Logik. Live
    nachgewiesen: „Die Foerderungen in Oesterreich sind stark gestiegen"
    lieferte `unverifiable@0.1` — „Förderungen … Österreich" traf."""
    from services.at_factbook import claim_mentions_factbook_cached
    from services.dach_factbook import claim_mentions_dach_factbook_cached
    from services.pks import claim_mentions_pks_cached

    for praedikat, paar in (
        (claim_mentions_factbook_cached,
         ("Die Förderungen in Österreich sind stark gestiegen",
          "Die Foerderungen in Oesterreich sind stark gestiegen")),
        (claim_mentions_factbook_cached,
         ("Pensionserhöhung Österreich 2026", "Pensionserhoehung Oesterreich 2026")),
        (claim_mentions_pks_cached,
         ("Jugendkriminalität in Österreich steigt",
          "Jugendkriminalitaet in Oesterreich steigt")),
        (claim_mentions_dach_factbook_cached,
         ("Bürgergeld in Deutschland", "Buergergeld in Deutschland")),
    ):
        mit_umlaut, ascii_form = paar
        assert praedikat(mit_umlaut), mit_umlaut
        assert praedikat(ascii_form), ascii_form


def test_modulgrenzen_normalisieren_defensiv():
    """Interne Praedikate bekommen von aussen (Tests, main.py) nur einen
    kleingeschriebenen Claim. Weil ihre Terme normalisiert sind, muessen sie
    die Eingabe selbst nachziehen — sonst brach genau das die
    Staatsbuergerschafts-Erkennung."""
    from services.at_factbook import _claim_mentions_citizenship, _has_at_context
    assert _claim_mentions_citizenship("wie viele ukrainer leben in österreich?")
    assert _claim_mentions_citizenship("wie viele ukrainer leben in oesterreich?")
    assert _has_at_context("in österreich") and _has_at_context("in oesterreich")


def test_terme_werden_einmalig_normalisiert():
    """Die Listen stehen bewusst mit Umlauten im Quelltext — lesbar —, liegen
    zur Laufzeit aber normalisiert vor."""
    from services.at_factbook import _AT_CONTEXT_TERMS
    assert all(t == normalisiere(t) for t in _AT_CONTEXT_TERMS)
    assert any("oesterreich" in t for t in _AT_CONTEXT_TERMS)


def test_wortgrenzen_schutz_ueberlebt_auch_hier():
    """Terme wie `"wien "` tragen ihre Rand-Leerzeichen als Schutz."""
    from services.at_factbook import _WIEN_TERMS
    assert any(t.startswith(" ") or t.endswith(" ") for t in _WIEN_TERMS)
