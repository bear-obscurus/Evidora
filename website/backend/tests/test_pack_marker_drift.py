"""Contract-Test: Pack-/Konsens-Source-Namen vs. AUTHORITATIVE_PACK_MARKERS
(Marker-Drift-Klasse aus #73 — der dort angekündigte Follow-up).

Befund #73: Drei kuratierte Static-First-Packs (geschichte_pack,
verschwoerungen_pack, at_courts) matchten die Marker NIE, weil ihre realen
Top-Level-source-Namen von den Markern abwichen — sie bekamen still die
STRENGEN statt der Pack-Caps und ihre Verdict-Direktiven konnten keinen
Floor auslösen. Die Bestandsaufnahme 2026-07-09 fand zwei weitere Fälle:
esoterik_pack (Marker "Esoterik-Pack" vs. realer Name "Esoterik-Faktencheck
(GWUP + …)", gedriftet seit Einführung der Marker-Liste 2026-05-03) und
dach_factbook ("DACH Factbook" fehlte komplett — 9 kuratierte Verdict-
Direktiven im JSON waren tot; #63 hatte at_factbook als "einziges fehlendes
kuratiertes Pack" ergänzt und die Schwester übersehen).

Dieser Test pinnt BEIDE Namensräume programmatisch gegen die Marker:
  A) Service-Top-Level-"source" (Import + Empty-Aufruf der search_*-Fn)
     → speist extract_pack_directive_floor (Direktiven-Floor).
  B) main.py-Dispatch-Labels (queried_names → hit_names → sources_used)
     → speist calibrate_confidence (Pack- vs. Streng-Caps).

Muster: test_st_model_revision.py (Drift-Pin Code vs. Config).
Dependency-light: Import + Empty-Aufruf (kein Netz — Static-First-Services
returnen bei leerem Claim vor jedem I/O), Regex auf main.py, JSON-Textscan.
"""

import asyncio
import glob
import importlib
import inspect
import json
import os
import re

import pytest

from services.confidence_calibration import (
    _DIRECTIVE_RE,
    _has_authoritative_pack,
)

BACKEND_DIR = os.path.join(os.path.dirname(__file__), "..")
SERVICES_DIR = os.path.join(BACKEND_DIR, "services")

# Kuratierte Konsens-Services AUSSERHALB der *_pack.py-Namenskonvention,
# die authoritative sein MÜSSEN (Marker-Match in beiden Namensräumen):
EXTRA_AUTHORITATIVE = (
    "at_courts",              # VfGH + VwGH Schlüsselerkenntnisse (#73)
    "at_factbook",            # AT Factbook (#63)
    "dach_factbook",          # DACH Factbook (Drift-Fix 2026-07-09)
    "destatis",               # DESTATIS — Statistisches Bundesamt
    "eurobarometer",          # Eurobarometer (EU-Kommission + EP)
    "internationale_quellen", # Internationale Quellen (Pew + WMO + …)
    # Drift-Fix 2026-07-09: amtliche RTR/KommAustria-Daten mit kuratierter
    # false@0.85-Kernsatz-Direktive, die ohne Marker tot war — Beförderung
    # nach dem at_factbook-Präzedenzfall (#63).
    "medientransparenz",
)

# Static-First-Services, die BEWUSST NICHT authoritative sind: Indikator-/
# Report-Services (einzelne Institution, Zahlen/Berichte statt kuratiertem
# Multi-Quellen-Verdict-Konsens). Sie standen in keiner Version der
# Marker-Liste und tragen KEINE Verdict-Direktiven in ihren JSONs
# (test_not_authoritative_services_have_no_dead_directives pinnt das —
# eine Direktive in so einem Service wäre tot, exakt die dach_factbook-
# Falle). Wer einen davon zum authoritative Pack befördern will: Marker
# in confidence_calibration.py ergänzen UND den Eintrag hier entfernen.
INTENTIONALLY_NOT_AUTHORITATIVE = (
    "ams_wifo",                 # AMS/WIFO Arbeitsmarkt-Berichte
    "awmf",                     # AWMF Leitlinienregister
    "constitute",               # Verfassungs-Volltext-Index (Live-API)
    "education_dach",           # TIMSS/PIRLS/PISA-Indikatoren
    "energy_charts",            # Fraunhofer Energy-Charts + APG
    "eter",                     # European Tertiary Education Register
    "eu_courts",                # EuGH+EGMR Schlüsselurteile (Zitate, keine Direktiven)
    "eu_crime",                 # Eurostat Crime + DACH PKS
    "housing_at",               # OeNB + EU-SILC Wohnen-Indikatoren
    "iqs_bildung",              # IQS Nationaler Bildungsbericht
    "oecd_health",              # OECD Health Indikatoren
    "oeif_zara",                # ÖIF/ZARA Berichte
    "parlgov",                  # ParlGov Wahl-/Kabinettsdaten-Index
    "rechnungshof_parteienfin", # RH Parteienfinanzierungs-Daten
    "rki_surveillance",         # RKI SurvStat Meldedaten
    "transport_at",             # ÖBB + UBA + KlimaTicket Indikatoren
    "who_hearing",              # WHO World Report on Hearing
)

ALL_PACK_MODULES = tuple(sorted(
    os.path.basename(p)[:-3]
    for p in glob.glob(os.path.join(SERVICES_DIR, "*_pack.py"))
))
EXPECTED_AUTHORITATIVE = ALL_PACK_MODULES + EXTRA_AUTHORITATIVE

_EMPTY_ANALYSIS = {"original_claim": "", "claim": "", "keywords": [],
                   "entities": []}


def _static_first_modules():
    """Universum des Kontrakts: alle Service-Module mit echtem
    _topic_match-Import (Static-First-Pattern, ARCHITECTURE §3.5).
    Die Factbooks (eigene Matching-Logik) sind über EXTRA_AUTHORITATIVE
    zusätzlich abgedeckt."""
    mods = []
    for path in sorted(glob.glob(os.path.join(SERVICES_DIR, "*.py"))):
        base = os.path.basename(path)[:-3]
        if base.startswith("_"):
            continue
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
        if re.search(r"^from services\._topic_match import", text, re.M):
            mods.append(base)
    return mods


def _search_fns(mod_name):
    mod = importlib.import_module(f"services.{mod_name}")
    return [
        (name, fn) for name, fn in sorted(vars(mod).items())
        if name.startswith("search_")
        and inspect.iscoroutinefunction(fn)
        and fn.__module__ == mod.__name__
    ]


def _top_level_sources(mod_name):
    """Reale Top-Level-source-Namen via Empty-Aufruf: bei leerem Claim
    liefert jeder Static-First-Service sein empty-Dict (kein I/O) mit
    demselben source-Literal wie der Treffer-Pfad."""
    sources = []
    for name, fn in _search_fns(mod_name):
        res = asyncio.run(fn(dict(_EMPTY_ANALYSIS)))
        assert isinstance(res, dict) and "source" in res, (
            f"{mod_name}.{name}: Empty-Aufruf lieferte kein Dict mit "
            f"Top-Level-'source' — Kontrakt-Voraussetzung verletzt"
        )
        sources.append(res["source"])
    return sources


def _dispatch_pairs():
    """(search_fn_name, label)-Paare aus dem main.py-Dispatch-Block.
    Jedes queried_names.append("Label") gehört zum tasks.append(…)
    unmittelbar davor — wir suchen die letzte search_*-Referenz im
    Abschnitt seit dem vorigen Label (robust gegen mehrzeilige
    tasks.append-Aufrufe)."""
    with open(os.path.join(BACKEND_DIR, "main.py"), encoding="utf-8") as fh:
        text = fh.read()
    pairs = []
    prev_end = 0
    for m in re.finditer(r'queried_names\.append\(\s*"([^"]+)"\s*\)', text):
        window = text[prev_end:m.start()]
        fns = re.findall(r"\b(search_\w+)\b", window)
        pairs.append((fns[-1] if fns else None, m.group(1)))
        prev_end = m.end()
    return pairs


# ---------------------------------------------------------------------------
# Namensraum A: Service-Top-Level-source (Direktiven-Floor-Pfad)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("mod_name", EXPECTED_AUTHORITATIVE)
def test_authoritative_service_source_matches_markers(mod_name):
    """Der reale Top-Level-source-Name jedes kuratierten Packs muss die
    Marker matchen — sonst bekommt es still die strengen Caps und seine
    Verdict-Direktiven sind tot (#73-Klasse)."""
    fns = _search_fns(mod_name)
    assert fns, f"{mod_name}: keine search_*-Coroutine gefunden"
    for src in _top_level_sources(mod_name):
        assert _has_authoritative_pack([src]), (
            f"MARKER-DRIFT: {mod_name} sendet source={src!r}, aber kein "
            f"AUTHORITATIVE_PACK_MARKERS-Eintrag matcht. Entweder Marker "
            f"ergänzen (confidence_calibration.py) oder den Service in "
            f"INTENTIONALLY_NOT_AUTHORITATIVE dokumentieren."
        )


@pytest.mark.parametrize("mod_name", EXPECTED_AUTHORITATIVE)
def test_authoritative_hit_path_source_matches_markers(mod_name):
    """Auch der TREFFER-Pfad muss das Marker-matchende source-Literal
    senden — Empty- und Treffer-Return können unterschiedliche Literale
    tragen (parlgov sendet z.B. 'ParlGov' leer, aber 'ParlGov (Univ.
    Bremen …)' bei Treffern). Aufruf mit einer echten claim_phrasing aus
    dem Pack-JSON; Module ohne Standard-facts-Struktur (Factbooks) sind
    über den Empty-Pfad-Test + ihre eigenen Suiten abgedeckt."""
    mod = importlib.import_module(f"services.{mod_name}")
    path = getattr(mod, "STATIC_JSON_PATH", None)
    if not path or not os.path.exists(path):
        pytest.skip(f"{mod_name}: kein STATIC_JSON_PATH")
    with open(path, encoding="utf-8") as fh:
        facts = json.load(fh).get("facts", [])
    phrasing = next(
        (ph for f in facts for ph in (f.get("claim_phrasings_handled") or [])),
        None,
    )
    if phrasing is None:
        pytest.skip(f"{mod_name}: keine claim_phrasings_handled im JSON")

    name, fn = _search_fns(mod_name)[0]
    res = asyncio.run(fn({"original_claim": phrasing, "claim": phrasing,
                          "keywords": [], "entities": []}))
    assert res.get("results"), (
        f"{mod_name}.{name}: Phrasing {phrasing!r} lieferte keine Treffer "
        f"— Trigger-Kontrakt verletzt (siehe test_konsens_exact_only)"
    )
    src = res["source"]
    assert _has_authoritative_pack([src]), (
        f"MARKER-DRIFT im Treffer-Pfad: {mod_name} sendet bei Treffern "
        f"source={src!r} ohne Marker-Match — Pack-Caps und Direktiven-"
        f"Floor feuern für echte Treffer nie."
    )


# ---------------------------------------------------------------------------
# Namensraum B: main.py-Dispatch-Labels (Pack-Caps-Pfad)
# ---------------------------------------------------------------------------

def test_dispatch_labels_consistent_with_service_auth():
    """Für jedes kuratierte Pack muss AUCH das main.py-Dispatch-Label die
    Marker matchen (Label → hit_names → sources_used → Pack-Caps); und
    umgekehrt darf KEIN fremdes Label versehentlich matchen (generische
    Marker wie 'Geographie' oder 'DESTATIS' könnten sonst beliebigen
    neuen Quellen unverdiente Pack-Caps schenken)."""
    fn_owner = {}
    for mod_name in sorted(set(_static_first_modules())
                           | set(EXPECTED_AUTHORITATIVE)):
        for name, _fn in _search_fns(mod_name):
            fn_owner[name] = mod_name

    expected = set(EXPECTED_AUTHORITATIVE)
    pairs = _dispatch_pairs()
    assert pairs, "main.py: kein queried_names.append-Dispatch gefunden"

    drift = []
    false_pos = []
    seen_expected = set()
    for fn, label in pairs:
        mod = fn_owner.get(fn or "")
        label_auth = _has_authoritative_pack([label])
        if mod in expected:
            seen_expected.add(mod)
            if not label_auth:
                drift.append((mod, label))
        elif label_auth:
            false_pos.append((fn, label))

    assert not drift, (
        f"DISPATCH-LABEL-DRIFT (Pack-Caps feuern nie, weil sources_used "
        f"die main.py-Labels trägt): {drift}"
    )
    assert not false_pos, (
        f"Marker matcht fremdes Dispatch-Label — unverdiente Pack-Caps "
        f"für Nicht-Pack-Quellen: {false_pos}"
    )
    missing = expected - seen_expected
    assert not missing, (
        f"Kuratierte Packs ohne main.py-Dispatch-Eintrag (nie abgefragt "
        f"oder Paarungs-Regex greift nicht mehr): {sorted(missing)}"
    )


# ---------------------------------------------------------------------------
# Universum-Vollständigkeit + Ehrlichkeit der Nicht-Auth-Doku
# ---------------------------------------------------------------------------

def test_static_first_universe_fully_classified():
    """Jeder Static-First-Service ist explizit klassifiziert: *_pack.py
    und EXTRA_AUTHORITATIVE → Marker-Pflicht; INTENTIONALLY_NOT_
    AUTHORITATIVE → dokumentiert. Ein neuer Static-First-Service ohne
    Klassifikation lässt diesen Test scheitern — die Entscheidung
    authoritative ja/nein muss BEWUSST fallen, nicht per Drift."""
    universe = [m for m in _static_first_modules() if _search_fns(m)]
    assert len(universe) >= 50, (
        f"Universum unplausibel klein ({len(universe)}) — "
        f"_static_first_modules()-Erkennung defekt?"
    )
    unclassified = [
        m for m in universe
        if m not in EXPECTED_AUTHORITATIVE
        and m not in INTENTIONALLY_NOT_AUTHORITATIVE
    ]
    assert not unclassified, (
        f"Unklassifizierte Static-First-Services: {unclassified} — "
        f"entweder Marker ergänzen + (falls kein *_pack.py-Name) in "
        f"EXTRA_AUTHORITATIVE eintragen, oder mit Begründung in "
        f"INTENTIONALLY_NOT_AUTHORITATIVE dokumentieren."
    )


def test_not_authoritative_entries_are_current():
    """Die Nicht-Auth-Liste bleibt ehrlich: Einträge müssen existieren,
    dürfen nicht gleichzeitig als authoritative erwartet werden und
    dürfen die Marker NICHT matchen (sonst ist der Eintrag veraltet)."""
    overlap = set(INTENTIONALLY_NOT_AUTHORITATIVE) & set(EXPECTED_AUTHORITATIVE)
    assert not overlap, f"Widersprüchlich klassifiziert: {sorted(overlap)}"
    for mod_name in INTENTIONALLY_NOT_AUTHORITATIVE:
        for src in _top_level_sources(mod_name):
            assert not _has_authoritative_pack([src]), (
                f"{mod_name} (source={src!r}) matcht inzwischen die "
                f"Marker — Eintrag aus INTENTIONALLY_NOT_AUTHORITATIVE "
                f"entfernen, sonst dokumentiert die Liste Falsches."
            )


def test_not_authoritative_services_have_no_dead_directives():
    """Die dach_factbook-Falle: Verdict-Direktiven ("Verdict false bei
    0.95 Konfidenz") in einem NICHT-authoritative Service sind TOT —
    extract_pack_directive_floor prüft _has_authoritative_pack auf den
    source-Namen. Wer eine Direktive in so ein JSON schreibt, muss den
    Service zum Pack befördern (Marker + EXTRA_AUTHORITATIVE) oder die
    Direktive weglassen."""
    for mod_name in INTENTIONALLY_NOT_AUTHORITATIVE:
        mod = importlib.import_module(f"services.{mod_name}")
        path = getattr(mod, "STATIC_JSON_PATH", None)
        if not path or not os.path.exists(path):
            continue
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
        dead = [
            m.group(0) for m in _DIRECTIVE_RE.finditer(text)
            # Floor-Gate verlangt 'verdict' im Feldtext — Direktiven
            # schreiben das Label direkt daneben ("Verdict false bei …")
            if "verdict" in text[max(0, m.start() - 80):m.start()].lower()
        ]
        assert not dead, (
            f"{mod_name}: Verdict-Direktive(n) im JSON eines NICHT-"
            f"authoritative Service — sie können nie einen Floor "
            f"auslösen (tote Direktiven, dach_factbook-Falle): {dead[:3]}"
        )


def test_no_dead_markers():
    """Jeder Marker muss mindestens einen realen Namen matchen (Service-
    source oder Dispatch-Label). Tote Marker sind wartende Drifts:
    'Esoterik-Pack' zeigte seit 2026-05-03 auf nichts, während
    esoterik_pack still die strengen Caps bekam."""
    from services.confidence_calibration import AUTHORITATIVE_PACK_MARKERS

    names = [label for _fn, label in _dispatch_pairs()]
    for mod_name in EXPECTED_AUTHORITATIVE:
        names.extend(_top_level_sources(mod_name))

    dead = [
        m for m in AUTHORITATIVE_PACK_MARKERS
        if not any(m.lower() in n.lower() for n in names)
    ]
    assert not dead, (
        f"Tote Marker (matchen keinen Service-source und kein Dispatch-"
        f"Label): {dead} — Tippfehler/Umbenennung? Marker an den realen "
        f"Namen angleichen oder entfernen."
    )
