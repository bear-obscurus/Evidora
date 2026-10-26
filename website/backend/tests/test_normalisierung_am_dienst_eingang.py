"""Der Dienst-Eingang warf den Claim weg, den der Trigger gekannt hätte.

Anlass war Punkt 4 der offenen Liste: zwei WCAG-Fragen liefern 0 Treffer.

    "Die Richtlinie verlangt Konformitätsstufe AA"   → 0 Rows
    "Wie viele WCAG-Erfolgskriterien gibt es?"       → 0 Rows

## Befund A — zwei Normalisierungs-Stände in einem Dienst

Die Trigger-Vokabeln laufen bei der Definition durch ``norm_terme`` und
stehen damit umlaut-gefaltet und ohne Bindestriche da
(``konformitaetsstufe aa``). ``search_wcag22`` übergab dem Prädikat aber
nur ``combined.lower()`` — ohne ``normalisiere``. Der Claim behielt sein
"ä", das Token hatte keins, und das Gate sagte Nein.

Der öffentliche Wrapper ``claim_mentions_wcag22_cached`` normalisiert; der
Dienst-Eingang nicht. Deshalb sah meine erste Messung einen Trigger-Treffer
und der Dienst lieferte trotzdem nichts.

**Dieselbe Ursache, 13 Dienste.** Erst die Kandidaten per AST gesucht (167
Aufrufe eines Trigger-Prädikats geprüft, 16 ohne Normalisierung, davon 3
Token-Schleifen in ``_topic_match``), dann je Dienst eine selbsterzeugende
Probe: Für jedes Vokabel, dessen Schreibweise die Normalisierung verändert,
ein Satz in der ORIGINAL-Schreibweise, einmal roh und einmal normalisiert
durch das Prädikat.

    110 von 256 betroffenen Vokabeln erkannte das Prädikat bei rohem Claim
    nicht

⚠️ **Eine eigene Fehlmessung dazwischen:** Danach habe ich "0 Abweichungen"
gemeldet — gemessen hatte ich aber den normalisierten Pfad gegen den
normalisierten Pfad, also eine Tautologie. Das Prädikat verlangt weiterhin
einen gefalteten Claim, und das ist Absicht (wie ``_flexion.trifft``).
Behoben ist der EINGANG: Jeder Gate-Aufruf geht jetzt durch
``normalisiere``. Geprüft wird das unten per Zensus über alle Dienste — mit
einer Probe, die den Fix testweise zurücknimmt und sich selbst findet.

Betroffen waren aiid, eba, era5, global_carbon_budget, mitre_attack, noaa,
nvd, observatory, osv, wcag22, webaim, wid, wjp_rol — alle dreizehn
gepatcht. Dazu ``_claim_mentions_eba_lru``: ein LRU-Wrapper, den niemand
aufruft und der seinen Claim ungefaltet weitergab; er normalisiert jetzt
wie sein Gegenstück in era5, damit er für den ersten Caller keine Falle
ist.

Es ist die Fortsetzung von #143/#144 und #147/#152 — dort wurden die
VOKABELN gefaltet, hier fehlte die Faltung auf der CLAIM-Seite am Eingang.

## Befund B — der Fix hat eine alte Schwäche sichtbar gemacht

Über 12.155 echte Claims gemessen, gewann ERA5 neun Treffer — sieben echte
Temperatur-Rekord-Claims ("2024 war global das wärmste Jahr", "der
kälteste Oktober seit 17 Jahren") und **zwei Fehltreffer**:

    "… die allerhöchsten Rentenerhöhungen der letzten 20 Jahre"
    "… Drosten … Wirkung von Masken …"   (höchster + Tag)

Ursache ist das Composite ``Rekord-Verb + Wetter-Substantiv``: "höchste"
ist ein allgemeiner Superlativ, "Jahr" und "Tag" sind Zeit-Einheiten, kein
Wetter. Die Kombination war schon vorher falsch — nur unerreichbar, weil
"höchste" einen Umlaut trägt.

Verengt: Entweder benennt das Rekord-Wort selbst das Wetter ("wärmste
Jahr"), oder das Substantiv tut es ("Rekord-Hitze"). Gemessen:

    vorher  11 Treffer, davon 3 Fehltreffer
    nachher 15 Treffer, davon 0 Fehltreffer, 0 verloren

Die fünf von der Verengung entfernten Claims sind alle keine Wetter-Claims
(Renten, Masken, "antichristliche Übergriffe", Kontaktsperren,
"Rekord-Todesfälle unter Piloten").

## Befund C — die Zählfrage, die in #87 eine Zahl erfunden hat

"Wie viele WCAG-Erfolgskriterien gibt es?" scheiterte an zwei Dingen:

1. Nur die VERSIONIERTEN Formen waren Trigger ("wcag 2.2", "wcag22") —
   bare "wcag" nicht.
2. ``erfolgskriterium`` trifft den Plural ``Erfolgskriterien`` nicht: Die
   Endung WECHSELT, sie wächst nicht hinten an. Jetzt steht der Stamm
   ``erfolgskriteri`` da (#140/#141).

Genau diese Frage hatte in #87 die erfundene Zahl "62" bekommen. Der
Renderer zählt seit damals richtig — aber ohne Treffer sieht das Modell die
Zahl gar nicht, und die Erfindungs-Falle stand strukturell offen.

Über 12.155 Claims gemessen: "wcag" 0 Treffer, "erfolgskriterien" 0,
"erfolgskriterium" 0. Kein Über-Trigger-Risiko. Die Begründung für das
Hinzufügen ist hier nicht die Korpus-Häufigkeit, sondern der dokumentierte
Vorfall aus #87.

Dependency-light: Dienste und Vokabeln, kein Netz.
"""
import ast
import os
import pathlib
import re
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

from services._schreibweise import normalisiere  # noqa: E402

#: Dienste, deren Trigger-Vokabeln am 26.10.2026 am Eingang verloren gingen.
#: Die Zahl je Dienst ist die gemessene Menge betroffener Vokabeln (Terme,
#: deren Schreibweise ``normalisiere`` veraendert) — zusammen 110 von 256,
#: die das Praedikat roh NICHT erkannte.
def _quelle(name: str) -> str:
    with open(os.path.join(BACKEND, "services", f"{name}.py"),
              encoding="utf-8") as fh:
        return fh.read()


BETROFFEN_AM_26_10_2026 = (
    "aiid", "eba", "era5", "global_carbon_budget", "mitre_attack", "noaa",
    "nvd", "observatory", "osv", "wcag22", "webaim", "wid", "wjp_rol",
)


def _gate_aufrufe_ohne_normalisierung() -> list[str]:
    """Zensus ueber ALLE Dienste mit eigenem Trigger-Vokabular.

    Keine Hand-Liste und keine Ausnahmen (#230): Gesucht wird jeder Aufruf
    eines Trigger-Praedikats, dessen Argument nicht durch ``normalisiere``
    gegangen ist — Variablen werden dabei bis zu ihrer Zuweisung in
    derselben Funktion aufgeloest, weil ``matchable = normalisiere(...)``
    sonst als Befund gezaehlt wuerde. Praedikate, die selbst normalisieren,
    sind ausgenommen.
    """
    befunde: list[str] = []
    for pfad in sorted(pathlib.Path(BACKEND, "services").glob("*.py")):
        quelle = pfad.read_text(encoding="utf-8")
        if "norm_terme" not in quelle:
            continue
        try:
            baum = ast.parse(quelle)
        except SyntaxError:          # pragma: no cover
            continue
        fns = {n.name: n for n in ast.walk(baum)
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
        praedikate = {k: v for k, v in fns.items()
                      if re.search(r"_claim_mentions|_has_\w*keyword", k)}
        for name, fn in fns.items():
            lokal = {}
            for n in ast.walk(fn):
                if isinstance(n, ast.Assign):
                    rechts = ast.unparse(n.value)
                    for ziel in n.targets:
                        if isinstance(ziel, ast.Name):
                            lokal[ziel.id] = "normalisiere" in rechts
            for n in ast.walk(fn):
                if not (isinstance(n, ast.Call)
                        and isinstance(n.func, ast.Name)):
                    continue
                if (n.func.id not in praedikate or not n.args
                        or n.func.id == name):
                    continue
                if any(isinstance(x, ast.Call) and isinstance(x.func, ast.Name)
                       and x.func.id == "normalisiere"
                       for x in ast.walk(praedikate[n.func.id])):
                    continue          # Praedikat normalisiert selbst
                arg = ast.unparse(n.args[0])
                if "normalisiere" in arg:
                    continue
                if isinstance(n.args[0], ast.Name) and lokal.get(n.args[0].id):
                    continue
                befunde.append(f"{pfad.name}:{n.lineno} {n.func.id}({arg[:40]})")
    return befunde


def test_kein_gate_bekommt_einen_rohen_claim():
    """Das eigentliche Gate dieses PRs.

    Am 26.10.2026 waren es 13 Dienste; ein Dienst, der kuenftig sein
    Praedikat mit einem ungefalteten Claim aufruft, faellt hier auf —
    ohne Hand-Liste.
    """
    assert not _gate_aufrufe_ohne_normalisierung()


def test_der_zensus_findet_den_eigenen_anlass():
    """Ein Instrument, das seinen Anlass nicht findet, taugt nichts (#229).

    Der Fix wird testweise zurueckgenommen und muss gemeldet werden.
    """
    pfad = pathlib.Path(BACKEND, "services", "wcag22.py")
    echt = pfad.read_text(encoding="utf-8")
    kaputt = echt.replace(
        "_claim_mentions_wcag22(normalisiere(combined_lc))",
        "_claim_mentions_wcag22(combined_lc)", 1)
    assert kaputt != echt, "Der Aufruf sieht anders aus als erwartet"
    try:
        pfad.write_text(kaputt, encoding="utf-8")
        befunde = _gate_aufrufe_ohne_normalisierung()
    finally:
        pfad.write_text(echt, encoding="utf-8")
    assert any("wcag22.py" in b for b in befunde), befunde


@pytest.mark.parametrize("name", BETROFFEN_AM_26_10_2026)
def test_die_betroffenen_dienste_haben_noch_ein_vokabular(name):
    """Haelt die Liste der gemessenen Faelle an der Wirklichkeit.

    Faellt das, wurde ein Dienst umgebaut und die Zahl 110 im Docstring
    beschreibt etwas anderes als den heutigen Code.
    """
    quelle = _quelle(name)
    assert "norm_terme" in quelle
    assert re.search(r"def (_claim_mentions_\w+|_has_\w*keyword)\(", quelle)


# ------------------------------------------------------- Giftproben WCAG

WCAG_PROBEN = [
    "Die Richtlinie verlangt Konformitätsstufe AA",
    "Bundes-Webseiten müssen Konformitätsstufe AA erfüllen",
    "Das Kontrastverhältnis muss 4,5:1 betragen",
    "Web-Zugänglichkeit ist in Österreich gesetzlich geregelt",
]


@pytest.mark.parametrize("claim", WCAG_PROBEN)
def test_umlaut_claims_erreichen_wcag(claim):
    """Alle vier scheiterten am 26.10.2026 am Dienst-Eingang."""
    from services.wcag22 import search_wcag22
    import asyncio
    r = asyncio.run(search_wcag22({"claim": claim}))
    assert r.get("results"), claim


@pytest.mark.parametrize("claim", [
    "Wie viele WCAG-Erfolgskriterien gibt es?",
    "WCAG hat 87 Erfolgskriterien",
    "Wie viele Erfolgskriterien hat WCAG 2.2?",
    "How many WCAG success criteria are there?",
])
def test_die_zaehlfrage_bekommt_daten(claim):
    """Die Frage aus #87, bei der eine Zahl erfunden wurde."""
    from services.wcag22 import search_wcag22
    import asyncio
    r = asyncio.run(search_wcag22({"claim": claim}))
    rows = r.get("results") or []
    assert rows, claim
    # Die Zahl steht im display_value der Ueberblick-Zeile, nicht im
    # indicator_name ("WCAG 2.2 — Überblick").
    text = " ".join(f"{x.get('indicator_name', '')} {x.get('display_value', '')}"
                    for x in rows)
    assert "86" in text and "87" in text, text[:200]


def test_der_stamm_trifft_beide_zahlformen():
    """``erfolgskriterium`` traf den Plural nicht — die Endung wechselt."""
    from services.wcag22 import claim_mentions_wcag22_cached as trifft
    assert trifft("Wie viele Erfolgskriterien gibt es in WCAG?")
    assert trifft("Das Erfolgskriterium 1.4.3 verlangt Kontrast")


def test_bare_wcag_ist_ein_trigger():
    from services.wcag22 import claim_mentions_wcag22_cached as trifft
    assert trifft("WCAG schreibt das vor")


# ---------------------------------------------------- Giftproben ERA5

@pytest.mark.parametrize("claim", [
    "2024 war global das wärmste Jahr seit Beobachtungsbeginn.",
    "2023 war das wärmste Jahr in Deutschland seit Messbeginn.",
    "Der April 2023 ist der kälteste seit über 90 Jahren.",
    "In einem Facebook-Post wird behauptet, wir erlebten den wohl "
    "kältesten Sommer seit 35 Jahren",
])
def test_echte_rekord_claims_erreichen_era5(claim):
    """Sieben solche Claims waren vorher unerreichbar."""
    from services.era5 import _claim_mentions_era5 as pred
    assert pred(normalisiere(claim.lower())), claim


@pytest.mark.parametrize("claim", [
    # Allgemeiner Superlativ + bloße Zeit-Einheit: kein Wetter-Claim.
    "Die zwei letzten Rentenerhöhungen waren die allerhöchsten "
    "Rentenerhöhungen der letzten 20 Jahre.",
    "Rekord-Todesfälle im Jahr 2021 unter Piloten nach Corona-Impfungen.",
    "Der höchste Tag der Woche war ein Montag im Jahr 2024.",
])
def test_allgemeiner_superlativ_plus_zeiteinheit_triggert_era5_nicht(claim):
    from services.era5 import _claim_mentions_era5 as pred
    assert not pred(normalisiere(claim.lower())), claim


@pytest.mark.parametrize("claim", [
    "Rekord-Hitze im Sommer 2024 in Wien",
    "Ein Niederschlags-Rekord im Jahr 2024",
])
def test_allgemeiner_superlativ_plus_echtes_wetter_triggert_weiter(claim):
    """Die Verengung darf 'Rekord-Hitze' nicht mitnehmen."""
    from services.era5 import _claim_mentions_era5 as pred
    assert pred(normalisiere(claim.lower())), claim


def test_die_zeiteinheiten_sind_eine_teilmenge_der_wetter_nomen():
    """Sonst prueft die Verengung gegen eine Liste, die nie greift."""
    from services.era5 import _WEATHER_NOUNS, _ZEIT_EINHEITEN
    assert set(_ZEIT_EINHEITEN) <= set(_WEATHER_NOUNS)
    assert _ZEIT_EINHEITEN, "leere Liste waere ein stiller No-op"


def test_wetter_superlative_sind_eine_teilmenge_der_rekord_verben():
    from services.era5 import _RECORD_VERBS, _WETTER_SUPERLATIVE
    assert set(_WETTER_SUPERLATIVE) <= set(_RECORD_VERBS)
    # Die allgemeinen Superlative muessen uebrig bleiben, sonst waere die
    # Unterscheidung sinnlos.
    assert set(_RECORD_VERBS) - set(_WETTER_SUPERLATIVE)
