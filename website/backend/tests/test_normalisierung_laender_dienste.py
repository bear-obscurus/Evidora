"""Die 13 Services, die #147 fälschlich übersprungen hat.

Der Filter dort schloss jede Datei aus, die `_topic_match` **importiert** —
Annahme: die laufen über den gemeinsamen, bereits normalisierten Matcher. **14
Services importieren es aber nur für den Politik-Tabu-Guard.** `freedom_house`
war Nummer eins (#150, dort kam „Freedom House Bewertung fuer die Tuerkei" als
`AT 94/100 'Free'` zurück), die übrigen 13 sind hier.

Gemessen vor dem Umbau, über 405 auslösende Trigger-Begriffe:

    Umlaut -> ae/oe/ue        70 %
    Bindestrich -> Leer       56 %
    Leer -> Bindestrich       34 %

und über die vier Länder-Karten (idea, rsf, transparency, wgi):

    ASCII-Umschrift des Landes    9/33

**Der Weg dorthin ist wieder die Lehre.** Drei Klassen von Trigger-Listen hat
die erste Spec nicht erwischt, und jede wurde von einer anderen Prüfung
gefangen:

  1. **Inline-Listen im Funktionsrumpf** (`constitute`s Spezial-Lemmata mit
     „b-vg", `polity5`s History-Marker, `wid`s Share-Buckets). Das
     Regressions-Gate meldete sie sofort: zwei Begriffe verloren.
  2. **Daten-seitige Vergleiche**, die wie Trigger aussehen.
     `_PRESIDENTIAL_ELECTION_TERMS` wird gegen `election["type"]` aus dem
     ParlGov-Export geprüft, nicht gegen den Claim — nach dem Falten der Liste
     traf „présidentielle" aus den Daten nicht mehr. Neun Tests aus #116
     schlugen an. Richtige Lösung: **beide** Seiten falten, dann werden die
     handgepflegten Doppel-Einträge („présidentielle"/„presidentielle")
     überflüssig.
  3. **Listen in Dicts** (`WGI_INDICATORS[*]["keywords"]`, 66 Stück, und
     `WJP_FACTORS[*]["keywords"]`, 48). Die fand **kein Test** — die Suite war
     grün, während der Haupt-Matching-Pfad beider Dienste still tot lag. Nur
     ein eigens gebautes statisches Audit über die Vergleichs-Seiten hat sie
     aufgedeckt. Das ist exakt die Lehre aus #147: eine grüne Suite beweist
     nichts über ungetesteten Code.

Bewusst NICHT gefaltet: ISO3-Code-Listen (`_DISPLAY_REFERENCE_COUNTRIES`,
`_DEFAULT_COUNTRIES`), Daten-Schlüssel (`_POLICY_AREAS_ORDER`, `_DISPLAY_REFS`)
und `wid._country_display_name`s `preferred`-Menge — die vergleicht gegen
Aliasse aus der JSON, nicht gegen den Claim. Sie zu falten hätte
Dict-Lookups zerrissen.
"""

import ast
import importlib
import re
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._schreibweise import normalisiere  # noqa: E402

DIENSTE = ("bti", "climate_action_tracker", "constitute", "idea", "mipex",
           "parlgov", "polity5", "rsf", "transparency", "vdem", "wgi",
           "wid", "wjp_rol")

UMLAUT = str.maketrans({"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"})


def _praedikat(name):
    """Nicht alle 13 heissen gleich — idea/transparency exportieren
    `_claim_mentions_idea` / `_claim_mentions_cpi` ohne `_cached`-Suffix."""
    mod = importlib.import_module(f"services.{name}")
    kand = [a for a in dir(mod) if "claim_mentions" in a or "claim_wants" in a]
    bevorzugt = [a for a in kand if a.endswith("_cached")] or kand
    return getattr(mod, bevorzugt[0])


def _trigger_literale(name):
    """Trigger-Literale aus dem QUELLTEXT, nicht aus der Laufzeit.

    Zur Laufzeit stehen dort schon „oesterreich" statt „Österreich" — die
    Umlaut-Variante liesse sich daran gar nicht mehr messen.
    """
    baum = ast.parse((BACKEND / "services" / f"{name}.py").read_text(
        encoding="utf-8"))
    raus = set()
    for k in ast.walk(baum):
        if not (isinstance(k, ast.Call) and isinstance(k.func, ast.Name)
                and k.func.id == "norm_terme"):
            continue
        for arg in k.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                x = arg.value.strip()
                if 4 <= len(x) <= 45 and not x.startswith(("http", "\\", "^")):
                    raus.add(x)
    return raus


# --------------------------------------------------------------------------
# Symmetrie: Konstanten UND Claim
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name", DIENSTE)
def test_dienst_normalisiert_konstanten_und_claim(name):
    """Die Falle des ersten Anlaufs in #147: Konstanten gefaltet, Claim nicht
    — dann trifft ein Umlaut-Trigger weder ASCII- noch Umlaut-Claim."""
    quelle = (BACKEND / "services" / f"{name}.py").read_text(encoding="utf-8")
    assert "norm_terme(" in quelle, f"{name}: keine Konstante gefaltet"
    assert re.search(r"normalisiere\(\s*(claim|analysis|f\"|\(claim|term|alias|kw|name|election|e\.get)",
                     quelle), f"{name}: Claim-Seite nicht gefaltet"


@pytest.mark.parametrize("name", DIENSTE)
def test_kein_trigger_verloren(name):
    """Das Regressions-Gate als dauerhafter Test. Genau diese Prüfung hat den
    Verlust von „b-vg" und „us-verfassung" gefangen — eine Inline-Liste im
    Funktionsrumpf, die die Spec nicht kannte."""
    praed = _praedikat(name)
    verloren = [t for t in _trigger_literale(name)
                if praed(t) and not praed(normalisiere(t))]
    assert not verloren, f"{name}: {verloren[:5]}"


def test_schreibweisen_quote():
    """Vor dem Umbau: Umlaut 70 %, Bindestrich 56 %, Leerzeichen 34 %."""
    for titel, wandler, minimum in (
        ("Umlaut", lambda t: t.translate(UMLAUT), 20),
        ("Bindestrich -> Leer", lambda t: t.replace("-", " "), 50),
        ("Leer -> Bindestrich", lambda t: t.replace(" ", "-"), 100),
    ):
        ok = fehl = 0
        for name in DIENSTE:
            praed = _praedikat(name)
            for t in _trigger_literale(name):
                if not praed(t):
                    continue
                v = wandler(t)
                if v == t:
                    continue
                ok, fehl = (ok + 1, fehl) if praed(v) else (ok, fehl + 1)
        assert ok + fehl >= minimum, f"{titel}: zu wenig Messpunkte ({ok+fehl})"
        assert fehl == 0, f"{titel}: {fehl} Ausfälle von {ok+fehl}"


# --------------------------------------------------------------------------
# Die Länder-Karten — dieselbe Schicht wie der Tuerkei-Fehler in #150
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name", ("idea", "rsf", "transparency", "wgi"))
def test_land_wird_auch_in_ascii_umschrift_erkannt(name):
    """Vor dem Umbau 9 von 33. Ein durchgefallenes Land heisst nicht
    „kein Ergebnis", sondern „Ergebnis für ein anderes Land" — der
    DACH-Default antwortet dann."""
    mod = importlib.import_module(f"services.{name}")
    karte = mod.COUNTRY_MAP
    finden = mod._find_countries
    fehlt = []
    for land in [k for k in karte if re.search(r"[äöüß]", k)]:
        ascii_form = land.translate(UMLAUT)
        frage = f"Wie steht {ascii_form} im Ranking?"
        treffer = finden({"claim": frage, "original_claim": frage}) or []
        if karte[land] not in treffer:
            fehlt.append((land, ascii_form))
    assert not fehlt, f"{name}: {fehlt}"


# --------------------------------------------------------------------------
# Die drei Klassen, die die erste Spec nicht erwischt hat
# --------------------------------------------------------------------------

def test_inline_listen_im_funktionsrumpf_sind_gefaltet():
    """`constitute`s „b-vg", `polity5`s History-Marker, `wid`s Buckets."""
    praed = _praedikat("constitute")
    for b in ("b-vg", "b vg", "us-verfassung", "us verfassung"):
        assert praed(b), f"constitute: {b!r}"

    from services.wid import _share_buckets
    for schreibweise in ("top-1%", "top 1%", "top-1 prozent", "top 1 prozent"):
        assert "top1" in _share_buckets(normalisiere(schreibweise)), schreibweise

    # `polity5`s History-Marker sitzen in `search_polity5`, nicht im Trigger —
    # ueber das Praedikat sind sie nicht erreichbar. Deshalb strukturell:
    # entscheidend ist, dass die Liste ueberhaupt durch norm_terme laeuft.
    quelle = (BACKEND / "services" / "polity5.py").read_text(encoding="utf-8")
    assert "history_marker_keywords = norm_terme(" in quelle


def test_datenseitige_vergleiche_falten_beide_seiten():
    """`_PRESIDENTIAL_ELECTION_TERMS` prüft `election["type"]` aus dem
    ParlGov-Export, NICHT den Claim. Nach dem Falten der Liste allein traf
    „présidentielle" aus den Daten nicht mehr — neun Tests aus #116 schlugen
    an. Beide Seiten falten macht die Hand-Dubletten überflüssig."""
    from services.parlgov import _is_presidential_election
    for typ in ("présidentielle", "presidentielle", "Présidentielle (Stichwahl)",
                "Präsidentschaftswahl", "Praesidentschaftswahl"):
        assert _is_presidential_election({"type": typ}), typ
    assert not _is_presidential_election({"type": "Nationalratswahl"})


def test_listen_in_dicts_sind_gefaltet():
    """66 + 48 Keywords, die KEIN Test abgedeckt hat. Ohne das statische Audit
    wäre der Haupt-Matching-Pfad von wgi und wjp_rol still tot geblieben,
    während die Suite grün meldete."""
    from services.wgi import WGI_INDICATORS
    from services.wjp_rol import WJP_FACTORS
    for label, tabelle in (("WGI", WGI_INDICATORS), ("WJP", WJP_FACTORS)):
        alle = [k for spec in tabelle.values() for k in spec["keywords"]]
        assert alle, label
        ungefaltet = [k for k in alle if normalisiere(k) != k]
        assert not ungefaltet, f"{label}: {ungefaltet[:5]}"
    from services.wgi import claim_mentions_wgi_cached
    from services.wjp_rol import claim_mentions_wjp_cached
    for schreibweise in ("regierungseffektivität", "regierungseffektivitaet"):
        assert claim_mentions_wgi_cached(schreibweise), schreibweise
    for schreibweise in ("amts-missbrauch rechtsstaat",
                         "amts missbrauch rechtsstaat"):
        assert claim_mentions_wjp_cached(schreibweise), schreibweise


def test_iso3_listen_bleiben_ungefaltet():
    """Die Gegenprobe. ISO3-Codes und Daten-Schlüssel sind KEINE Trigger; sie
    zu falten hätte Dict-Lookups zerrissen — „AUT" wäre zu „aut" geworden."""
    from services.mipex import _DISPLAY_REFERENCE_COUNTRIES, _POLICY_AREAS_ORDER
    from services.polity5 import _DEFAULT_COUNTRIES_FOR_DACH_CLAIMS
    from services.wid import _DEFAULT_COUNTRIES
    from services.wjp_rol import _DISPLAY_REFS
    assert _DEFAULT_COUNTRIES_FOR_DACH_CLAIMS == ("AUT", "DEU", "CHE")
    assert _DEFAULT_COUNTRIES[0] == "AUT"
    assert all(c.isupper() for c in _DISPLAY_REFERENCE_COUNTRIES)
    assert "labour_market_mobility" in _POLICY_AREAS_ORDER
    assert "Austria" in _DISPLAY_REFS


# --------------------------------------------------------------------------
# Über-Trigger
# --------------------------------------------------------------------------

@pytest.mark.parametrize("claim", (
    "Wie viele Kilometer Autobahn hat Österreich?",
    "Wie viele Kilometer Autobahn hat Oesterreich?",
    "Das Wetter in Wien ist heute schön",
    "Wie backe ich einen Marmorkuchen?",
    "Kristalle wachsen in Salzlösungen",
    "Der FC Bayern hat gestern gewonnen",
))
def test_kein_ueber_trigger(claim):
    """Die Faltung führt Schreibweisen zusammen — sie darf dabei keine neuen
    Treffer erzeugen. Gemessen über alle 13: 0 neue, 0 weggefallene."""
    feuernd = [n for n in DIENSTE if _praedikat(n)(claim)]
    assert not feuernd, f"{claim!r}: {feuernd}"
