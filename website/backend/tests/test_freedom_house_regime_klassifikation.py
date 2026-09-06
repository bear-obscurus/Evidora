"""Freedom House vergibt Freiheits-Ratings — keine Staatsform.

QA-Batterie 2026-09-06, Befund 2: „Ungarn ist laut Freedom House keine
Demokratie mehr" bekam ``true@0.9``, begruendet mit „Partly Free, nicht als
Demokratie eingestuft".

Die Diagnose war eindeutig — und der Dienst war unschuldig:

    Freedom House lieferte:  HU 65/100 'Partly Free' (PR 24/40, CL 41/60)
    demokratie_pack lieferte: V-Dem LDI 0.32 (ein stetiger Index, keine Klasse)
    im ganzen Korpus:        keine Aussage „Ungarn ist (k)eine Demokratie"

Den Sprung vom FREIHEITSGRAD auf die STAATSFORM machte der Synthesizer. Das
beruehrt den dritten Politik-Guardrail: eine Klassifikation darf nur zitiert
werden, wenn eine Quelle sie tatsaechlich vornimmt, und die Quelle muss
ausgewiesen sein. Hier wurde Freedom House eine Einstufung zugeschrieben, die
es gar nicht vergibt.

Der Fix haengt bei Regime-Claims einen Satz an den ``display_value``, der
genau das benennt. Er sagt NICHT, ob Ungarn eine Demokratie ist — das waere
derselbe Fehler mit umgekehrtem Vorzeichen.

Zweiter, beim Bauen gefundener Befund: ``freedom_house`` fehlte in der
Normalisierung aus #147. Der Filter dort schloss jede Datei aus, die
``_topic_match`` importiert — hier passiert das aber nur fuer den
Politik-Tabu-Guard, nicht fuer den Matcher. Folge in Produktion:

    „Freedom House Bewertung fuer die Tuerkei"  ->  AT 94/100 'Free'

Der ASCII-Claim traf den Alias „türkei" nicht, das Land fiel durch, und der
DACH-Default antwortete mit Oesterreich. Ein falsches Land, ohne Fehlermeldung.
"""

import asyncio
import re
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._schreibweise import normalisiere  # noqa: E402
from services._topic_match import politik_guard_action  # noqa: E402
from services.freedom_house import (  # noqa: E402
    _detect_countries_in_claim,
    _klassifikations_warnung,
    _load_data,
    claim_mentions_freedom_house_cached,
    search_freedom_house,
)

MAX_STR = 400  # synthesizer.MAX_STR


def _ergebnis(claim):
    r = asyncio.run(search_freedom_house({"claim": claim,
                                          "original_claim": claim}))
    assert r["results"], f"kein Treffer fuer {claim!r}"
    return r["results"][0]


def _display(claim):
    return _ergebnis(claim)["display_value"]


# --------------------------------------------------------------------------
# Der Befund selbst
# --------------------------------------------------------------------------

REGIME_CLAIMS = (
    "Ungarn ist laut Freedom House keine Demokratie mehr",
    "Ungarn ist laut freedom-house keine demokratische Ordnung mehr",
    "Russland ist laut Freedom House ein autoritäres Regime",
    "Russland ist laut Freedom House ein autoritaeres Regime",
    "Ist die Türkei laut Freedom House eine Diktatur?",
    "Ist die Tuerkei laut Freedom House eine Diktatur?",
    "Freedom House stuft Ungarn als Autokratie ein",
    "Welche Staatsform hat Ungarn laut Freedom House?",
)


@pytest.mark.parametrize("claim", REGIME_CLAIMS)
def test_regime_claim_bekommt_die_einordnung(claim):
    """Der QA-Claim und seine Verwandten muessen den Hinweis mitbekommen."""
    d = _display(claim)
    assert "FREIHEITSGRADE" in d, d
    assert "keine Regime-Klassifikation" in d.replace("\n", " "), d


def test_hinweis_nennt_die_gleichsetzung_beim_namen():
    """Ein blosses „FIW misst Freiheit" reicht nicht — der Fehlschluss selbst
    muss benannt sein, sonst zieht der Synthesizer ihn erneut."""
    d = _display("Ungarn ist laut Freedom House keine Demokratie mehr")
    assert "Partly Free" in d
    assert "gleichsetzt" in d or "ueberdehnt" in d or "überdehnt" in d, d


def test_hinweis_klassifiziert_nicht_selbst():
    """Guardrail 3 gilt in beide Richtungen: der Hinweis darf nicht seinerseits
    behaupten, Ungarn SEI eine Demokratie."""
    d = normalisiere(_display("Ungarn ist laut Freedom House keine Demokratie mehr"))
    for verboten in ("ungarn ist eine demokratie", "ungarn ist keine demokratie",
                     "ungarn bleibt eine demokratie", "sehr wohl eine demokratie"):
        assert verboten not in d, f"{verboten!r} steht im display_value"


def test_die_zahlen_bleiben_vorne():
    """Der Hinweis haengt HINTEN an. Vorne stehen Score und Status — sonst
    verdraengt die Warnung genau die Belege, die den Claim pruefbar machen."""
    d = _display("Ungarn ist laut Freedom House keine Demokratie mehr")
    assert d.startswith("HU 65/100 'Partly Free'"), d[:60]
    assert d.index("65/100") < d.index("WICHTIG")


# --------------------------------------------------------------------------
# Budget: der Hinweis darf nur zahlen, wo er gebraucht wird
# --------------------------------------------------------------------------

SACH_CLAIMS = (
    "Freedom House gibt Ungarn 65 von 100 Punkten",
    "Wie schneidet Österreich bei Freedom House ab?",
    "Freedom House Bewertung fuer die Tuerkei",
    "Wie hoch ist der Freiheitsindex von Polen?",
)


@pytest.mark.parametrize("claim", SACH_CLAIMS)
def test_sach_claim_bekommt_keinen_hinweis(claim):
    """Bei einem reinen Zahlen-Claim kostet die Warnung nur Prompt-Budget."""
    assert "FREIHEITSGRADE" not in _display(claim)


def test_display_bleibt_unter_der_kuerzungs_schwelle():
    """Der Vertrag aus #131: was ueber MAX_STR liegt, wird beschnitten — und
    der Hinweis steht hinten, also traefe es genau ihn. Ueber alle 55 Laender
    im Datensatz, mit dem laengsten Regime-Claim."""
    ca = _load_data().get("country_aliases") or {}
    laengste = 0
    for al in ca.values():
        c = (f"Ist {al[0]} laut Freedom House eine Demokratie, eine Autokratie "
             f"oder eine Diktatur? Welche Staatsform, welches Regime?")
        laengste = max(laengste, len(_display(c)))
    assert laengste <= MAX_STR, f"laengster display_value: {laengste}"


def test_warnung_ist_reine_funktion_des_claims():
    """Ohne Regime-Begriff leerer String — der Aufrufer haengt dann nichts an."""
    assert _klassifikations_warnung("wie viele punkte hat ungarn") == ""
    assert _klassifikations_warnung("") == ""
    assert _klassifikations_warnung("ist ungarn eine demokratie") != ""


# --------------------------------------------------------------------------
# Die Normalisierungs-Luecke aus #147
# --------------------------------------------------------------------------

def test_land_wird_auch_in_ascii_schreibweise_erkannt():
    """Der stille Fehler: „Tuerkei" fiel durch, der DACH-Default antwortete
    mit Oesterreich. Ein falsches Land ohne Fehlermeldung."""
    for schreibweise, iso in (("Türkei", "TUR"), ("Tuerkei", "TUR"),
                              ("Südafrika", "ZAF"), ("Suedafrika", "ZAF"),
                              ("Weißrussland", "BLR"), ("Weissrussland", "BLR"),
                              ("Österreich", "AUT"), ("Oesterreich", "AUT")):
        claim = f"Freedom House Bewertung fuer {schreibweise}"
        gefunden = _detect_countries_in_claim(normalisiere(claim), _load_data())
        assert iso in gefunden, f"{schreibweise}: {gefunden}"


def test_die_tuerkei_liefert_die_tuerkei():
    """Der konkrete Regressions-Fall, End-to-End."""
    assert _display("Freedom House Bewertung fuer die Tuerkei").startswith("TR ")


def test_trigger_in_beiden_schreibweisen():
    for paar in (("Bürgerrechte laut Freedom House", "Buergerrechte laut Freedom House"),
                 ("Freedom-House-Ranking", "Freedom House Ranking"),
                 ("Autoritäres Regime laut FIW", "Autoritaeres Regime laut FIW"),
                 ("Freie Länder laut Freedom House", "Freie Laender laut Freedom House")):
        for c in paar:
            assert claim_mentions_freedom_house_cached(c), c


def test_kein_ueber_trigger():
    """Die Normalisierung faltet Schreibweisen zusammen — sie darf dabei keine
    neuen Treffer erzeugen."""
    for c in ("Wie hoch ist die Inflation in Österreich?",
              "Das Haus der Freiheit steht in Wien",
              "Wer hat die Fußball-WM 2026 gewonnen?",
              "Freies WLAN in den Wiener Öffis",
              "Wie viele Kilometer Autobahn hat Österreich?"):
        assert not claim_mentions_freedom_house_cached(c), c


# --------------------------------------------------------------------------
# Was beim Aufraeumen NICHT passieren darf
# --------------------------------------------------------------------------

def test_politik_guard_bekommt_den_ungefalteten_claim():
    """Der Politik-Tabu-Guard wird bewusst mit ``claim.lower()`` aufgerufen,
    NICHT mit ``normalisiere(claim)``.

    Seine Token-Liste in ``_topic_match`` ist unnormalisiert und enthaelt
    Bindestrich-Namen („meinl-reisinger", „rendi-wagner"). Ein gefalteter
    Claim macht daraus „meinl reisinger" — der Guard greift nicht mehr, und
    Laender-Quellen feuern auf eine Partei-Korruptions-Aussage. Das ist die
    sensibelste Stelle im ganzen Projekt, deshalb steht sie hier als Test und
    nicht nur als Kommentar.
    """
    heikel = "Meinl-Reisinger ist die korrupteste Politikerin Österreichs"
    assert politik_guard_action(heikel.lower()) == "block_country_sources"
    assert politik_guard_action(normalisiere(heikel)) == "pass", (
        "Wenn das hier bricht, wurde _topic_match normalisiert — dann darf "
        "freedom_house den gefalteten Claim weiterreichen und dieser Test weg."
    )
    quelle = (BACKEND / "services" / "freedom_house.py").read_text(encoding="utf-8")
    assert re.search(r"is_party_corruption_superlative_claim\(claim\.lower\(\)\)",
                     quelle), "Guard-Aufruf wurde auf normalisiere() umgestellt"
    assert not claim_mentions_freedom_house_cached(
        "Die FPÖ ist die korrupteste Partei — Freedom House")
