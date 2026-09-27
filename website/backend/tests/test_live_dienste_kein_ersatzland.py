"""Die Live-Dienste: dieselbe Regel, andere Karten.

Fortsetzung des Laender-Rueckfall-Nachgangs (#177). Drei Live-Dienste mit
eigenen Erkennern zeigten denselben Fehler in eigener Form:

    unesco_uis  Nordkorea, Tuvalu, Oesterreich, „weltweit"  -> Nigeria
    cepii       „Handel zwischen Nordkorea und Deutschland"  -> Suedkorea<->Deutschland
    unhcr       unbekanntes Land                            -> WELT-Summe

Dazu zwei Erkennungsfehler, die beim Gate auffielen:

    unhcr       „sudan" steckt in „suedsudan": Gefluechtete aus dem
                Suedsudan gingen an den Sudan
    unesco_uis  Kartenschluessel nie gefaltet: Oesterreich, Aethiopien,
                Suedafrika, Aegypten und Tuerkei wurden nie erkannt

KEIN Test hier geht ins Netz. Die Such-Tests ersetzen den HTTP-Client durch
eine Attrappe, die jeden Zugriff mitschreibt und abbricht — ein Rueckfall
auf die alte Regel wuerde den Test rot machen, nicht die CI an eine API
haengen (Lehre aus #165).
"""

import asyncio
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._schreibweise import normalisiere  # noqa: E402


class _KeinNetz:
    """Ersetzt polite_client. Jeder Aufbau wird mitgeschrieben und bricht ab."""

    def __init__(self):
        self.aufgerufen = 0

    def __call__(self, *a, **kw):
        self.aufgerufen += 1
        raise RuntimeError("Netzzugriff im Test")


# --------------------------------------------------------------------------
# cepii
# --------------------------------------------------------------------------

def test_cepii_macht_aus_nordkorea_kein_suedkorea():
    from services.cepii import _detect_countries
    assert _detect_countries(normalisiere("Handel zwischen Nordkorea und Deutschland")) == ["DEU"]


def test_cepii_suedkorea_bleibt_erreichbar():
    from services.cepii import _detect_countries
    assert _detect_countries(normalisiere("Handel zwischen Südkorea und Deutschland")) == ["KOR", "DEU"]


def test_cepii_reihenfolge_und_iso_token():
    """Reporter vor Partner: die Textreihenfolge bleibt. Nackte ISO3-Codes
    („BACI DEU FRA") funktionieren weiter."""
    from services.cepii import _detect_countries
    assert _detect_countries(normalisiere("Exporte von Deutschland nach Frankreich")) == ["DEU", "FRA"]
    assert _detect_countries(normalisiere("BACI DEU FRA")) == ["DEU", "FRA"]


def test_cepii_keine_teilstring_suche_mehr():
    """Geprueft wird die Codezeile, nicht der Text — der Kommentar, der die
    alte Suche beschreibt, nennt sie bewusst beim Namen."""
    quelle = (BACKEND / "services" / "cepii.py").read_text(encoding="utf-8")
    assert "idx = claim_lc.find(" not in quelle


# --------------------------------------------------------------------------
# unhcr
# --------------------------------------------------------------------------

@pytest.mark.parametrize("text,erwartet", [
    ("fluechtlinge aus dem suedsudan", (None, None)),
    ("fluechtlinge aus dem sudan", (None, "SDN")),
    ("asylantraege in oesterreich", ("AUT", None)),
    ("fluechtlinge in nordkorea", (None, None)),
])
def test_unhcr_erkennung(text, erwartet):
    """„sudan" steckte als Teilstring in „suedsudan"."""
    from services.unhcr import _detect_countries
    assert _detect_countries(text) == erwartet, text


def test_unhcr_genannter_ort_ohne_daten_bekommt_keine_welt_summe(monkeypatch):
    import services.unhcr as unhcr
    attrappe = _KeinNetz()
    monkeypatch.setattr(unhcr, "polite_client", attrappe)
    res = asyncio.run(unhcr.search_unhcr({"claim": "Laut UNHCR leben in Tuvalu viele Fluechtlinge"}))
    assert res["results"] == []
    assert attrappe.aufgerufen == 0, "fuer Tuvalu wurde trotzdem abgefragt"


def test_unhcr_weltweit_fragt_die_welt_summe_ab(monkeypatch):
    """Die Ausnahme: fuer „weltweit" IST die Welt-Summe die gefragte Zahl."""
    import services.unhcr as unhcr
    attrappe = _KeinNetz()
    monkeypatch.setattr(unhcr, "polite_client", attrappe)
    with pytest.raises(RuntimeError, match="Netzzugriff"):
        asyncio.run(unhcr.search_unhcr({"claim": "Laut UNHCR gibt es weltweit viele Fluechtlinge"}))
    assert attrappe.aufgerufen == 1


def test_unhcr_ohne_ortsangabe_fragt_weiter_ab(monkeypatch):
    import services.unhcr as unhcr
    attrappe = _KeinNetz()
    monkeypatch.setattr(unhcr, "polite_client", attrappe)
    with pytest.raises(RuntimeError, match="Netzzugriff"):
        asyncio.run(unhcr.search_unhcr({"claim": "Laut UNHCR gibt es viele Fluechtlinge"}))
    assert attrappe.aufgerufen == 1


# --------------------------------------------------------------------------
# unesco_uis
# --------------------------------------------------------------------------

@pytest.mark.parametrize("land,iso", [
    ("Österreich", "AUT"), ("Äthiopien", "ETH"), ("Südafrika", "ZAF"),
    ("Ägypten", "EGY"), ("Türkei", "TUR"),
])
def test_unesco_umlaut_laender_werden_erkannt(land, iso):
    """Die Kartenschluessel waren nie gefaltet, der Claim schon."""
    from services.unesco_uis import _detect_countries
    treffer = _detect_countries(normalisiere(f"Kinder in {land} gehen zur Schule"))
    assert [i for i, _ in treffer] == [iso], treffer


def test_unesco_umlaut_land_neben_einem_anderen():
    """Der Ersatzland-Schutz greift nur, wenn gar nichts erkannt wird — ein
    verfehltes Umlaut-Land neben einem erkannten waere sonst still verloren."""
    from services.unesco_uis import _detect_countries
    treffer = _detect_countries(normalisiere("Kinder in Ägypten und Nigeria"))
    # Menge, nicht Reihenfolge: die UIS-Erkennung sortiert seit jeher nach
    # Schluessellaenge — das ist nicht Gegenstand dieses Fixes.
    assert {i for i, _ in treffer} == {"EGY", "NGA"}, treffer


@pytest.mark.parametrize("ort", ["in Nordkorea", "in Tuvalu", "weltweit"])
def test_unesco_genannter_ort_ohne_karte_bekommt_kein_nigeria(monkeypatch, ort):
    import services.unesco_uis as uis
    attrappe = _KeinNetz()
    monkeypatch.setattr(uis, "polite_client", attrappe)
    claim = f"Laut UNESCO UIS gehen {ort} viele Kinder nicht zur Schule"
    res = asyncio.run(uis.search_unesco_uis({"claim": claim, "original_claim": claim}))
    assert res["results"] == [], ort
    assert attrappe.aufgerufen == 0, f"fuer {ort} wurde trotzdem abgefragt"
