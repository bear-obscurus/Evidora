"""„Wie viele X gibt es?" muss den Fakt finden, der die Zahl trägt (#253).

QA50H-Befund: „Wie viele Sonderschüler gibt es in Österreich?" blieb
`unverifiable@0.1` mit **0 Belegen**, obwohl `nbb_2024_spf_bestand_at` die
Zahl führt. Seine Mengen-Gruppe listet `quote|anteil|prozent|anzahl|
bestand|…` — und damit kein Wort, das ein Mensch tippt, wenn er nach einer
Anzahl fragt. Die Notiz dazu ist vom 28.09.; #231 hatte nur „wie viel
gibt" für einen ANDEREN Fakt verengt.

Zensus: 71 Fakten tragen eine Mengen-Gruppe, **50 davon ohne jede
Frageform**, in 25 Dateien.

GELÖST WIRD DAS IM MATCHER, nicht in 50 Dateien — zwei Gründe: „wie viel"
ist eine Teilzeichenkette von „wie viele" (die Falle aus #231), und eine
Regel in 50 Kopien driftet.

⚠️ EHRLICH ZUR REICHWEITE: die Regel erreicht **16 der 50**. Die übrigen 34
haben Mengen-Gruppen, in denen die Mengen-Marker unter Themen-Tokens
untergehen (`frauen_in_politik_2026`: 20 Token, davon zwei Marker). Die
gehören aufgeräumt, nicht per Schwelle eingefangen — sonst zahlt man mit
Fremdtreffern.

MESSUNG (A/B in einem Lauf, Regel aus = Identität):
    Schwelle  Fremdtreffer  synthetische Zielsonden
      0,00        +6             +24
      0,10        +2             +13
      0,15        +1              +7
      0,25        +0              +5
⭐ Die synthetischen Sonden legten nahe, man müsse +6 Fremdtreffer für den
Gewinn bezahlen. Sie waren aber aus ROHEN Trigger-Tokens gebaut („Wie
viele sonderpaedagog gibt es") — keine Frage, die jemand stellt. An echten
Fragen gemessen trifft jede bei JEDER Schwelle, also auch bei 0,25 ohne
einen einzigen Fremdtreffer. Die Kosten waren eine Täuschung der
Messgrösse.

Muss-Treffer über alle 2.814 dokumentierten Phrasings: 2.616 → 2.616.
"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._mengenfrage import (  # noqa: E402
    FRAGEFORMEN, MINDESTANTEIL, ist_mengengruppe, mit_mengenfrage,
)
from services._topic_match import substring_or_composite_match  # noqa: E402

DATA = BACKEND / "data"


def _fakt(datei, fid):
    d = json.loads((DATA / datei).read_text(encoding="utf-8"))
    fakten = d.get("facts") if isinstance(d, dict) else d
    return next(f for f in fakten if (f.get("id") or f.get("topic")) == fid)


# --------------------------------------------------------------------------
# Die Erkennung
# --------------------------------------------------------------------------

def test_eine_reine_mengengruppe_wird_erkannt():
    assert ist_mengengruppe(["quote", "anteil", "prozent", "anzahl"])


def test_eine_verduennte_gruppe_wird_nicht_erkannt():
    """`frauen_in_politik_2026` hat 20 Token, davon zwei Marker. Die Gruppe
    fragt nach Repräsentation, nicht nach einer Zahl — und war das Einzige,
    was einen zu weiten Themen-Treffer noch aufhielt."""
    gruppe = ["frauen", "männer", "anteil", "vertret", "repräsentier",
              "quote", "wollen", "können", "weniger", "mehr", "über",
              "unter", "politik", "partei", "liste", "wahl"]
    assert not ist_mengengruppe(gruppe)


def test_ein_einzelner_marker_unter_vielen_genuegt_nicht():
    """`partg_par3_rahmen_pro_wahlberechtigtem` galt nur wegen 'pro kopf'
    als Mengen-Gruppe."""
    assert not ist_mengengruppe(
        ["pro wahlberechtig", "wahlberechtigt", "pro bürger", "pro kopf",
         "pro person", "11 euro", "11 eur", "3,10", "höchstgrenze",
         "obergrenze", "rahmen", "gesetzlich"])


def test_leere_gruppe_faellt_nicht_um():
    assert not ist_mengengruppe([])


def test_die_schwelle_ist_gemessen_nicht_geraten():
    assert 0.2 <= MINDESTANTEIL <= 0.34


# --------------------------------------------------------------------------
# Die Erweiterung
# --------------------------------------------------------------------------

def test_mengengruppe_bekommt_die_frageformen():
    erweitert = mit_mengenfrage(["quote", "anteil", "prozent", "anzahl"])
    for ff in FRAGEFORMEN:
        assert ff in erweitert


def test_andere_gruppen_bleiben_unberuehrt():
    """Sonst stünde die Frageform plötzlich in Themen-Gruppen."""
    gruppe = ["österreich", "oesterreich", " at "]
    assert mit_mengenfrage(gruppe) is gruppe


def test_beide_schreibweisen_sind_abgedeckt():
    assert "wie viele" in FRAGEFORMEN and "wieviele" in FRAGEFORMEN


# --------------------------------------------------------------------------
# Der Fall aus der QA — und was NICHT passieren darf
# --------------------------------------------------------------------------

ECHTE_FRAGEN = [
    ("iqs_bildung.json", "nbb_2024_spf_bestand_at",
     "Wie viele Sonderschüler gibt es in Österreich?"),
    ("iqs_bildung.json", "nbb_2024_spf_bestand_at",
     "Wieviele Sonderschüler hat Österreich?"),
]


@pytest.mark.parametrize("datei,fid,frage", ECHTE_FRAGEN)
def test_die_mengenfrage_erreicht_ihren_fakt(datei, fid, frage):
    assert substring_or_composite_match(_fakt(datei, fid), frage.lower())


def test_gift_probe_ohne_die_regel_trifft_sie_nicht(monkeypatch):
    """Nur wenn die Frage OHNE die Regel danebengeht, prüft der Test oben
    etwas."""
    import services._topic_match as TM
    monkeypatch.setattr(TM, "mit_mengenfrage", lambda alt: alt)
    assert not TM.substring_or_composite_match(
        _fakt("iqs_bildung.json", "nbb_2024_spf_bestand_at"),
        "Wie viele Sonderschüler gibt es in Österreich?".lower())


def test_eine_fremde_mengenfrage_trifft_nicht():
    """Die AND-Struktur trägt das Risiko: die Frageform befriedigt nur die
    MENGEN-Gruppe, Thema und Land müssen weiter treffen."""
    assert not substring_or_composite_match(
        _fakt("iqs_bildung.json", "nbb_2024_spf_bestand_at"),
        "wie viele menschen leben in wien?")


@pytest.mark.parametrize("datei,fid,phrasing", [
    ("gleichstellung_pack.json", "frauen_in_politik_2026",
     "Wie viel kostet ein Parlamentsklub?"),
    ("rechnungshof_parteienfin.json", "partg_par3_rahmen_pro_wahlberechtigtem",
     "AT Studienfoerderung wieviel"),
])
def test_die_beiden_gemessenen_fehlgriffe_bleiben_aus(datei, fid, phrasing):
    """Bei Schwelle 0,00 zog die Regel diese beiden Fakten mit — eine
    Klubkosten-Frage auf 'Frauen in der Politik', eine Studienförderungs-
    Frage auf die Parteienfinanzierung."""
    assert not substring_or_composite_match(_fakt(datei, fid), phrasing.lower())
