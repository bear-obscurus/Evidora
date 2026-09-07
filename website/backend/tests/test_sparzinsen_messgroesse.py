"""„Die Zinsen für mein Sparbuch sind niedrig" — hergeleitet aus dem Leitzins.

QA50E-Befund 4: `true@0.85`. Die Antwort sagte wörtlich

    „Sparbuchzinsen orientieren sich typischerweise am Leitzins und sind
     daher aktuell ebenfalls niedrig."

und erfand in der Nuance dazu „typischerweise unter 2 % p.a." — beides ohne
Beleg. Zu Sparzinsen gab es **keine einzige Reihe** im Konnektor.

GEMESSEN: DIE VAGE WERTUNG IST DAS SCHLUPFLOCH
==============================================
Vier Live-Läufe am 2026-09-07 zeigen, dass das System die allgemeinen Fälle
beherrscht — nur diese eine Kombination nicht:

    „Meine Stromrechnung ist zu hoch"           unverifiable@0.1   korrekt
    „Sparbuchzinsen liegen unter 1 Prozent"     unverifiable@0.1   korrekt
    „Der EZB-Leitzins ist niedrig"              mixed@0.75         korrekt
    „Die Zinsen für mein Sparbuch sind niedrig" true@0.85          ⚠

Der Unterschied ist **nicht** die Quellenlage: die EZB feuerte bei „unter 1
Prozent" genauso wie bei „niedrig". Wird eine ZAHL verlangt, merkt das Modell,
dass sie fehlt. Bei „niedrig" fällt die Lücke nicht auf — und der Leitzins
rückt als Ersatz ein.

DIE URSACHE STAND IM KONNEKTOR
==============================
`SERIES_MAP` bildete das Stichwort `"zinsen"` auf den LEITZINS ab, und zu
Sparzinsen gab es nichts. Ein Claim über Sparbücher bekam also zwangsläufig
den Leitzins geliefert.

Es gibt die echten Zahlen: EZB-MIR-Statistik, monatlich, nach Land und
Produkt. Am 2026-09-07 gegen die API geprüft (Stand Juli 2026):

    täglich fällige Einlagen (Sparbuch)   0,43 %
    gebundene Einlagen (Termin-/Festgeld) 2,10 %

**Ein Faktor 5 zwischen zwei Zahlen, die beide „Sparzinsen" heissen** — die
Fleisch-Falle (#321) in Reinform. Nur eine von beiden einzubauen hätte den
Messgrößen-Fehler im Fix wiederholt, deshalb stehen beide drin, mit Warnung.

NEBENBEFUND, DERSELBE LAUF
==========================
Der Konnektor liefert sechs Beobachtungen ohne Rangfolge, und das Modell griff
sich eine mittlere: es zitierte „Leitzins aktuell (Juni 2025) 2,15 %", obwohl
`2026-06-17 — 2,40 %` in derselben Antwort stand. Der jüngste Datenpunkt ist
jetzt als solcher gekennzeichnet.
"""

import asyncio
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services.ecb import SERIES_MAP, _find_series, _parse_sdmx_json  # noqa: E402

SPARBUCH_REIHE = "MIR/M.AT.B.L21.A.R.A.2250.EUR.N"
FESTGELD_REIHE = "MIR/M.AT.B.L22.A.R.A.2250.EUR.N"
LEITZINS_REIHE = "FM/B.U2.EUR.4F.KR.MRR_FR.LEV"


# --------------------------------------------------------------------------
# Die Lücke, die den Befund verursacht hat
# --------------------------------------------------------------------------

def test_sparzinsen_reihe_existiert_ueberhaupt():
    """Vorher gab es zu Sparzinsen keine einzige Reihe — deshalb musste der
    Leitzins herhalten."""
    reihen = {s["series"] for s in SERIES_MAP.values()}
    assert SPARBUCH_REIHE in reihen
    assert FESTGELD_REIHE in reihen


@pytest.mark.parametrize("claim", [
    "die zinsen für mein sparbuch sind niedrig",
    "sparbuchzinsen in österreich",
    "wie hoch ist der sparzins",
    "was bringt mein sparkonto",
    "zinsen auf spareinlagen",
    "tagesgeldkonto zinsen",
])
def test_sparbuch_claim_bekommt_die_sparzinsen(claim):
    """Und zwar an ERSTER Stelle: bei einem Sparbuch-Claim matcht auch
    „zinsen" und damit der Leitzins. `matching[:3]` würde sonst womöglich
    genau die Reihe wegschneiden, nach der gefragt wurde."""
    treffer = _find_series(claim)
    assert treffer, claim
    assert treffer[0]["series"] == SPARBUCH_REIHE, [t["label"] for t in treffer]


def test_festgeld_ist_eine_andere_reihe():
    """Faktor 5 zwischen den beiden — sie dürfen nicht zusammenfallen."""
    assert _find_series("wie hoch ist das festgeld")[0]["series"] == FESTGELD_REIHE
    assert _find_series("termingeld zinsen")[0]["series"] == FESTGELD_REIHE
    assert SPARBUCH_REIHE != FESTGELD_REIHE


def test_deutsche_komposita_treffen():
    """`\\bsparbuch\\b` hätte „Sparbuchzinsen" nicht getroffen. Für markierte
    Stichwörter steht die Wortgrenze deshalb nur vorne."""
    for wort in ("sparbuchzinsen", "sparzinsen", "festgeldzinsen",
                 "tagesgeldkonto", "spareinlagenzinssatz"):
        assert _find_series(f"wie hoch sind die {wort} 2026"), wort


def test_praefix_gilt_nur_fuer_markierte_stichwoerter():
    """Die Gegenprobe. Die Wortgrenze steht überhaupt da, weil „euro" sonst
    „europäische" trifft — das darf der Umbau nicht aufweichen."""
    assert _find_series("die europäische union hat 27 mitglieder") == []
    ohne_praefix = [k for k, v in SERIES_MAP.items() if not v.get("praefix")]
    assert "euro" in ohne_praefix and "dollar" in ohne_praefix


def test_leitzins_claim_bekommt_keine_sparzinsen():
    """Umgekehrt genauso: wer nach dem Leitzins fragt, soll den Leitzins
    bekommen."""
    treffer = _find_series("der ezb-leitzins ist niedrig")
    assert [t["series"] for t in treffer] == [LEITZINS_REIHE]


# --------------------------------------------------------------------------
# Die Messgrößen-Warnung
# --------------------------------------------------------------------------

def _antwort(reihe_label, werte):
    return {
        "dataSets": [{"series": {"0:0:0": {"observations": {
            str(i): [v] for i, v in enumerate(werte)}}}}],
        "structure": {"dimensions": {"observation": [
            {"id": "TIME_PERIOD",
             "values": [{"id": f"2026-{i+1:02d}"} for i in range(len(werte))]}]}},
    }


def _spec(schluessel):
    return next(v for k, v in SERIES_MAP.items() if k == schluessel)


def test_warnung_nennt_alle_drei_verwechselbaren_saetze():
    """Leitzins, Einlagefazilität und Haushalts-Sparzins heissen alle
    „Zinsen" — die Warnung muss alle drei auseinanderhalten."""
    res = _parse_sdmx_json(_antwort("x", [0.40, 0.41, 0.43]), _spec("sparbuch"))
    text = " ".join(r["title"] for r in res)
    assert "MESSGROESSE" in text
    assert "Leitzins" in text and "Einlagefazilitaet" in text
    assert "PRIVATEN HAUSHALTEN" in text
    assert "Vielfaches" in text, "Der Unterschied Sparbuch/Festgeld muss stehen"


def test_warnung_haengt_am_juengsten_wert_und_nur_einmal():
    """Sechsmal derselbe Satz frisst das Prompt-Budget, das die Zahlen
    brauchen (#131). Und wenn er nur einmal steht, dann an der Zeile, die
    zitiert wird."""
    res = _parse_sdmx_json(_antwort("x", [0.40, 0.41, 0.40, 0.41, 0.40, 0.43]),
                           _spec("sparbuch"))
    mit_warnung = [r for r in res if "MESSGROESSE" in r["title"]]
    assert len(mit_warnung) == 1
    assert mit_warnung[0] is res[-1], "Warnung gehört an den jüngsten Wert"
    assert mit_warnung[0]["value"] == 0.43


def test_juengster_wert_ist_gekennzeichnet():
    """Nebenbefund aus demselben Lauf: das Modell zitierte „Leitzins aktuell
    (Juni 2025) 2,15 %", obwohl 2026-06-17 mit 2,40 % danebenstand. Sechs
    Zeilen ohne Rangfolge laden dazu ein."""
    res = _parse_sdmx_json(_antwort("x", [3.15, 2.90, 2.65, 2.40, 2.15, 2.40]),
                           _spec("leitzins"))
    markiert = [r for r in res if "AKTUELLSTER WERT" in r["title"]]
    assert len(markiert) == 1
    assert markiert[0] is res[-1] and markiert[0]["value"] == 2.40


def test_leitzins_traegt_keine_sparzins_warnung():
    """Die Warnung gehört an die Einlagenzinsen, nicht an jede Reihe —
    sonst steht sie auch bei Wechselkursen und wird zu Rauschen."""
    res = _parse_sdmx_json(_antwort("x", [2.40, 2.15, 2.40]), _spec("leitzins"))
    assert not any("MESSGROESSE" in r["title"] for r in res)


# --------------------------------------------------------------------------
# Vage Wertungen brauchen die Spannweite, nicht eine Verweigerung
# --------------------------------------------------------------------------

@pytest.mark.parametrize("claim", [
    "Der EZB-Leitzins ist niedrig",
    "Die Zinsen sind hoch",
    "Die hohen Zinsen belasten Kreditnehmer",
    "niedrige Zinsen in Europa",
    "Sparbuchzinsen sind gering",
    "Der Euro ist stark",
])
def test_vage_wertung_zieht_die_historische_spannweite(claim):
    """Nebeneffekt des Stellvertreter-Fixes, live gemessen: „Der EZB-Leitzins
    ist niedrig" fiel von `mixed@0.75` auf `unverifiable@0.1`.

    Die Ursache lag tiefer als der Prompt: „ist niedrig" steht im POSITIV und
    war deshalb nicht in `HISTORICAL_KEYWORDS` — das Modell bekam sechs
    Monatswerte und keine Spannweite. Das alte `mixed` stützte sich auf
    ungestütztes Modellwissen („>4 % in den 2000ern").

    Mit Minimum und Maximum der Reihe wird aus der Wertung eine prüfbare
    Aussage. Eine Verweigerung wäre die schlechtere Antwort, wenn 15 Jahre
    Daten danebenliegen.
    """
    from services.ecb import _needs_historical
    assert _needs_historical(claim), claim


@pytest.mark.parametrize("claim", [
    "Die Hochschule in Wien",
    "Das Hochwasser 2024",
    "Die Hochrechnung zur Wahl",
    "Hochhaus in Wien",
    "Starkregen in Tirol",
    "Die Geringfügigkeitsgrenze",
    "Die Geringfuegigkeitsgrenze",
    "Die Stärkung des Euro",
    "Der Leitzins liegt bei 2,4 Prozent",
])
def test_vage_wertung_trifft_keine_zusammensetzungen(claim):
    """Der erste Entwurf nutzte ein offenes Präfix — und traf „Hochschule"
    und „Hochwasser". Genau der Fehler, gegen den die Wortgrenze im
    Reihen-Matching überhaupt existiert. Bis zu drei Buchstaben Flexion,
    danach muss ein Nicht-Buchstabe stehen.

    Der zweite Entwurf hatte `[a-z]{0,3}` ohne Umlaute — und dann matcht
    „Geringfügigkeit": nach „gering" steht „f" (in der Klasse), danach „ü"
    (nicht in der Klasse), der Lookahead ist erfüllt. **Dieser Test hat es
    gefangen**; eine Ad-hoc-Sonde mit ASCII-Schreibweise vorher nicht.
    """
    from services.ecb import _needs_historical
    assert not _needs_historical(claim), claim


# --------------------------------------------------------------------------
# Die Prompt-Schicht
# --------------------------------------------------------------------------

def test_prompt_verbietet_den_stellvertreter_schluss():
    quelle = (BACKEND / "services" / "synthesizer.py").read_text(encoding="utf-8")
    assert "STELLVERTRETER-MESSGRÖSSE" in quelle
    assert "VAGEN Wertungen" in quelle, (
        "Die vage Wertung ist das Schlupfloch — das muss im Prompt stehen")
    assert "NIEMALS eine Zahl für X schätzen" in quelle
    assert "PERSÖNLICHER BEZUG" in quelle


def test_prompt_nennt_die_konkreten_verwechslungen():
    """Eine abstrakte Regel allein hat bei den Messgrößen noch nie gereicht —
    #115/#117 brauchten zwei Anläufe. Die Paare stehen deshalb ausgeschrieben."""
    quelle = (BACKEND / "services" / "synthesizer.py").read_text(encoding="utf-8")
    for paar in ("Einlagefazilität", "Termin-/Festgeld", "AMS-Arbeitslosenquote",
                 "Akutbetten"):
        assert paar in quelle, paar


# --------------------------------------------------------------------------
# Echte Abfrage (nur wenn das Netz da ist)
# --------------------------------------------------------------------------

@pytest.mark.skipif("CI" in __import__("os").environ,
                    reason="geht ins Netz — in CI nicht erwuenscht")
def test_echte_reihen_liefern_plausible_werte():
    """Die Reihen-IDs sind von Hand aus der SDMX-Struktur gelesen. Ein Tippfehler
    dort liefert stillschweigend nichts — genau der WGI-Fehler (#156)."""
    from services.ecb import search_ecb
    r = asyncio.run(search_ecb(
        {"claim": "Wie hoch sind die Sparbuchzinsen in Österreich?", "entities": []}))
    werte = [x for x in (r.get("results") or []) if "taeglich" in x["indicator"]]
    assert werte, "keine Sparzins-Daten — Reihen-ID pruefen"
    assert 0.0 <= werte[-1]["value"] <= 5.0, werte[-1]
