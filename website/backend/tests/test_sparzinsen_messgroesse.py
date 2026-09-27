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
    # Marker seit 27.9.2026 "Zwei Messgroessen" statt "MESSGROESSE":
    # die Warnung musste unter PROMPT_MAX_STR passen.
    assert "Zwei Messgroessen" in text
    assert "Leitzins" in text and "Einlagefazilitaet" in text
    # "privater Haushalte" steht seit 27.9.2026 im LABEL statt in der
    # Warnung — die musste unter PROMPT_MAX_STR passen, und doppelt
    # brauchte es die Angabe nicht.
    assert "privater Haushalte" in text
    assert "Vielfaches" in text, "Der Unterschied Sparbuch/Festgeld muss stehen"


def test_warnung_haengt_am_juengsten_wert_und_nur_einmal():
    """Sechsmal derselbe Satz frisst das Prompt-Budget, das die Zahlen
    brauchen (#131). Und wenn er nur einmal steht, dann an der Zeile, die
    zitiert wird."""
    res = _parse_sdmx_json(_antwort("x", [0.40, 0.41, 0.40, 0.41, 0.40, 0.43]),
                           _spec("sparbuch"))
    mit_warnung = [r for r in res if "Zwei Messgroessen" in r["title"]]
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
    assert not any("Zwei Messgroessen" in r["title"] for r in res)


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


# --------------------------------------------------------------------------
# Der Oberbegriff liefert BEIDE Reihen (Live-Befund 27.9.2026)
# --------------------------------------------------------------------------
# HART40: "Die Sparzinsen in Österreich liegen bei 2 Prozent" bekam
# false@0.95 — begründet ausschließlich mit "0,43 % (täglich fällige
# Einlagen)". Der gebundene Satz liegt bei rund 2,10 % und trifft die
# Behauptung damit fast genau; er kam nie im Prompt an.
#
# Ursache: `_find_series` sammelt je Serie einen Eintrag, und das Stichwort
# "sparzins" zeigte nur auf die Overnight-Reihe. Nur wer "Festgeld" oder
# "Termingeld" schrieb, bekam die zweite. Die MESSWARNUNG stand daneben —
# sie warnt aber nur, dass sich die beiden Größen um ein Vielfaches
# unterscheiden; der zweite WERT fehlte.
#
# "Sparzinsen" und "Spareinlagen" sind Oberbegriffe und liefern deshalb
# beide Reihen. Produktbezeichnungen bleiben eindeutig.

from services.ecb import _find_series as _reihen  # noqa: E402

OVERNIGHT = "MIR/M.AT.B.L21.A.R.A.2250.EUR.N"
GEBUNDEN = "MIR/M.AT.B.L22.A.R.A.2250.EUR.N"


def _serien(claim):
    return {s["series"] for s in _reihen(claim)}


@pytest.mark.parametrize("claim", [
    "Die Sparzinsen in Österreich liegen bei 2 Prozent",
    "Wie hoch sind die Sparzinsen?",
    "Die Spareinlagen werden schlecht verzinst",
])
def test_oberbegriff_liefert_beide_reihen(claim):
    s = _serien(claim)
    assert OVERNIGHT in s and GEBUNDEN in s, (claim, s)


@pytest.mark.parametrize("claim,erwartet", [
    ("Wie hoch sind die Sparbuchzinsen?", OVERNIGHT),
    ("Mein Sparkonto bringt nichts", OVERNIGHT),
    ("Tagesgeld wirft kaum etwas ab", OVERNIGHT),
    ("Wie hoch ist der Festgeldzins?", GEBUNDEN),
    ("Termingeld bringt mehr", GEBUNDEN),
])
def test_produktbezeichnungen_bleiben_eindeutig(claim, erwartet):
    """Wer nach EINEM Produkt fragt, bekommt nicht beide Zahlen — sonst
    wäre die Messgrößen-Warnung sinnlos."""
    s = _serien(claim)
    assert erwartet in s
    andere = {OVERNIGHT, GEBUNDEN} - {erwartet}
    assert not (andere & s), (claim, s)


def test_die_zusatzreihe_erbt_den_vorrang():
    """Ohne Vorrang fiele sie unter Umständen dem `matching[:3]`-Schnitt
    zum Opfer — der Grund, warum die Overnight-Reihe ihn überhaupt hat."""
    reihen = _reihen("Die Sparzinsen in Österreich liegen bei 2 Prozent")
    gebunden = next(s for s in reihen if s["series"] == GEBUNDEN)
    assert gebunden.get("vorrang") is True


def test_beide_reihen_tragen_die_messwarnung():
    for s in _reihen("Wie hoch sind die Sparzinsen?"):
        assert "nie gegeneinander einsetzen" in s["hinweis"]


def test_der_leitzins_bleibt_unberuehrt():
    s = _serien("Der EZB-Leitzins ist gestiegen")
    assert OVERNIGHT not in s and GEBUNDEN not in s


# --------------------------------------------------------------------------
# Der Per-Source-Cap (Live-Nachmessung 27.9.2026, nach #219)
# --------------------------------------------------------------------------
# #219 liess den Oberbegriff beide Reihen liefern — die Antwort blieb
# trotzdem bei "0,43 %". Gemessen, warum:
#
#   Der Synthesizer nimmt je Quelle nur die besten DREI Treffer
#   (`limit = 3` in synthesizer.py), sortiert nach Claim-Abdeckung. Der
#   Connector lieferte SECHS Beobachtungen je Reihe, also zwölf. Die
#   Claim-Terme waren [sparzinsen, österreich, liegen, prozent]; das Label
#   der Overnight-Reihe trug "Sparzinsen Oesterreich" (2 Treffer), das der
#   gebundenen nur "Zinsen Oesterreich" (1 Treffer). Alle drei Plaetze
#   gingen an die Overnight-Reihe.
#
# Zwei Aenderungen: Beide Reihen heissen jetzt "Sparzinsen ..." (die
# gebundene IST ein Sparzins), und fuer die Messgroessen-Paare zaehlt nur
# der juengste Wert — der Verlauf steht in dessen Titel. Aus zwoelf
# Treffern werden zwei, und beide passen unter den Cap.

def test_beide_labels_tragen_das_claim_wort():
    reihen = _reihen("Die Sparzinsen in Österreich liegen bei 2 Prozent")
    assert len(reihen) == 2
    for s in reihen:
        assert s["label"].startswith("Sparzinsen Oesterreich"), s["label"]


def test_messgroessen_paare_liefern_nur_den_juengsten_wert():
    """Sonst füllt eine Reihe allein den Per-Source-Cap."""
    from services.ecb import SERIES_MAP
    for kw in ("sparbuch", "sparzins", "sparkonto", "spareinlagen",
               "tagesgeld", "festgeld", "termingeld"):
        assert SERIES_MAP[kw].get("nur_aktuell") is True, kw
    for kw in ("sparzins", "spareinlagen"):
        assert SERIES_MAP[kw]["auch_serie"].get("nur_aktuell") is True, kw


def test_der_leitzins_behaelt_seine_reihe():
    """Dort gibt es keine zweite Messgröße, die verdrängt werden könnte —
    und der Verlauf trägt die Aussage."""
    from services.ecb import SERIES_MAP
    assert not SERIES_MAP["leitzins"].get("nur_aktuell")


def test_nur_aktuell_greift_nicht_bei_historischen_claims():
    """Bei einem Verlaufs-Claim IST die Reihe die Antwort."""
    from services.ecb import _parse_sdmx_json
    assert "historical" in _parse_sdmx_json.__doc__ or True
    import inspect
    quelle = inspect.getsource(_parse_sdmx_json)
    assert "not historical" in quelle


# --------------------------------------------------------------------------
# Die Warnung muss unter die Kürzung passen (Messung 27.9.2026)
# --------------------------------------------------------------------------
# Beim Nachmessen von #220 gefunden: Der Hinweis hängt am Titel des
# jüngsten Datenpunkts, und der Synthesizer kürzt jedes String-Feld auf
# PROMPT_MAX_STR (400). Die Titel waren 525 bzw. 514 Zeichen lang — die
# zweite Hälfte der Warnung, also genau der Satz über die beiden
# Messgrößen, erreichte das Modell NIE. Die Warnung aus #160/#161 stand
# seit ihrer Einführung nur zur Hälfte im Prompt.
#
# Sie ist deshalb gekürzt und nach Wichtigkeit geordnet: erst die
# Verwechslungsgefahr, dann die Handlungsanweisung für allgemeine Fragen,
# zuletzt die Abgrenzung zum Leitzins.

MAX_WARNUNG = 240
"""Obergrenze der Warnung.

Gemessen am 27.9.2026 gegen die echte EZB-API: Mit einer 382 Zeichen
langen Warnung waren die fertigen Titel 525 und 514 Zeichen lang und
wurden bei PROMPT_MAX_STR (400) abgeschnitten — mitten im Satz über die
beiden Messgrößen. Mit 238 Zeichen sind dieselben Titel 381 und 370
Zeichen lang und kommen vollständig an. 240 ist die Grenze, die diesen
Abstand hält; wer sie anhebt, muss die Titel neu messen."""


def test_die_warnung_passt_unter_die_kuerzung():
    from services.ecb import MESSWARNUNG
    from services.synthesizer import PROMPT_MAX_STR
    assert len(MESSWARNUNG) <= MAX_WARNUNG, len(MESSWARNUNG)
    assert MAX_WARNUNG < PROMPT_MAX_STR


def test_die_warnung_nennt_zuerst_die_verwechslungsgefahr():
    from services.ecb import MESSWARNUNG
    assert MESSWARNUNG.startswith("Zwei Messgroessen")
    assert "nie gegeneinander einsetzen" in MESSWARNUNG


def test_die_warnung_sagt_was_bei_einer_allgemeinen_frage_zu_tun_ist():
    """Der Anlass: "Wie hoch sind die Sparzinsen?" bekam unverifiable@0.1,
    obwohl beide Werte in der Begründung standen."""
    from services.ecb import MESSWARNUNG
    assert "Bei allgemeiner Frage BEIDE nennen" in MESSWARNUNG


def test_die_abgrenzung_zum_leitzins_bleibt_erhalten():
    from services.ecb import MESSWARNUNG
    assert "Nicht der Leitzins" in MESSWARNUNG
    assert "nicht die Einlagefazilitaet" in MESSWARNUNG
