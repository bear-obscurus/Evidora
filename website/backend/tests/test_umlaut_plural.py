"""Der Umlaut-Plural: „Aufsichtsräte" muss „aufsichtsrat" erreichen.

Anlass (28.9.2026, Fund aus dem #228-Sweep). Der Claim

    "Männliche Führungskräfte dominieren die Aufsichtsräte"

erreichte ``frauenquote_wirksamkeit_2026`` nicht, obwohl der Fakt das Token
``aufsichtsrat`` führt: Normalisiert heißt der Plural „aufsichtsraete", und
darin steckt „aufsichtsrat" nicht. Derselbe Bruch trifft jeden
Umlaut-Plural — Löhne, Städte, Bundesländer, Häuser, Bücher, Ärzte, Wälder.

## Der erste Sweep war von der falschen Seite gedacht

Er ging von der Token-Seite: letzten Stammvokal umlauten, gegen ein Korpus
aus 3.730 echten Claims prüfen. 225 Paare — aber als Obermenge aus drei
Sorten, und zwei davon sind keine Plurale:

    bundesland -> bundeslaender     echter Plural
    gefahr     -> gefaehrlich        Ableitung, anderes Wort
    wahr       -> waehrend           Zufall ("während" hat nichts mit wahr zu tun)

Und der GEMESSENE Fall fehlte, weil „Aufsichtsräte" im Korpus nicht
vorkommt. Ein Instrument, das seinen eigenen Anlass nicht findet, taugt
nicht. Ein zweiter Versuch generierte Plurale selbst und meldete 3.657
betroffene Tokens — aber aus erfundenen Wörtern („deutschlaende",
„theraepiee"). Auch das misst nichts.

## Was stattdessen gemessen wurde

Nicht das Token umlauten, sondern das Claim-Wort ENTUMLAUTEN. Das braucht
kein Lexikon. Damit die Zufallstreffer draußen bleiben, muss der Rest hinter
dem Token eine echte Plural-Endung sein und das Wort auf ``token + endung``
ENDEN (nicht damit beginnen — sonst wäre „wahrend" wieder drin, weil es mit
„wahre" anfängt).

A/B am echten Korpus, 1.898 Claims mit Umlaut, Pass an und aus:

    Muss-Treffer verloren     54 -> 53     (einer zurückgewonnen)
    Fremdtreffer-Paare       330 -> 335    (die fünf unten, einzeln gesichtet)
    Batterie                 1/5 -> 4/5
    Über-Trigger-Kontrollen  0/2 -> 0/2    (Waehrung, Landschaften)

    Claims, die Fakten VERLIEREN: 0

Die erste Fassung dieser Messung filterte die Claims auf ä/ö/ü und zählte
1.587. Das war falsch: Die Daten tragen teils die ASCII-Umschrift („Vaeter
nehmen kein KBG"), und normalisiert ist das dasselbe Wort. Aufgefallen ist es
nicht der Messung, sondern einem Gate in
tests/test_wortformen_geschlecht.py, das einen Treffer meldete, den die
Messung nicht hatte — 1.898 ist die korrigierte Zahl.

Von den neuen Paaren sind fünf richtig oder am Thema (Unfälle und Städten bei
Tempo-30-Fakten, gesünder bei zwei Wasser-Mythen, Vaeter+KBG bei der
Vereinbarkeit), eines ist ein echter Über-Trigger („Mütter werden während
Stillzeit nicht schwanger" erreicht den Vereinbarkeits-Fakt). Eins zu 1.898,
gegen eine geschlossene Fehlerklasse — bewusst in Kauf genommen und unten
festgeschrieben.

Der fünfte Batterie-Fall scheitert NICHT am Umlaut: „Wie viele Sonderschüler
gibt es in Österreich?" verlangt beim Fakt eine Mengen-Gruppe
(anzahl|anteil|quote|…), und „wie viele" steht nicht darin. Den Umlaut hat
der Pass korrekt geliefert — meine Erwartung war zu streng.

Keine Netzabfrage, kein Modell.
"""

import copy
import glob
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, BACKEND)

import services._umlaut_plural as up  # noqa: E402
from services._schreibweise import normalisiere  # noqa: E402
from services._topic_match import (  # noqa: E402
    find_matching_items,
    substring_or_composite_match,
)
from services._umlaut_plural import (  # noqa: E402
    ENDUNGEN,
    entumlaute,
    plural_trifft,
    plural_woerter,
)

TF = ("trigger_keywords", "trigger_composite", "trigger_all")


def _fid(it):
    return it.get("id") or it.get("topic") or ""


def _laden():
    orte, alle = {}, []
    for p in sorted(glob.glob(os.path.join(DATA, "*.json"))):
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if not isinstance(v, list):
                continue
            for it in v:
                if isinstance(it, dict) and any(it.get(t) for t in TF):
                    orte.setdefault(_fid(it), []).append((os.path.basename(p), k))
                    alle.append((os.path.basename(p), k, it))
    return orte, alle


ORTE, ALLE = _laden()
FAKT = {_fid(it): it for _d, _k, it in ALLE}


def _trifft_echt(fid: str, claim: str) -> bool:
    for datei, key in ORTE[fid]:
        treffer = find_matching_items(os.path.join(DATA, datei), key,
                                      claim_lc=claim.lower(), full_claim=claim,
                                      descriptor_fn=None)
        if any(fid in (x.get("id"), x.get("topic")) for x in treffer):
            return True
    return False


# ---------------------------------------------------------------------------
# 1. Die Transformation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ein,aus", [
    ("aufsichtsraete", "aufsichtsrate"),
    ("haeuser", "hauser"),          # "aeu" zuerst, sonst wird es "haeser"
    ("buecher", "bucher"),
    ("aerzte", "arzte"),
    ("waelder", "walder"),
    ("leerstand", "leerstand"),     # ohne Umlaut unveraendert
])
def test_entumlauten(ein, aus):
    assert entumlaute(ein) == aus


def test_nur_woerter_mit_umlaut_kommen_mit():
    """Fuer alle anderen hat der exakte Pass schon entschieden."""
    assert plural_woerter("wie viele aufsichtsraete gibt es in wien") == ("aufsichtsrate",)
    assert plural_woerter("wie viele wohnungen stehen leer") == ()


@pytest.mark.parametrize("wort,tok", [
    ("aufsichtsrate", "aufsichtsrat"),      # + "e"
    ("bundeslander", "bundesland"),         # + "er"
    ("stadten", "stadt"),                   # + "en"
    ("vater", "vater"),                     # Väter: ohne Endung
    ("fuhrungskrafte", "kraft"),            # Kompositum, Wortende
    ("unfalle", "unfall"),
    ("betrage", "betrag"),                  # Beträge, echter Plural
])
def test_echter_plural_trifft(wort, tok):
    assert plural_trifft((wort,), tok)


@pytest.mark.parametrize("wort,tok", [
    ("hauser", "haus"),
    ("bucher", "buch"),
    ("arzte", "arzt"),
    ("walder", "wald"),
    ("lohne", "lohn"),
])
def test_kurze_nomen_bleiben_ungedeckt(wort, tok):
    """Bewusste Einschraenkung: Haus, Buch, Arzt, Wald, Lohn bilden echte
    Umlaut-Plurale, liegen aber unter der Mindestlaenge. Sie fallen mit
    "muss"/"müssen" in dieselbe Klasse — vier Buchstaben plus Endung trifft
    zu viel. Wer sie decken will, braucht ein Lexikon oder eine Positivliste
    und faehrt den A/B-Sweep neu.

    Wer eine Positivliste baut, dreht diese Zeile um."""
    assert len(tok) < up.MINDESTLAENGE
    assert not plural_trifft((wort,), tok)


@pytest.mark.parametrize("wort,tok,warum", [
    ("wahrend", "wahr", 'waehrend hat nichts mit wahr zu tun'),
    ("wahrung", "wahr", 'Waehrung ebenso'),
    ("osterreich", "oster", 'Oesterreich ist kein Plural von Ostern'),
    ("betragt", "betrag", 'betraegt ist ein Verb, t keine Plural-Endung'),
    ("schuler", "schule", 'Schueler ist kein Plural von Schule, r keine Endung'),
    ("gefahrlich", "gefahr", 'Ableitung, keine Plural-Endung'),
])
def test_zufall_und_ableitung_treffen_nicht(wort, tok, warum):
    assert not plural_trifft((wort,), tok), warum


def test_wortanfang_genuegt_nicht():
    """Verlangt wird das Wortende. „wahrend“ BEGINNT mit „wahre“ — genau
    deshalb steht hier endswith und nicht startswith."""
    assert "wahrend".startswith("wahr" + "e")
    assert not plural_trifft(("wahrend",), "wahre")


def test_mindestlaenge_haelt_die_kurzen_draussen():
    """„müssen“ entumlautet zu „mussen“ == „muss“ + „en“ — eine formal
    richtige Plural-Endung an einem Wort, das kein Plural ist. Solche Faelle
    haengen an kurzen Tokens, darum die Mindestlaenge."""
    assert not plural_trifft(("mussen",), "muss")
    assert up.MINDESTLAENGE == 5


def test_mindestlaenge_ist_das_wirksame_mittel(monkeypatch):
    """Selbstkontrolle: Mit gesenkter Schwelle feuert der Fehltreffer. Sonst
    bewiese der Test oben nur, dass „muss“ irgendwie nicht passt."""
    monkeypatch.setattr(up, "MINDESTLAENGE", 4)
    assert plural_trifft(("mussen",), "muss")
    assert plural_trifft(("wahrend",), "wahr") is False, (
        "„während“ darf auch mit kurzer Schwelle nicht treffen — die "
        "Endungs-Regel haelt unabhaengig von der Laenge")


def test_endungen_bleiben_eine_geschlossene_liste():
    """Wer hier ergaenzt, fahrt den A/B-Sweep neu — jede Endung mehr ist
    eine Einladung an die Ableitungen („lich“, „ung“, „end“)."""
    assert ENDUNGEN == ("", "e", "er", "en", "ern")


def test_gebundene_und_mehrwort_tokens_bleiben_aussen_vor():
    assert not plural_trifft(("stadten",), "stadt bundesland")


# ---------------------------------------------------------------------------
# 2. Der gemessene Fall und die drei, die er mitnahm
# ---------------------------------------------------------------------------

GESCHLOSSEN = [
    ("frauenquote_wirksamkeit_2026",
     "Männliche Führungskräfte dominieren die Aufsichtsräte", "aufsichtsrat"),
    ("mobilitaet-tempo_30_wirkung_2026", "Gilt Tempo 30 in allen Städten?", "stadt"),
    ("tempo_30_kindersicherheit_konsens", "Tempo 30 verhindert keine Unfälle", "unfall"),
    ("nbb_2024_spf_bestand_at", "Sonderschüler AT Anzahl", "sonderschul"),
]


@pytest.mark.parametrize("fid,claim,_tok", GESCHLOSSEN,
                         ids=[f[0] for f in GESCHLOSSEN])
def test_umlaut_plural_erreicht_den_fakt(fid, claim, _tok):
    assert _trifft_echt(fid, claim), claim


@pytest.mark.parametrize("fid,claim,tok", GESCHLOSSEN,
                         ids=[f[0] for f in GESCHLOSSEN])
def test_der_treffer_kommt_wirklich_vom_pass(fid, claim, tok):
    """Selbstkontrolle: Ohne den Pass faellt der Treffer weg. Sonst koennte
    er von irgendeinem anderen Token stammen."""
    ohne = copy.deepcopy(FAKT[fid])
    assert not _ohne_pass(ohne, claim), (
        f"{fid} trifft auch ohne den Pass — dieser Test prueft nichts")
    assert plural_trifft(plural_woerter(normalisiere(claim.lower())), tok)


def _ohne_pass(item, claim):
    """``substring_or_composite_match`` mit abgeschaltetem Umlaut-Pass."""
    import services._topic_match as tm
    tm_echt = tm.plural_woerter
    tm.plural_woerter = lambda claim_n: ()
    try:
        return substring_or_composite_match(item, claim.lower())
    finally:
        tm.plural_woerter = tm_echt


def test_ein_vorbestehender_muss_treffer_kommt_zurueck():
    """„Sonderschüler AT Anzahl“ ist eine DOKUMENTIERTE Phrasing ihres
    eigenen Fakts und traf ihn vorher nicht. 54 -> 53 verlorene
    Muss-Treffer."""
    assert _trifft_echt("nbb_2024_spf_bestand_at", "Sonderschüler AT Anzahl")


# ---------------------------------------------------------------------------
# 3. Was NICHT treffen darf
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fid,claim", [
    ("frauenquote_wirksamkeit_2026", "Während der Pandemie sank die Währungsstabilität"),
    ("leerstandsabgabe_wirkung_2026", "Österreich hat viele schöne Landschaften"),
    ("nbb_2024_spf_bestand_at", "Die Schule beginnt um acht"),
])
def test_ueber_trigger_kontrollen(fid, claim):
    assert not _trifft_echt(fid, claim), claim


# Der eine echte Ueber-Trigger aus dem A/B-Sweep, gesichtet und in Kauf
# genommen: „Mütter“ ist ein echter Umlaut-Plural, und der Claim erfuellt
# damit die Personen-Gruppe des Vereinbarkeits-Fakts. Ein Paar auf 1.587
# Umlaut-Claims. Wer ihn schliesst, dreht die Zeile um.
BEKANNTER_REST = ("vereinbarkeit_familie_beruf_2026",
                  "Mütter werden während Stillzeit nicht schwanger")


def test_bekannter_ueber_trigger_ist_festgeschrieben():
    assert substring_or_composite_match(FAKT[BEKANNTER_REST[0]],
                                        BEKANNTER_REST[1].lower()), (
        "Der bekannte Über-Trigger ist weg — Zeile umdrehen")


def test_batterie_rest_hat_eine_andere_ursache():
    """„Wie viele Sonderschüler gibt es in Österreich?“ scheitert an der
    Mengen-Gruppe des Fakts, nicht am Umlaut. Wer „wie viele“ als
    Mengen-Marker nachtraegt, dreht die Zeile um."""
    claim = "Wie viele Sonderschüler gibt es in Österreich?"
    assert not _trifft_echt("nbb_2024_spf_bestand_at", claim)
    # Der Umlaut selbst ist geliefert:
    assert plural_trifft(plural_woerter(normalisiere(claim.lower())), "sonderschul")
