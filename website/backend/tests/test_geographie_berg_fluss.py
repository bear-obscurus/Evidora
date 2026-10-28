"""Zwei Abfragen liefen seit Langem in den Timeout — und eine mischte Einheiten.

``geographie_berg`` und ``geographie_fluss`` lieferten bei jeder
Formulierung **0 Zeilen**. Der Eintrag stand lange als „vorbestehend" in der
offenen Liste; nachgemessen hatte ihn niemand.

## Befund 1 — der Berg kam nie an

Die Abfrage zaehlte ALLE Berge samt Unterklassen-Abschluss auf und filterte
danach nach Land:

    ?mountain wdt:P31/wdt:P279* wd:Q8502.
    ?mountain wdt:P17 ?country.

Direkt gegen WDQS gemessen: **HTTP 504 nach 65 Sekunden**. Der Dienst gibt
dem ersten Versuch 12 s (``SPARQL_ATTEMPT_TIMEOUTS = (12.0, 8.0)``) — die
Abfrage konnte also nie ankommen. „0 Zeilen" war kein Trigger-Problem.

Wikidata modelliert die gefragte Tatsache direkt: **P610, höchster Punkt**.
Damit 1,0 s und die richtige Antwort — Großglockner 3.798 m, Zugspitze
2.962 m.

## Befund 2 — die Flussliste mischte Meter und Kilometer

Nach dem Entfernen des Unterklassen-Abschlusses lief die Fluss-Abfrage
(24,6 s, immer noch zu langsam) und lieferte:

    Donau                        2850
    Rhein                        1232.7
    Westlicher Neuperhauserbach  1210
    Östlicher Neuperhauserbach   1090

⚠️ **Ich hielt die Bäche zuerst für Datenfehler bei Wikidata.** Die
Einheiten-Probe zeigte: sie sind **1.210 METER**, während Donau und Rhein in
**Kilometer** geführt sind. ``wdt:P2043`` liefert die blanke Zahl ohne
Einheit, und die Abfrage sortierte Meter gegen Kilometer. Der Fehler lag bei
uns, nicht bei Wikidata.

Jetzt wird der Wertknoten gelesen und auf ``wd:Q828224`` (Kilometer)
gefiltert.

## Befund 3 — die Label-Auflösung war der Kostentreiber

    Österreich    Label  5,6 s    QID 3,0 s
    Deutschland   Label 44,2 s    QID 5,8 s

Mit Label-Auflösung war Deutschland unerreichbar. Die Fluss-Abfrage nimmt
jetzt eine feste Länder-QID. Ist das Land nicht in ``_LAND_QID``, läuft sie
**gar nicht** — besser kein Ergebnis als zwölf Sekunden Wartezeit und dann
keines.

Country-QIDs sind unveränderliche Kennungen; hier droht kein Marker-Drift
(#74/#133).

## Befund 4 — drei von sechs Formulierungen trafen den Trigger nicht

    "Welcher ist der längste Fluss Österreichs?"      MISS
    "Der längste Fluss Österreichs ist die Donau"     MISS
    "Wie lang ist der längste Fluss Deutschlands?"    MISS
    "Österreichs längster Fluss ist die Donau"        ok
    "Welche sind die längsten Flüsse Europas?"        ok
    "Den längsten Fluss hat Brasilien"                ok

Die Liste führte „längster/längsten/längste **flüsse**" — die Form
„längste **fluss**" fehlte, und die steht in den häufigsten Fragen. Die
Berg-Liste hatte ihr Gegenstück („höchste berg") und war mit 6 von 6
vollständig. Aufzählen von Flexionsformen ist die Schwachstelle (#140/#141),
deshalb prüft dieser Test **sechs Formulierungen**, nicht die Liste.

Über 12.155 echte Claims gemessen trifft die neue Form genau **einen**:
„Die Donau ist der längste Fluss in Deutschland." — ein gewollter Treffer.

## Befund 5 (28.10.2026) — wessen Label in die Abfrage kam

Nach dem Deploy live gegen prod nachgemessen. Die Abfragen laufen
("Österreich" 1 Zeile, "Deutschland" 1, "Nepal" 1, "Deutschland" und
"Italien" je 3 Fluesse) — aber welches Label eingesetzt wird, entscheidet
die Analyse-Stufe, und sie nennt nicht immer das Land zuerst.

**Der Berg (1 von 12 Formulierungen).**

    "Der höchste Berg Boliviens ist der Nevado Sajama"
    -> Wikidata: 0 Treffer für 'Nevado Sajama' (geographie_berg)

Gefragt wurde nach dem hoechsten Punkt des LANDES "Nevado Sajama" — 0
Treffer, voellig korrekt. ⚠️ **Eine Laender-Liste loest das nicht:**
Bolivien steht nicht in ``_LAND_QID``, mein erster Fix hat den
gemessenen Fall also nicht angefasst. Die Abfrage nimmt jetzt per UNION
auch einen genannten Berg (ueber P17 das Land, darueber P610) — 0,4 s,
Sajama 6.542 m.

**Der Fluss (2 von 5 Formulierungen, NULL Zeilen).** Hier entscheidet die
Reihenfolge wirklich, denn die Fluss-Abfrage braucht eine QID:

    entities ['Donau', 'Deutschland', 'Fluss']   -> 0 Zeilen
    entities ['Deutschland', 'Fluss', 'Länge']   -> 3 Zeilen
    entities ['Österreich', 'Donau']             -> 3 Zeilen

Stand der Fluss vorn, war ``_land_qid('Donau')`` leer und die Auswahl
brach ab. Beide Antworten lagen trotzdem richtig — Wikipedia sprang ein.

⚠️ **Und der erste Fix sass in der falschen Funktion.**
``_detect_template_for_claim`` entscheidet nur, OB ein Template laeuft;
die abgefragten Labels kommen aus ``_entities_for_claim``. Das Prod-Log
zeigte es: Die Auswahl meldete ``{'name': 'Österreich'}``, die Abfrage
schickte weiter ``"Großglockner"@de`` als Land. Beide nutzen jetzt
``_geo_kandidaten``.

## Was die Abfrage NICHT kann, und was deshalb dasteht

P2043 ist die Länge des **ganzen** Flusses. Der Rhein berührt Österreich auf
rund 25 km und stand in dieser Liste mit 1.232 km. Wikidata führt die
Strecke je Land nicht, also kann hier niemand eine Rangliste „längster Fluss
des Landes" liefern. Die Ausgabe nennt die Messgröße deshalb ausdrücklich —
sonst liest das Modell eine Rangliste, die es nicht gibt.

Dependency-light: Templates, Trigger und Renderer — kein Netz. Die
Laufzeiten oben sind direkt gegen WDQS gemessen und hier dokumentiert, nicht
getestet.
"""
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

from services.wikidata import (  # noqa: E402
    _LAND_QID,
    _TEMPLATES,
    _entities_for_claim,
    _format_row,
    _geo_kandidaten,
    _land_qid,
    _detect_template_for_claim,
)


def _tmpl(name: str) -> dict:
    treffer = [x for x in _TEMPLATES if x["name"] == name]
    assert len(treffer) == 1, name
    return treffer[0]


# ------------------------------------------------------------------ der Berg

def test_der_berg_nutzt_den_hoechsten_punkt():
    s = _tmpl("geographie_berg")["sparql"]
    assert "wdt:P610" in s, "P610 (hoechster Punkt) fehlt"


def test_der_berg_zaehlt_nicht_mehr_alle_berge_auf():
    """`P31/P279* wd:Q8502` war die Abfrage, die nach 65 s mit 504 starb."""
    s = _tmpl("geographie_berg")["sparql"]
    assert "wd:Q8502" not in s, s


def test_der_berg_nennt_die_messgroesse():
    text, _, _ = _format_row("geographie_berg", {
        "mountainLabel": {"value": "Großglockner"},
        "elevation": {"value": "3798"},
        "countryLabel": {"value": "Österreich"},
    })
    assert "3.798 m" in text
    assert "hoechster Punkt" in text, text


def test_der_berg_erfindet_keine_hoehe():
    """P2044 ist OPTIONAL — ohne Wert darf keine Zahl erscheinen."""
    text, _, _ = _format_row("geographie_berg", {
        "mountainLabel": {"value": "Irgendein Gipfel"},
        "countryLabel": {"value": "Nirgendwo"},
    })
    assert "?" in text
    assert not any(z.isdigit() for z in text), text


# ----------------------------------------------------------------- der Fluss

def test_der_fluss_liest_die_einheit():
    """Ohne Einheit sortierte die Liste Meter gegen Kilometer."""
    s = _tmpl("geographie_fluss")["sparql"]
    assert "wikibase:quantityUnit wd:Q828224" in s, s
    assert "wikibase:quantityAmount" in s


def test_der_fluss_nutzt_die_laender_qid():
    s = _tmpl("geographie_fluss")["sparql"]
    assert "wd:{qid}" in s
    # Die Label-Aufloesung kostete fuer Deutschland 44 s.
    assert "rdfs:label" not in s, s


def test_der_fluss_zaehlt_nicht_mehr_ueber_unterklassen():
    s = _tmpl("geographie_fluss")["sparql"]
    assert "wdt:P31/wdt:P279* wd:Q4022" not in s
    assert "wdt:P31 wd:Q4022" in s


def test_der_fluss_nennt_die_messgroesse():
    """Das Entscheidende: P2043 ist die GESAMTlaenge, nicht die Strecke im
    Land. Ohne diesen Satz liest das Modell eine Rangliste, die es nicht
    gibt."""
    text, _, _ = _format_row("geographie_fluss", {
        "riverLabel": {"value": "Rhein"},
        "length": {"value": "1232.7"},
    })
    assert "1.232 km" in text
    assert "GESAMTlaenge" in text, text
    assert "NICHT die Strecke" in text, text


# ---------------------------------------------------------------- die Trigger

BERG_FORMEN = [
    "Welcher ist der höchste Berg Österreichs?",
    "Der höchste Berg Österreichs ist der Großglockner",
    "Österreichs höchster Berg ist der Großglockner",
    "Was ist der höchste Gipfel der Alpen?",
    "Wie heißt der höchste Berg in Deutschland?",
    "Den höchsten Berg hat Nepal",
]
FLUSS_FORMEN = [
    "Welcher ist der längste Fluss Österreichs?",
    "Der längste Fluss Österreichs ist die Donau",
    "Österreichs längster Fluss ist die Donau",
    "Welche sind die längsten Flüsse Europas?",
    "Den längsten Fluss hat Brasilien",
    "Wie lang ist der längste Fluss Deutschlands?",
]


@pytest.mark.parametrize("claim", BERG_FORMEN)
def test_jede_berg_formulierung_trifft(claim):
    """Die Auswahl vergleicht per SUBSTRING, nicht flexionstolerant —
    deshalb sechs Formulierungen statt eines Blicks in die Liste."""
    name, _ = _detect_template_for_claim(claim.lower(), {"entities": ["Österreich"]})
    assert name == "geographie_berg", claim


@pytest.mark.parametrize("claim", FLUSS_FORMEN)
def test_jede_fluss_formulierung_trifft(claim):
    """Drei dieser sechs trafen bis zum 28.10.2026 nicht."""
    name, params = _detect_template_for_claim(claim.lower(),
                                    {"entities": ["Österreich"]})
    assert name == "geographie_fluss", claim
    assert params.get("qid") == "Q40", params


# ------------------------------------------------------------- die QID-Karte

@pytest.mark.parametrize("label,qid", [
    ("Österreich", "Q40"), ("oesterreich", "Q40"), ("Austria", "Q40"),
    ("Deutschland", "Q183"), ("Schweiz", "Q39"),
])
def test_die_qid_karte_kennt_die_kernlaender(label, qid):
    assert _land_qid(label) == qid


def test_unbekanntes_land_ueberspringt_die_abfrage():
    """Besser kein Ergebnis als zwoelf Sekunden Timeout.

    Bolivien steht nicht in der Karte — dann darf die Fluss-Abfrage GAR
    NICHT laufen, statt in den Timeout zu gehen.
    """
    name, params = _detect_template_for_claim(
        "der längste fluss boliviens ist der mamoré",
        {"entities": ["Bolivien"]})
    assert name is None, (name, params)


def test_die_karte_fuehrt_keine_doppelten_qids_fuer_ein_land():
    """Schreibvarianten sind gewollt, widersprechende QIDs nicht."""
    nach_qid = {}
    for label, qid in _LAND_QID.items():
        nach_qid.setdefault(qid, []).append(label)
    # Jede QID darf mehrere Labels haben; aber kein Label zwei QIDs —
    # das faengt schon das dict. Geprueft wird die Gegenrichtung:
    # ein offensichtlicher Tippfehler wie Q40 fuer Deutschland.
    assert _land_qid("Deutschland") != _land_qid("Österreich")
    assert len(set(_LAND_QID.values())) >= 15, nach_qid


def test_der_berg_braucht_keine_qid():
    """Die Berg-Abfrage loest ueber das Label auf und ist damit fuer
    jedes Land brauchbar — sie kostet nur eine Sekunde."""
    s = _tmpl("geographie_berg")["sparql"]
    assert "{qid}" not in s
    assert "rdfs:label" in s
    name, params = _detect_template_for_claim("der höchste berg boliviens",
                                    {"entities": ["Bolivien"]})
    assert name == "geographie_berg"
    assert "qid" not in params


# ----------------------------------------------- welche Entity abgefragt wird

def test_das_land_wird_vorgezogen():
    """Der live gemessene Fluss-Fall — der Fluss stand zuerst.

    ⚠️ Mein erster Entwurf dieses Tests nahm Bolivien, den gemessenen
    BERG-Fall, und fiel durch: Bolivien steht nicht in ``_LAND_QID``, die
    Laender-Liste konnte ihn also nie loesen. Dafuer ist die UNION in der
    Berg-Abfrage da. Was die Reihenfolge wirklich entscheidet, ist der
    Fluss — er BRAUCHT eine QID.
    """
    assert _geo_kandidaten(["Donau", "Deutschland"],
                           "geographie_fluss")[0] == "Deutschland"
    assert _geo_kandidaten(["Großglockner", "Österreich"],
                           "geographie_berg")[0] == "Österreich"


def test_ohne_land_in_der_karte_bleibt_die_reihenfolge():
    """Sonst wuerde die Label-Fassung fuer Laender ausserhalb von
    _LAND_QID unbrauchbar — der Berg kostet nur eine Sekunde."""
    assert _geo_kandidaten(["Bolivien"], "geographie_berg") == ["Bolivien"]
    assert _geo_kandidaten(["Nevado Sajama", "Anden"],
                           "geographie_berg") == ["Nevado Sajama", "Anden"]


def test_andere_templates_bleiben_unberuehrt():
    """Bei person_lebensdaten ist die erste Entity die richtige."""
    assert _geo_kandidaten(["Mozart", "Österreich"],
                           "person_lebensdaten") == ["Mozart", "Österreich"]


def test_der_fluss_lief_ohne_das_land_gar_nicht():
    """2 von 5 Fluss-Formulierungen bekamen live NULL Wikidata-Zeilen.

    Gemessen am 28.10.2026 an den gespeicherten Antworten — es entscheidet
    die Reihenfolge der Analyse-Stufe:

        entities ['Donau', 'Deutschland', 'Fluss']   -> 0 Zeilen
        entities ['Deutschland', 'Fluss', 'Länge']   -> 3 Zeilen
        entities ['Österreich', 'Donau']             -> 3 Zeilen

    Stand der Fluss vorn, war ``_land_qid('Donau')`` leer und die Auswahl
    brach ab — die Abfrage lief NIE. Beide Antworten lagen trotzdem
    richtig, weil Wikipedia einsprang; belegt waren sie von Wikidata
    nicht.
    """
    claim = "die donau ist der längste fluss in deutschland."
    analyse = {"entities": ["Donau", "Deutschland", "Fluss"]}
    name, params = _detect_template_for_claim(claim, analyse)
    assert name == "geographie_fluss"
    assert params["qid"] == "Q183", params
    assert _entities_for_claim(claim, analyse, name)[0] == "Deutschland"


def test_die_abgefragten_labels_nutzen_dieselbe_reihenfolge():
    """``_detect_template_for_claim`` entscheidet nur, OB ein Template
    laeuft — abgefragt werden die Labels aus ``_entities_for_claim``.
    Vorher zogen die beiden auseinander: Die Auswahl meldete im Prod-Log
    ``{'name': 'Österreich'}``, und die Abfrage schickte weiter
    ``"Großglockner"@de`` als Land."""
    claim = "der höchste berg österreichs ist der großglockner"
    analyse = {"entities": ["Großglockner", "Österreich"]}
    name, params = _detect_template_for_claim(claim, analyse)
    assert name == "geographie_berg"
    assert params["name"] == "Österreich"
    assert _entities_for_claim(claim, analyse, name)[0] == "Österreich"


def test_der_berg_loest_auch_vom_berg_aus_auf():
    """Die UNION ist die Antwort auf den gemessenen Bolivien-Fall: Ein
    Land ausserhalb von ``_LAND_QID`` laesst sich durch keine Liste
    vorziehen, also muss die Abfrage den genannten BERG verkraften."""
    s = _tmpl("geographie_berg")["sparql"]
    assert "UNION" in s
    assert "?genannt wdt:P17 ?country" in s
    assert "?genannt wdt:P2044 []" in s, "Zweig nicht auf Erhebungen begrenzt"
    name, params = _detect_template_for_claim(
        "der höchste berg boliviens ist der nevado sajama",
        {"entities": ["Nevado Sajama", "Bolivien"]})
    assert name == "geographie_berg"
    assert "qid" not in params


def test_der_vergleich_behaelt_beide_entitaeten():
    """Bei einem Vergleichs-Claim fragt der Dienst beide Seiten ab — das
    Land-Vorziehen darf die zweite Entity nicht wegwerfen."""
    claim = "der höchste berg österreichs ist höher als der in deutschland"
    ents = _entities_for_claim(claim, {"entities": ["Österreich",
                                                    "Deutschland"]},
                               "geographie_berg")
    assert ents == ["Österreich", "Deutschland"]
