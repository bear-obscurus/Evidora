"""Die Kategorie hatte keinen deutschen Namen — also gab es sie nicht.

Live am 26.10.2026, bei der Pruefung von #278:

    Claim:   "Österreich wird vom Climate Action Tracker als kritisch
              unzureichend bewertet."
    Antwort: richtig verneint — mit der Begruendung "Die CAT-Skala kennt
              keine Kategorie 'kritisch unzureichend'."

Doch, kennt sie: ``Critically insufficient``. CAT publiziert nur englisch,
und im Prompt stand nur ``Overall 'Insufficient'``. Dem Modell fehlte jede
Zuordnung zwischen der deutschen Behauptung und der amtlichen Kategorie.
Das Label war richtig, die Begruendung falsch.

## Erst gezaehlt, und die Vermutung war zur Haelfte falsch

Vermutet hatte ich "betrifft auch BTI, MIPEX, Freedom House und Polity5".
Gemessen an den Feldern, die das Modell wirklich bekommt — der
Prompt-Whitelist aus ``services/synthesizer.py``, 33 Felder, per Regex aus
dem Code gelesen, damit die Messung nicht veraltet:

    CAT              KEINE deutsche Form
    Freedom House    KEINE deutsche Form
    Polity5          "+10 (Volle Demokratie / Full Democracy)"
    BTI              political_status "Harte Autokratie"
    V-Dem            label_de im Renderer
    RSF              Skala deutsch im Fakt
    WCAG             "Konformitätsstufe"

**2 von 7**, nicht 5 von 7.

⚠️ Und ein Messfehler auf dem Weg: Der erste Durchlauf las nur
``display_value`` und meldete 7 von 7 ohne deutsche Form — falsch fuer
fuenf Quellen, weil Polity5 die zweisprachige Form in den
``indicator_name`` schreibt und der ebenfalls in der Whitelist steht. Ohne
die Nachmessung haette ich fuenf Dienste ohne Anlass umgebaut.

## Was gefehlt hat, war nicht nur eine Uebersetzung

In ``services/freedom_house.py`` stand seit Langem:

    # Status-Übersetzung für indicator_name.
    _STATUS_DE = {"Free": "Free", "Partly Free": "Partly Free",
                  "Not Free": "Not Free"}

Eine Uebersetzungstabelle, die Englisch auf Englisch abbildete — und die
ausserdem nirgends aufgerufen wurde. Der Haken war da, die Uebersetzung
fehlte.

Und in ``services/climate_action_tracker.py`` stand die Skala als Literal
im Code, mit FUENF Stufen — waehrend die Laenderseiten sechs kennen
("1.5°C global least cost", gefunden in #276). Dieselbe Marker-Drift wie
#74/#133. Sie kommt jetzt aus den Daten.

## Exakt vergleichen, nicht enthalten

``Insufficient`` steckt als Teilstring in ``Highly insufficient`` UND in
``Critically insufficient``; ``Free`` in ``Partly Free`` und ``Not Free``.
Eine Enthaltensein-Pruefung wuerde die falsche Stufe zuordnen. Beide
Dienste vergleichen deshalb exakt — und dass das reicht, haelt ein Gate
fest: Jeder in den Daten vorkommende Wert hat einen Skala-Eintrag, und
jeder Skala-Eintrag kommt vor.

## Ein gemessenes NICHT-Finding: die Trigger bleiben, wie sie sind

Naheliegend waere, die deutschen Kategorie-Woerter auch als Trigger
aufzunehmen — "Österreichs Klimapolitik ist kritisch unzureichend"
erreicht CAT heute nicht, nur ein Claim, der den Tracker NENNT. Gemessen
ueber 12.155 echte Claims (``claimreview_index`` + die Stress-Korpora):

    kritisch unzureichend      0 Treffer
    hochgradig unzureichend    0 Treffer
    nahezu ausreichend         0 Treffer
    1,5-grad-kompatibel        0 Treffer
    nicht frei                 0 Treffer
    teilweise frei             0 Treffer

Kein Ueber-Trigger-Risiko — aber auch kein Bedarf. Ohne gemessene Nachfrage
wird die Trigger-Flaeche nicht angefasst. Die Null ist das Ergebnis, nicht
die Abwesenheit einer Messung.

Dependency-light: Daten, zwei Dienste, kein Netz.
"""
import asyncio
import json
import os
import re
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, BACKEND)

from services.climate_action_tracker import (  # noqa: E402
    _label_de as cat_label_de,
    _skala_zweisprachig as cat_skala,
    search_cat,
)
from services.freedom_house import (  # noqa: E402
    _status_de as fh_status_de,
    _skala_zweisprachig as fh_skala,
    search_freedom_house,
)


def _daten(name):
    with open(os.path.join(DATA, name), encoding="utf-8") as fh:
        return json.load(fh)


def _prompt_whitelist() -> set:
    """Die Felder, die das Modell bekommt — AUS DEM CODE gelesen.

    Abschreiben waere die Falle: Polity5 liefert seine zweisprachige Form im
    ``indicator_name``, und genau den hatte meine erste Messung nicht
    angesehen.
    """
    pfad = os.path.join(BACKEND, "services", "synthesizer.py")
    with open(pfad, encoding="utf-8") as fh:
        quelle = fh.read()
    start = quelle.index("compact = {k: v for k, v in r.items() if v and k in (")
    block = quelle[start:quelle.index(")}", start)]
    return set(re.findall(r'"([a-z_0-9]+)"', block))


def _prompt_text(fn, claim, laender=("AUT", "HUN", "DEU")) -> str:
    felder = _prompt_whitelist()
    r = asyncio.run(fn({"claim": claim, "country": laender[0],
                        "countries": list(laender)}))
    teile = []
    for row in (r.get("results") or []):
        teile += [str(v) for k, v in row.items() if k in felder and v]
    return " ".join(teile).lower()


# ------------------------------------------------- das Gate ueber ALLE Daten

def _label_strukturen():
    """Jede Struktur, deren Kinder ``label_en`` oder ``label_de`` fuehren.

    Zensus statt Liste: Der naechste Dienst, der eine Skala mitbringt, faellt
    hier automatisch mit hinein. KEINE Ausnahmeliste (#230).
    """
    raus = []
    for name in sorted(os.listdir(DATA)):
        if not name.endswith(".json"):
            continue
        try:
            d = _daten(name)
        except Exception:
            continue
        stapel = [(d, "")]
        while stapel:
            o, pfad = stapel.pop()
            if isinstance(o, dict):
                kinder = [(k, v) for k, v in o.items() if isinstance(v, dict)]
            elif isinstance(o, list):
                kinder = [(str(i), v) for i, v in enumerate(o)
                          if isinstance(v, dict)]
            else:
                kinder = []
            if kinder and any("label_en" in v or "label_de" in v
                              for _, v in kinder):
                raus.append((name, pfad or "(root)", kinder))
            if isinstance(o, dict):
                for k, v in o.items():
                    if isinstance(v, (dict, list)):
                        stapel.append((v, f"{pfad}.{k}" if pfad else k))
            elif isinstance(o, list):
                for v in o:
                    if isinstance(v, (dict, list)):
                        stapel.append((v, f"{pfad}[]"))
    return raus


def test_der_zensus_findet_die_bekannten_strukturen():
    """Faellt diese Pruefung, sucht der Zensus an der falschen Stelle."""
    gefunden = {(n, p) for n, p, _ in _label_strukturen()}
    for erwartet in (("climate_action_tracker.json", "rating_scale"),
                     ("freedom_house.json", "status_categories"),
                     ("polity5.json", "regime_categories"),
                     ("vdem_indicators.json", "indicators")):
        assert erwartet in gefunden, f"{erwartet} nicht gefunden: {gefunden}"


def test_jede_kategorie_hat_beide_sprachen():
    """Wer eine Sprache fuehrt, fuehrt beide — ueber alle Datendateien."""
    luecken = []
    for name, pfad, kinder in _label_strukturen():
        for k, v in kinder:
            if ("label_en" in v or "label_de" in v) and not (
                    v.get("label_en") and v.get("label_de")):
                luecken.append(f"{name}:{pfad}.{k}")
    assert not luecken, luecken


# ------------------------------------------------------------------- CAT

def _cat_rating_werte():
    d = _daten("climate_action_tracker.json")
    werte = set()
    stapel = [d]
    while stapel:
        o = stapel.pop()
        if isinstance(o, dict):
            for k, v in o.items():
                if k.endswith("_rating") and isinstance(v, str):
                    werte.add(v)
                elif isinstance(v, (dict, list)):
                    stapel.append(v)
        elif isinstance(o, list):
            stapel.extend(o)
    return werte


def test_jeder_cat_wert_loest_auf():
    """Der EXAKTE Vergleich reicht nur, wenn die Werte zur Skala passen."""
    d = _daten("climate_action_tracker.json")
    ohne = sorted(w for w in _cat_rating_werte() if not cat_label_de(w, d))
    assert not ohne, ohne


def test_kein_cat_skala_eintrag_liegt_brach():
    """Eine Stufe, die nirgends vorkommt, ist ein Hinweis auf Drift."""
    d = _daten("climate_action_tracker.json")
    labels = {e["label_en"] for e in d["rating_scale"].values()}
    assert not sorted(labels - _cat_rating_werte())


@pytest.mark.parametrize("rating,erwartet", [
    ("Insufficient", "unzureichend"),
    ("Highly insufficient", "hochgradig unzureichend"),
    ("Critically insufficient", "kritisch unzureichend"),
    ("Almost sufficient", "nahezu ausreichend"),
])
def test_cat_ordnet_exakt_zu(rating, erwartet):
    """Giftprobe gegen Teiltreffer: 'Insufficient' steckt in den anderen."""
    d = _daten("climate_action_tracker.json")
    assert cat_label_de(rating, d) == erwartet


def test_cat_skala_kommt_aus_den_daten():
    d = _daten("climate_action_tracker.json")
    text = cat_skala(d)
    for e in d["rating_scale"].values():
        assert e["label_en"] in text
        assert e["label_de"] in text


def test_die_sechste_cat_stufe_ist_dabei():
    """Das alte Code-Literal zaehlte fuenf — #276 fand die sechste."""
    d = _daten("climate_action_tracker.json")
    assert "1.5°C global least cost" in cat_skala(d)


def test_kein_hartcodiertes_cat_skala_literal_mehr():
    """Die Aufzaehlung im Code hat den Daten-Refresh stumm ueberlebt."""
    pfad = os.path.join(BACKEND, "services", "climate_action_tracker.py")
    with open(pfad, encoding="utf-8") as fh:
        quelle = fh.read()
    assert "'Almost sufficient' / " not in quelle


# ----------------------------------------------------------- Freedom House

@pytest.mark.parametrize("status,erwartet", [
    ("Free", "frei"),
    ("Partly Free", "teilweise frei"),
    ("Not Free", "nicht frei"),
])
def test_fh_ordnet_exakt_zu(status, erwartet):
    """Giftprobe: 'Free' steckt in 'Partly Free' und 'Not Free'."""
    d = _daten("freedom_house.json")
    assert fh_status_de(status, d) == erwartet


def test_jeder_fh_status_loest_auf():
    d = _daten("freedom_house.json")
    werte = {r.get("status") for r in d["ratings"].values() if r.get("status")}
    werte |= {r.get("vorjahr_status") for r in d["ratings"].values()
              if r.get("vorjahr_status")}
    ohne = sorted(w for w in werte if not fh_status_de(w, d))
    assert not ohne, ohne


def test_die_uebersetzungstabelle_uebersetzt_wirklich():
    """Bis zum 26.10.2026 bildete sie Englisch auf Englisch ab."""
    pfad = os.path.join(BACKEND, "services", "freedom_house.py")
    with open(pfad, encoding="utf-8") as fh:
        quelle = fh.read()
    assert '"Free": "Free"' not in quelle
    assert '"Partly Free": "Partly Free"' not in quelle


def test_fh_fallback_traegt_ohne_daten():
    """Ohne ``status_categories`` muss die Zuordnung trotzdem stehen."""
    assert fh_status_de("Partly Free", {}) == "teilweise frei"
    assert fh_status_de("Not Free", {}) == "nicht frei"


def test_fh_skala_kommt_aus_den_daten():
    d = _daten("freedom_house.json")
    text = fh_skala(d)
    for e in d["status_categories"].values():
        assert e["label_en"] in text
        assert e["label_de"] in text


# ------------------------------- das eigentliche Gate: was im Prompt landet

@pytest.mark.parametrize("form", [
    "kritisch unzureichend", "hochgradig unzureichend", "unzureichend",
    "nahezu ausreichend", "1,5-grad-kompatibel",
])
def test_die_cat_kategorien_erreichen_den_prompt_deutsch(form):
    text = _prompt_text(
        search_cat,
        "Österreich wird vom Climate Action Tracker als kritisch "
        "unzureichend bewertet")
    assert form in text


@pytest.mark.parametrize("form", ["Insufficient", "Critically insufficient"])
def test_die_amtliche_cat_form_bleibt_im_prompt(form):
    """Der englische Name ist der amtliche und darf nicht verschwinden."""
    text = _prompt_text(
        search_cat,
        "Österreich wird vom Climate Action Tracker als kritisch "
        "unzureichend bewertet")
    assert form.lower() in text


@pytest.mark.parametrize("form", ["teilweise frei", "nicht frei", "frei"])
def test_die_fh_stufen_erreichen_den_prompt_deutsch(form):
    text = _prompt_text(search_freedom_house,
                        "Ungarn ist laut Freedom House nur teilweise frei",
                        ("HUN", "AUT"))
    assert form in text


@pytest.mark.parametrize("form", ["Partly Free", "Not Free"])
def test_die_amtliche_fh_form_bleibt_im_prompt(form):
    text = _prompt_text(search_freedom_house,
                        "Ungarn ist laut Freedom House nur teilweise frei",
                        ("HUN", "AUT"))
    assert form.lower() in text


def test_die_whitelist_enthaelt_den_indicator_name():
    """Der Messfehler, der die Zaehlung zuerst verdorben hat.

    Polity5 liefert seine zweisprachige Form im ``indicator_name``. Wer nur
    ``display_value`` prueft, meldet fuenf Dienste als luckenhaft, die es
    nicht sind.
    """
    felder = _prompt_whitelist()
    assert "indicator_name" in felder
    assert "display_value" in felder
    assert len(felder) > 20, felder


def test_der_sprachhinweis_steht_in_beiden_dateien():
    """Beide Quellen publizieren nur englisch — das muss dastehen, damit
    niemand die deutsche Form fuer den amtlichen Namen haelt."""
    for name in ("climate_action_tracker.json", "freedom_house.json"):
        hinweis = _daten(name).get("kategorien_sprachhinweis", "")
        assert "nur" in hinweis.lower()
        assert "englisch" in hinweis.lower()
        assert "amtlich" in hinweis.lower()


# ---------------------------------------------- Feldgrenzen der Ausgabe

@pytest.mark.parametrize("fn,claim,laender", [
    (search_cat, "Wie bewertet der Climate Action Tracker Österreich?",
     ("AUT",)),
    (search_freedom_house, "Ist Ungarn laut Freedom House frei?",
     ("HUN", "AUT")),
])
def test_die_ausgabe_bleibt_im_budget(fn, claim, laender):
    """Nicht ueber das Prompt-Budget, sonst schneidet es die deutsche Form.

    Die Grenze kommt aus ``services.synthesizer``, nicht aus einer Zahl in
    diesem Test — eine erfundene 200 haette hier zuerst angeschlagen.
    """
    from services.synthesizer import PROMPT_MAX_STR
    r = asyncio.run(fn({"claim": claim, "country": laender[0],
                        "countries": list(laender)}))
    for row in (r.get("results") or []):
        assert len(row.get("display_value", "")) <= 480
        assert len(row.get("indicator_name", "")) <= PROMPT_MAX_STR
