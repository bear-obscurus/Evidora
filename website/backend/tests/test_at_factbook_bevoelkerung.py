"""Contract-Tests für den Bevölkerungs-Fakt im AT-Factbook (Refresh 2026-07-11).

Kontext: Die Bundesländer-Werte waren bis Juli 2026 vorläufig (PM 09.02.2026)
bzw. approximiert — der Weekly-Drift fand Wien mit 33,4 % statt real 37,0 %
(vorläufig) / 36,8 % (endgültig). Der Refresh 2026-07-11 zog den Fakt auf die
ENDGÜLTIGEN Ergebnisse (Statistik Austria PM 14 212-131/26 vom 29.06.2026 +
Zeitreihen-/Gebietseinheiten-ODS, eine Vintage).

Diese Suite pinnt die ARITHMETISCHEN Invarianten (Summen-Beweis, Anteils-
Konsistenz, Rang-Monotonie) — sie bricht, wenn künftige Refreshes
approximierte oder in sich widersprüchliche Werte einschleusen, ohne bei
jedem Jahres-Refresh angefasst werden zu müssen. Nur der Stichtags-Block
unten pinnt konkrete 1.1.2026-Werte und wandert beim nächsten Refresh mit.

Dependency-light: JSON only, kein Netz/LLM.
"""

import json
import os

import pytest

_DATA = os.path.join(
    os.path.dirname(__file__), "..", "data", "at_factbook.json"
)


def _fact() -> dict:
    d = json.load(open(_DATA, encoding="utf-8"))
    for f in d["facts"]:
        if f["id"] == "staatsbuergerschaft_bevoelkerung_2026":
            return f
    raise AssertionError("staatsbuergerschaft_bevoelkerung_2026 fehlt")


# --- Invarianten (refresh-stabil: gelten für JEDEN künftigen Datenstand) ---

def test_summen_beweis_bundeslaender():
    """Σ Bundesländer == Österreich gesamt, für gesamt UND Nicht-AT.
    Der Summen-Beweis fing 2026-07-06 die approximierten Wien-Werte."""
    data = _fact()["data"]
    bl = data["bundeslaender_anteil_nicht_at_pct"]
    assert len(bl) == 9
    assert sum(b["einwohner_gesamt"] for b in bl) == data["bevoelkerung_gesamt"]
    assert sum(b["absolut"] for b in bl) == data["bevoelkerung_nicht_at_staatsbuerger"]


def test_at_plus_nicht_at_ist_gesamt():
    data = _fact()["data"]
    assert (data["bevoelkerung_at_staatsbuerger"]
            + data["bevoelkerung_nicht_at_staatsbuerger"]
            == data["bevoelkerung_gesamt"])


def test_anteile_konsistent_mit_absolutwerten():
    """Publizierte 1-Dezimal-Anteile müssen zur eigenen Arithmetik passen
    (±0,05 pp) — fängt Zufallszeilen-/Approximations-Drift."""
    data = _fact()["data"]
    for b in data["bundeslaender_anteil_nicht_at_pct"]:
        calc = b["absolut"] / b["einwohner_gesamt"] * 100
        assert abs(calc - b["anteil_pct"]) <= 0.05, (b["land"], calc)
    gesamt_calc = (data["bevoelkerung_nicht_at_staatsbuerger"]
                   / data["bevoelkerung_gesamt"] * 100)
    assert abs(gesamt_calc - data["anteil_nicht_at_pct"]) <= 0.05


def test_raenge_streng_monoton_nach_exaktem_anteil():
    """Ränge folgen den EXAKTEN Anteilen (absolut/gesamt), nicht den
    gerundeten — Salzburg (20,947 %) vor Vorarlberg (20,930 %), obwohl
    beide publiziert auf 20,9 % runden."""
    bl = _fact()["data"]["bundeslaender_anteil_nicht_at_pct"]
    nach_exakt = sorted(bl, key=lambda b: -(b["absolut"] / b["einwohner_gesamt"]))
    assert [b["rang"] for b in nach_exakt] == list(range(1, 10)), \
        [(b["land"], b["rang"]) for b in nach_exakt]


def test_top10_monoton_und_anteile_konsistent():
    data = _fact()["data"]
    t10 = data["top_10_herkunftslaender_nicht_at_2026"]
    assert len(t10) == 10
    anz = [t["anzahl"] for t in t10]
    assert anz == sorted(anz, reverse=True)
    assert [t["rang"] for t in t10] == list(range(1, 11))
    nicht_at = data["bevoelkerung_nicht_at_staatsbuerger"]
    for t in t10:
        calc = t["anzahl"] / nicht_at * 100
        assert abs(calc - t["anteil_pct_an_nicht_at"]) <= 0.05, (t["land"], calc)


def test_trend_monoton_und_endpunkt_ist_gesamtwert():
    data = _fact()["data"]
    tr = data["historical_trend_anteil_nicht_at_pct"]
    assert all(tr[i]["absolut"] < tr[i + 1]["absolut"] for i in range(len(tr) - 1))
    assert tr[-1]["absolut"] == data["bevoelkerung_nicht_at_staatsbuerger"]
    assert tr[-1]["anteil_pct"] == data["anteil_nicht_at_pct"]


def test_wien_ueber_einem_drittel():
    """Invariante des Weekly-Drift-Claims 'mehr als jeder dritte Wiener':
    kippt Wien je unter 33,3 %, muss der Drift-Expected mitgeändert werden
    — dieser Test macht das zur bewussten Entscheidung statt stillem Drift."""
    bl = _fact()["data"]["bundeslaender_anteil_nicht_at_pct"]
    wien = next(b for b in bl if b["land"] == "Wien")
    assert wien["rang"] == 1
    assert wien["anteil_pct"] > 100 / 3


# --- Stichtags-Pins 1.1.2026 endgültig (beim Jahres-Refresh mitziehen) ---

def test_endgueltige_werte_1_1_2026():
    """Endgültige Ergebnisse, Statistik Austria PM 14 212-131/26 (29.06.2026),
    selbst aus PM-Tabelle 1 + Zeitreihen-ODS verifiziert (Session 2026-07-11)."""
    data = _fact()["data"]
    assert data["bevoelkerung_gesamt"] == 9215956
    assert data["bevoelkerung_nicht_at_staatsbuerger"] == 1881309
    assert data["bevoelkerung_at_staatsbuerger"] == 7334647
    assert data["anteil_nicht_at_pct"] == 20.4
    wien = next(b for b in data["bundeslaender_anteil_nicht_at_pct"]
                if b["land"] == "Wien")
    assert (wien["anteil_pct"], wien["absolut"], wien["einwohner_gesamt"]) \
        == (36.8, 751778, 2040914)


def test_status_note_kennzeichnet_endgueltig():
    """Die Status-Note muss den Datenstand als endgültig ausweisen und den
    nächsten Refresh-Termin nennen — ersetzt die alte 'vorläufig'-Note."""
    notes = " ".join(_fact()["context_notes"])
    assert "ENDGÜLTIGE Ergebnisse" in notes
    assert "22.06.2027" in notes
    assert "vorläufige Ergebnisse (Statistik Austria PM 09.02.2026)" not in notes


def test_ukraine_in_top10_und_trigger_kennt_ukrain():
    """Ukraine ist endgültig Rang 9 (94.030) — war in der approximierten
    Top-10 komplett vergessen. Der Service-Trigger muss 'ukrain' kennen,
    sonst erreichen Ukrainer:innen-Claims das Ranking nie."""
    t10 = _fact()["data"]["top_10_herkunftslaender_nicht_at_2026"]
    ukraine = next((t for t in t10 if t["land"] == "Ukraine"), None)
    assert ukraine is not None and ukraine["anzahl"] == 94030
    svc = open(os.path.join(os.path.dirname(__file__), "..",
                            "services", "at_factbook.py"),
               encoding="utf-8").read()
    assert '"ukrain"' in svc


# --- Gate + Rendering (Live-Befunde 2026-07-11 nach dem Refresh) ---

@pytest.mark.parametrize("claim", [
    "Wie viele Ukrainer leben in Österreich?",
    "Wie viele Syrer gibt es in Wien?",
    "Anzahl der Rumänen in Österreich",
    "Wieviele Türken wohnen in Österreich?",
])
def test_citizenship_gate_erkennt_bestandsfragen(claim):
    """'Wie viele <Nationalität> leben in AT?' erreichte das Topic-Gate
    nicht (Live: unverifiable@0.1 trotz Top-10-Daten). Trigger-Gate ist
    der Türsteher — die Intelligenz dahinter kam nie zum Zug."""
    from services.at_factbook import _claim_mentions_citizenship
    assert _claim_mentions_citizenship(claim.lower()), claim


@pytest.mark.parametrize("claim", [
    "Wie viele Metropolen gibt es in Österreich?",   # ' polen'-Substring-Falle
    "Wie viele Deutsche leben in Deutschland?",       # kein AT-Kontext
    "Wie viele Einwohner hat Österreich?",            # keine Nationalität
])
def test_citizenship_gate_bestandsfragen_negativ(claim):
    from services.at_factbook import _claim_mentions_citizenship
    assert not _claim_mentions_citizenship(claim.lower()), claim


def test_bundeslaender_ranking_disambiguiert_rundungs_ties():
    """Salzburg (20,947 %) und Vorarlberg (20,930 %) runden beide auf
    20,9 % — ohne exakte Werte folgerte der Synthesizer 'geteilter Platz 2'
    und kippte einen korrekten 'Salzburg ist Nr. 2'-Claim auf false@0.95
    (Live-Befund 2026-07-11). Das Ranking muss Ränge + exakte Prozente
    für Rundungs-Ties tragen."""
    from services.at_factbook import _build_citizenship_results
    results = _build_citizenship_results(
        _fact(),
        "salzburg hat nach wien den höchsten ausländeranteil aller bundesländer",
    )
    blk = next(r for r in results
               if r["indicator_name"].startswith("Anteil Nicht-AT-Staatsbürger"))
    desc = blk["description"]
    assert "#2 Salzburg" in desc and "#3 Vorarlberg" in desc, desc
    assert "exakt 20,95" in desc and "exakt 20,93" in desc, desc
    # Nicht-Tie-Länder bleiben kompakt (400-Zeichen-Budget)
    assert "exakt 36," not in desc, desc


def test_rang_satz_im_display_bei_genanntem_bundesland():
    """Zahlen im description reichen NICHT: der Synthesizer las live
    'Salzburg 20,95 / Vorarlberg 20,93' und folgerte trotzdem 'Vorarlberg
    knapp vor Salzburg' (Zweite-Dezimale-Vergleichsfehler, 2026-07-11).
    Analog zur Drittel-Arithmetik muss die Rang-Aussage als fertiger Satz
    im display_value stehen — lesen statt rechnen."""
    from services.at_factbook import _build_citizenship_results
    results = _build_citizenship_results(
        _fact(),
        "salzburg hat nach wien den höchsten ausländeranteil aller bundesländer",
    )
    blk = next(r for r in results
               if r["indicator_name"].startswith("Anteil Nicht-AT-Staatsbürger"))
    disp = blk["display_value"]
    assert "Salzburg liegt auf Rang 2 von 9" in disp, disp
    assert "knapp VOR Vorarlberg" in disp, disp
    assert len(disp) <= 400, len(disp)  # überlebt die Prompt-Truncation
    # Wien-Drittel-Pfad bleibt unberührt (kein fremder Rang-Satz)
    drittel = _build_citizenship_results(
        _fact(), "in wien ist mehr als jeder dritte einwohner ausländer")
    dblk = next(r for r in drittel
                if r["indicator_name"].startswith("Anteil Nicht-AT-Staatsbürger"))
    assert "liegt auf Rang" not in dblk["display_value"]
