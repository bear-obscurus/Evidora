"""Zwei Fakten, ein ``topic`` — und zwei verschiedene Zahlen fuer dieselbe Sache.

PR #233 gab den beiden Gender-Pay-Gap-Fakten getrennte IDs und schrieb den
Rest ausdruecklich offen:

    "Beide Fakten tragen weiterhin dasselbe ``topic``
     (``gender_pay_gap_konsens``). Das ist harmlos, solange beide eine eigene
     ``id`` haben […] Anders als die ID landet ``topic`` aber IM Ergebnis und
     damit im Prompt; es umzubenennen waere eine Verhaltensaenderung und
     braeuchte ihre eigene Messung."

Beim Nachsehen am 12.10.2026 war es nicht harmlos — nur nicht aus dem
erwarteten Grund. Die beiden Fakten trugen WIDERSPRUECHLICHE Zahlen unter
demselben Namen:

    Groesse          arbeitsmarkt   gleichstellung   Eurostat 2024
    EU-27                12,7 %         12,7 %          11,1 %
    DE                   17,6 %         18,0 %          15,6 %
    AT                   18,4 %         18,4 %          17,6 %
    Italien               5,0 %           ~3 %           5,3 %
    Luxemburg             0,7 %           ~3 %          -0,8 %
    Lettland                 —          20,9 %          13,9 %

Nicht nur untereinander verschieden, sondern beide falsch. Gemessen hat das
live geschadet: "Der Gender Pay Gap in Deutschland liegt bei 18 Prozent"
bekam ``true @ 0,85`` mit der Begruendung "lag 2023 bei 18,0 % (Eurostat)".
Eurostat weist fuer 2023 17,6 % aus und fuer 2024 15,6 %. Eine falsche Zahl
mit einer benannten Quelle davor.

Das ist [[lessons_learned]] "eine Zahl in zwei Kopien driftet unbemerkt",
woertlich: Der doppelte ``topic`` hat nicht den Schaden verursacht, aber er
hat verdeckt, dass zwei Quellen antworten.

## Die Pruefung

Die Zahlen kommen aus der Primaerquelle und sind doppelt gegengeprueft:
Eurostat ``sdg_05_20`` ueber die Dissemination-API (Datenstand 26.02.2026)
und, unabhaengig davon, die Genderstatistik von Statistik Austria vom
3.3.2026 — beide nennen AT 17,6 %, EU-27 11,1 % und Oesterreich auf Platz 3
hinter Estland 18,8 % und Tschechien 18,5 %.

## Was bewusst NICHT geaendert wurde

Die BEREINIGTEN Werte. Sie stammen aus anderen Quellen (DESTATIS-Zerlegung,
WIFO-Schaetzung, Heinze/Wolf) und nicht von Eurostat. DESTATIS bestaetigt 6 %
fuer 2024 — der Wert im Fakt war richtig und bleibt. Die AT-Schaetzung von
~5,5 % (WIFO) ist nicht nachgeprueft und bleibt mit ihrer Zuschreibung
stehen: nichts ersetzen, was man nicht belegen kann.

Dependency-light: nur Daten, kein Netz, kein Modell.
"""
import collections
import copy
import glob
import json
import os

import pytest

from services._satzgrenzen import teile_in_einheiten

DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "data")


def _alle_fakten():
    for p in sorted(glob.glob(os.path.join(DATA, "*.json"))):
        try:
            with open(p, encoding="utf-8") as fh:
                d = json.load(fh)
        except Exception:
            continue
        if not isinstance(d, dict):
            continue
        for f in d.get("facts") or []:
            if isinstance(f, dict):
                yield os.path.basename(p), f


ALLE = list(_alle_fakten())


def _fakt(datei, kennung):
    for d, f in ALLE:
        if d == datei and (f.get("id") or f.get("topic")) == kennung:
            return f
    raise AssertionError(f"{kennung} nicht in {datei}")


def _text(f):
    return json.dumps(f, ensure_ascii=False)


# --- Das Gate ueber ALLE Fakten ---------------------------------------

def test_kein_topic_ist_doppelt_belegt():
    """Census als Gate, ohne Ausnahmeliste.

    ``topic`` geht in das Ergebnis-Dict und damit in den Prompt. Zwei
    verschiedene Fakten unter demselben Namen sehen dort aus wie EINE
    Quelle, die sich selbst widerspricht — genau so entstand der
    Gender-Pay-Gap-Fall. Eine Liste der Fundstellen waere in zwei Wochen
    veraltet; dieses Gate rechnet sie jedes Mal neu
    ([[lessons_learned]]: der Zensus als Gate, nicht die Liste).
    """
    wo = collections.defaultdict(list)
    for datei, f in ALLE:
        if f.get("topic"):
            wo[f["topic"]].append((datei, f.get("id")))
    doppelt = {k: v for k, v in wo.items() if len(v) > 1}
    assert not doppelt, f"{len(doppelt)} doppelt belegte topics: {doppelt}"


def test_das_gate_schlaegt_an():
    """Giftprobe: ohne sie beweist das Gate oben nur, dass es laeuft."""
    wo = collections.defaultdict(list)
    zwilling = copy.deepcopy(ALLE[0][1])
    for datei, f in ALLE + [("gift.json", zwilling)]:
        if f.get("topic"):
            wo[f["topic"]].append(datei)
    assert any(len(v) > 1 for v in wo.values())


def test_alle_fakten_sind_erfasst():
    """Faengt ein leeres Ergebnis durch einen Lesefehler ab."""
    assert len(ALLE) > 600, len(ALLE)
    mit_topic = [f for _d, f in ALLE if f.get("topic")]
    assert len(mit_topic) > 600, len(mit_topic)


# --- Die zwei aufgeloesten Faelle --------------------------------------

def test_gender_pay_gap_topics_sind_getrennt():
    a = _fakt("arbeitsmarkt_pack.json", "gender_pay_gap_bereinigt_2026")
    b = _fakt("gleichstellung_pack.json", "gender_pay_gap_2026")
    assert a["topic"] == "gender_pay_gap_bereinigt_konsens"
    assert b["topic"] == "gender_pay_gap_konsens"
    assert a["topic"] != b["topic"]


def test_vpn_topics_sind_getrennt():
    """Umbenannt wurde der Eintrag MIT eigener ``id``.

    Der cybersecurity-Eintrag hat keine und lebt von seinem ``topic`` — das
    umzubenennen haette seine Kennung geaendert. Dieselbe Ueberlegung wie
    bei der ID-Umbenennung in #233.
    """
    tech = _fakt("tech_ki_pack.json", "vpn_anonymitaet_2026")
    assert tech["topic"] == "vpn_anonymitaet_angriffsflaechen"
    cyber = [f for d, f in ALLE
             if d == "cybersecurity_pack.json"
             and f.get("topic") == "vpn_anonymitaet_mythos"]
    assert len(cyber) == 1
    assert cyber[0].get("id") is None, (
        "Der cybersecurity-Eintrag hat jetzt eine id — dann darf sein "
        "topic umbenannt werden, und diese Begruendung ist veraltet")


def test_die_vpn_fakten_sind_wirklich_zwei_verschiedene():
    """Waeren sie inhaltsgleich, haette einer geloescht gehoert statt
    umbenannt — dieselbe Pruefung wie #233 fuer das andere Paar."""
    tech = _fakt("tech_ki_pack.json", "vpn_anonymitaet_2026")
    cyber = [f for d, f in ALLE
             if d == "cybersecurity_pack.json"
             and f.get("topic") == "vpn_anonymitaet_mythos"][0]
    assert tech.get("headline") != cyber.get("headline")
    assert tech.get("source_url") != cyber.get("source_url")


# --- Die Zahlen, gegen die Primaerquelle gepinnt -----------------------

#: Eurostat ``sdg_05_20`` (unadjusted), Jahr 2024, Datenstand 26.02.2026.
#: Unabhaengig bestaetigt durch die Genderstatistik von Statistik Austria
#: vom 3.3.2026 (AT 17,6 %, EU-27 11,1 %, AT auf Platz 3).
EUROSTAT_2024 = {
    "EU-27": "11,1", "AT": "17,6", "DE": "15,6",
    "Estland": "18,8", "Tschechien": "18,5", "Luxemburg": "-0,8",
}

#: Werte, die der Fakt frueher trug und die gegen diese Reihe falsch sind.
#: Sie duerfen in den beiden Fakten nicht mehr als Eurostat-Zahl stehen.
FRUEHER_FALSCH = ("12,7", "18,4", "18,0 %", "20,9")

GPG_FAKTEN = (("arbeitsmarkt_pack.json", "gender_pay_gap_bereinigt_2026"),
              ("gleichstellung_pack.json", "gender_pay_gap_2026"))


@pytest.mark.parametrize("datei,kennung", GPG_FAKTEN)
def test_eurostat_zahlen_stehen_im_fakt(datei, kennung):
    t = _text(_fakt(datei, kennung))
    for name in ("EU-27", "AT", "DE"):
        assert EUROSTAT_2024[name] in t, f"{kennung}: {name} fehlt"
    assert "2024" in t


#: Statistische Stellen, die EIGENE Werte veroeffentlichen und deshalb eine
#: von Eurostat abweichende Zahl tragen duerfen.
ANDERE_STELLEN = ("AK Wien", "BFS", "DESTATIS", "Statistisches Bundesamt",
                  "Statistik Austria", "WIFO", "Heinze")


@pytest.mark.parametrize("datei,kennung", GPG_FAKTEN)
def test_die_alten_zahlen_stehen_nur_mit_eigener_quelle(datei, kennung):
    """Keine Segensliste, sondern eine pruefbare Regel.

    Eine erste Fassung verbot die alten Zeichenketten pauschal — und fiel
    sofort auf einen legitimen Fall: "CH 2022: 18,0 % unbereinigt nach
    Lohnstrukturerhebung BFS". Dieselbe Ziffernfolge, anderes Land,
    andere Stelle. Verboten ist also nicht die ZAHL, sondern die Zahl
    OHNE eine Stelle, die sie veroeffentlicht.

    Geprueft wird SATZWEISE, nicht je Feld. Eine Giftprobe am 12.10.2026
    zeigte, warum: Mit "12,7 %" zurueck im ``kernsatz`` blieb der Test
    gruen, weil derselbe Kernsatz weiter unten DESTATIS und Statistik
    Austria nennt. Ein Kernsatz ist kein Feld, sondern ein Dokument — die
    Zuschreibung muss im selben Satz stehen wie die Zahl. Dieselbe Lehre
    wie #227: wer Saetze nicht trennt, macht alle Satz-Wachen blind.

    Zwei Werte bleiben damit zu Recht stehen:

      * 18,4 % bei der AK Wien, die ihren Equal-Pay-Day 2024 darauf
        gerechnet hat — ohne die Zahl ist das Datum nicht
        nachvollziehbar. Dieselbe Loesung wie bei den Korrekturnotizen
        (#135/#136): die alte Zahl darf bleiben, wenn der Satz sagt,
        wessen Zahl sie ist.
      * 18,0 % fuer die Schweiz 2022 nach BFS-Lohnstrukturerhebung. Nicht
        nachgeprueft, aus dem Bestand uebernommen und mit Zuschreibung
        belassen — nichts ersetzen, was man nicht belegen kann.
    """
    f = _fakt(datei, kennung)
    for feld, wert in list((f.get("data") or {}).items()) + list(f.items()):
        if not isinstance(wert, str):
            continue
        for satz in teile_in_einheiten(wert):
            for falsch in FRUEHER_FALSCH:
                if falsch not in satz:
                    continue
                assert any(q in satz for q in ANDERE_STELLEN), (
                    f"{kennung}.{feld}: {falsch!r} steht ohne eine Stelle, "
                    f"die diesen Wert veroeffentlicht — im Satz "
                    f"{satz.strip()[:110]!r}")


@pytest.mark.parametrize("datei,kennung", GPG_FAKTEN)
def test_die_regel_faengt_eine_wieder_eingesetzte_eurostat_zahl(datei, kennung):
    """Die Giftprobe IM Test, nicht nur als Notiz daneben.

    Gesetzt wird die alte Zahl in den echten Kernsatz des echten Fakts —
    genau dort, wo die erste Fassung sie durchliess.
    """
    f = copy.deepcopy(_fakt(datei, kennung))
    ks = f["data"]["kernsatz_fuer_synthesizer"]
    assert "11,1 %" in ks, f"{kennung}: Ankersatz nicht gefunden"
    f["data"]["kernsatz_fuer_synthesizer"] = ks.replace("11,1 %", "12,7 %", 1)
    gefunden = False
    for satz in teile_in_einheiten(f["data"]["kernsatz_fuer_synthesizer"]):
        if any(x in satz for x in FRUEHER_FALSCH) and not any(
                q in satz for q in ANDERE_STELLEN):
            gefunden = True
    assert gefunden, "Die Regel laesst die wieder eingesetzte Zahl durch"


@pytest.mark.parametrize("datei,kennung", GPG_FAKTEN)
def test_jede_zahl_traegt_ihre_quelle(datei, kennung):
    """Der gemessene Schaden war nicht die Zahl, sondern "18,0 %
    (Eurostat)" — eine nationale Zahl mit der falschen Quelle davor.
    Wo Eurostat steht, muss der Datenstand dabeistehen."""
    t = _text(_fakt(datei, kennung))
    assert "sdg_05_20" in t, f"{kennung}: Eurostat-Tabelle nicht benannt"
    assert "26.02.2026" in t, f"{kennung}: Datenstand fehlt"


def test_zwei_messwege_sind_benannt():
    """DESTATIS 16 % und Eurostat 15,6 % sind BEIDE richtig fuer DE 2024 —
    verschiedene Methodik. Der Fakt muss das sagen, sonst sieht die
    Differenz wie ein Fehler aus und das Modell waehlt eine Zahl."""
    for datei, kennung in GPG_FAKTEN:
        t = _text(_fakt(datei, kennung))
        assert "DESTATIS" in t, kennung
        assert "16 %" in t, f"{kennung}: nationale DE-Zahl fehlt"


def test_bereinigte_werte_sind_unangetastet():
    """DESTATIS bestaetigt 6 % fuer 2024 — der Wert war richtig. Die
    WIFO-Schaetzung ~5,5 % ist nicht nachgeprueft und bleibt mit ihrer
    Zuschreibung stehen."""
    a = _text(_fakt(*GPG_FAKTEN[0]))
    b = _text(_fakt(*GPG_FAKTEN[1]))
    assert "5,5 %" in a and "WIFO" in a
    assert "6 %" in b and "12,3 %" in b
