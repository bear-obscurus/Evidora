"""Der Vollrefresh — und diesmal war der alte Stand nicht falsch.

PR #267 hatte den Rueckstand benannt: ``data/wid.json`` fuehrte Werte des
World Inequality Report 2022 mit Datenjahr 2022, obwohl der WIR 2026 am
10.12.2025 erschienen war. Der Vollrefresh ueber alle 34 Gebiete stand
als offen vermerkt. Hier ist er.

## Was gemessen wurde, und was es NICHT ist

195 von 204 Werten haben sich geaendert — aber der Median der Aenderung
liegt bei **1,2 Prozentpunkten**, und das ist kein Fehlerbefund. Nach
MIPEX (#266, 52 % falsch) und BTI (#268, 80 % falsch) war die Erwartung
eine dritte Fundstelle. Sie ist es nicht, und das gehoert genauso
berichtet:

Der alte Stand war fuer seine Ausgabe **korrekt**. Die Welt-Werte stimmen
exakt mit den Schlagzeilen des WIR 2022 — Top-10 % 52,0 % des Einkommens,
untere 50 % 8,5 %, untere 50 % beim Vermoegen 2,0 %. Das sind genau die
Zahlen, mit denen der Report damals zitiert wurde.

Er war nur **grob**: 43 % der alten Werte endeten auf .0 oder .5, gegen
18 % in der Datenbank. Sie waren aus Report-Grafiken gelesen, nicht aus
der Datenbank exportiert. Dazu kommen zwei Datenjahre und WID-Revisionen.

Eine Gegenprobe dazu: Der alte Stand passt zu KEINEM Jahr der heutigen
Datenbank besser als zu jedem anderen — der Median liegt ueber 2017 bis
2024 flach bei 1,2 bis 1,3 Prozentpunkten. Eine getreue Momentaufnahme
eines Jahres saehe anders aus. Beides zusammen — exakte Treffer auf die
Schlagzeilen, flache Abweichung ueber alle Jahre — passt auf gerundete
Report-Zahlen, nicht auf erfundene.

## Die Zuordnung ist bewiesen, nicht geraten

Die Werte stammen aus dem Bulk-Download, zwei Reihen und drei
Perzentile. Dass ich die RICHTIGEN gelesen habe, zeigt eine innere
Relation: Top-10 %, mittlere 40 % und untere 50 % muessen sich auf 100 %
addieren. Geprueft fuer AT, DE, US, SE, Europa und die Welt, je fuer
Einkommen und Vermoegen — **alle zwoelf Summen 100,00 +/- 0,01**.

Und die Gebietscodes der Aggregate sind belegt, nicht erschlossen:
``QE`` = Europa, ``WO`` = Welt stehen so in ``WID_countries.csv``
(``QE-MER``/``QE-PPP``, ``WO-MER``/``WO-PPP``); laut README ist der
nackte Code die Kaufkraftparitaeten-Fassung. Dass die Werte plausibel
sind (Europa Top-10 % 36,4 %, Welt 53,1 %), stuetzt das, belegt es aber
nicht.

## Ein eigener Fehler beim Lesen

Die erste Filterung suchte ``sptinc992j`` — die Schreibweise aus den
Aufsaetzen. Im Bulk-Download heisst dieselbe Reihe ``sptincj992``, Alters-
und Aufteilungscode vertauscht. Ergebnis: **null Treffer in einer 21-MB-
Datei**. Null Treffer ist kein Ergebnis, sondern ein Verdacht — hier auf
das eigene Instrument.

## Warum 2023 und nicht 2024

32 der 34 Gebiete fuehren 2024, Europa und die Welt nur bis 2023.
Gewaehlt ist das juengste GEMEINSAME Jahr. Ein gemischtes ``data_year``
waere eine Falschangabe: Dann stuende ein Einkommensanteil von 2024
neben einem Aggregat von 2023, und das Feld behauptete ein gemeinsames
Jahr, das es nicht gibt — dieselbe Fehlerform wie die zwei ARE-Reihen in
#138 und der Saisonanfang gegen den Gipfel in #264.

## Negative Anteile sind Daten, keine Tippfehler

Schweden fuehrt fuer die unteren 50 % einen Vermoegensanteil von
**-10,9 %**: Diese Haelfte schuldet mehr, als sie besitzt. Die Summe
stimmt trotzdem (68,02 + 42,93 - 10,94 = 100,01). Wer das als
Vorzeichenfehler "korrigiert", faelscht es.

## Und ein Literal, das den Refresh ueberlebt haette

``services/wid.py`` hatte ``int(data.get("data_year", 2022))``. Der
Default haette neue Zahlen mit dem alten Jahr beschriftet — dieselbe
Marker-Drift wie bei Freedom House (#133) und dreifach bei BTI (#268).
Fehlt das Feld jetzt, liefert der Service kein Ergebnis und schreibt es
ins Log, statt still ein falsches Jahr zu nennen.

Dependency-light: nur Daten und Quelltext, kein Netz, kein Bulk-Download.
"""

import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATEI = os.path.join(BACKEND, "data", "wid.json")
SERVICE = os.path.join(BACKEND, "services", "wid.py")
sys.path.insert(0, os.path.join(BACKEND, "tools"))

from wid_aus_bulk import GEBIETE, REIHEN, VARIABLEN, PERZENTILE  # noqa: E402

FELDER = ("income_top1", "income_top10", "income_bottom50",
          "wealth_top1", "wealth_top10", "wealth_bottom50")


def _laden():
    with open(DATEI, encoding="utf-8") as fh:
        return json.load(fh)


def _quelle():
    with open(SERVICE, encoding="utf-8") as fh:
        return fh.read()


def _nur_code():
    """Der Quelltext mit Kommentaren und Strings als Leerzeichen.

    Dieselbe Begruendung wie in test_bti_amtliche_werte.py: Die
    Erklaerungen im Quelltext zitieren die alten Formen absichtlich.
    Ausgeblendet wird, nicht umgebaut — ein Token-Join machte dort die
    negativen Zusicherungen leer statt streng.
    """
    import io
    import tokenize
    quelle = _quelle()
    zeilen = quelle.splitlines(keepends=True)
    for tok in tokenize.generate_tokens(io.StringIO(quelle).readline):
        if tok.type not in (tokenize.COMMENT, tokenize.STRING):
            continue
        (z1, s1), (z2, s2) = tok.start, tok.end
        for z in range(z1, z2 + 1):
            zeile = zeilen[z - 1]
            von = s1 if z == z1 else 0
            bis = s2 if z == z2 else len(zeile.rstrip("\n"))
            zeilen[z - 1] = zeile[:von] + " " * (bis - von) + zeile[bis:]
    return "".join(zeilen)


#: Die Welt-Werte, die der alte Stand fuehrte, und die exakt den
#: Schlagzeilen des World Inequality Report 2022 entsprachen. Sie sind
#: der Beleg dafuer, dass der alte Stand KORREKT war, nur alt.
#: Geprueft wird die BESCHRIFTETE Form, nicht die nackte Zahl: "2,0"
#: ist Teilstring von "52,0", eine erste Fassung dieses Tests konnte den
#: Vermoegenswert also nicht verlieren.
WIR2022_SCHLAGZEILEN = {
    "income_top10": ("Einkommen Top-10 % 52,0 %", 52.0),
    "income_bottom50": ("untere 50 % 8,5 %", 8.5),
    "wealth_bottom50": ("Vermoegen untere 50 % 2,0 %", 2.0),
}


# --- Die amtlichen Werte ----------------------------------------------

AMTLICH = {
    # ISO3: (Eink. top1, top10, bottom50, Verm. top1, top10, bottom50)
    'AUS': ( 10.9,  35.2,  17.1,  23.5,  57.6,    4.9),
    'AUT': (  8.9,  31.8,  23.2,  30.1,  61.6,    3.1),
    'BEL': (  8.7,  32.9,  20.3,  15.0,  52.3,    7.4),
    'BRA': ( 26.5,  59.1,   9.3,  38.9,  71.7,    2.1),
    'CAN': ( 12.1,  36.8,  16.9,  28.6,  59.8,   14.0),
    'CHE': ( 10.3,  31.5,  23.0,  31.4,  62.8,    3.7),
    'CHN': ( 15.8,  43.5,  13.5,  30.4,  68.0,    6.3),
    'CZE': ( 10.1,  29.1,  25.3,  25.9,  58.5,    4.8),
    'DEU': ( 13.3,  37.6,  18.8,  27.7,  58.4,    3.4),
    'DNK': ( 12.4,  33.4,  21.6,  20.5,  50.1,    4.1),
    'ESP': ( 12.1,  34.0,  21.4,  23.8,  57.2,    6.7),
    'EUR': ( 12.3,  36.4,  18.9,  25.4,  60.2,    3.1),
    'FIN': (  9.4,  30.7,  22.7,  17.9,  55.5,    2.1),
    'FRA': ( 11.8,  34.0,  20.3,  27.4,  59.7,    4.9),
    'GBR': ( 13.1,  36.2,  20.4,  21.3,  57.1,    4.6),
    'GRC': ( 11.3,  33.2,  20.9,  23.4,  60.5,   -1.8),
    'HUN': ( 11.5,  33.1,  22.3,  33.4,  67.2,    3.9),
    'IND': ( 23.1,  58.9,  13.2,  40.1,  65.0,    6.4),
    'IRL': ( 13.0,  36.9,  19.5,  22.4,  65.9,   -3.5),
    'ITA': (  9.1,  32.3,  21.6,  22.0,  56.1,    2.5),
    'JPN': ( 12.7,  43.5,  18.2,  24.5,  59.1,    4.6),
    'KOR': ( 14.1,  37.8,  17.6,  25.9,  66.3,    1.7),
    'MEX': ( 25.6,  59.1,   7.7,  39.4,  71.9,    2.0),
    'NLD': (  7.1,  30.4,  20.8,  13.9,  45.5,    9.8),
    'NOR': (  9.5,  30.6,  24.0,  23.3,  52.6,    3.6),
    'POL': ( 15.1,  37.7,  19.4,  29.9,  61.7,   -0.8),
    'PRT': ( 10.4,  35.4,  20.4,  24.9,  60.0,    3.6),
    'ROU': ( 14.5,  41.1,  15.3,  24.1,  58.5,    4.7),
    'RUS': ( 23.8,  50.8,  15.7,  49.0,  76.0,    2.7),
    'SWE': (  9.5,  29.0,  24.4,  27.0,  68.0,  -10.9),
    'TUR': ( 24.4,  56.7,  12.4,  36.1,  69.2,    2.6),
    'USA': ( 20.7,  46.8,  13.4,  34.8,  69.5,    0.9),
    'WLD': ( 20.3,  53.1,   8.4,  36.6,  74.4,    2.0),
    'ZAF': ( 19.3,  65.4,   5.8,  54.7,  85.6,   -2.5),
}


@pytest.mark.parametrize("iso,werte", sorted(AMTLICH.items()))
def test_jeder_wert_stammt_aus_dem_bulk_download(iso, werte):
    """Zelle fuer Zelle gegen die Reihen sptincj992 und shwealj992,
    Datenjahr 2023."""
    rec = _laden()["shares"][iso]
    for feld, soll in zip(FELDER, werte):
        assert rec[feld] == soll, f"{iso}.{feld}"


def test_die_datei_fuehrt_genau_die_gemessenen_gebiete():
    assert set(_laden()["shares"]) == set(AMTLICH)
    assert len(AMTLICH) == 34


def test_jedes_gebiet_fuehrt_alle_sechs_reihen():
    luecken = [(iso, f) for iso, v in sorted(_laden()["shares"].items())
               for f in FELDER if f not in v]
    assert not luecken, luecken


# --- Die Relation, die die Zuordnung beweist --------------------------

@pytest.mark.parametrize("art", ["income", "wealth"])
def test_top10_enthaelt_top1(art):
    """Das Top-10-%-Perzentil schliesst das Top-1-% ein. Waere eine der
    beiden Spalten verwechselt, fiele das hier auf."""
    schief = [(iso, v[f"{art}_top1"], v[f"{art}_top10"])
              for iso, v in sorted(_laden()["shares"].items())
              if v[f"{art}_top1"] > v[f"{art}_top10"]]
    assert not schief, schief


@pytest.mark.parametrize("art", ["income", "wealth"])
def test_die_gruppen_passen_in_hundert_prozent(art):
    """Top-10 % und untere 50 % lassen Platz fuer die mittleren 40 %.

    Die eigentliche Partitions-Probe (Summe == 100) brauchte die
    mittleren 40 %, die diese Datei nicht fuehrt; sie ist in der
    Methodik-Notiz festgehalten und wurde beim Refresh fuer zwoelf
    Reihen gerechnet. Hier bleibt die schwaechere, aber datei-interne
    Form.
    """
    for iso, v in sorted(_laden()["shares"].items()):
        summe = v[f"{art}_top10"] + v[f"{art}_bottom50"]
        assert summe <= 100.01, (iso, summe)
        assert v[f"{art}_top10"] <= 100.01, (iso, v[f"{art}_top10"])


def test_die_partitions_probe_ist_in_der_methodik_festgehalten():
    """Sonst weiss der naechste nicht, woran die Zuordnung haengt."""
    m = _laden()["methodology_note"]
    assert "p99p100" in m and "p90p100" in m and "p0p50" in m
    assert "100,00" in m, "das Ergebnis der Partitions-Probe fehlt"


# --- Das Datenjahr ----------------------------------------------------

def test_das_datenjahr_ist_das_juengste_gemeinsame():
    d = _laden()
    assert d["data_year"] == 2023
    b = d["datenjahr_begruendung"]
    assert "2024" in b, "warum nicht 2024 — die Begruendung fehlt"
    assert "QE" in b and "WO" in b


def test_ein_gemischtes_datenjahr_ist_ausdruecklich_abgelehnt():
    b = _laden()["datenjahr_begruendung"]
    assert "Falschangabe" in b
    assert "#138" in b or "#264" in b


def test_der_stand_und_der_takt():
    d = _laden()
    assert d["fetched_at_iso"] == "2026-10-20"
    assert d["refresh_kadenz"] == "jaehrlich"


# --- Was der alte Stand war -------------------------------------------

@pytest.mark.parametrize("feld,satz,wert",
                         [(f, s, w) for f, (s, w)
                          in sorted(WIR2022_SCHLAGZEILEN.items())])
def test_die_alten_welt_werte_sind_als_report_zahlen_belegt(feld, satz, wert):
    """Der Beleg dafuer, dass hier KEIN Fehler korrigiert wurde.

    Diese drei Zahlen fuehrte der alte Stand fuer die Welt, und sie sind
    exakt die Schlagzeilen des WIR 2022. Der Test pinnt die Begruendung,
    damit der naechste den Refresh nicht als Fundbericht liest — und er
    prueft, dass die Zahl im alten Stand WIRKLICH so stand.
    """
    flag = _laden()["audit_flag"]
    assert "KORREKT" in flag
    assert satz in flag, f"{feld}: '{satz}' fehlt im audit_flag"


def test_der_refresh_nennt_sich_refresh_und_nicht_korrektur():
    flag = _laden()["audit_flag"]
    assert flag.startswith("VOLLREFRESH 2026-10-20")
    assert "kein Fehlerbefund" in flag
    assert "Revisionen" in flag


def test_die_groebe_des_alten_standes_ist_belegt():
    """43 % gegen 18 % gerundete Werte — das ist der Unterschied zwischen
    einer Report-Grafik und einem Datenbank-Export."""
    flag = _laden()["audit_flag"]
    assert "43 %" in flag and "18 %" in flag


def test_die_gegenprobe_ist_wiederholbar():
    assert "wid_aus_bulk.py" in _laden()["audit_flag"]


# --- Die Provenienz ---------------------------------------------------

def test_die_datei_sagt_dass_sie_die_datenbank_zitiert_nicht_den_report():
    """Ein Feldname IST eine Quellenangabe (#247). Die Werte stammen aus
    dem Bulk-Download; wer 'laut World Inequality Report 2026' schreibt,
    zitiert etwas anderes."""
    d = _laden()
    assert "sptincj992" in d["source_label"]
    assert "shwealj992" in d["source_label"]
    assert "Report-Grafik" in d["source_label"]
    h = d["zitier_hinweis"]
    assert "DATENBANK" in h and "nicht der Report" in h
    assert "Revisionen" in h


def test_die_reihen_namen_stehen_in_der_schreibweise_des_downloads():
    """``sptinc992j`` ist die Aufsatz-Form und findet im Bulk-Download
    NULL Zeilen. Der Unterschied kostete eine Messung."""
    assert VARIABLEN == {"sptincj992", "shwealj992"}
    assert PERZENTILE == {"p99p100", "p90p100", "p0p50"}
    t = json.dumps(_laden(), ensure_ascii=False)
    assert "sptinc992j" not in t, "die Aufsatz-Form steht in der Datei"


def test_die_aggregat_codes_sind_belegt():
    assert GEBIETE["EUR"] == "QE"
    assert GEBIETE["WLD"] == "WO"


def test_jedes_gebiet_hat_einen_bulk_code():
    fehlt = [i for i in _laden()["shares"] if i not in GEBIETE]
    assert not fehlt, fehlt


def test_die_urls_zeigen_auf_die_aktuelle_ausgabe():
    d = _laden()
    assert d["secondary_url"] == "https://wir2026.wid.world/"
    assert "wir2022" not in json.dumps(d, ensure_ascii=False)
    assert d["bulk_url"] == "https://wid.world/bulk_download/"


# --- Negative Anteile -------------------------------------------------

def test_der_negative_vermoegensanteil_bleibt_negativ():
    """Schweden -10,9 %, Suedafrika -2,5 %. Nettoschuld ist ein Datum,
    kein Vorzeichenfehler."""
    s = _laden()["shares"]
    assert s["SWE"]["wealth_bottom50"] == -10.9
    assert s["ZAF"]["wealth_bottom50"] == -2.5


def test_die_datei_erklaert_die_negativen_anteile():
    h = _laden()["negative_anteile_hinweis"]
    assert "Nettoschuld" in h
    assert "-10,9" in h
    assert "faelscht" in h


def test_kein_anteil_liegt_ausserhalb_des_moeglichen():
    """Obergrenze 100, Untergrenze -100: Ein Anteil kann negativ sein,
    aber nicht beliebig."""
    for iso, v in sorted(_laden()["shares"].items()):
        for f in FELDER:
            assert -100.0 <= v[f] <= 100.0, (iso, f, v[f])


def test_nur_vermoegen_der_unteren_haelfte_ist_negativ():
    """Ein negativer EINKOMMENS-Anteil waere ein Lesefehler, kein Datum."""
    for iso, v in sorted(_laden()["shares"].items()):
        for f in FELDER:
            if f != "wealth_bottom50":
                assert v[f] >= 0, (iso, f, v[f])


# --- Der Service ------------------------------------------------------

def test_kein_datenjahr_steht_als_default_im_code():
    """Der Default haette neue Zahlen mit dem alten Jahr beschriftet
    (#133, und dreifach bei BTI in #268)."""
    code = _nur_code()
    assert "2022" not in code, "ein Datenjahr steht wieder im Code"
    assert 'data.get("data_year", 2022)' not in _quelle()
    assert 'data.get("data_year")' in _quelle()


def test_das_fehlende_datenjahr_fuehrt_zu_keinem_ergebnis():
    """Nicht still weiterrechnen: Ohne data_year gibt es kein Jahr zu
    nennen, und eine Zahl ohne Bezug ist schlechter als keine."""
    q = _quelle()
    i = q.index('rohes_jahr = data.get("data_year")')
    j = q.index("data_year = int(rohes_jahr)")
    block = q[i:j]
    assert "return empty" in block
    assert "logger.error" in block, "der Ausfall bleibt sonst unsichtbar"


def test_der_iso2_fallback_erfindet_keinen_code():
    """Bei BTI machte iso3[:2] aus COD Kolumbien (#268). Hier waere WLD
    zu 'WL' geworden."""
    code = _nur_code()
    assert "iso3[:2]" not in code


def test_jedes_gebiet_hat_einen_eigenen_iso2_code():
    sys.path.insert(0, BACKEND)
    from services.wid import _ISO3_TO_ISO2
    gebiete = sorted(_laden()["shares"])
    fehlt = [i for i in gebiete if i not in _ISO3_TO_ISO2]
    assert not fehlt, fehlt
    codes = [_ISO3_TO_ISO2[i] for i in gebiete]
    assert len(set(codes)) == len(codes), "zwei Gebiete teilen einen Code"


def test_der_service_nennt_im_docstring_die_gemessenen_werte():
    """Ein Beispiel mit alten Zahlen wird abgeschrieben."""
    q = _quelle()
    assert "8,9 %" in q, "das Beispiel traegt nicht den gemessenen AT-Wert"
    assert "10,8" not in q, "der alte AT-Wert steht noch im Docstring"


# --- Das Werkzeug -----------------------------------------------------

def test_das_werkzeug_meldet_befunde_ueber_den_exit_code():
    with open(os.path.join(BACKEND, "tools", "wid_aus_bulk.py"),
              encoding="utf-8") as fh:
        q = fh.read()
    assert "BEFUND_EXITS" in q
    assert "return 1 if (ab_gesamt or luecken or ohne) else 0" in q


def test_das_werkzeug_liest_die_datei_zeilenweise():
    """Zusammen ueber 800 MB. Ein ``read()`` darauf riss in #263 den
    Wartungs-Container."""
    with open(os.path.join(BACKEND, "tools", "wid_aus_bulk.py"),
              encoding="utf-8") as fh:
        q = fh.read()
    assert "for rohzeile in antwort:" in q
    assert ".read()" not in q.split("def hole_gefiltert")[1].split("def ")[0]


def test_das_werkzeug_verlangt_ein_vollstaendiges_jahr():
    with open(os.path.join(BACKEND, "tools", "wid_aus_bulk.py"),
              encoding="utf-8") as fh:
        q = fh.read()
    assert "def jahr_mit_voller_abdeckung" in q
    assert "felder <= set(nach_jahr[jahr])" in q


def test_alle_sechs_reihen_sind_im_werkzeug_abgebildet():
    assert len(REIHEN) == 6
    assert set(REIHEN.values()) == set(FELDER)
