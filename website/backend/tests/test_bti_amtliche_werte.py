"""Der Vermerk stand fuenf Monate da, und 80 Prozent der Werte waren falsch.

``data/bti_2024.json`` trug seit seiner Anlage:

    audit_flag: AUDIT NEEDED — BTI-2024-Werte sind LLM-Approximationen
    aus dem oeffentlich kommunizierten Ranking. Vor produktivem Einsatz
    mit offiziellem Excel-Export gegenchecken.

Wortgleich mit dem MIPEX-Vermerk, der sich in #266 als **52 % falsche
Werte** entpuppte. Hier ist es schlimmer.

## Was die Gegenprobe ergab

Gegen ``BTI_2026_Scores.xlsx`` und ``BTI_2024_Scores.xlsx`` gerechnet:

    341 von 424 vergleichbaren Werten falsch (80 %)
    kein einziges der 53 zuordenbaren Laender fehlerfrei
    alle drei Dimensionswerte falsch bei allen 53 Laendern
    50 der 53 Raenge falsch, groesste Abweichung 23 Plaetze

Der alte Stand war schon in sich widerspruechlich, und das haette ohne
Netz auffallen koennen: Polen trug Index 8,13 bei Rang 10, Costa Rica
8,45 bei Rang 11 — ein niedrigerer Index bei besserem Rang. Beim BTI ist
der Rang die absteigende Ordnung des Index; das kann es nicht geben.
Sieben solche Paare standen in der Datei.

## Ein eigener Fehler beim Messen

Die erste Messung meldete 384 Abweichungen (90 %), darunter **53 von 53**
Demokratie-Einstufungen. Das war mein Instrument, nicht die Datei: BTI
schreibt die Stufen im Plural ("Hard-line autocracies"), ich hatte
geratene Singular-Formen uebersetzt. Ueber BTI's Kategorie-NUMMER
aufgeloest blieben 10 echte Fehler von 53. Die Zahl 80 % ist die nach der
Korrektur.

## Warum die Stufe nicht aus dem Score folgt

Die Stufen 3 und 4 der Demokratie-Skala UEBERLAPPEN: hochdefekte
Demokratien liegen bei 4,75-5,95, moderate Autokratien bei 4,03-5,48.
BTI zieht seine Demokratie/Autokratie-Bewertung hinzu. Der alte Stand
fuehrte dagegen erfundene Schwellen in der ``methodology_note``
("Moderate Autokratie (3-4)"), die so nicht gelten. Die Einstufungen
werden jetzt aus dem Export GELESEN, englisches Original daneben — wir
zitieren BTI's Klassifikation, wir klassifizieren nicht selbst.

## Drei Funde, die nicht in den Zahlen standen

**Jeder Beleg-Link war tot.** ``_country_url`` baute den ISO3-Code klein
ein; bti-project.org antwortet darauf mit HTTP 500. Gross: 200. Die
dafuer gefuehrte Tabelle hatte 54 Eintraege, und jeder war nur die
Kleinschreibung des ISO3-Codes — sie war die Ursache. Alle 66 URLs sind
jetzt geprueft, auf Erreichbarkeit UND Identitaet (#248): Seitentitel
nennt Land und Ausgabe.

**Die Dimensionswerte erreichten die Ausgabe nie.** ``description`` war
``(methodology_short + dim_summary)[:300]``, und die ``methodology_note``
hatte 786 Zeichen — 0 von 76 Zeichen der Dimensionswerte kamen durch.
Berechnet und still verworfen. Das Landesspezifische steht jetzt vorn.

**Die Tuerkei fiel an einem Trema aus der Zuordnung.** BTI schreibt
``Türkiye`` als ``u`` + kombinierendes Trema (NFD), unsere Alias-Liste
als ``ü`` (NFC). Optisch identisch, byteweise verschieden — dieselbe
Fehlerform wie bei den Phrasings in #143/#144.

## Und die Zusage, die nicht galt

Der alte Stand nannte sich "Top-20 + Bottom-20", fuhr aber nur 28 der 40
Plaetze: Slowakei (Rang 13), Montenegro, Nordmazedonien, Albanien,
Trinidad und Tobago fehlten oben, Niger, DR Kongo, Aequatorialguinea,
Tschad, Haiti, Libyen, Suedsudan unten. Jetzt sind es 66 Laender, und
die Zusage wird hier geprueft. Sechs der zwoelf neuen Laender haetten
ueber den Fallback ``iso3[:2]`` den Laendercode eines ANDEREN Staates
bekommen (COD->CO Kolumbien, SVK->SV El Salvador, LBY->LB Libanon,
MNE->MN Mongolei, GNQ->GN Guinea, TCD->TC).

Dependency-light: nur Daten und Quelltext, kein Netz, kein Excel.
"""

import json
import os
import re
import sys
import unicodedata

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATEI = os.path.join(BACKEND, "data", "bti.json")
SERVICE = os.path.join(BACKEND, "services", "bti.py")
sys.path.insert(0, os.path.join(BACKEND, "tools"))

AMTLICH = {
    # ISO3:  (Status-Index, politisch, wirtschaftlich, governance, Rang)
    'AFG': ( 1.94,  1.82,  2.07,  2.14,  132),
    'ALB': (  7.4,   7.7,  7.11,  6.71,   18),
    'ARG': (  6.6,  7.35,  5.86,  4.97,   30),
    'ARM': ( 6.84,   6.9,  6.79,   5.1,   26),
    'BGD': ( 4.75,  4.72,  4.79,  4.79,   80),
    'BGR': (  7.2,   6.9,   7.5,  5.72,   22),
    'BLR': ( 4.31,  3.45,  5.18,  2.36,   95),
    'BRA': ( 7.23,   7.5,  6.96,  5.69,   21),
    'BWA': ( 7.69,  8.45,  6.93,  6.84,   14),
    'CHL': ( 8.84,  9.25,  8.43,  7.07,    8),
    'CHN': ( 4.66,  3.18,  6.14,  4.93,   83),
    'COD': ( 3.32,  3.57,  3.07,  2.65,  119),
    'CRI': ( 8.49,   8.9,  8.07,   6.7,   11),
    'CUB': ( 2.97,  3.05,  2.89,   3.8,  122),
    'CZE': ( 9.22,  9.15,  9.29,  7.04,    4),
    'EGY': ( 3.87,  3.42,  4.32,  3.74,  106),
    'ERI': ( 1.54,  2.02,  1.07,  1.05,  135),
    'EST': ( 9.48,   9.6,  9.36,  7.22,    3),
    'GEO': ( 5.17,  4.37,  5.96,  4.21,   65),
    'GHA': ( 6.63,  7.75,   5.5,  6.41,   29),
    'GNQ': ( 3.08,  2.98,  3.18,   3.5,  121),
    'HRV': ( 8.58,  8.55,  8.61,  6.45,    9),
    'HTI': (  2.5,  2.38,  2.61,  2.62,  127),
    'HUN': ( 6.48,   6.2,  6.75,  3.45,   33),
    'IDN': ( 5.98,  5.95,   6.0,  5.27,   47),
    'IND': ( 5.98,   6.1,  5.86,  5.18,   47),
    'IRN': ( 2.53,  2.78,  2.29,  1.79,  126),
    'JAM': ( 7.09,   8.0,  6.18,  5.53,   24),
    'KOR': ( 8.29,   8.0,  8.57,  6.44,   12),
    'LBY': ( 2.31,  2.12,   2.5,   2.4,  128),
    'LTU': ( 9.22,   9.4,  9.04,  7.38,    4),
    'LVA': ( 8.97,  9.15,  8.79,  7.22,    7),
    'MDA': ( 6.37,   6.7,  6.04,  5.91,   38),
    'MEX': ( 5.82,   5.6,  6.04,  4.61,   53),
    'MKD': ( 7.41,  7.65,  7.18,  6.07,   17),
    'MMR': (  1.6,  1.55,  1.64,   1.3,  134),
    'MNE': ( 7.45,   7.4,   7.5,   5.9,   16),
    'MUS': ( 7.55,  7.85,  7.25,  6.13,   15),
    'NER': ( 3.37,  2.78,  3.96,  3.69,  118),
    'NIC': (  3.8,  2.92,  4.68,  2.61,  107),
    'PAK': ( 3.52,  3.25,  3.79,  3.35,  111),
    'PAN': ( 6.37,  6.35,  6.39,  5.36,   38),
    'PHL': ( 5.55,  5.25,  5.86,  4.34,   58),
    'POL': ( 8.58,   8.4,  8.75,  6.76,    9),
    'PRK': (  2.1,   2.6,  1.61,  1.14,  131),
    'ROU': ( 7.32,   7.1,  7.54,  4.81,   20),
    'RUS': ( 4.04,  3.22,  4.86,  2.41,  102),
    'SDN': ( 1.46,  1.63,  1.29,  1.06,  136),
    'SOM': (  1.8,  1.72,  1.89,  2.48,  133),
    'SRB': ( 5.98,  5.18,  6.79,  3.96,   47),
    'SSD': ( 2.19,  2.52,  1.86,  2.01,  130),
    'SVK': ( 8.28,   8.2,  8.36,  5.37,   13),
    'SVN': ( 9.52,  9.25,  9.79,  6.69,    2),
    'SYR': ( 2.22,  2.87,  1.57,  3.42,  129),
    'TCD': ( 2.77,  2.47,  3.07,  2.22,  124),
    'TJK': ( 3.22,  2.87,  3.57,  3.09,  120),
    'TKM': ( 2.69,   2.7,  2.68,  2.18,  125),
    'TTO': (  7.4,   8.2,  6.61,  5.99,   18),
    'TUN': ( 4.68,  4.33,  5.04,  4.05,   82),
    'TUR': ( 4.91,  4.18,  5.64,   3.4,   72),
    'TWN': ( 9.55,   9.6,   9.5,  7.49,    1),
    'UKR': ( 6.39,  6.85,  5.93,  6.58,   37),
    'URY': ( 9.13,   9.9,  8.36,  7.46,    6),
    'VEN': ( 2.89,  3.07,  2.71,  1.57,  123),
    'YEM': ( 1.38,   1.5,  1.25,  1.45,  137),
    'ZAF': ( 6.41,   7.1,  5.71,   5.6,   35),
}


from bti_aus_excel import SKALA, ZAHLEN_FELDER  # noqa: E402


def _laden():
    with open(DATEI, encoding="utf-8") as fh:
        return json.load(fh)


def _quelle():
    with open(SERVICE, encoding="utf-8") as fh:
        return fh.read()


def _nur_code(pfad=None):
    """Der Quelltext, in dem Kommentare und Strings durch Leerzeichen
    ersetzt sind — Zeilen und Spalten bleiben erhalten.

    Noetig, weil die Erklaerungen im Quelltext die alten, falschen Formen
    absichtlich zitieren, damit der naechste nachvollziehen kann, was hier
    schiefging. Eine erste Fassung dieser Tests suchte im Volltext und
    schlug an genau diesen Erklaerungen fehl, fuenfmal — dieselbe Form wie
    der ``except Exception``-Treffer im eigenen Kommentar in #238.

    Eine ZWEITE Fassung fuegte die Tokens mit Zeilenumbruechen zusammen.
    Damit konnte kein mehrteiliges Muster mehr treffen, und die negativen
    Zusicherungen ("steht NICHT mehr da") waren leer statt streng. Nur
    ausblenden, nicht umbauen: Das ist der Unterschied zwischen einem
    Gate und seiner Attrappe.
    """
    import io
    import tokenize
    quelle = _quelle() if pfad is None else open(pfad, encoding="utf-8").read()
    zeilen = quelle.splitlines(keepends=True)
    for tok in tokenize.generate_tokens(io.StringIO(quelle).readline):
        if tok.type not in (tokenize.COMMENT, tokenize.STRING):
            continue
        (z1, s1), (z2, s2) = tok.start, tok.end
        for z in range(z1, z2 + 1):
            zeile = zeilen[z - 1]
            von = s1 if z == z1 else 0
            bis = s2 if z == z2 else len(zeile.rstrip("\n"))
            zeilen[z - 1] = (zeile[:von] + " " * (bis - von)
                             + zeile[bis:])
    return "".join(zeilen)


#: Die 20 bestplatzierten und die 20 letzten Laender des BTI 2026,
#: aus BTI_2026_Scores.xlsx abgelesen. Beide Gruppen muss der Auszug
#: VOLLSTAENDIG fuehren — das ist die Zusage im source_label.
TOP20 = {"TWN", "SVN", "EST", "CZE", "LTU", "URY", "LVA", "CHL", "HRV",
         "POL", "CRI", "KOR", "SVK", "BWA", "MUS", "MNE", "MKD", "ALB",
         "TTO", "ROU"}
BOTTOM20 = {"NER", "COD", "TJK", "GNQ", "CUB", "VEN", "TCD", "TKM", "IRN",
            "HTI", "LBY", "SYR", "SSD", "PRK", "AFG", "SOM", "MMR", "ERI",
            "SDN", "YEM"}

#: Die zwoelf Laender, die der alte Stand versprach und nicht fuehrte.
NACHGETRAGEN = {"SVK", "MNE", "MKD", "ALB", "TTO", "NER", "COD", "GNQ",
                "TCD", "HTI", "LBY", "SSD"}

#: Werte, die der alte Stand fuehrte und die nachweislich falsch waren.
#: Sie duerfen nirgends wieder auftauchen.
WIDERLEGT = {
    "TWN": 9.66, "RUS": 3.35, "CHN": 4.15, "PRK": 1.50, "HUN": 6.94,
    "TUR": 4.70, "AFG": 1.95, "ERI": 1.85,
}


# --- Die amtlichen Werte ----------------------------------------------

@pytest.mark.parametrize("iso,werte", sorted(AMTLICH.items()))
def test_jeder_wert_stammt_aus_dem_amtlichen_export(iso, werte):
    """Zelle fuer Zelle gegen BTI_2026_Scores.xlsx.

    Gepinnt wird die gerundete Fassung, die BTI selbst fuehrt (Spalten
    DB/DE/DH/DK) — nicht ein nachgerechnetes Mittel: BTI rundet den Index
    aus den VOLLEN Werten, das Mittel der gerundeten Komponenten weicht
    um bis zu 0,01 ab.
    """
    si, pol, eco, gov, rang = werte
    rec = _laden()["scores"][iso]
    assert rec["status_index"] == si
    assert rec["political"] == pol
    assert rec["economic"] == eco
    assert rec["governance"] == gov
    assert rec["rank"] == rang


def test_der_auszug_hat_genau_die_gemessenen_laender():
    assert set(_laden()["scores"]) == set(AMTLICH)
    assert len(AMTLICH) == 66


@pytest.mark.parametrize("iso,alt", sorted(WIDERLEGT.items()))
def test_kein_widerlegter_wert_steht_noch_in_der_datei(iso, alt):
    """Gegenprobe gegen einen halben Refresh.

    Diese Zahlen waren die LLM-Approximationen. Ein Wert, der wieder
    auftaucht, heisst: Die Datei wurde teilweise zurueckgedreht.
    """
    rec = _laden()["scores"][iso]
    for feld in ("status_index", "political", "economic", "governance"):
        assert rec[feld] != alt, f"{iso}.{feld} traegt wieder {alt}"


# --- Die inneren Relationen, die der alte Stand verletzte -------------

def test_der_rang_folgt_dem_status_index():
    """Der Fund, der ohne Netz moeglich gewesen waere.

    Der alte Stand hatte sieben Paare, in denen ein niedrigerer Index
    einen besseren Rang hatte — bei einem Index-abgeleiteten Rang
    unmoeglich.
    """
    s = _laden()["scores"]
    nach = sorted(s.items(), key=lambda kv: -kv[1]["status_index"])
    schief = [(nach[i][0], nach[i + 1][0])
              for i in range(len(nach) - 1)
              if nach[i][1]["rank"] > nach[i + 1][1]["rank"]]
    assert not schief, schief


def test_der_status_index_ist_das_mittel_der_beiden_dimensionen():
    """Toleranz 0,011, weil BTI aus den vollen Werten rundet."""
    abweichend = []
    for iso, v in sorted(_laden()["scores"].items()):
        soll = (v["political"] + v["economic"]) / 2
        if abs(soll - v["status_index"]) > 0.011:
            abweichend.append((iso, round(soll, 3), v["status_index"]))
    assert not abweichend, abweichend


def test_jeder_rang_liegt_im_erfassten_feld():
    zahl = _laden()["laenderzahl"]
    assert zahl == 137
    for iso, v in _laden()["scores"].items():
        assert 1 <= v["rank"] <= zahl, (iso, v["rank"])


# --- Die Zusage "Top-20 + Bottom-20 vollstaendig" ---------------------

@pytest.mark.parametrize("iso", sorted(TOP20 | BOTTOM20))
def test_die_versprochenen_plaetze_sind_alle_da(iso):
    """Der alte Stand fuehrte 28 der 40 Plaetze und nannte sich trotzdem
    "Top-20 + Bottom-20". Eine Zusage im Datenfeld ist ungeprueft, bis ein
    Gate sie prueft (#230)."""
    assert iso in _laden()["scores"]


def test_die_zusage_steht_auch_im_label():
    label = _laden()["source_label"]
    assert "Top-20" in label and "Bottom-20" in label
    assert "66" in label


@pytest.mark.parametrize("iso", sorted(NACHGETRAGEN))
def test_die_nachgetragenen_laender_sind_vollstaendig_ausgestattet(iso):
    """Ein Land ohne Alias ist unauffindbar, eines ohne Anzeigename
    erscheint als ISO3-Code."""
    d = _laden()
    assert iso in d["scores"]
    assert d["country_display_names"].get(iso)
    assert len(d["country_aliases"].get(iso) or []) >= 2


# --- Die Einstufungen -------------------------------------------------

def test_jede_dimension_hat_genau_fuenf_stufen():
    """Der alte Stand fuhr fuer Wirtschaft und Governance nur VIER Namen
    fuer BTI's fuenf Stufen — eine Stufe hatte gar keinen Namen und wurde
    in eine andere gefaltet."""
    for dim in ("political", "economic", "governance"):
        assert len(SKALA[dim]) == 5
        deutsch = {de for _en, de in SKALA[dim].values()}
        assert len(deutsch) == 5, (dim, deutsch)


@pytest.mark.parametrize("dim", ["political", "economic", "governance"])
def test_jede_einstufung_stammt_aus_der_skala(dim):
    erlaubt_de = {de for _en, de in SKALA[dim].values()}
    erlaubt_en = {en for en, _de in SKALA[dim].values()}
    for iso, v in sorted(_laden()["scores"].items()):
        assert v[f"{dim}_status"] in erlaubt_de, (iso, v[f"{dim}_status"])
        assert v[f"{dim}_status_bti"] in erlaubt_en, (iso, v[f"{dim}_status_bti"])


@pytest.mark.parametrize("dim", ["political", "economic", "governance"])
def test_deutsche_und_englische_fassung_gehoeren_zur_selben_stufe(dim):
    """Sonst steht neben dem englischen Original eine andere Stufe."""
    paare = {en: de for en, de in SKALA[dim].values()}
    for iso, v in sorted(_laden()["scores"].items()):
        assert paare[v[f"{dim}_status_bti"]] == v[f"{dim}_status"], iso


def test_die_einstufung_wird_nicht_aus_dem_score_hergeleitet():
    """Die Begruendung, warum sie gelesen und nicht gerechnet wird.

    Haette man sie aus dem Score abgeleitet, waere sie im Band 4,75-5,48
    falsch — dort ueberlappen hochdefekte Demokratien und moderate
    Autokratien.
    """
    hinweis = _laden()["einstufungen_hinweis"]
    assert "UEBERLAPPEN" in hinweis
    assert "GELESEN" in hinweis
    s = _laden()["scores"]
    im_band = {v["political_status"] for v in s.values()
               if 4.75 <= v["political"] <= 5.48}
    assert len(im_band) >= 2, (
        f"nur {im_band} im Ueberlappungsband — dann belegt dieser Test die "
        f"Begruendung nicht mehr")


def test_die_erfundenen_schwellen_stehen_nicht_mehr_in_der_methodik():
    """Der alte Stand nannte 'Moderate Autokratie (3-4)'. Moderate
    Autokratien liegen tatsaechlich bei 4,03-5,48."""
    m = _laden()["methodology_note"]
    assert "Moderate Autokratie (3-4)" not in m
    assert "Harte Autokratie (<3)" not in m
    for muster in (r"\(>8\)", r"\(6-8\)", r"\(4-6\)", r"\(3-4\)"):
        assert not re.search(muster, m), muster


# --- Der Audit-Marker -------------------------------------------------

def test_der_audit_marker_ist_abgearbeitet_nicht_geloescht():
    d = _laden()
    # Der Marker darf NUR noch im audit_flag stehen, als Aufzeichnung
    # dessen, was er fuenf Monate lang unerledigt behauptet hat —
    # derselbe Split wie bei der alten parlgov-DOI in #267.
    aktiv = [k for k, v in d.items()
             if k != "audit_flag"
             and "AUDIT NEEDED" in json.dumps(v, ensure_ascii=False)]
    assert not aktiv, aktiv
    flag = d["audit_flag"]
    assert "AUDIT NEEDED" in flag, "die Aufzeichnung des Marker-Texts fehlt"
    assert flag.startswith("GEPRUEFT 2026-10-20")
    assert "341" in flag and "424" in flag and "80 %" in flag
    assert "bti_aus_excel.py" in flag, "die Gegenprobe muss wiederholbar sein"


def test_die_datei_nennt_ihre_ausgabe_und_den_erhebungszeitraum():
    d = _laden()
    assert d["edition_label"] == "BTI 2026"
    assert d["report_year"] == 2026
    assert d["covering_events_year"] == 2025
    z = d["erhebungszeitraum"]
    assert "1. Februar 2023" in z and "31. Januar 2025" in z
    assert "Codebook" in z, "die Fundstelle fuer den Zeitraum fehlt"


def test_der_stand_und_der_takt():
    d = _laden()
    assert d["fetched_at_iso"] == "2026-10-20"
    assert d["refresh_kadenz"] == "jaehrlich"
    assert "2026" in d["kadenz_begruendung"]


def test_das_politik_tabu_bleibt_formuliert():
    g = _laden()["guardrail_note"]
    assert "zitieren" in g
    assert "klassifizieren nicht selbst" in g


# --- Schreibweisen ----------------------------------------------------

def test_alle_namen_und_aliase_sind_nfc_normalisiert():
    """Die Tuerkei fiel an einem kombinierenden Trema aus der Zuordnung.
    Ein Alias in NFD trifft einen Claim in NFC nicht (#143/#144)."""
    d = _laden()
    schief = []
    for iso, name in d["country_display_names"].items():
        if not unicodedata.is_normalized("NFC", name):
            schief.append(("display", iso, name))
    for iso, liste in d["country_aliases"].items():
        for a in liste:
            if not unicodedata.is_normalized("NFC", a):
                schief.append(("alias", iso, a))
    assert not schief, schief


def test_die_tuerkei_ist_unter_ihrem_amtlichen_namen_auffindbar():
    """BTI schreibt 'Türkiye'. Ohne diesen Alias traf ein Claim, der den
    Namen so schreibt, den Fakt nicht."""
    aliase = {a.lower() for a in _laden()["country_aliases"]["TUR"]}
    assert "türkiye" in aliase
    assert "türkei" in aliase, "die deutsche Form darf nicht verloren gehen"


def test_die_anzeigenamen_sind_die_amtlichen():
    """Stichprobe gegen den Export: Wer hier eigene Namen fuehrt, kann die
    Identitaet eines Belegs nicht mehr pruefen (#248)."""
    nd = _laden()["country_display_names"]
    assert nd["COD"] == "Congo, DR"
    assert nd["TUR"] == "Türkiye"
    assert nd["MKD"] == "North Macedonia"
    assert nd["SVK"] == "Slovakia"


# --- Der Service ------------------------------------------------------

def test_der_service_liest_die_entjahrte_datei():
    q = _quelle()
    assert '"bti.json",' in q
    assert "bti_2024.json" not in q


def test_kein_ausgabe_jahr_steht_als_literal_im_quelltext():
    """Drei Stellen trugen 'BTI 2024' bzw. 2024 als Default: der
    Dateiname, der report_year-Default und das Suffix im display_value —
    dem Feld, das der Nutzer sieht. Marker-Drift (#74/#133)."""
    code = _nur_code()
    assert "2024" not in code, (
        "eine Ausgabe-Jahreszahl steht wieder im ausfuehrbaren Code")
    assert 'data.get("report_year")' in _quelle()
    assert "{edition}" in _quelle()


def test_die_laenderzahl_kommt_aus_den_daten():
    assert "137" not in _nur_code(), (
        "die Laenderzahl steht wieder als Literal im ausfuehrbaren Code")
    assert 'data.get("laenderzahl")' in _quelle()


def test_die_beleg_url_nutzt_den_grossen_iso3_code():
    """Klein geschrieben antwortet bti-project.org mit HTTP 500 — jeder
    Beleg-Link war tot. Die frueher dafuer gefuehrte Slug-Tabelle hatte
    54 Eintraege, von denen keiner etwas anderes war als die
    Kleinschreibung."""
    assert "iso3.upper()" in _nur_code()
    assert "_BTI_COUNTRY_SLUGS" not in _nur_code()
    # Kein Test auf das f-String-Muster: Strings sind in _nur_code()
    # ausgeblendet, die Zusicherung waere leer. Das Verbot der Tabelle
    # und das .upper() im Code tragen die Aussage.
    assert "country-report" in _quelle()


def test_der_iso2_fallback_erfindet_keinen_fremden_laendercode():
    """``iso3[:2]`` machte aus COD 'CO' (Kolumbien) und aus SVK 'SV'
    (El Salvador) — plausibel aussehende Codes anderer Staaten."""
    code = _nur_code()
    assert "iso3[:2]" not in code
    assert "primary_iso3[:2]" not in code


def test_jedes_land_der_datei_hat_einen_echten_iso2_code():
    """Gate ueber ALLE Laender, nicht nur die neuen."""
    sys.path.insert(0, BACKEND)
    from services.bti import _ISO3_TO_ISO2
    fehlt = [i for i in _laden()["scores"] if i not in _ISO3_TO_ISO2]
    assert not fehlt, fehlt
    codes = [_ISO3_TO_ISO2[i] for i in _laden()["scores"]]
    assert len(set(codes)) == len(codes), "zwei Laender teilen einen ISO2-Code"
    for i in _laden()["scores"]:
        assert len(_ISO3_TO_ISO2[i]) == 2, (i, _ISO3_TO_ISO2[i])


def test_die_dimensionswerte_stehen_vor_der_methodik():
    """Sie wurden berechnet und von der 300-Zeichen-Kuerzung vollstaendig
    verworfen: 0 von 76 Zeichen kamen durch."""
    q = _quelle()
    zeilen = [z for z in q.splitlines()
              if "description = (" in z and not z.lstrip().startswith("#")]
    assert len(zeilen) == 1, zeilen
    zeile = zeilen[0]
    assert zeile.index("dim_summary") < zeile.index("methodology_short"), zeile


def test_die_methodik_passt_nicht_allein_in_die_kuerzung():
    """Die Gegenprobe zum vorigen Test: Waere die methodology_note kurz,
    bewiese die Reihenfolge nichts. Sie ist es nicht."""
    assert len(_laden()["methodology_note"]) > 300


# --- Das Werkzeug -----------------------------------------------------

def test_das_werkzeug_vergleicht_alle_fuenf_zahlenfelder():
    assert set(ZAHLEN_FELDER) == {"rank", "status_index", "political",
                                  "economic", "governance"}


def test_das_werkzeug_meldet_befunde_ueber_den_exit_code():
    """Damit ein Lauf im Cron nicht gruen aussieht, wenn er Abweichungen
    gefunden hat (#259)."""
    with open(os.path.join(BACKEND, "tools", "bti_aus_excel.py"),
              encoding="utf-8") as fh:
        q = fh.read()
    assert "BEFUND_EXITS" in q
    assert "return 1 if ab_gesamt else 0" in q
