"""Fuenf stille Quellen — und zwei davon logen in der Fundstelle.

Letzte Runde des Freshness-Rueckstands vom 20.10.2026. Fuenf Dateien ohne
``refresh_kadenz``, die der Waechter deshalb gegen den 120-Tage-Standard
mass: ``polity5``, ``wcag22``, ``parlgov``, ``wid``, ``withdrawn_drugs``.
Erwartet hatte ich fuenf Bestaetigungen. Drei waren es.

## Die drei stillen

``polity5`` — systemicpeace.org fuehrt weiterhin Polity5, Abdeckung endet
2018. Kein v2019+. Die Quelle schweigt seit sechs Jahren; die Datei ist
nicht veraltet, sondern vollstaendig.

``wcag22`` — W3C Recommendation vom 12.12.2024, unveraendert. WCAG 3.0 ist
weiter in Entwicklung und keine Recommendation.

``withdrawn_drugs`` — die Wikipedia-Seite wurde am 30.09.2026 bearbeitet,
die Liste ist dabei NICHT gewachsen. 180 Eintraege hier gegen 179
Tabellenzeilen dort, und die Unterschiede sind reine Namensvarianten
desselben Wirkstoffs (Aminopyrine/Aminophenazon,
Dextropropoxyphen/Propoxyphen, Zimelidine/Zimeldin). Kein neuer Entzug.

## Der erste Befund: die DOI zeigte auf den falschen Release

``parlgov`` fuehrte als Fundstelle ``doi:10.7910/DVN/Q6CVHX`` und nannte
dazu den "ParlGov 2024 Release". Ueber die Dataverse-API geprueft: Q6CVHX
ist der **2020er** Release, publiziert am 08.12.2020. Der 2024er liegt
unter ``10.7910/DVN/2VZ5ZC``, publiziert am 12.08.2024.

Die Ausgabe-Angabe war also richtig, nur die Fundstelle nicht — und das
ist die gefaehrlichere Haelfte: Wer die Zahlen nachpruefen will, landet in
einem vier Jahre aelteren Datensatz und findet sie dort anders. Dieselbe
Lehre wie #248 (der lebende Link auf die falsche Arbeit) und #247 (ein
Feldname IST eine Quellenangabe): Erreichbarkeit und Identitaet sind zwei
Pruefungen, und die zweite hatte nie jemand gemacht.

## Der zweite Befund: eine ganze Ausgabe Rueckstand

``wid`` traegt Werte aus dem World Inequality Report 2022, Datenjahr 2022.
Der **WIR 2026** ist am 10.12.2025 erschienen und fuehrt Daten bis 2025.
Der Rueckstand steht jetzt ausdruecklich im ``source_label`` und im
``audit_flag``; der Vollrefresh betrifft alle 34 Laender und steht aus.
Nichts wurde ersetzt, was nicht belegt ist (#235) — die alten Werte
bleiben, sie sind nicht falsch, nur nicht aktuell.

## Ein totes Feld, das eine Ausgabe erfand

Nebenbei gefunden: ``wid.json`` trug ``report_year: 2024``. Einen World
Inequality Report 2024 gibt es nicht — die Ausgaben erschienen 2018, 2022
und 2026; 2024 war das Jahr des Snapshots. ``services/wid.py`` hat das
Feld **nie gelesen** (die Ausgabe nennt ausschliesslich ``data_year``),
aber der Fallback-Literal im Service nannte ebenfalls "World Inequality
Database 2024". Beides korrigiert: Feld zu ``snapshot_jahr`` umbenannt,
Literal entjahrt. Marker-Drift (#74) lauert auch in Feldern, die niemand
liest — sie werden zitiert, sobald jemand sie fuer eine Quellenangabe
haelt.

## Und eine Zahl, die zwei Zahlen war

``wcag22`` fuehrte ``total_criteria: 87``. WCAG 2.2 hat **86 aktive**
Erfolgskriterien; 4.1.1 (Parsing) ist obsolet und gilt stets als
bestanden, wird aber mitgelistet. Welche der beiden Zahlen gemeint war,
liess das Feld offen — dieselbe Form wie #233 (zwei Fakten unter einer
Kennung). Jetzt stehen beide getrennt da, und die Level-Zaehlungen sind
aus den Daten nachgerechnet statt gefuehrt.

Dependency-light: nur Daten und Quelltext, kein Netz, kein Modell.
"""
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, os.path.join(BACKEND, "tools"))

from data_freshness_check import KADENZ_MAX_AGE  # noqa: E402

#: Die fuenf Dateien dieser Runde.
DIE_FUENF = ("polity5", "wcag22", "parlgov", "wid", "withdrawn_drugs")

STICHTAG = "2026-10-20"


def _laden(name):
    with open(os.path.join(DATA, f"{name}.json"), encoding="utf-8") as fh:
        return json.load(fh)


def _volltext(name):
    return json.dumps(_laden(name), ensure_ascii=False)


# --- Der Anlass: die fehlende Kadenz ----------------------------------

@pytest.mark.parametrize("name", DIE_FUENF)
def test_jede_datei_erklaert_ihren_takt(name):
    """Der Grund, warum der Waechter sie ueberhaupt gemeldet hat.

    Ohne ``refresh_kadenz`` mass er gegen den 120-Tage-Standard — und
    meldete eine jaehrliche Quelle nach vier Monaten als ueberfaellig.
    """
    d = _laden(name)
    assert d["refresh_kadenz"] == "jaehrlich", d.get("refresh_kadenz")
    assert d["fetched_at_iso"] == STICHTAG
    begruendung = d["kadenz_begruendung"]
    assert len(begruendung) > 80, "eine Kadenz ohne Begruendung ist geraten"
    assert "jaehrlich" in begruendung.lower()


@pytest.mark.parametrize("name", DIE_FUENF)
def test_der_takt_steht_im_vokabular_des_waechters(name):
    """Ein Wert, den ``KADENZ_MAX_AGE`` nicht kennt, faellt still auf den
    Standard zurueck — die Zusage im Feld waere dann wirkungslos."""
    assert _laden(name)["refresh_kadenz"] in KADENZ_MAX_AGE


def test_keine_datei_traegt_einen_takt_den_der_waechter_nicht_kennt():
    """Zensus ueber ALLE Datendateien, ohne Ausnahmeliste.

    Nicht die Liste der fuenf ist das Gate, sondern die Rechnung ueber
    alles (#238). Fiele hier etwas durch, waere es eine Zusage, die
    niemand einloest — die Schuld mit Zinsen aus #230.
    """
    fremde, gepruefte = [], 0
    for datei in sorted(os.listdir(DATA)):
        if not datei.endswith(".json") or datei.startswith((".", "_")):
            continue
        pfad = os.path.join(DATA, datei)
        try:
            with open(pfad, encoding="utf-8") as fh:
                d = json.load(fh)
        except (ValueError, OSError):
            continue
        if not isinstance(d, dict):
            continue
        gepruefte += 1
        k = d.get("refresh_kadenz")
        if k is not None and k not in KADENZ_MAX_AGE:
            fremde.append((datei, k))
    assert gepruefte > 50, f"nur {gepruefte} Dateien gelesen"
    assert not fremde, fremde


# --- parlgov: die falsche Fundstelle ----------------------------------

FALSCHE_DOI = "10.7910/DVN/Q6CVHX"
RICHTIGE_DOI = "10.7910/DVN/2VZ5ZC"


def test_die_richtige_doi_steht_in_der_fundstelle_und_der_zitation():
    d = _laden("parlgov")
    assert RICHTIGE_DOI in d["harvard_dataverse_url"]
    assert RICHTIGE_DOI in d["citation"]


def test_die_falsche_doi_steht_in_keinem_aussage_feld():
    """Die alte DOI darf NUR noch in der Korrektur-Notiz vorkommen.

    Der Split ist derselbe wie bei den SIPRI-Zahlen in #262: In der
    Korrektur-Aufzeichnung gehoert der alte Wert hin, in jedem Feld, das
    eine Aussage macht, nicht.
    """
    d = _laden("parlgov")
    treffer = [k for k, v in d.items()
               if k != "note" and FALSCHE_DOI in json.dumps(v, ensure_ascii=False)]
    assert not treffer, treffer
    assert FALSCHE_DOI in d["note"], "die Korrektur haelt die alte DOI fest"


def test_die_korrektur_notiz_erreicht_den_prompt_nicht():
    """Sonst serviert die Korrektur selbst die falsche DOI (#135).

    ``services/parlgov.py`` liest ``note`` ausschliesslich je WAHL, nicht
    auf der obersten Ebene — und keine Wahl in dieser Datei hat eine. Die
    Korrektur bleibt damit Buchhaltung.
    """
    with open(os.path.join(BACKEND, "services", "parlgov.py"),
              encoding="utf-8") as fh:
        quelle = fh.read()
    for muster in ('data.get("note")', 'data["note"]'):
        assert muster not in quelle, (
            f"{muster} liest jetzt die Top-Level-Notiz — dann traegt die "
            f"Korrektur die alte DOI in die Ausgabe")
    for land in _laden("parlgov")["countries"].values():
        for wahl in land.get("elections") or []:
            assert FALSCHE_DOI not in json.dumps(wahl, ensure_ascii=False)


def test_die_ausgabe_nennt_release_und_publikationsdatum():
    d = _laden("parlgov")
    assert "2024" in d["version"]
    assert "12.08.2024" in d["version"], "das Publikationsdatum fehlt"
    assert "2026-10-20" in d["version"] or "2026-10-20" in d["note"]


def test_die_korrektur_benennt_was_falsch_war():
    """Nicht nur den neuen Wert — auch, dass der alte auf etwas anderes
    zeigte. Sonst prueft der naechste dieselbe DOI wieder."""
    note = _laden("parlgov")["note"]
    assert "KORREKTUR 2026-10-20" in note
    assert "2020" in note, "der echte Release hinter Q6CVHX fehlt"
    assert "belegt nichts" in note


# --- wid: der Ausgaben-Rueckstand -------------------------------------

def test_der_rueckstand_steht_im_label_das_zitiert_wird():
    """Das ``source_label`` geht in die Ausgabe. Dort muss es stehen, nicht
    nur in einem Audit-Feld, das niemand serviert."""
    label = _laden("wid")["source_label"]
    assert "ACHTUNG" in label
    assert "WIR 2026" in label
    assert "10.12.2025" in label
    assert "NICHT eingearbeitet" in label


def test_das_audit_feld_nennt_den_umfang_des_offenen_refresh():
    flag = _laden("wid")["audit_flag"]
    assert "RUECKSTAND" in flag
    assert "34 Laender" in flag
    assert "steht aus" in flag


def test_die_alten_werte_bleiben_unangetastet():
    """Nichts ersetzen, was man nicht belegen kann (#235). Die WIR-2022-
    Werte sind nicht falsch — sie sind eine Ausgabe alt."""
    d = _laden("wid")
    assert d["data_year"] == 2022
    assert len(d["shares"]) == 34
    assert "2022" in d["citation"]


def test_das_tote_feld_erfindet_keine_ausgabe_mehr():
    """``report_year: 2024`` nannte eine Ausgabe, die es nicht gibt.

    Die Reports erschienen 2018, 2022 und 2026. Niemand las das Feld —
    aber ein Feldname IST eine Quellenangabe (#247), und wer ihn liest,
    zitiert eine Ausgabe, die nie erschienen ist.
    """
    d = _laden("wid")
    assert "report_year" not in d
    assert d["snapshot_jahr"] == 2024
    assert "gibt es nicht" in d["jahr_hinweis"]


def test_der_service_nennt_im_fallback_kein_ausgabe_jahr():
    """Marker-Drift (#74): Das Literal im Service wird beim Daten-Refresh
    nicht mitgezogen. Dann nennt es besser gar keine Jahreszahl."""
    with open(os.path.join(BACKEND, "services", "wid.py"),
              encoding="utf-8") as fh:
        quelle = fh.read()
    i = quelle.index("source_label = data.get(")
    j = quelle.index("secondary_url = data.get(")
    fallback = quelle[i:j]
    assert "World Inequality Database" in fallback
    for jahr in ("2018", "2022", "2024", "2026"):
        assert jahr not in fallback, (
            f"der Fallback nennt {jahr} — beim naechsten Refresh driftet er")


def test_die_ausgabe_nennt_weiterhin_das_datenjahr():
    """Die Gegenprobe zur Entjahrung: ``data_year`` MUSS in die Ausgabe,
    sonst steht eine Zahl ohne Bezug da."""
    with open(os.path.join(BACKEND, "services", "wid.py"),
              encoding="utf-8") as fh:
        quelle = fh.read()
    assert 'data.get("data_year", 2022)' in quelle
    assert "{data_year}" in quelle


# --- wcag22: eine Zahl, die zwei Zahlen war ---------------------------

def test_gelistete_und_aktive_kriterien_stehen_getrennt():
    kf = _laden("wcag22")["key_facts"]
    assert kf["gelistete_kriterien"] == 87
    assert kf["aktive_kriterien"] == 86
    assert "4.1.1" in kf["kriterien_hinweis"]


def test_die_mehrdeutige_zahl_ist_verschwunden():
    """``total_criteria: 87`` liess offen, welche der beiden Zahlen gemeint
    ist — dieselbe Form wie zwei Fakten unter einer Kennung (#233).

    Geprueft werden SCHLUESSEL, nicht der Volltext: Der Hinweis nennt den
    alten Feldnamen absichtlich, damit der naechste die Umbenennung
    nachvollziehen kann. Eine erste Fassung dieses Tests suchte im
    serialisierten JSON und schlug an ihrer eigenen Erklaerung fehl.
    """
    def _schluessel(x):
        if isinstance(x, dict):
            for k, v in x.items():
                yield k
                yield from _schluessel(v)
        elif isinstance(x, list):
            for v in x:
                yield from _schluessel(v)

    alle = set(_schluessel(_laden("wcag22")))
    for alt in ("total_criteria", "level_a_count", "level_aa_count",
                "level_aaa_count"):
        assert alt not in alle, f"{alt} ist noch ein Feld"
    assert "aktive_kriterien" in alle, "liest der Test ins Leere?"


def test_die_level_zaehlungen_sind_aus_den_daten_nachgerechnet():
    """Eine gefuehrte Zahl neben einer berechenbaren driftet (#134).

    Diese Rechnung ist der Grund, warum die Zaehlungen ueberhaupt falsch
    waren: Sie summierten auf 87 und zaehlten damit das obsolete
    Kriterium mit.
    """
    d = _laden("wcag22")
    kf = d["key_facts"]
    obsolet = set(kf["obsoleted_in_2_2"])
    je_level = {}
    for k in d["success_criteria"]:
        if k["num"] in obsolet:
            continue
        je_level[k["level"]] = je_level.get(k["level"], 0) + 1
    assert kf["level_a_count_aktiv"] == je_level["A"]
    assert kf["level_aa_count_aktiv"] == je_level["AA"]
    assert kf["level_aaa_count_aktiv"] == je_level["AAA"]
    assert sum(je_level.values()) == kf["aktive_kriterien"] == 86
    assert len(d["success_criteria"]) == kf["gelistete_kriterien"] == 87


def test_das_obsolete_kriterium_bleibt_gelistet_und_markiert():
    """Es wird mitgefuehrt, weil Pruefberichte es noch nennen — aber es
    gilt stets als bestanden. Es zu loeschen waere falsch, es
    mitzuzaehlen auch."""
    d = _laden("wcag22")
    nummern = {k["num"] for k in d["success_criteria"]}
    assert "4.1.1" in nummern
    assert d["key_facts"]["obsoleted_in_2_2"] == ["4.1.1"]


def test_die_fassung_ist_datiert_und_grenzt_wcag3_positiv_ab():
    v = _laden("wcag22")["version"]
    assert "12.12.2024" in v
    assert "2026-10-20" in v or "geprueft 2026-10-20" in v
    assert "WCAG 3.0" in v and "KEINE Recommendation" in v


# --- polity5: die Quelle schweigt -------------------------------------

def test_polity5_haelt_fest_dass_die_quelle_schweigt():
    """Ein Refresh, der nichts aendert, ist auch ein Ergebnis — und er
    gehoert dokumentiert, sonst prueft der naechste von vorn."""
    flag = _laden("polity5")["audit_flag"]
    assert flag.startswith("GEPRUEFT 2026-10-20")
    assert "Kein v2019+" in flag
    assert "2018" in flag


def test_polity5_nennt_sein_abdeckungsende():
    d = _laden("polity5")
    assert "2018" in d["version_note"]
    assert len(d["scores_2018"]) == len(d["country_codes"]) == 43


def test_polity5_traegt_keinen_unerledigten_audit_marker_mehr():
    """``AUDIT NEEDED`` hiess bei MIPEX, dass 52 % der Werte falsch waren
    (#266). Der Marker ist eine offene Rechnung, kein Hinweis."""
    assert "AUDIT NEEDED" not in _volltext("polity5")


# --- withdrawn_drugs: nicht gewachsen ---------------------------------

def test_die_liste_ist_unveraendert_gross():
    assert len(_laden("withdrawn_drugs")["items"]) == 180


def test_die_pruefung_haelt_fest_dass_nichts_dazukam():
    note = _laden("withdrawn_drugs")["note"]
    assert "GEPRUEFT 2026-10-20" in note
    assert "NICHT gewachsen" in note
    assert "Kein neuer Entzug" in note


def test_die_namensvarianten_sind_benannt_nicht_verschwiegen():
    """180 hier gegen 179 dort ist eine Differenz, die erklaert werden
    muss — sonst liest der naechste sie als fehlenden Eintrag.

    Dass es Varianten sind und keine Luecke, war erst nach dem Vergleich
    ueber das Feld ``inn`` klar; eine erste Messung verglich ``drug``
    bzw. ``name``, fand 0 Namen und meldete "185 fehlen".
    """
    note = _laden("withdrawn_drugs")["note"]
    assert "Namensvarianten" in note
    for paar in ("Aminopyrine", "Dextropropoxyphen", "Zimelidine"):
        assert paar in note, f"{paar} fehlt in der Erklaerung"


def test_das_vergleichsfeld_existiert_in_jedem_eintrag():
    """Pinnt, worueber verglichen wurde. Ohne ``inn`` ist der Vergleich
    gegen die Wikipedia-Tabelle nicht wiederholbar."""
    ohne = [i for i in _laden("withdrawn_drugs")["items"]
            if not (i.get("inn") or "").strip()]
    assert not ohne, ohne[:5]


# --- Was diese Runde NICHT behauptet ----------------------------------

def test_der_offene_wid_refresh_ist_nicht_als_erledigt_markiert():
    """Die Gegenprobe zur Ehrlichkeit: Die Datei darf nicht behaupten,
    auf WIR 2026 zu stehen. Sie steht auf WIR 2022 und sagt das."""
    d = _laden("wid")
    assert "2022" in d["source_label"]
    assert "wir2026" not in (d.get("secondary_url") or "").lower(), (
        "die Zweit-URL zeigt auf WIR 2026, die Daten stammen aus WIR 2022")
