"""Der Service las ein Feld, das ich selbst umbenannt hatte — und erfand 62.

QA50I-Befund (20.10.2026). Der Claim "WCAG 2.2 hat 87 Erfolgskriterien"
bekam ``false@0.8``:

    "WCAG 2.2 enthält 62 Erfolgskriterien (Success Criteria), nicht 87."

62 ist frei erfunden. Die Ursache stand in ``services/wcag22.py``:

    total = key_facts.get("total_criteria", "?")

PR #267 hatte ``total_criteria`` in ``gelistete_kriterien`` (87) und
``aktive_kriterien`` (86) aufgeteilt, weil die eine Zahl zwei Zahlen war.
Diese Service-Zeile blieb stehen. Der Service lieferte seither woertlich

    "WCAG 2.2 (...): ? Success Criteria über 4 Prinzipien ..."

Ein Fragezeichen an der Stelle der Zahl — und das Modell hat die Luecke
gefuellt.

## Die eigentliche Lehre ist nicht die Zeile

Mein Test aus #267 prueffte, dass ``total_criteria`` in den DATEN weg ist.
Dass der SERVICE es nicht mehr liest, prueffte nichts. Das Gate war
einseitig, und ich bin beim Reparieren einer Marker-Drift in genau eine
hineingelaufen.

``tools/feldzugriffe_pruefen.py`` schliesst diese Einseitigkeit: Es liest
per AST-Taint-Analyse, welche Felder ein Service aus seiner JSON liest,
und prueft jedes gegen die JSON. Kein Textvergleich, keine Ausnahmeliste.

## Es hat zwei weitere Faelle gefunden

``services/mipex.py`` las weiterhin ``rank``, das PR #266 in
``rang_im_ausschnitt`` umbenannt hatte. Dort war der Schaden
spiegelbildlich: Nichts wurde erfunden, der Rang fiel STILL aus dem
``indicator_name`` — und das hartkodierte "/52" wartete im Quelltext
darauf, wieder zu greifen, obwohl der Ausschnitt 22 Laender hat. #266
hatte den ``display_value`` korrigiert und diese zweite Funktion
uebersehen.

``services/vdem.py`` prueffte ``ind.get("v14_verified") is False``. Das
Feld heisst seit PR #27 ``dataset_verified``; ``None is False`` ist
``False``, die Haelfte der Bedingung war seither tot. Kein Schaden, aber
eine Absicht im Code, die nichts tut.

## Ein eigener Fehler beim Bauen des Werkzeugs

Die erste Fassung prueffte Loader-Namen mit ``in`` statt auf Gleichheit.
``"json.load"`` traf damit auch ``json.loads`` — und ``datacommons.py``
parst damit HTTP-Antworten. Ergebnis: neun schema.org-Felder als
"fehlende Datenfelder" gemeldet. Dieselbe Teilstring-Falle wie "wie viel"
in "wie viele" (#231).

Die zweite Fassung wollte das mit einer Ausnahmeliste von vier Feldnamen
erschlagen. Das war Schuld mit Zinsen (#230). Stattdessen ist die URSACHE
benannt: Ein Modul, das JSON aus einer Zeichenkette parst, bekommt Daten
aus dem Netz, und ein Name kann dort zwei Gestalten tragen. Gemessen
trifft diese Regel GENAU EIN Modul.

Dependency-light: nur Quelltext und Daten, kein Netz, kein Modell.
"""
import ast
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVICES = os.path.join(BACKEND, "services")
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, os.path.join(BACKEND, "tools"))
sys.path.insert(0, BACKEND)

import feldzugriffe_pruefen as fp  # noqa: E402


def _quelle(name):
    with open(os.path.join(SERVICES, name), encoding="utf-8") as fh:
        return fh.read()


def _nur_code(name):
    """Der Quelltext mit Kommentaren und Strings als Leerzeichen.

    Dieselbe Begruendung wie in test_bti_amtliche_werte.py: Die
    Erklaerungen im Quelltext zitieren die alten, falschen Formen
    absichtlich. Eine erste Fassung dieser Pruefungen suchte im Volltext
    und schlug an genau diesen Kommentaren fehl.
    """
    import io
    import tokenize
    quelle = _quelle(name)
    zeilen = quelle.splitlines(keepends=True)
    for tok in tokenize.generate_tokens(io.StringIO(quelle).readline):
        # FSTRING_MIDDLE muss mit: Seit Python 3.12 ist ein f-String
        # nicht mehr EIN STRING-Token, sondern FSTRING_START /
        # FSTRING_MIDDLE / FSTRING_END. Ohne den Mittelteil bleibt der
        # Text eines f-Strings stehen — und die erste Fassung dieser
        # Pruefung schlug deshalb an `f"... (MIPEX 2020)"` fehl, das sie
        # fuer ausfuehrbaren Code hielt. (Der Treffer war echt und ist
        # behoben; die Pruefung war trotzdem unscharf.)
        blenden = {tokenize.COMMENT, tokenize.STRING}
        for name in ("FSTRING_MIDDLE",):
            if hasattr(tokenize, name):
                blenden.add(getattr(tokenize, name))
        if tok.type not in blenden:
            continue
        (z1, s1), (z2, s2) = tok.start, tok.end
        for z in range(z1, z2 + 1):
            zeile = zeilen[z - 1]
            von = s1 if z == z1 else 0
            bis = s2 if z == z2 else len(zeile.rstrip("\n"))
            zeilen[z - 1] = zeile[:von] + " " * (bis - von) + zeile[bis:]
    return "".join(zeilen)


def _daten(name):
    with open(os.path.join(DATA, name), encoding="utf-8") as fh:
        return json.load(fh)


# --- Das Gate ---------------------------------------------------------

def test_kein_service_liest_ein_feld_das_seine_json_nicht_hat():
    """Der Zensus ueber ALLE Services ist das Gate, nicht die Liste der
    drei Fundstellen (#238).

    Faellt dieser Test, hat jemand ein Datenfeld umbenannt oder entfernt,
    ohne den Leser mitzuziehen — und der Service serviert seither einen
    Default statt einer Zahl.
    """
    befunde = []
    gepruefte = 0
    for name in sorted(os.listdir(SERVICES)):
        if not name.endswith(".py") or name.startswith("_"):
            continue
        gepruefte += 1
        for datei, feld, zeile in fp.pruefe_service(
                os.path.join(SERVICES, name), DATA):
            befunde.append(f"{name}:{zeile} liest {feld!r}, fehlt in {datei}")
    assert gepruefte > 100, f"nur {gepruefte} Services gelesen"
    assert not befunde, "\n".join(befunde)


def test_das_gate_prueft_wirklich_etwas():
    """Die Gegenprobe zum vorigen Test.

    Ein Zensus, der nichts findet, weil er nichts ansieht, ist gruen und
    wertlos. Gemessen werden deshalb die ABGEDECKTEN Services und die
    Zahl der geprueften Feldzugriffe.
    """
    mit_feldern = 0
    zugriffe = 0
    for name in sorted(os.listdir(SERVICES)):
        if not name.endswith(".py") or name.startswith("_"):
            continue
        baum = ast.parse(_quelle(name))
        if not fp.json_dateien(baum) or fp.parst_zeichenketten(baum):
            continue
        namen = fp.getroffene_namen(baum)
        felder = set(fp.gelesene_felder(baum, namen)) if namen else set()
        if felder:
            mit_feldern += 1
            zugriffe += len(felder)
    # Gemessen am 20.10.2026: 82 Services, 1.202 Feldzugriffe. Vor der
    # Weiterleitung durch Modul-Funktionen waren es 22 und 409 — die
    # Pack-Services blieben ungeprueft, und dort liegen die meisten
    # Felder. Die Schranken sind Untergrenzen mit Abstand, damit ein
    # neuer Service sie nicht reisst, ein Verlust der Abdeckung aber
    # auffaellt.
    assert mit_feldern >= 70, f"nur {mit_feldern} Services mit Feldzugriffen"
    assert zugriffe >= 1000, f"nur {zugriffe} Feldzugriffe geprueft"


def test_der_loader_name_wird_exakt_verglichen():
    """``json.load`` liest eine Datei, ``json.loads`` eine Zeichenkette.

    Die erste Fassung verglich mit ``in`` und meldete deshalb neun
    schema.org-Felder aus einer HTTP-Antwort als fehlende Datenfelder —
    dieselbe Teilstring-Falle wie #231.
    """
    assert "json.load" in fp.LOADER_NAMEN
    assert "json.loads" not in fp.LOADER_NAMEN
    knoten = ast.parse("json.loads(x)").body[0].value
    assert not fp._ist_loader(knoten)
    knoten = ast.parse("json.load(f)").body[0].value
    assert fp._ist_loader(knoten)


def test_es_gibt_keine_ausnahmeliste():
    """Eine Ausnahmeliste ist Schuld mit Zinsen (#230). Die Ursache ist
    in ``parst_zeichenketten`` benannt, nicht in einer Liste versteckt."""
    assert fp.NICHT_DATENFELDER == frozenset()


def test_die_ausschlussregel_trifft_genau_ein_modul():
    """Sonst ist sie keine Regel, sondern eine getarnte Ausnahmeliste."""
    betroffen = []
    for name in sorted(os.listdir(SERVICES)):
        if not name.endswith(".py") or name.startswith("_"):
            continue
        baum = ast.parse(_quelle(name))
        if not fp.json_dateien(baum):
            continue
        if fp.parst_zeichenketten(baum) and fp.getroffene_namen(baum):
            betroffen.append(name)
    assert betroffen == ["datacommons.py"], betroffen


def test_die_pack_services_sind_abgedeckt():
    """61 Services laden ueber ``load_items`` und lesen ihre Felder von
    den ELEMENTEN der zurueckgegebenen Liste. Ohne die Listen-Weitergabe
    des Taints blieben sie ungeprueft — und dort liegen die meisten
    Felder."""
    assert "load_items" in fp.LOADER_NAMEN
    assert "load_items" in fp.LOADER_NAMEN
    assert "find_matching_items" in fp.LOADER_NAMEN
    baum = ast.parse(_quelle("arbeitsmarkt_pack.py"))
    # Die Pack-Services schreiben `return find_matching_items(...)` in eine
    # Modul-Funktion und rufen die auf. Ohne diese Weiterleitung endet der
    # Taint an der Funktionsgrenze.
    assert "_claim_matches_facts" in fp.weiterleitende_funktionen(baum)
    namen = fp.getroffene_namen(baum)
    felder = {f for f, _z in fp.gelesene_felder(baum, namen)}
    assert "topic" in felder, "die Fakten-Felder werden nicht erreicht"
    assert len(felder) >= 5, felder


# --- wcag22: die Zahl, die zwei Zahlen ist ---------------------------

def test_wcag_nennt_beide_zahlen_oder_keine():
    from services.wcag22 import _build_overview_result
    r = _build_overview_result(_daten("wcag22.json"))
    d = r["display_value"]
    assert "86 AKTIVE" in d
    assert "gelistet sind 87" in d
    assert "4.1.1" in d
    assert "?" not in d, "der Platzhalter steht wieder in der Ausgabe"
    assert r["value"] == "86"


def test_wcag_erfindet_keine_zahl_wenn_das_feld_fehlt():
    """Der eigentliche Schutz: Fehlt die Zahl, muss die Ausgabe das SAGEN.

    Vorher stand dort ein '?', das das Modell gefuellt hat. Jetzt steht
    eine Aufforderung, nicht zu schaetzen.
    """
    from services.wcag22 import _build_overview_result
    d = _daten("wcag22.json")
    d["key_facts"].pop("aktive_kriterien")
    r = _build_overview_result(d)
    assert "nicht verfuegbar" in r["display_value"]
    assert "nicht schaetzen" in r["display_value"]
    assert r["value"] == ""
    assert "?" not in r["display_value"]


def test_wcag_nimmt_die_daten_aus_den_daten():
    """Die Fassungs-Daten standen als Literale im Service und haetten
    jede kuenftige Fassung stumm ueberlebt (#74/#133)."""
    code = _nur_code("wcag22.py")
    i = code.index("def _build_overview_result")
    j = code.index("\ndef ", i + 1)
    block = code[i:j]
    for literal in ("05.10.2023", "12.12.2024", "2023"):
        assert literal not in block, f"{literal} steht wieder im Code"
    assert 'data.get("published")' in _quelle("wcag22.py")
    assert 'data.get("updated")' in _quelle("wcag22.py")
    from services.wcag22 import _build_overview_result
    d = _build_overview_result(_daten("wcag22.json"))
    assert "05.10.2023" in d["display_value"], "Datum nicht deutsch formatiert"
    assert d["year"] == "2024"


# --- mipex: der Rang fiel still aus dem Namen ------------------------

def test_mipex_nennt_den_platz_im_ausschnitt():
    from services.mipex import _build_indicator_name, _LAENDER_IM_AUSSCHNITT
    md = _daten("mipex.json")
    n = _build_indicator_name("AUT", md["scores"]["AUT"], md, None)
    assert "Platz 19" in n
    assert f"von {_LAENDER_IM_AUSSCHNITT}" in n
    assert "Ausschnitt" in n
    assert _LAENDER_IM_AUSSCHNITT == 22


def test_mipex_traegt_die_52_nicht_mehr_im_quelltext():
    """Die 52 war die Laenderzahl des vollen MIPEX-Index, nicht die des
    Auszugs. #266 hat sie aus dem display_value entfernt und diese
    Funktion uebersehen."""
    code = _nur_code("mipex.py")
    i = code.index("def _build_indicator_name")
    j = code.index("\ndef ", i + 1)
    block = code[i:j]
    assert "52" not in block, "die 52 steht wieder im ausfuehrbaren Code"
    assert 'rec.get("rank")' not in _quelle("mipex.py")
    assert 'rec.get("rang_im_ausschnitt")' in _quelle("mipex.py")


def test_wcag_folgt_einem_geaenderten_datum():
    """Verhaltens-Probe statt Textsuche.

    Eine Textsuche nach "05.10.2023" kann hier nicht greifen: In
    ``_nur_code`` sind Strings ausgeblendet, und im Volltext steht das
    Datum in meinem eigenen Kommentar. Geprueft wird deshalb, dass die
    Ausgabe einem GEAENDERTEN Datenfeld folgt — ein Literal im Code
    koennte das nicht.
    """
    from services.wcag22 import _build_overview_result
    d = _daten("wcag22.json")
    d["published"] = "2027-01-02"
    d["updated"] = "2027-03-04"
    r = _build_overview_result(d)
    assert "02.01.2027" in r["display_value"]
    assert "04.03.2027" in r["display_value"]
    assert "05.10.2023" not in r["display_value"]
    assert r["year"] == "2027"


def test_wcag_liest_das_datum_nicht_aus_einem_literal():
    """Die Gegenprobe auf der Code-Seite, ohne an Kommentaren zu
    scheitern: geprueft wird die ZUWEISUNG, nicht das Vorkommen."""
    q = _quelle("wcag22.py")
    assert 'veroeffentlicht = _de_datum(data.get("published")' in q
    assert 'veroeffentlicht = "' not in q, "das Datum steht wieder als Literal"
    assert 'geaendert = "' not in q


def test_mipex_folgt_einer_geaenderten_ausgabe():
    """Dieselbe Verhaltens-Probe fuer das Ausgabe-Literal.

    ``(MIPEX 2020)`` stand im f-String des display_value — heute noch
    zufaellig richtig, weil ``report_year`` 2020 ist. Es haette jeden
    Refresh stumm ueberlebt, wie ``(BTI 2024)`` in #268.
    """
    from services.mipex import _build_result_row
    d = _daten("mipex.json")
    d["report_year"] = 2031
    r = _build_result_row("AUT", d["scores"]["AUT"], ["SWE"], d["scores"],
                          d, None)
    assert "MIPEX 2031" in r["display_value"]
    assert "MIPEX 2020" not in r["display_value"]
    assert "MIPEX 2031" in r["indicator_name"]
    assert r["year"] == "2031"


def test_mipex_verlangt_die_ausgabe_als_pflicht_parameter():
    """Ein Default haette die Angabe lautlos weggelassen — der halbe
    Fehler. Gemeldet hat das ``test_der_servierte_satz_nennt_die_ausgabe``
    aus #266, als ich es zuerst mit Default gebaut habe."""
    import inspect
    from services.mipex import _build_display_value
    sig = inspect.signature(_build_display_value)
    assert "ausgabe" in sig.parameters
    assert sig.parameters["ausgabe"].default is inspect.Parameter.empty


def test_mipex_erfindet_keinen_fremden_laendercode():
    """``iso3[:2]`` machte bei BTI aus COD ein CO (Kolumbien, #268).
    In mipex stand es an drei Stellen."""
    code = _nur_code("mipex.py")
    assert "[:2]" not in code


def test_mipex_erfindet_kein_ausgabe_jahr():
    """Der Default stand an ZWEI Stellen. Die erste hatte #266 uebersehen,
    die zweite mein erster Fix von heute — deshalb wird der ganze
    ausfuehrbare Code geprueft, nicht eine Funktion."""
    assert "2020" not in _nur_code("mipex.py")
    assert 'data.get("report_year", 2020)' not in _quelle("mipex.py")


@pytest.mark.parametrize("iso,platz", [("AUT", 19), ("SWE", 1)])
def test_mipex_platz_stimmt_mit_den_daten(iso, platz):
    from services.mipex import _build_indicator_name
    md = _daten("mipex.json")
    assert md["scores"][iso]["rang_im_ausschnitt"] == platz
    assert f"Platz {platz} " in _build_indicator_name(
        iso, md["scores"][iso], md, None)


# --- vdem: die tote Haelfte ------------------------------------------

def test_vdem_prueft_nur_noch_das_feld_das_es_gibt():
    q = _quelle("vdem.py")
    assert 'ind.get("v14_verified")' not in q
    assert 'ind.get("dataset_verified") is False' in q


def test_vdem_fuehrt_das_feld_fuer_jeden_indikator():
    """Sonst greift die Warnung nirgends — und das waere genauso tot wie
    die entfernte Haelfte."""
    d = _daten("vdem_indicators.json")
    ind = d["indicators"]
    werte = [i.get("dataset_verified") for i in (
        ind.values() if isinstance(ind, dict) else ind)]
    assert len(werte) == 11, len(werte)
    assert all(v is True for v in werte), werte


# --- Das Werkzeug ----------------------------------------------------

def test_das_werkzeug_meldet_befunde_ueber_den_exit_code():
    q = open(os.path.join(BACKEND, "tools", "feldzugriffe_pruefen.py"),
             encoding="utf-8").read()
    assert "BEFUND_EXITS" in q
    assert "return 1 if befunde else 0" in q


def test_das_werkzeug_findet_einen_eingebauten_fehler(tmp_path):
    """Ein Instrument, das seinen eigenen Anlass nicht findet, taugt
    nicht (#229). Nachgestellt wird genau der Fall aus #267."""
    svc = tmp_path / "services"
    dat = tmp_path / "data"
    svc.mkdir()
    dat.mkdir()
    (dat / "probe.json").write_text(
        json.dumps({"key_facts": {"aktive_kriterien": 86}}), encoding="utf-8")
    (svc / "probe.py").write_text(
        "import json\n"
        "PFAD = 'probe.json'\n"
        "def _load_data():\n"
        "    with open(PFAD) as f:\n"
        "        return json.load(f)\n"
        "def bau():\n"
        "    data = _load_data()\n"
        "    kf = data.get('key_facts') or {}\n"
        "    return kf.get('total_criteria', '?')\n", encoding="utf-8")
    fehlend = fp.pruefe_service(str(svc / "probe.py"), str(dat))
    assert [f for _d, f, _z in fehlend] == ["total_criteria"], fehlend

    # Gegenprobe: das richtige Feld wird NICHT gemeldet.
    (svc / "probe.py").write_text(
        (svc / "probe.py").read_text(encoding="utf-8").replace(
            "total_criteria", "aktive_kriterien"), encoding="utf-8")
    assert fp.pruefe_service(str(svc / "probe.py"), str(dat)) == []
