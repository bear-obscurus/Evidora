"""RKI-Surveillance: Atemwegs-Saison 2025/26 — und drei falsche Zahlen im alten Stand.

Befund (2026-09-05, erster Mittel-Eintrag aus dem Freshness-Rückstand): Der Fakt
`rki_atemwegsinfekte` beschrieb den Winter 2024/25. Beim Nachrechnen gegen die
RKI-Open-Data zeigte sich, dass er nicht nur alt war:

  Wert                      Fakt sagte   RKI-Daten sagen
  Peak 2024/25 (KW 5)         9.870          9.290
  „Peak" KW 2/2024            7.320          5.004  — und KW 2/2024 war gar nicht
                                                     der Höhepunkt: die Saison
                                                     2023/24 gipfelte in KW 50/2023
                                                     bei 10.434
  typischer Vor-Pandemie-Peak 7.000          8.660 (2018/19) bzw. 8.529 (2019/20)

Daraus folgte eine RICHTUNGS-Aussage, die verkehrt ist: der Fakt nannte 2024/25
„deutlich stärker als 2023/24" — tatsächlich war 2024/25 mit 9.290 SCHWÄCHER als
2023/24 mit 10.434. Und „etwa 30 % über dem typischen Vor-Pandemie-Winterpeak"
wird zu rund 7 %, sobald man den echten Vor-Pandemie-Wert einsetzt.

**Die Messgrößen-Falle beim Refresh**: Es gibt zwei ARE-Reihen, und sie
unterscheiden sich um den Faktor vier. GrippeWeb misst bevölkerungsbasiert per
Selbstauskunft (Saison-Höchstwert 2025/26: 8.661 je 100.000), die
ARE-Konsultationsinzidenz misst Arztbesuche im Praxis-Sentinel (2.044). Der alte
Fakt nutzte GrippeWeb — erkennbar daran, dass seine Peak-WOCHE exakt zur
GrippeWeb-Reihe passt. Wer beim Refresh die Konsultationszahl einsetzt, erzeugt
einen Einbruch von 9.290 auf 2.044, den es nicht gibt.

Masern und Tuberkulose wurden bewusst NICHT aktualisiert: SurvStat 2.0 ist ein
interaktives Abfragewerkzeug ohne offenes Datenabbild, Infektionsradar und der
ECDC-Atlas liefern die Werte nicht maschinenlesbar. Statt zu schätzen, tragen
beide Fakten ihren Datenstand jetzt ausdrücklich in der Headline — insbesondere,
dass die 142 Masernfälle ein Q1-Zwischenstand sind und kein Jahreswert.
"""

import asyncio
import ast
import json
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
DATEN = json.loads((BACKEND / "data" / "rki_surveillance.json").read_text(encoding="utf-8"))
FAKTEN = {f["topic"]: f for f in DATEN["facts"]}
QUELLE = (BACKEND / "services" / "rki_surveillance.py").read_text(encoding="utf-8")

MAX_STR = 400


def _nur_code(quelle: str) -> str:
    """Quelltext ohne Kommentare und Docstrings — die dürfen Historie nennen."""
    baum = ast.parse(quelle)
    code = quelle
    for k in ast.walk(baum):
        if isinstance(k, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            doc = ast.get_docstring(k, clean=False)
            if doc:
                code = code.replace(doc, "")
    return "\n".join(z for z in code.splitlines() if not z.lstrip().startswith("#"))


CODE = _nur_code(QUELLE)

from services.rki_surveillance import (  # noqa: E402
    _saisonreihe,
    _zuwachs,
    search_rki_surveillance,
)


def _ergebnis(claim, topic):
    r = asyncio.run(search_rki_surveillance({"claim": claim, "original_claim": claim}))
    for x in r["results"]:
        if x["topic"] == topic:
            return x
    raise AssertionError(f"{claim!r} liefert nichts zu {topic!r}")


# --------------------------------------------------------------------------
# Die Saison 2025/26 und die korrigierte Richtung
# --------------------------------------------------------------------------

D = FAKTEN["rki_atemwegsinfekte"]["data"]


def test_saison_2025_26_ist_da():
    assert D["grippeweb_are_peak_je_100k_2025_26"] == 8661
    assert D["grippeweb_are_peak_woche_2025_26"] == "KW 5/2026"


def test_hoechstwert_sinkt_seit_2022_23():
    """Die zentrale Richtungsaussage — der alte Fakt behauptete das Gegenteil
    für 2024/25 gegenüber 2023/24."""
    reihe = [D[f"grippeweb_are_peak_je_100k_{s}"]
             for s in ("2022_23", "2023_24", "2024_25", "2025_26")]
    assert reihe == [11204, 10434, 9290, 8661]
    assert reihe == sorted(reihe, reverse=True), "die Reihe muss monoton fallen"


def test_2024_25_war_schwaecher_als_2023_24():
    assert (D["grippeweb_are_peak_je_100k_2024_25"]
            < D["grippeweb_are_peak_je_100k_2023_24"])
    kopf = FAKTEN["rki_atemwegsinfekte"]["headline"]
    assert "SCHWÄCHSTE" in kopf and "2025/26" in kopf


def test_vor_pandemie_referenz_ist_die_echte():
    """Der alte Fakt setzte 7.000 an — die tatsächlichen Vor-Pandemie-Peaks
    liegen bei 8.660 und 8.529."""
    assert D["grippeweb_are_peak_vor_pandemie_2018_19"] == 8660
    assert D["grippeweb_are_peak_vor_pandemie_2019_20"] == 8529
    for alt in ("7000", "7.000", "9870", "9.870", "7320", "7.320"):
        assert alt not in json.dumps(D, ensure_ascii=False), f"alter Wert {alt} noch da"


def test_sari_und_erreger_reihen_sind_vollstaendig():
    for praefix, erwartet in (
        ("sari_hospitalisierung_peak_je_100k_", [38.2, 32.1, 34.3, 29.1]),
        ("influenza_meldefaelle_ifsg_", [217770, 397681, 247202]),
        ("rsv_meldefaelle_ifsg_", [58851, 69386, 70587]),
    ):
        werte = [D[k] for k in sorted(k for k in D if k.startswith(praefix)
                                      and k[len(praefix):].replace("_", "").isdigit())]
        assert werte == erwartet, f"{praefix}: {werte}"


def test_covid_spielt_ambulant_kaum_noch_eine_rolle():
    assert D["covid_anteil_an_are_konsultationen_peakwoche_2025_26_pct"] == 0.6


# --------------------------------------------------------------------------
# Die Messgrößen-Falle
# --------------------------------------------------------------------------

def test_messgroessen_warnung_steht_im_ungekuerzten_kanal():
    """Beide ARE-Reihen unterscheiden sich um den Faktor vier — die Warnung
    muss die Prompt-Kürzung überleben, also in die Headline."""
    kopf = FAKTEN["rki_atemwegsinfekte"]["headline"]
    assert "MESSGRÖSSE" in kopf
    assert "GrippeWeb" in kopf and "Konsultationsinzidenz" in kopf
    assert len(kopf) <= MAX_STR


def test_messgroessen_feld_nennt_beide_zahlen():
    m = D["messgroesse"]
    assert "8.661" in m and "2.044" in m
    assert "NICHT TAUSCHEN" in m


# --------------------------------------------------------------------------
# Was NICHT aktualisiert wurde, darf keine Aktualität vortäuschen
# --------------------------------------------------------------------------

def test_masern_nennt_den_q1_zwischenstand_als_solchen():
    kopf = FAKTEN["rki_masern"]["headline"]
    assert "DATENSTAND" in kopf
    assert "Quartal" in kopf or "Q1" in kopf
    assert "kein Jahreswert" in kopf
    assert "datenstand" in FAKTEN["rki_masern"]["data"]


def test_tuberkulose_nennt_seinen_datenstand():
    kopf = FAKTEN["rki_tuberkulose"]["headline"]
    assert "DATENSTAND" in kopf and "2024" in kopf
    assert "datenstand" in FAKTEN["rki_tuberkulose"]["data"]


def test_pruefvermerk_sagt_was_nicht_aktualisiert_wurde():
    v = DATEN["letzte_pruefung"]
    assert "NICHT aktualisiert" in v
    assert "SurvStat" in v, "der Grund muss dastehen, nicht nur die Tatsache"


# --------------------------------------------------------------------------
# Renderer: nichts mehr verdrahtet
# --------------------------------------------------------------------------

def test_keine_ausgerechnete_prozentzahl_mehr_im_code():
    """'+706 %' stand als Literal im Renderer — beim nächsten Daten-Refresh
    hätte es nicht mehr zu den Zahlen gepasst."""
    assert "706" not in CODE
    assert "_zuwachs(" in CODE


def test_zuwachs_rechnet_und_faellt_weich():
    assert _zuwachs(79, 637).startswith("+706 %")
    assert _zuwachs(100, 50).startswith("-50 %")
    assert _zuwachs(0, 5) == "" and _zuwachs(None, 5) == ""


def test_saisonen_stehen_in_den_daten_nicht_im_code():
    """Eine neue Saison ergänzt man in der JSON, ohne den Renderer anzufassen."""
    assert "_saisonreihe(" in CODE
    for verdrahtet in ("2022/23", "2023/24", "2024/25", "2025/26",
                       "KW 5/2026", "Winter 2024/25"):
        assert verdrahtet not in CODE, f"{verdrahtet!r} steht im Code"


def test_saisonreihe_formatiert_deutsch_und_sortiert():
    d = {"x_2023_24": 1234, "x_2024_25": 5.5, "x_woche_2024_25": "KW 1"}
    assert _saisonreihe(d, "x_") == "2023/24 1.234, 2024/25 5,5"


def test_fehlendes_feld_kostet_nur_einen_satz():
    from services.rki_surveillance import _fuege
    assert _fuege(("A.", ["x"]), ("B.", ["y"]), d={"x": 1}) == "A."


def test_keine_platzhalter_und_keine_englischen_dezimalzahlen():
    for topic in FAKTEN:
        claim = FAKTEN[topic]["claim_phrasings_handled"][0]
        text = _ergebnis(claim, topic)["display_value"]
        for bad in ("None", "{", "}", "?,"):
            assert bad not in text, f"{topic}: {bad!r} in {text[:120]}"
        import re
        assert not re.search(r"\d+\.\d\b", text), f"{topic}: englische Dezimalzahl"


def test_alle_felder_unter_dem_prompt_budget():
    for topic in FAKTEN:
        claim = FAKTEN[topic]["claim_phrasings_handled"][0]
        x = _ergebnis(claim, topic)
        for feld in ("indicator_name", "display_value"):
            assert len(x[feld]) <= MAX_STR, f"{topic}/{feld}: {len(x[feld])}"


# --------------------------------------------------------------------------
# Trigger
# --------------------------------------------------------------------------

def test_alle_phrasings_treffen_ihren_fakt():
    fehl = [(f["topic"], p) for f in DATEN["facts"]
            for p in f.get("claim_phrasings_handled", [])
            if f["topic"] not in [x["topic"] for x in asyncio.run(
                search_rki_surveillance({"claim": p, "original_claim": p}))["results"]]]
    assert not fehl, f"Phrasings ohne Treffer: {fehl}"


def test_keine_jahresgebundenen_trigger_mehr():
    """'covid-welle 2024' und 'covid-welle 2025' veralten von selbst."""
    roh = json.dumps(DATEN, ensure_ascii=False)
    for f in DATEN["facts"]:
        for kw in f.get("trigger_keywords", []):
            assert not any(j in kw for j in ("2023", "2024", "2025", "2026")), \
                f"jahresgebundener Trigger: {kw!r}"
    assert "covid-welle" in roh


def test_kein_ueber_trigger_bei_fachfremden_claims():
    for claim in ("Die Inflation in Deutschland steigt",
                  "Die Grippe-Impfung ist ein Eingriff in die Freiheit",
                  "Wie viele Einwohner hat Deutschland?"):
        r = asyncio.run(search_rki_surveillance({"claim": claim,
                                                 "original_claim": claim}))
        assert not r["results"], f"Über-Trigger: {claim!r} -> {[x['topic'] for x in r['results']]}"
