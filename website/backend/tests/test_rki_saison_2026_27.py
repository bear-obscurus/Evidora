"""Die Saison hat begonnen — und das ist eine Vergleichsfalle.

Der Freshness-Check meldete heute Nacht ``rki_surveillance.json`` als
einzige Datei mit einem überzogenen FELD-Takt: ``fetched_at_iso`` 44 Tage
alt bei wöchentlicher Kadenz. Der Fakt stand auf Meldewoche 35/2026.

## Was die Prüfung ergab

Alle bisherigen Zahlen sind gegen RKI-Open-Data nachgerechnet und
**exakt bestätigt** — keine Revisionen:

    GrippeWeb-ARE-Höchstwerte  2018/19  8.660
                               2022/23 11.204
                               2023/24 10.434
                               2024/25  9.290
                               2025/26  8.661  (KW 5/2026)
    SARI-Hospitalisierung      2022/23    38,2
                               2025/26    29,1
    ARE-Konsultationsinzidenz  2025/26   2.044

Ein Refresh, der nichts ändert, ist auch ein Ergebnis — und er gehört
dokumentiert, sonst prüft ihn der nächste wieder von vorn.

## Was neu ist, und warum es gefährlich ist

Die Saison 2026/27 läuft seit zwei Meldewochen: ARE 6.640 (KW 40) und
6.854 (KW 41). Diese Zahlen sind **keine Höchstwerte**. Wer 6.854 gegen
8.661 hält, vergleicht einen Saisonanfang mit einem Gipfel und liest
einen Rückgang heraus, den die Daten nicht zeigen — dieselbe Fehlerform
wie die zwei ARE-Reihen in #138, nur auf der Zeitachse statt zwischen
Messgrößen.

Der Fakt sperrt das jetzt zweifach: er sagt ausdrücklich, dass zwei
Wochen kein Saisonverlauf sind, und er liefert den FAIREN Vergleich —
dieselbe Kalenderwoche über sechs Saisonen:

    KW 41   2026/27  6.854     2023/24  6.918
            2025/26  7.584     2022/23  7.170
            2024/25  8.568     2021/22  6.497

Der Start liegt damit am unteren Rand, aber im Bereich von 2021/22 und
2023/24 — nicht "die schwächste Saison", wie der Gipfel-Vergleich
suggerieren würde.

## Nicht angetastet

Masern und Tuberkulose. Beide tragen seit dem 5.9.2026 ihren Datenstand
ausdrücklich in der Headline, weil SurvStat 2.0 kein offenes Datenabbild
hat und die geprüften Alternativen die Werte nicht maschinenlesbar
liefern. Daran hat sich nichts geändert; zu schätzen wäre schlechter als
den Stand zu nennen.

Dependency-light: nur Daten, kein Netz, kein Modell.
"""
import json
import os

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATEI = os.path.join(BACKEND, "data", "rki_surveillance.json")


def _laden():
    with open(DATEI, encoding="utf-8") as fh:
        return json.load(fh)


def _fakt(kennung):
    for f in _laden().get("facts") or []:
        if (f.get("id") or f.get("topic")) == kennung:
            return f
    raise AssertionError(kennung)


ATEM = "rki_atemwegsinfekte_saisonvergleich"


# --- Die bestätigten Zahlen -------------------------------------------

#: Aus RKI-Open-Data am 19.10.2026 nachgerechnet
#: (GrippeWeb_Daten_des_Wochenberichts.tsv, ARE / 00+ / Bundesweit).
GRIPPEWEB_PEAKS = {
    "2018/19": "8.660", "2022/23": "11.204", "2023/24": "10.434",
    "2024/25": "9.290", "2025/26": "8.661",
}


@pytest.mark.parametrize("saison,wert", sorted(GRIPPEWEB_PEAKS.items()))
def test_die_saison_hoechstwerte_stehen_im_fakt(saison, wert):
    t = json.dumps(_fakt(ATEM), ensure_ascii=False)
    assert wert in t, f"{saison}: {wert} fehlt"


def test_die_weiteren_reihen_stehen_im_fakt():
    t = json.dumps(_fakt(ATEM), ensure_ascii=False)
    for wert in ("38,2", "29,1", "2.044"):
        assert wert in t, wert


# --- Die laufende Saison ----------------------------------------------

def test_die_laufende_saison_ist_erfasst():
    d = _fakt(ATEM)["data"]
    t = json.dumps(d, ensure_ascii=False)
    assert "2026/27" in t
    assert "6.640" in t and "6.854" in t
    assert "KW 41/2026" in t, "die Meldewoche des Stands fehlt"


def test_der_gipfel_vergleich_ist_ausdruecklich_gesperrt():
    """Die eigentliche Arbeit an diesem Refresh.

    Ohne diesen Satz liest das Modell 6.854 gegen 8.661 und meldet einen
    Rückgang, den die Daten nicht zeigen.
    """
    t = json.dumps(_fakt(ATEM)["data"], ensure_ascii=False)
    assert "ZWEI WOCHEN SIND KEIN SAISONVERLAUF" in t
    assert "KEINE Höchstwerte" in t
    assert "Anfangswert mit einem Gipfel" in t


def test_der_faire_vergleich_steht_daneben():
    """Eine Sperre allein hilft nicht — es braucht die Alternative,
    sonst bleibt die Frage 'und wie stark ist es nun?' unbeantwortet."""
    t = json.dumps(_fakt(ATEM)["data"], ensure_ascii=False)
    assert "DIESELBE Kalenderwoche" in t
    for wert in ("7.584", "8.568", "6.918", "7.170", "6.497"):
        assert wert in t, f"Vergleichswert {wert} fehlt"


def test_die_vorlaeufigen_werte_sind_als_vorlaeufig_benannt():
    t = json.dumps(_fakt(ATEM)["data"], ensure_ascii=False)
    assert "9,6" in t and "1.332" in t
    assert "vorläufig" in t


# --- Der Datenstand ---------------------------------------------------

def test_fetched_at_ist_aktuell():
    assert _laden()["fetched_at_iso"] == "2026-10-19"


def test_die_pruefung_haelt_fest_dass_nichts_revidiert_wurde():
    """Ein Refresh ohne Änderung ist auch ein Ergebnis.

    Steht es nicht da, prüft der nächste dieselben acht Zahlen wieder
    von vorn.
    """
    p = _laden()["letzte_pruefung"]
    assert "2026-10-19" in p
    assert "keine Revisionen" in p
    assert "Meldewoche 41/2026" in p


def test_die_kadenz_bleibt_woechentlich():
    d = _laden()
    assert d["refresh_kadenz"] == "woechentlich"
    assert "woechentlich" in d["kadenz_begruendung"]


# --- Was absichtlich unberührt blieb ---------------------------------

@pytest.mark.parametrize("kennung", ["rki_masern_2024_2025",
                                     "rki_tuberkulose_2024"])
def test_masern_und_tb_nennen_weiter_ihren_datenstand(kennung):
    """Beide sind nicht refreshbar (SurvStat 2.0 hat kein offenes
    Datenabbild). Statt zu schätzen, nennen sie den Stand — und das muss
    in der HEADLINE stehen, weil die Prompt-Kürzung sie zuerst sieht."""
    f = _fakt(kennung)
    assert "DATENSTAND" in (f.get("headline") or "")
    assert "datenstand" in (f.get("data") or {})


def test_der_masern_zwischenstand_bleibt_als_solcher_markiert():
    """142 ist ein Q1-Zwischenstand, kein Jahreswert. Ohne die Sperre
    vergleicht das Modell ihn mit 637."""
    t = json.dumps(_fakt("rki_masern_2024_2025"), ensure_ascii=False)
    assert "142" in t
    assert "kein Jahreswert" in t


# --- Die Messgrößen-Sperre aus #138 ----------------------------------

def test_die_zwei_are_reihen_bleiben_getrennt():
    """#138: GrippeWeb und Konsultationsinzidenz unterscheiden sich um
    etwa Faktor vier. Der Refresh darf diese Sperre nicht verwässern."""
    t = json.dumps(_fakt(ATEM)["data"], ensure_ascii=False)
    assert "MESSGRÖSSE NICHT TAUSCHEN" in t
    assert "viermal" in t
