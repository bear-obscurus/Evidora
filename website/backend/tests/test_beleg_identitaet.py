"""Ein Beleg muss die Arbeit sein, die das Label nennt (12.10.2026).

Neue Fehlerklasse, gefunden beim Prüfen der ACSM-Zitate: **neun verlinkte
Belege zeigten auf eine andere Arbeit.** Alle neun Links lebten — HTTP 200,
echte begutachtete Veröffentlichungen. Der Link-Wächter kann das prinzipiell
nicht sehen: er prüft Erreichbarkeit, nicht Identität.

Fünf teilten eine Signatur — **richtige Zeitschrift, richtiges Jahr,
falscher Artikel**:

  | Fakt | Label nennt | verlinkt war |
  |---|---|---|
  | `vollmond_schlaf_geburten` | Periti 1998, Minerva Ginecol | Hsp90-Partnerproteine (1998) |
  | `lactic_acid_2026` | Newham 1983 | *Cal West Med* 1946, „Advertisements" |
  | `10_prozent_gehirn` | Buckner 2008 | handchirurgischer Fallbericht |
  | `milch_schleim` | Balfour-Lynn, Arch Dis Child | Stadtwachstum per Satellitenbild |
  | `menstruation_synchronisation` | Yang & Schank 2006, Proc R Soc B | Cytomegalievirus-Modellierung |
  | `women_bulky_muscles` | Roberts 2020, Sports Med | Angstvermeidung bei Schmerz |
  | `fat_burning_zone` | Wewege, Obesity Reviews | BMI und Sterblichkeit |
  | `iris_diagnose` | Knipschild 1988, BMJ | BMJ 1988, Antikoagulantien |
  | `psychotherapie_wirkt_nicht` | Cuijpers 2020, World Psychiatry | Jarrett 2020, „Can we help more?" |

Jede Ersetzung wurde über die PubMed-/Crossref-API aufgelöst und am Titel
geprüft, nicht geraten. Dabei fielen auch Label-Fehler auf: Yang & Schank
erschien in *Human Nature*, nicht in Proc R Soc B; Roberts 2020 im *J
Strength Cond Res*, nicht in Sports Medicine; Wewege 2017, nicht 2022; und
beim Vollmond-Fakt waren Jahr, Band, Seiten UND der Zweitautor falsch
(Biagiotti, nicht Biagini).

Was NICHT ersetzt wurde: „Schwartz 1991 J Reprod Med (n=521.000)" liess
sich nicht auffinden — die Lunar-Arbeit dieser Zeitschrift ist Marks 1983.
Entfernt statt durch etwas Ähnliches ersetzt.

Dieser Test geht nicht ins Netz. Er pinnt das geprüfte Ergebnis; die
laufende Prüfung macht `tools/beleg_identitaet.py`.
"""

import json
from pathlib import Path

import pytest

DATA = Path(__file__).resolve().parents[1] / "data"

# (Datei, Fakt, Feld, verifizierter Beleg, Autor im source_label)
GEPRUEFT = [
    ("alltags_mythen_pack.json", "vollmond_schlaf_geburten_mythos",
     "source_url", "https://pubmed.ncbi.nlm.nih.gov/7970080/", "Periti"),
    ("sport_fitness_pack.json", "lactic_acid_2026",
     "secondary_url", "https://pubmed.ncbi.nlm.nih.gov/6822050/", "Newham"),
    ("bildung_pack.json", "10_prozent_gehirn_2026",
     "secondary_url", "https://pubmed.ncbi.nlm.nih.gov/18400922/", "Buckner"),
    ("ernaehrungs_pack.json", "milch_schleim_2026",
     "source_url", "https://pubmed.ncbi.nlm.nih.gov/30190283/", "Balfour-Lynn"),
    ("reproduktion_pack.json", "menstruation_synchronisation_mythos",
     "source_url", "https://pubmed.ncbi.nlm.nih.gov/26181612/", "Yang"),
    ("sport_fitness_pack.json", "women_bulky_muscles_2026",
     "secondary_url", "https://pubmed.ncbi.nlm.nih.gov/32218059/", "Roberts"),
    ("sport_fitness_pack.json", "fat_burning_zone_2026",
     "secondary_url", "https://pubmed.ncbi.nlm.nih.gov/28401638/", "Wewege"),
    ("esoterik_pack.json", "iris_diagnose_2026",
     "source_url", "https://pubmed.ncbi.nlm.nih.gov/3147081/", "Knipschild"),
    ("mental_health_pack.json", "psychotherapie_wirkt_nicht_mythos",
     "source_url", "https://pubmed.ncbi.nlm.nih.gov/31922679/", "Cuijpers"),
]

# Diese Kennungen bezeichnen die FALSCHEN Arbeiten. Keine davon darf
# irgendwo in data/ wieder auftauchen.
FALSCHE_KENNUNGEN = [
    ("9534179", "Hsp90-Partnerproteine, verlinkt beim Vollmond-Fakt"),
    ("PMC1473829", "Cal West Med 1946 'Advertisements', verlinkt beim Muskelkater"),
    ("10.1111/obr.13035", "BMI und Sterblichkeit, verlinkt bei der Fettverbrennungszone"),
    ("10.1007/s40279-020-01345-1", "Angstvermeidung bei Schmerz, verlinkt bei Krafttraining"),
]


def _fakt(datei, fid):
    d = json.loads((DATA / datei).read_text(encoding="utf-8"))
    fakten = d.get("facts") if isinstance(d, dict) else d
    return next(f for f in fakten if (f.get("id") or f.get("topic")) == fid)


@pytest.mark.parametrize("datei,fid,feld,url,autor", GEPRUEFT)
def test_der_beleg_steht_auf_der_geprueften_arbeit(datei, fid, feld, url, autor):
    assert _fakt(datei, fid)[feld] == url


@pytest.mark.parametrize("datei,fid,feld,url,autor", GEPRUEFT)
def test_das_label_nennt_den_autor_des_belegs(datei, fid, feld, url, autor):
    """Der Kern: Beleg und Label müssen dieselbe Arbeit meinen. Genau diese
    Übereinstimmung fehlte bei allen neun."""
    assert autor.lower() in _fakt(datei, fid)["source_label"].lower()


@pytest.mark.parametrize("kennung,was", FALSCHE_KENNUNGEN)
def test_die_falsche_arbeit_taucht_nicht_wieder_auf(kennung, was):
    """Die Kennung darf nur dort stehen, wo die Korrektur sie WIDERLEGT —
    dieselbe Unterscheidung wie bei WSAVA."""
    treffer = []
    for pfad in sorted(DATA.glob("*.json")):
        for zeile in pfad.read_text(encoding="utf-8").split('","'):
            if kennung in zeile and "KORREKTUR" not in zeile:
                treffer.append(pfad.name)
                break
    assert not treffer, f"{was} — wieder in {treffer}"


def test_die_korrigierten_zitatangaben_stimmen():
    """Mitgefundene Label-Fehler: falsche Zeitschrift, falsches Jahr,
    falscher Zweitautor."""
    assert "Human Nature" in _fakt("reproduktion_pack.json",
                                   "menstruation_synchronisation_mythos")["source_label"]
    assert "J Strength Cond Res" in _fakt("sport_fitness_pack.json",
                                          "women_bulky_muscles_2026")["source_label"]
    assert "Wewege 2017" in _fakt("sport_fitness_pack.json",
                                  "fat_burning_zone_2026")["source_label"]
    mond = _fakt("alltags_mythen_pack.json", "vollmond_schlaf_geburten_mythos")
    assert "Biagiotti" in mond["source_label"]


def test_die_korrektur_benennt_was_falsch_war():
    """Wie bei WSAVA: der alte Name darf nur dort stehen, wo die Korrektur
    ihn widerlegt — sonst liest ihn niemand als Korrektur."""
    mond = _fakt("alltags_mythen_pack.json", "vollmond_schlaf_geburten_mythos")
    traeger = [v for v in mond["data"].values()
               if "KORREKTUR 2026-10-12" in str(v)]
    assert traeger, "ohne Datum waere es keine Korrektur, sondern ein Rest"
    text = " ".join(str(v) for v in traeger)
    for stichwort in ("Biagini", "1998", "9534179", "Schwartz"):
        assert stichwort in text, stichwort


def test_die_unauffindbare_studie_wurde_entfernt_nicht_ersetzt():
    """'Schwartz 1991 J Reprod Med, n=521.000' war nicht auffindbar. Die
    Zahl steht nirgends mehr als Beleg — nur noch in der Korrektur."""
    mond = _fakt("alltags_mythen_pack.json", "vollmond_schlaf_geburten_mythos")
    assert "Schwartz" not in mond["source_label"]
    assert "Schwartz" not in mond["headline"]
    assert "521.000" not in mond["data"]["kernsatz_fuer_synthesizer"]


def test_das_studiendesign_wird_nicht_hochgestuft():
    """Der Fakt nannte retrospektive Beobachtungsstudien
    'randomisiert-kontrollierte Studien'. Für einen Faktencheck ist die
    Studienart Teil der Aussage."""
    mond = _fakt("alltags_mythen_pack.json", "vollmond_schlaf_geburten_mythos")
    assert "randomisiert-kontrollierten Studien" not in mond["headline"]
    assert "Beobachtungsstudien" in mond["headline"]
