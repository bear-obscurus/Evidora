"""Schutzstatus des Wolfs — die Abdeckungslücke aus QA50F/QA50G.

"Der Wolf ist in Österreich streng geschützt" war in beiden Läufen der
Ausfall: `unverifiable@0.1`, "die bereitgestellten Quellen enthalten keine
Informationen zum rechtlichen Schutzstatus des Wolfs in Österreich". Kein
Fakt deckte das Thema ab.

Recherche am 27.9.2026:

    Berner Konvention   Wolf von Anhang II auf Anhang III, wirksam März 2025
    EU-Parlament        8.5.2025, 371 zu 162 bei 37 Enthaltungen: FFH-Anhang
                        IV -> Anhang V, "streng geschützt" -> "geschützt";
                        in Kraft 20 Tage nach Veröffentlichung, 18 Monate
                        Umsetzungsfrist
    Was bleibt          Artikel 14 statt Artikel 12; Monitoring-Pflicht und
                        günstiger Erhaltungszustand bleiben; Staaten dürfen
                        national strenger schützen
    Österreich          Naturschutz und Jagd sind Landessache, umgesetzt in
                        neun Landesgesetzen

Die Erwartung in QA50G war `true` — das war nach dem Stand bis 2025 richtig
und ist es seither nicht mehr ohne Zusatz. Der Fakt sagt genau das.

Bewusst nicht im Fakt: die einzelnen neun Landesregelungen. Ein Test pinnt
diese Entscheidung, damit sie nicht später unbemerkt aufweicht.

Keine Netzabfrage in diesen Tests.
"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._struct_marker import has_false_verdict_override  # noqa: E402
from services._topic_match import substring_or_composite_match as trifft  # noqa: E402

PACK = json.loads((BACKEND / "data" / "tier_natur_pack.json").read_text(encoding="utf-8"))
F = next(x for x in PACK["facts"] if x.get("id") == "wolf_schutzstatus_at_2026")
TEXT = F["headline"] + " " + json.dumps(F["data"], ensure_ascii=False)


# --------------------------------------------------------------------------
# Die EU-Entscheidung
# --------------------------------------------------------------------------

@pytest.mark.parametrize("teil", ["8. Mai 2025", "371", "162", "37 Enthaltungen",
                                  "20 Tage", "18 Monaten"])
def test_abstimmung_und_fristen(teil):
    assert teil in F["data"]["eu_entscheidung"], teil


def test_anhang_wechsel_steht_in_der_headline():
    h = F["headline"]
    assert "Anhang IV" in h and "Anhang V" in h
    assert "nicht mehr streng geschützt" in h


def test_berner_konvention_ging_voraus():
    b = F["data"]["berner_konvention"]
    assert "Anhang II" in b and "Anhang III" in b


def test_beschluss_und_wirksamkeit_stehen_beide_da():
    """HART40-B: "Die Berner Konvention hat den Wolf 2024 herabgestuft"
    bekam false@0.8 — "bereits im März 2025, nicht 2024". Der Beschluss
    fiel aber am 6.12.2024; der Fakt nannte nur das Wirksamkeitsdatum, also
    konnte das Modell nicht fair urteilen."""
    b = F["data"]["berner_konvention"]
    assert "6.12.2024" in b, "Beschlussdatum fehlt"
    assert "7.3.2025" in b, "Wirksamkeitsdatum fehlt"
    assert "Einspruchsfrist" in b


def test_die_einsprechenden_parteien_stehen_da():
    """Für Monaco, Tschechien und das Vereinigte Königreich gilt die
    Herabstufung nicht — ohne das wäre "in Europa herabgestuft" zu grob."""
    b = F["data"]["berner_konvention"]
    for land in ("Monaco", "Tschechien", "Vereinigte Königreich"):
        assert land in b, land


# --------------------------------------------------------------------------
# Herabgestuft ist nicht freigegeben
# --------------------------------------------------------------------------

def test_artikel_12_und_14_werden_unterschieden():
    k = F["data"]["kernsatz_fuer_synthesizer"]
    assert "Artikel 12" in k and "Artikel 14" in k
    assert "Herabgestuft heißt nicht freigegeben" in k


def test_pflichten_bleiben():
    w = F["data"]["was_bleibt"]
    assert "günstigen Erhaltungszustand" in w
    assert "Monitoring" in w
    assert "strenger" in F["data"]["kernsatz_fuer_synthesizer"] or "streng geschützte Art" in w


def test_kein_satz_behauptet_freigabe():
    """Die Notiz haelt fest, warum: die Rechtsgrundlage aendert sich, nicht
    automatisch die Zulaessigkeit."""
    assert "darf jetzt geschossen werden" not in TEXT
    notiz = " ".join(F["context_notes"])
    assert "nicht automatisch die Zulässigkeit" in notiz


# --------------------------------------------------------------------------
# Österreich: Zuständigkeit statt Scheingenauigkeit
# --------------------------------------------------------------------------

def test_laendersache_ist_die_antwort_fuer_oesterreich():
    o = F["data"]["oesterreich_zustaendigkeit"]
    assert "Landessache" in o
    assert "neun Bundesländer" in o
    assert "bundesweit einheitliche Antwort" in o and "gibt es deshalb nicht" in o


def test_keine_einzelnen_landesregelungen_behauptet():
    """Bewusste Grenze: Neun Landesgesetze braeuchten neun Primaerquellen,
    und eine unvollstaendige Aufzaehlung waere schlechter als der Hinweis
    auf die Zustaendigkeit. Wer das aendert, soll es bewusst tun."""
    for land in ("Niederösterreich", "Tirol", "Vorarlberg", "Kärnten", "Steiermark",
                 "Salzburg", "Burgenland", "Oberösterreich"):
        assert land not in TEXT, land
    notiz = " ".join(F["context_notes"])
    assert "neun Primärquellen" in notiz


# --------------------------------------------------------------------------
# Marker, Trigger, Quellen
# --------------------------------------------------------------------------

def test_kein_struktureller_falsch_marker():
    """Der Claim ist nicht schlicht falsch, sondern überholt — keine
    Verdict-Direktive."""
    assert not has_false_verdict_override(F["data"]["kernsatz_fuer_synthesizer"])


@pytest.mark.parametrize("phrasing", F["claim_phrasings_handled"])
def test_phrasings_treffen(phrasing):
    assert trifft(F, phrasing.lower()), phrasing


@pytest.mark.parametrize("claim", [
    "Wölfe dürfen in Österreich abgeschossen werden",
    "Ist der Wolf noch streng geschützt?",
    "Der Schutzstatus des Wolfs wurde herabgestuft",
])
def test_batterie(claim):
    assert trifft(F, claim.lower()), claim


@pytest.mark.parametrize("claim", [
    "Der Hund sieht nur schwarz-weiß",
    "Haie greifen Menschen häufig an",
    "Wie hoch ist die Leerstandsabgabe in Tirol?",
])
def test_fremde_claims_treffen_nicht(claim):
    assert not trifft(F, claim.lower()), claim


def test_quellen_sind_die_geprueften():
    assert "europarl.europa.eu" in F["source_url"]
    assert "oekobuero.at" in F["secondary_url"]


def test_prompt_felder_bleiben_unter_der_kuerzung():
    zu_lang = {k: len(v) for k, v in F["data"].items()
               if k != "kernsatz_fuer_synthesizer" and len(v) > 400}
    assert not zu_lang, zu_lang


def test_datenstand_ist_benannt():
    assert "27.9.2026" in " ".join(F["context_notes"])
