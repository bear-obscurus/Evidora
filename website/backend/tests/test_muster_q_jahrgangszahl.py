"""Die veraltete Zahl stuetzte den Claim — mit einer richtigen Begruendung.

QA50I-Befund #77. "Der Gender Pay Gap in Deutschland liegt bei 18 Prozent"
bekam ``mostly_true@0.85``. Die Begruendung sagte dabei alles richtig:

    "Der unbereinigte Gender Pay Gap in Deutschland lag 2024 bei 15,6 %
     (Eurostat) bzw. 16 % (DESTATIS). Die Behauptung von 18 % bezieht sich
     vermutlich auf den Wert von 2023 (DESTATIS: 18 %), der 2024 um 2
     Prozentpunkte sank."

Falsch war nur das Label. Ein Claim im Praesens, der den Vorjahreswert
nennt, ist nicht "im Wesentlichen richtig" — PR #256 hatte die
widerspruechlichen Zahlen in den DATEN bereinigt, die Verdict-Bildung
behandelte eine veraltete Zahl aber weiter als Treffer, sobald sie
irgendeinem Jahrgang entsprach.

## Gerechnet, nicht gelesen

Muster Q zieht ``(Jahr, Prozentwert)``-Paare aus der Begruendung und
vergleicht sie, wie Muster N und P. Keine Phrasenliste: "bezieht sich
vermutlich auf", "Rueckgang von ... im Jahr", "trifft fuer ... zu" sind
drei Formulierungen derselben Lage, und die vierte waere nicht in der
Liste.

## Drei Bedingungen, die das Muster zum Schweigen bringen

**Der Claim nennt ein Jahr.** "Der Gender Pay Gap lag 2023 bei 18 Prozent"
ist wahr. Das ist die wichtigste Gegenprobe.

**Der Claim zitiert eine Quelle.** "Laut Eurobarometer vertrauen heute
51 Prozent der EU-Buerger der EU" ist eine Aussage DARUEBER, was
Eurobarometer publiziert hat — und Welle 102 hat 51 % publiziert. Dieselbe
Lehre wie die QA50I-Befunde #31 und #88, nur andersherum: Dort beantwortete
das System einen Claim ueber Freedom House mit V-Dem-Zahlen. Wer eine
Quelle nennt, fragt nach dieser Quelle.

**Der Claim nennt eine Schwelle.** "Die Staatsverschuldung liegt bei ueber
80 Prozent" wird durch einen neueren, HOEHEREN Wert nicht falsch, sondern
wahrer. Die Richtung rechnet Muster N aus; Q kennt sie nicht und haelt
sich heraus. Im Sweep schwieg dieser Fall zunaechst nur, weil 80 gegen
81,5 unter der Groessenschwelle lag — bei 95 % haette Q ihn getroffen.

## Zwei Dinge, die die Messung entschieden hat

**Die Komma-Grenze.** Der Abstand allein genuegte nicht. In der
aufgezeichneten Begruendung steht

    "... auf den Wert von 2023 (DESTATIS: 18 %), der 2024 um 2
     Prozentpunkte sank."

das "2024" eflf Zeichen NACH dem "18 %", das "2023" sechzehn davor. Nach
reinem Abstand gewinnt 2024 — und damit galt die Zahl des Claims als
aktueller Wert, das Muster schwieg an seinem eigenen Anlass. Das "2024"
gehoert zu "sank". Eine Zuordnung darf kein Komma ueberschreiten; die
Gegenform "von 18 % im Jahr 2023" hat keines.

Ob der PUNKT dazugehoert, hat eine Messung entschieden, nicht ein
Argument: mit ``,;.!?`` bestehen 7 von 7 Einheitsfaellen, mit ``,;!?``
nur 6 — bei identischem Sweep-Ergebnis.

**Die Groessenschwelle ist der schwaechste Teil.** Der Sweep ueber 164
echte (Claim, Summary)-Paare aus QA50H und QA50I lieferte genau zwei
Kandidaten, und sie klammern die Schwelle eng ein:

    Gender Pay Gap   Claim 18 %, aktuell 16 %   -> 11,1 %   SOLL feuern
    Eurobarometer    Claim 51 %, aktuell 49 %   ->  3,9 %   soll SCHWEIGEN

10 % liegt dazwischen, aber nur 1,1 Punkte unter dem Fall, der feuern
soll. Sinkt der Gender Pay Gap naechstes Jahr von 18 auf 17, waere die
Differenz 5,6 % und das Muster stumm. Deshalb loggt Muster Q beide Werte,
und deshalb steht die inhaltliche Zitat-Bedingung daneben, die den
Eurobarometer-Fall UNABHAENGIG von der Schwelle ausnimmt.

Dependency-light: nur Quelltext, kein Netz, kein Modell.
"""
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

from services.verdict_postprocess import (  # noqa: E402
    apply_verdict_postprocessing,
    _Q_MIN_ABWEICHUNG,
    _Q_TRENNER,
    claim_nennt_bereich,
    claim_nennt_jahr,
    claim_prozentzahl,
    claim_zitiert_quelle,
    jahr_wert_paare,
    veraltete_jahrgangszahl,
)

#: Die aufgezeichnete Begruendung aus QA50I #77, woertlich.
BEFUND_SUMMARY = (
    "Der unbereinigte Gender Pay Gap in Deutschland lag 2024 bei 15,6 % "
    "(Eurostat) bzw. 16 % (DESTATIS). Die Behauptung von 18 % bezieht sich "
    "vermutlich auf den Wert von 2023 (DESTATIS: 18 %), der 2024 um 2 "
    "Prozentpunkte sank."
)
BEFUND_CLAIM = "der gender pay gap in deutschland liegt bei 18 prozent"


def _lauf(verdict, summary, claim, confidence=0.85):
    """Durch die GANZE Pipeline, nicht nur durch den Helfer.

    Drei Giftproben hielten, weil diese Suite zuerst nur
    ``veraltete_jahrgangszahl`` prueffte: Muster Q ganz zu entfernen oder
    sein Ziel-Label auf 'true' zu drehen fiel nicht auf. Ein Guard, der
    nur als Funktion getestet ist, ist nicht als Guard getestet.
    """
    result = {"verdict": verdict, "confidence": confidence,
              "summary": summary,
              "evidence": [{"source": "Eurostat",
                            "url": "https://example.test/x"}]}
    return apply_verdict_postprocessing(result, [], claim)


# --- Durch die Pipeline ----------------------------------------------

@pytest.mark.parametrize("vorher", ["true", "mostly_true"])
def test_die_pipeline_wertet_den_befund_ab(vorher):
    r = _lauf(vorher, BEFUND_SUMMARY,
              "Der Gender Pay Gap in Deutschland liegt bei 18 Prozent")
    assert r["verdict"] == "mostly_false", r["verdict"]
    assert r["confidence"] <= 0.75


@pytest.mark.parametrize("vorher", ["false", "mostly_false", "mixed",
                                    "unverifiable", "misleading"])
def test_die_pipeline_hebt_kein_label_an(vorher):
    """Muster Q wertet nur ab. Ein 'false' darf es nicht zu
    'mostly_false' AUFwerten — das waere eine Verbesserung des Labels
    aus einem Befund heraus."""
    r = _lauf(vorher, BEFUND_SUMMARY,
              "Der Gender Pay Gap in Deutschland liegt bei 18 Prozent")
    assert r["verdict"] == vorher


def test_die_pipeline_laesst_den_datierten_claim_stehen():
    r = _lauf("true", "2023 lag er bei 18 %, 2024 bei 15,6 %.",
              "Der Gender Pay Gap lag 2023 in Deutschland bei 18 Prozent")
    assert r["verdict"] == "true"


def test_die_pipeline_laesst_die_zugeschriebene_zahl_stehen():
    r = _lauf("mostly_true",
              "Laut Standard Eurobarometer 102 (Herbst 2024) vertrauten 51 % "
              "der EU-Bürger:innen der EU. Eurobarometer 105 (Frühjahr 2026) "
              "zeigt 49 %.",
              "Laut Eurobarometer vertrauen heute 51 Prozent der EU-Bürger der EU")
    assert r["verdict"] == "mostly_true"


# --- Der Anlass -------------------------------------------------------

def test_der_befund_wird_erkannt():
    erg = veraltete_jahrgangszahl(BEFUND_CLAIM, BEFUND_SUMMARY)
    assert erg is not None, "das Muster findet seinen eigenen Anlass nicht"
    wert, jahr, neuestes, neuwert = erg
    assert wert == 18.0
    assert jahr == 2023
    assert neuestes == 2024
    assert neuwert in (15.6, 16.0)


def test_auch_die_zweite_formulierung_derselben_lage():
    """Die Belegzeile desselben Laufs, anders gebaut: Jahr VOR der Zahl,
    Zuschreibung ueber 'Rueckgang von ... im Jahr'."""
    s = ("Eurostat 2024: Deutschland unbereinigter Gender Pay Gap 15,6 %, "
         "DESTATIS 2024: 16 % (Rückgang von 18 % im Jahr 2023).")
    erg = veraltete_jahrgangszahl(
        "der gender pay gap in deutschland beträgt 18 prozent", s)
    assert erg is not None
    assert erg[0] == 18.0 and erg[1] == 2023 and erg[2] == 2024


def test_die_dritte_formulierung_ohne_zuschreibungs_formel():
    """Kein 'bezieht sich auf', kein 'Rueckgang' — nur zwei datierte
    Werte. Genau dafuer rechnet das Muster statt zu lesen."""
    s = "2024 lag der Wert bei 15,6 %. 2023 waren es 18 %."
    assert veraltete_jahrgangszahl(BEFUND_CLAIM, s) is not None


# --- Die Gegenproben --------------------------------------------------

def test_schweigt_wenn_der_claim_das_jahr_nennt():
    """Die wichtigste Gegenprobe: Mit Jahresangabe ist die Zahl richtig."""
    s = "2023 lag er bei 18 % (DESTATIS), 2024 bei 15,6 % (Eurostat)."
    assert veraltete_jahrgangszahl(
        "der gender pay gap lag 2023 in deutschland bei 18 prozent", s) is None


@pytest.mark.parametrize("claim", [
    "laut destatis liegt der gender pay gap bei 18 prozent",
    "nach angaben von destatis liegt er bei 18 prozent",
    "dem statistischen bundesamt zufolge sind es 18 prozent",
    "according to destatis the gap is 18 percent",
])
def test_schweigt_bei_einer_zugeschriebenen_zahl(claim):
    """Wer eine Quelle nennt, fragt nach dieser Quelle (#31/#88)."""
    s = "2023 lag er bei 18 %, 2024 bei 15,6 %."
    assert claim_zitiert_quelle(claim)
    assert veraltete_jahrgangszahl(claim, s) is None


@pytest.mark.parametrize("claim", [
    "die staatsverschuldung liegt bei über 80 prozent des bip",
    "der anteil liegt bei mindestens 40 prozent",
    "der anteil liegt unter 40 prozent",
])
def test_schweigt_bei_einem_schwellen_claim(claim):
    """Ein neuerer, hoeherer Wert macht 'ueber 80 %' wahrer, nicht
    falscher. Die Richtung rechnet Muster N."""
    s = "2024 lag er bei 80,0 %. 2025 stieg er auf 95,0 %."
    assert veraltete_jahrgangszahl(claim, s) is None


def test_schweigt_wenn_die_claim_zahl_der_aktuelle_wert_ist():
    s = "2024: 15,6 %. 2023: 18 %."
    assert veraltete_jahrgangszahl(
        "der gender pay gap liegt bei 15,6 prozent", s) is None


def test_schweigt_bei_zu_kleiner_differenz():
    """18 gegen 17,9 ist dieselbe Aussage, nicht ein Widerspruch."""
    s = "2024 lag er bei 17,9 %. 2023 waren es 18 %."
    assert veraltete_jahrgangszahl("der wert liegt bei 18 prozent", s) is None


@pytest.mark.parametrize("claim", [
    "der gap liegt zwischen 15 und 18 prozent",
    "der gap liegt bei 15 bis 18 prozent",
    "der anteil betrug 15-18 prozent",
])
def test_schweigt_bei_einem_bereichs_claim(claim):
    """Ein Bereich, der den aktuellen Wert ENTHAELT, ist wahr.

    Diese Luecke fand die Gegenprobe beim Schreiben der Tests, nicht der
    Betrieb: In "zwischen 15 und 18 prozent" traegt nur die 18 die
    Einheit, ``claim_prozentzahl`` sah deshalb einen einzelnen Wert 18 —
    und Muster Q haette den Claim abgewertet, obwohl 15,6 in der Spanne
    liegt.
    """
    s = "2024 bei 15,6 %, 2023 bei 18 %."
    assert claim_nennt_bereich(claim)
    assert veraltete_jahrgangszahl(claim, s) is None


def test_schweigt_bei_zwei_echten_prozentzahlen_im_claim():
    """Beide tragen die Einheit — dann ist nicht entscheidbar, welche
    gemeint ist."""
    s = "2024 bei 15,6 %, 2023 bei 18 %."
    assert claim_prozentzahl("erst 15 prozent, jetzt 18 prozent") is None
    assert veraltete_jahrgangszahl("erst 15 prozent, jetzt 18 prozent", s) is None


@pytest.mark.parametrize("summary", [
    "Der unbereinigte Gap liegt bei 15,6 % laut Eurostat.",
    "2023 lag der Gender Pay Gap bei 18 % (DESTATIS).",
])
def test_schweigt_ohne_zwei_datierte_werte(summary):
    assert veraltete_jahrgangszahl(BEFUND_CLAIM, summary) is None


def test_schweigt_ohne_prozentzahl_im_claim():
    s = "2024 bei 15,6 %, 2023 bei 18 %."
    assert veraltete_jahrgangszahl(
        "der gender pay gap in deutschland ist hoch", s) is None


# --- Die Komma-Grenze -------------------------------------------------

def test_die_zuordnung_ueberschreitet_kein_komma():
    """Der konkrete Fehler, isoliert.

    Im aufgezeichneten Text steht das '2024' eflf Zeichen NACH dem
    zweiten '18 %', das '2023' sechzehn davor. Nach reinem Abstand gewinnt
    2024 — dann gilt 18 % als aktueller Wert und das Muster schweigt an
    seinem Anlass.
    """
    paare = jahr_wert_paare(BEFUND_SUMMARY)
    jahre_von_18 = {j for w, j in paare if w == 18.0}
    assert jahre_von_18 == {2023}, (
        f"18 % ist {jahre_von_18} zugeordnet — das 2024 hinter dem Komma "
        f"gehoert zu 'sank', nicht zum Wert")


def test_die_gegenform_ohne_komma_bleibt_zugeordnet():
    """'von 18 % im Jahr 2023' hat kein Komma und muss zugeordnet
    bleiben — sonst waere die Grenze zu scharf."""
    s = "DESTATIS 2024: 16 % (Rückgang von 18 % im Jahr 2023)."
    assert (18.0, 2023) in jahr_wert_paare(s)


def test_der_punkt_gehoert_zu_den_trennern():
    """Gemessen, nicht argumentiert: mit ',;.!?' bestehen 7 von 7
    Einheitsfaellen, mit ',;!?' nur 6 — bei identischem Sweep."""
    assert _Q_TRENNER == frozenset(",;.!?")


def test_eine_jahreszahl_ist_kein_prozentwert():
    s = "Im Jahr 2024 lag der Anteil bei 15,6 %, 2023 bei 18 %."
    werte = {w for w, _j in jahr_wert_paare(s)}
    assert 2024.0 not in werte and 2023.0 not in werte
    assert werte == {15.6, 18.0}


@pytest.mark.parametrize("summary", [
    "Der Wert lag 2024 bei 15,6 Prozent. 2023 waren es 18 Prozent.",
    "Der Wert lag 2024 bei 15,6 PROZENT. 2023 waren es 18 PROZENT.",
    "Der Wert lag 2024 bei 15,6 %. 2023 waren es 18 %.",
])
def test_prozent_wird_in_jeder_schreibweise_erkannt(summary):
    """Begruendungen schreiben "15,6 Prozent" mit grossem P.

    Ohne ``re.IGNORECASE`` traf das Muster nur "%" und kleines "prozent" —
    im QA-Korpus stand ueberall "%", deshalb fiel es dort nicht auf.
    Gefunden hat es die Giftprobe zur Abstandsprobe: Sie HIELT, weil
    "2020 Prozent" gar nicht erkannt wurde, und das war der Hinweis.
    """
    assert veraltete_jahrgangszahl(
        "der gender pay gap liegt bei 18 prozent", summary) is not None


def test_eine_jahreszahl_direkt_vor_der_einheit_zaehlt_nicht():
    """Seit ``re.IGNORECASE`` hat diese Probe einen echten Anlass.

    Vorher hielt die zugehoerige Giftprobe, weil "2020 Prozent" vom
    Muster gar nicht erkannt wurde — das war kein Beweis fuer die
    Abstandsprobe, sondern der Hinweis auf die fehlende
    Gross-/Kleinschreibung. Mit dem Flag wuerde "2020 Prozent" als Wert
    2020 gelesen, wenn der Abstand nicht geprueft wuerde.
    """
    paare = jahr_wert_paare("Stand 2020 Prozent 2024: 15,6 %.")
    werte = {w for w, _j in paare}
    assert 2020.0 not in werte, (
        "eine Jahreszahl unmittelbar vor der Einheit wird als Wert gelesen")
    assert werte == {15.6}, paare


# --- Die Schwelle -----------------------------------------------------

def test_die_schwelle_liegt_zwischen_den_zwei_gemessenen_faellen():
    """Die beiden einzigen Kandidaten des Sweeps klammern sie ein:
    11,1 % soll feuern, 3,9 % soll schweigen."""
    gpg = abs(18.0 - 16.0) / 18.0
    eurobarometer = abs(51.0 - 49.0) / 51.0
    assert eurobarometer < _Q_MIN_ABWEICHUNG < gpg
    assert _Q_MIN_ABWEICHUNG == 0.10


def test_die_schwelle_wird_als_schwaechster_teil_benannt():
    """Eine Schwelle, die nur dieses Jahr passt, gehoert benannt — sonst
    haelt der Naechste sie fuer gemessen."""
    pfad = os.path.join(BACKEND, "services", "verdict_postprocess.py")
    with open(pfad, encoding="utf-8") as fh:
        quelle = fh.read()
    i = quelle.index("_Q_MIN_ABWEICHUNG")
    block = quelle[max(0, i - 1400):i + 200]
    assert "SCHWAECHSTE" in block
    assert "11,1" in block and "3,9" in block


# --- Der Ueber-Trigger-Korpus aus dem Lauf ---------------------------

#: Die funf echten Paare aus QA50H und QA50I, bei denen der Claim eine
#: Prozentzahl nennt UND die Begruendung mindestens zwei datierte Werte
#: fuehrt — also die einzigen, die Muster Q ueberhaupt betreffen kann.
#: Genau einer darf feuern.
SWEEP_KORPUS = [
    (False, "Laut Eurobarometer vertrauen heute 51 Prozent der EU-Bürger der EU",
     "Laut Standard Eurobarometer 102 (Herbst 2024) vertrauten 51 % der "
     "EU-Bürger:innen der EU 'eher' (tend to trust). Neuere Daten "
     "(Eurobarometer 105, Frühjahr 2026) zeigen 49 %."),
    (False, "Oesterreichs Staatsverschuldung liegt bei ueber 80 Prozent des BIP",
     "Österreichs Staatsverschuldung lag 2024 bei exakt 80,0 % des BIP "
     "(Eurostat) und stieg 2025 auf 81,5 % (Eurostat) bzw. 83,3 % "
     "(World Bank, 2026Q1)."),
    (True, "Der Gender Pay Gap in Deutschland liegt bei 18 Prozent",
     BEFUND_SUMMARY),
]


@pytest.mark.parametrize("soll,claim,summary", SWEEP_KORPUS)
def test_der_sweep_korpus_feuert_nur_wo_er_soll(soll, claim, summary):
    assert bool(veraltete_jahrgangszahl(claim.lower(), summary)) is soll


def test_die_hilfspruefungen_sind_fuer_sich_richtig():
    assert claim_prozentzahl("der wert liegt bei 15,6 prozent") == 15.6
    assert claim_prozentzahl("der wert liegt bei 18 %") == 18.0
    assert claim_prozentzahl("der wert ist hoch") is None
    assert claim_nennt_jahr("der wert lag 2023 bei 18 prozent")
    assert not claim_nennt_jahr("der wert liegt bei 18 prozent")
