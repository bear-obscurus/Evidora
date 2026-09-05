"""Der Früherkennungs-Fakt: zwei falsche Leitlinien-Angaben und 90 % Prompt-Verlust.

Befund (2026-09-05): `frueh_erkennung_uebertherapie_konsens` war EIN Fakt mit
4.031 Zeichen, der Mammographie, PSA, Darmkrebs, Schilddrüse und die
Übertherapie-Definition zugleich trug.

**Warum das den Prompt nicht erreicht.** Neun der zehn Pack-Fakten tragen einen
`STRUKTURELL FALSCH:`-Marker und sind damit von der 400-Zeichen-Kürzung im
Synthesizer ausgenommen. Dieser eine nicht — zu Recht, denn er ist bewusst
DIFFERENZIERT formuliert und trägt keine Verdict-Direktive. Die Folge: genau der
Fakt, bei dem die Differenzierung der ganze Inhalt ist, kam zu rund 10 % an. Und
die claim-zentrierte Kürzung schnitt das Falsche heraus:

  Claim „Sollte jede Frau ab 40 zur Mammographie?"
    → ankommend: dreimal „Mammographie 50-69 J."; die 40er-Aussage fiel weg
  Claim „Der PSA-Test rettet Leben"
    → ankommend: Übertherapie-Definition + DARMKREBS-Abschnitt; PSA fiel ganz weg

Die Lösung ist nicht, dem Fakt einen Override-Marker zu verpassen (er ist nicht
falsch, sondern differenziert), sondern ihn nach Screening-Typ zu TEILEN. Dann
kürzt der Prompt weiterhin — aber im richtigen Fakt, und die Headline (≤400
Zeichen, nachweislich ungekürzt) trägt die Entscheidung.

**Zwei Angaben waren zusätzlich sachlich falsch**, beide an der Quelle geprüft:

  Mammographie 40-49: Fakt sagte „USPSTF 2024 Grade C (individuelle
    Entscheidung)". Das USPSTF Final Recommendation Statement vom 30.4.2024
    sagt das Gegenteil: „Women aged 40 to 74 years: biennial screening
    mammography. Grade: B." Der Fakt nannte also das Jahr der Umstellung und
    referierte den Stand DAVOR.
  Darmkrebs: Fakt sagte „USPSTF Grade A 45-75". Tatsächlich (Statement vom
    18.5.2021): Grade A für 50-75, Grade B für 45-49, Grade C selektiv 76-85.

Ergänzt wurden die europäischen Anker, die ganz fehlten: die Empfehlung des
EU-Rates vom 9.12.2022 (2022/C 473/01) und das österreichische Brustkrebs-
Früherkennungsprogramm. Für einen österreichischen Dienst sind sie relevanter
als eine US-Task-Force.
"""

import asyncio
import json
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
DATEN = json.loads((BACKEND / "data" / "onkologie_pack.json").read_text(encoding="utf-8"))
FAKTEN = {f["topic"]: f for f in DATEN["facts"]}

MAX_STR = 400

from services.onkologie_pack import search_onkologie  # noqa: E402
from services.synthesizer import (  # noqa: E402
    _claim_centered_truncate,
    _prompt_claim_terms,
)

SCREENING_FAKTEN = ("screening_uebertherapie_grundprinzip", "mammographie_screening",
                    "psa_prostata_screening", "darmkrebs_screening")


def _treffer(claim):
    return asyncio.run(search_onkologie({"claim": claim,
                                         "original_claim": claim}))["results"]


def _fakt_fuer(claim, topic):
    for x in _treffer(claim):
        if x["topic"] == topic:
            return x
    raise AssertionError(f"{claim!r} liefert nichts zu {topic!r}, "
                         f"sondern {[x['topic'] for x in _treffer(claim)]}")


def _was_ankommt(x, claim):
    """Was der Synthesizer aus display_value tatsächlich sieht."""
    dv = x["display_value"]
    if "STRUKTURELL FALSCH:" in dv or len(dv) <= MAX_STR:
        return dv
    return _claim_centered_truncate(dv, _prompt_claim_terms({"claim": claim}, claim),
                                    MAX_STR)


# --------------------------------------------------------------------------
# Die Teilung
# --------------------------------------------------------------------------

def test_der_sammel_fakt_ist_aufgeteilt():
    assert "frueh_erkennung_uebertherapie_konsens" not in FAKTEN
    for t in SCREENING_FAKTEN:
        assert t in FAKTEN, f"{t} fehlt"


def test_jeder_claim_holt_seinen_eigenen_fakt():
    """Vorher bekam ein PSA-Claim den Darmkrebs-Abschnitt."""
    for claim, erwartet in (
        ("Sollte jede Frau ab 40 zur Mammographie?", "mammographie_screening"),
        ("Der PSA-Test rettet Leben", "psa_prostata_screening"),
        ("Ab welchem Alter Darmspiegelung?", "darmkrebs_screening"),
        ("Mehr Krebsfrüherkennung rettet Leben", "screening_uebertherapie_grundprinzip"),
    ):
        topics = [x["topic"] for x in _treffer(claim)]
        assert erwartet in topics, f"{claim!r} -> {topics}"


def test_die_entscheidung_steht_im_ungekuerzten_kanal():
    """Headline ≤ MAX_STR passiert die Prompt-Kürzung nachweislich ungekürzt."""
    for t in SCREENING_FAKTEN:
        kopf = FAKTEN[t]["headline"]
        assert len(kopf) <= MAX_STR, f"{t}: {len(kopf)} Zeichen"


def test_mammographie_claim_bekommt_die_40er_aussage():
    """Der Regressionstest zum eigentlichen Schaden: vorher kam dreimal
    'Mammographie 50-69 J.' an, obwohl der Claim nach 40 fragte."""
    claim = "Sollte jede Frau ab 40 zur Mammographie?"
    x = _fakt_fuer(claim, "mammographie_screening")
    assert "40 bis 74" in x["indicator_name"]
    assert "Grade B" in x["indicator_name"]


def test_psa_claim_bekommt_psa_evidenz_statt_darmkrebs():
    claim = "Der PSA-Test rettet Leben"
    x = _fakt_fuer(claim, "psa_prostata_screening")
    ankommend = x["indicator_name"] + " " + _was_ankommt(x, claim)
    assert "PSA" in ankommend
    assert "Darmkrebs" not in ankommend and "Kolonoskopie" not in ankommend


def test_kein_screening_fakt_traegt_eine_verdict_direktive():
    """Die Frage ist differenziert, nicht falsch. Ein STRUKTURELL-Marker wäre
    Missbrauch des Override-Mechanismus — und die bequeme Abkürzung, um die
    Kürzung zu umgehen."""
    for t in SCREENING_FAKTEN:
        for claim in FAKTEN[t]["claim_phrasings_handled"]:
            for x in _treffer(claim):
                if x["topic"] == t:
                    assert "STRUKTURELL FALSCH:" not in x["display_value"], (
                        f"{t} trägt einen Override-Marker")


# --------------------------------------------------------------------------
# Die zwei sachlich falschen Angaben
# --------------------------------------------------------------------------

def test_mammographie_ist_grade_b_ab_40_nicht_grade_c():
    """USPSTF Final Recommendation Statement vom 30.4.2024: 'Women aged 40 to
    74 years: biennial screening mammography. Grade: B.'"""
    f = FAKTEN["mammographie_screening"]
    text = f["headline"] + json.dumps(f["data"], ensure_ascii=False)
    assert "40 bis 74" in text and "Grade B" in text
    assert "30.4.2024" in text
    assert "Grade C" in text and "ÜBERHOLT" in text.upper(), (
        "die alte Einstufung muss ausdrücklich als überholt markiert sein")
    assert "breast-cancer-screening" in f["source_url"]


def test_darmkrebs_grade_ist_abgestuft_nicht_pauschal_a():
    """USPSTF vom 18.5.2021: A für 50-75, B für 45-49, C selektiv 76-85."""
    f = FAKTEN["darmkrebs_screening"]
    text = f["headline"] + json.dumps(f["data"], ensure_ascii=False)
    assert "Grade A" in text and "50-75" in text
    assert "Grade B" in text and "45-49" in text
    assert "Grade C" in text and "76-85" in text
    assert "Grade A 45-75" not in text, "die falsche pauschale Angabe ist zurück"


def test_psa_grade_stimmt():
    f = FAKTEN["psa_prostata_screening"]
    text = f["headline"] + json.dumps(f["data"], ensure_ascii=False)
    assert "55 bis 69" in text and "Grade C" in text
    assert "70" in text and "Grade D" in text


# --------------------------------------------------------------------------
# Die fehlenden europäischen Anker
# --------------------------------------------------------------------------

def test_eu_ratsempfehlung_ist_bei_allen_drei_tests_hinterlegt():
    for t in ("mammographie_screening", "psa_prostata_screening", "darmkrebs_screening"):
        d = FAKTEN[t]["data"]
        assert "eu_ratsempfehlung_2022" in d, f"{t}: EU-Anker fehlt"
        assert "9.12.2022" in d["eu_ratsempfehlung_2022"]
        assert "2022/C 473/01" in d["eu_ratsempfehlung_2022"]


def test_eu_werte_stimmen_mit_dem_amtsblatt():
    m = FAKTEN["mammographie_screening"]["data"]["eu_ratsempfehlung_2022"]
    assert "50 bis 69" in m and "45" in m and "74" in m
    dk = FAKTEN["darmkrebs_screening"]["data"]["eu_ratsempfehlung_2022"]
    assert "50 und 74" in dk and "FIT" in dk
    psa = FAKTEN["psa_prostata_screening"]["data"]["eu_ratsempfehlung_2022"]
    assert "STUFENWEISEN" in psa.upper() and "MRT" in psa


def test_oesterreichisches_programm_ist_hinterlegt():
    """Für einen österreichischen Dienst der relevanteste Anker — er fehlte."""
    d = FAKTEN["mammographie_screening"]["data"]["oesterreich_programm"]
    assert "45 bis 74" in d and "40 bis 44" in d
    assert "e-card" in d and "zwei Jahre" in d
    assert "frueh-erkennen.at" in FAKTEN["mammographie_screening"]["secondary_url"]


# --------------------------------------------------------------------------
# Messgrößen nicht tauschen
# --------------------------------------------------------------------------

def test_uebertherapie_zahlen_sind_als_zwei_groessen_markiert():
    """Marmot 2013 nennt ein VERHÄLTNIS (3 je verhindertem Todesfall), die
    NHS-Kohorte 2026 einen DIAGNOSE-ÜBERSCHUSS (4 %). Wer sie gegeneinander
    rechnet, hält zwei verschiedene Grössen gegeneinander (Lehre aus #321)."""
    d = FAKTEN["mammographie_screening"]["data"]
    assert "messgroesse" in d
    m = d["messgroesse"]
    assert "VERHÄLTNIS" in m and "ÜBERSCHUSS" in m.upper()
    assert "nicht gegeneinander rechnen" in m or "nicht gegeneinander" in m


def test_neue_langzeitdaten_sind_belegt():
    n = FAKTEN["mammographie_screening"]["data"]["nutzen_und_uebertherapie"]
    assert "42362870" in n, "PMID fehlt"
    assert "28 %" in n and "33 %" in n and "4 %" in n
    assert "Cochrane 2013" in n and "Marmot" in n, "die älteren Zahlen bleiben"


# --------------------------------------------------------------------------
# Trigger
# --------------------------------------------------------------------------

def test_alle_phrasings_treffen_ihren_eigenen_fakt():
    fehl = []
    for f in DATEN["facts"]:
        for p in f.get("claim_phrasings_handled", []):
            if f["topic"] not in [x["topic"] for x in _treffer(p)]:
                fehl.append((f["topic"], p))
    assert not fehl, f"Phrasings ohne den eigenen Fakt: {fehl}"


def test_kein_ueber_trigger_durch_die_neuen_eintraege():
    for claim in ("Wie hoch ist die Inflation in Österreich?",
                  "Der Darm ist ein wichtiges Organ",
                  "Wie alt wird man in Österreich?",
                  "Wer gewinnt die Wahl?"):
        topics = [x["topic"] for x in _treffer(claim)]
        assert not set(topics) & set(SCREENING_FAKTEN), f"{claim!r} -> {topics}"
