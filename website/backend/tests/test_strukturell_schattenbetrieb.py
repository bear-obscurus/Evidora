"""Der STRUKTURELL-Override laeuft im Schatten: er entscheidet, aendert nichts.

Audit vom 3.11.2026 (Memo „Audit_STRUKTURELL_Markierung" im Ordner
audit_strukturell_2026-11-03), 84 Live-Laeufe gegen Prod:

* **Nutzen:** 69 Mythen, zufaellig aus den markierten Fakten gezogen. Das
  Modell lieferte **0 Mal** true/mostly_true — der Override wurde nie
  gebraucht. Obere Schranke fuer den Bedarf: 4,3 % (einseitig 95 %).
* **Schaden:** Alle 5 an diesem Tag beobachteten Feuerungen gingen gegen ein
  RICHTIGES Modell-Urteil. Der Fall, der hier nachgestellt ist: „Väter nehmen
  in Österreich selten Karenz", Modell ``true @ 0,95``, die Begruendung nannte
  selbst die 4,1 % — und die stammen aus einem der beiden markierten Fakten.
  Anteil 2/6 = 33 %, Override → ``mostly_false``. Elf weitere richtige Claims
  ueberlebten nur, weil der Anteil unter 15 % lag; einer bei 1/8.

Gemessen waren die kanonischen Formulierungen. Gebaut wurde der Override fuer
schwierigere („in seltenen Faellen ist X moeglich"). Ob er DORT gebraucht
wird, zeigt erst echter Betrieb — deshalb Schatten statt Streichen: Jede
Begegnung von Marker und positivem Urteil wird mit ihrer Entscheidung in
``data/_wartung/strukturell_schatten.jsonl`` geschrieben.

## Ohne Claim-Text

Die Datenschutzerklaerung sagt „Deine eingegebenen Behauptungen werden nicht
geloggt" und „keine dauerhafte Speicherung" (frontend/i18n.js). Das Protokoll
ueberlebt den Deploy — also steht darin nichts, was der Nutzer eingegeben hat
oder was das Modell daraus formuliert hat: Datum, Urteil, Anteil,
Entscheidung, Themen der markierten Fakten.

Keine Netzabfrage in diesen Tests.
"""

import importlib.util
import json
import os
from pathlib import Path

import pytest

import services.verdict_postprocess as vp
from services.verdict_postprocess import apply_verdict_postprocessing

BACKEND = Path(__file__).resolve().parents[1]

VAETER_CLAIM = "Väter nehmen in Österreich selten Karenz"
VAETER_SUMMARY = (
    "In Österreich entfielen 2022 nur 4,1 % der Kinderbetreuungsgeld-"
    "Anspruchstage auf Väter, während 95,9 % auf Frauen entfielen. Nach "
    "Personen bezogen 15,4 % der Väter (Geburtsjahr 2022) Karenz, mit einer "
    "durchschnittlichen Bezugsdauer von 124 Tagen gegenüber 470 Tagen bei "
    "Müttern.")


def _quellen(markiert_themen, normal):
    res = [{"display_value": "STRUKTURELL FALSCH: widerlegt.", "topic": t}
           for t in markiert_themen]
    res += [{"display_value": f"normaler Treffer {i}"} for i in range(normal)]
    return [{"source": "Pack", "results": res}]


def _vaeter_quellen():
    # Live 3.11.2026 21:11 UTC: ratio 2/6
    return _quellen(["kinderbetreuungsgeld_2026", "vereinbarkeit_familie_beruf_2026"], 4)


def _lauf(claim, verdict, summary, quellen, konfidenz=0.95):
    return apply_verdict_postprocessing(
        {"verdict": verdict, "confidence": konfidenz, "summary": summary},
        quellen, claim)


def _protokoll():
    p = Path(vp.SCHATTEN_PROTOKOLL)
    if not p.exists():
        return []
    return [json.loads(z) for z in p.read_text(encoding="utf-8").splitlines() if z.strip()]


# --------------------------------------------------------------------------
# Der Schalter
# --------------------------------------------------------------------------

@pytest.mark.skipif("STRUKTURELL_OVERRIDE" in os.environ,
                    reason="Modus per Umgebung gesetzt")
def test_der_schatten_ist_der_standard():
    assert vp.STRUKTURELL_OVERRIDE_MODUS == "schatten"


# --------------------------------------------------------------------------
# Der Anlassfall
# --------------------------------------------------------------------------

def test_vaeter_bleibt_true_im_schatten():
    out = _lauf(VAETER_CLAIM, "true", VAETER_SUMMARY, _vaeter_quellen())
    assert out["verdict"] == "true", out
    assert out["confidence"] == 0.95


def test_vaeter_wird_aufgeschrieben_als_wuerde_feuern():
    _lauf(VAETER_CLAIM, "true", VAETER_SUMMARY, _vaeter_quellen())
    (z,) = _protokoll()
    assert z["entscheidung"] == "wuerde feuern"
    assert (z["markiert"], z["treffer"]) == (2, 6)
    assert z["verdict"] == "true" and z["confidence"] == 0.95
    assert z["themen"] == ["kinderbetreuungsgeld_2026", "vereinbarkeit_familie_beruf_2026"]
    assert z["modus"] == "schatten"


def test_aktiv_stellt_das_alte_verhalten_her(monkeypatch):
    """Giftprobe: Ohne den Schalter prueften die Tests oben nichts."""
    monkeypatch.setattr(vp, "STRUKTURELL_OVERRIDE_MODUS", "aktiv")
    out = _lauf(VAETER_CLAIM, "true", VAETER_SUMMARY, _vaeter_quellen())
    assert (out["verdict"], out["confidence"]) == ("mostly_false", 0.85)
    assert _protokoll()[-1]["entscheidung"] == "gefeuert"


def test_ein_falsches_modell_urteil_bleibt_falsch():
    """Der Schatten nimmt nur das Ueberschreiben weg, nicht den Marker."""
    out = _lauf("KBG bekommen nur Frauen", "false",
                "Das Kinderbetreuungsgeld steht beiden Elternteilen zu.",
                _vaeter_quellen())
    assert out["verdict"] == "false"
    assert _protokoll() == []   # keine Begegnung mit einem positiven Urteil


# --------------------------------------------------------------------------
# Jede Entscheidung wird aufgeschrieben, nicht nur "wuerde feuern"
# --------------------------------------------------------------------------

def test_tier_1_wird_aufgeschrieben():
    _lauf("Antibiotika helfen nicht gegen Erkältungen", "true",
          "Antibiotika wirken nur gegen Bakterien.",
          _quellen(["antibiotika_resistenz_konsens_2026"], 9))
    (z,) = _protokoll()
    assert (z["entscheidung"], z["markiert"], z["treffer"]) == ("Tier 1", 1, 10)


def test_tier_2_wird_aufgeschrieben():
    _lauf("Irgendein Claim", "true", "Die Behauptung ist korrekt.",
          _quellen(["x"], 3))
    assert _protokoll()[-1]["entscheidung"] == "Tier 2"


def test_tier_2b_wird_aufgeschrieben():
    _lauf("So schlecht ist die ÖBB-Pünktlichkeit gar nicht", "true",
          "Die ÖBB-Pünktlichkeit lag 2024 im Nahverkehr bei 94,2 %.",
          _quellen(["mobilitaet-oebb_puenktlichkeit_2026"], 3))
    assert _protokoll()[-1]["entscheidung"] == "Tier 2b"


def test_ohne_topic_steht_die_quelle_da():
    """Hart codierte Marker (Wikidata, ParlGov …) tragen kein topic."""
    _lauf("Irgendein Claim", "true", "Keine Bestaetigung.",
          [{"source": "Wikidata", "results": [{"display_value": "STRUKTURELL FALSCH: x"}]}])
    assert _protokoll()[-1]["themen"] == ["Wikidata"]


# --------------------------------------------------------------------------
# Datenschutz — nichts vom Nutzer, nichts vom Modell formuliert
# --------------------------------------------------------------------------

ERLAUBTE_FELDER = {"datum", "modus", "entscheidung", "verdict", "confidence",
                   "markiert", "treffer", "themen"}


def test_das_protokoll_hat_nur_die_erlaubten_felder():
    _lauf(VAETER_CLAIM, "true", VAETER_SUMMARY, _vaeter_quellen())
    (z,) = _protokoll()
    assert set(z) == ERLAUBTE_FELDER


def test_kein_claim_und_keine_summary_im_protokoll():
    _lauf(VAETER_CLAIM, "true", VAETER_SUMMARY, _vaeter_quellen())
    roh = Path(vp.SCHATTEN_PROTOKOLL).read_text(encoding="utf-8")
    for stueck in ("Väter", "Karenz", "4,1 %", "Anspruchstage"):
        assert stueck not in roh, stueck


def test_das_datum_ohne_uhrzeit():
    _lauf(VAETER_CLAIM, "true", VAETER_SUMMARY, _vaeter_quellen())
    (z,) = _protokoll()
    assert len(z["datum"]) == 10 and z["datum"][4] == "-"


# --------------------------------------------------------------------------
# Wo die Datei liegt, und dass ein Schreibfehler nichts kostet
# --------------------------------------------------------------------------

def test_der_echte_pfad_liegt_im_gitignorierten_wartungsordner():
    """conftest lenkt den Pfad in jedem Test um — hier der Originalwert."""
    spec = importlib.util.spec_from_file_location(
        "_vp_frisch", BACKEND / "services" / "verdict_postprocess.py")
    frisch = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(frisch)
    pfad = Path(frisch.SCHATTEN_PROTOKOLL)
    assert pfad == BACKEND / "data" / "_wartung" / "strukturell_schatten.jsonl"
    gitignore = (BACKEND.parents[1] / ".gitignore").read_text(encoding="utf-8")
    assert "website/backend/data/_wartung/" in gitignore


def test_ein_schreibfehler_kostet_keine_anfrage(monkeypatch, tmp_path):
    blockiert = tmp_path / "datei"
    blockiert.write_text("x")
    monkeypatch.setattr(vp, "SCHATTEN_PROTOKOLL", str(blockiert / "sub" / "p.jsonl"))
    out = _lauf(VAETER_CLAIM, "true", VAETER_SUMMARY, _vaeter_quellen())
    assert out["verdict"] == "true"


# --------------------------------------------------------------------------
# Dieselbe Entscheidung wie im Golden-Satz — nur ohne Wirkung
# --------------------------------------------------------------------------

def _golden_feuerfaelle():
    spec = importlib.util.spec_from_file_location(
        "_golden", BACKEND / "tests" / "test_verdict_postprocess_golden.py")
    g = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(g)
    return [c for c in g.CASES if c[0].startswith(("struct_fire", "struct_tier_boundary_015_no_confirm"))]


@pytest.mark.parametrize("fall", _golden_feuerfaelle(), ids=lambda c: c[0])
def test_golden_feuerfall_wird_im_schatten_nur_aufgeschrieben(fall):
    import copy
    cid, claim, result_in, quellen, _, _ = fall
    out = apply_verdict_postprocessing(copy.deepcopy(result_in), copy.deepcopy(quellen), claim)
    eintraege = _protokoll()
    assert eintraege and eintraege[0]["entscheidung"] == "wuerde feuern", (cid, eintraege)
    assert "_struct_override_fired" not in out


def test_es_gibt_golden_feuerfaelle():
    """Sonst liefe die Parametrisierung oben leer und waere gruen."""
    assert len(_golden_feuerfaelle()) >= 5
