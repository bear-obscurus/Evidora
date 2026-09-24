"""Prompt-Zensus: kommt die entscheidende Angabe ueberhaupt beim Modell an?

Anlass (2026-09-24). Der Synthesizer kuerzt jedes Prompt-Feld auf 400
Zeichen und waehlt dafuer die claim-relevantesten Saetze. Ein kuratierter
Fakt hat 3.000 bis 6.000 Zeichen — das Modell sieht also rund 7 % davon,
und WELCHE 7 % entscheidet eine Heuristik, die bis jetzt niemand gemessen
hat. Ein Fakt konnte die richtige Zahl enthalten, die Fakt-Tests konnten
gruen sein, und das Verdict trotzdem falsch.

Gemessen am Leerstandsabgaben-Fakt, Claim "Wie hoch ist die Leerstandsabgabe
in Tirol?" — was vorher ankam:

    "… | Tirol bis 2025: Tirol, TFLAG (LGBl. […] | Tirol seit 2026: Tirol
     seit 1.1.2026 (LGBl. […] | Tirol basismietwerte: Tirol seit 15.8.2026
     (LGBl. […]"

Drei abgeschnittene Satzanfaenge ohne eine einzige Zahl. Ursache: Die
Satztrennung ``(?<=[.!?])\\s+`` sieht hinter "LGBl." ein Satzende.

Dieser Test haelt drei Dinge fest:

    1. die Satzgrenzen halten deutsche Abkuerzungen und Ordnungszahlen aus
    2. die Claim-Terme ankern schreibweisen- und flexionstolerant
    3. die Batterie in tools/prompt_zensus_batterie.json kommt vollstaendig
       an — und die Liste bekannter Luecken waechst nicht

Keine Netzabfrage, kein Modell.
"""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._satzgrenzen import teile_in_einheiten  # noqa: E402
from services.synthesizer import (  # noqa: E402
    _claim_centered_truncate,
    _prompt_claim_terms,
)

_spec = importlib.util.spec_from_file_location(
    "prompt_zensus", BACKEND / "tools" / "prompt_zensus.py")
zensus = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(zensus)

BATTERIE = json.loads((BACKEND / "tools" / "prompt_zensus_batterie.json")
                      .read_text(encoding="utf-8"))
MAX = 400


# --------------------------------------------------------------------------
# 1. Satzgrenzen
# --------------------------------------------------------------------------

@pytest.mark.parametrize("text", [
    "Tirol, TFLAG (LGBl. Nr. 86/2022), in Kraft seit 1.1.2023: bis 30 m² 10 bis 25 Euro.",
    "Die Höhe legt der Gemeinderat fest (§ 9 Abs. 3 und 4 TFLAG).",
    "Das Agrarbudget betrug 2.652 Mio. Euro im Jahr 2024.",
    "Das Gesetz gilt seit 1. Jänner 2023 in ganz Tirol.",
    "Mariona Segú, J. Public Economics 185 (2020), doi:10.1016/j.jpubeco.2019.104079.",
])
def test_abkuerzung_beendet_keinen_satz(text):
    assert len(teile_in_einheiten(text)) == 1, teile_in_einheiten(text)


def test_echte_satzenden_trennen_weiterhin():
    t = "Das Gesetz gilt seit 2023. Die Gemeinden sind ermächtigt. Wirklich?"
    assert teile_in_einheiten(t) == [
        "Das Gesetz gilt seit 2023.",
        "Die Gemeinden sind ermächtigt.",
        "Wirklich?",
    ]


def test_feldseparator_trennt():
    """render_data_with_marker haengt die data-Felder mit ' | ' aneinander —
    die kleinste Einheit ist im Zweifel ein ganzes Feld."""
    t = "Headline. | Tirol bis 2025: zehn bis 215 Euro. | Vorarlberg: 8,20 Euro."
    assert len(teile_in_einheiten(t)) == 3


def test_leerer_text():
    assert teile_in_einheiten("") == []


# --------------------------------------------------------------------------
# 2. Ankern der Claim-Terme
# --------------------------------------------------------------------------

def _feld(kern: str) -> str:
    """Ein Feld ueber dem Limit, in dem ``kern`` hinten steht."""
    return ("Vorspann ohne Bezug zum Claim. " * 12) + kern


def test_umlaut_schreibweise_ankert():
    """'Oesterreich' muss 'Österreich' finden — sonst zaehlt der Term 0
    Treffer und ein beliebiger anderer Satz gewinnt das Budget."""
    s = _feld("In Österreich lagen die Ausgaben 2024 bei 265 Mio. Euro.")
    out = _claim_centered_truncate(s, _prompt_claim_terms({}, "Ausgaben in Oesterreich"), MAX)
    assert "265 Mio. Euro" in out


def test_flexionsendung_ankert():
    """'Partnern' muss 'Partnerschaftsgewalt' finden (Wortstamm statt
    Varianten-Liste — dieselbe Lehre wie bei den Frontex-Formen)."""
    s = _feld("Im Kontext von Partnerschaftsgewalt gab es 133 vollendete Taten.")
    out = _claim_centered_truncate(s, ["partnern"], MAX)
    assert "133 vollendete Taten" in out


def test_abdeckung_schlaegt_einzelnen_seltenen_treffer():
    """Zwei Claim-Terme in einem Satz schlagen einen seltenen Term allein."""
    zwei = "Die Leerstandsabgabe in Tirol beträgt 10 bis 215 Euro je Monat."
    einer = "Tirol hat 279 Gemeinden."
    s = einer + " " + ("Fülltext ohne Bezug. " * 12) + zwei
    out = _claim_centered_truncate(s, ["tirol", "leerstandsabgabe"], MAX)
    assert "10 bis 215 Euro" in out


# --------------------------------------------------------------------------
# 3. Die Batterie
# --------------------------------------------------------------------------

def test_batterie_ist_nicht_leer():
    assert len(BATTERIE["claims"]) >= 12


@pytest.mark.parametrize("eintrag", BATTERIE["claims"],
                         ids=[f"{e['fakt']}:{e['claim'][:40]}" for e in BATTERIE["claims"]])
def test_muss_treffer_kommt_im_prompt_an(eintrag):
    r = zensus.pruefe(eintrag, MAX)
    assert not r["fehlt"], (eintrag["claim"], r["fehlt"], r["text"]["display_value"])


def test_bekannte_luecken_wachsen_nicht():
    """Gemessene Faelle, in denen das 400-Zeichen-Budget die Angabe abschneidet.
    Sie sind der Auftrag fuer Massnahme B — und duerfen nicht mehr werden."""
    luecken = BATTERIE["bekannte_luecken"]
    assert len(luecken) <= 3, [l["claim"] for l in luecken]
    for l in luecken:
        assert len(l.get("grund", "")) > 40, l["claim"]


def test_luecken_sind_echte_luecken():
    """Was als Luecke gefuehrt wird, muss auch eine sein — sonst bleibt ein
    laengst geschlossener Fall ewig als Ausnahme stehen."""
    offen = [l["claim"] for l in BATTERIE["bekannte_luecken"]
             if not zensus.pruefe(l, MAX)["fehlt"]]
    assert not offen, f"kommt inzwischen an, gehoert in claims: {offen}"


def test_zensus_meldet_fehler_als_exitcode():
    """Das Gate muss rot werden koennen — sonst bewacht es nichts."""
    erfunden = {"claim": "Wie hoch ist die Leerstandsabgabe in Tirol?",
                "datei": "wohnen_pack.json",
                "fakt": "leerstandsabgabe_wirkung_2026",
                "muss": ["diese Zeichenfolge steht garantiert nirgends"]}
    assert zensus.pruefe(erfunden, MAX)["fehlt"]
