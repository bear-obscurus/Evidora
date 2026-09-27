"""Wartungs-Notizen erreichen den Prompt nicht mehr.

Anlass (2026-09-23, PR #191): Der Claim "Die Leerstandsabgabe in Vorarlberg
betraegt 3.000 Euro im Jahr" bekam mostly_true@0.85 mit der Begruendung
"laut RIS-Daten 1.000 bis 3.000 Euro pro Jahr" — eine Spanne, die am Vortag
als unbelegt aus dem Fakt entfernt worden war und nur noch in der Notiz
"Korrigiert 2026-09-23: Der Fakt nannte …" stand. Die Pack-Services haengen
alle context_notes an die ``description``, und die claim-zentrierte Kuerzung
legt ihr Fenster genau um die zitierte Zahl.

#191 hat das per Guard verboten (die Notiz darf die Zahl nicht mehr nennen).
Massnahme D macht es mechanisch unmoeglich: ``prompt_notizen()`` filtert
datierte Korrektur-Vermerke heraus, bevor sie in ein Ergebnis wandern.

ABSICHTLICH ENG: Gemessen ueber alle Pakete sind von 1.718 context_notes
genau 10 datierte Wartungs-Vermerke. Der Rest traegt Inhalt und bleibt drin
— pauschal alle Notizen zu streichen wuerde Substanz kosten und ausserdem
die Eingabe des Rerankers aendern, an der seine Schwellen haengen.

Keine Netzabfrage in diesen Tests.
"""

import asyncio
import json
import re
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
DATA = BACKEND / "data"
sys.path.insert(0, str(BACKEND))

from services._notizen import ist_wartungsnotiz, prompt_notizen  # noqa: E402


# --------------------------------------------------------------------------
# Die Erkennung
# --------------------------------------------------------------------------

@pytest.mark.parametrize("notiz", [
    "Korrigiert 2026-09-23: Die frühere Fassung nannte pauschale Jahresbeträge.",
    "Korrigiert 2026-09-22: Die frühere Fassung rechnete EU- und Bundesmittel zusammen.",
    "  Korrigiert 2026-01-01 : mit Leerzeichen um den Doppelpunkt",
])
def test_datierter_vermerk_wird_erkannt(notiz):
    assert ist_wartungsnotiz(notiz)


@pytest.mark.parametrize("notiz", [
    "Zensus 2022 hat die Bevölkerungs-Schätzung um ~1,4 Mio nach unten korrigiert.",
    "Die Zahlen wurden 2024 korrigiert veröffentlicht.",
    "Korrigiert wurde die Reihe erst später.",
    "Datenstand: ESSOSS-Reihe bis 2024, Stand 21.11.2025.",
    "Wichtige Differenzierung: Bio ist nicht pestizidfrei.",
])
def test_inhalt_bleibt_inhalt(notiz):
    """Ein beilaeufiges 'korrigiert' ist kein Wartungsvermerk."""
    assert not ist_wartungsnotiz(notiz)


def test_prompt_notizen_filtert_nur_die_vermerke():
    notizen = [
        "Wichtige Differenzierung: A gilt nur für B.",
        "Korrigiert 2026-09-23: Die frühere Fassung nannte X.",
        "Datenstand: Bericht 2024.",
    ]
    assert prompt_notizen(notizen) == [notizen[0], notizen[2]]


def test_leere_eingabe():
    assert prompt_notizen(None) == []
    assert prompt_notizen([]) == []


# --------------------------------------------------------------------------
# Kein Service haengt die Notizen mehr ungefiltert an
# --------------------------------------------------------------------------

def test_kein_service_joint_die_notizen_ungefiltert():
    """Der Filter sitzt in 55 Services an genau einer Stelle — wer einen
    neuen Pack-Service anlegt, darf die Zeile nicht abschreiben."""
    roh = re.compile(r'notes_joined\s*=\s*" \| "\.join\(notes\)')
    treffer = [p.name for p in sorted((BACKEND / "services").glob("*.py"))
               if roh.search(p.read_text(encoding="utf-8"))]
    assert not treffer, treffer


def test_alle_services_mit_notizen_importieren_den_filter():
    fehlend = []
    for p in sorted((BACKEND / "services").glob("*.py")):
        s = p.read_text(encoding="utf-8")
        if "notes_joined" in s and "prompt_notizen" not in s:
            fehlend.append(p.name)
    assert not fehlend, fehlend


# --------------------------------------------------------------------------
# Ende zu Ende: der Fall vom 23.9.2026
# --------------------------------------------------------------------------

def test_leerstandsabgabe_result_traegt_keinen_wartungsvermerk():
    from services.wohnen_pack import search_wohnen

    claim = "Die Leerstandsabgabe in Vorarlberg beträgt 3.000 Euro im Jahr"
    r = asyncio.run(search_wohnen({"original_claim": claim}))
    assert r["results"], "Fakt triggert nicht mehr"
    for x in r["results"]:
        nutzlast = " ".join(str(x.get(k) or "") for k in
                            ("description", "display_value", "indicator_name"))
        assert "Korrigiert 2026" not in nutzlast, x.get("indicator_name", "")[:60]


def test_die_widerlegte_zahl_steht_nur_noch_im_fakt_selbst():
    """Gegenprobe: Die Notiz im Fakt nennt die alte Spanne nicht mehr (das
    prueft test_korrekturnotiz_ohne_alte_zahlen) — und selbst wenn sie es
    taete, kaeme sie nicht mehr in den Prompt."""
    d = json.loads((DATA / "wohnen_pack.json").read_text(encoding="utf-8"))
    f = next(x for x in d["facts"] if x["id"] == "leerstandsabgabe_wirkung_2026")
    vermerke = [n for n in f["context_notes"] if ist_wartungsnotiz(n)]
    assert vermerke, "der Fakt soll seinen Korrektur-Vermerk behalten"
    assert prompt_notizen(f["context_notes"]) == [
        n for n in f["context_notes"] if not ist_wartungsnotiz(n)]


def test_substanz_bleibt_im_prompt():
    """Der Filter darf nicht die halbe Fakt-Erklaerung mitnehmen."""
    d = json.loads((DATA / "wohnen_pack.json").read_text(encoding="utf-8"))
    f = next(x for x in d["facts"] if x["id"] == "leerstandsabgabe_wirkung_2026")
    behalten = prompt_notizen(f["context_notes"])
    assert len(behalten) >= len(f["context_notes"]) - 1
    assert any("Steiermark" in n for n in behalten)
