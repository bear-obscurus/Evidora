"""Eine Korrektur-Notiz darf die widerlegte Zahl nicht mehr nennen.

Anlass (2026-09-23, Live-Messung nach #190): Der Claim „Die Leerstandsabgabe
in Vorarlberg betraegt 3.000 Euro im Jahr" bekam **mostly_true@0.85** mit der
Begruendung „laut RIS-Daten 1.000 bis 3.000 Euro pro Jahr" — obwohl genau
diese Spanne am Vortag als unbelegt aus dem Fakt entfernt worden war.

Der Weg dorthin ist mechanisch:

    _descriptor / description = data["context"] + " " + ALLE context_notes
    synthesizer:  MAX_STR = 400, _claim_centered_truncate(s, claim_terms)

Die Notiz „Korrigiert: Der Fakt nannte 'Vorarlberg 1.000 bis 3.000 Euro/Jahr'"
enthaelt die Claim-Begriffe (vorarlberg, 3.000, euro) praeziser als der
korrigierte Fakt selbst. Die claim-zentrierte Kuerzung schneidet ihr Fenster
also genau um die falsche Zahl — und der Rahmen („Der Fakt nannte …", „war
unbelegt") faellt weg, weil die Notizen zu einem langen String verkettet
werden. Je genauer die Notiz die widerlegte Zahl zitiert, desto sicherer wird
sie als Antwort serviert.

Regel: Wer eine Zahl widerlegt, nennt sie im **Fakt** (kurzes data-Feld unter
400 Zeichen, Widerlegung im selben Feld) — nicht in einer context_note. In der
Notiz steht, WAS falsch war, nicht WELCHE Zahl.

Der Test prueft alle kuratierten Pakete: In einer Notiz, die sich selbst als
Korrektur ausweist, muss jede mehrstellige Zahl auch in headline oder data
vorkommen. Jahreszahlen und Datumsangaben sind ausgenommen — sie benennen den
Stand, nicht die widerlegte Groesse.

Keine Netzabfrage in diesen Tests.
"""

import json
import re
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
DATA = BACKEND / "data"

_ZAHL = re.compile(r"\d[\d.,]*")
_DATUM = re.compile(r"^\d{1,2}\.\d{1,2}\.\d{4}$")
_JAHR = re.compile(r"^(19|20)\d\d$")
_KORREKTUR = re.compile(
    r"Korrigiert|Der Fakt (nannte|sagte|behauptete)|frühere Fassung|fruehere Fassung"
    r"|als unbelegt entfernt|Ebenfalls entfernt|Raus:", re.I)


def _facts(obj):
    if isinstance(obj, dict):
        if "id" in obj and "context_notes" in obj:
            yield obj
        for v in obj.values():
            yield from _facts(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _facts(v)


def _pakete() -> list[tuple[str, dict]]:
    aus = []
    for f in sorted(DATA.glob("*.json")):
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        for fakt in _facts(d):
            if any(_KORREKTUR.search(n) for n in (fakt.get("context_notes") or [])
                   if isinstance(n, str)):
                aus.append((f.name, fakt))
    return aus


PAKETE = _pakete()


def _waisen(fakt: dict) -> dict[str, list[str]]:
    """Zahlen aus Korrektur-Notizen, die in der Aussage des Fakts fehlen."""
    aussage = (fakt.get("headline") or "") + " " + json.dumps(fakt.get("data") or {},
                                                              ensure_ascii=False)
    aus: dict[str, list[str]] = {}
    for notiz in (fakt.get("context_notes") or []):
        if not isinstance(notiz, str) or not _KORREKTUR.search(notiz):
            continue
        offen = []
        for roh in _ZAHL.findall(notiz):
            zahl = roh.rstrip(".,")
            if _DATUM.match(zahl) or _JAHR.match(zahl):
                continue
            if len(re.sub(r"\D", "", zahl)) < 3:
                continue
            if zahl not in aussage:
                offen.append(zahl)
        if offen:
            aus[notiz] = sorted(set(offen))
    return aus


def test_es_gibt_ueberhaupt_korrekturnotizen():
    """Sonst prueft der Test nichts — und niemand merkt es."""
    assert len(PAKETE) >= 5, len(PAKETE)


@pytest.mark.parametrize("datei,fakt", PAKETE, ids=[f"{d}:{f['id']}" for d, f in PAKETE])
def test_korrekturnotiz_nennt_keine_widerlegte_zahl(datei, fakt):
    waisen = _waisen(fakt)
    assert not waisen, (datei, fakt["id"], waisen)


def test_regel_erkennt_den_fall_vom_23_9_2026():
    """Gegenprobe an der Notiz, die den Fehlverdict ausgeloest hat."""
    beispiel = {
        "headline": "Tirol seit 1.1.2023, Vorarlberg seit 1.1.2024.",
        "data": {"vorarlberg_saetze": "8,20 bis 18,50 Euro je m² und Jahr."},
        "context_notes": ["Korrigiert 2026-09-23: Der Fakt nannte 'Vorarlberg "
                          "1.000 bis 3.000 Euro/Jahr'."],
    }
    assert _waisen(beispiel), "die Regel muesste hier anschlagen"
    beispiel["context_notes"] = ["Korrigiert 2026-09-23: Die frühere Fassung nannte "
                                 "pauschale Jahresbeträge."]
    assert not _waisen(beispiel)


def test_jahreszahlen_und_daten_bleiben_erlaubt():
    beispiel = {
        "headline": "Stand 2026.",
        "data": {"x": "irgendwas"},
        "context_notes": ["Korrigiert 2026-09-22: Datenstand 21.11.2025, Bericht 2024."],
    }
    assert not _waisen(beispiel)
