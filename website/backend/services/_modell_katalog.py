"""Steht das Modell, mit dem wir antworten, noch im Katalog — und ist es
noch dasselbe?

Anlass: Mistral-Mail vom 30.10.2026. Prod faehrt
``MISTRAL_MODEL=mistral-medium-latest``, und der Katalog zeigt, dass
``mistral-medium-latest`` und ``mistral-medium-3-5`` EINANDER als Alias
fuehren — dasselbe Modell, datierte ID ``mistral-medium-2604``. Genau das
wird am 30.4.2027 abgeschaltet.

## Warum nicht einfach das Feld der API lesen

Weil es leer ist. Gemessen am 30.10.2026 ueber alle **64** Modelle, die
unser Key sieht: ``deprecation`` und ``deprecation_replacement_model``
sind **ueberall None** — auch bei ``zai-glm-5-2``, das am Tag nach der
Messung abgeschaltet wurde. Ein Waechter auf dieses Feld waere wertlos.

## Woran die Identitaet also haengt

Der Katalog nennt zu jedem Eintrag seine Alias-Geschwister. Unter ihnen
steht bei 43 der 64 Modelle eine **datierte ID** im Format JJMM
(``mistral-medium-2604`` = April 2026, ``mistral-large-2512`` = Dezember
2025). Wechselt die, ist das Modell unter uns getauscht worden — genau
das Risiko eines ``-latest``-Alias (#285: „das Modell laeuft hinter einem
-latest-Alias und kann sich unter uns aendern").

⚠️ **21 Modelle haben keine datierte ID** — darunter ``mistral-large-4``,
das Mistral als Ersatz empfiehlt. Fuer die ist die Identitaet hier NICHT
pinnbar; der Waechter sagt das dann ausdruecklich, statt eine Sicherheit
zu behaupten, die er nicht hat.

Die Alias-MENGE als Fingerabdruck waere empfindlicher, aber sie churnt
aus harmlosen Gruenden (``mistral-vibe-cli-latest`` und
``magistral-medium-latest`` haengen heute am Medium-Eintrag). Ein
Waechter, der bei jedem Schluckauf schreit, wird abgeschaltet (#128) —
deshalb loest nur die datierte ID einen Push aus, eine blosse
Alias-Aenderung bleibt eine Log-Zeile.

Dependency-light: nur re, kein Netz, kein httpx.
"""
from __future__ import annotations

import re

#: Die erwartete datierte ID je konfiguriertem Modellnamen. Bewusst im
#: Code und nicht in einer Datei, die ein Werkzeug selbst fortschreibt:
#: Ein Modellwechsel soll ein lesbarer Commit sein, kein stiller
#: Zustandssprung (#243 — „wer neu meldet, braucht ein Maschinen-
#: Gedaechtnis GETRENNT von der Handliste"; hier ist diese Liste die
#: Handliste, der Katalog das Gedaechtnis).
MODELL_PINS = {
    "mistral-medium-latest": "mistral-medium-2604",
    "mistral-medium": "mistral-medium-2604",
    "mistral-medium-3-5": "mistral-medium-2604",
    "mistral-small-latest": "mistral-small-2603",
    "mistral-large-latest": "mistral-large-2512",
}

#: JJMM am Ende einer Modell-ID. Vier Ziffern, nicht mehr: ``-4-0`` bei
#: ``mistral-large-4-0`` ist keine Datierung.
_DATIERT = re.compile(r"-(\d{4})$")

#: Modelle, die Mistral laut Mail vom 30.10.2026 abschaltet, mit dem
#: Datum. Die API nennt es nicht (siehe Modul-Docstring), also steht es
#: hier — als Datum, nicht als Schwelle, damit der Waechter sagen kann,
#: wie lange noch.
ABKUENDIGUNGEN = {
    "mistral-medium-2604": "2027-04-30",
    "mistral-large-2512": "2027-04-30",
}


def namen_des_eintrags(eintrag: dict) -> list[str]:
    """ID und Aliase eines Katalog-Eintrags, ohne Leerstrings."""
    namen = [(eintrag or {}).get("id") or ""]
    namen += list((eintrag or {}).get("aliases") or [])
    return [n for n in namen if n]


def datierte_id(eintrag: dict) -> str:
    """Die datierte Geschwister-ID, oder "" wenn der Eintrag keine hat."""
    treffer = sorted(n for n in namen_des_eintrags(eintrag)
                     if _DATIERT.search(n))
    return treffer[0] if treffer else ""


def finde_eintrag(katalog: list, modell: str) -> dict | None:
    """Der Katalog-Eintrag, unter dessen ID ODER Aliasen ``modell`` steht.

    Nach Alias zu suchen ist Pflicht, nicht Bequemlichkeit: Unser
    konfigurierter Name ``mistral-medium-latest`` IST bei mehreren
    Eintraegen nur ein Alias.
    """
    if not modell:
        return None
    for eintrag in katalog or []:
        if not isinstance(eintrag, dict):
            continue
        if modell in namen_des_eintrags(eintrag):
            return eintrag
    return None


def pruefe(katalog: list, modell: str) -> dict:
    """Befund zu einem konfigurierten Modell.

    ``lage`` ist einer von:
      ``ok``            — vorhanden, datierte ID wie gepinnt
      ``verschwunden``  — nicht mehr im Katalog: der Dienst faellt aus
      ``getauscht``     — vorhanden, aber eine andere datierte ID
      ``ohne_pin``      — vorhanden und datiert, aber nichts hinterlegt
      ``nicht_pinnbar`` — vorhanden, aber ohne datierte ID (large-4)
    """
    eintrag = finde_eintrag(katalog, modell)
    if eintrag is None:
        return {"lage": "verschwunden", "modell": modell, "datiert": "",
                "erwartet": MODELL_PINS.get(modell, ""),
                "abschaltung": ""}
    datiert = datierte_id(eintrag)
    erwartet = MODELL_PINS.get(modell, "")
    befund = {"modell": modell, "datiert": datiert, "erwartet": erwartet,
              "abschaltung": ABKUENDIGUNGEN.get(datiert, ""),
              "kontext": eintrag.get("max_context_length"),
              "aliase": sorted(namen_des_eintrags(eintrag))}
    if not datiert:
        befund["lage"] = "nicht_pinnbar"
    elif not erwartet:
        befund["lage"] = "ohne_pin"
    elif datiert != erwartet:
        befund["lage"] = "getauscht"
    else:
        befund["lage"] = "ok"
    return befund


def nachricht(befund: dict) -> str:
    """Der Text, der in den Push geht.

    Eigene Funktion, damit jeder Alarmweg einen Test hat, der die
    NACHRICHT baut (#236) — ein Waechter, dessen Meldung nie gebaut
    wurde, ist ungetestet.
    """
    lage = (befund or {}).get("lage")
    modell = (befund or {}).get("modell") or "<kein Modell konfiguriert>"
    if lage == "verschwunden":
        return (f"Modell {modell} steht NICHT mehr im Mistral-Katalog. "
                f"Jeder Faktencheck scheitert, sobald der Alias nicht mehr "
                f"aufloest. Ersatz konfigurieren (MISTRAL_MODEL in .env).")
    if lage == "getauscht":
        return (f"Modell {modell} zeigt jetzt auf "
                f"{befund.get('datiert')} statt auf "
                f"{befund.get('erwartet')}. Das Modell wurde unter uns "
                f"getauscht — Verdicts und Waechter sind gegen die alte "
                f"Fassung gemessen. MODELL_PINS pruefen und neu messen.")
    if lage == "ohne_pin":
        return (f"Modell {modell} loest auf {befund.get('datiert')} auf, "
                f"aber in MODELL_PINS steht kein Eintrag. Ohne Pin kann "
                f"ein Tausch nicht auffallen.")
    if lage == "nicht_pinnbar":
        return (f"Modell {modell} ist vorhanden, hat aber keine datierte "
                f"ID — ein Tausch unter diesem Namen bleibt unsichtbar. "
                f"21 der 64 Modelle sind so, darunter mistral-large-4.")
    abschaltung = (befund or {}).get("abschaltung")
    if abschaltung:
        return (f"Modell {modell} ({befund.get('datiert')}) ist wie "
                f"gepinnt vorhanden. Abschaltung laut Mistral: "
                f"{abschaltung}.")
    return f"Modell {modell} ({befund.get('datiert')}) ist wie gepinnt da."
