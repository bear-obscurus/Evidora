#!/usr/bin/env python3
"""Taeglicher Waechter: Steht unser Antwort-Modell noch im Katalog?

Anlass: Mistral-Mail vom 30.10.2026. Prod faehrt
``mistral-medium-latest``, und das IST ``mistral-medium-3-5`` — dasselbe
Modell, datierte ID ``mistral-medium-2604``, Abschaltung 30.4.2027.

Drei Luecken, die beim Nachmessen aufgefallen sind:

1. Die API warnt NICHT. ``deprecation`` ist bei allen 64 Modellen None —
   auch bei ``zai-glm-5-2``, das am Tag nach der Messung abgeschaltet
   wurde. Die Mail war das einzige Signal.
2. ``_mistral_health`` ruft genau ``/v1/models`` auf und wirft die Liste
   WEG: Sie prueft nur den HTTP-Status, nie, ob unser Modell drinsteht.
3. Ein 404 fuer ein abgeschaltetes Modell ist nicht eigens behandelt —
   drei Retries, dann scheitert der Check. Ohne diesen Waechter faellt
   der Dienst am Abschalttag ohne Vorwarnung aus.

Exit-Codes nach dem Vertrag aus #236 — 1/2 heisst „Werkzeug lief, hier
ist ein Befund", alles andere heisst „kam nicht zum Laufen", und jeder
Ausgang hat seinen eigenen Code:

    0   alles wie gepinnt (auch: vorhanden, aber nicht pinnbar)
    1   BEFUND: datierte ID weicht vom Pin ab, oder kein Pin hinterlegt
    2   ALARM: konfiguriertes Modell steht nicht mehr im Katalog
    64  EX_USAGE: kein Key oder kein Modell konfiguriert
    75  EX_TEMPFAIL: Netz, 5xx, oder Key abgelehnt (auch Kontingent)

Nur 1 und 2 schicken einen Push. ``nicht_pinnbar`` bleibt eine
Log-Zeile: Ein Waechter, der bei jedem Schluckauf schreit, wird
abgeschaltet (#128).

Laeuft im Wartungs-Container (``run_evidora_tool.sh``), nicht im
servierenden — ein zweiter schwerer Prozess dort hat am 8.8. den Host
mitgerissen. Dieses Werkzeug ist allerdings ein einziger HTTP-GET.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

_HIER = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HIER))
sys.path.insert(0, _HIER)

from _kanal import kanal_kurz  # noqa: E402
from services._modell_katalog import nachricht, pruefe  # noqa: E402

MODELS_URL = "https://api.mistral.ai/v1/models"

EX_OK = 0
EX_BEFUND = 1
EX_ALARM = 2
EX_USAGE = 64
EX_TEMPFAIL = 75

#: Die Zusage an den Cron: DIESE Codes heissen „Werkzeug lief, hier ist
#: ein Befund". Alles andere ungleich 0 heisst „kam nicht zum Laufen" und
#: muss eigens pushen, weil das Werkzeug dann selbst nichts mehr melden
#: kann (#236). ``test_cron_aufrufe`` liest diese Zeile und prueft die
#: Cron-Zeile dagegen — das Gate hat die Zusage hier eingefordert, nicht
#: ich.
BEFUND_EXITS = (1, 2)

#: Welche Lage welchen Ausgang nimmt. Als Tabelle, damit der Test sie
#: Zeile fuer Zeile pruefen kann statt einer if-Kette zu folgen.
AUSGANG = {
    "ok": (EX_OK, False),
    "nicht_pinnbar": (EX_OK, False),
    "ohne_pin": (EX_BEFUND, True),
    "getauscht": (EX_BEFUND, True),
    "verschwunden": (EX_ALARM, True),
}


def hole_katalog(key: str, timeout: float) -> list:
    """Die Modell-Liste. Wirft bei allem, was nicht 200 ist."""
    req = urllib.request.Request(
        MODELS_URL, headers={"Authorization": f"Bearer {key}"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return (json.loads(resp.read().decode("utf-8")) or {}).get("data") or []


def _push(webhook: str, titel: str, text: str, dringend: bool) -> None:
    if not webhook:
        print("WARN: kein EVIDORA_ALERT_WEBHOOK gesetzt — kein Push",
              file=sys.stderr)
        return
    try:
        req = urllib.request.Request(
            webhook, data=text.encode("utf-8"),
            headers={"Title": titel,
                     "Priority": "urgent" if dringend else "high",
                     "Tags": "rotating_light"})
        urllib.request.urlopen(req, timeout=10)
        print(f"Alert gesendet an {kanal_kurz(webhook)}.")
    except Exception as e:  # noqa: BLE001
        print(f"WARN: Alert-Push fehlgeschlagen: {e}", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--modell", default=os.getenv("MISTRAL_MODEL", ""))
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--alert-webhook",
                    default=os.getenv("EVIDORA_ALERT_WEBHOOK", ""))
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    key = os.getenv("MISTRAL_API_KEY", "")
    if not key:
        print("MISTRAL_API_KEY nicht gesetzt — nichts zu pruefen.",
              file=sys.stderr)
        return EX_USAGE
    if not args.modell:
        # Stiller Rueckfall auf mistral-small-latest waere die Folge —
        # ein anderes Modell beantwortet dann JEDEN Claim, ohne dass es
        # jemand sieht. Das ist ein Konfigurationsfehler, kein Befund.
        print("MISTRAL_MODEL ist nicht gesetzt. Der Dienst faellt damit "
              "still auf mistral-small-latest zurueck — ein anderes "
              "Modell beantwortet jeden Claim.", file=sys.stderr)
        return EX_USAGE

    try:
        katalog = hole_katalog(key, args.timeout)
    except urllib.error.HTTPError as e:
        # 401 heisst bei Mistral nicht zwingend „Key ungueltig": Derselbe
        # unveraenderte Key lieferte nach einem Kontingent-Zustand zehn
        # Tage spaeter wieder 200 (QA50D). Also vorbeihehend, nicht Befund.
        print(f"HTTP {e.code} vom Katalog — voruebergehend.",
              file=sys.stderr)
        return EX_TEMPFAIL
    except Exception as e:  # noqa: BLE001
        print(f"{type(e).__name__} beim Katalog-Abruf — voruebergehend: {e}",
              file=sys.stderr)
        return EX_TEMPFAIL

    if not katalog:
        print("Katalog war leer — voruebergehend.", file=sys.stderr)
        return EX_TEMPFAIL

    befund = pruefe(katalog, args.modell)
    text = nachricht(befund)
    code, pushen = AUSGANG.get(befund["lage"], (EX_TEMPFAIL, False))

    if args.json:
        print(json.dumps({**befund, "nachricht": text, "exit": code},
                         ensure_ascii=False))
    else:
        print(f"Modelle im Katalog: {len(katalog)}")
        print(f"Lage: {befund['lage']}")
        print(text)

    if pushen:
        _push(args.alert_webhook,
              "Evidora Modell-Wachhund: " + (
                  "Modell verschwunden" if befund["lage"] == "verschwunden"
                  else "Modell-Befund"),
              text, dringend=befund["lage"] == "verschwunden")
    return code


if __name__ == "__main__":
    sys.exit(main())
