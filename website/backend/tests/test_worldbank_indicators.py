"""Contract-Test für World-Bank-Indikator-Codes (Audit 2026-07-07).

Der Multi-Connector-Audit fand 3 Indikatoren, die keine Daten mehr lieferten:
- EN.ATM.CO2E.PC (co2): in „WDI Database Archives" verschoben → HTTP-Message
  „indicator not found" → CO2-Topics bekamen nie Daten.
- SM.POP.REFG (Flüchtlinge): ebenfalls archiviert.
- GC.DOD.TOTL.GD.ZS (Staatsschulden): lebt, aber ohne AT/DE/EU-Werte (nur
  Nicht-EU) → AT/DE-Schulden-Claims still leer.

Ersetzt durch live-verifizierte Codes. Dieser Test verhindert einen
Rückfall (dependency-light: liest nur die INDICATOR_MAP, kein Netz).
"""
from services.worldbank import INDICATOR_MAP


def test_no_dead_or_empty_indicator_codes():
    dead = {
        "EN.ATM.CO2E.PC": "archiviert (source 57) → EN.GHG.CO2.PC.CE.AR5",
        "SM.POP.REFG": "archiviert → SM.POP.RHCR.EA (Aufnahmeland)",
        "GC.DOD.TOTL.GD.ZS": "keine AT/DE-Daten → DP.DOD.DLD2A.CR.GG.Z1",
    }
    used = set(INDICATOR_MAP.values())
    for code, why in dead.items():
        assert code not in used, f"toter Code '{code}' wieder in Nutzung ({why})"


def test_replacement_codes_present():
    used = set(INDICATOR_MAP.values())
    for code in ("EN.GHG.CO2.PC.CE.AR5", "SM.POP.RHCR.EA", "DP.DOD.DLD2A.CR.GG.Z1"):
        assert code in used, f"Ersatz-Code '{code}' fehlt in INDICATOR_MAP"
