"""Zwei Kopien derselben Zahl: demokratie_pack gegen den Freedom-House-Connector.

Befund (2026-09-05, direkt nach dem FIW-2026-Refresh #133): `demokratie_pack.json`
führt eine ZWEITE Kopie der Freedom-House-Werte — im Kernsatz, den der
Synthesizer wörtlich liest. Nach dem Refresh sagte der Connector „FIW 2026,
20. Jahr Rückgang, USA 81" und das Pack im selben Prompt „FIW 2024, 18. Jahr,
USA 83". Live nachweisbar: die Summary zu „Die USA haben laut Freedom House an
Freiheit verloren" nannte den korrekten Rückgang auf 81 **und** im nächsten Satz
den FIW 2024 mit 18 Jahren.

Schlimmer als die Veraltung: **AT und DE waren schon bei FIW 2024 falsch**
abgeschrieben (Pack 92/94, Connector 93/93). Die Kopie war also nie korrekt —
gemerkt hat es niemand, weil nichts sie gegen die Quelle hielt.

Dieser Test ist die Gegenmaßnahme: Jede Freedom-House-Zahl, die das Pack nennt,
muss dem Connector entsprechen. Der Connector ist die Quelle, das Pack zitiert
ihn nur als Kontext im Argument.

Bewusst NICHT geprüft: die V-Dem-Werte im selben Kernsatz. Die stammen aus
V-Dem v14 und haben ihre eigene Vintage — sie hier mitzuziehen hiesse zwei
Stände zu mischen (V-Dem-Lehre).
"""

import json
import re
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
FH = json.loads((BACKEND / "data" / "freedom_house.json").read_text(encoding="utf-8"))
PACK_ROH = (BACKEND / "data" / "demokratie_pack.json").read_text(encoding="utf-8")

KURZ_ZU_ISO3 = {"AT": "AUT", "DE": "DEU", "CH": "CHE", "USA": "USA",
                "HU": "HUN", "RU": "RUS", "CN": "CHN"}


def _fh_block() -> str:
    """Der Satz im Kernsatz, der Freedom-House-Zahlen nennt."""
    m = re.search(r"FREEDOM HOUSE FIW.*?(?= \(4\) )", PACK_ROH, re.S)
    assert m, "Freedom-House-Block im Kernsatz nicht gefunden"
    return m.group(0)


def _genannte_werte(text: str) -> dict[str, int]:
    """Nur das eindeutige Muster 'XX 94/100' — sonst fängt man Fremdzahlen."""
    return {k: int(v) for k, v in re.findall(
        r"\b(AT|DE|CH|USA|HU|RU|CN) (\d{1,3})/100\b", text)}


# --------------------------------------------------------------------------
# Jede genannte Zahl muss dem Connector entsprechen
# --------------------------------------------------------------------------

def test_kernsatz_nennt_die_werte_des_connectors():
    genannt = _genannte_werte(_fh_block())
    assert genannt, "keine Werte im Muster 'XX nn/100' gefunden"
    falsch = {k: (v, FH["ratings"][KURZ_ZU_ISO3[k]]["total_score"])
              for k, v in genannt.items()
              if v != FH["ratings"][KURZ_ZU_ISO3[k]]["total_score"]}
    assert not falsch, f"Pack vs. Connector (Pack, Connector): {falsch}"


def test_detailfeld_nennt_die_dach_werte_des_connectors():
    fakt = next(f for f in json.loads(PACK_ROH)["facts"]
                if "freedom_house_fiw" in (f.get("data") or {}))
    text = fakt["data"]["freedom_house_fiw"]
    for kurz, iso3 in (("AT", "AUT"), ("DE", "DEU"), ("CH", "CHE")):
        soll = FH["ratings"][iso3]["total_score"]
        assert re.search(rf"\b{kurz} {soll}\b", text), (
            f"{kurz} im Detailfeld weicht ab, Connector sagt {soll}: {text[:160]}")


def test_status_bezeichnungen_stimmen_mit_dem_connector():
    block = _fh_block()
    for kurz, iso3 in KURZ_ZU_ISO3.items():
        m = re.search(rf"\b{kurz} \d{{1,3}}/100 \(([^)]+)\)", block)
        if m:
            assert m.group(1) == FH["ratings"][iso3]["status"], (
                f"{kurz}: Pack sagt {m.group(1)!r}, "
                f"Connector {FH['ratings'][iso3]['status']!r}")


# --------------------------------------------------------------------------
# Ausgabe + globale Zahlen
# --------------------------------------------------------------------------

def test_pack_nennt_dieselbe_fiw_ausgabe():
    jahr = FH["report_year"]
    assert f"FIW {jahr}" in _fh_block(), (
        f"Pack nennt eine andere Ausgabe als der Connector (FIW {jahr})")
    for veraltet in ("FIW 2024", "FIW 2025", "18th consecutive", "18. Jahr"):
        assert veraltet not in PACK_ROH, f"{veraltet!r} steht noch im Pack"


def test_rueckgangs_jahre_stimmen_mit_dem_world_summary():
    jahre = FH["world_summary"]["global_decline_years"]
    assert re.search(rf"{jahre}(th|\.) consecutive year", _fh_block()), (
        f"Pack nennt nicht {jahre} Jahre Rückgang")


def test_klassifikations_zaehlung_stimmt_mit_dem_world_summary():
    fakt = next(f for f in json.loads(PACK_ROH)["facts"]
                if "freedom_house_fiw" in (f.get("data") or {}))
    text = fakt["data"]["freedom_house_fiw"]
    w = FH["world_summary"]
    assert f"Free {w['free_countries']} Länder" in text
    assert f"Partly Free {w['partly_free_countries']}" in text
    assert f"Not Free {w['not_free_countries']}" in text


def test_key_traegt_kein_jahr_mehr():
    """`freedom_house_fiw_2024` hätte beim nächsten Refresh weitergelogen."""
    assert "freedom_house_fiw_2024" not in PACK_ROH
    assert '"freedom_house_fiw"' in PACK_ROH


def test_quellenlabel_nennt_die_aktuelle_ausgabe():
    jahr = FH["report_year"]
    assert f"Freedom House Freedom in the World {jahr}" in PACK_ROH


# --------------------------------------------------------------------------
# Abgrenzung: V-Dem bleibt unangetastet
# --------------------------------------------------------------------------

def test_vdem_vintage_wurde_nicht_mitgezogen():
    """V-Dem v14 hat eine eigene Vintage — mitziehen hiesse Stände mischen."""
    assert "V-Dem" in PACK_ROH and "v14" in PACK_ROH
    assert "V-Dem Democracy Report 2024" in PACK_ROH
