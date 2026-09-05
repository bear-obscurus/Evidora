"""EUvsDisinfo: Archiv-Fälle und aktuelle Feed-Artikel sahen im Prompt gleich aus.

Befund (2026-09-05, Klärung des ältesten Freshness-Fundes): `euvsdisinfo_db.json`
ist 6,3 MB gross und stand mit 153 Tagen ganz oben auf der Alarmliste. Die Frage
war, ob der Dump überhaupt Refresh braucht — daneben läuft ein stündlicher
RSS-Feed.

**Antwort: er ist echt veraltet, aber nicht refreshbar.**

* Die Datei enthält 14.495 Fälle vom 06.01.2015 bis 22.11.2022.
* Die offizielle EUvsDisinfo-Datenbank weist heute **19.758 Fälle** aus — es
  fehlen rund 5.263 (26 %), fast vier Jahre.
* Die Quelle ist ein Spiegel von `erosalie/euvsdisinfo`; letzter Push dort
  **30.04.2023**, seither tot.
* Es gibt keinen Ersatz: `cknabs/EUvsDisinfo` endet noch früher (Anfang 2021,
  10.892 Einträge) und trägt keine Lizenz; `joaoaleite/euvsdisinfo` ist nur der
  Scraper ohne Daten. euvsdisinfo.eu bietet keinen Export, die WordPress-API
  antwortet mit HTTP 403, und data.europa.eu verlinkt lediglich HTML-Seiten
  zurück auf die Website.

Der behebbare Teil ist deshalb nicht die Aktualität, sondern die
**Kennzeichnung**: Ein Fall von 2016 und ein Threat Report von letzter Woche
kamen mit derselben Quelle, derselben Feldform und ohne Zeitraum-Angabe im
Prompt an. Jetzt trägt jeder Datenbank-Treffer sein Abdeckungsfenster —
bestimmt AUS DEN DATEN, nicht verdrahtet.
"""

import ast
import json
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

import services.euvsdisinfo as evd  # noqa: E402

QUELLE = (BACKEND / "services" / "euvsdisinfo.py").read_text(encoding="utf-8")


def _nur_code(quelle: str) -> str:
    baum = ast.parse(quelle)
    code = quelle
    for k in ast.walk(baum):
        if isinstance(k, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            doc = ast.get_docstring(k, clean=False)
            if doc:
                code = code.replace(doc, "")
    return "\n".join(z for z in code.splitlines() if not z.lstrip().startswith("#"))


CODE = _nur_code(QUELLE)
MAX_STR = 400


# --------------------------------------------------------------------------
# Das Fenster kommt aus den Daten
# --------------------------------------------------------------------------

def test_abdeckung_wird_aus_den_daten_bestimmt():
    assert evd._abdeckung_bestimmen([
        {"date": "06.01.2015"}, {"date": "22.11.2022"}, {"date": "01.07.2019"},
    ]) == ("01/2015", "11/2022")


def test_abdeckung_ignoriert_kaputte_datumsangaben():
    assert evd._abdeckung_bestimmen(
        [{"date": ""}, {"date": "2019-07-01"}, {"date": "01.07.2019"}]
    ) == ("07/2019", "07/2019")
    assert evd._abdeckung_bestimmen([{"date": "kaputt"}]) is None
    assert evd._abdeckung_bestimmen([]) is None


def test_kein_zeitraum_im_code_verdrahtet():
    """Ein verdrahtetes Fenster behauptet irgendwann etwas, das der Datensatz
    nicht hergibt — dieselbe Falle wie in wifo_ihs (#131) und rki (#138)."""
    for verdrahtet in ("2015", "2022", "01/2015", "11/2022", "14495", "14.495"):
        assert verdrahtet not in CODE, f"{verdrahtet!r} steht im Code"
    assert "_abdeckung_bestimmen(" in CODE


def test_hinweis_ohne_geladene_daten_faellt_weich():
    alt = evd._db_abdeckung
    try:
        evd._db_abdeckung = None
        assert evd._archiv_hinweis() == "EUvsDisinfo-Fallarchiv"
    finally:
        evd._db_abdeckung = alt


# --------------------------------------------------------------------------
# Der echte Datensatz
# --------------------------------------------------------------------------

def test_datensatz_wird_geladen_und_traegt_sein_fenster():
    evd._load_db()
    assert evd._db_index and len(evd._db_index) > 14000
    assert evd._db_abdeckung == ("01/2015", "11/2022")


def test_jeder_datenbank_treffer_traegt_das_fenster():
    evd._load_db()
    treffer = evd._keyword_match_db(["Ukraine", "Krim"], top_k=3)
    assert treffer, "Kontroll-Suche liefert nichts"
    for t in treffer:
        beschreibung = f"[{evd._archiv_hinweis()}] {t['claim'][:300]}"
        assert beschreibung.startswith("[EUvsDisinfo-Fallarchiv 01/2015–11/2022]")
        assert len(beschreibung) <= MAX_STR, f"{len(beschreibung)} Zeichen"


def test_hinweis_verdraengt_den_fall_text_nicht():
    """Der Hinweis teilt sich das Prompt-Budget mit dem Inhalt. Ein erster
    Entwurf war 220 Zeichen lang und hätte den Fall-Text hinausgedrückt."""
    assert len(evd._archiv_hinweis()) < 60


def test_source_literal_bleibt_unveraendert():
    """Marker- und Dispatch-Logik hängen an 'EUvsDisinfo' — das Literal zu
    ändern wäre Marker-Drift (PR #74)."""
    assert '"source": "EUvsDisinfo"' in CODE


def test_feed_treffer_bekommen_keinen_archiv_hinweis():
    """Der RSS-Feed ist aktuell — ihn als Archiv zu kennzeichnen wäre falsch."""
    # Auf CODE ankern, nicht auf Kommentare — die stript _nur_code weg.
    stelle = CODE[CODE.index("for item in (rss_matched"):
                  CODE.index("for entry in db_matched")]
    assert "_archiv_hinweis" not in stelle
