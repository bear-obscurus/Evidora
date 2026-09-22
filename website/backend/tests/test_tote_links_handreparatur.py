"""Tote Beleg-Links von Hand repariert — und der Rest begruendet geparkt.

Anlass (2026-09-22): Der Vollcheck (#182) fand 274 tote Links in den
kuratierten Datenpaketen. Die Wayback Machine war nicht erreichbar (CDX 503,
Availability-API leer), also Handarbeit: jeder Ersatz einzeln gesucht, per
Status + Titel geprueft und nur eingetragen, wenn er dieselbe Aussage belegt.

    244 ersetzt   Nachfolgeseite, dieselbe Studie unter neuer Adresse oder
                  die im Fakt zitierte Primaerquelle (Urteil, DOI, Bericht)
     30 geparkt   kein gleichwertiger Nachfolger — oder der Fakt selbst
                  widerspricht der auffindbaren Quelle ("INHALT:")

Beim Suchen fiel auf: ein Teil der toten Links war nie echt (jcr:fixme,
Hex-Wiederholungen, Platzhalter-UUIDs). Solche Muster sind nur noch
erlaubt, solange sie begruendet in der Liste bekannter toter Links stehen.

Keine Netzabfrage in diesen Tests.
"""

import importlib.util
import json
import re
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
DATA = BACKEND / "data"

_spec = importlib.util.spec_from_file_location("check_urls", BACKEND / "tools" / "check_urls.py")
cu = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cu)

ERSETZT = json.loads((BACKEND / "tests" / "fixtures" / "reparierte_tote_links.json")
                     .read_text(encoding="utf-8"))["ersetzt"]
BEKANNT = json.loads((BACKEND / "tools" / "url_bekannt_tot.json").read_text(encoding="utf-8"))["tot"]

# Ganze URL, kein Praefix einer laengeren (".../sekten.at/" vs ".../sekten.at/cgi-bin/...").
_ENDE = r"(?![A-Za-z0-9_\-/~%?=&+#:]|\.[A-Za-z0-9])"


def _texte():
    dateien = (sorted(DATA.glob("*.json")) + sorted((BACKEND / "services").glob("*.py"))
               + [BACKEND / "tools" / "refresh_drug_data.py"])
    return {f.name: f.read_text(encoding="utf-8") for f in dateien}


TEXTE = _texte()


def _vorkommen(url: str) -> list[str]:
    m = re.compile(re.escape(url) + _ENDE)
    return [name for name, t in TEXTE.items() if m.search(t)]


def _fakt(datei: str, fakt_id: str) -> dict:
    def such(o):
        if isinstance(o, dict):
            if o.get("id") == fakt_id:
                return o
            for v in o.values():
                if (r := such(v)) is not None:
                    return r
        elif isinstance(o, list):
            for v in o:
                if (r := such(v)) is not None:
                    return r
        return None
    f = such(json.loads((DATA / datei).read_text(encoding="utf-8")))
    assert f is not None, (datei, fakt_id)
    return f


# --------------------------------------------------------------------------
# Die ersetzten Links kommen nicht zurueck
# --------------------------------------------------------------------------

def test_es_wurden_244_links_ersetzt():
    assert len(ERSETZT) == 244


@pytest.mark.parametrize("tot", sorted(ERSETZT))
def test_ersetzter_toter_link_steht_nirgends_mehr(tot):
    assert not _vorkommen(tot), tot


def test_jeder_ersatz_steht_in_den_daten_oder_im_code():
    fehlt = [neu for neu in set(ERSETZT.values()) if not _vorkommen(neu)]
    assert not fehlt, fehlt


def test_kein_ersatz_ist_selbst_als_tot_bekannt():
    assert not set(ERSETZT.values()) & set(BEKANNT)


def test_kein_ersatz_ist_ein_ersetzter_toter_link():
    assert not set(ERSETZT.values()) & set(ERSETZT)


# --------------------------------------------------------------------------
# Liste bekannter toter Links: nur noch begruendet Geparktes
# --------------------------------------------------------------------------

def test_liste_enthaelt_keinen_ersetzten_link():
    assert not set(ERSETZT) & set(BEKANNT)


def test_jeder_geparkte_link_hat_einen_grund():
    ohne = [u for u, e in BEKANNT.items() if not (e.get("grund") or "").strip()]
    assert not ohne, ohne


def test_jeder_geparkte_link_steht_noch_in_den_daten():
    """Sonst gehoert er aus der Liste — sie soll den Ist-Stand spiegeln."""
    weg = [u for u in BEKANNT if not _vorkommen(u)]
    assert not weg, weg


def test_inhaltskonflikte_sind_als_solche_markiert():
    inhalt = [u for u, e in BEKANNT.items() if e["grund"].startswith("INHALT:")]
    # u. a. Familiennachzug (seit 2025 ausgesetzt), Eurobarometer 67 % vs. 60-63 %,
    # EFSA-Probenzahlen, Agrarfoerderung "= 7 Mrd", Femizide 31 vs. 26, UBA 8,0 vs. 8,4 Mt
    assert len(inhalt) >= 15
    for teil in ("familiennachzug-2024", "themen/oea/eurobarometer", "efsajournal/pub/8957",
                 "jcr:fixme", "oeffentliche_sicherheit_02_2024", "khg-bilanz"):
        assert any(teil in u for u in inhalt), teil


def test_schreibe_bekannt_behaelt_den_grund():
    bekannt = {"https://a.test/x": {"dateien": ["alt.json"], "seit": "2026-09-01",
                                    "grund": "kein Nachfolger"}}
    neu = cu.bekannt_eintraege({"https://a.test/x", "https://b.test/y"}, bekannt,
                               {"https://a.test/x": ["neu.json"]}, "2026-09-22")
    assert neu["https://a.test/x"] == {"dateien": ["neu.json"], "seit": "2026-09-01",
                                       "grund": "kein Nachfolger"}
    assert neu["https://b.test/y"] == {"dateien": ["live"], "seit": "2026-09-22"}


# --------------------------------------------------------------------------
# Erfundene Links: nur noch, wenn begruendet geparkt
# --------------------------------------------------------------------------

_ERFUNDEN = [
    re.compile(r"jcr:fixme", re.I),
    re.compile(r"(?:b7c8){3,}", re.I),                      # dgppn .../d8c8b7c8b7c8...
    re.compile(r"1234-11ee-bcde"),                          # dgb ++co++8a8c5c8c-1234-...
    re.compile(r"5A5B5C5D5E5F"),                            # cambridge.org .../96BD3C05A05B5A5B5C5D5E5F...
]


def test_erfundene_url_muster_nur_in_geparkten_links():
    treffer = []
    for f in sorted(DATA.glob("*.json")):
        for u in cu.extract_urls(str(f)):
            if any(m.search(u) for m in _ERFUNDEN) and u not in BEKANNT:
                treffer.append((f.name, u))
    assert not treffer, treffer


# --------------------------------------------------------------------------
# Stichproben: der Ersatz belegt dieselbe Aussage wie der Fakt
# --------------------------------------------------------------------------

def test_drohnen_fakt_verlinkt_das_zitierte_bverwg_urteil():
    f = _fakt("sicherheitspolitik_pack.json", "drohnen_krieg_empirie_2026")
    assert "6 C 7.19" in f["headline"]
    assert f["secondary_url"] == "https://www.bverwg.de/251120U6C7.19.0"


def test_bestandsdaten_fakt_verlinkt_den_zitierten_bverfg_beschluss():
    f = _fakt("datenschutz_pack.json", "bestandsdaten_auskunft_konsens_2026")
    assert "1 BvR 1873/13" in f["headline"]
    links = {f.get("source_url"), f.get("secondary_url")}
    assert any(u and "rs20200527_1bvr187313" in u for u in links), links


def test_oesterreichischer_bundesrat_verlinkt_nicht_den_deutschen():
    f = _fakt("demokratie_pack.json", "at_bundesrat_funktion_2026")
    links = [f.get("source_url"), f.get("secondary_url")]
    assert not any(u and "bundesrat.de" in u for u in links), links
    assert any(u and u.startswith("https://www.parlament.gv.at/") for u in links), links


def test_hoerverlust_kosten_verlinken_den_world_report_on_hearing():
    f = _fakt("who_hearing.json", "cost_of_inaction_2026")
    assert "980" in f["headline"]
    assert "https://www.who.int/publications/i/item/9789240020481" in json.dumps(f)


def test_pflegende_angehoerige_verlinken_die_zitierte_studie():
    f = _fakt("sozialstaat_pack.json", "pflegende_angehoerige_2026")
    assert "947.000" in f["headline"]
    assert "broschuerenservice.sozialministerium.gv.at/Home/Download?publicationId=664" in json.dumps(f)


def test_widerspruechliche_quelle_wurde_nicht_untergeschoben():
    """Familiennachzug: die BAMF-Nachfolgeseite beschreibt die Aussetzung seit
    2025 — der Fakt behauptet 1.000/Monat als geltend. Also kein Ersatz,
    sondern geparkt, bis der Fakt korrigiert ist."""
    f = _fakt("migration_pack.json", "migration_familiennachzug_2026")
    assert "bamf.de/SharedDocs/Meldungen/DE/2024/familiennachzug-2024" in json.dumps(f)
    tot = next(u for u in BEKANNT if "familiennachzug-2024" in u)
    assert BEKANNT[tot]["grund"].startswith("INHALT:")
