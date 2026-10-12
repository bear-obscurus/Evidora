"""Beleg-Links: finden, was verrottet — und nicht wegwerfen, was lebt.

Anlass (2026-09-22): In zwei Tagen Prod-Betrieb verwarf die URL-Pruefung 17
Belege, alle mit HEAD 404. Einzeln nachgemessen, drei Ursachen:

    3x Our World in Data  HEAD 404, GET 200 — unser Fehler: die Pruefung
                          testete bei 404 nicht per GET nach
    2x WHO GHO            who.py baute eine URL-Form, die die WHO
                          abgeschafft hat
    5x Linkverfall        in UNSEREN Datenpaketen (IQS, OEBB, CDC,
                          KlimaTicket, VfGH)

Der woechentliche Link-Check sah keinen der fuenf Paket-Links: er pruefte
eine feste Liste von 21 Dateien. 61 weitere Datendateien mit 1.052 Links
wurden nie geprueft. Der Vollcheck aller kuratierten Links: 261 von 1.241
tot (21 %).

Keine Netzabfrage in diesen Tests.
"""

import asyncio
import importlib.util
import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

_spec = importlib.util.spec_from_file_location("check_urls", BACKEND / "tools" / "check_urls.py")
cu = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cu)

DATA = str(BACKEND / "data")


# --------------------------------------------------------------------------
# Abdeckung: keine feste Liste mehr
# --------------------------------------------------------------------------

def test_jede_datendatei_mit_links_wird_geprueft():
    """Die Klasse aus #147: eine Liste, die beim Wachsen nicht mitwaechst."""
    ausgenommen = set(cu.TIER_2_FILES) | set(cu.TIER_3_FILES)
    erwartet = sorted(p.name for p in Path(DATA).glob("*.json")
                      if p.name not in ausgenommen and cu.extract_urls(str(p)))
    assert cu.kuratierte_dateien(DATA) == erwartet


@pytest.mark.parametrize("datei", ["iqs_bildung.json", "substanzen_pack.json",
                                   "mobilitaet_pack.json", "demokratie_pack.json"])
def test_die_in_prod_betroffenen_dateien_sind_dabei(datei):
    assert datei in cu.kuratierte_dateien(DATA)


def test_keine_feste_tier1_liste_mehr():
    assert not hasattr(cu, "TIER_1_FILES")


# --------------------------------------------------------------------------
# Was „tot" heisst
# --------------------------------------------------------------------------

@pytest.mark.parametrize("r,tot", [
    ({"status": 404}, True),
    ({"status": 410}, True),
    ({"status": None, "error": "ConnectError: [Errno 8] nodename nor servname provided, or not known"}, True),
    ({"status": None, "error": "ConnectError: [Errno -2] Name or service not known"}, True),
    ({"status": 403}, False),           # Sperre, kein toter Link (#171)
    ({"status": 500}, False),
    ({"status": 503}, False),
    ({"status": None, "error": "ReadTimeout: "}, False),
    ({"status": 200}, False),
])
def test_tot_heisst_404_410_oder_erloschene_domain(r, tot):
    assert cu.ist_tot(r) is tot


def test_vergleich_mit_der_liste_bekannter_toter_links():
    neu, weiter, erledigt = cu.vergleiche({"a", "b"}, {"b": {}, "c": {}})
    assert (neu, weiter, erledigt) == (["a"], ["b"], ["c"])


# --------------------------------------------------------------------------
# Alarm nur bei NEU verrotteten Links
# --------------------------------------------------------------------------

def _lauf(monkeypatch, tmp_path, ergebnisse, bekannt, vorher=None):
    """Ein Lauf des Waechters. `vorher` ist der STAND des letzten Laufs —
    `None` heisst: es gab noch keinen, der Lauf legt die Basislinie an.

    Der Stand-Pfad zeigt bewusst in `tmp_path`. Vorher schrieb der Test in
    das ECHTE data/-Verzeichnis; dort lag nach dem ersten Testlauf eine
    Stand-Datei mit `https://alt.test/weg` darin, und der naechste Test sah
    sie. Ein Test, der Produktionsdaten anfasst, faellt frueher oder
    spaeter dem naechsten Test auf die Fuesse — hier nach zwei Minuten.
    """
    liste = tmp_path / "bekannt.json"
    liste.write_text(json.dumps({"tot": {u: {"dateien": ["x.json"], "seit": "2026-09-22"} for u in bekannt}}))
    stand = tmp_path / "stand.json"
    if vorher is not None:
        stand.write_text(json.dumps({"stand": "2026-10-05", "tot": vorher}))
    monkeypatch.setattr(cu, "collect_urls", lambda tier, d: {"tier1": {r["url"] for r in ergebnisse}})

    async def gepruefte(urls, concurrency=20):
        return ergebnisse
    monkeypatch.setattr(cu, "check_urls", gepruefte)
    gesendet = []
    monkeypatch.setattr(cu, "post_alert", lambda w, titel, text: gesendet.append((titel, text)))
    args = type("A", (), dict(data_dir=DATA, tier="1", live=0, out=None, backend="", api_key=None,
                              concurrency=4, bekannt=str(liste), schreibe_bekannt=False,
                              stand=str(stand),
                              alert_webhook="https://example.invalid"))()
    return asyncio.run(cu.main_async(args)), gesendet


def test_neu_verrotteter_link_alarmiert(monkeypatch, tmp_path):
    """Neu heisst: war beim letzten Lauf noch nicht tot."""
    rc, gesendet = _lauf(monkeypatch, tmp_path,
                         [{"url": "https://neu.test/weg", "status": 404}],
                         bekannt=[], vorher=[])
    assert rc == 2 and gesendet and "1 neu tote" in gesendet[0][0]


def test_derselbe_tote_link_alarmiert_nicht_zweimal(monkeypatch, tmp_path):
    """Der Kern des Umbaus (12.10.2026): der Waechter meldete jeden Montag
    dieselben 60+ Links als "neu", weil er gegen eine handgepflegte
    Parkliste verglich, die der Cron nie fortschreibt. 27 der 30
    angezeigten waren wortgleich die der Vorwoche."""
    rc, gesendet = _lauf(monkeypatch, tmp_path,
                         [{"url": "https://alt.test/weg", "status": 404}],
                         bekannt=[], vorher=["https://alt.test/weg"])
    assert rc == 0 and not gesendet


def test_erster_lauf_legt_die_basislinie_an(monkeypatch, tmp_path):
    """Ohne Stand-Datei waeren ALLE toten Links "neu" — ein Fehlalarm mit
    Ansage. Der erste Lauf schreibt nur."""
    rc, gesendet = _lauf(monkeypatch, tmp_path,
                         [{"url": "https://alt.test/weg", "status": 404}],
                         bekannt=[], vorher=None)
    assert rc == 0 and not gesendet
    assert json.loads((tmp_path / "stand.json").read_text())["tot"] == [
        "https://alt.test/weg"]


def test_stand_wird_fortgeschrieben(monkeypatch, tmp_path):
    """Sonst meldet der naechste Lauf denselben Link wieder als neu."""
    _lauf(monkeypatch, tmp_path,
          [{"url": "https://neu.test/weg", "status": 404}],
          bekannt=[], vorher=[])
    assert json.loads((tmp_path / "stand.json").read_text())["tot"] == [
        "https://neu.test/weg"]


def test_wieder_erreichbarer_link_alarmiert_nicht(monkeypatch, tmp_path):
    """Gute Nachrichten wecken niemanden."""
    rc, gesendet = _lauf(monkeypatch, tmp_path, [],
                         bekannt=[], vorher=["https://war.test/tot"])
    assert rc == 0 and not gesendet


def test_sperren_machen_den_lauf_nicht_rot(monkeypatch, tmp_path):
    """Frueher war der Exit-Code bei jedem Status >= 400 gesetzt — der
    Waechter war damit immer rot und wurde nie gelesen."""
    rc, gesendet = _lauf(monkeypatch, tmp_path,
                         [{"url": "https://gesperrt.test/", "status": 403},
                          {"url": "https://langsam.test/", "status": None, "error": "ReadTimeout"}],
                         bekannt=[])
    assert rc == 0 and not gesendet


# --------------------------------------------------------------------------
# Die reparierten Belege und die korrigierten Fakten
# --------------------------------------------------------------------------

def _text(datei):
    return (BACKEND / "data" / datei).read_text(encoding="utf-8")


@pytest.mark.parametrize("datei,tot", [
    ("iqs_bildung.json", "https://www.iqs.gv.at/themen/nationaler-bildungsbericht"),
    ("substanzen_pack.json", "https://www.cdc.gov/tobacco/basic_information/e-cigarettes/severe-lung-disease.html"),
    ("mobilitaet_pack.json", "https://presse.oebb.at/de/news/oebb-konzern-erfolgreichstes-jahr-2024"),
    ("demokratie_pack.json", "https://www.vfgh.gv.at/medien/Bundespraesidentenstichwahl_aufgehoben.de.php"),
])
def test_in_prod_verworfene_paket_links_sind_ersetzt(datei, tot):
    assert tot not in _text(datei)


def test_vfgh_aktenzahl_ist_die_der_entscheidung():
    """Die Stichwahl-Aufhebung ist W I 6/2016 (RIS JFT_20160701_16W_I00006_00),
    nicht „G203/2016". Bei einer Wahlentscheidung muss die Aktenzahl stimmen."""
    text = _text("demokratie_pack.json")
    assert "G203" not in text
    assert "W I 6/2016" in text


def test_vfgh_kein_zitat_das_die_entscheidung_nicht_enthaelt():
    """Das Pack zitierte den VfGH mit „kein konkreter Manipulations-Verdacht,
    aber Verfahrens-Verstoß" — das steht nicht in der Entscheidung. Jetzt
    steht dort der tatsaechliche Grundsatz."""
    text = _text("demokratie_pack.json")
    assert "kein konkreter Manipulations-Verdacht" not in text
    assert "jedenfalls auch ohne Nachweis einer konkreten Manipulation" in text
    assert "vor 17 Uhr" not in text, "der Fehler war das Oeffnen vor 9 Uhr am 23.5., nicht vor Wahlschluss"


def test_oebb_ueberschrift_stimmt_mit_dem_eigenen_faktentext():
    """Die Ueberschrift nannte ~95 % / 88-92 % / ~64 %, der Faktentext und die
    OEBB-Bilanz 2024 93,6 % / 78,2 %, die DB 62,5 %."""
    daten = json.loads(_text("mobilitaet_pack.json"))["facts"]
    seq = daten if isinstance(daten, list) else list(daten.values())
    f = next(x for x in seq if x.get("id") == "mobilitaet-oebb_puenktlichkeit_2026")
    for zahl in ("93,6 %", "78,2 %", "62,5 %"):
        assert zahl in f["headline"], (zahl, f["headline"])


def test_liste_bekannter_toter_links_ist_gueltig():
    liste = json.loads((BACKEND / "tools" / "url_bekannt_tot.json").read_text(encoding="utf-8"))
    assert liste["tot"], "Liste leer"
    for u, e in liste["tot"].items():
        assert u.startswith("http") and e.get("dateien") and e.get("seit"), u


# --------------------------------------------------------------------------
# Herkunft: was KANN jemand reparieren? (12.10.2026)
# --------------------------------------------------------------------------
# Gemessen am 12.10.2026: von 76 toten Links lagen 42 in mitre_attack.json
# und 14 in cordis_projects_slim.json — Fremdreferenzen in generierten
# Caches, die wir weder besitzen noch ersetzen koennen. Nur 20 standen in
# kuratierten Packs. Der alte Alarm warf alles in einen Topf, und die 20,
# an denen man haette arbeiten koennen, gingen in den 56 unter.

def test_fremdreferenz_wird_als_solche_erkannt():
    orte = {"https://x.test/a": ["mitre_attack.json"],
            "https://x.test/b": ["cordis_projects_slim.json"]}
    assert cu.aus_generiertem_cache("https://x.test/a", orte)
    assert cu.aus_generiertem_cache("https://x.test/b", orte)


def test_unser_beleg_bleibt_unserer():
    orte = {"https://x.test/c": ["sport_fitness_pack.json"]}
    assert not cu.aus_generiertem_cache("https://x.test/c", orte)


def test_dieselbe_url_in_beiden_gehoert_uns():
    """AUSSCHLIESSLICH im Cache, nicht irgendwo: steht die URL auch in
    einem kuratierten Pack, ist sie unser Beleg und unsere Aufgabe."""
    orte = {"https://x.test/d": ["mitre_attack.json", "recht_pack.json"]}
    assert not cu.aus_generiertem_cache("https://x.test/d", orte)


def test_unbekannte_herkunft_gilt_nicht_als_fremd():
    """Live aus dem Dienst gezogene URLs haben keinen Fundort in data/ —
    die als "nicht unser Problem" zu zaehlen waere die falsche Richtung."""
    assert not cu.aus_generiertem_cache("https://x.test/e", {})


def test_alarmtext_nennt_die_veraenderung_vor_dem_bestand():
    """Der Kern: im Titel und in der ersten Zeile steht, was sich geaendert
    hat — nicht, wie gross der Rueckstand ist."""
    orte = {"https://neu.test/a": ["sport_fitness_pack.json"],
            "https://alt.test/b": ["mitre_attack.json"]}
    text = cu.alarm_text(["https://neu.test/a"], [],
                         {"https://neu.test/a", "https://alt.test/b"},
                         orte, "2026-10-05")
    erste, zweite = text.splitlines()[:2]
    assert erste.startswith("1 neu tot seit 2026-10-05")
    assert "1 in Belegen, 0 in generierten Caches" in erste
    assert zweite.startswith("Bestand: 2 tot")
    assert "1 in Belegen, 1 in generierten Caches" in zweite


def test_alarmtext_bleibt_unter_dem_ntfy_limit():
    viele = [f"https://x.test/{i}" for i in range(500)]
    text = cu.alarm_text(viele, [], set(viele), {}, "2026-10-05")
    assert len(text) <= 3800


def test_pruefer_liest_seine_eigenen_arbeitsdateien_nicht(tmp_path):
    """Am 12.10.2026 habe ich `--out` versehentlich nach data/ zeigen
    lassen. `herkunft()` las die Ergebnisdatei danach ein, und der Pruefer
    fuehrte SICH SELBST als Fundort der toten Links auf. Ein Werkzeug, das
    in das Verzeichnis schreibt, das es liest, braucht diese Grenze."""
    (tmp_path / "echt_pack.json").write_text(
        json.dumps({"quelle": "https://echt.test/beleg"}), encoding="utf-8")
    (tmp_path / ".tmp-urlcheck.json").write_text(
        json.dumps({"url": "https://arbeitsdatei.test/x"}), encoding="utf-8")
    (tmp_path / "_wartung.json").write_text(
        json.dumps({"url": "https://wartung.test/x"}), encoding="utf-8")
    namen = cu.kuratierte_dateien(str(tmp_path))
    assert namen == ["echt_pack.json"]
