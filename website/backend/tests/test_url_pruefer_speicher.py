"""Der URL-Wächter las 108 MiB in einem Stück — und starb daran.

Gemessen am 19.10.2026, beim ersten Montags-Lauf unter der neuen
Cron-Mechanik aus #259:

    ALERT 2026-10-19T04:00:05 check_urls kam nicht zum Laufen (exit 137)
    Logdatei: nur "Container … Creating / Created", 108 Bytes

Exit 137 ist ein OOM-Kill. ``extract_urls`` machte ``f.read()`` auf jede
``data/*.json`` — und ``cordis_projects_slim.json`` ist 108 MiB gross. Im
Wartungs-Container mit 224 MB Limit (PR #249) reicht das nicht.

Das ist das VIERTE Werkzeug, das #249 gebrochen hat, nach dem
Kanarienvogel (#258), der CORDIS-Cron-Zeile (#258) und dem
Stichtags-Wächter (#259). Es lag eine Woche unsichtbar, weil die
check_urls-Cron-Zeile damals KEINEN Zweig hatte — der Zweig, den #259
ergänzt hat, hat es beim ersten Lauf gemeldet. Der Wächter über den
Wächtern hat funktioniert.

## Zeilenweise hätte nicht geholfen

Die Datei hat **null Zeilenumbrüche**: 113.929.377 Bytes in einer Zeile,
weil ``atomic_write_json_array`` aus #239 mit ``", "`` trennt. Eine
Umstellung auf ``for zeile in f`` wäre wirkungslos geblieben — nachgesehen,
nicht angenommen. Deshalb ein Schiebefenster.

## Und übersprungen wird sie auch nicht

Obwohl sie ein generierter Cache ist. ``aus_generiertem_cache`` (#243)
muss wissen, ob eine URL AUSSCHLIESSLICH dort steht, und das setzt voraus,
sie dort zu finden. Gemessen trägt sie 94 der 4.237 eindeutigen URLs —
wenig Ausbeute, aber sie entscheidet über die Herkunfts-Trennung von drei
Vierteln der toten Links.

## Zwei eigene Fehler beim Bauen

Die erste Fassung des Fensters nahm nur bis zur Annahme-Grenze mit und
**verlor 4 von 28.029 URLs**: Ein Treffer, der die Grenze überschreitet,
wird übersprungen — und begann vor ihr, also fehlte sein Anfang im
nächsten Fenster. Gefunden, weil ich gegen die alte Fassung byteweise
nachgerechnet habe statt zu schätzen.

Die erste Speicher-Messung lief gegen eine Testdatei mit 731.338
eindeutigen URLs und zeigte 100 MiB Spitze — dort dominiert das
Ergebnis-``set``, nicht das Fenster. Mit der echten Dichte (94 URLs in
108 MiB): 5 MiB gegen 216 MiB.

Dependency-light: nur Dateien, kein Netz, kein Docker.
"""
import os
import sys
import tracemalloc

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOLS = os.path.join(BACKEND, "tools")
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, TOOLS)

import check_urls as cu  # noqa: E402
from _generiert import GENERIERTE_CACHES  # noqa: E402


def _extract_in_einem_stueck(pfad):
    """Die Fassung VOR dem 19.10.2026 — als Vergleichsmassstab."""
    with open(pfad, encoding="utf-8") as f:
        text = f.read()
    return {cu.clean_url(u) for u in cu.URL_RE.findall(text) if cu.clean_url(u)}


def _eine_zeile(pfad, mib, url_anzahl):
    """Eine Datei OHNE Zeilenumbrüche, wie der CORDIS-Cache."""
    fuell = '{"id": %d, "t": "' + ("Lorem ipsum dolor sit amet " * 20) + '"}, '
    ziel = mib * 1024 * 1024
    with open(pfad, "w", encoding="utf-8") as f:
        f.write("[")
        n = 0
        je = max(1, ziel // (url_anzahl * len(fuell) // 1 or 1))
        while f.tell() < ziel:
            if n % je == 0 and n // je < url_anzahl:
                f.write('{"u": "https://example.org/p/%d"}, ' % (n // je))
            f.write(fuell % n)
            n += 1
        f.write("]")


# --- Gleiches Ergebnis wie vorher --------------------------------------

def test_dieselben_urls_wie_in_einem_stueck():
    """Über ALLE echten Datendateien, nicht über eine Stichprobe.

    Genau diese Rechnung hat den Fehler der ersten Fassung gefunden:
    28.029 gegen 28.025.
    """
    alt = neu = dateien = 0
    unterschiede = []
    for name in sorted(os.listdir(DATA)):
        if not name.endswith(".json") or name.startswith((".", "_")):
            continue
        pfad = os.path.join(DATA, name)
        a = _extract_in_einem_stueck(pfad)
        n = cu.extract_urls(pfad)
        alt += len(a)
        neu += len(n)
        dateien += 1
        if a != n:
            unterschiede.append((name, sorted(a - n)[:3], sorted(n - a)[:3]))
    # Die Plausibilitaets-Schranke darf KEINE umgebungsabhaengige Zahl
    # sein. Eine erste Fassung forderte > 20.000 — das war meine lokale
    # Summe (28.029) und fiel in CI auf 17.147 um. Die Differenz sind
    # genau die 10.882 URLs aus ``claimreview_index.json``, einem
    # generierten und darum nicht eingecheckten Cache. Geprueft wird
    # deshalb gegen die eingecheckten Dateien mit Abstand nach unten;
    # der Zweck der Schranke ist allein, ein leeres data/ zu erkennen.
    assert dateien > 50, f"nur {dateien} Dateien gelesen"
    assert alt > 10000, f"nur {alt} URLs gelesen — liest der Test ins Leere?"
    assert not unterschiede, unterschiede
    assert alt == neu


def test_ein_treffer_ueber_der_fenstergrenze_geht_nicht_verloren(tmp_path):
    """Der konkrete Fehler der ersten Fassung, isoliert.

    Die URL wird so plaziert, dass sie genau auf der Annahme-Grenze des
    ersten Fensters liegt.
    """
    url = "https://example.org/" + "a" * 60
    # Das Anfuehrungszeichen ist der Terminator — URL_RE ist
    # [^\s"<>\)]+ und frisst sonst die Fuellung mit. Eine erste Fassung
    # dieses Tests polsterte mit "y" und meldete deshalb die Fuellung als
    # Teil der URL; der Fehler lag im Fixture, nicht im Fenster.
    for versatz in range(-80, 81, 7):
        stelle = cu.LESEFENSTER - cu.UEBERLAPP + versatz
        pfad = tmp_path / f"f{versatz}.json"
        pfad.write_text('x' * max(0, stelle) + url + '" ' + 'y' * 200,
                        encoding="utf-8")
        assert url in cu.extract_urls(str(pfad)), f"verloren bei {versatz:+d}"


def test_kein_stumpf_im_ergebnis(tmp_path):
    """Die Gegenprobe: eine am Fensterrand abgeschnittene URL darf nicht
    als eigener Eintrag auftauchen. Der ``set`` fängt Doppelte, aber
    keine Stümpfe."""
    url = "https://example.org/" + "b" * 60
    pfad = tmp_path / "rand.json"
    pfad.write_text('x' * (cu.LESEFENSTER - 30) + url + '" ' + 'y' * 200,
                    encoding="utf-8")
    gefunden = cu.extract_urls(str(pfad))
    assert gefunden == {url}, gefunden


# --- Der Speicher ------------------------------------------------------

def test_eine_grosse_einzeilige_datei_sprengt_den_speicher_nicht(tmp_path):
    """Mit der ECHTEN URL-Dichte, nicht mit einer gefälligen.

    Eine erste Messung nahm eine Testdatei mit 731.338 eindeutigen URLs
    und zeigte 100 MiB — dort dominiert das Ergebnis-``set``. Der echte
    Cache trägt 94 URLs in 108 MiB.
    """
    pfad = tmp_path / "gross.json"
    _eine_zeile(str(pfad), mib=12, url_anzahl=10)
    assert sum(1 for _ in open(pfad, encoding="utf-8")) == 1, (
        "Testdatei hat Zeilenumbrüche — dann prüft sie den Fall nicht")

    tracemalloc.start()
    neu = cu.extract_urls(str(pfad))
    _a, spitze_neu = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    tracemalloc.start()
    alt = _extract_in_einem_stueck(str(pfad))
    _a, spitze_alt = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert neu == alt, "andere URLs als die alte Fassung"
    dateigroesse = os.path.getsize(pfad)
    assert spitze_neu < dateigroesse / 2, (
        f"Spitze {spitze_neu} bei Datei {dateigroesse} — das Fenster "
        f"greift nicht")
    assert spitze_alt > spitze_neu * 3, (
        f"Die alte Fassung braucht nur {spitze_alt} gegen {spitze_neu} — "
        f"dann beweist dieser Test den Gewinn nicht")


def test_das_fenster_ist_groesser_als_jede_url():
    """Die Überlappung muss jede reale URL fassen, sonst wird eine lange
    URL am Rand systematisch zerschnitten."""
    assert cu.UEBERLAPP >= 2048
    assert cu.LESEFENSTER > cu.UEBERLAPP * 2


# --- Der generierte Cache bleibt in der Prüfung -----------------------

def test_der_cordis_cache_wird_nicht_uebersprungen():
    """Anders als beim Stichtags-Wächter (#259), der ihn überspringt.

    Dort geht es um kuratierte FAKTEN, hier um die HERKUNFT von Links:
    ``aus_generiertem_cache`` muss wissen, ob eine URL ausschliesslich im
    Cache steht. Wer ihn hier überspringt, schickt drei Viertel der toten
    Links auf die Reparaturliste.
    """
    assert "cordis_projects_slim.json" in GENERIERTE_CACHES
    quelle = open(os.path.join(TOOLS, "check_urls.py"), encoding="utf-8").read()
    i = quelle.index("def kuratierte_dateien")
    j = quelle.index("def herkunft")
    assert "ueberspringen" not in quelle[i:j], (
        "kuratierte_dateien überspringt jetzt Dateien — dann prüfe, ob "
        "aus_generiertem_cache noch funktioniert")


def test_die_herkunfts_trennung_braucht_den_cache():
    """Pinnt die Begründung aus #243, damit sie beim nächsten
    Speicher-Problem nicht wegoptimiert wird."""
    quelle = open(os.path.join(TOOLS, "check_urls.py"), encoding="utf-8").read()
    assert "def aus_generiertem_cache" in quelle
    assert "AUSSCHLIESSLICH" in quelle


# --- Der Cron hat es gemeldet -----------------------------------------

def test_die_cron_zeile_meldet_einen_ausfall_von_check_urls():
    """Der Grund, warum dieser Fall überhaupt sichtbar wurde.

    Vor #259 hatte die Zeile keinen Zweig: ein OOM-Kill wäre weiterhin
    unbemerkt geblieben, wie eine Woche lang zuvor.
    """
    pfad = os.path.join(os.path.dirname(BACKEND), "crontab.prod")
    with open(pfad, encoding="utf-8") as fh:
        zeilen = [z for z in fh if "check_urls.py" in z
                  and not z.lstrip().startswith("#")]
    assert len(zeilen) == 1, zeilen
    zeile = zeilen[0]
    assert "rc=$?" in zeile
    assert "kam nicht zum Laufen" in zeile
    assert "curl" in zeile.split("elif")[-1], "der Ausfall pusht nicht"
