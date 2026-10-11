"""Die Wächter, die sich selbst nicht geprüft haben (2026-10-11).

Drei Befunde aus einer Durchsicht der Cron-Logs, nach zwölf unbeobachteten
Tagen:

  1. **Der Freshness-Push wurde gar nicht gebaut.** #139 (2026-09-05)
     erweiterte `stale_files` von 2- auf 5-Tupel. Die Zeile, die daraus den
     ntfy-Body baut, blieb bei zwei Namen und warf
     `too many values to unpack (expected 2)`. Der Fehler landete im
     `except` und erschien als eine Zeile unter achtzig Zeilen Tabelle —
     fünf Montagsalarme ohne Push, während `--strict` brav Exit 1 lieferte
     und der Cron „data_freshness FAILED" ins ALERTS.log schrieb. Es gab
     Tests für die Klassifikation, für die Kadenz und für die
     Cache-Gesundheit; **keiner baute den Body.**

  2. **Die CORDIS-Sonde wog die Hülle, nicht den Inhalt.** `_p_bytes` lud
     täglich 141 MB und meldete OK, solange das ZIP ankam. Gebrochen ist
     zweimal der Name der Datei DARIN (2026-07: eine Datei pro Projekt
     statt einer `project.json`) — beide Male lief der Refresh still auf
     0 Records, während die Sonde grün blieb.

  3. **Der Cron konnte Befund und Fehlstart nicht unterscheiden.** Das Tool
     gab 1 zurück, und 1 vergibt Python auch für jeden Absturz. Der Cron
     schrieb bei jedem Nicht-Null „kam nicht zum Laufen" — 18 Tage lang,
     während das Tool tadellos lief und DOAB als tot meldete.

Kein Test hier geht ins Netz.
"""

import importlib.util
import io
import json
import sys
import zipfile
from datetime import date
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services.cordis import _records_from_zip  # noqa: E402


def _modul(name):
    spec = importlib.util.spec_from_file_location(
        name, BACKEND / "tools" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


DF = _modul("data_freshness_check")
QM = _modul("quellen_lebenszeichen")

# Genau die Form, die `main()` erzeugt: (name, alter, kadenz, grenze, mass).
ECHTE_TUPEL = [
    ("parlgov.json", 140, None, 120, "fetched_at_iso"),
    ("rki_surveillance.json", 30, "woechentlich", 21, "fetched_at_iso"),
    ("wcag22.json", 140, None, 120, "Datei"),
]


# --------------------------------------------------------------------------
# 1. Der Push-Body
# --------------------------------------------------------------------------

def test_alert_body_baut_den_befund_aus_dem_echten_tupel():
    """Der Test, der fünf Wochen gefehlt hat."""
    body = DF.alert_body(date(2026, 10, 12), [], ECHTE_TUPEL)
    assert "parlgov.json" in body
    assert "140 d" in body and "Schwelle 120 d" in body


def test_alert_body_nennt_das_mass_das_entschieden_hat():
    """„140 d" allein lässt offen, ob die Datei oder das Feld alt ist —
    dieselbe Verwechslung, die #130 als 61 % Fehlalarm entlarvt hat."""
    body = DF.alert_body(date(2026, 10, 12), [], ECHTE_TUPEL)
    assert "fetched_at_iso 140 d" in body
    assert "Datei 140 d" in body
    assert "woechentlich" in body, "der Takt erklärt die enge Schwelle"


def test_alert_body_nennt_cache_probleme_zuerst():
    body = DF.alert_body(
        date(2026, 10, 12),
        ["cordis_projects_slim.json: 101 d alt (max 100)"], ECHTE_TUPEL)
    assert body.splitlines()[1].startswith("Cache: cordis_projects_slim.json")


def test_alert_body_bleibt_unter_dem_ntfy_limit():
    viele = [(f"datei_{i}.json", 200, "jaehrlich", 120, "Datei")
             for i in range(400)]
    assert len(DF.alert_body(date(2026, 10, 12), [], viele)) <= 3800


def test_gift_probe_die_alte_entpackung_wirft_auf_dieser_form():
    """Selbstprüfung der Sonde: hätte der Test oben den Fehler gefunden?

    Nur wenn die alte Schreibweise an genau diesen Tupeln scheitert, prüft
    `test_alert_body_baut_den_befund_aus_dem_echten_tupel` etwas. Sonst
    bestätigt er bloß, dass irgendein Code läuft.
    """
    with pytest.raises(ValueError, match="too many values to unpack"):
        [f"Stale: {n} ({a} d)" for n, a in ECHTE_TUPEL]


def test_klemmender_kanal_wird_als_befund_gemeldet(capsys, monkeypatch):
    """Ein Kanal, der klemmt, heißt: der Befund ist NICHT angekommen."""
    class _Httpx:
        @staticmethod
        def post(*a, **k):
            raise RuntimeError("connection refused")

    monkeypatch.setitem(sys.modules, "httpx", _Httpx)
    assert DF.sende_alert("https://ntfy.test/x", "egal") is False
    ausgabe = capsys.readouterr().out
    assert "ALERT-KANAL DEFEKT" in ausgabe
    assert "NICHT raus" in ausgabe


# --------------------------------------------------------------------------
# 2. Die CORDIS-Sonde — erreichbar ist nicht brauchbar
# --------------------------------------------------------------------------

_REC = {"id": "101069359", "acronym": "SolDAC", "title": "Full spectrum",
        "status": "SIGNED"}


def _zip_bytes(namen: list[str]) -> bytes:
    """Baut ein Archiv in den echten Formen: `project.json` traegt ein
    Array, die Einzeldateien je ein Objekt. Mit einem Objekt in
    `project.json` weist der Parser zu Recht ab — das hat dieser Test beim
    ersten Lauf gezeigt."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for n in namen:
            rumpf = [_REC] if n == "project.json" else _REC
            z.writestr(n, json.dumps(rumpf))
    return buf.getvalue()


def _wie_die_sonde_liest(rohdaten: bytes) -> str:
    """Die Sonde dekodiert mit errors='replace' — Membernamen sind ASCII
    und überleben das."""
    return rohdaten.decode("utf-8", "replace")


LAYOUTS = {
    "legacy": ["project.json"],
    "pro_projekt": ["project-rcn-290034_en.json", "project-rcn-290036_en.json"],
    "fremd": ["organization.json", "euroSciVoc.json"],
}


@pytest.mark.parametrize("layout", ["legacy", "pro_projekt"])
def test_cordis_sonde_erkennt_beide_bekannten_layouts(layout):
    sonde = next(s for s in QM.SONDEN if s[0] == "CORDIS")
    assert sonde[4](_wie_die_sonde_liest(_zip_bytes(LAYOUTS[layout]))) > 0


def test_gift_probe_cordis_sonde_wird_rot_bei_fremdem_layout():
    """Ohne diese Gegenprobe misst die Sonde nichts: eine Funktion, die
    immer > 0 zurückgibt, sähe oben genauso grün aus."""
    sonde = next(s for s in QM.SONDEN if s[0] == "CORDIS")
    assert sonde[4](_wie_die_sonde_liest(_zip_bytes(LAYOUTS["fremd"]))) == 0


@pytest.mark.parametrize("layout", list(LAYOUTS))
def test_sonde_und_parser_akzeptieren_genau_dasselbe(layout):
    """Die Kopplung ist der Punkt. Die Sonde sagt „die Quelle lebt", der
    Parser „wir können sie lesen" — solange beide dieselben Dateinamen
    akzeptieren, kann die Sonde nicht mehr grün bleiben, während der
    Refresh leer läuft."""
    rohdaten = _zip_bytes(LAYOUTS[layout])
    sonde = next(s for s in QM.SONDEN if s[0] == "CORDIS")
    sonde_sieht = sonde[4](_wie_die_sonde_liest(rohdaten)) > 0
    parser_liest = bool(
        _records_from_zip(zipfile.ZipFile(io.BytesIO(rohdaten)), "HORIZON"))
    assert sonde_sieht is parser_liest, layout


def test_cordis_sonde_holt_nur_den_archiv_schwanz():
    """141 MB täglich für die Frage „lebt die Quelle" wären unverhältnismäßig.
    Das Central Directory steht am Ende, ein Range-Header genügt."""
    sonde = next(s for s in QM.SONDEN if s[0] == "CORDIS")
    assert len(sonde) > 6, "ohne extra-Feld kein Range-Header"
    assert sonde[6]["headers"]["Range"] == "bytes=-131072"


def test_der_schwanz_eines_grossen_archivs_fuehrt_noch_membernamen():
    """Die Annahme hinter dem Range: bei vielen Membern besteht das Ende
    des Archivs aus Central-Directory-Einträgen — also aus Namen."""
    rohdaten = _zip_bytes(
        [f"project-rcn-{i}_en.json" for i in range(1500)])
    assert len(rohdaten) > 131072, "Testarchiv zu klein für die Aussage"
    schwanz = _wie_die_sonde_liest(rohdaten[-131072:])
    sonde = next(s for s in QM.SONDEN if s[0] == "CORDIS")
    assert sonde[4](schwanz) > 0


# --------------------------------------------------------------------------
# 3. DOAB und die Exit-Codes
# --------------------------------------------------------------------------

def test_doab_ist_bekannt_defekt_mit_datum_und_bedingung():
    grund = QM.BEKANNT_DEFEKT["DOAB"]
    assert "2026-10-11" in grund
    assert "WIEDER AUFNEHMEN" in grund


def test_doab_begruendung_nennt_die_gegenproben():
    """Ein 403 vom Prod-Host allein heißt „unsere IP ist gesperrt" (Getty).
    Erst die zweite Stelle und die Schwester-Instanz zeigen, dass die API
    selbst zu ist — und erst das rechtfertigt, den Konnektor nicht auf
    einen anderen Endpunkt umzubiegen."""
    grund = QM.BEKANNT_DEFEKT["DOAB"]
    assert "Entwickler-Mac" in grund
    assert "oapen" in grund.lower()
    assert "oai" in grund.lower(), "der lebende Ersatzweg muss dokumentiert sein"


def test_doab_sonde_bleibt_stehen():
    """Bekannt defekt heißt nicht „Sonde raus" — sonst merkt niemand, wenn
    die Quelle zurückkommt."""
    assert any(s[0] == "DOAB" for s in QM.SONDEN)


def test_bekannt_defekt_faerbt_den_exit_code_nicht():
    """Fünf tote Quellen dürfen den täglichen Lauf nicht dauerhaft rot
    halten — sonst stirbt der Wächter an seinem eigenen Rauschen."""
    assert "DOAB" in QM.BEKANNT_DEFEKT
    assert QM.ALARM == {"kaputte_abfrage", "stumm", "http_fehler",
                        "unlesbar", "unerreichbar"}
    assert "bekannt_defekt" not in QM.ALARM
    assert "gedrosselt" not in QM.ALARM


def test_exit_zwei_heisst_gelaufen_mit_befund(monkeypatch, capsys):
    monkeypatch.setattr(QM, "SONDEN", [
        ("Kaputt", "wgi", "https://example.test/x", {}, lambda t: -1, None)])
    monkeypatch.setattr(QM, "_post_alert", lambda *a, **k: None)

    class _Antwort:
        status = 200
        def read(self): return b"{}"
        def __enter__(self): return self
        def __exit__(self, *a): return False

    monkeypatch.setattr(QM.urllib.request, "urlopen",
                        lambda *a, **k: _Antwort())
    monkeypatch.setattr(sys, "argv", ["quellen_lebenszeichen.py"])
    assert QM.main() == 2
    capsys.readouterr()


def test_exit_null_heisst_alles_still(monkeypatch, capsys):
    monkeypatch.setattr(QM, "SONDEN", [
        ("Heil", "wgi", "https://example.test/x", {}, lambda t: 7, None)])
    monkeypatch.setattr(QM, "_post_alert", lambda *a, **k: None)

    class _Antwort:
        status = 200
        def read(self): return b"[1]"
        def __enter__(self): return self
        def __exit__(self, *a): return False

    monkeypatch.setattr(QM.urllib.request, "urlopen",
                        lambda *a, **k: _Antwort())
    monkeypatch.setattr(sys, "argv", ["quellen_lebenszeichen.py"])
    assert QM.main() == 0
    capsys.readouterr()


def test_befund_und_fehlstart_sind_unterscheidbar():
    """Der eigentliche Punkt: 1 ist NICHT mehr der Befund-Code. Python
    vergibt die 1 für jeden Absturz und `docker compose exec` für jeden
    eigenen Fehler — der Cron hat 18 Tage lang beides gleich gemeldet."""
    quelle = (BACKEND / "tools" / "quellen_lebenszeichen.py").read_text(
        encoding="utf-8")
    assert "return 2" in quelle
    assert "return 1" not in quelle
