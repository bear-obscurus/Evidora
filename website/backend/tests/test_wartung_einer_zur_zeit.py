"""Zwei Wartungs-Container gleichzeitig sprengen die Speicher-Rechnung.

Punkt 6 der offenen Liste hieß "Wartungs-Tools laufen im servierenden
Prod-Container". Das war **veraltet**: #249–#252 haben sie am 12.10.2026
herausgeholt, und auf prod geprüft ruft ``run_evidora_tool.sh`` seitdem
einen eigenen Einmal-Container (Profil ``wartung``, 224m + 512m Swap,
``--rm``). Alle neun Python-Cron-Jobs gehen darüber; der einzige
Nicht-Wrapper-Job ist ein ``docker prune``.

Was dabei auffiel, ist ein anderer Befund — und ein echter.

## Die Invariante stand da, die Crontab verletzte sie

Am ``wartung``-Service in ``docker-compose.yml`` steht die Rechnung:

    3.814 MB Host-RAM minus 3g Backend minus 512m Frontend lassen 230 MB.
    Also 224m — damit gilt die Invariante aus dem 08.08.-Vorfall WEITER,
    dass die Summe aller gleichzeitig laufenden Limits unter dem
    physischen RAM bleibt (3.072 + 512 + 224 = 3.808).

Sie rechnet mit **einem** Wartungs-Container. Zwei gleichzeitig ergeben
3.072 + 512 + 224 + 224 = **4.032 > 3.814**. Und genau zwei standen in der
Crontab auf derselben Minute:

    0 4 * * 1  run_evidora_tool.sh data_freshness_check.py …
    0 4 * * 1  run_evidora_tool.sh check_urls.py --live 30

## Was am 19.10.2026 passiert ist

    ALERT 2026-10-19T04:00:03  data_freshness: Befund (exit 1) — Tool lief
    ALERT 2026-10-19T04:00:05  check_urls kam nicht zum Laufen (exit 137)

Zwei Sekunden Abstand. Exit 137 ist SIGKILL. Das Logfile des Laufs enthält
**nur** "Container … Creating" und "Container … Created" — danach nichts.
Der Lauf starb beim Starten, nicht bei der Arbeit.

Gegenprobe: Dasselbe Werkzeug mit demselben 224m-Limit lief am 05.10.,
12.10. und 26.10. durch (heute 6,5 Minuten, Exit 2 = Befund).
``data_freshness`` braucht 3 Sekunden — das Kollisionsfenster war winzig,
und check_urls ist hineingelaufen.

⚠️ **Was ich NICHT beweisen kann:** den Host-Speicherstand in jener
Sekunde; der ist nicht aufgezeichnet. Und die Zahl von heute spricht gegen
eine einfache Mangel-Erklärung: ``MemAvailable`` lag bei der Verifikation
bei **1.320.624 kB ≈ 1,26 GiB** — zwei 224m-Container passen da bequem
hinein. Der Ausfall ist also nicht deterministisch, sondern ein
intermittierendes Rennen; dazu passt, dass **einer von vier** Wochenläufen
scheiterte.

Dafür spricht die Kollision: der Start in derselben Sekunde, die zwei
Sekunden Abstand der Alarme, die rechnerisch verletzte Invariante und drei
erfolgreiche Wochen mit demselben Limit. Dagegen spräche ein allein zu
knappes 224m-Limit — aber dann wäre der Lauf MITTEN in der Arbeit
gestorben, mit Teil-Ausgabe, wie bei der Lebenszeichen-Sonde in #250.
Hier gab es keine Zeile.

Der Fix entfernt die Nebenläufigkeit. Tritt es erneut auf, war es das
Limit — und dann sagt es der Alarm, jetzt mit dem Speicherstand dabei.
Genau das ist der Zweck der Kontext-Zeile: Sie macht den nächsten
Fehlschlag entscheidbar, statt ihn wieder zur Vermutung zu machen.

## Der Fix

``flock`` macht aus der Annahme eine Garantie: Der zweite Aufruf wartet,
statt neben dem ersten zu starten. Das deckt auch ab, was ein blosses
Verschieben der Crontab-Zeiten offen lässt — einen langen Lauf, der in das
nächste Fenster hineinragt.

``-E 75`` trennt "konnte nicht starten" von "Befund": 75 ist EX_TEMPFAIL
und kollidiert mit keinem Werkzeug-Code (0, 1, 2, 137). Die Crontab-Zweige
lesen 1 bzw. 2 als Befund und alles andere als "kam nicht zum Laufen"
(#236) — 75 landet also im richtigen Zweig, mit Push.

Dazu eine Zeile Kontext vor jedem Lauf: Zeitstempel, Werkzeug,
``MemAvailable``. Am 19.10. fehlte genau die — "exit 137" ohne den
Speicherstand ist nicht einzuordnen.

## Wo diese Suite läuft

``flock`` ist auf macOS nicht vorhanden — neun der sechzehn Prüfungen
werden lokal **übersprungen** und laufen erst in der CI (Ubuntu). Damit
der Fix nicht ungeprüft ausgeliefert wird, sind dieselben sechs
Verhaltensweisen auf dem Hetzner-Host (util-linux 2.39.3) mit demselben
Docker-Stub nachgefahren worden:

    Reihenfolge        START ENDE START ENDE      (keine Überlappung)
    Exit-Codes         0→0, 1→1, 2→2, 137→137
    Lock blockiert     → 75
    Kontext-Zeile      auch bei Exit 137 geschrieben
    Argumente          "… /app/tools/check_urls.py --live 30"
    ohne Werkzeug      → 64

Dependency-light: bash, kein Netz, kein Docker (gestubbt).
"""
import os
import re
import shutil
import stat
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
WRAPPER = BACKEND.parent / "run_evidora_tool.sh"
COMPOSE = BACKEND.parent / "docker-compose.yml"


def _quelle() -> str:
    return WRAPPER.read_text(encoding="utf-8")


def _nur_code() -> str:
    """Der Wrapper OHNE Kommentare.

    Die erste Fassung dieser Suite schlug an der eigenen Erklaerung fehl:
    Der Kopf des Wrappers zitiert den alten Aufruf ("Bis 12.10.2026 stand
    hier `docker compose exec backend`"). Eine Pruefung, die das als
    Rueckfall liest, prueft Prosa statt Code — dieselbe Falle wie in #276.
    """
    zeilen = []
    for zeile in _quelle().splitlines():
        ohne = zeile.split("#", 1)[0] if not zeile.lstrip().startswith("#!") else zeile
        if ohne.strip():
            zeilen.append(ohne)
    return "\n".join(zeilen)


# --------------------------------------------------------- der Vertrag im Text

def test_der_wrapper_existiert_und_ist_ausfuehrbar():
    assert WRAPPER.is_file(), WRAPPER
    assert os.access(WRAPPER, os.X_OK), "nicht ausfuehrbar"


def test_er_ruft_den_eigenen_wartungs_container():
    """Der Befund aus #249–#252 darf nicht zurueckkommen."""
    q = _nur_code()
    assert "--profile wartung run --rm" in q
    assert "compose exec backend" not in q, q
    assert "exec backend" not in q, q


def test_er_serialisiert_mit_flock():
    q = _nur_code()
    assert "flock" in q
    assert re.search(r"flock -w \"\$WARTEZEIT\" -E 75", q), q[-400:]


def test_der_timeout_code_kollidiert_mit_keinem_werkzeug_code():
    """0/1/2 sind Werkzeug-Aussagen, 137 ist der Kill. 75 ist frei."""
    q = _quelle()
    assert "-E 75" in q
    assert "EX_TEMPFAIL" in q, "die Wahl muss begruendet dastehen"


def test_die_lockdatei_liegt_nicht_im_ram():
    """``/tmp`` ist auf diesem Host RAM (#238) — fuer eine Lockdatei
    harmlos, aber der naechste Leser soll nicht raten muessen."""
    q = _nur_code()
    treffer = re.search(r'LOCK="\$\{EVIDORA_WARTUNG_LOCK:-([^}]+)\}"', q)
    assert treffer, "LOCK-Pfad nicht gefunden"
    assert not treffer.group(1).startswith("/tmp"), treffer.group(1)


def test_der_kontext_wird_vor_dem_lauf_geschrieben():
    """Die Zeile, die am 19.10. gefehlt hat."""
    q = _nur_code()
    assert "MemAvailable" in q
    # Vor dem exec, sonst ist sie bei einem Kill nicht geschrieben.
    assert q.index("MemAvailable") < q.index("exec flock")


def test_die_rechnung_steht_in_der_compose_datei():
    """Der Fix beruft sich auf eine Invariante — die muss belegt sein."""
    q = COMPOSE.read_text(encoding="utf-8")
    assert "3.072 + 512 + 224 = 3.808" in q
    assert "mem_limit: 224m" in q


# ------------------------------------------------- funktional, mit Docker-Stub

def _stub_umgebung(tmp: Path, docker_rumpf: str) -> dict:
    """Ein PATH, in dem ``docker`` das tut, was der Test braucht."""
    bin_dir = tmp / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "docker"
    stub.write_text("#!/bin/bash\n" + docker_rumpf, encoding="utf-8")
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    umgebung = dict(os.environ)
    umgebung.update({
        "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}",
        "EVIDORA_DIR": str(tmp),
        "EVIDORA_WARTUNG_LOCK": str(tmp / "wartung.lock"),
        "EVIDORA_WARTUNG_KONTEXT": str(tmp / "kontext.log"),
    })
    return umgebung


@pytest.fixture
def tmpdir_pfad():
    d = tempfile.mkdtemp(prefix="evidora-wartung-")
    try:
        yield Path(d)
    finally:
        shutil.rmtree(d, ignore_errors=True)


@pytest.mark.skipif(not shutil.which("flock"), reason="flock fehlt")
def test_zwei_laeufe_ueberlappen_nicht(tmpdir_pfad):
    """Der eigentliche Beweis: der zweite wartet.

    Der Stub schreibt Start und Ende in eine Datei. Ueberlappen sie, waere
    die Speicher-Invariante wieder verletzt.
    """
    spur = tmpdir_pfad / "spur.log"
    umgebung = _stub_umgebung(
        tmpdir_pfad,
        f'echo "START $(date +%s%N)" >> {spur}\n'
        f'sleep 1\n'
        f'echo "ENDE  $(date +%s%N)" >> {spur}\n'
        f'exit 0\n')
    umgebung["EVIDORA_WARTUNG_WARTEZEIT"] = "30"

    prozesse = [
        subprocess.Popen([str(WRAPPER), f"werkzeug_{i}.py"], env=umgebung,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for i in range(2)
    ]
    for p in prozesse:
        assert p.wait(timeout=60) == 0

    zeilen = spur.read_text().split()
    marken = [z for z in zeilen if z in ("START", "ENDE")]
    assert marken == ["START", "ENDE", "START", "ENDE"], marken


@pytest.mark.skipif(not shutil.which("flock"), reason="flock fehlt")
@pytest.mark.parametrize("code", [0, 1, 2, 137])
def test_der_exit_code_des_werkzeugs_kommt_durch(tmpdir_pfad, code):
    """Die Crontab-Zweige haengen daran (#236)."""
    umgebung = _stub_umgebung(tmpdir_pfad, f"exit {code}\n")
    fertig = subprocess.run([str(WRAPPER), "werkzeug.py"], env=umgebung,
                            capture_output=True, timeout=60)
    assert fertig.returncode == code


@pytest.mark.skipif(not shutil.which("flock"), reason="flock fehlt")
def test_blockiertes_lock_ergibt_75_und_nicht_2(tmpdir_pfad):
    """75 heisst "lief nicht". Eine 2 waere ein erfundener BEFUND."""
    lock = tmpdir_pfad / "wartung.lock"
    lock.touch()
    umgebung = _stub_umgebung(tmpdir_pfad, "exit 0\n")
    umgebung["EVIDORA_WARTUNG_WARTEZEIT"] = "1"

    halter = subprocess.Popen(
        ["flock", "-x", str(lock), "-c", "sleep 5"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(0.5)
        fertig = subprocess.run([str(WRAPPER), "werkzeug.py"], env=umgebung,
                                capture_output=True, timeout=60)
        assert fertig.returncode == 75, fertig.returncode
    finally:
        halter.kill()
        halter.wait(timeout=10)


@pytest.mark.skipif(not shutil.which("flock"), reason="flock fehlt")
def test_der_kontext_steht_auch_bei_einem_kill_schon_da(tmpdir_pfad):
    """Der Stub stirbt mit 137 — die Kontext-Zeile muss trotzdem da sein."""
    umgebung = _stub_umgebung(tmpdir_pfad, "exit 137\n")
    subprocess.run([str(WRAPPER), "check_urls.py", "--live", "30"],
                   env=umgebung, capture_output=True, timeout=60)
    kontext = Path(umgebung["EVIDORA_WARTUNG_KONTEXT"]).read_text()
    assert "check_urls.py" in kontext
    assert "MemAvailable=" in kontext


@pytest.mark.skipif(not shutil.which("flock"), reason="flock fehlt")
def test_ohne_werkzeug_gibt_es_64_statt_eines_befunds(tmpdir_pfad):
    """EX_USAGE — auch ein Aufruf-Fehler darf nicht wie ein Befund
    aussehen."""
    umgebung = _stub_umgebung(tmpdir_pfad, "exit 0\n")
    fertig = subprocess.run([str(WRAPPER)], env=umgebung,
                            capture_output=True, timeout=60)
    assert fertig.returncode == 64, fertig.returncode


@pytest.mark.skipif(not shutil.which("flock"), reason="flock fehlt")
def test_die_argumente_erreichen_das_werkzeug(tmpdir_pfad):
    """``--live 30`` darf nicht unterwegs verloren gehen."""
    spur = tmpdir_pfad / "argv.log"
    umgebung = _stub_umgebung(tmpdir_pfad, f'echo "$@" > {spur}\nexit 0\n')
    subprocess.run([str(WRAPPER), "check_urls.py", "--live", "30"],
                   env=umgebung, capture_output=True, timeout=60)
    zeile = spur.read_text()
    assert "/app/tools/check_urls.py --live 30" in zeile, zeile
    assert "--profile wartung run --rm -T" in zeile, zeile
