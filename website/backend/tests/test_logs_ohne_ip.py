"""Keine IP-Adressen in den Logs — dafuer 30 Tage statt bis zum naechsten Deploy.

Gemessen am 4.11.2026:

* Host-nginx schrieb ``/var/log/nginx/access.log`` im Format "combined":
  volle IP, Browserkennung, Referrer, Anfrage MIT Query-String; 14 Tage.
* Der nginx im Frontend-Container schrieb im Image-Format "main" dasselbe,
  die echte IP aus ``X-Forwarded-For`` — seit dem 26.9. rund 120.000 Zeilen.
* Die Teilen-Links trugen die Behauptung im Query-String
  (``evidora.eu?claim=…``): Wer einen oeffnete, stand mit IP UND Behauptung
  in beiden Logs.
* Das Backend-Log dagegen lebte nur im Container und verschwand bei jedem
  Deploy — Messungen ueber mehrere Tage waren unmoeglich.

Ohne IP laesst sich keine Zeile einer Person zuordnen; die Woerter der
Anfrage schwaerzt seit #306 die Log-Redaktion. Damit duerfen die Logs
laenger bleiben: 30 Tage. Das Fehlerlog des Host-nginx traegt die IP fest im
Format und bleibt 7 Tage (host_logs_ohne_ip.sh).

Keine Netzabfrage in diesen Tests.
"""

import logging
import re
import subprocess
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import pytest

from services import _log_datei as LD
from services import _log_redaktion as LR

BACKEND = Path(__file__).resolve().parents[1]
WEBSITE = BACKEND.parent
REPO = WEBSITE.parent

FRONTEND_NGINX = WEBSITE / "frontend" / "nginx.conf"
HOST_SKRIPT = WEBSITE / "host_logs_ohne_ip.sh"
APP_JS = WEBSITE / "frontend" / "app.js"

#: Variablen, die eine Person oder ihre Eingabe tragen.
VERBOTEN = ("$remote_addr", "$http_x_forwarded_for", "$proxy_add_x_forwarded_for",
            "$http_user_agent", "$http_referer", "$request_uri", "$args",
            "$query_string", "$arg_", "$remote_user", "$http_cookie")


def _format(text):
    """Der Formatstring von ``log_format ohne_ip``, ueber Zeilen verteilt."""
    m = re.search(r"log_format\s+ohne_ip\s+((?:'[^']*'\s*)+);", text)
    assert m, "log_format ohne_ip fehlt"
    return "".join(re.findall(r"'([^']*)'", m.group(1)))


def _pruefe_format(fmt):
    for v in VERBOTEN:
        assert v not in fmt, v
    # "$request" allein enthaelt den Query-String, "$request_method" nicht.
    assert not re.search(r"\$request(?![_a-z])", fmt), fmt
    for noetig in ("$time_local", "$request_method", "$uri", "$status"):
        assert noetig in fmt, noetig


# --------------------------------------------------------------------------
# Frontend-Container
# --------------------------------------------------------------------------

@pytest.mark.parametrize("alt", [
    # Image-Standard "main" und Host-Standard "combined" — so stand es vorher.
    "'$remote_addr - $remote_user [$time_local] \"$request\" $status "
    "$body_bytes_sent \"$http_referer\" \"$http_user_agent\" \"$http_x_forwarded_for\"'",
    "'[$time_local] \"$request\" $status'",
])
def test_giftprobe_das_alte_format_faellt_durch(alt):
    with pytest.raises(AssertionError):
        _pruefe_format(_format(f"log_format ohne_ip {alt};"))


def test_frontend_format_ohne_personenbezug():
    _pruefe_format(_format(FRONTEND_NGINX.read_text(encoding="utf-8")))


def test_frontend_nutzt_das_format_im_server_block():
    text = FRONTEND_NGINX.read_text(encoding="utf-8")
    server = text[text.index("server {"):]
    assert re.search(r"access_log\s+\S+\s+ohne_ip\s*;", server)
    assert "log_not_found off;" in server


def test_log_format_steht_ausserhalb_des_server_blocks():
    """log_format ist nur im http-Kontext erlaubt (conf.d/ liegt darin)."""
    text = FRONTEND_NGINX.read_text(encoding="utf-8")
    assert text.index("log_format ohne_ip") < text.index("server {")


# --------------------------------------------------------------------------
# Host-Skript
# --------------------------------------------------------------------------

def _skript():
    return HOST_SKRIPT.read_text(encoding="utf-8")


def _eingebettetes_python():
    m = re.search(r"python3 - \"\$NGINX\" <<'PY'\n(.*?)\nPY\n", _skript(), re.S)
    assert m
    return m.group(1)


def test_host_und_container_schreiben_dasselbe_format():
    roh = _eingebettetes_python()
    zeilen = re.findall(r"log_format ohne_ip ('.*?')\\n|^\s*f\"\{e\}\s+('.*?');\\n", roh, re.M)
    host = "".join(re.findall(r"'([^']*)'", "".join(a or b for a, b in zeilen)))
    host = host.replace('\\"', '"')
    assert host == _format(FRONTEND_NGINX.read_text(encoding="utf-8")), host


def test_das_eingebettete_python_ersetzt_genau_die_zeile(tmp_path):
    """Auf dem Ubuntu-Standard (so steht es auf dem Host, Zeile 40)."""
    conf = tmp_path / "nginx.conf"
    conf.write_text("http {\n\t##\n\t# Logging Settings\n\t##\n\n"
                    "\taccess_log /var/log/nginx/access.log;\n\n"
                    "\tinclude /etc/nginx/conf.d/*.conf;\n}\n", encoding="utf-8")
    skript = tmp_path / "edit.py"
    skript.write_text(_eingebettetes_python(), encoding="utf-8")
    subprocess.run([sys.executable, str(skript), str(conf)], check=True)
    neu = conf.read_text(encoding="utf-8")
    assert "\taccess_log /var/log/nginx/access.log ohne_ip;" in neu
    assert "access_log /var/log/nginx/access.log;" not in neu
    _pruefe_format(_format(neu))
    assert neu.index("log_format ohne_ip") < neu.index("access_log /var/log/nginx/access.log ohne_ip")


def test_das_eingebettete_python_bricht_ab_wenn_die_zeile_fehlt(tmp_path):
    conf = tmp_path / "nginx.conf"
    conf.write_text("http {\n}\n", encoding="utf-8")
    skript = tmp_path / "edit.py"
    skript.write_text(_eingebettetes_python(), encoding="utf-8")
    r = subprocess.run([sys.executable, str(skript), str(conf)], capture_output=True, text=True)
    assert r.returncode != 0
    assert conf.read_text(encoding="utf-8") == "http {\n}\n"


def test_logrotate_30_tage_zugriff_7_tage_fehler():
    s = _skript()
    regel = s[s.index("cat > \"$ROTATE\" <<'EOF'"):]
    regel = regel[:regel.index("\nEOF\n")]
    zugriff = regel[regel.index("/var/log/nginx/access.log {"):regel.index("/var/log/nginx/error.log {")]
    fehler = regel[regel.index("/var/log/nginx/error.log {"):]
    assert re.search(r"\brotate 30\b", zugriff)
    assert re.search(r"\brotate 7\b", fehler)
    assert "/var/log/nginx/*.log" not in regel, "Glob plus Einzelregel = doppelter Eintrag"


def test_skript_prueft_vor_dem_neuladen_und_setzt_zurueck():
    s = _skript()
    assert s.index("nginx -t") < s.index("systemctl reload nginx")
    assert s.index("logrotate -d") < s.index("systemctl reload nginx")
    assert "set -euo pipefail" in s


def test_sicherung_nicht_in_logrotate_d():
    """logrotate liest jede Datei in /etc/logrotate.d/ als Regel."""
    s = _skript()
    assert re.search(r'cp -a "\$ROTATE" "/root/', s)
    assert not re.search(r'cp -a "\$ROTATE" "\$ROTATE', s)


def test_alte_logs_werden_nur_beim_ersten_lauf_geloescht():
    """Ein zweiter Lauf loeschte sonst die guten 30 Tage."""
    s = _skript()
    loeschen = s.index('rm -f "$LOGS"/access.log.*')
    block = s.rfind('if [ "$ERSTER_LAUF" = 1 ]; then', 0, loeschen)
    assert block != -1
    assert not re.search(r"^\s*fi\s*$", s[block:loeschen], re.M), "rm steht nicht im Block"
    assert 'grep -q "log_format ohne_ip" "$NGINX"' in s


def test_skript_ist_bash_syntaktisch_gueltig():
    subprocess.run(["bash", "-n", str(HOST_SKRIPT)], check=True)


# --------------------------------------------------------------------------
# Teilen-Links: die Behauptung hinter "#"
# --------------------------------------------------------------------------

def test_teilen_link_traegt_die_behauptung_im_fragment():
    js = APP_JS.read_text(encoding="utf-8")
    m = re.search(r"function teilenLink\(claim\) \{(.*?)\n\}", js, re.S)
    assert m and "#claim=${encodeURIComponent(claim)}" in m.group(1)
    assert "?claim=${" not in js, "noch ein Link mit Query-String"
    assert js.count("teilenLink(claim)") >= 2, "Teilen UND Melden"


def test_alte_links_mit_query_funktionieren_weiter():
    js = APP_JS.read_text(encoding="utf-8")
    m = re.search(r"function checkUrlParams\(\) \{(.*?)\n\}", js, re.S)
    assert m
    assert "window.location.hash" in m.group(1)
    assert "window.location.search" in m.group(1)


# --------------------------------------------------------------------------
# Backend: Datei-Log, 30 Tage
# --------------------------------------------------------------------------

@pytest.fixture
def ohne_datei_handler():
    vorher = {n: list(logging.getLogger(n).handlers) for n in LD.ZIEL_LOGGER}
    yield
    for n in LD.ZIEL_LOGGER:
        lg = logging.getLogger(n)
        for h in list(lg.handlers):
            if h not in vorher[n]:
                lg.removeHandler(h)
                h.close()


def _datei_handler(name):
    return [h for h in logging.getLogger(name).handlers if isinstance(h, TimedRotatingFileHandler)]


def test_ohne_verzeichnis_kein_datei_log(tmp_path, ohne_datei_handler):
    assert LD.richte_datei_log_ein(str(tmp_path / "fehlt")) is None
    assert not (tmp_path / "fehlt").exists(), "nicht selbst anlegen"
    assert not _datei_handler("")


def test_datei_log_30_tage_um_mitternacht(tmp_path, ohne_datei_handler):
    pfad = LD.richte_datei_log_ein(str(tmp_path))
    assert pfad == str(tmp_path / "backend.log")
    for name in LD.ZIEL_LOGGER:
        (h,) = _datei_handler(name)
        assert h.when == "MIDNIGHT" and h.backupCount == 30 and h.utc


def test_uvicorn_access_bleibt_draussen():
    assert "uvicorn.access" not in LD.ZIEL_LOGGER


def test_die_datei_ist_geschwaerzt(tmp_path, monkeypatch, ohne_datei_handler):
    """Der Handler haengt VOR der Redaktion und wird von ihr umhuellt."""
    alte = logging.getLogRecordFactory()
    monkeypatch.setattr(LR, "_installiert", False)
    LR.vergiss_anfragen()
    try:
        LD.richte_datei_log_ein(str(tmp_path))
        LR.installiere()
        LR.merke_anfrage("Sitzerhöhung für fünfjährige Kinder")
        logging.getLogger("evidora").warning("Treffer zu Sitzerhöhung geprüft")
        for h in _datei_handler(""):
            h.flush()
        text = (tmp_path / "backend.log").read_text(encoding="utf-8")
        assert "Treffer zu" in text
        assert "Sitzerhöhung" not in text, text
    finally:
        LR.vergiss_anfragen()
        logging.setLogRecordFactory(alte)


def test_main_haengt_das_datei_log_vor_die_redaktion():
    q = (BACKEND / "main.py").read_text(encoding="utf-8")
    assert q.index("logging.basicConfig") < q.index("_datei_log_einrichten()") \
        < q.index("_redaktion_installieren()")


def test_compose_mountet_logs_ausserhalb_von_data():
    c = (WEBSITE / "docker-compose.yml").read_text(encoding="utf-8")
    backend = c[c.index("  backend:"):c.index("\n  frontend:")] if "\n  frontend:" in c else c
    assert "- ./logs:/app/logs" in backend
    assert "./backend/data/logs" not in c


def test_verzeichnis_kommt_mit_git_der_inhalt_nicht():
    assert (WEBSITE / "logs" / ".gitkeep").exists()
    gi = (REPO / ".gitignore").read_text(encoding="utf-8")
    assert "website/logs/*" in gi and "!website/logs/.gitkeep" in gi
