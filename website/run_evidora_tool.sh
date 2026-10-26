#!/bin/bash
# Wrapper: ruft ein Wartungs-Werkzeug in einem EIGENEN Einmal-Container auf.
#
# Bis 12.10.2026 stand hier `docker compose exec backend` — die Werkzeuge
# liefen also im Prozessraum des servierenden Dienstes. Der haelt ~2,5 GB
# von 3 GB Limit, und drei der fuenf Container-Neustarts zwischen dem
# 29.09. und dem 09.10. fielen auf die Startminute eines solchen Laufs.
# Der CORDIS-Refresh vom 01.10. starb mit dem Container, in dem er lief:
# leeres Logfile, nur der ALERT (#236, #239).
#
# Jetzt: eigener Container aus demselben Image, eigenes Speicherlimit
# (224m + 512m Swap), profil-gesperrt, `--rm`. Der Dienst nebenan merkt
# nichts davon.
#
# NEU 26.10.2026 — EINER ZUR ZEIT.
# Die Rechnung am `wartung`-Service in docker-compose.yml geht von EINEM
# Wartungs-Container aus: 3.072 + 512 + 224 = 3.808 MB von 3.814 MB
# Host-RAM. Zwei gleichzeitig sprengen sie (4.032 > 3.814). Genau das
# stand in der Crontab: `data_freshness_check.py` und `check_urls.py`
# liefen beide Montag 04:00. Am 19.10.2026 starb check_urls dabei mit
# Exit 137, BEVOR es eine Zeile ausgab — das Logfile enthaelt nur
# "Creating" und "Created". Dasselbe Werkzeug lief am 05., 12. und 26.10.
# mit demselben Limit durch. data_freshness braucht 3 Sekunden, das
# Kollisionsfenster war also winzig; check_urls ist genau hineingelaufen.
#
# `flock` macht aus der Annahme eine Garantie: Der zweite Aufruf wartet,
# statt neben dem ersten zu starten. Das deckt auch den Fall ab, den ein
# blosses Verschieben der Crontab-Zeiten offen laesst — ein langer Lauf,
# der in das naechste Fenster hineinragt.
#
# Die Aufruf-Form bleibt gleich, damit die Crontab-Zeilen unveraendert
# weiterlaufen:
#   ./run_evidora_tool.sh weekly_phrasing_check.py [args...]
#   ./run_evidora_tool.sh data_freshness_check.py --strict
#
# Der Exit-Code des Werkzeugs wird durchgereicht — die Crontab-Zweige
# haengen daran (2 = Befund, alles andere = nicht gelaufen, siehe #236).
set -u

# Alle Pfade ueberschreibbar, damit der Wrapper testbar ist. Die
# Vorgabewerte sind die Produktionswerte; die Crontab aendert sich nicht.
WARTUNG_DIR="${EVIDORA_DIR:-/opt/Evidora/website}"
LOCK="${EVIDORA_WARTUNG_LOCK:-/home/burrito/evidora-logs/.wartung.lock}"
KONTEXT="${EVIDORA_WARTUNG_KONTEXT:-/home/burrito/evidora-logs/wartung_kontext.log}"
# 45 Minuten. Laenger wartet niemand sinnvoll; danach ist der Lauf ein
# Befund fuer den Alarm, kein stilles Ueberspringen.
WARTEZEIT="${EVIDORA_WARTUNG_WARTEZEIT:-2700}"

if [ "$#" -lt 1 ]; then
  echo "Aufruf: $0 <werkzeug.py> [args...]" >&2
  exit 64            # EX_USAGE — kein Befund, kein Speicherproblem
fi
TOOL="$1"
shift
cd "$WARTUNG_DIR"

# Vor dem Start festhalten, wie viel Luft der Host hat. Am 19.10. fehlte
# genau diese Zeile: Der Alarm sagte "exit 137", aber nicht, bei wie viel
# freiem Speicher. Ein Fehlschlag ohne diesen Wert ist nicht einzuordnen.
{
  printf '%s %s MemAvailable=%s kB\n' \
    "$(date -Is)" "$TOOL" \
    "$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo 2>/dev/null)"
} >> "$KONTEXT" 2>/dev/null || true

# -w: warten statt scheitern.
# -E 75: "konnte nicht starten" ist NICHT "Befund". 75 ist EX_TEMPFAIL und
#        kollidiert mit keinem Werkzeug-Code (0, 1, 2, 137). Die
#        Crontab-Zweige lesen 1 bzw. 2 als Befund und alles andere als
#        "kam nicht zum Laufen" (#236) — 75 landet also richtig, mit
#        Push.
exec flock -w "$WARTEZEIT" -E 75 "$LOCK" \
  docker compose --profile wartung run --rm -T \
  wartung python3 /app/tools/"$TOOL" "$@"
