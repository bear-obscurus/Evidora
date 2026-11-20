#!/bin/sh
# Latenz-Probe auf dem Server: misst /api/legal und /api/health/full im Takt.
#
# Eingefuehrt am 20.11.2026 aus den Proben vom 11. und 12.11. (DataCommons-
# Stillstand). Seit #313 schreibt der Healthcheck keine Log-Zeile mehr;
# ein Stillstand der Event-Loop ist nur noch von aussen messbar.
#
# Aufruf (auf dem Server, im Hintergrund):
#   nohup sh latenz_probe.sh <dauer_s> [intervall_s] ["YYYY-MM-DD HH:MM:SS" Start UTC] \
#     > /home/burrito/evidora-logs/latenz_probe_<datum>.log 2>&1 < /dev/null &
# Zeilenformat: HH:MM:SS legal <code> <sekunden> health <code> <sekunden>
# Auswertung lokal: tools/messung/latenz_auswerten.py
# Schreibt nur Zeiten und Statuscodes — keine IP, keinen Claim.

DAUER=${1:?dauer_s fehlt}
TAKT=${2:-2}
START=$3
B=http://127.0.0.1:8000

if [ -n "$START" ]; then
  ZIEL=$(date -u -d "$START" +%s) || exit 2
  WARTE=$((ZIEL - $(date -u +%s)))
  [ "$WARTE" -gt 0 ] && sleep "$WARTE"
fi

ENDE=$(( $(date -u +%s) + DAUER ))
while [ "$(date -u +%s)" -lt "$ENDE" ]; do
  l=$(curl -s -o /dev/null -m 10 -w "%{http_code} %{time_total}" $B/api/legal)
  h=$(curl -s -o /dev/null -m 10 -w "%{http_code} %{time_total}" $B/api/health/full)
  echo "$(date -u +%H:%M:%S) legal $l health $h"
  sleep "$TAKT"
done
