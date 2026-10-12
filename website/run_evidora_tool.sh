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
# Die Aufruf-Form bleibt gleich, damit die Crontab-Zeilen unveraendert
# weiterlaufen:
#   ./run_evidora_tool.sh weekly_phrasing_check.py [args...]
#   ./run_evidora_tool.sh data_freshness_check.py --strict
#
# Der Exit-Code des Werkzeugs wird durchgereicht — die Crontab-Zweige
# haengen daran (2 = Befund, alles andere = nicht gelaufen, siehe #236).
set -u
TOOL="$1"
shift
cd /opt/Evidora/website
exec docker compose --profile wartung run --rm -T \
  wartung python3 /app/tools/"$TOOL" "$@"
