#!/bin/bash
# Host-nginx: Zugriffslog OHNE IP-Adressen, 30 Tage; Fehlerlog 7 Tage.
#
# Einmalig, mit sudo, nach dem Deploy des PRs, der dieses Skript bringt:
#
#     sudo bash /opt/Evidora/website/host_logs_ohne_ip.sh
#
# WARUM (gemessen 4.11.2026)
#   /var/log/nginx/access.log stand im Standardformat "combined": volle
#   Besucher-IP, Browserkennung, Referrer und die Anfrage MIT Query-String —
#   wer einen alten Teilen-Link "/?claim=<Behauptung>" oeffnete, stand mit
#   IP und Behauptung darin. 14 Tage lang. Die Datenschutzerklaerung sagt,
#   es wuerden keine personenbezogenen Daten gespeichert.
#
#   Ohne IP laesst sich keine Zeile mehr einer Person zuordnen, also darf das
#   Zugriffslog laenger bleiben: 30 Tage, wie das Datei-Log des Backends.
#
#   Das Fehlerlog kann nginx NICHT umformatieren — jede Zeile traegt
#   "client: <IP>". Es bleibt deshalb nur 7 Tage.
#
#   fail2ban liest auf diesem Host nur SSH, nicht nginx — ohne IP im
#   nginx-Log geht kein Schutz verloren. Das Rate-Limit des Backends haelt
#   die IP nur im Speicher (X-Real-IP), daran aendert sich nichts.
#
# WAS DAS SKRIPT TUT
#   1. sichert nginx.conf und /etc/logrotate.d/nginx
#   2. ersetzt in nginx.conf die Zeile "access_log /var/log/nginx/access.log;"
#      durch ein Format ohne IP, Browserkennung, Referrer und Query-String
#   3. prueft mit "nginx -t" — schlaegt das fehl, wird zurueckgesetzt
#   4. schreibt die logrotate-Regel neu: access.log 30, error.log 7 Tage
#   5. laedt nginx neu
#   6. loescht die ALTEN Zugriffslogs (mit IP) und die Fehlerlogs > 7 Tage —
#      nur beim ersten Lauf, ein zweiter Lauf loescht nichts mehr
#
# Gegenstueck im Container: frontend/nginx.conf (dasselbe Format).

set -euo pipefail

[ "$(id -u)" = 0 ] || { echo "Bitte mit sudo ausfuehren."; exit 1; }

NGINX=/etc/nginx/nginx.conf
ROTATE=/etc/logrotate.d/nginx
LOGS=/var/log/nginx
STAMP=$(date +%Y%m%d-%H%M%S)

# Die logrotate-Sicherung NICHT nach /etc/logrotate.d/ — logrotate liest
# jede Datei dort und hielte die Kopie fuer eine zweite Regel.
cp -a "$NGINX" "$NGINX.vor-ohne-ip-$STAMP"
cp -a "$ROTATE" "/root/logrotate-nginx.vor-ohne-ip-$STAMP"
echo "Sicherung: $NGINX.vor-ohne-ip-$STAMP, /root/logrotate-nginx.vor-ohne-ip-$STAMP"

ERSTER_LAUF=0
if grep -q "log_format ohne_ip" "$NGINX"; then
    echo "nginx.conf hat das Format schon — unveraendert."
else
    ERSTER_LAUF=1
    python3 - "$NGINX" <<'PY'
import re, sys
pfad = sys.argv[1]
s = open(pfad, encoding="utf-8").read()
m = re.search(r"^([ \t]*)access_log /var/log/nginx/access\.log;[ \t]*$", s, re.M)
if not m:
    sys.exit("Zeile 'access_log /var/log/nginx/access.log;' nicht gefunden — nichts geaendert.")
e = m.group(1)
neu = (f"{e}# Ohne IP, Browserkennung, Referrer und Query-String (Evidora, 4.11.2026,\n"
       f"{e}# website/host_logs_ohne_ip.sh). $uri ist der Pfad OHNE Query.\n"
       f"{e}log_format ohne_ip '[$time_local] \"$request_method $uri $server_protocol\" '\n"
       f"{e}                   '$status $body_bytes_sent $request_time';\n"
       f"{e}access_log /var/log/nginx/access.log ohne_ip;")
open(pfad, "w", encoding="utf-8").write(s[:m.start()] + neu + s[m.end():])
PY
fi

if ! nginx -t; then
    cp -a "$NGINX.vor-ohne-ip-$STAMP" "$NGINX"
    echo "nginx -t fehlgeschlagen — nginx.conf zurueckgesetzt, nichts neu geladen."
    exit 1
fi

cat > "$ROTATE" <<'EOF'
# Evidora, 4.11.2026 (website/host_logs_ohne_ip.sh): Zugriffslog ohne IP
# 30 Tage, Fehlerlog (traegt die IP, nicht umformatierbar) 7 Tage.
/var/log/nginx/access.log {
	daily
	missingok
	rotate 30
	compress
	delaycompress
	notifempty
	create 0640 www-data adm
	sharedscripts
	prerotate
		if [ -d /etc/logrotate.d/httpd-prerotate ]; then \
			run-parts /etc/logrotate.d/httpd-prerotate; \
		fi \
	endscript
	postrotate
		invoke-rc.d nginx rotate >/dev/null 2>&1
	endscript
}

/var/log/nginx/error.log {
	daily
	missingok
	rotate 7
	compress
	delaycompress
	notifempty
	create 0640 www-data adm
	sharedscripts
	prerotate
		if [ -d /etc/logrotate.d/httpd-prerotate ]; then \
			run-parts /etc/logrotate.d/httpd-prerotate; \
		fi \
	endscript
	postrotate
		invoke-rc.d nginx rotate >/dev/null 2>&1
	endscript
}
EOF

if ! logrotate -d "$ROTATE" >/dev/null 2>&1; then
    cp -a "/root/logrotate-nginx.vor-ohne-ip-$STAMP" "$ROTATE"
    echo "logrotate -d meldet einen Fehler — Regel zurueckgesetzt."
    exit 1
fi

systemctl reload nginx
echo "nginx neu geladen."

if [ "$ERSTER_LAUF" = 1 ]; then
    # Erst NACH dem Neuladen: Sonst schriebe ein alter Worker noch Zeilen im
    # alten Format in die frisch geleerte Datei.
    sleep 2
    : > "$LOGS/access.log"
    rm -f "$LOGS"/access.log.*
    for n in 8 9 10 11 12 13 14; do
        rm -f "$LOGS/error.log.$n.gz"
    done
    echo "Alte Zugriffslogs mit IP geloescht, Fehlerlogs aelter als 7 Tage geloescht."
fi

echo
echo "Pruefen:"
echo "  tail -3 $LOGS/access.log      # keine IP, kein Query-String"
echo "  ls $LOGS"
