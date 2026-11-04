"""Das Backend-Log zusaetzlich als Datei — 30 Tage, ueber jeden Deploy hinweg.

Bis 4.11.2026 lebte das Log nur im json-file-Treiber von Docker, und der
gehoert zum CONTAINER: Jeder Deploy erstellt ihn neu, und das Log ist weg.
Begrenzt hat also nicht die Groesse (20 MB x 5), sondern der naechste
Deploy — oft derselbe Tag. Messungen ueber mehrere Betriebstage (der
Cache-Anteil, der Schattenbetrieb des STRUKTURELL-Overrides) waren damit
nicht moeglich.

Laenger aufbewahren ist vertretbar, weil das Log keine personenbezogenen
Daten mehr traegt: Die Woerter jeder Anfrage schwaerzt
``services._log_redaktion`` (seit #306), IP-Adressen loggt das Backend nicht
— das Rate-Limit haelt sie nur im Speicher —, und die nginx-Logs davor
schreiben seit demselben PR keine IP mehr (frontend/nginx.conf,
host_logs_ohne_ip.sh).

Die Datei liegt in ``website/logs`` (Mount ``/app/logs``), NICHT unter
``data/``: ``data/`` sichert restic, und ein Backup behielte das Log drei
Monate statt dreissig Tage.

Die Schwaerzung haengt sich an jeden Handler, der beim Installieren
existiert. Deshalb MUSS ``richte_datei_log_ein`` vor
``_log_redaktion.installiere`` laufen — ein Test in
tests/test_logs_ohne_ip.py prueft die Reihenfolge in main.py.
"""

from __future__ import annotations

import logging
import os
from logging.handlers import TimedRotatingFileHandler

LOG_VERZEICHNIS = os.getenv("EVIDORA_LOG_VERZEICHNIS", "/app/logs")
LOG_DATEI = "backend.log"

#: Jede Mitternacht (UTC) eine neue Datei; die 31. aelteste loescht der
#: Handler selbst.
AUFBEWAHRUNG_TAGE = 30

FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

#: uvicorn haengt eigene Handler an "uvicorn" und propagiert nicht zur
#: Wurzel — ohne ihn fehlten "Exception in ASGI application" und der Start.
#: "uvicorn.access" bleibt bewusst draussen: Pfad und Status stehen schon im
#: nginx-Log.
ZIEL_LOGGER = ("", "uvicorn")


def richte_datei_log_ein(verzeichnis: str | None = None) -> str | None:
    """Haengt den Datei-Handler an. Gibt den Pfad zurueck oder None.

    Ohne das Verzeichnis (lokal, CI, Wartungs-Container) passiert nichts —
    angelegt wird es bewusst NICHT: Im Container hiesse ein fehlendes
    Verzeichnis, dass der Mount fehlt, und eine Datei im Container ginge
    beim Deploy wieder verloren.
    """
    v = verzeichnis or LOG_VERZEICHNIS
    if not os.path.isdir(v):
        return None
    pfad = os.path.join(v, LOG_DATEI)
    try:
        handler = TimedRotatingFileHandler(
            pfad, when="midnight", backupCount=AUFBEWAHRUNG_TAGE,
            utc=True, encoding="utf-8")
    except OSError as e:
        logging.getLogger("evidora").warning(
            f"Datei-Log nicht eingerichtet ({v}): {e}")
        return None
    handler.setFormatter(logging.Formatter(FORMAT))
    for name in ZIEL_LOGGER:
        logging.getLogger(name).addHandler(handler)
    return pfad
