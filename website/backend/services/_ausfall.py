"""Ein toter Konnektor muss im Log auffallen — einmal, nicht bei jedem Claim.

Befund 2026-10-11 (aus #236): DOAB war seit dem 24.9. tot. Gesehen hat das
nur die taegliche Sonde. Im Backend-Log stand nichts, weil der Fehlerpfad

    except Exception as e:
        logger.debug(f"DOAB fetch failed fuer '{query[:40]}': {e}")
        return []

auf `debug` loggt und Leeres zurueckgibt. Fuer den Aufrufer ist das von
"diese Quelle hat zu dem Claim nichts" nicht zu unterscheiden — und in
Produktion laeuft das Log auf INFO, die Zeile existiert dort also gar nicht.

Der Zensus ueber alle 100 Konnektoren mit Sonde fand **77 Handler um den
Abruf**, davon 49 auf `debug`, 7 auf `info`, 20 auf `warning`, einen ganz
stumm. In **37 Diensten** ging kein Abruf-Handler ueber `debug` hinaus.

Warum nicht einfach alles auf `warning` heben: eine Quelle, die bei jedem
Claim gefragt wird, schreibt dann bei jedem Claim eine Warnung. Ein
Wecker, der im Minutentakt klingelt, wird abgestellt — dieselbe Falle wie
der 61-%-Fehlalarm aus #130 und der 18-Tage-Dauerruf aus #236.

Deshalb: **die erste Meldung je Dienst ist laut, danach eine Stunde leise.**
Die Wiederholung nennt, wie viele Ausfaelle zwischenzeitlich kamen — damit
"einmal kurz gestolpert" von "seit Stunden tot" unterscheidbar bleibt.

Was dieses Modul NICHT tut: es unterscheidet nicht, WARUM der Abruf
scheiterte. Das ist die Aufgabe der Sonde (`tools/quellen_lebenszeichen.py`),
die den Grund kennt und klassifiziert. Hier geht es nur darum, dass der
Ausfall im Log des Dienstes ueberhaupt vorkommt.
"""

from __future__ import annotations

import time

# Nach dieser Stille darf derselbe Dienst wieder laut werden.
WIEDERHOLUNG_S = 3600.0

# dienst -> (Zeitpunkt der letzten lauten Meldung, Ausfaelle seitdem)
_STAND: dict[str, tuple[float, int]] = {}


def melde_ausfall(logger, dienst: str, nachricht: str) -> None:
    """Erste Meldung je Dienst auf WARNING, danach auf DEBUG.

    `nachricht` ist die unveraenderte Meldung des Konnektors — der Sweep hat
    nur den Kanal gewechselt, nicht den Text. Der Kontext darin (Query,
    Land, URL) ist beim Nachsehen genau das, was man braucht.
    """
    jetzt = time.monotonic()
    letzte, seitdem = _STAND.get(dienst, (0.0, 0))
    if letzte == 0.0 or jetzt - letzte >= WIEDERHOLUNG_S:
        if seitdem:
            nachricht = f"{nachricht} [+{seitdem} weitere seit der letzten Meldung]"
        _STAND[dienst] = (jetzt, 0)
        logger.warning(nachricht)
    else:
        _STAND[dienst] = (letzte, seitdem + 1)
        logger.debug(nachricht)


def _zuruecksetzen() -> None:
    """Nur fuer Tests: der Prozess-Zustand ist sonst zwischen Tests geteilt."""
    _STAND.clear()
