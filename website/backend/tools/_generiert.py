"""Welche Dateien in ``data/`` sind GENERIERT und nicht kuratiert.

Anlass (18.10.2026). ``stichtag_check.py`` laedt jede ``data/*.json`` mit
``json.loads(pfad.read_text())``. Seit PR #249 laeuft es im
Wartungs-Container mit 224 MB Speicherlimit — und
``cordis_projects_slim.json`` ist 108 MiB gross. Das Werkzeug wurde jeden
Morgen vom Kernel erschlagen:

    exit=137, Logdatei enthielt nur "Container … Creating / Created"

Sechs Tage lang (13.–18.10.) schrieb der Cron daraufhin "stichtag_check
FUNDE" — ein Befund, den es nie gab, weil das Werkzeug nie bis zur
Auswertung kam. Dieselbe Ursache wie bei der Lebenszeichen-Sonde in #250,
nur an einem anderen Werkzeug; damals habe ich nur deren Logdatei
angesehen.

Der Code WOLLTE diese Dateien schon ueberspringen. Am ``except`` stand
``# generierte Caches u. AE.`` — die Annahme war, sie wuerden am Parsen
scheitern. ``cordis_projects_slim.json`` parst aber einwandfrei; der
Prozess stirbt vorher.

## Warum ein eigenes Modul

Dasselbe Wissen lag schon zweimal getrennt herum: als Namensliste in
``check_urls.py`` (#243) und als Schwellen-Tabelle in
``data_freshness_check.py``. Eine dritte Kopie waere die Kopie, die
driftet ([[lessons_learned]]: eine Zahl in zwei Kopien driftet
unbemerkt). Die Namen stehen deshalb hier, und ein Gate prueft, dass die
Schwellen-Tabelle keine Datei kennt, die hier fehlt.

## Warum zusaetzlich eine Groessen-Schranke

Die Namensliste schuetzt gegen das, was wir kennen. Ein kuenftiger
generierter Cache unter neuem Namen wuerde das Werkzeug wieder
erschlagen. ``ist_zu_gross`` faengt das ab — und der Aufrufer MELDET den
Uebersprung, statt ihn zu verschlucken (#251: ein uebersprungener Teil
ist kein Befund ueber die Quelle, aber er gehoert in die Ausgabe).
"""

from __future__ import annotations

import os

#: Dateien in ``data/``, die ein Werkzeug erzeugt oder ein Prefetch
#: ablegt — keine kuratierten Fakten. Sie tragen keine
#: ``Stand <Monat> <Jahr>``-Marker und keine Beleg-Links von uns.
GENERIERTE_CACHES: frozenset[str] = frozenset({
    "cordis_projects_slim.json",
    "mitre_attack.json",
    "claimreview_index.json",
})

#: Oberhalb dieser Groesse liest ein Werkzeug im Wartungs-Container
#: (224 MB Limit) keine Datei mehr komplett ein. 24 MiB ist mit Abstand
#: gewaehlt: die groesste KURIERTE Datei lag am 18.10.2026 bei 6 MiB
#: (``euvsdisinfo_db.json``), die kleinste generierte bei 5 MiB
#: (``claimreview_index.json``, deshalb auch oben namentlich gelistet).
#: json.loads braucht grob das Vier- bis Sechsfache der Dateigroesse.
MAX_LESBAR_BYTES = 24 * 1024 * 1024


def ist_generiert(name: str) -> bool:
    """Ist das eine generierte Datei (nach Name)?"""
    return os.path.basename(name) in GENERIERTE_CACHES


def ist_zu_gross(pfad) -> bool:
    """Ist die Datei zu gross, um sie komplett zu lesen?

    Das Sicherheitsnetz unter der Namensliste. Ein Aufrufer, der ``True``
    bekommt, soll den Uebersprung AUSGEBEN — sonst sieht ein stiller
    Verlust aus wie ein saubererer Lauf.
    """
    try:
        return os.path.getsize(pfad) > MAX_LESBAR_BYTES
    except OSError:
        return False


def ueberspringen(pfad) -> str | None:
    """``None`` = lesen. Sonst der GRUND, zum Ausgeben."""
    if ist_generiert(pfad):
        return "generierter Cache, keine kuratierten Fakten"
    if ist_zu_gross(pfad):
        mib = os.path.getsize(pfad) // (1024 * 1024)
        return (f"{mib} MiB — groesser als das Leselimit von "
                f"{MAX_LESBAR_BYTES // (1024 * 1024)} MiB")
    return None
