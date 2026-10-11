"""Wie ein Alarm-Kanal im Log genannt wird — ohne ihn zu verraten.

Gefunden am 2026-10-11, als der seit #139 kaputte Freshness-Push erstmals
wieder durchlief: drei Wartungs-Tools schrieben die vollstaendige
Webhook-URL in ihre Logdatei ("alert webhook posted to
https://ntfy.sh/<topic>"). Der Topic IST das Zugangsmittel — wer ihn
kennt, liest die Alarme mit und kann eigene einspeisen. Die Logs liegen
unter /home/burrito/evidora-logs mit Modus 0664.

Der Fund ist ein Nebenprodukt der Reparatur: solange der Push warf, kam
die Zeile nie zum Drucken. Ein stiller Fehler hat ein Leck verdeckt.
"""

from __future__ import annotations


def kanal_kurz(url: str) -> str:
    """Host plus die letzten vier Zeichen.

    Genug, um zwei Kanaele im Log zu unterscheiden; zu wenig, um einen zu
    benutzen. Der Host bleibt sichtbar, weil "ging an den falschen Dienst"
    sonst nicht diagnostizierbar waere.
    """
    if not url:
        return "<kein Kanal konfiguriert>"
    host = url.split("://", 1)[-1].split("/", 1)[0]
    return f"{host}/…{url[-4:]}" if len(url) > 4 else host
