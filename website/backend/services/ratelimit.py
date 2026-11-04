"""Per-IP rate limiting for /api/check — pure-stdlib helper.

Ausgelagert aus main.py, damit die Logik dependency-light unit-getestet
werden kann (kein FastAPI-/ML-Import).

Trust-Modell (Audit-Befunde #2/#4, 2026-07-07): Der EINZIGE öffentliche Weg
zu /api/ ist Host-nginx → Backend. Das Backend bindet nur 127.0.0.1:8000,
und der Host-nginx-Block `location /api/` proxyt DIREKT auf :8000 (NICHT über
die Container-Frontend-nginx). Der Host-nginx setzt `X-Real-IP $remote_addr`
per `proxy_set_header` — das ÜBERSCHREIBT jeden vom Client gelieferten Wert,
die echte Client-IP ist also nicht fälschbar.

Der linkeste `X-Forwarded-For`-Eintrag ist dagegen client-kontrolliert
(nginx hängt die echte IP nur an) und darf NICHT fürs Rate-Limit-Keying
verwendet werden — genau das war die gefixte Lücke: ein rotierender
XFF-Wert pro Request erschien als neue IP und umging das Limit komplett.

Datenschutz (4.11.2026): Bis dahin stand die IP im Klartext als Schlüssel
im Speicher, und geräumt wurde erst ab ``max_keys`` (10.000) Einträgen —
praktisch blieb jede IP seit dem letzten Deploy bis zum Neustart liegen.
Der Server lagert Speicher aus (2 GB Swap, Container bis 512 MB), die
Tabelle konnte also auf der Platte landen. Jetzt:

* Schlüssel ist ein HMAC-SHA256-Kennwert der IP. Der Schlüssel dafür
  (``_salz``) entsteht pro Instanz, also pro Prozess, aus ``os.urandom`` und
  liegt nur im Speicher. In der Tabelle steht keine IP mehr.
* ``raeume_regelmaessig`` entfernt alle ``RAEUM_INTERVALL_S`` Sekunden die
  Einträge, deren letzte Prüfung länger als das Fenster zurückliegt — auch
  wenn niemand prüft. Damit ist ein Kennwert spätestens Fenster + Abstand
  nach der letzten Prüfung weg; ``HOECHSTDAUER_S`` legt einen Puffer für
  eine kurz belegte Event-Loop drauf. Diese Zahl nennt die
  Datenschutzerklärung (tests/test_datenschutz_exakt.py).
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import time as _time
from collections.abc import Mapping

logger = logging.getLogger("evidora")

#: Abstand der Hintergrund-Räumung.
RAEUM_INTERVALL_S = 20

#: Obere Schranke, die die Datenschutzerklärung nennt: Fenster (60 s) +
#: Abstand (20 s) + 10 s Puffer für eine kurz belegte Event-Loop.
HOECHSTDAUER_S = 90


def get_client_ip(headers: Mapping, peer_host: str | None) -> str:
    """Vertrauenswürdige Client-IP fürs Rate-Limit-Keying.

    ``X-Real-IP`` wird vom Trusted-Host-nginx gesetzt (nicht fälschbar).
    Fällt auf die direkte Peer-IP zurück, falls der Header fehlt (z.B. ein
    lokaler Direkt-Aufruf an 127.0.0.1:8000 ohne Proxy). Der
    client-kontrollierte ``X-Forwarded-For`` wird bewusst NICHT gelesen.
    """
    real_ip = (headers.get("x-real-ip") or "").strip()
    if real_ip:
        return real_ip
    return peer_host or "unknown"


class RateLimiter:
    """Fixed-Window-Limiter pro IP mit beschränktem Speicher.

    Keys werden opportunistisch geräumt, sobald der Store ``max_keys``
    überschreitet (stale = kein Request innerhalb des Fensters). Dadurch
    kann der Store selbst bei IP-Churn nie unbegrenzt wachsen — das
    schließt den OOM-Vektor (Befund #4), der Store-Keys nie löschte.
    """

    def __init__(self, limit: int, window: int, max_keys: int = 10_000):
        self.limit = limit
        self.window = window
        self.max_keys = max_keys
        self._store: dict[bytes, list[float]] = {}
        self._salz = os.urandom(32)

    def _kennung(self, ip: str) -> bytes:
        """Kennwert statt IP — ohne ``_salz`` nicht zurückzurechnen."""
        return hmac.new(self._salz, ip.encode("utf-8"), hashlib.sha256).digest()[:16]

    def raeume(self, now: float | None = None) -> int:
        """Entfernt Einträge ohne Prüfung im Fenster. Gibt die Zahl zurück."""
        now = _time.time() if now is None else now
        stale = [k for k, ts in self._store.items()
                 if not ts or now - ts[-1] >= self.window]
        for k in stale:
            del self._store[k]
        return len(stale)

    def allow(self, ip: str, now: float | None = None) -> bool:
        """True, wenn der Request erlaubt ist (und zählt ihn dann), sonst
        False (Limit erreicht)."""
        now = _time.time() if now is None else now
        if len(self._store) > self.max_keys:
            self.raeume(now)
        k = self._kennung(ip)
        timestamps = [t for t in self._store.get(k, []) if now - t < self.window]
        if len(timestamps) >= self.limit:
            self._store[k] = timestamps
            return False
        timestamps.append(now)
        self._store[k] = timestamps
        return True

    def __len__(self) -> int:
        return len(self._store)


async def raeume_regelmaessig(limiter: RateLimiter,
                              intervall: float = RAEUM_INTERVALL_S) -> None:
    """Hintergrund-Schleife: räumt auch, wenn niemand prüft.

    Ohne sie bliebe der letzte Kennwert einer ruhigen Stunde liegen, bis die
    nächste Prüfung kommt. Ein Fehler beendet die Schleife nicht.
    """
    while True:
        await asyncio.sleep(intervall)
        try:
            limiter.raeume()
        except Exception as e:  # pragma: no cover — darf nie die Schleife beenden
            logger.warning(f"Rate-Limit-Räumung fehlgeschlagen: {e}")
