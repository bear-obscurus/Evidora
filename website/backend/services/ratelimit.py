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
"""
from __future__ import annotations

import time as _time
from collections.abc import Mapping


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
        self._store: dict[str, list[float]] = {}

    def _sweep(self, now: float) -> None:
        stale = [k for k, ts in self._store.items()
                 if not ts or now - ts[-1] >= self.window]
        for k in stale:
            del self._store[k]

    def allow(self, ip: str, now: float | None = None) -> bool:
        """True, wenn der Request erlaubt ist (und zählt ihn dann), sonst
        False (Limit erreicht)."""
        now = _time.time() if now is None else now
        if len(self._store) > self.max_keys:
            self._sweep(now)
        timestamps = [t for t in self._store.get(ip, []) if now - t < self.window]
        if len(timestamps) >= self.limit:
            self._store[ip] = timestamps
            return False
        timestamps.append(now)
        self._store[ip] = timestamps
        return True

    def __len__(self) -> int:
        return len(self._store)
