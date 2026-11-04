"""Das Rate-Limit haelt keine IP mehr — und nichts laenger als 90 Sekunden.

Bis 4.11.2026 stand die IP im Klartext als Schluessel in der Tabelle, und
geraeumt wurde erst ab 10.000 Eintraegen: Jede IP seit dem letzten Deploy
blieb bis zum Neustart im Speicher. Der Server lagert Speicher aus (2 GB
Swap, Backend-Container bis 512 MB) — die Tabelle konnte also auf der Platte
landen, und der Satz der Datenschutzerklaerung "wird nicht auf Datentraeger
geschrieben" war nicht haltbar.

Jetzt: HMAC-Kennwert statt IP, Schluessel pro Prozess aus os.urandom, und
eine Hintergrund-Schleife raeumt alle 20 Sekunden — auch ohne Anfrage.

Keine Netzabfrage in diesen Tests.
"""

import asyncio
from pathlib import Path

import pytest

from services import ratelimit as RL
from services.ratelimit import RateLimiter

BACKEND = Path(__file__).resolve().parents[1]
IP = "203.0.113.42"          # Dokumentations-Adresse (RFC 5737)


def test_in_der_tabelle_steht_keine_ip():
    rl = RateLimiter(limit=10, window=60)
    rl.allow(IP, now=0.0)
    assert IP not in rl._store
    assert all(isinstance(k, bytes) for k in rl._store)
    assert IP.encode() not in b"".join(rl._store)
    assert IP not in repr(rl._store)


def test_gleiche_ip_ein_eimer_andere_ip_ein_anderer():
    rl = RateLimiter(limit=2, window=60)
    assert rl.allow(IP, now=0.0) and rl.allow(IP, now=1.0)
    assert not rl.allow(IP, now=2.0), "das Limit muss weiter greifen"
    assert rl.allow("198.51.100.7", now=2.0)
    assert len(rl) == 2


def test_der_schluessel_ist_pro_instanz_neu():
    """Pro Prozess neu: Ein Kennwert laesst sich nicht ueber Neustarts verfolgen."""
    a, b = RateLimiter(10, 60), RateLimiter(10, 60)
    assert a._kennung(IP) != b._kennung(IP)
    assert a._kennung(IP) == a._kennung(IP)


def test_raeume_entfernt_abgelaufene_eintraege_ohne_neue_anfrage():
    """Die alte Fassung raeumte erst ab max_keys — und nur bei einer Anfrage."""
    rl = RateLimiter(limit=10, window=60)
    rl.allow(IP, now=0.0)
    rl.allow("198.51.100.7", now=50.0)
    assert rl.raeume(now=61.0) == 1
    assert len(rl) == 1
    assert rl.raeume(now=111.0) == 1
    assert len(rl) == 0


def test_die_grenze_hat_puffer():
    import re
    m = re.search(r'^RATE_WINDOW = int\(os\.getenv\("RATE_WINDOW", "(\d+)"\)\)',
                  (BACKEND / "main.py").read_text(encoding="utf-8"), re.M)
    assert m
    assert int(m.group(1)) + RL.RAEUM_INTERVALL_S + 10 <= RL.HOECHSTDAUER_S


def test_die_schleife_raeumt_im_takt(monkeypatch):
    """Gegen echte Zeit, mit kurzem Takt: drei Runden, dann abgebrochen."""
    rl = RateLimiter(limit=10, window=60)
    aufrufe = []
    monkeypatch.setattr(rl, "raeume", lambda now=None: aufrufe.append(1) or 0)

    async def lauf():
        task = asyncio.create_task(RL.raeume_regelmaessig(rl, intervall=0.01))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(lauf())
    assert len(aufrufe) >= 3


def test_ein_fehler_beendet_die_schleife_nicht(monkeypatch):
    rl = RateLimiter(limit=10, window=60)
    zaehler = []

    def kaputt(now=None):
        zaehler.append(1)
        raise RuntimeError("absichtlich")

    monkeypatch.setattr(rl, "raeume", kaputt)

    async def lauf():
        task = asyncio.create_task(RL.raeume_regelmaessig(rl, intervall=0.01))
        await asyncio.sleep(0.05)
        assert not task.done(), "die Schleife ist am Fehler gestorben"
        task.cancel()

    asyncio.run(lauf())
    assert len(zaehler) >= 2


def test_main_startet_die_schleife_beim_start():
    q = (BACKEND / "main.py").read_text(encoding="utf-8")
    i = q.index("async def rate_limit_raeumen_starten")
    assert '@app.on_event("startup")' in q[i - 40:i]
    assert "asyncio.create_task(raeume_regelmaessig(_rate_limiter))" in q[i:i + 200]
