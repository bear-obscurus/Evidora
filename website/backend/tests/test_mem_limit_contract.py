"""Das cgroup-Limit muss den Host schuetzen — und den Start ueberleben.

Anlass: Bis 2026-09-05 stand `mem_limit: 4g` bei **3.814 MB** physischem
Host-RAM. Ein cgroup-Limit ueber dem physischen RAM schuetzt gar nichts:
Der Container darf sich legal mehr nehmen, als die Maschine hat. Genau das
riss am 08.08.2026 den Host mit (SSH und nginx tot, Hard-Reboot noetig).

Zwei Fehlrichtungen, beide hier gepinnt:
  * ZU HOCH  -> das Limit greift nie, der Host stirbt zuerst.
  * ZU NIEDRIG -> OOM-Kill waehrend des Model-Prefetch beim Start; der
    Dienst kaeme gar nicht erst hoch. Die Untergrenze ist deshalb am
    GEMESSENEN anon-Bedarf verankert, nicht geraten.

Messung 2026-09-05 (cgroup v2, nach echtem Cold Start mit Model-Prefetch):
  memory.peak 3.407 MB (dominiert von Page Cache), memory.current 2.703 MB,
  davon anon 2.178 MB (echt belegt) und file 461 MB (rueckholbar);
  memory.events: oom 0, oom_kill 0.

Dependency-light: Regex ueber die Compose-Datei, kein YAML-Parser noetig
(die CI installiert kein PyYAML), kein Netz, kein Docker.
"""

import re
from pathlib import Path

import pytest

COMPOSE = (Path(__file__).resolve().parents[2] / "docker-compose.yml")

# Hetzner CX22, `free -m` am 05.09.2026. Aendert sich der Server, gehoert
# diese Zahl mit — dann schlaegt der Test bewusst an.
HOST_RAM_MB = 3814

# Gemessener anon-Bedarf im Betrieb (2.178 MB) plus Reserve fuer den
# Cold-Start-Ausschlag. Darunter zu gehen waere ein OOM-Kill mit Ansage.
MIN_SICHER_MB = 2600


def _mb(wert: str) -> int:
    """Docker-Groessenangabe -> MB. Akzeptiert g/m/k und blanke Bytes."""
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([gGmMkK])?[bB]?", wert.strip())
    assert m, f"unparsbare Groessenangabe: {wert!r}"
    zahl, einheit = float(m.group(1)), (m.group(2) or "").lower()
    faktor = {"g": 1024, "m": 1, "k": 1 / 1024, "": 1 / 1048576}[einheit]
    return int(zahl * faktor)


def _service_block(name: str) -> str:
    text = COMPOSE.read_text(encoding="utf-8")
    start = text.index(f"\n  {name}:")
    m = re.search(r"\n  [a-z_]+:", text[start + 1:])
    return text[start:start + 1 + (m.start() if m else len(text))]


def _limits(name: str) -> tuple[int, int]:
    block = _service_block(name)
    mem = re.search(r"^\s*mem_limit:\s*(\S+)", block, re.M)
    swap = re.search(r"^\s*memswap_limit:\s*(\S+)", block, re.M)
    assert mem and swap, f"{name}: mem_limit/memswap_limit fehlen"
    return _mb(mem.group(1)), _mb(swap.group(1))


def test_groessenparser():
    assert _mb("3g") == 3072
    assert _mb("3584m") == 3584
    assert _mb("512m") == 512


@pytest.mark.parametrize("service", ["backend", "frontend"])
def test_limit_liegt_unter_dem_host_ram(service):
    """Der Kern: ein Limit >= physischem RAM ist kein Schutz, sondern eine
    Erlaubnis. Genau daran ist der Host am 08.08. gestorben."""
    mem, _ = _limits(service)
    assert mem < HOST_RAM_MB, (
        f"{service}: mem_limit {mem} MB >= Host-RAM {HOST_RAM_MB} MB — "
        f"das cgroup-Limit schuetzt den Host dann nicht."
    )


def test_backend_limit_ueberlebt_den_cold_start():
    """Gegenrichtung: zu niedrig ist auch kaputt. Verankert am gemessenen
    anon-Bedarf, nicht an einer Bauchzahl."""
    mem, _ = _limits("backend")
    assert mem >= MIN_SICHER_MB, (
        f"backend: mem_limit {mem} MB unter der gemessenen Untergrenze "
        f"{MIN_SICHER_MB} MB — OOM-Kill beim Model-Prefetch droht."
    )


@pytest.mark.parametrize("service", ["backend", "frontend"])
def test_memswap_nie_kleiner_als_mem(service):
    """memswap_limit ist die SUMME aus RAM und Swap. Kleiner als mem_limit
    ist eine Fehlkonfiguration, gleich bedeutet 'kein Swap erlaubt'."""
    mem, swap = _limits(service)
    assert swap >= mem, f"{service}: memswap {swap} MB < mem {mem} MB"


def test_backend_hat_ein_swap_polster():
    """Bewusste Entscheidung: mit Polster ist der Fehlermodus 'Container
    wird langsam' statt 'Container wird hart gekillt'."""
    mem, swap = _limits("backend")
    assert swap > mem, "kein Swap-Polster — Fehlermodus waere der harte Kill"
    assert swap - mem <= 1024, "mehr als 1 GB Swap wuerde die Modelle auslagern"


def test_summe_der_limits_passt_auf_den_host():
    """Beide Container zusammen duerfen den Host nicht sprengen — sonst
    verschiebt man das Problem nur."""
    summe = _limits("backend")[0] + _limits("frontend")[0]
    assert summe < HOST_RAM_MB, (
        f"backend+frontend = {summe} MB >= Host-RAM {HOST_RAM_MB} MB"
    )


def test_restart_policy_faengt_einen_oom_kill():
    """Wenn das Limit doch greift, muss der Dienst von selbst wiederkommen —
    compose-`restart` greift bei EXIT, und ein OOM-Kill ist ein Exit."""
    assert re.search(r"^\s*restart:\s*unless-stopped",
                     _service_block("backend"), re.M)
