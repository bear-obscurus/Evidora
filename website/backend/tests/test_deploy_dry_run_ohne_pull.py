"""``deploy.sh --dry-run`` darf nichts ziehen.

Befund vom 20.11.2026, beim Deploy von #317: ``./deploy.sh --dry-run``
führte den ``git pull --ff-only`` schon aus und brach erst NACH der
Entscheidung ab. Der echte Lauf danach fand "Keine neuen Commits" und
baute nichts. Gerettet hat nur ``--force-build`` — und dann prüfte
Beleg 2 (Hash-Vergleich geänderter Backend-Dateien Host vs. Container)
still gar nichts, weil die Änderungsliste leer war.

Der Fix: Im Dry-Run nur ``git fetch``; ``NEW_REV`` ist ``@{u}``, die
Änderungsliste kommt aus demselben ``git diff OLD..NEW`` wie im echten
Lauf. Also muss die Entscheidung dieselbe sein — das prüft diese Suite
am echten Skript, nicht am Text: In einem Wegwerf-Repo mit einem
"origin" und einem Klon, ``docker`` durch ein Skript ersetzt, das immer
mit 1 endet. Der echte Lauf scheitert dann am ``docker compose up``,
nachdem er seine Entscheidung geloggt hat — genau die Zeile, die wir
vergleichen.

Modellfrei: nur bash und git, kein Netz, kein Docker.
"""
import os
import re
import shutil
import stat
import subprocess

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEBSITE = os.path.dirname(BACKEND)
DEPLOY = os.path.join(WEBSITE, "deploy.sh")

pytestmark = pytest.mark.skipif(
    shutil.which("git") is None or shutil.which("bash") is None,
    reason="braucht git und bash",
)

_GIT_ENV = {
    "GIT_AUTHOR_NAME": "test", "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "test", "GIT_COMMITTER_EMAIL": "test@example.invalid",
    "GIT_CONFIG_NOSYSTEM": "1",
}


def _git(cwd, *args, env=None):
    return subprocess.run(["git", *args], cwd=cwd, env=env, check=True,
                          capture_output=True, text=True).stdout.strip()


def _commit(repo, rel, inhalt, msg, env):
    pfad = os.path.join(repo, rel)
    os.makedirs(os.path.dirname(pfad), exist_ok=True)
    with open(pfad, "w", encoding="utf-8") as fh:
        fh.write(inhalt)
    _git(repo, "add", rel, env=env)
    _git(repo, "commit", "-q", "-m", msg, env=env)


@pytest.fixture
def aufbau(tmp_path):
    """origin (bare) + Arbeitsklon "server" + zweiter Klon "dev", der pusht."""
    env = dict(os.environ, **_GIT_ENV, HOME=str(tmp_path))
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text("#!/bin/sh\nexit 1\n")
    docker.chmod(docker.stat().st_mode | stat.S_IEXEC)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"

    origin = tmp_path / "origin.git"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin), env=env)
    dev = tmp_path / "dev"
    _git(tmp_path, "clone", "-q", str(origin), str(dev), env=env)
    _git(dev, "checkout", "-q", "-b", "main", env=env)
    with open(DEPLOY, encoding="utf-8") as fh:
        skript = fh.read()
    _commit(dev, "website/deploy.sh", skript, "init", env)
    os.chmod(dev / "website" / "deploy.sh", 0o755)
    _commit(dev, "website/backend/main.py", "x = 1\n", "backend", env)
    _git(dev, "push", "-q", "origin", "main", env=env)

    server = tmp_path / "server"
    _git(tmp_path, "clone", "-q", "-b", "main", str(origin), str(server), env=env)
    return {"env": env, "dev": dev, "server": server}


def _neuer_backend_commit(a):
    _commit(a["dev"], "website/backend/services/neu.py", "y = 2\n", "neu", a["env"])
    _git(a["dev"], "push", "-q", "origin", "main", env=a["env"])


def _deploy(a, *args):
    return subprocess.run(
        ["bash", str(a["server"] / "website" / "deploy.sh"), *args],
        cwd=a["server"], env=a["env"], capture_output=True, text=True,
    )


def _entscheidung(ausgabe: str) -> str:
    m = re.search(r"Entscheidung:(.*)", ausgabe)
    assert m, f"keine Entscheidungszeile:\n{ausgabe}"
    return m.group(1).strip()


def test_dry_run_bewegt_head_nicht(aufbau):
    _neuer_backend_commit(aufbau)
    vorher = _git(aufbau["server"], "rev-parse", "HEAD")
    r = _deploy(aufbau, "--dry-run")
    assert r.returncode == 0, r.stdout + r.stderr
    assert _git(aufbau["server"], "rev-parse", "HEAD") == vorher, \
        "--dry-run hat gepullt"


def test_dry_run_sieht_die_neuen_commits(aufbau):
    _neuer_backend_commit(aufbau)
    r = _deploy(aufbau, "--dry-run")
    assert "website/backend/services/neu.py" in r.stdout
    assert _entscheidung(r.stdout) == "build-backend"


def test_zweiter_dry_run_entscheidet_gleich(aufbau):
    _neuer_backend_commit(aufbau)
    erst = _entscheidung(_deploy(aufbau, "--dry-run").stdout)
    zweit = _entscheidung(_deploy(aufbau, "--dry-run").stdout)
    assert erst == zweit == "build-backend"


def test_echter_lauf_nach_dry_run_entscheidet_gleich(aufbau):
    """Der Ablauf vom 20.11.: erst --dry-run, dann der echte Lauf."""
    _neuer_backend_commit(aufbau)
    trocken = _deploy(aufbau, "--dry-run")
    echt = _deploy(aufbau)
    # Der echte Lauf scheitert am Attrappen-docker, NACH der Entscheidung.
    assert echt.returncode != 0
    assert "backend-Build fehlgeschlagen" in echt.stdout
    assert _entscheidung(echt.stdout) == _entscheidung(trocken.stdout)
    assert "Keine neuen Commits" not in echt.stdout
    # und der echte Lauf hat wirklich gezogen
    assert _git(aufbau["server"], "rev-parse", "HEAD") == \
        _git(aufbau["dev"], "rev-parse", "HEAD")


def test_dry_run_ohne_neue_commits(aufbau):
    r = _deploy(aufbau, "--dry-run")
    assert r.returncode == 0, r.stdout + r.stderr
    assert "Keine neuen Commits" in r.stdout
    assert _entscheidung(r.stdout) == "keine (Hot-Reload/Docs-only)"


def test_dry_run_divergent_scheitert_wie_der_pull(aufbau):
    _neuer_backend_commit(aufbau)
    _commit(aufbau["server"], "website/backend/lokal.py", "z = 3\n", "lokal",
            aufbau["env"])
    vorher = _git(aufbau["server"], "rev-parse", "HEAD")
    r = _deploy(aufbau, "--dry-run")
    assert r.returncode != 0
    assert "divergente Historie" in r.stdout
    assert _git(aufbau["server"], "rev-parse", "HEAD") == vorher


def test_beleg2_sagt_wenn_er_nichts_zu_pruefen_hatte():
    """--force-build ohne neue Commits: Beleg 2 darf nicht still fehlen."""
    with open(DEPLOY, encoding="utf-8") as fh:
        text = fh.read()
    assert "Beleg 2: nichts zu prüfen" in text
