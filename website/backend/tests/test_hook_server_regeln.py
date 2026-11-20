"""PreToolUse-Hook fuer zwei harte Regeln aus CLAUDE.md (modellfrei).

1. Kein ``git pull`` auf dem Server vor ``deploy.sh``.
2. Kein Python im Prod-Backend-Container per ``docker exec``.
3. Kein zweiter Backend-Container (``docker compose run backend``,
   ``docker run evidora-backend``); nur der Dienst ``wartung`` ist frei.

Der Test liest den Hook-Befehl aus ``.claude/settings.json`` und ruft ihn so
auf, wie Claude Code es tut: Hook-JSON auf stdin, Exit 2 = gesperrt. Damit
prueft er auch die Verdrahtung. Ohne Hook (alter Stand) gibt es keinen
Befehl, und jeder Bash-Befehl gilt als erlaubt -- die Sperr-Faelle sind rot.
"""

import json
import os
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
SETTINGS = REPO / ".claude" / "settings.json"


def _hook_befehle():
    if not SETTINGS.exists():
        return []
    daten = json.loads(SETTINGS.read_text(encoding="utf-8"))
    befehle = []
    for eintrag in daten.get("hooks", {}).get("PreToolUse", []):
        if eintrag.get("matcher") in ("Bash", "*", ""):
            for h in eintrag.get("hooks", []):
                if h.get("type") == "command":
                    befehle.append(h["command"])
    return befehle


def _laeuft(befehl):
    """(exit, stderr) wie Claude Code den Hook-Satz auswertet."""
    payload = json.dumps({
        "session_id": "test", "hook_event_name": "PreToolUse",
        "tool_name": "Bash", "tool_input": {"command": befehl},
        "cwd": str(REPO),
    })
    env = dict(os.environ, CLAUDE_PROJECT_DIR=str(REPO))
    for hook in _hook_befehle():
        r = subprocess.run(["sh", "-c", hook], input=payload, text=True,
                           capture_output=True, env=env, cwd=str(REPO), timeout=20)
        if r.returncode == 2:
            return 2, r.stderr
        assert r.returncode == 0, f"Hook-Fehler {r.returncode}: {r.stderr}"
    return 0, ""


GESPERRT_GIT = [
    "ssh burrito@evidora.eu 'cd /opt/Evidora && git pull'",
    'ssh burrito@evidora.eu "cd /opt/Evidora && git pull --ff-only"',
    "ssh burrito@evidora.eu 'cd /opt/Evidora && git fetch && git merge origin/main'",
    "ssh burrito@evidora.eu 'cd /opt/Evidora && git fetch origin && git reset --hard origin/main'",
    "ssh burrito@evidora.eu git -C /opt/Evidora pull",
    "ssh -o ConnectTimeout=5 -p 22 burrito@evidora.eu 'cd /opt/Evidora/website && git pull && ./deploy.sh'",
    "ssh burrito@evidora.eu \"bash -lc 'cd /opt/Evidora; git pull'\"",
    "git status && ssh burrito@evidora.eu 'cd /opt/Evidora && git pull'",
    "ssh burrito@evidora.eu <<'EOF'\ncd /opt/Evidora\ngit pull\n./deploy.sh\nEOF",
    "ssh evidora 'git -C /opt/Evidora rebase origin/main'",
    "cd /opt/Evidora && git pull",
    "false && ssh burrito@evidora.eu 'cd /opt/Evidora && git pull'",
]

GESPERRT_PYTHON = [
    "ssh burrito@evidora.eu 'docker exec -it evidora-backend-1 python3 -c \"import main\"'",
    "docker exec -it evidora-backend-1 python3 -c 'print(1)'",
    'docker exec evidora-backend-1 sh -c "python /app/x.py"',
    "docker exec evidora-backend-1 bash -c 'cd /app && python -m pytest tests/'",
    "docker exec -u root -w /app evidora-backend-1 pytest -q",
    "docker exec evidora-backend-1 /usr/local/bin/python3.12 -m services.ner",
    "ssh burrito@evidora.eu 'cd /opt/Evidora/website && docker compose exec backend python -c \"import services.ner\"'",
    "ssh burrito@evidora.eu 'cd /opt/Evidora/website && docker compose exec -T backend sh -c \"python3 -m json.tool x\"'",
    "docker-compose exec backend python3 x.py",
    "docker exec evidora-backend-1 env PYTHONPATH=/app timeout 30 python3 x.py",
    "echo 'import main' | ssh burrito@evidora.eu docker exec -i evidora-backend-1 python3",
    "ssh burrito@evidora.eu \"docker exec evidora-backend-1 sh -c 'ls && python3 -V'\"",
    "docker container exec evidora-backend-1 pip list",
    "false && docker exec evidora-backend-1 python3 -c 1",
]

ERLAUBT = [
    "ssh burrito@evidora.eu 'cd /opt/Evidora/website && ./deploy.sh'",
    "ssh burrito@evidora.eu 'cd /opt/Evidora/website && ./deploy.sh --dry-run'",
    "ssh burrito@evidora.eu 'cd /opt/Evidora/website && ./deploy.sh --force-build'",
    "ssh burrito@evidora.eu 'cd /opt/Evidora && git status && git log --oneline -3'",
    "ssh burrito@evidora.eu 'cd /opt/Evidora && git fetch && git log HEAD..origin/main --oneline'",
    # lokal pullen gehoert zum /deploy-Ablauf
    "git switch main && git pull --ff-only && git log --oneline -1",
    "git pull --ff-only && ssh burrito@evidora.eu 'cd /opt/Evidora/website && ./deploy.sh'",
    # Shell-Befehle im Container
    "docker exec evidora-backend-1 grep -rn python /app/requirements.txt",
    "ssh burrito@evidora.eu 'docker exec evidora-backend-1 ls /usr/local/bin/python3'",
    "ssh burrito@evidora.eu 'docker exec evidora-backend-1 sha256sum /app/main.py'",
    "docker exec evidora-backend-1 sh -c 'grep -c python /app/Dockerfile | wc -l'",
    # Wartungs-Container ist der richtige Ort fuer Python
    ("ssh burrito@evidora.eu 'cd /opt/Evidora/website && flock -w 600 -E 75 "
     "/home/burrito/evidora-logs/.wartung.lock docker compose --profile wartung "
     "run --rm -T -v /tmp/skript.py:/tmp/skript.py:ro wartung python3 /tmp/skript.py'"),
    # Diagnose aus dem /deploy-Skill
    ("ssh burrito@evidora.eu 'docker inspect -f \"{{.State.Health.Status}} {{.State.StartedAt}}\" evidora-backend-1;\n"
     "  docker logs -t evidora-backend-1 2>&1 | grep -E \"Waiting for application startup|Application startup complete\" ;\n"
     "  docker logs --tail 40 evidora-backend-1 2>&1 | cut -c1-200'"),
    # Python in einem anderen Container oder lokal
    "docker exec evidora-frontend-1 python3 -V",
    "cd website/backend && python3 -m pytest tests/test_fmt.py",
    "python3 -c 'print(\"docker exec evidora-backend-1\")'",
]


@pytest.mark.parametrize("befehl", GESPERRT_GIT)
def test_git_update_auf_server_gesperrt(befehl):
    code, meldung = _laeuft(befehl)
    assert code == 2, f"nicht gesperrt: {befehl!r}"
    assert "deploy.sh" in meldung


@pytest.mark.parametrize("befehl", GESPERRT_PYTHON)
def test_python_im_backend_gesperrt(befehl):
    code, meldung = _laeuft(befehl)
    assert code == 2, f"nicht gesperrt: {befehl!r}"
    assert "wartung" in meldung and "von Hand" in meldung


GESPERRT_ZWEITES_BACKEND = [
    # docker compose run backend -- mit jedem Befehl, nicht nur Python
    "ssh burrito@evidora.eu 'cd /opt/Evidora/website && docker compose run --rm backend ls /app'",
    "docker compose run --rm -T backend python3 -c 1",
    "docker compose run -T --rm --entrypoint sh backend -c 'echo hallo'",
    "docker compose run --entrypoint=/bin/sh -e X=1 -v /tmp/a:/tmp/a:ro backend",
    "docker compose -f website/docker-compose.yml run --rm backend true",
    "docker compose --file=docker-compose.yml -p evidora run -d backend",
    "docker-compose run --rm backend echo hallo",
    # --profile wartung allein macht backend nicht zur Wartung
    "docker compose --profile wartung run --rm backend python3 x.py",
    "ssh burrito@evidora.eu \"sh -c 'cd /opt/Evidora/website && docker compose run --rm -T backend sh'\"",
    "sh -c 'docker compose run --no-deps backend uvicorn main:app'",
    # docker run mit dem Backend-Image (website/docker-compose.yml: evidora-backend)
    "docker run --rm evidora-backend:latest python3 -c 1",
    "docker run --rm -it -m 3g --entrypoint sh evidora-backend",
    "ssh burrito@evidora.eu 'docker run --rm -v /opt/Evidora/website/backend/data:/app/data evidora-backend:latest ls'",
    "docker container run -d --name probe evidora-backend",
    "docker run --rm docker.io/library/evidora_backend:latest true",
    "false && docker compose run --rm backend true",
]

ERLAUBT_RUN = [
    "docker compose --profile wartung run --rm -T wartung python3 /tmp/skript.py",
    "docker compose -f docker-compose.yml --profile wartung run --rm -T -v /tmp/b.py:/tmp/b.py:ro wartung python3 /tmp/b.py",
    ("ssh burrito@evidora.eu 'cd /opt/Evidora/website && flock -w 600 -E 75 "
     "/home/burrito/evidora-logs/.wartung.lock docker compose --profile wartung run --rm -T "
     "--entrypoint sh wartung -c \"ls /app\"'"),
    "docker compose run --rm frontend ls",
    "docker run --rm alpine echo hallo",
    "docker run --rm python:3.12-slim python3 -V",
    "docker compose up -d backend",
    "docker compose logs --tail 40 backend",
    "docker image ls evidora-backend",
]


@pytest.mark.parametrize("befehl", GESPERRT_ZWEITES_BACKEND)
def test_zweites_backend_gesperrt(befehl):
    code, meldung = _laeuft(befehl)
    assert code == 2, f"nicht gesperrt: {befehl!r}"
    assert "zweiter Backend-Container" in meldung and "wartung" in meldung


@pytest.mark.parametrize("befehl", ERLAUBT_RUN)
def test_run_erlaubt(befehl):
    code, meldung = _laeuft(befehl)
    assert code == 0, f"faelschlich gesperrt: {befehl!r}: {meldung}"


@pytest.mark.parametrize("befehl", ERLAUBT)
def test_erlaubt(befehl):
    code, meldung = _laeuft(befehl)
    assert code == 0, f"faelschlich gesperrt: {befehl!r}: {meldung}"


def test_andere_werkzeuge_unberuehrt():
    payload = json.dumps({"tool_name": "Write",
                          "tool_input": {"command": "cd /opt/Evidora && git pull"}})
    env = dict(os.environ, CLAUDE_PROJECT_DIR=str(REPO))
    for hook in _hook_befehle():
        r = subprocess.run(["sh", "-c", hook], input=payload, text=True,
                           capture_output=True, env=env, timeout=20)
        assert r.returncode == 0


def test_hook_ist_verdrahtet():
    assert _hook_befehle(), ".claude/settings.json hat keinen PreToolUse-Bash-Hook"
