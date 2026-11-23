"""Allowlist für lesende Befehle in ``.claude/settings.json`` (modellfrei).

Die getrackte Projektdatei ``.claude/settings.json`` erlaubt eine kurze Liste
rein lesender, lokaler Befehle ohne Rückfrage (``permissions.allow``). Anders
als ``.claude/settings.local.json`` liegt sie in jedem Worktree. Dieser Test
hält drei Dinge fest:

1. **Genau der Katalog.** Jeder Eintrag steht unten in ``KATALOG``, mit einem
   Satz Begründung. Ein neuer Eintrag braucht eine bewusste Änderung hier.
2. **Kein verbotenes Muster ist erlaubt.** Keine Regel deckt ssh, scp,
   docker, deploy.sh, git push/pull/merge/rebase/reset, gh pr merge, rm,
   schreibendes curl oder einen anderen schreibenden oder ausführenden
   Befehl, auch nicht als Präfix oder über einen Platzhalter. Geprüft wird
   nach den Regeln der Claude-Code-Doku (code.claude.com/docs/en/permissions):
   Ein zusammengesetzter Befehl ist nur erlaubt, wenn jeder Teilbefehl erlaubt
   ist, auch in ``$(...)``, Backticks und ``<(...)``. ``X *`` passt auch auf
   das blanke ``X``, ``*`` darf an jeder Stelle stehen, und ``:*`` ist dasselbe
   wie `` *``. Wrapper wie ``timeout`` und ``nohup`` werden abgestreift.
   Umleitungsziele prüft Claude Code getrennt, wie einen Edit: Eine Regel
   erlaubt den Befehl, nicht das Ziel.
3. **Der Hook bleibt wirksam.** ``.claude/hooks/server_regeln.py`` hängt
   weiter als PreToolUse-Hook an Bash, ``disableAllHooks`` ist nicht gesetzt,
   und der Hook sperrt auch dann, wenn ein Befehl mit einem erlaubten Präfix
   beginnt. Laut Doku läuft PreToolUse vor der Rechteprüfung. Eine
   Allow-Regel kann den Hook also nicht überspringen.

Gegen den alten Stand (ohne Allowlist) sind die Tests rot, die den Katalog und
die erlaubten Beispiele prüfen. Die Sperr- und Hook-Tests sind dort grün:
Ohne Regel war auch nichts Verbotenes erlaubt, und der Hook hing schon.

Modellfrei: Standardbibliothek, liest zwei Dateien, startet den Hook als
Unterprozess wie Claude Code (Hook-JSON auf stdin, Exit 2 = gesperrt).
"""

import importlib.util
import json
import os
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
SETTINGS = REPO / ".claude" / "settings.json"
HOOK_DATEI = REPO / ".claude" / "hooks" / "server_regeln.py"

# Der Shell-Zerleger des Hooks (Quoting, Ketten, $(...), Here-Docs) zerlegt
# hier auch die Befehle in Teilbefehle. Er ist in test_hook_server_regeln.py
# mit Quoting-Fallen geprüft.
_spec = importlib.util.spec_from_file_location("server_regeln", HOOK_DATEI)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


# ---------------------------------------------------------------------------
# Katalog: jeder erlaubte Eintrag mit Begründung
# ---------------------------------------------------------------------------

KATALOG = {
    "Bash(grep *)": "Durchsucht Dateien und gibt Treffer aus; grep hat keine "
                    "Option, die schreibt oder ein Programm startet.",
    "Bash(head *)": "Gibt den Anfang einer Datei oder Eingabe aus.",
    "Bash(echo *)": "Gibt nur Text aus; eine Umleitung in eine Datei prüft "
                    "Claude Code getrennt wie einen Edit.",
    "Bash(cat *)": "Gibt Dateien aus; schreiben kann cat nur über eine "
                   "Umleitung, und die prüft Claude Code getrennt.",
    "Bash(tail *)": "Gibt das Ende einer Datei aus, auch fortlaufend mit -f.",
    "Bash(gh pr checks *)": "Liest den CI-Stand eines PR von GitHub; keine "
                            "Option ändert etwas, --web öffnet nur den Browser.",
    "Bash(cut *)": "Schneidet Spalten oder Zeichen aus der Eingabe aus.",
    "Bash(ls *)": "Listet Verzeichnisse.",
    "Bash(gh run view *)": "Liest einen CI-Lauf und sein Log von GitHub.",
    "Bash(wc *)": "Zählt Zeilen, Wörter und Bytes.",
    "Bash(gh pr view *)": "Liest Titel, Status und Text eines PR von GitHub.",
    "Bash(gh run list *)": "Listet CI-Läufe von GitHub.",
    "Bash(sleep *)": "Wartet nur, etwa vor einem gh pr checks.",
    "Bash(gh pr diff *)": "Liest den Diff eines PR von GitHub.",
    "Bash(tr *)": "Ersetzt oder löscht Zeichen zwischen Eingabe und Ausgabe.",
    "Bash(gh pr list *)": "Listet PRs von GitHub.",
}

# Eine Regel ist ein Programm (mit Unterbefehlen) und höchstens ein
# abschließendes " *". Kein Pfad, kein Shell-Operator, kein Anführungszeichen,
# kein Platzhalter vor oder zwischen den Wörtern (Bash(git * main) würde
# git push origin main decken), keine Form ":*".
FORM = re.compile(r"^Bash\([a-z][a-z0-9._-]*(?: [a-z][a-z0-9._-]*)*(?: \*)?\)$")


def _settings():
    return json.loads(SETTINGS.read_text(encoding="utf-8"))


def _allow():
    return list((_settings().get("permissions") or {}).get("allow") or [])


# ---------------------------------------------------------------------------
# Matching wie in der Doku beschrieben
# ---------------------------------------------------------------------------

WRAPPER = {"timeout", "time", "nice", "nohup", "stdbuf", "command",
           "builtin", "noglob", "xargs"}
SHELL_SCHLUESSEL = {"do", "then", "else", "elif", "!", "{", "}"}
UMLEITUNG = re.compile(r"^(?:\d|&)?(?:>>?|<|>&|<&)(.*)$")


def _ohne_umleitungen(woerter):
    rein, i = [], 0
    while i < len(woerter):
        m = UMLEITUNG.match(woerter[i])
        if m:
            # "> datei" sind zwei Wörter, "2>/dev/null" und "2>&1" eines
            i += 2 if m.group(1) == "" else 1
            continue
        rein.append(woerter[i])
        i += 1
    return rein


def _ohne_vorsatz(woerter):
    """Zuweisungen, Shell-Schlüsselwörter und Wrapper vorn abstreifen.

    Das ist strenger als nötig: Was hinter einem Wrapper steht, muss selbst
    erlaubt sein. ``for``/``while``/``if`` bleiben stehen und sind nie erlaubt.
    """
    w = list(woerter)
    while w:
        kopf = os.path.basename(w[0])
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", w[0]) or w[0] in SHELL_SCHLUESSEL:
            w = w[1:]
        elif kopf in WRAPPER:
            w = w[1:]
            while w and w[0].startswith("-"):
                w = w[1:]
            if kopf == "timeout" and w:
                w = w[1:]          # Dauer
        else:
            break
    return w


def teilbefehle(befehl, tiefe=0):
    """Die Teilbefehle, die Claude Code einzeln prüft: getrennt an ``&&``,
    ``||``, ``;``, ``|``, ``&``, Klammern und Zeilenenden, dazu der Inhalt
    von ``$(...)`` und Backticks. ``<(...)`` zerfällt an den Klammern."""
    aus = []
    for seg in hook.zerlege(befehl):
        woerter = _ohne_vorsatz(_ohne_umleitungen(seg["woerter"]))
        if woerter:
            aus.append(" ".join(woerter))
        if tiefe < 8:
            for inner in seg["subst"]:
                aus += teilbefehle(inner, tiefe + 1)
    return aus


def _muster(regel):
    innen = regel[len("Bash("):-1]
    if innen.endswith(":*"):
        innen = innen[:-2] + " *"
    if innen.endswith(" *") and "*" not in innen[:-2]:
        # "X *" passt auf "X" und auf "X ..." (nicht auf "Xyz")
        return re.compile(re.escape(innen[:-2]) + r"(?: .*)?", re.S)
    return re.compile(".*".join(re.escape(t) for t in innen.split("*")), re.S)


def erlaubt_teil(regeln, teil):
    if "Bash" in regeln:
        return True                      # Werkzeug-Regel ohne Klammer: alles
    return any(_muster(r).fullmatch(teil) for r in regeln
               if r.startswith("Bash(") and r.endswith(")"))


def ganz_erlaubt(regeln, befehl):
    """True, wenn jeder Teilbefehl auf eine Regel passt (keine Rückfrage)."""
    try:
        teile = teilbefehle(befehl)
    except hook.ZerlegFehler:
        return "Bash" in regeln          # unzerlegbar: Claude Code fragt nach
    return bool(teile) and all(erlaubt_teil(regeln, t) for t in teile)


# ---------------------------------------------------------------------------
# Beispiele
# ---------------------------------------------------------------------------

# Formen aus den Evidora-Transkripten (Oktober/November 2026), ohne Inhalte.
# Gegen den alten Stand (keine Allowlist) ist jedes rot.
ERLAUBT_BEISPIELE = [
    "grep -n 'def pruefe' .claude/hooks/server_regeln.py | head -20",
    "grep -rn source_url_2 website/backend/data 2>/dev/null | wc -l",
    "cat website/deploy.sh | head -40",
    "ls -la website/backend/data | wc -l",
    "ls",
    "echo '---' && tail -5 website/crontab.prod",
    "cut -c1-80 README.md | tr -d '\\r' | head -5",
    "gh pr checks 325",
    "sleep 60 && gh pr checks 325",
    "timeout 900 gh pr checks 325 --watch",
    "gh run view 123456 --log-failed | tail -50",
    "gh run view $(gh run list --limit 1 --json databaseId -q '.[0].databaseId') --log | grep -n FAILED",
    "gh pr view 325 --json state,mergeable -q .state",
    "gh pr diff 325 | head -100",
    "gh pr list --state open",
]

# Was nie ohne Rückfrage laufen darf. Je Gruppe die Verbote aus dem Auftrag,
# dazu verwandte schreibende oder ausführende Befehle.
VERBOTEN = [
    # Server
    "ssh burrito@evidora.eu uptime",
    "ssh burrito@evidora.eu 'cd /opt/Evidora/website && ./deploy.sh'",
    "ssh evidora 'docker ps'",
    "scp skript.py burrito@evidora.eu:/tmp/",
    "sftp burrito@evidora.eu",
    "rsync -a website/ burrito@evidora.eu:/tmp/w/",
    "env FOO=1 ssh burrito@evidora.eu uptime",
    "timeout 60 ssh burrito@evidora.eu uptime",
    # docker, auch lesend
    "docker ps",
    "docker logs --tail 40 evidora-backend-1",
    "docker exec evidora-backend-1 ls /app",
    "docker compose --profile wartung run --rm -T wartung python3 /tmp/s.py",
    "docker-compose ps",
    "dd if=/dev/zero of=/tmp/x bs=1 count=1",
    # deploy.sh
    "./deploy.sh",
    "website/deploy.sh --force-build",
    "bash website/deploy.sh --dry-run",
    "nohup ./deploy.sh",
    # git, schreibend
    "git push",
    "git push origin main",
    "git push --force-with-lease",
    "git pull",
    "git pull --ff-only",
    "git merge origin/main",
    "git rebase origin/main",
    "git reset --hard origin/main",
    "git reset HEAD~1",
    "git -C /opt/Evidora pull",
    "git commit -m x",
    "git add -A",
    "git checkout main",
    "git switch main",
    "git stash",
    "git fetch origin",
    "git clean -fdx",
    "git branch -D alt",
    "git tag -d v1",
    "git config user.name x",
    "git worktree remove x",
    "git log --output=/tmp/log.txt",
    "git diff --output=/tmp/d.patch",
    # gh, schreibend
    "gh pr merge 325 --squash --delete-branch",
    "gh pr create --fill",
    "gh pr close 325",
    "gh pr comment 325 -b x",
    "gh pr edit 325 --title x",
    "gh pr ready 325",
    "gh pr review 325 --approve",
    "gh pr checkout 325",
    "gh api -X POST repos/o/r/issues -f title=x",
    "gh api repos/o/r/pulls/325/merge -X PUT",
    "gh run rerun 123",
    "gh run cancel 123",
    "gh run delete 123",
    "gh run download 123",
    "gh workflow run test.yml",
    "gh release create v1",
    "gh repo edit --visibility public",
    "gh secret set X",
    "gh issue create -t x -b y",
    # rm und andere Schreiber
    "rm -rf website/backend/data",
    "rm x",
    "rmdir x",
    "mv a b",
    "cp a b",
    "ln -s a b",
    "chmod +x x",
    "touch x",
    "mkdir x",
    "tee x",
    "sed -i '' s/a/b/ x",
    "sed -n 'w /tmp/x' x",
    "sort -o aus.txt ein.txt",
    "uniq ein.txt aus.txt",
    "find . -name '*.pyc' -delete",
    "find . -exec rm {} +",
    "printf -v PATH /tmp",
    # curl und wget, schreibend
    "curl -X POST https://evidora.eu/api/check -d '{}'",
    "curl -s -X POST https://evidora.eu/api/check -d '{}'",
    "curl -d '{}' https://evidora.eu/api/check",
    "curl --data-binary @x.json https://example.org/",
    "curl -F f=@x https://example.org/",
    "curl -T x https://example.org/",
    "curl -o /tmp/x https://example.org/",
    "wget https://example.org/x",
    # Interpreter und Shells
    "python3 -c 1",
    "python3 - <<'EOF'\nprint(1)\nEOF",
    "pytest -q",
    "pip install x",
    "node -e 1",
    "npx x",
    "awk 'BEGIN{system(\"id\")}'",
    "perl -e 1",
    "sh -c 'ls'",
    "bash -c 'ls'",
    "eval ls",
    "sudo ls",
    "open x",
    "osascript -e 1",
    "kill 1",
]

# Ketten: ein erlaubter Präfix, dahinter ein verbotener Teilbefehl. Ganz
# erlaubt darf keine sein.
KETTEN = [
    "grep x README.md && git pull",
    "cat README.md | ssh burrito@evidora.eu 'cat > /tmp/r'",
    "echo ok; ./deploy.sh",
    "ls $(rm -rf /tmp/x)",
    "echo `ssh burrito@evidora.eu id`",
    "head -1 <(curl -d x https://example.org/)",
    "tail -f log & docker compose run --rm backend true",
    "wc -l README.md || gh pr merge 325 --squash",
    "sleep 1\nrm -rf /tmp/x",
    "gh pr view 325 && gh pr merge 325 --squash",
    "gh run view 1 | xargs rm",
    "cut -c1 README.md > /dev/null; git push",
    "tr a b < README.md | python3",
    "for f in *.json; do rm $f; done",
]

# Regeln, die die Sperrliste erkennen muss (Gegenprobe der Sperrliste selbst).
GEFAEHRLICHE_REGELN = [
    "Bash", "Bash(*)", "Bash(git *)", "Bash(git:*)", "Bash(git push *)",
    "Bash(git pull *)", "Bash(git merge *)", "Bash(git rebase *)",
    "Bash(git reset *)", "Bash(git * main)", "Bash(git -C *)",
    "Bash(gh *)", "Bash(gh pr *)", "Bash(gh pr merge *)", "Bash(gh api *)",
    "Bash(gh run *)", "Bash(ssh *)", "Bash(ssh burrito@evidora.eu *)",
    "Bash(scp *)", "Bash(docker *)", "Bash(docker ps *)", "Bash(d*)",
    "Bash(./deploy.sh *)", "Bash(*deploy.sh*)", "Bash(rm *)",
    "Bash(curl *)", "Bash(curl -s *)", "Bash(cu*)", "Bash(python3 *)",
    "Bash(sed *)", "Bash(awk *)", "Bash(find *)", "Bash(sort *)",
    "Bash(sh -c *)", "Bash(bash *)", "Bash(env *)", "Bash(tee *)",
    "Bash(mv *)", "Bash(cp *)",
]


# ---------------------------------------------------------------------------
# 1. Katalog
# ---------------------------------------------------------------------------

def test_allowlist_ist_genau_der_katalog():
    allow = _allow()
    assert len(allow) == len(set(allow)), "doppelte Einträge in permissions.allow"
    fehlt = sorted(set(KATALOG) - set(allow))
    unbegruendet = sorted(set(allow) - set(KATALOG))
    assert not fehlt and not unbegruendet, (
        f"fehlt in .claude/settings.json: {fehlt}; "
        f"ohne Eintrag im KATALOG (Begründung fehlt): {unbegruendet}")


def test_jeder_katalog_eintrag_hat_eine_begruendung():
    for regel, grund in KATALOG.items():
        assert len(grund) >= 15, f"{regel}: Begründung fehlt oder ist zu kurz"


@pytest.mark.parametrize("regel", sorted(KATALOG))
def test_regel_hat_die_sichere_form(regel):
    assert FORM.match(regel), (
        f"{regel}: nur Programm + Unterbefehle + optional ' *' am Ende")


def test_kein_modus_der_alles_erlaubt():
    perm = _settings().get("permissions") or {}
    assert perm.get("defaultMode") != "bypassPermissions"


@pytest.mark.parametrize("befehl", ERLAUBT_BEISPIELE)
def test_lesende_form_ist_erlaubt(befehl):
    assert ganz_erlaubt(_allow(), befehl), f"nicht erlaubt: {befehl!r}"


# ---------------------------------------------------------------------------
# 2. Kein verbotenes Muster
# ---------------------------------------------------------------------------

def _ein_teil_erlaubt(regeln, befehl):
    try:
        return any(erlaubt_teil(regeln, t) for t in teilbefehle(befehl))
    except hook.ZerlegFehler:
        return "Bash" in regeln


@pytest.mark.parametrize("befehl", VERBOTEN)
def test_verbotener_befehl_passt_auf_keine_regel(befehl):
    """Auch als Teil einer Kette darf er auf keine Regel passen."""
    assert not _ein_teil_erlaubt(_allow(), befehl), (
        f"die Allowlist erlaubt {befehl!r} ohne Rückfrage")


@pytest.mark.parametrize("befehl", KETTEN)
def test_kette_ist_nicht_ganz_erlaubt(befehl):
    assert not ganz_erlaubt(_allow(), befehl), (
        f"die Allowlist erlaubt die Kette {befehl!r} ohne Rückfrage")


@pytest.mark.parametrize("regel", GEFAEHRLICHE_REGELN)
def test_sperrliste_erkennt_gefaehrliche_regel(regel):
    """Gegenprobe: Käme diese Regel dazu, wäre ein verbotener Befehl frei."""
    regeln = list(KATALOG) + [regel]
    frei = ([b for b in VERBOTEN if _ein_teil_erlaubt(regeln, b)]
            + [k for k in KETTEN if ganz_erlaubt(regeln, k)])
    assert frei, f"die Sperrliste erkennt {regel} nicht"


@pytest.mark.parametrize("regel, befehl, erwartet", [
    # Beispiele aus code.claude.com/docs/en/permissions
    ("Bash(ls *)", "ls -la", True),
    ("Bash(ls *)", "ls", True),
    ("Bash(ls *)", "lsof", False),
    ("Bash(ls*)", "lsof", True),
    ("Bash(git * main)", "git push origin main", True),
    ("Bash(git * main)", "git log", False),
    ("Bash(ls:*)", "ls -la", True),
    ("Bash(safe-cmd *)", "safe-cmd && other-cmd", False),
    # Annahme, strenger als die Doku es ausspricht: Was in $(...) steht,
    # muss selbst erlaubt sein.
    ("Bash(safe-cmd *)", "safe-cmd $(other-cmd)", False),
    # Das Umleitungsziel prüft Claude Code getrennt wie einen Edit; die
    # Regel erlaubt nur den Befehl.
    ("Bash(safe-cmd *)", "safe-cmd x > /tmp/aus.txt", True),
])
def test_matching_wie_in_der_doku(regel, befehl, erwartet):
    assert ganz_erlaubt([regel], befehl) is erwartet


# ---------------------------------------------------------------------------
# 3. Hook bleibt verdrahtet und wirksam
# ---------------------------------------------------------------------------

def _hook_befehle():
    befehle = []
    for eintrag in (_settings().get("hooks") or {}).get("PreToolUse", []):
        if eintrag.get("matcher") in ("Bash", "*", ""):
            for h in eintrag.get("hooks", []):
                if h.get("type") == "command":
                    befehle.append(h["command"])
    return befehle


def _hook_sperrt(befehl):
    """(exit, stderr) wie Claude Code den PreToolUse-Hook aufruft."""
    payload = json.dumps({
        "session_id": "test", "hook_event_name": "PreToolUse",
        "tool_name": "Bash", "tool_input": {"command": befehl},
        "cwd": str(REPO),
    })
    env = dict(os.environ, CLAUDE_PROJECT_DIR=str(REPO))
    for h in _hook_befehle():
        r = subprocess.run(["sh", "-c", h], input=payload, text=True,
                           capture_output=True, env=env, cwd=str(REPO), timeout=20)
        if r.returncode == 2:
            return 2, r.stderr
        assert r.returncode == 0, f"Hook-Fehler {r.returncode}: {r.stderr}"
    return 0, ""


def test_hook_bleibt_verdrahtet():
    daten = _settings()
    assert daten.get("disableAllHooks") is not True, "disableAllHooks schaltet den Hook ab"
    befehle = _hook_befehle()
    assert any(".claude/hooks/server_regeln.py" in b for b in befehle), (
        f"PreToolUse-Bash-Hook server_regeln.py fehlt: {befehle}")
    eintraege = (daten.get("hooks") or {}).get("PreToolUse", [])
    assert any(e.get("matcher") == "Bash" for e in eintraege)
    assert HOOK_DATEI.is_file()


def _kopf(regel):
    return regel[len("Bash("):-1].removesuffix(" *")


@pytest.mark.parametrize("regel", sorted(KATALOG))
def test_hook_sperrt_hinter_erlaubtem_praefix(regel):
    kopf = _kopf(regel)
    faelle = [
        (f"{kopf} 1 && ssh burrito@evidora.eu 'cd /opt/Evidora && git pull'", "deploy.sh"),
        (f"{kopf} $(docker exec evidora-backend-1 python3 -c 1)", "wartung"),
        (f"{kopf} 1 | docker compose run --rm backend true", "zweiter Backend-Container"),
    ]
    for befehl, kennwort in faelle:
        code, meldung = _hook_sperrt(befehl)
        assert code == 2, f"Hook sperrt nicht: {befehl!r}"
        assert kennwort in meldung, f"falsche Meldung für {befehl!r}: {meldung}"
