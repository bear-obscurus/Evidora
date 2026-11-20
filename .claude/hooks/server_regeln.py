#!/usr/bin/env python3
"""PreToolUse-Hook auf Bash: setzt zwei harte Regeln aus CLAUDE.md durch.

1. Kein ``git pull`` (auch ``git merge``, ``git rebase``, ``git reset --hard
   origin/...``) auf dem Server. ``deploy.sh`` pullt selbst und leitet seine
   Aktion aus dem ab, was der Pull bringt; nach einem Pull von Hand baut es
   nichts.
2. Kein Python im Prod-Backend-Container: jeder Python-Aufruf per
   ``docker exec`` / ``docker compose exec`` in ``evidora-backend-1`` bzw. im
   Dienst ``backend`` ist gesperrt, auch hinter ``sh -c``. Am 08.08.2026 riss
   ein solcher Prozess den ganzen Host mit.
3. Kein zweiter Backend-Container: ``docker compose run ... backend`` und
   ``docker run`` mit dem Image ``evidora-backend`` starten einen zweiten
   Container mit eigener 3-GB-Grenze. Nur der Dienst ``wartung`` (224m) ist frei.

Der Hook liest das Hook-JSON von stdin. Gesperrt wird mit Exit 2 und einer
Begruendung auf stderr; alles andere endet mit Exit 0.

Nur Standardbibliothek, Python >= 3.8. Der Befehl wird wie von einer Shell
zerlegt (Quoting, ``&&``/``;``/``|``-Ketten, Here-Docs), und die Strings
hinter ``ssh <host>``, ``sh -c``/``bash -c`` und ``$(...)`` werden rekursiv
geprueft. Laesst sich ein Befehl nicht zerlegen (offene Anfuehrungszeichen),
greift eine grobe Textpruefung: im Zweifel wird gesperrt.
"""

import json
import os
import re
import sys

# --------------------------------------------------------------------------
# Meldungen
# --------------------------------------------------------------------------

MELDUNG_GIT = (
    "Gesperrt (Hook .claude/hooks/server_regeln.py, harte Regel aus CLAUDE.md): "
    "kein git pull/merge/rebase/reset auf dem Server vor deploy.sh. "
    "deploy.sh pullt selbst und leitet seine Aktion aus dem Pull ab; nach einem "
    "Pull von Hand baut es nichts. Richtig ist: "
    "ssh burrito@evidora.eu 'cd /opt/Evidora/website && ./deploy.sh' "
    "(--dry-run zeigt nur die Entscheidung). Hat schon jemand von Hand gepullt: "
    "./deploy.sh --force-build. Lesende git-Befehle (status, log, fetch) bleiben erlaubt."
)

MELDUNG_PYTHON = (
    "Gesperrt (Hook .claude/hooks/server_regeln.py, harte Regel aus CLAUDE.md): "
    "kein Python im Prod-Backend-Container (docker exec / docker compose exec in "
    "evidora-backend-1 bzw. Dienst backend) - jeder Python-Aufruf, nicht nur der "
    "mit Modell-Imports. Am 08.08.2026 riss ein solcher Prozess den ganzen Host mit. "
    "Shell-Befehle wie ls, grep, sha256sum im Container bleiben erlaubt. "
    "Eine leichte Pruefung (reine Logik, keine Modell-Imports) gibt der Nutzer "
    "einzeln von Hand frei: bitte ihn, den Befehl selbst auszufuehren. "
    "Messlaeufe mit Python gehoeren in den Wartungs-Container: "
    "cd /opt/Evidora/website && flock -w 600 -E 75 /home/burrito/evidora-logs/.wartung.lock "
    "docker compose --profile wartung run --rm -T ... wartung python3 /tmp/skript.py"
)

MELDUNG_ZWEITES_BACKEND = (
    "Gesperrt (Hook .claude/hooks/server_regeln.py, harte Regel aus CLAUDE.md): "
    "kein zweiter Backend-Container. docker compose run ... backend und docker run "
    "mit dem Image evidora-backend starten einen zweiten Container mit eigener "
    "3-GB-Grenze neben dem laufenden Backend - dieselbe Fehlerklasse wie am "
    "08.08.2026, als der Host mitgerissen wurde. Das gilt fuer jeden Befehl, auch "
    "mit --rm, -T oder --entrypoint. Erlaubt ist nur der Wartungs-Container "
    "(224m): cd /opt/Evidora/website && flock -w 600 -E 75 "
    "/home/burrito/evidora-logs/.wartung.lock docker compose --profile wartung "
    "run --rm -T ... wartung <befehl>. Braucht es wirklich ein zweites Backend, "
    "fuehrt der Nutzer den Befehl selbst aus."
)

# --------------------------------------------------------------------------
# Muster
# --------------------------------------------------------------------------

SERVER_HOST = re.compile(r"evidora", re.I)          # evidora.eu, burrito@evidora.eu, Alias "evidora"
SERVER_PFAD = re.compile(r"/opt/Evidora\b")
BACKEND_CONTAINER = re.compile(r"backend", re.I)     # evidora-backend-1, website_backend_1, Dienst backend

PYTHON_PROGRAMM = re.compile(
    r"^(python[0-9.]*|pypy[0-9.]*|ipython[0-9.]*|pytest|py\.test|pip[0-9.]*|"
    r"uvicorn|gunicorn|hypercorn)$"
)
SHELLS = {"sh", "bash", "dash", "zsh", "ash", "ksh", "busybox"}

# Programme, die nur einen anderen Befehl starten.
VORSATZ_OHNE_ARG = {"exec", "nohup", "time", "command", "builtin", "sudo", "doas",
                    "stdbuf", "ionice", "chrt", "setsid", "unbuffer", "xargs"}



SSH_OPTS_MIT_ARG = set("bcDEeFIiJLlmOopQRSWwB")
DOCKER_GLOBAL_MIT_ARG = {"-H", "--host", "-c", "--context", "--config", "-l",
                         "--log-level", "--tlscacert", "--tlscert", "--tlskey"}
DOCKER_EXEC_MIT_ARG = {"-e", "--env", "--env-file", "-u", "--user", "-w",
                       "--workdir", "--detach-keys"}
COMPOSE_GLOBAL_MIT_ARG = {"-f", "--file", "-p", "--project-name", "--profile",
                          "--project-directory", "--env-file", "--ansi",
                          "--progress", "--parallel"}
COMPOSE_EXEC_MIT_ARG = {"-e", "--env", "-u", "--user", "-w", "--workdir", "--index"}
COMPOSE_RUN_MIT_ARG = {"-e", "--env", "--env-from-file", "-l", "--label", "-p",
                       "--publish", "-u", "--user", "-v", "--volume", "-w",
                       "--workdir", "--name", "--entrypoint", "--cap-add",
                       "--cap-drop", "--pull", "--network", "--link"}
# Image des Backends: Projekt "evidora" + Dienst "backend" mit build: ergibt
# evidora-backend (website/docker-compose.yml; der Dienst wartung nennt es
# ausdruecklich: image: evidora-backend:latest). evidora_backend ist die
# Schreibweise von docker-compose v1.
BACKEND_IMAGE = re.compile(
    r"^(?:[\w.-]+(?::\d+)?/)*evidora[-_]backend(?::[\w.-]+)?(?:@sha256:[0-9a-f]+)?$",
    re.I)
WARTUNG_DIENST = "wartung"
GIT_GLOBAL_MIT_ARG = {"-C", "-c", "--git-dir", "--work-tree", "--namespace",
                      "--exec-path", "--config-env"}


class ZerlegFehler(Exception):
    pass


# --------------------------------------------------------------------------
# Zerlegen
# --------------------------------------------------------------------------

def zerlege(text):
    """Zerlegt einen Shell-Befehl in Segmente.

    Rueckgabe: Liste von Segmenten; jedes Segment ist ein dict mit
    ``woerter`` (Liste von Strings, Quotes entfernt), ``heredoc`` (Text der
    Here-Docs dieses Segments) und ``subst`` (Inhalte von $(...)/`...`).
    """
    segmente = []
    woerter, wort, im_wort = [], [], False
    heredocs_offen = []   # Begrenzer, deren Text nach dem naechsten Zeilenende kommt
    seg = {"woerter": woerter, "heredoc": [], "subst": []}
    i, n = 0, len(text)

    def wort_ende():
        nonlocal wort, im_wort
        if im_wort:
            woerter.append("".join(wort))
        wort, im_wort = [], False

    def segment_ende():
        nonlocal woerter, seg
        wort_ende()
        if seg["woerter"] or seg["heredoc"] or seg["subst"]:
            segmente.append(seg)
        woerter = []
        seg = {"woerter": woerter, "heredoc": [], "subst": []}

    def lies_klammer(start):
        """Liefert (inhalt, ende) fuer $( ... ) ab start (nach '$(')."""
        tiefe, j, q = 1, start, None
        while j < n:
            c = text[j]
            if q:
                if c == "\\" and q == '"':
                    j += 2
                    continue
                if c == q:
                    q = None
            elif c in "'\"":
                q = c
            elif c == "(":
                tiefe += 1
            elif c == ")":
                tiefe -= 1
                if tiefe == 0:
                    return text[start:j], j + 1
            j += 1
        raise ZerlegFehler("offene $(")

    while i < n:
        c = text[i]
        if c == "\\":
            if i + 1 < n and text[i + 1] == "\n":
                i += 2
                continue
            wort.append(text[i + 1] if i + 1 < n else "")
            im_wort = True
            i += 2
            continue
        if c == "'":
            j = text.find("'", i + 1)
            if j < 0:
                raise ZerlegFehler("offenes '")
            wort.append(text[i + 1:j])
            im_wort = True
            i = j + 1
            continue
        if c == '"':
            j, teil = i + 1, []
            while True:
                if j >= n:
                    raise ZerlegFehler('offenes "')
                d = text[j]
                if d == "\\" and j + 1 < n and text[j + 1] in '"\\$`\n':
                    teil.append(text[j + 1])
                    j += 2
                    continue
                if d == '"':
                    break
                if d == "$" and j + 1 < n and text[j + 1] == "(":
                    inhalt, ende = lies_klammer(j + 2)
                    seg["subst"].append(inhalt)
                    teil.append(text[j:ende])
                    j = ende
                    continue
                if d == "`":
                    k = text.find("`", j + 1)
                    if k < 0:
                        raise ZerlegFehler("offenes `")
                    seg["subst"].append(text[j + 1:k])
                    teil.append(text[j:k + 1])
                    j = k + 1
                    continue
                teil.append(d)
                j += 1
            wort.append("".join(teil))
            im_wort = True
            i = j + 1
            continue
        if c == "$" and i + 1 < n and text[i + 1] == "(":
            inhalt, ende = lies_klammer(i + 2)
            seg["subst"].append(inhalt)
            wort.append(text[i:ende])
            im_wort = True
            i = ende
            continue
        if c == "`":
            k = text.find("`", i + 1)
            if k < 0:
                raise ZerlegFehler("offenes `")
            seg["subst"].append(text[i + 1:k])
            im_wort = True
            i = k + 1
            continue
        if c == "#" and not im_wort:
            j = text.find("\n", i)
            i = n if j < 0 else j
            continue
        if c == "<" and text.startswith("<<", i) and not text.startswith("<<<", i):
            wort_ende()
            i += 2
            if i < n and text[i] == "-":
                i += 1
            while i < n and text[i] in " \t":
                i += 1
            m = re.match(r"""(['"]?)([^\s'";&|<>()]+)\1""", text[i:])
            if not m:
                raise ZerlegFehler("Here-Doc ohne Begrenzer")
            heredocs_offen.append((m.group(2), seg))
            i += m.end()
            continue
        if c == "\n":
            if heredocs_offen:
                i += 1
                for begrenzer, ziel in heredocs_offen:
                    zeilen = []
                    while i <= n:
                        j = text.find("\n", i)
                        zeile = text[i:] if j < 0 else text[i:j]
                        i = n + 1 if j < 0 else j + 1
                        if zeile.strip() == begrenzer:
                            break
                        zeilen.append(zeile)
                    ziel["heredoc"].append("\n".join(zeilen))
                heredocs_offen = []
                segment_ende()
                continue
            segment_ende()
            i += 1
            continue
        if c in " \t":
            wort_ende()
            i += 1
            continue
        zwei = text[i:i + 2]
        if zwei in ("&&", "||", "|&", ";;"):
            segment_ende()
            i += 2
            continue
        if c == "&" and i > 0 and text[i - 1] in "<>" or (
                c == "&" and text.startswith("&>", i)):
            wort.append(c)          # Umleitung 2>&1, &>datei
            im_wort = True
            i += 1
            continue
        if c in ";|&()":
            segment_ende()
            i += 1
            continue
        wort.append(c)
        im_wort = True
        i += 1
    segment_ende()
    return segmente


# --------------------------------------------------------------------------
# Pruefen
# --------------------------------------------------------------------------

def programm_start(woerter):
    """Ueberspringt Zuweisungen und Starter (env, timeout, nice ...).

    Liefert den Index des eigentlichen Programms oder None.
    """
    i = 0
    while i < len(woerter):
        w = woerter[i]
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", w):
            i += 1
            continue
        name = os.path.basename(w)
        if name == "env":
            i += 1
            while i < len(woerter) and (woerter[i].startswith("-")
                                        or "=" in woerter[i]):
                if woerter[i] in ("-u", "--unset", "-C", "--chdir", "-S"):
                    i += 1
                i += 1
            continue
        if name in ("timeout", "nice", "taskset"):
            i += 1
            while i < len(woerter) and woerter[i].startswith("-"):
                if woerter[i] in ("-s", "--signal", "-k", "--kill-after", "-n",
                                  "--adjustment"):
                    i += 1
                i += 1
            if name in ("timeout", "taskset") and i < len(woerter):
                i += 1   # Dauer bzw. Maske
            continue
        if name in VORSATZ_OHNE_ARG:
            i += 1
            while i < len(woerter) and woerter[i].startswith("-"):
                i += 1
            continue
        return i
    return None


def shell_c_strings(woerter, start):
    """Fuer ``sh -c 'cmd'``: liefert die Strings hinter -c (sonst [])."""
    rest = woerter[start + 1:]
    for k, w in enumerate(rest):
        if w == "--":
            break
        if w.startswith("-") and not w.startswith("--") and "c" in w[1:]:
            return [" ".join(rest[k + 1:k + 2])] if k + 1 < len(rest) else []
        if not w.startswith("-"):
            break
    return []


def ist_python_aufruf(text, tiefe):
    """True, wenn ``text`` (Befehl im Container) Python startet."""
    try:
        segmente = zerlege(text)
    except ZerlegFehler:
        return bool(re.search(r"\b(python[0-9.]*|pytest|pip[0-9.]*|uvicorn)\b", text))
    for seg in segmente:
        for s in seg["subst"]:
            if tiefe < 8 and ist_python_aufruf(s, tiefe + 1):
                return True
        w = seg["woerter"]
        p = programm_start(w)
        if p is None:
            continue
        name = os.path.basename(w[p])
        if PYTHON_PROGRAMM.match(name):
            return True
        if name in SHELLS:
            for inner in shell_c_strings(w, p) + seg["heredoc"]:
                if tiefe < 8 and ist_python_aufruf(inner, tiefe + 1):
                    return True
    return False


def ueberspringe_opts(woerter, i, mit_arg):
    """Ueberspringt Optionen ab i; liefert den Index des ersten Nicht-Options-Worts."""
    while i < len(woerter):
        w = woerter[i]
        if w == "--":
            return i + 1
        if not w.startswith("-") or w == "-":
            return i
        if "=" in w:
            i += 1
            continue
        if w in mit_arg:
            i += 2
            continue
        i += 1
    return i


def docker_exec_ziel(w, p):
    """Bei docker exec / docker compose exec: (container, befehl_index), sonst None."""
    name = os.path.basename(w[p])
    i = p + 1
    if name == "docker":
        i = ueberspringe_opts(w, i, DOCKER_GLOBAL_MIT_ARG)
        if i < len(w) and w[i] == "container":
            i += 1
        if i < len(w) and w[i] == "exec":
            i = ueberspringe_opts(w, i + 1, DOCKER_EXEC_MIT_ARG)
            return (w[i], i + 1) if i < len(w) else None
        if i < len(w) and w[i] == "compose":
            i += 1
        else:
            return None
    elif name != "docker-compose":
        return None
    i = ueberspringe_opts(w, i, COMPOSE_GLOBAL_MIT_ARG)
    if i < len(w) and w[i] == "exec":
        i = ueberspringe_opts(w, i + 1, COMPOSE_EXEC_MIT_ARG)
        return (w[i], i + 1) if i < len(w) else None
    return None


def startet_zweites_backend(w, p):
    """docker compose run ... backend / docker run <evidora-backend-Image>.

    Der Dienst wartung bleibt erlaubt. Im Zweifel (ein Wort "backend" bzw.
    das Backend-Image irgendwo hinter run) wird gesperrt.
    """
    name = os.path.basename(w[p])
    i = p + 1
    if name == "docker":
        i = ueberspringe_opts(w, i, DOCKER_GLOBAL_MIT_ARG)
        if i < len(w) and w[i] == "container":
            i += 1
        if i < len(w) and w[i] in ("run", "create"):
            return any(BACKEND_IMAGE.match(x) for x in w[i + 1:])
        if i < len(w) and w[i] == "compose":
            i += 1
        else:
            return False
    elif name != "docker-compose":
        return False
    i = ueberspringe_opts(w, i, COMPOSE_GLOBAL_MIT_ARG)
    if i >= len(w) or w[i] != "run":
        return False
    rest = w[i + 1:]
    k = ueberspringe_opts(rest, 0, COMPOSE_RUN_MIT_ARG)
    dienst = rest[k] if k < len(rest) else ""
    if dienst == WARTUNG_DIENST:
        return False
    return bool(BACKEND_CONTAINER.search(dienst)) or "backend" in rest


def ssh_ziel(w, p):
    """Bei ssh: (host, entfernter_befehl), sonst None."""
    if os.path.basename(w[p]) != "ssh":
        return None
    i = p + 1
    while i < len(w):
        x = w[i]
        if x == "--":
            i += 1
            break
        if not x.startswith("-") or len(x) < 2:
            break
        # Gebuendelte Flags wie -tt oder -p22: letzte Option mit Argument?
        rest = x[1:]
        for k, ch in enumerate(rest):
            if ch in SSH_OPTS_MIT_ARG:
                if k == len(rest) - 1:
                    i += 1   # Argument im naechsten Wort
                break
        i += 1
    if i >= len(w):
        return None
    return w[i], " ".join(w[i + 1:])


def ist_git_update(w, p):
    """git pull / merge / rebase / reset --hard <remote>."""
    if os.path.basename(w[p]) != "git":
        return False, None
    i, pfad = p + 1, None
    while i < len(w) and w[i].startswith("-"):
        if w[i] == "-C" and i + 1 < len(w):
            pfad = w[i + 1]
        if w[i] in GIT_GLOBAL_MIT_ARG:
            i += 2
            continue
        if w[i].startswith("--work-tree=") or w[i].startswith("--git-dir="):
            pfad = w[i].split("=", 1)[1]
        i += 1
    if i >= len(w):
        return False, pfad
    sub, args = w[i], w[i + 1:]
    if sub in ("pull", "merge", "rebase"):
        return True, pfad
    if sub == "reset" and "--hard" in args and any(
            a.startswith("origin") or "@{u" in a or "@{upstream" in a
            or a == "FETCH_HEAD" for a in args):
        return True, pfad
    return False, pfad


def pruefe(text, auf_server=False, tiefe=0):
    """Liefert eine Sperrmeldung oder None."""
    try:
        segmente = zerlege(text)
    except ZerlegFehler:
        return grobe_pruefung(text, auf_server)
    im_serverpfad = False
    for seg in segmente:
        for s in seg["subst"]:
            if tiefe < 8:
                m = pruefe(s, auf_server, tiefe + 1)
                if m:
                    return m
        w = seg["woerter"]
        p = programm_start(w)
        if p is None:
            continue
        name = os.path.basename(w[p])

        if name == "cd":
            im_serverpfad = any(SERVER_PFAD.search(a) for a in w[p + 1:])
            continue

        # ssh <host> <befehl>
        ziel = ssh_ziel(w, p)
        if ziel:
            host, remote = ziel
            if SERVER_HOST.search(host) and tiefe < 8:
                for inner in [remote] + seg["heredoc"]:
                    m = pruefe(inner, True, tiefe + 1)
                    if m:
                        return m
            continue

        # git pull & Co.
        update, pfad = ist_git_update(w, p)
        if update and (auf_server or im_serverpfad
                       or (pfad and SERVER_PFAD.search(pfad))):
            return MELDUNG_GIT

        # docker compose run backend / docker run evidora-backend
        if startet_zweites_backend(w, p):
            return MELDUNG_ZWEITES_BACKEND

        # docker exec ... backend ... python
        dz = docker_exec_ziel(w, p)
        if dz:
            container, idx = dz
            if BACKEND_CONTAINER.search(container):
                befehl = " ".join(quote(x) for x in w[idx:])
                if ist_python_aufruf(befehl, tiefe + 1):
                    return MELDUNG_PYTHON
                for inner in seg["heredoc"]:
                    if ist_python_aufruf(inner, tiefe + 1):
                        return MELDUNG_PYTHON
            continue

        # sh -c '...', bash -c '...', eval '...'
        if name in SHELLS or name == "eval":
            innen = shell_c_strings(w, p) if name in SHELLS else [" ".join(w[p + 1:])]
            for inner in innen + seg["heredoc"]:
                if tiefe < 8:
                    m = pruefe(inner, auf_server, tiefe + 1)
                    if m:
                        return m
    return None


def quote(s):
    if s and re.match(r"^[A-Za-z0-9_./=:@%+,-]+$", s):
        return s
    return "'" + s.replace("'", "'\"'\"'") + "'"


def grobe_pruefung(text, auf_server):
    """Rueckfall, wenn sich der Befehl nicht zerlegen laesst: im Zweifel sperren."""
    server = auf_server or SERVER_HOST.search(text) or SERVER_PFAD.search(text)
    if server and re.search(r"\bgit\b[^;&|]*\b(pull|merge|rebase|reset\s+--hard)\b", text):
        return MELDUNG_GIT
    if (re.search(r"\bdocker(-compose|\s+compose)?\b[^;&|]*\bexec\b[^;&|]*backend", text)
            and re.search(r"\b(python[0-9.]*|pytest|pip[0-9.]*|uvicorn)\b", text)):
        return MELDUNG_PYTHON
    if (re.search(r"\bdocker(-compose|\s+compose)?\b[^;&|]*\brun\b[^;&|]*\bbackend\b", text)
            and not re.search(r"\brun\b[^;&|]*\bwartung\b", text)):
        return MELDUNG_ZWEITES_BACKEND
    if re.search(r"\bdocker\b[^;&|]*\b(run|create)\b[^;&|]*\bevidora[-_]backend\b", text):
        return MELDUNG_ZWEITES_BACKEND
    return None


def main():
    try:
        daten = json.load(sys.stdin)
    except Exception:
        return 0
    if daten.get("tool_name") != "Bash":
        return 0
    befehl = (daten.get("tool_input") or {}).get("command") or ""
    meldung = pruefe(befehl)
    if meldung:
        sys.stderr.write(meldung + "\n")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
