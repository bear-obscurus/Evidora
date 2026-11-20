---
name: deploy
description: Evidora-PR mergen, mit deploy.sh auf Prod bringen und live nachweisen. Verwenden, wenn der Nutzer "CI ist grün" sagt oder für einen PR vorab "nach grüner CI deployen" freigegeben hat. Nie ohne diese Freigabe deployen.
---

# /deploy — Merge, Deploy, Nachweis

Voraussetzung ist eine Freigabe des Nutzers für **genau diesen PR**: „CI ist
grün“ oder eine Vorab-Freigabe. Eine Freigabe gilt nicht für den nächsten
PR. Server und Zugang stehen in `CLAUDE.local.md`.

## 1. CI wirklich prüfen

```bash
gh pr checks <PR>                       # beide Checks pass?
RUN=$(gh run list --branch <branch> --limit 1 --json databaseId -q '.[0].databaseId')
gh run view $RUN --log | grep -E "<neue_testdatei>|[0-9]+ passed" | tail -3
```

- Die **neue Testdatei muss im Log stehen**, mit ihren Punkten. Sonst fehlt
  sie in der CI-Liste. Dann nicht mergen, sondern das zuerst beheben.
- Die Zahl der bestandenen Tests notieren; sie kommt in den Bericht.

## 2. Merge und lokal nachziehen

```bash
gh pr merge <PR> --squash --delete-branch
git switch main && git pull --ff-only && git log --oneline -1
```

## 3. Deploy — nur so

```bash
ssh <server> 'cd /opt/Evidora/website && ./deploy.sh'
```

- Vorher **kein `git pull`** auf dem Server. Nur wenn das schon jemand getan
  hat: `./deploy.sh --force-build`.
- Mit Image-Build dauert das Minuten: im Bash-Tool `run_in_background`, auf
  die Meldung warten, **keinen zweiten Deploy starten**.
- Reine Doku-Änderungen: `deploy.sh` entscheidet „keine“. Das ist korrekt
  und hält den Server-Worktree aktuell.

## 4. Ausgabe lesen

Erfolgreich ist der Deploy erst, wenn diese Zeilen kommen:

- `Entscheidung: …` (build-backend / recreate / keine)
- `backend healthy nach ~Ns`
- `Beleg 1`: StartedAt neu · `Beleg 2`: geänderte Dateien hash-identisch ·
  `Beleg 3`: `/api/legal` → 200
- `OK — Deploy <alt> -> <neu> abgeschlossen.`

**Kommt „FEHLER“, nicht neu deployen.** Erst den Zustand lesen:

```bash
ssh <server> 'docker inspect -f "{{.State.Health.Status}} {{.State.StartedAt}}" evidora-backend-1;
  docker logs -t evidora-backend-1 2>&1 | grep -E "Waiting for application startup|Application startup complete" ;
  docker logs --tail 40 evidora-backend-1 2>&1 | cut -c1-200'
```

- Startet der Dienst nur langsam (Start dauert länger als der Timeout),
  wird ein Beleg nach dem anderen von Hand nachgeholt. Die Startzeit wird
  benannt.
- Ist der Dienst wirklich kaputt: sofort an den Nutzer melden, mit dem
  Logauszug.
- `docker exec` nur mit Shell-Befehlen (`ls`, `grep`, `sha256sum`), **nie
  mit `python`**. Der Hook `.claude/hooks/server_regeln.py` sperrt jeden
  Python-Aufruf im Backend; eine leichte Prüfung führt der Nutzer selbst aus.

## 5. Live prüfen, was der PR ändert

- Das Verhalten, das der PR ändert, gegen Prod nachweisen. Kriterien kommen
  aus dem PR oder aus dem `Plan.md` der Messung.
- Echte Prüfungen mit `tools/messung/prod_pruefen.py`. Das Zeitfenster
  landet automatisch in `<ergebnis>.zeitfenster` und wird für
  Betriebsmessungen notiert.
- Latenz oder Stillstand messen: `tools/messung/latenz_probe.sh` auf dem
  Server, auswerten mit `tools/messung/latenz_auswerten.py`.

## 6. Abschluss

- Bericht an den Nutzer:
  - Merge-Commit und Prod-Stand,
  - die `deploy.sh`-Belege,
  - je Kriterium ✅, ⚠️ oder ⏳, mit Zahl.

  Ein verfehltes Kriterium heißt verfehlt.
- Eine Zeile in den One-Pager `current_status.md`; Ergebnisse ins
  `Plan.md` der Messung.
- Offenes, das erst später messbar ist (z. B. „morgen 18:40 UTC“), steht im
  One-Pager mit Datum und Werkzeug.
