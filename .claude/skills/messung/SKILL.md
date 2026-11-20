---
name: messung
description: Eine Evidora-Messung nach dem Standard "erst messen" anlegen, durchführen und auswerten — Ordner, Plan.md mit vorab festgelegten Kriterien, Grundlinie, Gegenbeweis, Ergebnis. Verwenden, wenn der Nutzer "erst messen" sagt, bevor eine Änderung gebaut wird, und für jede Nachmessung nach einem Deploy.
---

# /messung — erst messen, dann bauen

## 1. Ordner und Plan, bevor es Daten gibt

Ordner: `~/Documents/Evidora Dokumente/<thema>_<JJJJ-MM-TT>/`, dazu
`Plan.md` mit:

```markdown
# <Frage in einem Satz> (vorab festgelegt, <Datum>)

**Befund / Anlass:** was bekannt ist und woher (Code gelesen? Log? Nutzer?)
**Material:** was gemessen wird, Stichprobe, Seed, Quelle der Eingaben
## Messungen
- M1 …: Definition so genau, dass jemand anderes dieselbe Zahl bekommt
## Entscheidungsregeln (vorab)
- Wenn <Kennzahl> <Schwelle>, dann <Folge>; sonst <Folge>
## Grenzen (vorab benannt)
```

- Kriterien und Schwellen stehen **vor** dem Lauf fest. Eine Schwelle, die
  erst nach den Daten kommt, beweist nichts.
- Bei „erst messen“ endet die Arbeit nach der Messung. Bericht an den
  Nutzer, gebaut wird erst nach seiner Freigabe.

## 2. Grundlinie

- Vor jeder Änderung den Ist-Zustand mit **demselben** Werkzeug messen, das
  hinterher den neuen Stand misst. Die Rohdaten kommen in den Ordner
  (`vorher.txt`, `*.jsonl`).
- Werkzeuge in `website/backend/tools/messung/`:

  | Werkzeug | wofür |
  |---|---|
  | `prod_pruefen.py` | Prüfungen gegen Prod mit Test-Key: lokal starten, fortsetzbar, schreibt das Zeitfenster mit |
  | `analyse_lauf.py` | nur die Analyse-Stufe, im Wartungs-Container mit flock (Aufruf in `CLAUDE.local.md`) |
  | `latenz_probe.sh` | Latenz von `/api/legal` und `/api/health/full` im Takt, auf dem Server mit `nohup` |
  | `latenz_auswerten.py` | Probe-Log je Zeitfenster: n, OK, Fehlschläge, Median, p95, Maximum, Ausfallfenster |
  | `../retrieval_census.py` | welche Quelle welchen Claim beantwortet hat |

- Zuordnung und Trigger misst man lokal und kostenlos, über
  `find_matching_items` und die Schalter in `main.py`. `main.py` selbst wird
  dafür nicht importiert. Vorbild: `Evidora Dokumente/varianten_batterie_2026-11-05/`.

## 3. Gegenbeweis

- Ein Test für die Änderung muss **gegen den alten Code rot** sein, also
  mit `git stash` oder `git show main:<datei>`. Im Bericht steht, wie viele
  rot waren und warum die übrigen grün.
- Rot nur wegen fehlender Funktionen ist ein billiger Gegenbeweis. Die
  Kern-Behauptung wird zusätzlich am alten Code gezeigt, zum Beispiel:
  „Ticks der Event-Loop während des Neuaufbaus: alt 0, neu 46“.

## 4. Auswerten — und die eigene Auswertung prüfen

- Bevor eine Zahl aus einem eigenen Skript im Bericht steht: Ist sie
  plausibel? Ein „0,0“, 100 % oder eine leere Liste ist verdächtig. Am
  11.11. waren genau das drei Skriptfehler.
- Ein Lauf ist eine Beobachtung. Ein Unterschied zählt erst, wenn er sich
  wiederholt (3×-Regel). Urteile schwanken auch bei identischen Belegen.
- Widerlegt die Messung eine eigene frühere Aussage, wird sie ausdrücklich
  zurückgenommen.

## 5. Ergebnis festhalten

Ans `Plan.md` anhängen:

```markdown
# Ergebnis (<Datum>, Prod `<commit>`)
| Kriterium | gemessen | ✅ / ⚠️ / ⏳ |
## Entscheidung nach der Regel
## Grenzen
```

- ⚠️ heißt verfehlt; Ursache und Zahl stehen dabei.
- ⏳ bekommt ein Datum und das Werkzeug, mit dem nachgemessen wird.
- Prüfungen gegen Prod: Das Zeitfenster aus `*.zeitfenster` kommt in die
  Ausschlussliste der Cache- und Schatten-Auswertung
  (`~/.claude/scheduled-tasks/evidora-cache-trefferanteil/SKILL.md`).
- Memory: eine Zeile in `current_status.md`. Was dauerhaft gilt, kommt in
  ein Projekt-Memory oder nach `CLAUDE.md`.
