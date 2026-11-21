# CLAUDE.md — Arbeitsregeln für Evidora

Evidora (https://evidora.eu) ist ein europäischer Open-Source-Faktencheck
(MIT). FastAPI-Backend mit SSE, Mistral Cloud als Sprachmodell, 191 Quellen
im Fan-out, davon 78 statische Dienste mit kuratierten Fakten in
`website/backend/data/`. Entwickelt von einem Lehrer als Solo-Projekt; der
Nutzer schreibt Deutsch und wird geduzt.

- Für Menschen: [README.md](README.md), [ARCHITECTURE.md](ARCHITECTURE.md),
  [CONTRIBUTING.md](CONTRIBUTING.md)
- Server, Zugang und Pfade stehen in `CLAUDE.local.md` (nicht im Repo).
- Der aktuelle Stand steht im Auto-Memory: `current_status.md`, ein One-Pager.

## Arbeitsweise

- **Eine Sitzung ist eine Aufgabe.** Sie beginnt mit dem One-Pager und dem
  Auftrag und endet mit einem PR oder einem Messbericht. Danach kommt eine
  Zeile in den One-Pager, und die Sitzung wird archiviert. Was über die
  Sitzung hinaus gelten soll, gehört in eine Datei, nicht in den Verlauf.
- **Was der Nutzer mit seinen Freigaben meint:**
  - **„erst messen“**: Erst Frage, Kriterien und Entscheidungsregel
    schriftlich festlegen, dann messen. Gebaut wird nichts, bevor der
    Nutzer das Ergebnis gesehen hat. Ablauf: Skill `/messung`.
  - **„bau das so“**: den vorgelegten Entwurf umsetzen, als PR mit Tests.
    Weicht die Umsetzung vom Entwurf ab, die Abweichung im PR und im
    Bericht nennen.
  - **„CI ist grün“**: Im CI-Log prüfen, dass die neue Suite mitlief; dann
    Squash-Merge mit Löschen des Branches, lokal pullen, über `deploy.sh`
    deployen, live prüfen. Erst danach der nächste Punkt. Ablauf: Skill
    `/deploy`.
- **Ehrlich berichten.** Gemessenes und Vermutetes werden getrennt
  ausgewiesen. Ein verfehltes Kriterium heißt verfehlt, auch wenn die
  Ursache harmlos ist. Eigene frühere Aussagen, die eine Messung widerlegt,
  ausdrücklich zurücknehmen.

## Harte Regeln

Diese Regeln gelten ohne Ausnahme.

- **Deploy nur mit `website/deploy.sh` auf dem Server**, nie von Hand. Vorher
  **kein `git pull`** auf dem Server: Das Skript pullt selbst und leitet
  seine Aktion aus dem ab, was der Pull bringt. Nach einem manuellen Pull
  baut es nichts, Rettung ist dann `--force-build`.
- **Im Prod-Backend-Container nie `python` mit Modell-Imports** starten:
  `import main`, `services.ner`, `services._st_model`,
  `services._reranker_backup`, FastAPI-`TestClient`. Am 08.08.2026 riss das
  den ganzen Host mit. Erlaubt sind nur leichte Module (reine Logik) oder
  Shell-Befehle (`ls`, `grep`, `sha256sum`).
- **Messläufe mit Modellaufrufen** laufen im Einmal-Container `wartung`, mit
  `flock` und immer nur einer zur Zeit. Die Speicherrechnung des Hosts lässt
  genau einen zu.
- **`data/*.json` nie komplett mit `json.dump` neu schreiben.** Textuell
  einfügen oder nur das betroffene Objekt spleißen, damit Formatierung,
  Reihenfolge und Diff lesbar bleiben.
- **Keine Secrets ausgeben**, auch nicht in Logs, PRs oder Berichten. Den
  Test-Key liest nur der Server in eine Variable.
- **Datenschutz:** Kein Claim-Text und keine IP-Adresse in Dateien, Logs
  oder Protokolle. Wer Logging oder Aufbewahrung ändert, ändert im selben
  PR die Datenschutzerklärung in `frontend/i18n.js` (DE und EN).
  `tests/test_datenschutz_exakt.py` bindet jede Zahl darin an den Code.
  Vor jedem neuen dauerhaften Protokoll zuerst die Erklärung lesen.
- **Politik, 4 Tabus:** keine eigenen Partei-Bewertungen oder -Indizes,
  keine Wahlprognosen, keine eigene Links/Rechts-Einordnung, kein
  `true`/`false` auf politisch wertende Etiketten, wenn nur
  Wikipedia/Wikidata vorliegen.
- **netidee-gesperrte Features** (Multimodal/A11Y-Phasen des Förderantrags)
  nicht anfassen, bis die Förderentscheidung da ist.
- **Keine persönlichen Dokumente ins Repo** (Verträge, AVV, Rechnungen,
  echte Nutzer-Claims). Messordner liegen außerhalb, in
  `~/Documents/Evidora Dokumente/`.
- **Kein Cron und kein Werkzeug schreibt eine getrackte Datei.** `data/`
  ist auf dem Host gemountet; eine Schreibung dort verschmutzt den
  Server-Worktree, und `deploy.sh` verweigert den nächsten Deploy.
  Generierte Caches gehören in `.gitignore`.
- Die ersten beiden Regeln setzt ein PreToolUse-Hook durch
  (`.claude/hooks/server_regeln.py`, verdrahtet in `.claude/settings.json`):
  Er sperrt `git pull`/`merge`/`rebase`/`reset --hard origin/…` auf dem
  Server, **jeden** Python-Aufruf per `docker exec` im Prod-Backend und
  jeden zweiten Backend-Container (`docker compose run … backend`,
  `docker run evidora-backend`); frei bleibt der Dienst `wartung`.
  Eine leichte Prüfung führt der Nutzer selbst aus.

## Code-Regeln

- **Packs:** Sie matchen exakt (`descriptor_fn=None`, kein Cosine-Rückfall)
  und auf `analysis["original_claim"]`.
  - Jede `claim_phrasings_handled` muss ihren eigenen Fakt treffen. Ein Teil
    davon soll so formuliert sein, wie Menschen tippen: Alltagswort,
    österreichisch, Sorgefrage. Dort verliert die Zuordnung heute Fakten
    (Varianten-Batterie 5.11.2026: 67 %).
  - Neue Trigger gegen fremde Claims prüfen. Kurze, allgemeine Tokens
    („ei“, „sicher“) ziehen fremde Fakten an.
- **Neue Testdatei → in die CI-Liste** in `.github/workflows/test.yml` (Job
  `light-unit-tests`). Was dort nicht steht, läuft in der CI nie. Tests in
  der CI-Liste sind modellfrei: kein SpaCy, kein sentence-transformers, kein
  Netz.
- **Gegenbeweis:** Ein neuer Test muss gegen den alten Code rot sein. Im PR
  steht, wie viele rot waren, und warum die übrigen grün.
- **Keine synchrone CPU-Arbeit in `async def`**: kein Embedding, kein
  großes JSON, kein Laden von Modellen. Sie hält den ganzen Dienst an. Das
  gehört in `asyncio.to_thread` oder in einen Zwischenspeicher. Das
  geteilte ST-Modell hat eine Encode-Sperre (`services/_st_model.py`); wer
  es aus einem Thread nutzt, rechnet in kleinen Stücken.
- **Commits und PRs:** Deutsch, `scope(bereich): was jetzt stimmt (#PR)`,
  im Text Warum und gemessene Zahlen. Commits enden mit
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`, PR-Texte mit
  `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

## Messen

- **Vor dem Lauf:** Frage, Material, Kennzahlen, Erfolgskriterien und
  Entscheidungsregel stehen in `Plan.md`. Eine Schwelle, die erst nach den
  Daten gewählt wird, beweist nichts.
- **Ein Lauf ist eine Beobachtung, kein Befund.** Urteile schwanken. Ein
  Unterschied zählt erst, wenn er sich wiederholt (3×-Regel).
- **Eigene Auswertungsskripte gegenprüfen**, bevor ihre Zahl im Bericht
  steht. Ein „0,0“ oder ein Ergebnis von 100 % ist verdächtig.
- **Prüfungen gegen Prod verfälschen Betriebsmessungen**, zum Beispiel
  Cache-Anteil und Schattenprotokoll. Ihr Zeitfenster in UTC wird notiert.
- Werkzeuge: `website/backend/tools/messung/`. Ablauf: Skill `/messung`.

## Wo was steht

| Was | Wo |
|---|---|
| Deploy-Logik, Cron | `website/deploy.sh`, `website/crontab.prod`, `website/run_evidora_tool.sh` |
| CI | `.github/workflows/test.yml` (`light-unit-tests`, `check-new-urls`) |
| Messwerkzeuge | `website/backend/tools/messung/` |
| Skills | `.claude/skills/deploy/`, `.claude/skills/messung/` |
| Stand, Lehrgelder, Historie | Auto-Memory: `current_status.md`, `lessons_learned.md` (groß, gezielt per grep), Archive |
| Mess-Unterlagen | `~/Documents/Evidora Dokumente/<thema>_<datum>/Plan.md` |
