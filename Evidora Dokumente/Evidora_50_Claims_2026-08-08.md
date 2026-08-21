# Evidora QA50D — 50-Claim-Transfer-QA, 2026-08-08

Prod `3718f03`. Design, Lauf und Auswertung komplett Opus 5 + Ultracode (kein Fable-Guthaben).
Rohdaten: Server `/home/burrito/qa50d/` (`qa50d_claims.json`, `qa50d_main.jsonl`,
`rerun_r1..r5.jsonl`, `cachepol.jsonl`, `packs.jsonl`).

> **Vorgeschichte:** Der Lauf war am 29.07. schon einmal angesetzt und scheiterte an einem
> Produktions-Ausfall (Mistral-Key lieferte 401). Chronologie in Abschnitt 1 — sie ist der
> wichtigste Betriebs-Befund dieses Laufs und bleibt relevant, auch weil der Ausfall inzwischen
> behoben ist.

---

## 0. Bilanz

| | |
|---|---|
| **Hauptlauf** | **29/50 PASS**, 20 FAIL, 1 GUARD, **0 NEAR** |
| Nach 5 Re-Run-Runden (Restart vor jeder Runde, 6 Läufe je Nicht-PASS) | **14 deterministisch falsch**, 6 schwankend, 1 GUARD bestanden (6/6) |
| „Stabil korrekt" | **34/50** (29 PASS + 4 mehrheitlich korrekte + 1 GUARD) |
| Politik-Tabus | **4/4 + Kontroll-Claim**, keine Überblockung |
| Technische Fehler / Marker-Leaks / Konfidenz-Inkohärenzen / Claims ohne Quellen | **0 / 0 / 0 / 0** |
| `evidence_n=0` trotz Quellen | 27/50 (QA100: 42/100) |

**Verteilung:** 32 Regressions-Proben (64 %) / 18 Neu-Terrain (36 %), 17 Problemklassen,
max. 4 Claims je Klasse. Die angefragten ~55/45 habe ich zugunsten der Regression verschoben —
die Fix-Liste #89–#103 umfasst neun eigenständige Klassen, und jede braucht Hin- **und**
Gegenrichtung. Das ist eine bewusste Abweichung, keine Nachlässigkeit.

**Die zentrale Beobachtung ist keine Zahl, sondern eine Log-Statistik:** über **155 Claim-Durch­läufe**
(Hauptlauf + 5 Runden) gab es insgesamt **drei** Override-Ereignisse in der L4-Kaskade.
**Pattern G2, H, I, J und K haben kein einziges Mal gefeuert.** Die einzige aktive Komponente war
Pattern E — und die hat in **2 von 3** Fällen ein korrektes Ergebnis zerstört.

---

## 1. Der Produktions-Ausfall vom 29.07. (behoben)

Am 29.07. brach **jeder** Faktencheck im ersten Pipeline-Schritt ab; die Website lieferte HTTP 200,
aber `/api/check` antwortete durchgehend mit `Fehler bei der Claim-Analyse`.

```
httpx.HTTPStatusError: Client error '401 Unauthorized' for url 'https://api.mistral.ai/v1/chat/completions'
  File "/app/services/claim_analyzer.py", line 196, in analyze_claim
    raise ValueError("Invalid MISTRAL_API_KEY")
```

**Es war kein Konfigurationsfehler:** Container-Key und `.env`-Key waren identisch (gleicher
SHA-256-Präfix, 32 Zeichen, sauber alphanumerisch), und ein direkter Aufruf von
`GET api.mistral.ai/v1/models` mit genau diesem Key aus dem Container gab 401.
Letzte erfolgreiche Synthese: **28.07. 22:34:57 UTC** — mit demselben Key. Erster 401: **29.07. 10:41:17**.
Davor **0 ×** 401 im gesamten Container-Log. Da `docker compose restart` die `.env` nicht neu lädt,
trug der Container vorher und nachher denselben Key.

**Auflösung am 08.08.:** Derselbe, unveränderte Key liefert wieder `HTTP 200`. Es war also **keine
Widerrufung**, sondern eine upstream-seitige Limit-/Kontingent-Situation, die Mistral als `401`
beantwortet — nicht als `402` oder `429`. Das ist eine wichtige Diagnose-Lehre: **ein 401 von Mistral
bedeutet nicht zwingend „Key ungültig".**

### Drei Gründe, warum das ~12 Stunden unbemerkt blieb — ✅ (a) und (b) **GEFIXT (PR #107, prod `9412ddb`)**

> **Fix-Status 2026-08-08:** Der Backend-Healthcheck prüft jetzt `/api/health/full`, das
> zusätzlich `check_llm_auth()` aufruft — eine gecachte (TTL 300 s), **token-freie** Probe gegen
> `/v1/models`. **Tri-State**: nur 401/403 machen unhealthy; Timeout/429/5xx zählen als OK, damit
> ein Upstream-Schluckauf den Container nicht flappen lässt. Keine Restart-Schleife möglich, weil
> compose-`restart` bei Container-**EXIT** greift, nicht bei `unhealthy`. Dazu
> `tools/canary_check.py` als Cron-Job (täglich 05:00 UTC, 8 Jobs jetzt): ein fixer, zeitloser
> Claim durch die **komplette** Pipeline, ntfy-Push bei Ausfall — plus `||`-Fallback-Push für den
> Fall, dass der Container gar nicht läuft und das Tool nie zum Zug kommt.
> **Verifiziert:** Positiv-Lauf `verdict=true@0.9, 4 Quellen`; Negativ-Läufe erkennen sowohl ein
> nicht erreichbares Backend als auch **exakt die 29.07.-Signatur** (Error-Frame ohne
> Verdict-Frame, im Alert zitiert); `check_llm_auth` liefert gegen die reale Mistral-API mit einem
> Wegwerf-Key `ok=False`; der 503-Pfad des Handlers ist lokal gegen den extrahierten Quelltext
> bewiesen (200 / **503 `degraded`** / 200 für ok / abgelehnt / unbekannt). CI 36 Suiten / **800 Tests**.
>
> **(c) bleibt offen** — es gibt weiterhin keinen LLM-Fallback.



1. **Der Healthcheck testet den Kernpfad nicht.**
   `python -c "urllib.request.urlopen('http://localhost:8000/api/legal')"` — ein statischer Endpunkt
   ohne LLM-Beteiligung. Der Container meldete durchgehend `healthy`, während die Kernfunktion
   zu 100 % tot war.
2. **Kein Cron-Job übt den LLM-Pfad aus.** Die 7 Jobs decken phrasing_check, data_freshness
   (mit ntfy-Alert), url_health, cordis_refresh, restic-backup/-prune und docker-cleanup ab —
   keiner feuert je einen echten Claim gegen `/api/check`.
3. **Kein Fallback.** Gesetzt sind nur `MISTRAL_API_KEY`, `MISTRAL_MODEL`, `OLLAMA_URL`;
   `analyze_claim` wirft bei 401 direkt, ein Ollama-Fallback greift dort nicht.

---

## 2. Live-Stand-Verifikation

| Prüfpunkt | Erwartung | Ist | |
|---|---|---|---|
| Prod-HEAD `/opt/Evidora` | `3718f03` | `3718f03` | ✅ |
| `origin/main` | `3718f03` | **`5fca4d3`** | ⚠️ Docs-only-Commit (PR #104) liegt vor Prod. Kein Code-Delta. |
| Container | healthy | beide healthy, `RestartCount=0`, `OOMKilled=false` | ✅ |
| Reranker-Startup-Log | „Reranker using shared Sentence Transformer instance" | vorhanden (+ „Shared SentenceTransformer loaded once …@e8f8c211") | ✅ |
| CI | 34 Suiten / 761 Tests | **34 Dateien / 761 passed, 1 skipped** | ✅ |
| Offene PRs | — | keine | ✅ |
| 401-Fehler im Messlauf | 0 | **0**, 51 erfolgreiche Synthesen | ✅ |

---

## 3. Fix-Validierung — was aus PRs #89–#103 live hält

| Fix | Status | Beleg |
|---|---|---|
| **PR #92 Cache-Polarität** | ✅ **hält vollständig** | Eigener Lauf **ohne Suffix**, sequenziell nach Restart: #903 `true@0.9` gespeichert → #904 (Operanden vertauscht) läuft die **volle Pipeline** (18,0 s) und liefert `false@0.95`, **kein** Cache-Treffer. Echte Paraphrase #905 → `SEMANTIC HIT cos=0.954`, korrektes `true@0.9` in **0,0 s**. Genau **ein** SEMANTIC HIT im gesamten Log — der richtige. |
| **PRs #93/#94 Pack-Entkopplung** | ✅ **hält vollständig** | Alle 6 entkoppelten Packs treffen ihre **eigenen** Themen (**6/6** im Muss-Treffer-Lauf) und liegen über die 50 Claims bei **0–2 %** Trefferquote (vor dem Fix: 24–37 %). |
| **PR #102 `organisation_gruendung`** | ✅ hält | #328 („Gibt es in Österreich noch die Wehrpflicht?") löst das Template nicht mehr aus — kein SPARQL-Aufruf im Log. |
| **PR #103 Multi-Entity** | ✅ hält | Log: `Wikidata: Multi-Entity-Vergleich ['Graz', 'Klagenfurt'] (land_bevoelkerung) — 1 Treffer zusammengeführt`; #325 `true@0.85`. |
| **PR #97 Pattern F** | ✅ hält | #315 gerettet: `Pattern F: superlative/threshold claim REFUTED by summary but verdict='true' → false` (Steuerquote 43,2 % vs. Claim „über 50 %"). |
| **Pattern-H-Toleranz (0,5 %)** | ✅ hält | #312 Grenzfall **6/6**: H schweigt korrekt, weil 9.197.213 nur 0,03 % unter der 9,2-Mio-Schwelle liegt. |
| **Politik-Tabus 1–4** | ✅ **4/4 + Kontrolle** | #329 `unverifiable@0.1`, #330 `mixed@0.5` (exakt der Wikipedia-only-Cap), #331 `unverifiable@0.1`, Kontroll-Claim #332 FPÖ 28,8 % `true@0.95`. |
| **PR #98 Pattern-E-Attribution** | ⚠️ greift, aber E hat eine **größere** Lücke | siehe HOCH 1 |
| **PR #96 Pattern H symmetrisch** | ❌ **in 155 Durchläufen nie gefeuert** | siehe HOCH 2 |
| **Pattern G2 (PR #90) / Pattern K (PR #91)** | ❌ **nie gefeuert** | Die Claims, die sie adressieren sollten, landeten in Abdeckungslücken (#303–#306) oder wurden vom LLM direkt richtig beantwortet. |

---

## 4. Befund-Tabelle

### HOCH 1 — Pattern E bestätigt Schwellen-Claims mit **Alters-Angaben** (#316) — ✅ **GEFIXT (PR #106, prod `41e7590`)**

> **Fix-Status 2026-08-08:** `_is_bound_or_age()` portiert Pattern H's Schranken-Präfix- und
> Alters-Suffix-Prüfung nach E; `_threshold_refuted` erkennt zusätzlich „keine … über X"; die
> Zahlgrenze dort nutzt `(?![.,]?\d)` statt `\b`. **Gegenbeweis:** ohne Fix liefern beide
> Live-Summaries `true@0.95`, mit Fix `false@0.95`.
> **Live-Verifikation:** 5/5 `false@0.95`, **0 Pattern-E-Overrides** — und entscheidend: einer der
> drei **echten** Pipeline-Läufe (21:28:40 UTC, `Synthesis verdict` + `STORED`, kein Cache) erzeugte
> eine Summary mit „Die Jugendarbeitslosenquote in Österreich **(unter 25 Jahre)** lag 2026 bei
> 8,0–9,9 %" — **die auslösende Konstellation trat live auf und der Guard hielt**. Kontrollen
> #315/#334/#312 unverändert korrekt. CI 35 Suiten / **784 Tests**.



| | |
|---|---|
| **Ist** | `true@0.95` |
| **Soll** | `false` |
| **Häufigkeit** | 2 von 6 Läufen — aber **jedes Mal, wenn die auslösende Formulierung auftrat** |

Das LLM lieferte in **allen** Läufen das korrekte `false`. Zweimal hat **Pattern E** es aktiv invertiert:

| Lauf | Summary-Fragment | E liest | Ergebnis |
|---|---|---|---|
| Hauptlauf | „die Jugendarbeitslosigkeit **(unter 25)** bei 32.037 Personen" | 25 > 20 | `false` → **`true@0.95`** |
| Runde 3 | „nach ILO-Methodik 2025 bei 11,5 % **(15–24 Jahre)**" | 24 > 20 | `false` → **`true@0.95`** |

Beide Summaries enden wörtlich mit „**Keine Quelle bestätigt Werte über 20 %**" — eine
unmissverständliche Widerlegung, die der Override überstimmt.

**Wurzel** (`services/verdict_postprocess.py`, Pattern-E-Block ab Z. 827):
E iteriert über **jede** Zahl der Summary (`num_candidates`, Z. 900) und prüft nur:
Jahres-Ausschluss 1900–2100 (Z. 921), Fremd-Attribution (`_value_attributed_elsewhere`, PR #98),
und einen `_threshold_refuted`-Guard (Z. 883), der ausschließlich nach der Phrase „unter **20**"
sucht — die Summary schreibt aber „keine Quote **über 20 %**", also greift er nicht.

**Es fehlen genau die zwei Prüfungen, die Pattern H seit QA50B besitzt:**
1. **Alters-Qualifikator-Ausschluss** — H: `re.match(r"\s*-?\s*jährig|\s*jahre\b", …)` (Z. 1303)
2. **Schranken-Präfix-Ausschluss pro Kandidat** — H via `_entity_percent_from_queries`:
   Werte mit `unter|über|weniger als|mehr als|bis zu|maximal|mindestens|höchstens` davor sind
   keine Punktwerte (Z. 232–235)

Der QA50B-Review-Befund „*‚Unter 25-Jährige' ist keine Schwelle*" wurde damals **nur in H** gefixt und
**nie nach E portiert**. E läuft **vor** H — die dokumentierte Lehre „ein laxer früher Guard vor einem
strengen späten ist effektiv der laxe" trifft hier wörtlich zu.

**Warum HOCH:** Das ist der einzige Befund, bei dem das System eine **korrekte** Antwort in eine
**falsche mit 0,95 Konfidenz** verwandelt — auf einer politisch aufgeladenen Arbeitsmarkt-Statistik.
Alle anderen Fehler sind unterlassene Rettungen.

### HOCH 2 — Pattern H ist bei negierten Schwellen abgeschaltet (#311) — ✅ **GEFIXT (PR #108, prod `c94f545`)**

> **Fix-Status 2026-08-11:** Nicht abschalten, sondern die Richtung **umkehren** — „nicht über 10"
> behauptet dasselbe wie „höchstens 10". Bedingung ist eine an die Schwelle **gebundene** Negation
> (max. 25 Zeichen davor); eine ungebundene Negation bleibt konservativ.
> **Korrektur meiner Diagnose:** die Klasse war **symmetrisch** kaputt — nicht nur die Bestätigung
> fehlte, die widerlegende Richtung feuerte auch aktiv falsch (`„nicht über 2 %"` + 2,9 % → `true`
> statt `false`). Beides ist jetzt korrekt.
> Suite `test_pattern_h_negierte_schwelle.py` (13 Cases), 6 davon ohne den Fix rot.
> Im deployten Container über alle 7 Konstellationen verifiziert. CI 37 Suiten / **813 Tests**.



| | |
|---|---|
| **Ist** | `false@0.95` (1 von 6 Läufen; 5/6 korrekt) |
| **Soll** | `true` |

Claim: „Die Inflation in Österreich liegt **nicht** über 10 Prozent."
Summary: „…liegt laut Eurostat (2024: 2,9 %) und Statistik Austria (2024: 2,9 %) **deutlich unter 10 %**."
Der Claim ist damit wahr — das Label sagte falsch. **Kein Pattern-H-Log.**

**Wurzel** (Z. 1294): `_h_confirm_ok = not re.search(r"\b(?:nicht|kein\w*|nie|niemals|keineswegs)\b", claim_lower)`.
Die **bestätigende** Richtung wird bei negiertem Claim **komplett abgeschaltet**, statt die Richtung
umzukehren. Der einzige Fall, in dem H hier helfen könnte, ist der einzige, den es nicht anfassen darf.

⚠️ **Korrektur meiner Vorab-Hypothese:** Ich hatte erwartet, dass die *widerlegende* Richtung
fälschlich **feuert** (sie trägt kein Negations-Gate). Tatsächlich wird die *bestätigende* Richtung
**blockiert**. Anderer Mechanismus, gleicher Effekt — meine Vorhersage war falsch begründet.

### HOCH 3 — Die Schlussformel-Erkennung blockiert Pattern H (#309) — ⛔ **BEWUSST NICHT GEFIXT**

> **Entscheidung 2026-08-11:** Ich habe eine Lockerung der Sperre gebaut (H darf eine Schlussformel
> überstimmen, wenn **≥ 2 distinkte** Summary-Werte alle auf derselben Seite liegen) und gegen die
> volle Suite getestet — 800 grün, kein gepinnter QA50B-Fall brach. Dann **verworfen**: die echte
> Live-Summary von #309 nennt nur **eine** Zahl (9.197.213). Der Fix hätte also Risiko-Oberfläche
> hinzugefügt, ohne den Fall zu lösen. Auf einen einzelnen ungebundenen Wert zu lockern ist genau
> das, wogegen die QA50B-Härtung gebaut wurde.
> **Der saubere Weg** wäre ein eigenes Muster, das die **interne Widersprüchlichkeit** der Summary
> erkennt („9.197.213 Personen, **also unter 9 Millionen**") — ein neues Muster, kein Gate-Tweak.
> Die Entscheidung ist als Test gepinnt, damit sie nicht versehentlich aufgeweicht wird.
> Severität dämpfend: #309 war 5/6 korrekt (Varianz), nicht deterministisch.



Claim: „Sind wir in Österreich eigentlich schon über 9 Millionen Leute?" → `false@0.95` (1/6).
Summary: „…liegt laut Eurostat am 1. Januar 2025 bei **9.197.213** Personen, also **unter 9 Millionen**.
Die Behauptung … ist damit **falsch**."

Zwei Fehler in einem: (a) 9.197.213 ist nicht „unter 9 Millionen" — grober Rechenfehler;
(b) veraltete Vintage (1.1.2025 statt 1.4.2026 = **9.220.882**).

**Wurzel:** Die Summary enthält die Schlussformel „ist damit falsch" → die 4-Tier-Erkennung setzt
`verdict_from_summary='false'` → H's Gate `not verdict_from_summary` (Z. 1291) schließt.
Dieselbe Kaskaden-Blockade wie QA100 #34, nur mit der Schlussformel-Erkennung statt Pattern F
als Blocker. **Die Klasse „über 9 Millionen" ist damit nicht geschlossen** — sie ist nur für die
Phrasierungen geschlossen, in denen die Summary keine Schlussformel schreibt.

### MITTEL 1 — Messgrößen-Verwechslung Verbrauch/Verzehr (#321), **deterministisch 0/6** — ✅ **GEFIXT (PRs #115 + #117, prod `df658a6`)**

Ist `true@0.9`, Soll `mostly_false`. Die Summary zitiert den Cluster-A-Fakt vollständig korrekt
(„Verzehr 60,1 kg … Verbrauch 64,8 kg") und schließt dann: „Die Behauptung von ‚fast 65 Kilo'
bezieht sich auf den **Verbrauch** und ist damit korrekt." Der Claim sagt aber explizit **Verzehr**.

**Wurzel — bestätigt:** Der Fakt nennt bewusst beide Messgrößen (Cluster-A-Design), aber nichts
bindet die im Claim genannte Messgröße an den zu vergleichenden Wert. Das LLM wählt die passende
Zahl selbst. → Die Lehre „der LLM soll lesen, nicht rechnen" gilt auch für die **Auswahl der
Messgröße**. Bei der Diagnose kam ein Nebenbefund dazu: die claim-zentrierte 400-Zeichen-
Trunkierung schnitt den `display_value` exakt hinter „Verbrauch 64,8 kg/Kopf (+0,9 kg ggü. […]"
ab — **60,1 war für einen Verzehrs-Claim gar nicht prompt-sichtbar**. Getragen hat den Fakt nur
`indicator_name` (= Headline, 311 Zeichen, unter dem Cap).

**Fix — Fakt-Ebene, nicht Override-Kaskade.** Weg (b) (generisches Muster in
`verdict_postprocess`) wurde geprüft und **verworfen**: die Bindung „Messgröße im Claim →
zugehöriger Wert" ist generisch nicht sauber erkennbar (die Summary argumentiert sogar
ausdrücklich *über* die Verwechslung — „bezieht sich vermutlich auf den Verbrauch"), ein Muster
müsste also die Zulässigkeit einer Umdeutung bewerten. In einer Kaskade, die auf 155 Durchläufe
3 Overrides brachte (2 davon schädlich), ist das die falsche Stelle.

**Zwei Iterationen, beide live erzwungen** — Iteration 1 wirkte, verfehlte das Ziel aber:

| | prod `580fe0e` | nach PR #115 | nach PR #117 |
|---|---|---|---|
| „Der Fleischverzehr … fast 65 Kilo" | `true@0.9` | `mostly_true@0.9` | **`mostly_false@0.9`** ✅ |

Iteration 1 setzte den Bindungs-Satz an Headline-Position 1. Die Summary zitierte ihn danach
**wörtlich** als `evidence.finding` — und schloss trotzdem „bezieht sich vermutlich auf den
Verbrauch". Die *Zuordnung* kam an, das *Verbot der wohlwollenden Umdeutung* nicht: es stand nur
im kernsatz und wurde von derselben Trunkierung herausgeschnitten. Iteration 2 zog das Verbot in
die Headline (394 Zeichen, unter dem Cap):

> „… Eine VERZEHRS-Behauptung ist an 60,1 kg zu messen und **darf NICHT als Verbrauchs-Aussage
> gelesen werden** — ‚Verzehr rund 65 kg' ist **unzutreffend**."

**Live-Beleg (prod `b1b1e76`, Backend frisch neugestartet, alle vier echte Pipeline-Läufe mit
`Synthesis verdict` + `STORED`, 0 × `SEMANTIC HIT`):**

| Claim | Verdict | |
|---|---|---|
| „Der Fleischverzehr … fast 65 Kilo pro Kopf" | `mostly_false@0.9` | ✅ Ziel |
| „Der Fleischverbrauch … fast 65 Kilo pro Kopf" | `true@0.9` | ✅ Gegenrichtung hält |
| „Der Fleischkonsum … geht seit Jahren zurück" | `mostly_true@0.85` | ✅ Zweck des Fakts intakt |

Suite `test_fleisch_messgroessen_bindung.py` (14 Cases), Gegenbeweis 4 Cases ohne Fix rot.
Gepinnt wird u. a. die fragilste Eigenschaft: **Headline ≤ 400 Zeichen**, sonst verliert der
Bindungs-Satz seine Garantie.

### MITTEL 2 — ParlGov-Kabinett wird mit dem Staatsoberhaupt verwechselt (#327) — ✅ **GEFIXT (PR #116, prod `b85d1f1`)**

Ist `false@0.9` (1/6), Soll `true`. Log: `Wikidata: 5 strukturierte Fakten geliefert für 'Emmanuel Macron' (politiker_amtszeit)`
— der Wikidata-Pfad hat korrekt gefeuert. Die Inversion kommt aus **ParlGov**:
„Laut ParlGov wurde das **Kabinett** unter Emmanuel Macron (Borne/Attal) … durch das Kabinett
Barnier/Bayrou abgelöst. Aktuell regiert Macron Frankreich daher nicht mehr."

**Wurzel — bestätigt, aber schärfer als vermutet.** Die Vorab-Hypothese („kein Guard trennt
Kabinetts-Ende von Amtszeit-Ende") trifft zu, benennt den Mechanismus aber zu weich. Lokal
deterministisch reproduziert: die ParlGov-Zeile für die **Présidentielle 2022** trägt **zwei
Ämter in einem Datensatz** — `winner` ist das Staatsoberhaupt (Emmanuel Macron, Amtszeit bis
2027), `cabinet` die Regierungschef-Ebene (Borne / Attal). `_is_cabinet_superseded` fand die
Législatives-Zeile 2024 und setzte den harten `STRUKTURELL FALSCH:`-Marker auf die **ganze
Zeile** — also auch auf den Präsidenten. Es fehlte also nicht bloß ein Guard: der Marker sprach
über die *Zeile* statt über das *Kabinett*. **Neue Klasse** — nicht die Orbán/Meloni-Kante
(dort ist Wikidata die Quelle und es geht um End-Daten derselben Position).

**Fix, strukturell und claim-unabhängig:** `_is_presidential_election()` erkennt
Staatsoberhaupt-Wahl-Zeilen; auf ihnen entscheidet über den harten Marker nur noch
`_presidency_successor()` — eine **spätere Präsidentschaftswahl mit anderem Sieger**.
Wiederwahl derselben Person zählt nicht (Macron 2017→2022) — dieselbe Klasse wie die
Wiedereintritts-Falle in `wikidata.py` (Trump 2017+2025). Ohne solchen Nachfolger: ein
deskriptiver `AMTS-ABGRENZUNG`-Hinweis **ohne** Prefix, der den Kabinettswechsel vollständig
benennt (Kabinetts-Claims bleiben widerlegbar), aber festhält, dass daraus nichts über das
Staatsoberhaupt folgt. Der unveränderte Kabinetts-Pfad (DE/AT/UK/IT/ES) sagt jetzt ebenfalls
ausdrücklich, dass er nichts über Staatsoberhäupter aussagt — die generische Hälfte des Fixes.

**Live-Beleg (prod `b1b1e76`) — mit auslösender Konstellation:** In beiden Macron-Läufen zeigt
das Log `Source 12 (ParlGov) returned 2 results`, die problematische Zeile war also im Prompt.
Die Summary führt die neue Abgrenzung sichtbar aus:

> „Emmanuel Macron ist seit Mai 2017 Staatspräsident Frankreichs und regiert das Land noch immer.
> **Die Parlamentswahlen 2024 führten zu einem Kabinettswechsel, betrafen aber nicht seine
> Position als Staatsoberhaupt.**"

| Claim | Verdict | |
|---|---|---|
| „Emmanuel Macron regiert Frankreich noch immer." | `true@0.9` | ✅ Ziel |
| „Emmanuel Macron ist Präsident Frankreichs." | `true@0.9` | ✅ |
| „Viktor Orbán ist Ungarns Ministerpräsident." | `false@0.7` | ✅ Orbán-Klasse hält |
| „Regiert die Ampel-Koalition noch in Deutschland?" | `false@0.95` | ✅ ParlGov liefert, harter Marker feuert |

Suite `test_parlgov_kabinett_vs_staatsoberhaupt.py` (25 Cases). Gegenbeweis per Ad-hoc-Skript
(der `git stash`-Weg scheiterte an einem Import-Fehler): **5 von 7 Verhaltens-Checks ohne den
Fix rot**.

### NEU MITTEL 9 — Verdict-Cache verwechselt Messgrößen-Paare — ✅ **GEFIXT (PR #118, prod `b1b1e76`)**

Bei der Live-Verifikation von #321 aufgetaucht, **nicht** durch den Fix verursacht: die vom
Auftrag geforderte Gegenrichtungs-Kontrolle war live gar nicht messbar, weil die Pipeline nie
lief. Log aus prod `b85d1f1`:

```
verdict_cache: SEMANTIC HIT cos=0.985 for 'Der Fleischverbrauch in Österreich liegt bei fast 65 Kilo pr'
  -> matched 'der fleischverzehr in österreich liegt bei fast 65 kilo pro '
verdict_cache: SEMANTIC HIT cos=0.963 for 'Österreich kommt beim Pro-Kopf-Verbrauch von Fleisch auf etw'
  -> matched 'der fleischverzehr in österreich liegt bei fast 65 kilo pro '
```

Beide Male bekam die **Verbrauchs**-Frage (bei der ~65 kg korrekt ist) das `mostly_false` des
**Verzehrs**-Claims mit voller Konfidenz. Der zweite Treffer ist der entscheidende: auch eine
deutlich umformulierte Gegenfrage traf noch — kein Phrasierungs-Artefakt, das Embedding trennt
die beiden Größen schlicht nicht. Dieselbe Klasse wie PR #92, in nicht abgedeckter Variante:
alle vier bestehenden Zweige von `_polarity_mismatch` schweigen (keine Negation, kein
Operanden-Tausch, kein Richtungs-Antonym, und die Zahl ist auf beiden Seiten dieselbe — 65).

Fix: neuer Zweig `_measure_mismatch` mit eigener Tabelle `_MEASURE_PAIRS`, bewusst getrennt von
den Polaritäts-Antonymen (kein Richtungs-Wechsel, sondern ein Bezugsgrößen-Wechsel). Live
gemessen ist nur `verbrauch`/`verzehr`; `kapazität`/`erzeugung` (QA100 #44) und `brutto`/`netto`
sind konservativ mitgenommen — ein False-Positive kostet nur einen Cache-Miss.
**Live nach dem Deploy: beide Claims echte Läufe, 0 × `SEMANTIC HIT`.**

### NEU MITTEL 10 — Meloni-Regel greift nicht mehr ⛔ **OFFEN**

Als Kontrolle für #327 mitgelaufen und dabei aufgefallen — ein eigener, **vorbestehender**
Befund:

> Claim „Giorgia Meloni ist Italiens Ministerpräsidentin." → `mostly_false@0.85`,
> Summary: „Giorgia Meloni ist seit Oktober 2022 italienische Ministerpräsidentin, wie ParlGov
> und Wikipedia übereinstimmend **bestätigen**."

Log: `STRUKTURELL FALSCH override: LLM returned 'true' @ 0.9 despite STRUKTURELL FALSCH marker
in sources (ratio 3/9 = 33%). Enforcing 'mostly_false' @ 0.85.` Das LLM lag also richtig, der
L2-Override kippte es.

**Nicht von PR #116 verursacht** — nachgewiesen, nicht vermutet: die ITA-Zeile 2022 ist die
jüngste ihres Landes, bekommt also den *stale*-Soft-Caveat statt eines Markers. Gegen beide
Fassungen von `parlgov.py` (vor `580fe0e` und nach `b85d1f1`) geprüft: in beiden `STRUKT: False`.
Die drei Marker stammen aus den 5 **Wikidata**-Results, d. h. die Meloni-Regel (aktives
Spitzenamt unterdrückt beendete Nebenämter, PR #88) unterdrückt sie nicht mehr — in QA100 war
diese Kontrolle noch 6/6. Braucht eine eigene Diagnose der `active_positions`-Unterdrückung in
`services/wikidata.py`; hier bewusst **nicht** mitgefixt, um den Auftrags-Scope nicht
auszuweiten.

### MITTEL 3 — 14 deterministische Abdeckungslücken (0/6 in **allen** Läufen) — ✅ **GESCHLOSSEN (PR #109, prod `a9f454e`)**

> **Cluster B, 2026-08-14:** 10 kuratierte Fakten schließen die 13 Abdeckungslücken (der 14.
> deterministische Fall #321 ist eine Messgrößen-Bindung, keine Lücke). Alle Zahlen an der
> **Primärquelle** verifiziert; wo Stände auseinandergingen, wurde bewusst **eine** Vintage
> gewählt statt gemischt (Verkehrstote: endgültige 351 von Statistik Austria statt der
> vorläufigen BMI-Zahl 349 — die Straßenart-Summe geht in dieser Fassung exakt auf).
>
> **Live-Ergebnis: 14/14 haben jetzt Daten, kein einziges `unverifiable@0.1` mehr.**
> Verdict-Bilanz nach dem Folge-Fix: **12/14 korrekt.**
>
> Trigger-Qualität: Phrasings-Battery 100 %, **0 Over-Trigger** (15 Treffer über alle 50
> QA50D-Claims, alle auf den eigenen Themen). Die Battery fand beim Bauen drei echte
> Trigger-Lücken, ein Composite-Leck wurde im Smoke-Test gefangen (`"sterb"` + `"österreich"`
> zog den Herz-Kreislauf-Claim), und die 2-Zeichen-Falle wurde bewusst vermieden.
>
> ⚠️ **Nebenwirkung, die den eigentlichen Wert des Sprints ausmacht:** Das Schließen der
> Lücken hat zwei **Verdict-Logik-Fehler freigelegt**, die vorher unsichtbar waren, weil die
> Claims mangels Daten gar nicht bewertet wurden — siehe HOCH 4 und MITTEL 8.

### MITTEL 3 — 14 deterministische Abdeckungslücken (0/6 in **allen** Läufen)

Alle mit `unverifiable@0.1` und `evidence_n=0`. Für fast jede gibt es einen natürlichen Ziel-Pack:

| Claims | Thema | Ziel-Pack |
|---|---|---|
| #303/#304 | Feuerwehr: Freiwillige vs. Berufs (4.438 FF vs. **6** BF, ~99 % ehrenamtlich) | neu |
| #305 | Bahn vs. Auto: Todesfälle je Personenkilometer | `verkehrssicherheit_pack` |
| #313 | ASFINAG-Netzlänge (2.258 km) | `mobilitaet_pack` |
| #319 | Pensions- vs. Bildungsausgaben | `sozialstaat_pack` |
| #328/#337 | Wehrpflicht / Grundwehrdienst (6 Mon.) / Zivildienst (9 Mon.) | `sicherheitspolitik_pack` |
| #340 | U-Haft vs. Strafvollzug | `at_courts` |
| #341 | Cybercrime-Anzeigen (BK: 2023 Peak 65.864, 2024 −5,4 %, 2025 +1,8 %) | `eu_crime` |
| #342/#343 | Verkehrstote AT nach Straßentyp / 2024 | `verkehrssicherheit_pack` |
| #345 | Nährwerte Ei vs. Magertopfen | `sport_fitness_pack` |
| #350 | Amtssprachen AT (Art. 8 B-VG + Art. 7 StV 1955) | **kein Ort** |

**Wichtig:** `verkehrssicherheit_pack` **existiert**, enthält aber ausschließlich Verhaltens-Mythen
(Tempo 30, Fahrradhelm, Kindersitz, Promille, Winterreifen) und **keinen einzigen Fakt zu
österreichischen Verkehrstoten**. ITF/OECD IRTAD feuert, liefert aber nur 2023 (402 Tote) —
für den 2024-Claim zu alt.

### HOCH 4 — `"eu"` ⊂ „F**eu**erwehr": 2-Zeichen-Token invertierte ein korrektes Verdict — ✅ **GEFIXT (PR #110, prod `24c4f2d`)**

Beim Cluster-B-Live-Test lieferte das LLM für #303 korrekt `true@0.95` — und wurde invertiert:

```
STRUKTURELL FALSCH override: LLM returned 'true' @ 0.95 despite STRUKTURELL FALSCH
marker in sources (ratio 2/11 = 18%). Enforcing 'mostly_false' @ 0.85.
```

Der Marker kam von `eu_netto_zahler_konsens` (wirtschaftspolitik_pack) — einem Fakt über
EU-Nettozahler. Dessen Composite lautet `["eu", …] AND ["österreich", …]`, und **`"eu"`
steckt in „F-EU-erwehr"**. Der Tier-1-Guard griff nicht: Ratio 2/11 = **18 %**, knapp über
der 15-%-Schwelle.

**Warum es bisher nicht auffiel:** Das Token stand im Welle-2-Sweep (PR #20) als „empirisch
eingehegt (0 Cross-Topic auf 103 Proben)". Eingehegt war es aber nur, weil im damaligen
Proben-Korpus **kein Claim das Wort „Feuerwehr" enthielt**. Der neue Cluster-B-Fakt hat diese
Claim-Familie eingeführt und die Einhegung gebrochen.

Fix: bare `"eu"` → `"eu-"`, `" eu "`, `" eu."`, `" eu,"`, `"die eu"`, `"der eu"`. Alle fünf
echten EU-Claims treffen weiter, beide Feuerwehr-Claims nicht mehr. #303 live `true@0.9`.

### MITTEL 8 — Pattern A's Attributions-Guard kennt nur GEOGRAFISCHE Entitäten (#342) — ✅ **GEFIXT (PR #111, prod `6b2ca6c`)**

> **Fix 2026-08-17:** Neuer Zweig `_superlative_attributed_elsewhere()`, attributions-gebunden
> in zwei deutschen Wortstellungen (präpositional + Verb-Zweitstellung), läuft **vor** der
> Länder-Vorbedingung. **Live mit echter Auslöse-Konstellation:** die Summary enthielt weiterhin
> „Die meisten Getöteten entfielen auf Landesstraßen B" — Verdict jetzt `false@0.9` statt
> `true@0.9`. Krone-Confirm-Kontrolle unverändert `true@0.9`.
### MITTEL 8 — Pattern A's Attributions-Guard kennt nur GEOGRAFISCHE Entitäten (#342)

Ist `true@0.9`, Soll `false`. Die Summary ist faktisch vollständig korrekt:

> „2024 starben in Österreich 351 Menschen im Straßenverkehr, davon **25 auf Autobahnen
> (7,1 %)** … Die meisten Getöteten entfielen auf **Landesstraßen B** … mit 142 Fällen (40,5 %)."

Sie **widerlegt** den Claim — trotzdem flippt Pattern A das korrekte `false` auf `true`, weil
die Superlativ-Phrase „die meisten" in der Summary steht.

**Wurzel, im deployten Container isoliert nachgewiesen:**

| Summary-Variante | `_summary_refutes_superlative` |
|---|---|
| „Die meisten Getöteten entfielen auf **Landesstraßen B**" | **`False`** |
| dieselbe Aussage mit „**Deutschland**" statt der Straßenart | `True` |

Der Attributions-Guard (PR aus QA100 #52/#74/#81) prüft gegen eine **Whitelist aus Ländern
und Bundesländern**. Jede andere konkurrierende Entitätsklasse — Straßenarten, Altersgruppen,
Berufsgruppen, Kategorien — ist für ihn unsichtbar. Das ist strukturell dieselbe
Überanpassung wie bei Pattern G vor G2: *das Muster wurde in der Geografie-Domäne gebaut und
kennt nur sie.*

Der Fix wäre eine generische Entitäts-Extraktion analog `_generic_comparison_pair` (deutsche
Substantiv-Großschreibung) — ein Neubau, kein Tweak, und damit bewusst nicht Teil dieses Sprints.

### MITTEL 4 — Pattern K erkennt 5 von 8 gängigen Zurückweisungen nicht — ✅ **GEFIXT (PR #112, prod `abdaf20`)**

> **Fix 2026-08-17:** Literale `schwachsinn`/`unfug`/`nonsens` ergänzt und das Kopula-Gate um
> **unbestimmten Artikel** und Verstärker erweitert. 12 von 12 Formulierungen werden jetzt
> erkannt. Live: #305 `true@0.9`, die AT-Form „ist doch ein Schmarrn" ebenfalls `true@0.9`,
> die Gegenprobe #306 bleibt `false@0.9`. Log-Beleg, dass K wirklich feuerte:
> `Pattern K Anti-Mythos … verteidigt verdict 'true' gegen Schlussformel 'false'`.
> Bewusst NICHT aufgenommen: „käse" und „topfen" — beide sind zugleich Lebensmittel.
### MITTEL 4 — Pattern K erkennt 5 von 8 gängigen Zurückweisungen nicht

Die Claims #305/#306 landeten in einer Abdeckungslücke, Pattern K kam im Lauf nie zum Zug.
Deshalb **Reproduktion direkt gegen den deployten Container** (`docker exec -i … python3 -`),
mit einer Summary, die ¬P über das Antonym belegt:

| Zurückweisung | `_antimythos_flip` |
|---|---|
| „ist ja wohl **Unsinn**" | ✅ feuert |
| „ist doch **Quatsch**" | ✅ feuert |
| „ist **Humbug**" | ✅ feuert |
| „ist doch **Schwachsinn**" | ❌ **stumm** |
| „ist doch **Unfug**" | ❌ **stumm** |
| „ist **ein Schmarrn**" | ❌ **stumm** — obwohl `"schmarrn"` im Literal-Set steht! |
| „ist doch **ein Blödsinn**" | ❌ **stumm** — dito |
| „ist **totaler** Unsinn" | ❌ **stumm** |

Zwei getrennte Ursachen: (a) `_ANTI_MYTHOS_DISMISSALS` kennt „schwachsinn"/„unfug"/„nonsens" nicht;
(b) das **Kopula-Gate** (Z. 332–337) erlaubt weder den **unbestimmten Artikel** noch den Verstärker
„totaler/purer/absoluter" — deshalb fallen sogar Formulierungen durch, deren Dismissal-Wort im
Literal-Set steht. Österreichisches „ist ein Schmarrn" ist genau so eine.
**Status: im Container reproduziert.**

### MITTEL 5 — Cache-Polaritäts-Guard schweigt bei drei gängigen Antonym-Paaren — ✅ **GEFIXT (PR #113, prod `abdaf20`)**

> **Fix 2026-08-17:** `_POLARITY_ANTONYMS` von 25 auf 44 Paare erweitert.
> **Live-Beweis (suffixlos, sequenziell nach Restart):** „häufiger" → `true@0.85` in 34,9 s,
> „seltener" → `false@0.85` in **29,9 s volle Pipeline** (kein Cache-Treffer trotz hoher
> Ähnlichkeit), echte Paraphrase → `true@0.85` in **0,0 s** mit `SEMANTIC HIT cos=0.956`.
> Der Guard schärft, der Cache lebt.
### MITTEL 5 — Cache-Polaritäts-Guard schweigt bei drei gängigen Antonym-Paaren

Live nicht messbar gewesen (beide Seiten `unverifiable@0.1`, und unterhalb der Cache-Mindest­konfidenz
0,8 wird nichts gespeichert), deshalb ebenfalls **im Container reproduziert** (`_polarity_mismatch`):

| Paar | Guard |
|---|---|
| „häufiger … als Männer" / „seltener … als Männer" | ❌ **schweigt** |
| „wärmer als im Vorjahr" / „kälter als im Vorjahr" | ❌ **schweigt** |
| „länger als früher" / „kürzer als früher" | ❌ **schweigt** |
| Operanden-Tausch (Windkraft/PV) — Kontrolle | ✅ greift |
| klimaschädlicher/klimafreundlicher — Kontrolle | ✅ greift |

`_POLARITY_ANTONYMS` hat 25 Paare, aber **häufiger/seltener, wärmer/kälter, länger/kürzer,
älter/jünger, früher/später, oft/selten** fehlen. Weil `_operands_swapped` zwingend ein `als` mit
**vertauschten** Operanden verlangt (hier steht auf beiden Seiten dasselbe Vergleichsobjekt),
greift auch dieser Zweig nicht. **Produktionswirkung:** Wer die Gegenfrage stellt, bekommt bei
diesen Paaren das gegenteilige Verdict mit voller Konfidenz, ohne dass die Pipeline läuft —
exakt die Klasse, die PR #92 schließen sollte, in nicht abgedeckter Variante.
**Status: im Container reproduziert.**

---

## 5. Pack-Trefferquote über die 50 Claims

**Die 6 im Juli entkoppelten Packs sind sauber** — alle einstellig oder null:

| Pack | Trefferquote | Muss-Treffer-Lauf |
|---|---|---|
| MedienTransparenz | 1/50 (2 %) | ✅ |
| Energy-Charts | 0/50 | ✅ |
| Bildung-DACH | 0/50 | ✅ |
| Verkehr-AT | 0/50 | ✅ |
| Wohnen-AT | 0/50 | ✅ |
| Eurostat-Crime | 0/50 | ✅ |

> ### ⚠️ KORREKTUR 2026-08-17 — die folgende Tabelle war zu einem großen Teil ein Messartefakt
>
> Der Über-Trigger-Sweep (PR #114) hat gezeigt: die Quoten unten wurden **mit** dem
> Cache-Bust-Suffix `(Pruefsatz-NNN)` gemessen — und darin steckt `"efsa"` (Pr-**uefsa**-tz),
> ein legitimer Trigger des Landwirtschafts-Packs. **Ohne Suffix fällt Landwirtschaft-Konsens
> von 15/50 auf 1/50**; alle statischen Packs liegen dann im einstelligen Bereich.
> Ein Index-Bug wurde als Ursache geprüft und ausgeschlossen (`hit_names` deckt sich exakt mit
> den Service-`source`-Feldern).
>
> **Übrig blieben vier ECHTE Lecks** — nicht die hier genannten, sondern:
> EIGE Gender Equality Index (`"eige"` ⊂ „eigentlich" + `"eu"` ⊂ „Leute", 4 Off-Topic-Treffer),
> ETER (`"eter"` ⊂ „Kilometer"), AT-Neutralität (`["österreich"] AND ["abgeschafft"]`) und
> MedienTransparenz (`["anzeigen"] AND ["österreich"]` — Straf-Anzeigen ≠ Inserate).
> Alle vier sind in PR #114 geschlossen und live gegengeprüft; Pack-Treffer über einen
> 60-Claim-Korpus **60 → 46**, alle verbleibenden on-topic.
>
> Die Lehre: **ein Cache-Bust-Suffix ist nie neutral** — er verfälscht nicht nur den Cache und
> die Entitäts-Extraktion, sondern auch das exakte Trigger-Matching. Quoten-Messungen gehören
> ohne Suffix gefahren.

**Die ursprünglich gemessenen Auffälligkeiten** (mit Suffix — nur noch historisch):

| Quelle | Quote | Bewertung |
|---|---|---|
| Wikipedia | 50/50 (100 %) | by design, unkritisch |
| GDELT v2 GKG | 45/50 (90 %) | Breitband-Nachrichtenquelle, by design |
| DataCommons ClaimReview | 29/50 (58 %) | grenzwertig |
| DESTATIS | 19/50 (38 %) | **auffällig** — DE-Statistik auf AT-Claims |
| OECD Health (DACH) | 17/50 (34 %) | **auffällig** |
| Eurostat (EU) | 16/50 (32 %) | grenzwertig |
| ~~Landwirtschaft-Konsens~~ | ~~15/50 (30 %)~~ | ❌ **Artefakt** — ohne Suffix 1/50 |
| Correctiv | 13/50 (26 %) | grenzwertig |
| CDC Newsroom | 12/50 (24 %) | **auffällig** — US-Gesundheitsbehörde auf AT-Claims |
| Wirtschaftspolitik-Konsens | 10/50 (20 %) | **auffällig** |

---

## 6. Nebenbefunde

1. **Der Cache-Bust-Suffix wird als Entität extrahiert.** Log:
   `entities: ['Zugfahren', 'Autofahren', …, 'Pruefsatz-305']`. Der Token landet in Quellen-Queries
   (Wikidata-Label-Lookup, Faktencheck-Suchen). Methodik-Kosten, kein Produktionsfehler —
   für künftige Läufe wäre ein Suffix ohne Wort-Charakter besser.
2. **Der Suffix schaltet den Verdict-Cache faktisch ab.** **0 × `SEMANTIC HIT`** im gesamten
   Hauptlauf, weil jeder Claim eine eigene Zahl trägt und damit der Disjunkt-Zahlen-Zweig bei
   **jedem** Paar feuert. Das ist die Kehrseite der QA100-Lehre: eine *gemeinsame* Zahl erzeugt
   Phantom-Treffer, eine *eigene* Zahl unterdrückt alle. **Polaritäts-Guards sind im Hauptlauf
   grundsätzlich nicht prüfbar** — dafür braucht es immer den separaten suffixlosen Lauf.
3. **#320 richtiges Verdict, unsinnige Zahlen:** „Bildung 10,5 % des Staatsbudgets, aber
   Sozialschutz 4,9 %" — Sozialschutz ist der mit Abstand größte Budgetposten. COFOG-Werte
   fehlinterpretiert; das Verdict stimmt trotzdem.
4. **#312 richtiges Verdict, falsche Begründung:** „9.197.213 …, was **über** 9,2 Millionen liegt".
5. **Veraltete Vintage:** Mehrere Claims stützen sich auf Eurostat 1.1.2025 (9.197.213), während
   Statistik Austria für 1.4.2026 bereits **9.220.882** ausweist.
6. **Jeder Backend-Restart kostet ~4–5 min** Cold-Start inkl. **199 MB DataCommons-Download**.
   Bei 5 Re-Run-Runden sind das ~25 min reine Restart-Zeit.

---

## 7. Priorisierte Fix-Liste

| Prio | Fix | Aufwand |
|---|---|---|
| ~~HOCH 1~~ | ✅ **ERLEDIGT (PR #106, prod `41e7590`)** — Pattern E gehärtet, live verifiziert inkl. echter Auslöse-Konstellation. | erledigt |
| ~~HOCH 2~~ | ✅ **ERLEDIGT (PR #107, prod `9412ddb`)** — Healthcheck auf `/api/health/full` mit LLM-Auth-Probe (gecacht, token-frei, Tri-State ohne Flapping) + `tools/canary_check.py` als täglicher Cron-Kanarienvogel um 05:00 UTC. | erledigt |
| ~~HOCH 3a~~ | ✅ **ERLEDIGT (PR #108, prod `c94f545`)** — negierte Schwelle kehrt die Richtung um. Die Klasse war **symmetrisch** kaputt; beide Richtungen sind gefixt. | erledigt |
| **HOCH 3b** | ⛔ **#309 bewusst offen.** Gate-Lockerung gebaut, gegen die volle Suite getestet (800 grün) und **verworfen** — sie löst den Live-Fall nicht (nur eine Zahl in der Summary) und hätte nur Risiko hinzugefügt. Braucht ein eigenes Muster für die **interne Widersprüchlichkeit** der Summary („9.197.213 Personen, also unter 9 Millionen"). | mittel, Neubau |
| ~~MITTEL 1~~ | ✅ **ERLEDIGT (PR #109, prod `a9f454e`)** — Cluster B: 10 kuratierte Fakten, 14/14 Claims haben jetzt Daten (0 × `unverifiable`), 12/14 Verdicts korrekt, 0 Over-Trigger. | erledigt |
| ~~NEU HOCH 4~~ | ✅ **ERLEDIGT (PR #110, prod `24c4f2d`)** — `"eu"` ⊂ „F**eu**erwehr" invertierte ein korrektes Verdict; Token eingehegt. | erledigt |
| ~~NEU MITTEL 8~~ | ✅ **ERLEDIGT (PR #111)** — Pattern A's Attributions-Guard (#342): `_summary_refutes_superlative` prüft nur gegen eine Whitelist aus Ländern/Bundesländern — „Die meisten Getöteten entfielen auf **Landesstraßen B**" wird nicht als Fremd-Attribution erkannt. Analog zu G→G2 auf deutsche Substantiv-Großschreibung umstellen. | mittel, Neubau |
| ~~MITTEL 2~~ | ✅ **ERLEDIGT (PRs #115 + #117, prod `df658a6`)** — Messgrößen-Bindung (#321) als fertiger Satz an Headline-Position 1. Zwei Iterationen: die bloße Zuordnung brachte nur `true`→`mostly_true`; erst das ausdrückliche **Verbot der Umdeutung** in der Headline erreichte `mostly_false@0.9`. Gegenrichtung `true@0.9`. | erledigt |
| ~~MITTEL 3~~ | ✅ **ERLEDIGT (PR #116, prod `b85d1f1`)** — ParlGov-Rollen-Guard (#327): Präsidentschaftswahl-Zeilen bekommen den harten Marker nur noch bei einer späteren Präsidentschaftswahl mit **anderem** Sieger; sonst deskriptive Amts-Abgrenzung. Macron `true@0.9` mit ParlGov im Prompt. | erledigt |
| ~~NEU MITTEL 9~~ | ✅ **ERLEDIGT (PR #118, prod `b1b1e76`)** — **Verdict-Cache trennt Messgrößen-Paare.** Bei der #321-Verifikation aufgetaucht: `SEMANTIC HIT cos=0.985` zwischen Fleisch**verzehr**- und Fleisch**verbrauch**-Claim (und cos=0.963 auch bei stark umformulierter Gegenfrage) — die korrekte Verbrauchs-Frage bekam das `mostly_false` des Verzehrs-Claims mit voller Konfidenz. Neuer Zweig `_measure_mismatch` + `_MEASURE_PAIRS`. | erledigt |
| **NEU MITTEL 10** | ⛔ **OFFEN — Meloni-Regel greift nicht mehr.** Kontroll-Claim „Giorgia Meloni ist Italiens Ministerpräsidentin." → `mostly_false@0.85`, obwohl die Summary den Claim **bestätigt**. Log: `STRUKTURELL FALSCH override: LLM returned 'true' @ 0.9 despite STRUKTURELL FALSCH marker in sources (ratio 3/9 = 33%)`. Die Marker kommen aus **Wikidata** (5 Results) — ParlGov emittiert für die ITA-Zeile nachweislich keinen (gegen die Fassungen vor und nach PR #116 geprüft, beide `STRUKT: False`). Also **nicht** durch die #327-Änderung verursacht, sondern eine seit QA100 (dort 6/6) eingetretene Drift der Meloni-Regel. Braucht eigene Diagnose der `active_positions`-Unterdrückung in `wikidata.py`. | mittel, Neubau |
| ~~MITTEL 4~~ | ✅ **ERLEDIGT (PR #114, prod `580fe0e`)** — Über-Trigger-Sweep. Die genannten Quoten waren überwiegend ein Suffix-Artefakt; die vier **echten** Lecks (EIGE, ETER, AT-Neutralität, MedienTransparenz) sind geschlossen und live gegengeprüft. | erledigt |
| **NEU MITTEL 7** | **`mem_limit` senken.** `docker-compose.yml` setzt `mem_limit: 4g` / `memswap_limit: 4g` für das Backend — der CX22-Host hat aber nur **3.814 MB**. Das cgroup-Limit liegt damit **über** dem physischen RAM und schützt den Host nicht: am 08.08. riss ein zweiter Python-Prozess im Container die ganze Maschine mit (SSH und nginx tot, Hard-Reboot nötig). Empfehlung `3g` — vorher den realen Cold-Start-Bedarf messen, sonst OOM-Kill während des Model-Prefetch. | klein, aber messpflichtig |
| ~~MITTEL 5~~ | ✅ **ERLEDIGT (PR #112)** — Pattern K (im Container reproduziert): „schwachsinn"/„unfug"/„nonsens" ergänzen **und** das Kopula-Gate um den unbestimmten Artikel + Verstärker („totaler/purer/absoluter") erweitern — sonst fällt sogar „ist **ein** Schmarrn" durch, obwohl das Wort im Literal-Set steht. | klein |
| ~~MITTEL 6~~ | ✅ **ERLEDIGT (PR #113)** — `_POLARITY_ANTONYMS` (im Container reproduziert) um häufiger/seltener, wärmer/kälter, länger/kürzer, älter/jünger, früher/später, oft/selten ergänzen. Produktionsrisiko: invertiertes Verdict mit voller Konfidenz. | klein |
| NIEDRIG | Bevölkerungs-Vintage auf 1.4.2026 (9.220.882) nachziehen. | klein |

---

## 8. Methodik-Lehren

1. **Ein QA-Lauf ist auch ein Verfügbarkeits-Test — und war hier der einzige Monitor, der
   angeschlagen hat.** Container `healthy` + Website HTTP 200 + CI grün beschrieben einen Dienst,
   der zu 100 % seiner Kernfunktion beraubt war. Neben „grüne CI ≠ Guard läuft" gilt jetzt
   **„healthy ≠ funktionsfähig": ein Healthcheck, der den teuersten externen Abhängigkeitspfad
   auslässt, misst die falsche Sache.**
2. **Ein 401 ist nicht zwingend ein ungültiger Key.** Derselbe unveränderte Key lieferte zehn Tage
   später wieder 200. Vor dem Key-Tausch immer erst prüfen, ob es ein Kontingent-Zustand ist.
3. **Der Cache-Bust-Suffix ist nie neutral.** Gemeinsame Zahl → Phantom-Treffer (QA100);
   eigene Zahl → Cache komplett aus (QA50D). **Polaritäts- und Cache-Verhalten gehören immer in
   einen separaten, suffixlosen Lauf nach Restart.** Und der Suffix landet als Entität im Retrieval.
4. **„Schwankend" ist nicht dasselbe wie „harmlos".** #316 wirkt mit 4/6 wie Varianz — tatsächlich
   ist der Defekt deterministisch und nur die **Auslösebedingung** variiert: immer wenn die Summary
   eine Altersangabe über der Schwelle enthält, invertiert E ein korrektes Ergebnis. Die richtige
   Frage ist nicht „wie oft war das Verdict falsch", sondern **„wie oft trat die Konstellation auf,
   und was passierte dann"**.
5. **Override-Aktivität mitzählen.** Drei Overrides auf 155 Durchläufe, davon zwei schädlich, ist
   eine aussagekräftigere Kennzahl als die PASS-Quote — sie zeigt, dass die im Juli gebaute
   L4-Kaskade in diesem Claim-Satz praktisch inaktiv war.
6. **Adjudikations-Agents scheitern am Spend-Limit — das muss im Report stehen.** 1 von 8 Batches
   fiel aus (`infrastruktur-energie`); die betroffenen Claims #303/#304/#313/#322/#323 habe ich
   danach solo per WebSearch geprüft (alle bestätigten die Vorgabe). Zwei echte Überstimmungen:
   **#326** Linz 214.987 statt „rund 210.000" → `mostly_true`; **#341** Cybercrime-Peak war 2023,
   2024 ging es um 5,4 % **zurück** → `mostly_true`. Beide fielen in mein `acceptable`-Set.
