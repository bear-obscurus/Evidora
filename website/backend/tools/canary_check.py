#!/usr/bin/env python3
"""Kanarienvogel: feuert EINEN echten Claim gegen die Pipeline.

Warum es das gibt (QA50D HOCH 2): Am 28./29.07.2026 war Evidora rund
zwölf Stunden vollständig ausgefallen — der Mistral-Key lieferte 401,
jeder Faktencheck brach im Analyze-Schritt ab. Gemeldet hat es nichts:
der Container-Healthcheck prüfte einen statischen Endpunkt, die Website
lieferte HTTP 200, die CI war grün, und **keiner der sieben Cron-Jobs
berührte je `/api/check`**. Entdeckt wurde der Ausfall nur, weil zufällig
ein QA-Lauf anstand.

Dieser Job schließt die Lücke: 1× täglich ein fixer Claim durch die
KOMPLETTE Pipeline (Analyzer → Quellen → Synthesizer). Schlägt er fehl,
geht ein ntfy-Push raus.

Der Claim ist bewusst zeitlos und unstrittig ("Wien ist die Hauptstadt
Österreichs") — er soll die Verfügbarkeit messen, nicht die Verdict-
Qualität. Bei täglichem Lauf ist der 30-Minuten-Verdict-Cache immer kalt,
es ist also garantiert ein echter Pipeline-Durchlauf.

DIE ADRESSE (korrigiert am 18.10.2026)
=====================================
Der Default war ``http://localhost:8000``, und das war richtig, solange
der Job mit ``docker compose exec backend`` IM servierenden Container
lief. PR #249 hat die Wartung in einen eigenen Einmal-Container verlegt —
dort ist ``localhost`` sein eigener Netzwerk-Namespace. Der Kanarienvogel
lief danach sechs Tage ins Leere (13.–18.10.), jeden Morgen mit
``Connection refused``, und meldete das als "Pipeline liefert kein
Ergebnis". Gemessen aus dem Wartungs-Container:

    http://backend:8000/api/health   -> 200
    http://localhost:8000/api/health -> Connection refused

Der Default heisst deshalb jetzt ``http://backend:8000`` — der
Compose-Servicename, der aus jedem Container des Projekts aufloest, auch
aus dem Backend selbst. Auf einem Entwicklungsrechner OHNE Compose-Netz
loest er nicht auf; dort ``EVIDORA_CANARY_URL`` setzen oder ``--url``
uebergeben.

Verwendung:
  python3 tools/canary_check.py [--url http://backend:8000]
                                [--timeout 180] [--alert-webhook URL]

Cron (auf prod), über run_evidora_tool.sh im Wartungs-Container:
  0 5 * * * /opt/Evidora/website/run_evidora_tool.sh canary_check.py \
            >> /home/burrito/evidora-logs/canary.log 2>&1

EXIT-CODES — je Ausgang ein eigener (Lehre aus #236)
====================================================
  0  Kanarienvogel lebt.
  1  Das Backend ANTWORTET, liefert aber kein brauchbares Verdict.
     Das ist der Ausfall vom 29.07.: Pipeline defekt.
  3  Das Backend ist unter der konfigurierten Adresse NICHT ERREICHBAR.
     Das kann heissen: der Dienst ist weg — oder die Adresse ist falsch.
     Beides ist ein Alarm, aber es sind zwei verschiedene Ursachen, und
     der Alarm behauptet keine davon. Genau diese Verwechslung kostete
     sechs Tage: Der Cron-Zweig schrieb "Backend-Container down?",
     waehrend der Container gesund lief.
  2  Aufruf-/Konfigurationsfehler.
  4  Das Backend liefert ein brauchbares Verdict, aber ZU LANGSAM: Die
     ganze Pruefung oder die Synthese lag ueber ihrer Schwelle. Neu am
     21.11.2026. Am 20.11. dauerte eine Synthese 128 s, und der
     Kanarienvogel war still, weil er nur Fehler kannte. Ein Fehler hat
     Vorrang: Kommt kein Verdict, bleibt es Exit 1, auch wenn es zudem
     langsam war.

DIE SCHWELLEN (gewaehlt am 21.11.2026, Begruendung im PR)
=========================================================
Grundlage sind 17 Laeufe des Kanarienvogels vom 5. bis 21.11.2026, aus
den Zeitstempeln in backend.log:

  ganze Pruefung   Median 11,5 s, Maximum 34,5 s  -> Schwelle 60 s
  Synthese         Median  5,5 s, Maximum 17,3 s  -> Schwelle 30 s
                   (alle Pruefungen seit 4.11.: Maximum 19 s)

Gemessen wird auf der Seite des Kanarienvogels: die ganze Pruefung vom
Health-Check bis zum Ende des SSE-Streams, die Synthese vom Frame
``step: synthesize`` bis zum Ergebnis-Frame. Bei einem Cache-Treffer gibt
es keine Synthese; dann zaehlt nur die Gesamtdauer. Die Dauern stehen bei
jedem Lauf im canary.log, damit eine spaetere Schwellenwahl Daten hat.
"""
import argparse
import json
import os
import sys
import time
import urllib.request

#: Welche Exit-Codes einen BEFUND bedeuten — also: das Werkzeug ist
#: gelaufen, hat etwas gefunden und den ntfy-Push SELBST geschickt. Der
#: Cron darf dann keinen zweiten schicken. Alles andere ausser 0 heisst
#: "kam nicht zum Laufen", und DAS muss der Cron pushen, weil das
#: Werkzeug es nicht mehr kann.
#:
#: Maschinenlesbar und nicht nur im Docstring, weil ein Gate es prueft:
#: tests/test_cron_aufrufe.py las bis zum 18.10.2026 eine von Hand
#: gefuehrte Liste im Test selbst. Darin fehlte stichtag_check.py — und
#: genau dessen Cron-Zeile warf sechs Tage lang einen OOM-Kill (exit 137)
#: mit einem Befund zusammen. Eine Liste, die man pflegen muss, vergisst
#: man.
BEFUND_EXITS: tuple[int, ...] = (1, 3, 4)

#: Exit-Code fuer "Ergebnis kam, aber zu langsam". Kollidiert mit nichts,
#: was Wrapper oder Docker selbst liefern (64, 75, 125-127, 137).
LANGSAM_EXIT = 4

#: Schwellen in Sekunden. Herleitung im Docstring oben.
SCHWELLE_GESAMT_S = 60.0
SCHWELLE_SYNTHESE_S = 30.0

#: Die Uhr, als Modul-Attribut, damit ein Test sie ersetzen kann.
_uhr = time.monotonic

#: Der Compose-Servicename des Backends. Siehe "DIE ADRESSE" oben.
STANDARD_URL = "http://backend:8000"

#: Ausnahmen, die "ich komme nicht hin" bedeuten — im Unterschied zu
#: "ich war da, die Antwort taugt nicht". ``URLError`` deckt Namens- und
#: Verbindungsfehler ab; ``HTTPError`` ist eine Unterklasse davon und
#: bedeutet das Gegenteil (der Dienst hat geantwortet), deshalb steht sie
#: in der Pruefung VOR dieser Liste.
NICHT_ERREICHBAR = (urllib.error.URLError, TimeoutError, ConnectionError,
                    OSError)

CANARY_CLAIM = "Wien ist die Hauptstadt Österreichs."
VALID_VERDICTS = {"true", "mostly_true", "mixed",
                  "mostly_false", "false", "unverifiable"}


def _post_alert(webhook: str, title: str, message: str,
                priority: str = "urgent",
                tags: str = "rotating_light") -> None:
    """ntfy-Push — gleiche Mechanik wie data_freshness_check."""
    if not webhook:
        print("WARN: kein EVIDORA_ALERT_WEBHOOK gesetzt — kein Push",
              file=sys.stderr)
        return
    try:
        req = urllib.request.Request(
            webhook, data=message.encode("utf-8"),
            headers={"Title": title, "Priority": priority,
                     "Tags": tags})
        urllib.request.urlopen(req, timeout=10)
        print("Alert gesendet.")
    except Exception as e:
        print(f"WARN: Alert-Push fehlgeschlagen: {e}", file=sys.stderr)


def _check_health(base_url: str, timeout: float) -> tuple[str, str]:
    """``("ok"|"antwortet_nicht"|"schlechte_antwort", Meldung)``.

    Drei Ausgaenge statt zwei: Ein ``HTTPError`` heisst, der Dienst hat
    geantwortet (nur mit einem Fehlerstatus) — ein ``URLError`` heisst,
    niemand war da. Die alte Fassung warf beides in ein ``False`` und
    liess den Aufrufer "Pipeline liefert kein Ergebnis" daraus machen.
    """
    try:
        with urllib.request.urlopen(
                f"{base_url}/api/health/full", timeout=timeout) as r:
            body = json.loads(r.read().decode("utf-8"))
        return "ok", f"ready ({body.get('details', {}).get('llm_auth', '?')})"
    except urllib.error.HTTPError as e:
        # VOR NICHT_ERREICHBAR, weil HTTPError eine URLError-Unterklasse
        # ist und das Gegenteil bedeutet: der Dienst hat geantwortet.
        try:
            body = json.loads(e.read().decode("utf-8"))
        except Exception:
            body = {}
        return "schlechte_antwort", (
            f"HTTP {e.code} — {json.dumps(body, ensure_ascii=False)[:300]}")
    except NICHT_ERREICHBAR as e:
        return "antwortet_nicht", f"{type(e).__name__}: {e}"
    except Exception as e:
        return "schlechte_antwort", f"{type(e).__name__}: {e}"


def _run_claim(base_url: str, timeout: float,
               key: str) -> tuple[bool, str, float | None]:
    """Feuert den Kanarienvogel-Claim und sucht den Verdict-Frame.

    Das dritte Feld ist die Dauer der Synthese in Sekunden: vom Frame
    ``{"step": "synthesize"}`` bis zum letzten Frame mit ``"verdict"``
    (dem Ergebnis). ``None``, wenn es keine Synthese gab (Cache-Treffer)
    oder kein Verdict kam.
    """
    body = json.dumps({"claim": CANARY_CLAIM}).encode()
    headers = {"Content-Type": "application/json"}
    if key:
        headers["X-Evidora-Test-Key"] = key
    req = urllib.request.Request(f"{base_url}/api/check",
                                 data=body, headers=headers)
    last_error_frame = ""
    t_synthese = t_verdict = None
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            verdict_line = None
            for raw in r:
                line = raw.decode("utf-8", "replace").strip()
                if not line.startswith("data:"):
                    continue
                payload = line[5:].strip()
                if '"verdict"' in payload:
                    verdict_line = payload
                    t_verdict = _uhr()
                elif ('"step"' in payload and '"synthesize"' in payload
                      and t_synthese is None):
                    t_synthese = _uhr()
                elif '"detail"' in payload:
                    last_error_frame = payload[:200]
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code} auf /api/check", None
    except NICHT_ERREICHBAR as e:
        return False, f"nicht erreichbar — {type(e).__name__}: {e}", None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}", None

    synthese_s = (t_verdict - t_synthese
                  if t_synthese is not None and t_verdict is not None
                  and t_verdict >= t_synthese else None)
    ok, msg = _pruefe_verdict(verdict_line, last_error_frame)
    return ok, msg, synthese_s


def _pruefe_verdict(verdict_line: str | None,
                    last_error_frame: str) -> tuple[bool, str]:
    """Taugt der Verdict-Frame? Unveraendert aus ``_run_claim`` gezogen."""
    if not verdict_line:
        # Genau die Signatur des 29.07.-Ausfalls:
        # event: error / data: {"detail": "Fehler bei der Claim-Analyse…"}
        return False, ("kein Verdict-Frame im SSE"
                       + (f" — Fehler-Frame: {last_error_frame}"
                          if last_error_frame else ""))
    try:
        d = json.loads(verdict_line)
    except Exception as e:
        return False, f"Verdict-Frame unparsbar: {e}"

    verdict = d.get("verdict")
    if verdict not in VALID_VERDICTS:
        return False, f"unbekanntes Verdict-Label: {verdict!r}"
    cov = d.get("source_coverage") or {}
    with_results = cov.get("with_results") or 0
    if with_results < 1:
        return False, (f"Verdict {verdict} aber KEINE Quelle mit Treffern "
                       f"— Retrieval tot?")
    return True, (f"verdict={verdict}@{d.get('confidence')}, "
                  f"{with_results} Quellen mit Treffern")


def alarm_text(base: str, health_lage: str, health_msg: str,
               claim_ok: bool, claim_msg: str) -> tuple[str, str]:
    """``(Titel, Nachricht)`` fuer den Push — als eigene Funktion.

    Lehre aus #236: Je Alarmweg ein Test, der die NACHRICHT baut. Der
    Fehler, der sechs Tage kostete, steckte nicht in der Logik, sondern
    im Text: "Backend-Container down?" war eine BEHAUPTUNG ueber die
    Ursache, und sie war falsch. Diese Funktion behauptet keine Ursache
    mehr — sie nennt, was geprueft wurde, unter welcher Adresse, und
    welche zwei Erklaerungen in Frage kommen.
    """
    if health_lage == "antwortet_nicht":
        return ("Evidora Kanarienvogel: Backend nicht erreichbar",
                (f"Unter {base} antwortet nichts.\n"
                 f"health/full: {health_msg}\n"
                 f"Zwei Erklaerungen kommen in Frage: der Dienst laeuft "
                 f"nicht, oder diese Adresse ist die falsche. Beides ist "
                 f"zu pruefen; der Kanarienvogel kann es nicht "
                 f"unterscheiden.\n"
                 f"Claim nicht gefeuert."))
    problems = []
    if health_lage != "ok":
        problems.append(f"health/full: {health_msg}")
    if not claim_ok:
        problems.append(f"canary-claim: {claim_msg}")
    return ("Evidora Kanarienvogel ALARM",
            (f"Das Backend unter {base} antwortet, liefert aber kein "
             f"brauchbares Ergebnis.\n"
             + "\n".join(problems)
             + f"\nClaim: {CANARY_CLAIM}"))


def langsam_befunde(gesamt_s: float, synthese_s: float | None,
                    schwelle_gesamt: float = SCHWELLE_GESAMT_S,
                    schwelle_synthese: float = SCHWELLE_SYNTHESE_S
                    ) -> list[str]:
    """Welche Dauer liegt ueber ihrer Schwelle? Leer heisst: keine."""
    out = []
    if gesamt_s > schwelle_gesamt:
        out.append(f"Gesamtdauer {gesamt_s:.1f} s "
                   f"(Schwelle {schwelle_gesamt:.0f} s)")
    if synthese_s is not None and synthese_s > schwelle_synthese:
        out.append(f"Synthese {synthese_s:.1f} s "
                   f"(Schwelle {schwelle_synthese:.0f} s)")
    return out


def _dauer_text(gesamt_s: float, synthese_s: float | None) -> str:
    syn = (f"{synthese_s:.1f} s" if synthese_s is not None
           else "nicht gemessen (keine Synthese)")
    return f"gesamt={gesamt_s:.1f} s, synthese={syn}"


def langsam_text(base: str, gesamt_s: float, synthese_s: float | None,
                 befunde: list[str]) -> tuple[str, str]:
    """``(Titel, Nachricht)`` fuer den Langsam-Push.

    Enthaelt bewusst keinen Claim-Text: Was gemeldet wird, ist eine
    Dauer, kein Inhalt. Auch der ntfy-Topic steht nicht darin.
    """
    return ("Evidora Kanarienvogel: langsam",
            ("Die Pruefung lieferte ein gueltiges Ergebnis, aber zu "
             "langsam.\n"
             "Ueberschritten:\n"
             + "\n".join(f"- {b}" for b in befunde)
             + f"\nGemessen: {_dauer_text(gesamt_s, synthese_s)}"
             + f"\nAdresse: {base}"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.getenv(
        "EVIDORA_CANARY_URL", STANDARD_URL))
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--max-gesamt", type=float,
                        default=SCHWELLE_GESAMT_S,
                        help="Schwelle fuer die ganze Pruefung in s")
    parser.add_argument("--max-synthese", type=float,
                        default=SCHWELLE_SYNTHESE_S,
                        help="Schwelle fuer die Synthese in s")
    parser.add_argument("--alert-webhook",
                        default=os.getenv("EVIDORA_ALERT_WEBHOOK", ""))
    args = parser.parse_args()
    base = args.url.rstrip("/")
    key = os.getenv("EVIDORA_TEST_API_KEY", "")  # umgeht das Rate-Limit

    start = _uhr()
    synthese_s = None
    health_lage, health_msg = _check_health(base, min(args.timeout, 30.0))
    health_ok = health_lage == "ok"
    # Ist niemand da, hat ein zweiter Anlauf ueber /api/check keinen Wert —
    # er wartet nur das Timeout ab (bis 180 s) und sagt dasselbe.
    if health_lage == "antwortet_nicht":
        claim_ok, claim_msg = False, "nicht versucht — Backend antwortet nicht"
    else:
        claim_ok, claim_msg, synthese_s = _run_claim(
            base, args.timeout, key)
    gesamt_s = _uhr() - start

    print(f"health/full : {'OK' if health_ok else 'FEHLER'} — {health_msg}")
    print(f"canary-claim: {'OK' if claim_ok else 'FEHLER'} — {claim_msg}")
    print(f"dauer       : {_dauer_text(gesamt_s, synthese_s)}")

    if health_ok and claim_ok:
        befunde = langsam_befunde(gesamt_s, synthese_s,
                                  args.max_gesamt, args.max_synthese)
        if not befunde:
            print("Kanarienvogel lebt.")
            return 0
        titel, msg = langsam_text(base, gesamt_s, synthese_s, befunde)
        print("LANGSAM:\n" + msg, file=sys.stderr)
        _post_alert(args.alert_webhook, titel, msg,
                    priority="high", tags="hourglass")
        return LANGSAM_EXIT

    titel, msg = alarm_text(base, health_lage, health_msg, claim_ok, claim_msg)
    print("FEHLER:\n" + msg, file=sys.stderr)
    _post_alert(args.alert_webhook, titel, msg)
    return 3 if health_lage == "antwortet_nicht" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(2)
