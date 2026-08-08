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

Verwendung:
  python3 tools/canary_check.py [--url http://localhost:8000]
                                [--timeout 180] [--alert-webhook URL]

Cron (auf prod), über run_evidora_tool.sh im Backend-Container:
  0 5 * * * /opt/Evidora/website/run_evidora_tool.sh canary_check.py \
            >> /home/burrito/evidora-logs/canary.log 2>&1

Exit-Codes: 0 = alles gut, 1 = Pipeline defekt (Alert gesendet),
2 = Aufruf-/Konfigurationsfehler.
"""
import argparse
import json
import os
import sys
import urllib.request

CANARY_CLAIM = "Wien ist die Hauptstadt Österreichs."
VALID_VERDICTS = {"true", "mostly_true", "mixed",
                  "mostly_false", "false", "unverifiable"}


def _post_alert(webhook: str, title: str, message: str) -> None:
    """ntfy-Push — gleiche Mechanik wie data_freshness_check."""
    if not webhook:
        print("WARN: kein EVIDORA_ALERT_WEBHOOK gesetzt — kein Push",
              file=sys.stderr)
        return
    try:
        req = urllib.request.Request(
            webhook, data=message.encode("utf-8"),
            headers={"Title": title, "Priority": "urgent",
                     "Tags": "rotating_light"})
        urllib.request.urlopen(req, timeout=10)
        print("Alert gesendet.")
    except Exception as e:
        print(f"WARN: Alert-Push fehlgeschlagen: {e}", file=sys.stderr)


def _check_health(base_url: str, timeout: float) -> tuple[bool, str]:
    """/api/health/full — meldet u.a. den LLM-Auth-Pfad."""
    try:
        with urllib.request.urlopen(
                f"{base_url}/api/health/full", timeout=timeout) as r:
            body = json.loads(r.read().decode("utf-8"))
        return True, f"ready ({body.get('details', {}).get('llm_auth', '?')})"
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode("utf-8"))
        except Exception:
            body = {}
        return False, f"HTTP {e.code} — {json.dumps(body, ensure_ascii=False)[:300]}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _run_claim(base_url: str, timeout: float, key: str) -> tuple[bool, str]:
    """Feuert den Kanarienvogel-Claim und sucht den Verdict-Frame."""
    body = json.dumps({"claim": CANARY_CLAIM}).encode()
    headers = {"Content-Type": "application/json"}
    if key:
        headers["X-Evidora-Test-Key"] = key
    req = urllib.request.Request(f"{base_url}/api/check",
                                 data=body, headers=headers)
    last_error_frame = ""
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
                elif '"detail"' in payload:
                    last_error_frame = payload[:200]
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.getenv(
        "EVIDORA_CANARY_URL", "http://localhost:8000"))
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--alert-webhook",
                        default=os.getenv("EVIDORA_ALERT_WEBHOOK", ""))
    args = parser.parse_args()
    base = args.url.rstrip("/")
    key = os.getenv("EVIDORA_TEST_API_KEY", "")  # umgeht das Rate-Limit

    health_ok, health_msg = _check_health(base, min(args.timeout, 30.0))
    claim_ok, claim_msg = _run_claim(base, args.timeout, key)

    print(f"health/full : {'OK' if health_ok else 'FEHLER'} — {health_msg}")
    print(f"canary-claim: {'OK' if claim_ok else 'FEHLER'} — {claim_msg}")

    if health_ok and claim_ok:
        print("Kanarienvogel lebt.")
        return 0

    problems = []
    if not health_ok:
        problems.append(f"health/full: {health_msg}")
    if not claim_ok:
        problems.append(f"canary-claim: {claim_msg}")
    msg = ("Evidora-Pipeline liefert kein Ergebnis.\n"
           + "\n".join(problems)
           + f"\nClaim: {CANARY_CLAIM}\nURL: {base}")
    print("FEHLER:\n" + msg, file=sys.stderr)
    _post_alert(args.alert_webhook, "Evidora Kanarienvogel ALARM", msg)
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(2)
