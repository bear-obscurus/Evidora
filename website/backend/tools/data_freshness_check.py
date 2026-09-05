#!/usr/bin/env python3
"""Data-Freshness-Check für die Static-First-Quellen.

Prüft das `fetched_at_iso` Feld in jeder data/*.json und warnt bei
> 90 Tagen Alter. Cron-tauglich, Output ist menschen-lesbar.

Hintergrund: Statt für jede Static-First-Quelle einen vollen Live-API-
Pfad zu bauen (was pro Quelle 4–8 h kostet), erinnert dieser Job
einmal pro Woche an manuellen Refresh-Bedarf. Die meisten Static-
Quellen sind ohnehin nur 1–4× pro Jahr aktualisiert (Eurostat-Crime,
RKI-TB, OECD Health, OeNB-Wohnindex). Eine wöchentliche Cron-Mahnung
ist genug.

Aufruf:
  python3 tools/data_freshness_check.py [--max-age-days N] [--strict]

Cron (auf prod) — über run_evidora_tool.sh im Backend-Container (reicht
EVIDORA_ALERT_WEBHOOK aus .env für den ntfy-Push durch; /var/log ist für
burrito nicht beschreibbar, daher evidora-logs/):
  0 4 * * 1 /opt/Evidora/website/run_evidora_tool.sh data_freshness_check.py \
            --max-age-days 120 --strict \
            >> /home/burrito/evidora-logs/data_freshness.log 2>&1
"""
import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Generierte Caches: kein fetched_at_iso, werden separat auf Gesundheit
# geprüft (Existenz/Größe/Alter) statt json-geladen — cordis ist ~110 MB.
# Format: name -> (min_bytes, max_age_days | None = Alter egal)
GENERATED_CACHES = {
    "cordis_projects_slim.json": (50_000_000, 100),   # Quartals-Cron + Puffer
    "claimreview_index.json": (1_000_000, None),      # beim Backend-Start neu gebaut
    "mitre_attack.json": (1_000_000, None),           # STIX-Prefetch beim Backend-Start
}


def check_generated_caches(data_dir: Path) -> list[str]:
    """Gesundheits-Check der generierten Cache-Dateien.

    Fängt stille Refresh-Fehlschläge (Lehrgeld 2026-07-02: der CORDIS-
    Quartals-Cron lief nach einer Upstream-Format-Umstellung unbemerkt
    auf 0 Records). Returns: Liste menschenlesbarer Probleme (leer = ok).
    """
    problems = []
    for name, (min_bytes, max_age_days) in GENERATED_CACHES.items():
        p = data_dir / name
        if not p.exists():
            problems.append(
                f"{name}: FEHLT (Refresh nie gelaufen oder fehlgeschlagen?)"
            )
            continue
        size = p.stat().st_size
        if size < min_bytes:
            problems.append(
                f"{name}: nur {size/1e6:.1f} MB (< {min_bytes/1e6:.0f} MB "
                f"Minimum) — Refresh lieferte vermutlich leere/kaputte Daten"
            )
        if max_age_days is not None:
            age_days = (datetime.now().timestamp() - p.stat().st_mtime) / 86400
            if age_days > max_age_days:
                problems.append(
                    f"{name}: {age_days:.0f} d alt (> {max_age_days} d) — "
                    f"Refresh-Cron läuft nicht mehr durch"
                )
    return problems


def _parse_iso_date(s: str) -> date | None:
    if not s:
        return None
    s = s.strip()
    # akzeptiere "2026-04-29" oder "2026-04-29T..."
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(s[:10])
        except ValueError:
            return None


def _scan_json(path: Path) -> tuple[date | None, str, str | None]:
    """Returns (fetched_at_date, source_label, refresh_kadenz)."""
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return (None, f"<read error: {e}>", None)
    if not isinstance(data, dict):
        return (None, "<not a dict — list-style data>", None)
    fetched_at = _parse_iso_date(data.get("fetched_at_iso", ""))
    label = data.get("source_label", "")
    return (fetched_at, label, data.get("refresh_kadenz"))


# Klassen der Freshness-Pruefung. Rein, damit sie ohne Dateisystem
# testbar ist — in der CI sind alle mtimes der Checkout-Zeitpunkt und
# damit als Testgrundlage wertlos.
VERALTET = "VERALTET"                   # Datei laenger nicht angefasst
FELD_UNGEPFLEGT = "FELD_UNGEPFLEGT"     # Feld alt, Datei aber frisch
FRISCH = "FRISCH"


# Wie oft die Quelle upstream ueberhaupt neu erscheint. Ohne diese Angabe
# gilt fuer alle derselbe Schwellwert — und der ist zwangslaeufig fuer die
# einen zu lax und fuer die anderen zu streng:
#
#   rki_surveillance wird woechentlich fortgeschrieben. Bei 120 Tagen
#   Schwelle schlaegt der Wecker erst nach 17 verpassten Ausgaben an.
#   rsf traegt den World Press Freedom Index 2026 — die neueste Ausgabe,
#   die es gibt, denn RSF publiziert einmal im Jahr im Mai. Nach 127 Tagen
#   ohne Aenderung meldete der Check ihn trotzdem als VERALTET, obwohl die
#   Werte am 2026-09-05 Zeichen fuer Zeichen mit rsf.org uebereinstimmten
#   (Oesterreich Rang 19, Score 79,43).
#
# Deshalb: Die Datei sagt selbst, in welchem Takt ihre Quelle erscheint.
# Ohne Angabe bleibt es beim CLI-Standard.
KADENZ_MAX_AGE = {
    "woechentlich": 21,        # eine verpasste Ausgabe plus Puffer
    "monatlich": 60,
    "quartalsweise": 150,
    "jaehrlich": 400,          # ein Jahr plus Puffer fuer den Publikationstermin
    "ereignisgetrieben": None,  # Wahlen, Urteile: kein Takt, kein Alarm
}
ANLASSBEZOGEN = "ANLASSBEZOGEN"  # ereignisgetrieben -> nie VERALTET


def schwelle_fuer(kadenz, standard: int) -> int | None:
    """Welcher Schwellwert gilt fuer diese Datei?

    ``None`` heisst: kein Alters-Alarm (ereignisgetriebene Quellen).
    Unbekannte oder fehlende Angabe -> Standard.
    """
    if kadenz in KADENZ_MAX_AGE:
        return KADENZ_MAX_AGE[kadenz]
    return standard


def klassifiziere(feld_age, mtime_age: int, max_age: int, kadenz=None) -> str:
    """Welche Klasse liegt vor?

    ``feld_age`` ist das Alter von ``fetched_at_iso`` (oder None, wenn die
    Datei das Feld nicht traegt), ``mtime_age`` das Alter der letzten
    echten Dateiaenderung, ``kadenz`` der deklarierte Erscheinungstakt der
    Quelle (siehe ``KADENZ_MAX_AGE``).

    Die Datei-mtime hat Vorrang: Sie sagt, ob wirklich seit Langem niemand
    hingesehen hat. Das alte Mass (nur ``fetched_at_iso``) meldete
    2026-09-05 51 Dateien, davon 31 FEHLALARME — Dateien, die kuerzlich
    geaendert worden waren, ohne dass jemand das Feld mitgezogen hatte.
    """
    grenze = schwelle_fuer(kadenz, max_age)
    if grenze is None:
        return ANLASSBEZOGEN

    # Ist eine Kadenz DEKLARIERT, entscheidet das Feld — nicht die mtime.
    # Grund: Die mtime sagt nur, dass jemand die Datei angefasst hat. Schon
    # das Eintragen der Kadenz selbst setzt sie zurueck und wuerde die
    # Veraltung verdecken (beim Bau dieser Funktion genau so passiert:
    # frontex.json rutschte durch das blosse Tagging von VERALTET auf
    # „kein Alarm", obwohl der Inhalt unveraendert 129 Tage alt war).
    # Wer eine Kadenz deklariert, verpflichtet sich zugleich,
    # ``fetched_at_iso`` zu pflegen — dann ist das Feld das ehrlichere Mass.
    if kadenz in KADENZ_MAX_AGE:
        if isinstance(feld_age, int):
            return VERALTET if feld_age > grenze else FRISCH
        return VERALTET if mtime_age > grenze else FRISCH

    # Ohne Kadenz-Angabe bleibt es beim Mass aus PR #130: die mtime hat
    # Vorrang, weil ``fetched_at_iso`` dort erfahrungsgemaess nicht
    # mitgezogen wird (61 % Fehlalarme).
    if mtime_age > grenze:
        return VERALTET
    if isinstance(feld_age, int) and feld_age > grenze:
        return FELD_UNGEPFLEGT
    return FRISCH


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-age-days", type=int, default=120,
        help="Schwellwert in Tagen (default: 120)")
    parser.add_argument("--strict", action="store_true",
        help="Exit code 1 wenn Schwellwert überschritten oder ein "
             "generierter Cache fehlt/zu klein/zu alt ist")
    parser.add_argument("--alert-webhook", default=os.getenv("EVIDORA_ALERT_WEBHOOK", ""),
        help="Optional URL für Alert-POST (JSON) bei Problemen — "
             "gleiche Mechanik wie weekly_phrasing_check; Default aus "
             "env EVIDORA_ALERT_WEBHOOK")
    args = parser.parse_args()

    if not DATA_DIR.exists():
        print(f"ERROR: data dir not found: {DATA_DIR}", file=sys.stderr)
        sys.exit(2)

    today = date.today()
    rows = []
    for path in sorted(DATA_DIR.glob("*.json")):
        if path.name in GENERATED_CACHES:
            continue  # separat geprüft; cordis (~110 MB) nicht json-laden
        fetched_at, label, kadenz = _scan_json(path)
        # mtime = wann die Datei zuletzt WIRKLICH geaendert wurde. Der
        # Docker-Build kopiert sie aus dem git-Checkout, und `git pull`
        # setzt die mtime auf den Pull-Zeitpunkt — verifiziert 2026-09-05:
        # 86 von 87 Dateien haben mtime == Datum des letzten Commits
        # (einzige Abweichung: mitre_attack.json, ein Laufzeit-Cache).
        mtime = date.fromtimestamp(path.stat().st_mtime)
        mtime_age = (today - mtime).days
        if fetched_at is None:
            rows.append((path.name, None, label, "—", mtime_age, kadenz))
            continue
        age = (today - fetched_at).days
        rows.append((path.name, fetched_at, label, age, mtime_age, kadenz))

    print(f"=== Evidora Data-Freshness-Check ({today.isoformat()}, max-age {args.max_age_days} d) ===")
    print()
    fmt = "  {:32s}  {:12s}  {:>6s}  {:32s}"
    print(fmt.format("file", "fetched_at", "feld/datei", "source"))
    print(fmt.format("-" * 32, "-" * 12, "-" * 6, "-" * 32))
    # ZWEI Klassen statt einer (2026-09-05). Vorher meldete der Job 51
    # Dateien als „stale" — 31 davon (61 %) waren FEHLALARME: die Datei war
    # kuerzlich geaendert worden, nur `fetched_at_iso` hatte niemand
    # mitgezogen. at_courts.json galt als 129 Tage alt, obwohl es am selben
    # Tag bearbeitet worden war. Ein Alarm, der zu 61 % daneben liegt,
    # trainiert an, ihn zu ignorieren — der ALERT vom 31.08. lag deshalb
    # eine Woche unbeachtet.
    #
    #   VERALTET        Datei seit > max-age NICHT angefasst -> echter
    #                   Refresh-Bedarf, loest Alarm + Exit 1 aus
    #   FELD UNGEPFLEGT fetched_at_iso alt, Datei aber frisch geaendert ->
    #                   die deklarierte Vintage stimmt nicht mehr. Nur Log,
    #                   KEIN Alarm: das ist Buchhaltung, kein Ausfall.
    stale_files, feld_ungepflegt, anlassbezogen = [], [], []
    for name, fetched_at, label, age, mtime_age, kadenz in rows:
        fa = fetched_at.isoformat() if fetched_at else "—"
        age_s = f"{age}d" if isinstance(age, int) else "—"
        klasse = klassifiziere(age, mtime_age, args.max_age_days, kadenz)
        grenze = schwelle_fuer(kadenz, args.max_age_days)
        marker = {VERALTET: "⚠", FELD_UNGEPFLEGT: "·",
                  ANLASSBEZOGEN: "~"}.get(klasse, " ")
        takt = f"[{kadenz}]" if kadenz else ""
        print(f"  {marker} " + fmt.format(
            name, fa, f"{age_s}/{mtime_age}d", (label or "")[:30])[2:] + takt)
        if klasse == VERALTET:
            # Welches Mass die Entscheidung getragen hat, muss auch in der
            # Meldung stehen — sonst steht dort "unveraendert seit 0 d
            # (Schwelle 60 d)" und niemand versteht den Alarm.
            if kadenz in KADENZ_MAX_AGE and isinstance(age, int):
                stale_files.append((name, age, kadenz, grenze, "fetched_at_iso"))
            else:
                stale_files.append((name, mtime_age, kadenz, grenze, "Datei"))
        elif klasse == FELD_UNGEPFLEGT:
            feld_ungepflegt.append((name, age, mtime_age))
        elif klasse == ANLASSBEZOGEN:
            anlassbezogen.append((name, mtime_age))

    print()
    cache_problems = check_generated_caches(DATA_DIR)
    if cache_problems:
        print(f"ALERT — {len(cache_problems)} generierte Caches ungesund:")
        for pr in cache_problems:
            print(f"  - {pr}")
    else:
        print(f"OK — {len(GENERATED_CACHES)} generierte Caches gesund "
              f"(Existenz/Größe/Alter).")

    print()
    if stale_files:
        print(f"⚠ {len(stale_files)} Dateien ueber ihrer Schwelle "
              f"(echter Refresh-Bedarf):")
        for name, age, kadenz, grenze, mass in stale_files:
            takt = f", Takt {kadenz}" if kadenz else ""
            print(f"  - {name}: {mass} {age} d alt "
                  f"(Schwelle {grenze} d{takt})")
    else:
        print(f"OK — alle {len(rows)} Dateien innerhalb ihrer Schwelle.")

    if anlassbezogen:
        print()
        print(f"~ {len(anlassbezogen)} ereignisgetriebene Quellen — kein Takt, "
              f"kein Alters-Alarm (Refresh bei Lage-Änderung):")
        for name, mtime_age in anlassbezogen:
            print(f"  - {name}: unverändert seit {mtime_age} d")

    if feld_ungepflegt:
        print()
        print(f"· {len(feld_ungepflegt)} × `fetched_at_iso` nicht mitgezogen "
              f"(Datei frisch, Feld alt) — Buchhaltung, kein Ausfall:")
        for name, feld_age, mtime_age in feld_ungepflegt:
            print(f"  - {name}: Feld {feld_age} d, Datei aber vor "
                  f"{mtime_age} d geändert")

    if (stale_files or cache_problems) and args.alert_webhook:
        try:
            import httpx
            # Klartext-Body statt JSON: ntfy.sh zeigt ihn direkt als
            # Push-Nachricht an (Title/Priority/Tags via Header).
            lines = ([f"Cache: {p}" for p in cache_problems]
                     + [f"Stale: {n} ({a} d)" for n, a in stale_files])
            httpx.post(
                args.alert_webhook,
                content=("data_freshness_check "
                         f"{today.isoformat()}\n" + "\n".join(lines))[:3800],
                headers={"Title": "Evidora Data-Freshness ALERT",
                         "Priority": "high", "Tags": "warning"},
                timeout=15,
            )
            print(f"  alert webhook posted to {args.alert_webhook}")
        except Exception as e:
            print(f"  alert webhook failed: {e}")

    if (stale_files or cache_problems) and args.strict:
        sys.exit(1)


if __name__ == "__main__":
    main()
