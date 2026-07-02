#!/bin/bash
# Evidora Deploy — ein Befehl, richtige Aktion, belegtes Ergebnis.
#
# Kodifiziert die Deploy-Decision-Table (ARCHITECTURE §4.1) und beendet
# zwei wiederkehrende Fehlklassen:
#   - restart-vs-build-Falle: nach Code-Änderungen wurde restartet statt
#     gebaut -> Container lief still mit altem Image weiter.
#   - unbelegte Deploys: "Container-Uptime-Reset ist der einzige
#     verlässliche Deploy-Beleg" (Lehrgeld 2026-06-14) — das Skript
#     prüft StartedAt vorher/nachher UND vergleicht die Hashes geänderter
#     Backend-Dateien Host vs. Container.
#
# Nutzung (auf dem Server):
#   /opt/Evidora/website/deploy.sh [--dry-run] [--force-build]
#
# Entscheidungslogik (aus git diff alt..neu):
#   nur website/backend/data/*      -> nichts (Hot-Reload via Mount;
#                                      Mount wird verifiziert, sonst Build)
#   website/backend/* (sonst)       -> docker compose up -d --build backend
#   website/frontend/*              -> docker compose up -d --build frontend
#   website/docker-compose.yml      -> docker compose up -d (Recreate)
#   alles andere (Docs, .github, …) -> nichts
#
# Bei Fehlschlag: Exit != 0 + ntfy-Push (EVIDORA_ALERT_WEBHOOK aus .env).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_DIR="$SCRIPT_DIR"
CONTAINER=evidora-backend-1
HEALTH_TIMEOUT_S=420

DRY_RUN=0
FORCE_BUILD=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    --force-build) FORCE_BUILD=1 ;;
    *) echo "Unbekannte Option: $arg (erlaubt: --dry-run, --force-build)"; exit 2 ;;
  esac
done

log() { printf '[deploy %s] %s\n' "$(date +%H:%M:%S)" "$*"; }

fail() {
  log "FEHLER: $*"
  local url
  url=$(grep '^EVIDORA_ALERT_WEBHOOK=' "$COMPOSE_DIR/.env" 2>/dev/null | cut -d= -f2- || true)
  if [ -n "${url:-}" ] && [ "$DRY_RUN" -eq 0 ]; then
    curl -s -m 10 -H "Title: Evidora Deploy FAILED" -H "Priority: high" \
      -H "Tags: rotating_light" -d "deploy.sh: $*" "$url" >/dev/null || true
  fi
  exit 1
}

cd "$REPO_DIR"

# --- 0) Vorbedingungen -----------------------------------------------------
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  git status --short --untracked-files=no
  fail "Arbeitsbaum hat lokale Änderungen an getrackten Dateien — erst committen/stashen."
fi

STARTED_BEFORE=$(docker inspect -f '{{.State.StartedAt}}' "$CONTAINER" 2>/dev/null || echo "n/a")
OLD_REV=$(git rev-parse --short HEAD)

# --- 1) Pull ----------------------------------------------------------------
log "git pull --ff-only (aktuell: $OLD_REV)…"
git pull --ff-only || fail "git pull --ff-only fehlgeschlagen (divergente Historie?)"
NEW_REV=$(git rev-parse --short HEAD)

CHANGED=""
if [ "$OLD_REV" != "$NEW_REV" ]; then
  CHANGED=$(git diff --name-only "$OLD_REV..$NEW_REV")
  log "Änderungen $OLD_REV..$NEW_REV:"
  echo "$CHANGED" | sed 's/^/    /'
else
  log "Keine neuen Commits."
fi

# --- 2) Entscheidung --------------------------------------------------------
build_backend=0; build_frontend=0; recreate=0; data_changed=0
while IFS= read -r f; do
  [ -z "$f" ] && continue
  case "$f" in
    website/backend/data/*)   data_changed=1 ;;
    website/docker-compose.yml) recreate=1 ;;
    website/frontend/*)       build_frontend=1 ;;
    website/backend/*)        build_backend=1 ;;
    website/run_evidora_tool.sh|website/deploy.sh) : ;;  # Host-Skripte, wirken ab sofort
    *) : ;;                                              # Docs/CI/Memory
  esac
done <<< "$CHANGED"

[ "$FORCE_BUILD" -eq 1 ] && build_backend=1

# data-only setzt funktionierenden Mount voraus — sonst Build als Fallback
if [ "$data_changed" -eq 1 ] && [ "$build_backend" -eq 0 ]; then
  if ! docker inspect -f '{{range .Mounts}}{{.Destination}}{{"\n"}}{{end}}' "$CONTAINER" 2>/dev/null \
      | grep -qx '/app/data'; then
    log "WARNUNG: /app/data-Mount fehlt — data-Änderungen erreichen den Container nicht. Fallback: Build."
    build_backend=1
  fi
fi

ACTIONS=""
[ "$build_backend" -eq 1 ]  && ACTIONS="$ACTIONS build-backend"
[ "$build_frontend" -eq 1 ] && ACTIONS="$ACTIONS build-frontend"
[ "$recreate" -eq 1 ]       && ACTIONS="$ACTIONS recreate"
[ -z "$ACTIONS" ] && ACTIONS=" keine (Hot-Reload/Docs-only)"
log "Entscheidung:$ACTIONS"

if [ "$DRY_RUN" -eq 1 ]; then
  log "--dry-run: keine Aktion ausgeführt."
  exit 0
fi

# --- 3) Ausführen -------------------------------------------------------------
cd "$COMPOSE_DIR"
backend_restart_expected=0
if [ "$build_backend" -eq 1 ]; then
  log "docker compose up -d --build backend…"
  docker compose up -d --build backend || fail "backend-Build fehlgeschlagen"
  backend_restart_expected=1
fi
if [ "$build_frontend" -eq 1 ]; then
  log "docker compose up -d --build frontend…"
  docker compose up -d --build frontend || fail "frontend-Build fehlgeschlagen"
fi
if [ "$recreate" -eq 1 ]; then
  # Läuft auch NACH einem Backend-Build: compose up -d ist dann für das
  # Backend ein No-op, wendet aber Compose-Änderungen an ALLEN Services an
  # (vorher wurden z.B. Frontend-Änderungen verschluckt).
  log "docker compose up -d (Compose-Änderung -> Recreate aller Services)…"
  docker compose up -d || fail "compose up fehlgeschlagen"
  backend_restart_expected=1
fi

# --- 4) Health-Wait -----------------------------------------------------------
if [ "$backend_restart_expected" -eq 1 ]; then
  log "Warte auf healthy (Timeout ${HEALTH_TIMEOUT_S}s — Model-Prefetch dauert ~5 min)…"
  waited=0
  while true; do
    st=$(docker inspect -f '{{.State.Health.Status}}' "$CONTAINER" 2>/dev/null || echo "missing")
    [ "$st" = "healthy" ] && { log "healthy nach ~${waited}s."; break; }
    [ "$waited" -ge "$HEALTH_TIMEOUT_S" ] && fail "Backend nach ${HEALTH_TIMEOUT_S}s nicht healthy (Status: $st)"
    sleep 5; waited=$((waited + 5))
  done
fi

# --- 5) Belege ------------------------------------------------------------------
STARTED_AFTER=$(docker inspect -f '{{.State.StartedAt}}' "$CONTAINER" 2>/dev/null || echo "n/a")
if [ "$backend_restart_expected" -eq 1 ]; then
  if [ "$STARTED_BEFORE" = "$STARTED_AFTER" ]; then
    fail "Container-StartedAt unverändert ($STARTED_AFTER) — Deploy hat NICHT stattgefunden!"
  fi
  log "Beleg 1: Container neu gestartet ($STARTED_BEFORE -> $STARTED_AFTER)."
  # Beleg 2: geänderte Backend-Dateien im Container identisch mit Host?
  checked=0
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    case "$f" in
      website/backend/data/*) continue ;;
      website/backend/*) : ;;
      *) continue ;;
    esac
    rel="${f#website/backend/}"
    [ -f "$REPO_DIR/$f" ] || continue          # gelöschte Dateien überspringen
    h_host=$(md5sum "$REPO_DIR/$f" | cut -d' ' -f1)
    h_cont=$(docker exec "$CONTAINER" md5sum "/app/$rel" 2>/dev/null | cut -d' ' -f1 || echo "fehlt")
    if [ "$h_host" != "$h_cont" ]; then
      fail "Beleg 2 FEHLGESCHLAGEN: /app/$rel im Container != Host (altes Image?)"
    fi
    checked=$((checked + 1)); [ "$checked" -ge 3 ] && break
  done <<< "$CHANGED"
  [ "$checked" -gt 0 ] && log "Beleg 2: $checked geänderte Backend-Datei(en) im Container hash-identisch."
else
  log "Kein Backend-Neustart nötig (StartedAt: $STARTED_AFTER)."
fi

# --- 6) Smoke -------------------------------------------------------------------
code=$(curl -s -o /dev/null -w '%{http_code}' -m 15 http://127.0.0.1:8000/api/legal || echo "000")
[ "$code" = "200" ] || fail "Smoke-Check /api/legal -> HTTP $code"
log "Beleg 3: /api/legal -> 200."

log "OK — Deploy $OLD_REV -> $NEW_REV abgeschlossen."
