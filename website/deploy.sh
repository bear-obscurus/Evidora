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
#     Backend-Dateien Host vs. Container. Seit Audit #3 (2026-07-07) gilt
#     dasselbe fürs Frontend: bei Frontend-Build/Recreate wird auf
#     frontend healthy gewartet (Healthcheck via busybox-wget, R2-3) +
#     StartedAt-Reset + Smoke http://127.0.0.1:3000 -> 200 geprüft. Vorher
#     druckte das Skript "OK" auch bei einem still gecrashten Frontend.
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
FRONTEND_CONTAINER=evidora-frontend-1
HEALTH_TIMEOUT_S=420
FRONTEND_HEALTH_TIMEOUT_S=60   # nginx startet schnell, kein Model-Prefetch

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
FE_STARTED_BEFORE=$(docker inspect -f '{{.State.StartedAt}}' "$FRONTEND_CONTAINER" 2>/dev/null || echo "n/a")
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
if [ "$build_backend" -eq 1 ]; then
  log "docker compose up -d --build backend…"
  docker compose up -d --build backend || fail "backend-Build fehlgeschlagen"
fi
if [ "$build_frontend" -eq 1 ]; then
  log "docker compose up -d --build frontend…"
  docker compose up -d --build frontend || fail "frontend-Build fehlgeschlagen"
fi
if [ "$recreate" -eq 1 ]; then
  # compose up -d erstellt NUR Services mit geänderter Config neu (NICHT
  # zwingend beide!) — läuft auch nach einem Build (dann No-op fürs
  # Gebaute) und wendet Compose-Änderungen an allen betroffenen Services an.
  log "docker compose up -d (Compose-Änderung -> Recreate geänderter Services)…"
  docker compose up -d || fail "compose up fehlgeschlagen"
fi

# Welche Container sollen jetzt laufen/healthy sein? Ein Build betrifft
# genau seinen Service; ein Recreate potenziell beide (welche, hängt von
# der geänderten Config ab — daher beide prüfen).
check_backend=0; check_frontend=0
[ "$build_backend" -eq 1 ] || [ "$recreate" -eq 1 ] && check_backend=1
[ "$build_frontend" -eq 1 ] || [ "$recreate" -eq 1 ] && check_frontend=1

# --- 4) Health-Wait -----------------------------------------------------------
if [ "$check_backend" -eq 1 ]; then
  log "Warte auf backend healthy (Timeout ${HEALTH_TIMEOUT_S}s — Model-Prefetch dauert ~5 min)…"
  waited=0
  while true; do
    st=$(docker inspect -f '{{.State.Health.Status}}' "$CONTAINER" 2>/dev/null || echo "missing")
    [ "$st" = "healthy" ] && { log "backend healthy nach ~${waited}s."; break; }
    [ "$waited" -ge "$HEALTH_TIMEOUT_S" ] && fail "Backend nach ${HEALTH_TIMEOUT_S}s nicht healthy (Status: $st)"
    sleep 5; waited=$((waited + 5))
  done
fi
if [ "$check_frontend" -eq 1 ]; then
  log "Warte auf frontend healthy (Timeout ${FRONTEND_HEALTH_TIMEOUT_S}s)…"
  waited=0
  while true; do
    st=$(docker inspect -f '{{.State.Health.Status}}' "$FRONTEND_CONTAINER" 2>/dev/null || echo "missing")
    [ "$st" = "healthy" ] && { log "frontend healthy nach ~${waited}s."; break; }
    [ "$waited" -ge "$FRONTEND_HEALTH_TIMEOUT_S" ] && fail "Frontend nach ${FRONTEND_HEALTH_TIMEOUT_S}s nicht healthy (Status: $st) — nginx-Config kaputt?"
    sleep 3; waited=$((waited + 3))
  done
fi

# --- 5) Belege ------------------------------------------------------------------
STARTED_AFTER=$(docker inspect -f '{{.State.StartedAt}}' "$CONTAINER" 2>/dev/null || echo "n/a")
FE_STARTED_AFTER=$(docker inspect -f '{{.State.StartedAt}}' "$FRONTEND_CONTAINER" 2>/dev/null || echo "n/a")
backend_restarted=0;  [ "$STARTED_BEFORE" != "$STARTED_AFTER" ] && backend_restarted=1
frontend_restarted=0; [ "$FE_STARTED_BEFORE" != "$FE_STARTED_AFTER" ] && frontend_restarted=1

# Ein Build MUSS seinen Container neu gestartet haben (sonst altes Image).
if [ "$build_backend" -eq 1 ] && [ "$backend_restarted" -eq 0 ]; then
  fail "Backend-StartedAt unverändert ($STARTED_AFTER) — Backend-Build hat nichts neu gestartet!"
fi
if [ "$build_frontend" -eq 1 ] && [ "$frontend_restarted" -eq 0 ]; then
  fail "Frontend-StartedAt unverändert ($FE_STARTED_AFTER) — Frontend-Build hat nichts neu gestartet!"
fi
# Reine Compose-Änderung (ohne Build): mindestens EIN Service muss neu
# erstellt worden sein — sonst wirkte die Änderung nicht zur Laufzeit.
if [ "$recreate" -eq 1 ] && [ "$build_backend" -eq 0 ] && [ "$build_frontend" -eq 0 ] \
   && [ "$backend_restarted" -eq 0 ] && [ "$frontend_restarted" -eq 0 ]; then
  fail "Compose-Recreate hat KEINEN Container neu gestartet — Änderung ohne Laufzeit-Wirkung?"
fi
[ "$backend_restarted" -eq 1 ]  && log "Beleg 1: Backend neu gestartet ($STARTED_BEFORE -> $STARTED_AFTER)."
[ "$frontend_restarted" -eq 1 ] && log "Beleg 1b: Frontend neu gestartet ($FE_STARTED_BEFORE -> $FE_STARTED_AFTER)."

# Beleg 2: geänderte Backend-Dateien im Container identisch mit Host?
# (nur nach einem Backend-Build sinnvoll)
if [ "$build_backend" -eq 1 ]; then
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
fi

# --- 6) Smoke -------------------------------------------------------------------
if [ "$check_backend" -eq 1 ]; then
  code=$(curl -s -o /dev/null -w '%{http_code}' -m 15 http://127.0.0.1:8000/api/legal || echo "000")
  [ "$code" = "200" ] || fail "Smoke-Check backend /api/legal -> HTTP $code"
  log "Beleg 3: backend /api/legal -> 200."
fi
# Frontend-Smoke direkt gegen den Container-Port — fängt eine kaputte
# nginx.conf, die der Build nicht bemerkt (Audit #3: vorher druckte das
# Skript OK trotz Site-Ausfall).
if [ "$check_frontend" -eq 1 ]; then
  fe_code=$(curl -s -o /dev/null -w '%{http_code}' -m 15 http://127.0.0.1:3000/ || echo "000")
  [ "$fe_code" = "200" ] || fail "Smoke-Check frontend http://127.0.0.1:3000/ -> HTTP $fe_code"
  log "Beleg 3b: frontend :3000 -> 200."
fi

log "OK — Deploy $OLD_REV -> $NEW_REV abgeschlossen."
