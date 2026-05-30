#!/bin/bash
# Wrapper: ruft ein Tool im laufenden Backend-Container auf.
# Verwendung:
#   ./run_evidora_tool.sh weekly_phrasing_check.py [args...]
#   ./run_evidora_tool.sh data_freshness_check.py [args...]
TOOL="$1"
shift
cd /opt/Evidora/website
exec docker compose exec -T \
  -e EVIDORA_TEST_API_KEY="$(grep ^EVIDORA_TEST_API_KEY .env | cut -d= -f2-)" \
  backend python3 /app/tools/"$TOOL" "$@"
