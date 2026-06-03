#!/bin/bash
# Usage: ./run_stress_batch.sh <start_id> <end_id> <output_file>
# Runs claims from stress_test_100_claims.json via SSH to evidora.eu
# Extracts verdict, confidence, and first 200 chars of summary

START=$1
END=$2
OUTPUT=$3
CLAIMS_FILE="$(dirname "$0")/stress_test_100_claims.json"

echo "[]" > "$OUTPUT"

for ID in $(seq $START $END); do
  CLAIM=$(python3 -c "
import json
with open('$CLAIMS_FILE') as f:
    claims = json.load(f)
for c in claims:
    if c['id'] == $ID:
        print(c['claim'])
        break
")
  EXPECTED=$(python3 -c "
import json
with open('$CLAIMS_FILE') as f:
    claims = json.load(f)
for c in claims:
    if c['id'] == $ID:
        print(c['expected'])
        break
")
  CLUSTER=$(python3 -c "
import json
with open('$CLAIMS_FILE') as f:
    claims = json.load(f)
for c in claims:
    if c['id'] == $ID:
        print(c['cluster'])
        break
")

  if [ -z "$CLAIM" ]; then
    echo "SKIP: No claim found for ID $ID"
    continue
  fi

  echo "Testing #$ID: $CLAIM"

  # Escape claim for JSON
  ESCAPED_CLAIM=$(echo "$CLAIM" | python3 -c "import sys,json; print(json.dumps(sys.stdin.read().strip()))")

  RESULT=$(ssh -o ConnectTimeout=10 burrito@evidora.eu "curl -s --max-time 120 -X POST -H 'Content-Type: application/json' -d '{\"claim\":$ESCAPED_CLAIM}' 'http://localhost:8000/api/check'" 2>/dev/null | grep '^data:' | grep '"verdict"' | tail -1 | sed 's/^data: //')

  if [ -z "$RESULT" ]; then
    VERDICT="ERROR"
    CONFIDENCE="0"
    SUMMARY="No response from API"
  else
    VERDICT=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('verdict','ERROR'))" 2>/dev/null || echo "PARSE_ERROR")
    CONFIDENCE=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('confidence',0))" 2>/dev/null || echo "0")
    SUMMARY=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); s=d.get('summary',''); print(s[:200])" 2>/dev/null || echo "")
  fi

  # Determine PASS/FAIL
  PASS="UNKNOWN"
  EXPECTED_LC=$(echo "$EXPECTED" | tr '[:upper:]' '[:lower:]')
  VERDICT_LC=$(echo "$VERDICT" | tr '[:upper:]' '[:lower:]')

  if echo "$EXPECTED_LC" | grep -q "needs_data"; then
    PASS="SKIP"
  elif echo "$EXPECTED_LC" | grep -q "/"; then
    # Multiple acceptable verdicts (e.g., "false/mostly_false")
    if echo "$EXPECTED_LC" | grep -q "$VERDICT_LC"; then
      PASS="PASS"
    else
      PASS="FAIL"
    fi
  elif echo "$EXPECTED_LC" | grep -q "@"; then
    # Verdict + confidence (e.g., "mixed@0.50")
    EXP_VERDICT=$(echo "$EXPECTED_LC" | cut -d@ -f1)
    if [ "$VERDICT_LC" = "$EXP_VERDICT" ]; then
      PASS="PASS"
    else
      PASS="FAIL"
    fi
  else
    if [ "$VERDICT_LC" = "$EXPECTED_LC" ]; then
      PASS="PASS"
    else
      PASS="FAIL"
    fi
  fi

  echo "  → #$ID: $VERDICT @ $CONFIDENCE (expected: $EXPECTED) → $PASS"

  # Append to output JSON
  python3 -c "
import json, sys
with open('$OUTPUT') as f:
    results = json.load(f)
results.append({
    'id': $ID,
    'claim': $(echo "$CLAIM" | python3 -c "import sys,json; print(json.dumps(sys.stdin.read().strip()))"),
    'cluster': '$CLUSTER',
    'expected': '$EXPECTED',
    'verdict': '$VERDICT',
    'confidence': $CONFIDENCE,
    'summary': $(echo "$SUMMARY" | python3 -c "import sys,json; print(json.dumps(sys.stdin.read().strip()))"),
    'pass': '$PASS'
})
with open('$OUTPUT', 'w') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
"
done

echo ""
echo "=== Batch $START-$END complete ==="
TOTAL=$(python3 -c "import json; d=json.load(open('$OUTPUT')); print(len(d))")
PASSES=$(python3 -c "import json; d=json.load(open('$OUTPUT')); print(sum(1 for r in d if r['pass']=='PASS'))")
FAILS=$(python3 -c "import json; d=json.load(open('$OUTPUT')); print(sum(1 for r in d if r['pass']=='FAIL'))")
SKIPS=$(python3 -c "import json; d=json.load(open('$OUTPUT')); print(sum(1 for r in d if r['pass']=='SKIP'))")
echo "Total: $TOTAL | PASS: $PASSES | FAIL: $FAILS | SKIP: $SKIPS"
