#!/usr/bin/env bash
# Read the adapter's topic (CANTON_KAFKA_BOOTSTRAP / CANTON_TX_TOPIC from .env), one line per record.
#
#   demo/topic.sh              # follow new records
#   demo/topic.sh all          # every record from the beginning, then exit
#   demo/topic.sh find <id>    # the record containing a contract_id / update_id / tracking id
set -euo pipefail
. "$(cd "$(dirname "$0")" && pwd)/env.sh"
require CANTON_KAFKA_BOOTSTRAP CANTON_TX_TOPIC
KCAT="$(command -v kcat || command -v kafkacat || true)"
[ -n "$KCAT" ] || { echo "kcat/kafkacat not found" >&2; exit 1; }
if ! "$KCAT" -b "$CANTON_KAFKA_BOOTSTRAP" -L -t "$CANTON_TX_TOPIC" 2>/dev/null | grep -q "topic \"$CANTON_TX_TOPIC\" with"; then
  echo "topic $CANTON_TX_TOPIC does not exist on $CANTON_KAFKA_BOOTSTRAP yet: start the adapter first (it creates it)" >&2
  exit 1
fi

FMT="$(cat <<'PY'
import json, sys

def name(e):
    t = e.get("template_id", "")
    if ":" not in t:
        return e.get("party", "")
    return t.split(":", 1)[1] + (f"/{e['choice']}" if e.get("choice") else "")

def line(u):
    ev = [f"{e['type']} {name(e)[:70]}".strip() for e in u.get("events", [])]
    more = f" (+{len(ev) - 6} more)" if len(ev) > 6 else ""
    return f"{u['kind']:<12} offset={u.get('offset')} update_id={(u.get('update_id') or '-')[:24]}  " + ", ".join(ev[:6]) + more

mode = sys.argv[1]
if mode == "find":
    needle = sys.argv[2]
    hits = [json.loads(r) for r in sys.stdin if needle in r]
    if not hits:
        sys.exit(f"not on the topic (yet): {needle}")
    for u in hits:
        print(f"FOUND: kind={u['kind']} offset={u['offset']} update_id={u.get('update_id')}")
        for e in u.get("events", []):
            print(f"    {e['type']:<22} {name(e)}")
else:
    for raw in sys.stdin:
        print(line(json.loads(raw)), flush=True)
PY
)"

case "${1:-watch}" in
  watch) "$KCAT" -b "$CANTON_KAFKA_BOOTSTRAP" -t "$CANTON_TX_TOPIC" -C -o end -q -u | python3 -u -c "$FMT" watch ;;
  all)   "$KCAT" -b "$CANTON_KAFKA_BOOTSTRAP" -t "$CANTON_TX_TOPIC" -C -o beginning -e -q | python3 -c "$FMT" all ;;
  find)
    [ -n "${2:-}" ] || { echo "usage: $0 find <contract_id|update_id|tracking_id>" >&2; exit 2; }
    "$KCAT" -b "$CANTON_KAFKA_BOOTSTRAP" -t "$CANTON_TX_TOPIC" -C -o beginning -e -q | python3 -c "$FMT" find "$2" ;;
  *) sed -n '2,6p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
