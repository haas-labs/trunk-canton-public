#!/usr/bin/env bash
# End-to-end smoke test against a Splice LocalNet: local Redpanda -> adapter bootstrap -> faucet tap
# -> adapter resume -> the tapped Amulet contract found on the local topic.
#
#   ./localnet-smoke.sh            # full run, one shot (no other terminal needed)
#   ./localnet-smoke.sh --reset    # forget the offset first (re-bootstrap: topology baseline + snapshot)
#   ./localnet-smoke.sh --tap      # only tap the faucet and print the new Amulet contract id
#
# Config: the repo-root .env (see .env.example). Needs network access to the node in .env (e.g. a
# VPN, if it is private), docker compose, python3, kcat, and the package installed. Publishes to a
# LOCAL broker only, never to a shared Kafka.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/../env.sh"
adapter_env
require WALLET_SENDER_API WALLET_SENDER_USER TAP_AMOUNT
PYTHON="${PYTHON:-python}"

tap() {
  echo "==> faucet tap $TAP_AMOUNT as $WALLET_SENDER_USER ($WALLET_SENDER_API)" >&2
  local resp
  resp="$(curl -sS --max-time 30 -X POST "$WALLET_SENDER_API/api/validator/v0/wallet/tap" \
    -H "Authorization: Bearer $(jwt_for "$WALLET_SENDER_USER")" -H "Content-Type: application/json" \
    -d "{\"amount\":\"$TAP_AMOUNT\"}")"
  printf '%s' "$resp" | python3 -c "import json,sys; print(json.load(sys.stdin)['contract_id'])" 2>/dev/null \
    || { echo "tap failed: $resp" >&2; return 1; }
}

RESET=0
case "${1:-}" in
  --tap)
    CID="$(tap)"
    echo "contract_id: $CID"
    echo "find it:     demo/topic.sh find $CID"
    exit 0 ;;
  --reset) RESET=1 ;;
  "") ;;
  *) sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac

echo "==> local Redpanda on $CANTON_KAFKA_BOOTSTRAP"
docker compose -f "$HERE/../localnet/docker-compose.yml" up -d redpanda
KCAT="$(command -v kcat || command -v kafkacat || true)"
[ -n "$KCAT" ] || { echo "kcat/kafkacat not found" >&2; exit 1; }
for _ in $(seq 30); do "$KCAT" -b "$CANTON_KAFKA_BOOTSTRAP" -L -m 2 >/dev/null 2>&1 && break; sleep 1; done

echo "==> adapter: bootstrap / catch up"
[ "$RESET" -eq 1 ] && rm -f "$CANTON_OFFSET_FILE"
"$PYTHON" -m trunk_canton --once

CID="$(tap)"
echo "    amulet contract: $CID"

echo "==> adapter: resume from offset and ingest the tap"
sleep 3
"$PYTHON" -m trunk_canton --once

echo "==> looking for the tapped contract on $CANTON_TX_TOPIC"
"$HERE/../topic.sh" find "$CID" && echo "PASS" || { echo "FAIL: $CID not on $CANTON_TX_TOPIC" >&2; exit 1; }
