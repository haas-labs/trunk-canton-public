#!/usr/bin/env bash
# One-command Canton Coin transfer on a Splice LocalNet.
#
# Mints wallet tokens, creates a transfer offer from the sender wallet (app-user) to the receiver
# (app-provider), accepts it, and prints the tracking id to look up on the adapter's topic
# (demo/topic.sh find <tracking id>). The sender's automation settles the transfer a second or
# two later on its own.
#
# Config: WALLET_SENDER_* / WALLET_RECEIVER_* and CANTON_JWT_* from the repo-root .env.
# Needs: network access to the node in .env (e.g. a VPN, if it is private), curl and python3.
#
#   ./devnet-transfer.sh                 # transfer 7.25, auto tracking id
#   ./devnet-transfer.sh --amount 12.5 --description "smoke test"
#
set -euo pipefail
. "$(cd "$(dirname "$0")" && pwd)/../env.sh"
require WALLET_SENDER_API WALLET_SENDER_USER WALLET_RECEIVER_API WALLET_RECEIVER_USER CANTON_JWT_SECRET CANTON_JWT_AUDIENCE

AMOUNT="7.25"
DESCRIPTION="devnet-transfer.sh"
CURL=(curl -sS --max-time 20)

while [ $# -gt 0 ]; do
  case "$1" in
    --amount)      AMOUNT="$2"; shift 2 ;;
    --description) DESCRIPTION="$2"; shift 2 ;;
    -h|--help)     sed -n '2,14p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

command -v curl    >/dev/null || { echo "curl not found" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 not found" >&2; exit 1; }

# JSON field extractor: jget '<expr on the parsed dict `d`>'
jget() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }

echo "==> minting tokens"
U="$(jwt_for "$WALLET_SENDER_USER")"
P="$(jwt_for "$WALLET_RECEIVER_USER")"

echo "==> checking the sender's wallet is reachable ($WALLET_SENDER_API)"
BAL="$("${CURL[@]}" "$WALLET_SENDER_API/api/validator/v0/wallet/balance" -H "Authorization: Bearer $U" || true)"
if [ -z "$BAL" ]; then
  echo "no reply from the wallet API. Is WALLET_SENDER_API ($WALLET_SENDER_API) reachable from here?" >&2; exit 1
fi
UNLOCKED="$(printf '%s' "$BAL" | jget "d['effective_unlocked_qty']" 2>/dev/null || echo "")"
if [ -z "$UNLOCKED" ]; then
  echo "unexpected balance reply (bad token?): $BAL" >&2; exit 1
fi
echo "    sender unlocked balance: $UNLOCKED"

# Faucet only if the sender cannot cover the amount (testing convenience, LocalNet/DevNet only).
if python3 -c "import sys; sys.exit(0 if float('$UNLOCKED') < float('$AMOUNT') else 1)"; then
  TAP="$(python3 -c "import math; print(max(100, math.ceil(float('$AMOUNT'))))")"
  echo "==> balance below $AMOUNT, tapping the faucet for $TAP"
  "${CURL[@]}" -X POST "$WALLET_SENDER_API/api/validator/v0/wallet/tap" -H "Authorization: Bearer $U" \
    -H "Content-Type: application/json" -d "{\"amount\":\"$TAP\"}" >/dev/null
fi

echo "==> resolving the receiver's party id ($WALLET_RECEIVER_API)"
RECV="$("${CURL[@]}" "$WALLET_RECEIVER_API/api/validator/v0/validator-user" -H "Authorization: Bearer $P" \
  | jget "d['party_id']")"
echo "    receiver: $RECV"

TID="devnet-transfer-$(date +%s)"
EXP="$(python3 -c "import time; print(int((time.time()+3600)*1_000_000))")"  # microseconds
echo "==> creating the offer  (tracking id: $TID, amount: $AMOUNT)"
OFFER="$("${CURL[@]}" -X POST "$WALLET_SENDER_API/api/validator/v0/wallet/transfer-offers" \
  -H "Authorization: Bearer $U" -H "Content-Type: application/json" \
  -d "{\"receiver_party_id\":\"$RECV\",\"amount\":\"$AMOUNT\",\"description\":\"$DESCRIPTION\",\"expires_at\":$EXP,\"tracking_id\":\"$TID\"}")"
OFFER_CID="$(printf '%s' "$OFFER" | jget "d['offer_contract_id']" 2>/dev/null || echo "")"
if [ -z "$OFFER_CID" ]; then
  echo "offer not created: $OFFER" >&2; exit 1
fi
echo "    offer contract: ${OFFER_CID:0:24}..."

echo "==> finding the offer in the receiver's wallet"
CID=""
for attempt in 1 2 3 4 5; do
  CID="$("${CURL[@]}" "$WALLET_RECEIVER_API/api/validator/v0/wallet/transfer-offers" -H "Authorization: Bearer $P" \
    | jget "next((o['contract_id'] for o in d['offers'] if o['payload']['trackingId']=='$TID'), '')" 2>/dev/null || echo "")"
  [ -n "$CID" ] && break
  sleep 2
done
if [ -z "$CID" ]; then
  echo "the receiver never saw the offer for $TID (expired, or already accepted)" >&2; exit 1
fi

echo "==> accepting"
"${CURL[@]}" -X POST "$WALLET_RECEIVER_API/api/validator/v0/wallet/transfer-offers/$CID/accept" \
  -H "Authorization: Bearer $P" >/dev/null
echo "    accepted; the sender's automation settles the transfer in a second or two."

echo
echo "done. $AMOUNT moved $WALLET_SENDER_USER -> $WALLET_RECEIVER_USER."
echo "  tracking id : $TID"
echo "  find it     : demo/topic.sh find $TID"
