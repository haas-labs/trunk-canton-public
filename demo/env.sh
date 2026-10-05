#!/usr/bin/env bash
# Demo config from the repo-root .env (gitignored; template in .env.example). ENV_FILE overrides the
# path; variables already set in the environment win over .env, so one run can target another node.
#
#   eval "$(demo/env.sh)"                     # export the adapter's CANTON_* (token minted if needed)
#   . "$(dirname "$0")/../env.sh"             # from a bash script: load_env, require, jwt_for, adapter_env

_DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-$_DEMO_DIR/../.env}"

load_env() {
  [ -f "$ENV_FILE" ] || { echo "missing $ENV_FILE (cp .env.example .env and fill it in)" >&2; return 1; }
  local line k v
  while IFS= read -r line || [ -n "$line" ]; do
    [[ "$line" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]] || continue
    k="${BASH_REMATCH[1]}"; v="${BASH_REMATCH[2]}"
    v="${v%\"}"; v="${v#\"}"; v="${v%\'}"; v="${v#\'}"
    [ -n "${!k+x}" ] || export "$k=$v"
  done < "$ENV_FILE"
}

require() {
  local v missing=()
  for v in "$@"; do [ -n "${!v:-}" ] || missing+=("$v"); done
  [ ${#missing[@]} -eq 0 ] || { echo "not set (add to $ENV_FILE): ${missing[*]}" >&2; return 1; }
}

# HS256 JWT for a Ledger API / wallet user, signed with CANTON_JWT_SECRET, valid 7 days.
jwt_for() {
  require CANTON_JWT_SECRET CANTON_JWT_AUDIENCE || return 1
  python3 - "$1" <<'PY'
import base64, hashlib, hmac, json, os, sys, time
b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=")
h = b64(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
p = b64(json.dumps({"sub": sys.argv[1], "aud": os.environ["CANTON_JWT_AUDIENCE"],
                    "exp": int(time.time()) + 7 * 86400}, separators=(",", ":")).encode())
s = h + b"." + p
print((s + b"." + b64(hmac.new(os.environ["CANTON_JWT_SECRET"].encode(), s, hashlib.sha256).digest())).decode())
PY
}

# The adapter's settings; the token is minted for CANTON_USER unless the node runs without auth
# (CANTON_JWT_SECRET empty) or a CANTON_LEDGER_TOKEN is supplied.
adapter_env() {
  require CANTON_JSON_API CANTON_USER CANTON_KAFKA_BOOTSTRAP CANTON_TX_TOPIC CANTON_OFFSET_FILE || return 1
  if [ -z "${CANTON_LEDGER_TOKEN:-}" ] && [ -n "${CANTON_JWT_SECRET:-}" ]; then
    CANTON_LEDGER_TOKEN="$(jwt_for "$CANTON_USER")" || return 1
    export CANTON_LEDGER_TOKEN
  fi
}

load_env || { return 1 2>/dev/null || exit 1; }

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -euo pipefail
  adapter_env
  for v in CANTON_JSON_API CANTON_USER CANTON_LEDGER_TOKEN CANTON_KAFKA_BOOTSTRAP CANTON_TX_TOPIC \
           CANTON_TOPOLOGY_ALL_PARTIES CANTON_OFFSET_FILE; do
    printf 'export %s=%q\n' "$v" "${!v:-}"
  done
fi
