#!/usr/bin/env bash
# Fire the contract-based Canton detectors by creating an Iou with many stakeholders. This produces
# REAL ledger activity in the monitored party's view, which the ingestion adapter publishes to the
# tenant's topic and the deployed sentries turn into alerts:
#   - stakeholder anomaly (371): a contract with more stakeholders than the policy limit
#   - informee anomaly (373): the same create reaches parties outside the template's allowed list
#
# Unlike the topology triggers (devnet-trigger.sh), this one reaches the sentries at the default
# CANTON_TOPOLOGY_ALL_PARTIES=0, because it is a transaction in the monitored party's own view.
#
# It talks to the participant's JSON Ledger API v2 (CANTON_JSON_API, the same participant the adapter
# reads; no SSH needed) and authenticates as SUBMIT_USER, a user with
# ParticipantAdmin + CanActAs on SUBMIT_PARTY, HS256-signed with CANTON_JWT_SECRET.
# Observer parties come from a FIXED, reusable pool (extractor_obs_NNN): allocated once, reused on
# every run, so repeated triggers do not accumulate throwaway parties. Only the Iou contract is new
# each run (that is the create event the detector fires on).
#
# Config comes from the repo-root .env; override per run for another participant:
#
#   ./devnet-contract-trigger.sh                    # Iou with 15 observers (> limit 10)
#   ./devnet-contract-trigger.sh --observers 60
#   CANTON_JSON_API=http://<validator-host> CANTON_JWT_AUDIENCE=<its audience> SUBMIT_PARTY=<its party> \
#     ./devnet-contract-trigger.sh                  # another participant
#
# Needs: network access to the node in .env (e.g. a VPN, if it is private), and python3.
set -euo pipefail
. "$(cd "$(dirname "$0")" && pwd)/../env.sh"
require CANTON_JSON_API CANTON_JWT_SECRET CANTON_JWT_AUDIENCE SUBMIT_USER SUBMIT_PARTY

OBSERVERS=15
while [ $# -gt 0 ]; do
  case "$1" in
    --observers) OBSERVERS="$2"; shift 2 ;;
    -h|--help) sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
command -v python3 >/dev/null || { echo "python3 not found" >&2; exit 1; }

VM="$CANTON_JSON_API" OBSERVERS="$OBSERVERS" PAYER="$SUBMIT_PARTY" USER_NAME="$SUBMIT_USER" \
  SUBMIT_TOKEN="$(jwt_for "$SUBMIT_USER")" python3 - <<'PY'
import io, json, os, sys, tarfile, time, urllib.request

VM = os.environ["VM"]
N = int(os.environ["OBSERVERS"])
PAYER = os.environ["PAYER"]
USER = os.environ["USER_NAME"]
TOK = os.environ["SUBMIT_TOKEN"]
IOU_PKG = "764252f6c2236376a83c134318e7856e046ff469481b28dcdc84372fa636d91a"  # CantonExamples Iou
POOL = "extractor_obs_"                             # fixed reusable observer-party pool
DAR_URL = "https://github.com/digital-asset/canton/releases/download/v3.5.16/canton-open-source-3.5.16.tar.gz"
DAR_MEMBER = "canton-open-source-3.5.16/dars/CantonExamples.dar"

def call(method, path, body=None, octet=False):
    headers = {"Authorization": f"Bearer {TOK}"}
    data = None
    if body is not None:
        data = body if octet else json.dumps(body).encode()
        headers["Content-Type"] = "application/octet-stream" if octet else "application/json"
    req = urllib.request.Request(VM + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()

# 1. the Iou package must be vetted (one-time DAR upload for a fresh participant)
st, resp = call("GET", "/v2/packages")
if st != 200:
    sys.exit(f"cannot reach the JSON Ledger API at {VM} (is CANTON_JSON_API reachable from here?): {st} {resp[:200]}")
if IOU_PKG not in resp:
    print("Iou package not vetted; downloading CantonExamples.dar from the Canton release (~284MB, one-time)...")
    with urllib.request.urlopen(DAR_URL, timeout=600) as r:
        dar = tarfile.open(fileobj=io.BytesIO(r.read()), mode="r:gz").extractfile(DAR_MEMBER).read()
    st, resp = call("POST", "/v2/packages", dar, octet=True)
    if st != 200:
        sys.exit(f"DAR upload failed: {st} {resp[:200]}")
    print("uploaded CantonExamples.dar")
else:
    print("Iou package already vetted")

# 2. fixed observer pool: reuse existing extractor_obs_NNN parties, allocate only the missing ones
st, resp = call("GET", "/v2/parties")
existing = {}
if st == 200:
    for pd in json.loads(resp).get("partyDetails", []):
        party = pd.get("party", "")
        hint = party.split("::", 1)[0]
        if hint.startswith(POOL):
            existing[hint] = party
obs, reused, allocated = [], 0, 0
for i in range(N):
    hint = f"{POOL}{i:03d}"
    if hint in existing:
        obs.append(existing[hint]); reused += 1
    else:
        st, resp = call("POST", "/v2/parties", {"partyIdHint": hint, "identityProviderId": ""})
        if st != 200:
            sys.exit(f"party allocation failed for {hint}: {st} {resp[:200]}")
        obs.append(json.loads(resp)["partyDetails"]["party"]); allocated += 1
print(f"observer pool: {len(obs)} parties ({reused} reused, {allocated} newly allocated)")

# 3. create a fresh Iou as the payer (signatory); owner + viewers = the observer pool
run = int(time.time())
cmd = {
    "commands": [{"CreateCommand": {
        "templateId": f"{IOU_PKG}:Iou:Iou",
        "createArguments": {
            "payer": PAYER,
            "owner": obs[0],
            "amount": {"value": "100.0", "currency": "USD"},
            "viewers": obs[1:],
        }}}],
    "commandId": f"bloat-{run}",
    "actAs": [PAYER],
    "userId": USER,
}
st, resp = call("POST", "/v2/commands/submit-and-wait", cmd)
if st != 200:
    sys.exit(f"create failed: {st} {resp[:300]}")
upd = json.loads(resp).get("updateId", "?")
print()
print(f"done. Iou created with {len(obs)+1} stakeholders (1 signatory + {len(obs)} observers).")
print(f"  update id : {upd}")
print(f"  expect    : canton_stakeholder_anomaly (371) and canton_informee_anomaly (373)")
print(f"  find it   : demo/topic.sh find {upd}")
PY
