#!/usr/bin/env bash
# Fire real Canton detector triggers on the shared DevNet VM, so the resulting activity is on the
# node (visible in the network's scanner, CANTON_SCAN_URL) and the running sentries emit alerts to triage.
#
# It drives a Canton remote console against the VM's participant nodes over SSH+docker and performs
# the exact topology operations the localnet scenarios use, on an ISOLATED throwaway party
# (`extractor_canary`, hosted on the appprovider participant). Topology reads/writes go through the
# admin-api, which on this LocalNet needs no auth. The canary has no contracts and no automation, so
# changing its hosting/threshold affects no real party. `--cleanup` reverts it to a single host.
#
# The canary party must already exist on the node (it is a topology-only party; allocating a fresh
# one needs a ledger-api JWT the remote console cannot supply). Point TOPOLOGY_CANARY at another
# existing party to drift that one instead.
#
#   ./devnet-trigger.sh --list                 # what can be fired and what each does
#   ./devnet-trigger.sh 369                     # one
#   ./devnet-trigger.sh 369 370 370t            # several
#   ./devnet-trigger.sh all                     # every trigger doable on this VM (369, 370, 370t)
#   ./devnet-trigger.sh --dry-run 369           # resolve handles + print the plan, mutate nothing
#   ./devnet-trigger.sh --cleanup               # restore the canary to a single host, threshold 1
#
# Triggers:
#   369   topology drift        appuser's node is added as a host of the canary, then removed
#   370   capability drift      appuser's host permission on the canary Observation -> Submission
#   370t  capability threshold  canary confirmation threshold raised to 2, then dropped to 1
#   371   stakeholder bloat     not here: use devnet-contract-trigger.sh
#   373   informee anomaly      not here: use devnet-contract-trigger.sh
#   372   reassignment limbo    NOT POSSIBLE on this VM (single synchronizer)
#
# Config (repo-root .env): TOPOLOGY_SSH_HOST, TOPOLOGY_CONTAINER, TOPOLOGY_MONITORED_{ADMIN,LEDGER}_PORT
# (appuser, the monitored node), TOPOLOGY_CPHOST_{ADMIN,LEDGER}_PORT (appprovider, hosts the canary),
# TOPOLOGY_CANARY (the isolated throwaway party to drift).
#
set -euo pipefail

DRY_RUN=0; CLEANUP=0; SELECT=()
while [ $# -gt 0 ]; do
  case "$1" in
    --list) sed -n '/^# Triggers:/,/reassignment limbo/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --cleanup) CLEANUP=1; shift ;;
    -h|--help) sed -n '2,33p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    all) SELECT=(369 370 370t); shift ;;
    369|370|370t) SELECT+=("$1"); shift ;;
    371|373) echo "!! $1 is a contract trigger: run ./devnet-contract-trigger.sh instead." >&2; shift ;;
    372) echo "!! 372 (reassignment limbo) is not possible on this VM: it has a single synchronizer." >&2; shift ;;
    *) echo "unknown trigger '$1' (expected: 369 370 370t 371 373 372 all)" >&2; exit 2 ;;
  esac
done
if [ "$CLEANUP" -eq 0 ] && [ ${#SELECT[@]} -eq 0 ]; then
  echo "nothing selected. Try: $0 --list" >&2; exit 2
fi

. "$(cd "$(dirname "$0")" && pwd)/../env.sh"
require TOPOLOGY_SSH_HOST TOPOLOGY_CONTAINER TOPOLOGY_MONITORED_ADMIN_PORT TOPOLOGY_MONITORED_LEDGER_PORT \
        TOPOLOGY_CPHOST_ADMIN_PORT TOPOLOGY_CPHOST_LEDGER_PORT TOPOLOGY_CANARY
SSH_HOST="$TOPOLOGY_SSH_HOST"
CONTAINER="$TOPOLOGY_CONTAINER"
MONITORED_ADMIN="$TOPOLOGY_MONITORED_ADMIN_PORT"; MONITORED_LEDGER="$TOPOLOGY_MONITORED_LEDGER_PORT"
CPHOST_ADMIN="$TOPOLOGY_CPHOST_ADMIN_PORT";       CPHOST_LEDGER="$TOPOLOGY_CPHOST_LEDGER_PORT"
CP_FILTER="$TOPOLOGY_CANARY"

command -v ssh >/dev/null || { echo "ssh not found" >&2; exit 1; }

# ---- Canton remote console config ----
read -r -d '' REMOTE_CONF <<EOF || true
canton {
  remote-participants {
    appuser     { admin-api { address = "127.0.0.1", port = ${MONITORED_ADMIN} }, ledger-api { address = "127.0.0.1", port = ${MONITORED_LEDGER} } }
    appprovider { admin-api { address = "127.0.0.1", port = ${CPHOST_ADMIN} },      ledger-api { address = "127.0.0.1", port = ${CPHOST_LEDGER} } }
  }
}
EOF

# ---- READ-ONLY preamble. Resolves handles, synchronizer, the canary party. Mutates nothing.
# filterParty on party_to_participant_mappings.list does not prefix-match here, so hosts/threshold
# are read by matching partyId exactly against the full mapping list.
read -r -d '' PREAMBLE <<EOF || true
val monitored = appuser        // hosts the monitored party (app_user) — the client's node
val cpHost    = appprovider    // hosts the canary
val conn      = monitored.synchronizers.list_connected().head
val syncId    = conn.synchronizerId
val syncAlias = conn.synchronizerAlias
val CP_FILTER = "${CP_FILTER}"
val cp        = cpHost.parties.list(filterParty = CP_FILTER).headOption.map(_.party)
                  .getOrElse(throw new RuntimeException("no party matching " + CP_FILTER + " on cpHost — the canary must exist first"))
def cpMapping() = cpHost.topology.party_to_participant_mappings.list(syncId).find(_.item.partyId == cp)
def cpHosts(): Set[ParticipantId] = cpMapping().map(_.item.participants.map(_.participantId).toSet).getOrElse(Set.empty)
def cpThreshold(): Int = cpMapping().map(_.item.threshold.value).getOrElse(-1)
println("[preamble] synchronizer=" + syncId.toProtoPrimitive)
println("[preamble] monitored=" + monitored.id.toProtoPrimitive)
println("[preamble] canary=" + cp.toProtoPrimitive)
println("[preamble] canary hosts=" + cpHosts().map(_.toProtoPrimitive.take(24)).mkString(", ") + " threshold=" + cpThreshold())
EOF

# ---- Trigger blocks (Scala), ported from localnet/scenarios; unique val names so several concatenate cleanly ----
block_369() { cat <<'EOF'
println("=== 369 topology drift ===")
if (!cpHosts().contains(monitored.id)) {
  println("369: adding appuser as a Confirmation host of the canary")
  Seq(cpHost, monitored).foreach(_.topology.party_to_participant_mappings.propose_delta(
    cp, adds = List((monitored.id, ParticipantPermission.Confirmation)), store = syncId))
  utils.retry_until_true { monitored.topology.party_to_participant_mappings.are_known(syncId, Set(cp -> monitored.id)) }
} else println("369: appuser already a host of the canary, skipping the add")
println("369: removing appuser again (the revoke half)")
cpHost.topology.party_to_participant_mappings.propose_delta(cp, removes = List(monitored.id), store = syncId)
utils.retry_until_true { !cpHosts().contains(monitored.id) }
println("369 done")
EOF
}
block_370() { cat <<'EOF'
println("=== 370 capability drift (permission) ===")
if (!cpHosts().contains(monitored.id)) {
  println("370: adding appuser as an Observation host of the canary")
  Seq(cpHost, monitored).foreach(_.topology.party_to_participant_mappings.propose_delta(
    cp, adds = List((monitored.id, ParticipantPermission.Observation)), store = syncId))
  utils.retry_until_true { monitored.topology.party_to_participant_mappings.are_known(syncId, Set(cp -> monitored.id)) }
  Thread.sleep(2000)
}
println("370: upgrading appuser's permission on the canary to Submission")
Seq(cpHost, monitored).foreach(_.topology.party_to_participant_mappings.propose_delta(
  cp, adds = List((monitored.id, ParticipantPermission.Submission)), store = syncId))
Thread.sleep(2000)
println("370 done (run --cleanup to remove appuser as a host again)")
EOF
}
block_370t() { cat <<'EOF'
println("=== 370 capability drift (threshold, Admin API signal) ===")
if (!cpHosts().contains(monitored.id)) {
  println("370t: adding appuser as a Submission host first")
  Seq(cpHost, monitored).foreach(_.topology.party_to_participant_mappings.propose_delta(
    cp, adds = List((monitored.id, ParticipantPermission.Submission)), store = syncId))
  utils.retry_until_true { cpHosts().contains(monitored.id) }
  Thread.sleep(2000)
}
println("370t: raising the confirmation threshold to 2")
cpHost.topology.party_to_participant_mappings.propose(cp,
  newParticipants = Seq((cpHost.id, ParticipantPermission.Submission), (monitored.id, ParticipantPermission.Submission)),
  threshold = PositiveInt.two, store = syncId)
utils.retry_until_true { cpThreshold() == 2 }
Thread.sleep(2000)
println("370t: dropping the threshold back to 1 (the drop the detector alerts on)")
cpHost.topology.party_to_participant_mappings.propose(cp,
  newParticipants = Seq((cpHost.id, ParticipantPermission.Submission), (monitored.id, ParticipantPermission.Submission)),
  threshold = PositiveInt.one, store = syncId)
utils.retry_until_true { cpThreshold() == 1 }
println("370t done (run --cleanup to remove appuser as a host again)")
EOF
}
block_cleanup() { cat <<'EOF'
println("=== cleanup: restore the canary to a single host, threshold 1 ===")
if (cpHosts().contains(monitored.id) || cpThreshold() != 1) {
  cpHost.topology.party_to_participant_mappings.propose(cp,
    newParticipants = Seq((cpHost.id, ParticipantPermission.Submission)), threshold = PositiveInt.one, store = syncId)
  utils.retry_until_true { !cpHosts().contains(monitored.id) && cpThreshold() == 1 }
  println("cleanup: canary restored to cpHost-only, threshold 1")
} else println("cleanup: canary already single-host / threshold 1, nothing to do")
println("cleanup done")
EOF
}

# ---- assemble ----
SCRIPT="$PREAMBLE"$'\n'
if [ "$CLEANUP" -eq 1 ]; then
  SCRIPT+="$(block_cleanup)"$'\n'
else
  for t in "${SELECT[@]}"; do
    case "$t" in
      369)  SCRIPT+="$(block_369)"$'\n' ;;
      370)  SCRIPT+="$(block_370)"$'\n' ;;
      370t) SCRIPT+="$(block_370t)"$'\n' ;;
    esac
  done
fi

if [ "$DRY_RUN" -eq 1 ]; then
  echo "===== full script a real run WOULD execute ====="; printf '%s\n' "$SCRIPT"
  echo "===== running the READ-ONLY preamble against the VM (mutates nothing) ====="
  RUN="$PREAMBLE"
else
  RUN="$SCRIPT"
fi

printf '%s\n' "$REMOTE_CONF" | ssh -o BatchMode=yes "$SSH_HOST" "docker exec -i $CONTAINER sh -c 'cat > /tmp/trigger-remote.conf'"
printf '%s\n' "$RUN"         | ssh -o BatchMode=yes "$SSH_HOST" "docker exec -i $CONTAINER sh -c 'cat > /tmp/trigger.canton'"
echo ">> launching Canton remote console on $SSH_HOST (may take ~30s)..."
ssh -o BatchMode=yes "$SSH_HOST" "docker exec $CONTAINER sh -c 'cd /app && bin/canton run /tmp/trigger.canton -c /tmp/trigger-remote.conf --no-tty --log-level-stdout=ERROR --log-file-appender=off 2>&1'" \
  | grep -vE 'logback|ch\.qos|_JAVA_OPTIONS|Picked up' || true
[ "$DRY_RUN" -eq 1 ] && echo ">> dry-run only: no triggers were executed." || echo ">> done. Check ${CANTON_SCAN_URL:-the network scanner} for the canary activity and your detector pipeline for the alerts."
