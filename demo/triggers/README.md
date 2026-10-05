# Splice LocalNet trigger scripts

Scripts that drive a Splice LocalNet VM to produce real ledger activity: Create / Exercise /
Archive transactions and topology changes. Use them to check that the adapter ingests them, and to
exercise deployed Canton detectors. The VM can be:

- a Splice LocalNet, the layout `.env.example` targets;
- a DevNet node that also runs its own ingestion adapter and detectors, which turn the activity
  into alerts (see [Verifying on a deployed detector pipeline](#verifying-on-a-deployed-detector-pipeline)).

Which VM a script hits is set by the repo-root `.env` (`CANTON_JSON_API`, `WALLET_*`), not by the
script.

| Script | What it does | Ledger events | Fires a detector? | Needs SSH |
|---|---|---|---|---|
| `localnet-smoke.sh` | faucet tap as app-user; with no flag, a full one-shot smoke test | `created Splice.Amulet:Amulet` | ❌ | no |
| `devnet-transfer.sh` | Canton Coin transfer app-user → app-provider | TransferOffer create / accept, Amulet archive + create | ❌ benign | no |
| `devnet-contract-trigger.sh` | creates an `Iou` with many stakeholders in `app_user`'s view | `created Iou:Iou` | ✅ 371 stakeholder anomaly + 373 informee anomaly | no |
| `devnet-trigger.sh` | topology changes on a throwaway party (369 / 370 / 370t) | `authorization_added` / `_changed` / `_revoked` | ⚠️ only with `CANTON_TOPOLOGY_ALL_PARTIES=1` | yes |

## Prerequisites

- **Network access to the node in `.env`** (e.g. a VPN, if it is private).
- `python3`, `curl`, `docker compose` (for the local Redpanda), `kcat` / `kafkacat`, and the package
  installed (`pip install -e ".[dev]"`, conda env `trunk-canton`).
- `devnet-trigger.sh` only: SSH access to the VM (see its section).

## Configuration: `.env`

Every script reads the repo-root `.env` (gitignored; copy `.env.example`). Variables set in your
shell win over `.env`, so `CANTON_JSON_API=… ./devnet-contract-trigger.sh` targets another node for
one run. Tokens are HS256 JWTs the scripts mint with `CANTON_JWT_SECRET`, so there are no tokens to
paste.

| Variable | Used by | LocalNet value |
|---|---|---|
| `CANTON_JSON_API` | adapter, `devnet-contract-trigger.sh` | `http://<vm-host>:2975` (app-user participant JSON Ledger API) |
| `CANTON_USER` | adapter | `extractor` (`CanReadAs` app_user + `CanReadAsAnyParty`) |
| `CANTON_KAFKA_BOOTSTRAP`, `CANTON_TX_TOPIC` | adapter, `demo/topic.sh` | `localhost:19092`, `canton.localnet.tx` |
| `CANTON_TOPOLOGY_ALL_PARTIES` | adapter | `1` (needed to see the canary's topology) |
| `CANTON_OFFSET_FILE` | adapter | any path, one per participant |
| `CANTON_JWT_SECRET`, `CANTON_JWT_AUDIENCE` | every script | the LocalNet dev secret, `https://canton.network.global` |
| `WALLET_SENDER_API`, `WALLET_SENDER_USER` | `localnet-smoke.sh`, `devnet-transfer.sh` | `http://<vm-host>:2903`, `app-user` |
| `WALLET_RECEIVER_API`, `WALLET_RECEIVER_USER` | `devnet-transfer.sh` | `http://<vm-host>:3903`, `app-provider` |
| `TAP_AMOUNT` | `localnet-smoke.sh` | `13.37` |
| `SUBMIT_USER`, `SUBMIT_PARTY` | `devnet-contract-trigger.sh` | `ledger-api-user` (ParticipantAdmin + CanActAs), the app_user party id |
| `CANTON_SCAN_URL`, `DETECTOR_*` | `devnet-trigger.sh`, [detector pipeline checks](#verifying-on-a-deployed-detector-pipeline) | optional: the network's scanner URL and the detector pipeline's broker, alert topic, k8s namespace and release |
| `TOPOLOGY_*` | `devnet-trigger.sh` | SSH alias, container name, admin/ledger ports `2902/2901` (appuser), `3902/3901` (appprovider), canary party |

To find the app_user party id: `curl http://<vm-host>:2903/api/validator/v0/validator-user`
(`party_id`).

## The three terminals

Open three terminals and `cd` to the repo root in each. Terminals 1 and 2 stay up for every
trigger below.

**Terminal 1: local broker and the adapter** (start this first; it creates the topic):
```bash
docker compose -f demo/localnet/docker-compose.yml up -d redpanda
eval "$(demo/env.sh)"
python -m trunk_canton
```
Expected on first start:
```
topic canton.localnet.tx created
topology baseline published: N updates; snapshot: M entries at offset X
published 12 updates, offset now X
```
It keeps printing `published … offset now …` as the node's background activity (mining rounds,
validator liveness) arrives. Leave it running.

**Terminal 2: live view of the topic** (after terminal 1 printed `topic … created` / `exists`):
```bash
demo/topic.sh
```
One line per record: `kind offset update_id  events…`. Background `transaction` lines appear
without you doing anything. To pick out your own events, use `demo/topic.sh find <id>`.

**Terminal 3: the triggers.** Each section below is run here.

---

## 1. Faucet tap: `localnet-smoke.sh --tap`

The smallest possible transaction: mint Amulet into app-user's wallet.

**Terminal 3:**
```bash
demo/triggers/localnet-smoke.sh --tap
```
Expected:
```
==> faucet tap 13.37 as app-user (http://<vm-host>:2903)
contract_id: 00c1…
find it:     demo/topic.sh find 00c1…
```

**Terminal 2** (within a second or two):
```
transaction  offset=61318 update_id=12200aeab459c8d59a1ceb54  exercised Splice.Wallet.Install:WalletAppInstall/WalletAppInstall_ExecuteBatch, exercised Splice.AmuletRules:AmuletRules/AmuletRules_DevNet_Tap, …, created Splice.Amulet:Amulet, …
```

**Terminal 3: confirm it was ingested:**
```bash
demo/topic.sh find 00c1…
```
```
FOUND: kind=transaction offset=61318 update_id=12200aeab459…
    exercised              Splice.Wallet.Install:WalletAppInstall/WalletAppInstall_ExecuteBatch
    exercised              Splice.AmuletRules:AmuletRules/AmuletRules_DevNet_Tap
    exercised              Splice.AmuletRules:AmuletRules/AmuletRules_Mint
    created                Splice.Amulet:Amulet
    exercised              Splice.AmuletRules:AmuletRules/EventLog_HoldingsChange
```

**One-terminal alternative:** `demo/triggers/localnet-smoke.sh` (add `--reset` to re-bootstrap).
It starts Redpanda, runs the adapter `--once`, taps, runs the adapter `--once` again (resuming
from the offset), and ends with `PASS` or `FAIL`. Don't run it while terminal 1's adapter is up:
both would write the same offset file.

## 2. Canton Coin transfer: `devnet-transfer.sh`

A transfer offer from app-user to app-provider, accepted by the receiver and settled by the
sender's automation. Normal traffic: it trips no detector. If app-user's balance is below the
amount, the script taps the faucet first.

**Terminal 3:**
```bash
demo/triggers/devnet-transfer.sh --amount 5 --description "smoke test"
```
Expected:
```
==> minting tokens
==> checking the sender's wallet is reachable (http://<vm-host>:2903)
    sender unlocked balance: 945332.16
==> resolving the receiver's party id (http://<vm-host>:3903)
==> creating the offer  (tracking id: devnet-transfer-1790…, amount: 5)
==> finding the offer in the receiver's wallet
==> accepting
done. 5 moved app-user -> app-provider.
  tracking id : devnet-transfer-1790…
  find it     : demo/topic.sh find devnet-transfer-1790…
```

**Terminal 2:** three transactions within a few seconds:
```
transaction  offset=61345 …  exercised Splice.Wallet.Install:WalletAppInstall/WalletAppInstall_CreateTransferOffer, created Splice.Wallet.TransferOffer:TransferOffer
transaction  offset=61348 …  exercised Splice.Wallet.TransferOffer:TransferOffer/TransferOffer_Accept, created Splice.Wallet.TransferOffer:AcceptedTransferOffer
transaction  offset=61351 …  exercised …/AcceptedTransferOffer_Complete, …, exercised Splice.AmuletRules:AmuletRules/AmuletRules_Transfer, exercised Splice.Amulet:Amulet/Archive, created Splice.Amulet:Amulet, …
```
Together these cover all three event types: the offer is created, then exercised
(`TransferOffer_Accept`), and the sender's Amulet is archived and re-created as change plus the
receiver's share.

**Terminal 3: confirm.** All three transactions carry the tracking id in their payloads:
```bash
demo/topic.sh find devnet-transfer-1790…
```
```
FOUND: kind=transaction offset=61345 …
    exercised              Splice.Wallet.Install:WalletAppInstall/WalletAppInstall_CreateTransferOffer
    created                Splice.Wallet.TransferOffer:TransferOffer
FOUND: kind=transaction offset=61348 …
    exercised              Splice.Wallet.TransferOffer:TransferOffer/TransferOffer_Accept
    created                Splice.Wallet.TransferOffer:AcceptedTransferOffer
FOUND: kind=transaction offset=61351 …
    …
```

## 3. Iou with many stakeholders: `devnet-contract-trigger.sh`

Creates an `Iou` signed by `SUBMIT_PARTY` (app_user) with 15 observers from a fixed, reusable
party pool (`extractor_obs_000…014`). This is the trigger that reliably fires detectors 371 and
373 on the shared pipeline. Only the Iou is new each run.

On a fresh participant the first run also allocates the pool parties. If the `CantonExamples` DAR
is not there yet, it also downloads the Canton 3.5.16 release (~284 MB) and uploads the DAR. After
that, runs are instant.

**Terminal 3:**
```bash
demo/triggers/devnet-contract-trigger.sh                  # 1 payer + 15 observers
demo/triggers/devnet-contract-trigger.sh --observers 10   # fewer; > 15 allocates new pool parties
```
Expected:
```
Iou package already vetted
observer pool: 15 parties (15 reused, 0 newly allocated)

done. Iou created with 16 stakeholders (1 signatory + 15 observers).
  update id : 1220…
  expect    : canton_stakeholder_anomaly (371) and canton_informee_anomaly (373)
  find it   : demo/topic.sh find 1220…
```

**Terminal 2:**
```
transaction  offset=61375 update_id=12204c67827cf9cf1caf676f  created Iou:Iou
```

**Terminal 3: confirm:**
```bash
demo/topic.sh find 1220…
```
```
FOUND: kind=transaction offset=61375 update_id=12204c67827c…
    created                Iou:Iou
```

## 4. Topology changes: `devnet-trigger.sh`

Drives a Canton remote console on the VM over `ssh` + `docker exec` and changes the hosting of an
**isolated throwaway party** (`TOPOLOGY_CANARY`, default `extractor_canary`, hosted on appprovider).
The canary has no contracts and no automation, so no real party is affected. The events only
reach the topic because the adapter runs with `CANTON_TOPOLOGY_ALL_PARTIES=1` (the canary is not
one of `extractor`'s own parties).

**One-time prerequisites:**
1. A `Host <alias>` block in `~/.ssh/config` for the VM, with your public key authorised on it
   (`ubuntu@<vm-host>`). Arrange that with whoever operates the VM, and never share a private key.
   Put the alias in `TOPOLOGY_SSH_HOST`.
2. The Canton container's name on the VM (`ssh <alias> docker ps`) in `TOPOLOGY_CONTAINER`.
3. The canary party must already exist on the appprovider participant. Allocating one needs a
   Ledger API JWT, which the remote console cannot supply.

**Terminal 3: dry run first** (reads the current state, changes nothing):
```bash
demo/triggers/devnet-trigger.sh --list            # what each trigger does
demo/triggers/devnet-trigger.sh --dry-run 369
```
Expected: the full script, then `[preamble] synchronizer=…`, `monitored=…`, `canary=…`,
`canary hosts=… threshold=1`.

**Terminal 3: fire, then clean up:**
```bash
demo/triggers/devnet-trigger.sh 369     # add appuser as a host of the canary, then remove it
demo/triggers/devnet-trigger.sh 370     # Observation -> Submission (leaves appuser as a host)
demo/triggers/devnet-trigger.sh --cleanup
```
Each run takes ~30 s (the console starts a JVM).

**Terminal 2:**
```
topology     offset=… update_id=…  authorization_added extractor_canary::1220…
topology     offset=… update_id=…  authorization_revoked extractor_canary::1220…
```
`370` shows `authorization_added`, then `authorization_changed`. `370t` changes only the
confirmation threshold, which produces no Ledger API event (it is visible on the Admin API only).

Not possible here: 371 / 373 (contract triggers: use `devnet-contract-trigger.sh`), and 372
reassignment limbo, which needs a second synchronizer the VM does not have. Use
`demo/localnet` for 372.

---

## Check restart from the persisted offset

1. **Terminal 1:** Ctrl-C.
2. **Terminal 3:** any trigger above, e.g. `demo/triggers/localnet-smoke.sh --tap`.
3. **Terminal 1:** `python -m trunk_canton`. Expected: `resuming after offset X` and
   no new snapshot.
4. **Terminal 3:** `demo/topic.sh find <id from step 2>` shows `FOUND`.

## Cleanup

```bash
docker compose -f demo/localnet/docker-compose.yml down   # local Redpanda (topic data is gone)
rm -f "$CANTON_OFFSET_FILE"                                # else the next start resumes from it
demo/triggers/devnet-trigger.sh --cleanup                  # if you ran 370 / 370t
```

## Verifying on a deployed detector pipeline

With `.env` pointing at a node that runs its own adapter and detectors, the triggers also reach that
pipeline. It is separate from your local broker. Its coordinates are the optional `DETECTOR_*`
variables in `.env`; load them into your shell first:

```bash
set -a; . ./.env; set +a
```

- **Alert UI:** your detector pipeline's UI, where the alerts land for triage.
- **Detector logs:** `kubectl -n "$DETECTOR_NAMESPACE" logs -l "app.kubernetes.io/instance=$DETECTOR_RELEASE"`;
  look for `stakeholder anomaly` / `informee anomaly`.
- **Kafka:** the emitted alerts, searchable by the update id or tracking id a trigger prints:
  `kcat -b "$DETECTOR_KAFKA_BOOTSTRAP" -t "$DETECTOR_ALERT_TOPIC" -C -o beginning -e -q | grep <id>`

## Notes

- These mutate a **shared** node. `devnet-contract-trigger.sh` reuses a fixed observer pool but
  leaves one new `Iou` per run. `devnet-trigger.sh` is reversible with `--cleanup`. Transfers and
  taps move test Amulet only.
- Contract creates reach the shared sentries at `CANTON_TOPOLOGY_ALL_PARTIES=0` because they are
  transactions in `app_user`'s own view. Topology (369/370) needs `=1`.
- Troubleshooting (`Unknown topic`, `401`, `no CanReadAs rights`, `Connection refused`) is in
  `../README.md`.
