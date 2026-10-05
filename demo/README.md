# Demo

End-to-end demo of the Canton ingestion adapter against a real Canton node, walking through the
Milestone-1 acceptance criteria. There are two targets:

| Target | What it is | Auth | Events come from |
|---|---|---|---|
| **A. `localnet/`** | a self-contained Canton 3.5.16 network in Docker on your machine (two participants, two synchronizers, the example `Iou` template, a read-only `extractor` user) | none | the scripted `.canton` scenarios in `localnet/scenarios` (see `localnet/README.md`) |
| **B. Splice LocalNet VM** | a shared Splice LocalNet you can reach, e.g. `http://<vm-host>` set in `.env` | HS256 JWT | the scripts in `triggers/` (see `triggers/README.md`) |

Both publish to a **local** Redpanda broker on `localhost:19092`, never to a shared Kafka.

## Setup (once)

1. Install the package (Python 3.10+, pip 21.3+; with an older pip run
   `python -m pip install --upgrade pip` first). The repo's `.condaenv` names the conda env `trunk-canton`:
   ```bash
   pip install -e ".[dev]"
   ```
2. Tools: `docker compose`, `kcat` (or `kafkacat`), `python3`, `curl`.
3. Config lives in the repo-root **`.env`**. It is gitignored because it holds the JWT secret.
   Create it from the template:
   ```bash
   cp .env.example .env
   ```
   Then set the values for your target:

   | Variable | A. `localnet/` | B. Splice LocalNet VM |
   |---|---|---|
   | `CANTON_JSON_API` | `http://localhost:7575` | `http://<vm-host>:2975` (the app-user participant) |
   | `CANTON_USER` | `extractor` | `extractor` |
   | `CANTON_JWT_SECRET` | *empty* (no auth) | the LocalNet dev secret |
   | `CANTON_JWT_AUDIENCE` | ignored | `https://canton.network.global` |
   | `CANTON_OFFSET_FILE` | e.g. `/tmp/trunk-canton/demo/offset.txt` | e.g. `/tmp/trunk-canton/localnet/offset.txt` |
   | `WALLET_*`, `SUBMIT_*`, `TOPOLOGY_*` | unused | see `triggers/README.md` |

   Keep one offset file per participant. When you switch targets, change `CANTON_OFFSET_FILE` or
   run the adapter once with `--reset`. Variables already set in your shell override `.env`, so a
   single command can target another node, e.g. `CANTON_JSON_API=… ./demo/triggers/…`.

Helpers used below (run from the repo root):

| Command | What it does |
|---|---|
| `eval "$(demo/env.sh)"` | exports the adapter's `CANTON_*` from `.env` and mints `CANTON_LEDGER_TOKEN` when a secret is set |
| `demo/topic.sh` | follows new records on the topic, one line each |
| `demo/topic.sh all` | every record from the beginning |
| `demo/topic.sh find <id>` | the record(s) containing a contract id, update id or tracking id |

## The three terminals

Every walkthrough uses the same layout. Open three terminals and `cd` to the repo root in each.

| Terminal | Runs | Start |
|---|---|---|
| **1** | the adapter, following forever | first: it creates the topic |
| **2** | `demo/topic.sh`, the live view of the topic | after terminal 1 printed `topic … created` / `exists` |
| **3** | triggers: scenarios or scripts that make ledger events happen | any time after that |

Start terminal 2 only after terminal 1 is up. Before that the topic does not exist, and
`kcat` fails with `Unknown topic or partition`.

## Walkthrough A: the self-contained Canton network

`.env`: `CANTON_JSON_API=http://localhost:7575`, `CANTON_JWT_SECRET=` (empty),
`CANTON_TOPOLOGY_ALL_PARTIES=1`.

**Terminal 3: bring up Canton and Redpanda** (~1 min until `demo/localnet/out/ids.json` appears):
```bash
docker compose -f demo/localnet/docker-compose.yml build
docker compose -f demo/localnet/docker-compose.yml up -d
ls demo/localnet/out/ids.json
```

**Terminal 1: start the adapter.** It bootstraps the visible active state, then follows:
```bash
eval "$(demo/env.sh)"
python -m trunk_canton
```
Expected:
```
topic canton.localnet.tx created
topology baseline published: N updates; snapshot: M entries at offset X
```

**Terminal 2: watch the topic**:
```bash
demo/topic.sh
```

**Terminal 3: fire scenarios one at a time** and watch terminal 2:
```bash
run() { docker compose -f demo/localnet/docker-compose.yml exec canton bin/canton run "$@"; }
run /scripts/scenarios/373_informee_anomaly.canton -c /conf/remote.conf   # Create + Exercise
run /scripts/scenarios/367_archive.canton -c /conf/remote.conf            # Exercise(Archive)
run /scripts/scenarios/369_topology_drift.canton -c /conf/remote.conf     # authorization_added/revoked
run /scripts/scenarios/000_reset_hosting.canton -c /conf/remote.conf      # before repeating 369/370
```

| Scenario | Terminal 2 shows |
|---|---|
| `373_informee_anomaly` | `transaction … created Iou:Iou`, then `transaction … exercised Iou:Iou/Share, created Iou:Iou`, then `transaction … exercised Iou:Iou/Transfer, created Iou:Iou` |
| `367_archive` | `transaction … created Iou:Iou`, then `transaction … exercised Iou:Iou/Archive` |
| `369_topology_drift` | `topology … authorization_added counterparty::…`, then `topology … authorization_revoked counterparty::…` |
| `371_stakeholder_bloat` | 60 × `topology … authorization_added obsNNN::…`, then `transaction … created Iou:Iou` with 61 stakeholders |

The full scenario table is in `localnet/README.md`. Terminal 1 prints
`published N updates, offset now X` for each page it ingests.

## Walkthrough B: the Splice LocalNet VM

`.env` as in `.env.example`, pointing at the VM, and network access to it (e.g. a VPN, if it is private).

**Terminal 1: local broker and adapter**:
```bash
docker compose -f demo/localnet/docker-compose.yml up -d redpanda    # the broker only, no Canton
eval "$(demo/env.sh)"
python -m trunk_canton
```

**Terminal 2**:
```bash
demo/topic.sh
```

**Terminal 3: one of the triggers**:
```bash
demo/triggers/localnet-smoke.sh --tap       # faucet tap -> created Splice.Amulet:Amulet
demo/triggers/devnet-transfer.sh            # Canton Coin transfer app-user -> app-provider
demo/triggers/devnet-contract-trigger.sh    # Iou with 16 stakeholders
demo/topic.sh find <id the trigger printed> # confirm it was ingested
```
A Splice LocalNet has background activity (mining rounds, validator liveness), so terminal 2 also
shows `transaction` lines you did not trigger. Use `find` to pick out yours. Step-by-step
instructions and expected output for every trigger are in `triggers/README.md`.

## Verify restart from the persisted offset

1. **Terminal 1:** stop the adapter with Ctrl-C.
2. **Terminal 3:** fire another scenario or trigger while it is down.
3. **Terminal 1:** start it again:
   ```bash
   python -m trunk_canton
   ```
   Expected: `resuming after offset X` and no new snapshot. The events from step 2 then show up in
   terminal 2. That is the crash-safe recovery guarantee: the offset is persisted only after the
   broker has acknowledged each page.

To start from scratch instead: `python -m trunk_canton --reset`.

## Acceptance criteria mapping

| Criterion | Shown by |
|---|---|
| Fresh deployment bootstraps visible active state | terminal 1 on first start (`snapshot` record) |
| Continues from persisted offset after restart | "Verify restart" above (`resuming after offset X`) |
| Ingests Create / Exercise / Archive events | scenarios `373` (create + exercise) and `367` (archive = consuming exercise in the LEDGER_EFFECTS shape); on the VM, `devnet-contract-trigger.sh` and `devnet-transfer.sh` |
| Ingests topology changes | scenario `369`; on the VM, `devnet-trigger.sh 369` |

## Cleanup

```bash
docker compose -f demo/localnet/docker-compose.yml down     # Canton (A) and Redpanda; state is in memory
rm -f "$CANTON_OFFSET_FILE"                                  # else the next start resumes from it
```

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Unknown topic or partition` in terminal 2 | the adapter has not created the topic yet: start terminal 1 first and wait for `topic … created` |
| `transient error on GET http://localhost:7575/…` on target B | `eval "$(demo/env.sh)"` was not run in that terminal, so the adapter fell back to its defaults |
| `401` / `security-sensitive error` | wrong or missing token: check `CANTON_JWT_SECRET` / `CANTON_JWT_AUDIENCE`. An exported `CANTON_LEDGER_TOKEN` from an earlier `eval` wins over `.env`: `unset CANTON_LEDGER_TOKEN` |
| `user 'X' has no CanReadAs rights` | `CANTON_USER` must be a read user (`extractor`). `ledger-api-user` can act but has no `CanReadAs` |
| `missing …/.env` / `not set (add to …/.env): …` | create `.env` from `.env.example` and fill the listed variables |
| trigger: `Connection refused` | the VM is not reachable from here (e.g. VPN down), or `.env` points at another VM |
| `resuming after offset X` when you expected a snapshot | an offset file from an earlier run: `--reset` |
