# trunk-canton

Open-source ingestion adapter for [Canton](https://www.canton.network/). It reads a participant
node through its **JSON Ledger API** (Update and State services), normalizes every update into a
single `CantonUpdate` envelope, and publishes it to a Kafka topic. It is crash-safe: a persisted
offset lets a fresh deployment bootstrap the visible active state and resume exactly where it left
off after a restart.

## How it works

```
Canton participant ──JSON Ledger API──▶ trunk_canton ──CantonUpdate JSON──▶ Kafka topic
   (Update/State services)              (this adapter)                       (canton.*.tx)
```

Canton has no blocks and no global chain: a participant only sees updates in which one of its
parties is a stakeholder or informee. The adapter follows the order a correct consumer needs:

1. **Parties** come from the Ledger API user's rights (`CanReadAs`), not from the token.
2. **First start** (no persisted offset): replay every topology event since offset 0 (the hosting
   baseline), then `GetLedgerEnd` + `GetActiveContracts` → one `kind=snapshot` record of the active
   contract set.
3. **Stream** pages of updates after the offset, normalize each to `CantonUpdate`, publish.
4. The offset is persisted **only after** the broker acknowledges the page (idempotent producer,
   key = `update_id`), so a crash between produce and persist can only cause a republish, never a gap.

Each update is one of: `transaction`, `reassignment`, `topology`, `checkpoint`, or the bootstrap
`snapshot`; its `events[]` carry typed entries (`created`, `exercised`, `archived`, `unassigned`,
`assigned`, `authorization_*`). The `CantonUpdate` model comes from the Sentinel SDK
(`sentinel.models.chains.canton.update`).

## Install / build

Prerequisites:

- **Python 3.10+** and **pip 21.3+**: to run the adapter or the tests (details below).
- **git**, with access to the Sentinel SDK repo: `pip install` fetches it as a git dependency.
- **Docker** with the Compose v2 plugin (`docker compose`): to build the image, run the compose
  stack and the `demo/localnet` Canton node.
- **kcat** (or `kafkacat`): to read the topics; the demo scripts use it.
- **Helm 3**: only to deploy the chart in `deploy/helm`.

The adapter requires Python 3.10+ and pip 21.3+ (editable installs from `pyproject.toml`, PEP 660). An older
pip fails before the Python version check is even reached, e.g. on the stock macOS Python 3.9 /
pip 21.2: use a Python 3.10+ environment (the conda line below), and run
`python -m pip install --upgrade pip` if its pip is older than 21.3.
```bash
git clone https://github.com/haas-labs/trunk-canton-public.git && cd trunk-canton
conda create -n trunk-canton python=3.10 && conda activate trunk-canton   # optional; the name .condaenv expects
pip install .            # or: pip install -e ".[dev]" for tests
```

Build the image:

```bash
docker build -f deploy/Dockerfile -t trunk-canton:0.1.0 .
```

## Run

Configuration is entirely by environment variable:

| Variable | Default | Meaning |
|---|---|---|
| `CANTON_JSON_API` | `http://localhost:7575` | Participant JSON Ledger API base URL |
| `CANTON_USER` | `extractor` | Ledger API user the adapter reads as |
| `CANTON_LEDGER_TOKEN` | `` (empty) | JWT; empty for a node without auth |
| `CANTON_KAFKA_BOOTSTRAP` | `localhost:19092` | Kafka bootstrap servers |
| `CANTON_TX_TOPIC` | `canton.localnet.tx` | Destination topic (`CANTON_UPDATES_TOPIC` read as a fallback) |
| `CANTON_DLQ_TOPIC` | `<topic>.dlq` | Dead-letter topic for updates that fail to parse |
| `CANTON_MAX_CONSECUTIVE_DLQ` | `50` | Stop after this many dead-letters in a row (circuit breaker) |
| `CANTON_OFFSET_FILE` | `/tmp/canton/offset.txt` | Persisted offset; put on durable storage |
| `CANTON_TOPOLOGY_ALL_PARTIES` | unset (`0`) | `1` only when the user has `CanReadAsAnyParty` |
| `CANTON_KEEP_PAYLOAD` | `1` | `0` strips contract payloads before they leave the boundary |
| `CANTON_HTTP_MAX_RETRIES` | `5` | Retries on transient Ledger API errors (network, 5xx, 429) |
| `CANTON_HTTP_BACKOFF_BASE` | `0.5` | Base seconds for exponential backoff between retries |
| `CANTON_HTTP_BACKOFF_MAX` | `30` | Cap in seconds for the backoff delay |

```bash
python -m trunk_canton            # follow forever
python -m trunk_canton --once     # drain and exit
python -m trunk_canton --reset    # forget offset, republish baseline + snapshot
python -m trunk_canton --from 0 --once   # replay from an offset (testing)
```

The `trunk-canton` console script is installed as an equivalent entry point.

An update that fails to parse (unknown kind, newer schema, malformed payload) is written to the
dead-letter topic (`CANTON_DLQ_TOPIC`, default `<topic>.dlq`) with its raw body and the error, and
the stream advances past it. One bad update is isolated and auditable instead of wedging ingestion.
As a safety net, if `CANTON_MAX_CONSECUTIVE_DLQ` updates are dead-lettered in a row with no
successful parse, the adapter stops, so a parser bug or schema change cannot silently blind
downstream by dead-lettering everything.

### Docker Compose (adapter + broker)

Brings up a local Redpanda broker and the adapter. Point `CANTON_JSON_API` at a reachable
participant. To use the demo node, start only the `canton` service of `demo/localnet`, because
this stack brings its own broker:

```bash
docker compose -f demo/localnet/docker-compose.yml up -d canton    # optional: the demo participant
CANTON_JSON_API=http://host.docker.internal:7575 \
  docker compose -f deploy/docker-compose.yml up --build
kcat -b localhost:29092 -t canton.localnet.tx -C                   # read the normalized records
```

This broker is published on `localhost:29092`, so it does not clash with the `demo/localnet`
broker on `19092`. Inside the stack the adapter reaches it at `redpanda:9092`.

Both services have `restart: unless-stopped`. The adapter exits when Ledger API retries run
out or the dead-letter circuit breaker trips, and the restart resumes from the persisted
offset. The offset lives on a named volume, so `docker compose restart adapter` also resumes
instead of re-bootstrapping. The image sets `PYTHONUNBUFFERED=1` and the adapter logs through
`logging`, so `docker compose logs -f adapter` shows each line as it happens.

## Deploy (Helm)

A minimal reference chart is in `deploy/helm/trunk-canton`. It renders a single-replica Deployment
(strategy `Recreate` — the adapter is a single writer to the offset file), a ConfigMap for the
`CANTON_*` settings, and a PVC that backs the offset file so restarts resume from it.

```bash
helm install trunk-canton deploy/helm/trunk-canton \
  --set image.repository=<registry>/trunk-canton --set image.tag=0.1.0 \
  --set canton.jsonApi=http://canton-participant:7575 \
  --set canton.kafkaBootstrap=<broker>:9092 \
  --set canton.txTopic=canton.devnet.tx
```

For an authenticated participant, put the JWT in a Secret and set
`canton.tokenSecret.name` / `canton.tokenSecret.key`. See `deploy/helm/trunk-canton/values.yaml`
for the full interface.

## Demo

`demo/localnet` runs a self-contained Canton node with scripted scenarios; `demo/triggers` holds
scripts that generate Create / Exercise / Archive and topology events. Point the adapter at the demo
node and watch normalized records land on the topic. See `demo/README.md`.

## Test

```bash
pip install -e ".[dev]"
pytest
```

The suite validates the model against recorded ingestion scenarios in `tests/fixtures/`, covering
snapshot bootstrap plus Create / Exercise / Archive and topology events.

## Known limitations

- Transient Ledger API errors are retried, but there is no **token refresh**: an expired JWT is a
  permanent `401` and fails fast. Supply a valid token (or a refreshed one on restart) out of band.
- The Ledger API calls (`httpx`) are synchronous inside the async producer loop. Harmless for a
  single-participant adapter, but a production adapter serving many participants should move to
  `httpx.AsyncClient`.

## Portability, automatic python environment switch

 This repo has optional instructions how to make your python automatically switch to project-related virtual environment in [AUTOCONDA.md](AUTOCONDA.md).

## Demo video



https://github.com/user-attachments/assets/76b57a22-3b7e-4d81-986b-df32e8016a38



## License

Apache License 2.0. See [LICENSE](LICENSE). Copyright 2026 Hacken <https://hacken.io>.
