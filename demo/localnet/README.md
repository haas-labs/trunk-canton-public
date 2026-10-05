# Local Canton network for the demo scenarios

The scripted demo scenarios for the Canton ingestion adapter, runnable against a real Canton 3.5.16:
two participants (`p1` hosts the monitored party `client`; `p2` hosts `counterparty`,
`stranger` and whatever a scenario creates), two synchronizers (`sync1`, `sync2`), the example
`Iou` template from the Canton release, and a Ledger API user `extractor` that can only read as
`client`. The fixtures in `../../tests/fixtures` were captured from exactly this network.

```sh
docker compose build && docker compose up -d      # ~1 min until out/ids.json appears
docker compose exec canton bin/canton run /scripts/scenarios/369_topology_drift.canton -c /conf/remote.conf
```

| Scenario | What it does on the network | What the client's participant sees |
|---|---|---|
| `000_reset_hosting` | puts `counterparty` back to "hosted on p2 only" | `authorization_revoked` for p1, if it was a host |
| `369_topology_drift` | adds p1 as host of counterparty (Confirmation), then removes it | `authorization_added`, `authorization_revoked` |
| `370_capability_drift` | adds p1 as Observation host, upgrades it to Submission; threshold 2 then 1 | `authorization_added`, `authorization_changed`; the threshold change produces no Ledger API event |
| `371_stakeholder_bloat` | enables 60 parties, creates an Iou with all of them as observers | `created` with 61 observers |
| `372_reassignment_limbo` | unassigns an Iou from sync1 to sync2 and stops | `unassigned`; the active-contracts snapshot lists it as incomplete |
| `372b_reassignment_complete` | assigns the pending one | `assigned` on sync2 |
| `373_informee_anomaly` | the client shares an Iou with `stranger`, then transfers it | `exercised(Share)` + child `created` with `stranger` as observer; `exercised(Transfer)` |
| `367_archive` | the payer archives an Iou the client holds, through the built-in `Archive` choice | `exercised(Archive, consuming)`; the contract leaves the active set (`ArchivedEvent` in the `ACS_DELTA` shape) |
| `370b_threshold_two` | p1 becomes a Submission host of counterparty (if it is not yet), confirmation threshold 2 | `authorization_added` only if p1 was not a host; nothing for the threshold. Admin API (`localhost:5012`): threshold 2 |
| `370c_threshold_one` | the threshold drops back to 1 with the same two hosts | **nothing on the Ledger API**; Admin API: threshold 1. The drop the capability drift detector alerts on with `admin_api_url` |

Run `000` before repeating `369` or `370` on the same network (`370` leaves p1 as a host).
`371` uses unique party names and can be repeated; `370b` and `370c` can be repeated in that order.

Reading the client's view: the JSON Ledger API is on `localhost:7575`
(`GET /v2/state/ledger-end`, `POST /v2/state/active-contracts`, `POST /v2/updates/get-updates-page`,
`GET /docs/openapi`). The ingestion adapter (`python -m trunk_canton`) is what turns
it into `CantonUpdate` records on the Kafka topic, readable on `localhost:19092`. Party and
synchronizer ids of a fresh network are in `out/ids.json`.

Trigger scripts for a shared DevNet participant instead are in `../triggers`.

The network runs without authentication and binds to every interface: keep it on a machine or
VPN you control. `config/jwt/*.conf` in the Canton release shows how to enable JWT.
