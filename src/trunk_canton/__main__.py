"""trunk-canton: read a Canton participant through its JSON Ledger API and publish CantonUpdate
records to a Kafka topic. Open-source reference ingestion for the Canton connector.

  python -m trunk_canton            # follow forever
  python -m trunk_canton --once     # drain and exit
  ... --reset          # forget the offset, republish the topology baseline and the snapshot
  ... --from 0 --once  # replay history from an offset (testing only)

Participant and credentials come from CANTON_JSON_API, CANTON_USER and CANTON_LEDGER_TOKEN (a JWT;
empty for a node without auth), the offset file from CANTON_OFFSET_FILE (one per participant, on
durable storage so a restart resumes from the persisted offset).
Broker and topic come from CANTON_KAFKA_BOOTSTRAP (default localhost:19092) and CANTON_TX_TOPIC
(default canton.localnet.tx; CANTON_UPDATES_TOPIC still read as a fallback name).

Behaviour, in the order the adapter follows:
  1. parties come from the user's rights (CanReadAs), not from a token
  2. first start (no persisted offset): every topology event since offset 0 (the hosting baseline),
     then GetLedgerEnd + GetActiveContracts -> one kind=snapshot record
  3. stream pages after the persisted offset, normalize each update to CantonUpdate, publish
  4. the offset is persisted only after the broker acknowledged the page (idempotent producer,
     key = update_id), so a crash between produce and persist can only cause a republish, never a gap
  5. an update that fails to parse is dead-lettered to CANTON_DLQ_TOPIC (default <topic>.dlq) with
     the raw body and the error, and the stream advances past it, so one bad update never wedges it
  6. but if CANTON_MAX_CONSECUTIVE_DLQ updates are dead-lettered in a row with no successful parse,
     the adapter stops, so a parser bug or schema change cannot silently dead-letter everything
"""
import asyncio, json, logging, os, pathlib, sys, time

import httpx
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from sentinel.models.chains.canton.update import CantonUpdate

log = logging.getLogger("trunk_canton")
API = os.environ.get("CANTON_JSON_API", "http://localhost:7575")   # the participant's JSON Ledger API
USER = os.environ.get("CANTON_USER", "extractor")                  # the Ledger API user the adapter runs as
TOKEN = os.environ.get("CANTON_LEDGER_TOKEN", "")                  # its JWT; empty for a participant without auth
BROKER = os.environ.get("CANTON_KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = os.environ.get("CANTON_TX_TOPIC") or os.environ.get("CANTON_UPDATES_TOPIC") or "canton.localnet.tx"  # tx: the platform's name for a transaction stream
# Updates that fail to parse (unknown kind, newer schema, malformed payload) go here with the raw
# body and the error, instead of wedging the stream. Defaults to <topic>.dlq.
DLQ_TOPIC = os.environ.get("CANTON_DLQ_TOPIC") or f"{TOPIC}.dlq"
# Circuit breaker: stop once this many updates are dead-lettered in a row with no successful parse in
# between. Dead-lettering keeps one bad update from wedging the stream, but a parser bug or an
# unhandled schema change would otherwise dead-letter *everything* while the offset advances,
# silently blinding downstream. The default is one full streaming page (see page(size=50)): a whole
# page with nothing parseable is the signal that the failure is systemic, not a stray record.
MAX_CONSECUTIVE_DLQ = int(os.environ.get("CANTON_MAX_CONSECUTIVE_DLQ", "50"))
OFFSET_FILE = pathlib.Path(os.environ.get("CANTON_OFFSET_FILE", "/tmp/canton/offset.txt"))  # one per participant, on durable storage
HEADERS = {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}
# Topology events are authorized per party like transactions: a user with CanReadAs on its own
# parties only receives their hosting changes. Counterparties' topology needs CanReadAsAnyParty
# (every party on the participant, so only acceptable on a participant the client owns) or the
# Admin API. Set CANTON_TOPOLOGY_ALL_PARTIES=1 when the user has that right.
TOPOLOGY_ALL = os.environ.get("CANTON_TOPOLOGY_ALL_PARTIES", "") == "1"
# Contract payloads (create and choice arguments) stay in the records by default: the topic is local
# infrastructure next to the participant, and the detectors of the later packs read them. The alert
# path hashes them; set CANTON_KEEP_PAYLOAD=0 for a topic shared beyond the client's boundary.
KEEP_PAYLOAD = os.environ.get("CANTON_KEEP_PAYLOAD", "1") != "0"
WILDCARD = {"cumulative": [{"identifierFilter": {"WildcardFilter": {"value": {"includeCreatedEventBlob": False}}}}]}
# Transient Ledger API failures (a network blip, a 5xx, a rate-limit) are retried with exponential
# backoff instead of terminating the process. A permanent error (4xx, e.g. an expired token) fails
# fast: retrying will not help. A sustained outage exhausts the retries and raises, so the
# deployment's restart policy takes over (the offset is only persisted after success, so no gap).
HTTP_MAX_RETRIES = int(os.environ.get("CANTON_HTTP_MAX_RETRIES", "5"))
HTTP_BACKOFF_BASE = float(os.environ.get("CANTON_HTTP_BACKOFF_BASE", "0.5"))  # seconds
HTTP_BACKOFF_MAX = float(os.environ.get("CANTON_HTTP_BACKOFF_MAX", "30"))     # seconds
TRANSIENT_STATUS = {429, 500, 502, 503, 504}


def _request(method, url, **kwargs):
    """httpx request with bounded exponential backoff on transient failures. Non-transient responses
    (2xx return; other 4xx raise immediately) are not retried."""
    last = None
    for attempt in range(HTTP_MAX_RETRIES + 1):
        try:
            r = httpx.request(method, url, headers=HEADERS, **kwargs)
        except (httpx.TransportError, httpx.TimeoutException) as e:
            last = e
        else:
            if r.status_code not in TRANSIENT_STATUS:
                r.raise_for_status()  # permanent 4xx -> raise now, no retry
                return r
            last = httpx.HTTPStatusError(f"transient status {r.status_code}", request=r.request, response=r)
        if attempt < HTTP_MAX_RETRIES:
            delay = min(HTTP_BACKOFF_BASE * (2 ** attempt), HTTP_BACKOFF_MAX)
            log.warning(f"transient error on {method} {url} (attempt {attempt + 1}/{HTTP_MAX_RETRIES + 1}): {last}; retry in {delay:.1f}s")
            time.sleep(delay)
    raise last


def parties():
    r = _request("GET", f"{API}/v2/users/{USER}/rights").json()
    ps = [x["kind"]["CanReadAs"]["value"]["party"] for x in r["rights"] if "CanReadAs" in x["kind"]]
    if not ps:
        raise SystemExit(f"user {USER!r} has no CanReadAs rights at {API}; nothing to ingest")
    return ps


def event_format(ps):
    return {"filtersByParty": {p: WILDCARD for p in ps}, "verbose": True}


def update_format(ps):
    return {"includeTransactions": {"eventFormat": event_format(ps), "transactionShape": "TRANSACTION_SHAPE_LEDGER_EFFECTS"},
            "includeReassignments": event_format(ps),
            "includeTopologyEvents": {"includeParticipantAuthorizationEvents": {"parties": topology_parties(ps)}}}


def snapshot(ps) -> CantonUpdate:
    end = _request("GET", f"{API}/v2/state/ledger-end").json()["offset"]
    r = _request("POST", f"{API}/v2/state/active-contracts", json={"eventFormat": event_format(ps), "activeAtOffset": end}, timeout=30)
    return CantonUpdate.snapshot_from_json_api(r.json(), end, keep_payload=KEEP_PAYLOAD)


def topology_parties(ps):
    return [] if TOPOLOGY_ALL else ps


def topology_format(ps):
    return {"includeTopologyEvents": {"includeParticipantAuthorizationEvents": {"parties": topology_parties(ps)}}}


def _raw_offset(raw):
    """Best-effort offset of an update we could not parse: the deepest 'offset' int in the raw body.
    Used only to advance past a dead-lettered update so a poison record does not re-loop forever."""
    best, stack = None, [raw]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            for k, v in node.items():
                if k == "offset" and isinstance(v, int):
                    best = v if best is None else max(best, v)
                elif isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(node, list):
            stack.extend(node)
    return best


def _parse_updates(raw_updates):
    """Parse each update independently so one malformed update dead-letters instead of aborting the
    whole page. Returns (parsed, poison) where poison is a list of (raw, error, offset)."""
    parsed, poison = [], []
    for u in raw_updates:
        try:
            parsed.append(CantonUpdate.from_json_api(u, keep_payload=KEEP_PAYLOAD))
        except Exception as e:
            poison.append((u, f"{type(e).__name__}: {e}", _raw_offset(u)))
    return parsed, poison


def topology_history(ps, end_offset, size=200):
    """
    Every ParticipantAuthorization* event since the participant's beginning, up to the snapshot
    offset. The stream proper starts at the snapshot, so without this replay a detector would not
    know where any existing party is hosted; topology events are few, so the replay is cheap.
    Returns (updates, poison) like page().
    """
    offset, out, dead = 0, [], []
    while offset < end_offset:
        r = _request("POST", f"{API}/v2/updates/get-updates-page", json={"beginOffsetExclusive": offset, "endOffsetInclusive": end_offset, "maxPageSize": size, "updateFormat": topology_format(ps)}, timeout=30)
        updates, poison = _parse_updates(r.json().get("updates", []))
        dead += poison
        page_offsets = [u.offset for u in updates] + [o for _, _, o in poison if o is not None]
        if not page_offsets:
            break
        out += updates
        offset = max(page_offsets)
    return out, dead


def page(ps, offset, size=50):
    r = _request("POST", f"{API}/v2/updates/get-updates-page", json={"beginOffsetExclusive": offset, "maxPageSize": size, "updateFormat": update_format(ps)}, timeout=30)
    return _parse_updates(r.json().get("updates", []))


async def ensure_topic():
    admin = AIOKafkaAdminClient(bootstrap_servers=BROKER)
    await admin.start()
    try:
        # aiokafka does not raise on a per-topic error: read the response
        new = [NewTopic(t, num_partitions=1, replication_factor=1, topic_configs={"retention.ms": "86400000"})  # one day, like the ingest topics
               for t in (TOPIC, DLQ_TOPIC)]
        resp = await admin.create_topics(new)
        errors = {t: code for t, code, *_ in getattr(resp, "topic_errors", [])}
        for t in (TOPIC, DLQ_TOPIC):
            code = errors.get(t, 0)
            log.log(logging.INFO if code in (0, 36) else logging.WARNING,
                    f"topic {t} " + ("created" if code == 0 else "exists" if code == 36 else f"not created (error {code}); the broker may not allow it"))
    except TopicAlreadyExistsError:
        log.info(f"topics {TOPIC}, {DLQ_TOPIC} exist")
    finally:
        await admin.close()


def _key(u):
    # idempotent-producer key: update_id is the cross-participant dedup key; checkpoints carry no
    # update_id, so fall back to the offset so consumers can still dedup a crash-republish.
    return u.update_id or f"checkpoint-{u.offset}"


async def _dead_letter(producer, poison):
    """Publish updates that could not be parsed to the dead-letter topic with the raw body and the
    error, so the stream keeps moving and the poison record stays auditable and reprocessable."""
    for raw, error, off in poison:
        record = json.dumps({"error": error, "offset": off, "raw": raw}, default=str)
        await producer.send_and_wait(DLQ_TOPIC, key=(str(off) if off is not None else None), value=record)
        log.warning(f"dead-lettered update at offset {off}: {error}")


async def _publish_all(producer, updates):
    """Enqueue every update, then block until the broker has acknowledged each one.

    AIOKafkaProducer.send() returns a Future whose result/exception carries the per-record delivery
    outcome; flush() alone drains the batches but never surfaces a delivery failure. Awaiting the
    futures is what makes 'the broker acknowledged the page' true, so the caller must not advance the
    persisted offset unless this returns without raising.
    """
    futures = [await producer.send(TOPIC, key=_key(u), value=u.model_dump_json(exclude_none=True)) for u in updates]
    for f in futures:
        await f  # raises if this record's delivery ultimately failed


def _write_offset(value):
    """Persist the offset atomically: write a temp file, fsync it, then rename over the target.
    os.replace is atomic on the same filesystem, so the offset file is never torn or empty. A crash
    leaves either the previous offset or the new one, so recovery is always a resume (a republish of
    the updates since that offset), never a re-bootstrap -- a re-bootstrap would restore the active
    state via a fresh snapshot but drop the individual events in between, which is what the
    downstream detectors alert on."""
    tmp = OFFSET_FILE.with_name(OFFSET_FILE.name + ".tmp")
    with open(tmp, "w") as f:
        f.write(str(value))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, OFFSET_FILE)


async def main(once: bool, reset: bool, start_from: int | None = None):
    if reset and OFFSET_FILE.exists():
        OFFSET_FILE.unlink()
    OFFSET_FILE.parent.mkdir(parents=True, exist_ok=True)
    if start_from is not None:
        _write_offset(start_from)  # replay from an explicit offset, no snapshot
    ps = parties()
    await ensure_topic()
    producer = AIOKafkaProducer(bootstrap_servers=BROKER, enable_idempotence=True, acks="all",
                                key_serializer=lambda k: k.encode() if k else None, value_serializer=lambda v: v.encode())
    await producer.start()
    published = 0
    try:
        if OFFSET_FILE.exists():
            offset = int(OFFSET_FILE.read_text())  # atomic writes guarantee a complete value
            log.info(f"resuming after offset {offset}")
        else:
            snap = snapshot(ps)
            baseline, baseline_poison = topology_history(ps, snap.offset)
            await _publish_all(producer, baseline)  # hosting baseline first, then the contracts
            await producer.send_and_wait(TOPIC, key="snapshot", value=snap.model_dump_json(exclude_none=True))
            if baseline_poison:
                await _dead_letter(producer, baseline_poison)
            offset = snap.offset
            _write_offset(offset)
            published += len(baseline) + 1
            log.info(f"topology baseline published: {len(baseline)} updates; snapshot: {len(snap.events)} entries at offset {offset}")
        consecutive_dlq = 0
        while True:
            updates, poison = page(ps, offset)
            if not updates and not poison:  # genuinely empty page: caught up with the stream
                if once:
                    break
                await asyncio.sleep(1)
                continue
            if poison:
                await _dead_letter(producer, poison)
            consecutive_dlq = 0 if updates else consecutive_dlq + len(poison)
            if consecutive_dlq >= MAX_CONSECUTIVE_DLQ:  # circuit breaker: systemic parse failure
                raise RuntimeError(f"{consecutive_dlq} updates dead-lettered in a row with no successful parse; "
                                   "stopping so a parser bug or schema change cannot silently blind downstream")
            await _publish_all(producer, updates)  # every record of the page acknowledged by the broker
            page_offsets = [u.offset for u in updates] + [o for _, _, o in poison if o is not None]
            if not page_offsets:  # a poison-only page with no discoverable offset: cannot advance safely
                raise RuntimeError(f"page of {len(poison)} undecodable updates with no offset; cannot advance")
            offset = max(offset, max(page_offsets))
            _write_offset(offset)  # only now, atomically
            published += len(updates)
            log.info(f"published {len(updates)} updates" + (f", {len(poison)} dead-lettered" if poison else "") + f", offset now {offset}")
    finally:
        await producer.stop()
    log.info(f"done: {published} records published to {TOPIC}")


def _parse_from(argv):
    if "--from" not in argv:
        return None
    i = argv.index("--from") + 1
    if i >= len(argv):
        raise SystemExit("--from requires an integer offset")
    try:
        return int(argv[i])
    except ValueError:
        raise SystemExit(f"--from expects an integer offset, got {argv[i]!r}")


def cli():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    for noisy in ("httpx", "httpcore", "aiokafka"):  # a request line per poll would bury the adapter's own log
        logging.getLogger(noisy).setLevel(logging.WARNING)
    start_from = _parse_from(sys.argv)
    asyncio.run(main(once="--once" in sys.argv, reset="--reset" in sys.argv, start_from=start_from))


if __name__ == "__main__":
    cli()
