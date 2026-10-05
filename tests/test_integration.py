"""Hermetic end-to-end test of the adapter's control flow, with no Canton node and no Kafka broker.

The Canton JSON Ledger API is faked with hand-crafted raw v2 payloads (routed by URL and request
body), and the Kafka producer/admin are replaced with in-memory fakes that record every publish.
The *real* `main()` drives the whole flow, so this exercises the Milestone-1 acceptance criteria:

  * bootstrap of visible active state (topology baseline + snapshot),
  * ingesting Create / Exercise(Archive) / topology events,
  * resume-from-persisted-offset after restart (no re-bootstrap),
  * crash-between-produce-and-persist leaves a republish, never a gap.

Runs on stdlib + httpx + aiokafka + pydantic (the runtime deps). Also `pytest`-discoverable.
"""
import asyncio
import io
import json
import logging
import pathlib
import sys
import tempfile
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sentinel.models.chains.canton.update import CantonUpdate, CantonEventType, CantonUpdateKind

PARTY = "client::122012abcd"
PARTICIPANT = "p1::122099ef"
SYNC = "sync1::1220aa11"


# ----------------------------------------------------------------- raw JSON Ledger API v2 builders

def _created(contract_id, offset):
    """Inner CreatedEvent value (as active-contracts nests it under createdEvent)."""
    return {
        "offset": offset, "nodeId": 0, "contractId": contract_id,
        "templateId": "pkg:Iou:Iou", "packageName": "CantonExamples",
        "witnessParties": [PARTY], "acsDelta": True,
        "signatories": [PARTY], "observers": [],
        "createdAt": "2026-01-01T00:00:00Z", "createArgument": {"amount": "10"},
    }


def _active_contract(contract_id, offset):
    return {"contractEntry": {"JsActiveContract": {
        "createdEvent": _created(contract_id, offset),
        "synchronizerId": SYNC, "reassignmentCounter": 0}}}


def _txn(update_id, offset):
    """A transaction that creates a contract and archives another (consuming exercise)."""
    return {"update": {"Transaction": {"value": {
        "updateId": update_id, "synchronizerId": SYNC,
        "recordTime": "2026-01-01T00:01:00Z", "offset": offset,
        "events": [
            {"CreatedEvent": _created(f"c-{offset}", offset)},
            {"ExercisedEvent": {
                "offset": offset, "nodeId": 1, "contractId": "c-old",
                "templateId": "pkg:Iou:Iou", "packageName": "CantonExamples",
                "witnessParties": [PARTY], "acsDelta": True,
                "choice": "Archive", "actingParties": [PARTY], "consuming": True,
                "lastDescendantNodeId": 1, "choiceArgument": {}}},
        ]}}}}


def _topology(update_id, offset):
    return {"update": {"TopologyTransaction": {"value": {
        "updateId": update_id, "synchronizerId": SYNC,
        "recordTime": "2026-01-01T00:00:30Z", "offset": offset,
        "events": [{"ParticipantAuthorizationAdded": {
            "partyId": PARTY, "participantId": PARTICIPANT,
            "participantPermission": "PARTICIPANT_PERMISSION_SUBMISSION"}}]}}}}


# ----------------------------------------------------------------------------- fake HTTP transport

class _Resp:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code
    def json(self): return self._data
    def raise_for_status(self): pass


class FakeLedger:
    """Serves the JSON Ledger API. `tip` is the current ledger end for the stream; move it between
    runs to simulate new activity arriving while the adapter was down."""

    SNAPSHOT_OFFSET = 48
    TOPOLOGY_OFFSET = 10

    def __init__(self, tip):
        self.tip = tip

    def request(self, method, url, headers=None, **kw):
        # the adapter routes all HTTP through httpx.request (see _request with retry)
        return self.get(url, headers=headers, **kw) if method == "GET" else self.post(url, headers=headers, **kw)

    def get(self, url, headers=None, **kw):
        if url.endswith("/v2/users/extractor/rights"):
            return _Resp({"rights": [
                {"kind": {"CanReadAs": {"value": {"party": PARTY}}}},
                {"kind": {"CanActAs": {"value": {"party": PARTY}}}},  # must be ignored
            ]})
        if url.endswith("/v2/state/ledger-end"):
            return _Resp({"offset": self.SNAPSHOT_OFFSET})
        raise AssertionError(f"unexpected GET {url}")

    def post(self, url, headers=None, json=None, timeout=None, **kw):
        if url.endswith("/v2/state/active-contracts"):
            return _Resp([_active_contract("c-snap", 47)])
        if url.endswith("/v2/updates/get-updates-page"):
            begin = json["beginOffsetExclusive"]
            fmt = json["updateFormat"]
            if "includeTransactions" not in fmt:      # topology_history replay (0..snapshot]
                if begin < self.TOPOLOGY_OFFSET:
                    return _Resp({"updates": [_topology("t-10", self.TOPOLOGY_OFFSET)]})
                return _Resp({"updates": []})
            if begin < self.tip:                       # stream: one txn at the current tip
                return _Resp({"updates": [_txn(f"u-{self.tip}", self.tip)]})
            return _Resp({"updates": []})
        raise AssertionError(f"unexpected POST {url}")


# ------------------------------------------------------------------------------ fake Kafka producer

class FakeProducer:
    """Records every publish. send() returns a Future like the real producer; `fail_send_on` sets
    the delivery of the Nth record to fail (exception on its future), modelling a record that is
    enqueued but never acknowledged by the broker."""

    def __init__(self, records, *, fail_send_on=None, **kw):
        self.records = records          # shared list of (topic, key, value)
        self._fail_send_on = fail_send_on
        self._n = 0

    async def start(self): pass
    async def stop(self): pass

    async def send(self, topic, key=None, value=None):
        self.records.append((topic, key, value))
        self._n += 1
        fut = asyncio.get_running_loop().create_future()
        if self._fail_send_on == self._n:
            fut.set_exception(RuntimeError("delivery failed: record not acknowledged"))
        else:
            fut.set_result(("meta", topic, key, self._n))
        return fut

    async def send_and_wait(self, topic, key=None, value=None):
        self.records.append((topic, key, value))
        self._n += 1
        if self._fail_send_on == self._n:
            raise RuntimeError("delivery failed: snapshot not acknowledged")

    async def flush(self): pass


class FakeAdmin:
    def __init__(self, **kw): pass
    async def start(self): pass
    async def close(self): pass
    async def create_topics(self, topics):
        return type("R", (), {"topic_errors": [(t.name if hasattr(t, "name") else t, 0) for t in topics]})()


# ------------------------------------------------------------------------------------- test harness

def _run(offset_file, ledger, *, once=True, reset=False, start_from=None, fail_send_on=None):
    """Import a fresh copy of the adapter bound to this offset file + fake ledger, run main()."""
    import importlib
    import trunk_canton.__main__ as tc
    importlib.reload(tc)

    records = []
    tc.OFFSET_FILE = pathlib.Path(offset_file)
    prod = mock.patch.object(tc, "AIOKafkaProducer",
                             lambda **kw: FakeProducer(records, fail_send_on=fail_send_on, **kw))
    admin = mock.patch.object(tc, "AIOKafkaAdminClient", lambda **kw: FakeAdmin(**kw))
    hreq = mock.patch("httpx.request", ledger.request)

    buf = io.StringIO()
    handler = logging.StreamHandler(buf)   # the adapter logs through the "trunk_canton" logger
    tc.log.addHandler(handler)
    tc.log.setLevel(logging.INFO)
    try:
        with prod, admin, hreq:
            asyncio.run(tc.main(once=once, reset=reset, start_from=start_from))
    finally:
        tc.log.removeHandler(handler)
    return records, buf.getvalue()


def _kinds(records):
    return [CantonUpdate.model_validate_json(v).kind for _, _, v in records]


# --------------------------------------------------------------------------------------- the tests

def test_bootstrap_publishes_baseline_then_snapshot_then_stream():
    with tempfile.TemporaryDirectory() as d:
        off = pathlib.Path(d) / "offset.txt"
        records, out = _run(off, FakeLedger(tip=60))

        keys = [k for _, k, _ in records]
        kinds = _kinds(records)

        # order: topology baseline, then snapshot, then the streamed transaction
        assert kinds[0] == CantonUpdateKind.TOPOLOGY, kinds
        assert keys[0] == "t-10"                       # keyed by update_id, stable across republish
        assert kinds[1] == CantonUpdateKind.SNAPSHOT
        assert keys[1] == "snapshot"
        assert kinds[-1] == CantonUpdateKind.TRANSACTION
        assert keys[-1] == "u-60"

        # the snapshot carries the active contract set as a created event
        snap = CantonUpdate.model_validate_json(records[1][2])
        assert snap.offset == FakeLedger.SNAPSHOT_OFFSET
        assert [e.type for e in snap.events] == [CantonEventType.CREATED]

        # the streamed txn carries Create + a consuming Exercise (an archive under LEDGER_EFFECTS)
        txn = CantonUpdate.model_validate_json(records[-1][2])
        assert CantonEventType.CREATED in {e.type for e in txn.events}
        assert any(e.type == CantonEventType.EXERCISED and e.consuming for e in txn.events)

        # offset persisted to the last streamed offset
        assert off.read_text() == "60"
        assert "snapshot: 1 entries at offset 48" in out


def test_restart_resumes_from_offset_without_rebootstrap():
    with tempfile.TemporaryDirectory() as d:
        off = pathlib.Path(d) / "offset.txt"
        _run(off, FakeLedger(tip=60))                  # boot -> offset 60
        assert off.read_text() == "60"

        # new activity arrives (tip moves to 80) while the adapter restarts
        records, out = _run(off, FakeLedger(tip=80))

        assert "resuming after offset 60" in out
        keys = [k for _, k, _ in records]
        assert "snapshot" not in keys                  # no re-bootstrap
        assert keys == ["u-80"]                         # continued from 60
        assert off.read_text() == "80"


def test_delivery_failure_midpage_does_not_advance_offset_no_gap():
    """Regression for the fire-and-forget gap: a record that is enqueued but whose broker delivery
    ultimately fails must NOT let the offset advance past it. The adapter awaits each send future,
    so the failure propagates before the offset is written; the un-acked update is republished on
    the next run instead of being silently dropped."""
    with tempfile.TemporaryDirectory() as d:
        off = pathlib.Path(d) / "offset.txt"
        _run(off, FakeLedger(tip=60))                  # boot -> offset 60
        assert off.read_text() == "60"

        # restart with more activity (tip 80); the single streamed record fails delivery
        try:
            _run(off, FakeLedger(tip=80), fail_send_on=1)
            raised = False
        except RuntimeError:
            raised = True
        assert raised, "an unacked delivery must propagate, not be swallowed by flush()"
        assert off.read_text() == "60", "offset must not advance past an unacked record (gap!)"

        # next restart with a healthy broker republishes from 60 -> the un-acked update reappears
        records, out = _run(off, FakeLedger(tip=80))
        assert [k for _, k, _ in records] == ["u-80"]  # republished, not lost
        assert off.read_text() == "80"


def test_offset_is_written_atomically():
    """The offset is persisted via a temp file + rename, so it is never torn/empty and no temp is
    left behind. This is what lets recovery always resume (republish) instead of re-bootstrapping,
    which would restore state via a snapshot but drop the in-between events the detectors alert on."""
    import importlib
    import trunk_canton.__main__ as tc
    importlib.reload(tc)
    with tempfile.TemporaryDirectory() as d:
        tc.OFFSET_FILE = pathlib.Path(d) / "offset.txt"
        tc.OFFSET_FILE.write_text("5")
        tc._write_offset(9)
        assert tc.OFFSET_FILE.read_text() == "9"                       # replaced in place
        assert not (pathlib.Path(d) / "offset.txt.tmp").exists()       # temp renamed away, none left

    # end to end: after a bootstrap run the offset holds a complete value and no temp remains
    with tempfile.TemporaryDirectory() as d:
        off = pathlib.Path(d) / "offset.txt"
        _run(off, FakeLedger(tip=60))
        assert off.read_text() == "60"
        assert not (off.with_name("offset.txt.tmp")).exists()


def test_reset_republishes_baseline_and_snapshot():
    with tempfile.TemporaryDirectory() as d:
        off = pathlib.Path(d) / "offset.txt"
        _run(off, FakeLedger(tip=60))
        records, _ = _run(off, FakeLedger(tip=60), reset=True)
        keys = [k for _, k, _ in records]
        assert "snapshot" in keys                      # reset forces a fresh bootstrap
        assert keys[0] == "t-10"


ALL = [v for k, v in sorted(globals().items()) if k.startswith("test_")]

if __name__ == "__main__":
    failed = 0
    for t in ALL:
        try:
            t()
            print(f"PASS {t.__name__}")
        except Exception as e:
            failed += 1
            import traceback
            print(f"FAIL {t.__name__}: {e}")
            traceback.print_exc()
    print(f"\n{len(ALL) - failed}/{len(ALL)} passed")
    sys.exit(1 if failed else 0)
