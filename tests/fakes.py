"""Shared fakes for the hermetic tests: a fake Ledger API (JSON API v2) and a fake Kafka that drive
the real main(), plus the update payloads they serve. Imported by the tests, never collected itself."""
import asyncio
import io
import pathlib
import tempfile
from contextlib import redirect_stdout
from unittest import mock

PARTY = "client::1220abcd"
SYNC = "sync1::1220aa11"


def _created(cid, offset):
    return {"offset": offset, "nodeId": 0, "contractId": cid, "templateId": "pkg:Iou:Iou",
            "packageName": "CantonExamples", "witnessParties": [PARTY], "acsDelta": True,
            "signatories": [PARTY], "observers": [], "createdAt": "2026-01-01T00:00:00Z",
            "createArgument": {"amount": "10"}}


def _good_txn(offset):
    return {"update": {"Transaction": {"value": {
        "updateId": f"u-{offset}", "synchronizerId": SYNC, "recordTime": "2026-01-01T00:01:00Z",
        "offset": offset, "events": [{"CreatedEvent": _created(f"c-{offset}", offset)}]}}}}


def _poison(offset):
    # A kind from_json_api does not know -> ValueError. Its offset is still discoverable for advance.
    return {"update": {"BrandNewUpdateKind": {"value": {"offset": offset, "updateId": f"bad-{offset}"}}}}


def _topology(offset):
    return {"update": {"TopologyTransaction": {"value": {
        "updateId": f"t-{offset}", "synchronizerId": SYNC, "recordTime": "2026-01-01T00:00:30Z",
        "offset": offset, "events": [{"ParticipantAuthorizationAdded": {
            "partyId": PARTY, "participantId": "p1::1220ef",
            "participantPermission": "PARTICIPANT_PERMISSION_SUBMISSION"}}]}}}}


class _Resp:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code
    def json(self): return self._data
    def raise_for_status(self): pass


class FakeLedger:
    def request(self, method, url, headers=None, **kw):
        return self.get(url, headers=headers, **kw) if method == "GET" else self.post(url, headers=headers, **kw)

    def get(self, url, headers=None, **kw):
        if url.endswith("/v2/users/extractor/rights"):
            return _Resp({"rights": [{"kind": {"CanReadAs": {"value": {"party": PARTY}}}}]})
        if url.endswith("/v2/state/ledger-end"):
            return _Resp({"offset": 46})
        raise AssertionError(url)

    def post(self, url, headers=None, json=None, timeout=None, **kw):
        if url.endswith("/v2/state/active-contracts"):
            return _Resp([{"contractEntry": {"JsActiveContract": {
                "createdEvent": _created("c-snap", 45), "synchronizerId": SYNC, "reassignmentCounter": 0}}}])
        if url.endswith("/v2/updates/get-updates-page"):
            begin = json["beginOffsetExclusive"]
            if "includeTransactions" not in json["updateFormat"]:      # topology baseline
                return _Resp({"updates": [_topology(10)]} if begin < 10 else {"updates": []})
            if begin < 60:  # one good update and one poison update on the same page
                return _Resp({"updates": [_good_txn(60), _poison(61)]})
            return _Resp({"updates": []})
        raise AssertionError(url)


class FakeProducer:
    def __init__(self, records, fail_send_key=None, **kw):
        self.records = records
        self._fail_send_key = fail_send_key  # make the delivery of this key's record fail
    async def start(self): pass
    async def stop(self): pass
    async def send(self, topic, key=None, value=None):
        self.records.append((topic, key, value))
        fut = asyncio.get_running_loop().create_future()
        if key == self._fail_send_key:
            fut.set_exception(RuntimeError("delivery failed: record not acknowledged"))
        else:
            fut.set_result(None)  # delivery ack; _publish_all awaits this
        return fut
    async def send_and_wait(self, topic, key=None, value=None): self.records.append((topic, key, value))
    async def flush(self): pass


class FakeAdmin:
    def __init__(self, **kw): pass
    async def start(self): pass
    async def close(self): pass
    async def create_topics(self, topics):
        return type("R", (), {"topic_errors": [(topic.name, 0) for topic in topics]})()


def _run(ledger=None, max_consecutive_dlq=None, offset_file=None, fail_send_key=None):
    import importlib
    import trunk_canton.__main__ as adapter
    importlib.reload(adapter)
    tmp_dir = tempfile.mkdtemp()
    adapter.OFFSET_FILE = pathlib.Path(offset_file) if offset_file else pathlib.Path(tmp_dir) / "offset.txt"
    adapter.TOPIC = "canton.test.tx"
    adapter.DLQ_TOPIC = "canton.test.tx.dlq"
    if max_consecutive_dlq is not None:
        adapter.MAX_CONSECUTIVE_DLQ = max_consecutive_dlq
    ledger = ledger or FakeLedger()
    records = []
    with mock.patch.object(adapter, "AIOKafkaProducer", lambda **kw: FakeProducer(records, fail_send_key=fail_send_key, **kw)), \
         mock.patch.object(adapter, "AIOKafkaAdminClient", lambda **kw: FakeAdmin(**kw)), \
         mock.patch("httpx.request", ledger.request), \
         redirect_stdout(io.StringIO()):
        asyncio.run(adapter.main(once=True, reset=False))
    return adapter, records
