"""Poison-update isolation: one update that fails to parse must not wedge the stream.

Demonstrates both the problem (an unparseable update aborts a whole-page parse) and the fix (the
adapter routes it to the dead-letter topic, publishes the good updates, and advances the offset).
Hermetic: fake Ledger API + fake Kafka driving the real main().
"""
import asyncio
import io
import pathlib
import sys
import tempfile
from contextlib import redirect_stdout
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sentinel.models.chains.canton.update import CantonUpdate, CantonUpdateKind

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
        return type("R", (), {"topic_errors": [(t.name, 0) for t in topics]})()


def _run(ledger=None, max_consecutive_dlq=None, offset_file=None, fail_send_key=None):
    import importlib
    import trunk_canton.__main__ as tc
    importlib.reload(tc)
    d = tempfile.mkdtemp()
    tc.OFFSET_FILE = pathlib.Path(offset_file) if offset_file else pathlib.Path(d) / "offset.txt"
    tc.TOPIC = "canton.test.tx"
    tc.DLQ_TOPIC = "canton.test.tx.dlq"
    if max_consecutive_dlq is not None:
        tc.MAX_CONSECUTIVE_DLQ = max_consecutive_dlq
    ledger = ledger or FakeLedger()
    records = []
    with mock.patch.object(tc, "AIOKafkaProducer", lambda **kw: FakeProducer(records, fail_send_key=fail_send_key, **kw)), \
         mock.patch.object(tc, "AIOKafkaAdminClient", lambda **kw: FakeAdmin(**kw)), \
         mock.patch("httpx.request", ledger.request), \
         redirect_stdout(io.StringIO()):
        asyncio.run(tc.main(once=True, reset=False))
    return tc, records


def test_unparseable_update_would_abort_a_whole_page_parse():
    """Documents the problem: parsing a page containing the poison update raises."""
    from sentinel.models.chains.canton.update import CantonUpdate as CU
    try:
        [CU.from_json_api(u) for u in [_good_txn(60), _poison(61)]]
    except Exception:
        return  # expected: one bad update aborts the whole-page comprehension
    raise AssertionError("expected the poison update to raise")


def test_poison_is_dead_lettered_good_update_published_offset_advances():
    tc, records = _run()
    main_topic = [(k, v) for t, k, v in records if t == tc.TOPIC]
    dlq = [(k, v) for t, k, v in records if t == tc.DLQ_TOPIC]

    # the good update reached the main topic; the poison one did not
    main_keys = [k for k, _ in main_topic]
    assert "u-60" in main_keys
    assert "bad-61" not in main_keys

    # the poison update reached the DLQ with its error and raw body
    assert len(dlq) == 1
    import json as _json
    payload = _json.loads(dlq[0][1])
    assert payload["offset"] == 61
    assert "BrandNewUpdateKind" in payload["error"] or "Unknown" in payload["error"]
    assert payload["raw"]["update"]

    # the offset advanced past the poison (61), so the page is not re-fetched forever
    assert tc.OFFSET_FILE.read_text() == "61"

    # the good record is a valid CantonUpdate
    good = CantonUpdate.model_validate_json(dict(main_topic)["u-60"])
    assert good.kind == CantonUpdateKind.TRANSACTION


def test_delivery_failure_on_good_update_keeps_offset_even_with_a_dead_lettered_poison():
    """Interaction of the two crash-safety features on one page: the page has a good update and a
    poison one; the poison is dead-lettered, but the good update's delivery fails. The offset must NOT
    advance (it stays at the bootstrap offset), even though the poison was already dead-lettered."""
    with tempfile.TemporaryDirectory() as d:
        off = pathlib.Path(d) / "offset.txt"
        try:
            _run(offset_file=off, fail_send_key="u-60")  # fail the good update's delivery
            raised = False
        except RuntimeError:
            raised = True
        assert raised, "main() must raise when the good update's delivery is not acknowledged"
        # bootstrap wrote 46; the failed streaming page must not have advanced past it (no gap on retry)
        assert off.read_text() == "46"


class PoisonLedger:
    """Every streaming page is nothing but a poison update (a new offset each time), i.e. a systemic
    parse failure. Bootstrap succeeds so the run reaches the stream."""

    def request(self, method, url, headers=None, **kw):
        return self.get(url) if method == "GET" else self.post(url, **kw)

    def get(self, url, **kw):
        if url.endswith("/v2/users/extractor/rights"):
            return _Resp({"rights": [{"kind": {"CanReadAs": {"value": {"party": PARTY}}}}]})
        if url.endswith("/v2/state/ledger-end"):
            return _Resp({"offset": 46})
        raise AssertionError(url)

    def post(self, url, json=None, **kw):
        if url.endswith("/v2/state/active-contracts"):
            return _Resp([{"contractEntry": {"JsActiveContract": {
                "createdEvent": _created("c-snap", 45), "synchronizerId": SYNC, "reassignmentCounter": 0}}}])
        if url.endswith("/v2/updates/get-updates-page"):
            begin = json["beginOffsetExclusive"]
            if "includeTransactions" not in json["updateFormat"]:  # empty topology baseline
                return _Resp({"updates": []})
            return _Resp({"updates": [_poison(begin + 1)]})  # only poison, offset advances each page
        raise AssertionError(url)


def test_circuit_breaker_stops_after_consecutive_dead_letters():
    """A systemic parse failure (every update dead-lettered) must stop, not silently blind downstream."""
    try:
        _run(ledger=PoisonLedger(), max_consecutive_dlq=3)
    except RuntimeError as e:
        assert "in a row" in str(e)
        return
    raise AssertionError("expected the circuit breaker to raise after consecutive dead-letters")


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t(); print(f"PASS {t.__name__}")
        except Exception as e:
            failed += 1
            import traceback; print(f"FAIL {t.__name__}: {e}"); traceback.print_exc()
    print(f"\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
