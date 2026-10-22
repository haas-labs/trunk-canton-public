"""Poison-update isolation: one update that fails to parse must not wedge the stream.

Demonstrates both the problem (an unparseable update aborts a whole-page parse) and the fix (the
adapter routes it to the dead-letter topic, publishes the good updates, and advances the offset).
Hermetic: fake Ledger API + fake Kafka driving the real main().
"""
import pathlib
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

from fakes import PARTY, SYNC, _created, _good_txn, _poison, _Resp, _run  # noqa: E402
from sentinel.models.chains.canton.update import CantonUpdate, CantonUpdateKind  # noqa: E402


def test_unparseable_update_would_abort_a_whole_page_parse():
    """Documents the problem: parsing a page containing the poison update raises."""
    try:
        [CantonUpdate.from_json_api(update) for update in [_good_txn(60), _poison(61)]]
    except Exception:
        return  # expected: one bad update aborts the whole-page comprehension
    raise AssertionError("expected the poison update to raise")


def test_poison_is_dead_lettered_good_update_published_offset_advances():
    adapter, records = _run()
    main_topic = [(key, value) for topic, key, value in records if topic == adapter.TOPIC]
    dlq = [(key, value) for topic, key, value in records if topic == adapter.DLQ_TOPIC]

    # the good update reached the main topic; the poison one did not
    main_keys = [key for key, _ in main_topic]
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
    assert adapter.OFFSET_FILE.read_text() == "61"

    # the good record is a valid CantonUpdate
    good = CantonUpdate.model_validate_json(dict(main_topic)["u-60"])
    assert good.kind == CantonUpdateKind.TRANSACTION


def test_delivery_failure_on_good_update_keeps_offset_even_with_a_dead_lettered_poison():
    """Interaction of the two crash-safety features on one page: the page has a good update and a
    poison one; the poison is dead-lettered, but the good update's delivery fails. The offset must NOT
    advance (it stays at the bootstrap offset), even though the poison was already dead-lettered."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        offset_file = pathlib.Path(tmp_dir) / "offset.txt"
        try:
            _run(offset_file=offset_file, fail_send_key="u-60")  # fail the good update's delivery
            raised = False
        except RuntimeError:
            raised = True
        assert raised, "main() must raise when the good update's delivery is not acknowledged"
        # bootstrap wrote 46; the failed streaming page must not have advanced past it (no gap on retry)
        assert offset_file.read_text() == "46"


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
    except RuntimeError as error:
        assert "in a row" in str(error)
        return
    raise AssertionError("expected the circuit breaker to raise after consecutive dead-letters")


if __name__ == "__main__":
    import traceback
    tests = [test for name, test in sorted(globals().items()) if name.startswith("test_")]
    failed = 0
    for test in tests:
        try:
            test()
            print(f"PASS {test.__name__}")
        except Exception as error:
            failed += 1
            print(f"FAIL {test.__name__}: {error}")
            traceback.print_exc()
    print(f"\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
