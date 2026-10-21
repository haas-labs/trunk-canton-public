"""Every record names the node it was read from (participant_id), so several adapters can share one
topic; the keys are scoped by node so two adapters never share a key. Hermetic: the fake Ledger API
and fake Kafka of tests/fakes.py driving the real main()."""
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

from fakes import FakeLedger, _Resp, _run  # noqa: E402

NODE = "participant-01::1220abcd"


class NodeLedger(FakeLedger):
    def get(self, url, headers=None, **kw):
        if url.endswith("/v2/parties/participant-id"):
            return _Resp({"participantId": NODE})
        return super().get(url, headers=headers, **kw)


def _main_topic(adapter, records):
    return [(key, json.loads(value)) for topic, key, value in records if topic == adapter.TOPIC]


def test_every_record_carries_the_node_and_a_node_scoped_key():
    adapter, records = _run(ledger=NodeLedger())
    main = _main_topic(adapter, records)
    kinds = {record["kind"] for _, record in main}
    assert {"topology", "snapshot", "transaction"} <= kinds
    assert all(record["participant_id"] == NODE for _, record in main), "baseline, snapshot and stream are all stamped"
    keys = [key for key, _ in main]
    assert f"{NODE}/snapshot" in keys and f"{NODE}/u-60" in keys
    assert all(key.startswith(f"{NODE}/") for key in keys)


def test_a_participant_that_does_not_answer_keeps_the_records_as_before():
    # FakeLedger knows no /v2/parties/participant-id: the adapter goes on without the stamp
    adapter, records = _run()
    main = _main_topic(adapter, records)
    assert main and all("participant_id" not in record for _, record in main)
    keys = [key for key, _ in main]
    assert "snapshot" in keys and "u-60" in keys


def test_the_env_wins_over_the_participant(monkeypatch):
    monkeypatch.setenv("CANTON_PARTICIPANT_ID", "override::1220ff")
    adapter, records = _run(ledger=NodeLedger())
    assert {record["participant_id"] for _, record in _main_topic(adapter, records)} == {"override::1220ff"}


def test_two_adapters_on_one_topic_never_share_a_key():
    import trunk_canton.__main__ as adapter
    from sentinel.models.chains.canton.update import CantonUpdate
    snapshot = CantonUpdate(kind="snapshot", offset=5)
    tx = CantonUpdate(kind="transaction", offset=5, update_id="u-5")
    checkpoint = CantonUpdate(kind="checkpoint", offset=5)
    for update, name in ((snapshot, "snapshot"), (tx, None), (checkpoint, None)):
        assert adapter._key(update, "a::1220", name) != adapter._key(update, "b::1220", name)
    assert adapter._key(tx) == "u-5" and adapter._key(checkpoint) == "checkpoint-5", "no node known: the keys of before"


def _dead_letters(adapter, records):
    return [(key, json.loads(value)) for topic, key, value in records if topic == adapter.DLQ_TOPIC]


def test_the_dead_letter_names_the_node_and_has_a_node_scoped_key():
    adapter, records = _run(ledger=NodeLedger())
    assert [(key, record["participant_id"], record["offset"])
            for key, record in _dead_letters(adapter, records)] == [(f"{NODE}/61", NODE, 61)]


def test_without_a_node_the_dead_letter_is_as_before():
    adapter, records = _run()
    dead = _dead_letters(adapter, records)
    assert [key for key, _ in dead] == ["61"]
    assert all("participant_id" not in record for _, record in dead)


def test_two_adapters_never_share_a_dead_letter_key_for_the_same_offset():
    import asyncio
    import trunk_canton.__main__ as adapter
    from fakes import FakeProducer
    records = []
    poison = [({"update": {}}, "ValueError: unknown kind", 61)]
    for pid in ("a::1220", "b::1220"):
        asyncio.run(adapter._dead_letter(FakeProducer(records), poison, pid))
    keys = [key for _, key, _ in records]
    assert keys == ["a::1220/61", "b::1220/61"]
    assert [json.loads(value)["participant_id"] for _, _, value in records] == ["a::1220", "b::1220"]
