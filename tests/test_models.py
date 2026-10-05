"""Validate the vendored CantonUpdate model against recorded ingestion scenarios.

The fixtures are normalized CantonUpdate records (the adapter's Kafka output) captured from a demo
Canton node. Parsing every line proves the model round-trips the full set of update/event kinds the
Milestone-1 acceptance criteria require: contract Create / Exercise / Archive and topology changes.
"""
import json
import pathlib

import pytest

from sentinel.models.chains.canton.update import CantonUpdate, CantonEventType, CantonUpdateKind

FIXTURES = sorted((pathlib.Path(__file__).parent / "fixtures").glob("*.jsonl"))


def _records():
    for path in FIXTURES:
        for i, line in enumerate(path.read_text().splitlines()):
            line = line.strip()
            if line:
                yield path.name, i, line


@pytest.mark.parametrize("name,i,line", [(n, i, l) for n, i, l in _records()])
def test_fixture_line_parses_and_roundtrips(name, i, line):
    update = CantonUpdate.model_validate_json(line)
    # every record carries a kind and a participant-local offset
    assert isinstance(update.kind, CantonUpdateKind)
    assert isinstance(update.offset, int)
    # dump -> reload is stable
    again = CantonUpdate.model_validate_json(update.model_dump_json(exclude_none=True))
    assert again.kind == update.kind
    assert again.offset == update.offset
    assert len(again.events) == len(update.events)


def test_acceptance_event_types_present_across_fixtures():
    """The corpus exercises the event types the milestone calls out.

    Note on Archive: the adapter streams TRANSACTION_SHAPE_LEDGER_EFFECTS, where archiving a
    contract is a *consuming* `exercised` event (of the built-in `Archive` choice), not a separate
    `archived` event. A literal `archived` event only appears in the ACS_DELTA shape. So an archive
    is verified here as a consuming exercise.
    """
    seen_kinds = set()
    seen_events = set()
    consuming_exercise = False
    for _, _, line in _records():
        u = CantonUpdate.model_validate_json(line)
        seen_kinds.add(u.kind)
        for e in u.events:
            seen_events.add(e.type)
            if e.type == CantonEventType.EXERCISED and e.consuming:
                consuming_exercise = True

    assert CantonUpdateKind.SNAPSHOT in seen_kinds       # bootstrap of active state
    assert CantonUpdateKind.TRANSACTION in seen_kinds
    assert CantonEventType.CREATED in seen_events        # Create
    assert CantonEventType.EXERCISED in seen_events      # Exercise
    assert consuming_exercise                            # Archive (consuming exercise in LEDGER_EFFECTS)
    topology = {
        CantonEventType.AUTHORIZATION_ADDED,
        CantonEventType.AUTHORIZATION_CHANGED,
        CantonEventType.AUTHORIZATION_REVOKED,
        CantonEventType.AUTHORIZATION_ONBOARDING,
    }
    assert seen_events & topology                        # topology changes
