# Changelog

All notable changes to this project are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/); while the version is 0.y.z, a minor or patch
release may still carry breaking changes, listed under "Breaking changes".

## [0.1.1] - Unreleased

### Breaking changes

- **Record keys are scoped by node.** Whenever the participant id is known, every key gets the
  `<participant_id>/` prefix:

  | Record | Old key | New key |
  |---|---|---|
  | transaction, reassignment, topology | `<update_id>` | `<participant_id>/<update_id>` |
  | checkpoint | `checkpoint-<offset>` | `<participant_id>/checkpoint-<offset>` |
  | snapshot | `snapshot` | `<participant_id>/snapshot` |
  | dead-letter | `<offset>` | `<participant_id>/<offset>` |

  The id comes from `CANTON_PARTICIPANT_ID`, otherwise from the participant itself, so the new
  keys apply by default. Keys stay as before only when neither answers.
  **What to change:** a consumer that looks records up, compacts or deduplicates by the old key
  format must strip the `<participant_id>/` prefix or key on `(participant_id, update_id)`.
  A consumer that rejects unknown JSON fields must accept the new `participant_id` field.

### Added

- `participant_id`, the node the record was read from, on every record: the topology baseline,
  the updates, the snapshot and the dead-letter records. It comes from `CANTON_PARTICIPANT_ID`,
  else from `GET /v2/parties/participant-id` at start; when neither answers, the field is left out
  and ingestion goes on. Several adapters can then publish to one topic and consumers tell the
  nodes apart.
- `CANTON_PARTICIPANT_ID` environment variable and the Helm value `canton.participantId`
  (empty asks the participant).
- Release workflow: pushing a version tag (`X.Y.Z`, matching the version in `pyproject.toml`)
  builds and publishes the wheel.

### Changed

- Helm chart and image version 0.1.1.

## [0.1.0] - 2026-10-05

Initial public release.
