# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- A retry resends the sequence numbers saved in the outbox. It no longer asks `thread status`
  for a new number, which could store the same turn twice after a timeout.
- A partial append drops only the receipt-confirmed prefix (`recorded`, `duplicate`, or `fenced`).
- A non-retryable Delta error pauses recording instead of being retried as "unreachable".
  `reload_required` tells you to Fork & Restore or `delta thread discard --force`. `/delta on` retries.
- Pruning a week-old binding leaves it alone when its outbox still has unsent records.
- `delta_read` search puts `--` before the query words so a leading dash is not a flag.

### Added

- A contract check against `delta thread contract`. This build speaks contract 8 and stops
  recording outside that range.
- A live turn is marked running, then idle, with one `turnSequence`. A thread watch stops the
  Pi turn when Delta reports that turn superseded or a record comes back fenced.

### Removed

- `delta_db`. Delta's database is internal; reading it is not part of the plugin contract.

## [0.2.0] - 2026-11-01

### Added

- Durable outbox: records are fsynced under `~/.pi/agent/delta-db/outbox/` before Delta is
  contacted, and a failed append is retried on the next message and the next session start.
- Thoughts, tool calls, and tool results are recorded, not only message text.
- `/delta off` and `/delta on` are remembered for the session.
- Bindings whose session file is gone and that have not been touched for a week are pruned.
- Tests for record shapes, sequence assignment, outbox retry, and pruning (`npm test`).
- CI runs `npm run check` and `npm test` on every push.
- npm publish workflow: a published release publishes to npmjs.org when `NPM_TOKEN` is set.
- GitHub Packages mirror: `.github/workflows/publish-github-packages.yml` publishes
  `@morizkay/pi-delta-db` on every published release, populating the repository's Packages tab.
  npmjs.org (`pi-delta-db`) remains the installable channel.

## [0.1.0] - 2026-11-01

### Added

- `delta_store` tool: append text to this session's private Delta thread, with optional
  `title` and `role`.
- `delta_read` tool: list threads, search thread text, or show one transcript.
- `delta_db` tool: read-only inspection of Delta's local store (`stats`, `root <id>`).
- Automatic recording: every finalized user and assistant message is appended to the
  session's Delta thread in order, with the thread created on first use and titled from
  the first prompt.
- `/delta` command: show recording state, thread id, title, and share URL; `/delta on|off`
  toggles recording for the session.
- `PI_DELTA_RECORD=0` to start Pi with recording disabled.
- Writes are serialized through one queue and take their sequence from `delta thread status`,
  so concurrent appends cannot reuse a sequence.
- Recording failures warn once per session and never interrupt a Pi turn.

### Fixed

- A repeated `messageId` made Delta replace earlier records, silently erasing previous turns
  in a continued session. Message ids are now unique per message.
- `session_info_update` re-titled the thread on every continuation. The title is now written
  once and remembered in the session binding.

[Unreleased]: https://github.com/morizkay/pi-delta-db/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/morizkay/pi-delta-db/releases/tag/v0.2.0
[0.1.0]: https://github.com/morizkay/pi-delta-db/releases/tag/v0.1.0
