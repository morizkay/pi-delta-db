# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

[Unreleased]: https://github.com/morizkay/pi-delta-db/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/morizkay/pi-delta-db/releases/tag/v0.1.0
