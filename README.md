# pi-delta-db

[![npm](https://img.shields.io/npm/v/pi-delta-db.svg)](https://www.npmjs.com/package/pi-delta-db)
[![license](https://img.shields.io/npm/l/pi-delta-db.svg)](./LICENSE)

Pi extension that lets Pi store and read data through the [Delta](https://delta.dev) CLI (`delta`).

Delta's SQLite store behind `delta db` is internal and read-only from outside, so writes go
through Delta's supported path: a private Delta thread bound to the Pi session, recorded with
schema-1 JSONL (`delta thread create` + `delta thread append`). Everything Pi stores shows up
in the Delta app and can be read back with or without Pi.

## Auto-recording

Every finalized user and assistant message is appended to this session's Delta thread
automatically, in order. The thread is created on the first message and titled with the first
prompt; continuing a session (`pi -c`) keeps appending to the same thread.

| Control | Effect |
| --- | --- |
| `/delta` | Show recording state, thread id, title, share URL |
| `/delta off` / `/delta on` | Toggle recording for this session |
| `PI_DELTA_RECORD=0` | Start Pi with recording off |

Only text is recorded — thinking blocks and tool calls are skipped.

## Tools

| Tool | What it does |
| --- | --- |
| `delta_store` | Append text to this session's thread with an explicit `title`/`role`. |
| `delta_read` | `list` threads, `search` by keywords, or `show` one transcript. |
| `delta_db` | Read-only storage inspection: `stats` (schema version, node/root counts) or `root <id>`. |

## How it binds

One thread per Pi session:

- Producer: `--source pi --external-id <pi-session-id>`
- Created with `--visibility private --organization <current org>`, which is what makes
  `thread append` allowed (Delta rejects appends to unshared threads).
- The thread id and title state live in `~/.pi/agent/delta-db/<session-id>.json`.
- Each write asks `delta thread status` for the next sequence, and writes are serialized through
  one queue, so records stay ordered.

## Install

```bash
# from npm
pi install npm:pi-delta-db

# from git, or a local checkout in development
pi install git:github.com/morizkay/pi-delta-db
pi install ./pi-delta-db
```

Restart Pi (or run `/reload`). Requirements: Pi `>= 1.4.0`, Node `>= 22.19.0`, the `delta` CLI
on `PATH`, and a signed-in account (`delta auth login`).

For local development of this repository, link it into the agent directory instead:

```bash
ln -sfn "$(pwd)" ~/.pi/agent/extensions/pi-delta-db
```

## Development

```bash
npm install    # host packages are peer dependencies, dev-only here
npm run check  # tsc --noEmit
```

## Gotchas found against delta 0.23.0

- `messageId` must be globally unique: a record whose `messageId` matches an earlier one
  **replaces** it, so a repeated id silently erases earlier messages.
- `session_info_update` re-titles the thread on every append, so the title is written once and
  remembered in the binding file.
- Appends send records to Delta's cloud (private visibility) — not purely local storage.
- Records are append-only: nothing can be overwritten or deleted individually.

## License

[MIT](./LICENSE) © 2026 morizkay. Changes are documented in the [changelog](./CHANGELOG.md).
