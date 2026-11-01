# pi-delta-db

Pi extension that lets Pi store and read data through the [Delta](https://delta.dev) CLI (`delta`).

Delta's SQLite store behind `delta db` is internal and read-only from outside, so writes go
through Delta's supported path: a private Delta thread bound to the Pi session, recorded with
schema-1 JSONL (`delta thread create` + `delta thread append`). Everything Pi stores shows up
in the Delta app and can be read back with or without Pi.

## Tools

| Tool | What it does |
| --- | --- |
| `delta_store` | Append text to this session's private Delta thread (creates it on first use). Optional `title` sets the thread title, `role` picks user/assistant. |
| `delta_read` | `list` threads, `search` by keywords, or `show` one transcript. |
| `delta_db` | Read-only storage inspection: `stats` (schema version, node/root counts) or `root <id>` (decode one stored root). |

`/delta` shows the current session's thread and its share URL.

## How it binds

One thread per Pi session:

- Producer: `--source pi --external-id <pi-session-id>`
- Created with `--visibility private --organization <current org>`, which is what makes
  `thread append` allowed (Delta rejects appends to unshared threads).
- The thread id is cached in `~/.pi/agent/delta-db/<session-id>.json`.
- Each write asks `delta thread status` for the next sequence, so records stay ordered.

## Install

```bash
ln -sfn "$(pwd)" ~/.pi/agent/extensions/pi-delta-db
```

Restart Pi (or `/reload`). Requirements: `delta` on `PATH`, signed in (`delta auth login`).

## Notes

- Appends send records to Delta's cloud (`--visibility private`) — they are not purely local.
- Writes are content, not a key/value store: `delta_store` appends; it never overwrites.
- Deleting or rewriting recorded records is not supported by the CLI.
