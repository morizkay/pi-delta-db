# pi-delta-db

[![npm](https://img.shields.io/npm/v/pi-delta-db.svg)](https://www.npmjs.com/package/pi-delta-db)
[![license](https://img.shields.io/npm/l/pi-delta-db.svg)](./LICENSE)

Pi extension that lets Pi store and read data through the [Delta](https://delta.dev) CLI (`delta`).

Writes go through Delta's supported path: a private thread bound to the Pi session, recorded
with schema-1 JSONL (`delta thread create` + `delta thread append`). Everything Pi stores shows
up in the Delta app and can be read back with or without Pi. This is a recording plugin, not a
full join: it does not mount the thread's files, pull teammate messages into Pi, or run
subagents as child threads.

## Auto-recording

Every finalized user message, assistant message, thought, tool call, and tool result is
appended to this session's Delta thread, in order. The thread is created on the first message
and titled with the first prompt; continuing a session (`pi -c`) keeps appending to the same thread.

Records are numbered when they are written to `~/.pi/agent/delta-db/outbox/` and fsynced
before Delta is contacted. A retry sends those same bytes, so Delta returns `duplicate` instead
of storing a second copy. Only the receipt-confirmed prefix is deleted. A retryable error waits
until `retry_after_ms`, then tries again. A non-retryable error pauses recording until `/delta on`.
While a turn is running, a `state_update` marks it running and then idle, and a thread watch
stops the Pi turn if Delta reports that turn superseded.

| Control | Effect |
| --- | --- |
| `/delta` | Show recording state, thread id, queued count, title, share URL |
| `/delta off` / `/delta on` | Toggle recording for this session, remembered next start |
| `PI_DELTA_RECORD=0` | Start Pi with recording off for this process |

Bindings whose Pi session file is gone and that have not been touched for a week are deleted
on session start. A binding with an unsent outbox is kept.

## Tools

| Tool | What it does |
| --- | --- |
| `delta_store` | Append text to this session's thread with an explicit `title`/`role`. |
| `delta_read` | `list` threads, `search` by keywords, or `show` one transcript. |

## How it binds

One thread per Pi session:

- Producer: `--source pi --external-id <pi-session-id>`
- Created with `--visibility private --organization <current org>`, which is what makes
  `thread append` allowed (Delta rejects appends to unshared threads).
- The thread id, title, and next sequence live in `~/.pi/agent/delta-db/<session-id>.json`.
- Sequence numbers are assigned locally before the send. Writes are serialized through one queue.
- Recording stops if `delta thread contract` is outside contract 8.

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

### GitHub Packages mirror

Every release is also mirrored to GitHub Packages as `@morizkay/pi-delta-db` by
[`.github/workflows/publish-github-packages.yml`](./.github/workflows/publish-github-packages.yml),
which is why the repository's Packages tab lists it. Prefer npm: GitHub Packages requires a token
with `read:packages` even for public packages.

```bash
# ~/.npmrc: //npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
pi install npm:@morizkay/pi-delta-db
```

## Development

```bash
npm install    # host packages are peer dependencies, dev-only here
npm run check  # tsc --noEmit
npm test       # record shapes, outbox retry, and prune, against a fake delta
```

CI runs both on every push. Publishing a GitHub release also mirrors the package to GitHub
Packages, and publishes to npmjs.org when the `NPM_TOKEN` secret is set
(`gh secret set NPM_TOKEN`; use an automation token so OTP is not required).

## Gotchas found against delta 0.23.0

- `messageId` must be globally unique: a record whose `messageId` matches an earlier one
  **replaces** it, so a repeated id silently erases earlier messages.
- `session_info_update` re-titles the thread on every append, so the title is written once and
  remembered in the binding file.
- Appends send records to Delta's cloud (private visibility) — not purely local storage.
- Records are append-only: nothing can be overwritten or deleted individually.
- A retry must resend the saved sequence. Renumbering from `thread status` stores a second copy.

## License

[MIT](./LICENSE) © 2026 morizkay. Changes are documented in the [changelog](./CHANGELOG.md).
