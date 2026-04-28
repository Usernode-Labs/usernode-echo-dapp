# Echo — notes for Claude Code

A latency-test dapp on the Usernode chain. The user sends N tokens to the
echo address; the server-side process returns N-1, and the round-trip is
timed and displayed.

This app runs as a child app inside Usernode Social Vibecoding. Read the
authoritative platform conventions before making changes:

**Platform conventions (always current):**
https://usernode.evanshapiro.dev/claude.md

If a rule below this line conflicts with the hosted conventions, the hosted
conventions win.

## Architecture

- `server.js` — Express server. JWT auth middleware, mock API (--local-dev),
  echo state endpoint, explorer proxy, static `public/`, dual chain pollers.
- `echo-logic.js` — Core state machine: dedups incoming tx, calls the
  sidecar `/wallet/send`, watches outgoing tx for confirmation, exposes
  `/__echo/state`.
- `lib/dapp-server.js` — Vendored helpers (mock API, chain poller, explorer
  proxy, env loader). Copied from `usernode-dapp-starter`; do not edit
  in-place — re-vendor from upstream when fixes land there.
- `public/` — Single-file HTML/JS UI plus the shared `usernode-bridge.js`
  and `usernode-usernames.js`. The bridge is shared infrastructure; do not
  fork it per-app.

## Running locally

```bash
npm install
npm run dev          # mock mode, no auth, http://localhost:3000
npm start            # production mode (requires .env)
```

## Memo schema

Memos are JSON. Echo only acts on these:

- `user → echo`:  `{"app":"echo","type":"send"}`
- `echo → user`:  `{"app":"echo","type":"echo","ref":"<requestTxId>"}`

The `ref` field lets the outgoing poller match echo confirmations back to
their originating sends.

## Sidecar dependency

In production echo calls `POST /wallet/tracked_owner/add` and `POST
/wallet/signer` against the social-vibecoding `usernode-node` sidecar at
startup, then `POST /wallet/send` for each echo. Both are idempotent
(`ensureReady` retries on transient failure). No `--wallet-owner` flag is
needed on the sidecar.

## App-specific conventions

- The pot is conceptually empty — echoes are funded by the difference
  between N and N-1 (a one-token "fee" per echo). Do not deplete the
  echo address; ensure it has a positive balance before deploying.
- Memo size is well under the 1024-byte chain limit; keep it that way.
- `/__echo/state` is intentionally public. It exposes a global summary
  that is the same for every viewer.
