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

- `server.js` — Express server. Mock API (--local-dev), echo state endpoint,
  explorer proxy, static `public/`, dual chain pollers. No auth middleware
  (echo is public — see "Auth model" below).
- `echo-logic.js` — Core state machine: dedups incoming tx, calls the
  sidecar `/wallet/send`, watches outgoing tx for confirmation, exposes
  `/__echo/state`.
- `lib/dapp-server.js` — Vendored helpers (mock API, chain poller, explorer
  proxy, env loader). Copied from `usernode-dapp-starter`; do not edit
  in-place — re-vendor from upstream when fixes land there.
- `public/` — Single-file HTML/JS UI plus the shared `usernode-usernames.js`.
  The bridge is loaded from
  `https://social-vibecoding.usernodelabs.org/usernode-bridge/v1/bridge.js` —
  canonical source lives in the social-vibecoding repo at
  `public/usernode-bridge/v1/bridge.js`. Never vendor it per-app; bridge
  fixes ship from one SV redeploy, fleet-wide.

## Running locally

```bash
npm install
npm run dev          # mock mode, http://localhost:3000
npm start            # production mode (requires .env)
```

## Auth model

Echo is **public**. There is no JWT, no platform login required, no
`req.user` consulted anywhere. The `JWT_SECRET` env var is no longer used
and `jsonwebtoken` is not a dependency. Wallet operations are signed
client-side via `usernode-bridge.js`, which has three modes and picks one
automatically:

- **Native (top frame in Flutter WebView)** — the Usernode mobile app
  injects a `Usernode` JS channel on every loaded page (see
  `flutter-mobile-app/lib/features/dapps/dapp_webview_screen.dart`,
  `addJavaScriptChannel('Usernode', …)` on the `WebViewController`). The
  bridge detects this with `!!window.Usernode` and routes
  `sendTransaction` / `signMessage` through the channel.
- **Iframe-relay (echo embedded inside another page that has the native
  channel — e.g. dapp-starter loaded inside the WebView)** — the bridge
  posts a `discover` message to `window.parent`; if the parent ACKs, the
  child flips into relay mode and round-trips its native calls through
  the parent's `Usernode.postMessage`.
- **QR fallback (desktop browser, no native channel anywhere in the
  frame stack)** — `sendTransaction` shows a QR code for the user to
  scan with the Usernode mobile app, then polls for inclusion.

This means the share URL `https://echo.<USERNODE_DOMAIN>` works the same
for anyone who opens it: they get the app, and tx signing routes through
whichever transport their environment supports.

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

## Direct-to-node live tail (opt-in)

Set `USE_NODE_STREAM=1` in `.env` to bypass the explorer's 5–60s indexing
lag for live transaction delivery. The cache replaces the explorer poller
for the `recipient` queryField with `createNodeRecentTxStream` (SSE +
catch-up poll against the sidecar's `/transactions/stream` and
`/transactions/by_recipient` endpoints). Backfill and the `sender`
queryField still go through the explorer. Off by default — needs a
sidecar usernode build that exposes those endpoints.

## App-specific conventions

- The pot is conceptually empty — echoes are funded by the difference
  between N and N-1 (a one-token "fee" per echo). Do not deplete the
  echo address; ensure it has a positive balance before deploying.
- Memo size is well under the 1024-byte chain limit; keep it that way.
- `/__echo/state` is intentionally public. It exposes a global summary
  that is the same for every viewer.
