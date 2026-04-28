# Echo

A Usernode latency-test dapp. Send `N` tokens to the echo address; the
server-side process returns `N-1`. The UI times the round-trip end-to-end
(user submit → on-chain confirm → echo on-chain → user observes) and breaks
the latency down into wall-clock and server-side components.

Designed to run as a child app inside Usernode Social Vibecoding, but also
works standalone (mobile WebView or desktop QR) when fronted by a node.

## Quick start

```bash
npm install
npm run dev          # mock mode at http://localhost:3000
```

For production:

```bash
cp .env.example .env # fill in JWT_SECRET, ECHO_APP_PUBKEY, ECHO_APP_SECRET_KEY
npm start
```

## Layout

```
echo/
  server.js          Express server: JWT auth, mock API, echo state,
                     explorer proxy, static, chain pollers.
  echo-logic.js      Core state machine.
  lib/
    dapp-server.js   Vendored helpers (mock API, chain poller, explorer
                     proxy, env loader). Source: usernode-dapp-starter.
  public/
    index.html       UI (single-file HTML/CSS/JS).
    usernode-bridge.js
    usernode-usernames.js
  Dockerfile         node:22-alpine, port 3000, /health probe.
  .env.example
  CLAUDE.md          App-specific notes for AI tooling.
```

## How it works

```
user → sendTransaction(echoAddr, N, memo)
                  │
                  ▼
       [Usernode Blockchain]
                  │
       recipient poller picks it up
                  │
                  ▼
       echo-logic.processTransaction
                  │
       /wallet/send N-1 → sidecar
                  │
                  ▼
       [Usernode Blockchain]
                  │
       sender poller catches the echo
                  │
                  ▼
       /__echo/state shows round-trip
```

Two pollers, one for `recipient` and one for `sender`, both feed the same
deduping handler. The recipient poller drives the echo; the sender poller
records the confirmation timestamp.

## Memo schema

```js
// user → echo
{ app: "echo", type: "send" }

// echo → user
{ app: "echo", type: "echo", ref: "<requestTxId>" }
```

## Configuration

| Var | Purpose |
| --- | --- |
| `JWT_SECRET` | Shared with the social-vibecoding platform; verifies iframe tokens. Unused in `--local-dev`. |
| `ECHO_APP_PUBKEY` | Echo's on-chain address. |
| `ECHO_APP_SECRET_KEY` | Used to sign outgoing `/wallet/send` calls. |
| `NODE_RPC_URL` | Sidecar URL. Default `http://usernode-node:3000` (compose internal). |
| `PORT` | HTTP port (default 3000). |

## Origin

Forked from [`usernode-dapp-starter/examples/echo`](https://github.com/Usernode-Labs/usernode-dapp-starter)
and adapted into a standalone repo so it can be deployed as an
independently-versioned child app on social-vibecoding.
