/**
 * Echo — standalone server for Usernode social-vibecoding.
 *
 * Hosts the echo latency-test dapp:
 *   - User sends N tokens to ECHO_APP_PUBKEY (memo {"app":"echo","type":"send"})
 *   - Server detects the tx via chain poller (recipient query)
 *   - Server uses the sidecar /wallet/send RPC to return N-1 to the sender
 *   - Server's outgoing chain poller (sender query) records the round-trip
 *   - Client polls /__echo/state to render send/echo/total latencies
 *
 * Modes:
 *   node server.js              — production mode (real chain)
 *   node server.js --local-dev  — local dev (mock transaction store)
 *
 * Auth model: echo is public. There is no JWT gate on the HTTP surface —
 * any visitor can load the page and read /__echo/state. Transaction signing
 * happens client-side via the bridge: native Usernode channel inside the
 * Flutter WebView (top frame OR iframe-relay), QR fallback in a desktop
 * browser. Echo's server never reads or relies on a platform identity.
 *
 * Env vars:
 *   PORT                — HTTP port (default 3000 — matches platform scaffold)
 *   ECHO_APP_PUBKEY     — echo dapp address (required for chain mode)
 *   ECHO_APP_SECRET_KEY — secret key for outgoing /wallet/send (required for chain mode)
 *   NODE_RPC_URL        — sidecar URL (default http://usernode-node:3000 inside compose)
 */

const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const express = require("express");

const {
  loadEnvFile,
  handleExplorerProxy,
  createMockApi,
  createAppStateCache,
  createUsernamesCache,
  createNodeStatusProbe,
  createDappServerStatus,
  discoverChainInfo,
} = require("./lib/dapp-server");
const createEcho = require("./echo-logic");

loadEnvFile();

// Public explorer URL the dapp links to from each round-trip's "Block #N"
// chip. Deliberately *separate* from EXPLORER_UPSTREAM (which is the API
// host the dapp polls): the explorer SPA and the explorer API are served
// from different subdomains in the canonical deploy
// (explorer.testnet.usernodelabs.org vs testnet-explorer.usernodelabs.org).
//
// Set EXPLORER_PUBLIC_BASE to disable links entirely (empty string) — the
// chip falls back to a non-clickable span showing the height. Useful when
// running against a localnet whose explorer SPA isn't reachable from the
// user's browser.
function getExplorerPublicBase() {
  const v = process.env.EXPLORER_PUBLIC_BASE;
  if (typeof v === "string") return v.trim();
  return "https://explorer.testnet.usernodelabs.org";
}

// ── CLI flags ────────────────────────────────────────────────────────────────
const LOCAL_DEV = process.argv.includes("--local-dev");
const PORT = parseInt(process.env.PORT, 10) || 3000;

// Environment + anomaly-detection persistence config.
const IS_STAGING = process.env.USERNODE_ENV === "staging";
// Directory for the better-sqlite3 metrics DB. Mounted as a volume in the
// Dockerfile so historical baselines survive container restarts; if it isn't
// writable (or the native module can't load) the metrics store degrades to
// in-memory and reports persistent:false.
const ECHO_DATA_DIR = process.env.ECHO_DATA_DIR || "/app/data";
// Opt-in synthetic baseline seeding so the detector has history to compare
// against in a fresh staging container. Ignored outside staging.
const ECHO_SEED_BASELINE = process.env.ECHO_SEED_BASELINE === "1";

// ── Echo config ──────────────────────────────────────────────────────────────
const ECHO_APP_PUBKEY = process.env.ECHO_APP_PUBKEY || "ut1_echo_default_pubkey";
const ECHO_APP_SECRET_KEY = process.env.ECHO_APP_SECRET_KEY || "";
const NODE_RPC_URL = process.env.NODE_RPC_URL || "http://usernode-node:3000";

// ── Express app ──────────────────────────────────────────────────────────────
const app = express();

// One hop (Caddy) in front of us.
app.set("trust proxy", 1);

// Health check — used by Docker healthcheck and platform polling.
app.get("/health", (_req, res) => res.json({ status: "ok" }));

// ── Identity capture (non-gating) ─────────────────────────────────────────────
// Echo is public and stays public — there is NO auth gate. But the platform
// shell injects a signed session JWT (?token=… on load, x-usernode-token on
// subsequent fetches). When present and valid we decode `req.user` so echo can
// record the sender's real Usernode username for the leaderboard. A missing or
// invalid token simply leaves `req.user` undefined; the request proceeds.
// RS256 is verified with the built-in crypto module against the platform's
// public key (USERNODE_JWT_PUBLIC_KEY) so we don't reintroduce a
// `jsonwebtoken` dependency (see CLAUDE.md "Auth model"). The algorithm,
// issuer and audience are pinned, and only `pur: "iframe"` tokens count.
const USERNODE_JWT_PUBLIC_KEY = process.env.USERNODE_JWT_PUBLIC_KEY || "";

function b64urlToBuf(s) {
  return Buffer.from(s.replace(/-/g, "+").replace(/_/g, "/"), "base64");
}

function verifyRs256Jwt(token, publicKey) {
  if (!token || !publicKey) return null;
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  const [headerB64, payloadB64, sigB64] = parts;
  let header;
  try {
    header = JSON.parse(b64urlToBuf(headerB64).toString("utf8"));
  } catch (_) {
    return null;
  }
  if (!header || header.alg !== "RS256") return null;
  let signatureOk = false;
  try {
    signatureOk = crypto
      .createVerify("RSA-SHA256")
      .update(`${headerB64}.${payloadB64}`)
      .verify(publicKey, b64urlToBuf(sigB64));
  } catch (_) {
    return null;
  }
  if (!signatureOk) return null;
  let payload;
  try {
    payload = JSON.parse(b64urlToBuf(payloadB64).toString("utf8"));
  } catch (_) {
    return null;
  }
  if (!payload) return null;
  if (payload.exp && Date.now() / 1000 > payload.exp) return null;
  if (payload.iss !== "usernode") return null;
  const audience = "usernode:app:" + process.env.USERNODE_APP_ID;
  const aud = payload.aud;
  const audOk = Array.isArray(aud) ? aud.includes(audience) : aud === audience;
  if (!audOk) return null;
  // Platform iframe sessions only — anything else is not an app session.
  if (payload.pur !== "iframe") return null;
  return payload;
}

app.use((req, _res, next) => {
  const token = req.query.token || req.headers["x-usernode-token"];
  if (token && USERNODE_JWT_PUBLIC_KEY) {
    const payload = verifyRs256Jwt(String(token), USERNODE_JWT_PUBLIC_KEY);
    if (payload) req.user = payload;
  }
  next();
});

// ── Mock API (only --local-dev) ──────────────────────────────────────────────
const mockApi = createMockApi({ localDev: LOCAL_DEV });
app.use((req, res, next) => {
  if (mockApi.handleRequest(req, res, req.path)) return;
  next();
});

// ── Echo state endpoint ──────────────────────────────────────────────────────
// Echo state is a global, read-only summary of recent round-trips. Per
// conventions, GET routes that don't live under /api/ are public; the data
// here is the same for every viewer, so that's fine.
const echo = createEcho({
  appPubkey: ECHO_APP_PUBKEY,
  appSecretKey: ECHO_APP_SECRET_KEY,
  nodeRpcUrl: NODE_RPC_URL,
  localDev: LOCAL_DEV,
  mockTransactions: LOCAL_DEV ? mockApi.transactions : null,
  // Anomaly-detection persistence
  dataDir: ECHO_DATA_DIR,
  isStaging: IS_STAGING,
  seedBaseline: ECHO_SEED_BASELINE,
});
// echo.start() runs the sidecar /wallet/signer ensureReady loop + hydrates
// the durable diagnostic log into memory; chain plumbing (recipient + sender
// pollers, backfill, mock drain) is in echoCache below. We discover the
// active chain id *first* (in chain mode) so the durable rows are stamped and
// hydrated under the right chain_id. onChainReset keeps it current afterwards.
(async () => {
  if (!LOCAL_DEV) {
    try {
      const info = await discoverChainInfo();
      if (info && info.chainId) echo.setChainId(info.chainId);
    } catch (_) {}
  }
  echo.start();
})();

const echoCache = createAppStateCache({
  name: "echo",
  appPubkey: ECHO_APP_PUBKEY,
  queryFields: ["recipient", "sender"],
  processTransaction: echo.processTransaction,
  handleRequest: echo.handleRequest,
  onChainReset(newId, oldId) {
    console.log(`[echo] chain reset ${oldId} -> ${newId}, resetting state`);
    // Stamp new rows with the new chain id; the durable log keeps the old
    // chain's rows (history is preserved, reads scope to the current chain).
    // Also pass it as the metrics epoch so post-reset samples baseline
    // independently and prior-epoch anomalies auto-resolve.
    echo.setChainId(newId);
    echo.reset(newId);
  },
  localDev: LOCAL_DEV,
  mockTransactions: LOCAL_DEV ? mockApi.transactions : null,
  nodeRpcUrl: NODE_RPC_URL,
});
echoCache.start();

app.use((req, res, next) => {
  if (echoCache.handleRequest(req, res, req.path)) return;
  next();
});

// ── Global usernames cache ───────────────────────────────────────────────────
// Same shared wiring as echoCache, just for the global usernames address.
// Connected echo clients (and any other dapp the usernames module is loaded
// into) hit `GET /__usernames/state` instead of independently paginating
// the explorer. Public on purpose: usernames are global, identical for every
// viewer.
const usernamesCache = createUsernamesCache({
  localDev: LOCAL_DEV,
  mockTransactions: LOCAL_DEV ? mockApi.transactions : null,
  nodeRpcUrl: NODE_RPC_URL,
});
usernamesCache.start();

app.use((req, res, next) => {
  if (usernamesCache.handleRequest(req, res, req.path)) return;
  next();
});

// ── Sidecar /status probe (powers /status page node card) ────────────────────
// Polls the sidecar every 2s (fast during boot, slow once Synced) and caches
// the snapshot at /__usernode/node_status. Per-cache stream readiness is
// registered so the status page can show whether each cache's SSE link is
// up — and so any future opt-in of usernode-loading.js's streamKey gate
// works without further changes.
const nodeStatusProbe = createNodeStatusProbe({
  nodeRpcUrl: NODE_RPC_URL,
  localDev: LOCAL_DEV,
});
nodeStatusProbe.registerStream("echo", () => echoCache.isStreamReady());
nodeStatusProbe.registerStream("usernames", () => usernamesCache.isStreamReady());
nodeStatusProbe.start();

app.use((req, res, next) => {
  if (nodeStatusProbe.handleRequest(req, res, req.path)) return;
  next();
});

// ── Explorer proxy ───────────────────────────────────────────────────────────
// Proxies /explorer-api/* to the public block explorer so the iframe can
// discover the chain id and (optionally) bypass the bridge for direct reads.
app.use((req, res, next) => {
  if (handleExplorerProxy(req, res, req.path)) return;
  next();
});

// ── Build version ────────────────────────────────────────────────────────────
// A short hash of every file in public/ — surfaced to the client three ways:
//   1. As an X-App-Version response header (visible via curl / DevTools).
//   2. Substituted into __BUILD_VERSION__ placeholders in index.html (we
//      use it both for a visible "Build XXXXXXXX" footer label and as
//      a ?v=… query string on the bridge <script src=…> tags so a stale
//      WebView cache can't shadow a new bridge).
//   3. As JSON at /__build (handy for scripted health checks).
// Recomputed on every request in --local-dev so iterating without a server
// restart still flips the version. In production the file set is fixed
// once the server starts, so a single startup compute is enough.
const PUBLIC_DIR = path.join(__dirname, "public");

function computeBuildVersion() {
  const hash = crypto.createHash("sha1");
  let names;
  try { names = fs.readdirSync(PUBLIC_DIR).sort(); } catch (_) { return "unknown"; }
  for (const file of names) {
    if (file.startsWith(".")) continue;
    try {
      const data = fs.readFileSync(path.join(PUBLIC_DIR, file));
      hash.update(file).update(data);
    } catch (_) {}
  }
  return hash.digest("hex").slice(0, 8);
}

const STARTUP_BUILD_VERSION = computeBuildVersion();
function getBuildVersion() {
  return LOCAL_DEV ? computeBuildVersion() : STARTUP_BUILD_VERSION;
}
console.log(`  Build version: ${STARTUP_BUILD_VERSION}`);

// Lightweight build-info endpoint. Public on purpose — it's just a hash.
app.get("/__build", (_req, res) => {
  res.set("Cache-Control", "no-store");
  res.json({ version: getBuildVersion(), localDev: LOCAL_DEV });
});

// ── Aggregated dapp-server status (HTML viewer + SSE) ───────────────────────
// Exposes /status (HTML), /__usernode/status (JSON), and
// /__usernode/status/stream (SSE). Operator-facing — public on purpose
// (matches /__usernode/node_status and /__usernames/state).
//
// Mounted after the build-version block because getBuildVersion's body
// references STARTUP_BUILD_VERSION (a `const`, in TDZ until evaluated).
// Mounted before the catch-all HTML shell so /status doesn't fall through
// to the index.html renderer.
const dappServerStatus = createDappServerStatus({
  name: "echo",
  nodeProbe: nodeStatusProbe,
  localDev: LOCAL_DEV,
  port: PORT,
  getBuildVersion,
});
dappServerStatus.registerCache(echoCache);
dappServerStatus.registerCache(usernamesCache);
dappServerStatus.registerPending("echo", () => echo.getPending());
// Surface open anomalies as their own section on the operator /status page.
// Uses the existing registerPending hook (no edits to the vendored
// lib/dapp-server.js); each open anomaly renders as one row.
dappServerStatus.registerPending("anomalies", () => echo.getAnomalyStatusRows());

app.use((req, res, next) => {
  if (dappServerStatus.handleRequest(req, res, req.path)) return;
  next();
});

// ── Static assets ────────────────────────────────────────────────────────────
// usernode-bridge.js, usernode-usernames.js, and any future CSS/images.
// These are always served — they're public infrastructure, not app data.
//
// Cache strategy: `no-cache` (NOT `no-store`) means the browser MAY keep a
// copy locally but MUST revalidate with the server every time before using
// it. Combined with the ?v=BUILD_VERSION query strings injected into
// index.html, this guarantees that any change to a bridge file produces a
// new URL the browser hasn't seen, bypassing the cache entirely.
app.use(express.static(PUBLIC_DIR, {
  index: false,
  etag: true,
  lastModified: true,
  setHeaders: (res) => {
    res.setHeader("Cache-Control", "no-cache, must-revalidate");
    res.setHeader("X-App-Version", getBuildVersion());
  },
}));

// ── HTML shell ───────────────────────────────────────────────────────────────
// Public — anyone can load the page. Wallet operations are signed
// client-side via the bridge (native channel inside the Flutter WebView,
// QR fallback in a desktop browser).

// Render the index.html template with placeholders substituted. Cached in
// production (file set + env are frozen) and re-rendered on each request
// in --local-dev so edits show up without a server restart.
//
// Substitutions:
//   __BUILD_VERSION__       — content hash of public/, also used as a
//                             cache-buster on bridge <script src=…> URLs.
//   __EXPLORER_PUBLIC_BASE__ — base URL for the public block explorer (e.g.
//                             https://testnet-explorer.usernodelabs.org)
//                             so the dapp can link "Block #N" chips to
//                             /blocks/<height> on the explorer SPA.
//   __ECHO_IS_STAGING__     — "true" in a staging container, else "false".
//                             Gates the Max button's ?demo=1 balance override
//                             so production can never inject a fake balance.
let _indexHtmlCache = null;
let _indexHtmlVersion = null;
function renderIndexHtml() {
  const version = getBuildVersion();
  if (LOCAL_DEV || _indexHtmlCache == null || _indexHtmlVersion !== version) {
    let raw;
    try {
      raw = fs.readFileSync(path.join(PUBLIC_DIR, "index.html"), "utf8");
    } catch (e) {
      return `<!doctype html><pre>Failed to read index.html: ${e.message}</pre>`;
    }
    _indexHtmlCache = raw
      .split("__BUILD_VERSION__").join(version)
      .split("__EXPLORER_PUBLIC_BASE__").join(getExplorerPublicBase())
      // Gates the client's ?demo=1 Max-balance override to staging only.
      .split("__ECHO_IS_STAGING__").join(IS_STAGING ? "true" : "false");
    _indexHtmlVersion = version;
  }
  return _indexHtmlCache;
}

app.get("*", (_req, res) => {
  // HTML is the entry point. We never want a stale copy: it carries the
  // ?v=BUILD_VERSION cache-busters for the bridge scripts, so an old
  // cached HTML loading a new bridge (or vice-versa) is a real bug.
  res.set("Cache-Control", "no-cache, no-store, must-revalidate");
  res.set("Pragma", "no-cache");
  res.set("Expires", "0");
  res.set("X-App-Version", getBuildVersion());
  res.set("Content-Type", "text/html; charset=utf-8");
  res.send(renderIndexHtml());
});

// ── Start ────────────────────────────────────────────────────────────────────
app.listen(PORT, "0.0.0.0", () => {
  console.log(`\nEcho server running at http://localhost:${PORT}`);
  console.log(`  App pubkey:    ${ECHO_APP_PUBKEY.slice(0, 24)}…`);
  console.log(`  Node RPC:      ${NODE_RPC_URL}`);
  console.log(`  Mode:          ${LOCAL_DEV ? "LOCAL DEV (mock API)" : "production (chain pollers running, public access)"}`);
  console.log(`  Echo signing:  ${ECHO_APP_SECRET_KEY ? "enabled" : "DISABLED (no ECHO_APP_SECRET_KEY)"}\n`);
});
