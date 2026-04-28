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
 *   node server.js              — production mode (auth-enforced, real chain)
 *   node server.js --local-dev  — local dev (no auth, mock transaction store)
 *
 * Env vars:
 *   PORT                — HTTP port (default 3000 — matches platform scaffold)
 *   JWT_SECRET          — shared with the social-vibecoding platform
 *   ECHO_APP_PUBKEY     — echo dapp address (required for chain mode)
 *   ECHO_APP_SECRET_KEY — secret key for outgoing /wallet/send (required for chain mode)
 *   NODE_RPC_URL        — sidecar URL (default http://usernode-node:3000 inside compose)
 */

const http = require("http");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const express = require("express");
const jwt = require("jsonwebtoken");

const {
  loadEnvFile,
  handleExplorerProxy,
  createMockApi,
  createChainPoller,
} = require("./lib/dapp-server");
const createEcho = require("./echo-logic");

loadEnvFile();

// ── CLI flags ────────────────────────────────────────────────────────────────
const LOCAL_DEV = process.argv.includes("--local-dev");
const PORT = parseInt(process.env.PORT, 10) || 3000;

// ── Echo config ──────────────────────────────────────────────────────────────
const ECHO_APP_PUBKEY = process.env.ECHO_APP_PUBKEY || "ut1_echo_default_pubkey";
const ECHO_APP_SECRET_KEY = process.env.ECHO_APP_SECRET_KEY || "";
const NODE_RPC_URL = process.env.NODE_RPC_URL || "http://usernode-node:3000";
const JWT_SECRET = process.env.JWT_SECRET || "";

// ── Express app ──────────────────────────────────────────────────────────────
const app = express();

// One hop (Caddy) in front of us.
app.set("trust proxy", 1);

// Health check (always public — used by Docker healthcheck and platform polling).
app.get("/health", (_req, res) => res.json({ status: "ok" }));

// ── Auth middleware ──────────────────────────────────────────────────────────
// Mirrors the social-vibecoding scaffold (see src/prompts/app-conventions.md):
//   * GET non-/api requests pass through (HTML shell + static assets).
//   * Non-GET and /api/* requests require a verified platform JWT.
//   * /explorer-api/* is a transparent proxy to the public block explorer
//     — gating it accomplishes nothing (anyone can hit the upstream
//     directly) and breaks the bridge's POST /<chain_id>/transactions
//     polling from inside the iframe (which has no token to forward).
// In --local-dev we skip the gate entirely so the mock flow works without the
// platform ever issuing a token.
const PUBLIC_API_PATHS = new Set(["/health"]);
const PUBLIC_PREFIXES = ["/explorer-api/"];
app.use((req, res, next) => {
  if (LOCAL_DEV) return next();
  const token = req.query.token || req.headers["x-usernode-token"];
  if (token && JWT_SECRET) {
    try { req.user = jwt.verify(token, JWT_SECRET); } catch { /* fall through */ }
  }
  if (req.method !== "GET" || req.path.startsWith("/api/")) {
    if (PUBLIC_API_PATHS.has(req.path)) return next();
    if (PUBLIC_PREFIXES.some((p) => req.path.startsWith(p))) return next();
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });
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
});
echo.start();

app.use((req, res, next) => {
  if (echo.handleRequest(req, res, req.path)) return;
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
// Auth-gated when running in production. Matches the template.js pattern of
// returning a small "Open in Usernode" landing page so direct visits don't
// reveal the dapp UI before auth.
const PLATFORM_DOMAIN = process.env.USERNODE_DOMAIN || "usernode.evanshapiro.dev";
const LANDING_HTML = `<!doctype html><meta charset=utf-8><title>Open in Usernode</title>
<body style="font-family:system-ui;background:#09090b;color:#e4e4e7;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0">
  <div style="max-width:24rem;padding:2rem;text-align:center">
    <h1 style="font-size:1.25rem;margin:0 0 0.5rem">Open this app inside Usernode</h1>
    <p style="color:#a1a1aa;font-size:0.9rem;margin:0 0 1.25rem">This page is served via the platform; direct visits aren't authenticated.</p>
    <a href="https://${PLATFORM_DOMAIN}" style="display:inline-block;padding:0.5rem 1rem;background:#7c3aed;color:white;border-radius:0.5rem;text-decoration:none;font-size:0.9rem">Go to Usernode</a>
  </div>
</body>`;

// Render the index.html template with __BUILD_VERSION__ substituted. Cached
// in production (file set is frozen) and re-rendered on each request in
// --local-dev so edits show up without a server restart.
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
    _indexHtmlCache = raw.split("__BUILD_VERSION__").join(version);
    _indexHtmlVersion = version;
  }
  return _indexHtmlCache;
}

app.get("*", (req, res) => {
  if (!LOCAL_DEV && !req.user) return res.status(401).send(LANDING_HTML);
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

// ── Chain pollers (production only) ──────────────────────────────────────────
// Two pollers are required: the recipient one feeds incoming user→echo sends
// into echo.processTransaction (which then triggers /wallet/send), and the
// sender one catches the outgoing echo landing on chain so the round-trip
// can be timestamped. Both share echo.processTransaction; deduplication is
// handled inside echo-logic.
function resetEcho(newId, oldId) {
  console.log(`[echo] chain reset ${oldId} -> ${newId}, resetting state`);
  echo.reset();
}
if (!LOCAL_DEV) {
  createChainPoller({
    appPubkey: ECHO_APP_PUBKEY,
    queryField: "recipient",
    onTransaction: echo.processTransaction,
    onChainReset: resetEcho,
  }).start();
  createChainPoller({
    appPubkey: ECHO_APP_PUBKEY,
    queryField: "sender",
    onTransaction: echo.processTransaction,
    onChainReset: resetEcho,
  }).start();
}

// ── Start ────────────────────────────────────────────────────────────────────
app.listen(PORT, "0.0.0.0", () => {
  console.log(`\nEcho server running at http://localhost:${PORT}`);
  console.log(`  App pubkey:    ${ECHO_APP_PUBKEY.slice(0, 24)}…`);
  console.log(`  Node RPC:      ${NODE_RPC_URL}`);
  console.log(`  Mode:          ${LOCAL_DEV ? "LOCAL DEV (auth bypassed, mock API)" : "production (JWT enforced, chain pollers running)"}`);
  console.log(`  Echo signing:  ${ECHO_APP_SECRET_KEY ? "enabled" : "DISABLED (no ECHO_APP_SECRET_KEY)"}\n`);
});
