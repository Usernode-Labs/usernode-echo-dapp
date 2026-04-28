/**
 * Echo — server-side logic for the latency-test dapp.
 *
 * Behavior:
 *   1. User sends N tokens to ECHO_APP_PUBKEY with memo {"app":"echo","type":"send"}
 *   2. Server (this module) detects the tx via the chain poller (or mock drain
 *      in --local-dev), then submits N-1 back to the sender via the sidecar
 *      `/wallet/send` RPC.
 *   3. A second chain poller (sender = ECHO_APP_PUBKEY) catches the echo
 *      reaching the chain and records the confirmation time.
 *
 * The /__echo/state endpoint exposes a per-event timing breakdown so the
 * client can render send/echo/total latencies.
 *
 * Memo schema:
 *   user → echo: {"app":"echo","type":"send"}
 *   echo → user: {"app":"echo","type":"echo","ref":<requestTxId>}
 */

const http = require("http");
const https = require("https");

const APP_ID = "echo";

function parseMemo(m) {
  if (m == null) return null;
  const s = String(m).trim();
  if (!s) return null;
  // Direct JSON (mock store, or an explorer that decodes UTF-8 memos)
  try { return JSON.parse(s); } catch (_) {}
  // Fallback: base64url-encoded JSON (some explorers return raw memo bytes)
  try {
    const decoded = Buffer.from(s, "base64url").toString("utf8");
    return JSON.parse(decoded);
  } catch (_) {}
  return null;
}

function extractTimestamp(tx) {
  const candidates = [tx.timestamp_ms, tx.created_at, tx.createdAt, tx.timestamp, tx.time];
  for (const v of candidates) {
    if (typeof v === "number" && Number.isFinite(v))
      return v < 10_000_000_000 ? v * 1000 : v;
    if (typeof v === "string" && v.trim()) {
      const t = Date.parse(v);
      if (!Number.isNaN(t)) return t;
    }
  }
  return null;
}

function normalizeTx(tx) {
  if (!tx || typeof tx !== "object") return null;
  return {
    id: tx.tx_id || tx.id || tx.txid || tx.hash || null,
    from: tx.from_pubkey || tx.from || tx.source || null,
    to: tx.destination_pubkey || tx.to || tx.destination || null,
    amount: tx.amount != null ? Number(tx.amount) : 0,
    memo: tx.memo != null ? String(tx.memo) : null,
    ts: extractTimestamp(tx) || Date.now(),
  };
}

function httpJson(method, urlStr, body) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const transport = url.protocol === "https:" ? https : http;
    const bodyBuf = body ? Buffer.from(JSON.stringify(body)) : null;
    const req = transport.request(url, {
      method,
      headers: {
        "content-type": "application/json",
        accept: "application/json",
        ...(bodyBuf ? { "content-length": bodyBuf.length } : {}),
      },
    }, (res) => {
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        const text = Buffer.concat(chunks).toString();
        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`HTTP ${res.statusCode}: ${text.slice(0, 300)}`));
          return;
        }
        try { resolve(JSON.parse(text)); }
        catch (e) { reject(new Error(`JSON parse: ${e.message}`)); }
      });
    });
    req.on("error", reject);
    if (bodyBuf) req.write(bodyBuf);
    req.end();
  });
}

function createEcho(opts) {
  const appPubkey = opts.appPubkey || "ut1_echo_default_pubkey";
  const appSecretKey = opts.appSecretKey || "";
  const nodeRpcUrl = opts.nodeRpcUrl || "http://localhost:3000";
  const localDev = !!opts.localDev;
  const mockTransactions = opts.mockTransactions || null;

  const MAX_EVENTS = 200;
  const MAX_SEEN_TX_IDS = 5000;

  // requestTxId → event row
  const events = new Map();
  // Deduplication for chain poller
  const seenTxIds = new Set();
  // Outstanding echoes (avoid double-send if poller sees same tx twice in flight)
  const inFlight = new Set();
  // Serializes /wallet/send calls. The sidecar enforces "one wallet send pending
  // per owner", so concurrent sends from the same address get rejected with
  // "wallet send already pending for owner …". We chain them off this promise so
  // a startup burst from the chain poller drains one-at-a-time instead of racing.
  let sendChain = Promise.resolve();
  let signerConfigured = false;
  let trackedOwnerAdded = false;

  function trimSeenTxIds() {
    if (seenTxIds.size <= MAX_SEEN_TX_IDS) return;
    const drop = seenTxIds.size - Math.floor(MAX_SEEN_TX_IDS / 2);
    let i = 0;
    for (const id of seenTxIds) {
      if (i++ >= drop) break;
      seenTxIds.delete(id);
    }
  }

  function trimEvents() {
    if (events.size <= MAX_EVENTS) return;
    const sorted = Array.from(events.entries()).sort((a, b) => b[1].requestTs - a[1].requestTs);
    events.clear();
    for (let i = 0; i < Math.min(MAX_EVENTS, sorted.length); i++) {
      events.set(sorted[i][0], sorted[i][1]);
    }
  }

  function getStateResponse() {
    const list = Array.from(events.values()).sort((a, b) => b.requestTs - a.requestTs);
    return {
      appPubkey,
      events: list.slice(0, 50),
      eventCount: events.size,
      mode: localDev ? "mock" : "chain",
    };
  }

  // ── Chain poller entrypoint ──────────────────────────────────────────────

  function processTransaction(rawTx) {
    const tx = normalizeTx(rawTx);
    if (!tx || !tx.id || !tx.from || !tx.to) return;
    if (seenTxIds.has(tx.id)) return;
    seenTxIds.add(tx.id);
    trimSeenTxIds();

    if (tx.from === appPubkey) {
      handleOutgoing(tx);
      return;
    }
    if (tx.to === appPubkey) {
      const memo = parseMemo(tx.memo);
      if (!memo || memo.app !== APP_ID || memo.type !== "send") return;
      handleIncoming(tx);
    }
  }

  function handleIncoming(tx) {
    if (events.has(tx.id)) return;

    const event = {
      requestTxId: tx.id,
      requestFrom: tx.from,
      requestAmount: tx.amount,
      requestTs: tx.ts,
      requestSeenAtServerMs: Date.now(),
      echoAmount: null,
      echoSentAtServerMs: null,
      echoTxId: null,
      echoConfirmedTs: null,
      echoConfirmedAtServerMs: null,
      error: null,
      status: "pending",
    };
    events.set(tx.id, event);
    trimEvents();

    if (!Number.isFinite(tx.amount) || tx.amount < 2) {
      event.error = "amount must be ≥ 2 (echo returns N-1)";
      event.status = "skipped";
      console.log(`[echo] skip: ${tx.from.slice(0, 16)}… amount=${tx.amount} < 2`);
      return;
    }

    if (inFlight.has(tx.id)) return;
    inFlight.add(tx.id);

    // Chain off sendChain so concurrent incoming requests serialize at our layer
    // instead of racing the sidecar's per-owner "already pending" guard.
    sendChain = sendChain
      .catch(() => {}) // never let one failure poison the chain
      .then(() => sendEchoFor(tx, event))
      .finally(() => inFlight.delete(tx.id));
  }

  function handleOutgoing(tx) {
    // First try matching by tx_id (the sidecar returned an id when we called /wallet/send)
    for (const event of events.values()) {
      if (event.echoTxId && event.echoTxId === tx.id) {
        if (event.echoConfirmedTs == null) {
          event.echoConfirmedTs = tx.ts;
          event.echoConfirmedAtServerMs = Date.now();
          event.status = "confirmed";
          console.log(`[echo] confirmed (id-match): req=${event.requestTxId.slice(0, 12)}… echo=${tx.id.slice(0, 12)}…`);
        }
        return;
      }
    }
    // Fallback: parse memo for ref tx id
    const memo = parseMemo(tx.memo);
    if (memo && memo.app === APP_ID && memo.type === "echo" && memo.ref) {
      const event = events.get(memo.ref);
      if (event && event.echoConfirmedTs == null) {
        event.echoConfirmedTs = tx.ts;
        event.echoConfirmedAtServerMs = Date.now();
        if (!event.echoTxId) event.echoTxId = tx.id;
        event.status = "confirmed";
        console.log(`[echo] confirmed (memo-match): req=${memo.ref.slice(0, 12)}… echo=${tx.id.slice(0, 12)}…`);
      }
    }
  }

  // ── Sidecar interactions ─────────────────────────────────────────────────

  async function ensureReady() {
    if (!appSecretKey) return false;
    try {
      if (!trackedOwnerAdded) {
        // Idempotent on the node side; ignore "already tracked" errors.
        try {
          await httpJson("POST", `${nodeRpcUrl}/wallet/tracked_owner/add`, { owner: appPubkey });
        } catch (e) {
          if (!/already/i.test(e.message)) throw e;
        }
        trackedOwnerAdded = true;
        console.log("[echo] tracked_owner registered");
      }
      if (!signerConfigured) {
        const resp = await httpJson("POST", `${nodeRpcUrl}/wallet/signer`, { secret_key: appSecretKey });
        if (resp && resp.ok) {
          signerConfigured = true;
          console.log("[echo] signer configured");
        } else {
          console.error("[echo] signer config failed:", resp);
          return false;
        }
      }
      return true;
    } catch (e) {
      console.error("[echo] ensureReady error:", e.message);
      return false;
    }
  }

  async function sendEchoFor(tx, event) {
    const replyAmount = Math.max(1, tx.amount - 1);
    event.echoAmount = replyAmount;

    if (localDev && mockTransactions) {
      // In --local-dev there's no chain. Inject the echo directly into the
      // mock store after a small delay so the round-trip is observable.
      await new Promise((r) => setTimeout(r, 200));
      const crypto = require("crypto");
      const echoTxId = crypto.randomUUID();
      const nowIso = new Date().toISOString();
      const echoTx = {
        id: echoTxId,
        from_pubkey: appPubkey,
        destination_pubkey: tx.from,
        amount: replyAmount,
        memo: JSON.stringify({ app: APP_ID, type: "echo", ref: tx.id }),
        created_at: nowIso,
      };
      mockTransactions.push(echoTx);
      event.echoTxId = echoTxId;
      event.echoSentAtServerMs = Date.now();
      event.echoConfirmedTs = Date.parse(nowIso);
      event.echoConfirmedAtServerMs = Date.now();
      event.status = "confirmed";
      console.log(`[echo] mock echo: ${replyAmount} → ${tx.from.slice(0, 16)}…`);
      return;
    }

    const ready = await ensureReady();
    if (!ready) {
      event.error = "sidecar not ready (signer/tracked_owner)";
      event.status = "failed";
      return;
    }

    const memoB64 = Buffer.from(JSON.stringify({
      app: APP_ID,
      type: "echo",
      ref: tx.id,
    })).toString("base64url");

    try {
      const t0 = Date.now();
      const resp = await httpJson("POST", `${nodeRpcUrl}/wallet/send`, {
        from_pk_hash: appPubkey,
        amount: replyAmount,
        to_pk_hash: tx.from,
        fee: 0,
        memo: memoB64,
      });
      event.echoSentAtServerMs = Date.now();
      const sendDurationMs = event.echoSentAtServerMs - t0;
      if (resp && resp.queued) {
        event.echoTxId = resp.tx_id || resp.txid || resp.hash || null;
        event.status = "echoing";
        console.log(`[echo] queued ${replyAmount} → ${tx.from.slice(0, 16)}… (req=${tx.id.slice(0, 12)}…, rpc=${sendDurationMs}ms)`);
      } else {
        event.error = (resp && resp.error) || "send not queued";
        event.status = "failed";
        console.error("[echo] send rejected:", resp);
      }
    } catch (e) {
      event.error = e.message;
      event.status = "failed";
      console.error("[echo] send error:", e.message);
    }
  }

  // ── HTTP handler ─────────────────────────────────────────────────────────

  function handleRequest(req, res, pathname) {
    if (pathname === "/__echo/state" && (req.method === "GET" || req.method === "HEAD")) {
      const body = JSON.stringify(getStateResponse());
      const headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
        "Access-Control-Allow-Origin": "*",
      };
      if (req.method === "HEAD") {
        res.writeHead(200, { ...headers, "content-length": Buffer.byteLength(body) });
        res.end();
        return true;
      }
      res.writeHead(200, headers);
      res.end(body);
      return true;
    }
    return false;
  }

  // ── Lifecycle ────────────────────────────────────────────────────────────

  function start() {
    if (localDev && mockTransactions) {
      let idx = 0;
      setInterval(() => {
        while (idx < mockTransactions.length) {
          processTransaction(mockTransactions[idx]);
          idx++;
        }
      }, 1000);
      console.log("[echo] mock drain loop started (--local-dev)");
    } else {
      // Pre-warm sidecar registration so the first echo is fast. Retry quietly
      // in the background — the sidecar may need time to come online.
      let attempts = 0;
      (function tryReady() {
        ensureReady().then((ok) => {
          if (!ok && attempts++ < 30) setTimeout(tryReady, 5000);
        });
      })();
    }
  }

  function reset() {
    seenTxIds.clear();
    inFlight.clear();
    events.clear();
    signerConfigured = false;
    trackedOwnerAdded = false;
    console.log("[echo] state reset (chain restart detected)");
  }

  return {
    processTransaction,
    handleRequest,
    getStateResponse,
    start,
    reset,
    appPubkey,
  };
}

module.exports = createEcho;
