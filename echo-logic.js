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
          // Redirects on POST are almost always a misconfiguration (e.g.
          // NODE_RPC_URL points at http:// when the upstream is behind an
          // HTTPS reverse proxy). Surface the Location header + request URL
          // so it's immediately diagnosable instead of an empty `HTTP 302:`.
          if (res.statusCode >= 300 && res.statusCode < 400) {
            const loc = res.headers && res.headers.location;
            reject(new Error(
              `HTTP ${res.statusCode} from ${method} ${urlStr}` +
              (loc ? ` → Location: ${loc}` : " (no Location header)")
            ));
            return;
          }
          reject(new Error(`HTTP ${res.statusCode} from ${method} ${urlStr}: ${text.slice(0, 300)}`));
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

  // Retry policy for transient send failures (no UTXOs for owner, sidecar not
  // ready, "wallet send already pending"). Exponential backoff capped at 5 min,
  // gives up after MAX_ATTEMPTS or RETRY_TTL_MS — whichever comes first.
  // Designed for the partial-ledger sidecar mode where /wallet/send transiently
  // returns "no UTXOs for owner" while the wallet overlay catches up.
  const RETRY_BASE_MS = 30 * 1000;
  const RETRY_MAX_DELAY_MS = 5 * 60 * 1000;
  const RETRY_MAX_ATTEMPTS = 30;
  const RETRY_TTL_MS = 60 * 60 * 1000;
  // `no eligible base-currency UTXOs found for owner` and `requires a single
  // base-currency UTXO` are emitted by the wallet's single-input selector
  // (crates/node/src/rpc/rpcs/wallet_tx.rs) and are typically transient races
  // between the recent-tx SSE/ring fire and the wallet UTXO-DB applying the
  // block that funds the next echo. Treat them as transient so the standard
  // backoff/retry covers the race; if the wallet is genuinely depleted the
  // retry loop still gives up at MAX_ATTEMPTS / RETRY_TTL_MS.
  const TRANSIENT_RE = /no UTXOs for owner|no eligible base-currency UTXOs found for owner|requires a single base-currency UTXO|wallet send already pending|sidecar not ready|signer not configured|tracked owner not registered|queued tx not included|ECONNREFUSED|ECONNRESET|ETIMEDOUT|HTTP 5\d\d/i;

  // After /wallet/send returns queued:true with a tx_id, the sidecar can still
  // drop the tx silently — we've seen it disappear from mempool without ever
  // making it into a block (likely orphaned during a short fork). Without
  // active recovery the event sits at status="echoing" forever and the only
  // unblock is a container restart. The watchdog scheduleInclusionCheck() polls
  // /blockchain/tx/<id> + /mempool after a grace period and requeues via
  // scheduleRetry if both report it absent. 90s easily clears typical
  // 5–15s testnet inclusion latency.
  const INCLUSION_CHECK_MS = 90 * 1000;

  // requestTxId → event row
  const events = new Map();
  // Deduplication for chain poller
  const seenTxIds = new Set();
  // Outstanding echoes (avoid double-send if poller sees same tx twice in flight)
  const inFlight = new Set();
  // User-tx-ids we've already echoed (populated from chain history). Lets the
  // backfill replay short-circuit when we see a request whose response is
  // already on chain — without this every restart re-queues 100s of duplicate
  // echoes, and any transient sidecar failure (e.g. partial-ledger "no UTXOs
  // for owner") cascades into a flood of dead "failed" events.
  const respondedRefs = new Set();
  // Per-tx retry bookkeeping for transient sidecar failures.
  // requestTxId → { attempts, timer }
  const retryState = new Map();
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
    const dropped = sorted.slice(Math.min(MAX_EVENTS, sorted.length));
    events.clear();
    for (let i = 0; i < Math.min(MAX_EVENTS, sorted.length); i++) {
      events.set(sorted[i][0], sorted[i][1]);
    }
    // Cancel retry timers for events that just got evicted so they don't fire
    // against a missing event row.
    for (const [id, state] of retryState.entries()) {
      if (!events.has(id)) {
        if (state.timer) clearTimeout(state.timer);
        retryState.delete(id);
      }
    }
    // Same for inclusion-watchdog timers attached to the dropped events.
    for (const [, ev] of dropped) {
      if (ev && ev._inclusionTimer) {
        clearTimeout(ev._inclusionTimer);
        ev._inclusionTimer = null;
      }
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

  // Snapshot of in-flight echoes for /__usernode/status. Only the rows that
  // haven't been confirmed yet — confirmed/skipped echoes belong in the
  // recent-tx feed, not the pending queue. Shape matches the
  // `registerPending(name, fn)` contract in createDappServerStatus:
  //   [{ id, kind, fromOrTo, amount, status, ageMs, error?, note? }, ...]
  function getPending() {
    const now = Date.now();
    const out = [];
    for (const e of events.values()) {
      if (e.status === "confirmed" || e.status === "skipped") continue;
      const noteParts = [];
      if (e.echoTxId) noteParts.push("echo tx " + String(e.echoTxId).slice(0, 12) + "…");
      if (e.retryAttempts) noteParts.push(`retry ${e.retryAttempts}/${RETRY_MAX_ATTEMPTS}`);
      out.push({
        id: e.requestTxId,
        kind: "echo",
        fromOrTo: e.requestFrom,
        amount: e.echoAmount != null ? e.echoAmount : e.requestAmount,
        status: e.status,
        ageMs: e.requestSeenAtServerMs ? now - e.requestSeenAtServerMs : null,
        error: e.error || null,
        note: noteParts.length ? noteParts.join(" · ") : null,
      });
    }
    out.sort((a, b) => (b.ageMs || 0) - (a.ageMs || 0));
    return out;
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
      retryAttempts: 0,
    };
    events.set(tx.id, event);
    trimEvents();

    if (!Number.isFinite(tx.amount) || tx.amount < 2) {
      event.error = "amount must be ≥ 2 (echo returns N-1)";
      event.status = "skipped";
      console.log(`[echo] skip: ${tx.from.slice(0, 16)}… amount=${tx.amount} < 2`);
      return;
    }

    // Already-responded short-circuit. If handleOutgoing has already seen our
    // own echo for this user-tx — typical during a backfill replay — do NOT
    // queue another /wallet/send. Mark the row as confirmed (handleOutgoing
    // already filled in echoTxId/timestamp).
    if (respondedRefs.has(tx.id)) {
      event.echoAmount = Math.max(1, tx.amount - 1);
      if (event.echoConfirmedTs == null) {
        event.status = "confirmed";
      }
      return;
    }

    if (inFlight.has(tx.id)) return;
    inFlight.add(tx.id);

    // Defer to the next tick. During a synchronous backfill loop this lets
    // every later tx — especially our own outgoing echo for this same request
    // — reach handleOutgoing first and populate respondedRefs, so we can skip
    // the wallet send entirely instead of double-echoing on every restart.
    // For live traffic the deferral is one tick of latency; negligible.
    setImmediate(() => {
      // Re-check dedup at fire time. If the matching echo arrived during the
      // deferred microtask window, skip the send.
      if (!events.has(tx.id)) {
        inFlight.delete(tx.id);
        return;
      }
      const ev = events.get(tx.id);
      if (respondedRefs.has(tx.id) || ev.echoConfirmedTs != null) {
        ev.echoAmount = Math.max(1, tx.amount - 1);
        if (ev.echoConfirmedTs == null) ev.status = "confirmed";
        inFlight.delete(tx.id);
        return;
      }
      // Chain off sendChain so concurrent incoming requests serialize at our
      // layer instead of racing the sidecar's per-owner "already pending" guard.
      sendChain = sendChain
        .catch(() => {}) // never let one failure poison the chain
        .then(() => sendEchoFor(tx, ev))
        .finally(() => inFlight.delete(tx.id));
    });
  }

  function handleOutgoing(tx) {
    const memo = parseMemo(tx.memo);
    const ref = memo && memo.app === APP_ID && memo.type === "echo" ? memo.ref : null;

    // Track every on-chain echo so the dedup short-circuit in handleIncoming
    // can recognise already-responded user txs and skip re-sending them.
    if (ref) respondedRefs.add(ref);

    // First try matching by tx_id (the sidecar returned an id when we called /wallet/send)
    for (const event of events.values()) {
      if (event.echoTxId && event.echoTxId === tx.id) {
        if (event.echoConfirmedTs == null) {
          event.echoConfirmedTs = tx.ts;
          event.echoConfirmedAtServerMs = Date.now();
          event.status = "confirmed";
          if (event._inclusionTimer) {
            clearTimeout(event._inclusionTimer);
            event._inclusionTimer = null;
          }
          console.log(`[echo] confirmed (id-match): req=${event.requestTxId.slice(0, 12)}… echo=${tx.id.slice(0, 12)}…`);
        }
        return;
      }
    }
    // Fallback: match outgoing to event by the user-tx-id in the memo's `ref`.
    if (ref) {
      const event = events.get(ref);
      if (event && event.echoConfirmedTs == null) {
        event.echoConfirmedTs = tx.ts;
        event.echoConfirmedAtServerMs = Date.now();
        if (!event.echoTxId) event.echoTxId = tx.id;
        event.status = "confirmed";
        // Cancel any retry / inclusion watchdog that might still fire.
        const state = retryState.get(ref);
        if (state && state.timer) {
          clearTimeout(state.timer);
          retryState.delete(ref);
        }
        if (event._inclusionTimer) {
          clearTimeout(event._inclusionTimer);
          event._inclusionTimer = null;
        }
        console.log(`[echo] confirmed (memo-match): req=${ref.slice(0, 12)}… echo=${tx.id.slice(0, 12)}…`);
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

  // Watchdog: after the sidecar accepts /wallet/send and gives us a tx_id,
  // verify the tx actually lands. If it's neither on chain nor in mempool
  // after the grace period, requeue via scheduleRetry. Re-arms itself while
  // the tx is still in mempool (no decision yet) or while the sidecar is
  // unreachable (don't requeue blindly).
  function scheduleInclusionCheck(tx, event) {
    if (event._inclusionTimer) clearTimeout(event._inclusionTimer);
    const t = setTimeout(async () => {
      event._inclusionTimer = null;
      if (event.echoConfirmedTs != null) return;
      if (respondedRefs.has(tx.id)) return;
      if (event.status !== "echoing" || !event.echoTxId) return;
      if (!events.has(tx.id)) return;

      const echoTxId = event.echoTxId;
      let included = false;
      try {
        const r = await httpJson("GET", `${nodeRpcUrl}/blockchain/tx/${encodeURIComponent(echoTxId)}`);
        included = !!(r && r.included);
      } catch (_) {
        scheduleInclusionCheck(tx, event);
        return;
      }
      if (included) {
        // Chain poller will catch up via handleOutgoing.
        return;
      }

      let inMempool = false;
      try {
        const r = await httpJson("GET", `${nodeRpcUrl}/mempool`);
        const entries = (r && r.entries) || [];
        inMempool = entries.some(function (e) {
          if (!e) return false;
          const id = e.id || e.tx_id || (e.tx && (e.tx.id || e.tx.tx_id));
          return id === echoTxId;
        });
      } catch (_) {
        scheduleInclusionCheck(tx, event);
        return;
      }
      if (inMempool) {
        scheduleInclusionCheck(tx, event);
        return;
      }

      const ageS = Math.round((Date.now() - (event.echoSentAtServerMs || event.requestSeenAtServerMs)) / 1000);
      console.log(
        `[echo] echo ${String(echoTxId).slice(0, 12)}… missing on chain & mempool after ${ageS}s — requeueing for req=${tx.id.slice(0, 12)}…`
      );
      event.echoTxId = null;
      scheduleRetry(tx, event, "queued tx not included");
    }, INCLUSION_CHECK_MS);
    if (typeof t.unref === "function") t.unref();
    event._inclusionTimer = t;
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

    // Late dedup against historical responses (e.g. the matching echo became
    // visible while we were waiting in the sendChain queue).
    if (respondedRefs.has(tx.id)) {
      event.echoAmount = replyAmount;
      if (event.echoConfirmedTs == null) event.status = "confirmed";
      const state = retryState.get(tx.id);
      if (state && state.timer) clearTimeout(state.timer);
      retryState.delete(tx.id);
      if (event._inclusionTimer) {
        clearTimeout(event._inclusionTimer);
        event._inclusionTimer = null;
      }
      return;
    }

    const ready = await ensureReady();
    if (!ready) {
      scheduleRetry(tx, event, "sidecar not ready (signer/tracked_owner)");
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
        event.error = null;
        const state = retryState.get(tx.id);
        if (state && state.timer) clearTimeout(state.timer);
        retryState.delete(tx.id);
        console.log(`[echo] queued ${replyAmount} → ${tx.from.slice(0, 16)}… (req=${tx.id.slice(0, 12)}…, rpc=${sendDurationMs}ms)`);
        // The sidecar can still drop this tx silently before inclusion. Arm
        // a watchdog that requeues if the tx_id never makes it on-chain.
        if (event.echoTxId) scheduleInclusionCheck(tx, event);
      } else {
        const errMsg = (resp && resp.error) || "send not queued";
        if (TRANSIENT_RE.test(errMsg)) {
          scheduleRetry(tx, event, errMsg);
        } else {
          event.error = errMsg;
          event.status = "failed";
          console.error("[echo] send rejected:", resp);
        }
      }
    } catch (e) {
      if (TRANSIENT_RE.test(e.message || "")) {
        scheduleRetry(tx, event, e.message);
      } else {
        event.error = e.message;
        event.status = "failed";
        console.error("[echo] send error:", e.message);
      }
    }
  }

  function scheduleRetry(tx, event, errMsg) {
    // If the matching echo landed since we last checked, abandon the retry.
    if (respondedRefs.has(tx.id)) {
      event.echoAmount = Math.max(1, tx.amount - 1);
      if (event.echoConfirmedTs == null) event.status = "confirmed";
      event.error = null;
      const state = retryState.get(tx.id);
      if (state && state.timer) clearTimeout(state.timer);
      retryState.delete(tx.id);
      if (event._inclusionTimer) {
        clearTimeout(event._inclusionTimer);
        event._inclusionTimer = null;
      }
      return;
    }
    // Cap retries by attempt count and overall age so failures don't loop forever.
    const ageMs = Date.now() - event.requestSeenAtServerMs;
    const state = retryState.get(tx.id) || { attempts: 0, timer: null };
    if (state.attempts >= RETRY_MAX_ATTEMPTS || ageMs >= RETRY_TTL_MS) {
      event.error = `${errMsg} (gave up after ${state.attempts} retries, ${Math.round(ageMs / 1000)}s)`;
      event.status = "failed";
      retryState.delete(tx.id);
      console.error(`[echo] giving up on ${tx.id.slice(0, 12)}… after ${state.attempts} retries`);
      return;
    }
    state.attempts++;
    const delayMs = Math.min(
      RETRY_MAX_DELAY_MS,
      RETRY_BASE_MS * Math.pow(1.5, state.attempts - 1),
    );
    event.status = "pending";
    event.retryAttempts = state.attempts;
    event.error = `${errMsg} — retry ${state.attempts}/${RETRY_MAX_ATTEMPTS} in ${Math.round(delayMs / 1000)}s`;
    if (state.timer) clearTimeout(state.timer);
    state.timer = setTimeout(() => {
      state.timer = null;
      // Skip if event was evicted (trim) or already confirmed by handleOutgoing.
      if (!events.has(tx.id)) {
        retryState.delete(tx.id);
        return;
      }
      const ev = events.get(tx.id);
      if (ev.status === "confirmed" || respondedRefs.has(tx.id)) {
        retryState.delete(tx.id);
        return;
      }
      sendChain = sendChain
        .catch(() => {})
        .then(() => sendEchoFor(tx, ev));
    }, delayMs);
    if (typeof state.timer.unref === "function") state.timer.unref();
    retryState.set(tx.id, state);
    console.log(`[echo] retry ${state.attempts}/${RETRY_MAX_ATTEMPTS} in ${Math.round(delayMs / 1000)}s for ${tx.id.slice(0, 12)}…: ${errMsg}`);
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
    // Chain plumbing (live polling, backfill, mock-drain) is owned by the
    // surrounding createAppStateCache wiring in server.js. We only do
    // app-specific tasks here.
    if (!localDev) {
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
    for (const ev of events.values()) {
      if (ev && ev._inclusionTimer) {
        clearTimeout(ev._inclusionTimer);
        ev._inclusionTimer = null;
      }
    }
    events.clear();
    respondedRefs.clear();
    for (const state of retryState.values()) {
      if (state.timer) clearTimeout(state.timer);
    }
    retryState.clear();
    signerConfigured = false;
    trackedOwnerAdded = false;
    console.log("[echo] state reset (chain restart detected)");
  }

  return {
    processTransaction,
    handleRequest,
    getStateResponse,
    getPending,
    start,
    reset,
    appPubkey,
  };
}

module.exports = createEcho;
