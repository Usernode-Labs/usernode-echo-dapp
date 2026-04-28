/**
 * Shared server utilities for Usernode dapps.
 *
 * Provides: JSON body parsing, HTTPS fetch, explorer proxy, mock transaction
 * API, chain poller, and path resolution. Used by both the combined examples
 * server and standalone sub-app servers.
 */

const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

// ── .env loader ─────────────────────────────────────────────────────────────
// Loads KEY=VALUE pairs from a .env file into process.env (does not overwrite
// existing env vars). Zero dependencies.

function loadEnvFile(filePath) {
  if (!filePath) {
    const candidates = [
      path.resolve(process.cwd(), ".env"),
      path.resolve(__dirname, "..", "..", ".env"),
    ];
    filePath = candidates.find((p) => fs.existsSync(p));
  }
  if (!filePath || !fs.existsSync(filePath)) return;
  const lines = fs.readFileSync(filePath, "utf8").split("\n");
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const m = trimmed.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)/);
    if (!m) continue;
    const key = m[1];
    let val = m[2];
    if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
      val = val.slice(1, -1);
    }
    if (process.env[key] == null) process.env[key] = val;
  }
}

function getExplorerUpstream() {
  return process.env.EXPLORER_UPSTREAM || "alpha1.usernodelabs.org";
}
function getExplorerUpstreamBase() {
  return process.env.EXPLORER_UPSTREAM_BASE != null
    ? process.env.EXPLORER_UPSTREAM_BASE
    : "/api";
}

// ── JSON body parser ─────────────────────────────────────────────────────────

function readJson(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => {
      data += chunk;
      if (data.length > 1_000_000) { reject(new Error("Body too large")); req.destroy(); }
    });
    req.on("end", () => {
      if (!data) return resolve({});
      try { resolve(JSON.parse(data)); } catch (e) { reject(e); }
    });
  });
}

// ── Protocol helper ──────────────────────────────────────────────────────────

function explorerProto(host) {
  return /^(localhost|127\.|192\.|10\.|172\.)/.test(host) ? "http" : "https";
}

function explorerTransport(host) {
  return explorerProto(host) === "https" ? https : http;
}

// ── JSON requester ───────────────────────────────────────────────────────────

function httpsJson(method, urlStr, body) {
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
          reject(new Error(`HTTP ${res.statusCode}: ${text.slice(0, 200)}`));
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

// ── Explorer API proxy ───────────────────────────────────────────────────────
//
// Returns true if the request was handled (pathname starts with /explorer-api/).

function handleExplorerProxy(req, res, pathname, opts) {
  const upstream = (opts && opts.upstream) || getExplorerUpstream();
  const upstreamBase = (opts && opts.upstreamBase) || getExplorerUpstreamBase();
  const prefix = "/explorer-api/";

  if (!pathname.startsWith(prefix)) return false;

  const upstreamPath = upstreamBase + "/" + pathname.slice(prefix.length);
  const proto = explorerProto(upstream);
  const upstreamUrl = new URL(`${proto}://${upstream}${upstreamPath}`);

  void (async () => {
    try {
      let bodyBuf = null;
      if (req.method === "POST") {
        const chunks = [];
        for await (const chunk of req) {
          chunks.push(chunk);
          if (chunks.reduce((s, c) => s + c.length, 0) > 1_000_000) {
            res.writeHead(413, { "Content-Type": "text/plain" });
            res.end("Body too large");
            return;
          }
        }
        bodyBuf = Buffer.concat(chunks);
      }
      const proxyReq = explorerTransport(upstream).request(upstreamUrl, {
        method: req.method,
        headers: {
          "content-type": req.headers["content-type"] || "application/json",
          accept: "application/json",
          ...(bodyBuf ? { "content-length": bodyBuf.length } : {}),
        },
      }, (proxyRes) => {
        res.writeHead(proxyRes.statusCode || 502, {
          "content-type": proxyRes.headers["content-type"] || "application/json",
          "access-control-allow-origin": "*",
        });
        proxyRes.pipe(res);
      });
      proxyReq.on("error", (err) => {
        console.error(`Explorer proxy error: ${err.message}`);
        res.writeHead(502, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: `Proxy error: ${err.message}` }));
      });
      if (bodyBuf) proxyReq.write(bodyBuf);
      proxyReq.end();
    } catch (err) {
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: `Proxy error: ${err.message}` }));
    }
  })();

  return true;
}

// ── Mock transaction API ─────────────────────────────────────────────────────
//
// Returns { transactions, handleRequest }.
// handleRequest(req, res, pathname) returns true if handled.

function createMockApi(opts) {
  const localDev = (opts && opts.localDev) || false;
  const delayMs = (opts && opts.delayMs) || 5000;
  const delayOverrides = (opts && opts.delayOverrides) || {};
  const transactions = [];

  function handleRequest(req, res, pathname) {
    if (pathname === "/__mock/enabled") {
      if (!localDev) {
        res.writeHead(404); res.end("Not found");
        return true;
      }
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify({ enabled: true }));
      return true;
    }

    if (pathname === "/__mock/sendTransaction" && req.method === "POST") {
      if (!localDev) {
        res.writeHead(404); res.end("Not found (start with --local-dev)");
        return true;
      }
      readJson(req).then((body) => {
        const from_pubkey = String(body.from_pubkey || "").trim();
        const destination_pubkey = String(body.destination_pubkey || "").trim();
        const amount = body.amount;
        const memo = body.memo == null ? undefined : String(body.memo);
        if (!from_pubkey || !destination_pubkey) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "from_pubkey and destination_pubkey required" }));
          return;
        }
        console.log(`[tx] received from=${from_pubkey.slice(0, 16)}… dest=${destination_pubkey.slice(0, 16)}…`);
        const tx = { id: crypto.randomUUID(), from_pubkey, destination_pubkey, amount, memo, created_at: new Date().toISOString() };
        const txDelay = (destination_pubkey in delayOverrides) ? delayOverrides[destination_pubkey] : delayMs;
        setTimeout(() => { transactions.push(tx); }, txDelay);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ queued: true, tx }));
      }).catch((e) => {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: e.message }));
      });
      return true;
    }

    if (pathname === "/__mock/getTransactions" && req.method === "POST") {
      if (!localDev) {
        res.writeHead(404); res.end("Not found (start with --local-dev)");
        return true;
      }
      readJson(req).then((body) => {
        const filterOptions = body.filterOptions || {};
        const owner = String(body.owner_pubkey || filterOptions.account || "").trim();
        const limit = typeof filterOptions.limit === "number" ? filterOptions.limit : 50;
        const cursor = filterOptions.cursor || body.cursor || null;

        const filtered = transactions
          .filter((tx) => !owner || tx.from_pubkey === owner || tx.destination_pubkey === owner);

        // Cursor is a 0-based index into the filtered array (descending).
        // Reverse so newest is first (index 0 = newest).
        const reversed = filtered.slice().reverse();
        let startIdx = 0;
        if (cursor != null) {
          try {
            startIdx = parseInt(Buffer.from(String(cursor), "base64").toString("utf8"), 10);
            if (!Number.isFinite(startIdx) || startIdx < 0) startIdx = 0;
          } catch (_) { startIdx = 0; }
        }

        const page = reversed.slice(startIdx, startIdx + limit);
        const nextIdx = startIdx + limit;
        const hasMore = nextIdx < reversed.length;
        const nextCursor = hasMore
          ? Buffer.from(String(nextIdx)).toString("base64")
          : null;

        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({
          items: page,
          has_more: hasMore,
          next_cursor: nextCursor,
        }));
      }).catch((e) => {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: e.message }));
      });
      return true;
    }

    return false;
  }

  return { transactions, handleRequest };
}

// ── Confirmation status ──────────────────────────────────────────────────────

function isExplorerConfirmed(status) {
  return status === "confirmed" || status === "canonical";
}

// ── Chain poller ─────────────────────────────────────────────────────────────
//
// Polls the explorer API for new transactions and calls onTransaction(tx) for
// each unseen one. Returns { start() }.

function createChainPoller(opts) {
  const appPubkey = opts.appPubkey;
  const onTransaction = opts.onTransaction;
  const onChainReset = opts.onChainReset || null;
  const intervalMs = opts.intervalMs || 3000;
  const upstream = opts.upstream || getExplorerUpstream();
  const upstreamBase = opts.upstreamBase || getExplorerUpstreamBase();
  const queryField = opts.queryField || "account";
  const maxPages = opts.maxPages || 200;
  const seenIdsCap = opts.seenIdsCap || 5000;
  const recheckIntervalPolls = opts.recheckIntervalPolls || 10;
  const skipOrphaned = opts.skipOrphaned !== false;

  let chainId = null;
  const seenTxIds = new Set();
  let lastHeight = (opts.initialLastHeight != null) ? opts.initialLastHeight : null;
  let pollCount = 0;

  async function fetchActiveChainId() {
    const data = await httpsJson("GET", `${explorerProto(upstream)}://${upstream}${upstreamBase}/active_chain`);
    return (data && data.chain_id) ? data.chain_id : null;
  }

  async function discoverChainId() {
    try {
      const id = await fetchActiveChainId();
      if (id) {
        chainId = id;
        console.log(`[chain] discovered chain_id: ${chainId}`);
      }
    } catch (e) {
      console.warn(`[chain] could not discover chain ID: ${e.message}`);
    }
  }

  async function recheckChainId() {
    try {
      const id = await fetchActiveChainId();
      if (id && id !== chainId) {
        const oldId = chainId;
        console.log(`[chain] chain_id changed: ${oldId} -> ${id} — resetting poller state`);
        chainId = id;
        seenTxIds.clear();
        lastHeight = null;
        if (onChainReset) onChainReset(id, oldId);
      }
    } catch (e) {
      // Transient error — keep using current chainId
    }
  }

  function extractTxTimestamp(tx) {
    const candidates = [tx.timestamp_ms, tx.created_at, tx.createdAt, tx.timestamp, tx.time];
    for (const v of candidates) {
      if (typeof v === "number" && Number.isFinite(v))
        return v < 10_000_000_000 ? v * 1000 : v;
      if (typeof v === "string" && v.trim()) {
        const t = Date.parse(v);
        if (!Number.isNaN(t)) return t;
      }
    }
    return 0;
  }

  async function poll() {
    if (!chainId) { await discoverChainId(); if (!chainId) return; }
    else if (pollCount > 0 && pollCount % recheckIntervalPolls === 0) {
      await recheckChainId();
    }

    pollCount++;
    const baseUrl = `${explorerProto(upstream)}://${upstream}${upstreamBase}/${chainId}`;
    const url = `${baseUrl}/transactions`;
    const MAX_PAGES = maxPages;
    let cursor = null, totalItems = 0;
    const newTxs = [];
    const fromHeight = lastHeight;
    let maxHeight = lastHeight;

    try {
      for (let page = 0; page < MAX_PAGES; page++) {
        const body = { [queryField]: appPubkey, limit: 50 };
        if (cursor) body.cursor = cursor;
        if (fromHeight != null) body.from_height = fromHeight;
        const resp = await httpsJson("POST", url, body);

        if (pollCount <= 2 && page === 0) {
          const keys = resp ? Object.keys(resp) : [];
          const firstItem = resp && resp.items && resp.items[0]
            ? JSON.stringify(resp.items[0]).slice(0, 200) : "none";
          console.log(`[chain] poll #${pollCount} keys=[${keys}] first=${firstItem}`);
        }

        const items = Array.isArray(resp) ? resp
          : (resp && Array.isArray(resp.items)) ? resp.items
          : (resp && Array.isArray(resp.transactions)) ? resp.transactions
          : (resp && resp.data && Array.isArray(resp.data.items)) ? resp.data.items
          : [];

        if (items.length === 0) break;
        totalItems += items.length;

        let allSeen = true;
        for (const tx of items) {
          const txId = tx.tx_id || tx.id || tx.txid || tx.hash || tx.tx_hash;
          if (!txId) continue;
          if (seenTxIds.has(txId)) continue;
          if (skipOrphaned && tx.status && !isExplorerConfirmed(tx.status)) continue;
          allSeen = false;
          seenTxIds.add(txId);
          newTxs.push(tx);

          const bh = tx.block_height;
          if (typeof bh === "number" && (maxHeight == null || bh > maxHeight)) {
            maxHeight = bh;
          }
        }

        if (allSeen) break;
        const hasMore = resp && resp.has_more;
        const nextCursor = resp && resp.next_cursor;
        if (!hasMore || !nextCursor) break;
        cursor = nextCursor;
      }

      if (maxHeight != null) lastHeight = maxHeight;

      // Bound seenTxIds to prevent unbounded memory growth.
      if (seenTxIds.size > seenIdsCap) {
        const arr = Array.from(seenTxIds);
        seenTxIds.clear();
        for (let i = arr.length - seenIdsCap; i < arr.length; i++) {
          seenTxIds.add(arr[i]);
        }
      }

      // Process in chronological order so stateful consumers (game logic,
      // vote resolution) see events oldest-first.
      newTxs.sort((a, b) => extractTxTimestamp(a) - extractTxTimestamp(b));
      for (const tx of newTxs) {
        if (onTransaction) onTransaction(tx);
      }

      if (newTxs.length > 0 || pollCount <= 3) {
        console.log(`[chain] poll #${pollCount}: ${totalItems} tx(s) scanned, ${newTxs.length} new (lastHeight=${lastHeight ?? "none"})`);
      }
    } catch (e) {
      console.warn(`[chain] poll #${pollCount} error: ${e.message}`);
    }
  }

  function setInitialLastHeight(h) {
    if (lastHeight == null && h != null) lastHeight = h;
  }

  function addSeenIds(ids) {
    for (const id of ids) if (id) seenTxIds.add(id);
  }

  function start() {
    poll();
    setInterval(poll, intervalMs);
  }

  return { start, setInitialLastHeight, addSeenIds };
}

// ── Bulk transaction fetch ───────────────────────────────────────────────────
//
// One-shot paginated fetch of all transactions for a pubkey/chain.
// Returns { transactions: [...], lastHeight, txIds: [...] } sorted oldest-first.

async function fetchAllTransactions(opts) {
  const chainId = opts.chainId;
  const appPubkey = opts.appPubkey;
  const queryField = opts.queryField || "recipient";
  const upstream = opts.upstream || getExplorerUpstream();
  const upstreamBase = opts.upstreamBase || getExplorerUpstreamBase();
  const maxPages = opts.maxPages || 500;

  if (!chainId || !appPubkey) return { transactions: [], lastHeight: null };

  const baseUrl = `${explorerProto(upstream)}://${upstream}${upstreamBase}/${chainId}`;
  const url = `${baseUrl}/transactions`;
  const allTxs = [];
  let lastHeight = null;
  let cursor = null;

  try {
    for (let page = 0; page < maxPages; page++) {
      const body = { [queryField]: appPubkey, limit: 50 };
      if (cursor) body.cursor = cursor;
      const resp = await httpsJson("POST", url, body);

      const items = Array.isArray(resp) ? resp
        : (resp && Array.isArray(resp.items)) ? resp.items
        : (resp && Array.isArray(resp.transactions)) ? resp.transactions
        : [];

      if (items.length === 0) break;

      for (const tx of items) {
        allTxs.push(tx);
        const bh = tx.block_height;
        if (typeof bh === "number" && (lastHeight == null || bh > lastHeight)) {
          lastHeight = bh;
        }
      }

      if (page % 10 === 0 && page > 0) {
        console.log(`[fetch-txs] page ${page}, ${allTxs.length} txs so far...`);
      }

      const hasMore = resp && resp.has_more;
      const nextCursor = resp && resp.next_cursor;
      if (!hasMore || !nextCursor) break;
      cursor = nextCursor;
    }
  } catch (e) {
    console.warn(`[fetch-txs] error after ${allTxs.length} txs: ${e.message}`);
  }

  function extractTs(tx) {
    const candidates = [tx.timestamp_ms, tx.created_at, tx.createdAt, tx.timestamp, tx.time];
    for (const v of candidates) {
      if (typeof v === "number" && Number.isFinite(v))
        return v < 10_000_000_000 ? v * 1000 : v;
      if (typeof v === "string" && v.trim()) {
        const t = Date.parse(v);
        if (!Number.isNaN(t)) return t;
      }
    }
    return 0;
  }

  allTxs.sort((a, b) => extractTs(a) - extractTs(b));
  const txIds = allTxs.map(tx => tx.tx_id || tx.id || tx.txid || tx.hash || tx.tx_hash).filter(Boolean);
  console.log(`[fetch-txs] fetched ${allTxs.length} transaction(s), lastHeight=${lastHeight ?? "none"}`);
  return { transactions: allTxs, lastHeight, txIds };
}

// ── Chain info discovery ─────────────────────────────────────────────────────
//
// Discovers chain_id and estimates genesis timestamp from the block explorer.
// Returns { chainId, genesisTimestampMs } — either field may be null on failure.

async function discoverChainInfo(opts) {
  const upstream = (opts && opts.upstream) || getExplorerUpstream();
  const upstreamBase = (opts && opts.upstreamBase) || getExplorerUpstreamBase();
  const baseUrl = `${explorerProto(upstream)}://${upstream}${upstreamBase}`;

  const result = { chainId: null, genesisTimestampMs: null };

  try {
    const data = await httpsJson("GET", `${baseUrl}/active_chain`);
    if (data && data.chain_id) result.chainId = data.chain_id;
  } catch (e) {
    console.warn(`[chain-info] could not discover chain: ${e.message}`);
    return result;
  }

  if (!result.chainId) return result;

  try {
    const data = await httpsJson("GET", `${baseUrl}/${result.chainId}/blocks?limit=2`);
    const blocks = (data && data.items) || [];

    if (blocks.length >= 2) {
      const [b1, b2] = blocks;
      const slotDiff = b1.global_slot - b2.global_slot;
      const timeDiff = b1.timestamp_ms - b2.timestamp_ms;
      if (slotDiff > 0 && timeDiff > 0) {
        const slotMs = timeDiff / slotDiff;
        result.genesisTimestampMs = Math.round(b1.timestamp_ms - b1.global_slot * slotMs);
        console.log(`[chain-info] genesis: ${new Date(result.genesisTimestampMs).toISOString()} (slot=${slotMs}ms, chain=${result.chainId.slice(0, 16)}…)`);
      }
    } else if (blocks.length === 1 && blocks[0].global_slot > 0) {
      const b = blocks[0];
      const slotMs = 5000;
      result.genesisTimestampMs = Math.round(b.timestamp_ms - b.global_slot * slotMs);
      console.log(`[chain-info] genesis (estimated, 1 block): ${new Date(result.genesisTimestampMs).toISOString()}`);
    }
  } catch (e) {
    console.warn(`[chain-info] could not fetch blocks for genesis time: ${e.message}`);
  }

  return result;
}

// ── Genesis accounts fetch ───────────────────────────────────────────────
//
// One-shot fetch of genesis-ledger accounts by querying the explorer for
// genesis-type transactions (block height 0). Returns an array of unique
// destination addresses that received genesis distributions.

async function fetchGenesisAccounts(opts) {
  const upstream = (opts && opts.upstream) || getExplorerUpstream();
  const upstreamBase = (opts && opts.upstreamBase) || getExplorerUpstreamBase();

  let chainId = opts && opts.chainId;
  if (!chainId) {
    try {
      const data = await httpsJson("GET", `${explorerProto(upstream)}://${upstream}${upstreamBase}/active_chain`);
      chainId = data && data.chain_id;
    } catch (e) {
      console.warn(`[genesis] could not discover chain: ${e.message}`);
      return [];
    }
  }
  if (!chainId) return [];

  const baseUrl = `${explorerProto(upstream)}://${upstream}${upstreamBase}/${chainId}`;
  const accounts = new Set();

  try {
    let cursor = null;
    for (let page = 0; page < 20; page++) {
      const body = { to_height: 1, limit: 200 };
      if (cursor) body.cursor = cursor;
      const resp = await httpsJson("POST", `${baseUrl}/transactions`, body);

      const items = (resp && Array.isArray(resp.items)) ? resp.items
        : (resp && Array.isArray(resp.transactions)) ? resp.transactions
        : [];

      for (const tx of items) {
        if (tx.tx_type === "genesis" && tx.destination) {
          accounts.add(tx.destination);
        }
      }

      if (!resp || !resp.has_more || !resp.next_cursor) break;
      cursor = resp.next_cursor;
    }
    console.log(`[genesis] found ${accounts.size} genesis account(s)`);
  } catch (e) {
    console.warn(`[genesis] could not fetch genesis accounts: ${e.message}`);
  }

  return Array.from(accounts);
}

// ── Path resolution ──────────────────────────────────────────────────────────

function resolvePath(...candidates) {
  for (const p of candidates) {
    if (fs.existsSync(p)) return p;
  }
  return candidates[candidates.length - 1];
}

module.exports = {
  get EXPLORER_UPSTREAM() { return getExplorerUpstream(); },
  get EXPLORER_UPSTREAM_BASE() { return getExplorerUpstreamBase(); },
  loadEnvFile,
  readJson,
  httpsJson,
  handleExplorerProxy,
  createMockApi,
  isExplorerConfirmed,
  createChainPoller,
  fetchAllTransactions,
  fetchGenesisAccounts,
  discoverChainInfo,
  resolvePath,
};
