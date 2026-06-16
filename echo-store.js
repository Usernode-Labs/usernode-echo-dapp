/**
 * echo-store.js — durable, write-through Postgres mirror of the in-memory
 * echo events Map (echo-logic.js). App-specific; NOT vendored.
 *
 * Design (see the spec, "Data Model"):
 *   - The in-memory `events` Map stays the source of truth for live state.
 *     This table is a write-through mirror so the diagnostic log survives
 *     restarts and is the same global record for every viewer.
 *   - Writes are idempotent UPSERTs keyed on request_tx_id. Backfill replay
 *     and the dedup short-circuits revisit the same request many times; an
 *     UPSERT keyed on the request id collapses those safely. The conflict
 *     clause never downgrades a `confirmed` row back to a pending status.
 *   - The table is PUBLIC (no `staging:private` comment): every column is
 *     already exposed by the public /__echo/state endpoint and holds only
 *     on-chain-public data (pubkeys, amounts, tx/block ids, timings).
 *
 * Graceful degradation (REQUIRED): if DATABASE_URL is absent, the `pg`
 * module is missing, or the initial connection / CREATE TABLE fails, the
 * store disables itself and every method becomes a no-op. Echo then runs
 * in-memory only — identical to its pre-DB behaviour. It must never fail to
 * start because the DB is unavailable.
 */

let Pool = null;
try {
  ({ Pool } = require("pg"));
} catch (_) {
  Pool = null; // pg not installed — store stays disabled, in-memory only.
}

// Retention: prune rows older than this OR beyond the newest N per chain.
const RETENTION_MS = 30 * 24 * 60 * 60 * 1000; // 30 days
const MAX_ROWS_PER_CHAIN = 5000;
const PRUNE_INTERVAL_MS = 6 * 60 * 60 * 1000; // 6h

const CREATE_SQL = `
CREATE TABLE IF NOT EXISTS echo_events (
  request_tx_id            TEXT PRIMARY KEY,
  chain_id                 TEXT,
  request_from             TEXT NOT NULL,
  request_amount           BIGINT NOT NULL,
  echo_amount              BIGINT,
  status                   TEXT NOT NULL,
  error                    TEXT,
  error_category           TEXT,
  retry_attempts           INTEGER NOT NULL DEFAULT 0,
  request_ts               BIGINT,
  request_seen_at_ms       BIGINT,
  echo_sent_at_ms          BIGINT,
  echo_tx_id               TEXT,
  echo_confirmed_ts        BIGINT,
  echo_confirmed_at_ms     BIGINT,
  request_block_height     BIGINT,
  request_block_hash       TEXT,
  echo_block_height        BIGINT,
  echo_block_hash          TEXT,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS echo_events_created_at_idx ON echo_events (created_at DESC);
CREATE INDEX IF NOT EXISTS echo_events_chain_created_idx ON echo_events (chain_id, created_at DESC);
`;

// Column order shared by INSERT params and rowToEvent.
const COLS = [
  "request_tx_id",
  "chain_id",
  "request_from",
  "request_amount",
  "echo_amount",
  "status",
  "error",
  "error_category",
  "retry_attempts",
  "request_ts",
  "request_seen_at_ms",
  "echo_sent_at_ms",
  "echo_tx_id",
  "echo_confirmed_ts",
  "echo_confirmed_at_ms",
  "request_block_height",
  "request_block_hash",
  "echo_block_height",
  "echo_block_hash",
];

// `confirmed` is the success terminal — once a row is confirmed, a later
// (out-of-order) pending/echoing replay must not clobber it. COALESCE keeps
// any already-recorded field that a newer partial row left null.
const UPSERT_SQL = `
INSERT INTO echo_events (${COLS.join(", ")})
VALUES (${COLS.map((_, i) => "$" + (i + 1)).join(", ")})
ON CONFLICT (request_tx_id) DO UPDATE SET
  chain_id             = EXCLUDED.chain_id,
  request_from         = EXCLUDED.request_from,
  request_amount       = EXCLUDED.request_amount,
  echo_amount          = COALESCE(EXCLUDED.echo_amount, echo_events.echo_amount),
  status               = CASE WHEN echo_events.status = 'confirmed' THEN echo_events.status ELSE EXCLUDED.status END,
  error                = CASE WHEN echo_events.status = 'confirmed' THEN echo_events.error ELSE EXCLUDED.error END,
  error_category       = CASE WHEN echo_events.status = 'confirmed' THEN echo_events.error_category ELSE EXCLUDED.error_category END,
  retry_attempts       = GREATEST(EXCLUDED.retry_attempts, echo_events.retry_attempts),
  request_ts           = COALESCE(echo_events.request_ts, EXCLUDED.request_ts),
  request_seen_at_ms   = COALESCE(echo_events.request_seen_at_ms, EXCLUDED.request_seen_at_ms),
  echo_sent_at_ms      = COALESCE(EXCLUDED.echo_sent_at_ms, echo_events.echo_sent_at_ms),
  echo_tx_id           = COALESCE(EXCLUDED.echo_tx_id, echo_events.echo_tx_id),
  echo_confirmed_ts    = COALESCE(EXCLUDED.echo_confirmed_ts, echo_events.echo_confirmed_ts),
  echo_confirmed_at_ms = COALESCE(EXCLUDED.echo_confirmed_at_ms, echo_events.echo_confirmed_at_ms),
  request_block_height = COALESCE(EXCLUDED.request_block_height, echo_events.request_block_height),
  request_block_hash   = COALESCE(EXCLUDED.request_block_hash, echo_events.request_block_hash),
  echo_block_height    = COALESCE(EXCLUDED.echo_block_height, echo_events.echo_block_height),
  echo_block_hash      = COALESCE(EXCLUDED.echo_block_hash, echo_events.echo_block_hash),
  updated_at           = now()
`;

function numOrNull(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

// Map a DB row (pg returns BIGINT as strings) back into the camelCase event
// shape the client already consumes from /__echo/state, so the UI renders
// history rows with the exact same code path.
function rowToEvent(row) {
  return {
    requestTxId: row.request_tx_id,
    chainId: row.chain_id,
    requestFrom: row.request_from,
    requestAmount: numOrNull(row.request_amount),
    echoAmount: numOrNull(row.echo_amount),
    status: row.status,
    error: row.error || null,
    errorCategory: row.error_category || null,
    nextRetryAtMs: null, // transient/live-only; never persisted
    retryAttempts: numOrNull(row.retry_attempts) || 0,
    requestTs: numOrNull(row.request_ts),
    requestSeenAtServerMs: numOrNull(row.request_seen_at_ms),
    echoSentAtServerMs: numOrNull(row.echo_sent_at_ms),
    echoTxId: row.echo_tx_id || null,
    echoConfirmedTs: numOrNull(row.echo_confirmed_ts),
    echoConfirmedAtServerMs: numOrNull(row.echo_confirmed_at_ms),
    requestBlockHeight: numOrNull(row.request_block_height),
    requestBlockHash: row.request_block_hash || null,
    echoBlockHeight: numOrNull(row.echo_block_height),
    echoBlockHash: row.echo_block_hash || null,
  };
}

function eventToParams(ev, chainId) {
  return [
    ev.requestTxId,
    chainId || null,
    ev.requestFrom,
    ev.requestAmount != null ? Math.trunc(ev.requestAmount) : 0,
    ev.echoAmount != null ? Math.trunc(ev.echoAmount) : null,
    ev.status,
    ev.error || null,
    ev.errorCategory || null,
    ev.retryAttempts || 0,
    ev.requestTs != null ? Math.trunc(ev.requestTs) : null,
    ev.requestSeenAtServerMs != null ? Math.trunc(ev.requestSeenAtServerMs) : null,
    ev.echoSentAtServerMs != null ? Math.trunc(ev.echoSentAtServerMs) : null,
    ev.echoTxId || null,
    ev.echoConfirmedTs != null ? Math.trunc(ev.echoConfirmedTs) : null,
    ev.echoConfirmedAtServerMs != null ? Math.trunc(ev.echoConfirmedAtServerMs) : null,
    ev.requestBlockHeight != null ? Math.trunc(ev.requestBlockHeight) : null,
    ev.requestBlockHash || null,
    ev.echoBlockHeight != null ? Math.trunc(ev.echoBlockHeight) : null,
    ev.echoBlockHash || null,
  ];
}

// Opaque cursor = base64("<iso created_at>|<request_tx_id>").
function encodeCursor(row) {
  if (!row) return null;
  const iso = row.created_at instanceof Date ? row.created_at.toISOString() : String(row.created_at);
  return Buffer.from(`${iso}|${row.request_tx_id}`, "utf8").toString("base64url");
}
function decodeCursor(cursor) {
  if (!cursor || typeof cursor !== "string") return null;
  try {
    const raw = Buffer.from(cursor, "base64url").toString("utf8");
    const idx = raw.lastIndexOf("|");
    if (idx < 0) return null;
    const iso = raw.slice(0, idx);
    const id = raw.slice(idx + 1);
    if (!iso || Number.isNaN(Date.parse(iso))) return null;
    return { iso, id };
  } catch (_) {
    return null;
  }
}

function createEchoStore(opts = {}) {
  const databaseUrl = opts.databaseUrl || process.env.DATABASE_URL || "";
  let pool = null;
  let ready = false;
  let disabled = !databaseUrl || !Pool;
  let pruneTimer = null;

  function isReady() {
    return ready && !disabled;
  }

  async function init() {
    if (disabled) {
      console.log("[echo-store] disabled (no DATABASE_URL or pg module) — running in-memory only");
      return false;
    }
    try {
      pool = new Pool({ connectionString: databaseUrl, max: 4 });
      pool.on("error", (e) => console.error("[echo-store] pool error:", e.message));
      await pool.query(CREATE_SQL);
      ready = true;
      console.log("[echo-store] ready (echo_events table ensured)");
      pruneTimer = setInterval(() => {
        prune().catch((e) => console.error("[echo-store] prune error:", e.message));
      }, PRUNE_INTERVAL_MS);
      if (typeof pruneTimer.unref === "function") pruneTimer.unref();
      return true;
    } catch (e) {
      disabled = true;
      ready = false;
      console.warn(`[echo-store] init failed (${e.message}) — running in-memory only`);
      return false;
    }
  }

  // Fire-and-forget write-through. Never throws into the caller; a DB hiccup
  // must not perturb the in-memory state machine.
  function persist(ev, chainId) {
    if (!isReady() || !ev || !ev.requestTxId || !ev.requestFrom) return;
    pool
      .query(UPSERT_SQL, eventToParams(ev, chainId))
      .catch((e) => console.error("[echo-store] persist error:", e.message));
  }

  // Boot hydration: most-recent rows for the current chain, newest first,
  // as inert events (no timers, no re-send). Returns [] when disabled.
  async function hydrate(chainId, limit = 200) {
    if (!isReady()) return [];
    try {
      const r = await pool.query(
        `SELECT * FROM echo_events WHERE chain_id = $1 ORDER BY created_at DESC LIMIT $2`,
        [chainId || null, limit]
      );
      return r.rows.map(rowToEvent);
    } catch (e) {
      console.error("[echo-store] hydrate error:", e.message);
      return [];
    }
  }

  // Paginated, newest-first history for the current chain. Cursor pagination
  // on (created_at, request_tx_id) so concurrent inserts don't shift pages.
  async function queryHistory(chainId, limit, before) {
    if (!isReady()) return null;
    const lim = Math.max(1, Math.min(100, limit || 25));
    const cur = decodeCursor(before);
    const params = [chainId || null];
    let sql = `SELECT * FROM echo_events WHERE chain_id = $1`;
    if (cur) {
      params.push(cur.iso, cur.id);
      sql += ` AND (created_at, request_tx_id) < ($2::timestamptz, $3::text)`;
    }
    sql += ` ORDER BY created_at DESC, request_tx_id DESC LIMIT ${lim + 1}`;
    const r = await pool.query(sql, params);
    const rows = r.rows;
    const hasMore = rows.length > lim;
    const page = hasMore ? rows.slice(0, lim) : rows;
    return {
      events: page.map(rowToEvent),
      nextCursor: hasMore ? encodeCursor(page[page.length - 1]) : null,
    };
  }

  // Aggregate stats over the last `windowSize` terminal events for the chain.
  // Latencies clamp negatives (chain-ts vs server-ts skew) to 0 before the
  // percentile. Returns null when disabled.
  async function queryStats(chainId, windowSize = 200) {
    if (!isReady()) return null;
    const sql = `
      WITH recent AS (
        SELECT * FROM echo_events
        WHERE chain_id = $1 AND status IN ('confirmed','failed','skipped')
        ORDER BY created_at DESC
        LIMIT $2
      )
      SELECT
        count(*)::int AS total,
        count(*) FILTER (WHERE status='confirmed')::int AS confirmed,
        count(*) FILTER (WHERE status='failed')::int AS failed,
        count(*) FILTER (WHERE status='skipped')::int AS skipped,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY GREATEST(echo_confirmed_ts - request_ts, 0))
          FILTER (WHERE status='confirmed' AND echo_confirmed_ts IS NOT NULL AND request_ts IS NOT NULL) AS median_total,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY GREATEST(echo_confirmed_ts - request_ts, 0))
          FILTER (WHERE status='confirmed' AND echo_confirmed_ts IS NOT NULL AND request_ts IS NOT NULL) AS p95_total,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY GREATEST(echo_sent_at_ms - request_seen_at_ms, 0))
          FILTER (WHERE status='confirmed' AND echo_sent_at_ms IS NOT NULL AND request_seen_at_ms IS NOT NULL) AS median_queue
      FROM recent`;
    const r = await pool.query(sql, [chainId || null, windowSize]);
    const row = r.rows[0] || {};
    const total = row.total || 0;
    const confirmed = row.confirmed || 0;
    return {
      total,
      confirmed,
      failed: row.failed || 0,
      skipped: row.skipped || 0,
      successRate: total > 0 ? confirmed / total : null,
      medianTotalMs: numOrNull(row.median_total),
      p95TotalMs: numOrNull(row.p95_total),
      medianServerEchoQueueMs: numOrNull(row.median_queue),
    };
  }

  async function getLeaderboard(chainId) {
    if (!isReady()) return [];
    const sql = `
      SELECT
        request_from          AS sender,
        SUM(request_amount)   AS total_sent,
        COUNT(*)::int         AS echo_count
      FROM echo_events
      WHERE chain_id = $1
        AND status = 'confirmed'
      GROUP BY request_from
      ORDER BY total_sent DESC
      LIMIT 10
    `;
    const r = await pool.query(sql, [chainId || null]);
    return r.rows.map((row) => ({
      sender: row.sender,
      totalSent: numOrNull(row.total_sent) || 0,
      echoCount: Number(row.echo_count) || 0,
    }));
  }

  // Boot-time staging seed: inserts confirmed echo events for 5 distinct fake
  // senders so the leaderboard renders with multiple rows in a fresh staging
  // container. Idempotent via ON CONFLICT DO NOTHING.
  async function seedStagingLeaderboard(chainId) {
    if (!isReady()) return;
    const seeds = [
      ["seed-lb-alpha-1", "staging-demo-alpha",   150, 149],
      ["seed-lb-alpha-2", "staging-demo-alpha",   200, 199],
      ["seed-lb-alpha-3", "staging-demo-alpha",   100,  99],
      ["seed-lb-beta-1",  "staging-demo-beta",     50,  49],
      ["seed-lb-beta-2",  "staging-demo-beta",    250, 249],
      ["seed-lb-gamma-1", "staging-demo-gamma",    50,  49],
      ["seed-lb-gamma-2", "staging-demo-gamma",    50,  49],
      ["seed-lb-gamma-3", "staging-demo-gamma",    25,  24],
      ["seed-lb-gamma-4", "staging-demo-gamma",    25,  24],
      ["seed-lb-delta-1", "staging-demo-delta",    80,  79],
      ["seed-lb-eps-1",   "staging-demo-epsilon",  10,   9],
      ["seed-lb-eps-2",   "staging-demo-epsilon",  10,   9],
    ];
    for (const [txId, sender, amt, echoAmt] of seeds) {
      await pool.query(
        `INSERT INTO echo_events
           (request_tx_id, chain_id, request_from, request_amount,
            echo_amount, status, retry_attempts)
         VALUES ($1, $2, $3, $4, $5, 'confirmed', 0)
         ON CONFLICT (request_tx_id) DO NOTHING`,
        [txId, chainId || null, sender, amt, echoAmt]
      );
    }
    console.log("[echo-store] staging leaderboard seed applied");
  }

  async function prune() {
    if (!isReady()) return;
    // Age-based prune across all chains.
    const cutoffMs = Date.now() - RETENTION_MS;
    const cutoffIso = new Date(cutoffMs).toISOString();
    const aged = await pool.query(`DELETE FROM echo_events WHERE created_at < $1`, [cutoffIso]);
    // Count-based prune: keep newest MAX_ROWS_PER_CHAIN per chain.
    const capped = await pool.query(
      `DELETE FROM echo_events e USING (
         SELECT request_tx_id FROM (
           SELECT request_tx_id,
                  row_number() OVER (PARTITION BY chain_id ORDER BY created_at DESC) AS rn
           FROM echo_events
         ) ranked WHERE rn > $1
       ) doomed
       WHERE e.request_tx_id = doomed.request_tx_id`,
      [MAX_ROWS_PER_CHAIN]
    );
    const n = (aged.rowCount || 0) + (capped.rowCount || 0);
    if (n > 0) console.log(`[echo-store] pruned ${n} old rows (${aged.rowCount || 0} aged, ${capped.rowCount || 0} over cap)`);
  }

  async function close() {
    if (pruneTimer) clearInterval(pruneTimer);
    if (pool) {
      try { await pool.end(); } catch (_) {}
    }
  }

  return { init, isReady, persist, hydrate, queryHistory, queryStats, getLeaderboard, seedStagingLeaderboard, prune, close };
}

module.exports = createEchoStore;
