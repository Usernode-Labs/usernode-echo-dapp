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

  // Hourly success-rate buckets for the chart. Returns an array of bucket
  // objects sorted oldest-to-newest, or null when the store is not ready.
  // Uses IS NOT DISTINCT FROM for null-safe chain_id comparison.
  async function getSuccessRateBuckets(chainId, windowHours = 24) {
    if (!isReady()) return null;
    const hours = Math.max(1, Math.min(168, windowHours || 24));
    const sql = `
      SELECT
        date_trunc('hour', created_at) AS hour_start,
        COUNT(*) FILTER (WHERE status = 'confirmed')::int AS confirmed,
        COUNT(*) FILTER (WHERE status = 'failed')::int    AS failed,
        COUNT(*) FILTER (WHERE status = 'skipped')::int   AS skipped
      FROM echo_events
      WHERE chain_id IS NOT DISTINCT FROM $1
        AND status IN ('confirmed', 'failed', 'skipped')
        AND created_at >= now() - ($2 || ' hours')::interval
      GROUP BY 1
      ORDER BY 1 ASC
    `;
    try {
      const r = await pool.query(sql, [chainId || null, hours]);
      return r.rows.map((row) => {
        const confirmed = row.confirmed || 0;
        const failed    = row.failed    || 0;
        const skipped   = row.skipped   || 0;
        const total     = confirmed + failed + skipped;
        const hourStart = row.hour_start instanceof Date
          ? row.hour_start.toISOString()
          : String(row.hour_start);
        return {
          hourStart,
          confirmed,
          failed,
          skipped,
          total,
          // Percentage 0-100 (null when no settled events in the bucket).
          successRate: total > 0 ? Math.round((confirmed / total) * 1000) / 10 : null,
        };
      });
    } catch (e) {
      console.error("[echo-store] getSuccessRateBuckets error:", e.message);
      return null;
    }
  }

  // IS_STAGING-gated seed: inserts demo rows for both the success-rate chart
  // (hourly buckets, idempotent per calendar day) and the transaction history
  // log (fixed rows with varied statuses). Called from echo-logic.js once the
  // chain_id is known. Idempotent throughout — ON CONFLICT DO NOTHING.
  async function seedStaging(chainId) {
    const IS_STAGING = process.env.USERNODE_ENV === "staging";
    if (!IS_STAGING || !isReady()) return;

    // Hourly chart demo rows — success-rate buckets for the past 24h.
    try {
      const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
      const now   = new Date();
      const hourMs = 3_600_000;
      const fakePubkey = "ut1stagingdemouser000000000000000000000000000000000";
      let seeded = 0;

      for (let h = 23; h >= 0; h--) {
        const hourStart = Math.floor((now.getTime() - h * hourMs) / hourMs) * hourMs;
        const hasFailure     = (h % 4 === 0);
        const confirmedCount = hasFailure ? 6 : 7;

        for (let i = 0; i < confirmedCount; i++) {
          const tsMs = hourStart + i * 8 * 60_000;
          const ts   = new Date(tsMs);
          const txId = `staging-chart-${today}-h${h}-c${i}`;
          const { rowCount } = await pool.query(
            `INSERT INTO echo_events
               (request_tx_id, chain_id, request_from, request_amount, echo_amount,
                status, request_ts, request_seen_at_ms,
                echo_confirmed_ts, echo_confirmed_at_ms, created_at, updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$7,$8,$9,$10,$10)
             ON CONFLICT (request_tx_id) DO NOTHING`,
            [txId, chainId || null, fakePubkey, 5, 4, "confirmed",
             tsMs, tsMs + 30_000, tsMs + 31_000, ts]
          );
          seeded += rowCount || 0;
        }

        if (hasFailure) {
          const tsMs = hourStart + confirmedCount * 8 * 60_000;
          const ts   = new Date(tsMs);
          const txId = `staging-chart-${today}-h${h}-fail`;
          const { rowCount } = await pool.query(
            `INSERT INTO echo_events
               (request_tx_id, chain_id, request_from, request_amount, echo_amount,
                status, error, error_category, request_ts, request_seen_at_ms,
                created_at, updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9,$10,$10)
             ON CONFLICT (request_tx_id) DO NOTHING`,
            [txId, chainId || null, fakePubkey, 5, null, "failed",
             "Staging demo failure", "permanent", tsMs, ts]
          );
          seeded += rowCount || 0;
        }
      }

      if (seeded > 0) {
        console.log(`[echo-store] seeded ${seeded} staging chart demo rows`);
      }
    } catch (e) {
      console.error("[echo-store] seedStaging error:", e.message);
    }

    // Fixed demo log rows — varied entries for the transaction history view.
    const BASE = 1749996000000; // 2026-06-15 16:00 UTC
    const SENDER = "ut1demodemo000000000sender00000000aabbccddeeffgg";
    const rows = [
      // 5 confirmed rows with full timing + block data
      [
        "txreqdemo0000000001aabbccddeeffgghhjj", chainId, SENDER + "01",
        5, 4, "confirmed", null, null, 0,
        BASE, BASE + 1200, BASE + 2000,
        "txechodemo000000001aabbccddeeffgghhjj",
        BASE + 42000, BASE + 42800,
        1001001, "bhdemo_req_0001aabbccddeeff", 1001003, "bhdemo_echo0001aabbccddeeff",
      ],
      [
        "txreqdemo0000000002aabbccddeeffgghhjj", chainId, SENDER + "02",
        10, 9, "confirmed", null, null, 0,
        BASE + 120000, BASE + 121300, BASE + 122000,
        "txechodemo000000002aabbccddeeffgghhjj",
        BASE + 165000, BASE + 165900,
        1001010, "bhdemo_req_0002aabbccddeeff", 1001012, "bhdemo_echo0002aabbccddeeff",
      ],
      [
        "txreqdemo0000000003aabbccddeeffgghhjj", chainId, SENDER + "03",
        2, 1, "confirmed", null, null, 0,
        BASE + 240000, BASE + 241100, BASE + 241800,
        "txechodemo000000003aabbccddeeffgghhjj",
        BASE + 290000, BASE + 290700,
        1001020, "bhdemo_req_0003aabbccddeeff", 1001022, "bhdemo_echo0003aabbccddeeff",
      ],
      [
        "txreqdemo0000000004aabbccddeeffgghhjj", chainId, SENDER + "04",
        20, 19, "confirmed", null, null, 0,
        BASE + 360000, BASE + 361400, BASE + 362100,
        "txechodemo000000004aabbccddeeffgghhjj",
        BASE + 398000, BASE + 398600,
        1001030, "bhdemo_req_0004aabbccddeeff", 1001032, "bhdemo_echo0004aabbccddeeff",
      ],
      [
        "txreqdemo0000000005aabbccddeeffgghhjj", chainId, SENDER + "05",
        3, 2, "confirmed", null, null, 0,
        BASE + 480000, BASE + 481200, BASE + 481900,
        "txechodemo000000005aabbccddeeffgghhjj",
        BASE + 533000, BASE + 533700,
        1001040, "bhdemo_req_0005aabbccddeeff", 1001042, "bhdemo_echo0005aabbccddeeff",
      ],
      // 1 failed row (permanent — too many retries)
      [
        "txreqdemo0000000006aabbccddeeffgghhjj", chainId, SENDER + "06",
        5, null, "failed",
        "Echo send failed: gave up after 30 retries", "permanent", 30,
        BASE + 600000, BASE + 601000, null,
        null, null, null,
        1001050, "bhdemo_req_0006aabbccddeeff", null, null,
      ],
      // 1 skipped row (amount < 2)
      [
        "txreqdemo0000000007aabbccddeeffgghhjj", chainId, SENDER + "07",
        1, null, "skipped",
        "Amount must be ≥ 2 — the echo returns N-1, so 1 or less leaves nothing to send back.",
        "skip", 0,
        BASE + 720000, BASE + 720500, null,
        null, null, null,
        1001060, "bhdemo_req_0007aabbccddeeff", null, null,
      ],
      // 1 pending row (echo not yet confirmed)
      [
        "txreqdemo0000000008aabbccddeeffgghhjj", chainId, SENDER + "08",
        7, null, "pending", null, null, 0,
        BASE + 840000, BASE + 841100, null,
        null, null, null,
        1001070, "bhdemo_req_0008aabbccddeeff", null, null,
      ],
    ];
    const colList = COLS.join(", ");
    const placeholders = COLS.map((_, i) => "$" + (i + 1)).join(", ");
    const sql = `INSERT INTO echo_events (${colList}) VALUES (${placeholders}) ON CONFLICT (request_tx_id) DO NOTHING`;
    let seeded = 0;
    for (const params of rows) {
      try {
        const r = await pool.query(sql, params);
        seeded += r.rowCount || 0;
      } catch (e) {
        console.warn("[echo-store] staging seed row failed:", e.message);
      }
    }
    if (seeded > 0) console.log(`[echo-store] seeded ${seeded} staging demo rows (chain=${chainId})`);
  }

  async function close() {
    if (pruneTimer) clearInterval(pruneTimer);
    if (pool) {
      try { await pool.end(); } catch (_) {}
    }
  }

  return { init, isReady, persist, hydrate, queryHistory, queryStats,
           getSuccessRateBuckets, seedStaging, prune, close };
}

module.exports = createEchoStore;
