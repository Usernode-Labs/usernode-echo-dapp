/**
 * echo-metrics.js — historical metrics store + automatic anomaly detection
 * for the Echo latency-test dapp.
 *
 * This is APP-SPECIFIC code (note the `echo-` prefix). It is NOT one of the
 * vendored shared files (`lib/dapp-server.js`, `lib/tx-match.js`) that get
 * re-vendored from usernode-dapp-starter — do not treat it as such.
 *
 * Responsibilities
 *   1. Persist one row per *settled* echo (confirmed/failed/skipped) into
 *      `echo_samples`, tagged with the current `chain_epoch`. This table is
 *      PRIVATE — it holds the sender wallet address (`request_from`) and is
 *      never exposed through any HTTP endpoint.
 *   2. Maintain per-minute rollups in `metric_buckets` (the detector's
 *      working set) and an `anomalies` table with an open→resolved lifecycle.
 *   3. Run a rolling-baseline (robust z-score: median + MAD) detector plus
 *      absolute-threshold safety nets every ~30s.
 *
 * Storage backend: better-sqlite3 under ECHO_DATA_DIR. If the native module
 * can't load (e.g. build without toolchain) or the data dir isn't writable,
 * the store transparently degrades to an in-memory backend and reports
 * `persistent: false` so operators know baselines won't survive a restart.
 */

"use strict";

const fs = require("fs");
const path = require("path");

const SEED_EPOCH = "__seed__";

// Series we track. `direction` is the side that counts as anomalous:
//   up   → a rise is bad (latencies, failure/skip/retry rates)
//   down → a fall is bad (request volume collapsing toward zero)
const METRICS = {
  chain_rtt_ms:  { kind: "latency", direction: "up",   label: "chain round-trip" },
  poll_lag_ms:   { kind: "latency", direction: "up",   label: "poll detect lag" },
  echo_queue_ms: { kind: "latency", direction: "up",   label: "echo build + RPC" },
  rpc_send_ms:   { kind: "latency", direction: "up",   label: "wallet/send RPC" },
  failure_rate:  { kind: "ratio",   direction: "up",   label: "failure rate" },
  skip_rate:     { kind: "ratio",   direction: "up",   label: "skip rate" },
  retry_rate:    { kind: "count",   direction: "up",   label: "retries/echo" },
  request_volume:{ kind: "count",   direction: "down", label: "request volume" },
};

// Which UI banner category each metric maps to (for the public health hint).
const CATEGORY = {
  failure_rate: "failures",
  skip_rate: "failures",
  unconfirmed_age_ms: "stall",
  // everything else (latencies, volume, retries, clock skew) → "latency"
};
const CATEGORY_PRIORITY = { stall: 3, failures: 2, latency: 1 };

function num(v, dflt) {
  const n = Number(v);
  return Number.isFinite(n) ? n : dflt;
}

function buildConfig() {
  return {
    bucketMs: 60 * 1000,
    baselineWindowMs: num(process.env.ANOMALY_BASELINE_MS, 24 * 60 * 60 * 1000),
    evalWindowMs: num(process.env.ANOMALY_EVAL_MS, 5 * 60 * 1000),
    evalIntervalMs: num(process.env.ANOMALY_INTERVAL_MS, 30 * 1000),
    retentionMs: num(process.env.ANOMALY_RETENTION_MS, 7 * 24 * 60 * 60 * 1000),
    z: num(process.env.ANOMALY_Z, 3.5),
    minBaselineBuckets: num(process.env.ANOMALY_MIN_BUCKETS, 30),
    stallMs: num(process.env.ANOMALY_STALL_MS, 5 * 60 * 1000),
    rttHardMs: num(process.env.ANOMALY_RTT_HARD_MS, 60 * 1000),
    failRate: num(process.env.ANOMALY_FAILRATE, 0.5),
    minSettledFailrate: num(process.env.ANOMALY_MIN_SETTLED, 3),
    skewMs: num(process.env.ANOMALY_SKEW_MS, 2000),
    clearStreak: num(process.env.ANOMALY_CLEAR_STREAK, 2),
    reservoirMax: 256,
  };
}

// ── small numeric helpers ────────────────────────────────────────────────
function median(arr) {
  if (!arr.length) return null;
  const a = arr.slice().sort((x, y) => x - y);
  const mid = a.length >> 1;
  return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
}
function mad(arr, med) {
  if (!arr.length) return 0;
  const dev = arr.map((x) => Math.abs(x - med));
  return median(dev) || 0;
}
function percentile(arr, p) {
  if (!arr.length) return null;
  const a = arr.slice().sort((x, y) => x - y);
  const idx = Math.min(a.length - 1, Math.max(0, Math.round((p / 100) * (a.length - 1))));
  return a[idx];
}
function fmtMs(ms) {
  if (ms == null) return "—";
  return ms >= 1000 ? `${(ms / 1000).toFixed(1)}s` : `${Math.round(ms)}ms`;
}
function pct(x) {
  return x == null ? "—" : `${Math.round(x * 100)}%`;
}

// ── storage backends ──────────────────────────────────────────────────────
// Both expose: saveSample(row), saveBucket(key, bucket), saveAnomaly(a),
// loadBuckets(sinceMs), loadAnomalies(), prune(beforeMs), persistent.
function createMemoryStore() {
  return {
    persistent: false,
    saveSample() {},
    saveBucket() {},
    saveAnomaly() {},
    loadBuckets() { return []; },
    loadAnomalies() { return []; },
    prune() {},
    close() {},
  };
}

function createSqliteStore(dataDir) {
  let Database;
  try {
    Database = require("better-sqlite3");
  } catch (e) {
    console.warn(`[echo-metrics] better-sqlite3 unavailable (${e.message}) — using in-memory store (persistent:false)`);
    return null;
  }
  let db;
  try {
    fs.mkdirSync(dataDir, { recursive: true });
    db = new Database(path.join(dataDir, "echo-metrics.db"));
    db.pragma("journal_mode = WAL");
    db.pragma("synchronous = NORMAL");
  } catch (e) {
    console.warn(`[echo-metrics] cannot open DB in ${dataDir} (${e.message}) — using in-memory store (persistent:false)`);
    return null;
  }

  // echo_samples is PRIVATE: holds request_from (a user wallet address) and is
  // never returned by any HTTP endpoint. (The platform's staging DB copy is
  // Postgres-only; this sqlite file is not propagated to staging at all.)
  db.exec(`
    CREATE TABLE IF NOT EXISTS echo_samples (
      request_tx_id TEXT PRIMARY KEY,
      request_from TEXT,
      request_amount INTEGER,
      echo_amount INTEGER,
      request_ts INTEGER,
      request_seen_ms INTEGER,
      echo_sent_ms INTEGER,
      echo_confirmed_ts INTEGER,
      echo_confirmed_ms INTEGER,
      status TEXT,
      error TEXT,
      retry_attempts INTEGER,
      poll_lag_ms INTEGER,
      echo_queue_ms INTEGER,
      chain_rtt_ms INTEGER,
      rpc_send_ms INTEGER,
      chain_epoch TEXT,
      bucket_start_ms INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_samples_confirmed ON echo_samples(echo_confirmed_ms);
    CREATE INDEX IF NOT EXISTS idx_samples_status ON echo_samples(status);

    CREATE TABLE IF NOT EXISTS metric_buckets (
      bucket_start_ms INTEGER,
      chain_epoch TEXT,
      data TEXT,
      PRIMARY KEY (bucket_start_ms, chain_epoch)
    );

    CREATE TABLE IF NOT EXISTS anomalies (
      id INTEGER PRIMARY KEY,
      metric TEXT,
      detector TEXT,
      direction TEXT,
      opened_at_ms INTEGER,
      last_seen_at_ms INTEGER,
      resolved_at_ms INTEGER,
      baseline_value REAL,
      observed_value REAL,
      peak_value REAL,
      robust_z REAL,
      severity TEXT,
      status TEXT,
      chain_epoch TEXT,
      note TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_anomalies_status ON anomalies(status);
  `);

  const insSample = db.prepare(`
    INSERT INTO echo_samples
      (request_tx_id, request_from, request_amount, echo_amount, request_ts,
       request_seen_ms, echo_sent_ms, echo_confirmed_ts, echo_confirmed_ms,
       status, error, retry_attempts, poll_lag_ms, echo_queue_ms, chain_rtt_ms,
       rpc_send_ms, chain_epoch, bucket_start_ms)
    VALUES
      (@request_tx_id, @request_from, @request_amount, @echo_amount, @request_ts,
       @request_seen_ms, @echo_sent_ms, @echo_confirmed_ts, @echo_confirmed_ms,
       @status, @error, @retry_attempts, @poll_lag_ms, @echo_queue_ms, @chain_rtt_ms,
       @rpc_send_ms, @chain_epoch, @bucket_start_ms)
    ON CONFLICT(request_tx_id) DO UPDATE SET
      status=excluded.status, error=excluded.error,
      echo_confirmed_ts=excluded.echo_confirmed_ts,
      echo_confirmed_ms=excluded.echo_confirmed_ms,
      chain_rtt_ms=excluded.chain_rtt_ms, poll_lag_ms=excluded.poll_lag_ms,
      echo_queue_ms=excluded.echo_queue_ms, rpc_send_ms=excluded.rpc_send_ms
  `);
  const upBucket = db.prepare(`
    INSERT INTO metric_buckets (bucket_start_ms, chain_epoch, data)
    VALUES (@bucket_start_ms, @chain_epoch, @data)
    ON CONFLICT(bucket_start_ms, chain_epoch) DO UPDATE SET data=excluded.data
  `);
  const upAnomaly = db.prepare(`
    INSERT INTO anomalies
      (id, metric, detector, direction, opened_at_ms, last_seen_at_ms, resolved_at_ms,
       baseline_value, observed_value, peak_value, robust_z, severity, status, chain_epoch, note)
    VALUES
      (@id, @metric, @detector, @direction, @opened_at_ms, @last_seen_at_ms, @resolved_at_ms,
       @baseline_value, @observed_value, @peak_value, @robust_z, @severity, @status, @chain_epoch, @note)
    ON CONFLICT(id) DO UPDATE SET
      detector=excluded.detector, last_seen_at_ms=excluded.last_seen_at_ms,
      resolved_at_ms=excluded.resolved_at_ms, observed_value=excluded.observed_value,
      peak_value=excluded.peak_value, robust_z=excluded.robust_z,
      severity=excluded.severity, status=excluded.status, note=excluded.note
  `);

  return {
    persistent: true,
    saveSample(row) { try { insSample.run(row); } catch (_) {} },
    saveBucket(key, bucket) {
      try {
        upBucket.run({
          bucket_start_ms: bucket.start,
          chain_epoch: bucket.epoch,
          data: JSON.stringify(serializeBucket(bucket)),
        });
      } catch (_) {}
    },
    saveAnomaly(a) {
      try {
        upAnomaly.run({
          id: a.id, metric: a.metric, detector: a.detector, direction: a.direction,
          opened_at_ms: a.openedAtMs, last_seen_at_ms: a.lastSeenAtMs, resolved_at_ms: a.resolvedAtMs,
          baseline_value: a.baselineValue, observed_value: a.observedValue, peak_value: a.peakValue,
          robust_z: a.robustZ, severity: a.severity, status: a.status, chain_epoch: a.epoch, note: a.note,
        });
      } catch (_) {}
    },
    loadBuckets(sinceMs) {
      try {
        const rows = db.prepare(`SELECT bucket_start_ms, chain_epoch, data FROM metric_buckets WHERE bucket_start_ms >= ? OR chain_epoch = ?`).all(sinceMs, SEED_EPOCH);
        return rows.map((r) => deserializeBucket(JSON.parse(r.data), r.bucket_start_ms, r.chain_epoch));
      } catch (_) { return []; }
    },
    loadAnomalies() {
      try {
        return db.prepare(`SELECT * FROM anomalies ORDER BY opened_at_ms DESC LIMIT 500`).all().map((r) => ({
          id: r.id, metric: r.metric, detector: r.detector, direction: r.direction,
          openedAtMs: r.opened_at_ms, lastSeenAtMs: r.last_seen_at_ms, resolvedAtMs: r.resolved_at_ms,
          baselineValue: r.baseline_value, observedValue: r.observed_value, peakValue: r.peak_value,
          robustZ: r.robust_z, severity: r.severity, status: r.status, epoch: r.chain_epoch, note: r.note,
          _clearStreak: 0,
        }));
      } catch (_) { return []; }
    },
    prune(beforeMs) {
      try {
        db.prepare(`DELETE FROM metric_buckets WHERE bucket_start_ms < ? AND chain_epoch != ?`).run(beforeMs, SEED_EPOCH);
        db.prepare(`DELETE FROM echo_samples WHERE bucket_start_ms < ?`).run(beforeMs);
        db.prepare(`DELETE FROM anomalies WHERE status='resolved' AND resolved_at_ms < ?`).run(beforeMs);
      } catch (_) {}
    },
    close() { try { db.close(); } catch (_) {} },
  };
}

// ── bucket (de)serialization ───────────────────────────────────────────────
function newBucket(start, epoch) {
  return {
    start, epoch,
    total: 0, confirmed: 0, failed: 0, skipped: 0,
    retrySum: 0, skewCount: 0, skewSamples: 0,
    lat: { chain_rtt_ms: [], poll_lag_ms: [], echo_queue_ms: [], rpc_send_ms: [] },
  };
}
function serializeBucket(b) {
  return {
    total: b.total, confirmed: b.confirmed, failed: b.failed, skipped: b.skipped,
    retrySum: b.retrySum, skewCount: b.skewCount, skewSamples: b.skewSamples, lat: b.lat,
  };
}
function deserializeBucket(d, start, epoch) {
  const b = newBucket(start, epoch);
  b.total = d.total || 0; b.confirmed = d.confirmed || 0; b.failed = d.failed || 0;
  b.skipped = d.skipped || 0; b.retrySum = d.retrySum || 0;
  b.skewCount = d.skewCount || 0; b.skewSamples = d.skewSamples || 0;
  if (d.lat) for (const k of Object.keys(b.lat)) if (Array.isArray(d.lat[k])) b.lat[k] = d.lat[k];
  return b;
}

// ── per-bucket / window value extraction ───────────────────────────────────
function perBucketValue(b, metric) {
  switch (metric) {
    case "chain_rtt_ms": case "poll_lag_ms": case "echo_queue_ms": case "rpc_send_ms":
      return b.lat[metric] && b.lat[metric].length ? median(b.lat[metric]) : null;
    case "failure_rate": return b.total > 0 ? b.failed / b.total : null;
    case "skip_rate": return b.total > 0 ? b.skipped / b.total : null;
    case "retry_rate": return b.total > 0 ? b.retrySum / b.total : null;
    case "request_volume": return b.total > 0 ? b.total : null;
    default: return null;
  }
}
function sumField(buckets, f) { let s = 0; for (const b of buckets) s += b[f] || 0; return s; }
function collectReservoir(buckets, metric) {
  const out = [];
  for (const b of buckets) if (b.lat[metric]) for (const v of b.lat[metric]) out.push(v);
  return out;
}

function createEchoMetrics(opts) {
  opts = opts || {};
  const cfg = buildConfig();
  const localDev = !!opts.localDev;
  // In local-dev we keep everything in memory (don't litter the repo with a db
  // file); in production we persist under ECHO_DATA_DIR.
  const dataDir = localDev ? null : (opts.dataDir || null);
  const isStaging = !!opts.isStaging;
  const seedEnabled = !!opts.seedBaseline && isStaging;

  let store = dataDir ? createSqliteStore(dataDir) : null;
  if (!store) store = createMemoryStore();

  let currentEpoch = opts.epoch || "genesis";
  let liveProbe = typeof opts.liveProbe === "function" ? opts.liveProbe : () => ({ unconfirmedAges: [] });

  const buckets = new Map();            // `${start}:${epoch}` -> bucket
  let openAnomalies = [];               // status === 'open'
  let recentResolved = [];              // capped
  let nextAnomalyId = 1;
  const baselinesCache = {};            // metric -> { median, mad, sampleCount }
  let lastEvalAt = 0;
  let lastOpportunisticAt = 0;
  let evalTimer = null;

  // ── hydrate from durable store ───────────────────────────────────────────
  (function hydrate() {
    const since = Date.now() - cfg.baselineWindowMs - cfg.bucketMs;
    for (const b of store.loadBuckets(since)) buckets.set(`${b.start}:${b.epoch}`, b);
    const all = store.loadAnomalies();
    for (const a of all) {
      if (a.id >= nextAnomalyId) nextAnomalyId = a.id + 1;
      if (a.status === "open") openAnomalies.push(a);
      else recentResolved.push(a);
    }
    recentResolved = recentResolved.slice(0, 50);
  })();

  if (seedEnabled) {
    maybeSeedBaseline();
    maybeSeedAnomalies();
  }

  function maybeSeedBaseline() {
    // Only seed if we don't already have a seed set. Synthetic ~normal traffic
    // so the detector has a baseline before any real staging traffic arrives.
    const haveSeed = Array.from(buckets.values()).some((b) => b.epoch === SEED_EPOCH);
    if (haveSeed) return;
    const now = Date.now();
    const N = 180; // 3h of minute-buckets — comfortably past minBaselineBuckets
    for (let i = 0; i < N; i++) {
      const start = Math.floor((now - (i + 1) * cfg.bucketMs) / cfg.bucketMs) * cfg.bucketMs;
      const b = newBucket(start, SEED_EPOCH);
      const samples = 2 + Math.floor(Math.random() * 4);
      for (let s = 0; s < samples; s++) {
        b.total++; b.confirmed++;
        b.lat.chain_rtt_ms.push(Math.round(2000 + Math.random() * 800));
        b.lat.poll_lag_ms.push(Math.round(300 + Math.random() * 400));
        b.lat.echo_queue_ms.push(Math.round(150 + Math.random() * 250));
        b.lat.rpc_send_ms.push(Math.round(40 + Math.random() * 80));
        b.skewSamples++;
      }
      buckets.set(`${start}:${SEED_EPOCH}`, b);
      store.saveBucket(`${start}:${SEED_EPOCH}`, b);
    }
    console.log(`[echo-metrics] seeded ${N} synthetic baseline buckets (staging, ECHO_SEED_BASELINE)`);
  }

  function maybeSeedAnomalies() {
    // Only seed if no seed-epoch anomalies exist yet. Inserts resolved-only
    // entries so the anomaly history panel has rows to display in staging
    // without falsely triggering the health banner.
    const haveAnomalySeed = recentResolved.some((a) => a.epoch === SEED_EPOCH) ||
                            openAnomalies.some((a) => a.epoch === SEED_EPOCH);
    if (haveAnomalySeed) return;
    const now = Date.now();
    const DAY = 24 * 60 * 60 * 1000;
    const seeds = [
      {
        metric: "chain_rtt_ms", severity: "critical",
        note: "chain round-trip p50 82.4s vs 2.1s baseline",
        openOffset: 5 * DAY, duration: 8 * 60 * 1000,
      },
      {
        metric: "failure_rate", severity: "critical",
        note: "failure rate 61% over last 5m",
        openOffset: 3 * DAY, duration: 4 * 60 * 1000,
      },
      {
        metric: "poll_lag_ms", severity: "warn",
        note: "poll detect lag p50 4.2s vs 0.8s baseline",
        openOffset: 2 * DAY, duration: 12 * 60 * 1000,
      },
      {
        metric: "unconfirmed_age_ms", severity: "critical",
        note: "oldest unconfirmed echo 5m 34s (≥ 5m)",
        openOffset: 1 * DAY, duration: 6 * 60 * 1000,
      },
      {
        metric: "request_volume", severity: "warn",
        note: "request volume 0.2/min vs 3.1/min baseline",
        openOffset: 0.5 * DAY, duration: 18 * 60 * 1000,
      },
    ];
    // Use IDs starting high enough not to collide with real nextAnomalyId.
    let seedId = 9000;
    for (const s of seeds) {
      const openedAtMs = now - s.openOffset;
      const resolvedAtMs = openedAtMs + s.duration;
      const a = {
        id: seedId++,
        metric: s.metric,
        detector: "threshold",
        direction: "up",
        openedAtMs,
        lastSeenAtMs: resolvedAtMs - 30000,
        resolvedAtMs,
        baselineValue: null,
        observedValue: null,
        peakValue: null,
        robustZ: null,
        severity: s.severity,
        status: "resolved",
        epoch: SEED_EPOCH,
        note: s.note,
        _clearStreak: 0,
      };
      recentResolved.push(a);
      store.saveAnomaly(a);
      if (a.id >= nextAnomalyId) nextAnomalyId = a.id + 1;
    }
    recentResolved.sort((a, b) => b.openedAtMs - a.openedAtMs);
    recentResolved = recentResolved.slice(0, 50);
    console.log(`[echo-metrics] seeded ${seeds.length} staging anomaly history rows`);
  }

  function getBucket(start, epoch) {
    const key = `${start}:${epoch}`;
    let b = buckets.get(key);
    if (!b) { b = newBucket(start, epoch); buckets.set(key, b); }
    return b;
  }

  function pushLat(bucket, metric, raw) {
    if (raw == null || !Number.isFinite(raw)) return;
    const v = raw < 0 ? 0 : raw; // clamp negatives (clock skew) for the baseline
    const arr = bucket.lat[metric];
    arr.push(v);
    if (arr.length > cfg.reservoirMax) arr.shift();
  }

  // ── ingest one settled echo ──────────────────────────────────────────────
  function recordSample(event) {
    if (!event || event._sampleRecorded) return;
    const status = event.status;
    if (status !== "confirmed" && status !== "failed" && status !== "skipped") return;

    // Backfill-replay short-circuit: a "confirmed" row with no real timing
    // (echoConfirmedTs / requestSeenAtServerMs) came from the respondedRefs
    // replay, not a measured round-trip. Skip it so it doesn't inject
    // zero-latency noise into the baselines.
    if (status === "confirmed" && (event.echoConfirmedTs == null || event.requestSeenAtServerMs == null)) {
      return;
    }
    event._sampleRecorded = true;

    const now = Date.now();
    const bucketStart = Math.floor(now / cfg.bucketMs) * cfg.bucketMs;
    const b = getBucket(bucketStart, currentEpoch);
    b.total++;
    if (status === "confirmed") b.confirmed++;
    else if (status === "failed") b.failed++;
    else if (status === "skipped") b.skipped++;
    b.retrySum += event.retryAttempts || 0;

    let pollLag = null, echoQueue = null, chainRtt = null, rpcSend = null;
    if (status === "confirmed") {
      if (event.requestTs != null) {
        const rawRtt = event.echoConfirmedTs - event.requestTs;
        b.skewSamples++;
        if (rawRtt < -cfg.skewMs) b.skewCount++;
        chainRtt = rawRtt < 0 ? 0 : rawRtt;
        pushLat(b, "chain_rtt_ms", rawRtt);

        const rawLag = event.requestSeenAtServerMs - event.requestTs;
        pollLag = rawLag < 0 ? 0 : rawLag;
        pushLat(b, "poll_lag_ms", rawLag);
      }
      if (event.echoSentAtServerMs != null) {
        echoQueue = Math.max(0, event.echoSentAtServerMs - event.requestSeenAtServerMs);
        pushLat(b, "echo_queue_ms", echoQueue);
      }
      if (event.rpcSendMs != null) {
        rpcSend = Math.max(0, event.rpcSendMs);
        pushLat(b, "rpc_send_ms", rpcSend);
      }
    }

    store.saveSample({
      request_tx_id: event.requestTxId,
      request_from: event.requestFrom || null,
      request_amount: event.requestAmount != null ? event.requestAmount : null,
      echo_amount: event.echoAmount != null ? event.echoAmount : null,
      request_ts: event.requestTs != null ? event.requestTs : null,
      request_seen_ms: event.requestSeenAtServerMs != null ? event.requestSeenAtServerMs : null,
      echo_sent_ms: event.echoSentAtServerMs != null ? event.echoSentAtServerMs : null,
      echo_confirmed_ts: event.echoConfirmedTs != null ? event.echoConfirmedTs : null,
      echo_confirmed_ms: event.echoConfirmedAtServerMs != null ? event.echoConfirmedAtServerMs : null,
      status,
      error: event.error || null,
      retry_attempts: event.retryAttempts || 0,
      poll_lag_ms: pollLag,
      echo_queue_ms: echoQueue,
      chain_rtt_ms: chainRtt,
      rpc_send_ms: rpcSend,
      chain_epoch: currentEpoch,
      bucket_start_ms: bucketStart,
    });
    store.saveBucket(`${bucketStart}:${currentEpoch}`, b);

    // Opportunistic re-evaluation (throttled) so big shifts surface fast.
    if (now - lastOpportunisticAt > 5000) {
      lastOpportunisticAt = now;
      try { evaluate(); } catch (_) {}
    }
  }

  // ── eval-window value for a metric ───────────────────────────────────────
  function evalValue(metric, evalBuckets) {
    const def = METRICS[metric];
    if (def.kind === "latency") {
      const vals = collectReservoir(evalBuckets, metric);
      return vals.length ? median(vals) : null;
    }
    const total = sumField(evalBuckets, "total");
    if (metric === "failure_rate") return total > 0 ? sumField(evalBuckets, "failed") / total : null;
    if (metric === "skip_rate") return total > 0 ? sumField(evalBuckets, "skipped") / total : null;
    if (metric === "retry_rate") return total > 0 ? sumField(evalBuckets, "retrySum") / total : null;
    if (metric === "request_volume") return total / (cfg.evalWindowMs / cfg.bucketMs);
    return null;
  }

  function material(metric, evalVal, med) {
    const def = METRICS[metric];
    if (def.kind === "latency") return evalVal >= med * 1.5 && (evalVal - med) >= 250;
    if (def.kind === "ratio") return Math.abs(evalVal - med) >= 0.1;
    if (metric === "request_volume") return evalVal <= med * 0.5;
    return (evalVal - med) >= 0.5; // retry_rate
  }

  function noteFor(metric, detail) {
    const def = METRICS[metric] || { label: metric, kind: "latency" };
    if (metric === "unconfirmed_age_ms")
      return `oldest unconfirmed echo ${fmtMs(detail.observed)} (≥ ${fmtMs(cfg.stallMs)})`;
    if (metric === "clock_skew")
      return `clock skew: ${detail.observed} samples with node timestamp ahead of server`;
    if (def.kind === "latency")
      return `${def.label} p50 ${fmtMs(detail.observed)} vs ${fmtMs(detail.baseline)} baseline`;
    if (def.kind === "ratio")
      return detail.detector === "threshold"
        ? `${def.label} ${pct(detail.observed)} over last ${Math.round(cfg.evalWindowMs / 60000)}m`
        : `${def.label} ${pct(detail.observed)} vs ${pct(detail.baseline)} baseline`;
    if (metric === "request_volume")
      return `request volume ${detail.observed.toFixed(1)}/min vs ${(detail.baseline || 0).toFixed(1)}/min baseline`;
    return `${def.label} ${detail.observed} vs ${detail.baseline} baseline`;
  }

  function consider(firing, metric, detail) {
    const existing = firing[metric];
    const sev = detail.severity;
    if (!existing || (sev === "critical" && existing.severity !== "critical")) {
      detail.note = noteFor(metric, detail);
      firing[metric] = detail;
    }
  }

  function evaluate() {
    const now = Date.now();
    const evalStart = now - cfg.evalWindowMs;
    const baselineStart = now - cfg.baselineWindowMs;

    // memory prune
    for (const [k, b] of buckets) {
      if (b.epoch === SEED_EPOCH) continue;
      if (b.start < baselineStart - cfg.bucketMs) buckets.delete(k);
    }

    const all = Array.from(buckets.values());
    const evalBuckets = all.filter((b) => b.epoch === currentEpoch && b.start >= evalStart);
    const realBase = all.filter((b) => b.epoch === currentEpoch && b.start < evalStart && b.start >= baselineStart);
    let baseBuckets = realBase;
    if (realBase.length < cfg.minBaselineBuckets) {
      const seed = all.filter((b) => b.epoch === SEED_EPOCH);
      if (seed.length) baseBuckets = realBase.concat(seed);
    }

    const firing = {};

    for (const metric of Object.keys(METRICS)) {
      const series = [];
      for (const b of baseBuckets) { const v = perBucketValue(b, metric); if (v != null) series.push(v); }
      const med = series.length ? median(series) : null;
      const madv = med != null ? mad(series, med) : 0;
      baselinesCache[metric] = { median: med, mad: madv, sampleCount: series.length };

      const ev = evalValue(metric, evalBuckets);
      if (ev == null) continue;
      // cold start: not enough baseline → leave z-score to the threshold nets
      if (series.length < cfg.minBaselineBuckets || madv <= 0) continue;

      const z = 0.6745 * (ev - med) / madv;
      const def = METRICS[metric];
      const dirOk = def.direction === "up" ? z > 0 : z < 0;
      if (dirOk && Math.abs(z) >= cfg.z && material(metric, ev, med)) {
        consider(firing, metric, {
          detector: "zscore", direction: def.direction,
          baseline: med, observed: ev, robustZ: z, severity: "warn",
        });
      }
    }

    // ── absolute-threshold safety nets (baseline-independent) ──
    const settled = sumField(evalBuckets, "total");
    const failed = sumField(evalBuckets, "failed");
    if (settled >= cfg.minSettledFailrate && failed / settled >= cfg.failRate) {
      consider(firing, "failure_rate", {
        detector: "threshold", direction: "up",
        baseline: cfg.failRate, observed: failed / settled, robustZ: null, severity: "critical",
      });
    }

    const rttP50 = percentile(collectReservoir(evalBuckets, "chain_rtt_ms"), 50);
    if (rttP50 != null && rttP50 >= cfg.rttHardMs) {
      consider(firing, "chain_rtt_ms", {
        detector: "threshold", direction: "up",
        baseline: cfg.rttHardMs, observed: rttP50, robustZ: null, severity: "critical",
      });
    }

    // Stall — the primary "no respond?" signal. Read from live events, not the
    // sample store, so a total confirmation stall is caught even when no new
    // samples are being written.
    let probe;
    try { probe = liveProbe() || {}; } catch (_) { probe = {}; }
    const ages = Array.isArray(probe.unconfirmedAges) ? probe.unconfirmedAges : [];
    const maxAge = ages.length ? Math.max.apply(null, ages) : 0;
    if (maxAge >= cfg.stallMs) {
      consider(firing, "unconfirmed_age_ms", {
        detector: "threshold", direction: "up",
        baseline: cfg.stallMs, observed: maxAge, robustZ: null, severity: "critical",
      });
    }

    // Clock skew — low-severity, baseline-protecting.
    const skewCount = sumField(evalBuckets, "skewCount");
    const skewSamples = sumField(evalBuckets, "skewSamples");
    if (skewSamples > 0 && skewCount >= 3 && skewCount / skewSamples >= 0.5) {
      consider(firing, "clock_skew", {
        detector: "threshold", direction: "up",
        baseline: 0, observed: skewCount, robustZ: null, severity: "warn",
      });
    }

    reconcile(firing, now);

    // durable prune (cheap; runs every tick, deletes are bounded)
    store.prune(now - cfg.retentionMs);
    lastEvalAt = now;
    return getHealth();
  }

  function reconcile(firing, now) {
    const stillOpen = [];
    for (const a of openAnomalies) {
      const f = firing[a.metric];
      if (f) {
        a.lastSeenAtMs = now;
        a.observedValue = f.observed;
        a.robustZ = f.robustZ;
        a.note = f.note;
        a.detector = f.detector;
        a.direction = f.direction;
        if (f.severity === "critical") a.severity = "critical";
        a.peakValue = a.direction === "down"
          ? Math.min(a.peakValue != null ? a.peakValue : f.observed, f.observed)
          : Math.max(a.peakValue != null ? a.peakValue : f.observed, f.observed);
        a._clearStreak = 0;
        store.saveAnomaly(a);
        delete firing[a.metric];
        stillOpen.push(a);
      } else {
        a._clearStreak = (a._clearStreak || 0) + 1;
        if (a._clearStreak >= cfg.clearStreak) {
          a.status = "resolved";
          a.resolvedAtMs = now;
          store.saveAnomaly(a);
          recentResolved.unshift(a);
        } else {
          stillOpen.push(a);
        }
      }
    }
    // brand-new anomalies
    for (const metric of Object.keys(firing)) {
      const f = firing[metric];
      const a = {
        id: nextAnomalyId++, metric, detector: f.detector, direction: f.direction,
        openedAtMs: now, lastSeenAtMs: now, resolvedAtMs: null,
        baselineValue: f.baseline != null ? f.baseline : null,
        observedValue: f.observed, peakValue: f.observed, robustZ: f.robustZ,
        severity: f.severity, status: "open", epoch: currentEpoch, note: f.note, _clearStreak: 0,
      };
      stillOpen.push(a);
      store.saveAnomaly(a);
      console.log(`[echo-metrics] ANOMALY ${a.severity} ${metric} — ${a.note}`);
    }
    openAnomalies = stillOpen;
    recentResolved = recentResolved.slice(0, 50);
  }

  // ── chain reset → new epoch ──────────────────────────────────────────────
  function setEpoch(newEpoch) {
    if (!newEpoch || newEpoch === currentEpoch) return;
    const now = Date.now();
    // Auto-resolve everything from the prior epoch — the new chain may have
    // entirely different latency characteristics, so old anomalies are moot.
    for (const a of openAnomalies) {
      a.status = "resolved";
      a.resolvedAtMs = now;
      a.note = (a.note ? a.note + " · " : "") + "chain reset";
      store.saveAnomaly(a);
      recentResolved.unshift(a);
    }
    openAnomalies = [];
    recentResolved = recentResolved.slice(0, 50);
    currentEpoch = newEpoch;
    console.log(`[echo-metrics] chain epoch → ${newEpoch}; baselines restart for the new epoch`);
  }

  function setLiveProbe(fn) { if (typeof fn === "function") liveProbe = fn; }

  // ── public read surfaces ─────────────────────────────────────────────────
  function publicAnomaly(a) {
    return {
      id: a.id, metric: a.metric, detector: a.detector, direction: a.direction,
      severity: a.severity, status: a.status, note: a.note,
      openedAtMs: a.openedAtMs, lastSeenAtMs: a.lastSeenAtMs, resolvedAtMs: a.resolvedAtMs,
      baselineValue: a.baselineValue, observedValue: a.observedValue,
      peakValue: a.peakValue, robustZ: a.robustZ, chainEpoch: a.epoch,
    };
  }

  function getHealth() {
    if (!openAnomalies.length) return { degraded: false, openAnomalyCount: 0, worst: null };
    let worst = null, worstP = 0;
    for (const a of openAnomalies) {
      const cat = CATEGORY[a.metric] || "latency";
      const p = CATEGORY_PRIORITY[cat] || 0;
      // critical outranks category priority for picking the headline
      const score = p + (a.severity === "critical" ? 10 : 0);
      if (score > worstP) { worstP = score; worst = cat; }
    }
    return { degraded: true, openAnomalyCount: openAnomalies.length, worst };
  }

  function getAnomaliesResponse() {
    const dataPoints = Array.from(buckets.values()).filter((b) => b.epoch === currentEpoch).length;
    return {
      persistent: store.persistent,
      chainEpoch: currentEpoch,
      dataPoints,
      lastEvalAt,
      thresholds: {
        z: cfg.z, stallMs: cfg.stallMs, rttHardMs: cfg.rttHardMs,
        failRate: cfg.failRate, minBaselineBuckets: cfg.minBaselineBuckets,
      },
      openAnomalies: openAnomalies.map(publicAnomaly),
      recentResolved: recentResolved.slice(0, 25).map(publicAnomaly),
      baselines: baselinesCache,
    };
  }

  function getMetricsResponse() {
    const out = {};
    const sorted = Array.from(buckets.values())
      .filter((b) => b.epoch === currentEpoch)
      .sort((a, b) => a.start - b.start);
    for (const metric of Object.keys(METRICS)) {
      out[metric] = sorted.map((b) => ({ t: b.start, v: perBucketValue(b, metric), n: b.total }))
        .filter((p) => p.v != null);
    }
    return { chainEpoch: currentEpoch, persistent: store.persistent, bucketMs: cfg.bucketMs, series: out };
  }

  // Rows shaped for the operator /status page (registerPending contract).
  function getStatusRows() {
    const now = Date.now();
    return openAnomalies
      .slice()
      .sort((a, b) => (b.severity === "critical" ? 1 : 0) - (a.severity === "critical" ? 1 : 0) || a.openedAtMs - b.openedAtMs)
      .map((a) => ({
        id: a.metric,
        kind: "anomaly",
        fromOrTo: (METRICS[a.metric] && METRICS[a.metric].label) || a.metric,
        amount: null,
        status: a.severity,
        ageMs: now - a.openedAtMs,
        error: a.note,
        note: `${a.detector}${a.robustZ != null ? ` · z=${a.robustZ.toFixed(1)}` : ""}`,
      }));
  }

  function start() {
    if (evalTimer) return;
    evalTimer = setInterval(() => { try { evaluate(); } catch (e) { console.error("[echo-metrics] evaluate error:", e.message); } }, cfg.evalIntervalMs);
    if (evalTimer.unref) evalTimer.unref();
  }
  function stop() { if (evalTimer) { clearInterval(evalTimer); evalTimer = null; } store.close(); }

  return {
    recordSample, evaluate, start, stop, setEpoch, setLiveProbe,
    getAnomaliesResponse, getMetricsResponse, getStatusRows, getHealth,
    get persistent() { return store.persistent; },
    get epoch() { return currentEpoch; },
  };
}

module.exports = createEchoMetrics;
