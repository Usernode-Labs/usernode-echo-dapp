"use strict";

// Unit tests for the per-user stats query + staging seed in echo-store.js.
// No live Postgres: a fake pool is injected via opts.pool so the JS wiring
// (param passing, BIGINT-as-string normalization, idempotency, gating) is
// exercised directly.

const test = require("node:test");
const assert = require("node:assert");
const createEchoStore = require("../echo-store");

// Minimal fake pg pool. Routes queries by SQL substring; records UPSERT params.
function makeFakePool({ aggregateRow, existsRowCount = 0 } = {}) {
  const calls = { inserts: [], queries: [] };
  // In-memory favorites store so the favorites round-trip tests work.
  const favorites = new Map(); // "owner|target" -> true
  return {
    calls,
    favorites,
    on() {},
    async end() {},
    async query(sql, params) {
      calls.queries.push({ sql, params });
      if (/CREATE TABLE|COMMENT ON TABLE|CREATE INDEX/i.test(sql)) return { rows: [], rowCount: 0 };
      if (/SELECT 1 FROM echo_events/i.test(sql)) {
        return { rows: existsRowCount ? [{ "?column?": 1 }] : [], rowCount: existsRowCount };
      }
      if (/INSERT INTO echo_events/i.test(sql)) {
        calls.inserts.push(params);
        return { rows: [], rowCount: 1 };
      }
      if (/WITH recent AS/i.test(sql)) {
        return { rows: [aggregateRow || {}] };
      }
      // Favorites queries
      if (/INSERT INTO echo_favorites/i.test(sql)) {
        const key = params[0] + "|" + params[1];
        const isNew = !favorites.has(key);
        favorites.set(key, true);
        return { rows: [], rowCount: isNew ? 1 : 0 };
      }
      if (/DELETE FROM echo_favorites/i.test(sql)) {
        const key = params[0] + "|" + params[1];
        const had = favorites.has(key);
        favorites.delete(key);
        return { rows: [], rowCount: had ? 1 : 0 };
      }
      if (/SELECT target_address FROM echo_favorites/i.test(sql)) {
        const owner = params[0];
        const rows = [];
        for (const key of favorites.keys()) {
          const [o, t] = key.split("|");
          if (o === owner) rows.push({ target_address: t });
        }
        return { rows, rowCount: rows.length };
      }
      return { rows: [], rowCount: 0 };
    },
  };
}

test("queryUserStats normalizes BIGINT strings and computes success rate", async () => {
  const pool = makeFakePool({
    aggregateRow: {
      total: 5, confirmed: 3, failed: 1, skipped: 1, in_flight: 2,
      // pg returns BIGINT/numeric as strings.
      avg_total: "1500", min_total: "1000", max_total: "2000",
    },
  });
  const store = createEchoStore({ pool, databaseUrl: "x" });
  await store.init();
  assert.equal(store.isReady(), true);

  const stats = await store.queryUserStats("chainA", "ut1abcdef", 1000);
  assert.equal(stats.total, 5);
  assert.equal(stats.confirmed, 3);
  assert.equal(stats.failed, 1);
  assert.equal(stats.skipped, 1);
  assert.equal(stats.inFlight, 2);
  assert.equal(stats.successRate, 3 / 5);
  assert.equal(stats.avgTotalMs, 1500);
  assert.equal(stats.minTotalMs, 1000);
  assert.equal(stats.maxTotalMs, 2000);
  assert.equal(typeof stats.avgTotalMs, "number");

  // chain_id + address are passed through as the scoping params.
  const aggCall = pool.calls.queries.find((q) => /WITH recent AS/i.test(q.sql));
  assert.deepEqual(aggCall.params.slice(0, 2), ["chainA", "ut1abcdef"]);
});

test("queryUserStats returns null successRate for a sender with no rows", async () => {
  const pool = makeFakePool({
    aggregateRow: {
      total: 0, confirmed: 0, failed: 0, skipped: 0, in_flight: 0,
      avg_total: null, min_total: null, max_total: null,
    },
  });
  const store = createEchoStore({ pool, databaseUrl: "x" });
  await store.init();
  const stats = await store.queryUserStats("chainA", "ut1nobody", 1000);
  assert.equal(stats.total, 0);
  assert.equal(stats.successRate, null);
  assert.equal(stats.avgTotalMs, null);
  assert.equal(stats.minTotalMs, null);
  assert.equal(stats.maxTotalMs, null);
});

test("queryUserStats is a no-op (null) without an address", async () => {
  const pool = makeFakePool({});
  const store = createEchoStore({ pool, databaseUrl: "x" });
  await store.init();
  assert.equal(await store.queryUserStats("chainA", "", 1000), null);
});

test("recordIdentity caches the username and upserts echo_identities", async () => {
  const pool = makeFakePool({});
  const store = createEchoStore({ pool, databaseUrl: "x" });
  await store.init();

  store.recordIdentity("ut1alice", "alice");
  // Allow the fire-and-forget promise chain to flush.
  await new Promise((r) => setImmediate(r));

  assert.equal(store.lookupIdentity("ut1alice"), "alice");
  const idIns = pool.calls.queries.find((q) => /INSERT INTO echo_identities/i.test(q.sql));
  assert.ok(idIns, "expected an echo_identities upsert");
  assert.deepEqual(idIns.params, ["ut1alice", "alice"]);
  const backfill = pool.calls.queries.find((q) => /UPDATE echo_events SET username/i.test(q.sql));
  assert.ok(backfill, "expected an echo_events backfill update");
});

test("recordIdentity skips the DB write when the username is unchanged", async () => {
  const pool = makeFakePool({});
  const store = createEchoStore({ pool, databaseUrl: "x" });
  await store.init();

  store.recordIdentity("ut1bob", "bob");
  await new Promise((r) => setImmediate(r));
  const before = pool.calls.queries.filter((q) => /echo_identities/i.test(q.sql)).length;

  store.recordIdentity("ut1bob", "bob"); // same value → no new DB call
  await new Promise((r) => setImmediate(r));
  const after = pool.calls.queries.filter((q) => /echo_identities/i.test(q.sql)).length;

  assert.equal(before, after);
});

test("recordIdentity ignores blank addresses and usernames", async () => {
  const pool = makeFakePool({});
  const store = createEchoStore({ pool, databaseUrl: "x" });
  await store.init();
  store.recordIdentity("", "alice");
  store.recordIdentity("ut1x", "  ");
  store.recordIdentity("ut1x", null);
  await new Promise((r) => setImmediate(r));
  assert.equal(store.lookupIdentity("ut1x"), null);
  assert.equal(pool.calls.queries.some((q) => /INSERT INTO echo_identities/i.test(q.sql)), false);
});

test("persist stamps the cached username onto the echo_events row", async () => {
  const pool = makeFakePool({});
  const store = createEchoStore({ pool, databaseUrl: "x" });
  await store.init();
  store.recordIdentity("ut1carol", "carol");
  await new Promise((r) => setImmediate(r));

  store.persist(
    { requestTxId: "tx1", requestFrom: "ut1carol", requestAmount: 10, status: "confirmed" },
    "chainA"
  );
  await new Promise((r) => setImmediate(r));

  const upsert = pool.calls.inserts.find((p) => p[0] === "tx1");
  assert.ok(upsert, "expected the event upsert");
  // username is the last positional param (COLS appends it after echo_block_hash).
  assert.equal(upsert[upsert.length - 1], "carol");
});

test("seedStagingDemo inserts the demo mix when staging + empty", async () => {
  const pool = makeFakePool({ existsRowCount: 0 });
  const store = createEchoStore({ pool, databaseUrl: "x", isStaging: true });
  await store.init();
  await store.seedStagingDemo("chainA", "ut1demo");

  // status lives at COLS index 5 (request_tx_id, chain_id, request_from,
  // request_amount, echo_amount, status, ...).
  const statuses = pool.calls.inserts.map((p) => p[5]);
  const fromAddrs = new Set(pool.calls.inserts.map((p) => p[2]));
  const chainIds = new Set(pool.calls.inserts.map((p) => p[1]));

  assert.equal(statuses.length, 25);
  assert.equal(statuses.filter((s) => s === "confirmed").length, 20);
  assert.equal(statuses.filter((s) => s === "failed").length, 3);
  assert.equal(statuses.filter((s) => s === "skipped").length, 1);
  assert.equal(statuses.filter((s) => s === "echoing").length, 1);
  assert.deepEqual([...fromAddrs], ["ut1demo"]);
  assert.deepEqual([...chainIds], ["chainA"]);
});

test("seedStagingDemo is idempotent when rows already exist", async () => {
  const pool = makeFakePool({ existsRowCount: 1 });
  const store = createEchoStore({ pool, databaseUrl: "x", isStaging: true });
  await store.init();
  await store.seedStagingDemo("chainA", "ut1demo");
  assert.equal(pool.calls.inserts.length, 0);
});

test("seedStagingDemo is a strict no-op outside staging", async () => {
  const pool = makeFakePool({ existsRowCount: 0 });
  const store = createEchoStore({ pool, databaseUrl: "x", isStaging: false });
  await store.init();
  await store.seedStagingDemo("chainA", "ut1demo");
  assert.equal(pool.calls.inserts.length, 0);
});

// ── echo_favorites ────────────────────────────────────────────────────────────

test("getFavorites returns [] when store is not ready", async () => {
  const store = createEchoStore({ databaseUrl: "", isStaging: false });
  // No init() — store stays disabled.
  const favs = await store.getFavorites("ut1owner");
  assert.deepEqual(favs, []);
});

test("addFavorite/getFavorites/removeFavorite round-trip", async () => {
  const pool = makeFakePool({});
  const store = createEchoStore({ pool, databaseUrl: "x" });
  await store.init();

  await store.addFavorite("ut1owner", "ut1target");
  let favs = await store.getFavorites("ut1owner");
  assert.ok(favs.includes("ut1target"), "should contain the added target");

  await store.removeFavorite("ut1owner", "ut1target");
  favs = await store.getFavorites("ut1owner");
  assert.equal(favs.length, 0, "should be empty after removal");
});

test("addFavorite is idempotent (ON CONFLICT DO NOTHING)", async () => {
  const pool = makeFakePool({});
  const store = createEchoStore({ pool, databaseUrl: "x" });
  await store.init();

  await store.addFavorite("ut1owner", "ut1target");
  await store.addFavorite("ut1owner", "ut1target"); // second add — no throw, no duplicate
  const favs = await store.getFavorites("ut1owner");
  assert.equal(favs.filter((a) => a === "ut1target").length, 1, "only one entry");
});

test("getFavorites scopes to the requesting owner", async () => {
  const pool = makeFakePool({});
  const store = createEchoStore({ pool, databaseUrl: "x" });
  await store.init();

  await store.addFavorite("ut1alice", "ut1shared");
  await store.addFavorite("ut1bob", "ut1shared");

  const aliceFavs = await store.getFavorites("ut1alice");
  const bobFavs   = await store.getFavorites("ut1bob");
  assert.ok(aliceFavs.includes("ut1shared"));
  assert.ok(bobFavs.includes("ut1shared"));
  // Neither owner's list bleeds into the other.
  assert.equal(aliceFavs.length, 1);
  assert.equal(bobFavs.length, 1);
});

test("seedStagingFavorites is a no-op outside staging", async () => {
  const pool = makeFakePool({});
  const store = createEchoStore({ pool, databaseUrl: "x", isStaging: false });
  await store.init();
  await store.seedStagingFavorites("ut1demoowner");
  // No INSERT INTO echo_favorites queries should have been issued.
  const favInserts = pool.calls.queries.filter((q) => /INSERT INTO echo_favorites/i.test(q.sql));
  assert.equal(favInserts.length, 0);
});

test("seedStagingFavorites inserts two demo favorites in staging", async () => {
  const pool = makeFakePool({});
  const store = createEchoStore({ pool, databaseUrl: "x", isStaging: true });
  await store.init();
  await store.seedStagingFavorites("ut1demoowner");
  const favs = await store.getFavorites("ut1demoowner");
  assert.ok(favs.includes("staging-demo-gamma"), "should contain staging-demo-gamma");
  assert.ok(favs.includes("staging-demo-beta"), "should contain staging-demo-beta");
  assert.equal(favs.length, 2);
});
