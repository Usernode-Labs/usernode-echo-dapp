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
  return {
    calls,
    on() {},
    async end() {},
    async query(sql, params) {
      calls.queries.push({ sql, params });
      if (/CREATE TABLE/i.test(sql)) return { rows: [], rowCount: 0 };
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
