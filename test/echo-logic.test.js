"use strict";

// Unit tests for the in-memory per-user stats fallback + the public
// /__echo/my-stats endpoint handler. Uses a not-ready store stub so the
// computeUserStatsFromMemory path is exercised without a live DB.

const test = require("node:test");
const assert = require("node:assert");
const createEcho = require("../echo-logic");
const { isValidStatsAddress, emptyUserStats } = createEcho;

// Store stub that's never "ready" → handlers fall back to the in-memory Map.
function notReadyStore() {
  return {
    async init() {},
    isReady() { return false; },
    persist() {},
    async hydrate() { return []; },
    async queryHistory() { return null; },
    async queryStats() { return null; },
    async queryUserStats() { return null; },
    async seedStagingDemo() {},
    async close() {},
  };
}

function makeEcho() {
  return createEcho({
    appPubkey: "ut1echoapp",
    localDev: true,
    store: notReadyStore(),
  });
}

function ev(over) {
  return Object.assign(
    {
      requestTxId: "t" + Math.random().toString(36).slice(2),
      requestFrom: "ut1userA",
      status: "confirmed",
      requestTs: 1000,
      echoConfirmedTs: 2000,
    },
    over
  );
}

function mockRes() {
  return {
    statusCode: null,
    headers: null,
    body: "",
    ended: false,
    writeHead(code, hdrs) { this.statusCode = code; this.headers = hdrs; },
    end(b) { if (b != null) this.body = String(b); this.ended = true; },
  };
}

test("isValidStatsAddress accepts ut1 pubkeys and rejects junk", () => {
  assert.equal(isValidStatsAddress("ut1abcdefg"), true);
  assert.equal(isValidStatsAddress(""), false);
  assert.equal(isValidStatsAddress("nope"), false);
  assert.equal(isValidStatsAddress(null), false);
  assert.equal(isValidStatsAddress("ut1"), false); // too short
});

test("computeUserStatsFromMemory buckets, filters by sender, and computes latency", () => {
  const echo = makeEcho();
  const { events, computeUserStatsFromMemory } = echo._test;

  // User A: 3 confirmed (latencies 1000/3000/5000), 1 failed, 1 skipped, 1 echoing.
  events.set("a1", ev({ requestFrom: "ut1userA", status: "confirmed", requestTs: 0, echoConfirmedTs: 1000 }));
  events.set("a2", ev({ requestFrom: "ut1userA", status: "confirmed", requestTs: 0, echoConfirmedTs: 3000 }));
  events.set("a3", ev({ requestFrom: "ut1userA", status: "confirmed", requestTs: 0, echoConfirmedTs: 5000 }));
  events.set("a4", ev({ requestFrom: "ut1userA", status: "failed", echoConfirmedTs: null }));
  events.set("a5", ev({ requestFrom: "ut1userA", status: "skipped", echoConfirmedTs: null }));
  events.set("a6", ev({ requestFrom: "ut1userA", status: "echoing", echoConfirmedTs: null }));
  // User B: noise that must not leak into A's aggregate.
  events.set("b1", ev({ requestFrom: "ut1userB", status: "confirmed", requestTs: 0, echoConfirmedTs: 9000 }));

  const s = computeUserStatsFromMemory("ut1userA");
  assert.equal(s.total, 5); // 3 confirmed + 1 failed + 1 skipped (echoing excluded)
  assert.equal(s.confirmed, 3);
  assert.equal(s.failed, 1);
  assert.equal(s.skipped, 1);
  assert.equal(s.inFlight, 1);
  assert.equal(s.successRate, 3 / 5);
  assert.equal(s.avgTotalMs, (1000 + 3000 + 5000) / 3);
  assert.equal(s.minTotalMs, 1000);
  assert.equal(s.maxTotalMs, 5000);
});

test("computeUserStatsFromMemory clamps negative skew and nulls latency when unconfirmed", () => {
  const echo = makeEcho();
  const { events, computeUserStatsFromMemory } = echo._test;
  events.set("n1", ev({ requestFrom: "ut1skewuser", status: "confirmed", requestTs: 5000, echoConfirmedTs: 4000 }));
  const s1 = computeUserStatsFromMemory("ut1skewuser");
  assert.equal(s1.minTotalMs, 0); // clamped, not -1000
  assert.equal(s1.maxTotalMs, 0);

  events.set("f1", ev({ requestFrom: "ut1noneuser", status: "failed", echoConfirmedTs: null }));
  const s2 = computeUserStatsFromMemory("ut1noneuser");
  assert.equal(s2.total, 1);
  assert.equal(s2.confirmed, 0);
  assert.equal(s2.successRate, 0);
  assert.equal(s2.avgTotalMs, null);
  assert.equal(s2.minTotalMs, null);
  assert.equal(s2.maxTotalMs, null);
});

test("computeUserStatsFromMemory returns zeroed stats for an unknown sender", () => {
  const echo = makeEcho();
  const s = echo._test.computeUserStatsFromMemory("ut1ghost");
  assert.deepEqual(s, emptyUserStats());
});

test("handleUserStats returns zeroed payload for missing/invalid address", async () => {
  const echo = makeEcho();
  const res = mockRes();
  await echo._test.handleUserStats({ url: "/__echo/my-stats", method: "GET" }, res);
  const body = JSON.parse(res.body);
  assert.equal(body.address, null);
  assert.equal(body.total, 0);
  assert.equal(body.successRate, null);
});

test("handleUserStats is public and no-store, and scopes to the address", async () => {
  const echo = makeEcho();
  echo._test.events.set("a1", ev({ requestFrom: "ut1userA", status: "confirmed", requestTs: 0, echoConfirmedTs: 2000 }));
  const res = mockRes();
  await echo._test.handleUserStats(
    { url: "/__echo/my-stats?address=ut1userA", method: "GET" },
    res
  );
  assert.equal(res.statusCode, 200);
  // Public posture: permissive CORS + no caching (same as /__echo/state).
  assert.equal(res.headers["Cache-Control"], "no-store");
  assert.equal(res.headers["Access-Control-Allow-Origin"], "*");
  const body = JSON.parse(res.body);
  assert.equal(body.address, "ut1userA");
  assert.equal(body.total, 1);
  assert.equal(body.confirmed, 1);
  assert.equal(body.avgTotalMs, 2000);
});

test("handleRequest routes /__echo/my-stats", async () => {
  const echo = makeEcho();
  const res = mockRes();
  const handled = echo.handleRequest(
    { url: "/__echo/my-stats?address=ut1userA", method: "GET" },
    res,
    "/__echo/my-stats"
  );
  assert.equal(handled, true);
  // handler runs async; give the microtask queue a tick to flush.
  await new Promise((r) => setImmediate(r));
  assert.equal(res.ended, true);
  assert.equal(res.headers["Cache-Control"], "no-store");
});
