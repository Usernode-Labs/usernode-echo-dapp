/**
 * usernode-usernames.js — Global username system for Usernode dapps.
 *
 * Include after usernode-bridge.js. Provides UsernodeUsernames on window:
 *   await UsernodeUsernames.init()
 *   await UsernodeUsernames.setUsername("alice")
 *   UsernodeUsernames.getUsernameSync(pubkey)
 *
 * All dapps share one usernames address. Set your name once, every dapp sees it.
 */
(function () {
  "use strict";

  var USERNAMES_PUBKEY =
    window.localStorage.getItem("usernode:usernames_pubkey") ||
    "ut1p0p7y8ujacndc60r4a7pzk45dufdtarp6satvc0md7866633u8sqagm3az";

  var TX_SEND_OPTS = { timeoutMs: 90000, pollIntervalMs: 1500 };
  var CACHE_TTL_MS = 30000;

  var cache = new Map();
  var lastFetch = 0;
  var fetchPromise = null;
  var myAddress = null;

  /* ── Helpers ──────────────────────────────────────────── */

  function last6(addr) {
    return addr ? addr.slice(-6) : "";
  }

  function usernameSuffix(addr) {
    return addr ? "_" + last6(addr) : "_unknown";
  }

  function defaultUsername(addr) {
    return addr ? "user_" + last6(addr) : "user";
  }

  function normalizeUsername(raw, addr) {
    var suffix = usernameSuffix(addr);
    var maxBase = Math.max(1, 24 - suffix.length);
    var v = String(raw || "")
      .trim()
      .replace(/[^\w-]/g, "");
    if (!v) return defaultUsername(addr);
    if (v.endsWith(suffix)) v = v.slice(0, -suffix.length);
    v = v.replace(/_[A-Za-z0-9]{6}$/, "");
    return (v.slice(0, maxBase) || "user") + suffix;
  }

  /* ── Transaction parsing ─────────────────────────────── */

  function parseMemo(m) {
    if (m == null) return null;
    try {
      return JSON.parse(String(m));
    } catch (_) {
      return null;
    }
  }

  function extractTimestamp(tx) {
    var candidates = [
      tx.timestamp_ms,
      tx.created_at,
      tx.createdAt,
      tx.timestamp,
      tx.time,
    ];
    for (var i = 0; i < candidates.length; i++) {
      var v = candidates[i];
      if (typeof v === "number" && Number.isFinite(v))
        return v < 10000000000 ? v * 1000 : v;
      if (typeof v === "string" && v.trim()) {
        var t = Date.parse(v);
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
      amount: tx.amount || null,
      memo: tx.memo != null ? String(tx.memo) : null,
      ts: extractTimestamp(tx) || Date.now(),
    };
  }

  /* ── Core fetch ──────────────────────────────────────── */

  function fetchUsernameTxs() {
    return window
      .getTransactions({ limit: 500, account: USERNAMES_PUBKEY })
      .then(function (data) {
        var items = data.items || [];
        for (var i = 0; i < items.length; i++) {
          var tx = normalizeTx(items[i]);
          if (!tx || !tx.from || tx.to !== USERNAMES_PUBKEY) continue;
          var memo = parseMemo(tx.memo);
          if (
            !memo ||
            memo.app !== "usernames" ||
            memo.type !== "set_username"
          )
            continue;
          var name = normalizeUsername(memo.username, tx.from);
          var prev = cache.get(tx.from);
          if (!prev || tx.ts >= prev.ts) {
            cache.set(tx.from, { name: name, ts: tx.ts });
          }
        }
        lastFetch = Date.now();
      })
      .catch(function (e) {
        console.warn("UsernodeUsernames: fetch failed:", e.message || e);
      });
  }

  function ensureFresh() {
    if (Date.now() - lastFetch < CACHE_TTL_MS)
      return Promise.resolve();
    if (fetchPromise) return fetchPromise;
    fetchPromise = fetchUsernameTxs().then(
      function () { fetchPromise = null; },
      function () { fetchPromise = null; }
    );
    return fetchPromise;
  }

  /* ── Public API ──────────────────────────────────────── */

  window.UsernodeUsernames = {
    USERNAMES_PUBKEY: USERNAMES_PUBKEY,

    defaultUsername: defaultUsername,
    usernameSuffix: usernameSuffix,
    normalizeUsername: normalizeUsername,

    init: function () {
      return window
        .getNodeAddress()
        .then(function (addr) {
          myAddress = addr || null;
        })
        .catch(function () {})
        .then(fetchUsernameTxs);
    },

    getMyAddress: function () {
      return myAddress;
    },

    getUsername: function (pubkey) {
      return ensureFresh().then(function () {
        var entry = cache.get(pubkey);
        return entry ? entry.name : defaultUsername(pubkey);
      });
    },

    getUsernameSync: function (pubkey) {
      var entry = cache.get(pubkey);
      return entry ? entry.name : defaultUsername(pubkey);
    },

    getAllUsernamesSync: function () {
      var map = {};
      cache.forEach(function (v, k) {
        map[k] = v.name;
      });
      return map;
    },

    setUsername: function (baseName) {
      var p = myAddress
        ? Promise.resolve(myAddress)
        : window.getNodeAddress().then(function (a) {
            myAddress = a;
            return a;
          });

      return p.then(function (addr) {
        var value = normalizeUsername(baseName, addr);
        var memo = JSON.stringify({
          app: "usernames",
          type: "set_username",
          username: value,
        });
        if (memo.length > 1024) throw new Error("Username too long");
        return window
          .sendTransaction(USERNAMES_PUBKEY, 1, memo, TX_SEND_OPTS)
          .then(function () {
            cache.set(addr, { name: value, ts: Date.now() });
            return value;
          });
      });
    },

    refresh: function () {
      lastFetch = 0;
      return fetchUsernameTxs();
    },

    /**
     * Import legacy per-app usernames as fallback entries.
     * Only sets a name if no global username exists for that pubkey.
     */
    importLegacy: function (legacyMap) {
      if (!legacyMap) return;
      var entries =
        legacyMap instanceof Map
          ? Array.from(legacyMap.entries())
          : Object.entries(legacyMap);
      for (var i = 0; i < entries.length; i++) {
        var pubkey = entries[i][0];
        var name = entries[i][1];
        if (typeof name === "object" && name !== null) name = name.name;
        if (!cache.has(pubkey) && name) {
          cache.set(pubkey, { name: String(name), ts: 0 });
        }
      }
    },
  };
})();
