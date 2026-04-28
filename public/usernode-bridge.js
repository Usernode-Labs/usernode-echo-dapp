/**
 * usernode-bridge.js
 *
 * Included by dapps to access Usernode-provided APIs when running inside the
 * mobile app WebView. When running in a normal browser, it provides stubbed
 * implementations so local development still works.
 *
 * Three operating modes:
 *   1. Native mode — inside the Flutter WebView (Usernode.postMessage).
 *   2. Mock mode   — server runs --local-dev, /__mock/enabled returns 200.
 *   3. QR mode     — desktop browser, no native bridge, no mock.
 *                    sendTransaction shows a QR code for the user to scan
 *                    with the mobile app, then polls for on-chain inclusion.
 *
 * Mock-mode detection: when the server runs with --local-dev, it exposes
 * /__mock/enabled. If that endpoint responds 200, ALL transaction calls go
 * through mock endpoints — even inside the Flutter WebView. This lets
 * developers test dapps on-device without hitting the real chain.
 */

(function () {
  window.usernode = window.usernode || {};
  // "dapp mode" (inside the Flutter WebView) exposes a JS channel object named
  // `Usernode` with a `postMessage` function.
  window.usernode.isNative =
    !!window.Usernode && typeof window.Usernode.postMessage === "function";

  // ── Configuration for QR/desktop mode ─────────────────────────────────
  // Apps call window.usernode.configure({ address: "ut1..." }) to set the
  // user's public key for getNodeAddress() in non-native environments.
  var _configuredAddress = null;

  window.usernode.configure = function configure(opts) {
    if (opts && typeof opts.address === "string" && opts.address.trim()) {
      _configuredAddress = opts.address.trim();
    }
  };

  // Shared promise bridge for native calls (Flutter resolves via
  // `window.__usernodeResolve(id, value, error)`).
  window.__usernodeBridge = window.__usernodeBridge || { pending: {} };
  window.__usernodeResolve = function (id, value, error) {
    var entry = window.__usernodeBridge.pending[id];
    if (!entry) return;
    delete window.__usernodeBridge.pending[id];
    if (error) entry.reject(new Error(error));
    else entry.resolve(value);
  };

  function callNative(method, args) {
    var id = String(Date.now()) + "-" + Math.random().toString(16).slice(2);
    return new Promise(function (resolve, reject) {
      window.__usernodeBridge.pending[id] = { resolve: resolve, reject: reject };
      if (!window.usernode.isNative) {
        delete window.__usernodeBridge.pending[id];
        reject(new Error("Usernode native bridge not available"));
        return;
      }
      window.Usernode.postMessage(JSON.stringify({ method: method, id: id, args: args || {} }));
    });
  }

  function sleep(ms) {
    return new Promise(function (resolve) { setTimeout(resolve, ms); });
  }

  function normalizeTransactionsResponse(resp) {
    if (Array.isArray(resp)) return resp;
    if (!resp || typeof resp !== "object") return [];
    if (Array.isArray(resp.items)) return resp.items;
    if (Array.isArray(resp.transactions)) return resp.transactions;
    if (resp.data && Array.isArray(resp.data.items)) return resp.data.items;
    return [];
  }

  /**
   * Merge the matched on-chain transaction (returned by waitForTransactionVisible)
   * into the sendResult so dapp code can extract the on-chain tx id via
   * sendResult.tx.tx_id even when the underlying transport (e.g. the Flutter
   * native bridge) only resolved with `{ queued, error }`. Without this, dapps
   * that need to correlate their submission with server-side state by tx id
   * can't, because the transport's response gets propagated unchanged.
   */
  function attachMatchedTx(sendResult, matchedTx) {
    if (!matchedTx) return sendResult;
    if (sendResult == null) return { queued: true, tx: matchedTx };
    if (typeof sendResult !== "object") return sendResult;
    if (sendResult.tx) return sendResult;
    sendResult.tx = matchedTx;
    return sendResult;
  }

  function extractTxId(sendResult) {
    if (!sendResult) return null;
    var candidates = [];
    if (typeof sendResult === "string") candidates.push(sendResult);
    if (typeof sendResult === "object") {
      candidates.push(
        sendResult.txid,
        sendResult.txId,
        sendResult.hash,
        sendResult.tx_hash,
        sendResult.txHash,
        sendResult.id
      );
      if (sendResult.tx && typeof sendResult.tx === "object") {
        candidates.push(
          sendResult.tx.id,
          sendResult.tx.txid,
          sendResult.tx.txId,
          sendResult.tx.hash,
          sendResult.tx.tx_hash,
          sendResult.tx.txHash
        );
      }
    }
    for (var i = 0; i < candidates.length; i++) {
      var v = candidates[i];
      if (typeof v === "string" && v.trim()) return v.trim();
    }
    return null;
  }

  function extractTxTimestampMs(tx) {
    if (!tx || typeof tx !== "object") return null;
    var candidates = [
      tx.timestamp_ms,
      tx.created_at,
      tx.createdAt,
      tx.timestamp,
      tx.time,
      tx.seen_at,
      tx.seenAt,
    ];
    for (var i = 0; i < candidates.length; i++) {
      var v = candidates[i];
      if (typeof v === "number" && Number.isFinite(v)) {
        return v < 10000000000 ? v * 1000 : v;
      }
      if (typeof v === "string" && v.trim()) {
        var t = Date.parse(v);
        if (!Number.isNaN(t)) return t;
      }
    }
    return null;
  }

  function pickFirst(obj, keys) {
    for (var i = 0; i < keys.length; i++) {
      if (obj[keys[i]] != null) return obj[keys[i]];
    }
    return null;
  }

  function txMatches(tx, expected) {
    if (!tx || typeof tx !== "object") return false;

    if (expected.txId) {
      var txIdCandidates = [
        tx.id, tx.txid, tx.txId, tx.tx_id, tx.hash, tx.tx_hash, tx.txHash,
      ]
        .filter(function (v) { return typeof v === "string"; })
        .map(function (v) { return v.trim(); })
        .filter(Boolean);
      if (txIdCandidates.indexOf(expected.txId) >= 0) return true;
    }

    if (typeof expected.minCreatedAtMs === "number") {
      var txTime = extractTxTimestampMs(tx);
      if (typeof txTime === "number") {
        var SKEW_MS = 5000;
        if (txTime < expected.minCreatedAtMs - SKEW_MS) return false;
      }
    }

    if (expected.memo != null) {
      var memo = tx.memo == null ? null : String(tx.memo);
      if (memo !== expected.memo) return false;
    }
    if (expected.destination_pubkey != null) {
      var raw = pickFirst(tx, ["destination_pubkey", "destination", "to"]);
      var dest = raw == null ? null : String(raw);
      if (dest !== expected.destination_pubkey) return false;
    }
    if (expected.from_pubkey != null) {
      var raw2 = pickFirst(tx, ["from_pubkey", "source", "from"]);
      var from = raw2 == null ? null : String(raw2);
      if (from !== expected.from_pubkey) return false;
    }
    return true;
  }

  function waitForTransactionVisible(expected, opts) {
    var timeoutMs =
      opts && typeof opts.timeoutMs === "number" ? opts.timeoutMs : 20000;
    var pollIntervalMs =
      opts && typeof opts.pollIntervalMs === "number" ? opts.pollIntervalMs : 750;
    var limit = opts && typeof opts.limit === "number" ? opts.limit : 50;
    var filterOptions =
      (opts && opts.filterOptions && typeof opts.filterOptions === "object"
        ? opts.filterOptions
        : null) || {};

    var query = Object.assign({ limit: limit }, filterOptions);
    if (expected.from_pubkey && !query.sender && !query.account) {
      query.sender = expected.from_pubkey;
    }

    var startedAt = Date.now();
    var attempt = 0;

    function poll() {
      attempt++;
      return window.getTransactions(query).then(function (resp) {
        var items = normalizeTransactionsResponse(resp);
        var found = null;
        for (var i = 0; i < items.length; i++) {
          if (txMatches(items[i], expected)) { found = items[i]; break; }
        }
        if (found) {
          console.log("[usernode-bridge] tx found after", attempt, "polls,", Date.now() - startedAt, "ms");
          return found;
        }

        if (attempt <= 3 || attempt % 10 === 0) {
          console.log("[usernode-bridge] waitForTx poll #" + attempt + ", " + items.length + " items, no match yet");
        }

        if (Date.now() - startedAt >= timeoutMs) {
          var details = [
            expected.txId ? "txId=" + expected.txId : null,
            expected.memo != null ? "memo=" + expected.memo : null,
          ]
            .filter(Boolean)
            .join(", ");
          console.warn("[usernode-bridge] waitForTx timed out. expected:", JSON.stringify(expected));
          if (items.length > 0) {
            console.warn("[usernode-bridge] last poll sample (first item):", JSON.stringify(items[0]));
          }
          throw new Error(
            "Timed out waiting for transaction to appear in getTransactions (" + timeoutMs + "ms, " + attempt + " polls" + (details ? ", " + details : "") + ")"
          );
        }
        return sleep(pollIntervalMs).then(poll);
      });
    }
    return poll();
  }

  function randomHex(bytes) {
    var a = new Uint8Array(bytes);
    if (window.crypto && window.crypto.getRandomValues) {
      window.crypto.getRandomValues(a);
    } else {
      for (var i = 0; i < a.length; i++) a[i] = Math.floor(Math.random() * 256);
    }
    return Array.from(a, function (b) { return b.toString(16).padStart(2, "0"); }).join("");
  }

  function getOrCreateMockPubkey() {
    var key = "usernode:mockPubkey";
    var v = window.localStorage.getItem(key);
    if (!v) {
      v = "mockpk_" + randomHex(16);
      window.localStorage.setItem(key, v);
    }
    return v;
  }

  // ── Mock-mode detection ────────────────────────────────────────────────
  var _mockEnabledResult = null;

  function isMockEnabled() {
    if (_mockEnabledResult !== null) return Promise.resolve(_mockEnabledResult);
    return fetch("/__mock/enabled", { method: "GET" }).then(function (resp) {
      _mockEnabledResult = resp.ok;
      if (_mockEnabledResult) {
        console.log("[usernode-bridge] mock API detected — using local-dev endpoints");
      }
      return _mockEnabledResult;
    }).catch(function () {
      _mockEnabledResult = false;
      return false;
    });
  }

  window.usernode.isMockEnabled = isMockEnabled;

  // ── QR mode detection ──────────────────────────────────────────────────
  // Returns true when we're in a regular desktop browser (not native, not mock).
  function isQrMode() {
    if (window.usernode.isNative) return Promise.resolve(false);
    return isMockEnabled().then(function (mock) { return !mock; });
  }

  // =====================================================================
  //  Minimal QR Code encoder (Alphanumeric/Byte, versions 1-10, ECC L)
  //  Self-contained — no external dependencies.
  // =====================================================================
  var QR = (function () {
    // GF(256) arithmetic for Reed-Solomon
    var EXP = new Uint8Array(256), LOG = new Uint8Array(256);
    (function () {
      var x = 1;
      for (var i = 0; i < 255; i++) {
        EXP[i] = x; LOG[x] = i;
        x = (x << 1) ^ (x & 128 ? 0x11d : 0);
      }
      EXP[255] = EXP[0];
    })();

    function gfMul(a, b) {
      if (a === 0 || b === 0) return 0;
      return EXP[(LOG[a] + LOG[b]) % 255];
    }

    function rsGenPoly(n) {
      var g = [1];
      for (var i = 0; i < n; i++) {
        var ng = new Array(g.length + 1);
        for (var j = 0; j < ng.length; j++) ng[j] = 0;
        for (var j2 = 0; j2 < g.length; j2++) {
          ng[j2] ^= gfMul(g[j2], EXP[i]);
          ng[j2 + 1] ^= g[j2];
        }
        g = ng;
      }
      return g;
    }

    function rsEncode(data, eccLen) {
      var gen = rsGenPoly(eccLen);
      var pad = new Array(eccLen); for (var i = 0; i < eccLen; i++) pad[i] = 0;
      var msg = data.concat(pad);
      for (var i2 = 0; i2 < data.length; i2++) {
        var coef = msg[i2];
        if (coef !== 0) {
          for (var j = 0; j < gen.length; j++) {
            msg[i2 + j] ^= gfMul(gen[j], coef);
          }
        }
      }
      return msg.slice(data.length);
    }

    // Version/ECC capacity table (versions 1-10, ECC level L)
    // [totalCodewords, eccPerBlock, numBlocks, dataCodewords]
    var VERSIONS = [
      null,
      [26, 7, 1, 19],
      [44, 10, 1, 34],
      [70, 15, 1, 55],
      [100, 20, 1, 80],
      [134, 26, 1, 108],
      [172, 18, 2, 136],
      [196, 20, 2, 156],
      [242, 24, 2, 194],
      [292, 30, 2, 232],
      [346, 18, 2, 274],
    ];

    function pickVersion(byteLen) {
      for (var v = 1; v <= 10; v++) {
        var overhead = v <= 9 ? 3 : 4; // mode(4) + length(8 or 16) bits → ~2-3 bytes overhead
        if (VERSIONS[v][3] >= byteLen + overhead) return v;
      }
      return null;
    }

    function encode(text) {
      var dataBytes = [];
      for (var i = 0; i < text.length; i++) {
        var c = text.charCodeAt(i);
        if (c < 128) {
          dataBytes.push(c);
        } else if (c < 2048) {
          dataBytes.push(0xc0 | (c >> 6), 0x80 | (c & 0x3f));
        } else {
          dataBytes.push(0xe0 | (c >> 12), 0x80 | ((c >> 6) & 0x3f), 0x80 | (c & 0x3f));
        }
      }

      var ver = pickVersion(dataBytes.length);
      if (!ver) throw new Error("QR data too large (max ~274 bytes for version 10)");

      var info = VERSIONS[ver];
      var totalCW = info[0], eccPerBlock = info[1], numBlocks = info[2], dataCW = info[3];

      // Build data stream: mode 0100 (byte), length, data, terminator, padding
      var bits = [];
      function pushBits(val, count) {
        for (var b = count - 1; b >= 0; b--) bits.push((val >> b) & 1);
      }
      pushBits(4, 4); // byte mode
      pushBits(dataBytes.length, ver <= 9 ? 8 : 16);
      for (var d = 0; d < dataBytes.length; d++) pushBits(dataBytes[d], 8);
      pushBits(0, Math.min(4, dataCW * 8 - bits.length)); // terminator
      while (bits.length % 8) bits.push(0);
      var codewords = [];
      for (var b2 = 0; b2 < bits.length; b2 += 8) {
        codewords.push(
          (bits[b2] << 7) | (bits[b2 + 1] << 6) | (bits[b2 + 2] << 5) | (bits[b2 + 3] << 4) |
          (bits[b2 + 4] << 3) | (bits[b2 + 5] << 2) | (bits[b2 + 6] << 1) | bits[b2 + 7]
        );
      }
      var padBytes = [0xec, 0x11];
      for (var p = 0; codewords.length < dataCW; p++) {
        codewords.push(padBytes[p % 2]);
      }

      // Split into blocks, compute ECC
      var blockDataCW = Math.floor(dataCW / numBlocks);
      var remainder = dataCW % numBlocks;
      var dataBlocks = [], eccBlocks = [];
      var offset = 0;
      for (var bl = 0; bl < numBlocks; bl++) {
        var bLen = blockDataCW + (bl < remainder ? 1 : 0);
        var block = codewords.slice(offset, offset + bLen);
        offset += bLen;
        dataBlocks.push(block);
        eccBlocks.push(rsEncode(block, eccPerBlock));
      }

      // Interleave data + ecc
      var result = [];
      var maxDataLen = blockDataCW + (remainder > 0 ? 1 : 0);
      for (var ci = 0; ci < maxDataLen; ci++) {
        for (var bi = 0; bi < numBlocks; bi++) {
          if (ci < dataBlocks[bi].length) result.push(dataBlocks[bi][ci]);
        }
      }
      for (var ei = 0; ei < eccPerBlock; ei++) {
        for (var bi2 = 0; bi2 < numBlocks; bi2++) {
          result.push(eccBlocks[bi2][ei]);
        }
      }

      // Place into matrix
      var size = 17 + ver * 4;
      var grid = []; var reserved = [];
      for (var r = 0; r < size; r++) {
        grid[r] = new Uint8Array(size);
        reserved[r] = new Uint8Array(size);
      }

      function setModule(r, c, val) {
        grid[r][c] = val ? 1 : 0;
        reserved[r][c] = 1;
      }

      // Finder patterns
      function finderPattern(row, col) {
        for (var dr = -1; dr <= 7; dr++) {
          for (var dc = -1; dc <= 7; dc++) {
            var rr = row + dr, cc = col + dc;
            if (rr < 0 || rr >= size || cc < 0 || cc >= size) continue;
            var dark = (dr >= 0 && dr <= 6 && (dc === 0 || dc === 6)) ||
                       (dc >= 0 && dc <= 6 && (dr === 0 || dr === 6)) ||
                       (dr >= 2 && dr <= 4 && dc >= 2 && dc <= 4);
            setModule(rr, cc, dark);
          }
        }
      }
      finderPattern(0, 0);
      finderPattern(0, size - 7);
      finderPattern(size - 7, 0);

      // Timing patterns
      for (var t = 8; t < size - 8; t++) {
        setModule(6, t, t % 2 === 0);
        setModule(t, 6, t % 2 === 0);
      }

      // Alignment pattern (version >= 2)
      if (ver >= 2) {
        var alignPos = [6, ver * 4 + 10];
        for (var ai = 0; ai < alignPos.length; ai++) {
          for (var aj = 0; aj < alignPos.length; aj++) {
            var ar = alignPos[ai], ac = alignPos[aj];
            if (reserved[ar][ac]) continue;
            for (var dr2 = -2; dr2 <= 2; dr2++) {
              for (var dc2 = -2; dc2 <= 2; dc2++) {
                setModule(ar + dr2, ac + dc2,
                  Math.abs(dr2) === 2 || Math.abs(dc2) === 2 || (dr2 === 0 && dc2 === 0));
              }
            }
          }
        }
      }

      // Reserve format info areas
      for (var f = 0; f < 8; f++) {
        if (!reserved[8][f]) { reserved[8][f] = 1; }
        if (!reserved[8][size - 1 - f]) { reserved[8][size - 1 - f] = 1; }
        if (!reserved[f][8]) { reserved[f][8] = 1; }
        if (!reserved[size - 1 - f][8]) { reserved[size - 1 - f][8] = 1; }
      }
      reserved[8][8] = 1;
      setModule(size - 8, 8, 1); // dark module

      // Place data bits
      var bitIdx = 0;
      var resultBits = [];
      for (var rb = 0; rb < result.length; rb++) {
        for (var sb = 7; sb >= 0; sb--) resultBits.push((result[rb] >> sb) & 1);
      }

      var right = true;
      for (var col = size - 1; col >= 1; col -= 2) {
        if (col === 6) col = 5; // skip timing column
        var rows = [];
        for (var rr2 = 0; rr2 < size; rr2++) rows.push(rr2);
        if (!right) rows.reverse();
        right = !right;
        for (var ri = 0; ri < rows.length; ri++) {
          for (var dx = 0; dx <= 1; dx++) {
            var cc2 = col - dx;
            if (reserved[rows[ri]][cc2]) continue;
            grid[rows[ri]][cc2] = bitIdx < resultBits.length ? resultBits[bitIdx] : 0;
            bitIdx++;
          }
        }
      }

      // Apply mask 0 (checkerboard) and format info
      for (var mr = 0; mr < size; mr++) {
        for (var mc = 0; mc < size; mc++) {
          if (!reserved[mr][mc]) {
            grid[mr][mc] ^= ((mr + mc) % 2 === 0) ? 1 : 0;
          }
        }
      }

      // Format info for ECC L, mask 0 = 0x77c4
      var fmtBits = 0x77c4;
      var fmtPositions = [
        [0, 8], [1, 8], [2, 8], [3, 8], [4, 8], [5, 8], [7, 8], [8, 8],
        [8, 7], [8, 5], [8, 4], [8, 3], [8, 2], [8, 1], [8, 0],
      ];
      for (var fi = 0; fi < 15; fi++) {
        var bit = (fmtBits >> fi) & 1;
        grid[fmtPositions[fi][0]][fmtPositions[fi][1]] = bit;
      }
      var fmtPositions2 = [
        [8, size - 1], [8, size - 2], [8, size - 3], [8, size - 4],
        [8, size - 5], [8, size - 6], [8, size - 7],
        [size - 7, 8], [size - 6, 8], [size - 5, 8], [size - 4, 8],
        [size - 3, 8], [size - 2, 8], [size - 1, 8], [size - 8, 8],
      ];
      for (var fi2 = 0; fi2 < 15; fi2++) {
        var bit2 = (fmtBits >> fi2) & 1;
        if (fi2 < fmtPositions2.length) {
          grid[fmtPositions2[fi2][0]][fmtPositions2[fi2][1]] = bit2;
        }
      }

      return { grid: grid, size: size };
    }

    function toCanvas(qrData, pixelSize) {
      pixelSize = pixelSize || 4;
      var quiet = 4;
      var totalSize = (qrData.size + quiet * 2) * pixelSize;
      var canvas = document.createElement("canvas");
      canvas.width = totalSize;
      canvas.height = totalSize;
      var ctx = canvas.getContext("2d");
      ctx.fillStyle = "#ffffff";
      ctx.fillRect(0, 0, totalSize, totalSize);
      ctx.fillStyle = "#000000";
      for (var r = 0; r < qrData.size; r++) {
        for (var c = 0; c < qrData.size; c++) {
          if (qrData.grid[r][c]) {
            ctx.fillRect((c + quiet) * pixelSize, (r + quiet) * pixelSize, pixelSize, pixelSize);
          }
        }
      }
      return canvas;
    }

    return { encode: encode, toCanvas: toCanvas };
  })();

  // =====================================================================
  //  QR Transaction Modal
  // =====================================================================
  var _qrOverlay = null;
  var _qrCancelReject = null;

  function createQrOverlayStyles() {
    if (document.getElementById("__usernode-qr-styles")) return;
    var style = document.createElement("style");
    style.id = "__usernode-qr-styles";
    style.textContent = [
      ".__un-qr-overlay{position:fixed;inset:0;z-index:999999;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,0.65);font-family:-apple-system,system-ui,sans-serif}",
      ".__un-qr-card{background:#1a1f2e;color:#e7edf7;border-radius:16px;padding:28px 24px;max-width:340px;width:90%;text-align:center;box-shadow:0 8px 32px rgba(0,0,0,0.4)}",
      "@media(prefers-color-scheme:light){.__un-qr-card{background:#fff;color:#0b1220;box-shadow:0 8px 32px rgba(0,0,0,0.15)}}",
      ".__un-qr-title{font-size:17px;font-weight:600;margin:0 0 4px}",
      ".__un-qr-subtitle{font-size:13px;opacity:0.7;margin:0 0 20px}",
      ".__un-qr-canvas{border-radius:12px;margin:0 auto 16px}",
      ".__un-qr-status{font-size:12px;opacity:0.6;margin:0 0 16px;min-height:16px}",
      ".__un-qr-cancel{background:none;border:1px solid rgba(255,255,255,0.2);color:inherit;border-radius:8px;padding:8px 24px;font-size:14px;cursor:pointer;opacity:0.8}",
      ".__un-qr-cancel:hover{opacity:1}",
      "@media(prefers-color-scheme:light){.__un-qr-cancel{border-color:rgba(0,0,0,0.15)}}",
    ].join("\n");
    document.head.appendChild(style);
  }

  function showQrModal(payload, opts) {
    createQrOverlayStyles();

    var title = (opts && opts.confirmTitle) || "Confirm Transaction";
    var subtitle = (opts && opts.confirmSubtitle) || "Scan this QR code with the Usernode mobile app.";
    var json = JSON.stringify(payload);
    var qrData = QR.encode(json);
    var canvas = QR.toCanvas(qrData, 5);
    canvas.className = "__un-qr-canvas";
    canvas.style.display = "block";

    var overlay = document.createElement("div");
    overlay.className = "__un-qr-overlay";

    var card = document.createElement("div");
    card.className = "__un-qr-card";

    var h = document.createElement("div");
    h.className = "__un-qr-title";
    h.textContent = title;

    var sub = document.createElement("div");
    sub.className = "__un-qr-subtitle";
    sub.textContent = subtitle;

    var status = document.createElement("div");
    status.className = "__un-qr-status";
    status.textContent = "Waiting for transaction...";
    status.id = "__un-qr-status";

    var btn = document.createElement("button");
    btn.className = "__un-qr-cancel";
    btn.textContent = "Cancel";
    btn.onclick = function () {
      hideQrModal();
      if (_qrCancelReject) {
        _qrCancelReject(new Error("User cancelled QR transaction"));
        _qrCancelReject = null;
      }
    };

    card.appendChild(h);
    card.appendChild(sub);
    card.appendChild(canvas);
    card.appendChild(status);
    card.appendChild(btn);
    overlay.appendChild(card);
    document.body.appendChild(overlay);
    _qrOverlay = overlay;
  }

  function updateQrStatus(text) {
    var el = document.getElementById("__un-qr-status");
    if (el) el.textContent = text;
  }

  function hideQrModal() {
    if (_qrOverlay && _qrOverlay.parentNode) {
      _qrOverlay.parentNode.removeChild(_qrOverlay);
    }
    _qrOverlay = null;
  }

  // ── QR sendTransaction ─────────────────────────────────────────────────
  function qrSendTransaction(destination_pubkey, amount, memo, opts) {
    var timeoutMs = (opts && typeof opts.timeoutMs === "number") ? opts.timeoutMs : 90000;
    var pollIntervalMs = (opts && typeof opts.pollIntervalMs === "number") ? opts.pollIntervalMs : 2000;

    var payload = {
      type: "tx",
      to: destination_pubkey,
      amount: typeof amount === "number" ? amount : parseInt(amount, 10),
      memo: memo || "",
    };
    if (opts && opts.confirmTitle) payload.confirmTitle = opts.confirmTitle;
    if (opts && opts.confirmSubtitle) payload.confirmSubtitle = opts.confirmSubtitle;

    return new Promise(function (resolve, reject) {
      _qrCancelReject = reject;

      showQrModal(payload, opts);

      var startedAt = Date.now();
      var attempt = 0;
      var stopped = false;

      function pollForTx() {
        if (stopped) return;
        attempt++;

        var query = { limit: 50, account: destination_pubkey };

        window.getTransactions(query).then(function (resp) {
          if (stopped) return;
          var items = normalizeTransactionsResponse(resp);

          for (var i = 0; i < items.length; i++) {
            var tx = items[i];
            var txTime = extractTxTimestampMs(tx);
            if (txTime && txTime < startedAt - 10000) continue;

            var txMemo = tx.memo == null ? null : String(tx.memo);
            var txDest = pickFirst(tx, ["destination_pubkey", "destination", "to"]);
            if (txDest && String(txDest) === destination_pubkey && txMemo === (memo || "")) {
              stopped = true;
              hideQrModal();
              _qrCancelReject = null;
              console.log("[usernode-bridge] QR tx confirmed after", attempt, "polls");
              resolve({ queued: true, tx: tx });
              return;
            }
          }

          if (attempt <= 3 || attempt % 10 === 0) {
            updateQrStatus("Waiting for transaction... (" + Math.round((Date.now() - startedAt) / 1000) + "s)");
          }

          if (Date.now() - startedAt >= timeoutMs) {
            stopped = true;
            hideQrModal();
            _qrCancelReject = null;
            resolve({ queued: true, tx: null });
            return;
          }

          setTimeout(pollForTx, pollIntervalMs);
        }).catch(function (err) {
          if (stopped) return;
          console.warn("[usernode-bridge] QR poll error:", err.message);
          if (Date.now() - startedAt >= timeoutMs) {
            stopped = true;
            hideQrModal();
            _qrCancelReject = null;
            resolve({ queued: true, tx: null });
            return;
          }
          setTimeout(pollForTx, pollIntervalMs);
        });
      }

      setTimeout(pollForTx, 1000);
    });
  }

  // =====================================================================
  //  Public API: getNodeAddress
  // =====================================================================
  if (typeof window.getNodeAddress !== "function") {
    if (window.usernode.isNative) {
      window.getNodeAddress = function getNodeAddress() {
        return callNative("getNodeAddress");
      };
    } else {
      window.getNodeAddress = function getNodeAddress() {
        if (_configuredAddress) return Promise.resolve(_configuredAddress);
        return Promise.resolve(
          window.localStorage.getItem("usernode:mockAddress") ||
          getOrCreateMockPubkey()
        );
      };
    }
  }

  // =====================================================================
  //  Public API: sendTransaction
  // =====================================================================
  if (typeof window.sendTransaction !== "function") {
    function mockSendTransaction(destination_pubkey, amount, memo, opts) {
      var startedAt = Date.now();
      return fetch("/__mock/sendTransaction", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          from_pubkey: null, // filled below
          destination_pubkey: destination_pubkey,
          amount: amount,
          memo: memo,
        }),
      }).then(function () {
        return window.getNodeAddress();
      }).then(function (addr) {
        return fetch("/__mock/sendTransaction", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            from_pubkey: addr,
            destination_pubkey: destination_pubkey,
            amount: amount,
            memo: memo,
          }),
        });
      }).then(function (resp) {
        if (!resp.ok) {
          return resp.text().then(function (text) {
            if (resp.status === 404) {
              throw new Error("Mock API not enabled. Start server with `node server.js --local-dev`.");
            }
            throw new Error("Mock sendTransaction failed (" + resp.status + "): " + text);
          });
        }
        return resp.json();
      }).then(function (sendResult) {
        var sendFailed = sendResult && (sendResult.error || sendResult.queued === false);
        var shouldWait =
          !sendFailed && (!opts || opts.waitForInclusion == null ? true : !!opts.waitForInclusion);
        if (!shouldWait) return sendResult;
        return window.getNodeAddress().then(function (from) {
          var txId = extractTxId(sendResult);
          return waitForTransactionVisible({
            txId: txId,
            minCreatedAtMs: startedAt,
            memo: memo == null ? null : String(memo),
            destination_pubkey: destination_pubkey == null ? null : String(destination_pubkey),
            from_pubkey: from ? String(from).trim() : null,
            amount: amount,
          }, opts).then(function () { return sendResult; });
        });
      });
    }

    // Rewrite mockSendTransaction to actually work (the above double-fetch was wrong)
    mockSendTransaction = function mockSendTransaction(destination_pubkey, amount, memo, opts) {
      var startedAt = Date.now();
      return window.getNodeAddress().then(function (addr) {
        return fetch("/__mock/sendTransaction", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            from_pubkey: addr,
            destination_pubkey: destination_pubkey,
            amount: amount,
            memo: memo,
          }),
        });
      }).then(function (resp) {
        if (!resp.ok) {
          return resp.text().then(function (text) {
            if (resp.status === 404) {
              throw new Error("Mock API not enabled. Start server with `node server.js --local-dev`.");
            }
            throw new Error("Mock sendTransaction failed (" + resp.status + "): " + text);
          });
        }
        return resp.json();
      }).then(function (sendResult) {
        var sendFailed = sendResult && (sendResult.error || sendResult.queued === false);
        var shouldWait =
          !sendFailed && (!opts || opts.waitForInclusion == null ? true : !!opts.waitForInclusion);
        if (!shouldWait) return sendResult;
        return window.getNodeAddress().then(function (from) {
          var txId = extractTxId(sendResult);
          return waitForTransactionVisible({
            txId: txId,
            minCreatedAtMs: startedAt,
            memo: memo == null ? null : String(memo),
            destination_pubkey: destination_pubkey == null ? null : String(destination_pubkey),
            from_pubkey: from ? String(from).trim() : null,
            amount: amount,
          }, opts).then(function (matchedTx) { return attachMatchedTx(sendResult, matchedTx); });
        });
      });
    };

    function nativeSendTransaction(destination_pubkey, amount, memo, opts) {
      var startedAt = Date.now();
      var from_pubkey;
      return window.getNodeAddress().then(function (v) {
        from_pubkey = v == null ? null : String(v).trim();
        return callNative("sendTransaction", {
          destination_pubkey: destination_pubkey,
          amount: amount,
          memo: memo,
          confirm_title: (opts && opts.confirmTitle) || undefined,
          confirm_subtitle: (opts && opts.confirmSubtitle) || undefined,
        });
      }).then(function (sendResult) {
        var sendError = sendResult && sendResult.error;
        if (sendError) throw new Error(String(sendError));
        var sendFailed = sendResult && sendResult.queued === false;
        var shouldWait =
          !sendFailed && (!opts || opts.waitForInclusion == null ? true : !!opts.waitForInclusion);
        if (!shouldWait) return sendResult;
        var txId = extractTxId(sendResult);
        return waitForTransactionVisible({
          txId: txId,
          minCreatedAtMs: startedAt,
          memo: memo == null ? null : String(memo),
          destination_pubkey: destination_pubkey == null ? null : String(destination_pubkey),
          from_pubkey: from_pubkey || null,
          amount: amount,
        }, opts).then(function (matchedTx) { return attachMatchedTx(sendResult, matchedTx); });
      });
    }

    window.sendTransaction = function sendTransaction(destination_pubkey, amount, memo, opts) {
      return isMockEnabled().then(function (useMock) {
        if (useMock) return mockSendTransaction(destination_pubkey, amount, memo, opts);
        if (window.usernode.isNative) return nativeSendTransaction(destination_pubkey, amount, memo, opts);
        return qrSendTransaction(destination_pubkey, amount, memo, opts);
      });
    };
  }

  // =====================================================================
  //  Public API: getTransactions
  // =====================================================================
  if (typeof window.getTransactions !== "function") {
    function mockGetTransactions(filterOptions) {
      return window.getNodeAddress().then(function (addr) {
        var ownerPubkey = (filterOptions && filterOptions.account) ? filterOptions.account : addr;
        return fetch("/__mock/getTransactions", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            owner_pubkey: ownerPubkey,
            filterOptions: filterOptions || {},
          }),
        });
      }).then(function (resp) {
        if (!resp.ok) {
          return resp.text().then(function (text) {
            if (resp.status === 404) {
              throw new Error("Mock API not enabled. Start server with `node server.js --local-dev`.");
            }
            throw new Error("Mock getTransactions failed (" + resp.status + "): " + text);
          });
        }
        return resp.json();
      });
    }

    function nativeGetTransactions(filterOptions) {
      var base = window.usernode.transactionsBaseUrl;
      if (!base) {
        return Promise.reject(new Error(
          "transactionsBaseUrl not configured (set window.usernode.transactionsBaseUrl)"
        ));
      }
      return fetch(base + "/transactions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(filterOptions || {}),
      }).then(function (resp) {
        if (!resp.ok) {
          return resp.text().then(function (text) {
            throw new Error("getTransactions failed (" + resp.status + "): " + text);
          });
        }
        return resp.json();
      });
    }

    window.getTransactions = function getTransactions(filterOptions) {
      return isMockEnabled().then(function (useMock) {
        if (useMock) return mockGetTransactions(filterOptions);
        if (window.usernode.isNative) return nativeGetTransactions(filterOptions);
        if (window.usernode.transactionsBaseUrl) return nativeGetTransactions(filterOptions);
        return mockGetTransactions(filterOptions);
      });
    };
  }

  // =====================================================================
  //  Public API: signMessage
  // =====================================================================
  if (typeof window.signMessage !== "function") {
    window.signMessage = function signMessage(message) {
      if (window.usernode.isNative) {
        return callNative("signMessage", { message: message });
      }
      return isMockEnabled().then(function (useMock) {
        if (useMock) {
          return window.getNodeAddress().then(function (pubkey) {
            return {
              pubkey: pubkey,
              signature: "mock_signature_" + btoa(message).replace(/=+$/, ""),
            };
          });
        }
        return Promise.reject(new Error(
          "signMessage is not available in QR mode. Use the Usernode mobile app directly."
        ));
      });
    };
  }
})();
