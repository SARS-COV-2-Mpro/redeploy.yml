/* ======================================================================
   Exchange Server Worker — External Ideas Only + Fan-out Fallback
   (10/10 suggester compatible: dynamic RRR, prob/EV via env flags)
   SECTION 1/7 — Constants, Utils, Crypto, DB, Telegram
   ====================================================================== */

/* ---------- Constants (Exchanges) ---------- */
const SUPPORTED_EXCHANGES = {
  crypto_parrot: { label: "Crypto Parrot (Demo)", kind: "demoParrot", hasOrders: true },

  // Spot
  mexc:    { label: "MEXC",    kind: "binanceLike", baseUrl: "https://api.mexc.com",     accountPath: "/api/v3/account", apiKeyHeader: "X-MEXC-APIKEY",  defaultQuery: "",                hasOrders: true },
  binance: { label: "Binance", kind: "binanceLike", baseUrl: "https://api.binance.com",  accountPath: "/api/v3/account", apiKeyHeader: "X-MBX-APIKEY",   defaultQuery: "recvWindow=5000", hasOrders: true },
  lbank:   { label: "LBank",   kind: "lbankV2",     baseUrl: "https://api.lbkex.com", hasOrders: true },
  coinex:  { label: "CoinEx",  kind: "coinexV2",    baseUrl: "https://api.coinex.com", hasOrders: true },

  // Futures / Margin (USDT-M)
  binance_futures: { label: "Binance Futures (USDT-M)", kind: "binanceFuturesUSDT", baseUrl: "https://fapi.binance.com", apiKeyHeader: "X-MBX-APIKEY", hasOrders: true },

  // Read-only in this worker
  bybit:   { label: "Bybit",   kind: "bybitV5",     baseUrl: "https://api.bybit.com", hasOrders: false },
  kraken:  { label: "Kraken",  kind: "krakenV0",    baseUrl: "https://api.kraken.com", hasOrders: false },
  gate:    { label: "Gate",    kind: "gateV4",      baseUrl: "https://api.gateio.ws", hasOrders: false },
  huobi:   { label: "Huobi",   kind: "huobiV1",     baseHost: "api.huobi.pro", scheme: "https", hasOrders: false }
};

/* ---------- Brain & Risk Parameters (original behavior) ---------- */
const STRICT_RRR = 2.0;          // default 2:1 (used when idea exits are not provided)
const ACP_FRAC = 0.80;           // keep constant to avoid Demo bank_closed
const PER_TRADE_CAP_FRAC = 0.10;
const DAILY_OPEN_RISK_CAP_FRAC = 0.30;

/* HSE config */
const HSE_CFG = {
  AF: 1.0, gamma: 0.5, w_cost: 1.0, w_vol: 1.0, w_rs: 1.0, w_es: 1.0,
  alpha0: -0.4, alpha1: 1.0,
  BAS_avg: 0.0005, OBD_avg: 200000, MV_avg: 0.01, RS_avg: 1.0, ES_avg: 1.0,
  sSlipStarBps: 3.0
};

/* Kelly blend and numerics */
const LAMBDA_BLEND = 0.5;
const EPS_VARIANCE = 1e-9;
const MIN_STOP_PCT = 0.0025;     // floor used when idea exits absent
const SQS_MIN_DEFAULT = 0.30;

/* ---------- System-level ---------- */
const CRON_LOCK_KEY = "cron_running";
const CRON_LOCK_TTL = 55; // seconds
const AUTO_NO_FUNDS_THRESHOLD = 0.000001;

const DEFAULT_MAX_CONCURRENT_POS = 3;
const DEFAULT_MAX_NEW_POSITIONS_PER_CYCLE = 3;

/* ---------- Tiny utils ---------- */
const te = new TextEncoder();
const td = new TextDecoder();
const b64url = {
  encode: (buf) => btoa(String.fromCharCode(...new Uint8Array(buf))).replace(/\+/g, "-").replace(/\//g, "_"),
  decode: (str) => Uint8Array.from(atob(str.replace(/-/g, "+").replace(/_/g, "/")), (c) => c.charCodeAt(0)),
};
const b64std = {
  encode: (buf) => btoa(String.fromCharCode(...new Uint8Array(buf))),
  decode: (str) => Uint8Array.from(atob(str), (c) => c.charCodeAt(0)),
};
const nowISO = () => new Date().toISOString();
const toHex = (buf) => [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
const clamp01 = (x) => Math.max(0, Math.min(1, x));
const clampRange = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
const sigmoid = (x) => 1 / (1 + Math.exp(-x));
const percentEncode = (str) => encodeURIComponent(str).replace(/[!*'()]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }
function pctChange(a, b) { return b > 0 ? (a - b) / b : 0; }
const bpsToFrac = (bps) => Number(bps || 0) / 10000;

/* Env helpers (used later to respect exec.post_only and mgmt overlays) */
function envFlag(env, key, def = "0") { return String(env?.[key] ?? def) === "1"; }
function envNum(env, key, def) { const v = Number(env?.[key]); return Number.isFinite(v) ? v : def; }

/* UI helpers */
function formatMoney(amount) { return `$${Number(amount || 0).toFixed(8)}`; }
function formatPercent(decimal) { return `${(decimal * 100).toFixed(2)}%`; }
function formatDurationShort(ms) {
  if (!isFinite(ms) || ms <= 0) return "0m";
  const totalMin = Math.round(ms / 60000);
  const h = Math.floor(totalMin / 60);
  const m = totalMin % 60;
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

/* ---------- LRU+TTL cache (flat CPU; bounded size) ---------- */
class TTLLRU {
  constructor(capacity = 512, ttlMs = 60000) {
    this.cap = Math.max(1, capacity);
    this.ttl = Math.max(1, ttlMs);
    this.map = new Map(); // key -> { v, e }
  }
  get(key) {
    const ent = this.map.get(key);
    if (!ent) return undefined;
    if (ent.e <= Date.now()) { this.map.delete(key); return undefined; }
    // refresh recency
    this.map.delete(key);
    this.map.set(key, ent);
    return ent.v;
  }
  set(key, value, ttlMs = this.ttl) {
    const e = Date.now() + Math.max(1, ttlMs);
    if (this.map.has(key)) this.map.delete(key);
    this.map.set(key, { v: value, e });
    if (this.map.size > this.cap) {
      const oldest = this.map.keys().next().value;
      this.map.delete(oldest);
    }
  }
  delete(key) { this.map.delete(key); }
  clear() { this.map.clear(); }
  size() { return this.map.size; }
}

/* ---------- Resource guards ---------- */
const DEFAULT_FETCH_TIMEOUT_MS = 3000;
async function safeFetch(url, options = {}, timeoutMs = DEFAULT_FETCH_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const to = setTimeout(() => ctrl.abort("timeout"), timeoutMs);
  try { return await fetch(url, { ...options, signal: ctrl.signal }); }
  finally { clearTimeout(to); }
}

const DEFAULT_CPU_BUDGET_MS = 12000;
function createCpuBudget(ms = DEFAULT_CPU_BUDGET_MS) {
  const deadline = Date.now() + Math.max(250, ms);
  return {
    deadline,
    isExpired() { return Date.now() > deadline; },
    timeLeft() { return Math.max(0, deadline - Date.now()); },
    ensure(label) { if (this.isExpired()) throw new Error(`CPU budget exceeded at ${label}`); }
  };
}

/* ---------- Telegram helpers (safe edit fallback) ---------- */
async function answerCallbackQuery(env, id) {
  const url = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/answerCallbackQuery`;
  await safeFetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ callback_query_id: id }) }).catch(() => {});
}
async function sendMessage(chatId, text, buttons, env) {
  const url = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`;
  const body = { chat_id: chatId, text };
  if (buttons && buttons.length) body.reply_markup = { inline_keyboard: buttons };
  const r = await safeFetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }, 6000);
  if (!r.ok) console.error("sendMessage error:", await r.text());
}
async function editMessage(chatId, messageId, newText, buttons, env) {
  const url = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/editMessageText`;
  const body = { chat_id: chatId, message_id: messageId, text: newText };
  if (buttons && buttons.length) body.reply_markup = { inline_keyboard: buttons };
  else body.reply_markup = { inline_keyboard: [] };
  const r = await safeFetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }, 6000);
  if (!r.ok) {
    let desc = "";
    try { const j = await r.json(); desc = j?.description || ""; } catch { desc = await r.text(); }
    if (r.status === 400 && /message is not modified/i.test(desc)) return;
    if (r.status === 400 && /(can't be edited|message to edit not found)/i.test(desc)) {
      await sendMessage(chatId, newText, buttons, env);
      return;
    }
    console.error("editMessage error:", r.status, desc);
  }
}

/* ---------- Crypto (AES-GCM for DB) ---------- */
async function getCryptoKey(env) {
  const key = env.ENCRYPTION_KEY;
  if (!key || key.length < 32) throw new Error("ENCRYPTION_KEY is not set or too short");
  const hash = await crypto.subtle.digest("SHA-256", te.encode(key));
  return crypto.subtle.importKey("raw", hash, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}
async function encrypt(plain, env) {
  const k = await getCryptoKey(env);
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const enc = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, k, te.encode(plain));
  return `${b64url.encode(iv)}.${b64url.encode(enc)}`;
}
async function decrypt(cipher, env) {
  const [ivB64, dataB64] = (cipher || "").split(".");
  if (!ivB64 || !dataB64) throw new Error("Bad cipher format");
  const k = await getCryptoKey(env);
  const dec = await crypto.subtle.decrypt({ name: "AES-GCM", iv: b64url.decode(ivB64) }, k, b64url.decode(dataB64));
  return td.decode(dec);
}

/* ---------- DB helpers ---------- */
async function getSession(env, userId) { return env.DB.prepare("SELECT * FROM user_sessions WHERE user_id = ?").bind(userId).first(); }
async function createSession(env, userId) {
  await env.DB.prepare("INSERT INTO user_sessions (user_id, current_step, last_interaction_ts, status) VALUES (?, 'start', ?, 'initializing')").bind(userId, nowISO()).run();
  return getSession(env, userId);
}
async function saveSession(env, userId, fields) {
  const keys = Object.keys(fields);
  if (!keys.length) return;
  await env.DB.prepare(`UPDATE user_sessions SET ${keys.map(k => `${k} = ?`).join(", ")}, last_interaction_ts = ? WHERE user_id = ?`)
    .bind(...keys.map(k => fields[k]), nowISO(), userId).run();
}
async function deleteSession(env, userId) { await env.DB.prepare("DELETE FROM user_sessions WHERE user_id = ?").bind(userId).run(); }
async function handleDirectStop(env, userId) { await deleteSession(env, userId); await sendMessage(userId, "Session ended. Send /start to begin again.", null, env); }

/* Simple KV via D1 */
async function kvGet(env, key) { const row = await env.DB.prepare("SELECT value FROM kv_state WHERE key = ?").bind(key).first(); return row ? row.value : null; }
async function kvSet(env, key, value) { await env.DB.prepare("INSERT OR REPLACE INTO kv_state (key, value) VALUES (?, ?)").bind(key, String(value)).run(); }

/* ---------- Protocol state ---------- */
async function getProtocolState(env, userId) {
  return env.DB.prepare("SELECT * FROM protocol_state WHERE user_id = ?").bind(userId).first();
}
async function initProtocolDynamic(env, userId, feeRate, riskPct = 0.01) {
  await env.DB.prepare(
    `INSERT OR REPLACE INTO protocol_state
     (user_id, initial_capital, acp_balance, pr_balance, trigger_threshold, phase, risk_pct, fee_rate, created_at, updated_at)
     VALUES (?, 0, 0, 0, 0, 'normal', ?, ?, ?, ?)`
  ).bind(userId, riskPct, feeRate, nowISO(), nowISO()).run();
  return { initial_capital: 0, acp_balance: 0, pr_balance: 0, trigger_threshold: 0, phase: 'normal', risk_pct: riskPct, fee_rate: feeRate };
}
async function updateProtocolState(env, userId, updates) {
  const keys = Object.keys(updates);
  if (!keys.length) return;
  await env.DB.prepare(`UPDATE protocol_state SET ${keys.map(k => `${k} = ?`).join(", ")}, updated_at = ? WHERE user_id = ?`)
    .bind(...keys.map(k => updates[k]), nowISO(), userId).run();
}
async function reconcileProtocol(env, userId, isWin, riskAmount, rrr, fees) {
  await updateProtocolState(env, userId, { updated_at: nowISO() });
}

/* ======================================================================
   SECTION 2/7 — Market Data, Orderbook, and Ideas selection
   (Prefer latest GitHub snapshot; origin 'gha' or 'github_actions')
   ====================================================================== */

/* ---------- Cache setup (LRU + TTL; flat CPU) ---------- */
const PRICE_CACHE_TTL_MS = 30000;              // 30s price snapshots
const STOP_PCT_CACHE_TTL_MS = 45 * 60 * 1000;  // 45m sticky ATR-like stop
const ORDERBOOK_CACHE_TTL_MS = 8000;           // 8s depth snapshot

const priceCache     = new TTLLRU(512, PRICE_CACHE_TTL_MS);     // key: BASE, val: number price
const stopPctCache   = new TTLLRU(512, STOP_PCT_CACHE_TTL_MS);  // key: BASE, val: number stop pct
const orderbookCache = new TTLLRU(512, ORDERBOOK_CACHE_TTL_MS); // key: BASE, val: raw book JSON

/* ---------- Market Data (robust, cached) ---------- */
async function getCurrentPrice(symbol, hintPrice) {
  const base = (symbol || "").toUpperCase();
  if (!base) return 0;

  // 1) Use hint if provided (and cache)
  if (isFinite(hintPrice) && hintPrice > 0) {
    priceCache.set(base, Number(hintPrice));
    return Number(hintPrice);
  }

  // 2) LRU+TTL cache
  const hit = priceCache.get(base);
  if (typeof hit !== "undefined") return hit;

  const pairs = [`${base}USDT`];
  const bases = [
    SUPPORTED_EXCHANGES.binance.baseUrl,
    SUPPORTED_EXCHANGES.mexc.baseUrl,
  ];

  const tryJson = async (url) => {
    for (let i = 0; i < 2; i++) {
      try {
        const r = await safeFetch(url, { cf: { cacheTtl: 5 } }, 2500).catch(() => null);
        if (!r) { await sleep(80 + i*100); continue; }
        const text = await r.text().catch(()=> "");
        if (!r.ok) { await sleep(80 + i*100); continue; }
        const j = JSON.parse(text);
        const v = parseFloat(
          j?.price ?? j?.lastPrice ?? j?.weightedAvgPrice ?? (Array.isArray(j) ? j?.[0]?.[4] : undefined)
        );
        if (isFinite(v) && v > 0) return v;
      } catch (_) { await sleep(80 + i*100); }
    }
    return 0;
  };

  for (const p of pairs) {
    for (const b of bases) {
      let v = await tryJson(`${b}/api/v3/ticker/price?symbol=${p}`);
      if (v > 0) { priceCache.set(base, v); return v; }

      v = await tryJson(`${b}/api/v3/ticker/24hr?symbol=${p}`);
      if (v > 0) { priceCache.set(base, v); return v; }

      v = await tryJson(`${b}/api/v3/klines?symbol=${p}&interval=1m&limit=1`);
      if (v > 0) { priceCache.set(base, v); return v; }
    }
  }

  return 0;
}

/* ATR-like stop percentage (sticky cache; MIN_STOP_PCT constant respected) */
async function calculateStopPercent(symbol) {
  const base = (symbol || "").toUpperCase();
  if (!base) return 0.01;

  const hit = stopPctCache.get(base);
  if (typeof hit !== "undefined") return hit;

  const tryOne = async (baseUrl, interval, limit) => {
    const url = `${baseUrl}/api/v3/klines?symbol=${base}USDT&interval=${interval}&limit=${limit}`;
    for (let i=0;i<2;i++){
      try {
        const res = await safeFetch(url, {}, 2500);
        const klines = await res.json();
        if (!Array.isArray(klines) || klines.length < 14) continue;
        let atrSum = 0;
        for (let j = 1; j < klines.length; j++) {
          const high = parseFloat(klines[j][2]);
          const low = parseFloat(klines[j][3]);
          const prevClose = parseFloat(klines[j - 1][4]);
          const tr = Math.max(high - low, Math.abs(high - prevClose), Math.abs(low - prevClose));
          atrSum += tr;
        }
        const atr = atrSum / (klines.length - 1);
        const currentPrice = parseFloat(klines[klines.length - 1][4]);
        if (!isFinite(currentPrice) || currentPrice <= 0) continue;
        const atrPercent = atr / currentPrice;
        return clamp(atrPercent, MIN_STOP_PCT, 0.03);
      } catch (_) {}
      await sleep(60 + i*80);
    }
    return null;
  };

  for (const baseUrl of [SUPPORTED_EXCHANGES.mexc.baseUrl, SUPPORTED_EXCHANGES.binance.baseUrl]) {
    let v = await tryOne(baseUrl, "1h", 14);  if (v !== null) { stopPctCache.set(base, v); return v; }
    v = await tryOne(baseUrl, "30m", 30);     if (v !== null) { stopPctCache.set(base, v); return v; }
  }

  const fallback = Math.max(MIN_STOP_PCT, 0.01);
  stopPctCache.set(base, fallback);
  return fallback;
}

/* Orderbook snapshot + microstructure metrics (cached) */
async function getOrderBookSnapshot(symbol) {
  const base = (symbol || "").toUpperCase();
  const hit = orderbookCache.get(base);
  if (typeof hit !== "undefined") return hit;

  const tryFetch = async (baseUrl) => {
    const depthUrl = `${baseUrl}/api/v3/depth?symbol=${base}USDT&limit=20`;
    const res = await safeFetch(depthUrl, {}, 2500);
    return await res.json();
  };
  for (const b of [SUPPORTED_EXCHANGES.mexc.baseUrl, SUPPORTED_EXCHANGES.binance.baseUrl]) {
    try {
      const v = await tryFetch(b);
      orderbookCache.set(base, v);
      return v;
    } catch (_) {}
  }
  return null;
}
function computeSpreadDepth(book) {
  if (!book || !Array.isArray(book.bids) || !Array.isArray(book.asks) || book.bids.length === 0 || book.asks.length === 0) {
    return { BAS: 0.0005, OBD: 200000, mid: 1 };
  }
  const bestBid = parseFloat(book.bids[0][0]);
  const bestAsk = parseFloat(book.asks[0][0]);
  const mid = (bestBid + bestAsk) / 2;
  const BAS = mid > 0 ? (bestAsk - bestBid) / mid : 0.0005;
  const levels = 10;
  const sumDepth = (side) => {
    let total = 0;
    for (let i = 0; i < Math.min(levels, side.length); i++) {
      const price = parseFloat(side[i][0]);
      const qty = parseFloat(side[i][1]);
      if (isFinite(price) && isFinite(qty)) total += price * qty;
    }
    return total;
  };
  const OBD = Math.min(sumDepth(book.bids), sumDepth(book.asks));
  return { BAS: clamp(BAS, 0, 0.05), OBD: Math.max(1, OBD), mid };
}
/* Best bid/ask helper (for maker_join execution) */
function computeBestBidAsk(book) {
  if (!book || !Array.isArray(book.bids) || !Array.isArray(book.asks) || !book.bids.length || !book.asks.length) {
    return { bestBid: 0, bestAsk: 0 };
  }
  return { bestBid: parseFloat(book.bids[0][0] || 0), bestAsk: parseFloat(book.asks[0][0] || 0) };
}

/* Candles + returns utilities (for correlation checks) */
async function fetchKlines(symbol, interval = "1h", limit = 120) {
  const base = (symbol || "").toUpperCase();
  const urls = [
    `${SUPPORTED_EXCHANGES.mexc.baseUrl}/api/v3/klines?symbol=${base}USDT&interval=${interval}&limit=${limit}`,
    `${SUPPORTED_EXCHANGES.binance.baseUrl}/api/v3/klines?symbol=${base}USDT&interval=${interval}&limit=${limit}`
  ];
  for (const url of urls) {
    try {
      const r = await safeFetch(url, {}, 3000);
      const j = await r.json();
      if (Array.isArray(j) && j.length >= 2) return j;
    } catch (_) {}
  }
  return [];
}
function seriesReturnsFromKlines(klines) {
  const rets = [];
  for (let i = 1; i < klines.length; i++) {
    const c0 = parseFloat(klines[i - 1][4]);
    const c1 = parseFloat(klines[i][4]);
    if (c0 > 0 && isFinite(c1)) rets.push(Math.log(c1 / c0));
  }
  return rets;
}
function pearsonCorr(a, b) {
  const n = Math.min(a.length, b.length);
  if (n < 5) return 0;
  let sa = 0, sb = 0, Saa = 0, Sbb = 0, Sab = 0;
  for (let i = 0; i < n; i++) {
    const x = a[a.length - n + i], y = b[b.length - n + i];
    sa += x; sb += y; Saa += x*x; Sbb += y*y; Sab += x*y;
  }
  const cov = Sab - (sa*sb)/n;
  const va = Saa - (sa*sa)/n;
  const vb = Sbb - (sb*sb)/n;
  if (va <= 1e-12 || vb <= 1e-12) return 0;
  return clamp(cov / Math.sqrt(va*vb), -1, 1);
}

/* ---------- Ideas selection (prefer GitHub or largest set) ---------- */
async function getLatestIdeas(env) {
  // Prefer newest from GitHub/GHA
  const rowGit = await env.DB.prepare(
    "SELECT ideas_json FROM ideas WHERE json_extract(ideas_json,'$.meta.origin') IN ('gha','github_actions') ORDER BY ts DESC LIMIT 1"
  ).first();
  if (rowGit && rowGit.ideas_json) {
    try { return JSON.parse(rowGit.ideas_json); } catch {}
  }

  // Fallback: whatever has most ideas, then newest
  const rowBig = await env.DB.prepare(
    "SELECT ideas_json FROM ideas ORDER BY json_array_length(json_extract(ideas_json,'$.ideas')) DESC, ts DESC LIMIT 1"
  ).first();
  if (rowBig && rowBig.ideas_json) {
    try { return JSON.parse(rowBig.ideas_json); } catch {}
  }
  return null;
}

/* ---------- Exports for later sections ---------- */
export {
  PRICE_CACHE_TTL_MS, STOP_PCT_CACHE_TTL_MS, ORDERBOOK_CACHE_TTL_MS,
  priceCache, stopPctCache, orderbookCache,
  getCurrentPrice, calculateStopPercent,
  getOrderBookSnapshot, computeSpreadDepth, computeBestBidAsk,
  fetchKlines, seriesReturnsFromKlines, pearsonCorr,
  getLatestIdeas
};

/* ======================================================================
   SECTION 3/7 — Wallet/Equity, Cooldowns, HSE/Kelly, Exposure,
                  Logging, TCR pacing (flat CPU via KV counters)
   ====================================================================== */

/* ---------- Wallet & Equity ---------- */
async function getWalletBalance(env, userId) {
  const session = await getSession(env, userId);
  if (!session) return 0;

  // DEMO equity from PnL (original behavior)
  if (session.exchange_name === 'crypto_parrot') {
    let currentEquity = 10000; // demo starting equity

    // Aggregate closed PnL
    const sumClosed = await env.DB
      .prepare("SELECT COALESCE(SUM(realized_pnl),0) AS s FROM trades WHERE user_id = ? AND status = 'closed'")
      .bind(userId).first();
    currentEquity += Number(sumClosed?.s || 0);

    // Open trades PnL using cached prices
    const openTrades = await env.DB
      .prepare("SELECT symbol, side, entry_price, qty FROM trades WHERE user_id = ? AND status = 'open'")
      .bind(userId).all();

    for (const t of (openTrades.results || [])) {
      let p = await getCurrentPrice(t.symbol);
      if (!isFinite(p) || p <= 0) {
        try {
          const ob = await getOrderBookSnapshot(t.symbol);
          const { mid } = computeSpreadDepth(ob);
          if (isFinite(mid) && mid > 0) p = mid;
        } catch {}
      }
      if (!isFinite(p) || p <= 0) continue;

      const pnl = (t.side === 'SELL')
        ? (t.entry_price - p) * t.qty  // short
        : (p - t.entry_price) * t.qty; // long
      currentEquity += pnl;
    }
    return currentEquity;
  }

  // Real exchanges
  if (!session.api_key_encrypted) return 0;
  const apiKey = await decrypt(session.api_key_encrypted, env);
  const apiSecret = await decrypt(session.api_secret_encrypted, env);
  try {
    const result = await verifyApiKeys(apiKey, apiSecret, session.exchange_name);
    if (result.success && result.data.balance) {
      return parseFloat(String(result.data.balance).replace(" USDT", ""));
    }
  } catch (e) {
    console.error("getWalletBalance error:", e);
  }
  return 0;
}

async function getTotalCapital(env, userId) {
  const tc = await getWalletBalance(env, userId);
  const key = `peak_equity_user_${userId}`;
  const prev = Number(await kvGet(env, key) || 0);
  if (!prev || tc > prev) await kvSet(env, key, tc);
  return tc;
}
async function getPeakEquity(env, userId) {
  const key = `peak_equity_user_${userId}`;
  const val = Number(await kvGet(env, key) || 0);
  return val > 0 ? val : null;
}
async function getDrawdown(env, userId, tc) {
  const peak = (await getPeakEquity(env, userId)) ?? tc;
  return clamp(peak > 0 ? (peak - tc) / peak : 0, 0, 1);
}
async function getOpenPortfolioRisk(env, userId) {
  const row = await env.DB
    .prepare("SELECT SUM(risk_usd) as s FROM trades WHERE user_id = ? AND status = 'open'")
    .bind(userId).first();
  return Number(row?.s || 0);
}
async function getOpenPositionsCount(env, userId) {
  const row = await env.DB
    .prepare("SELECT COUNT(*) as c FROM trades WHERE user_id = ? AND status = 'open'")
    .bind(userId).first();
  return Number(row?.c || 0);
}

/* ---------- Notional tracking ---------- */
async function getOpenNotional(env, userId) {
  const open = await env.DB.prepare(
    "SELECT qty, entry_price FROM trades WHERE user_id = ? AND status = 'open'"
  ).bind(userId).all();

  let total = 0;
  for (const r of (open.results || [])) {
    const qty = Number(r.qty || 0);
    const entry = Number(r.entry_price || 0);
    if (isFinite(qty) && isFinite(entry)) total += qty * entry;
  }

  // include pending quotes
  const pend = await env.DB.prepare(
    "SELECT extra_json FROM trades WHERE user_id = ? AND status = 'pending'"
  ).bind(userId).all();

  for (const r of (pend.results || [])) {
    try {
      const ex = JSON.parse(r.extra_json || "{}");
      const q = Number(ex.quote_size || 0);
      if (isFinite(q)) total += q;
    } catch (_) {}
  }
  return total;
}

/* ---------- Active exposure helpers ---------- */
async function hasActiveExposure(env, userId, symbol) {
  const row = await env.DB
    .prepare("SELECT COUNT(*) AS c FROM trades WHERE user_id = ? AND symbol = ? AND status IN ('open','pending')")
    .bind(userId, String(symbol || '').toUpperCase())
    .first();
  return Number(row?.c || 0) > 0;
}
async function getActiveExposureSymbols(env, userId) {
  const rows = await env.DB
    .prepare("SELECT DISTINCT symbol FROM trades WHERE user_id = ? AND status IN ('open','pending')")
    .bind(userId)
    .all();
  const set = new Set();
  for (const r of (rows.results || [])) {
    const s = String(r.symbol || '').toUpperCase();
    if (s) set.add(s);
  }
  return set;
}

/* ---------- Cooldown (default from env.COOLDOWN_HOURS, fallback 3h) ---------- */
function defaultCooldownMs(env) {
  return Math.max(30 * 60 * 1000, Number(env.COOLDOWN_HOURS || 3) * 60 * 60 * 1000);
}
async function setSymbolCooldown(env, userId, symbol, ms) {
  const key = `cooldown_${userId}_${String(symbol || '').toUpperCase()}`;
  const until = Date.now() + Math.max(0, isFinite(ms) ? ms : defaultCooldownMs(env));
  await kvSet(env, key, until);
}
async function getSymbolCooldownRemainingMs(env, userId, symbol) {
  const key = `cooldown_${userId}_${String(symbol || '').toUpperCase()}`;
  const v = Number(await kvGet(env, key) || 0);
  const left = v - Date.now();
  return left > 0 ? left : 0;
}
async function isSymbolOnCooldown(env, userId, symbol) {
  return (await getSymbolCooldownRemainingMs(env, userId, symbol)) > 0;
}

/* ---------- ACP V20 Brain helpers ---------- */
function pLCBFromSQS(SQS) {
  const p = 0.25 + 0.5 * clamp(SQS, 0, 1);
  return clamp(p, 0.0, 1.0);
}
function ddScaler(dd) {
  if (dd < 0.02) return 1.0;
  if (dd < 0.06) return 0.7;
  return 0.4;
}
async function maxCorrelationWithOpen(env, userId, candidateSymbol) {
  const rows = await env.DB
    .prepare("SELECT symbol FROM trades WHERE user_id = ? AND status = 'open'")
    .bind(userId).all();

  const maxCompare = Number(env?.MAX_CORR_COMPARE || 3);
  if (!(maxCompare > 0)) return 0.0;

  const openSymsAll = (rows?.results || []).map(r => (r.symbol || "").toUpperCase());
  const openSyms = openSymsAll.slice(0, Math.max(0, maxCompare));
  if (!openSyms.length) return 0.0;

  const kA = await fetchKlines(candidateSymbol, "1h", 120);
  const a = seriesReturnsFromKlines(kA);
  if (a.length < 10) return 0.0;

  let rho_max = 0.0;
  for (const s of openSyms) {
    const kB = await fetchKlines(s, "1h", 120);
    const b = seriesReturnsFromKlines(kB);
    if (b.length < 10) continue;
    const rho = pearsonCorr(a, b);
    rho_max = Math.max(rho_max, rho);
  }
  return clamp(rho_max, -1, 1);
}
async function computeHSEAndCosts(symbol, sL, feeRatePerSide) {
  const book = await getOrderBookSnapshot(symbol);
  const { BAS, OBD } = computeSpreadDepth(book);
  const MV = sL, RS = 1.0, ES = 1.0;

  const HSE_raw =
    HSE_CFG.AF *
    Math.pow(1.0, HSE_CFG.gamma) *
    Math.pow((BAS / (HSE_CFG.BAS_avg || 1)) * ((HSE_CFG.OBD_avg || 1) / Math.max(OBD, 1)), HSE_CFG.w_cost) *
    Math.pow((MV / (HSE_CFG.MV_avg || 1)), HSE_CFG.w_vol) *
    Math.pow((RS / (HSE_CFG.RS_avg || 1)), HSE_CFG.w_rs) *
    Math.pow((ES / (HSE_CFG.ES_avg || 1)), HSE_CFG.w_es);

  const pH = sigmoid(HSE_CFG.alpha0 + HSE_CFG.alpha1 * Math.log(Math.max(HSE_raw, 1e-12)));

  const stop_bps = sL * 10000.0;
  const slip_R = (HSE_CFG.sSlipStarBps * pH) / Math.max(stop_bps, 1e-6);
  const spread_R = BAS / Math.max(sL, 1e-9);
  const fee_R = (2 * feeRatePerSide) / Math.max(sL, 1e-9);
  const borrow_funding_R = 0.0;
  const cost_R = slip_R + spread_R + fee_R + borrow_funding_R;

  return { HSE_raw, pH, slip_R, spread_R, fee_R, borrow_funding_R, cost_R, BAS, OBD, MV, RS, ES };
}
function kellyFraction(EV_R, p, RRR) {
  const Var_R = p * Math.pow(RRR - EV_R, 2) + (1 - p) * Math.pow(-1 - EV_R, 2);
  const f_k = clamp(EV_R / Math.max(Var_R, EPS_VARIANCE), 0, 1);
  return { f_k, Var_R };
}
function SQSfromScore(score) {
  return clamp(Math.sqrt(clamp((Number(score) || 0) / 100, 0, 1)), 0, 1);
}

/* ---------- Logging ---------- */
async function logEvent(env, userId, eventType, payload) {
  const body = JSON.stringify({ ts: nowISO(), event: eventType, user_id: userId, ...payload });
  try {
    await env.DB
      .prepare("INSERT INTO events_log (user_id, event_type, payload) VALUES (?, ?, ?)")
      .bind(userId, eventType, body)
      .run();
  } catch (e) {
    console.error("logEvent DB error:", e);
  }

  if (env.SHEETS_WEBHOOK_URL) {
    try {
      await safeFetch(env.SHEETS_WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": `Bearer ${env.SHEETS_WEBHOOK_TOKEN || ''}` },
        body
      }, 6000);
    } catch (e) {
      console.error("Sheets webhook error:", e);
    }
  }
}

/* ---------- Flat-CPU TCR pacing counters (KV) ---------- */
/* UTC day key helper */
function utcDayKey(d = new Date()) {
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`;
}

/* Ensure day roll: move yesterday's todayCount into rolling, reset todayCount on UTC midnight */
async function ensureDayRoll(env, userId) {
  const today = utcDayKey();
  const kDate = `tcr_today_date_${userId}`;
  const kCount = `tcr_today_count_${userId}`;
  const kRoll = `tcr_7d_roll_${userId}`;

  let curDate = await kvGet(env, kDate);
  if (!curDate) {
    await kvSet(env, kDate, today);
    await kvSet(env, kCount, 0);
    await kvSet(env, kRoll, JSON.stringify({ last7: [] }));
    return;
  }
  if (curDate !== today) {
    const yCount = Number(await kvGet(env, kCount) || 0);
    let roll;
    try { roll = JSON.parse((await kvGet(env, kRoll)) || '{"last7":[]}'); } catch { roll = { last7: [] }; }
    // Append yesterday with its date (curDate)
    roll.last7.push({ d: curDate, c: yCount });
    // Keep only last 7 days (completed days). We will still compute average with today later.
    if (roll.last7.length > 7) roll.last7 = roll.last7.slice(-7);

    await kvSet(env, kRoll, JSON.stringify(roll));
    await kvSet(env, kDate, today);
    await kvSet(env, kCount, 0);
  }
}

/* Increment today's trade count (called on each INSERT into trades) */
async function bumpTradeCounters(env, userId, delta = 1) {
  await ensureDayRoll(env, userId);
  const kCount = `tcr_today_count_${userId}`;
  const cur = Number(await kvGet(env, kCount) || 0);
  await kvSet(env, kCount, cur + (isFinite(delta) ? delta : 1));
}

/* Read pacing counters (today + last7 completed days) */
async function getPacingCounters(env, userId) {
  await ensureDayRoll(env, userId);
  const kCount = `tcr_today_count_${userId}`;
  const kRoll = `tcr_7d_roll_${userId}`;
  const todayCount = Number(await kvGet(env, kCount) || 0);
  let roll;
  try { roll = JSON.parse((await kvGet(env, kRoll)) || '{"last7":[]}'); } catch { roll = { last7: [] }; }
  const last7 = Array.isArray(roll.last7) ? roll.last7 : [];
  // Normalize to only keep last 6 previous days (optional), but we'll compute avg with 7 slots including today.
  const last7Counts = last7.slice(-6).map(x => Number(x?.c || 0));
  return { todayCount, last7Counts };
}

/* Optional DB resync for pacing counters (used after bulk deletes like Clean 🧹) */
async function resyncPacingFromDB(env, userId) {
  const today = utcDayKey();
  const kDate = `tcr_today_date_${userId}`;
  const kCount = `tcr_today_count_${userId}`;
  const kRoll = `tcr_7d_roll_${userId}`;

  // Fetch per-day counts for the last 7 days including today
  const rows = await env.DB.prepare(
    "SELECT DATE(created_at) AS d, COUNT(*) AS c FROM trades WHERE user_id = ? AND DATE(created_at) >= DATE('now', '-6 days') GROUP BY DATE(created_at) ORDER BY DATE(created_at)"
  ).bind(userId).all();

  const map = new Map();
  for (const r of (rows.results || [])) {
    const d = String(r.d || '');
    const c = Number(r.c || 0);
    if (d) map.set(d, c);
  }

  const todayCount = Number(map.get(today) || 0);
  // Build last7 for the previous 6 days (oldest->newest)
  const last7 = [];
  for (let i = 6; i >= 1; i--) {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - i);
    const dk = utcDayKey(d);
    last7.push({ d: dk, c: Number(map.get(dk) || 0) });
  }

  await kvSet(env, kDate, today);
  await kvSet(env, kCount, todayCount);
  await kvSet(env, kRoll, JSON.stringify({ last7 }));
  return { todayCount, last7 };
}

/* ---------- TCR gate per user (pacing — flat CPU) ---------- */
function dayProgressUTC() {
  const now = new Date();
  const utc = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), now.getUTCHours(), now.getUTCMinutes(), now.getUTCSeconds());
  const start = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0);
  return Math.min(1, Math.max(0, (utc - start) / 86400000));
}
function computeDynamicSqsGate(baseGate, targetDaily, openedSoFar) {
  const p = dayProgressUTC();
  const expected = targetDaily * p;
  const diff = openedSoFar - expected;
  let adj = 0;
  if (diff > 1) adj = +0.05;
  else if (diff < -1) adj = -0.05;
  return clampRange((Number(baseGate) || 0.30) + adj, 0.25, 0.50);
}

/* Flat-CPU version: no history scans per cycle; exact same pacing behavior */
async function computeUserSqsGate(env, userId) {
  const baseGate = Number(env?.SQS_MIN_GATE ?? SQS_MIN_DEFAULT);
  const explicitTarget = Number(env?.TCR_TARGET_TRADES || 0);

  const { todayCount, last7Counts } = await getPacingCounters(env, userId);
  // Original average used last 7 days inclusive of today; replicate: divide by 7 always
  const sum7 = todayCount + last7Counts.reduce((a, b) => a + b, 0);
  const avg7 = sum7 / 7;
  const target = explicitTarget > 0 ? explicitTarget : Math.max(1, Math.round(avg7));

  const openedSoFar = todayCount; // same as DATE(created_at)=today in original
  return computeDynamicSqsGate(baseGate, target, openedSoFar);
}

/* ---------- Exports ---------- */
export {
  getWalletBalance, getTotalCapital, getPeakEquity, getDrawdown,
  getOpenPortfolioRisk, getOpenPositionsCount, getOpenNotional,
  hasActiveExposure, getActiveExposureSymbols,
  defaultCooldownMs, setSymbolCooldown, getSymbolCooldownRemainingMs, isSymbolOnCooldown,
  pLCBFromSQS, ddScaler, maxCorrelationWithOpen, computeHSEAndCosts, kellyFraction, SQSfromScore,
  logEvent,
  // pacing (flat-CPU)
  utcDayKey, ensureDayRoll, bumpTradeCounters, getPacingCounters, resyncPacingFromDB,
  dayProgressUTC, computeDynamicSqsGate, computeUserSqsGate
};

/* ======================================================================
   SECTION 4/7 — Exchange verification, signing helpers, orders & routing
   ====================================================================== */

/* ---------- HMAC / Hash helpers ---------- */
async function hmacHexStr(secret, data, algo = "SHA-256") {
  const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(secret), { name: "HMAC", hash: algo }, false, ["sign"]);
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(data));
  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, "0")).join("");
}
async function hmacB64Str(secret, data, algo = "SHA-256") {
  const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(secret), { name: "HMAC", hash: algo }, false, ["sign"]);
  const sig = await crypto.subtle.sign("HMAC", key, data instanceof Uint8Array ? data : new TextEncoder().encode(data));
  return btoa(String.fromCharCode(...new Uint8Array(sig)));
}
async function hmacB64Bytes(secretBytes, dataBytes, algo = "SHA-512") {
  const key = await crypto.subtle.importKey("raw", secretBytes, { name: "HMAC", hash: algo }, false, ["sign"]);
  const sig = await crypto.subtle.sign("HMAC", key, dataBytes);
  return btoa(String.fromCharCode(...new Uint8Array(sig)));
}
async function sha256Bytes(dataBytes) {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", dataBytes));
}
function md5Hex(str) {
  function cmn(q, a, b, x, s, t) { a = (((a + q) | 0) + ((x + t) | 0)) | 0; return (((a << s) | (a >>> (32 - s))) + b) | 0; }
  function ff(a,b,c,d,x,s,t){ return cmn((b & c) | ((~b) & d), a, b, x, s, t); }
  function gg(a,b,c,d,x,s,t){ return cmn((b & d) | (c & (~d)), a, b, x, s, t); }
  function hh(a,b,c,d,x,s,t){ return cmn(b ^ c ^ d, a, b, x, s, t); }
  function ii(a,b,c,d,x,s,t){ return cmn(c ^ (b | (~d)), a, b, x, s, t); }
  function md51(s) {
    const n = s.length; const state = [1732584193, -271733879, -1732584194, 271733878];
    let i; for (i = 64; i <= n; i += 64) md5cycle(state, md5blk(s.substring(i - 64, i)));
    s = s.substring(i - 64); const tail = new Array(16).fill(0);
    for (let i = 0; i < s.length; i++) tail[i >> 2] |= s.charCodeAt(i) << ((i % 4) << 3);
    tail[i >> 2] |= 0x80 << ((i % 4) << 3);
    if (i > 55) { md5cycle(state, tail); for (i = 0; i < 16; i++) tail[i] = 0; }
    const tmp = n * 8; tail[14] = tmp & 0xffffffff; tail[15] = (tmp / 0x100000000) | 0;
    md5cycle(state, tail); return state;
  }
  function md5blk(s) { const md5blks = new Array(16); for (let i = 0; i < 64; i += 4) md5blks[i >> 2] = s.charCodeAt(i) + (s.charCodeAt(i+1)<<8) + (s.charCodeAt(i+2)<<16) + (s.charCodeAt(i+3)<<24); return md5blks; }
  function md5cycle(x, k) {
    let [a,b,c,d] = x;
    a = ff(a,b,c,d,k[0],7,-680876936); d = ff(d,a,b,c,d,k[1],12,-389564586); c = ff(c,d,a,b,k[2],17,606105819); b = ff(b,c,d,a,k[3],22,-1044525330);
    a = ff(a,b,c,d,k[4],7,1770035416); d = ff(d,a,b,c,d,k[5],12,-40341101); c = ff(c,d,a,b,k[6],17,-1473231341); b = ff(b,c,d,a,k[7],22,-45705983);
    a = ff(a,b,c,d,k[8],7,1770035416); d = ff(d,a,b,c,d,k[9],12,-1958414417); c = ff(c,d,a,b,k[10],17,-42063); b = ff(b,c,d,a,k[11],22,-1990404162);
    a = ff(a,b,c,d,k[12],7,1804603682); d = ff(d,a,b,c,d,k[13],12,-40341101); c = ff(c,d,a,b,k[14],17,-1502002290); b = ff(b,c,d,a,k[15],22,1236535329);
    a = gg(a,b,c,d,k[1],5,-165796510); d = gg(d,a,b,c,d,k[6],9,-1069501632); c = gg(c,d,a,b,k[11],14,643717713); b = gg(b,c,d,a,k[0],20,-373897302);
    a = gg(a,b,c,d,k[5],5,-701558691); d = gg(d,a,b,c,d,k[10],9,38016083); c = gg(c,d,a,b,k[15],14,-660478335); b = gg(b,c,d,a,k[4],20,-405537848);
    a = gg(a,b,c,d,k[9],5,568446438); d = gg(d,a,b,c,d,k[14],9,-1019803690); c = gg(c,d,a,b,k[3],14,-187363961); b = gg(b,c,d,a,k[8],20,1163531501);
    a = ii(a,b,c,d,k[0],6,-198630844); d = ii(d,a,b,c,d,k[7],10,1126891415); c = ii(c,d,a,b,k[14],15,-1416354905); b = ii(b,c,d,a,k[5],21,-57434055);
    a = ii(a,b,c,d,k[12],6,1700485571); d = ii(d,a,b,c,d,k[3],10,-1894986606); c = ii(c,d,a,b,k[10],15,-1051523); b = ii(b,c,d,a,k[1],21,-2054922799);
    a = ii(a,b,c,d,k[8],6,1873313359); d = ii(d,a,b,c,d,k[15],10,-30611744); c = ii(c,d,a,b,k[6],15,-1560198380); b = ii(b,c,d,a,k[13],21,1309151649);
    a = ii(a,b,c,d,k[4],6,-145523070); d = ii(d,a,b,c,d,k[11],10,-1120210379); c = ii(c,d,a,b,k[2],15,718787259); b = ii(b,c,d,a,k[9],21,-343485551);
  }
  function rhex(n){ let s="", j=0; for(; j<4; j++) s += ((n >> (j*8+4)) & 0x0F).toString(16) + ((n >> (j*8)) & 0x0F).toString(16); return s; }
  return md51(str).map(rhex).join("");
}

/* ---------- Balance parsers ---------- */
function parseUSDT_BinanceLike(data) {
  if (Array.isArray(data?.balances)) {
    const usdt = data.balances.find((b) => b.asset === "USDT");
    if (usdt) return Number(usdt.free || "0").toFixed(2);
  }
  return "0.00";
}
function parseUSDT_Gate(data) {
  if (Array.isArray(data)) {
    const row = data.find((x) => (x.currency || "").toUpperCase() === "USDT");
    if (row) return Number(row.available || "0").toFixed(2);
  }
  return "0.00";
}
function parseUSDT_Bybit(data) {
  const coins = data?.result?.list?.[0]?.coin;
  if (Array.isArray(coins)) {
    const c = coins.find((x) => (x.coin || "").toUpperCase() === "USDT");
    if (c) {
      const v = c.availableToWithdraw ?? c.walletBalance ?? c.equity ?? "0";
      return Number(v).toFixed(2);
    }
  }
  const spot = data?.result?.balances || data?.result?.spot || [];
  if (Array.isArray(spot)) {
    const c = spot.find((x) => (x.coin || x.asset || "").toUpperCase() === "USDT");
    if (c) {
      const v = c.free ?? c.available ?? "0";
      return Number(v).toFixed(2);
    }
  }
  return "0.00";
}
function parseUSDT_Kraken(data) {
  if (data?.result && typeof data.result === "object") {
    const v = data.result.USDT || data.result.usdt || "0";
    return Number(v).toFixed(2);
  }
  return "0.00";
}
function parseUSDT_CoinEx(data) {
  const balances = data?.data?.balances;
  if (Array.isArray(balances)) {
    const row = balances.find((b) => (b.asset || b.currency || "").toUpperCase() === "USDT");
    if (row) return Number(row.available || row.available_balance || row.balance || "0").toFixed(2);
  }
  const list = data?.data?.list;
  if (list && typeof list === "object" && list.USDT) {
    const v = list.USDT.available || list.USDT.available_amount || list.USDT.balance || "0";
    return Number(v).toFixed(2);
  }
  return "0.00";
}
function parseUSDT_Huobi(data) {
  const arr = data?.data?.list || [];
  let sum = 0;
  for (const r of arr) {
    if ((r.currency || "").toUpperCase() === "USDT" && r.type === "trade") sum += Number(r.balance || "0");
  }
  return sum.toFixed(2);
}
function parseUSDT_LBank(data) {
  const free = data?.info?.funds?.free || data?.data?.free || data?.free;
  const v = free?.usdt || free?.USDT || "0";
  return Number(v).toFixed(2);
}

/* ---------- Exchange verification calls ---------- */
async function verifyBinanceLike(apiKey, apiSecret, ex) {
  const ts = Date.now();
  const parts = [];
  if (ex.defaultQuery) parts.push(ex.defaultQuery);
  parts.push(`timestamp=${ts}`);
  const query = parts.join("&");
  const signature = await hmacHexStr(apiSecret, query, "SHA-256");
  const url = `${ex.baseUrl}${ex.accountPath}?${query}&signature=${signature}`;
  const res = await safeFetch(url, { headers: { [ex.apiKeyHeader]: apiKey } }, 5000);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const reason = data?.msg || data?.message || `HTTP ${res.status}`;
    return { success: false, reason };
  }
  const bal = parseUSDT_BinanceLike(data);
  const maker = (data?.makerCommission ?? 10) / 10000;
  const taker = (data?.takerCommission ?? 10) / 10000;
  return { success: true, data: { balance: `${bal} USDT`, feeRate: taker, fees: { maker, taker } } };
}
async function verifyBinanceFutures(apiKey, apiSecret, ex) {
  const ts = Date.now();
  const query = `timestamp=${ts}`;
  const signature = await hmacHexStr(apiSecret, query, "SHA-256");
  const url = `${ex.baseUrl}/fapi/v2/account?${query}&signature=${signature}`;
  const res = await safeFetch(url, { headers: { [ex.apiKeyHeader]: apiKey } }, 5000).catch(() => null);
  if (!res) return { success: false, reason: "Network error" };
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const reason = data?.msg || data?.message || `HTTP ${res.status}`;
    return { success: false, reason };
  }
  let bal = "0.00";
  try {
    const usdt = (data.assets || []).find(a => (a.asset || "").toUpperCase() === "USDT");
    if (usdt) bal = Number(usdt.availableBalance ?? usdt.walletBalance ?? 0).toFixed(2);
  } catch {}
  return { success: true, data: { balance: `${bal} USDT`, feeRate: 0.0004 } };
}
async function verifyBybit(apiKey, apiSecret, ex) {
  const ts = Date.now().toString();
  const recv = "5000";
  const tryKind = async (accountType) => {
    const qs = `accountType=${accountType}&coin=USDT`;
    const pre = ts + apiKey + recv + qs;
    const sig = await hmacHexStr(apiSecret, pre, "SHA-256");
    const url = `${ex.baseUrl}/v5/account/wallet-balance?${qs}`;
    const r = await safeFetch(url, {
      headers: {
        "X-BAPI-API-KEY": apiKey, "X-BAPI-TIMESTAMP": ts, "X-BAPI-RECV-WINDOW": recv, "X-BAPI-SIGN": sig,
      },
    }, 5000);
    const d = await r.json().catch(() => ({}));
    return { ok: r.ok && d?.retCode === 0, d, status: r.status };
  };
  let r = await tryKind("UNIFIED");
  if (!r.ok) r = await tryKind("SPOT");
  if (!r.ok) return { success: false, reason: r.d?.retMsg || `HTTP ${r.status}` };
  const bal = parseUSDT_Bybit(r.d);
  return { success: true, data: { balance: `${bal} USDT` } };
}
async function verifyKraken(apiKey, apiSecret, ex) {
  const path = "/0/private/Balance";
  const nonce = (Date.now() * 1000).toString();
  const bodyStr = `nonce=${nonce}`;
  const messageBytes = new TextEncoder().encode(nonce + bodyStr);
  const hash256 = await sha256Bytes(messageBytes);
  const pathBytes = new TextEncoder().encode(path);
  const preBytes = new Uint8Array(pathBytes.length + hash256.length);
  preBytes.set(pathBytes, 0);
  preBytes.set(hash256, pathBytes.length);
  const secretBytes = Uint8Array.from(atob(apiSecret), (c) => c.charCodeAt(0));
  const sig = await hmacB64Bytes(secretBytes, preBytes, "SHA-512");
  const url = `${ex.baseUrl}${path}`;
  const res = await safeFetch(url, {
    method: "POST",
    headers: { "API-Key": apiKey, "API-Sign": sig, "Content-Type": "application/x-www-form-urlencoded" },
    body: bodyStr,
  }, 6000);
  const data = await res.json().catch(() => ({}));
  if (!res.ok || (data?.error && data.error.length)) {
    const reason = (data?.error && data.error.join(", ")) || `HTTP ${res.status}`;
    return { success: false, reason };
  }
  const bal = parseUSDT_Kraken(data);
  return { success: true, data: { balance: `${bal} USDT` } };
}
async function verifyGate(apiKey, apiSecret, ex) {
  const path = "/api/v4/spot/accounts";
  const ts = Math.floor(Date.now() / 1000).toString();
  const prehash = `GET\n${path}\n\n\n${ts}`;
  const sign = await hmacHexStr(apiSecret, prehash, "SHA-512");
  const url = `${ex.baseUrl}${path}`;
  const res = await safeFetch(url, { method: "GET", headers: { KEY: apiKey, Timestamp: ts, SIGN: sign } }, 6000);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) return { success: false, reason: data?.message || `HTTP ${res.status}` };
  const bal = parseUSDT_Gate(data);
  return { success: true, data: { balance: `${bal} USDT` } };
}
async function huobiSignedGet(ex, apiKey, apiSecret, path, extraParams = {}) {
  const host = ex.baseHost;
  const method = "GET";
  const ts = new Date().toISOString().replace(/\.\d+Z$/, "Z");
  const params = { AccessKeyId: apiKey, SignatureMethod: "HmacSHA256", SignatureVersion: "2", Timestamp: ts, ...extraParams };
  const sorted = Object.keys(params).sort();
  const canonical = sorted.map((k) => `${k}=${percentEncode(String(params[k]))}`).join("&");
  const toSign = `${method}\n${host}\n${path}\n${canonical}`;
  const sig = await hmacB64Str(apiSecret, toSign, "SHA-256");
  const finalQuery = `${canonical}&Signature=${percentEncode(sig)}`;
  const url = `${ex.scheme}://${host}${path}?${finalQuery}`;
  const res = await safeFetch(url, {}, 6000);
  const data = await res.json().catch(() => ({}));
  return { res, data };
}
async function verifyHuobi(apiKey, apiSecret, ex) {
  const r1 = await huobiSignedGet(ex, apiKey, apiSecret, "/v1/account/accounts");
  if (!r1.res.ok || r1.data?.status !== "ok") {
    const reason = r1.data?.err_msg || r1.data?.message || `HTTP ${r1.res.status}`;
    return { success: false, reason };
  }
  const acc = (r1.data.data || []).find((a) => a.type === "spot" && a.state === "working") || (r1.data.data || [])[0];
  if (!acc) return { success: false, reason: "No spot account found" };
  const path2 = `/v1/account/accounts/${acc.id}/balance`;
  const r2 = await huobiSignedGet(ex, apiKey, apiSecret, path2);
  if (!r2.res.ok || r2.data?.status !== "ok") {
    const reason = r2.data?.err_msg || r2.data?.message || `HTTP ${r2.res.status}`;
    return { success: false, reason };
  }
  const bal = parseUSDT_Huobi(r2.data);
  return { success: true, data: { balance: `${bal} USDT` } };
}
async function verifyCoinEx(apiKey, apiSecret, ex) {
  const path = "/v2/assets/balance";
  const ts = Date.now().toString();
  const body = "";
  const signStr = ts + "GET" + path + body;
  const sign = await hmacHexStr(apiSecret, signStr);
  const url = `${ex.baseUrl}${path}`;
  const res = await safeFetch(url, {
    method: "GET",
    headers: {
      "X-COINEX-TIMESTAMP": ts,
      "X-COINEX-KEY": apiKey,
      "X-COINEX-SIGN": sign
    }
  }, 6000);
  const data = await res.json().catch(()=>({}));
  if (!res.ok || (data?.code && data.code !== 0)) {
    return { success: false, reason: data?.message || data?.msg || `HTTP ${res.status}` };
  }
  const bal = parseUSDT_CoinEx(data);
  return { success: true, data: { balance: `${bal} USDT` } };
}
async function verifyLBank(apiKey, apiSecret, ex) {
  const path = "/v2/user_info.do";
  const ts = Date.now().toString();
  const params = { api_key: apiKey, timestamp: ts };
  const paramStr = Object.entries(params).sort(([a],[b])=>a.localeCompare(b)).map(([k, v]) => `${k}=${v}`).join("&");
  const signStr = paramStr + `&secret_key=${apiSecret}`;
  const sign = md5Hex(signStr).toUpperCase();
  const body = paramStr + `&sign=${sign}`;
  const url = `${ex.baseUrl}${path}`;
  const res = await safeFetch(url, { method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body }, 6000);
  const data = await res.json().catch(()=>({}));
  if (!res.ok || data?.result === false) {
    return { success: false, reason: data?.error_code || data?.msg || `HTTP ${res.status}` };
  }
  return { success: true, data: { balance: parseUSDT_LBank(data) + " USDT" } };
}

/* ---------- Verify API keys (dispatcher) ---------- */
async function verifyApiKeys(apiKey, apiSecret, exchangeName) {
  const id = (exchangeName || "").toLowerCase();
  const ex = SUPPORTED_EXCHANGES[id];
  if (!ex) return { success: false, reason: `Exchange '${exchangeName}' not supported.` };
  try {
    switch (ex.kind) {
      case "demoParrot":
        if (!apiKey || !apiSecret) return { success: false, reason: "Please enter any text for API Key and Secret (e.g., 'demo')." };
        return { success: true, data: { balance: "10000.00 USDT", feeRate: 0.001, fees: { maker: 0.001, taker: 0.001 } } };

      // Spot
      case "binanceLike":       return await verifyBinanceLike(apiKey, apiSecret, ex);
      case "bybitV5":           return await verifyBybit(apiKey, apiSecret, ex);
      case "krakenV0":          return await verifyKraken(apiKey, apiSecret, ex);
      case "gateV4":            return await verifyGate(apiKey, apiSecret, ex);
      case "huobiV1":           return await verifyHuobi(apiKey, apiSecret, ex);
      case "coinexV2":          return await verifyCoinEx(apiKey, apiSecret, ex);
      case "lbankV2":           return await verifyLBank(apiKey, apiSecret, ex);

      // Futures
      case "binanceFuturesUSDT": return await verifyBinanceFutures(apiKey, apiSecret, ex);

      default: return { success: false, reason: "Not implemented." };
    }
  } catch (e) {
    console.error("verifyApiKeys error:", e);
    return { success: false, reason: "Error talking to exchange." };
  }
}

/* ---------- Orders (Spot/Futures) ---------- */
/* Spot MARKET (Binance-like: Binance, MEXC) */
async function placeBinanceLikeOrder(ex, apiKey, apiSecret, symbol, side, amount, isQuoteOrder) {
  const endpoint = "/api/v3/order";
  const params = { symbol: symbol + "USDT", side, type: "MARKET", newOrderRespType: "FULL", timestamp: Date.now() };

  if (side === "BUY" && isQuoteOrder) {
    params.quoteOrderQty = Number(amount).toFixed(2);
  } else if (side === "SELL" && !isQuoteOrder) {
    params.quantity = Number(amount).toFixed(6);
  } else if (side === "SELL" && isQuoteOrder) {
    const price = await getCurrentPrice(symbol);
    if (!price || !isFinite(price) || price <= 0) throw new Error("Failed to fetch valid price for quantity calculation (SELL)");
    params.quantity = (Number(amount) / price).toFixed(6);
  } else {
    const price = await getCurrentPrice(symbol);
    if (!price || !isFinite(price) || price <= 0) throw new Error("Failed to fetch valid price for quantity calculation");
    params.quantity = (Number(amount) / price).toFixed(6);
  }

  if (ex.defaultQuery) {
    const defaults = new URLSearchParams(ex.defaultQuery);
    for (const [k, v] of defaults) params[k] = v;
  }

  const queryString = Object.entries(params).map(([k, v]) => `${k}=${v}`).join("&");
  const signature = await hmacHexStr(apiSecret, queryString);
  const url = `${ex.baseUrl}${endpoint}?${queryString}&signature=${signature}`;
  const res = await safeFetch(url, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.msg || data?.message || `HTTP ${res.status}`);

  let avgPrice = 0;
  if (Array.isArray(data.fills) && data.fills.length > 0) {
    let totalQuote = 0, totalBase = 0;
    for (const f of data.fills) {
      const qty = parseFloat(f.qty || f.qty_filled || 0);
      const prc = parseFloat(f.price || 0);
      totalBase += qty; totalQuote += qty * prc;
    }
    if (totalBase > 0) avgPrice = totalQuote / totalBase;
  }
  if ((!avgPrice || !isFinite(avgPrice)) && data.cummulativeQuoteQty && data.executedQty) {
    const executed = parseFloat(data.executedQty), cumQuote = parseFloat(data.cummulativeQuoteQty);
    if (executed > 0 && isFinite(cumQuote)) avgPrice = cumQuote / executed;
  }
  return { orderId: data.orderId, executedQty: parseFloat(data.executedQty || 0), avgPrice: avgPrice || (data.price ? parseFloat(data.price) : 0), status: data.status };
}

/* Spot LIMIT_MAKER (post-only) for Binance-like (Binance, likely MEXC) */
async function placeBinanceLikeLimitMaker(ex, apiKey, apiSecret, symbol, side, limitPrice, amount, isQuoteOrder) {
  const endpoint = "/api/v3/order";
  const params = {
    symbol: symbol.toUpperCase() + "USDT",
    side,
    type: "LIMIT_MAKER", // post-only
    newOrderRespType: "FULL",
    price: Number(limitPrice).toFixed(8),
    timestamp: Date.now()
  };

  // quoteOrderQty not supported for LIMIT; compute quantity
  if (side === "BUY" && isQuoteOrder) {
    const p = Number(limitPrice);
    params.quantity = (Number(amount) / Math.max(1e-12, p)).toFixed(6);
  } else if (side === "SELL" && !isQuoteOrder) {
    params.quantity = Number(amount).toFixed(6);
  } else if (side === "SELL" && isQuoteOrder) {
    const p = Number(limitPrice);
    params.quantity = (Number(amount) / Math.max(1e-12, p)).toFixed(6);
  } else {
    const p = Number(limitPrice);
    params.quantity = (Number(amount) / Math.max(1e-12, p)).toFixed(6);
  }

  if (ex.defaultQuery) {
    const defaults = new URLSearchParams(ex.defaultQuery);
    for (const [k, v] of defaults) params[k] = v;
  }

  const queryString = Object.entries(params).map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join("&");
  const signature = await hmacHexStr(apiSecret, queryString);
  const url = `${ex.baseUrl}${endpoint}?${queryString}&signature=${signature}`;
  const res = await safeFetch(url, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    // Typical post-only rejection if price would cross: "Order would immediately match and take"
    throw new Error(data?.msg || data?.message || `HTTP ${res.status}`);
  }

  // If posted (NEW) no fills; if maker-filled instantly, executedQty>0
  const executedQty = parseFloat(data.executedQty || 0);
  let avgPrice = 0;
  if (executedQty > 0) {
    const cumQuote = parseFloat(data.cummulativeQuoteQty || 0);
    if (isFinite(cumQuote) && executedQty > 0) avgPrice = cumQuote / executedQty;
  }
  return { orderId: data.orderId, executedQty, avgPrice, status: data.status || "NEW" };
}

/* LBank MARKET (limit not implemented here for post-only) */
async function placeLBankOrder(ex, apiKey, apiSecret, symbol, type, amount) {
  const path = "/v2/create_order.do";
  const ts = Date.now().toString();
  const params = { api_key: apiKey, symbol: symbol.toLowerCase() + "_usdt", type, amount: Number(amount).toFixed(6), timestamp: ts };
  const paramStr = Object.entries(params).sort(([a],[b])=>a.localeCompare(b)).map(([k, v]) => `${k}=${v}`).join("&");
  const signStr = paramStr + `&secret_key=${apiSecret}`;
  const sign = md5Hex(signStr).toUpperCase();
  const body = paramStr + `&sign=${sign}`;
  const url = `${ex.baseUrl}${path}`;
  const res = await safeFetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body
  }, 6000);
  const data = await res.json().catch(()=>({}));
  if (!res.ok || data?.result === false) throw new Error(data?.error_code || data?.msg || `HTTP ${res.status}`);
  return { orderId: data.order_id, executedQty: 0, avgPrice: 0, status: "NEW" };
}

/* CoinEx MARKET (post-only unsupported here) */
async function placeCoinExOrder(ex, apiKey, apiSecret, symbol, side, amount) {
  const path = "/v2/order/market";
  const ts = Date.now().toString();
  const body = { market: symbol.toUpperCase() + "USDT", side, amount: Number(amount).toFixed(6) };
  const bodyStr = JSON.stringify(body);
  const signStr = ts + "POST" + path + bodyStr;
  const sign = await hmacHexStr(apiSecret, signStr);
  const url = `${ex.baseUrl}${path}`;
  const res = await safeFetch(url, {
    method: "POST",
    headers: {
      "X-COINEX-TIMESTAMP": ts,
      "X-COINEX-KEY": apiKey,
      "X-COINEX-SIGN": sign,
      "Content-Type": "application/json"
    },
    body: bodyStr
  }, 6000);
  const data = await res.json().catch(()=>({}));
  if (!res.ok || (data?.code && data.code !== 0)) throw new Error(data?.message || data?.msg || `HTTP ${res.status}`);
  return { orderId: data.data.order_id, executedQty: parseFloat(data.data.filled_amount || 0), avgPrice: parseFloat(data.data.avg_price || 0), status: data.data.status };
}

// Binance Futures (USDT-M) MARKET
async function placeBinanceFuturesOrder(ex, apiKey, apiSecret, symbol, side, baseQty, reduceOnly = false) {
  const endpoint = "/fapi/v1/order";
  const params = {
    symbol: symbol.toUpperCase() + "USDT",
    side, type: "MARKET",
    quantity: Number(baseQty).toFixed(6),
    reduceOnly: reduceOnly ? "true" : "false",
    newOrderRespType: "RESULT",
    timestamp: Date.now()
  };
  const queryString = Object.entries(params).map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join("&");
  const signature = await hmacHexStr(apiSecret, queryString);
  const url = `${ex.baseUrl}${endpoint}?${queryString}&signature=${signature}`;
  const res = await safeFetch(url, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.msg || data?.message || `HTTP ${res.status}`);
  const executedQty = parseFloat(data.executedQty || data.origQty || 0);
  const cumQuote = parseFloat(data.cumQuote || data.cummulativeQuoteQty || 0);
  const avgPrice = executedQty > 0 ? (cumQuote / executedQty) : (parseFloat(data.avgPrice || 0) || 0);
  return { orderId: data.orderId, executedQty, avgPrice, status: data.status || 'FILLED' };
}

/* Binance Futures LIMIT (post-only via GTX) */
async function placeBinanceFuturesLimitPostOnly(ex, apiKey, apiSecret, symbol, side, baseQty, price, reduceOnly = false) {
  const endpoint = "/fapi/v1/order";
  const params = {
    symbol: symbol.toUpperCase() + "USDT",
    side, type: "LIMIT",
    timeInForce: "GTX", // post-only
    price: Number(price).toFixed(8),
    quantity: Number(baseQty).toFixed(6),
    reduceOnly: reduceOnly ? "true" : "false",
    newOrderRespType: "RESULT",
    timestamp: Date.now()
  };
  const queryString = Object.entries(params).map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join("&");
  const signature = await hmacHexStr(apiSecret, queryString);
  const url = `${ex.baseUrl}${endpoint}?${queryString}&signature=${signature}`;
  const res = await safeFetch(url, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.msg || data?.message || `HTTP ${res.status}`);
  const executedQty = parseFloat(data.executedQty || 0);
  const cumQuote = parseFloat(data.cumQuote || 0);
  const avgPrice = executedQty > 0 ? (cumQuote / Math.max(1e-12, executedQty)) : 0;
  return { orderId: data.orderId, executedQty, avgPrice, status: data.status || 'NEW' };
}

/* ---------- Demo order (robust price fallback) ---------- */
async function placeDemoOrder(symbol, side, amount, isQuoteOrder) {
  let currentPrice = await getCurrentPrice(symbol);

  if (!currentPrice || !isFinite(currentPrice) || currentPrice <= 0) {
    try {
      const ob = await getOrderBookSnapshot(symbol);
      const { mid } = computeSpreadDepth(ob);
      if (isFinite(mid) && mid > 0) currentPrice = mid;
    } catch {}
  }
  if (!currentPrice || !isFinite(currentPrice) || currentPrice <= 0) {
    const cached = priceCache.get((symbol || "").toUpperCase());
    if (typeof cached !== "undefined" && isFinite(cached) && cached > 0) currentPrice = cached;
  }
  if (!currentPrice || !isFinite(currentPrice) || currentPrice <= 0) {
    throw new Error(`Demo trade failed: bad price for ${symbol}.`);
  }

  const executedQty = isQuoteOrder ? Number(amount) / currentPrice : Number(amount);
  return { orderId: `demo-${crypto.randomUUID()}`, executedQty, avgPrice: currentPrice, status: 'FILLED' };
}

/* ---------- Market order routing based on session exchange ---------- */
async function placeMarketBuy(env, userId, symbol, quoteAmount, opts = {}) {
  const session = await getSession(env, userId);
  const ex = SUPPORTED_EXCHANGES[session.exchange_name];

  if (ex.kind === 'demoParrot') {
    return await placeDemoOrder(symbol, "BUY", quoteAmount, true);
  }

  const apiKey = await decrypt(session.api_key_encrypted, env);
  const apiSecret = await decrypt(session.api_secret_encrypted, env);

  switch (ex.kind) {
    case "binanceLike":
      return await placeBinanceLikeOrder(ex, apiKey, apiSecret, symbol, "BUY", quoteAmount, true);
    case "lbankV2": {
      const price = await getCurrentPrice(symbol);
      return await placeLBankOrder(ex, apiKey, apiSecret, symbol, "buy_market", Number(quoteAmount) / price);
    }
    case "coinexV2": {
      const price = await getCurrentPrice(symbol);
      return await placeCoinExOrder(ex, apiKey, apiSecret, symbol, "buy", Number(quoteAmount) / price);
    }
    case "binanceFuturesUSDT": {
      const price = await getCurrentPrice(symbol);
      const baseQty = Number(quoteAmount) / price;
      return await placeBinanceFuturesOrder(ex, apiKey, apiSecret, symbol, "BUY", baseQty, opts.reduceOnly === true);
    }
    default: throw new Error(`Orders not supported for ${ex.label}`);
  }
}

async function placeMarketSell(env, userId, symbol, amount, isQuoteOrder = false, opts = {}) {
  const session = await getSession(env, userId);
  const ex = SUPPORTED_EXCHANGES[session.exchange_name];

  if (ex.kind === 'demoParrot') {
    return await placeDemoOrder(symbol, "SELL", amount, isQuoteOrder);
  }

  const apiKey = await decrypt(session.api_key_encrypted, env);
  const apiSecret = await decrypt(session.api_secret_encrypted, env);

  switch (ex.kind) {
    case "binanceLike":
      return await placeBinanceLikeOrder(ex, apiKey, apiSecret, symbol, "SELL", amount, isQuoteOrder);
    case "lbankV2": {
      const baseAmt = isQuoteOrder ? Number(amount) / (await getCurrentPrice(symbol)) : Number(amount);
      return await placeLBankOrder(ex, apiKey, apiSecret, symbol, "sell_market", baseAmt);
    }
    case "coinexV2": {
      const baseAmt = isQuoteOrder ? Number(amount) / (await getCurrentPrice(symbol)) : Number(amount);
      return await placeCoinExOrder(ex, apiKey, apiSecret, symbol, "sell", baseAmt);
    }
    case "binanceFuturesUSDT": {
      let baseQty = Number(amount);
      if (isQuoteOrder) {
        const price = await getCurrentPrice(symbol);
        baseQty = Number(amount) / price;
      }
      return await placeBinanceFuturesOrder(ex, apiKey, apiSecret, symbol, "SELL", baseQty, opts.reduceOnly === true);
    }
    default: throw new Error(`Orders not supported for ${ex.label}`);
  }
}

/* Exports */
export {
  hmacHexStr, hmacB64Str, hmacB64Bytes, sha256Bytes, md5Hex,
  parseUSDT_BinanceLike, parseUSDT_Gate, parseUSDT_Bybit, parseUSDT_Kraken, parseUSDT_CoinEx, parseUSDT_Huobi, parseUSDT_LBank,
  verifyBinanceLike, verifyBinanceFutures, verifyBybit, verifyKraken, verifyGate, huobiSignedGet, verifyHuobi, verifyCoinEx, verifyLBank,
  verifyApiKeys,
  placeBinanceLikeOrder, placeBinanceLikeLimitMaker,
  placeLBankOrder, placeCoinExOrder,
  placeBinanceFuturesOrder, placeBinanceFuturesLimitPostOnly,
  placeDemoOrder,
  placeMarketBuy, placeMarketSell
};

/* ======================================================================
   SECTION 5/7 — Protocol Status, Create/Execute/Close Trades,
                  Auto exits, Win stats (10/10 dynamic RRR aware)
   ====================================================================== */

/* ---------- Time stop (env-driven; default 14 minutes; used as fallback) ---------- */
function getMaxTradeAgeMs(env) {
  const minsRaw = Number(env?.MAX_TRADE_AGE_MIN ?? 14);
  const mins = isFinite(minsRaw) ? minsRaw : 14;
  return Math.max(1, Math.floor(mins)) * 60 * 1000;
}

/* ---------- TTL helpers (per-idea, per-trade) ---------- */
function ttlBounds(env) {
  const lo = Math.max(1, Math.floor(Number(env?.TTL_MIN_SEC ?? 540)));
  const hi = Math.max(lo, Math.floor(Number(env?.TTL_MAX_SEC ?? 1200)));
  return { TTL_MIN_SEC: lo, TTL_MAX_SEC: hi };
}
function ttlDeadlineMsFromIdea(idea, nowMs, env) {
  const { TTL_MIN_SEC, TTL_MAX_SEC } = ttlBounds(env);
  const holdSecRaw = Number(idea?.hold_sec ?? idea?.ttl_sec ?? NaN);
  const ttlTsRaw = Number(idea?.ttl_ts_ms ?? NaN);
  if (isFinite(ttlTsRaw) && ttlTsRaw > 0) {
    const minMs = nowMs + TTL_MIN_SEC * 1000;
    const maxMs = nowMs + TTL_MAX_SEC * 1000;
    return Math.min(Math.max(ttlTsRaw, minMs), maxMs);
  }
  if (isFinite(holdSecRaw) && holdSecRaw > 0) {
    const clamped = Math.min(Math.max(holdSecRaw, TTL_MIN_SEC), TTL_MAX_SEC);
    return nowMs + clamped * 1000;
  }
  return null;
}
function ensureTradeTtl(extra, openedAtMs, env) {
  if (!extra) extra = {};
  if (!extra.ttl) extra.ttl = {};
  const { TTL_MIN_SEC, TTL_MAX_SEC } = ttlBounds(env);
  if (!isFinite(Number(extra.ttl.ttl_ts_ms))) {
    const holdSec = Math.max(TTL_MIN_SEC, Math.min(TTL_MAX_SEC, Number(extra.ttl.hold_sec ?? TTL_MIN_SEC)));
    extra.ttl.ttl_ts_ms = openedAtMs + holdSec * 1000;
  } else {
    const minMs = openedAtMs + TTL_MIN_SEC * 1000;
    const maxMs = openedAtMs + TTL_MAX_SEC * 1000;
    extra.ttl.ttl_ts_ms = Math.min(Math.max(Number(extra.ttl.ttl_ts_ms), minMs), maxMs);
  }
  return extra;
}

/* ---------- HSE min sizing floor (env-driven; default 0 => original) ---------- */
function getHSEMinG(env) {
  const raw = Number(env?.HSE_MIN_G_FLOOR ?? 0);
  return Math.max(0, Math.min(1, isFinite(raw) ? raw : 0));
}

/* ---------- No-rejects toggle (env-driven) ---------- */
function noRejects(env) {
  return String(env?.NO_REJECTS || '0') === '1';
}

/* ---------- Fixed per-trade notional (env-driven) ---------- */
function getFixedTradeNotional(env, TC) {
  // Precedence: USD > fraction * TC
  const usd = Number(env?.FIXED_TRADE_NOTIONAL_USD);
  if (isFinite(usd) && usd > 0) return usd;

  const frac = Number(env?.FIXED_TRADE_NOTIONAL_FRAC);
  if (isFinite(frac) && frac > 0) return Math.max(0, frac) * TC;

  return null;
}
function fixedStrict(env) {
  // If '1', ignore notional caps (still bounded by funds)
  return String(env?.FIXED_TRADE_STRICT || '0') === '1';
}

/* ---------- Duplicate symbols policy (env-driven) ---------- */
function allowDuplicateSymbols(env) {
  return String(env?.ALLOW_DUPLICATE_SYMBOLS || '0') === '1';
}
function maxPosPerSymbol(env) {
  const n = Number(env?.MAX_POS_PER_SYMBOL || 1);
  return Math.max(1, Math.floor(isFinite(n) ? n : 1));
}
/* Cycle-only duplicate guard toggle */
function cycleOnlyDupGuard(env) {
  return String(env?.CYCLE_ONLY_DUP_GUARD || '0') === '1';
}

/* ---------- Protocol status text with Suggester/TCR insights ---------- */
function protocolStatusTextB(protocol, liveBalance, openRisk, winStats, extras = {}) {
  const D_risk = DAILY_OPEN_RISK_CAP_FRAC * liveBalance;
  const D_risk_left = Math.max(0, D_risk - openRisk);

  const perTradeNotionalCapFrac = Number(extras.perTradeNotionalCapFrac ?? 0.10);
  const dailyNotionalCapFrac    = Number(extras.dailyNotionalCapFrac ?? 0.30);
  const perTradeNotionalCap     = perTradeNotionalCapFrac * liveBalance;
  const dailyNotionalCap        = dailyNotionalCapFrac * liveBalance;

  const winLine = winStats
    ? `Win rate: ${winStats.winRatePct.toFixed(1)}% (${winStats.wins}/${winStats.total})`
    : `Win rate: n/a`;

  const lines = [
    "Protocol Status (ACP V20, Multi-Position):",
    "",
    `Live Balance (TC): ${formatMoney(liveBalance)}`,
    `ACP (${(ACP_FRAC*100).toFixed(0)}% of TC): ${formatMoney(ACP_FRAC * liveBalance)}`,
    `Per-trade risk cap (M): ${formatMoney(PER_TRADE_CAP_FRAC * liveBalance)}`,
    `Daily open risk cap (D): ${formatMoney(D_risk)} | Used: ${formatMoney(openRisk)} | Left: ${formatMoney(D_risk_left)}`,
    "",
    `Bank Notional Caps:`,
    `Per-trade notional cap: ${formatMoney(perTradeNotionalCap)} (${(perTradeNotionalCapFrac*100).toFixed(0)}% of TC)`,
    `Daily notional cap: ${formatMoney(dailyNotionalCap)} (${(dailyNotionalCapFrac*100).toFixed(0)}% of TC)`,
    "",
    `Fee Rate (per side): ${formatPercent(protocol.fee_rate)}`,
    winLine
  ];

  const src = extras?.ideasSource || null;
  const subreqUsed = extras?.subreqUsed;
  const longShare = extras?.longShareTarget;
  const sqsGate = extras?.sqsGate;
  const todayOpened = extras?.todayOpened;
  const targetDaily = extras?.targetDaily;

  const info = [];
  if (src || subreqUsed != null || longShare != null || sqsGate != null || todayOpened != null) {
    info.push("", "Suggester/TCR:");
    if (src) info.push(`Ideas source: ${src}`);
    if (subreqUsed != null) info.push(`Subrequests last cycle: ${subreqUsed}`);
    if (longShare != null) info.push(`Bias (long share target): ${(longShare * 100).toFixed(0)}%`);
    if (sqsGate != null) info.push(`Dynamic SQS gate: ${sqsGate.toFixed(2)}`);
    if (todayOpened != null && targetDaily != null) info.push(`Today trades: ${todayOpened}/${targetDaily} (pace control)`);
  }

  lines.push(...info, "", "This is live and updates as trades open/close. RRR may be dynamic per idea (if enabled), otherwise 2:1.");
  return lines.join("\n");
}

async function renderProtocolStatus(env, userId, messageId = 0) {
  const protocol = await getProtocolState(env, userId);
  const tc = await getTotalCapital(env, userId);
  const openRisk = await getOpenPortfolioRisk(env, userId);
  const stats = await getWinStats(env, userId);

  let ideasMeta = {};
  try {
    const row = await env.DB.prepare("SELECT ideas_json FROM ideas ORDER BY ts DESC LIMIT 1").first();
    if (row?.ideas_json) {
      const j = JSON.parse(row.ideas_json);
      ideasMeta = {
        ideasSource: j?.meta?.origin || j?.source || "external",
        subreqUsed: j?.meta?.subrequestsUsed,
        longShareTarget: j?.meta?.longShareTarget
      };
    }
  } catch {}

  let sqsGate = null, todayOpened = null, targetDaily = null;
  try {
    sqsGate = await computeUserSqsGate(env, userId);
    const { todayCount } = await getPacingCounters(env, userId);
    todayOpened = todayCount;
    const explicitTarget = Number(env?.TCR_TARGET_TRADES || 0);
    if (explicitTarget > 0) targetDaily = explicitTarget;
    else {
      const { last7Counts } = await getPacingCounters(env, userId);
      targetDaily = Math.max(1, Math.round((todayOpened + last7Counts.reduce((a,b)=>a+b,0))/7));
    }
  } catch {}

  const extras = {
    ...ideasMeta,
    sqsGate,
    todayOpened,
    targetDaily,
    perTradeNotionalCapFrac: Number(env?.PER_TRADE_NOTIONAL_CAP_FRAC ?? 0.10),
    dailyNotionalCapFrac: Number(env?.DAILY_OPEN_NOTIONAL_CAP_FRAC ?? 0.30)
  };

  const text = protocol
    ? protocolStatusTextB(protocol, tc, openRisk, stats, extras)
    : "No protocol initialized yet.";
  const buttons = [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }]];
  if (messageId) await editMessage(userId, messageId, text, buttons, env);
  else await sendMessage(userId, text, buttons, env);
}

/* ---------- Auto skip record (for auto mode idea rejections) ---------- */
async function createAutoSkipRecord(env, userId, idea, protocol, reason, meta) {
  try {
    const session = await getSession(env, userId);
    const symbol = String(idea?.symbol || "").toUpperCase();
    const currentPrice = await getCurrentPrice(symbol, idea?.price);

    // Prefer idea exits if present (dynamic RRR preview for skipped rows)
    const useIdeaExits = String(env.USE_IDEA_EXITS || '1') === '1';
    const tpBps = Number(idea?.tp_bps ?? NaN);
    const slBps = Number(idea?.sl_bps ?? NaN);
    const hasIdeaExits = useIdeaExits && isFinite(tpBps) && tpBps > 0 && isFinite(slBps) && slBps > 0;

    let sL = hasIdeaExits ? clamp(bpsToFrac(slBps), MIN_STOP_PCT, 0.20) : Math.max(await calculateStopPercent(symbol), MIN_STOP_PCT);
    const R_used = hasIdeaExits ? clamp(tpBps / slBps, 0.8, 3.0) : STRICT_RRR;

    const isShort = String(idea?.side || 'long').toLowerCase() === 'short';
    const stopPrice = isFinite(currentPrice) && currentPrice > 0
      ? (!isShort ? currentPrice * (1 - sL) : currentPrice * (1 + sL))
      : 0;
    const tpPrice   = isFinite(currentPrice) && currentPrice > 0
      ? (!isShort ? currentPrice * (1 + sL * R_used) : currentPrice * (1 - sL * R_used))
      : 0;

    const extra_json = {
      auto_skip: true,
      skip_reason: reason,
      skip_meta: meta || {},
      idea_score: idea?.score,
      direction: isShort ? 'short' : 'long',
      r_target: R_used,
      exits_from_idea: !!hasIdeaExits,
      strict_rrr: true,
      bracket_frozen: true
    };

    await env.DB.prepare(
      `INSERT INTO trades (user_id, exchange_name, mode, symbol, side, qty, price, status,
                           stop_pct, stop_price, tp_price, risk_usd, strict_rrr, bracket_frozen, extra_json)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).bind(
      userId, session.exchange_name, session.bot_mode, symbol, isShort ? 'SELL' : 'BUY',
      0, currentPrice || 0, 'rejected',
      sL, stopPrice, tpPrice, 0, 'true', 'true', JSON.stringify(extra_json)
    ).run();

    // Flat-CPU pacing: count every trade row created today (same as original DB COUNT)
    await bumpTradeCounters(env, userId, 1);
  } catch (e) {
    console.error('createAutoSkipRecord error:', e);
  }
}

/* ---------- Create pending trade (EV/Kelly + budgets + guards) ---------- */
async function createPendingTrade(env, userId, idea, protocol) {
  const session = await getSession(env, userId);
  const symbol = String(idea.symbol || '').toUpperCase();

  // Duplicate exposure (cross-cycle DB check only when not cycle-only)
  {
    const allowDup = allowDuplicateSymbols(env);
    const capPerSym = maxPosPerSymbol(env);

    if (!cycleOnlyDupGuard(env)) {
      const rowCnt = await env.DB
        .prepare("SELECT COUNT(*) AS c FROM trades WHERE user_id = ? AND symbol = ? AND status IN ('open','pending')")
        .bind(userId, symbol).first();
      const symCount = Number(rowCnt?.c || 0);

      if (!allowDup && symCount > 0) {
        return { id: null, meta: { symbol, skipReason: "duplicate_symbol", symCount, capPerSym: 1 } };
      }
      if (allowDup && symCount >= capPerSym) {
        return { id: null, meta: { symbol, skipReason: "duplicate_symbol_cap", symCount, capPerSym } };
      }
    }
  }

  // Cooldown gate (skippable by NO_REJECTS on purpose)
  const cdMs = await getSymbolCooldownRemainingMs(env, userId, symbol);
  if (cdMs > 0 && !noRejects(env)) {
    return { id: null, meta: { symbol, skipReason: "cooldown_active", remain_ms: cdMs } };
  }

  // Price (robust fallbacks)
  let currentPrice = await getCurrentPrice(symbol, idea?.price ?? idea?.entry_mid ?? idea?.entry_limit);
  if (!isFinite(currentPrice) || currentPrice <= 0) {
    try {
      const ob = await getOrderBookSnapshot(symbol);
      const { mid } = computeSpreadDepth(ob);
      if (isFinite(mid) && mid > 0) currentPrice = mid;
    } catch {}
  }
  if (!isFinite(currentPrice) || currentPrice <= 0) {
    const cached = priceCache.get((symbol || "").toUpperCase());
    if (typeof cached !== "undefined" && isFinite(cached) && cached > 0) currentPrice = cached;
  }
  if (!isFinite(currentPrice) || currentPrice <= 0) {
    // In no-rejects mode, allow demo to force a synthetic price
    if (noRejects(env) && session.exchange_name === 'crypto_parrot') {
      currentPrice = 1;
    }
    if (!isFinite(currentPrice) || currentPrice <= 0) {
      return { id: null, meta: { symbol, skipReason: "market_data" } };
    }
  }

  // Direction
  const dir = String(idea.side || 'long').toLowerCase();
  const isShort = (dir === 'short' || dir === 'sell');

  // Capital (original constants)
  const TC = await getTotalCapital(env, userId);
  const ACP = ACP_FRAC * TC;
  if (ACP < 6 && !noRejects(env)) {
    return { id: null, meta: { symbol, skipReason: "bank_closed", TC, ACP } };
  }

  // Notional caps (env-driven, same as original)
  const perTradeNotionalCapFrac = Number(env?.PER_TRADE_NOTIONAL_CAP_FRAC ?? 0.10);
  const dailyNotionalCapFrac    = Number(env?.DAILY_OPEN_NOTIONAL_CAP_FRAC ?? 0.30);
  const perTradeNotionalCap     = perTradeNotionalCapFrac * TC;
  const dailyNotionalCap        = dailyNotionalCapFrac * TC;
  const openNotional            = await getOpenNotional(env, userId);
  let dailyNotionalLeft         = Math.max(0, dailyNotionalCap - openNotional);
  if (dailyNotionalLeft <= 0 && !noRejects(env)) {
    return { id: null, meta: { symbol, skipReason: "daily_notional_exhausted", dailyNotionalLeft, dailyNotionalCap, openNotional, TC } };
  }

  // Risk budgets (original constants)
  const M = PER_TRADE_CAP_FRAC * TC;
  const D = DAILY_OPEN_RISK_CAP_FRAC * TC;
  const openRisk = await getOpenPortfolioRisk(env, userId);
  let D_left = Math.max(0, D - openRisk);
  const B = Math.min(M, D_left);
  if (B <= 0 && !noRejects(env)) {
    return { id: null, meta: { symbol, skipReason: "daily_risk_budget_exhausted", D_left, D, openRisk, TC } };
  }
  const B_eff = B > 0 ? B : (noRejects(env) ? Math.max(1, 0.0001 * TC) : 0); // tiny floor in NO_REJECTS

  // Env toggles for dynamic fields
  const useIdeaP = String(env.USE_IDEA_P || '1') === '1';
  const useIdeaExits = String(env.USE_IDEA_EXITS || '1') === '1';
  const minEvBps = Number(env.EV_BPS_MIN ?? 1);

  // Probability and exits from suggester
  const pIdeaRaw = Number(idea?.p_lcb ?? idea?.p_win ?? NaN);
  let SQS, p;
  if (useIdeaP && isFinite(pIdeaRaw)) {
    p = clamp01(pIdeaRaw);
    SQS = p;
  } else {
    SQS = SQSfromScore(idea.score ?? 0);
    p = pLCBFromSQS(SQS);
  }

  const tpBps = Number(idea?.tp_bps ?? NaN);
  const slBps = Number(idea?.sl_bps ?? NaN);
  const hasIdeaExits = useIdeaExits && isFinite(tpBps) && tpBps > 0 && isFinite(slBps) && slBps > 0;

  // Stop size in pct (for budgets)
  let sL = hasIdeaExits ? clamp(bpsToFrac(slBps), MIN_STOP_PCT, 0.20) : Math.max(await calculateStopPercent(symbol), MIN_STOP_PCT);
  const demoEasy = (session.exchange_name === 'crypto_parrot' && String(env.DEMO_EASY_MODE || '0') === '1');
  if (demoEasy) sL = Math.max(sL, 0.01); // floor for demo

  // Fee
  let feeRate = (protocol?.fee_rate ?? 0.001);
  if (demoEasy) feeRate = Math.min(feeRate, 0.0002);

  // HSE/EV
  const hse = await computeHSEAndCosts(symbol, sL, feeRate);
  const { pH } = hse;
  let cost_R = hse.cost_R;

  // If suggester supplied after-cost cost in bps, prefer conservative max()
  const costBpsIdea = Number(idea?.cost_bps ?? NaN);
  if (hasIdeaExits && isFinite(costBpsIdea) && slBps > 0) {
    const costR_fromIdea = Math.max(0, costBpsIdea) / slBps;
    cost_R = Math.max(cost_R, costR_fromIdea);
  }

  // Side-aware RRR
  const RRR_used = hasIdeaExits ? clamp(tpBps / slBps, 0.8, 3.0) : STRICT_RRR;

  // EV gates (prefer exp_lcb_bps)
  const expLCBBps = Number(idea?.exp_lcb_bps ?? NaN);
  let EV_R;
  if (isFinite(expLCBBps) && hasIdeaExits) {
    // EV_R ≈ EV_bps / SL_bps
    EV_R = expLCBBps / Math.max(slBps, 1);
    if ((expLCBBps <= Math.max(0, minEvBps - 1e-9)) && !noRejects(env) && !demoEasy) {
      return { id: null, meta: { symbol, skipReason: "ev_gate", exp_lcb_bps: expLCBBps, p, tp_bps: tpBps, sl_bps: slBps, cost_bps: costBpsIdea } };
    }
  } else {
    EV_R = p * RRR_used - (1 - p) - cost_R;
    if (!(EV_R > 0) && !noRejects(env) && !demoEasy) {
      return { id: null, meta: { symbol, skipReason: "ev_gate", EV_R, p, RRR: RRR_used, cost_R, pH, sL, currentPrice } };
    }
  }

  // Dynamic SQS gate (KV-backed, flat CPU)
  let sqsMinDynamic = await computeUserSqsGate(env, userId);
  if (demoEasy) sqsMinDynamic = Math.min(sqsMinDynamic, 0.20);
  if ((SQS < sqsMinDynamic) && !noRejects(env) && !demoEasy) {
    return { id: null, meta: { symbol, skipReason: "low_sqs", SQS, sL, currentPrice, sqsMin: sqsMinDynamic } };
  }

  // HSE (operational risk) gate — allow override via env floor
  const hseMinG = getHSEMinG(env);
  if ((pH >= 0.85) && !demoEasy && hseMinG === 0 && !noRejects(env)) {
    return { id: null, meta: { symbol, skipReason: "hse_gate", pH, sL, currentPrice } };
  }

  // Kelly sizing
  const { f_k, Var_R } = kellyFraction(EV_R, p, RRR_used);
  const w_sqs = SQS * SQS;
  const f_raw = clamp(LAMBDA_BLEND * w_sqs + (1 - LAMBDA_BLEND) * f_k, 0, 1);

  // State scalers
  const dd = await getDrawdown(env, userId, TC);
  const k_dd = ddScaler(dd);
  const rho_max = await maxCorrelationWithOpen(env, userId, symbol);
  const c_corr = clamp(1 - rho_max, 0, 1);

  let g;
  if (pH < 0.60) g = 1.0;
  else if (pH < 0.75) g = 0.7;
  else if (pH < 0.85) g = 0.5;
  else g = hseMinG; // use env floor instead of zero when pH is very high

  // Desired risk and notional mapping (supports fixed sizing via env)
  const Desired_Risk_USD = B_eff * k_dd * f_raw * c_corr * g;

  const fixedNotional = getFixedTradeNotional(env, TC);
  let notionalDesired = fixedNotional != null ? fixedNotional : (Desired_Risk_USD / sL);

  // Respect caps unless FIXED_TRADE_STRICT='1'
  const capPerTrade  = fixedStrict(env) ? Number.POSITIVE_INFINITY : perTradeNotionalCap;
  const capDailyLeft = fixedStrict(env) ? Number.POSITIVE_INFINITY : dailyNotionalLeft;

  let notionalFinal = Math.min(notionalDesired, capPerTrade, capDailyLeft);

  // Floor notional in no-rejects mode to avoid zero-sizing
  if (notionalFinal <= 0 && noRejects(env)) {
    const MIN_QUOTE_FLOOR_USD = 5;
    notionalFinal = MIN_QUOTE_FLOOR_USD;
  }
  if (notionalFinal <= 0 && !noRejects(env)) {
    return { id: null, meta: { symbol, skipReason: "bank_notional_zero", perTradeNotionalCap, dailyNotionalLeft, openNotional, sL, currentPrice } };
  }

  let Final_Risk_USD = notionalFinal * sL;

  // Micro-skip
  const minRiskFrac = Number(env?.MIN_RISK_FRAC_OF_ACP ?? 0.0025);
  const minRisk = minRiskFrac * (ACP_FRAC * TC);
  if (Final_Risk_USD < minRisk && !noRejects(env)) {
    return {
      id: null,
      meta: { symbol, skipReason: "too_small", Final_Risk_USD, minRisk, minRiskFrac, ACP: ACP_FRAC * TC, TC, sL, currentPrice, notionalFinal, perTradeNotionalCap, dailyNotionalLeft }
    };
  }

  // Funds check — shrink to available funds if needed (demo: floor in no-rejects)
  const requiredFunds = notionalFinal * (1 + 2 * feeRate);
  let fundsOk = requiredFunds <= TC;
  if (!fundsOk && noRejects(env)) {
    const maxNotional = TC / (1 + 2 * feeRate);
    notionalFinal = Math.max(0, Math.min(notionalFinal, maxNotional));
    Final_Risk_USD = notionalFinal * sL;
    fundsOk = notionalFinal > 0;
    if (!fundsOk && session.exchange_name === 'crypto_parrot') {
      notionalFinal = 5; // tiny floor on demo
      Final_Risk_USD = notionalFinal * sL;
      fundsOk = true;
    }
  }
  if (!fundsOk && !noRejects(env)) {
    return { id: null, meta: { symbol, skipReason: "insufficient_funds", available: TC, required: requiredFunds, sL, currentPrice, notionalFinal } };
  }

  // Bracket (prefer idea tp/sl)
  let stopPrice, tpPrice;
  if (hasIdeaExits) {
    const slf = bpsToFrac(slBps), tpf = bpsToFrac(tpBps);
    stopPrice = isShort ? currentPrice * (1 + slf) : currentPrice * (1 - slf);
    tpPrice   = isShort ? currentPrice * (1 - tpf) : currentPrice * (1 + tpf);
  } else {
    stopPrice = isShort ? currentPrice * (1 + sL) : currentPrice * (1 - sL);
    tpPrice   = isShort ? currentPrice * (1 - sL * STRICT_RRR) : currentPrice * (1 + sL * STRICT_RRR);
  }

  // Extra snapshot (UI + audit)
  const extra_json = {
    idea_score: idea.score,
    direction: isShort ? 'short' : 'long',
    SQS, p, RRR: RRR_used, EV_R, Var_R, f_kelly: f_k, w_sqs, f_raw,
    dd, k_dd, rho_max, c_corr, pH, g,
    sL, stop_price: stopPrice, tp_price: tpPrice,
    fee_rate: feeRate,
    idea_fields: {
      p_win: isFinite(Number(idea.p_win)) ? Number(idea.p_win) : undefined,
      p_lcb: isFinite(Number(idea.p_lcb)) ? Number(idea.p_lcb) : undefined,
      exp_lcb_bps: isFinite(expLCBBps) ? expLCBBps : undefined,
      tp_bps: isFinite(tpBps) ? tpBps : undefined,
      sl_bps: isFinite(slBps) ? slBps : undefined,
      cost_bps: isFinite(costBpsIdea) ? costBpsIdea : undefined,
      ttl_sec: idea.ttl_sec
    },
    // NEW: persist entry/exec/mgmt from idea for maker/post-only and overlay
    entry: {
      policy: String(idea.entry_policy || ''),
      limit: isFinite(idea.entry_limit) ? Number(idea.entry_limit) : null,
      book: idea.entry_book || null
    },
    exec: idea.exec || null,
    mgmt: idea.mgmt || null,
    exits_from_idea: !!hasIdeaExits,
    budgets: { TC, ACP: ACP_FRAC*TC, M: PER_TRADE_CAP_FRAC*TC, D: DAILY_OPEN_RISK_CAP_FRAC*TC, openRisk, D_left: D_left, B: B_eff, Desired_Risk_USD, Final_Risk_USD, minRisk, minRiskFrac },
    bank_notional: {
      per_trade_cap_frac: perTradeNotionalCapFrac,
      daily_cap_frac: dailyNotionalCapFrac,
      per_trade_cap: perTradeNotionalCap,
      daily_cap: dailyNotionalCap,
      open_notional: openNotional,
      daily_left: dailyNotionalLeft,
      desired_notional: notionalDesired,
      final_notional: notionalFinal
    },
    quote_size_raw: notionalDesired,
    quote_size: notionalFinal,
    required_funds: requiredFunds,
    available_funds: TC,
    funds_ok: fundsOk,
    r_target: RRR_used,
    strict_rrr: true,
    bracket_frozen: true,
    used_price: currentPrice,
    sqs_gate_used: sqsMinDynamic
  };

  // Capture per-idea TTL (absolute or relative) now
  const nowMs = Date.now();
  const ttlTs = ttlDeadlineMsFromIdea(idea, nowMs, env);
  if (ttlTs) {
    extra_json.ttl = { ttl_ts_ms: ttlTs, hold_sec: Math.round((ttlTs - nowMs) / 1000), source: 'idea' };
  }

  const res = await env.DB.prepare(
    `INSERT INTO trades (user_id, exchange_name, mode, symbol, side, qty, price, status, stop_pct, stop_price, tp_price, risk_usd, strict_rrr, bracket_frozen, extra_json)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     RETURNING id`
  ).bind(
    userId, session.exchange_name, session.bot_mode, symbol, isShort ? 'SELL' : 'BUY',
    0, currentPrice, 'pending', sL, stopPrice, tpPrice, Final_Risk_USD, 'true', 'true', JSON.stringify(extra_json)
  ).first();

  const id = res?.id || null;

  // Flat-CPU pacing: count every trade row created today (same as original DB COUNT)
  if (id) await bumpTradeCounters(env, userId, 1);

  return { id, meta: { symbol, fundsOk, required: requiredFunds, available: TC, sL, currentPrice, notionalFinal, Final_Risk_USD } };
}

/* ---------- Execute trade (supports maker_join + post_only; aligns TTL to open) ---------- */
async function executeTrade(env, userId, tradeId) {
  const trade = await env.DB.prepare("SELECT * FROM trades WHERE id = ? AND user_id = ?").bind(tradeId, userId).first();
  if (!trade || trade.status !== 'pending') return false;

  // Cooldown guard
  const cdMs = await getSymbolCooldownRemainingMs(env, userId, trade.symbol);
  if (cdMs > 0) {
    await logEvent(env, userId, 'cooldown_block', { tradeId, symbol: trade.symbol, remain_ms: cdMs });
    return false;
  }

  const extra = JSON.parse(trade.extra_json || '{}');
  const protocol = await getProtocolState(env, userId);
  const isShort = trade.side === 'SELL';

  // Exec policy
  const respectExec = envFlag(env, 'RESPECT_EXEC_POLICY', '1');
  const takerFallback = envFlag(env, 'POST_ONLY_TAKER_FALLBACK', '1');
  const wantPostOnly = respectExec && ((extra?.exec?.exec || '').toLowerCase() === 'post_only' || (extra?.entry?.policy || '').toLowerCase() === 'maker_join');

  try {
    const session = await getSession(env, userId);
    const ex = SUPPORTED_EXCHANGES[session.exchange_name];

    if (!wantPostOnly) {
      // Original MARKET behavior
      let orderResult;
      if (!isShort) {
        orderResult = await placeMarketBuy(env, userId, trade.symbol, extra.quote_size, { reduceOnly: false });
      } else {
        orderResult = await placeMarketSell(env, userId, trade.symbol, extra.quote_size, true, { reduceOnly: false });
      }

      // Use r_target (dynamic) if present
      const rUsed = Number(extra?.r_target || STRICT_RRR);

      const actualStopPrice = !isShort
        ? orderResult.avgPrice * (1 - trade.stop_pct)
        : orderResult.avgPrice * (1 + trade.stop_pct);
      const actualTpPrice = !isShort
        ? orderResult.avgPrice * (1 + trade.stop_pct * rUsed)
        : orderResult.avgPrice * (1 - trade.stop_pct * rUsed);

      // Capital-at-entry metrics
      const tcAtEntry = await getTotalCapital(env, userId);
      const notionalAtEntry = (orderResult.avgPrice || 0) * (orderResult.executedQty || 0);
      const pctOfTCAtEntry = tcAtEntry > 0 ? notionalAtEntry / tcAtEntry : 0;

      // Align TTL to open time (and clamp to env bounds)
      const openedAtMs = Date.now();
      ensureTradeTtl(extra, openedAtMs, env);

      extra.metrics = { ...(extra.metrics || {}), tc_at_entry: tcAtEntry, notional_at_entry: notionalAtEntry, pct_of_tc_at_entry: pctOfTCAtEntry };

      await env.DB.prepare(
        `UPDATE trades SET status = 'open', qty = ?, entry_price = ?, stop_price = ?, tp_price = ?, extra_json = ?, updated_at = ? WHERE id = ?`
      ).bind(orderResult.executedQty, orderResult.avgPrice, actualStopPrice, actualTpPrice, JSON.stringify(extra), nowISO(), tradeId).run();

      await logEvent(env, userId, trade.mode === 'manual' ? 'trade_open_manual' : 'trade_open', {
        symbol: trade.symbol, side: trade.side, qty: orderResult.executedQty, entry: orderResult.avgPrice,
        stop_pct: trade.stop_pct, stop_price: actualStopPrice, tp_price: actualTpPrice, r: rUsed,
        phase: protocol?.phase, fee_rate: protocol?.fee_rate, strict_rrr: true, bracket_frozen: true,
        tc_at_entry: tcAtEntry, notional_at_entry: notionalAtEntry, pct_of_tc_at_entry: pctOfTCAtEntry,
        ttl_ts_ms: extra?.ttl?.ttl_ts_ms
      });
      return true;
    }

    // Post-only maker branch
    // Decide maker price
    let makerPx = Number(extra?.entry?.limit);
    if (!isFinite(makerPx) || makerPx <= 0) {
      try {
        const ob = await getOrderBookSnapshot(trade.symbol);
        const { bestBid, bestAsk } = computeBestBidAsk(ob || {});
        makerPx = !isShort ? bestBid : bestAsk;
      } catch { /* noop */ }
    }
    if (!isFinite(makerPx) || makerPx <= 0) {
      // As a last resort, use current price
      makerPx = await getCurrentPrice(trade.symbol);
    }

    // Compute base quantity from quote if needed
    // For spot LIMIT_MAKER, we must pass quantity; we derive from quote_size and makerPx
    const quoteAmt = Number(extra.quote_size || 0);
    let result;
    if (ex.kind === 'demoParrot') {
      // Emulate posted then filled
      result = await placeDemoOrder(trade.symbol, isShort ? "SELL" : "BUY", quoteAmt, true);
    } else if (ex.kind === 'binanceLike') {
      try {
        result = await placeBinanceLikeLimitMaker(ex, await decrypt(session.api_key_encrypted, env), await decrypt(session.api_secret_encrypted, env),
          trade.symbol, isShort ? 'SELL' : 'BUY', makerPx, quoteAmt, true);
      } catch (e) {
        if (takerFallback) {
          // Fallback to market
          if (!isShort) result = await placeMarketBuy(env, userId, trade.symbol, quoteAmt, { reduceOnly: false });
          else result = await placeMarketSell(env, userId, trade.symbol, quoteAmt, true, { reduceOnly: false });
        } else {
          // Keep as rejected
          await env.DB.prepare("UPDATE trades SET status='rejected', updated_at=? WHERE id=?").bind(nowISO(), tradeId).run();
          await logEvent(env, userId, 'order_rejected', { policy: 'post_only', symbol: trade.symbol, reason: String(e?.message||e) });
          return false;
        }
      }
    } else if (ex.kind === 'binanceFuturesUSDT') {
      const baseQty = (quoteAmt / Math.max(1e-12, makerPx));
      try {
        result = await placeBinanceFuturesLimitPostOnly(ex, await decrypt(session.api_key_encrypted, env), await decrypt(session.api_secret_encrypted, env),
          trade.symbol, isShort ? 'SELL' : 'BUY', baseQty, makerPx, false);
      } catch (e) {
        if (takerFallback) {
          result = await placeBinanceFuturesOrder(ex, await decrypt(session.api_key_encrypted, env), await decrypt(session.api_secret_encrypted, env),
            trade.symbol, isShort ? 'SELL' : 'BUY', baseQty, false);
        } else {
          await env.DB.prepare("UPDATE trades SET status='rejected', updated_at=? WHERE id=?").bind(nowISO(), tradeId).run();
          await logEvent(env, userId, 'order_rejected', { policy: 'post_only', symbol: trade.symbol, reason: String(e?.message||e) });
          return false;
        }
      }
    } else {
      // Other exchanges: no native post-only supported here; fallback depending on flag
      if (takerFallback) {
        if (!isShort) result = await placeMarketBuy(env, userId, trade.symbol, quoteAmt, { reduceOnly: false });
        else result = await placeMarketSell(env, userId, trade.symbol, quoteAmt, true, { reduceOnly: false });
      } else {
        await env.DB.prepare("UPDATE trades SET status='rejected', updated_at=? WHERE id=?").bind(nowISO(), tradeId).run();
        await logEvent(env, userId, 'order_rejected', { policy: 'post_only', symbol: trade.symbol, reason: 'unsupported_exchange' });
        return false;
      }
    }

    // If immediate fill happened (executedQty>0), finalize as open; otherwise keep pending with open_order
    if (result && Number(result.executedQty || 0) > 0) {
      const rUsed = Number(extra?.r_target || STRICT_RRR);
      const entryPx = Number(result.avgPrice || makerPx);
      const qty = Number(result.executedQty || 0);

      const actualStopPrice = !isShort
        ? entryPx * (1 - trade.stop_pct)
        : entryPx * (1 + trade.stop_pct);
      const actualTpPrice = !isShort
        ? entryPx * (1 + trade.stop_pct * rUsed)
        : entryPx * (1 - trade.stop_pct * rUsed);

      const tcAtEntry = await getTotalCapital(env, userId);
      const notionalAtEntry = entryPx * qty;
      const pctOfTCAtEntry = tcAtEntry > 0 ? notionalAtEntry / tcAtEntry : 0;

      const openedAtMs = Date.now();
      ensureTradeTtl(extra, openedAtMs, env);
      extra.metrics = { ...(extra.metrics || {}), tc_at_entry: tcAtEntry, notional_at_entry: notionalAtEntry, pct_of_tc_at_entry: pctOfTCAtEntry };

      await env.DB.prepare(
        `UPDATE trades SET status='open', qty=?, entry_price=?, stop_price=?, tp_price=?, extra_json=?, updated_at=? WHERE id=?`
      ).bind(qty, entryPx, actualStopPrice, actualTpPrice, JSON.stringify(extra), nowISO(), tradeId).run();

      await logEvent(env, userId, 'trade_open', {
        symbol: trade.symbol, side: trade.side, qty, entry: entryPx, r: rUsed,
        stop_pct: trade.stop_pct, stop_price: actualStopPrice, tp_price: actualTpPrice,
        ttl_ts_ms: extra?.ttl?.ttl_ts_ms, policy: 'post_only'
      });
      return true;
    } else {
      // Posted maker order still working; track it in extra.open_order
      extra.open_order = {
        id: result?.orderId || null,
        px: makerPx,
        post_only: true,
        posted_at: nowISO()
      };
      await env.DB.prepare("UPDATE trades SET extra_json=?, updated_at=? WHERE id=?")
        .bind(JSON.stringify(extra), nowISO(), tradeId).run();
      await logEvent(env, userId, 'order_posted', { symbol: trade.symbol, id: extra.open_order.id, px: makerPx, post_only: true });
      return true;
    }
  } catch (e) {
    console.error('executeTrade error:', e);
    await logEvent(env, userId, 'error', { where: 'executeTrade', message: e.message });
    return false;
  }
}

/* ---------- Order status resolvers for posted maker orders ---------- */
async function getBinanceLikeOrderStatus(ex, apiKey, apiSecret, symbol, orderId) {
  const endpoint = "/api/v3/order";
  const params = { symbol: symbol.toUpperCase() + "USDT", orderId: String(orderId), timestamp: Date.now() };
  const query = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, query);
  const url = `${ex.baseUrl}${endpoint}?${query}&signature=${sig}`;
  const r = await safeFetch(url, { headers: { [ex.apiKeyHeader]: apiKey } }, 6000);
  const d = await r.json().catch(()=>({}));
  if (!r.ok) throw new Error(d?.msg || d?.message || `HTTP ${r.status}`);
  const status = d.status || d.statusCode || '';
  const executedQty = parseFloat(d.executedQty || 0);
  const cumQuote = parseFloat(d.cummulativeQuoteQty || 0);
  const avgPrice = executedQty > 0 ? (cumQuote / Math.max(1e-12, executedQty)) : 0;
  return { status, executedQty, avgPrice };
}
async function getBinanceFuturesOrderStatus(ex, apiKey, apiSecret, symbol, orderId) {
  const endpoint = "/fapi/v1/order";
  const params = { symbol: symbol.toUpperCase() + "USDT", orderId: String(orderId), timestamp: Date.now() };
  const query = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, query);
  const url = `${ex.baseUrl}${endpoint}?${query}&signature=${sig}`;
  const r = await safeFetch(url, { headers: { [ex.apiKeyHeader]: apiKey } }, 6000);
  const d = await r.json().catch(()=>({}));
  if (!r.ok) throw new Error(d?.msg || d?.message || `HTTP ${r.status}`);
  const status = d.status || '';
  const executedQty = parseFloat(d.executedQty || 0);
  const cumQuote = parseFloat(d.cumQuote || 0);
  const avgPrice = executedQty > 0 ? (cumQuote / Math.max(1e-12, executedQty)) : 0;
  return { status, executedQty, avgPrice };
}

/* ---------- Resolve posted maker orders (working -> open) ---------- */
async function checkWorkingOrders(env, userId) {
  const session = await getSession(env, userId);
  if (!session) return;
  const ex = SUPPORTED_EXCHANGES[session.exchange_name];

  // Select pending with open_order
  const rows = await env.DB.prepare(
    "SELECT id, symbol, side, stop_pct, extra_json FROM trades WHERE user_id = ? AND status = 'pending'"
  ).bind(userId).all();

  for (const t of (rows.results || [])) {
    try {
      const extra = JSON.parse(t.extra_json || '{}');
      const oo = extra?.open_order;
      if (!oo?.id) continue;

      // Query order status
      let st;
      if (ex.kind === 'demoParrot') {
        // Emulate immediate fill
        st = { status: 'FILLED', executedQty: (extra.quote_size || 0) / Math.max(1e-12, oo.px), avgPrice: oo.px };
      } else if (ex.kind === 'binanceLike') {
        const apiKey = await decrypt(session.api_key_encrypted, env);
        const apiSecret = await decrypt(session.api_secret_encrypted, env);
        st = await getBinanceLikeOrderStatus(ex, apiKey, apiSecret, t.symbol, oo.id);
      } else if (ex.kind === 'binanceFuturesUSDT') {
        const apiKey = await decrypt(session.api_key_encrypted, env);
        const apiSecret = await decrypt(session.api_secret_encrypted, env);
        st = await getBinanceFuturesOrderStatus(ex, apiKey, apiSecret, t.symbol, oo.id);
      } else {
        continue;
      }

      // Only act on FILLED
      if ((st.status || '').toUpperCase() !== 'FILLED' || !(Number(st.executedQty) > 0)) {
        continue;
      }

      const isShort = t.side === 'SELL';
      const rUsed = Number(extra?.r_target || STRICT_RRR);

      const entryPx = Number(st.avgPrice || oo.px);
      const qty = Number(st.executedQty || 0);

      const actualStopPrice = !isShort
        ? entryPx * (1 - t.stop_pct)
        : entryPx * (1 + t.stop_pct);
      const actualTpPrice = !isShort
        ? entryPx * (1 + t.stop_pct * rUsed)
        : entryPx * (1 - t.stop_pct * rUsed);

      const tcAtEntry = await getTotalCapital(env, userId);
      const notionalAtEntry = entryPx * qty;
      const pctOfTCAtEntry = tcAtEntry > 0 ? notionalAtEntry / tcAtEntry : 0;

      const openedAtMs = Date.now();
      ensureTradeTtl(extra, openedAtMs, env);
      delete extra.open_order;
      extra.metrics = { ...(extra.metrics || {}), tc_at_entry: tcAtEntry, notional_at_entry: notionalAtEntry, pct_of_tc_at_entry: pctOfTCAtEntry };

      await env.DB.prepare(
        `UPDATE trades SET status='open', qty=?, entry_price=?, stop_price=?, tp_price=?, extra_json=?, updated_at=? WHERE id=?`
      ).bind(qty, entryPx, actualStopPrice, actualTpPrice, JSON.stringify(extra), nowISO(), t.id).run();

      await logEvent(env, userId, 'trade_open', {
        symbol: t.symbol, side: t.side, qty, entry: entryPx, r: rUsed,
        stop_pct: t.stop_pct, stop_price: actualStopPrice, tp_price: actualTpPrice,
        ttl_ts_ms: extra?.ttl?.ttl_ts_ms, policy: 'post_only_fill'
      });
    } catch (e) {
      console.error('checkWorkingOrders error:', e);
    }
  }
}

/* ---------- Helpers for mgmt overlay ---------- */
function unrealizedR(trade, price) {
  const pnl = trade.side==='SELL' ? (trade.entry_price - price)*trade.qty : (price - trade.entry_price)*trade.qty;
  return trade.risk_usd > 0 ? pnl / trade.risk_usd : 0;
}
function parseThreshold(expr, costBps) {
  if (expr == null) return null;
  if (typeof expr === 'number') return expr;
  const s = String(expr).trim().toUpperCase();
  if (!s) return null;
  const COST = Number(costBps || 0);
  const safe = s.replace(/COST/g, String(COST));
  if (!/^[0-9+\-*/.\s]+$/.test(safe)) return null;
  try {
    // eslint-disable-next-line no-new-func
    const v = Function(`"use strict";return (${safe});`)();
    return Number(v);
  } catch { return null; }
}

/* ---------- Manual close & automated exits (with mgmt overlay) ---------- */
async function closeTradeNow(env, userId, tradeId) {
  const trade = await env.DB.prepare("SELECT * FROM trades WHERE id = ? AND user_id = ?").bind(tradeId, userId).first();
  if (!trade || trade.status !== "open") return { ok: false, msg: "Trade not open." };
  const protocol = await getProtocolState(env, userId);
  const isShort = trade.side === 'SELL';
  try {
    let exitPrice = 0;
    if (!isShort) {
      const sellResult = await placeMarketSell(env, userId, trade.symbol, trade.qty, false, { reduceOnly: true });
      exitPrice = sellResult.avgPrice || (await getCurrentPrice(trade.symbol));
    } else {
      const session = await getSession(env, userId);
      if (session.exchange_name === 'crypto_parrot') {
        const buyResult = await placeDemoOrder(trade.symbol, "BUY", trade.qty, false);
        exitPrice = buyResult.avgPrice;
      } else {
        const requiredQuote = trade.qty * (await getCurrentPrice(trade.symbol));
        const buyResult = await placeMarketBuy(env, userId, trade.symbol, requiredQuote, { reduceOnly: true });
        exitPrice = buyResult.avgPrice || (await getCurrentPrice(trade.symbol));
      }
    }

    const grossPnl = !isShort ? (exitPrice - trade.entry_price) * trade.qty
                              : (trade.entry_price - exitPrice) * trade.qty;

    const fees = (trade.entry_price * trade.qty + exitPrice * trade.qty) * (protocol?.fee_rate ?? 0.001);
    const netPnl = grossPnl - fees;
    const realizedR = trade.risk_usd > 0 ? netPnl / trade.risk_usd : 0;
    await env.DB.prepare(
      `UPDATE trades SET status='closed', price=?, realized_pnl=?, realized_r=?, fees=?, close_type='MANUAL', updated_at=? WHERE id=?`
    ).bind(exitPrice, netPnl, realizedR, fees, nowISO(), tradeId).run();

    await setSymbolCooldown(env, userId, trade.symbol);

    await logEvent(env, userId, 'trade_close', { symbol: trade.symbol, entry: trade.entry_price, exit: exitPrice, realizedR, close_type: 'MANUAL', pnl: netPnl, fees });
    return { ok: true, msg: "Closed." };
  } catch (e) {
    await logEvent(env, userId, 'error', { where: 'closeTradeNow', message: e.message });
    return { ok: false, msg: e.message || "Close failed." };
  }
}

/* Efficient price getter for exits (symbol-level de-dup) */
async function robustPriceFor(symbol) {
  let p = await getCurrentPrice(symbol);
  if (!isFinite(p) || p <= 0) {
    try {
      const ob = await getOrderBookSnapshot(symbol);
      const { mid } = computeSpreadDepth(ob);
      if (isFinite(mid) && mid > 0) p = mid;
    } catch {}
  }
  if (!isFinite(p) || p <= 0) {
    const cached = priceCache.get((symbol || "").toUpperCase());
    if (typeof cached !== "undefined" && isFinite(cached) && cached > 0) p = cached;
  }
  return isFinite(p) && p > 0 ? p : 0;
}

/* Robust auto-close with mgmt overlay + batched price fetching */
async function checkAndExitTrades(env, userId) {
  const openTrades = await env.DB.prepare(
    "SELECT id, symbol, side, qty, entry_price, stop_price, tp_price, risk_usd, stop_pct, extra_json, created_at, updated_at FROM trades WHERE user_id = ? AND status = 'open'"
  ).bind(userId).all();
  if (!openTrades?.results?.length) return;

  const protocol = await getProtocolState(env, userId);

  // Batch unique symbols for one-shot pricing
  const symbols = [...new Set(openTrades.results.map(t => t.symbol))];
  const priceMap = new Map();
  for (const s of symbols) {
    priceMap.set(s, await robustPriceFor(s));
  }

  // Fallback global time-stop (only if per-trade TTL not present)
  const AGE_MS_FALLBACK = getMaxTradeAgeMs(env);
  const overlayOn = envFlag(env, 'ENABLE_MGMT_OVERLAY', '1');

  for (const trade of (openTrades.results || [])) {
    try {
      let currentPrice = Number(priceMap.get(trade.symbol) || 0);
      if (!isFinite(currentPrice) || currentPrice <= 0) {
        await logEvent(env, userId, 'error', { where: 'checkAndExitTrades', message: `bad price for ${trade.symbol}` });
        continue;
      }

      const isShort = (trade.side === 'SELL');
      let shouldExit = false;
      let exitReason = '';

      // Management overlay (BE, trail, partial, TTL rescue, early cut)
      let extra = {};
      try { extra = JSON.parse(trade.extra_json || '{}'); } catch { extra = {}; }
      const mgmt = extra?.mgmt || {};
      const rNow = unrealizedR(trade, currentPrice);
      const costBps = extra?.idea_fields?.cost_bps;

      let newStop = trade.stop_price;
      let stateChanged = false;

      if (overlayOn) {
        // 1) Break-even shift
        if (isFinite(mgmt.be_at_r) && rNow >= Number(mgmt.be_at_r) && !extra?.mgmt_state?.be_done) {
          newStop = trade.entry_price;
          extra.mgmt_state = { ...(extra.mgmt_state || {}), be_done: true };
          stateChanged = true;
        }

        // 2) Trailing stop
        if (isFinite(mgmt.trail_atr_mult) && Number(mgmt.trail_atr_mult) > 0) {
          const baseDist = trade.stop_pct * trade.entry_price; // initial risk distance in $
          const trailDist = baseDist * Number(mgmt.trail_atr_mult);
          const trailed = isShort ? Math.min(newStop, currentPrice + trailDist)
                                  : Math.max(newStop, currentPrice - trailDist);
          const improved = isShort ? trailed < newStop : trailed > newStop;
          if (improved) { newStop = trailed; stateChanged = true; }
        }

        // 3) TTL rescue (extend per-trade TTL if profitable enough)
        if (mgmt.extend_ttl_if_unrealized_bps_ge != null) {
          const thr = parseThreshold(mgmt.extend_ttl_if_unrealized_bps_ge, costBps);
          const bpsUnreal = ((currentPrice / trade.entry_price - 1) * (isShort ? -1 : 1)) * 10000;
          if (thr != null && bpsUnreal >= thr && isFinite(mgmt.extend_ttl_sec) && mgmt.extend_ttl_sec > 0) {
            const ttl = (extra.ttl || {});
            const newTtl = Math.max(Number(ttl.ttl_ts_ms || 0), Date.now()) + Number(mgmt.extend_ttl_sec) * 1000;
            extra.ttl = { ...(ttl || {}), ttl_ts_ms: newTtl, source: 'mgmt_extend' };
            stateChanged = true;
          }
        }

        // 4) Early cut (soft SL)
        if (mgmt.early_cut_if_unrealized_bps_le != null) {
          const thrLe = parseThreshold(mgmt.early_cut_if_unrealized_bps_le, costBps);
          const bpsUnreal = ((currentPrice / trade.entry_price - 1) * (isShort ? -1 : 1)) * 10000;
          if (thrLe != null && bpsUnreal <= thrLe) {
            // Immediate close
            let exitPrice = currentPrice;
            if (!isShort) {
              const sellResult = await placeMarketSell(env, userId, trade.symbol, trade.qty, false, { reduceOnly: true });
              exitPrice = sellResult.avgPrice || currentPrice;
            } else {
              const session = await getSession(env, userId);
              if (session.exchange_name === 'crypto_parrot') {
                const buyResult = await placeDemoOrder(trade.symbol, "BUY", trade.qty, false);
                exitPrice = buyResult.avgPrice || currentPrice;
              } else {
                const buyResult = await placeMarketBuy(env, userId, trade.symbol, trade.qty * currentPrice, { reduceOnly: true });
                exitPrice = buyResult.avgPrice || currentPrice;
              }
            }

            const grossPnl = !isShort ? (exitPrice - trade.entry_price) * trade.qty
                                      : (trade.entry_price - exitPrice) * trade.qty;
            const fees = (trade.entry_price * trade.qty + exitPrice * trade.qty) * (protocol?.fee_rate ?? 0.001);
            const netPnl = grossPnl - fees;
            const realizedR = trade.risk_usd > 0 ? netPnl / trade.risk_usd : 0;

            await env.DB.prepare(
              `UPDATE trades SET status='closed', price=?, realized_pnl=?, realized_r=?, fees=?, close_type='MGMT_EARLY_CUT', updated_at=? WHERE id=?`
            ).bind(exitPrice, netPnl, realizedR, fees, nowISO(), trade.id).run();
            await setSymbolCooldown(env, userId, trade.symbol);
            await logEvent(env, userId, 'trade_close', { symbol: trade.symbol, entry: trade.entry_price, exit: exitPrice, realizedR, close_type: 'MGMT_EARLY_CUT', pnl: netPnl, fees });
            continue; // proceed next trade
          }
        }

        // 5) Partial take
        if (isFinite(mgmt.partial_take_at_r) && rNow >= Number(mgmt.partial_take_at_r) && !extra?.mgmt_state?.pt_done) {
          const pct = clamp(Number(mgmt.partial_take_pct || 0.5), 0.05, 0.95);
          const qtyPart = trade.qty * pct;
          try {
            if (trade.side === 'SELL') {
              const buy = await placeMarketBuy(env, userId, trade.symbol, qtyPart * currentPrice, { reduceOnly: true });
              // adjust remaining qty
              const newQty = Math.max(0, trade.qty - (buy.executedQty || qtyPart));
              await env.DB.prepare("UPDATE trades SET qty = ?, updated_at = ? WHERE id = ?")
                .bind(newQty, nowISO(), trade.id).run();
            } else {
              const sell = await placeMarketSell(env, userId, trade.symbol, qtyPart, false, { reduceOnly: true });
              const newQty = Math.max(0, trade.qty - (sell.executedQty || qtyPart));
              await env.DB.prepare("UPDATE trades SET qty = ?, updated_at = ? WHERE id = ?")
                .bind(newQty, nowISO(), trade.id).run();
            }
            extra.mgmt_state = { ...(extra.mgmt_state || {}), pt_done: true };
            stateChanged = true;
          } catch (e) {
            console.error('partial_take error:', e);
          }
        }
      } // overlayOn

      if (stateChanged) {
        await env.DB.prepare("UPDATE trades SET stop_price = ?, extra_json = ?, updated_at = ? WHERE id = ?")
          .bind(newStop, JSON.stringify(extra), nowISO(), trade.id).run();
      }

      // Priority: TP > SL > TTL
      if (!isShort) {
        if (currentPrice <= newStop) { shouldExit = true; exitReason = 'SL'; }
        else if (currentPrice >= trade.tp_price) { shouldExit = true; exitReason = 'TP'; }
      } else {
        if (currentPrice >= newStop) { shouldExit = true; exitReason = 'SL'; }
        else if (currentPrice <= trade.tp_price) { shouldExit = true; exitReason = 'TP'; }
      }

      // TTL enforcement (per-trade)
      if (!shouldExit) {
        let ttlTs = null;
        try { ttlTs = Number(JSON.parse(trade.extra_json || '{}')?.ttl?.ttl_ts_ms ?? null); } catch {}
        if (isFinite(ttlTs) && ttlTs > 0 && Date.now() >= ttlTs) {
          shouldExit = true;
          exitReason = 'TTL';
        }
      }

      // Fallback global TIME stop (only if no per-trade TTL stored)
      if (!shouldExit) {
        let ttlTs = null; try { ttlTs = Number(JSON.parse(trade.extra_json || '{}')?.ttl?.ttl_ts_ms ?? null); } catch {}
        if (!(isFinite(ttlTs) && ttlTs > 0)) {
          const openedAtMs = Date.parse(trade.updated_at || trade.created_at || "");
          if (isFinite(openedAtMs) && (Date.now() - openedAtMs >= AGE_MS_FALLBACK)) {
            shouldExit = true;
            exitReason = 'TIME';
          }
        }
      }

      if (!shouldExit) continue;

      let exitPrice = currentPrice;
      if (!isShort) {
        const sellResult = await placeMarketSell(env, userId, trade.symbol, trade.qty, false, { reduceOnly: true });
        exitPrice = sellResult.avgPrice || currentPrice;
      } else {
        const session = await getSession(env, userId);
        if (session.exchange_name === 'crypto_parrot') {
          const buyResult = await placeDemoOrder(trade.symbol, "BUY", trade.qty, false);
          exitPrice = buyResult.avgPrice || currentPrice;
        } else {
          const requiredQuote = trade.qty * currentPrice;
          const buyResult = await placeMarketBuy(env, userId, trade.symbol, requiredQuote, { reduceOnly: true });
          exitPrice = buyResult.avgPrice || currentPrice;
        }
      }

      const grossPnl = !isShort ? (exitPrice - trade.entry_price) * trade.qty
                                : (trade.entry_price - exitPrice) * trade.qty;
      const fees = (trade.entry_price * trade.qty + exitPrice * trade.qty) * (protocol?.fee_rate ?? 0.001);
      const netPnl = grossPnl - fees;
      const realizedR = trade.risk_usd > 0 ? netPnl / trade.risk_usd : 0;

      await env.DB.prepare(
        `UPDATE trades SET status = 'closed', price = ?, realized_pnl = ?, realized_r = ?, fees = ?, close_type = ?, updated_at = ? WHERE id = ?`
      ).bind(exitPrice, netPnl, realizedR, fees, exitReason, nowISO(), trade.id).run();

      await setSymbolCooldown(env, userId, trade.symbol);

      const r_target = (() => { try { return Number(JSON.parse(trade.extra_json||"{}")?.r_target || STRICT_RRR); } catch { return STRICT_RRR; } })();
      await reconcileProtocol(env, userId, exitReason === 'TP', trade.risk_usd, r_target, fees);
      await logEvent(env, userId, 'trade_close', {
        symbol: trade.symbol, entry: trade.entry_price, exit: exitPrice, realizedR,
        hit: exitReason, exit_source: (exitReason === 'TTL' ? 'ttl_scan' : (exitReason === 'TIME' ? 'time_stop' : 'sl_tp_scan')),
        pnl: netPnl, fees, strict_rrr: true, r_target,
        ttl_ts_ms: (() => { try { return JSON.parse(trade.extra_json||"{}")?.ttl?.ttl_ts_ms ?? null; } catch { return null; } })()
      });
    } catch (e) {
      console.error('checkAndExitTrades error:', e);
      await logEvent(env, userId, 'error', { where: 'checkAndExitTrades', message: e.message });
    }
  }
}

/* Win-rate for Protocol Status */
async function getWinStats(env, userId) {
  const row = await env.DB.prepare(
    "SELECT SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins, COUNT(*) AS total " +
    "FROM trades WHERE user_id = ? AND status = 'closed'"
  ).bind(userId).first();
  const wins = Number(row?.wins || 0);
  const total = Number(row?.total || 0);
  const losses = Math.max(0, total - wins);
  const winRatePct = total > 0 ? (wins / total) * 100 : 0;
  return { winRatePct, wins, losses, total };
}

/* Exports */
export {
  protocolStatusTextB, renderProtocolStatus,
  createAutoSkipRecord, createPendingTrade, executeTrade, closeTradeNow, checkWorkingOrders, checkAndExitTrades,
  getWinStats
};

/* ======================================================================
   SECTION 6/7 — UI Texts, Cards, Keyboards, Dashboard, Lists,
                  Trade Details, Actions, Concurrency helpers
   ====================================================================== */

/* ---------- UI text blocks ---------- */
function welcomeText() {
  return [
    "Welcome. Please accept the risk note to continue.",
    "",
    "This quick wizard configures how the bot runs (Manual or Auto), selects your exchange, and verifies API keys.",
    "You can always restart the wizard from the Exchange page using Refresh if something looks stuck."
  ].join("\n");
}
function modeText() {
  return [
    "How should the bot run?",
    "",
    "Manual: you receive trade suggestions with full details (entry/stop/target) and approve or reject each one.",
    "Auto: the bot opens/closes trades by itself within strict risk budgets. Review via Manage Trades in the dashboard."
  ].join("\n");
}
function exchangeText() {
  return [
    "Choose your exchange.",
    "",
    "Spot orders supported: MEXC, Binance, LBank, CoinEx.",
    "Futures (USDT-M): Binance Futures is integrated.",
    "",
    "Bybit/Kraken/Gate/Huobi are read-only here (balances only).",
    "Tip: If it doesn’t work click Refresh to restart this wizard step cleanly."
  ].join("\n");
}
function askApiKeyText(exName, reason) {
  const head = exName ? `You chose ${exName.toUpperCase()}.\n` : "";
  const expl = "\nUse read+trade permission (no withdrawal). You can switch exchange via Back.";
  if (reason) return `Key check failed.\nReason: ${reason}\n\n${head}Send your API Key:${expl}`;
  return `${head}Send your API Key:${expl}`;
}
function askApiSecretText() {
  return [
    "Got your API Key.",
    "Now send your API Secret:",
    "",
    "We’ll verify your keys with the exchange, detect your balance and fee rate, and store keys encrypted."
  ].join("\n");
}
function confirmManualTextB(bal, feeRate) {
  return [
    "Setup complete (ACP V20 Brain, Manual Mode).",
    "",
    `Live Balance (TC): ${formatMoney(bal)}`,
    `ACP (${(ACP_FRAC*100).toFixed(0)}% of TC): ${formatMoney(ACP_FRAC * bal)}`,
    `Per-trade cap: ${formatPercent(PER_TRADE_CAP_FRAC)} of TC`,
    `Daily open risk cap: ${formatPercent(DAILY_OPEN_RISK_CAP_FRAC)} of TC`,
    `Fee Rate (per side): ${formatPercent(feeRate)}`,
    "Exits: dynamic RRR per idea when provided; fallback 2:1.",
    "",
    "In Manual mode, the bot will suggest trades with full details (entry, stop, target). You decide to Approve or Reject."
  ].join("\n");
}
function confirmAutoTextB(bal, feeRate) {
  return [
    "Setup complete (ACP V20 Brain, Auto Mode).",
    "",
    `Live Balance (TC): ${formatMoney(bal)}`,
    `ACP (${(ACP_FRAC*100).toFixed(0)}% of TC): ${formatMoney(ACP_FRAC * bal)}`,
    `Per-trade cap: ${formatPercent(PER_TRADE_CAP_FRAC)} of TC`,
    `Daily open risk cap: ${formatPercent(DAILY_OPEN_RISK_CAP_FRAC)} of TC`,
    `Fee Rate (per side): ${formatPercent(feeRate)}`,
    "Exits: dynamic RRR per idea when provided; fallback 2:1.",
    "",
    "In Auto mode, the bot opens/closes trades by itself, respecting budgets and EV gates. Review any time via Manage Trades."
  ].join("\n");
}

/* ---------- Cards ---------- */
function fmtExecIntent(extra) {
  const pol = String(extra?.entry?.policy || '').toLowerCase();
  const ex = String(extra?.exec?.exec || '').toLowerCase();
  const lim = extra?.entry?.limit;
  const parts = [];
  if (pol) parts.push(`policy=${pol}`);
  if (ex) parts.push(`exec=${ex}`);
  if (isFinite(lim)) parts.push(`limit=${formatMoney(lim)}`);
  return parts.length ? parts.join(" | ") : "policy=market";
}
function fmtMgmtOverlay(extra) {
  const m = extra?.mgmt || {};
  const parts = [];
  if (isFinite(m.be_at_r)) parts.push(`BE@${Number(m.be_at_r).toFixed(2)}R`);
  if (isFinite(m.partial_take_at_r)) parts.push(`PT@${Number(m.partial_take_at_r).toFixed(2)}R x${Number(m.partial_take_pct ?? 0.5).toFixed(2)}`);
  if (isFinite(m.trail_atr_mult) && m.trail_atr_mult > 0) parts.push(`Trail x${Number(m.trail_atr_mult).toFixed(2)}`);
  if (m.extend_ttl_if_unrealized_bps_ge != null) parts.push(`TTL+ if >= ${String(m.extend_ttl_if_unrealized_bps_ge)}`);
  if (m.early_cut_if_unrealized_bps_le != null) parts.push(`EarlyCut if <= ${String(m.early_cut_if_unrealized_bps_le)}`);
  return parts.length ? parts.join(" | ") : "none";
}
function pendingTradeCard(trade, extra) {
  const planned = Number(extra.quote_size || 0);
  const tcSnap = Number(extra.available_funds || 0);
  const pct = tcSnap > 0 ? planned / tcSnap : 0;
  const rTxt = Number(extra?.r_target || STRICT_RRR).toFixed(2);

  const entryIntent = fmtExecIntent(extra);
  const mgmtTxt = fmtMgmtOverlay(extra);

  return [
    `Trade Suggestion #${trade.id}`,
    "",
    `Symbol: ${trade.symbol}`,
    `Direction: ${trade.side === 'SELL' ? 'Short' : 'Long'}`,
    `Entry (intent): ${entryIntent}`,
    `Planned Entry~: ${formatMoney(trade.price)}`,
    `Stop Loss: ${formatMoney(extra.stop_price)} (${formatPercent(trade.stop_pct)})`,
    `Take Profit: ${formatMoney(extra.tp_price)} (RRR ${rTxt}:1)`,
    "",
    `Planned Capital: ${formatMoney(planned)} (${formatPercent(pct)} of TC snapshot)`,
    `Risk (Final): ${formatMoney(extra.budgets?.Final_Risk_USD || trade.risk_usd)}`,
    "",
    `Required: ${formatMoney(extra.required_funds)}`,
    `Available: ${formatMoney(extra.available_funds)}`,
    `Funds: ${extra.funds_ok ? "OK" : "Not enough"}`,
    "",
    `EV_R: ${Number(extra.EV_R ?? 0).toFixed(3)} | SQS: ${Number(extra.SQS ?? 0).toFixed(2)} | pH: ${Number(extra.pH ?? 0).toFixed(2)}`,
    `Mgmt overlay: ${mgmtTxt}`,
    "",
    "Approve to place the order now, or Reject to skip."
  ].join("\n");
}
function openTradeDetails(trade, extra, currentPrice) {
  const entry = trade.entry_price;
  const isShort = (trade.side === 'SELL') || (String(extra?.direction || '').toLowerCase() === 'short');
  const pnlUsd = isShort ? (entry - currentPrice) * trade.qty : (currentPrice - entry) * trade.qty;
  const pnlR = trade.risk_usd > 0 ? pnlUsd / trade.risk_usd : 0;

  const tcAtEntry = Number(extra?.metrics?.tc_at_entry || 0);
  const notionalAtEntry = Number(extra?.metrics?.notional_at_entry || (entry * trade.qty));
  const pctEntry = tcAtEntry > 0 ? notionalAtEntry / tcAtEntry : 0;

  const rTxt = Number(extra?.r_target || STRICT_RRR).toFixed(2);
  const entryIntent = fmtExecIntent(extra);
  const mgmtTxt = fmtMgmtOverlay(extra);

  return [
    `Trade #${trade.id}`,
    "",
    `Symbol: ${trade.symbol}`,
    `Direction: ${isShort ? 'Short' : 'Long'}`,
    `Entry (intent): ${entryIntent}`,
    `Entry: ${formatMoney(entry)}`,
    `Current: ${formatMoney(currentPrice)}`,
    `Stop: ${formatMoney(trade.stop_price)} (${formatPercent(trade.stop_pct)})`,
    `Target: ${formatMoney(trade.tp_price)} (RRR ${rTxt}:1)`,
    "",
    `Quantity: ${trade.qty.toFixed(6)}`,
    `Capital used at entry: ${formatMoney(notionalAtEntry)} (${formatPercent(pctEntry)} of TC at entry)`,
    `Risk (Final): ${formatMoney(trade.risk_usd)}`,
    "",
    `Live P&L:`,
    `USD: ${pnlUsd >= 0 ? "+" : ""}${formatMoney(pnlUsd)}`,
    `R: ${pnlR >= 0 ? "+" : ""}${pnlR.toFixed(2)}R`,
    "",
    `Mgmt overlay: ${mgmtTxt}`,
    "SL/TP are enforced by the bot. You can Close Now anytime."
  ].join("\n");
}
function skippedTradeCard(trade, extra) {
  const lines = [
    `Suggestion #${trade.id} (Skipped)`,
    "",
    `Symbol: ${trade.symbol}`,
    `Direction: ${trade.side === 'SELL' ? 'Short' : 'Long'}`,
    `Planned Entry: ${formatMoney(trade.price)}`,
    `Stop: ${formatMoney(trade.stop_price)} (${formatPercent(trade.stop_pct)})`,
    `Target: ${formatMoney(trade.tp_price)} (RRR ${Number(extra?.r_target || STRICT_RRR).toFixed(2)}:1)`,
    "",
    `Status: SKIPPED — ${String(extra?.skip_reason || 'reason unknown').toUpperCase()}`,
  ];
  const meta = extra?.skip_meta || {};
  if (extra?.skip_reason === 'insufficient_funds') {
    if (typeof meta.available !== 'undefined') lines.push(`Available: ${formatMoney(meta.available)}`);
    if (typeof meta.required  !== 'undefined') lines.push(`Required:  ${formatMoney(meta.required)}`);
  }
  if (extra?.skip_reason === 'ev_gate' && typeof meta.EV_R !== 'undefined') lines.push(`EV_R: ${Number(meta.EV_R).toFixed(3)}`);
  if (extra?.skip_reason === 'hse_gate' && typeof meta.pH  !== 'undefined') lines.push(`pH: ${Number(meta.pH).toFixed(2)}`);
  if (extra?.skip_reason === 'duplicate_symbol') {
    lines.push(`Duplicate exposure: already open or pending on ${trade.symbol}.`);
  }
  if (extra?.skip_reason === 'cooldown_active') {
    const ms = Number(meta?.remain_ms || 0);
    if (ms > 0) lines.push(`Cooldown: ${formatDurationShort(ms)} remaining`);
  }
  return lines.join("\n");
}

/* ---------- Keyboards ---------- */
function exchangeButtons() {
  return [
    [{ text: "Crypto Parrot (Demo) 🦜", callback_data: "exchange_crypto_parrot" }],
    [{ text: "MEXC", callback_data: "exchange_mexc" }, { text: "Binance", callback_data: "exchange_binance" }],
    [{ text: "LBank", callback_data: "exchange_lbank" }, { text: "CoinEx", callback_data: "exchange_coinex" }],
    [{ text: "Binance Futures (USDT-M)", callback_data: "exchange_binance_futures" }],
    [{ text: "Bybit", callback_data: "exchange_bybit" }, { text: "Kraken", callback_data: "exchange_kraken" }],
    [{ text: "Gate", callback_data: "exchange_gate" }, { text: "Huobi", callback_data: "exchange_huobi" }],
    [{ text: "Back ◀️", callback_data: "action_back_to_mode" }],
    [{ text: "Refresh 🔄", callback_data: "action_refresh_wizard" }, { text: "Stop Bot ⛔", callback_data: "action_stop_confirm" }]
  ];
}
function kbDashboard(mode, paused, hasProtocol, hasOpenPositions) {
  const buttons = [[{ text: "Manage Trades 📋", callback_data: "manage_trades" }]];
  if (hasProtocol) buttons.push([{ text: "Protocol Status 🛡️", callback_data: "protocol_status" }]);
  if (mode === 'auto') buttons.push([{ text: paused ? "Resume Auto ▶️" : "Pause Auto ⏸️", callback_data: "auto_toggle" }]);
  if (!hasOpenPositions) buttons.push([{ text: "Delete History 🧽", callback_data: "action_delete_history" }]);
  buttons.push([{ text: "Stop Bot ⛔", callback_data: "action_stop_confirm" }]);
  return buttons;
}
function kbPendingTrade(tradeId, fundsOk) {
  const rows = [];
  if (fundsOk) rows.push([{ text: "Approve ✅", callback_data: `tr_appr:${tradeId}` }, { text: "Reject ❌", callback_data: `tr_rej:${tradeId}` }]);
  else rows.push([{ text: "Reject ❌", callback_data: `tr_rej:${tradeId}` }]);
  rows.push([{ text: "Continue ▶️", callback_data: "continue_dashboard" }]);
  rows.push([{ text: "Back to list", callback_data: "manage_trades" }], [{ text: "Stop ⛔", callback_data: "action_stop_confirm" }]);
  return rows;
}
function kbOpenTradeStrict(tradeId) {
  return [
    [{ text: "Close Now ⏹️", callback_data: `tr_close:${tradeId}` }, { text: "Refresh 🔄", callback_data: `tr_refresh:${tradeId}` }],
    [{ text: "Continue ▶️", callback_data: "continue_dashboard" }],
    [{ text: "Back to list", callback_data: "manage_trades" }],
    [{ text: "Stop ⛔", callback_data: "action_stop_confirm" }]
  ];
}

/* ---------- Dashboard ---------- */
async function sendDashboard(env, userId, messageId = 0) {
  const session = await getSession(env, userId);
  const protocol = await getProtocolState(env, userId);
  if (!session) return;
  const openCount = await getOpenPositionsCount(env, userId);
  const hasOpenPositions = openCount > 0;

  const paused = (session.auto_paused || "false") === "true";
  let text;
  if (session.bot_mode === "manual") {
    text = "Bot is active (Manual).\n\nExits: dynamic RRR per idea when provided; fallback 2:1.\nACP V20 sizing.\nUse Manage Trades to inspect suggestions and open/closed positions.";
  } else {
    text = `Bot is active (Auto).\nStatus: ${paused ? "Paused" : "Running"}.\n\nExits: dynamic RRR per idea when provided; fallback 2:1.\nACP V20 sizing.\nUse Manage Trades to inspect positions.`;
  }
  const buttons = kbDashboard(session.bot_mode, paused, !!protocol, hasOpenPositions);
  if (messageId) await editMessage(userId, messageId, text, buttons, env);
  else await sendMessage(userId, text, buttons, env);
}

/* ---------- Lists ---------- */
async function getTradeCounts(env, userId) {
  const total = (await env.DB.prepare("SELECT COUNT(*) as c FROM trades WHERE user_id = ?").bind(userId).first())?.c || 0;
  const open = (await env.DB.prepare("SELECT COUNT(*) as c FROM trades WHERE user_id = ? AND status = 'open'").bind(userId).first())?.c || 0;
  const pending = (await env.DB.prepare("SELECT COUNT(*) as c FROM trades WHERE user_id = ? AND status = 'pending'").bind(userId).first())?.c || 0;
  const closed = (await env.DB.prepare("SELECT COUNT(*) as c FROM trades WHERE user_id = ? AND status = 'closed'").bind(userId).first())?.c || 0;
  const rejected = (await env.DB.prepare("SELECT COUNT(*) as c FROM trades WHERE user_id = ? AND status = 'rejected'").bind(userId).first())?.c || 0;
  return { total: Number(total), open: Number(open), pending: Number(pending), closed: Number(closed), rejected: Number(rejected) };
}

async function sendTradesListUI(env, userId, page = 1, messageId = 0) {
  const counts = await getTradeCounts(env, userId);
  const rows = await env.DB.prepare(
    "SELECT id, symbol, side, qty, status, close_type, stop_pct, stop_price, tp_price, entry_price, price, realized_pnl, realized_r, extra_json " +
    "FROM trades WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?"
  ).bind(userId, 5, (page - 1) * 5).all();

  const list = rows.results || [];
  const header = `Trades — Page ${page}\nTotals: ${counts.total} (Pending ${counts.pending} | Open ${counts.open} | Closed ${counts.closed} | Rej ${counts.rejected})\n\n`;

  let body;
  if (!list.length) {
    body = "No trades on this page.";
  } else {
    body = list.map(r => {
      const qty = Number(r.qty || 0);
      const stopPctTxt = r.stop_pct !== undefined ? ` (${formatPercent(Number(r.stop_pct || 0))})` : "";
      const ex = (()=>{ try{ return JSON.parse(r.extra_json||'{}'); }catch{return{};}})();
      const rTxt = Number(ex?.r_target || STRICT_RRR).toFixed(2);
      const intent = fmtExecIntent(ex);
      if (r.status === 'open') {
        return `#${r.id} ${r.symbol} ${r.side} ${qty.toFixed(4)} — open | Entry ${formatMoney(r.entry_price)} [${intent}] | SL ${formatMoney(r.stop_price)}${stopPctTxt} | TP ${formatMoney(r.tp_price)} (R ${rTxt})`;
      } else if (r.status === 'pending') {
        return `#${r.id} ${r.symbol} ${r.side} ${qty.toFixed(4)} — pending | Entry~ ${formatMoney(r.price)} [${intent}] | SL ${formatMoney(r.stop_price)}${stopPctTxt} | TP ${formatMoney(r.tp_price)} (R ${rTxt})`;
      } else if (r.status === 'rejected') {
        let reason = '';
        try { const exj = JSON.parse(r.extra_json || '{}'); if (exj.auto_skip && exj.skip_reason) reason = ` — skipped: ${String(exj.skip_reason).replace(/_/g,' ')}`; } catch (_) {}
        return `#${r.id} ${r.symbol} ${r.side} ${qty.toFixed(4)} — rejected${reason} | Entry~ ${formatMoney(r.price)} [${intent}] | SL ${formatMoney(r.stop_price)}${stopPctTxt} | TP ${formatMoney(r.tp_price)} (R ${rTxt})`;
      } else if (r.status === 'closed') {
        const pnl = Number(r.realized_pnl || 0);
        const rVal = Number(r.realized_r || 0);
        const hit = r.close_type ? ` (${r.close_type})` : "";
        return `#${r.id} ${r.symbol} ${r.side} ${qty.toFixed(4)} — closed${hit} | P&L ${pnl >= 0 ? "+" : ""}${formatMoney(pnl)} | R ${rVal >= 0 ? "+" : ""}${rVal.toFixed(2)}`;
      } else {
        return `#${r.id} ${r.symbol} ${r.side} ${qty.toFixed(4)} — ${r.status}`;
      }
    }).join("\n");
  }

  const text = header + body;
  const buttons = [];
  for (const r of list) buttons.push([{ text: `View #${r.id}`, callback_data: `tr_view:${r.id}` }]);
  buttons.push([{ text: "Prev", callback_data: `tr_page:${Math.max(1, page - 1)}` }, { text: "Next", callback_data: `tr_page:${page + 1}` }]);
  buttons.push([{ text: "Clean 🧹", callback_data: "clean_pending" }]);
  buttons.push([{ text: "Continue ▶️", callback_data: "continue_dashboard" }]);

  if (messageId) await editMessage(userId, messageId, text, buttons, env);
  else await sendMessage(userId, text, buttons, env);
}

/* ---------- Trade details UI ---------- */
async function sendTradeDetailsUI(env, userId, tradeId, messageId = 0) {
  const trade = await env.DB.prepare("SELECT * FROM trades WHERE id = ? AND user_id = ?").bind(tradeId, userId).first();
  if (!trade) {
    const t = "Trade not found.";
    const b = [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }]];
    if (messageId) await editMessage(userId, messageId, t, b, env);
    else await sendMessage(userId, t, b, env);
    return;
  }
  const extra = JSON.parse(trade.extra_json || '{}');

  if (trade.status === 'pending') {
    const text = pendingTradeCard(trade, extra);
    const buttons = kbPendingTrade(tradeId, extra.funds_ok);
    if (messageId) await editMessage(userId, messageId, text, buttons, env);
    else await sendMessage(userId, text, buttons, env);
    return;
  }

  if (trade.status === 'open') {
    let currentPrice = await getCurrentPrice(trade.symbol);
    if (!isFinite(currentPrice) || currentPrice <= 0) {
      try {
        const ob = await getOrderBookSnapshot(trade.symbol);
        const { mid } = computeSpreadDepth(ob);
        if (isFinite(mid) && mid > 0) currentPrice = mid;
      } catch {}
    }
    const text = openTradeDetails(trade, extra, currentPrice);
    const buttons = kbOpenTradeStrict(tradeId);
    if (messageId) await editMessage(userId, messageId, text, buttons, env);
    else await sendMessage(userId, text, buttons, env);
    return;
  }

  if (trade.status === 'rejected' && extra?.auto_skip) {
    const text = skippedTradeCard(trade, extra);
    const buttons = [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }], [{ text: "Back to list", callback_data: "manage_trades" }]];
    if (messageId) await editMessage(userId, messageId, text, buttons, env);
    else await sendMessage(userId, text, buttons, env);
    return;
  }

  // Closed
  const tcAtEntry = Number(extra?.metrics?.tc_at_entry || 0);
  const notionalAtEntry = Number(extra?.metrics?.notional_at_entry || (trade.entry_price * trade.qty));
  const pctEntry = tcAtEntry > 0 ? notionalAtEntry / tcAtEntry : 0;

  const text = [
    `Trade #${trade.id} (Closed)`,
    "",
    `Symbol: ${trade.symbol}`,
    `Direction: ${trade.side === 'SELL' ? 'Short' : 'Long'}`,
    `Entry: ${formatMoney(trade.entry_price)}`,
    `Exit: ${formatMoney(trade.price)}`,
    `Close Type: ${trade.close_type || "-"}`,
    `P&L: ${trade.realized_pnl >= 0 ? "+" : ""}${formatMoney(trade.realized_pnl)}`,
    `Realized R: ${trade.realized_r >= 0 ? "+" : ""}${trade.realized_r.toFixed(2)}R`,
    `Capital used at entry: ${formatMoney(notionalAtEntry)} (${formatPercent(pctEntry)} of TC at entry)`
  ].join("\n");

  const buttons = [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }], [{ text: "Back to list", callback_data: "manage_trades" }]];
  if (messageId) await editMessage(userId, messageId, text, buttons, env);
  else await sendMessage(userId, text, buttons, env);
}

/* ---------- Trade action callbacks ---------- */
async function handleTradeAction(env, userId, action, messageId = 0) {
  const parts = action.split(':');
  const cmd = parts[0];
  const tradeId = parseInt(parts[1]);
  switch (cmd) {
    case 'tr_view':
      await sendTradeDetailsUI(env, userId, tradeId, messageId);
      break;
    case 'tr_page':
      await sendTradesListUI(env, userId, tradeId, messageId);
      break;
    case 'tr_appr': {
      const row = await env.DB.prepare("SELECT extra_json, status FROM trades WHERE id = ? AND user_id = ?").bind(tradeId, userId).first();
      if (!row || row.status !== 'pending') {
        await editMessage(userId, messageId, `Trade #${tradeId} not pending.`, [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }]], env);
        break;
      }
      const extra = JSON.parse(row.extra_json || '{}');
      if (extra.funds_ok !== true) {
        await editMessage(userId, messageId, `Insufficient funds to approve trade #${tradeId}.`, [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }]], env);
        break;
      }
      const approved = await executeTrade(env, userId, tradeId);
      if (approved) await sendTradeDetailsUI(env, userId, tradeId, messageId);
      else await editMessage(userId, messageId, `Failed to execute trade #${tradeId}.`, [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }]], env);
      break;
    }
    case 'tr_rej':
      await env.DB.prepare("UPDATE trades SET status = 'rejected', updated_at = ? WHERE id = ? AND user_id = ?").bind(nowISO(), tradeId, userId).run();
      await sendTradesListUI(env, userId, 1, messageId);
      break;
    case 'tr_refresh':
      await sendTradeDetailsUI(env, userId, tradeId, messageId);
      break;
    case 'tr_close': {
      const res = await closeTradeNow(env, userId, tradeId);
      if (res.ok) await sendTradeDetailsUI(env, userId, tradeId, messageId);
      else await editMessage(userId, messageId, `Close failed: ${res.msg}`, [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }]], env);
      break;
    }
  }
}

/* ---------- Stop/Restart helpers ---------- */
async function handleStopRequest(env, userId, messageId = 0) {
  const openCount = await getOpenPositionsCount(env, userId);
  if (openCount > 0) {
    const text = "You have open positions and cannot stop until they are closed.\nYou can close all now, or continue and manage them individually.";
    const buttons = [
      [{ text: "Close all now ⏹️", callback_data: "action_stop_closeall" }],
      [{ text: "Continue ▶️", callback_data: "continue_dashboard" }]
    ];
    if (messageId) await editMessage(userId, messageId, text, buttons, env);
    else await sendMessage(userId, text, buttons, env);
  } else {
    await handleDirectStop(env, userId);
  }
}
async function restartWizardAtExchange(env, userId, messageId = 0) {
  await deleteSession(env, userId);
  await createSession(env, userId);
  await saveSession(env, userId, { current_step: 'awaiting_exchange' });
  const text = exchangeText();
  const buttons = exchangeButtons();
  if (messageId) await editMessage(userId, messageId, text, buttons, env);
  else await sendMessage(userId, text, buttons, env);
}
async function showWelcomeStep(env, userId, messageId = 0) {
  await saveSession(env, userId, { current_step: 'awaiting_accept' });
  const text = welcomeText();
  const buttons = [[{ text: "Accept ✅", callback_data: "action_accept_terms" }], [{ text: "Stop Bot ⛔", callback_data: "action_stop_confirm" }]];
  if (messageId) await editMessage(userId, messageId, text, buttons, env);
  else await sendMessage(userId, text, buttons, env);
}

/* ---------- Concurrency helpers ---------- */
function getMaxConcurrent(env) {
  const v = Number(env.MAX_CONCURRENT_POSITIONS || DEFAULT_MAX_CONCURRENT_POS);
  return Math.max(1, Math.floor(isFinite(v) ? v : DEFAULT_MAX_CONCURRENT_POS));
}
function getMaxNewPerCycle(env) {
  const v = Number(env.MAX_NEW_POSITIONS_PER_CYCLE || DEFAULT_MAX_NEW_POSITIONS_PER_CYCLE);
  return Math.max(1, Math.floor(isFinite(v) ? v : DEFAULT_MAX_NEW_POSITIONS_PER_CYCLE));
}

/* Exports */
export {
  welcomeText, modeText, exchangeText, askApiKeyText, askApiSecretText, confirmManualTextB, confirmAutoTextB,
  pendingTradeCard, openTradeDetails, skippedTradeCard,
  exchangeButtons, kbDashboard, kbPendingTrade, kbOpenTradeStrict,
  sendDashboard, getTradeCounts, sendTradesListUI, sendTradeDetailsUI, handleTradeAction,
  handleStopRequest, restartWizardAtExchange, showWelcomeStep,
  getMaxConcurrent, getMaxNewPerCycle
};

/* ======================================================================
   SECTION 7/7 — processUser, Telegram handler, HTTP endpoints,
                  Cron with fan-out fallback, Worker export
   ====================================================================== */

/* ---------- Per-user processing (flat-CPU: early exits, time guards) ---------- */
async function processUser(env, userId, budget = null) {
  try {
    if (budget?.isExpired()) return;
    const session = await getSession(env, userId);
    if (!session) return;

    // Helper: force all ideas through (env.NO_REJECTS = '1')
    const forceAll = () => String(env?.NO_REJECTS || '0') === '1';
    // Cycle-only duplicate guard flag
    const cycleOnly = String(env?.CYCLE_ONLY_DUP_GUARD || '0') === '1';

    // Resolve posted-maker orders first, then enforce SL/TP
    await checkWorkingOrders(env, userId);
    if (budget?.isExpired()) return;

    await checkAndExitTrades(env, userId);
    if (budget?.isExpired()) return;

    const openPauseTh = Number(env.OPEN_PAUSE_THRESHOLD || 2);
    const openCountNow = await getOpenPositionsCount(env, userId);
    const overConcurrency = openCountNow >= openPauseTh;

    const protocol = await getProtocolState(env, userId);
    if (session.status !== 'active' || !protocol || protocol.phase === 'terminated') return;

    const maxPos = getMaxConcurrent(env);
    const maxNew = getMaxNewPerCycle(env);

    const TC = await getTotalCapital(env, userId);

    // Notional budgets (env-driven caps; behavior matches original logic)
    const perTradeNotionalCapFrac = Number(env?.PER_TRADE_NOTIONAL_CAP_FRAC ?? 0.10);
    const dailyNotionalCapFrac    = Number(env?.DAILY_OPEN_NOTIONAL_CAP_FRAC ?? 0.30);
    const dailyNotionalCap        = dailyNotionalCapFrac * TC;
    const openNotional            = await getOpenNotional(env, userId);
    let dailyNotionalLeft         = Math.max(0, dailyNotionalCap - openNotional);

    // Risk budgets (original constants)
    const D = DAILY_OPEN_RISK_CAP_FRAC * TC;
    const openRisk = await getOpenPortfolioRisk(env, userId);
    let D_left = Math.max(0, D - openRisk);

    // Ideas (prefer GitHub/GHA 10-pack)
    const ideas = await getLatestIdeas(env);
    if (!ideas || !ideas.ideas || !ideas.ideas.length) return;

    // Freshness guard
    try {
      const ts = new Date(ideas.ts || 0).getTime();
      const maxAgeMs = Number(env.IDEAS_MAX_AGE_MS || 20 * 60 * 1000);
      if (!ts || (Date.now() - ts) > maxAgeMs) return;
    } catch (_) {}

    // Snapshot dedupe — process each ideas.ts only once per user
    const ideasTs = String(ideas.ts || "");
    const lastKey = `last_ideas_ts_user_${userId}`;
    const lastSeenTs = await kvGet(env, lastKey);

    // Early skip when nothing to do: no open positions and snapshot already processed
    if (openCountNow === 0 && lastSeenTs && lastSeenTs === ideasTs) return;

    // Duplicate symbols control map (per-cycle)
    const counts = new Map();

    // If NOT cycle-only, pre-populate with existing open/pending exposures (cross-cycle enforcement)
    let allowDup = false, capPerSym = 1;
    if (!cycleOnly) {
      try {
        const rowsCnt = await env.DB.prepare(
          "SELECT symbol, COUNT(*) AS c FROM trades WHERE user_id = ? AND status IN ('open','pending') GROUP BY symbol"
        ).bind(userId).all();
        for (const r of (rowsCnt.results || [])) {
          const s = String(r.symbol || '').toUpperCase();
          const c = Number(r.c || 0);
          if (s) counts.set(s, c);
        }
      } catch (_) {}
      allowDup = String(env?.ALLOW_DUPLICATE_SYMBOLS || '0') === '1';
      capPerSym = Math.max(1, Math.floor(Number(env?.MAX_POS_PER_SYMBOL || 1)));
    }

    const activeSyms = await getActiveExposureSymbols(env, userId);
    let placed = 0; // opened/pending count this cycle
    const stopReason = (!D_left ? 'daily_risk_budget_exhausted' : (!dailyNotionalLeft ? 'daily_notional_exhausted' : null));

    const ideasTsMs = new Date(ideas.ts || 0).getTime();

    for (let idx = 0; idx < ideas.ideas.length; idx++) {
      if (budget?.isExpired()) break;
      budget?.ensure("ideas_loop");

      const idea = ideas.ideas[idx];

      // TTL gating (skip only if NO_REJECTS is off)
      try {
        const ttlMs = Number(idea.ttl_sec || 0) * 1000;
        if (ttlMs > 0 && ideasTsMs > 0 && Date.now() - ideasTsMs > ttlMs) {
          if (!forceAll()) {
            await createAutoSkipRecord(env, userId, idea, protocol, 'stale_idea_ttl', { ttl_sec: idea.ttl_sec });
          }
          continue;
        }
      } catch {}

      const symU = String(idea.symbol || '').toUpperCase();

      // Duplicate symbol enforcement
      const curCount = counts.get(symU) || 0;
      if (cycleOnly) {
        // Block only within this cycle
        if (curCount > 0) {
          await createAutoSkipRecord(env, userId, idea, protocol, 'duplicate_symbol_cycle', { curCount });
          continue;
        }
      } else {
        // Env-driven cross-cycle policy
        if ((!allowDup && curCount > 0) || (allowDup && curCount >= capPerSym)) {
          await createAutoSkipRecord(env, userId, idea, protocol, 'duplicate_symbol', { curCount, capPerSym });
          continue;
        }
      }

      // Concurrency limit (UNCONDITIONAL)
      if (overConcurrency || (openCountNow + placed) >= maxPos) {
        if (!forceAll()) {
          await createAutoSkipRecord(env, userId, idea, protocol, 'concurrency_limit', { openCountNow, placed, maxPos, th: openPauseTh });
        }
        continue;
      }

      // Budget stop reason (skip only if NO_REJECTS is off)
      if (stopReason && !forceAll()) {
        await createAutoSkipRecord(env, userId, idea, protocol, stopReason, { D_left, dailyNotionalLeft });
        continue;
      }

      // Try to create a pending trade
      const beforeOpenRisk = await getOpenPortfolioRisk(env, userId);
      const { id, meta } = await createPendingTrade(env, userId, idea, protocol);
      if (!id) {
        // If createPendingTrade still rejects (e.g., hard market_data), log as before
        await createAutoSkipRecord(env, userId, idea, protocol, meta?.skipReason || 'unknown', meta);
        continue;
      }

      // Execute (Auto) or show card (Manual)
      if (session.bot_mode === 'auto' && session.auto_paused !== 'true') {
        const executed = await executeTrade(env, userId, id);
        if (!executed) {
          await env.DB.prepare("UPDATE trades SET status = 'failed', updated_at = ? WHERE id = ?").bind(nowISO(), id).run();
          continue;
        }

        // Update per-cycle duplicate count
        counts.set(symU, (counts.get(symU) || 0) + 1);

        activeSyms.add(symU);
        placed++;

        // Cycle cap enforcement (UNCONDITIONAL)
        if (placed >= maxNew) {
          if (!forceAll()) {
            for (const rest of ideas.ideas.slice(idx + 1)) {
              await createAutoSkipRecord(env, userId, rest, protocol, 'cycle_cap_reached', { placed, maxNew });
            }
          }
          break;
        }

        // Update budgets after open
        const afterOpenRisk = await getOpenPortfolioRisk(env, userId);
        D_left -= Math.max(0, afterOpenRisk - beforeOpenRisk);
        const afterOpenNotional = await getOpenNotional(env, userId);
        dailyNotionalLeft = Math.max(0, dailyNotionalCap - afterOpenNotional);
      } else if (session.bot_mode === 'manual') {
        // Fetch and show the pending card
        const trow = await env.DB.prepare("SELECT * FROM trades WHERE id = ? AND user_id = ?").bind(id, userId).first();
        const extra = JSON.parse(trow.extra_json || '{}');
        await sendMessage(userId, pendingTradeCard(trow, extra), kbPendingTrade(id, extra.funds_ok), env);

        // Update per-cycle duplicate count
        counts.set(symU, (counts.get(symU) || 0) + 1);

        activeSyms.add(symU);
        placed++;

        // Cycle cap enforcement (UNCONDITIONAL)
        if (placed >= maxNew) {
          if (!forceAll()) {
            for (const rest of ideas.ideas.slice(idx + 1)) {
              await createAutoSkipRecord(env, userId, rest, protocol, 'cycle_cap_reached', { placed, maxNew });
            }
          }
          break;
        }
        dailyNotionalLeft -= Number(extra.quote_size || 0);
      }

      // If budgets became exhausted mid-loop, log the rest as exhausted (only if NO_REJECTS is off)
      if ((D_left <= 0 || dailyNotionalLeft <= 0) && !forceAll()) {
        const reason = D_left <= 0 ? 'daily_risk_budget_exhausted' : 'daily_notional_exhausted';
        for (const rest of ideas.ideas.slice(idx + 1)) {
          await createAutoSkipRecord(env, userId, rest, protocol, reason, { D_left, dailyNotionalLeft });
        }
        break;
      }
    }

    // Mark this snapshot as processed for this user (prevents reprocessing every minute)
    if (ideasTs) await kvSet(env, lastKey, ideasTs);
  } catch (e) {
    console.error(`processUser error for ${userId}:`, e);
    await logEvent(env, userId, 'error', { where: 'processUser', message: e.message });
  }
}

/* ---------- Telegram update handler (debounced exits + working orders) ---------- */
async function handleTelegramUpdate(update, env, ctx) {
  const isCb = !!update.callback_query;
  const msg = isCb ? update.callback_query.message : update.message;
  if (!msg) return;

  const userId = msg.chat.id;
  const text = isCb ? update.callback_query.data : (update.message.text || "");
  const messageId = isCb ? update.callback_query.message.message_id : (update.message?.message_id || 0);
  if (isCb) ctx.waitUntil(answerCallbackQuery(env, update.callback_query.id));

  // Debounced checks: working orders + exits
  try {
    const openCount = await getOpenPositionsCount(env, userId);
    // Count pending with open_order
    const rowPend = await env.DB
      .prepare("SELECT COUNT(*) as c FROM trades WHERE user_id = ? AND status = 'pending' AND json_extract(extra_json,'$.open_order.id') IS NOT NULL")
      .bind(userId).first();
    const postedCount = Number(rowPend?.c || 0);

    if (openCount > 0 || postedCount > 0) {
      const k = `exit_check_ts_${userId}`;
      const last = Number(await kvGet(env, k) || 0);
      const now = Date.now();
      if (now - last > 3000) {
        await kvSet(env, k, now);
        ctx.waitUntil(checkWorkingOrders(env, userId));
        ctx.waitUntil(checkAndExitTrades(env, userId));
      }
    }
  } catch (_) {}

  let session = await getSession(env, userId);
  if (!session) {
    if (text !== "/start") { await sendMessage(userId, "Session expired. Send /start to begin.", null, env); return; }
    session = await createSession(env, userId);
  }

  // Quick navigation
  if (text === "continue_dashboard" || text === "back_dashboard") { await sendDashboard(env, userId, isCb ? messageId : 0); return; }
  if (text === "action_refresh_wizard") { await restartWizardAtExchange(env, userId, isCb ? messageId : 0); return; }

  // Stop actions
  if (text === "action_direct_stop") { await handleDirectStop(env, userId); return; }
  if (text === "action_stop_confirm") {
    if (["start","awaiting_accept","awaiting_mode","awaiting_exchange","awaiting_api_key","awaiting_api_secret"].includes(session.current_step)) {
      await handleDirectStop(env, userId);
    } else {
      await handleStopRequest(env, userId, isCb ? messageId : 0);
    }
    return;
  }
  if (text === "action_stop_keep") {
    await saveSession(env, userId, { status: 'halted' });
    const t = "Session is alive. Send /continue to go to the Dashboard.";
    if (isCb && messageId) await editMessage(userId, messageId, t, null, env); else await sendMessage(userId, t, null, env);
    return;
  }
  if (text === "action_stop_closeall") {
    const openTrades = await env.DB.prepare("SELECT id FROM trades WHERE user_id = ? AND status = 'open'").bind(userId).all();
    let closed = 0;
    for (const t of (openTrades.results || [])) { try { const res = await closeTradeNow(env, userId, t.id); if (res.ok) closed++; } catch(_){} }
    const t = `All positions closed (${closed}).`; if (isCb && messageId) await editMessage(userId, messageId, t, null, env); else await sendMessage(userId, t, null, env);
    await handleDirectStop(env, userId); return;
  }
  if (text === "action_wipe_keys") {
    const openCount = await getOpenPositionsCount(env, userId);
    if (openCount > 0) {
      const t = "Cannot wipe keys while positions are open. Close them first.";
      const b = [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }]];
      if (isCb && messageId) await editMessage(userId, messageId, t, b, env); else await sendMessage(userId, t, b, env);
      return;
    }
    await saveSession(env, userId, { current_step: 'awaiting_exchange', api_key_encrypted: null, api_secret_encrypted: null });
    await restartWizardAtExchange(env, userId, isCb ? messageId : 0);
    return;
  }
  if (text === "action_retry_apikey") {
    await saveSession(env, userId, { current_step: 'awaiting_api_key', temp_api_key: null });
    if (isCb && messageId) await editMessage(userId, messageId, askApiKeyText(session.exchange_name), null, env);
    else await sendMessage(userId, askApiKeyText(session.exchange_name), null, env);
    return;
  }
  if (text === "clean_pending") {
    // Remove pending/rejected and resync flat-CPU pacing counters
    await env.DB.prepare("DELETE FROM trades WHERE user_id = ? AND status IN ('pending','rejected')").bind(userId).run();
    await resyncPacingFromDB(env, userId);
    await sendTradesListUI(env, userId, 1, isCb ? messageId : 0);
    return;
  }
  if (text === "action_delete_history") {
    const openCount = await getOpenPositionsCount(env, userId);
    if (openCount > 0) {
      const t = "You have open positions. Close all current trades to delete history.";
      const b = [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }]];
      if (isCb && messageId) await editMessage(userId, messageId, t, b, env); else await sendMessage(userId, t, b, env);
      return;
    }
    try {
      await env.DB.prepare("DELETE FROM events_log WHERE user_id = ?").bind(userId).run();
      await env.DB.prepare("DELETE FROM trades WHERE user_id = ?").bind(userId).run();
      await env.DB.prepare("DELETE FROM protocol_state WHERE user_id = ?").bind(userId).run();

      // Reset KV (peak equity + pacing counters)
      await kvSet(env, `peak_equity_user_${userId}`, 0);
      const today = utcDayKey();
      await kvSet(env, `tcr_today_date_${userId}`, today);
      await kvSet(env, `tcr_today_count_${userId}`, 0);
      await kvSet(env, `tcr_7d_roll_${userId}`, JSON.stringify({ last7: [] }));

      await deleteSession(env, userId);
      await createSession(env, userId);
      await showWelcomeStep(env, userId, isCb ? messageId : 0);
    } catch (e) {
      console.error("action_delete_history error:", e);
      const t = "Failed to delete history (internal error)."; const b = [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }]];
      if (isCb && messageId) await editMessage(userId, messageId, t, b, env); else await sendMessage(userId, t, b, env);
    }
    return;
  }
  if (text === "/continue") {
    if (session.status === 'halted') { await saveSession(env, userId, { status: 'active' }); await sendDashboard(env, userId, isCb ? messageId : 0); }
    else { const t = "Bot is already active."; if (isCb && messageId) await editMessage(userId, messageId, t, null, env); else await sendMessage(userId, t, null, env); }
    return;
  }

  // Mode switches require no open positions
  if (text === '/manual' || text === '/auto') {
    const openCount = await getOpenPositionsCount(env, userId);
    if (openCount > 0) {
      const t = "Cannot change mode while positions are open. Close positions first.";
      const b = [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }]];
      if (isCb && messageId) await editMessage(userId, messageId, t, b, env); else await sendMessage(userId, t, b, env);
      return;
    }
  }

  // Slash commands
  if (/^\/price\s+/i.test(text)) {
    const parts = text.trim().split(/\s+/); const sym = (parts[1] || "").toUpperCase();
    if (!sym) { await sendMessage(userId, "Usage: /price BTC", null, env); return; }
    const p = await getCurrentPrice(sym); await sendMessage(userId, `${sym}USDT price: ${formatMoney(p)}`, null, env); return;
  }
  if (text.startsWith('/approve')) {
    const id = parseInt((text.split(/\s+/)[1] || ''), 10);
    const row = await env.DB.prepare("SELECT id, extra_json, status FROM trades WHERE user_id = ? AND status = 'pending' ORDER BY created_at DESC LIMIT 1").bind(userId).first();
    const tradeId = id || row?.id;
    if (!tradeId) { await sendMessage(userId, "No pending trade to approve.", null, env); return; }
    const row2 = row || await env.DB.prepare("SELECT extra_json, status FROM trades WHERE id = ? AND user_id = ?").bind(tradeId, userId).first();
    if (!row2 || row2.status !== 'pending') { await sendMessage(userId, `Trade #${tradeId} not pending.`, null, env); return; }
    const extra = JSON.parse(row2.extra_json || '{}');
    if (extra.funds_ok !== true) { await sendMessage(userId, `Insufficient funds to approve trade #${tradeId}.`, null, env); return; }
    const ok = await executeTrade(env, userId, tradeId);
    await sendMessage(userId, ok ? `Trade #${tradeId} approved and executed.` : `Failed to execute trade #${tradeId}.`, null, env);
    return;
  }
  if (text.startsWith('/reject')) {
    const id = parseInt((text.split(/\s+/)[1] || ''), 10);
    const row = await env.DB.prepare("SELECT id FROM trades WHERE user_id = ? AND status = 'pending' ORDER BY created_at DESC LIMIT 1").bind(userId).first();
    const tradeId = id || row?.id;
    if (!tradeId) { await sendMessage(userId, "No pending trade to reject.", null, env); return; }
    await env.DB.prepare("UPDATE trades SET status = 'rejected', updated_at = ? WHERE id = ? AND user_id = ?").bind(nowISO(), tradeId, userId).run();
    await sendMessage(userId, `Trade #${tradeId} rejected.`, null, env);
    return;
  }
  if (text.startsWith('/refresh')) {
    const id = parseInt((text.split(/\s+/)[1] || ''), 10);
    if (!id) { await sendMessage(userId, "Usage: /refresh <tradeId>", null, env); return; }
    await sendTradeDetailsUI(env, userId, id);
    return;
  }
  if (text.startsWith('/close')) {
    const id = parseInt((text.split(/\s+/)[1] || ''), 10);
    if (!id) { await sendMessage(userId, "Usage: /close <tradeId>", null, env); return; }
    const res = await closeTradeNow(env, userId, id);
    await sendMessage(userId, res.ok ? `Closed trade #${id}.` : `Close failed: ${res.msg}`, null, env);
    return;
  }
  if (text === '/trades') { await sendTradesListUI(env, userId, 1); return; }
  if (text === '/funds')  { const bal = await getWalletBalance(env, userId); await sendMessage(userId, `Wallet Available (USDT): ${formatMoney(bal)}`, null, env); return; }
  if (text === '/detect') {
    const s2 = await getSession(env, userId);
    if (!s2?.api_key_encrypted) { await sendMessage(userId, "No API keys saved yet.", null, env); return; }
    const apiKey = await decrypt(s2.api_key_encrypted, env);
    const apiSecret = await decrypt(s2.api_secret_encrypted, env);
    const res = await verifyApiKeys(apiKey, apiSecret, s2.exchange_name);
    if (!res.success) { await sendMessage(userId, `Detect failed: ${res.reason}`, null, env); return; }
    const detectedBal = parseFloat(String(res.data.balance || "0").replace(" USDT",""));
    const feeRate = res.data.feeRate ?? 0.001;
    const protocol = await getProtocolState(env, userId);
    if (!protocol) {
      await initProtocolDynamic(env, userId, feeRate, 0.01);
      await sendMessage(userId, `Protocol initialized.\nBalance: ${formatMoney(detectedBal)}\nFee: ${formatPercent(feeRate)}`, null, env);
    } else {
      await updateProtocolState(env, userId, { fee_rate: feeRate });
      await sendMessage(userId, `Detected.\nBalance: ${formatMoney(detectedBal)}\nFee updated: ${formatPercent(feeRate)}`, null, env);
    }
    return;
  }
  if (text.startsWith('/risk')) { await sendMessage(userId, "ACP V20 uses EV-based sizing with strict budgets; /risk is informational only.", null, env); return; }
  if (text === '/manual') { await saveSession(env, userId, { bot_mode: 'manual', status: 'active' }); await sendDashboard(env, userId, isCb ? messageId : 0); return; }
  if (text === '/auto')   { await saveSession(env, userId, { bot_mode: 'auto', status: 'active', auto_paused: 'false' }); await sendDashboard(env, userId, isCb ? messageId : 0); return; }
  if (text === '/pause')  { await saveSession(env, userId, { auto_paused: 'true' }); await sendDashboard(env, userId, isCb ? messageId : 0); return; }
  if (text === '/resume') { await saveSession(env, userId, { auto_paused: 'false' }); await sendDashboard(env, userId, isCb ? messageId : 0); return; }

  // Navigation
  if (text === "manage_trades") { await sendTradesListUI(env, userId, 1, isCb ? messageId : 0); return; }
  if (text === "action_back_to_exchange") { await saveSession(env, userId, { current_step: 'awaiting_exchange' }); await restartWizardAtExchange(env, userId, isCb ? messageId : 0); return; }
  if (text === "auto_toggle") {
    const row = await env.DB.prepare("SELECT auto_paused FROM user_sessions WHERE user_id = ?").bind(userId).first();
    const paused = (row?.auto_paused || "false") === "true";
    await saveSession(env, userId, { auto_paused: paused ? "false" : "true" });
    await sendDashboard(env, userId, isCb ? messageId : 0);
    return;
  }
  if (text === "/report" || text === "protocol_status") { await renderProtocolStatus(env, userId, isCb ? messageId : 0); return; }

  // Delegate trade-specific callbacks
  if (text.startsWith("tr_")) { await handleTradeAction(env, userId, text, isCb ? messageId : 0); return; }

  // Wizard state machine (condensed)
  let nextStep = session.current_step, updates = {};
  switch (session.current_step) {
    case "start": if (text === "/start") nextStep = "awaiting_accept"; break;
    case "awaiting_accept": if (text === "action_accept_terms") nextStep = "awaiting_mode"; break;
    case "awaiting_mode":
      if (text === "mode_manual" || text === "mode_auto") { updates.bot_mode = text === "mode_manual" ? "manual" : "auto"; nextStep = "awaiting_exchange"; }
      else if (text === "action_back_to_start") nextStep = "awaiting_accept"; break;
    case "awaiting_exchange":
      if (text.startsWith("exchange_")) {
        const exKey = text.slice("exchange_".length);
        if (SUPPORTED_EXCHANGES[exKey]) { updates.exchange_name = exKey; nextStep = "awaiting_api_key"; }
      } else if (text === "action_back_to_mode") nextStep = "awaiting_mode";
      break;
    case "awaiting_api_key":
      if (!isCb) { updates.temp_api_key = text; nextStep = "awaiting_api_secret"; }
      else if (text === "action_back_to_exchange") nextStep = "awaiting_exchange";
      break;
    case "awaiting_api_secret":
      if (!isCb) {
        const tempKey = session.temp_api_key; const exName = session.exchange_name;
        if (tempKey && exName) {
          try {
            const result = await verifyApiKeys(tempKey, text, exName);
            if (result.success) {
              updates.api_key_encrypted = await encrypt(tempKey, env);
              updates.api_secret_encrypted = await encrypt(text, env);
              updates.temp_api_key = null;
              const detectedBal = parseFloat(String(result.data.balance || "0").replace(" USDT",""));
              const feeRate = result.data.feeRate ?? 0.001;
              await initProtocolDynamic(env, userId, feeRate, 0.01);
              updates.pending_action_data = JSON.stringify({ bal: detectedBal, feeRate });
              nextStep = session.bot_mode === 'manual' ? "confirm_manual" : "confirm_auto_funds";
            } else {
              const t = `Key check failed.\nReason: ${result.reason || "Unknown error"}`;
              if (isCb && messageId) await editMessage(userId, messageId, t, [[{ text: "Try again", callback_data: "action_retry_apikey" }], [{ text: "Stop", callback_data: "action_direct_stop" }]], env);
              else await sendMessage(userId, t, [[{ text: "Try again", callback_data: "action_retry_apikey" }], [{ text: "Stop", callback_data: "action_direct_stop" }]], env);
              return;
            }
          } catch (e) {
            const t = "Key check failed.\nReason: internal error";
            if (isCb && messageId) await editMessage(userId, messageId, t, [[{ text: "Try again", callback_data: "action_retry_apikey" }], [{ text: "Stop", callback_data: "action_direct_stop" }]], env);
            else await sendMessage(userId, t, [[{ text: "Try again", callback_data: "action_retry_apikey" }], [{ text: "Stop", callback_data: "action_direct_stop" }]], env);
            return;
          }
        }
      }
      break;
    case "confirm_manual":
      if (text === "action_start_manual") {
        updates.status = 'active';
        updates.needs_protocol_onboarding = 'true';
        nextStep = "active_manual_running";
        await renderProtocolStatus(env, userId, isCb ? messageId : 0);
      } break;
    case "confirm_auto_funds":
      if (text === "action_start_auto") {
        const bal = await getWalletBalance(env, userId);
        if (bal < 16) {
          const t = `Wallet available: ${formatMoney(bal)}. Please deposit to enable auto trades.`;
          if (isCb && messageId) await editMessage(userId, messageId, t, [[{ text: "Wipe API Key & Go Back", callback_data: "action_wipe_keys" }]], env);
          else await sendMessage(userId, t, [[{ text: "Wipe API Key & Go Back", callback_data: "action_wipe_keys" }]], env);
          return;
        }
        updates.status = 'active';
        updates.auto_paused = 'false';
        updates.needs_protocol_onboarding = 'true';
        nextStep = "active_auto_running";
        await renderProtocolStatus(env, userId, isCb ? messageId : 0);
      } break;
    case "active_manual_running":
    case "active_auto_running":
      break;
  }

  let outText = "", buttons = null;
  switch (nextStep) {
    case "awaiting_accept":
      outText = welcomeText();
      buttons = [[{ text: "Accept ✅", callback_data: "action_accept_terms" }],[{ text: "Stop ⛔", callback_data: "action_stop_confirm" }]];
      break;
    case "awaiting_mode":
      outText = modeText();
      buttons = [[{ text: "Manual (approve each trade)", callback_data: "mode_manual" }],[{ text: "Fully Automatic", callback_data: "mode_auto" }],[{ text: "Back ◀️", callback_data: "action_back_to_start" },{ text: "Stop ⛔", callback_data: "action_stop_confirm" }]];
      break;
    case "awaiting_exchange":
      outText = exchangeText(); buttons = exchangeButtons(); break;
    case "awaiting_api_key":
      outText = askApiKeyText(session.exchange_name);
      buttons = [[{ text: "Back ◀️", callback_data: "action_back_to_exchange" }],[{ text: "Stop ⛔", callback_data: "action_stop_confirm" }]];
      break;
    case "awaiting_api_secret":
      outText = askApiSecretText();
      buttons = [[{ text: "Back ◀️", callback_data: "action_back_to_exchange" }],[{ text: "Stop ⛔", callback_data: "action_stop_confirm" }]];
      break;
    case "confirm_manual": {
      const { bal, feeRate } = JSON.parse(updates.pending_action_data || session.pending_action_data || '{}');
      outText = confirmManualTextB(bal ?? 0, feeRate ?? 0.001);
      buttons = [[{ text: "Start Manual ▶️", callback_data: "action_start_manual" }],[{ text: "Wipe API Key & Go Back", callback_data: "action_wipe_keys" }]];
      break;
    }
    case "confirm_auto_funds": {
      const { bal, feeRate } = JSON.parse(updates.pending_action_data || session.pending_action_data || '{}');
      outText = confirmAutoTextB(bal ?? 0, feeRate ?? 0.001);
      buttons = [[{ text: "Yes, Start Auto 🚀", callback_data: "action_start_auto" }],[{ text: "Wipe API Key & Go Back", callback_data: "action_wipe_keys" }]];
      break;
    }
    default: /* no text */ ;
  }

  updates.current_step = nextStep;
  await saveSession(env, userId, updates);
  if (outText) {
    if (isCb && messageId) await editMessage(userId, messageId, outText, buttons, env);
    else await sendMessage(userId, outText, buttons, env);
  }
}

/* ---------- Cron with fan out fallback (never needs manual calls) ---------- */
async function runCron(env) {
  const lockValue = Date.now().toString();
  const existing = await env.DB.prepare(
    "SELECT value FROM kv_state WHERE key = ? AND CAST(value AS INTEGER) > ?"
  ).bind(CRON_LOCK_KEY, Date.now() - CRON_LOCK_TTL * 1000).first();
  if (existing) { console.log('Cron already running, skipping'); return; }

  await env.DB.prepare("INSERT OR REPLACE INTO kv_state (key, value) VALUES (?, ?)").bind(CRON_LOCK_KEY, lockValue).run();

  try {
    // Active users
    const activeUsers = await env.DB
      .prepare("SELECT user_id FROM user_sessions WHERE status IN ('active','halted') AND bot_mode IN ('manual','auto')")
      .all();

    // Config (trim to avoid stray whitespace)
    const base = String(env.SELF_BASE_URL || "").trim().replace(/\/+$/, "");
    const token = String(env.TASK_TOKEN || "").trim();
    const haveTaskEndpoint = !!base && !!token;
    const forceInline = String(env.DEBUG_FORCE_INLINE || "0") === "1";

    for (const u of (activeUsers.results || [])) {
      const uid = u.user_id;
      const budgetMs = Number(env.USER_CPU_BUDGET_MS || 20000);

      if (haveTaskEndpoint && !forceInline) {
        try {
          const url = `${base}/task/process-user?uid=${encodeURIComponent(uid)}`;
          const r = await safeFetch(url, {
            method: "POST",
            headers: { "Authorization": `Bearer ${token}` }
          }, 6000);

          if (!r || !r.ok) {
            console.warn("[cron] fanout non-OK; fallback inline", { uid, status: r?.status });
            await processUser(env, uid, createCpuBudget(budgetMs));
          }
        } catch (e) {
          console.error("[cron] fanout error; fallback inline", { uid, err: e?.message || e });
          await processUser(env, uid, createCpuBudget(budgetMs));
        }
      } else {
        // Inline mode
        await processUser(env, uid, createCpuBudget(budgetMs));
      }
    }

    // No local idea generation; GitHub pushes to /signals/push
  } catch (e) {
    console.error("runCron error:", e);
  } finally {
    await env.DB.prepare("DELETE FROM kv_state WHERE key = ? AND value = ?").bind(CRON_LOCK_KEY, lockValue).run();
  }
}

/* ---------- Worker Export ---------- */
export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      // Per-user task endpoint (self-fan-out), token-protected
      if (url.pathname === "/task/process-user" && request.method === "POST") {
        const auth = request.headers.get("Authorization") || "";
        const token = String(env.TASK_TOKEN || "").trim();
        const good = auth.startsWith("Bearer ") && auth.split(" ")[1] === token;
        if (!good) return new Response("Unauthorized", { status: 401 });

        const uid = url.searchParams.get("uid");
        if (!uid) return new Response("Missing uid", { status: 400 });

        try {
          const ms = Number(env.TASK_WALL_BUDGET_MS || 12000);
          await processUser(env, uid, createCpuBudget(ms));
          return new Response(JSON.stringify({ ok: true, uid }), { headers: { "Content-Type": "application/json" } });
        } catch (e) {
          console.error("task/process-user error:", e);
          return new Response(JSON.stringify({ ok: false, error: e.message || "fail" }), {
            status: 500, headers: { "Content-Type": "application/json" }
          });
        }
      }

      if (url.pathname === "/health") {
        return new Response(JSON.stringify({ ok: true, time: nowISO() }), { headers: { "Content-Type": "application/json" } });
      }

      if (url.pathname === "/telegram" && request.method === "POST") {
        const update = await request.json();
        await handleTelegramUpdate(update, env, ctx);
        return new Response("OK");
      }

      if (url.pathname === "/signals/push" && request.method === "POST") {
        // Optional bearer protection (set PUSH_TOKEN to enforce)
        const auth = request.headers.get("Authorization") || "";
        const expected = String(env.PUSH_TOKEN || "").trim();
        if (expected && auth !== `Bearer ${expected}`) {
          console.warn("[push] 401 unauthorized");
          return new Response("Unauthorized", { status: 401 });
        }

        let ideas;
        try {
          ideas = await request.json();
        } catch (e) {
          console.error("[push] bad json", e);
          return new Response("Bad JSON", { status: 400 });
        }

        // Visibility in tail/logs
        const len = Array.isArray(ideas?.ideas) ? ideas.ideas.length : 0;
        console.log(`[push] received ${len} ideas, origin=${ideas?.meta?.origin}, ts=${ideas?.ts || nowISO()}`);

        try {
          await env.DB.prepare("INSERT INTO ideas (ts, mode, ideas_json) VALUES (?, ?, ?)")
            .bind(ideas.ts || nowISO(), ideas.mode || 'normal', JSON.stringify(ideas)).run();
          console.log("[push] stored ideas snapshot in D1");
          return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });
        } catch (e) {
          console.error("[push] D1 insert error:", e);
          return new Response("DB error", { status: 500 });
        }
      }

      return new Response("Not found", { status: 404 });
    } catch (e) {
      console.error("fetch error:", e);
      return new Response("Error", { status: 500 });
    }
  },

  async scheduled(event, env, ctx) {
    await runCron(env);
  },
};

