/* ======================================================================
   Unified Worker — Telegram UX (Worker-1 style) + Gist Execution Truth
   SECTION 1/7 — Constants, Utils, Crypto, DB, Telegram, Gist helpers
   ====================================================================== */

/* ---------- Constants (Exchanges) ---------- */
const SUPPORTED_EXCHANGES = {
  crypto_parrot: { label: "Crypto Parrot (Demo)", kind: "demoParrot", hasOrders: true },

  // Spot (Binance-like REST shape; includes MEXC)
  mexc:    { label: "MEXC",    kind: "binanceLike", baseUrl: "https://api.mexc.com",    accountPath: "/api/v3/account", apiKeyHeader: "X-MEXC-APIKEY",  defaultQuery: "",                hasOrders: true },
  binance: { label: "Binance", kind: "binanceLike", baseUrl: "https://api.binance.com", accountPath: "/api/v3/account", apiKeyHeader: "X-MBX-APIKEY",   defaultQuery: "recvWindow=5000", hasOrders: true },

  // Others (legacy spot)
  lbank:   { label: "LBank",   kind: "lbankV2",     baseUrl: "https://api.lbkex.com", hasOrders: true },
  coinex:  { label: "CoinEx",  kind: "coinexV2",    baseUrl: "https://api.coinex.com", hasOrders: true },

  // Futures / Margin (USDT-M)
  binance_futures: { label: "Binance Futures (USDT-M)", kind: "binanceFuturesUSDT", baseUrl: "https://fapi.binance.com", apiKeyHeader: "X-MBX-APIKEY", hasOrders: true },

  // Read-only
  bybit:   { label: "Bybit",   kind: "bybitV5",     baseUrl: "https://api.bybit.com", hasOrders: false },
  kraken:  { label: "Kraken",  kind: "krakenV0",    baseUrl: "https://api.kraken.com", hasOrders: false },
  gate:    { label: "Gate",    kind: "gateV4",      baseUrl: "https://api.gateio.ws", hasOrders: false },
  huobi:   { label: "Huobi",   kind: "huobiV1",     baseHost: "api.huobi.pro", scheme: "https", hasOrders: false }
};

/* ---------- Brain & Risk Parameters ---------- */
const STRICT_RRR = 2.0;
const ACP_FRAC = 0.80;
const PER_TRADE_CAP_FRAC = 0.10;
const DAILY_OPEN_RISK_CAP_FRAC = 0.30;

/* ---------- System-level ---------- */
const CRON_LOCK_KEY = "cron_running";
const CRON_LOCK_TTL = 55; // seconds

/* ---------- Feature toggles (env) ---------- */
function envFlag(env, key, def = "0") { return String(env?.[key] ?? def) === "1"; }
function envNum(env, key, def) { const v = Number(env?.[key]); return Number.isFinite(v) ? v : def; }
function ENABLE_TTL(env) { return envFlag(env, "ENABLE_TTL_MARKET_CLOSE", "1"); }
function ENABLE_MFE_MAE(env) { return envFlag(env, "MFE_MAE_ENABLE", "1"); }

/* ---------- Tiny utils (Worker-1 style formatters + side emoji) ---------- */
const te = new TextEncoder();
const td = new TextDecoder();
const b64url = {
  encode: (buf) => btoa(String.fromCharCode(...new Uint8Array(buf))).replace(/\+/g, "-").replace(/\//g, "_"),
  decode: (str) => Uint8Array.from(atob(str.replace(/-/g, "+").replace(/_/g, "/")), (c) => c.charCodeAt(0)),
};
const nowISO = () => new Date().toISOString();
const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

function formatMoney(amount) { return `$${Number(amount || 0).toFixed(8)}`; }
function formatPercent(decimal) { return `${(Number(decimal || 0) * 100).toFixed(2)}%`; }
function formatDurationShort(ms) {
  if (!isFinite(ms) || ms <= 0) return "0m";
  const totalMin = Math.round(ms / 60000);
  const h = Math.floor(totalMin / 60);
  const m = totalMin % 60;
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}
function sideEmoji(sideOrDir) {
  const s = String(sideOrDir || "").toLowerCase();
  return (s === "sell" || s === "short") ? "🔴" : "🟢";
}

/* ---------- LRU+TTL cache ---------- */
class TTLLRU {
  constructor(capacity = 512, ttlMs = 60000) {
    this.cap = Math.max(1, capacity);
    this.ttl = Math.max(1, ttlMs);
    this.map = new Map();
  }
  get(key) {
    const ent = this.map.get(key);
    if (!ent) return undefined;
    if (ent.e <= Date.now()) { this.map.delete(key); return undefined; }
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
const DEFAULT_FETCH_TIMEOUT_MS = 4000;
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

/* ---------- Telegram helpers ---------- */
async function answerCallbackQuery(env, id) {
  if (!env.TELEGRAM_BOT_TOKEN) return;
  const url = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/answerCallbackQuery`;
  await safeFetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ callback_query_id: id }) }).catch(() => {});
}
async function sendMessage(chatId, text, buttons, env) {
  if (!env.TELEGRAM_BOT_TOKEN) return;
  const url = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`;
  const body = { chat_id: chatId, text, parse_mode: "Markdown" };
  if (buttons && buttons.length) body.reply_markup = { inline_keyboard: buttons };
  const r = await safeFetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }, 8000);
  if (!r.ok) console.error("sendMessage error:", await r.text());
}
async function editMessage(chatId, messageId, newText, buttons, env) {
  if (!env.TELEGRAM_BOT_TOKEN) return;
  const url = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/editMessageText`;
  const body = { chat_id: chatId, message_id: messageId, text: newText, parse_mode: "Markdown" };
  if (buttons && buttons.length) body.reply_markup = { inline_keyboard: buttons };
  else body.reply_markup = { inline_keyboard: [] };
  const r = await safeFetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }, 8000);
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

/* ---------- DB helpers (Telegram wizard + KV) ---------- */
async function getSession(env, userId) { return env.DB.prepare("SELECT * FROM user_sessions WHERE user_id = ?").bind(userId).first(); }
async function createSession(env, userId) {
  await env.DB.prepare("INSERT INTO user_sessions (user_id, current_step, last_interaction_ts, status, bot_mode, auto_paused) VALUES (?, 'start', ?, 'initializing', 'manual', 'false')")
    .bind(userId, nowISO()).run();
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

/* ---------- Global mode + approvals (Manual gate for gist ideas) ---------- */
async function setGlobalMode(env, mode) { await kvSet(env, "global_mode", mode === "auto" ? "auto" : "manual"); }
async function getGlobalMode(env) { return (await kvGet(env, "global_mode")) || "manual"; }
function approvalKey(cid) { return `approval_cid_${cid}`; }
async function setIdeaApproval(env, cid, verdict) { await kvSet(env, approvalKey(cid), verdict); }
async function getIdeaApproval(env, cid) { return (await kvGet(env, approvalKey(cid))) || "pending"; }

/* ---------- Protocol state (for UI summaries; fees tracked in gist) ---------- */
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

/* ======================================================================
   Gist helpers (state.json) — Worker is execution truth writer
   ====================================================================== */
function gistEnabled(env) {
  return Boolean(String(env.GIST_ID || "").trim() && String(env.GIST_TOKEN || "").trim());
}
function baseFromSymbolFull(symbolFull) {
  const full = String(symbolFull || "").toUpperCase();
  const quotes = ["USDT","USDC","USD"];
  for (const q of quotes) if (full.endsWith(q)) return full.slice(0, -q.length);
  return full.replace(/USDT|USDC|USD$/,'');
}
function normalizeGistState(s) {
  const state = s && typeof s === "object" ? s : {};
  if (!Array.isArray(state.pending)) state.pending = [];
  if (!Array.isArray(state.closed)) state.closed = [];
  if (!Array.isArray(state.equity)) state.equity = [];
  if (!state.sym_stats_real) state.sym_stats_real = {};
  if (!Number.isFinite(Number(state.lastReconcileTs))) state.lastReconcileTs = 0;
  if (!Number.isFinite(Number(state.loop_heartbeat_ts))) state.loop_heartbeat_ts = 0;
  if (!Number.isFinite(Number(state.version_ts))) state.version_ts = Date.now();
  return state;
}
async function loadGistState(env) {
  if (!gistEnabled(env)) return { state: normalizeGistState({}), rev: null, etag: null };
  const id = String(env.GIST_ID).trim();
  const token = String(env.GIST_TOKEN).trim();
  const r = await safeFetch(`https://api.github.com/gists/${id}`, {
    headers: { Authorization: `Bearer ${token}`, "User-Agent":"worker", "Accept":"application/vnd.github+json" }
  }, 12000);
  const etag = r.headers.get("ETag") || null;
  const g = await r.json().catch(()=> ({}));
  const content = g?.files?.["state.json"]?.content || "";
  let parsed = {};
  try { parsed = content ? JSON.parse(content) : {}; } catch { parsed = {}; }
  const state = normalizeGistState(parsed);
  const rev = g?.history?.[0]?.version || null;
  return { state, rev, etag };
}
function computeSymStatsRealFromClosed(closedArr) {
  const agg = {};
  for (const c of (closedArr || [])) {
    const base = baseFromSymbolFull(c?.symbolFull || "");
    if (!base) continue;
    const pnl = Number(c?.pnl_bps || 0);
    if (!agg[base]) agg[base] = { n: 0, wins: 0, pnl_sum: 0 };
    agg[base].n += 1;
    if (pnl > 0) agg[base].wins += 1;
    agg[base].pnl_sum += pnl;
  }
  return agg;
}
function mergeGistState(remote, local) {
  const out = normalizeGistState({ ...remote });

  // pending by CID
  const byCid = new Map();
  for (const p of (remote.pending || [])) if (p?.client_order_id) byCid.set(p.client_order_id, p);
  for (const p of (local.pending || [])) {
    const cid = p?.client_order_id;
    if (!cid) continue;
    const prev = byCid.get(cid);
    byCid.set(cid, { ...(prev||{}), ...p });
  }
  out.pending = [...byCid.values()];

  // closed append-union (key by cid+ts_exit_ms)
  const seen = new Set();
  out.closed = [];
  const pushUnique = (arr) => {
    for (const c of (arr||[])) {
      const k = `${c?.trade_details?.client_order_id||""}|${c?.ts_exit_ms||""}`;
      if (seen.has(k)) continue;
      seen.add(k); out.closed.push(c);
    }
  };
  pushUnique(remote.closed); pushUnique(local.closed);

  // equity union (ts_ms+pnl key)
  const eqSeen = new Set();
  out.equity = [];
  const pushEQ = (arr)=>{ for(const e of (arr||[])){ const k=`${e?.ts_ms||""}|${e?.pnl_bps||""}`; if(eqSeen.has(k)) continue; eqSeen.add(k); out.equity.push(e);} };
  pushEQ(remote.equity); pushEQ(local.equity);

  // recompute sym_stats_real from unioned closed
  out.sym_stats_real = computeSymStatsRealFromClosed(out.closed);

  // fees carry (prefer local)
  out.fees = (local && local.fees) ? local.fees : (remote && remote.fees ? remote.fees : undefined);

  out.lastReconcileTs = Math.max(Number(remote.lastReconcileTs||0), Number(local.lastReconcileTs||0));
  out.version_ts = Date.now();
  return out;
}
async function saveGistState(env, state, etag = null, attempt = 1) {
  if (!gistEnabled(env)) return false;
  const id = String(env.GIST_ID).trim();
  const token = String(env.GIST_TOKEN).trim();
  state.version_ts = Date.now();

  const body = { files: { "state.json": { content: JSON.stringify(state, null, 2) } } };
  const headers = { Authorization: `Bearer ${token}`, "Content-Type": "application/json", "Accept":"application/vnd.github+json" };
  if (etag) headers["If-Match"] = etag;

  const r = await safeFetch(`https://api.github.com/gists/${id}`, { method: "PATCH", headers, body: JSON.stringify(body) }, 15000);

  if (r.status === 412 && attempt < 2) {
    const latest = await loadGistState(env);
    const merged = mergeGistState(latest.state, state);
    return saveGistState(env, merged, latest.etag, attempt + 1);
  }
  return r.ok;
}

/* Update helpers — used by gist trade open/close paths */
function updatePendingOpenByCID(state, cid, updates) {
  if (!cid) return false;
  const idx = (state.pending || []).findIndex(p => p?.client_order_id === cid);
  if (idx === -1) return false;
  const cur = state.pending[idx] || {};
  state.pending[idx] = { ...cur, ...updates, exits: { ...(cur.exits||{}), ...(updates?.exits||{}) } };
  return true;
}
function removePendingByCID(state, cid) {
  if (!cid) return false;
  const before = state.pending?.length || 0;
  state.pending = (state.pending || []).filter(p => p?.client_order_id !== cid);
  return (state.pending?.length || 0) < before;
}
function appendClosed(state, row) {
  if (!Array.isArray(state.closed)) state.closed = [];
  state.closed.push(row);
  const base = baseFromSymbolFull(row?.symbolFull || "");
  if (base) {
    if (!state.sym_stats_real) state.sym_stats_real = {};
    const s = state.sym_stats_real[base] || { n: 0, wins: 0, pnl_sum: 0 };
    const pnl = Number(row?.pnl_bps || 0);
    s.n += 1; if (pnl > 0) s.wins += 1; s.pnl_sum += pnl;
    state.sym_stats_real[base] = s;
  }
}
function bumpSymStatsReal(state, base, pnl_bps) {
  if (!state.sym_stats_real) state.sym_stats_real = {};
  const s = state.sym_stats_real[base] || { n: 0, wins: 0, pnl_sum: 0 };
  s.n++;
  if ((pnl_bps || 0) > 0) s.wins++;
  s.pnl_sum += (pnl_bps || 0);
  state.sym_stats_real[base] = s;
}
/* ======================================================================
   SECTION 2/7 — Market Data, Orderbook, and Klines helpers (incl. MFE/MAE)
   (Prefer Binance/MEXC endpoints; adds fetchKlinesRange + computeMFE_MAE)
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

  const pair = `${base}USDT`;
  const bases = [
    SUPPORTED_EXCHANGES.binance.baseUrl,
    SUPPORTED_EXCHANGES.mexc.baseUrl,
  ];

  const tryJson = async (url) => {
    for (let i = 0; i < 2; i++) {
      try {
        const r = await safeFetch(url, {}, 2500).catch(() => null);
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

  for (const b of bases) {
    let v = await tryJson(`${b}/api/v3/ticker/price?symbol=${pair}`);
    if (v > 0) { priceCache.set(base, v); return v; }

    v = await tryJson(`${b}/api/v3/ticker/24hr?symbol=${pair}`);
    if (v > 0) { priceCache.set(base, v); return v; }

    v = await tryJson(`${b}/api/v3/klines?symbol=${pair}&interval=1m&limit=1`);
    if (v > 0) { priceCache.set(base, v); return v; }
  }

  return 0;
}

/* ATR-like stop percentage (sticky cache; MIN_STOP_PCT constant respected) */
async function calculateStopPercent(symbol) {
  const base = (symbol || "").toUpperCase();
  if (!base) return 0.01;

  const hit = stopPctCache.get(base);
  if (typeof hit !== "undefined") return hit;

  const pair = `${base}USDT`;
  const tryOne = async (baseUrl, interval, limit) => {
    const url = `${baseUrl}/api/v3/klines?symbol=${pair}&interval=${interval}&limit=${limit}`;
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

  const pair = `${base}USDT`;
  const tryFetch = async (baseUrl) => {
    const depthUrl = `${baseUrl}/api/v3/depth?symbol=${pair}&limit=20`;
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
  const pair = `${base}USDT`;
  const urls = [
    `${SUPPORTED_EXCHANGES.mexc.baseUrl}/api/v3/klines?symbol=${pair}&interval=${interval}&limit=${limit}`,
    `${SUPPORTED_EXCHANGES.binance.baseUrl}/api/v3/klines?symbol=${pair}&interval=${interval}&limit=${limit}`
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

/* Klines with time window (for MFE/MAE) */
async function fetchKlinesRange(baseSymbol, interval = "1m", startTime = 0, endTime = 0) {
  const base = (baseSymbol || "").toUpperCase();
  const pair = `${base}USDT`;
  const params = [];
  if (Number.isFinite(Number(startTime)) && startTime > 0) params.push(`startTime=${Math.floor(Number(startTime))}`);
  if (Number.isFinite(Number(endTime)) && endTime > 0) params.push(`endTime=${Math.floor(Number(endTime))}`);
  const qs = params.length ? `&${params.join("&")}` : "";

  const urls = [
    `${SUPPORTED_EXCHANGES.mexc.baseUrl}/api/v3/klines?symbol=${pair}&interval=${interval}${qs}`,
    `${SUPPORTED_EXCHANGES.binance.baseUrl}/api/v3/klines?symbol=${pair}&interval=${interval}${qs}`
  ];
  for (const url of urls) {
    try {
      const r = await safeFetch(url, {}, 6000);
      const j = await r.json();
      if (Array.isArray(j) && j.length > 0) return j;
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

/* ---------- MFE/MAE computation ---------- */
/*
  computeMFE_MAE(entryPrice, side, klines1m) →
    - klines1m: array of [openTime, open, high, low, close, volume, closeTime, ...]
    - Long:
        mfe_bps = max(high/entry - 1) * 10000 (≥0)
        mae_bps = min(low/entry - 1) * 10000 (≤0)
    - Short:
        mfe_bps = max(entry/low - 1) * 10000 (≥0)
        mae_bps = -max(high/entry - 1) * 10000 (≤0)
*/
function computeMFE_MAE(entryPrice, side, klines1m) {
  const e = Number(entryPrice || 0);
  if (!(e > 0) || !Array.isArray(klines1m) || klines1m.length === 0) return { mfe_bps: 0, mae_bps: 0 };

  const isShort = String(side || 'long').toLowerCase() === 'short';
  let maxHigh = -Infinity;
  let minLow  = +Infinity;

  for (const k of klines1m) {
    const hi = Number(k[2] || 0);
    const lo = Number(k[3] || 0);
    if (isFinite(hi) && hi > maxHigh) maxHigh = hi;
    if (isFinite(lo) && lo < minLow)  minLow  = lo;
  }

  if (!isFinite(maxHigh) || !isFinite(minLow)) return { mfe_bps: 0, mae_bps: 0 };

  if (!isShort) {
    const mfe = (maxHigh / e) - 1;
    const mae = (minLow  / e) - 1;
    return { mfe_bps: Math.round(Math.max(0, mfe) * 10000), mae_bps: Math.round(Math.min(0, mae) * 10000) };
  } else {
    const mfe = (e / Math.max(1e-12, minLow)) - 1;  // favorable for shorts when low goes down
    const mae = (maxHigh / e) - 1;                   // adverse for shorts when high goes up
    return { mfe_bps: Math.round(Math.max(0, mfe) * 10000), mae_bps: Math.round(-Math.max(0, mae) * 10000) };
  }
}
/* ======================================================================
   SECTION 3/7 — Wallet/Equity, Cooldowns, HSE/Kelly, Exposure,
                  Logging, TCR pacing (KV-based)
   ====================================================================== */

/* ---------- HSE config (microstructure cost model) ---------- */
const HSE_CFG = {
  AF: 1.0, gamma: 0.5, w_cost: 1.0, w_vol: 1.0, w_rs: 1.0, w_es: 1.0,
  alpha0: -0.4, alpha1: 1.0,
  BAS_avg: 0.0005, OBD_avg: 200000, MV_avg: 0.01, RS_avg: 1.0, ES_avg: 1.0,
  sSlipStarBps: 3.0
};

/* ---------- Wallet & Equity ---------- */
async function getWalletBalance(env, userId) {
  // Demo mode: fixed equity + PnL from DB, if any
  const session = await getSession(env, userId);
  if (session?.exchange_name === "crypto_parrot") {
    let eq = 10000;
    try {
      const sumClosed = await env.DB
        .prepare("SELECT COALESCE(SUM(realized_pnl),0) AS s FROM trades WHERE user_id = ? AND status = 'closed'")
        .bind(userId).first();
      eq += Number(sumClosed?.s || 0);
    } catch {}
    return eq;
  }

  // If user has saved API keys, detect via exchange
  if (session?.api_key_encrypted && session?.api_secret_encrypted && session?.exchange_name) {
    try {
      const apiKey = await decrypt(session.api_key_encrypted, env);
      const apiSecret = await decrypt(session.api_secret_encrypted, env);
      const res = await verifyApiKeys(apiKey, apiSecret, session.exchange_name);
      if (res?.success && res.data?.balance) {
        return parseFloat(String(res.data.balance).replace(" USDT",""));
      }
    } catch (e) {
      console.error("getWalletBalance (user) error:", e?.message || e);
    }
  }

  // Fallback: try global exec (env-based)
  try {
    const exName = (env.GLOBAL_EXCHANGE || env.EXCHANGE || "mexc").toLowerCase();
    const ex = SUPPORTED_EXCHANGES[exName];
    if (ex?.hasOrders) {
      const apiKey = env.MEXC_API_KEY || env.BINANCE_API_KEY || env.BINANCE_FUTURES_API_KEY || "";
      const apiSecret = env.MEXC_SECRET_KEY || env.BINANCE_SECRET_KEY || env.BINANCE_FUTURES_SECRET_KEY || "";
      if (apiKey && apiSecret) {
        const res = await verifyApiKeys(apiKey, apiSecret, exName);
        if (res?.success && res.data?.balance) {
          return parseFloat(String(res.data.balance).replace(" USDT",""));
        }
      }
    }
  } catch (e) {
    console.error("getWalletBalance (global) error:", e?.message || e);
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
  return Math.max(0, Math.min(1, peak > 0 ? (peak - tc) / peak : 0));
}

/* ---------- Exposure (Gist-first; DB fallback) ---------- */
async function getOpenPositionsCount(env, userId) {
  // Prefer gist pending ("open" items)
  try {
    const { state } = await loadGistState(env);
    const open = (state.pending || []).filter(p => String(p.status||"").toLowerCase() === "open").length;
    if (Number.isFinite(open)) return open;
  } catch {}
  // Fallback DB
  try {
    const row = await env.DB.prepare("SELECT COUNT(*) as c FROM trades WHERE user_id = ? AND status = 'open'").bind(userId).first();
    return Number(row?.c || 0);
  } catch { return 0; }
}

/* ---------- Notional (DB fallback for legacy UI) ---------- */
async function getOpenNotional(env, userId) {
  // If you mirror open sizes in DB, compute; else 0
  try {
    const open = await env.DB.prepare("SELECT qty, entry_price FROM trades WHERE user_id = ? AND status = 'open'").bind(userId).all();
    let total = 0;
    for (const r of (open.results || [])) {
      const qty = Number(r.qty || 0);
      const entry = Number(r.entry_price || 0);
      if (isFinite(qty) && isFinite(entry)) total += qty * entry;
    }
    const pend = await env.DB.prepare("SELECT extra_json FROM trades WHERE user_id = ? AND status = 'pending'").bind(userId).all();
    for (const r of (pend.results || [])) {
      try {
        const ex = JSON.parse(r.extra_json || "{}");
        const q = Number(ex.quote_size || 0);
        if (isFinite(q)) total += q;
      } catch {}
    }
    return total;
  } catch { return 0; }
}

/* ---------- Cooldowns (KV-based, per-user per-symbol) ---------- */
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

/* ---------- ACP V20 SQS helpers ---------- */
function pLCBFromSQS(SQS) {
  const p = 0.25 + 0.5 * Math.max(0, Math.min(1, SQS));
  return Math.max(0.0, Math.min(1.0, p));
}
function SQSfromScore(score) {
  const s = Number(score) || 0;
  const z = Math.min(1, Math.max(0, s / 100));
  return Math.min(1, Math.max(0, Math.sqrt(z)));
}

/* ---------- Microstructure cost and Kelly ---------- */
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

  const pH = 1 / (1 + Math.exp(-(HSE_CFG.alpha0 + HSE_CFG.alpha1 * Math.log(Math.max(HSE_raw, 1e-12)))));

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
  const f_k = Math.max(0, Math.min(1, EV_R / Math.max(Var_R, EPS_VARIANCE)));
  return { f_k, Var_R };
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
    console.error("logEvent DB error:", e?.message || e);
  }

  if (env.SHEETS_WEBHOOK_URL) {
    try {
      await safeFetch(env.SHEETS_WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": `Bearer ${env.SHEETS_WEBHOOK_TOKEN || ''}` },
        body
      }, 6000);
    } catch (e) {
      console.error("Sheets webhook error:", e?.message || e);
    }
  }
}

/* ---------- TCR pacing counters (KV) ---------- */
function utcDayKey(d = new Date()) {
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`;
}
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
    roll.last7.push({ d: curDate, c: yCount });
    if (roll.last7.length > 7) roll.last7 = roll.last7.slice(-7);

    await kvSet(env, kRoll, JSON.stringify(roll));
    await kvSet(env, kDate, today);
    await kvSet(env, kCount, 0);
  }
}
async function bumpTradeCounters(env, userId, delta = 1) {
  await ensureDayRoll(env, userId);
  const kCount = `tcr_today_count_${userId}`;
  const cur = Number(await kvGet(env, kCount) || 0);
  await kvSet(env, kCount, cur + (isFinite(delta) ? delta : 1));
}
async function getPacingCounters(env, userId) {
  await ensureDayRoll(env, userId);
  const kCount = `tcr_today_count_${userId}`;
  const kRoll = `tcr_7d_roll_${userId}`;
  const todayCount = Number(await kvGet(env, kCount) || 0);
  let roll;
  try { roll = JSON.parse((await kvGet(env, kRoll)) || '{"last7":[]}'); } catch { roll = { last7: [] }; }
  const last7 = Array.isArray(roll.last7) ? roll.last7 : [];
  const last7Counts = last7.slice(-6).map(x => Number(x?.c || 0));
  return { todayCount, last7Counts };
}

/* ---------- Dynamic SQS gate per user ---------- */
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
  return Math.max(0.25, Math.min(0.50, (Number(baseGate) || 0.30) + adj));
}
async function computeUserSqsGate(env, userId) {
  const baseGate = Number(env?.SQS_MIN_GATE ?? 0.30);
  const explicitTarget = Number(env?.TCR_TARGET_TRADES || 0);

  const { todayCount, last7Counts } = await getPacingCounters(env, userId);
  const sum7 = todayCount + last7Counts.reduce((a, b) => a + b, 0);
  const avg7 = sum7 / 7;
  const target = explicitTarget > 0 ? explicitTarget : Math.max(1, Math.round(avg7));

  const openedSoFar = todayCount;
  return computeDynamicSqsGate(baseGate, target, openedSoFar);
}

/* ======================================================================
   SECTION 4/7 — Exchange verification, signing helpers, orders & routing
   (ClientOrderId threading; spot/futures list APIs; TP/SL/TTL exits; OCO)
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
    for (let i2 = 0; i2 < s.length; i2++) tail[i2 >> 2] |= s.charCodeAt(i2) << ((i2 % 4) << 3);
    tail[i2 >> 2] |= 0x80 << ((i2 % 4) << 3);
    if (i2 > 55) { md5cycle(state, tail); for (i2 = 0; i2 < 16; i2++) tail[i2] = 0; }
    const tmp = n * 8; tail[14] = tmp & 0xffffffff; tail[15] = (tmp / 0x100000000) | 0;
    md5cycle(state, tail); return state;
  }
  function md5blk(s) { const md5blks = new Array(16); for (let i = 0; i < 64; i += 4) md5blks[i >> 2] = s.charCodeAt(i) + (s.charCodeAt(i+1)<<8) + (s.charCodeAt(i+2)<<16) + (s.charCodeAt(i+3)<<24); return md5blks; }
  function md5cycle(x, k) {
    let [a,b,c,d] = x;
    a = ff(a,b,c,d,k[0],7,-680876936); d = ff(d,a,b,c,d,k[1],12,-389564586); c = ff(c,d,a,b,k[2],17,606105819); b = ff(b,c,d,a,k[3],22,-1044525330);
    a = ff(a,b,c,d,k[4],7,1804603682); d = ff(d,a,b,c,d,k[5],12,-40341101); c = ff(c,d,a,b,k[6],17,-1473231341); b = ff(b,c,d,a,k[7],22,-45705983);
    a = ff(a,b,c,d,k[8],7,1770035416); d = ff(d,a,b,c,d,k[9],12,-1958414417); c = ff(c,d,a,b,k[10],17,-42063); b = ff(b,c,d,a,k[11],22,-1990404162);
    a = ff(a,b,c,d,k[12],7,1804603682); d = ff(d,a,b,c,d,k[13],12,-40341101); c = ff(c,d,a,b,k[14],17,-1502002290); b = ff(b,c,d,a,k[15],22,1236535329);
    a = gg(a,b,c,d,k[1],5,-165796510); d = gg(d,a,b,c,d,k[6],9,-1069501632); c = gg(c,d,a,b,k[11],14,643717713); b = gg(b,c,d,a,k[0],20,-373897302);
    a = gg(a,b,c,d,k[5],5,-701558691); d = gg(d,a,b,c,d,k[10],9,38016083); c = gg(c,d,a,b,k[15],14,-660478335); b = gg(b,c,d,a,k[4],20,-405537848);
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
    if (c) return Number(c.availableToWithdraw ?? c.walletBalance ?? c.equity ?? "0").toFixed(2);
  }
  const spot = data?.result?.balances || data?.result?.spot || [];
  if (Array.isArray(spot)) {
    const c = spot.find((x) => (x.coin || x.asset || "").toUpperCase() === "USDT");
    if (c) return Number(c.free ?? c.available ?? "0").toFixed(2);
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
  const canonical = sorted.map((k) => `${k}=${encodeURIComponent(String(params[k]))}`).join("&");
  const toSign = `${method}\n${host}\n${path}\n${canonical}`;
  const sig = await hmacB64Str(apiSecret, toSign, "SHA-256");
  const finalQuery = `${canonical}&Signature=${encodeURIComponent(sig)}`;
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
  const params = { api_key: apiKey, symbol: "usdt_usdt", timestamp: ts }; // symbol ignored
  const paramStr = Object.entries(params).sort(([a],[b])=>a.localeCompare(b)).map(([k, v]) => `${k}=${v}`).join("&");
  const signStr = paramStr + `&secret_key=${apiSecret}`;
  const sign = md5Hex(signStr).toUpperCase();
  const body = paramStr + `&sign=${sign}`;
  const url = `${ex.baseUrl}${path}`;
  const res = await safeFetch(url, { method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body }, 6000);
  const data = await res.json().catch(()=>({}));
  if (!res.ok || data?.result === false) throw new Error(data?.error_code || data?.msg || `HTTP ${res.status}`);
  return { orderId: data.order_id, executedQty: 0, avgPrice: 0, status: "NEW" };
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

/* ---------- Spot/Futures order & trade listings ---------- */
async function listAllOrdersBinanceLike(ex, apiKey, apiSecret, symbolBase, startTime = undefined, limit = 1000) {
  const endpoint = "/api/v3/allOrders";
  const params = {
    symbol: `${symbolBase.toUpperCase()}USDT`,
    limit: Math.max(1, Math.min(1000, Number(limit) || 1000)),
    timestamp: Date.now()
  };
  if (Number.isFinite(Number(startTime))) params.startTime = Number(startTime);
  if (ex.defaultQuery) {
    const defaults = new URLSearchParams(ex.defaultQuery);
    for (const [k,v] of defaults) params[k] = v;
  }
  const queryString = Object.entries(params).map(([k,v]) => `${k}=${encodeURIComponent(v)}`).join("&");
  const signature = await hmacHexStr(apiSecret, queryString);
  const url = `${ex.baseUrl}${endpoint}?${queryString}&signature=${signature}`;
  const r = await safeFetch(url, { headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const j = await r.json().catch(()=>([]));
  if (!r.ok) return [];
  return Array.isArray(j) ? j : [];
}
async function listMyTradesBinanceLike(ex, apiKey, apiSecret, symbolBase, startTime = undefined, limit = 1000) {
  const endpoint = "/api/v3/myTrades";
  const params = {
    symbol: `${symbolBase.toUpperCase()}USDT`,
    limit: Math.max(1, Math.min(1000, Number(limit) || 1000)),
    timestamp: Date.now()
  };
  if (Number.isFinite(Number(startTime))) params.startTime = Number(startTime);
  if (ex.defaultQuery) {
    const defaults = new URLSearchParams(ex.defaultQuery);
    for (const [k,v] of defaults) params[k] = v;
  }
  const queryString = Object.entries(params).map(([k,v]) => `${k}=${encodeURIComponent(v)}`).join("&");
  const signature = await hmacHexStr(apiSecret, queryString);
  const url = `${ex.baseUrl}${endpoint}?${queryString}&signature=${signature}`;
  const r = await safeFetch(url, { headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const j = await r.json().catch(()=>([]));
  if (!r.ok) return [];
  return Array.isArray(j) ? j : [];
}

/* ---------- Futures order/trade listings (Binance USDT-M) ---------- */
async function listAllOrdersFutures(ex, apiKey, apiSecret, symbolBase, startTime = undefined, limit = 1000) {
  const endpoint = "/fapi/v1/allOrders";
  const params = {
    symbol: `${symbolBase.toUpperCase()}USDT`,
    limit: Math.max(1, Math.min(1000, Number(limit) || 1000)),
    timestamp: Date.now()
  };
  if (Number.isFinite(Number(startTime))) params.startTime = Number(startTime);
  const queryString = Object.entries(params).map(([k,v]) => `${k}=${encodeURIComponent(v)}`).join("&");
  const signature = await hmacHexStr(apiSecret, queryString);
  const url = `${ex.baseUrl}${endpoint}?${queryString}&signature=${signature}`;
  const r = await safeFetch(url, { headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const j = await r.json().catch(()=>([]));
  if (!r.ok) return [];
  return Array.isArray(j) ? j : [];
}
async function listMyTradesFutures(ex, apiKey, apiSecret, symbolBase, startTime = undefined) {
  const endpoint = "/fapi/v1/userTrades";
  const params = {
    symbol: `${symbolBase.toUpperCase()}USDT`,
    timestamp: Date.now()
  };
  if (Number.isFinite(Number(startTime))) params.startTime = Number(startTime);
  const queryString = Object.entries(params).map(([k,v]) => `${k}=${encodeURIComponent(v)}`).join("&");
  const signature = await hmacHexStr(apiSecret, queryString);
  const url = `${ex.baseUrl}${endpoint}?${queryString}&signature=${signature}`;
  const r = await safeFetch(url, { headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const j = await r.json().catch(()=>([]));
  if (!r.ok) return [];
  return Array.isArray(j) ? j : [];
}

/* ---------- Orders (Spot/Futures) with clientOrderId threading ---------- */
/* Spot MARKET (Binance-like: Binance, MEXC) */
async function placeBinanceLikeOrder(ex, apiKey, apiSecret, symbol, side, amount, isQuoteOrder, clientOrderId = undefined) {
  const endpoint = "/api/v3/order";
  const params = {
    symbol: symbol.toUpperCase() + "USDT",
    side,
    type: "MARKET",
    newOrderRespType: "FULL",
    timestamp: Date.now()
  };
  if (clientOrderId) params.newClientOrderId = String(clientOrderId);

  if (side === "BUY" && isQuoteOrder) {
    params.quoteOrderQty = Number(amount).toFixed(2);
  } else if (side === "SELL" && !isQuoteOrder) {
    params.quantity = Number(amount).toFixed(6);
  } else if (side === "SELL" && isQuoteOrder) {
    const price = await getCurrentPrice(symbol);
    if (!price || !isFinite(price) || price <= 0) throw new Error("Bad price for SELL qty calc");
    params.quantity = (Number(amount) / price).toFixed(6);
  } else {
    const price = await getCurrentPrice(symbol);
    if (!price || !isFinite(price) || price <= 0) throw new Error("Bad price for quantity calculation");
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

/* Spot LIMIT_MAKER (post-only) for Binance-like (Binance, MEXC) */
async function placeBinanceLikeLimitMaker(ex, apiKey, apiSecret, symbol, side, limitPrice, amount, isQuoteOrder, clientOrderId = undefined) {
  const endpoint = "/api/v3/order";
  const params = {
    symbol: symbol.toUpperCase() + "USDT",
    side,
    type: "LIMIT_MAKER", // post-only
    newOrderRespType: "FULL",
    price: Number(limitPrice).toFixed(8),
    timestamp: Date.now()
  };
  if (clientOrderId) params.newClientOrderId = String(clientOrderId);

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
  if (!res.ok) throw new Error(data?.msg || data?.message || `HTTP ${res.status}`);

  const executedQty = parseFloat(data.executedQty || 0);
  let avgPrice = 0;
  if (executedQty > 0) {
    const cumQuote = parseFloat(data.cummulativeQuoteQty || 0);
    if (isFinite(cumQuote) && executedQty > 0) avgPrice = cumQuote / executedQty;
  }
  return { orderId: data.orderId, executedQty, avgPrice, status: data.status || "NEW" };
}

/* LBank/CoinEx MARKET (legacy, no CID threading for exits) */
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

/* ---------- Futures Orders (Binance USDT-M) ---------- */
async function placeBinanceFuturesOrder(ex, apiKey, apiSecret, symbol, side, baseQty, reduceOnly = false, clientOrderId = undefined) {
  const endpoint = "/fapi/v1/order";
  const params = {
    symbol: symbol.toUpperCase() + "USDT",
    side, type: "MARKET",
    quantity: Number(baseQty).toFixed(6),
    reduceOnly: reduceOnly ? "true" : "false",
    newOrderRespType: "RESULT",
    timestamp: Date.now()
  };
  if (clientOrderId) params.newClientOrderId = String(clientOrderId);
  const queryString = Object.entries(params).map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join("&");
  const signature = await hmacHexStr(apiSecret, queryString);
  const url = `${ex.baseUrl}${endpoint}?${queryString}&signature=${signature}`;
  const res = await safeFetch(url, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.msg || data?.message || `HTTP ${res.status}`);
  const executedQty = parseFloat(data.executedQty || data.origQty || 0);
  const cumQuote = parseFloat(data.cumQuote || data.cummulativeQuoteQty || 0);
  const avgPrice = executedQty > 0 ? (cumQuote / Math.max(1e-12, executedQty)) : (parseFloat(data.avgPrice || 0) || 0);
  return { orderId: data.orderId, executedQty, avgPrice, status: data.status || 'FILLED' };
}
async function placeBinanceFuturesLimitPostOnly(ex, apiKey, apiSecret, symbol, side, baseQty, price, reduceOnly = false, clientOrderId = undefined) {
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
  if (clientOrderId) params.newClientOrderId = String(clientOrderId);
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
async function placeDemoOrder(symbol, side, amount, isQuoteOrder, clientOrderId = undefined) {
  let currentPrice = await getCurrentPrice(symbol);
  if (!currentPrice || !isFinite(currentPrice) || currentPrice <= 0) currentPrice = 1;
  const executedQty = isQuoteOrder ? Number(amount) / currentPrice : Number(amount);
  return { orderId: `demo-${crypto.randomUUID?.() || Math.random().toString(36).slice(2)}`, executedQty, avgPrice: currentPrice, status: 'FILLED' };
}

/* ---------- Market order routing based on session exchange (adds cid) ---------- */
async function placeMarketBuy(env, userId, symbol, quoteAmount, opts = {}) {
  const session = await getSession(env, userId);
  const ex = SUPPORTED_EXCHANGES[session.exchange_name || (env.EXCHANGE || "mexc")];
  const cid = opts.clientOrderId;

  if (ex?.kind === 'demoParrot') {
    return await placeDemoOrder(symbol, "BUY", quoteAmount, true, cid);
  }

  const apiKey = await decrypt(session.api_key_encrypted, env);
  const apiSecret = await decrypt(session.api_secret_encrypted, env);

  switch (ex.kind) {
    case "binanceLike":
      return await placeBinanceLikeOrder(ex, apiKey, apiSecret, symbol, "BUY", quoteAmount, true, cid);
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
      return await placeBinanceFuturesOrder(ex, apiKey, apiSecret, symbol, "BUY", baseQty, opts.reduceOnly === true, cid);
    }
    default: throw new Error(`Orders not supported for ${ex?.label || session.exchange_name}`);
  }
}

async function placeMarketSell(env, userId, symbol, amount, isQuoteOrder = false, opts = {}) {
  const session = await getSession(env, userId);
  const ex = SUPPORTED_EXCHANGES[session.exchange_name || (env.EXCHANGE || "mexc")];
  const cid = opts.clientOrderId;

  if (ex?.kind === 'demoParrot') {
    return await placeDemoOrder(symbol, "SELL", amount, isQuoteOrder, cid);
  }

  const apiKey = await decrypt(session.api_key_encrypted, env);
  const apiSecret = await decrypt(session.api_secret_encrypted, env);

  switch (ex.kind) {
    case "binanceLike":
      return await placeBinanceLikeOrder(ex, apiKey, apiSecret, symbol, "SELL", amount, isQuoteOrder, cid);
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
      return await placeBinanceFuturesOrder(ex, apiKey, apiSecret, symbol, "SELL", baseQty, opts.reduceOnly === true, cid);
    }
    default: throw new Error(`Orders not supported for ${ex?.label || session.exchange_name}`);
  }
}

/* ---------- Order status resolvers ---------- */
async function getBinanceLikeOrderStatus(ex, apiKey, apiSecret, symbol, orderId) {
  const endpoint = "/api/v3/order";
  const params = { symbol: symbol.toUpperCase() + "USDT", orderId: String(orderId), timestamp: Date.now() };
  if (ex.defaultQuery) {
    const defaults = new URLSearchParams(ex.defaultQuery);
    for (const [k,v] of defaults) params[k] = v;
  }
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

/* ---------- Exit placement helpers (CID suffixes) ---------- */
/* Spot (Binance-like): TP(limit), SL(stop-limit), TTL(market) */
async function placeSpotTPLimit(ex, apiKey, apiSecret, base, qty, tpPrice, cidTp) {
  const endpoint = "/api/v3/order";
  const params = {
    symbol: `${base}USDT`,
    side: "SELL",
    type: "LIMIT",
    timeInForce: "GTC",
    price: Number(tpPrice).toFixed(8),
    quantity: Number(qty).toFixed(6),
    timestamp: Date.now(),
    newOrderRespType: "RESULT",
    newClientOrderId: cidTp
  };
  if (ex.defaultQuery) {
    const defaults = new URLSearchParams(ex.defaultQuery);
    for (const [k, v] of defaults) params[k] = v;
  }
  const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, qs);
  const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
  const r = await safeFetch(url, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const j = await r.json().catch(()=> ({}));
  if (!r.ok) throw new Error(j?.msg || j?.message || `HTTP ${r.status}`);
  return j.orderId;
}
async function placeSpotSLStopLimit(ex, apiKey, apiSecret, base, qty, slPrice, cidSl) {
  const endpoint = "/api/v3/order";
  const limitPx = Number(slPrice) * 0.997;
  const params = {
    symbol: `${base}USDT`,
    side: "SELL",
    type: "STOP_LOSS_LIMIT",
    timeInForce: "GTC",
    stopPrice: Number(slPrice).toFixed(8),
    price: Number(limitPx).toFixed(8),
    quantity: Number(qty).toFixed(6),
    timestamp: Date.now(),
    newOrderRespType: "RESULT",
    newClientOrderId: cidSl
  };
  if (ex.defaultQuery) {
    const defaults = new URLSearchParams(ex.defaultQuery);
    for (const [k, v] of defaults) params[k] = v;
  }
  const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, qs);
  const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
  const r = await safeFetch(url, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const j = await r.json().catch(()=> ({}));
  if (!r.ok) throw new Error(j?.msg || j?.message || `HTTP ${r.status}`);
  return j.orderId;
}
async function placeSpotTTLMarket(ex, apiKey, apiSecret, base, qty, cidTtl) {
  const endpoint = "/api/v3/order";
  const params = {
    symbol: `${base}USDT`,
    side: "SELL",
    type: "MARKET",
    quantity: Number(qty).toFixed(6),
    timestamp: Date.now(),
    newOrderRespType: "RESULT",
    newClientOrderId: cidTtl
  };
  if (ex.defaultQuery) {
    const defaults = new URLSearchParams(ex.defaultQuery);
    for (const [k, v] of defaults) params[k] = v;
  }
  const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, qs);
  const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
  const r = await safeFetch(url, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const j = await r.json().catch(()=> ({}));
  if (!r.ok) throw new Error(j?.msg || j?.message || `HTTP ${r.status}`);
  return j.orderId;
}

/* Futures (Binance USDT-M): TP/SL market triggers, TTL market reduce-only */
async function placeFutTPMarket(ex, apiKey, apiSecret, base, sideClose, stopPrice, cidTp) {
  const endpoint = "/fapi/v1/order";
  const params = {
    symbol: `${base}USDT`,
    side: sideClose, // SELL for long close, BUY for short close
    type: "TAKE_PROFIT_MARKET",
    stopPrice: Number(stopPrice).toFixed(8),
    closePosition: "true",
    reduceOnly: "true",
    timestamp: Date.now(),
    newClientOrderId: cidTp
  };
  const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, qs);
  const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
  const r = await safeFetch(url, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const j = await r.json().catch(()=> ({}));
  if (!r.ok) throw new Error(j?.msg || j?.message || `HTTP ${r.status}`);
  return j.orderId;
}
async function placeFutSLMarket(ex, apiKey, apiSecret, base, sideClose, stopPrice, cidSl) {
  const endpoint = "/fapi/v1/order";
  const params = {
    symbol: `${base}USDT`,
    side: sideClose,
    type: "STOP_MARKET",
    stopPrice: Number(stopPrice).toFixed(8),
    closePosition: "true",
    reduceOnly: "true",
    timestamp: Date.now(),
    newClientOrderId: cidSl
  };
  const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, qs);
  const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
  const r = await safeFetch(url, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const j = await r.json().catch(()=> ({}));
  if (!r.ok) throw new Error(j?.msg || j?.message || `HTTP ${r.status}`);
  return j.orderId;
}
async function placeFutTTLMarket(ex, apiKey, apiSecret, base, sideClose, cidTtl) {
  const endpoint = "/fapi/v1/order";
  const params = {
    symbol: `${base}USDT`,
    side: sideClose,
    type: "MARKET",
    reduceOnly: "true",
    closePosition: "true",
    timestamp: Date.now(),
    newClientOrderId: cidTtl
  };
  const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, qs);
  const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
  const r = await safeFetch(url, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const j = await r.json().catch(()=> ({}));
  if (!r.ok) throw new Error(j?.msg || j?.message || `HTTP ${r.status}`);
  return j.orderId;
}

/* ---------- Cancel helpers (by CID) and Spot OCO ---------- */
function supportsSpotOCO(ex) {
  const u = (ex?.baseUrl || "").toLowerCase();
  return u.includes("binance.com"); // OCO guaranteed on Binance; others may vary
}

// Cancel spot order by origClientOrderId (best-effort)
async function cancelSpotOrderByCID(ex, apiKey, apiSecret, base, clientOrderId) {
  const endpoint = "/api/v3/order";
  const params = {
    symbol: `${base}USDT`,
    origClientOrderId: clientOrderId,
    timestamp: Date.now()
  };
  if (ex.defaultQuery) {
    const defaults = new URLSearchParams(ex.defaultQuery);
    for (const [k, v] of defaults) params[k] = v;
  }
  const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, qs);
  const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
  const r = await safeFetch(url, { method: "DELETE", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  if (!r.ok) {
    try {
      const j = await r.json();
      const msg = (j?.msg || j?.message || "").toLowerCase();
      if (r.status === 400 || r.status === 404 || /unknown order|not found|already/i.test(msg)) return true;
    } catch {}
  }
  return true;
}

// Cancel futures order by origClientOrderId (best-effort)
async function cancelFuturesOrderByCID(ex, apiKey, apiSecret, base, clientOrderId) {
  const endpoint = "/fapi/v1/order";
  const params = {
    symbol: `${base}USDT`,
    origClientOrderId: clientOrderId,
    timestamp: Date.now()
  };
  const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, qs);
  const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
  const r = await safeFetch(url, { method: "DELETE", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  if (!r.ok) {
    try {
      const j = await r.json();
      const msg = (j?.msg || j?.message || "").toLowerCase();
      if (r.status === 400 || r.status === 404 || /unknown order|not found|already/i.test(msg)) return true;
    } catch {}
  }
  return true;
}

// Spot OCO (Binance) — uses leg clientOrderIds to preserve :tp/:sl tagging
async function placeSpotOCO(ex, apiKey, apiSecret, base, qty, tpPrice, slPrice, cidTp, cidSl, listCid) {
  const endpoint = "/api/v3/order/oco";
  const params = {
    symbol: `${base}USDT`,
    side: "SELL",
    quantity: Number(qty).toFixed(6),
    price: Number(tpPrice).toFixed(8),
    stopPrice: Number(slPrice).toFixed(8),
    stopLimitPrice: Number(slPrice * 0.997).toFixed(8),
    stopLimitTimeInForce: "GTC",
    newOrderRespType: "RESULT",
    listClientOrderId: listCid,
    limitClientOrderId: cidTp,
    stopClientOrderId:  cidSl,
    timestamp: Date.now()
  };
  if (ex.defaultQuery) {
    const defaults = new URLSearchParams(ex.defaultQuery);
    for (const [k, v] of defaults) params[k] = v;
  }
  const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, qs);
  const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
  const r = await safeFetch(url, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
  const j = await r.json().catch(()=> ({}));
  if (!r.ok) throw new Error(j?.msg || j?.message || `HTTP ${r.status}`);
  return j;
}
/* ======================================================================
   SECTION 5/7 — Gist reconciliation
   (Precise commissions; partial fills; TP/SL placement; TTL; exit inference;
    OCO on Binance; resize spot exits on partial fills; learned=false)
   ====================================================================== */

/* ---------- Small helpers ---------- */
function ideaBaseFromSymbolFull(idea) {
  if (idea?.base) return String(idea.base).toUpperCase();
  const full = String(idea?.symbolFull || "").toUpperCase();
  const quotes = ["USDT","USDC","USD"];
  for (const q of quotes) if (full.endsWith(q)) return full.slice(0, -q.length);
  return full.replace(/USDT|USDC|USD$/,'');
}
function entrySideToOrderSide(idea) {
  const s = String(idea?.side || 'long').toLowerCase();
  return s === 'short' ? 'SELL' : 'BUY';
}
function isShortSide(idea) {
  return String(idea?.side || 'long').toLowerCase() === 'short';
}
function makerTakerSummary(trades) {
  let m=0,t=0;
  for (const tr of trades||[]) {
    const mk = tr?.isMaker;
    if (mk === true) m++;
    else t++;
  }
  return { maker: m, taker: t };
}
function avgPxQtyFromTrades(trades) {
  let q=0, v=0;
  for (const tr of (trades||[])) {
    const qty = parseFloat(tr.qty || tr.executedQty || tr.qty_filled || tr.baseQty || 0);
    const px  = parseFloat(tr.price || tr.p || tr.avgPrice || 0);
    if (isFinite(qty) && isFinite(px) && qty>0 && px>0) { q += qty; v += qty*px; }
  }
  const avg = q>0 ? v/q : 0;
  return { avg, qty: q };
}
function tradesFingerprint(trs) {
  if (!trs?.length) return null;
  const first = trs[0], last = trs[trs.length - 1];
  const fid = first?.id ?? first?.tradeId ?? first?.T ?? '';
  const lid = last?.id ?? last?.tradeId  ?? last?.T ?? '';
  const ft  = Number(first?.time || first?.updateTime || first?.T || 0);
  const lt  = Number(last?.time  || last?.updateTime  || last?.T  || 0);
  return `${fid}:${lid}:${ft}:${lt}:${trs.length}`;
}

// Precise commission calc from fills (USDT + BASE converted using fill price)
async function commissionFromTradesUSDT(trades, base) {
  let total = 0;
  const BASE = String(base || "").toUpperCase();
  for (const t of (trades || [])) {
    const c = parseFloat(t.commission ?? t.commissionAmount ?? 0);
    if (!isFinite(c) || c <= 0) continue;
    const asset = String(t.commissionAsset ?? t.commissionCoin ?? t.feeAsset ?? "").toUpperCase();

    if (asset === "USDT") {
      total += c;
    } else if (asset === BASE) {
      const px = parseFloat(t.price ?? t.p ?? t.avgPrice ?? 0);
      if (isFinite(px) && px > 0) total += c * px; // convert base-asset fee to USDT using fill price
    }
  }
  return total;
}

// Approx fee fallback (used by demo or when no precise fills are present)
function approxCommissionFromNotional(entryNotional, exitNotional, feeRatePerSide) {
  const f = Number(feeRatePerSide || 0);
  const eN = Math.max(0, Number(entryNotional || 0));
  const xN = Math.max(0, Number(exitNotional || 0));
  const commission_quote_usdt = (eN + xN) * f;
  const commission_bps = eN > 0 ? Math.round((commission_quote_usdt / eN) * 10000) : 0;
  return { commission_quote_usdt, commission_bps };
}

/* Demo TP/SL evaluation from 1m klines — earliest hit since entry */
async function evaluateDemoExit(idea, base, entryPrice, entryTsMs) {
  try {
    const short = isShortSide(idea);
    const tpBps = Number(idea.tp_bps || 0);
    const slBps = Number(idea.sl_bps || 0);

    let tpAbs = Number(idea.tp_abs || NaN);
    let slAbs = Number(idea.sl_abs || NaN);
    if (!(tpAbs>0) || !(slAbs>0)) {
      if (short) {
        if (!(tpAbs>0)) tpAbs = entryPrice * (1 - tpBps/10000);
        if (!(slAbs>0)) slAbs = entryPrice * (1 + slBps/10000);
      } else {
        if (!(tpAbs>0)) tpAbs = entryPrice * (1 + tpBps/10000);
        if (!(slAbs>0)) slAbs = entryPrice * (1 - slBps/10000);
      }
    }

    const since = Math.max(0, Number(entryTsMs || 0) - 60_000);
    const kl1m = await fetchKlinesRange(base, "1m", since, Date.now() + 60_000);
    if (!Array.isArray(kl1m) || kl1m.length === 0) return null;

    let best = null; // {reason, exitPrice, exitTsMs}
    for (const k of kl1m) {
      const hi = Number(k[2] || 0);
      const lo = Number(k[3] || 0);
      const ts = Number(k[6] || k[0] || 0);

      if (!short) {
        const tpHit = isFinite(hi) && hi >= tpAbs;
        const slHit = isFinite(lo) && lo <= slAbs;
        if (tpHit) { best = { reason: "tp", exitPrice: tpAbs, exitTsMs: ts }; break; }
        if (slHit) { best = { reason: "sl", exitPrice: slAbs, exitTsMs: ts }; break; }
      } else {
        const tpHit = isFinite(lo) && lo <= tpAbs;  // favorable for short
        const slHit = isFinite(hi) && hi >= slAbs;  // adverse for short
        if (tpHit) { best = { reason: "tp", exitPrice: tpAbs, exitTsMs: ts }; break; }
        if (slHit) { best = { reason: "sl", exitPrice: slAbs, exitTsMs: ts }; break; }
      }
    }
    return best;
  } catch {
    return null;
  }
}

/* ---------- Entry placement (if not present) ---------- */
async function ensureEntryOrderExists(idea, exAccount) {
  const { ex, apiKey, apiSecret } = exAccount || {};
  if (!ex || !apiKey || !apiSecret) return null;

  const base = ideaBaseFromSymbolFull(idea);
  const cid = String(idea.client_order_id || "");
  const side = entrySideToOrderSide(idea);
  const notional = Number(idea.notional_usd || idea.notional_usdt || 0);
  if (!cid || !(notional > 0) || !base) return null;

  if (ex.kind === 'demoParrot') {
    try {
      // Simulate immediate fill at live price
      const o = await placeDemoOrder(base, side, notional, true, cid);
      return { orderId: o.orderId, executedQty: o.executedQty, avgPrice: o.avgPrice, status: o.status || "FILLED" };
    } catch { return null; }
  }

  if (ex.kind === 'binanceLike') {
    // Spot: only long entries supported
    if (side === 'SELL') return null;
    try {
      if (isFinite(Number(idea.entry_limit))) {
        return await placeBinanceLikeLimitMaker(ex, apiKey, apiSecret, base, 'BUY', Number(idea.entry_limit), notional, true, cid);
      } else {
        return await placeBinanceLikeOrder(ex, apiKey, apiSecret, base, 'BUY', notional, true, cid);
      }
    } catch {
      try { return await placeBinanceLikeOrder(ex, apiKey, apiSecret, base, 'BUY', notional, true, cid); }
      catch { return null; }
    }
  } else if (ex.kind === 'binanceFuturesUSDT') {
    const price = await getCurrentPrice(base);
    if (!(price>0)) return null;
    const baseQty = notional / price;
    try {
      if (isFinite(Number(idea.entry_limit))) {
        return await placeBinanceFuturesLimitPostOnly(ex, apiKey, apiSecret, base, side, baseQty, Number(idea.entry_limit), false, cid);
      } else {
        return await placeBinanceFuturesOrder(ex, apiKey, apiSecret, base, side, baseQty, false, cid);
      }
    } catch {
      try { return await placeBinanceFuturesOrder(ex, apiKey, apiSecret, base, side, baseQty, false, cid); }
      catch { return null; }
    }
  }
  return null;
}

/* ---------- Place or resize TP/SL exits when entry is (partially) filled ---------- */
async function ensureOrResizeBracketExits(state, idea, exAccount, entryPrice, filledQty, orders) {
  const { ex, apiKey, apiSecret } = exAccount || {};
  if (!ex || !apiKey || !apiSecret) return;
  const isShort = isShortSide(idea);
  const base = ideaBaseFromSymbolFull(idea);
  const cid = String(idea.client_order_id);
  if (!(filledQty > 0) || !base || !cid) return;

  // Compute absolute exits
  let tpAbs = Number(idea.tp_abs || NaN);
  let slAbs = Number(idea.sl_abs || NaN);
  if (!(tpAbs>0) || !(slAbs>0)) {
    const tpBps = Number(idea.tp_bps || 0), slBps = Number(idea.sl_bps || 0);
    if (isShort) {
      if (!(tpAbs>0)) tpAbs = entryPrice * (1 - tpBps/10000);
      if (!(slAbs>0)) slAbs = entryPrice * (1 + slBps/10000);
    } else {
      if (!(tpAbs>0)) tpAbs = entryPrice * (1 + tpBps/10000);
      if (!(slAbs>0)) slAbs = entryPrice * (1 - slBps/10000);
    }
  }

  const tpCID  = `${cid}:tp`;
  const slCID  = `${cid}:sl`;

  // Current metadata and need for resize
  const meta = idea.exits || {};
  const placedQty = Number(meta.placed_qty || 0);
  const needResize = filledQty > placedQty + 1e-8;

  // Find any existing open exit legs
  const tpOrder = (orders||[]).find(o => String(o.clientOrderId||"") === tpCID && ["NEW","PARTIALLY_FILLED"].includes(String(o.status||"").toUpperCase()));
  const slOrder = (orders||[]).find(o => String(o.clientOrderId||"") === slCID && ["NEW","PARTIALLY_FILLED"].includes(String(o.status||"").toUpperCase()));
  const haveBoth = !!tpOrder && !!slOrder;

  if (ex.kind === 'demoParrot') {
    // No exchange orders; just record exits metadata
    if (!meta.placed || needResize) {
      updatePendingOpenByCID(state, cid, { exits: { placed: true, via: "demo", placed_qty: filledQty, tp_abs: tpAbs, sl_abs: slAbs } });
    }
    return;
  }

  if (ex.kind === 'binanceLike') {
    // Spot long-only exits; use OCO when supported
    if (isShort) return; // safety
    if (!meta.placed || needResize || !haveBoth || (meta.via === "dual" && supportsSpotOCO(ex))) {
      // Best-effort cancel both legs first (no-op if not present)
      try { await cancelSpotOrderByCID(ex, apiKey, apiSecret, base, tpCID); } catch {}
      try { await cancelSpotOrderByCID(ex, apiKey, apiSecret, base, slCID); } catch {}

      // Prefer OCO on Binance; fallback to dual orders
      let via = "dual";
      try {
        if (supportsSpotOCO(ex)) {
          await placeSpotOCO(ex, apiKey, apiSecret, base, filledQty, tpAbs, slAbs, tpCID, slCID, `${cid}:oco`);
          via = "oco";
        } else {
          await placeSpotTPLimit(ex, apiKey, apiSecret, base, filledQty, tpAbs, tpCID);
          await placeSpotSLStopLimit(ex, apiKey, apiSecret, base, filledQty, slAbs, slCID);
          via = "dual";
        }
      } catch (e) {
        // If OCO failed, fallback to dual
        if (via === "oco") {
          await placeSpotTPLimit(ex, apiKey, apiSecret, base, filledQty, tpAbs, tpCID);
          await placeSpotSLStopLimit(ex, apiKey, apiSecret, base, filledQty, slAbs, slCID);
          via = "dual";
        } else {
          throw e;
        }
      }
      updatePendingOpenByCID(state, cid, { exits: { placed: true, via, placed_qty: filledQty, tp_abs: tpAbs, sl_abs: slAbs } });
    }
  } else if (ex.kind === 'binanceFuturesUSDT') {
    // Futures: triggers use closePosition=true; no resizing required
    const sideClose = isShort ? "BUY" : "SELL";
    if (!meta.placed) {
      await placeFutTPMarket(ex, apiKey, apiSecret, base, sideClose, tpAbs, `${cid}:tp`);
      await placeFutSLMarket(ex, apiKey, apiSecret, base, sideClose, slAbs, `${cid}:sl`);
      updatePendingOpenByCID(state, cid, { exits: { placed: true, via: "triggers", placed_qty: filledQty, tp_abs: tpAbs, sl_abs: slAbs } });
    } else if (needResize) {
      // Just track the latest filled quantity; triggers still close the full position
      updatePendingOpenByCID(state, cid, { exits: { ...(meta||{}), placed_qty: filledQty } });
    }
  }
}

/* ---------- Reconcile single Gist pending idea (full lifecycle) ---------- */
async function reconcilePendingIdea(env, state, idea, exAccount) {
  try {
    const cid = String(idea.client_order_id || "");
    if (!cid) return { status: 'noop', reason: 'no_cid' };

    const base = ideaBaseFromSymbolFull(idea);
    const { ex, apiKey, apiSecret } = exAccount || {};
    const since = Math.max(0, Number(idea.ts_ms || 0) - 60_000);
    const isDemo = ex?.kind === 'demoParrot';

    if (!ex || !apiKey || !apiSecret) return { status: 'error', reason: 'no_exec_creds' };

    // Remove unsupported spot shorts immediately (only for binanceLike)
    if (ex.kind === 'binanceLike' && isShortSide(idea)) {
      console.warn(`[gist] Removing unsupported spot short ${base} (CID ${cid})`);
      removePendingByCID(state, cid);
      return { status: 'removed', cid, reason: 'spot_short_unsupported' };
    }

    // Helper to compute fee side
    const resolvedFeeSide = Number((state?.fees && (state.fees.taker ?? state.fees.maker)) ?? env?.FEE_RATE_PER_SIDE ?? 0.001);

    if (isDemo) {
      // DEMO FLOW — no exchange queries; simulate fills and exits using prices/klines
      const st = String(idea.status || "planned").toLowerCase();

      // If planned, "enter" now if not already open
      if (st === "planned") {
        const side = entrySideToOrderSide(idea);
        const notional = Number(idea.notional_usd || idea.notional_usdt || 0);
        if (notional > 0) {
          // Simulate market entry
          const o = await placeDemoOrder(base, side, notional, true, cid);
          const entryPrice = Number(o.avgPrice || 0);
          const qty = Number(o.executedQty || 0);
          const entryTsMs = Date.now();

          updatePendingOpenByCID(state, cid, {
            status: "open",
            entry_ts_ms: entryTsMs,
            entry_price: entryPrice,
            qty
          });

          // Record exits metadata (demo)
          await ensureOrResizeBracketExits(state, { ...idea, entry_price: entryPrice }, exAccount, entryPrice, qty, null);
        } else {
          return { status: 'pending', cid };
        }
      }

      // From here treat it as open
      const openIdea = (state.pending||[]).find(x => String(x.client_order_id||"") === cid);
      const entryPrice = Number(openIdea?.entry_price || idea.entry_price || 0);
      const entryQty   = Number(openIdea?.qty || idea.qty || 0);
      const entryTsMs  = Number(openIdea?.entry_ts_ms || idea.entry_ts_ms || Date.now());
      if (!(entryPrice > 0 && entryQty > 0)) return { status: 'pending', cid };

      // Resolve absolute exits from (possibly) updated metadata
      const meta = openIdea?.exits || idea.exits || {};
      const short = isShortSide(idea);
      let tpAbs = Number(meta.tp_abs || idea.tp_abs || NaN);
      let slAbs = Number(meta.sl_abs || idea.sl_abs || NaN);
      if (!(tpAbs>0) || !(slAbs>0)) {
        const tpBps = Number(idea.tp_bps || 0), slBps = Number(idea.sl_bps || 0);
        if (short) {
          if (!(tpAbs>0)) tpAbs = entryPrice * (1 - tpBps/10000);
          if (!(slAbs>0)) slAbs = entryPrice * (1 + slBps/10000);
        } else {
          if (!(tpAbs>0)) tpAbs = entryPrice * (1 + tpBps/10000);
          if (!(slAbs>0)) slAbs = entryPrice * (1 - slBps/10000);
        }
      }

      // First: TP/SL detection from klines since entry
      let exit = await evaluateDemoExit(idea, base, entryPrice, entryTsMs);

      // Second: TTL market close if past TTL and still no exit
      const ttlTs = Number(
        idea.ttl_ts_ms ||
        ((entryTsMs && Number(idea.hold_sec)) ? (entryTsMs + 1000 * Number(idea.hold_sec)) : 0)
      );
      if (!exit && envFlag(env, "ENABLE_TTL_MARKET_CLOSE", "1") && isFinite(ttlTs) && ttlTs > 0 && Date.now() >= ttlTs) {
        const px = await getCurrentPrice(base, entryPrice);
        exit = { reason: "ttl", exitPrice: Number(px || entryPrice), exitTsMs: Date.now() };
      }

      if (!exit) return { status: 'open', cid };

      // Compute net pnl and close
      const entryNotional = entryPrice * entryQty;
      const exitNotional  = exit.exitPrice * entryQty;
      const up = !short;
      const ret = up ? (exit.exitPrice/entryPrice - 1) : (entryPrice/exit.exitPrice - 1);

      // Commission (approx for demo)
      const approx = approxCommissionFromNotional(entryNotional, exitNotional, resolvedFeeSide);
      const commission_quote_usdt = approx.commission_quote_usdt;
      const commission_bps = approx.commission_bps;

      // PnL (net of commissions)
      const pnl_bps = Math.round(ret*10000) - commission_bps;
      const exit_outcome = (pnl_bps > 0) ? "win" : "loss";

      // MFE/MAE (optional)
      let mfe_bps = 0, mae_bps = 0;
      if (envFlag(env, "MFE_MAE_ENABLE", "1")) {
        try {
          const kl1m = await fetchKlinesRange(base, "1m", Number(entryTsMs)-60_000, Number(exit.exitTsMs)+60_000);
          const r = computeMFE_MAE(entryPrice, idea.side, kl1m);
          mfe_bps = r.mfe_bps || 0;
          mae_bps = r.mae_bps || 0;
        } catch (e) {
          console.warn("[gist] MFE/MAE compute warn (demo):", e?.message || e);
        }
      }

      appendClosed(state, {
        symbolFull: idea.symbolFull || `${base}USDT`,
        side: idea.side,
        pnl_bps,
        ts_entry_ms: Number(entryTsMs),
        ts_exit_ms: Number(exit.exitTsMs),
        price_entry: entryPrice,
        price_exit: exit.exitPrice,
        qty: entryQty,
        reconciliation: "demo_sim",
        exit_reason: exit.reason,
        exit_outcome,
        p_pred: idea.p_lcb,
        p_raw: idea.p_raw,
        calib_key: idea.calib_key,
        regime: idea.regime,
        predicted_snapshot: idea.predicted,
        trade_details: {
          client_order_id: cid,
          maker_taker_entry: { maker: 0, taker: 0 },
          maker_taker_exit:  { maker: 0, taker: 0 },
          commission_bps,
          commission_quote_usdt,
          fingerprint_entry: null,
          fingerprint_exit:  null
        },
        realized: { tp_hit: exit.reason==="tp", sl_hit: exit.reason==="sl", ttl_exit: exit.reason==="ttl", mfe_bps, mae_bps },
        learned: false,
        learned_at_ts: null
      });

      bumpSymStatsReal(state, base, pnl_bps);
      (state.equity ||= []).push({ ts_ms: Number(exit.exitTsMs), pnl_bps, recon: "demo" });

      removePendingByCID(state, cid);
      return { status: 'closed', cid, pnl_bps, reason: exit.reason };
    }

    // Live exchange paths (binanceLike / binanceFutures)
    const listOrders = async () => {
      if (ex.kind === 'binanceFuturesUSDT') return await listAllOrdersFutures(ex, apiKey, apiSecret, base, since, 1000);
      return await listAllOrdersBinanceLike(ex, apiKey, apiSecret, base, since, 1000);
    };
    const listTrades = async () => {
      if (ex.kind === 'binanceFuturesUSDT') return await listMyTradesFutures(ex, apiKey, apiSecret, base, since);
      return await listMyTradesBinanceLike(ex, apiKey, apiSecret, base, since, 1000);
    };

    let orders = await listOrders();
    let trades = await listTrades();

    // Ensure entry exists
    let entryOrder = (orders || []).find(o => (o.clientOrderId || '') === cid);
    if (!entryOrder) {
      await ensureEntryOrderExists(idea, exAccount);
      orders = await listOrders();
      trades = await listTrades();
      entryOrder = (orders || []).find(o => (o.clientOrderId || '') === cid);
    }

    // Determine entry fill state (support PARTIALLY_FILLED)
    let entryPrice = Number(idea.entry_price || 0);
    let entryQty = Number(idea.qty || 0);
    let entryTsMs = Number(idea.entry_ts_ms || 0);
    let entryTrades = [];

    const eStatus = String(entryOrder?.status || '').toUpperCase();
    const entryOrderId = entryOrder?.orderId;
    const entryTradesAll = (trades || []).filter(t => String(t.orderId) === String(entryOrderId));
    const filledQty = parseFloat(entryOrder?.executedQty || 0);

    if (entryOrder && (eStatus === "FILLED" || (eStatus === "PARTIALLY_FILLED" && filledQty > 0))) {
      const wap = avgPxQtyFromTrades(entryTradesAll);
      if (wap.avg > 0) entryPrice = wap.avg;
      if (filledQty > 0) entryQty = filledQty;
      entryTsMs = Number(entryOrder.updateTime || entryTradesAll?.[0]?.time || idea.ts_ms || Date.now());
      entryTrades = entryTradesAll;

      updatePendingOpenByCID(state, cid, { status: "open", entry_ts_ms: entryTsMs, entry_price: entryPrice, qty: entryQty });

      // Place or resize brackets for current filled qty
      if (entryPrice > 0 && entryQty > 0) {
        await ensureOrResizeBracketExits(state, idea, exAccount, entryPrice, entryQty, orders);
        orders = await listOrders();
      }

      // TTL submit at/after deadline (compute from hold_sec if missing)
      const ttlTs = Number(
        idea.ttl_ts_ms ||
        ((entryTsMs && Number(idea.hold_sec)) ? (entryTsMs + 1000 * Number(idea.hold_sec)) : 0)
      );
      const now = Date.now();
      const ttlCID = `${cid}:ttl`;
      const ttlAlready = (orders || []).some(o => String(o.clientOrderId || '') === ttlCID);
      if (envFlag(env, "ENABLE_TTL_MARKET_CLOSE", "1") && isFinite(ttlTs) && ttlTs > 0 && now >= ttlTs && !ttlAlready) {
        try {
          const sideClose = isShortSide(idea) ? "BUY" : "SELL";
          if (ex.kind === 'binanceLike') {
            await placeSpotTTLMarket(ex, apiKey, apiSecret, base, entryQty, ttlCID);
          } else if (ex.kind === 'binanceFuturesUSDT') {
            await placeFutTTLMarket(ex, apiKey, apiSecret, base, sideClose, ttlCID);
          }
        } catch (e) { console.error("[gist] TTL submit error:", e?.message || e); }
        orders = await listOrders();
        trades = await listTrades();
      }
    } else {
      return { status: 'pending', cid };
    }

    // Detect exit fill via CID suffix
    const tpCID = `${cid}:tp`, slCID = `${cid}:sl`, ttlCID = `${cid}:ttl`;
    const exitOrder = (orders || []).find(o => {
      const c = String(o.clientOrderId || "");
      return (c === tpCID || c === slCID || c === ttlCID) && String(o.status || "").toUpperCase() === "FILLED";
    });

    let exitReason = "";
    let exitTrades = [];
    let exitPrice = 0;
    let exitTsMs = 0;

    if (exitOrder) {
      const co = String(exitOrder.clientOrderId || "");
      exitReason = co.endsWith(":tp") ? "tp" : co.endsWith(":sl") ? "sl" : "ttl";
      const oid = exitOrder.orderId;
      exitTrades = (trades || []).filter(t => String(t.orderId) === String(oid));
      const wap = avgPxQtyFromTrades(exitTrades);
      exitPrice = wap.avg || Number(exitOrder.price || 0);
      exitTsMs = Number(exitOrder.updateTime || exitTrades?.at(-1)?.time || Date.now());

      // Best-effort cleanup of sibling legs (spot dual or futures triggers)
      if (ex.kind === 'binanceLike' && (idea?.exits?.via || "dual") !== "oco") {
        if (exitReason === "tp") { try { await cancelSpotOrderByCID(ex, apiKey, apiSecret, base, slCID); } catch {} }
        else if (exitReason === "sl") { try { await cancelSpotOrderByCID(ex, apiKey, apiSecret, base, tpCID); } catch {} }
        else if (exitReason === "ttl") {
          try { await cancelSpotOrderByCID(ex, apiKey, apiSecret, base, tpCID); } catch {}
          try { await cancelSpotOrderByCID(ex, apiKey, apiSecret, base, slCID); } catch {}
        }
      }
      if (ex.kind === 'binanceFuturesUSDT') {
        // triggers may remain open; cancel both best-effort
        for (const leg of [tpCID, slCID]) { try { await cancelFuturesOrderByCID(ex, apiKey, apiSecret, base, leg); } catch {} }
      }
    } else {
      // Fallback: net-flat check from trade history (post entry)
      if (entryQty > 0) {
        let netBase = entryQty;
        const later = (trades || []).filter(t => Number(t.time || 0) > entryTsMs);
        for (const t of later) {
          const q = parseFloat(t.qty || t.executedQty || 0);
          const isBuyer = (t.isBuyer === true || String(t.side||"").toUpperCase() === "BUY");
          netBase += isBuyer ? q : -q;
          exitTrades.push(t);
        }
        if (netBase <= entryQty * 0.02 && exitTrades.length) {
          const wap = avgPxQtyFromTrades(exitTrades);
          exitPrice = wap.avg || 0;
          exitTsMs = Number(exitTrades?.at(-1)?.time || Date.now());

          // Infer exit_reason vs planned targets
          const tol_bps = Math.max(7, Number(idea.cost_bps || 10));
          const short = isShortSide(idea);
          const tpAbs = Number(idea.tp_abs || (entryPrice * (short ? (1 - (idea.tp_bps||0)/10000) : (1 + (idea.tp_bps||0)/10000))));
          const slAbs = Number(idea.sl_abs || (entryPrice * (short ? (1 + (idea.sl_bps||0)/10000) : (1 - (idea.sl_bps||0)/10000))));
          const diffToTP_bps = Math.abs(((exitPrice / entryPrice) - (tpAbs / entryPrice)) * 10000);
          const diffToSL_bps = Math.abs(((exitPrice / entryPrice) - (slAbs / entryPrice)) * 10000);
          if (isFinite(diffToTP_bps) && diffToTP_bps <= tol_bps) exitReason = "tp";
          else if (isFinite(diffToSL_bps) && diffToSL_bps <= tol_bps) exitReason = "sl";
          else exitReason = "manual";
        }
      }
    }

    // Write closed[] if exited
    if (exitReason && exitPrice > 0 && entryPrice > 0 && entryQty > 0) {
      // Commission (prefer precise from fills; keep approx fallback)
      const feeSide = resolvedFeeSide;
      const c1 = await commissionFromTradesUSDT(entryTrades, base);
      const c2 = await commissionFromTradesUSDT(exitTrades, base);
      let commission_quote_usdt = (c1 || 0) + (c2 || 0);

      let commission_bps = 0;
      const entryNotional = entryPrice * entryQty;
      if (entryNotional > 0 && commission_quote_usdt > 0) {
        commission_bps = Math.round((commission_quote_usdt / entryNotional) * 10000);
      } else {
        const approx = approxCommissionFromNotional(entryNotional, exitPrice * entryQty, feeSide);
        commission_quote_usdt = commission_quote_usdt || approx.commission_quote_usdt;
        commission_bps = approx.commission_bps;
      }

      // PnL (net of commissions)
      const up = !isShortSide(idea);
      const ret = up ? (exitPrice/entryPrice - 1) : (entryPrice/exitPrice - 1);
      const pnl_bps = Math.round(ret*10000) - commission_bps;
      const exit_outcome = (pnl_bps > 0) ? "win" : "loss";

      // MFE/MAE (optional)
      let mfe_bps = 0, mae_bps = 0;
      if (envFlag(env, "MFE_MAE_ENABLE", "1")) {
        try {
          const kl1m = await fetchKlinesRange(base, "1m", Number(entryTsMs)-60_000, Number(exitTsMs)+60_000);
          const r = computeMFE_MAE(entryPrice, idea.side, kl1m);
          mfe_bps = r.mfe_bps || 0;
          mae_bps = r.mae_bps || 0;
        } catch (e) {
          console.warn("[gist] MFE/MAE compute warn:", e?.message || e);
        }
      }

      const mtEntry = makerTakerSummary(entryTrades);
      const mtExit  = makerTakerSummary(exitTrades);

      appendClosed(state, {
        symbolFull: idea.symbolFull || `${base}USDT`,
        side: idea.side,
        pnl_bps,
        ts_entry_ms: Number(entryTsMs),
        ts_exit_ms: Number(exitTsMs),
        price_entry: entryPrice,
        price_exit: exitPrice,
        qty: entryQty,
        reconciliation: "exchange_trade_history",
        exit_reason: exitReason,
        exit_outcome,
        p_pred: idea.p_lcb,
        p_raw: idea.p_raw,
        calib_key: idea.calib_key,
        regime: idea.regime,
        predicted_snapshot: idea.predicted,
        trade_details: {
          client_order_id: cid,
          maker_taker_entry: mtEntry,
          maker_taker_exit:  mtExit,
          commission_bps,
          commission_quote_usdt,
          fingerprint_entry: tradesFingerprint(entryTrades),
          fingerprint_exit:  tradesFingerprint(exitTrades)
        },
        realized: { tp_hit: exitReason==="tp", sl_hit: exitReason==="sl", ttl_exit: exitReason==="ttl", mfe_bps, mae_bps },
        learned: false,
        learned_at_ts: null
      });

      bumpSymStatsReal(state, base, pnl_bps);
      (state.equity ||= []).push({ ts_ms: Number(exitTsMs), pnl_bps, recon: "api" });

      removePendingByCID(state, cid);
      return { status: 'closed', cid, pnl_bps, reason: exitReason };
    }

    return { status: 'open', cid };
  } catch (e) {
    console.error("[gist] reconcilePendingIdea error:", e?.message || e);
    return { status: 'error', error: e?.message || String(e) };
  }
}
/* ======================================================================
   SECTION 6/7 — UI Texts, Cards, Keyboards, Dashboard, Lists
   (Worker-1 UX: Manage Trades submenu + W1-style cards + Protocol Status)
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
    "Manual: you receive gist ideas (plans) and choose Approve/Reject per idea. Approved ideas are executed via the exchange.",
    "Auto: the bot executes gist ideas automatically within strict risk budgets."
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
    "Setup complete (Manual Mode).",
    "",
    `Live Balance (TC): $${Number(bal||0).toFixed(2)}`,
    `ACP (80% of TC): $${Number(0.80 * (bal||0)).toFixed(2)}`,
    `Per-trade cap: ${(0.10*100).toFixed(0)}% of TC`,
    `Daily open risk cap: ${(0.30*100).toFixed(0)}% of TC`,
    `Fee Rate (per side): ${(Number(feeRate||0)*100).toFixed(2)}%`,
    "Exits: TP/SL from idea when provided; TTL fallback; OCO on Binance when possible.",
    "",
    "In Manual mode, the bot will present gist ideas for your approval."
  ].join("\n");
}
function confirmAutoTextB(bal, feeRate) {
  return [
    "Setup complete (Auto Mode).",
    "",
    `Live Balance (TC): $${Number(bal||0).toFixed(2)}`,
    `ACP (80% of TC): $${Number(0.80 * (bal||0)).toFixed(2)}`,
    `Per-trade cap: ${(0.10*100).toFixed(0)}% of TC`,
    `Daily open risk cap: ${(0.30*100).toFixed(0)}% of TC`,
    `Fee Rate (per side): ${(Number(feeRate||0)*100).toFixed(2)}%`,
    "Exits: TP/SL from idea when provided; TTL fallback; OCO on Binance when possible.",
    "",
    "In Auto mode, the bot executes gist ideas automatically (no approval prompts)."
  ].join("\n");
}

/* ---------- Performance stats from closed[] ---------- */
function computeClosedStatsFromState(state) {
  const closed = Array.isArray(state?.closed) ? state.closed : [];
  let wins = 0, losses = 0, netBps = 0, netUsdt = 0;
  for (const t of closed) {
    const bps = Number(t?.pnl_bps || 0);
    netBps += bps;
    if (bps > 0) wins++; else losses++;
    const entryNotional = Number(t?.price_entry || 0) * Number(t?.qty || 0);
    if (entryNotional > 0) netUsdt += (bps / 10000) * entryNotional;
  }
  const total = closed.length;
  const winRate = total > 0 ? wins / total : 0;
  return { wins, losses, total, winRate, netBps, netPct: netBps / 100, netUsdt };
}

/* ---------- Protocol Status (Worker-1 style presentation) ---------- */
function protocolStatusTextW1(protocol, tc, counts, extras = {}) {
  const perTradeNotionalCapFrac = Number(extras.perTradeNotionalCapFrac ?? 0.10);
  const dailyNotionalCapFrac    = Number(extras.dailyNotionalCapFrac ?? 0.30);
  const perTradeNotionalCap     = perTradeNotionalCapFrac * tc;
  const dailyNotionalCap        = dailyNotionalCapFrac * tc;

  const lines = [
    "Protocol Status (ACP V20, Multi-Position):",
    "",
    `Live Balance (TC): ${formatMoney(tc)}`,
    `ACP (${(ACP_FRAC*100).toFixed(0)}% of TC): ${formatMoney(ACP_FRAC * tc)}`,
    `Per-trade risk cap (M): ${formatMoney(PER_TRADE_CAP_FRAC * tc)}`,
    `Open positions: ${counts.open} | Planned ideas: ${counts.planned} | Closed trades: ${counts.closed}`,
    "",
    `Bank Notional Caps:`,
    `Per-trade notional cap: ${formatMoney(perTradeNotionalCap)} (${(perTradeNotionalCapFrac*100).toFixed(0)}% of TC)`,
    `Daily notional cap: ${formatMoney(dailyNotionalCap)} (${(dailyNotionalCapFrac*100).toFixed(0)}% of TC)`,
    "",
    `Fee Rate (per side): ${formatPercent(protocol?.fee_rate ?? 0.001)}`
  ];

  // Optional perf/stat block
  if (extras.stats) {
    const s = extras.stats;
    lines.push(
      "",
      "Performance:",
      `Closed: ${s.total} | Wins: ${s.wins} | Win rate: ${(s.winRate*100).toFixed(1)}%`,
      `Net PnL: ${s.netBps >= 0 ? "+" : ""}${s.netBps} bps (${s.netPct >= 0 ? "+" : ""}${s.netPct.toFixed(2)}%)` + (isFinite(s.netUsdt) ? ` | ~ ${s.netUsdt >= 0 ? "+" : ""}${formatMoney(s.netUsdt)}` : "")
    );
  }

  const info = [];
  if (extras.ideasSource || extras.subreqUsed != null || extras.longShare != null || extras.sqsGate != null || extras.todayOpened != null) {
    info.push("", "Suggester/TCR:");
    if (extras.ideasSource) info.push(`Ideas source: ${extras.ideasSource}`);
    if (extras.subreqUsed != null) info.push(`Subrequests last cycle: ${extras.subreqUsed}`);
    if (extras.longShare != null) info.push(`Bias (long share target): ${(extras.longShare * 100).toFixed(0)}%`);
    if (extras.sqsGate != null) info.push(`Dynamic SQS gate: ${extras.sqsGate.toFixed(2)}`);
    if (extras.todayOpened != null && extras.targetDaily != null) info.push(`Today trades: ${extras.todayOpened}/${extras.targetDaily} (pace control)`);
  }

  lines.push(...info, "", "This is live and updates as trades open/close. Exits: native CID TP/SL when supported, TTL fallback.");
  return lines.join("\n");
}

async function renderProtocolStatus(env, userId, messageId = 0) {
  const protocol = await getProtocolState(env, userId);
  const tc = await getWalletBalance(env, userId);
  const { state } = await loadGistState(env);
  const counts = gistCounts(state);

  let ideasMeta = {};
  try {
    const row = await env.DB.prepare("SELECT ideas_json FROM ideas ORDER BY ts DESC LIMIT 1").first();
    if (row?.ideas_json) {
      const j = JSON.parse(row.ideas_json);
      ideasMeta = {
        ideasSource: j?.meta?.origin || j?.source || "external",
        subreqUsed: j?.meta?.subrequestsUsed,
        longShare: j?.meta?.longShareTarget
      };
    }
  } catch {}

  let sqsGate = null, todayOpened = null, targetDaily = null;
  try {
    sqsGate = await computeUserSqsGate(env, userId);
    const { todayCount, last7Counts } = await getPacingCounters(env, userId);
    todayOpened = todayCount;
    targetDaily = Math.max(1, Math.round((todayOpened + last7Counts.reduce((a,b)=>a+b,0))/7));
  } catch {}

  // Compute performance stats from closed[]
  let stats = null;
  try { stats = computeClosedStatsFromState(state); } catch {}

  const extras = {
    ...ideasMeta,
    sqsGate,
    todayOpened,
    targetDaily,
    perTradeNotionalCapFrac: Number(env?.PER_TRADE_NOTIONAL_CAP_FRAC ?? 0.10),
    dailyNotionalCapFrac: Number(env?.DAILY_OPEN_NOTIONAL_CAP_FRAC ?? 0.30),
    stats
  };

  const text = protocol
    ? protocolStatusTextW1(protocol, tc, counts, extras)
    : "No protocol initialized yet.";
  const buttons = [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }]];
  if (messageId) await editMessage(userId, messageId, text, buttons, env);
  else await sendMessage(userId, text, buttons, env);
}

/* ---------- Manage Trades UX (Worker-1 style) ---------- */
function entryIntentFromIdea(idea) {
  const policy = isFinite(Number(idea?.entry_limit)) ? "limit" : "market";
  const parts = [`policy=${policy}`];
  if (isFinite(Number(idea?.entry_limit))) parts.push(`limit=${formatMoney(Number(idea.entry_limit))}`);
  return parts.join(" | ");
}
function isShortSideIdea(idea) {
  return String(idea?.side || 'long').toLowerCase() === 'short';
}
function symbolFromIdea(idea) {
  return String(idea?.symbolFull || (idea?.base ? String(idea.base).toUpperCase() + "USDT" : "") || "");
}
function stopPctFromIdea(idea, entryPx) {
  const slAbs = Number(idea?.sl_abs || 0);
  const slBps = Number(idea?.sl_bps || 0);
  if (slBps > 0) return slBps / 10000;
  if (!isFinite(entryPx) || entryPx <= 0 || slAbs <= 0) return 0;
  if (!isShortSideIdea(idea)) return Math.max(0, (entryPx - slAbs) / entryPx);
  return Math.max(0, (slAbs / entryPx) - 1);
}
function rrrFromIdea(idea, entryPx) {
  const tpBps = Number(idea?.tp_bps || 0);
  const slBps = Number(idea?.sl_bps || 0);
  if (tpBps > 0 && slBps > 0) return tpBps / slBps;
  const tpAbs = Number(idea?.tp_abs || 0);
  const slAbs = Number(idea?.sl_abs || 0);
  if (!(tpAbs>0) || !(slAbs>0) || !(entryPx>0)) return STRICT_RRR;
  const up = !isShortSideIdea(idea);
  const gain = up ? (tpAbs - entryPx) : (entryPx - tpAbs);
  const risk = up ? (entryPx - slAbs) : (slAbs - entryPx);
  if (!(risk>0)) return STRICT_RRR;
  return Math.max(0.1, gain / risk);
}
function notionalFromIdea(idea) {
  return Number(idea?.notional_usd ?? idea?.notional_usdt ?? 0);
}
function feeRateFromStateOrEnv(state, env) {
  const f = (state?.fees && (state.fees.taker ?? state.fees.maker)) ?? Number(env?.FEE_RATE_PER_SIDE || 0.001);
  return Number(f || 0.001);
}

/* Pending Idea card (Worker-1 look & feel) */
function pendingIdeaCardW1(idea, snapshot) {
  const sym = symbolFromIdea(idea);
  const sideStr = isShortSideIdea(idea) ? 'SELL' : 'BUY';
  const sideMark = sideEmoji(sideStr);
  const entry = isFinite(Number(idea.entry_limit)) ? Number(idea.entry_limit) : Number(idea.entry_mid || 0);
  const stopPct = stopPctFromIdea(idea, entry);
  const rTxt = rrrFromIdea(idea, entry).toFixed(2);
  const notional = notionalFromIdea(idea);
  const feeRate = Number(snapshot?.feeRate || 0.001);
  const required = notional * (1 + 2 * feeRate);
  const available = Number(snapshot?.available || 0);
  const fundsOk = required <= available;
  const riskUsd = notional * stopPct;
  const SQS = Number(idea?.score != null ? Math.min(1, Math.max(0, Math.sqrt((Number(idea.score)||0)/100))) : 0);

  return [
    `Trade Suggestion`,
    "",
    `${sideMark} Symbol: ${sym.replace('USDT','')}`,
    `Direction: ${sideStr === 'SELL' ? 'Short' : 'Long'}`,
    `Entry (intent): ${entryIntentFromIdea(idea)}`,
    `Planned Entry~: ${entry>0?formatMoney(entry):'-'}`,
    `Stop Loss: ${Number(idea?.sl_abs||0)>0?formatMoney(Number(idea.sl_abs)):'-'} (${formatPercent(stopPct)})`,
    `Take Profit: ${Number(idea?.tp_abs||0)>0?formatMoney(Number(idea.tp_abs)):'-'} (RRR ${rTxt}:1)`,
    "",
    `Planned Capital: ${formatMoney(notional)}${available>0?` (${formatPercent(available>0 ? notional/available : 0)} of TC snapshot)`:''}`,
    `Risk (Final): ${formatMoney(riskUsd)}`,
    "",
    `Required: ${formatMoney(required)}`,
    `Available: ${formatMoney(available)}`,
    `Funds: ${fundsOk ? "OK" : "Not enough"}`,
    "",
    `SQS: ${SQS.toFixed(2)}`,
    "",
    "Approve to place the order now, or Reject to skip."
  ].join("\n");
}

/* Open Trade details (Worker-1 look & feel) */
function openTradeDetailsW1(idea, currentPrice, tcSnap) {
  const sym = symbolFromIdea(idea);
  const entry = Number(idea?.entry_price || 0);
  const qty = Number(idea?.qty || 0);
  const short = isShortSideIdea(idea);
  const sideStr = short ? 'SELL' : 'BUY';
  const sideMark = sideEmoji(sideStr);
  const pnlUsd = short ? (entry - currentPrice) * qty : (currentPrice - entry) * qty;
  const stopPct = stopPctFromIdea(idea, entry);
  const riskUsd = Math.max(0, qty * entry * stopPct);
  const rTxt = rrrFromIdea(idea, entry).toFixed(2);

  const notionalAtEntry = entry * qty;
  const pctEntry = tcSnap > 0 ? notionalAtEntry / tcSnap : 0;

  const exitCIDs = {
    cid_root: idea.client_order_id,
    cid_tp: `${idea.client_order_id}:tp`,
    cid_sl: `${idea.client_order_id}:sl`
  };

  const cidLines = [];
  if (exitCIDs.cid_root) cidLines.push(`CID Root: ${exitCIDs.cid_root}`);
  if (exitCIDs.cid_tp)   cidLines.push(`TP CID: ${exitCIDs.cid_tp}`);
  if (exitCIDs.cid_sl)   cidLines.push(`SL CID: ${exitCIDs.cid_sl}`);

  return [
    `Trade`,
    "",
    `${sideMark} Symbol: ${sym.replace('USDT','')}`,
    `Direction: ${short ? 'Short' : 'Long'}`,
    `Entry (intent): ${entryIntentFromIdea(idea)}`,
    `Entry: ${formatMoney(entry)}`,
    `Current: ${formatMoney(currentPrice)}`,
    `Stop: ${Number(idea?.sl_abs||0)>0?formatMoney(Number(idea.sl_abs)):'-'} (${formatPercent(stopPct)})`,
    `Target: ${Number(idea?.tp_abs||0)>0?formatMoney(Number(idea.tp_abs)):'-'} (RRR ${rTxt}:1)`,
    "",
    `Quantity: ${qty.toFixed(6)}`,
    `Capital used at entry: ${formatMoney(notionalAtEntry)} (${formatPercent(pctEntry)} of TC at entry)`,
    `Risk (Final): ${formatMoney(riskUsd)}`,
    "",
    `Live P&L:`,
    `USD: ${pnlUsd >= 0 ? "+" : ""}${formatMoney(pnlUsd)}`,
    "",
    cidLines.join("\n"),
    "SL/TP are enforced by the bot (native CID exits when supported). You can Close Now anytime."
  ].filter(Boolean).join("\n");
}

/* ---------- Keyboards (Worker-1) ---------- */
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

function kbDashboard(mode, paused, hasProtocol, counts) {
  const rows = [];
  rows.push([{ text: "Manage Trades 📋", callback_data: "manage_trades" }]);
  rows.push([{ text: "Protocol Status 🛡️", callback_data: "protocol_status" }]);
  if (mode === 'auto') rows.push([{ text: paused ? "Resume Auto ▶️" : "Pause Auto ⏸️", callback_data: "auto_toggle" }]);
  rows.push([{ text: "Reset History 🧹", callback_data: "reset_history_confirm" }]);
  rows.push([{ text: "Stop Bot ⛔", callback_data: "action_stop_confirm" }]);
  return rows;
}
function kbManageTradesMenu() {
  return [
    [{ text: "Open Trades 📋", callback_data: "mt_open" }],
    [{ text: "Ideas 💡",       callback_data: "mt_ideas" }],
    [{ text: "Continue ▶️",    callback_data: "continue_dashboard" }]
  ];
}
function kbPendingTradeW1(cid, fundsOk) {
  const rows = [];
  if (fundsOk) rows.push([{ text: "Approve ✅", callback_data: `tr_appr:${cid}` }, { text: "Reject ❌", callback_data: `tr_rej:${cid}` }]);
  else rows.push([{ text: "Reject ❌", callback_data: `tr_rej:${cid}` }]);
  rows.push([{ text: "Continue ▶️", callback_data: "continue_dashboard" }]);
  rows.push([{ text: "Back to list", callback_data: "mt_ideas" }], [{ text: "Stop ⛔", callback_data: "action_stop_confirm" }]);
  return rows;
}
function kbOpenTradeStrictW1(cid) {
  return [
    [{ text: "Close Now ⏹️", callback_data: `tr_close:${cid}` }, { text: "Refresh 🔄", callback_data: `tr_refresh:${cid}` }],
    [{ text: "Continue ▶️", callback_data: "continue_dashboard" }],
    [{ text: "Back to list", callback_data: "mt_open" }],
    [{ text: "Stop ⏹️", callback_data: "action_stop_confirm" }]
  ];
}

/* ---------- Dashboard ---------- */
function gistCounts(state) {
  const p = (state.pending||[]);
  const planned = p.filter(x => String(x.status||"planned")==="planned").length;
  const open = p.filter(x => String(x.status||"") === "open").length;
  const closed = (state.closed||[]).length;
  return { planned, open, closed };
}
async function sendDashboard(env, userId, messageId = 0) {
  const session = await getSession(env, userId);
  if (!session) return;
  const { state } = await loadGistState(env);
  const counts = gistCounts(state);

  const paused = (session.auto_paused || "false") === "true";
  const text = (session.bot_mode === "manual")
    ? `Bot is active (Manual).\n\nIdeas (planned): ${counts.planned}\nOpen positions: ${counts.open}\nClosed trades: ${counts.closed}`
    : `Bot is active (Auto).\nStatus: ${paused ? "Paused" : "Running"}.\n\nIdeas (planned): ${counts.planned}\nOpen positions: ${counts.open}\nClosed trades: ${counts.closed}`;

  const buttons = kbDashboard(session.bot_mode, paused, true, counts);
  if (messageId) await editMessage(userId, messageId, text, buttons, env);
  else await sendMessage(userId, text, buttons, env);
}

/* ---------- Manage Trades submenu ---------- */
async function sendManageTradesMenu(env, userId, messageId = 0) {
  const text = "Manage Trades\n\nChoose a list to view:";
  const kb = kbManageTradesMenu();
  if (messageId) await editMessage(userId, messageId, text, kb, env);
  else await sendMessage(userId, text, kb, env);
}

/* ---------- Lists (Worker-1 style but gist-backed; emoji side marker) ---------- */
async function sendOpenTradesListUI(env, userId, page = 1, messageId = 0) {
  const { state } = await loadGistState(env);
  const listAll = (state.pending||[]).filter(x => String(x.status||"") === "open");
  const perPage = 5;
  const start = (page - 1) * perPage;
  const list = listAll.slice(start, start + perPage);

  const header = `Trades — Open (Page ${page})\nTotals: ${listAll.length}\n\n`;
  let body = "";

  if (!list.length) {
    body = "No open trades on this page.";
  } else {
    body = list.map(r => {
      const sym = symbolFromIdea(r).replace('USDT','');
      const sideStr = isShortSideIdea(r) ? 'SELL' : 'BUY';
      const mark = sideEmoji(sideStr);
      const qty = Number(r.qty || 0);
      const stopPct = stopPctFromIdea(r, Number(r.entry_price || 0));
      const rTxt = rrrFromIdea(r, Number(r.entry_price || 0)).toFixed(2);
      return `${mark} ${sym} ${sideStr} ${qty.toFixed(4)} — open | Entry ${formatMoney(r.entry_price)} | SL ${Number(r.sl_abs||0)>0?formatMoney(Number(r.sl_abs)):'-'} (${formatPercent(stopPct)}) | TP ${Number(r.tp_abs||0)>0?formatMoney(Number(r.tp_abs)):'-'} (R ${rTxt})`;
    }).join("\n");
  }

  const text = header + body;
  const buttons = [];
  for (const r of list) buttons.push([{ text: `View ${r.client_order_id}`, callback_data: `tr_view:${r.client_order_id}` }]);
  buttons.push([{ text: "Prev", callback_data: `mt_open_page:${Math.max(1, page - 1)}` }, { text: "Next", callback_data: `mt_open_page:${page + 1}` }]);
  buttons.push([{ text: "Back ◀️", callback_data: "manage_trades" }], [{ text: "Continue ▶️", callback_data: "continue_dashboard" }]);

  if (messageId) await editMessage(userId, messageId, text, buttons, env);
  else await sendMessage(userId, text, buttons, env);
}

async function sendIdeasListUI(env, userId, page = 1, messageId = 0) {
  const { state } = await loadGistState(env);
  const listAll = (state.pending||[]).filter(x => String(x.status||"planned") === "planned");
  const perPage = 5;
  const start = (page - 1) * perPage;
  const list = listAll.slice(start, start + perPage);

  const header = `Ideas — Pending (Page ${page})\nTotals: ${listAll.length}\n\n`;
  let body = "";

  if (!list.length) {
    body = "No pending ideas on this page.";
  } else {
    body = list.map(r => {
      const sym = symbolFromIdea(r).replace('USDT','');
      const sideStr = isShortSideIdea(r) ? 'SELL' : 'BUY';
      const mark = sideEmoji(sideStr);
      const stopPct = stopPctFromIdea(r, Number(r.entry_mid || r.entry_limit || 0));
      const rTxt = rrrFromIdea(r, Number(r.entry_mid || r.entry_limit || 0)).toFixed(2);
      return `${mark} ${sym} ${sideStr} — planned | Entry~ ${formatMoney(Number(r.entry_limit || r.entry_mid || 0))} | SL ${Number(r.sl_abs||0)>0?formatMoney(Number(r.sl_abs)):'-'} (${formatPercent(stopPct)}) | TP ${Number(r.tp_abs||0)>0?formatMoney(Number(r.tp_abs)):'-'} (R ${rTxt})`;
    }).join("\n");
  }

  const text = header + body;
  const buttons = [];
  for (const r of list) buttons.push([{ text: `View ${r.client_order_id}`, callback_data: `tr_view:${r.client_order_id}` }]);
  buttons.push([{ text: "Prev", callback_data: `mt_ideas_page:${Math.max(1, page - 1)}` }, { text: "Next", callback_data: `mt_ideas_page:${page + 1}` }]);
  buttons.push([{ text: "Back ◀️", callback_data: "manage_trades" }], [{ text: "Continue ▶️", callback_data: "continue_dashboard" }]);

  if (messageId) await editMessage(userId, messageId, text, buttons, env);
  else await sendMessage(userId, text, buttons, env);
}

/* ---------- Detail views (Worker-1 style using gist) ---------- */
async function sendOpenTradeDetailsUI(env, userId, cid, messageId = 0) {
  const { state } = await loadGistState(env);
  const idea = (state.pending||[]).find(x => String(x.client_order_id||"") === String(cid) && String(x.status||"") === "open");
  if (!idea) {
    const t = "Trade not found or not open.";
    const b = [[{ text: "Back to list", callback_data: "mt_open" }], [{ text: "Continue ▶️", callback_data: "continue_dashboard" }]];
    if (messageId) await editMessage(userId, messageId, t, b, env);
    else await sendMessage(userId, t, b, env);
    return;
  }
  const tcSnap = await getWalletBalance(env, userId);
  const base = symbolFromIdea(idea).replace('USDT','');
  const currentPrice = await getCurrentPrice(base, idea?.entry_price);
  const text = openTradeDetailsW1(idea, currentPrice, tcSnap);
  const kb = kbOpenTradeStrictW1(cid);
  if (messageId) await editMessage(userId, messageId, text, kb, env);
  else await sendMessage(userId, text, kb, env);
}

async function sendPendingIdeaUI(env, userId, cid, messageId = 0) {
  const { state } = await loadGistState(env);
  const idea = (state.pending||[]).find(x => String(x.client_order_id||"") === String(cid) && String(x.status||"planned") === "planned");
  if (!idea) {
    const t = "Idea not found.";
    const b = [[{ text: "Back to list", callback_data: "mt_ideas" }], [{ text: "Continue ▶️", callback_data: "continue_dashboard" }]];
    if (messageId) await editMessage(userId, messageId, t, b, env);
    else await sendMessage(userId, t, b, env);
    return;
  }

  const { state: s2 } = await loadGistState(env);
  const feeRate = feeRateFromStateOrEnv(s2, env);
  const available = await getWalletBalance(env, userId);
  const card = pendingIdeaCardW1(idea, { feeRate, available });
  const notional = notionalFromIdea(idea);
  const fundsOk = notional * (1 + 2 * feeRate) <= available;
  const kb = kbPendingTradeW1(cid, fundsOk);

  if (messageId) await editMessage(userId, messageId, card, kb, env);
  else await sendMessage(userId, card, kb, env);
}

/* ======================================================================
   SECTION 7/7 — processUser, Telegram handler, HTTP endpoints,
                  Cron with Gist reconciliation, Worker export
   (Updates: Crypto Parrot quick onboarding, Protocol Status action,
    Manage Trades lists with 🟢/🔴 emoji formatting)
   ====================================================================== */

/* ---------- Small helpers (presentation-only) ---------- */
function sideEmojiFromIdea(idea) {
  const s = String(idea?.side || "long").toLowerCase();
  return s === "short" ? "🔴" : "🟢";
}
function ideaIsShort(idea) {
  return String(idea?.side || "long").toLowerCase() === "short";
}
function ideaSymbolBase(idea) {
  const full = String(idea?.symbolFull || "").toUpperCase();
  if (full) return full.replace(/USDT|USDC|USD$/,'') || full;
  if (idea?.base) return String(idea.base).toUpperCase();
  return "";
}
function computeStopPctInline(idea, entryPx) {
  const slAbs = Number(idea?.sl_abs || 0);
  const slBps = Number(idea?.sl_bps || 0);
  if (slBps > 0) return slBps / 10000;
  if (!(entryPx > 0) || !(slAbs > 0)) return 0;
  return !ideaIsShort(idea) ? Math.max(0, (entryPx - slAbs) / entryPx)
                            : Math.max(0, (slAbs / entryPx) - 1);
}
function computeRRRInline(idea, entryPx) {
  const tpBps = Number(idea?.tp_bps || 0);
  const slBps = Number(idea?.sl_bps || 0);
  if (tpBps > 0 && slBps > 0) return Math.max(0.1, tpBps / slBps);
  const tpAbs = Number(idea?.tp_abs || 0);
  const slAbs = Number(idea?.sl_abs || 0);
  if (!(tpAbs > 0) || !(slAbs > 0) || !(entryPx > 0)) return STRICT_RRR;
  const up = !ideaIsShort(idea);
  const gain = up ? (tpAbs - entryPx) : (entryPx - tpAbs);
  const risk = up ? (entryPx - slAbs) : (slAbs - entryPx);
  if (!(risk > 0)) return STRICT_RRR;
  return Math.max(0.1, gain / risk);
}

/* ---------- Protocol Status (presentation-only, gist-backed) ---------- */
async function sendProtocolStatus(env, userId, messageId = 0) {
  try {
    const tc = await getWalletBalance(env, userId);
    const { state } = await loadGistState(env);
    const pending = Array.isArray(state.pending) ? state.pending : [];
    const open = pending.filter(x => String(x.status || "").toLowerCase() === "open");

    // Compute open risk (approx) and open notional from gist
    let openRisk = 0;
    let openNotional = 0;
    for (const p of open) {
      const entryPx = Number(p.entry_price || 0);
      const qty = Number(p.qty || 0);
      if (entryPx > 0 && qty > 0) {
        const sPct = computeStopPctInline(p, entryPx);
        openRisk += (entryPx * qty * sPct);
        openNotional += (entryPx * qty);
      }
    }

    // Caps
    const D = DAILY_OPEN_RISK_CAP_FRAC * tc;
    const D_left = Math.max(0, D - openRisk);

    const perTradeNotionalCapFrac = Number(env?.PER_TRADE_NOTIONAL_CAP_FRAC ?? 0.10);
    const dailyNotionalCapFrac    = Number(env?.DAILY_OPEN_NOTIONAL_CAP_FRAC ?? 0.30);
    const perTradeNotionalCap     = perTradeNotionalCapFrac * tc;
    const dailyNotionalCap        = dailyNotionalCapFrac * tc;
    const dailyNotionalLeft       = Math.max(0, dailyNotionalCap - openNotional);

    // Fee
    const feeRate = (state?.fees && (state.fees.taker ?? state.fees.maker)) ?? Number(env?.FEE_RATE_PER_SIDE || 0.001);

    // Pacing/SQS
    let sqsGate = null, todayOpened = null, targetDaily = null;
    try {
      sqsGate = await computeUserSqsGate(env, userId);
      const { todayCount, last7Counts } = await getPacingCounters(env, userId);
      todayOpened = todayCount;
      const explicitTarget = Number(env?.TCR_TARGET_TRADES || 0);
      if (explicitTarget > 0) targetDaily = explicitTarget;
      else targetDaily = Math.max(1, Math.round((todayCount + last7Counts.reduce((a,b)=>a+b,0))/7));
    } catch {}

    const counts = {
      planned: pending.filter(x => String(x.status || "planned") === "planned").length,
      open: open.length,
      closed: (state.closed || []).length
    };

    // Performance summary
    const stats = computeClosedStatsFromState(state);

    const lines = [
      "Protocol Status (ACP V20, Multi-Position):",
      "",
      `Live Balance (TC): ${formatMoney(tc)}`,
      `ACP (${(ACP_FRAC*100).toFixed(0)}% of TC): ${formatMoney(ACP_FRAC * tc)}`,
      `Per-trade risk cap (M): ${formatMoney(PER_TRADE_CAP_FRAC * tc)}`,
      `Daily open risk cap (D): ${formatMoney(D)} | Used: ${formatMoney(openRisk)} | Left: ${formatMoney(D_left)}`,
      "",
      `Bank Notional Caps:`,
      `Per-trade notional cap: ${formatMoney(perTradeNotionalCap)} (${(perTradeNotionalCapFrac*100).toFixed(0)}% of TC)`,
      `Daily notional cap: ${formatMoney(dailyNotionalCap)} (${(dailyNotionalCapFrac*100).toFixed(0)}% of TC)`,
      `Open notional: ${formatMoney(openNotional)} | Daily left: ${formatMoney(dailyNotionalLeft)}`,
      "",
      `Fee Rate (per side): ${formatPercent(feeRate)}`,
      "",
      `Ideas (planned): ${counts.planned} | Open positions: ${counts.open} | Closed trades: ${counts.closed}`,
      "",
      "Performance:",
      `Closed: ${stats.total} | Wins: ${stats.wins} | Win rate: ${(stats.winRate*100).toFixed(1)}%`,
      `Net PnL: ${stats.netBps >= 0 ? "+" : ""}${stats.netBps} bps (${stats.netPct >= 0 ? "+" : ""}${stats.netPct.toFixed(2)}%)` + (isFinite(stats.netUsdt) ? ` | ~ ${stats.netUsdt >= 0 ? "+" : ""}${formatMoney(stats.netUsdt)}` : "")
    ];

    if (sqsGate != null || todayOpened != null || targetDaily != null) {
      lines.push("", "Suggester/TCR:");
      if (sqsGate != null) lines.push(`Dynamic SQS gate: ${Number(sqsGate).toFixed(2)}`);
      if (todayOpened != null && targetDaily != null) lines.push(`Today trades: ${todayOpened}/${targetDaily} (pace control)`);
    }

    lines.push("", "This is live and updates as trades open/close. Exits: native CID TP/SL when supported, TTL fallback.");
    const text = lines.join("\n");
    const buttons = [
      [{ text: "Manage Trades 📋", callback_data: "manage_trades" }],
      [{ text: "Continue ▶️", callback_data: "continue_dashboard" }]
    ];
    if (messageId) await editMessage(userId, messageId, text, buttons, env);
    else await sendMessage(userId, text, buttons, env);
  } catch (e) {
    await sendMessage(userId, "Failed to render Protocol Status.", null, env);
  }
}

/* ---------- Emoji-enhanced lists (override calls in this section) ---------- */
async function sendOpenTradesListUI_W1Emoji(env, userId, page = 1, messageId = 0) {
  const { state } = await loadGistState(env);
  const listAll = (state.pending||[]).filter(x => String(x.status||"") === "open");
  const perPage = 5;
  const start = (page - 1) * perPage;
  const list = listAll.slice(start, start + perPage);

  const header = `Trades — Open (Page ${page})\nTotals: ${listAll.length}\n\n`;
  let body = "";

  if (!list.length) {
    body = "No open trades on this page.";
  } else {
    body = list.map(r => {
      const emoji = sideEmojiFromIdea(r);
      const sym = ideaSymbolBase(r);
      const side = ideaIsShort(r) ? 'SELL' : 'BUY';
      const qty = Number(r.qty || 0);
      const entryPx = Number(r.entry_price || 0);
      const stopPct = computeStopPctInline(r, entryPx);
      const rTxt = computeRRRInline(r, entryPx).toFixed(2);
      const slTxt = Number(r.sl_abs||0)>0 ? formatMoney(Number(r.sl_abs)) : '-';
      const tpTxt = Number(r.tp_abs||0)>0 ? formatMoney(Number(r.tp_abs)) : '-';
      return `${emoji} ${sym} ${side} ${qty.toFixed(4)} — open | Entry ${formatMoney(entryPx)} | SL ${slTxt} (${formatPercent(stopPct)}) | TP ${tpTxt} (R ${rTxt})`;
    }).join("\n");
  }

  const text = header + body;
  const buttons = [];
  for (const r of list) buttons.push([{ text: `View ${r.client_order_id}`, callback_data: `tr_view:${r.client_order_id}` }]);
  buttons.push([{ text: "Prev", callback_data: `mt_open_page:${Math.max(1, page - 1)}` }, { text: "Next", callback_data: `mt_open_page:${page + 1}` }]);
  buttons.push([{ text: "Back ◀️", callback_data: "manage_trades" }], [{ text: "Continue ▶️", callback_data: "continue_dashboard" }]);

  if (messageId) await editMessage(userId, messageId, text, buttons, env);
  else await sendMessage(userId, text, buttons, env);
}

async function sendIdeasListUI_W1Emoji(env, userId, page = 1, messageId = 0) {
  const { state } = await loadGistState(env);
  const listAll = (state.pending||[]).filter(x => String(x.status||"planned") === "planned");
  const perPage = 5;
  const start = (page - 1) * perPage;
  const list = listAll.slice(start, start + perPage);

  const header = `Ideas — Pending (Page ${page})\nTotals: ${listAll.length}\n\n`;
  let body = "";

  if (!list.length) {
    body = "No pending ideas on this page.";
  } else {
    body = list.map(r => {
      const emoji = sideEmojiFromIdea(r);
      const sym = ideaSymbolBase(r);
      const side = ideaIsShort(r) ? 'SELL' : 'BUY';
      const entryHint = Number(r.entry_limit || r.entry_mid || 0);
      const stopPct = computeStopPctInline(r, entryHint);
      const rTxt = computeRRRInline(r, entryHint).toFixed(2);
      const slTxt = Number(r.sl_abs||0)>0 ? formatMoney(Number(r.sl_abs)) : '-';
      const tpTxt = Number(r.tp_abs||0)>0 ? formatMoney(Number(r.tp_abs)) : '-';
      return `${emoji} ${sym} ${side} — planned | Entry~ ${formatMoney(entryHint)} | SL ${slTxt} (${formatPercent(stopPct)}) | TP ${tpTxt} (R ${rTxt})`;
    }).join("\n");
  }

  const text = header + body;
  const buttons = [];
  for (const r of list) buttons.push([{ text: `View ${r.client_order_id}`, callback_data: `tr_view:${r.client_order_id}` }]);
  buttons.push([{ text: "Prev", callback_data: `mt_ideas_page:${Math.max(1, page - 1)}` }, { text: "Next", callback_data: `mt_ideas_page:${page + 1}` }]);
  buttons.push([{ text: "Back ◀️", callback_data: "manage_trades" }], [{ text: "Continue ▶️", callback_data: "continue_dashboard" }]);

  if (messageId) await editMessage(userId, messageId, text, buttons, env);
  else await sendMessage(userId, text, buttons, env);
}

/* ---------- Global execution credentials (for Gist execution) ---------- */
function getGlobalExecAccount(env) {
  const exName = String(env.GLOBAL_EXCHANGE || env.EXCHANGE || "mexc").toLowerCase().trim();
  const ex = SUPPORTED_EXCHANGES[exName];
  if (!ex || !ex.hasOrders) return null;

  // Demo support: no real keys required
  if (ex.kind === "demoParrot") {
    return { exName, ex, apiKey: "demo", apiSecret: "demo" };
  }

  let apiKey = "";
  let apiSecret = "";

  if (ex.kind === "binanceLike") {
    apiKey    = String(env.BINANCE_API_KEY || env.MEXC_API_KEY || "").trim();
    apiSecret = String(env.BINANCE_SECRET_KEY || env.MEXC_SECRET_KEY || "").trim();
  } else if (ex.kind === "binanceFuturesUSDT") {
    apiKey    = String(env.BINANCE_FUTURES_API_KEY || env.BINANCE_API_KEY || "").trim();
    apiSecret = String(env.BINANCE_FUTURES_SECRET_KEY || env.BINANCE_SECRET_KEY || "").trim();
  }

  if (!apiKey || !apiSecret) return null;
  return { exName, ex, apiKey, apiSecret };
}

/* ---------- Fee rate detection (cache in state.fees) ---------- */
async function detectFeeRates(exec, state) {
  try {
    if (state.fees && (state.fees.maker != null || state.fees.taker != null)) return;
    if (exec.ex.kind === "binanceLike") {
      const v = await verifyBinanceLike(exec.apiKey, exec.apiSecret, exec.ex);
      if (v?.success && v.data?.fees) state.fees = { maker: v.data.fees.maker, taker: v.data.fees.taker };
    } else if (exec.ex.kind === "binanceFuturesUSDT") {
      const v = await verifyBinanceFutures(exec.apiKey, exec.apiSecret, exec.ex);
      if (v?.success && v.data?.feeRate != null) state.fees = { maker: v.data.feeRate, taker: v.data.feeRate };
    } else if (exec.ex.kind === "demoParrot") {
      // Default demo fees
      state.fees = { maker: 0.001, taker: 0.001 };
    }
  } catch {}
}

/* ---------- Reconciliation loop (Gist) ---------- */
async function runGistReconciliation(env) {
  if (!gistEnabled(env)) return { ok: true, changed: false, reason: "gist_disabled" };

  const exec = getGlobalExecAccount(env);
  if (!exec) {
    console.warn("[gist] Missing GLOBAL_EXCHANGE or API keys; set GLOBAL_EXCHANGE and API key/secret envs.");
    return { ok: false, changed: false, reason: "no_exec_creds" };
  }

  const { state, etag } = await loadGistState(env);
  await detectFeeRates(exec, state);

  const mode = await getGlobalMode(env);
  let changed = false;

  const pending = Array.isArray(state.pending) ? [...state.pending] : [];
  for (const idea of pending) {
    try {
      const st = String(idea.status || "planned").toLowerCase();
      const cid = String(idea.client_order_id || "");

      if (st === "planned") {
        if (mode === "auto") {
          const res = await reconcilePendingIdea(env, state, idea, exec);
          if (["closed","open","removed"].includes(res?.status)) changed = true;
        } else {
          const verdict = await getIdeaApproval(env, cid);
          if (verdict === "approve") {
            const res = await reconcilePendingIdea(env, state, idea, exec);
            if (["closed","open","removed"].includes(res?.status)) changed = true;
          } else if (verdict === "reject") {
            removePendingByCID(state, cid);
            changed = true;
          }
        }
      } else if (st === "open") {
        const res = await reconcilePendingIdea(env, state, idea, exec);
        if (["closed","open","removed"].includes(res?.status)) changed = true;
      }
    } catch (e) {
      console.error("[gist] reconcile error:", e?.message || e);
    }
  }

  state.loop_heartbeat_ts = Date.now();
  if (changed) state.lastReconcileTs = Date.now();

  const saveHeartbeatAlways = true;
  if (changed || saveHeartbeatAlways) {
    await saveGistState(env, state, etag);
  }

  return { ok: true, changed, reason: changed ? "updated" : "no_change" };
}

/* ---------- Manual close helper (by CID) ---------- */
async function closeGistCIDNow(env, cid) {
  const exec = getGlobalExecAccount(env);
  if (!exec) throw new Error("no_exec_creds");
  const { state } = await loadGistState(env);
  const idea = (state.pending || []).find(x => String(x.client_order_id||"") === String(cid));
  if (!idea) throw new Error("idea_not_found");
  const base = ideaSymbolBase(idea);
  const qty = Number(idea.qty || 0);
  if (!(qty > 0)) throw new Error("qty_zero");

  // Demo: close immediately at current price and write closed[]
  if (exec.ex.kind === 'demoParrot') {
    const entryPrice = Number(idea.entry_price || 0);
    const entryTsMs = Number(idea.entry_ts_ms || Date.now());
    const currentPrice = await getCurrentPrice(base, entryPrice);
    const exitPrice = Number(currentPrice || entryPrice || 1);
    const exitTsMs = Date.now();
    const short = ideaIsShort(idea);
    const ret = short ? (entryPrice/exitPrice - 1) : (exitPrice/entryPrice - 1);

    const entryNotional = entryPrice * qty;
    const exitNotional  = exitPrice * qty;
    const feeSide = Number((state?.fees && (state.fees.taker ?? state.fees.maker)) ?? env?.FEE_RATE_PER_SIDE ?? 0.001);
    const approx = approxCommissionFromNotional(entryNotional, exitNotional, feeSide);
    const pnl_bps = Math.round(ret * 10000) - approx.commission_bps;

    // Optional MFE/MAE
    let mfe_bps = 0, mae_bps = 0;
    if (envFlag(env, "MFE_MAE_ENABLE", "1") && entryPrice > 0) {
      try {
        const kl1m = await fetchKlinesRange(base, "1m", Number(entryTsMs)-60_000, Number(exitTsMs)+60_000);
        const r = computeMFE_MAE(entryPrice, idea.side, kl1m);
        mfe_bps = r.mfe_bps || 0;
        mae_bps = r.mae_bps || 0;
      } catch {}
    }

    appendClosed(state, {
      symbolFull: idea.symbolFull || `${base}USDT`,
      side: idea.side,
      pnl_bps,
      ts_entry_ms: Number(entryTsMs),
      ts_exit_ms: Number(exitTsMs),
      price_entry: entryPrice,
      price_exit: exitPrice,
      qty,
      reconciliation: "demo_manual_close",
      exit_reason: "manual",
      exit_outcome: (pnl_bps > 0) ? "win" : "loss",
      p_pred: idea.p_lcb,
      p_raw: idea.p_raw,
      calib_key: idea.calib_key,
      regime: idea.regime,
      predicted_snapshot: idea.predicted,
      trade_details: {
        client_order_id: cid,
        maker_taker_entry: { maker: 0, taker: 0 },
        maker_taker_exit:  { maker: 0, taker: 0 },
        commission_bps: approx.commission_bps,
        commission_quote_usdt: approx.commission_quote_usdt,
        fingerprint_entry: null,
        fingerprint_exit:  null
      },
      realized: { tp_hit: false, sl_hit: false, ttl_exit: false, mfe_bps, mae_bps },
      learned: false,
      learned_at_ts: null
    });

    bumpSymStatsReal(state, base, pnl_bps);
    (state.equity ||= []).push({ ts_ms: Number(exitTsMs), pnl_bps, recon: "demo_close" });

    removePendingByCID(state, cid);
    await saveGistState(env, state, null);
    return true;
  }

  // Live exchanges: submit reduce-only market close (TTL style)
  try {
    if (exec.ex.kind === 'binanceLike') {
      await placeSpotTTLMarket(exec.ex, exec.apiKey, exec.apiSecret, base, qty, `${cid}:ttl`);
    } else if (exec.ex.kind === 'binanceFuturesUSDT') {
      const sideClose = ideaIsShort(idea) ? "BUY" : "SELL";
      await placeFutTTLMarket(exec.ex, exec.apiKey, exec.apiSecret, base, sideClose, `${cid}:ttl`);
    }
  } catch (e) {
    console.error("closeGistCIDNow error:", e?.message || e);
    throw e;
  }

  try {
    const { state: s2 } = await loadGistState(env);
    await reconcilePendingIdea(env, s2, idea, exec);
    await saveGistState(env, s2, null);
  } catch {}

  return true;
}

/* ---------- Reset history helper ---------- */
async function resetHistory(env, userId) {
  const { state, etag } = await loadGistState(env);
  const openExists = (state.pending || []).some(p => String(p.status || "") === "open");
  if (openExists) throw new Error("open_positions_exist");

  state.pending = [];
  state.closed = [];
  state.equity = [];
  state.sym_stats_real = {};
  // optional: keep state.fees as-is; or reset if you want
  state.lastReconcileTs = Date.now();
  await saveGistState(env, state, etag);

  // Clear pacing and peak equity KV for this user
  try {
    await kvSet(env, `tcr_today_date_${userId}`, "");
    await kvSet(env, `tcr_today_count_${userId}`, 0);
    await kvSet(env, `tcr_7d_roll_${userId}`, JSON.stringify({ last7: [] }));
  } catch {}
  try {
    await kvSet(env, `peak_equity_user_${userId}`, 0);
  } catch {}

  // Optional: wipe user-scoped DB rows for trades/events
  try {
    await env.DB.prepare("DELETE FROM trades WHERE user_id = ?").bind(userId).run();
  } catch {}
  try {
    await env.DB.prepare("DELETE FROM events_log WHERE user_id = ?").bind(userId).run();
  } catch {}

  return true;
}

/* ---------- Telegram handler (wizard + UX) ---------- */
async function handleTelegramUpdate(update, env, ctx) {
  const isCb = !!update.callback_query;
  const msg = isCb ? update.callback_query.message : update.message;
  if (!msg) return;

  const userId = msg.chat.id;
  const text = isCb ? update.callback_query.data : (update.message?.text || "");
  const messageId = isCb ? update.callback_query.message.message_id : (update.message?.message_id || 0);
  if (isCb) ctx.waitUntil(answerCallbackQuery(env, update.callback_query.id));

  let session = await getSession(env, userId);
  if (!session) {
    if (text !== "/start") {
      await sendMessage(userId, "Session expired. Send /start to begin.", null, env);
      return;
    }
    session = await createSession(env, userId);
  }

  // Quick nav
  if (text === "continue_dashboard" || text === "back_dashboard") { await sendDashboard(env, userId, isCb ? messageId : 0); return; }

  // Manage Trades submenu
  if (text === "manage_trades") { await sendManageTradesMenu(env, userId, isCb ? messageId : 0); return; }
  if (text === "mt_open")       { await sendOpenTradesListUI_W1Emoji(env, userId, 1, isCb ? messageId : 0); return; }
  if (text === "mt_ideas")      { await sendIdeasListUI_W1Emoji(env, userId, 1, isCb ? messageId : 0); return; }
  if (text.startsWith("mt_open_page:")) {
    const page = parseInt(text.split(":")[1] || "1", 10);
    await sendOpenTradesListUI_W1Emoji(env, userId, Math.max(1, page), isCb ? messageId : 0);
    return;
  }
  if (text.startsWith("mt_ideas_page:")) {
    const page = parseInt(text.split(":")[1] || "1", 10);
    await sendIdeasListUI_W1Emoji(env, userId, Math.max(1, page), isCb ? messageId : 0);
    return;
  }

  // W1-style trade actions mapped to gist
  if (text.startsWith("tr_view:")) {
    const cid = text.split(":")[1];
    const { state } = await loadGistState(env);
    const idea = (state.pending||[]).find(x => String(x.client_order_id||"") === String(cid));
    if (!idea) { await sendMessage(userId, "Trade/Idea not found.", null, env); return; }
    const st = String(idea.status||"planned");
    if (st === "open") await sendOpenTradeDetailsUI(env, userId, cid, isCb ? messageId : 0);
    else await sendPendingIdeaUI(env, userId, cid, isCb ? messageId : 0);
    return;
  }
  if (text.startsWith("tr_refresh:")) {
    const cid = text.split(":")[1];
    const { state } = await loadGistState(env);
    const idea = (state.pending||[]).find(x => String(x.client_order_id||"") === String(cid));
    if (!idea) { await sendMessage(userId, "Trade/Idea not found.", null, env); return; }
    const st = String(idea.status||"planned");
    if (st === "open") await sendOpenTradeDetailsUI(env, userId, cid, isCb ? messageId : 0);
    else await sendPendingIdeaUI(env, userId, cid, isCb ? messageId : 0);
    return;
  }
  if (text.startsWith("tr_close:")) {
    const cid = text.split(":")[1];
    try {
      await closeGistCIDNow(env, cid);
      await sendMessage(userId, `Close submitted for ${cid}.`, null, env);
      ctx.waitUntil(runGistReconciliation(env));
      await sendOpenTradeDetailsUI(env, userId, cid, isCb ? messageId : 0);
    } catch (e) {
      await sendMessage(userId, `Close failed: ${e?.message || 'error'}`, null, env);
    }
    return;
  }
  if (text.startsWith("tr_appr:")) {
    const cid = text.split(":")[1];
    await setIdeaApproval(env, cid, "approve");
    await sendMessage(userId, `Approved ${cid}. I’ll try to execute it now.`, null, env);
    ctx.waitUntil(runGistReconciliation(env));
    await sendPendingIdeaUI(env, userId, cid, isCb ? messageId : 0);
    return;
  }
  if (text.startsWith("tr_rej:")) {
    const cid = text.split(":")[1];
    await setIdeaApproval(env, cid, "reject");
    try {
      const { state } = await loadGistState(env);
      removePendingByCID(state, cid);
      await saveGistState(env, state, null);
    } catch {}
    await sendMessage(userId, `Rejected ${cid}.`, null, env);
    await sendIdeasListUI_W1Emoji(env, userId, 1, isCb ? messageId : 0);
    return;
  }

  // Gist-native actions (kept; route to W1 views)
  if (text === "g_list_planned") { await sendIdeasListUI_W1Emoji(env, userId, 1, isCb ? messageId : 0); return; }
  if (text === "g_list_open")    { await sendOpenTradesListUI_W1Emoji(env, userId, 1, isCb ? messageId : 0); return; }
  if (text.startsWith("g_view:"))  { const cid = text.split(":")[1]; await handleTelegramUpdate({ callback_query: { id: update.callback_query?.id, message: msg, data: `tr_view:${cid}` } }, env, ctx); return; }
  if (text.startsWith("g_appr:"))  { const cid = text.split(":")[1]; await handleTelegramUpdate({ callback_query: { id: update.callback_query?.id, message: msg, data: `tr_appr:${cid}` } }, env, ctx); return; }
  if (text.startsWith("g_rej:"))   { const cid = text.split(":")[1]; await handleTelegramUpdate({ callback_query: { id: update.callback_query?.id, message: msg, data: `tr_rej:${cid}` } }, env, ctx); return; }
  if (text.startsWith("g_close:")) { const cid = text.split(":")[1]; await handleTelegramUpdate({ callback_query: { id: update.callback_query?.id, message: msg, data: `tr_close:${cid}` } }, env, ctx); return; }

  // Protocol Status
  if (text === "protocol_status" || text === "/report") {
    await sendProtocolStatus(env, userId, isCb ? messageId : 0);
    return;
  }

  // Mode toggles
  if (text === "auto_toggle") {
    const paused = (session.auto_paused || "false") === "true";
    await saveSession(env, userId, { auto_paused: paused ? "false" : "true" });
    await sendDashboard(env, userId, isCb ? messageId : 0);
    return;
  }
  if (text === "/manual") { await setGlobalMode(env, "manual"); await sendMessage(userId, "Global mode set to MANUAL.", null, env); return; }
  if (text === "/auto")   { await setGlobalMode(env, "auto");   await sendMessage(userId, "Global mode set to AUTO.", null, env);   return; }

  // Reset History flow
  if (text === "reset_history_confirm") {
    const { state } = await loadGistState(env);
    const openExists = (state.pending || []).some(p => String(p.status || "") === "open");
    if (openExists) {
      const t = "You have open positions. Please close them before resetting history.\nYou can close all now, or cancel.";
      const b = [
        [{ text: "Close all now ⏹️", callback_data: "action_stop_closeall" }],
        [{ text: "Cancel", callback_data: "reset_history_cancel" }],
        [{ text: "Continue ▶️", callback_data: "continue_dashboard" }]
      ];
      if (isCb && messageId) await editMessage(userId, messageId, t, b, env);
      else await sendMessage(userId, t, b, env);
    } else {
      const t = "Reset History will clear all pending ideas, closed trades, equity timeline, symbol stats, pacing counters, and local DB logs for your user.\nAre you sure?";
      const b = [
        [{ text: "Yes, reset now 🧹", callback_data: "reset_history_do" }, { text: "Cancel", callback_data: "reset_history_cancel" }],
        [{ text: "Continue ▶️", callback_data: "continue_dashboard" }]
      ];
      if (isCb && messageId) await editMessage(userId, messageId, t, b, env);
      else await sendMessage(userId, t, b, env);
    }
    return;
  }
  if (text === "reset_history_do") {
    try {
      await resetHistory(env, userId);
      await sendMessage(userId, "History reset completed.", null, env);
      await sendDashboard(env, userId, isCb ? messageId : 0);
    } catch (e) {
      if (String(e?.message || "").includes("open_positions_exist")) {
        await sendMessage(userId, "Cannot reset: open positions exist. Close them first.", null, env);
      } else {
        await sendMessage(userId, `Reset failed: ${e?.message || "error"}`, null, env);
      }
    }
    return;
  }
  if (text === "reset_history_cancel") {
    await sendDashboard(env, userId, isCb ? messageId : 0);
    return;
  }

  // Stop actions
  if (text === "action_stop_confirm") {
    const { state } = await loadGistState(env);
    const open = (state.pending||[]).some(p => String(p.status||"") === "open");
    if (open) {
      const t = "You have open positions and cannot stop until they are closed.\nYou can close all now, or continue and manage them individually.";
      const b = [[{ text: "Close all now ⏹️", callback_data: "action_stop_closeall" }],[{ text: "Continue ▶️", callback_data: "continue_dashboard" }]];
      if (isCb && messageId) await editMessage(userId, messageId, t, b, env); else await sendMessage(userId, t, b, env);
    } else {
      await handleDirectStop(env, userId);
    }
    return;
  }
  if (text === "action_stop_closeall") {
    const { state } = await loadGistState(env);
    const open = (state.pending||[]).filter(p => String(p.status||"") === "open");
    let closed = 0;
    for (const idea of open) {
      try { await closeGistCIDNow(env, idea.client_order_id); closed++; } catch {}
    }
    await sendMessage(userId, `Close-all submitted (${closed}).`, null, env);
    return;
  }

  // Wizard: /start
  if (text === "/start") {
    await saveSession(env, userId, { current_step: 'awaiting_accept', status: 'initializing' });
    const buttons = [[{ text: "Accept ✅", callback_data: "action_accept_terms" }], [{ text: "Stop Bot ⛔", callback_data: "action_stop_confirm" }]];
    await sendMessage(userId, welcomeText(), buttons, env);
    return;
  }
  if (text === "action_accept_terms") {
    await saveSession(env, userId, { current_step: 'awaiting_mode' });
    const buttons = [[{ text: "Manual (approve each idea)", callback_data: "mode_manual" }],[{ text: "Fully Automatic", callback_data: "mode_auto" }]];
    await (isCb ? editMessage(userId, messageId, modeText(), buttons, env) : sendMessage(userId, modeText(), buttons, env));
    return;
  }
  if (text === "mode_manual" || text === "mode_auto") {
    const mode = text === "mode_manual" ? "manual" : "auto";
    await setGlobalMode(env, mode);
    await saveSession(env, userId, { bot_mode: mode, current_step: 'awaiting_exchange' });
    await (isCb ? editMessage(userId, messageId, exchangeText(), exchangeButtons(), env) : sendMessage(userId, exchangeText(), exchangeButtons(), env));
    return;
  }

  // Exchanges (special: Crypto Parrot quick onboarding)
  if (text.startsWith("exchange_")) {
    const exKey = text.slice("exchange_".length);
    if (!SUPPORTED_EXCHANGES[exKey]) { await sendMessage(userId, "Unsupported exchange.", null, env); return; }

    if (exKey === "crypto_parrot") {
      // Demo: auto-provision keys and jump to confirm screen (W1 UX)
      const demoKey = "demo";
      const demoSecret = "demo";
      const feeRate = 0.001;
      const detectedBal = 10000.00;

      await saveSession(env, userId, {
        exchange_name: exKey,
        api_key_encrypted: await encrypt(demoKey, env),
        api_secret_encrypted: await encrypt(demoSecret, env),
        temp_api_key: null,
        current_step: (session.bot_mode === 'manual') ? 'confirm_manual' : 'confirm_auto'
      });
      await initProtocolDynamic(env, userId, feeRate, 0.01);

      const t = (session.bot_mode === 'manual') ? confirmManualTextB(detectedBal, feeRate) : confirmAutoTextB(detectedBal, feeRate);
      const b = (session.bot_mode === 'manual')
        ? [[{ text: "Start Manual ▶️", callback_data: "action_start_manual" }]]
        : [[{ text: "Yes, Start Auto 🚀", callback_data: "action_start_auto" }]];
      if (isCb && messageId) await editMessage(userId, messageId, t, b, env); else await sendMessage(userId, t, b, env);
      return;
    }

    // Normal exchanges → ask API key
    await saveSession(env, userId, { exchange_name: exKey, current_step: 'awaiting_api_key' });
    const buttons = [[{ text: "Back ◀️", callback_data: "action_back_to_mode" }]];
    await (isCb ? editMessage(userId, messageId, askApiKeyText(exKey), buttons, env) : sendMessage(userId, askApiKeyText(exKey), buttons, env));
    return;
  }

  if (text === "action_back_to_mode") {
    await saveSession(env, userId, { current_step: 'awaiting_mode' });
    const buttons = [[{ text: "Manual (approve each idea)", callback_data: "mode_manual" }],[{ text: "Fully Automatic", callback_data: "mode_auto" }]];
    await (isCb ? editMessage(userId, messageId, modeText(), buttons, env) : sendMessage(userId, modeText(), buttons, env));
    return;
  }

  // Handle "Refresh" in exchange step to restart that section cleanly
  if (text === "action_refresh_wizard") {
    await saveSession(env, userId, { current_step: 'awaiting_exchange' });
    await (isCb ? editMessage(userId, messageId, exchangeText(), exchangeButtons(), env) : sendMessage(userId, exchangeText(), exchangeButtons(), env));
    return;
  }

  // Keys input (messages only)
  if (!isCb && session.current_step === "awaiting_api_key") {
    await saveSession(env, userId, { temp_api_key: text, current_step: 'awaiting_api_secret' });
    const buttons = [[{ text: "Back ◀️", callback_data: "action_back_to_mode" }]];
    await sendMessage(userId, askApiSecretText(), buttons, env);
    return;
  }
  if (!isCb && session.current_step === "awaiting_api_secret") {
    const tempKey = session.temp_api_key;
    const exName = session.exchange_name;
    if (!tempKey || !exName) { await sendMessage(userId, "Missing step context. Restart with /start.", null, env); return; }
    try {
      const result = await verifyApiKeys(tempKey, text, exName);
      if (!result.success) {
        const buttons = [[{ text: "Try again", callback_data: "action_back_to_mode" }]];
        await sendMessage(userId, `Key check failed.\nReason: ${result.reason || "Unknown error"}`, buttons, env);
        return;
      }
      const feeRate = result.data.feeRate ?? 0.001;
      const detectedBal = parseFloat(String(result.data.balance || "0").replace(" USDT",""));
      await saveSession(env, userId, {
        api_key_encrypted: await encrypt(tempKey, env),
        api_secret_encrypted: await encrypt(text, env),
        temp_api_key: null,
        current_step: session.bot_mode === 'manual' ? 'confirm_manual' : 'confirm_auto'
      });
      await initProtocolDynamic(env, userId, feeRate, 0.01);
      const t = session.bot_mode === 'manual' ? confirmManualTextB(detectedBal, feeRate) : confirmAutoTextB(detectedBal, feeRate);
      const b = session.bot_mode === 'manual'
        ? [[{ text: "Start Manual ▶️", callback_data: "action_start_manual" }]]
        : [[{ text: "Yes, Start Auto 🚀", callback_data: "action_start_auto" }]];
      await sendMessage(userId, t, b, env);
      return;
    } catch (e) {
      await sendMessage(userId, "Key check failed (internal).", null, env);
      return;
    }
  }
  if (text === "action_start_manual" || text === "action_start_auto") {
    const auto = text === "action_start_auto";
    await saveSession(env, userId, { status: 'active', auto_paused: auto ? 'false' : session.auto_paused, current_step: auto ? 'active_auto_running' : 'active_manual_running' });
    await sendDashboard(env, userId, isCb ? messageId : 0);
    return;
  }

  // Optional W1-like slash commands (aliases)
  if (text === "/trades") { await sendManageTradesMenu(env, userId); return; }
  if (text.startsWith("/refresh")) { const cid = (text.split(/\s+/)[1]||"").trim(); if (cid) await handleTelegramUpdate({ callback_query: { id: update.callback_query?.id, message: msg, data: `tr_refresh:${cid}` } }, env, ctx); else await sendMessage(userId, "Usage: /refresh <CID>", null, env); return; }
  if (text.startsWith("/close"))   { const cid = (text.split(/\s+/)[1]||"").trim(); if (cid) await handleTelegramUpdate({ callback_query: { id: update.callback_query?.id, message: msg, data: `tr_close:${cid}` } }, env, ctx); else await sendMessage(userId, "Usage: /close <CID>", null, env); return; }

  // Default: show dashboard
  await sendDashboard(env, userId, isCb ? messageId : 0);
}

/* ---------- HTTP endpoints ---------- */
async function handleHealth() {
  return new Response(JSON.stringify({ ok: true, time: nowISO() }), { headers: { "Content-Type": "application/json" } });
}
async function handleGistState(env, request) {
  const token = String(env.TASK_TOKEN || "").trim();
  const auth = request.headers.get("Authorization") || "";
  if (token && (!auth.startsWith("Bearer ") || auth.split(" ")[1] !== token)) {
    return new Response("Unauthorized", { status: 401 });
  }
  const { state } = await loadGistState(env);
  return new Response(JSON.stringify(state), { headers: { "Content-Type": "application/json" } });
}
async function handleStateMirror(env, request) {
  const token = String(env.TASK_TOKEN || "").trim();
  const auth = request.headers.get("Authorization") || "";
  if (token && (!auth.startsWith("Bearer ") || auth.split(" ")[1] !== token)) {
    return new Response("Unauthorized", { status: 401 });
  }
  let db = {};
  try {
    const total   = (await env.DB.prepare("SELECT COUNT(*) AS c FROM trades").first())?.c || 0;
    const open    = (await env.DB.prepare("SELECT COUNT(*) AS c FROM trades WHERE status='open'").first())?.c || 0;
    const pending = (await env.DB.prepare("SELECT COUNT(*) AS c FROM trades WHERE status='pending'").first())?.c || 0;
    const closed  = (await env.DB.prepare("SELECT COUNT(*) AS c FROM trades WHERE status='closed'").first())?.c || 0;
    const rejected= (await env.DB.prepare("SELECT COUNT(*) AS c FROM trades WHERE status='rejected'").first())?.c || 0;
    const users   = (await env.DB.prepare("SELECT COUNT(*) AS u FROM user_sessions WHERE status IN ('active','halted')").first())?.u || 0;
    db = { counts: { total, open, pending, closed, rejected }, users };
  } catch (e) {
    db = { error: e?.message || "db_error" };
  }
  const { state } = await loadGistState(env);
  return new Response(JSON.stringify({ gist: state, db, ts: nowISO() }), { headers: { "Content-Type": "application/json" } });
}
async function handleGistReconcile(env, request) {
  const token = String(env.TASK_TOKEN || "").trim();
  const auth = request.headers.get("Authorization") || "";
  if (token && (!auth.startsWith("Bearer ") || auth.split(" ")[1] !== token)) {
    return new Response("Unauthorized", { status: 401 });
  }
  const res = await runGistReconciliation(env);
  return new Response(JSON.stringify({ ok: res.ok, changed: res.changed, ts: nowISO(), reason: res.reason || "ok" }), { headers: { "Content-Type": "application/json" } });
}

/* ---------- Cron with lock: Gist reconciliation every run ---------- */
async function runCron(env) {
  const now = Date.now();
  const existing = await env.DB.prepare(
    "SELECT value FROM kv_state WHERE key = ? AND CAST(value AS INTEGER) > ?"
  ).bind(CRON_LOCK_KEY, now - CRON_LOCK_TTL * 1000).first();

  if (existing) {
    console.log("Cron already running; skipping.");
    return;
  }

  await env.DB.prepare("INSERT OR REPLACE INTO kv_state (key, value) VALUES (?, ?)").bind(CRON_LOCK_KEY, String(now)).run();

  try {
    await runGistReconciliation(env);
  } catch (e) {
    console.error("runCron error:", e);
  } finally {
    await env.DB.prepare("DELETE FROM kv_state WHERE key = ?").bind(CRON_LOCK_KEY).run();
  }
}

/* ---------- Worker export ---------- */
export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      if (url.pathname === "/health" && request.method === "GET") {
        return await handleHealth();
      }
      if (url.pathname === "/telegram" && request.method === "POST") {
        const update = await request.json();
        await handleTelegramUpdate(update, env, ctx);
        return new Response("OK");
      }
      if (url.pathname === "/gist/state" && request.method === "GET") {
        return await handleGistState(env, request);
      }
      if (url.pathname === "/state" && request.method === "GET") {
        return await handleStateMirror(env, request);
      }
      if (url.pathname === "/gist/reconcile" && request.method === "POST") {
        return await handleGistReconcile(env, request);
      }

      // Optional: ideas push passthrough (for observability)
      if (url.pathname === "/signals/push" && request.method === "POST") {
        const expected = String(env.PUSH_TOKEN || "").trim();
        const auth = request.headers.get("Authorization") || "";
        if (expected && auth !== `Bearer ${expected}`) return new Response("Unauthorized", { status: 401 });

        let ideas;
        try { ideas = await request.json(); } catch { return new Response("Bad JSON", { status: 400 }); }
        try {
          await env.DB.prepare("INSERT INTO ideas (ts, mode, ideas_json) VALUES (?, ?, ?)").bind(ideas.ts || nowISO(), ideas.mode || 'normal', JSON.stringify(ideas)).run();
          return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });
        } catch (e) {
          return new Response("DB error", { status: 500 });
        }
      }

      // Compatibility endpoint (fan-out no-op)
      if (url.pathname === "/task/process-user" && request.method === "POST") {
        const token = String(env.TASK_TOKEN || "").trim();
        const auth = request.headers.get("Authorization") || "";
        const good = token ? (auth.startsWith("Bearer ") && auth.split(" ")[1] === token) : true;
        if (!good) return new Response("Unauthorized", { status: 401 });
        const uid = url.searchParams.get("uid") || "global";
        ctx.waitUntil(runGistReconciliation(env));
        return new Response(JSON.stringify({ ok: true, uid }), { headers: { "Content-Type": "application/json" } });
      }

      return new Response("Not found", { status: 404 });
    } catch (e) {
      console.error("fetch error:", e);
      return new Response("Error", { status: 500 });
    }
  },

  async scheduled(_event, env, _ctx) {
    await runCron(env);
  },
};

