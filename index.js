/* ======================================================================
Exchange Server Worker — External Ideas Only + Fan-out Fallback
(Gist ledger owner of pending/closed/sym_stats_real; CID exits + TTL + MFE/MAE)
SECTION 1/7 — Constants, Utils, Crypto, DB, Telegram, Gist helpers
====================================================================== */

/* ---------- Constants (Exchanges) ---------- */
const SUPPORTED_EXCHANGES = {
crypto_parrot: { label: "Crypto Parrot (Demo)", kind: "demoParrot", hasOrders: true },

// Spot (Binance-like REST shape)
mexc: { label: "MEXC", kind: "binanceLike", baseUrl: "https://api.mexc.com", accountPath: "/api/v3/account", apiKeyHeader: "X-MEXC-APIKEY", defaultQuery: "", hasOrders: true },
binance: { label: "Binance", kind: "binanceLike", baseUrl: "https://api.binance.com", accountPath: "/api/v3/account", apiKeyHeader: "X-MBX-APIKEY", defaultQuery: "recvWindow=5000", hasOrders: true },

// Others (legacy DB/manual mode if kept)
lbank: { label: "LBank", kind: "lbankV2", baseUrl: "https://api.lbkex.com", hasOrders: true },
coinex: { label: "CoinEx", kind: "coinexV2", baseUrl: "https://api.coinex.com", hasOrders: true },

// Futures / Margin (USDT-M)
binance_futures: { label: "Binance Futures (USDT-M)", kind: "binanceFuturesUSDT", baseUrl: "https://fapi.binance.com", apiKeyHeader: "X-MBX-APIKEY", hasOrders: true },

// Read-only
bybit: { label: "Bybit", kind: "bybitV5", baseUrl: "https://api.bybit.com", hasOrders: false },
kraken: { label: "Kraken", kind: "krakenV0", baseUrl: "https://api.kraken.com", hasOrders: false },
gate: { label: "Gate", kind: "gateV4", baseUrl: "https://api.gateio.ws", hasOrders: false },
huobi: { label: "Huobi", kind: "huobiV1", baseHost: "api.huobi.pro", scheme: "https", hasOrders: false }
};

/* ---------- Brain & Risk Parameters ---------- */
const STRICT_RRR = 2.0;
const ACP_FRAC = 0.80;
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
const MIN_STOP_PCT = 0.0025;
const SQS_MIN_DEFAULT = 0.30;

/* ---------- System-level ---------- */
const CRON_LOCK_KEY = "cron_running";
const CRON_LOCK_TTL = 55; // seconds

const DEFAULT_MAX_CONCURRENT_POS = 3;
const DEFAULT_MAX_NEW_POSITIONS_PER_CYCLE = 3;

/* Feature toggles (env) */
function envFlag(env, key, def = "0") { return String(env?.[key] ?? def) === "1"; }
function envNum(env, key, def) { const v = Number(env?.[key]); return Number.isFinite(v) ? v : def; }
// TTL/MAE/MFE toggles (default ON)
function ENABLE_TTL(env) { return envFlag(env, "ENABLE_TTL_MARKET_CLOSE", "1"); }
function ENABLE_MFE_MAE(env) { return envFlag(env, "MFE_MAE_ENABLE", "1"); }

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
const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
const clamp01 = (x) => Math.max(0, Math.min(1, x));
const clampRange = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
const sigmoid = (x) => 1 / (1 + Math.exp(-x));
function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }
const bpsToFrac = (bps) => Number(bps || 0) / 10000;

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
const DEFAULT_FETCH_TIMEOUT_MS = 3000;
async function safeFetch(url, options = {}, timeoutMs = DEFAULT_FETCH_TIMEOUT_MS) {
const ctrl = new AbortController();
const to = setTimeout(() => ctrl.abort("timeout"), timeoutMs);
try { return await fetch(url, { ...options, signal: ctrl.signal }); }
finally { clearTimeout(to); }
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
const body = { chat_id: chatId, text };
if (buttons && buttons.length) body.reply_markup = { inline_keyboard: buttons };
const r = await safeFetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }, 6000);
if (!r.ok) console.error("sendMessage error:", await r.text());
}
async function editMessage(chatId, messageId, newText, buttons, env) {
if (!env.TELEGRAM_BOT_TOKEN) return;
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

/* ---------- DB helpers (legacy UI/manual/auto) ---------- */
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
`INSERT OR REPLACE INTO protocol_state (user_id, initial_capital, acp_balance, pr_balance, trigger_threshold, phase, risk_pct, fee_rate, created_at, updated_at) VALUES (?, 0, 0, 0, 0, 'normal', ?, ?, ?, ?)`
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
Gist helpers (state.json) — Worker is execution truth writer
====================================================================== */
function gistEnabled(env) {
return Boolean(String(env.GIST_ID || "").trim() && String(env.GIST_TOKEN || "").trim());
}

// derive base from "BTCUSDT"/"BTCUSD" style
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
if (!Number.isFinite(Number(state.loop_heartbeat_ts))) state.loop_heartbeat_ts = 0; // heartbeat for liveness
if (!Number.isFinite(Number(state.version_ts))) state.version_ts = Date.now();
return state;
}

async function loadGistState(env) {
if (!gistEnabled(env)) {
return { state: normalizeGistState({}), rev: null, etag: null };
}
const id = String(env.GIST_ID).trim();
const token = String(env.GIST_TOKEN).trim();
const r = await safeFetch(`https://api.github.com/gists/${id}`, {
headers: { "Authorization": `Bearer ${token}`, "User-Agent":"worker", "Accept":"application/vnd.github+json" }
}, 10000);
const etag = r.headers.get("ETag") || null;
const g = await r.json().catch(()=> ({}));
const content = g?.files?.["state.json"]?.content || "";
let parsed = {};
try { parsed = content ? JSON.parse(content) : {}; } catch { parsed = {}; }
const state = normalizeGistState(parsed);
const rev = g?.history?.[0]?.version || null;
return { state, rev, etag };
}

// recompute sym_stats_real strictly from closed[]
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

// Merge state with conflict safety — recompute sym_stats_real from closed union
function mergeGistState(remote, local) {
const out = normalizeGistState({ ...remote });

// pending by CID
const byCid = new Map();
for (const p of (remote.pending || [])) if (p?.client_order_id) byCid.set(p.client_order_id, p);
for (const p of (local.pending || [])) {
const cid = p?.client_order_id;
if (!cid) continue;
const prev = byCid.get(cid);
// prefer local updates; shallow-merge to preserve fields (e.g., exits)
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

// equity append-union (by ts_ms+pnl)
const eqSeen = new Set();
out.equity = [];
const pushEQ = (arr)=>{ for(const e of (arr||[])){ const k=`${e?.ts_ms||""}|${e?.pnl_bps||""}`; if(eqSeen.has(k)) continue; eqSeen.add(k); out.equity.push(e);} };
pushEQ(remote.equity); pushEQ(local.equity);

// sym_stats_real recompute from unioned closed (avoids double counting on conflicts)
out.sym_stats_real = computeSymStatsRealFromClosed(out.closed);

// carry over fees if present (prefer latest local)
out.fees = (local && local.fees) ? local.fees : (remote && remote.fees ? remote.fees : undefined);

// lastReconcileTs/version_ts keep the latest; heartbeat is set by caller
out.lastReconcileTs = Math.max(Number(remote.lastReconcileTs||0), Number(local.lastReconcileTs||0));
out.version_ts = Date.now();
return out;
}

// Save with If-Match and conflict merge (1 retry)
async function saveGistState(env, state, etag = null, attempt = 1) {
if (!gistEnabled(env)) return false;
const id = String(env.GIST_ID).trim();
const token = String(env.GIST_TOKEN).trim();
state.version_ts = Date.now();

const body = { files: { "state.json": { content: JSON.stringify(state, null, 2) } } };
const headers = { "Authorization": `Bearer ${token}`, "Content-Type": "application/json", "Accept":"application/vnd.github+json" };
if (etag) headers["If-Match"] = etag;

const r = await safeFetch(`https://api.github.com/gists/${id}`, { method: "PATCH", headers, body: JSON.stringify(body) }, 12000);

if (r.status === 412 && attempt < 2) {
// conflict; re-read, merge, retry once
const latest = await loadGistState(env);
const merged = mergeGistState(latest.state, state);
return saveGistState(env, merged, latest.etag, attempt + 1);
}
return r.ok;
}

/* Update helpers — used by trade open/close paths */
function updatePendingOpenByCID(state, cid, updates) {
if (!cid) return false;
const idx = (state.pending || []).findIndex(p => p?.client_order_id === cid);
if (idx === -1) return false;
const cur = state.pending[idx] || {};
// shallow merge; keep nested exits fields when provided
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
// keep sym_stats_real consistent incrementally; recomputed on merges anyway
const base = baseFromSymbolFull(row?.symbolFull || "");
if (base) {
if (!state.sym_stats_real) state.sym_stats_real = {};
const s = state.sym_stats_real[base] || { n: 0, wins: 0, pnl_sum: 0 };
const pnl = Number(row?.pnl_bps || 0);
s.n += 1; if (pnl > 0) s.wins += 1; s.pnl_sum += pnl;
state.sym_stats_real[base] = s;
}
}

/* Symbol stats updater (real fills) */
function bumpSymStatsReal(state, base, pnl_bps) {
if (!state.sym_stats_real) state.sym_stats_real = {};
const s = state.sym_stats_real[base] || { n: 0, wins: 0, pnl_sum: 0 };
s.n++;
if ((pnl_bps || 0) > 0) s.wins++;
s.pnl_sum += (pnl_bps || 0);
state.sym_stats_real[base] = s;
}

/* Commission approximator (quote USDT; returns { commission_quote_usdt, commission_bps }) */
function approxCommissionFromNotional(entryNotional, exitNotional, perSideFeeRate) {
const feeEntry = Math.max(0, Number(entryNotional || 0)) * Math.max(0, Number(perSideFeeRate || 0));
const feeExit = Math.max(0, Number(exitNotional || 0)) * Math.max(0, Number(perSideFeeRate || 0));
const total = feeEntry + feeExit;
const commission_bps = (entryNotional > 0) ? Math.round((total / entryNotional) * 10000) : Math.round(2 * (perSideFeeRate || 0) * 10000);
return { commission_quote_usdt: total, commission_bps };
}

/* ---------- Exports ---------- */
export {
// utils
TTLLRU, DEFAULT_FETCH_TIMEOUT_MS, safeFetch, clamp, clamp01, clampRange, nowISO, sleep, sigmoid, bpsToFrac,
te, td, b64url, b64std, envFlag, envNum, ENABLE_TTL, ENABLE_MFE_MAE,

// sessions/db/ui
getSession, createSession, saveSession, deleteSession, handleDirectStop, kvGet, kvSet,
getProtocolState, initProtocolDynamic, updateProtocolState, reconcileProtocol,
answerCallbackQuery, sendMessage, editMessage,

// gist
gistEnabled, loadGistState, saveGistState, normalizeGistState, mergeGistState,
updatePendingOpenByCID, removePendingByCID, appendClosed, bumpSymStatsReal, approxCommissionFromNotional,

// constants
SUPPORTED_EXCHANGES, STRICT_RRR, ACP_FRAC, PER_TRADE_CAP_FRAC, DAILY_OPEN_RISK_CAP_FRAC,
HSE_CFG, LAMBDA_BLEND, EPS_VARIANCE, MIN_STOP_PCT, SQS_MIN_DEFAULT,
CRON_LOCK_KEY, CRON_LOCK_TTL, DEFAULT_MAX_CONCURRENT_POS, DEFAULT_MAX_NEW_POSITIONS_PER_CYCLE
};

/* ======================================================================
SECTION 2/7 — Market Data, Orderbook, and Klines helpers (incl. MFE/MAE)
(Prefer Binance/MEXC endpoints; adds fetchKlinesRange + computeMFE_MAE)
====================================================================== */

/* ---------- Cache setup (LRU + TTL; flat CPU) ---------- */
const PRICE_CACHE_TTL_MS = 30000; // 30s price snapshots
const STOP_PCT_CACHE_TTL_MS = 45 * 60 * 1000; // 45m sticky ATR-like stop
const ORDERBOOK_CACHE_TTL_MS = 8000; // 8s depth snapshot

const priceCache = new TTLLRU(512, PRICE_CACHE_TTL_MS); // key: BASE, val: number price
const stopPctCache = new TTLLRU(512, STOP_PCT_CACHE_TTL_MS); // key: BASE, val: number stop pct
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
let v = await tryOne(baseUrl, "1h", 14); if (v !== null) { stopPctCache.set(base, v); return v; }
v = await tryOne(baseUrl, "30m", 30); if (v !== null) { stopPctCache.set(base, v); return v; }
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

/* ---------- Gist pending accessor (primary for execution) ---------- */
async function getGistPending(env) {
const { state } = await loadGistState(env);
return Array.isArray(state.pending) ? state.pending : [];
}

/* ---------- Legacy ideas snapshot (DB) — optional UI support ---------- */
async function getLatestIdeas(env) {
// Prefer newest from GitHub/GHA stored in DB (legacy path)
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
let minLow = +Infinity;

for (const k of klines1m) {
const hi = Number(k[2] || 0);
const lo = Number(k[3] || 0);
if (isFinite(hi) && hi > maxHigh) maxHigh = hi;
if (isFinite(lo) && lo < minLow) minLow = lo;
}

if (!isFinite(maxHigh) || !isFinite(minLow)) return { mfe_bps: 0, mae_bps: 0 };

if (!isShort) {
const mfe = (maxHigh / e) - 1;
const mae = (minLow / e) - 1;
return { mfe_bps: Math.round(Math.max(0, mfe) * 10000), mae_bps: Math.round(Math.min(0, mae) * 10000) };
} else {
const mfe = (e / Math.max(1e-12, minLow)) - 1; // favorable for shorts when low goes down
const mae = (maxHigh / e) - 1; // adverse for shorts when high goes up
return { mfe_bps: Math.round(Math.max(0, mfe) * 10000), mae_bps: Math.round(-Math.max(0, mae) * 10000) };
}
}

/* ---------- Exports ---------- */
export {
PRICE_CACHE_TTL_MS, STOP_PCT_CACHE_TTL_MS, ORDERBOOK_CACHE_TTL_MS,
priceCache, stopPctCache, orderbookCache,
getCurrentPrice, calculateStopPercent,
getOrderBookSnapshot, computeSpreadDepth, computeBestBidAsk,
fetchKlines, fetchKlinesRange, seriesReturnsFromKlines, pearsonCorr,
getGistPending, getLatestIdeas,
computeMFE_MAE
};

/* ======================================================================
SECTION 3/7 — Wallet/Equity, Cooldowns, HSE/Kelly, Exposure,
Logging, TCR pacing (flat CPU via KV counters)
====================================================================== */

import {
// from section1
// getSession, decrypt, kvGet, kvSet, nowISO,
// HSE_CFG, EPS_VARIANCE
} from "./section1.js";

import {
// from section2
// getCurrentPrice, getOrderBookSnapshot, computeSpreadDepth,
// fetchKlines, seriesReturnsFromKlines, pearsonCorr
} from "./section2.js";

import {
// from section4
verifyApiKeys
} from "./section4.js";

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
return Math.max(0, Math.min(1, peak > 0 ? (peak - tc) / peak : 0));
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
const p = 0.25 + 0.5 * Math.max(0, Math.min(1, SQS));
return Math.max(0.0, Math.min(1.0, p));
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
return Math.max(-1, Math.min(1, rho_max));
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
function SQSfromScore(score) {
return Math.max(0, Math.min(1, Math.sqrt(Math.max(0, Math.min(1, (Number(score) || 0) / 100)))));
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
async function resyncPacingFromDB(env, userId) {
const today = utcDayKey();
const kDate = `tcr_today_date_${userId}`;
const kCount = `tcr_today_count_${userId}`;
const kRoll = `tcr_7d_roll_${userId}`;

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
(Threads clientOrderId to newClientOrderId; adds spot/futures list APIs;
provides TP/SL/TTL exit placement helpers for spot and futures)
====================================================================== */

import { safeFetch, SUPPORTED_EXCHANGES } from "./section1.js";
import { getCurrentPrice } from "./section2.js";

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
a = ff(a,b,c,d,k[4],7,-1770035416); d = ff(d,a,b,c,d,k[5],12,1700485571); c = ff(c,d,a,b,k[6],17,-1894986606); b = ff(b,c,d,a,k[7],22,-45705983);
a = ff(a,b,c,d,k[8],7,1770035416); d = ff(d,a,b,c,d,k[9],12,-1958414417); c = ff(c,d,a,b,k[10],17,-42063); b = ff(b,c,d,a,k[11],22,-1990404162);
a = ff(a,b,c,d,k[12],7,1804603682); d = ff(d,a,b,c,d,k[13],12,-40341101); c = ff(c,d,a,b,k[14],17,-1502002290); b = ff(b,c,d,a,k[15],22,1236535329);
a = gg(a,b,c,d,k[1],5,-165796510); d = gg(d,a,b,c,d,k[6],9,-1069501632); c = gg(c,d,a,b,k[11],14,643717713); b = gg(b,c,d,a,k[0],20,-373897302);
a = gg(a,b,c,d,k[5],5,-701558691); d = gg(d,a,b,c,d,k[10],9,38016083); c = gg(c,d,a,b,k[15],14,-660478335); b = gg(b,c,d,a,k[4],20,-405537848);
a = hh(a,b,c,d,k[5],4,-94573760); d = hh(d,a,b,c,d,k[8],11,-42122340); c = hh(c,d,a,b,k[11],16,-198630844); b = hh(b,c,d,a,k[14],23,1126891415);
a = ii(a,b,c,d,k[0],6,-198630844); d = ii(d,a,b,c,d,k[7],10,1126891415); c = ii(c,d,a,b,k[14],15,-1416354905); b = ii(b,c,d,a,k[5],21,-57434055);
a = ii(a,b,c,d,k[12],6,1700485571); d = ii(d,a,b,c,d,k[3],10,-1894986606); c = ii(c,d,a,b,k[10],15,-1051523); b = ii(b,c,d,a,k[1],21,-2054922799);
a = ii(a,b,c,d,k[8],6,1873313359); d = ii(d,a,b,c,d,k[15],10,-30611744); c = ii(c,d,a,b,k[6],15,-1560198380); b = ii(b,c,d,a,k[13],21,1309151649);
a = ii(a,b,c,d,k[4],6,-145523070); d = ii(d,a,b,c,d,k[11],10,-1120210379); c = ii(c,d,a,b,k[2],15,718787259); b = ii(b,c,d,a,k[9],21,-343485551);
x[0] = (a+x[0])|0; x[1] = (b+x[1])|0; x[2] = (c+x[2])|0; x[3] = (d+x[3])|0;
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
const res = await safeFetch(url, { method: "GET", headers: { "KEY": apiKey, "Timestamp": ts, "SIGN": sign } }, 6000);
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
const reason = r1.data?.["err-msg"] || r1.data?.message || `HTTP ${r1.res.status}`;
return { success: false, reason };
}
const acc = (r1.data.data || []).find((a) => a.type === "spot" && a.state === "working") || (r1.data.data || [])[0];
if (!acc) return { success: false, reason: "No spot account found" };
const path2 = `/v1/account/accounts/${acc.id}/balance`;
const r2 = await huobiSignedGet(ex, apiKey, apiSecret, path2);
if (!r2.res.ok || r2.data?.status !== "ok") {
const reason = r2.data?.["err-msg"] || r2.data?.message || `HTTP ${r2.res.status}`;
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
const res = await safeFetch(url, { method: "GET", headers: { "X-COINEX-TIMESTAMP": ts, "X-COINEX-KEY": apiKey, "X-COINEX-SIGN": sign } }, 6000);
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

/* ---------- Spot order/trade listings (Binance-like) ---------- */
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
const avgPrice = executedQty > 0 ? (cumQuote / executedQty) : (parseFloat(data.avgPrice || 0) || 0);
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
return { orderId: `demo-${crypto.randomUUID()}`, executedQty, avgPrice: currentPrice, status: 'FILLED' };
}

/* ---------- Market order routing based on session exchange (adds cid) ---------- */
async function placeMarketBuy(env, userId, symbol, quoteAmount, opts = {}) {
const session = await getSession(env, userId);
const ex = SUPPORTED_EXCHANGES[session.exchange_name];
const cid = opts.clientOrderId;

if (ex.kind === 'demoParrot') {
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
default: throw new Error(`Orders not supported for ${ex.label}`);
}
}

async function placeMarketSell(env, userId, symbol, amount, isQuoteOrder = false, opts = {}) {
const session = await getSession(env, userId);
const ex = SUPPORTED_EXCHANGES[session.exchange_name];
const cid = opts.clientOrderId;

if (ex.kind === 'demoParrot') {
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
default: throw new Error(`Orders not supported for ${ex.label}`);
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

/* ---------- New: Cancel helpers (by CID) and Spot OCO ---------- */
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
// swallow common not-found/filled errors
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
stopClientOrderId: cidSl,
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

/* ---------- Exports ---------- */
export {
hmacHexStr, hmacB64Str, hmacB64Bytes, sha256Bytes, md5Hex,
parseUSDT_BinanceLike, parseUSDT_Gate, parseUSDT_Bybit, parseUSDT_Kraken, parseUSDT_CoinEx, parseUSDT_Huobi, parseUSDT_LBank,
verifyBinanceLike, verifyBinanceFutures, verifyBybit, verifyKraken, verifyGate, huobiSignedGet, verifyHuobi, verifyCoinEx, verifyLBank,
verifyApiKeys,

// listings
listAllOrdersBinanceLike, listMyTradesBinanceLike,
listAllOrdersFutures, listMyTradesFutures,

// order placement
placeBinanceLikeOrder, placeBinanceLikeLimitMaker,
placeLBankOrder, placeCoinExOrder,
placeBinanceFuturesOrder, placeBinanceFuturesLimitPostOnly,
placeDemoOrder,

// routing
placeMarketBuy, placeMarketSell,

// statuses
getBinanceLikeOrderStatus, getBinanceFuturesOrderStatus,

// exit helpers (CID suffix)
placeSpotTPLimit, placeSpotSLStopLimit, placeSpotTTLMarket,
placeFutTPMarket, placeFutSLMarket, placeFutTTLMarket,

// new: cancel + OCO
supportsSpotOCO, cancelSpotOrderByCID, cancelFuturesOrderByCID, placeSpotOCO
};
/* ======================================================================
SECTION 5/7 — Gist reconciliation
(Precise commissions from fills; remove unsupported spot shorts;
partial fills; TP/SL placement; TTL; exit inference; learned_at_ts;
OCO usage on Binance; resize spot exits on partial fills)
====================================================================== */

import {
// utils and toggles
// clamp, nowISO, ENABLE_TTL, ENABLE_MFE_MAE,
// gist helpers
// updatePendingOpenByCID, removePendingByCID, appendClosed,
// bumpSymStatsReal, approxCommissionFromNotional
} from "./section1.js";

import {
// market/klines
// getCurrentPrice, fetchKlinesRange, computeMFE_MAE
} from "./section2.js";

import {
// listings (spot + futures)
// listAllOrdersBinanceLike, listMyTradesBinanceLike,
// listAllOrdersFutures, listMyTradesFutures,
// order placement (entry variants)
// placeBinanceLikeOrder, placeBinanceLikeLimitMaker,
// placeBinanceFuturesOrder, placeBinanceFuturesLimitPostOnly,
// exit placement helpers (CID suffix)
// placeSpotTPLimit, placeSpotSLStopLimit, placeSpotTTLMarket,
// placeFutTPMarket, placeFutSLMarket, placeFutTTLMarket,
// cancel + OCO
// supportsSpotOCO, cancelSpotOrderByCID, cancelFuturesOrderByCID, placeSpotOCO
} from "./section4.js";

/* ---------- Small helpers ---------- */
function ideaBaseFromSymbolFull(idea) {
if (idea?.base) return String(idea.base).toUpperCase();
const full = String(idea?.symbolFull || "").toUpperCase();
const quotes = ["USDT","USDC","USD"];
for (const q of quotes) if (full.endsWith(q)) return full.slice(0, -q.length);
return full.replace(/USDT|USDC|USD/,''); }
function entrySideToOrderSide(idea) { const s = String(idea?.side || 'long').toLowerCase(); return s === 'short' ? 'SELL' : 'BUY'; }
function isShortSide(idea) { return String(idea?.side || 'long').toLowerCase() === 'short'; }
function makerTakerSummary(trades) { let m=0,t=0; for (const tr of trades||[]) { const mk = tr?.isMaker; if (mk === true) m++; else t++; } return { maker: m, taker: t }; }
function avgPxQtyFromTrades(trades) { let q=0, v=0; for (const tr of trades||[]) { const qty = parseFloat(tr.qty || tr.executedQty || tr.qty_filled || tr.baseQty || 0); const px = parseFloat(tr.price || tr.p || tr.avgPrice || 0); if (isFinite(qty) && isFinite(px) && qty>0 && px>0) { q += qty; v += qty*px; } } const avg = q>0 ? v/q : 0; return { avg, qty: q }; }
function tradesFingerprint(trs) {
if (!trs?.length) return null;
const first = trs[0], last = trs[trs.length - 1];
const fid = first?.id ?? first?.tradeId ?? first?.T ?? '';
const lid = last?.id ?? last?.tradeId ?? last?.T ?? '';
const ft = Number(first?.time || first?.updateTime || first?.T || 0);
const lt = Number(last?.time || last?.updateTime || last?.T || 0);
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

/* ---------- Entry placement (if not present) ---------- */
async function ensureEntryOrderExists(idea, exAccount) {
const { ex, apiKey, apiSecret } = exAccount;
const base = ideaBaseFromSymbolFull(idea);
const cid = String(idea.client_order_id || "");
const side = entrySideToOrderSide(idea);
const notional = Number(idea.notional_usd || idea.notional_usdt || 0);
if (!cid || !(notional > 0)) return null;

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
const { ex, apiKey, apiSecret } = exAccount;
const isShort = isShortSide(idea);
const base = ideaBaseFromSymbolFull(idea);
const cid = String(idea.client_order_id);
if (!(filledQty > 0)) return;

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

const tpCID = `${cid}:tp`;
const slCID = `${cid}:sl`;

// Current metadata and need for resize
const meta = idea.exits || {};
const placedQty = Number(meta.placed_qty || 0);
const needResize = filledQty > placedQty + 1e-8;

// Find any existing open exit legs
const tpOrder = (orders||[]).find(o => String(o.clientOrderId||"") === tpCID && ["NEW","PARTIALLY_FILLED"].includes(String(o.status||"").toUpperCase()));
const slOrder = (orders||[]).find(o => String(o.clientOrderId||"") === slCID && ["NEW","PARTIALLY_FILLED"].includes(String(o.status||"").toUpperCase()));
const haveBoth = !!tpOrder && !!slOrder;

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
await placeFutTPMarket(ex, apiKey, apiSecret, base, sideClose, tpAbs, tpCID);
await placeFutSLMarket(ex, apiKey, apiSecret, base, sideClose, slAbs, slCID);
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
const { ex, apiKey, apiSecret } = exAccount;
const since = Math.max(0, Number(idea.ts_ms || 0) - 60_000);

// Remove unsupported spot shorts immediately (so they don't linger)
if (ex.kind === 'binanceLike' && isShortSide(idea)) {
  console.warn(`[gist] Removing unsupported spot short ${base} (CID ${cid})`);
  removePendingByCID(state, cid);
  return { status: 'removed', cid, reason: 'spot_short_unsupported' };
}

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
  if (ENABLE_TTL(env) && isFinite(ttlTs) && ttlTs > 0 && now >= ttlTs && !ttlAlready) {
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
    const exitSide = isShortSide(idea) ? "BUY" : "SELL";
    for (const t of later) {
      const q = parseFloat(t.qty || t.executedQty || 0);
      const isBuyer = (t.isBuyer === true || String(t.side||"").toUpperCase() === "BUY");
      const side = isBuyer ? "BUY" : "SELL";
      if (side === exitSide) {
         netBase -= q;
         exitTrades.push(t);
      } else {
         netBase += q;
      }
    }
    if (netBase <= entryQty * 0.02 && exitTrades.length) {
      const wap = avgPxQtyFromTrades(exitTrades);
      exitPrice = wap.avg || 0;
      exitTsMs = Number(exitTrades?.at(-1)?.time || Date.now());

      // Infer exit_reason vs targets
      const tol_bps = Math.max(7, Number(idea.cost_bps || 10));
      const isShort = isShortSide(idea);
      const tpAbs = Number(idea.tp_abs || (entryPrice * (isShort ? (1 - (idea.tp_bps||0)/10000) : (1 + (idea.tp_bps||0)/10000))));
      const slAbs = Number(idea.sl_abs || (entryPrice * (isShort ? (1 + (idea.sl_bps||0)/10000) : (1 - (idea.sl_bps||0)/10000))));
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
  const feeTaker = Number((state?.fees && (state.fees.taker ?? state.fees.maker)) ?? env?.FEE_RATE_PER_SIDE ?? 0.001);
  const c1 = await commissionFromTradesUSDT(entryTrades, base);
  const c2 = await commissionFromTradesUSDT(exitTrades, base);
  let commission_quote_usdt = (c1 || 0) + (c2 || 0);

  let commission_bps = 0;
  const entryNotional = entryPrice * entryQty;
  if (entryNotional > 0 && commission_quote_usdt > 0) {
    commission_bps = Math.round((commission_quote_usdt / entryNotional) * 10000);
  } else {
    const approx = approxCommissionFromNotional(entryNotional, exitPrice * entryQty, feeTaker);
    commission_quote_usdt = commission_quote_usdt || approx.commission_quote_usdt;
    commission_bps = approx.commission_bps;
  }

  // PnL
  const up = !isShortSide(idea);
  const ret = up ? (exitPrice/entryPrice - 1) : (entryPrice/exitPrice - 1);
  const pnl_bps = Math.round(ret*10000) - commission_bps;
  const exit_outcome = (pnl_bps > 0) ? "win" : "loss";

  // MFE/MAE (optional via helper)
  let mfe_bps = 0, mae_bps = 0;
  if (ENABLE_MFE_MAE(env)) {
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

/* ---------- Exports ---------- */
export {
reconcilePendingIdea
};

/* ======================================================================
SECTION 6/7 — UI Texts, Cards, Keyboards, Dashboard, Lists,
Trade Details, Actions, Concurrency helpers
====================================================================== */

import {
// constants + UI helpers
// STRICT_RRR,
// UI send/edit
// sendMessage, editMessage,
// sessions and protocol
// getSession, getProtocolState,
// session lifecycle helpers
// handleDirectStop, deleteSession, createSession, saveSession
} from "./section1.js";

import {
// getCurrentPrice, getOrderBookSnapshot, computeSpreadDepth
} from "./section2.js";

import {
// getOpenPositionsCount
} from "./section3.js";

/* ---------- Local UI helpers ---------- */
function formatMoney(amount) { return `$${Number(amount || 0).toFixed(8)}`; }
function formatPercent(decimal) { return `${(Number(decimal || 0) * 100).toFixed(2)}%`; }
function formatDurationShort(ms) {
if (!isFinite(ms) || ms <= 0) return "0m";
const totalMin = Math.round(ms / 60000);
const h = Math.floor(totalMin / 60);
const m = totalMin % 60;
return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

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
`ACP (80% of TC): ${formatMoney(0.80 * bal)}`,
`Per-trade cap: ${formatPercent(0.10)} of TC`,
`Daily open risk cap: ${formatPercent(0.30)} of TC`,
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
`ACP (80% of TC): ${formatMoney(0.80 * bal)}`,
`Per-trade cap: ${formatPercent(0.10)} of TC}`,
`Daily open risk cap: ${formatPercent(0.30)} of TC`,
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
`Quantity: ${Number(trade.qty || 0).toFixed(6)}`,
`Capital used at entry: ${formatMoney(notionalAtEntry)} (${formatPercent(pctEntry)} of TC at entry)`,
`Risk (Final): ${formatMoney(trade.risk_usd)}`,
"",
"Live P&L:",
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
if (typeof meta.required !== 'undefined') lines.push(`Required: ${formatMoney(meta.required)}`);
}
if (extra?.skip_reason === 'ev_gate' && typeof meta.EV_R !== 'undefined') lines.push(`EV_R: ${Number(meta.EV_R).toFixed(3)}`);
if (extra?.skip_reason === 'hse_gate' && typeof meta.pH !== 'undefined') lines.push(`pH: ${Number(meta.pH).toFixed(2)}`);
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
[{ text: "Stop ⏸️", callback_data: "action_stop_confirm" }]
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
const stopPctTxt = r.stop_pct !== undefined ? `(${formatPercent(Number(r.stop_pct || 0))})` : "";
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
const hit = r.close_type ? `(${r.close_type})` : "";
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
// executeTrade is expected to exist in your app (manual mode flow)
const approved = await executeTrade(env, userId, tradeId);
if (approved) await sendTradeDetailsUI(env, userId, tradeId, messageId);
else await editMessage(userId, messageId, `Failed to execute trade #${tradeId}.`, [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }]], env);
break;
}
case 'tr_rej':
await env.DB.prepare("UPDATE trades SET status = 'rejected', updated_at = ? WHERE id = ? AND user_id = ?").bind(new Date().toISOString(), tradeId, userId).run();
await sendTradesListUI(env, userId, 1, messageId);
break;
case 'tr_refresh':
await sendTradeDetailsUI(env, userId, tradeId, messageId);
break;
case 'tr_close': {
// closeTradeNow is expected to exist in your app (manual mode flow)
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
const v = Number(env.MAX_CONCURRENT_POSITIONS || 3);
return Math.max(1, Math.floor(isFinite(v) ? v : 3));
}
function getMaxNewPerCycle(env) {
const v = Number(env.MAX_NEW_POSITIONS_PER_CYCLE || 3);
return Math.max(1, Math.floor(isFinite(v) ? v : 3));
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
SECTION 7/7 — processUser (no-op for Gist), HTTP endpoints,
Cron with Gist reconciliation, Worker export
====================================================================== */

import {
// gistEnabled, loadGistState, saveGistState, kvGet, kvSet, nowISO,
// SUPPORTED_EXCHANGES, CRON_LOCK_KEY, CRON_LOCK_TTL, envFlag
} from "./section1.js";
import { verifyBinanceLike, verifyBinanceFutures } from "./section4.js";
import { reconcilePendingIdea } from "./section5.js";

/* ---------- Global execution credentials (Gist flow) ---------- */
function getGlobalExecAccount(env) {
const exName = String(env.GLOBAL_EXCHANGE || env.EXCHANGE || "mexc").toLowerCase().trim();
const ex = SUPPORTED_EXCHANGES[exName];
if (!ex || !ex.hasOrders) return null;

let apiKey = "";
let apiSecret = "";

if (ex.kind === "binanceLike") {
apiKey = String(env.MEXC_API_KEY || env.BINANCE_API_KEY || "").trim();
apiSecret = String(env.MEXC_SECRET_KEY || env.BINANCE_SECRET_KEY || "").trim();
} else if (ex.kind === "binanceFuturesUSDT") {
apiKey = String(env.BINANCE_FUTURES_API_KEY || env.BINANCE_API_KEY || "").trim();
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
}
} catch {}
}

/* ---------- Gist reconciliation loop (persist heartbeat every run) ---------- */
async function runGistReconciliation(env) {
if (!gistEnabled(env)) return { ok: true, changed: false, reason: "gist_disabled" };

const exec = getGlobalExecAccount(env);
if (!exec) {
console.warn("[gist] Missing GLOBAL_EXCHANGE or API keys; set GLOBAL_EXCHANGE and API key/secret envs.");
return { ok: false, changed: false, reason: "no_exec_creds" };
}

const { state, etag } = await loadGistState(env);
await detectFeeRates(exec, state);

const pending = Array.isArray(state.pending) ? [...state.pending] : [];

let changed = false;
for (const idea of pending) {
try {
const res = await reconcilePendingIdea(env, state, idea, exec);
if (["closed","open","removed","skipped"].includes(res?.status)) changed = true;
} catch (e) {
console.error("[gist] reconcile error:", e?.message || e);
}
}

// Heartbeat always; lastReconcileTs only on data changes
state.loop_heartbeat_ts = Date.now();
if (changed) state.lastReconcileTs = Date.now();

const saveHeartbeatAlways = envFlag(env, "HEARTBEAT_ALWAYS_SAVE", "1"); // default ON
if (changed || saveHeartbeatAlways) {
await saveGistState(env, state, etag);
}

return { ok: true, changed, reason: changed ? "updated" : "no_change" };
}

/* ---------- processUser (no-op for Gist-aligned worker) ---------- */
async function processUser(_env, _userId, _budget = null) {
return;
}

/* ---------- HTTP endpoints ---------- */
async function handleHealth() {
return new Response(JSON.stringify({ ok: true, time: nowISO() }), {
headers: { "Content-Type": "application/json" }
});
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
const total = (await env.DB.prepare("SELECT COUNT(*) AS c FROM trades").first())?.c || 0;
const open = (await env.DB.prepare("SELECT COUNT(*) AS c FROM trades WHERE status='open'").first())?.c || 0;
const pending = (await env.DB.prepare("SELECT COUNT(*) AS c FROM trades WHERE status='pending'").first())?.c || 0;
const closed = (await env.DB.prepare("SELECT COUNT(*) AS c FROM trades WHERE status='closed'").first())?.c || 0;
const rejected= (await env.DB.prepare("SELECT COUNT(*) AS c FROM trades WHERE status='rejected'").first())?.c || 0;
const users = (await env.DB.prepare("SELECT COUNT(*) AS u FROM user_sessions WHERE status IN ('active','halted')").first())?.u || 0;
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
return new Response(JSON.stringify({ ok: res.ok, changed: res.changed, ts: nowISO(), reason: res.reason || "ok" }), {
headers: { "Content-Type": "application/json" }
});
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
  if (url.pathname === "/gist/state" && request.method === "GET") {
    return await handleGistState(env, request);
  }
  if (url.pathname === "/state" && request.method === "GET") {
    return await handleStateMirror(env, request);
  }
  if (url.pathname === "/gist/reconcile" && request.method === "POST") {
    return await handleGistReconcile(env, request);
  }

  // Legacy compatibility endpoints (optional)
  if (url.pathname === "/task/process-user" && request.method === "POST") {
    const token = String(env.TASK_TOKEN || "").trim();
    const auth = request.headers.get("Authorization") || "";
    const good = token ? (auth.startsWith("Bearer ") && auth.split(" ")[1] === token) : true;
    if (!good) return new Response("Unauthorized", { status: 401 });
    const uid = url.searchParams.get("uid");
    if (!uid) return new Response("Missing uid", { status: 400 });
    ctx.waitUntil(processUser(env, uid, null));
    return new Response(JSON.stringify({ ok: true, uid }), { headers: { "Content-Type": "application/json" } });
  }

  if (url.pathname === "/signals/push" && request.method === "POST") {
    const expected = String(env.PUSH_TOKEN || "").trim();
    const auth = request.headers.get("Authorization") || "";
    if (expected && auth !== `Bearer ${expected}`) return new Response("Unauthorized", { status: 401 });

    let ideas;
    try { ideas = await request.json(); } catch { return new Response("Bad JSON", { status: 400 }); }
    try {
      await env.DB.prepare("INSERT INTO ideas (ts, mode, ideas_json) VALUES (?, ?, ?)")
        .bind(ideas.ts || nowISO(), ideas.mode || 'normal', JSON.stringify(ideas)).run();
      return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });
    } catch (e) {
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
