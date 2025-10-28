/* ======================================================================
   Exchange Server Worker — External Ideas Only + Fan-out Fallback
   (10/10 suggester compatible: dynamic RRR, prob/EV via env flags)
   SECTION 1/7 — Constants, Utils, Crypto, DB, Telegram, Logging
   ====================================================================== */

/* ---------- Constants (Exchanges) ---------- */
const SUPPORTED_EXCHANGES = {
  crypto_parrot: { label: "Crypto Parrot (Demo)", kind: "demoParrot", hasOrders: true },

  // Spot
  mexc:   { label: "MEXC",   kind: "binanceLike", baseUrl: "https://api.mexc.com",    accountPath: "/api/v3/account", apiKeyHeader: "X-MEXC-APIKEY", defaultQuery: "",          hasOrders: true },
  binance:{ label: "Binance",kind: "binanceLike", baseUrl: "https://api.binance.com", accountPath: "/api/v3/account", apiKeyHeader: "X-MBX-APIKEY",  defaultQuery: "recvWindow=5000", hasOrders: true },
  lbank:  { label: "LBank",  kind: "lbankV2",     baseUrl: "https://api.lbkex.com",   hasOrders: true },
  coinex: { label: "CoinEx",  kind: "coinexV2",   baseUrl: "https://api.coinex.com",  hasOrders: true },

  // Futures / Margin (USDT-M)
  binance_futures: { label: "Binance Futures (USDT-M)", kind: "binanceFuturesUSDT", baseUrl: "https://railway-2z9h.onrender.com", apiKeyHeader: "X-MBX-APIKEY", hasOrders: true },

  // Bybit Futures
  bybit_futures_testnet: { label: "Bybit Futures (Demo-Testnet)", kind: "bybitFuturesV5", baseUrl: "https://api-testnet.bybit.com", hasOrders: true },
  bybit_futures:         { label: "Bybit Futures",           kind: "bybitFuturesV5", baseUrl: "https://api.bybit.com",        hasOrders: true },

  // Read-only
  bybit:  { label: "Bybit (Wallet)", kind: "bybitV5", baseUrl: "https://api.bybit.com", hasOrders: false },
  kraken: { label: "Kraken", kind: "krakenV0", baseUrl: "https://api.kraken.com", hasOrders: false },
  gate:   { label: "Gate",   kind: "gateV4",   baseUrl: "https://api.gateio.ws",   hasOrders: false },
  huobi:  { label: "Huobi",  kind: "huobiV1",  baseHost: "api.huobi.pro", scheme: "https", hasOrders: false }
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
const AUTO_NO_FUNDS_THRESHOLD = 0.000001;

const DEFAULT_MAX_CONCURRENT_POS = 3;
const DEFAULT_MAX_NEW_POSITIONS_PER_CYCLE = 3;

const CRON_LAST_RUN_KEY = "cron_last_run_ts";
function cronIntervalMs(env) { return Math.max(60000, Number(env.CRON_INTERVAL_MS || 60000)); }
function cronCatchupMax(env) { return Math.max(0, Math.floor(Number(env.CRON_CATCHUP_MAX_CYCLES || 3))); }
function cronGapAlertMs(env) { return Math.max(0, Number(env.CRON_GAP_ALERT_MS || 300000)); }

/* ---------- Tiny utils ---------- */
const te = new TextEncoder();
const td = new TextDecoder();
const b64url = {
  encode: (buf) =>
    btoa(String.fromCharCode(...new Uint8Array(buf)))
      .replace(/\+/g, "-")
      .replace(/\//g, "_"),
  decode: (str) =>
    Uint8Array.from(
      atob(str.replace(/-/g, "+").replace(/_/g, "/")),
      (c) => c.charCodeAt(0)
    ),
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
const percentEncode = (str) =>
  encodeURIComponent(str).replace(/[!*'()]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function pctChange(a, b) { return b > 0 ? (a - b) / b : 0; }
const bpsToFrac = (bps) => Number(bps || 0) / 10000;

/* FIXED: Env helpers with validation */
function envFlag(env, key, def = "0") { 
  if (!env || typeof env !== 'object') return def === "1";
  return String(env[key] ?? def) === "1"; 
}
function envNum(env, key, def) { 
  if (!env || typeof env !== 'object') return def;
  const v = Number(env[key]); 
  return Number.isFinite(v) ? v : def; 
}

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

/* ---------- FIXED: LRU+TTL cache (properly bounded) ---------- */
class TTLLRU {
  constructor(capacity = 512, ttlMs = 60000) {
    this.cap = Math.max(1, capacity);
    this.ttl = Math.max(1, ttlMs);
    this.map = new Map();
  }
  
  get(key) {
    const ent = this.map.get(key);
    if (!ent) return undefined;
    if (ent.e <= Date.now()) { 
      this.map.delete(key); 
      return undefined; 
    }
    // Refresh recency
    this.map.delete(key);
    this.map.set(key, ent);
    return ent.v;
  }
  
  set(key, value, ttlMs = this.ttl) {
    const e = Date.now() + Math.max(1, ttlMs);
    if (this.map.has(key)) this.map.delete(key);
    this.map.set(key, { v: value, e });
    
    // FIXED: Evict oldest entries until size <= cap
    while (this.map.size > this.cap) {
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

// FIXED: Proper AbortError
async function safeFetch(url, options = {}, timeoutMs = DEFAULT_FETCH_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const to = setTimeout(() => ctrl.abort(new Error('Fetch timeout')), timeoutMs);
  try { 
    return await fetch(url, { ...options, signal: ctrl.signal }); 
  } finally { 
    clearTimeout(to); 
  }
}

const DEFAULT_CPU_BUDGET_MS = 12000;
function createCpuBudget(ms = DEFAULT_CPU_BUDGET_MS) {
  const deadline = Date.now() + Math.max(250, ms);
  return {
    deadline,
    isExpired() { return Date.now() > deadline; },
    timeLeft() { return Math.max(0, deadline - Date.now()); },
    ensure(label) { 
      if (this.isExpired()) throw new Error(`CPU budget exceeded at ${label}`); 
    }
  };
}

/* ======================================================================
   LOGGING FRAMEWORK — Structured, timestamped, filterable
   ====================================================================== */

const LOG_LEVELS = {
  DEBUG: 0,
  INFO: 1,
  WARN: 2,
  ERROR: 3,
  CRITICAL: 4
};

class Logger {
  constructor(env, context = {}) {
    this.env = env;
    this.context = context;
    this.level = LOG_LEVELS[env?.LOG_LEVEL || 'INFO'] || LOG_LEVELS.INFO;
    // FIXED: Timestamp-based session ID (no collision risk)
    this.sessionId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
    this.webhookFailures = 0;
    this.dbFailures = 0;
  }

  _log(level, message, data = {}) {
    if (LOG_LEVELS[level] < this.level) return;
    
    const entry = {
      ts: new Date().toISOString(),
      level,
      session: this.sessionId,
      msg: message,
      ...this.context,
      ...data
    };

    // Console output (readable format)
    const prefix = `[${entry.ts}][${level}][${this.sessionId}]`;
    const suffix = Object.keys(data).length 
      ? ` | ${JSON.stringify(data)}` 
      : '';
    console.log(`${prefix} ${message}${suffix}`);

    // FIXED: Circuit breaker for webhook
    if (this.env?.LOG_WEBHOOK_URL && 
        LOG_LEVELS[level] >= LOG_LEVELS.WARN && 
        this.webhookFailures < 3) {
      this._sendToWebhook(entry).catch(() => {
        this.webhookFailures++;
      });
    }

    // FIXED: Circuit breaker for DB + existence check
    if (this.env?.DB && 
        LOG_LEVELS[level] >= LOG_LEVELS.ERROR && 
        this.dbFailures < 3) {
      this._storeToDB(entry).catch(() => {
        this.dbFailures++;
      });
    }
  }

  async _sendToWebhook(entry) {
    try {
      await safeFetch(this.env.LOG_WEBHOOK_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(entry)
      }, 3000);
    } catch (e) {
      console.error('[Logger] Webhook failed:', e.message);
      throw e;
    }
  }

  async _storeToDB(entry) {
    try {
      await this.env.DB.prepare(
        "INSERT INTO error_log (ts, level, session_id, message, data) VALUES (?, ?, ?, ?, ?)"
      ).bind(
        entry.ts,
        entry.level,
        entry.session,
        entry.msg,
        JSON.stringify(entry)
      ).run();
    } catch (e) {
      console.error('[Logger] DB storage failed:', e.message);
      throw e;
    }
  }

  debug(msg, data) { this._log('DEBUG', msg, data); }
  info(msg, data) { this._log('INFO', msg, data); }
  warn(msg, data) { this._log('WARN', msg, data); }
  error(msg, data) { this._log('ERROR', msg, data); }
  critical(msg, data) { this._log('CRITICAL', msg, data); }

  // Scoped logger for sub-operations
  scope(additionalContext) {
    const scoped = new Logger(this.env, { ...this.context, ...additionalContext });
    scoped.sessionId = this.sessionId; // Share session ID
    scoped.webhookFailures = this.webhookFailures;
    scoped.dbFailures = this.dbFailures;
    return scoped;
  }

  // FIXED: Performance timing with proper cleanup
  time(label) {
    const start = performance?.now ? performance.now() : Date.now();
    let ended = false;
    return {
      end: (data = {}) => {
        if (ended) return 0; // Prevent double-end
        ended = true;
        const duration = (performance?.now ? performance.now() : Date.now()) - start;
        this.info(`⏱️ ${label}`, { duration_ms: duration.toFixed(2), ...data });
        return duration;
      }
    };
  }
}

/* Migration SQL (run once in D1):
CREATE TABLE IF NOT EXISTS error_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  level TEXT NOT NULL,
  session_id TEXT,
  message TEXT,
  data TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_error_log_ts ON error_log(ts);
CREATE INDEX IF NOT EXISTS idx_error_log_level ON error_log(level);
CREATE INDEX IF NOT EXISTS idx_error_log_session ON error_log(session_id);
*/

/* ---------- FIXED: Telegram helpers (with error propagation) ---------- */
async function answerCallbackQuery(env, id) {
  const log = new Logger(env, { fn: 'answerCallbackQuery' });
  const url = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/answerCallbackQuery`;
  try {
    const r = await safeFetch(url, { 
      method: "POST", 
      headers: { "Content-Type": "application/json" }, 
      body: JSON.stringify({ callback_query_id: id }) 
    });
    if (r) await r.text().catch(() => {});
    if (!r || !r.ok) {
      log.warn('Callback query answer failed', { status: r?.status });
    }
  } catch (e) {
    log.error('answerCallbackQuery error', { error: e.message });
  }
}

async function sendMessage(chatId, text, buttons, env) {
  const log = new Logger(env, { fn: 'sendMessage', chatId });
  const url = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`;
  const body = { chat_id: chatId, text };
  if (buttons && buttons.length) body.reply_markup = { inline_keyboard: buttons };
  
  try {
    const r = await safeFetch(url, { 
      method: "POST", 
      headers: { "Content-Type": "application/json" }, 
      body: JSON.stringify(body) 
    }, 6000);
    
    if (!r) {
      log.error('sendMessage: No response');
      return false;
    }
    
    const responseText = await r.text().catch(() => '');
    
    if (!r.ok) {
      log.error('sendMessage failed', { 
        status: r.status, 
        response: responseText.slice(0, 200) 
      });
      return false;
    }
    
    log.debug('Message sent successfully');
    return true;
  } catch (e) {
    log.error('sendMessage exception', { error: e.message });
    return false;
  }
}

async function editMessage(chatId, messageId, newText, buttons, env) {
  const log = new Logger(env, { fn: 'editMessage', chatId, messageId });
  const url = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/editMessageText`;
  const body = { chat_id: chatId, message_id: messageId, text: newText };
  if (buttons && buttons.length) body.reply_markup = { inline_keyboard: buttons };
  else body.reply_markup = { inline_keyboard: [] };
  
  try {
    const r = await safeFetch(url, { 
      method: "POST", 
      headers: { "Content-Type": "application/json" }, 
      body: JSON.stringify(body) 
    }, 6000);
    
    if (!r) {
      log.error('editMessage: No response');
      return false;
    }

    const responseText = await r.text().catch(() => '');

    if (r.ok) {
      log.debug('Message edited successfully');
      return true;
    }

    let desc = responseText;
    try {
      const j = JSON.parse(responseText);
      desc = j?.description || responseText;
    } catch {}

    // Ignore "message not modified"
    if (r.status === 400 && /message is not modified/i.test(desc)) {
      log.debug('Message not modified (no change)');
      return true;
    }
    
    // Fallback to sendMessage if can't edit
    if (r.status === 400 && /(can't be edited|message to edit not found)/i.test(desc)) {
      log.warn('Message not editable, falling back to sendMessage');
      return await sendMessage(chatId, newText, buttons, env);
    }
    
    log.error('editMessage failed', { status: r.status, description: desc });
    return false;
  } catch (e) {
    log.error('editMessage exception', { error: e.message });
    return false;
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

/* ---------- FIXED: DB helpers (with error handling) ---------- */
async function getSession(env, userId) { 
  try {
    return await env.DB.prepare("SELECT * FROM user_sessions WHERE user_id = ?").bind(userId).first(); 
  } catch (e) {
    console.error('[DB] getSession error:', e.message);
    return null;
  }
}

async function createSession(env, userId) {
  try {
    await env.DB.prepare(
      "INSERT INTO user_sessions (user_id, current_step, last_interaction_ts, status) VALUES (?, 'start', ?, 'initializing')"
    ).bind(userId, nowISO()).run();
    return getSession(env, userId);
  } catch (e) {
    console.error('[DB] createSession error:', e.message);
    throw e;
  }
}

async function saveSession(env, userId, fields) {
  const keys = Object.keys(fields);
  if (!keys.length) return;
  try {
    await env.DB.prepare(
      `UPDATE user_sessions SET ${keys.map(k => `${k} = ?`).join(", ")}, last_interaction_ts = ? WHERE user_id = ?`
    ).bind(...keys.map(k => fields[k]), nowISO(), userId).run();
  } catch (e) {
    console.error('[DB] saveSession error:', e.message);
    throw e;
  }
}

async function deleteSession(env, userId) { 
  try {
    await env.DB.prepare("DELETE FROM user_sessions WHERE user_id = ?").bind(userId).run(); 
  } catch (e) {
    console.error('[DB] deleteSession error:', e.message);
  }
}

async function handleDirectStop(env, userId) { 
  await deleteSession(env, userId); 
  await sendMessage(userId, "Session ended. Send /start to begin again.", null, env); 
}

/* FIXED: Simple KV via D1 with error handling */
async function kvGet(env, key) { 
  try {
    const row = await env.DB.prepare("SELECT value FROM kv_state WHERE key = ?").bind(key).first(); 
    return row ? row.value : null; 
  } catch (e) {
    console.error('[KV] kvGet error:', e.message, 'key:', key);
    return null;
  }
}

async function kvSet(env, key, value) { 
  try {
    await env.DB.prepare("INSERT OR REPLACE INTO kv_state (key, value) VALUES (?, ?)").bind(key, String(value)).run(); 
  } catch (e) {
    console.error('[KV] kvSet error:', e.message, 'key:', key);
    throw e;
  }
}

/* ---------- Protocol state ---------- */
async function getProtocolState(env, userId) {
  try {
    return await env.DB.prepare("SELECT * FROM protocol_state WHERE user_id = ?").bind(userId).first();
  } catch (e) {
    console.error('[DB] getProtocolState error:', e.message);
    return null;
  }
}

async function initProtocolDynamic(env, userId, feeRate, riskPct = 0.01) {
  await env.DB.prepare(
    `INSERT OR REPLACE INTO protocol_state
     (user_id, initial_capital, acp_balance, pr_balance, trigger_threshold, phase, risk_pct, fee_rate, created_at, updated_at)
     VALUES (?, 0, 0, 0, 0, 'normal', ?, ?, ?, ?)`
  ).bind(userId, riskPct, feeRate, nowISO(), nowISO()).run();
  return { 
    initial_capital: 0, 
    acp_balance: 0, 
    pr_balance: 0, 
    trigger_threshold: 0, 
    phase: 'normal', 
    risk_pct: riskPct, 
    fee_rate: feeRate 
  };
}

async function updateProtocolState(env, userId, updates) {
  const keys = Object.keys(updates);
  if (!keys.length) return;
  await env.DB.prepare(
    `UPDATE protocol_state SET ${keys.map(k => `${k} = ?`).join(", ")}, updated_at = ? WHERE user_id = ?`
  ).bind(...keys.map(k => updates[k]), nowISO(), userId).run();
}

async function reconcileProtocol(env, userId, isWin, riskAmount, rrr, fees) {
  await updateProtocolState(env, userId, { updated_at: nowISO() });
}

/* ---------- FIXED: Gist adapter (consistent return types + security) ---------- */
const GIST_FILE = "state.json";

function gistOn(env) {
  const v = String(env?.GIST_ENABLED ?? "").trim().toLowerCase();
  const enabled = v === "1" || v === "true" || v === "yes";
  const id = String(env?.GIST_ID ?? "").trim();
  const tok = String(env?.GIST_TOKEN ?? "").trim();
  return enabled && !!id && !!tok;
}

function gistHdr(env, method = 'GET') {
  const base = {
    "Authorization": `Bearer ${String(env.GIST_TOKEN || "").trim()}`,
    "Accept": "application/vnd.github+json",
    "User-Agent": "CloudflareWorker/1.0"
  };
  if (method !== 'GET') {
    base["Content-Type"] = "application/json";
  }
  return base;
}

// FIXED: Safe deep clone (handles circular refs)
function deepClone(o) {
  try {
    return JSON.parse(JSON.stringify(o || {}));
  } catch (e) {
    console.error('[deepClone] Failed (circular reference?):', e.message);
    return {};
  }
}

// FIXED: Consistent return type (always returns { state, etag } or null)
async function gistGetState(env) {
  const log = new Logger(env, { fn: 'gistGetState' });
  
  if (!gistOn(env)) {
    log.debug('Gist disabled');
    return null;
  }

  const GID = String(env.GIST_ID || "").trim();
  const headers = {
    ...gistHdr(env, 'GET'),
    "X-GitHub-Api-Version": "2022-11-28",
  };
  const url = `https://api.github.com/gists/${GID}`;

  // Controlled debug logging (only if LOG_LEVEL=DEBUG)
  log.debug('Fetching Gist', { gist_id: GID.slice(0, 8) + '...' });

  let r;
  try {
    r = await safeFetch(url, { headers }, 10000);
  } catch (e) {
    log.error('Gist fetch exception', { error: e.message });
    return null;
  }

  if (!r) {
    log.error('Gist: No response');
    return null;
  }

  log.debug('Gist response', { 
    status: r.status, 
    ratelimit_remaining: r.headers.get("x-ratelimit-remaining") 
  });

  if (!r.ok) {
    const errBody = await r.text().catch(() => '');
    log.error('Gist request failed', { 
      status: r.status, 
      statusText: r.statusText,
      body_preview: errBody.slice(0, 200) 
    });
    return null;
  }

  const etag = r.headers.get("etag") || "";
  let bodyText = '';
  let j;

  try {
    bodyText = await r.text();
    j = JSON.parse(bodyText);
  } catch (e) {
    log.error('Gist JSON parse error', { 
      error: e.message, 
      body_length: bodyText?.length || 0 
    });
    return { 
      state: { pending: [], closed: [], sym_stats_real: {}, equity: [], lastReconcileTs: 0 }, 
      etag 
    };
  }

  const content = j?.files?.[GIST_FILE]?.content;
  if (!content) {
    log.warn('Gist file content missing');
    return { 
      state: { pending: [], closed: [], sym_stats_real: {}, equity: [], lastReconcileTs: 0 }, 
      etag 
    };
  }

  try {
    const state = JSON.parse(content);
    log.info('Gist state loaded', { 
      pending: state?.pending?.length || 0,
      closed: state?.closed?.length || 0 
    });
    return { state, etag };
  } catch (e) {
    log.error('Gist content parse error', { error: e.message });
    return { 
      state: { pending: [], closed: [], sym_stats_real: {}, equity: [], lastReconcileTs: 0 }, 
      etag 
    };
  }
}

async function gistPatchState(env, mutator) {
  const log = new Logger(env, { fn: 'gistPatchState' });
  
  if (!gistOn(env)) {
    log.debug('Gist disabled, skipping patch');
    return false;
  }

  const strict = String(env?.GIST_STRICT_PATCH || "0") === "1";

  const doPatch = async (next, etag) => {
    const GID = String(env.GIST_ID || "").trim();
    const url = `https://api.github.com/gists/${GID}`;
    const body = JSON.stringify({ 
      files: { 
        [GIST_FILE]: { 
          content: JSON.stringify(next, null, 2) 
        } 
      } 
    });
    const headers = {
      ...gistHdr(env, 'PATCH'),
      "X-GitHub-Api-Version": "2022-11-28",
    };
    if (etag) headers["If-Match"] = etag;
    
    const r = await safeFetch(url, { method: "PATCH", headers, body }, 8000).catch(()=>null);
    if (r) await r.text().catch(() => {});
    return r;
  };

  if (!strict) {
    const cur = await gistGetState(env);
    const base = deepClone(cur?.state || { pending: [], closed: [], sym_stats_real: {}, equity: [], lastReconcileTs: 0 });
    let next = base;
    try { 
      next = (await mutator(deepClone(base))) || base; 
    } catch (e) {
      log.error('Mutator error (non-strict)', { error: e.message });
    }
    const r = await doPatch(next, null);
    const success = !!(r && r.ok);
    log.info('Gist patch (non-strict)', { success, status: r?.status });
    return success;
  }

  // Strict mode with retry on 412
  try {
    const cur = await gistGetState(env);
    if (!cur) {
      log.warn('Gist patch: No current state');
      return false;
    }
    
    const base1 = deepClone(cur.state || {});
    const next1 = (await mutator(deepClone(base1))) || base1;

    let r = await doPatch(next1, cur.etag || "");
    if (r && r.ok) {
      log.info('Gist patch (strict) succeeded');
      return true;
    }

    if (r && r.status === 412) {
      log.warn('Gist 412 conflict, retrying');
      const cur2 = await gistGetState(env);
      if (!cur2) return false;
      const base2 = deepClone(cur2.state || {});
      const next2 = (await mutator(deepClone(base2))) || base2;

      const r2 = await doPatch(next2, cur2.etag || "");
      const success = !!(r2 && r2.ok);
      log.info('Gist patch retry', { success, status: r2?.status });
      return success;
    }
    
    log.error('Gist patch failed', { status: r?.status });
    return false;
  } catch (e) {
    log.error('Gist patch exception', { error: e.message });
    return false;
  }
}

function gistFindPendingIdxByCID(state, cid) {
  const arr = Array.isArray(state?.pending) ? state.pending : [];
  return arr.findIndex(p => (p?.client_order_id || "") === cid);
}

/* Exports */
export { 
  gistOn, 
  gistGetState, 
  gistPatchState, 
  gistFindPendingIdxByCID,
  Logger
};
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

  let apiKey, apiSecret;
  try {
    apiKey = await decrypt(session.api_key_encrypted, env);
    apiSecret = await decrypt(session.api_secret_encrypted, env);
  } catch (e) {
    console.error("getWalletBalance decrypt error:", e?.message || e);
    await saveSession(env, userId, { last_balance_err: "decrypt_failed" }).catch(()=>{});
    return 0;
  }

  try {
    const result = await verifyApiKeys(apiKey, apiSecret, session.exchange_name);

    if (!result?.success) {
      console.warn("getWalletBalance verify fail:", result?.reason);
      await saveSession(env, userId, { last_balance_err: String(result?.reason || "verify_failed") }).catch(()=>{});
      return 0;
    }

    // Success: clear any stale error flag
    if (session.last_balance_err) {
      await saveSession(env, userId, { last_balance_err: null }).catch(()=>{});
    }

    const thr = Number(env.DUST_AVAIL_THRESHOLD ?? 1e-6); // e.g., 1e-6 or 1e-5
    const a = Number(result?.data?.available);
    if (Number.isFinite(a) && a > thr) return a; // real available
    const e = Number(result?.data?.equity);
    if (Number.isFinite(e) && e > 0) return e; // fallback to equity (Bybit/MEXC)
    if (result.data?.balance) {
      const n = Number(String(result.data.balance).replace(/[^\d.]/g, ""));
      if (Number.isFinite(n)) return n; // legacy fallback
      await saveSession(env, userId, { last_balance_err: "bad_balance_format" }).catch(()=>{});
    }
  } catch (e) {
    console.error("getWalletBalance error:", e?.message || e);
    await saveSession(env, userId, { last_balance_err: "network_or_parse" }).catch(()=>{});
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
const num = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};
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
    a = ff(a,b,c,d,k[4],7,1804603682); d = ff(d,a,b,c,d,k[5],12,-40341101); c = ff(c,d,a,b,k[6],17,-1473231341); b = ff(b,c,d,a,k[7],22,-45705983);
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

/* ---------- Bybit V5 sign + wrappers (linear futures) ---------- */
async function bybitV5Headers(apiKey, apiSecret, payloadStr = "") {
  const ts = Date.now().toString();
  const recv = "5000";
  const pre = ts + apiKey + recv + payloadStr;
  const sig = await hmacHexStr(apiSecret, pre, "SHA-256");
  return {
    "X-BAPI-API-KEY": apiKey,
    "X-BAPI-TIMESTAMP": ts,
    "X-BAPI-RECV-WINDOW": recv,
    "X-BAPI-SIGN": sig,
    "Content-Type": "application/json"
  };
}
async function bybitV5POST(ex, apiKey, apiSecret, path, payload, timeoutMs = 8000) {
  const body = JSON.stringify(payload || {});
  const headers = await bybitV5Headers(apiKey, apiSecret, body);
  const url = `${ex.baseUrl}${path}`;
  const r = await safeFetch(url, { method: "POST", headers, body }, timeoutMs).catch(()=>null);
  const d = r ? await r.json().catch(()=> ({})) : {};
  if (!r || !r.ok || d?.retCode !== 0) throw new Error(d?.retMsg || `HTTP ${r?.status || 0}`);
  return d;
}
async function bybitV5GET(ex, apiKey, apiSecret, path, qsObj = {}, timeoutMs = 8000) {
  const sp = new URLSearchParams(qsObj);
  const qs = sp.toString();
  const headers = await bybitV5Headers(apiKey, apiSecret, qs);
  const url = `${ex.baseUrl}${path}?${qs}`;
  const r = await safeFetch(url, { method: "GET", headers }, timeoutMs).catch(()=>null);
  const d = r ? await r.json().catch(()=> ({})) : {};
  if (!r || !r.ok || d?.retCode !== 0) throw new Error(d?.retMsg || `HTTP ${r?.status || 0}`);
  return d;
}

/* ---------- Balance parsers ---------- */
function parseUSDT_BinanceLike(data) {
  const balances = Array.isArray(data?.balances) ? data.balances : [];
  const get = (asset) => {
    const row = balances.find(b => (b.asset || b.currency || "").toUpperCase() === asset);
    return row ? (num(row.free ?? row.available ?? row.freeBalance ?? row.balance ?? row.total) ?? null) : null;
  };

  const usdt = get("USDT");
  if (usdt !== null) return usdt.toFixed(2);

  // Fallback: sum known USD stables if USDT row missing/empty
  const stables = ["USDT","USDC","BUSD","TUSD","FDUSD","USDP","DAI","USD"];
  let sum = 0, got = false;
  for (const s of stables) {
    const v = get(s);
    if (v !== null) { sum += v; got = true; }
  }
  return got ? sum.toFixed(2) : "0.00";
}
function parseUSDT_Gate(data) {
  const arr = Array.isArray(data) ? data : [];
  const row = arr.find(x => (x.currency || "").toUpperCase() === "USDT");
  if (!row) return "0.00";
  const n = num(row.available ?? row.free ?? row.balance);
  return n !== null ? n.toFixed(2) : "0.00";
}
function parseUSDT_Bybit(data) {
  // Unified account (v5) typical shape: result.list[0]
  const list0 = data?.result?.list?.[0] || {};
  const coins = Array.isArray(list0.coin) ? list0.coin : [];

  // Prefer USDT from coin[] (ignore empty strings)
  const usdt = coins.find(c => (c.coin || "").toUpperCase() === "USDT");
  if (usdt) {
    const n =
      num(usdt.availableToWithdraw) ??
      num(usdt.walletBalance) ??
      num(usdt.equity) ??
      num(usdt.marginBalance);
    if (n !== null) return n.toFixed(2);
  }

  // Fallback to unified totals
  const nAvail = num(list0.totalAvailableBalance);
  if (nAvail !== null) return nAvail.toFixed(2);

  const nWallet = num(list0.totalWalletBalance);
  if (nWallet !== null) return nWallet.toFixed(2);

  const nEquity = num(list0.totalEquity);
  if (nEquity !== null) return nEquity.toFixed(2);

  // Older spot-like fallbacks
  const spot = data?.result?.balances || data?.result?.spot || [];
  if (Array.isArray(spot)) {
    const row = spot.find(x => (x.coin || x.asset || "").toUpperCase() === "USDT");
    if (row) {
      const n = num(row.free ?? row.available ?? row.walletBalance ?? row.equity);
      if (n !== null) return n.toFixed(2);
    }
  }
  return "0.00";
}
function parseUSDT_Kraken(data) {
  const v = data?.result ? (data.result.USDT ?? data.result.usdt) : null;
  const n = num(v);
  return n !== null ? n.toFixed(2) : "0.00";
}
function parseUSDT_CoinEx(data) {
  const balances = Array.isArray(data?.data?.balances) ? data.data.balances : null;
  if (balances) {
    const row = balances.find(b => (b.asset || b.currency || "").toUpperCase() === "USDT");
    const n = num(row?.available ?? row?.available_balance ?? row?.balance);
    if (n !== null) return n.toFixed(2);
  }
  const list = data?.data?.list;
  if (list && typeof list === "object" && list.USDT) {
    const v = list.USDT.available ?? list.USDT.available_amount ?? list.USDT.balance;
    const n = num(v);
    if (n !== null) return n.toFixed(2);
  }
  return "0.00";
}
function parseUSDT_Huobi(data) {
  const arr = Array.isArray(data?.data?.list) ? data.data.list : [];
  let sum = 0, got = false;
  for (const r of arr) {
    if ((r.currency || "").toUpperCase() === "USDT" && r.type === "trade") {
      const n = num(r.balance);
      if (n !== null) { sum += n; got = true; }
    }
  }
  return got ? sum.toFixed(2) : "0.00";
}
function parseUSDT_LBank(data) {
  const free = data?.info?.funds?.free || data?.data?.free || data?.free || {};
  const v = free.usdt ?? free.USDT;
  const n = num(v);
  return n !== null ? n.toFixed(2) : "0.00";
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
  // Build numeric available/equity
  const balances = Array.isArray(data?.balances) ? data.balances : [];
  const pick = (sym) => {
    const row = balances.find(b => (b.asset || b.currency || "").toUpperCase() === sym);
    return row
      ? {
          free: num(row.free ?? row.available ?? row.freeBalance ?? row.balance ?? row.total),
          locked: num(row.locked ?? row.freeze ?? row.frozen),
        }
      : null;
  };

  let available = 0, equity = 0;
  const usdt = pick("USDT");
  if (usdt && usdt.free !== null) {
    available = usdt.free || 0;
    equity = (usdt.free || 0) + (usdt.locked || 0);
  } else {
    // Stable-coin fallback if no USDT row (covers USDC/BUSD/TUSD/FDUSD/USDP/DAI)
    const stables = ["USDT","USDC","BUSD","TUSD","FDUSD","USDP","DAI","USD"];
    let freeSum = 0, lockedSum = 0, hit = false;
    for (const s of stables) {
      const r = pick(s);
      if (!r) continue;
      if (r.free !== null) { freeSum += r.free; hit = true; }
      if (r.locked !== null) lockedSum += r.locked;
    }
    if (hit) { available = freeSum; equity = freeSum + lockedSum; }
  }

  // total USD equity across all assets (fallback when USDT free is dust)
  let totalEq = 0;
  for (const b of balances) {
    const asset = (b.asset || b.currency || '').toUpperCase();
    const free = num(b.free ?? b.available ?? b.freeBalance ?? b.balance ?? b.total) ?? 0;
    const locked = num(b.locked ?? b.freeze ?? b.frozen) ?? 0;
    const qty = free + locked;
    if (qty <= 0) continue;
    if (asset === 'USDT') { totalEq += qty; continue; }
    try {
      const px = await getCurrentPrice(asset);
      if (px > 0) totalEq += qty * px;
    } catch {}
  }
  // prefer the larger of the two (covers non-stable holdings)
  equity = Math.max(equity, totalEq);

  const maker = (data?.makerCommission ?? 10) / 10000;
  const taker = (data?.takerCommission ?? 10) / 10000;
  return {
    success: true,
    data: {
      balance: `${available.toFixed(2)} USDT`,
      available,
      equity,
      feeRate: taker,
      fees: { maker, taker }
    }
  };
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
  let available = 0, wallet = 0;
  try {
    const usdt = (data.assets || []).find(a => (a.asset || "").toUpperCase() === "USDT");
    if (usdt) {
      available = num(usdt.availableBalance) ?? 0;
      wallet = num(usdt.walletBalance) ?? available;
    }
  } catch {}
  return {
    success: true,
    data: {
      balance: `${available.toFixed(2)} USDT`,
      available,
      equity: wallet,
      feeRate: 0.0004
    }
  };
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
        "X-BAPI-API-KEY": apiKey,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv,
        "X-BAPI-SIGN": sig
      }
    }, 5000).catch(()=>null);
    const d = r ? await r.json().catch(()=>({})) : {};
    return { ok: r && r.ok && d?.retCode === 0, d, status: r?.status || 0 };
  };

  let last = null;
  for (const kind of ["UNIFIED", "CONTRACT", "SPOT"]) {
    const r = await tryKind(kind);
    last = r;
    if (r.ok) {
      const list0 = r.d?.result?.list?.[0] || {};
      const coins = Array.isArray(list0.coin) ? list0.coin : [];
      const usdt = coins.find(x => (x.coin || "").toUpperCase() === "USDT");
      const availCoin = (num(usdt?.availableToWithdraw) ?? num(usdt?.walletBalance) ?? num(usdt?.equity));
      const totalAvail = num(list0.totalAvailableBalance);
      const available = (totalAvail ?? availCoin ?? 0);
      const equity = (num(list0.totalEquity) ?? num(list0.totalWalletBalance) ?? available);
      const balanceLabel = availCoin !== null ? "USDT" : "USD";
      return {
        success: true,
        data: {
          balance: `${available.toFixed(2)} ${balanceLabel}`,
          available,
          equity,
          feeRate: 0.0006
        }
      };
    }
  }
  return { success: false, reason: last?.d?.retMsg || `HTTP ${last?.status || 0}` };
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
  const v = data?.result ? (data.result.USDT ?? data.result.usdt) : null;
  const available = num(v) ?? 0;
  return {
    success: true,
    data: {
      balance: `${available.toFixed(2)} USDT`,
      available,
      equity: available
    }
  };
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
  const arr = Array.isArray(data) ? data : [];
  const row = arr.find(x => (x.currency || "").toUpperCase() === "USDT");
  const available = num(row?.available ?? row?.free ?? row?.balance) ?? 0;
  const locked = num(row?.locked ?? row?.freeze) ?? 0;
  const equity = available + locked;
  return {
    success: true,
    data: {
      balance: `${available.toFixed(2)} USDT`,
      available,
      equity
    }
  };
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
  const arr = Array.isArray(r2.data?.data?.list) ? r2.data.data.list : [];
  let available = 0, frozen = 0;
  for (const it of arr) {
    if ((it.currency || "").toUpperCase() !== "USDT") continue;
    const n = num(it.balance);
    if (n === null) continue;
    if (it.type === "trade") available += n;
    else if (it.type === "frozen") frozen += n;
  }
  const equity = available + frozen;
  return {
    success: true,
    data: {
      balance: `${available.toFixed(2)} USDT`,
      available,
      equity
    }
  };
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
  const balances = Array.isArray(data?.data?.balances) ? data.data.balances : null;
  let available = 0, frozen = 0;
  if (balances) {
    const row = balances.find(b => (b.asset || b.currency || "").toUpperCase() === "USDT");
    available = num(row?.available ?? row?.available_balance ?? row?.balance) ?? 0;
    frozen = num(row?.frozen ?? row?.freeze) ?? 0;
  } else if (data?.data?.list?.USDT) {
    const r = data.data.list.USDT;
    available = num(r.available ?? r.available_amount ?? r.balance) ?? 0;
    frozen = num(r.frozen ?? r.freeze) ?? 0;
  }
  const equity = available + frozen;
  return {
    success: true,
    data: {
      balance: `${available.toFixed(2)} USDT`,
      available,
      equity
    }
  };
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
  if (!res.ok || data?.result === false) throw new Error(data?.error_code || data?.msg || `HTTP ${res.status}`);
  const free = data?.info?.funds?.free || data?.data?.free || data?.free || {};
  const freeze = data?.info?.funds?.freeze || data?.info?.funds?.freezed || data?.data?.freeze || data?.freeze || {};
  const available = num(free.usdt ?? free.USDT) ?? 0;
  const locked = num(freeze.usdt ?? freeze.USDT) ?? 0;
  const equity = available + locked;
  return {
    success: true,
    data: {
      balance: `${available.toFixed(2)} USDT`,
      available,
      equity
    }
  };
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
        return {
          success: true,
          data: {
            balance: "10000.00 USDT",
            available: 10000,
            equity: 10000,
            feeRate: 0.001,
            fees: { maker: 0.001, taker: 0.001 }
          }
        };

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
      case "bybitFuturesV5":     return await verifyBybit(apiKey, apiSecret, ex); // uses same wallet call

      default: return { success: false, reason: "Not implemented." };
    }
  } catch (e) {
    console.error("verifyApiKeys error:", e);
    return { success: false, reason: "Error talking to exchange." };
  }
}

/* ---------- Orders (Spot/Futures) ---------- */
/* Spot MARKET (Binance-like: Binance, MEXC) */
async function placeBinanceLikeOrder(ex, apiKey, apiSecret, symbol, side, amount, isQuoteOrder, clientOrderId) {
  const endpoint = "/api/v3/order";
  const params = { symbol: symbol + "USDT", side, type: "MARKET", newOrderRespType: "FULL", timestamp: Date.now() };

  if (clientOrderId) params.newClientOrderId = clientOrderId;

  if (side === "BUY" && isQuoteOrder) {
    params.quoteOrderQty = Number(amount).toFixed(8);
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
async function placeBinanceLikeLimitMaker(ex, apiKey, apiSecret, symbol, side, limitPrice, amount, isQuoteOrder, clientOrderId) {
  const endpoint = "/api/v3/order";
  const params = {
    symbol: symbol.toUpperCase() + "USDT",
    side,
    type: "LIMIT_MAKER",
    newOrderRespType: "FULL",
    price: Number(limitPrice).toFixed(8),
    timestamp: Date.now()
  };

  if (clientOrderId) params.newClientOrderId = clientOrderId;

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
  if (!res.ok) throw new Error(data?.msg || data?.message || `HTTP ${res.status}`);

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
async function placeBinanceFuturesOrder(ex, apiKey, apiSecret, symbol, side, baseQty, reduceOnly = false, clientOrderId) {
  const endpoint = "/fapi/v1/order";
  const params = {
    symbol: symbol.toUpperCase() + "USDT",
    side, type: "MARKET",
    quantity: Number(baseQty).toFixed(6),
    reduceOnly: reduceOnly ? "true" : "false",
    newOrderRespType: "RESULT",
    timestamp: Date.now()
  };
  if (clientOrderId) params.newClientOrderId = clientOrderId;

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
async function placeBinanceFuturesLimitPostOnly(ex, apiKey, apiSecret, symbol, side, baseQty, price, reduceOnly = false, clientOrderId) {
  const endpoint = "/fapi/v1/order";
  const params = {
    symbol: symbol.toUpperCase() + "USDT",
    side, type: "LIMIT",
    timeInForce: "GTX",
    price: Number(price).toFixed(8),
    quantity: Number(baseQty).toFixed(6),
    reduceOnly: reduceOnly ? "true" : "false",
    newOrderRespType: "RESULT",
    timestamp: Date.now()
  };
  if (clientOrderId) params.newClientOrderId = clientOrderId;

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

/* ---------- Bybit Futures V5 MARKET ---------- */
async function placeBybitFuturesOrder(ex, apiKey, apiSecret, symbol, side, baseQty, reduceOnly = false, clientOrderId) {
  const sym = symbol.toUpperCase() + "USDT";
  const payload = {
    category: "linear",
    symbol: sym,
    side: side === "BUY" ? "Buy" : "Sell",
    orderType: "Market",
    qty: Number(baseQty).toFixed(6),
    reduceOnly: !!reduceOnly,
    orderLinkId: clientOrderId || undefined
  };
  const d = await bybitV5POST(ex, apiKey, apiSecret, "/v5/order/create", payload, 8000);
  const orderId = d?.result?.orderId || d?.result?.orderID;

  // Query status for exec details
  const st = await getBybitFuturesOrderStatus(ex, apiKey, apiSecret, symbol, { orderId, orderLinkId: clientOrderId });
  return { orderId, executedQty: st.executedQty, avgPrice: st.avgPrice, status: st.status };
}

/* ---------- Bybit Futures V5 LIMIT (PostOnly) ---------- */
async function placeBybitFuturesLimitPostOnly(ex, apiKey, apiSecret, symbol, side, baseQty, price, reduceOnly = false, clientOrderId) {
  const sym = symbol.toUpperCase() + "USDT";
  const payload = {
    category: "linear",
    symbol: sym,
    side: side === "BUY" ? "Buy" : "Sell",
    orderType: "Limit",
    qty: Number(baseQty).toFixed(6),
    price: Number(price).toFixed(8),
    timeInForce: "PostOnly",
    reduceOnly: !!reduceOnly,
    orderLinkId: clientOrderId || undefined
  };
  const d = await bybitV5POST(ex, apiKey, apiSecret, "/v5/order/create", payload, 8000);
  const orderId = d?.result?.orderId || d?.result?.orderID;
  const st = await getBybitFuturesOrderStatus(ex, apiKey, apiSecret, symbol, { orderId, orderLinkId: clientOrderId });
  return { orderId, executedQty: st.executedQty, avgPrice: st.avgPrice, status: st.status || "NEW" };
}

/* ---------- Optional: Exit/Bracket orders (default OFF, UX unchanged) ---------- */
/* Spot (Binance-like): place TAKE_PROFIT_LIMIT and STOP_LOSS_LIMIT with CID suffixes.
   Note: OCO is possible but per-order client IDs are limited; this approach keeps explicit suffixes. */
async function placeBinanceLikeExitOrders(ex, apiKey, apiSecret, symbol, isLong, baseQty, tpPrice, slPrice, clientOrderIdPrefix) {
  // For spot, exits are placed on SELL side for long, BUY side for short (rare on spot).
  const sideExit = isLong ? "SELL" : "BUY";
  const common = {
    symbol: symbol.toUpperCase() + "USDT",
    side: sideExit,
    timeInForce: "GTC",
    newOrderRespType: "RESULT",
    timestamp: Date.now()
  };

  // TAKE_PROFIT_LIMIT
  const tpParams = {
    ...common,
    type: "TAKE_PROFIT_LIMIT",
    quantity: Number(baseQty).toFixed(6),
    price: Number(tpPrice).toFixed(8),
    stopPrice: Number(tpPrice).toFixed(8),
    newClientOrderId: clientOrderIdPrefix ? `${clientOrderIdPrefix}:tp` : undefined
  };

  // STOP_LOSS_LIMIT
  const slParams = {
    ...common,
    type: "STOP_LOSS_LIMIT",
    quantity: Number(baseQty).toFixed(6),
    price: Number(slPrice).toFixed(8),
    stopPrice: Number(slPrice).toFixed(8),
    newClientOrderId: clientOrderIdPrefix ? `${clientOrderIdPrefix}:sl` : undefined
  };

  const endpoint = "/api/v3/order";
  const submit = async (params) => {
    const p = { ...params };
    if (ex.defaultQuery) {
      const defaults = new URLSearchParams(ex.defaultQuery);
      for (const [k, v] of defaults) p[k] = v;
    }
    const qs = Object.entries(p).filter(([,v])=>v!==undefined).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
    const sig = await hmacHexStr(apiSecret, qs);
    const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
    const r = await safeFetch(url, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
    const d = await r.json().catch(()=>({}));
    if (!r.ok) throw new Error(d?.msg || d?.message || `HTTP ${r.status}`);
    return d;
  };

  const tpRes = await submit(tpParams).catch(e => { console.warn("TP order failed:", e?.message||e); return null; });
  const slRes = await submit(slParams).catch(e => { console.warn("SL order failed:", e?.message||e); return null; });
  return {
    tp: tpRes ? { orderId: tpRes.orderId, status: tpRes.status } : null,
    sl: slRes ? { orderId: slRes.orderId, status: slRes.status } : null
  };
}

/* Futures (Binance USDT-M): place TAKE_PROFIT_MARKET and STOP_MARKET reduceOnly with CID suffixes */
async function placeBinanceFuturesBracketOrders(ex, apiKey, apiSecret, symbol, isLong, baseQty, tpPrice, slPrice, clientOrderIdPrefix) {
  const endpoint = "/fapi/v1/order";
  const placeOne = async (type, stopPrice, side, cidSuffix) => {
    const params = {
      symbol: symbol.toUpperCase() + "USDT",
      side,
      type,
      stopPrice: Number(stopPrice).toFixed(8),
      reduceOnly: "true",
      workingType: "CONTRACT_PRICE",
      newOrderRespType: "RESULT",
      timestamp: Date.now(),
      quantity: Number(baseQty).toFixed(6)
    };
    if (clientOrderIdPrefix) params.newClientOrderId = `${clientOrderIdPrefix}:${cidSuffix}`;
    const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
    const sig = await hmacHexStr(apiSecret, qs);
    const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
    const r = await safeFetch(url, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 8000);
    const d = await r.json().catch(()=>({}));
    if (!r.ok) throw new Error(d?.msg || d?.message || `HTTP ${r.status}`);
    return d;
  };

  // For a long: exit side is SELL; for a short: BUY
  const sideExit = isLong ? "SELL" : "BUY";
  // TP trigger
  const tpType = "TAKE_PROFIT_MARKET";
  // SL trigger
  const slType = "STOP_MARKET";

  const tpRes = await placeOne(tpType, tpPrice, sideExit, "tp").catch(e => { console.warn("Futures TP failed:", e?.message||e); return null; });
  const slRes = await placeOne(slType, slPrice, sideExit, "sl").catch(e => { console.warn("Futures SL failed:", e?.message||e); return null; });
  return {
    tp: tpRes ? { orderId: tpRes.orderId, status: tpRes.status } : null,
    sl: slRes ? { orderId: slRes.orderId, status: slRes.status } : null
  };
}

/* ---------- Bybit Futures V5 Brackets (reduce-only TP/SL as conditional) ---------- */
async function placeBybitFuturesBracketOrders(ex, apiKey, apiSecret, symbol, isLong, baseQty, tpPrice, slPrice, clientOrderIdPrefix) {
  const sym = symbol.toUpperCase() + "USDT";
  const exitSide = isLong ? "Sell" : "Buy";
  const common = { category: "linear", symbol: sym, side: exitSide, orderType: "Market", qty: Number(baseQty).toFixed(6), reduceOnly: true, triggerBy: "LastPrice" };

  const mkPayload = (triggerPrice, suffix) => ({
    ...common,
    triggerPrice: Number(triggerPrice).toFixed(8),
    orderLinkId: clientOrderIdPrefix ? `${clientOrderIdPrefix}:${suffix}` : undefined
  });

  let tp = null, sl = null;
  try { const d1 = await bybitV5POST(ex, apiKey, apiSecret, "/v5/order/create", mkPayload(tpPrice, "tp"), 8000); tp = { orderId: d1?.result?.orderId, status: "NEW" }; } catch(e) { console.warn("Bybit TP failed:", e?.message||e); }
  try { const d2 = await bybitV5POST(ex, apiKey, apiSecret, "/v5/order/create", mkPayload(slPrice, "sl"), 8000); sl = { orderId: d2?.result?.orderId, status: "NEW" }; } catch(e) { console.warn("Bybit SL failed:", e?.message||e); }

  return { tp, sl };
}

/* ---------- Ensure 1x Isolated (Binance Futures + Bybit V5) ---------- */
async function ensureBinanceFuturesIsolated1x(ex, apiKey, apiSecret, symbol) {
  const sym = symbol.toUpperCase() + "USDT";
  // margin type
  try {
    const p1 = { symbol: sym, marginType: "ISOLATED", timestamp: Date.now() };
    const qs1 = Object.entries(p1).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
    const sig1 = await hmacHexStr(apiSecret, qs1);
    const url1 = `${ex.baseUrl}/fapi/v1/marginType?${qs1}&signature=${sig1}`;
    await safeFetch(url1, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 6000);
  } catch(_) { /* already isolated */ }
  // leverage=1
  try {
    const p2 = { symbol: sym, leverage: 1, timestamp: Date.now() };
    const qs2 = Object.entries(p2).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
    const sig2 = await hmacHexStr(apiSecret, qs2);
    const url2 = `${ex.baseUrl}/fapi/v1/leverage?${qs2}&signature=${sig2}`;
    await safeFetch(url2, { method: "POST", headers: { [ex.apiKeyHeader]: apiKey } }, 6000);
  } catch(_) {}
}
async function ensureBybitFuturesIsolated1x(ex, apiKey, apiSecret, symbol) {
  const sym = symbol.toUpperCase() + "USDT";
  // tradeMode=1 (Isolated)
  try {
    await bybitV5POST(ex, apiKey, apiSecret, "/v5/position/switch-isolated", {
      category: "linear", symbol: sym, tradeMode: 1
    }, 6000);
  } catch(_) {}
  // leverage=1/1
  try {
    await bybitV5POST(ex, apiKey, apiSecret, "/v5/position/set-leverage", {
      category: "linear", symbol: sym, buyLeverage: "1", sellLeverage: "1"
    }, 6000);
  } catch(_) {}
}
async function ensureFuturesIsolated1x(ex, apiKey, apiSecret, symbol) {
  if (ex.kind === "binanceFuturesUSDT") return ensureBinanceFuturesIsolated1x(ex, apiKey, apiSecret, symbol);
  if (ex.kind === "bybitFuturesV5")     return ensureBybitFuturesIsolated1x(ex, apiKey, apiSecret, symbol);
}

/* ---------- Demo order (robust price fallback) ---------- */
async function placeDemoOrder(symbol, side, amount, isQuoteOrder, clientOrderId) {
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
  return { orderId: clientOrderId || `demo-${crypto.randomUUID()}`, executedQty, avgPrice: currentPrice, status: 'FILLED' };
}

/* ---------- Market order routing based on session exchange ---------- */
async function placeMarketBuy(env, userId, symbol, quoteAmount, opts = {}) {
  const session = await getSession(env, userId);
  const ex = SUPPORTED_EXCHANGES[session.exchange_name];

  if (ex.kind === 'demoParrot') {
    return await placeDemoOrder(symbol, "BUY", quoteAmount, true, opts.clientOrderId);
  }

  // Futures-only enforcement (non-demo)
  if (["binanceLike","lbankV2","coinexV2"].includes(ex.kind)) {
    throw new Error("Futures-only mode: pick a futures exchange (Bybit Futures or Binance Futures).");
  }

  const apiKey = await decrypt(session.api_key_encrypted, env);
  const apiSecret = await decrypt(session.api_secret_encrypted, env);

  switch (ex.kind) {
    case "binanceFuturesUSDT": {
      const price = await getCurrentPrice(symbol);
      const baseQty = Number(quoteAmount) / price;
      await ensureFuturesIsolated1x(ex, apiKey, apiSecret, symbol);
      return await placeBinanceFuturesOrder(ex, apiKey, apiSecret, symbol, "BUY", baseQty, opts.reduceOnly === true, opts.clientOrderId);
    }
    case "bybitFuturesV5": {
      const price = await getCurrentPrice(symbol);
      const baseQty = Number(quoteAmount) / price;
      await ensureFuturesIsolated1x(ex, apiKey, apiSecret, symbol);
      return await placeBybitFuturesOrder(ex, apiKey, apiSecret, symbol, "BUY", baseQty, opts.reduceOnly === true, opts.clientOrderId);
    }
    default:
      throw new Error(`Orders not supported for ${ex.label}`);
  }
}

async function placeMarketSell(env, userId, symbol, amount, isQuoteOrder = false, opts = {}) {
  const session = await getSession(env, userId);
  const ex = SUPPORTED_EXCHANGES[session.exchange_name];

  if (ex.kind === 'demoParrot') {
    return await placeDemoOrder(symbol, "SELL", amount, isQuoteOrder, opts.clientOrderId);
  }

  // Futures-only enforcement (non-demo)
  if (["binanceLike","lbankV2","coinexV2"].includes(ex.kind)) {
    throw new Error("Futures-only mode: pick a futures exchange (Bybit Futures or Binance Futures).");
  }

  const apiKey = await decrypt(session.api_key_encrypted, env);
  const apiSecret = await decrypt(session.api_secret_encrypted, env);

  switch (ex.kind) {
    case "binanceFuturesUSDT": {
      let baseQty = Number(amount);
      if (isQuoteOrder) {
        const price = await getCurrentPrice(symbol);
        baseQty = Number(amount) / price;
      }
      await ensureFuturesIsolated1x(ex, apiKey, apiSecret, symbol);
      return await placeBinanceFuturesOrder(ex, apiKey, apiSecret, symbol, "SELL", baseQty, opts.reduceOnly === true, opts.clientOrderId);
    }
    case "bybitFuturesV5": {
      let baseQty = Number(amount);
      if (isQuoteOrder) {
        const price = await getCurrentPrice(symbol);
        baseQty = Number(amount) / price;
      }
      await ensureFuturesIsolated1x(ex, apiKey, apiSecret, symbol);
      return await placeBybitFuturesOrder(ex, apiKey, apiSecret, symbol, "SELL", baseQty, opts.reduceOnly === true, opts.clientOrderId);
    }
    default:
      throw new Error(`Orders not supported for ${ex.label}`);
  }
}

/* ---------- Order status/cancel helpers (used by reconciliation/mgmt) ---------- */
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
async function cancelBinanceLikeOrder(ex, apiKey, apiSecret, symbol, { orderId, origClientOrderId }) {
  const endpoint = "/api/v3/order";
  const params = {
    symbol: symbol.toUpperCase() + "USDT",
    timestamp: Date.now(),
    ...(orderId ? { orderId: String(orderId) } : {}),
    ...(origClientOrderId ? { origClientOrderId } : {})
  };
  const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, qs);
  const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
  const r = await safeFetch(url, { method: "DELETE", headers: { [ex.apiKeyHeader]: apiKey } }, 6000);
  const d = await r.json().catch(()=>({}));
  if (!r.ok) throw new Error(d?.msg || d?.message || `HTTP ${r.status}`);
  return d;
}
async function cancelBinanceFuturesOrder(ex, apiKey, apiSecret, symbol, { orderId, origClientOrderId }) {
  const endpoint = "/fapi/v1/order";
  const params = {
    symbol: symbol.toUpperCase() + "USDT",
    timestamp: Date.now(),
    ...(orderId ? { orderId: String(orderId) } : {}),
    ...(origClientOrderId ? { origClientOrderId } : {})
  };
  const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, qs);
  const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
  const r = await safeFetch(url, { method: "DELETE", headers: { [ex.apiKeyHeader]: apiKey } }, 6000);
  const d = await r.json().catch(()=>({}));
  if (!r.ok) throw new Error(d?.msg || d?.message || `HTTP ${r.status}`);
  return d;
}

/* ---------- Bybit Futures V5 Status/Cancel ---------- */
async function getBybitFuturesOrderStatus(ex, apiKey, apiSecret, symbol, { orderId, orderLinkId }) {
  const sym = symbol.toUpperCase() + "USDT";
  const d = await bybitV5GET(ex, apiKey, apiSecret, "/v5/order/realtime", {
    category: "linear",
    symbol: sym,
    ...(orderId ? { orderId } : {}),
    ...(orderLinkId ? { orderLinkId } : {})
  }, 8000);
  const row = (d?.result?.list || [])[0] || {};
  const status = row.orderStatus || "";
  const executedQty = parseFloat(row.cumExecQty || "0");
  const avgPrice = parseFloat(row.avgPrice || "0");
  return { status, executedQty, avgPrice };
}
async function cancelBybitFuturesOrder(ex, apiKey, apiSecret, symbol, { orderId, orderLinkId }) {
  const sym = symbol.toUpperCase() + "USDT";
  const d = await bybitV5POST(ex, apiKey, apiSecret, "/v5/order/cancel", {
    category: "linear",
    symbol: sym,
    ...(orderId ? { orderId } : {}),
    ...(orderLinkId ? { orderLinkId } : {})
  }, 8000);
  return d?.result || {};
}

/* ---------- Trades/fills fetchers (for reconciliation; default OFF, UX unchanged) ---------- */
async function getBinanceLikeMyTrades(ex, apiKey, apiSecret, symbol, { startTime, endTime, limit = 1000 } = {}) {
  const endpoint = "/api/v3/myTrades";
  const params = {
    symbol: symbol.toUpperCase() + "USDT",
    timestamp: Date.now(),
    ...(Number.isFinite(startTime) ? { startTime: Math.floor(startTime) } : {}),
    ...(Number.isFinite(endTime)   ? { endTime:   Math.floor(endTime) }   : {}),
    ...(limit ? { limit: Math.max(1, Math.min(1000, limit)) } : {})
  };
  if (ex.defaultQuery) {
    const defaults = new URLSearchParams(ex.defaultQuery);
    for (const [k, v] of defaults) params[k] = v;
  }
  const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, qs);
  const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
  const r = await safeFetch(url, { headers: { [ex.apiKeyHeader]: apiKey } }, 8000).catch(()=>null);
  if (!r) return [];
  const d = await r.json().catch(()=>[]);
  return Array.isArray(d) ? d : [];
}
async function getBinanceFuturesUserTrades(ex, apiKey, apiSecret, symbol, { startTime, endTime, limit = 1000 } = {}) {
  const endpoint = "/fapi/v1/userTrades";
  const params = {
    symbol: symbol.toUpperCase() + "USDT",
    timestamp: Date.now(),
    ...(Number.isFinite(startTime) ? { startTime: Math.floor(startTime) } : {}),
    ...(Number.isFinite(endTime)   ? { endTime:   Math.floor(endTime) }   : {}),
    ...(limit ? { limit: Math.max(1, Math.min(1000, limit)) } : {})
  };
  const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
  const sig = await hmacHexStr(apiSecret, qs);
  const url = `${ex.baseUrl}${endpoint}?${qs}&signature=${sig}`;
  const r = await safeFetch(url, { headers: { [ex.apiKeyHeader]: apiKey } }, 8000).catch(()=>null);
  if (!r) return [];
  const d = await r.json().catch(()=>[]);
  return Array.isArray(d) ? d : [];
}

/* ---------- Bybit Futures V5 fills (for reconciliation) ---------- */
async function getBybitFuturesUserTrades(ex, apiKey, apiSecret, symbol, { startTime, endTime, limit = 100 } = {}) {
  const sym = symbol.toUpperCase() + "USDT";
  const d = await bybitV5GET(ex, apiKey, apiSecret, "/v5/execution/list", {
    category: "linear",
    symbol: sym,
    ...(Number.isFinite(startTime) ? { startTime: Math.floor(startTime) } : {}),
    ...(Number.isFinite(endTime) ? { endTime: Math.floor(endTime) } : {}),
    ...(limit ? { limit: Math.max(1, Math.min(1000, limit)) } : {})
  }, 8000).catch(()=>({ result: { list: [] }}));
  const list = d?.result?.list || [];
  // Normalize to resemble Binance-like records used downstream
  return list.map(x => ({
    id: x.execId,
    orderId: x.orderId,
    side: (x.side || "").toUpperCase() === "BUY" ? "BUY" : "SELL",
    isBuyer: (x.side || "").toUpperCase() === "BUY",
    price: x.execPrice,
    qty: x.execQty,
    isMaker: x.isMaker === "1" || x.isMaker === true,
    commission: x.fee,
    commissionAsset: x.feeAsset
  }));
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

  // NEW: Bybit futures orders/brackets
  placeBybitFuturesOrder, placeBybitFuturesLimitPostOnly, placeBybitFuturesBracketOrders,

  // optional exits (enabled from other sections; defaults keep UX unchanged)
  placeBinanceLikeExitOrders, placeBinanceFuturesBracketOrders,

  // demo + routing
  placeDemoOrder,
  placeMarketBuy, placeMarketSell,

  // status/cancel + fills for reconciliation
  getBinanceLikeOrderStatus, getBinanceFuturesOrderStatus,
  cancelBinanceLikeOrder, cancelBinanceFuturesOrder,
  getBinanceLikeMyTrades, getBinanceFuturesUserTrades,

  // NEW: Bybit status/cancel/fills
  getBybitFuturesOrderStatus, cancelBybitFuturesOrder, getBybitFuturesUserTrades,

  // NEW: ensure isolated 1x
  ensureFuturesIsolated1x
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

/* ---------- Gist lifecycle hooks (no UX impact) ---------- */
async function gistMarkOpen(env, trade, entryPrice, qty) {
  try {
    let ex = {};
    try { ex = JSON.parse(trade.extra_json || '{}'); } catch {}
    const cid = ex?.client_order_id;
    if (!cid) return;
    await gistPatchState(env, (state) => {
      const idx = gistFindPendingIdxByCID(state, cid);
      if (idx >= 0) {
        state.pending[idx].status = "open";
        state.pending[idx].entry_ts_ms = Date.now();
        state.pending[idx].entry_price = entryPrice;
        state.pending[idx].qty = qty;
      }
      return state;
    });
  } catch {}
}

/* Normalize exit reason to spec: 'tp' | 'sl' | 'ttl' | 'manual' */
function normalizeExitReason(reason) {
  const r = String(reason || "").toLowerCase();
  if (r.includes("tp")) return "tp";
  if (r.includes("sl")) return "sl";
  if (r === "ttl" || r === "time" || r === "time_stop") return "ttl";
  if (r === "manual" || r === "mgmt_early_cut") return "manual";
  return r || "manual";
}

/* Safe bps helper */
const toBps = (x) => Math.round((Number(x) || 0) * 10000);

/* ---------- gistReportClosed: enriched details (backward compatible) ---------- */
async function gistReportClosed(env, trade, exitPrice, exitReason, protocolFeeRate, details = null) {
  try {
    let extra = {};
    try { extra = JSON.parse(trade.extra_json || '{}'); } catch {}
    const cid = extra?.client_order_id || "";
    const isShort = trade.side === 'SELL';
    const entry = Number(trade.entry_price || 0), qty = Number(trade.qty || 0);
    const gross = !isShort ? (exitPrice - entry) * qty : (entry - exitPrice) * qty;

    // Fees: either estimated from fee rate or from exchange reconciliation
    let commissionQuoteUSDT = (entry * qty + exitPrice * qty) * (protocolFeeRate ?? 0.001);
    let commissionBps = entry > 0 && qty > 0 ? (commissionQuoteUSDT / (entry * qty)) * 10000 : 0;
    let makerTakerEntry = null, makerTakerExit = null;
    let commissionAssetEntry = null, commissionAssetExit = null;
    let fingerprintEntry = null, fingerprintExit = null;

    if (details && details.fromExchange === true) {
      if (Number.isFinite(details.commission_quote_usdt)) commissionQuoteUSDT = details.commission_quote_usdt;
      if (Number.isFinite(details.commission_bps)) commissionBps = details.commission_bps;
      makerTakerEntry = details.maker_taker_entry ?? null;
      makerTakerExit = details.maker_taker_exit ?? null;
      commissionAssetEntry = details.commission_asset_entry ?? null;
      commissionAssetExit = details.commission_asset_exit ?? null;
      fingerprintEntry = details.fingerprint_entry ?? null;
      fingerprintExit = details.fingerprint_exit ?? null;
    }

    const net = gross - commissionQuoteUSDT;
    const retFrac = entry > 0 ? (!isShort ? (exitPrice / entry - 1) : (entry / exitPrice - 1)) : 0;
    const pnl_bps = (retFrac * 10000) - (commissionBps || 0);
    const base = String(trade.symbol || "").toUpperCase();

    const closedRec = {
      symbolFull: `${base}USDT`,
      side: isShort ? "short" : "long",
      pnl_bps: Math.round(pnl_bps),
      ts_entry_ms: Date.parse(trade.updated_at || trade.created_at || "") || Date.now(),
      ts_exit_ms: Date.now(),
      price_entry: entry, price_exit: exitPrice, qty,
      reconciliation: "exchange_trade_history",
      exit_reason: normalizeExitReason(exitReason),
      exit_outcome: net >= 0 ? "win" : "loss",
      p_pred: Number(extra?.idea_fields?.p_lcb ?? extra?.idea_fields?.p_win ?? NaN),
      p_raw: Number(extra?.idea_fields?.p_raw ?? NaN),
      calib_key: extra?.idea_fields?.calib_key || null,
      regime: extra?.idea_fields?.regime || null,
      predicted_snapshot: extra?.predicted_snapshot || null,
      trade_details: {
        client_order_id: cid,
        maker_taker_entry: makerTakerEntry, maker_taker_exit: makerTakerExit,
        commission_bps: commissionBps,
        commission_quote_usdt: commissionQuoteUSDT,
        commission_asset_entry: commissionAssetEntry,
        commission_asset_exit: commissionAssetExit,
        fingerprint_entry: fingerprintEntry,
        fingerprint_exit: fingerprintExit
      },
      realized: {
        tp_hit: normalizeExitReason(exitReason) === 'tp',
        sl_hit: normalizeExitReason(exitReason) === 'sl',
        ttl_exit: normalizeExitReason(exitReason) === 'ttl'
      },
      learned: false
    };

    // Optional: realized MFE/MAE (env.REALIZED_MFE_MAE=1)
    if (String(env?.REALIZED_MFE_MAE || "0") === "1") {
      try {
        const startMs = closedRec.ts_entry_ms - 60 * 1000;
        const endMs = closedRec.ts_exit_ms + 60 * 1000;
        const k = await fetchKlines(base, "1m", 120); // Section 2 helper
        let mfe = 0, mae = 0;
        const entryPx = entry;
        for (const row of (k || [])) {
          const ts = Number(row[0] || 0);
          if (!(ts >= startMs && ts <= endMs)) continue;
          const hi = Number(row[2] || 0), lo = Number(row[3] || 0);
          if (isShort) {
            // favorable: price down (entry/lo - 1); adverse: (entry/hi - 1)
            const fav = (entryPx / Math.max(1e-12, lo) - 1);
            const adv = (entryPx / Math.max(1e-12, hi) - 1);
            mfe = Math.max(mfe, fav * 10000);
            mae = Math.min(mae, adv * 10000);
          } else {
            const fav = (Math.max(1e-12, hi) / entryPx - 1);
            const adv = (Math.max(1e-12, lo) / entryPx - 1);
            mfe = Math.max(mfe, fav * 10000);
            mae = Math.min(mae, adv * 10000);
          }
        }
        closedRec.realized.max_favorable_excursion_bps = Math.round(mfe);
        closedRec.realized.max_adverse_excursion_bps = Math.round(mae);
      } catch {}
    }

    await gistPatchState(env, (state) => {
      state.closed = Array.isArray(state.closed) ? state.closed : [];
      state.closed.push(closedRec);

      state.sym_stats_real = state.sym_stats_real || {};
      const s = state.sym_stats_real[base] || { n: 0, wins: 0, pnl_sum: 0 };
      s.n += 1; if (net > 0) s.wins += 1; s.pnl_sum += Math.round(pnl_bps);
      state.sym_stats_real[base] = s;

      state.equity = Array.isArray(state.equity) ? state.equity : [];
      state.equity.push({ pnl_bps: Math.round(pnl_bps), ts_ms: Date.now(), recon: "api" });

      if (cid) {
        const idx = gistFindPendingIdxByCID(state, cid);
        if (idx >= 0) state.pending.splice(idx, 1);
      }
      state.lastReconcileTs = Date.now();
      return state;
    });
  } catch (e) {
    console.error("gistReportClosed warn:", e?.message || e);
  }
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

function useIdeaNotional(env) {
  return String(env?.USE_IDEA_NOTIONAL || '1') === '1'; // keep legacy behavior unless you set it to "0"
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
  const buttons = [
    [{ text: "Refresh 🔄", callback_data: "action_refresh_balance" },
    { text: "Continue ▶️", callback_data: "continue_dashboard" }]
  ];
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
  const log = new Logger(env, { fn: 'createPendingTrade', userId, symbol: idea?.symbol });
  log.info('🔄 Creating pending trade');
  const timer = log.time('createPendingTrade');

  const session = await getSession(env, userId);
  const symbol = String(idea.symbol || '').toUpperCase();

  {
    log.debug('Step 1: Checking duplicate symbols');
    const allowDup = allowDuplicateSymbols(env);
    const capPerSym = maxPosPerSymbol(env);

    if (!cycleOnlyDupGuard(env)) {
      const rowCnt = await env.DB
        .prepare("SELECT COUNT(*) AS c FROM trades WHERE user_id = ? AND symbol = ? AND status IN ('open','pending')")
        .bind(userId, symbol).first();
      const symCount = Number(rowCnt?.c || 0);

      if (!allowDup && symCount > 0) {
        log.warn('🚫 Duplicate symbol rejected', { symCount, capPerSym: 1 });
        return { id: null, meta: { symbol, skipReason: "duplicate_symbol", symCount, capPerSym: 1 } };
      }
      if (allowDup && symCount >= capPerSym) {
        log.warn('🚫 Duplicate symbol cap reached', { symCount, capPerSym });
        return { id: null, meta: { symbol, skipReason: "duplicate_symbol_cap", symCount, capPerSym } };
      }
    }
    log.info('✅ Duplicate check passed');
  }

  log.debug('Step 2: Checking symbol cooldown');
  const cdMs = await getSymbolCooldownRemainingMs(env, userId, symbol);
  if (cdMs > 0 && !noRejects(env)) {
    log.warn('🚫 Symbol on cooldown', { remain_ms: cdMs });
    return { id: null, meta: { symbol, skipReason: "cooldown_active", remain_ms: cdMs } };
  }
  log.info('✅ Cooldown check passed');

  log.debug('Step 3: Fetching current price');
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
    if (noRejects(env) && session.exchange_name === 'crypto_parrot') currentPrice = 1;
    if (!isFinite(currentPrice) || currentPrice <= 0) {
      log.error('❌ Could not fetch valid market price');
      return { id: null, meta: { symbol, skipReason: "market_data" } };
    }
  }
  log.info('✅ Price fetched', { currentPrice });

  const dir = String(idea.side || 'long').toLowerCase();
  const isShort = (dir === 'short' || dir === 'sell');

  log.debug('Step 4: Checking budgets and risk');
  const TC = await getTotalCapital(env, userId);
  const ACP = ACP_FRAC * TC;
  if (ACP < 6 && !noRejects(env)) {
    log.warn('🚫 Bank closed (ACP too low)', { TC, ACP });
    return { id: null, meta: { symbol, skipReason: "bank_closed", TC, ACP } };
  }

  const perTradeNotionalCapFrac = Number(env?.PER_TRADE_NOTIONAL_CAP_FRAC ?? 0.10);
  const dailyNotionalCapFrac    = Number(env?.DAILY_OPEN_NOTIONAL_CAP_FRAC ?? 0.30);
  const perTradeNotionalCap     = perTradeNotionalCapFrac * TC;
  const dailyNotionalCap        = dailyNotionalCapFrac * TC;
  const openNotional            = await getOpenNotional(env, userId);
  let dailyNotionalLeft         = Math.max(0, dailyNotionalCap - openNotional);
  if (dailyNotionalLeft <= 0 && !noRejects(env)) {
    log.warn('🚫 Daily notional exhausted', { dailyNotionalLeft, dailyNotionalCap, openNotional, TC });
    return { id: null, meta: { symbol, skipReason: "daily_notional_exhausted", dailyNotionalLeft, dailyNotionalCap, openNotional, TC } };
  }

  const M = PER_TRADE_CAP_FRAC * TC;
  const D = DAILY_OPEN_RISK_CAP_FRAC * TC;
  const openRisk = await getOpenPortfolioRisk(env, userId);
  let D_left = Math.max(0, D - openRisk);
  const B = Math.min(M, D_left);
  if (B <= 0 && !noRejects(env)) {
    log.warn('🚫 Daily risk budget exhausted', { D_left, D, openRisk, TC });
    return { id: null, meta: { symbol, skipReason: "daily_risk_budget_exhausted", D_left, D, openRisk, TC } };
  }
  const B_eff = B > 0 ? B : (noRejects(env) ? Math.max(1, 0.0001 * TC) : 0);
  log.info('✅ Budgets and risk OK');

  log.debug('Step 5: Starting sizing logic (EV, Kelly)');
  const useIdeaP = String(env.USE_IDEA_P || '1') === '1';
  const useIdeaExits = String(env.USE_IDEA_EXITS || '1') === '1';
  const minEvBps = Number(env.EV_BPS_MIN ?? 1);

  const pIdeaRaw = Number(idea?.p_lcb ?? idea?.p_win ?? NaN);
  let SQS, p;
  if (useIdeaP && isFinite(pIdeaRaw)) { p = clamp01(pIdeaRaw); SQS = p; }
  else { SQS = SQSfromScore(idea.score ?? 0); p = pLCBFromSQS(SQS); }

  const tpBps = Number(idea?.tp_bps ?? NaN);
  const slBps = Number(idea?.sl_bps ?? NaN);
  const hasIdeaExits = useIdeaExits && isFinite(tpBps) && tpBps > 0 && isFinite(slBps) && slBps > 0;

  let sL = hasIdeaExits ? clamp(bpsToFrac(slBps), MIN_STOP_PCT, 0.20) : Math.max(await calculateStopPercent(symbol), MIN_STOP_PCT);
  const demoEasy = (session.exchange_name === 'crypto_parrot' && String(env.DEMO_EASY_MODE || '0') === '1');
  if (demoEasy) sL = Math.max(sL, 0.01);

  let feeRate = (protocol?.fee_rate ?? 0.001);
  if (demoEasy) feeRate = Math.min(feeRate, 0.0002);

  const hse = await computeHSEAndCosts(symbol, sL, feeRate);
  const { pH } = hse;
  let cost_R = hse.cost_R;

  const costBpsIdea = Number(idea?.cost_bps ?? NaN);
  if (hasIdeaExits && isFinite(costBpsIdea) && slBps > 0) {
    const costR_fromIdea = Math.max(0, costBpsIdea) / slBps;
    cost_R = Math.max(cost_R, costR_fromIdea);
  }

  let RRR_used = STRICT_RRR;
  if (hasIdeaExits) {
    if (slBps > 0 && isFinite(tpBps / slBps)) {
      RRR_used = clamp(tpBps / slBps, 0.8, 3.0);
    } else {
      log.warn('⚠️ Invalid RRR from idea exits, using default', { tpBps, slBps });
    }
  }

  const expLCBBps = Number(idea?.exp_lcb_bps ?? NaN);
  let EV_R;
  if (isFinite(expLCBBps) && hasIdeaExits) {
    EV_R = expLCBBps / Math.max(slBps, 1);
    if ((expLCBBps <= Math.max(0, minEvBps - 1e-9)) && !noRejects(env) && !demoEasy) {
      log.warn('🚫 EV gate failed (from idea)', { exp_lcb_bps: expLCBBps, minEvBps });
      return { id: null, meta: { symbol, skipReason: "ev_gate", exp_lcb_bps: expLCBBps, p, tp_bps: tpBps, sl_bps: slBps, cost_bps: costBpsIdea } };
    }
  } else {
    EV_R = p * RRR_used - (1 - p) - cost_R;
    if (!(EV_R > 0) && !noRejects(env) && !demoEasy) {
      log.warn('🚫 EV gate failed (calculated)', { EV_R, p, RRR_used, cost_R });
      return { id: null, meta: { symbol, skipReason: "ev_gate", EV_R, p, RRR: RRR_used, cost_R, pH, sL, currentPrice } };
    }
  }

  let sqsMinDynamic = await computeUserSqsGate(env, userId);
  if (demoEasy) sqsMinDynamic = Math.min(sqsMinDynamic, 0.20);
  if ((SQS < sqsMinDynamic) && !noRejects(env) && !demoEasy) {
    log.warn('🚫 SQS gate failed', { SQS, sqsMinDynamic });
    return { id: null, meta: { symbol, skipReason: "low_sqs", SQS, sL, currentPrice, sqsMin: sqsMinDynamic } };
  }

  const hseMinG = getHSEMinG(env);
  if ((pH >= 0.85) && !demoEasy && hseMinG === 0 && !noRejects(env)) {
    log.warn('🚫 HSE gate failed', { pH });
    return { id: null, meta: { symbol, skipReason: "hse_gate", pH, sL, currentPrice } };
  }

  const { f_k, Var_R } = kellyFraction(EV_R, p, RRR_used);
  const w_sqs = SQS * SQS;
  const f_raw = clamp(LAMBDA_BLEND * w_sqs + (1 - LAMBDA_BLEND) * f_k, 0, 1);

  const dd = await getDrawdown(env, userId, TC);
  const k_dd = ddScaler(dd);
  const rho_max = await maxCorrelationWithOpen(env, userId, symbol);
  const c_corr = clamp(1 - rho_max, 0, 1);

  let g;
  if (pH < 0.60) g = 1.0;
  else if (pH < 0.75) g = 0.7;
  else if (pH < 0.85) g = 0.5;
  else g = hseMinG;

  const Desired_Risk_USD = B_eff * k_dd * f_raw * c_corr * g;

  const fixedNotional = getFixedTradeNotional(env, TC);
  const planNotionalRaw = Number(idea?.notional_usd);
  const planNotional = (useIdeaNotional(env) && isFinite(planNotionalRaw) && planNotionalRaw > 0) ? planNotionalRaw : null;

  let notionalDesired =
    (fixedNotional != null)
    ? fixedNotional // your FIXED_TRADE_NOTIONAL_* wins
    : (planNotional != null ? planNotional // otherwise honor idea.notional_usd
    : (Desired_Risk_USD / sL)); // else risk-based

  const capPerTrade  = fixedStrict(env) ? Number.POSITIVE_INFINITY : perTradeNotionalCap;
  const capDailyLeft = fixedStrict(env) ? Number.POSITIVE_INFINITY : dailyNotionalLeft;

  let notionalFinal = Math.min(notionalDesired, capPerTrade, capDailyLeft);

  if (notionalFinal <= 0 && noRejects(env)) {
    const MIN_QUOTE_FLOOR_USD = 5;
    notionalFinal = MIN_QUOTE_FLOOR_USD;
  }
  if (notionalFinal <= 0 && !noRejects(env)) {
    log.warn('🚫 Final notional is zero after caps', { notionalDesired, capPerTrade, capDailyLeft, sL });
    return { id: null, meta: { symbol, skipReason: "bank_notional_zero", perTradeNotionalCap, dailyNotionalLeft, openNotional, sL, currentPrice } };
  }

  let Final_Risk_USD = notionalFinal * sL;

  const minRiskFrac = Number(env?.MIN_RISK_FRAC_OF_ACP ?? 0.0025);
  const minRisk = minRiskFrac * (ACP_FRAC * TC);
  if (Final_Risk_USD < minRisk && !noRejects(env)) {
    log.warn('🚫 Final risk too small', { Final_Risk_USD, minRisk });
    return {
      id: null,
      meta: { symbol, skipReason: "too_small", Final_Risk_USD, minRisk, minRiskFrac, ACP: ACP_FRAC * TC, TC, sL, currentPrice, notionalFinal, perTradeNotionalCap, dailyNotionalLeft }
    };
  }
  log.info('✅ Sizing calculation complete', { notionalFinal, Final_Risk_USD });

  log.debug('Step 6: Checking available funds');
  const requiredFunds = notionalFinal * (1 + 2 * feeRate);
  let fundsOk = requiredFunds <= TC;
  if (!fundsOk && noRejects(env)) {
    const maxNotional = TC / (1 + 2 * feeRate);
    notionalFinal = Math.max(0, Math.min(notionalFinal, maxNotional));
    Final_Risk_USD = notionalFinal * sL;
    fundsOk = notionalFinal > 0;
    if (!fundsOk && session.exchange_name === 'crypto_parrot') {
      notionalFinal = 5;
      Final_Risk_USD = notionalFinal * sL;
      fundsOk = true;
    }
  }
  if (!fundsOk && !noRejects(env)) {
    log.error('❌ Insufficient funds', { available: TC, required: requiredFunds, notionalFinal });
    return { id: null, meta: { symbol, skipReason: "insufficient_funds", available: TC, required: requiredFunds, sL, currentPrice, notionalFinal } };
  }
  log.info('✅ Funds OK');

  let stopPrice, tpPrice;
  if (hasIdeaExits) {
    const slf = bpsToFrac(slBps), tpf = bpsToFrac(tpBps);
    stopPrice = isShort ? currentPrice * (1 + slf) : currentPrice * (1 - slf);
    tpPrice   = isShort ? currentPrice * (1 - tpf) : currentPrice * (1 + tpf);
  } else {
    stopPrice = isShort ? currentPrice * (1 + sL) : currentPrice * (1 - sL);
    tpPrice   = isShort ? currentPrice * (1 - sL * STRICT_RRR) : currentPrice * (1 + sL * STRICT_RRR);
  }

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
      p_raw: isFinite(Number(idea.p_raw)) ? Number(idea.p_raw) : undefined,
      exp_lcb_bps: isFinite(expLCBBps) ? expLCBBps : undefined,
      tp_bps: isFinite(tpBps) ? tpBps : undefined,
      sl_bps: isFinite(slBps) ? slBps : undefined,
      cost_bps: isFinite(costBpsIdea) ? costBpsIdea : undefined,
      ttl_sec: idea.ttl_sec,
      calib_key: idea.calib_key,
      regime: idea.regime
    },
    entry: { policy: String(idea.entry_policy || ''), limit: isFinite(idea.entry_limit) ? Number(idea.entry_limit) : null, book: idea.entry_book || null },
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
    sqs_gate_used: sqsMinDynamic,

    // Linkage to Pusher plan (if provided)
    client_order_id: String(idea?.client_order_id || ""),
    idea_id: String(idea?.idea_id || ""),
    predicted_snapshot: idea?.predicted || null
  };

  const nowMs = Date.now();
  const ttlTs = ttlDeadlineMsFromIdea(idea, nowMs, env);
  if (ttlTs) {
    extra_json.ttl = { ttl_ts_ms: ttlTs, hold_sec: Math.round((ttlTs - nowMs) / 1000), source: 'idea' };
  }

  log.debug('Step 7: Inserting pending trade into DB');
  const res = await env.DB.prepare(
    `INSERT INTO trades (user_id, exchange_name, mode, symbol, side, qty, price, status, stop_pct, stop_price, tp_price, risk_usd, strict_rrr, bracket_frozen, extra_json)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     RETURNING id`
  ).bind(
    userId, session.exchange_name, session.bot_mode, symbol, isShort ? 'SELL' : 'BUY',
    0, currentPrice, 'pending', sL, stopPrice, tpPrice, Final_Risk_USD, 'true', 'true', JSON.stringify(extra_json)
  ).first();

  const id = res?.id || null;
  if (id) await bumpTradeCounters(env, userId, 1);
  timer.end({ success: true, tradeId: id });
  log.info('✅ Pending trade created successfully', { tradeId: id });
  return { id, meta: { symbol, fundsOk, required: requiredFunds, available: TC, sL, currentPrice, notionalFinal, Final_Risk_USD } };
}

/* ---------- Helper: cancel open brackets if any (best-effort) ---------- */
async function cancelBracketsIfAny(env, userId, trade, session, ex, extra) {
  try {
    const ob = extra?.open_brackets;
    if (!ob) return;

    const apiKey = await decrypt(session.api_key_encrypted, env);
    const apiSecret = await decrypt(session.api_secret_encrypted, env);

    if (ex.kind === "binanceLike") {
      if (ob.tp?.orderId) { try { await cancelBinanceLikeOrder(ex, apiKey, apiSecret, trade.symbol, { orderId: ob.tp.orderId }); } catch {} }
      if (ob.sl?.orderId) { try { await cancelBinanceLikeOrder(ex, apiKey, apiSecret, trade.symbol, { orderId: ob.sl.orderId }); } catch {} }
    } else if (ex.kind === "binanceFuturesUSDT") {
      if (ob.tp?.orderId) { try { await cancelBinanceFuturesOrder(ex, apiKey, apiSecret, trade.symbol, { orderId: ob.tp.orderId }); } catch {} }
      if (ob.sl?.orderId) { try { await cancelBinanceFuturesOrder(ex, apiKey, apiSecret, trade.symbol, { orderId: ob.sl.orderId }); } catch {} }
    } else if (ex.kind === "bybitFuturesV5") {
      if (ob.tp?.orderId) { try { await cancelBybitFuturesOrder(ex, apiKey, apiSecret, trade.symbol, { orderId: ob.tp.orderId }); } catch {} }
      if (ob.sl?.orderId) { try { await cancelBybitFuturesOrder(ex, apiKey, apiSecret, trade.symbol, { orderId: ob.sl.orderId }); } catch {} }
    }

    // remove from extra and persist
    delete extra.open_brackets;
    await env.DB.prepare("UPDATE trades SET extra_json = ?, updated_at = ? WHERE id = ?")
      .bind(JSON.stringify(extra), nowISO(), trade.id).run();
  } catch (e) {
    console.warn("cancelBracketsIfAny warn:", e?.message || e);
  }
}

/* ---------- Execute trade (FIXED + LOGGED) ---------- */
async function executeTrade(env, userId, tradeId) {
  const log = new Logger(env, { fn: 'executeTrade', userId, tradeId });
  log.info('🚀 Starting trade execution');

  const timer = log.time('executeTrade');

  try {
    // Step 1: Load trade
    log.debug('Step 1: Loading trade from DB');
    const trade = await env.DB.prepare(
      "SELECT * FROM trades WHERE id = ? AND user_id = ?"
    ).bind(tradeId, userId).first();

    if (!trade) {
      log.warn('❌ Trade not found', { tradeId, userId });
      return { status: 'not_found', success: false };
    }

    if (trade.status !== 'pending') {
      log.warn('❌ Trade not pending', { status: trade.status, tradeId });
      return { status: 'not_pending', success: false };
    }

    log.info('✅ Trade loaded', { 
      symbol: trade.symbol, 
      side: trade.side, 
      status: trade.status 
    });

    // Step 2: Cooldown check
    log.debug('Step 2: Checking symbol cooldown');
    const cdMs = await getSymbolCooldownRemainingMs(env, userId, trade.symbol);
    if (cdMs > 0) {
      log.warn('❌ Symbol on cooldown', { 
        symbol: trade.symbol, 
        remaining_ms: cdMs 
      });
      await logEvent(env, userId, 'cooldown_block', { 
        tradeId, 
        symbol: trade.symbol, 
        remain_ms: cdMs 
      });
      return { status: 'cooldown', success: false };
    }
    log.info('✅ Cooldown check passed');

    // Step 3: Parse trade data
    log.debug('Step 3: Parsing trade metadata');
    const extra = JSON.parse(trade.extra_json || '{}');
    const CID = extra?.client_order_id || null;
    const protocol = await getProtocolState(env, userId);
    const isShort = trade.side === 'SELL';

    log.info('✅ Trade metadata parsed', {
      cid: CID,
      isShort,
      quote_size: extra.quote_size,
      r_target: extra.r_target
    });

    // Step 4: Exchange setup
    log.debug('Step 4: Loading exchange configuration');
    const session = await getSession(env, userId);
    const ex = SUPPORTED_EXCHANGES[session.exchange_name];
    const isFutures = (ex.kind === "binanceFuturesUSDT" || ex.kind === "bybitFuturesV5");
    const useBrackets = isFutures ? true : (String(env?.USE_BRACKETS || "0") === "1");

    log.info('✅ Exchange configured', {
      exchange: session.exchange_name,
      kind: ex.kind,
      isFutures,
      useBrackets
    });

    // Step 5: Execution policy
    log.debug('Step 5: Determining execution policy');
    const respectExec = envFlag(env, 'RESPECT_EXEC_POLICY', '1');
    const takerFallback = envFlag(env, 'POST_ONLY_TAKER_FALLBACK', '1');
    const wantPostOnly = respectExec && (
      (extra?.exec?.exec || '').toLowerCase() === 'post_only' || 
      (extra?.entry?.policy || '').toLowerCase() === 'maker_join'
    );

    log.info('✅ Execution policy determined', {
      wantPostOnly,
      respectExec,
      takerFallback,
      execPolicy: extra?.exec?.exec,
      entryPolicy: extra?.entry?.policy
    });

    // Step 6A: MARKET ORDER PATH
    if (!wantPostOnly) {
      log.info('📊 Executing MARKET order');
      const orderTimer = log.time('market_order');

      let orderResult;
      try {
        if (!isShort) {
          log.debug('Placing market BUY order', { 
            symbol: trade.symbol, 
            quoteAmount: extra.quote_size 
          });
          orderResult = await placeMarketBuy(env, userId, trade.symbol, extra.quote_size, { 
            reduceOnly: false, 
            clientOrderId: CID 
          });
        } else {
          log.debug('Placing market SELL order', { 
            symbol: trade.symbol, 
            quoteAmount: extra.quote_size 
          });
          orderResult = await placeMarketSell(env, userId, trade.symbol, extra.quote_size, true, { 
            reduceOnly: false, 
            clientOrderId: CID 
          });
        }

        orderTimer.end({ 
          orderId: orderResult.orderId, 
          avgPrice: orderResult.avgPrice, 
          executedQty: orderResult.executedQty 
        });

        log.info('✅ Market order filled', {
          orderId: orderResult.orderId,
          avgPrice: orderResult.avgPrice,
          executedQty: orderResult.executedQty,
          status: orderResult.status
        });

      } catch (e) {
        orderTimer.end({ error: e.message });
        log.error('❌ Market order failed', { 
          error: e.message, 
          stack: e.stack 
        });
        throw e;
      }

      // Step 6B: Calculate actual prices
      log.debug('Step 6B: Calculating actual stop/target prices');
      const rUsed = Number(extra?.r_target || STRICT_RRR);
      const actualStopPrice = !isShort 
        ? orderResult.avgPrice * (1 - trade.stop_pct) 
        : orderResult.avgPrice * (1 + trade.stop_pct);
      const actualTpPrice = !isShort 
        ? orderResult.avgPrice * (1 + trade.stop_pct * rUsed) 
        : orderResult.avgPrice * (1 - trade.stop_pct * rUsed);

      log.info('✅ Exit prices calculated', {
        entry: orderResult.avgPrice,
        stop: actualStopPrice,
        tp: actualTpPrice,
        r_target: rUsed
      });

      // Step 6C: Metrics snapshot
      log.debug('Step 6C: Capturing entry metrics');
      const tcAtEntry = await getTotalCapital(env, userId);
      const notionalAtEntry = (orderResult.avgPrice || 0) * (orderResult.executedQty || 0);
      const pctOfTCAtEntry = tcAtEntry > 0 ? notionalAtEntry / tcAtEntry : 0;

      log.info('✅ Entry metrics captured', {
        tc_at_entry: tcAtEntry,
        notional_at_entry: notionalAtEntry,
        pct_of_tc: (pctOfTCAtEntry * 100).toFixed(2) + '%'
      });

      // Step 6D: TTL alignment
      log.debug('Step 6D: Aligning TTL to entry time');
      const openedAtMs = Date.now();
      ensureTradeTtl(extra, openedAtMs, env);
      extra.metrics = { 
        ...(extra.metrics || {}), 
        tc_at_entry: tcAtEntry, 
        notional_at_entry: notionalAtEntry, 
        pct_of_tc_at_entry: pctOfTCAtEntry 
      };

      log.info('✅ TTL aligned', {
        ttl_ts_ms: extra?.ttl?.ttl_ts_ms,
        ttl_remaining_min: extra?.ttl?.ttl_ts_ms 
          ? ((extra.ttl.ttl_ts_ms - Date.now()) / 60000).toFixed(1)
          : 'N/A'
      });

      // Step 6E: FIXED — Place brackets BEFORE marking trade open
      let bracketsPlaced = false;
      if (useBrackets) {
        log.info('📌 Placing bracket orders (TP/SL)');
        const bracketTimer = log.time('bracket_placement');

        try {
          let br = null;
          const isLong = !isShort;

          if (ex.kind === "binanceLike") {
            log.debug('Placing Binance spot brackets');
            const apiKey = await decrypt(session.api_key_encrypted, env);
            const apiSecret = await decrypt(session.api_secret_encrypted, env);
            br = await placeBinanceLikeExitOrders(
              ex, apiKey, apiSecret, trade.symbol, isLong, 
              orderResult.executedQty, actualTpPrice, actualStopPrice, CID || undefined
            );
          } else if (ex.kind === "binanceFuturesUSDT") {
            log.debug('Placing Binance Futures brackets');
            const apiKey = await decrypt(session.api_key_encrypted, env);
            const apiSecret = await decrypt(session.api_secret_encrypted, env);
            br = await placeBinanceFuturesBracketOrders(
              ex, apiKey, apiSecret, trade.symbol, isLong, 
              orderResult.executedQty, actualTpPrice, actualStopPrice, CID || undefined
            );
          } else if (ex.kind === "bybitFuturesV5") {
            log.debug('Placing Bybit Futures brackets');
            const apiKey = await decrypt(session.api_key_encrypted, env);
            const apiSecret = await decrypt(session.api_secret_encrypted, env);
            br = await placeBybitFuturesBracketOrders(
              ex, apiKey, apiSecret, trade.symbol, isLong, 
              orderResult.executedQty, actualTpPrice, actualStopPrice, CID || undefined
            );
          }

          bracketTimer.end({
            tp_orderId: br?.tp?.orderId,
            sl_orderId: br?.sl?.orderId
          });

          // FIXED: Fail trade if brackets required but not placed
          if (!br?.tp && !br?.sl) {
            log.critical('❌ CRITICAL: Brackets required but placement failed', {
              symbol: trade.symbol,
              exchange: ex.kind
            });
            throw new Error('Required bracket orders failed to place');
          }

          extra.open_brackets = { 
            tp: br.tp ? { orderId: br.tp.orderId, status: br.tp.status } : null, 
            sl: br.sl ? { orderId: br.sl.orderId, status: br.sl.status } : null 
          };
          bracketsPlaced = true;

          log.info('✅ Brackets placed successfully', {
            tp_orderId: br?.tp?.orderId,
            sl_orderId: br?.sl?.orderId
          });

        } catch (e) {
          bracketTimer.end({ error: e.message });
          log.error('❌ Bracket placement failed', { 
            error: e.message, 
            stack: e.stack 
          });

          // FIXED: Rollback entry order if brackets fail
          log.warn('⚠️ Rolling back entry order due to bracket failure');
          try {
            if (!isShort) {
              await placeMarketSell(env, userId, trade.symbol, orderResult.executedQty, false, { reduceOnly: true });
            } else {
              const currentPrice = await getCurrentPrice(trade.symbol);
              await placeMarketBuy(env, userId, trade.symbol, orderResult.executedQty * currentPrice, { reduceOnly: true });
            }
            log.info('✅ Entry order reversed');
          } catch (rollbackErr) {
            log.critical('❌ ROLLBACK FAILED — MANUAL INTERVENTION REQUIRED', {
              symbol: trade.symbol,
              qty: orderResult.executedQty,
              error: rollbackErr.message
            });
          }

          throw new Error(`Bracket placement failed: ${e.message}`);
        }
      }

      // Step 6F: FIXED — Only mark as open AFTER brackets succeed
      log.debug('Step 6F: Marking trade as open in DB');
      const dbTimer = log.time('db_update_open');

      await env.DB.prepare(
        `UPDATE trades SET status = 'open', qty = ?, entry_price = ?, stop_price = ?, tp_price = ?, extra_json = ?, updated_at = ? WHERE id = ?`
      ).bind(
        orderResult.executedQty, 
        orderResult.avgPrice, 
        actualStopPrice, 
        actualTpPrice, 
        JSON.stringify(extra), 
        nowISO(), 
        tradeId
      ).run();

      dbTimer.end();
      log.info('✅ Trade marked as OPEN in database');

      // Step 6G: Gist update
      log.debug('Step 6G: Updating Gist state');
      const gistTimer = log.time('gist_mark_open');
      await gistMarkOpen(env, trade, orderResult.avgPrice, orderResult.executedQty);
      gistTimer.end();
      log.info('✅ Gist state updated');

      // Step 6H: Event logging
      log.debug('Step 6H: Logging trade_open event');
      await logEvent(env, userId, trade.mode === 'manual' ? 'trade_open_manual' : 'trade_open', {
        symbol: trade.symbol, 
        side: trade.side, 
        qty: orderResult.executedQty, 
        entry: orderResult.avgPrice,
        stop_pct: trade.stop_pct, 
        stop_price: actualStopPrice, 
        tp_price: actualTpPrice, 
        r: rUsed,
        phase: protocol?.phase, 
        fee_rate: protocol?.fee_rate, 
        strict_rrr: true, 
        bracket_frozen: true,
        brackets_placed: bracketsPlaced,
        tc_at_entry: tcAtEntry, 
        notional_at_entry: notionalAtEntry, 
        pct_of_tc_at_entry: pctOfTCAtEntry,
        ttl_ts_ms: extra?.ttl?.ttl_ts_ms
      });

      const totalDuration = timer.end({ 
        success: true, 
        status: 'filled',
        entry_price: orderResult.avgPrice,
        qty: orderResult.executedQty
      });

      log.info('🎉 Trade execution completed successfully', {
        total_duration_ms: totalDuration.toFixed(2),
        entry_price: orderResult.avgPrice,
        qty: orderResult.executedQty,
        brackets_placed: bracketsPlaced
      });

      return { status: 'filled', success: true, orderResult };
    }

    // Step 7: POST-ONLY PATH
    log.info('📊 Executing POST-ONLY order');
    const postOnlyTimer = log.time('post_only_execution');

    try {
        // 7A: Determine maker price
        log.debug('Step 7A: Determining maker price');
        let makerPx = Number(extra?.entry?.limit);
        if (!isFinite(makerPx) || makerPx <= 0) {
            try {
                const ob = await getOrderBookSnapshot(trade.symbol);
                const { bestBid, bestAsk } = computeBestBidAsk(ob || {});
                makerPx = !isShort ? bestBid : bestAsk;
                log.debug('Got maker price from order book', { bestBid, bestAsk, makerPx });
            } catch (e) {
                log.warn('Could not get price from order book, falling back', { error: e.message });
            }
        }
        if (!isFinite(makerPx) || makerPx <= 0) {
            makerPx = await getCurrentPrice(trade.symbol);
            log.debug('Got maker price from current price API', { makerPx });
        }
        log.info('✅ Maker price determined', { makerPx });

        const quoteAmt = Number(extra.quote_size || 0);
        let result;

        // 7B: Place limit order
        log.debug('Step 7B: Placing exchange-specific limit order', { exchange: ex.kind, quoteAmt });

        if (ex.kind === 'demoParrot') {
            log.debug('Placing demo post-only order');
            result = await placeDemoOrder(trade.symbol, isShort ? "SELL" : "BUY", quoteAmt, true, CID);
            log.info('✅ Demo order placed/filled');
        } else if (ex.kind === 'binanceLike') {
            log.debug('Placing Binance Spot post-only limit order');
            try {
                result = await placeBinanceLikeLimitMaker(ex, await decrypt(session.api_key_encrypted, env), await decrypt(session.api_secret_encrypted, env),
                    trade.symbol, isShort ? 'SELL' : 'BUY', makerPx, quoteAmt, true, CID);
                log.info('✅ Binance Spot post-only order accepted by exchange');
            } catch (e) {
                log.warn('❌ Binance Spot post-only order rejected', { error: e.message });
                if (takerFallback) {
                    log.warn('⚠️ Falling back to TAKER order');
                    if (!isShort) result = await placeMarketBuy(env, userId, trade.symbol, quoteAmt, { reduceOnly: false, clientOrderId: CID });
                    else result = await placeMarketSell(env, userId, trade.symbol, quoteAmt, true, { reduceOnly: false, clientOrderId: CID });
                    log.info('✅ Taker fallback order filled');
                } else {
                    log.error('❌ Post-only failed without fallback');
                    await env.DB.prepare("UPDATE trades SET status='rejected', updated_at=? WHERE id=?").bind(nowISO(), tradeId).run();
                    await logEvent(env, userId, 'order_rejected', { policy: 'post_only', symbol: trade.symbol, reason: String(e?.message||e) });
                    return { status: 'rejected', success: false };
                }
            }
        } else if (ex.kind === 'binanceFuturesUSDT') {
            log.debug('Placing Binance Futures post-only limit order');
            const baseQty = (quoteAmt / Math.max(1e-12, makerPx));
            try {
                const apiKey = await decrypt(session.api_key_encrypted, env);
                const apiSecret = await decrypt(session.api_secret_encrypted, env);
                await ensureFuturesIsolated1x(ex, apiKey, apiSecret, trade.symbol);
                result = await placeBinanceFuturesLimitPostOnly(ex, apiKey, apiSecret,
                    trade.symbol, isShort ? 'SELL' : 'BUY', baseQty, makerPx, false, CID);
                log.info('✅ Binance Futures post-only order accepted');
            } catch (e) {
                log.warn('❌ Binance Futures post-only order rejected', { error: e.message });
                if (takerFallback) {
                    log.warn('⚠️ Falling back to TAKER order');
                    result = await placeBinanceFuturesOrder(ex, await decrypt(session.api_key_encrypted, env), await decrypt(session.api_secret_encrypted, env),
                        trade.symbol, isShort ? 'SELL' : 'BUY', baseQty, false, CID);
                    log.info('✅ Taker fallback order filled');
                } else {
                    log.error('❌ Post-only failed without fallback');
                    await env.DB.prepare("UPDATE trades SET status='rejected', updated_at=? WHERE id=?").bind(nowISO(), tradeId).run();
                    await logEvent(env, userId, 'order_rejected', { policy: 'post_only', symbol: trade.symbol, reason: String(e?.message||e) });
                    return { status: 'rejected', success: false };
                }
            }
        } else if (ex.kind === 'bybitFuturesV5') {
            log.debug('Placing Bybit Futures post-only limit order');
            const baseQty = (quoteAmt / Math.max(1e-12, makerPx));
            try {
                const apiKey = await decrypt(session.api_key_encrypted, env);
                const apiSecret = await decrypt(session.api_secret_encrypted, env);
                await ensureFuturesIsolated1x(ex, apiKey, apiSecret, trade.symbol);
                result = await placeBybitFuturesLimitPostOnly(
                    ex, apiKey, apiSecret, trade.symbol,
                    isShort ? 'SELL' : 'BUY', baseQty, makerPx, false, CID
                );
                log.info('✅ Bybit Futures post-only order accepted');
            } catch (e) {
                log.warn('❌ Bybit Futures post-only order rejected', { error: e.message });
                if (takerFallback) {
                    log.warn('⚠️ Falling back to TAKER order');
                    const apiKey = await decrypt(session.api_key_encrypted, env);
                    const apiSecret = await decrypt(session.api_secret_encrypted, env);
                    result = await placeBybitFuturesOrder(
                        ex, apiKey, apiSecret, trade.symbol,
                        isShort ? 'SELL' : 'BUY', baseQty, false, CID
                    );
                    log.info('✅ Taker fallback order filled');
                } else {
                    log.error('❌ Post-only failed without fallback');
                    await env.DB.prepare("UPDATE trades SET status='rejected', updated_at=? WHERE id=?").bind(nowISO(), tradeId).run();
                    await logEvent(env, userId, 'order_rejected', { policy: 'post_only', symbol: trade.symbol, reason: String(e?.message||e) });
                    return { status: 'rejected', success: false };
                }
            }
        } else {
            log.error('❌ Post-only not supported for this exchange, trying fallback', { exchange: ex.kind });
            if (takerFallback) {
                log.warn('⚠️ Falling back to TAKER order');
                if (!isShort) result = await placeMarketBuy(env, userId, trade.symbol, quoteAmt, { reduceOnly: false, clientOrderId: CID });
                else result = await placeMarketSell(env, userId, trade.symbol, quoteAmt, true, { reduceOnly: false, clientOrderId: CID });
                log.info('✅ Taker fallback order filled');
            } else {
                log.error('❌ Post-only failed without fallback (unsupported exchange)');
                await env.DB.prepare("UPDATE trades SET status='rejected', updated_at=? WHERE id=?").bind(nowISO(), tradeId).run();
                await logEvent(env, userId, 'order_rejected', { policy: 'post_only', symbol: trade.symbol, reason: 'unsupported_exchange' });
                return { status: 'rejected', success: false };
            }
        }

        // 7C: Handle result
        log.debug('Step 7C: Handling order placement result');

        // Case 1: Order filled immediately
        if (result && Number(result.executedQty || 0) > 0) {
            log.info('✅ Post-only order was filled immediately (or fell back to taker)');
            const orderResult = result;

            log.debug('Calculating actual stop/target prices post-fill');
            const rUsed = Number(extra?.r_target || STRICT_RRR);
            const entryPx = Number(orderResult.avgPrice || makerPx);
            const qty = Number(orderResult.executedQty || 0);
            const actualStopPrice = !isShort ? entryPx * (1 - trade.stop_pct) : entryPx * (1 + trade.stop_pct);
            const actualTpPrice = !isShort ? entryPx * (1 + trade.stop_pct * rUsed) : entryPx * (1 - trade.stop_pct * rUsed);
            log.info('✅ Exit prices calculated', { entry: entryPx, stop: actualStopPrice, tp: actualTpPrice });

            log.debug('Capturing entry metrics post-fill');
            const tcAtEntry = await getTotalCapital(env, userId);
            const notionalAtEntry = entryPx * qty;
            const pctOfTCAtEntry = tcAtEntry > 0 ? notionalAtEntry / tcAtEntry : 0;
            log.info('✅ Entry metrics captured', { tc_at_entry: tcAtEntry, notional_at_entry: notionalAtEntry, pct_of_tc: (pctOfTCAtEntry * 100).toFixed(2) + '%' });

            log.debug('Aligning TTL to entry time post-fill');
            const openedAtMs = Date.now();
            ensureTradeTtl(extra, openedAtMs, env);
            extra.metrics = { ...(extra.metrics || {}), tc_at_entry: tcAtEntry, notional_at_entry: notionalAtEntry, pct_of_tc_at_entry: pctOfTCAtEntry };
            log.info('✅ TTL aligned', { ttl_ts_ms: extra?.ttl?.ttl_ts_ms });

            let bracketsPlaced = false;
            if (useBrackets) {
              log.info('📌 Placing bracket orders (TP/SL)');
              const bracketTimer = log.time('bracket_placement_post_only');
              try {
                  let br = null;
                  const isLong = !isShort;
                  if (ex.kind === "binanceLike") {
                      log.debug('Placing Binance spot brackets');
                      const apiKey = await decrypt(session.api_key_encrypted, env);
                      const apiSecret = await decrypt(session.api_secret_encrypted, env);
                      br = await placeBinanceLikeExitOrders(ex, apiKey, apiSecret, trade.symbol, isLong, qty, actualTpPrice, actualStopPrice, CID || undefined);
                  } else if (ex.kind === "binanceFuturesUSDT") {
                      log.debug('Placing Binance Futures brackets');
                      const apiKey = await decrypt(session.api_key_encrypted, env);
                      const apiSecret = await decrypt(session.api_secret_encrypted, env);
                      br = await placeBinanceFuturesBracketOrders(ex, apiKey, apiSecret, trade.symbol, isLong, qty, actualTpPrice, actualStopPrice, CID || undefined);
                  } else if (ex.kind === "bybitFuturesV5") {
                      log.debug('Placing Bybit Futures brackets');
                      const apiKey = await decrypt(session.api_key_encrypted, env);
                      const apiSecret = await decrypt(session.api_secret_encrypted, env);
                      br = await placeBybitFuturesBracketOrders(ex, apiKey, apiSecret, trade.symbol, isLong, qty, actualTpPrice, actualStopPrice, CID || undefined);
                  }
                  bracketTimer.end({ tp_orderId: br?.tp?.orderId, sl_orderId: br?.sl?.orderId });

                  if (!br?.tp && !br?.sl) {
                      log.critical('❌ CRITICAL: Brackets required but placement failed', { symbol: trade.symbol, exchange: ex.kind });
                      throw new Error('Required bracket orders failed to place');
                  }
                  extra.open_brackets = { tp: br.tp ? { orderId: br.tp.orderId, status: br.tp.status } : null, sl: br.sl ? { orderId: br.sl.orderId, status: br.sl.status } : null };
                  bracketsPlaced = true;
                  log.info('✅ Brackets placed successfully', { tp_orderId: br?.tp?.orderId, sl_orderId: br?.sl?.orderId });
              } catch (e) {
                  bracketTimer.end({ error: e.message });
                  log.error('❌ Bracket placement failed', { error: e.message, stack: e.stack });
                  log.warn('⚠️ Rolling back entry order due to bracket failure');
                  try {
                      if (!isShort) {
                          await placeMarketSell(env, userId, trade.symbol, qty, false, { reduceOnly: true });
                      } else {
                          const currentPrice = await getCurrentPrice(trade.symbol);
                          await placeMarketBuy(env, userId, trade.symbol, qty * currentPrice, { reduceOnly: true });
                      }
                      log.info('✅ Entry order reversed');
                  } catch (rollbackErr) {
                      log.critical('❌ ROLLBACK FAILED — MANUAL INTERVENTION REQUIRED', { symbol: trade.symbol, qty: qty, error: rollbackErr.message });
                  }
                  throw new Error(`Bracket placement failed: ${e.message}`);
              }
            }

            log.debug('Marking trade as OPEN in database');
            await env.DB.prepare(
                `UPDATE trades SET status='open', qty=?, entry_price=?, stop_price=?, tp_price=?, extra_json=?, updated_at=? WHERE id=?`
            ).bind(qty, entryPx, actualStopPrice, actualTpPrice, JSON.stringify(extra), nowISO(), tradeId).run();
            log.info('✅ Trade marked as OPEN');

            log.debug('Updating Gist state');
            await gistMarkOpen(env, trade, entryPx, qty);
            log.info('✅ Gist state updated');

            await logEvent(env, userId, 'trade_open', {
                symbol: trade.symbol, side: trade.side, qty, entry: entryPx, r: rUsed,
                stop_pct: trade.stop_pct, stop_price: actualStopPrice, tp_price: actualTpPrice,
                ttl_ts_ms: extra?.ttl?.ttl_ts_ms, policy: 'post_only_fill', brackets_placed
            });
            const totalDuration = timer.end({ success: true, status: 'filled', entry_price: entryPx, qty });
            log.info('🎉 Trade execution completed successfully (post-only fill)', { total_duration_ms: totalDuration.toFixed(2) });
            return { status: 'filled', success: true, orderResult };

        // Case 2: Order posted, not filled
        } else {
            log.info('✅ Order posted to book, waiting for fill', { orderId: result?.orderId, price: makerPx });
            extra.open_order = { id: result?.orderId || null, px: makerPx, post_only: true, posted_at: nowISO() };
            await env.DB.prepare("UPDATE trades SET extra_json=?, updated_at=? WHERE id=?")
                .bind(JSON.stringify(extra), nowISO(), tradeId).run();

            await logEvent(env, userId, 'order_posted', { symbol: trade.symbol, id: extra.open_order.id, px: makerPx, post_only: true });

            const totalDuration = timer.end({ success: true, status: 'posted', orderId: result?.orderId });
            log.info('🎉 Trade execution completed (order posted)', { total_duration_ms: totalDuration.toFixed(2) });
            return { status: 'posted', success: true };
        }
    } catch (e) {
        postOnlyTimer.end({ error: e.message });
        log.error('❌ Post-only path failed', { error: e.message, stack: e.stack });
        throw e;
    }

  } catch (e) {
    timer.end({ error: e.message });
    log.error('❌ executeTrade failed', { 
      error: e.message, 
      stack: e.stack,
      tradeId,
      userId
    });
    await logEvent(env, userId, 'error', { 
      where: 'executeTrade', 
      message: e.message,
      tradeId 
    });
    return { status: 'error', success: false, error: e.message };
  }
}

/* ---------- Resolve posted maker orders (working -> open) ---------- */
async function checkWorkingOrders(env, userId) {
  const session = await getSession(env, userId);
  if (!session) return;
  const ex = SUPPORTED_EXCHANGES[session.exchange_name];

  const rows = await env.DB.prepare(
    "SELECT id, symbol, side, stop_pct, extra_json FROM trades WHERE user_id = ? AND status = 'pending'"
  ).bind(userId).all();
  
  const log = new Logger(env, { fn: 'checkWorkingOrders', userId });

  for (const t of (rows.results || [])) {
    const tradeLog = log.scope({ tradeId: t.id, symbol: t.symbol });
    try {
      const extra = JSON.parse(t.extra_json || '{}');
      const oo = extra?.open_order;
      if (!oo?.id) continue;
      
      const apiKey = await decrypt(session.api_key_encrypted, env);
      const apiSecret = await decrypt(session.api_secret_encrypted, env);

      // OPTIMIZATION: Reuse status query
      let cachedStatus = null;

      // TTL cancel for unfilled entry orders (RACE CONDITION FIX)
      const ttlTs = Number(extra?.ttl?.ttl_ts_ms || 0);
      if (ttlTs > 0 && Date.now() >= ttlTs) {
        tradeLog.warn('⏱️ TTL expired for unfilled entry order', { orderId: oo.id });
        try {
          let orderStatus;
          if (ex.kind === 'demoParrot') {
             orderStatus = { status: 'NEW' }; // Assume not filled
          } else if (ex.kind === 'binanceLike') {
            orderStatus = await getBinanceLikeOrderStatus(ex, apiKey, apiSecret, t.symbol, oo.id);
          } else if (ex.kind === 'binanceFuturesUSDT') {
            orderStatus = await getBinanceFuturesOrderStatus(ex, apiKey, apiSecret, t.symbol, oo.id);
          } else if (ex.kind === 'bybitFuturesV5') {
            orderStatus = await getBybitFuturesOrderStatus(ex, apiKey, apiSecret, t.symbol, { orderId: oo.id });
          }

          if ((orderStatus?.status || '').toUpperCase() === 'FILLED' && Number(orderStatus.executedQty || 0) > 0) {
            tradeLog.info('✅ Order filled just before TTL cancellation, processing as a fill.');
            cachedStatus = orderStatus; // Save for reuse
          } else {
            tradeLog.info('Order is not filled, proceeding with cancellation.');
            if (ex.kind !== 'demoParrot') {
              if (ex.kind === 'binanceLike') await cancelBinanceLikeOrder(ex, apiKey, apiSecret, t.symbol, { orderId: oo.id });
              else if (ex.kind === 'binanceFuturesUSDT') await cancelBinanceFuturesOrder(ex, apiKey, apiSecret, t.symbol, { orderId: oo.id });
              else if (ex.kind === 'bybitFuturesV5') await cancelBybitFuturesOrder(ex, apiKey, apiSecret, t.symbol, { orderId: oo.id });
            }
            
            await env.DB.prepare("UPDATE trades SET status='rejected', close_type='TTL_EXPIRED', updated_at=? WHERE id=?")
              .bind(nowISO(), t.id).run();
            if (extra?.client_order_id) {
              await gistPatchState(env, (state) => {
                const idx = gistFindPendingIdxByCID(state, extra.client_order_id);
                if (idx >= 0) state.pending.splice(idx, 1);
                return state;
              });
            }
            await logEvent(env, userId, 'order_ttl_cancel', { symbol: t.symbol, id: t.id, orderId: oo.id });
            continue;
          }
        } catch (e) {
            tradeLog.error('Error during TTL cancellation check', { error: e.message });
            continue;
        }
      }

      // Query order status (or use cached)
      let st = cachedStatus;
      if (!st) {
        if (ex.kind === 'demoParrot') {
          st = { status: 'FILLED', executedQty: (extra.quote_size || 0) / Math.max(1e-12, oo.px), avgPrice: oo.px };
        } else if (ex.kind === 'binanceLike') {
          st = await getBinanceLikeOrderStatus(ex, apiKey, apiSecret, t.symbol, oo.id);
        } else if (ex.kind === 'binanceFuturesUSDT') {
          st = await getBinanceFuturesOrderStatus(ex, apiKey, apiSecret, t.symbol, oo.id);
        } else if (ex.kind === 'bybitFuturesV5') {
          st = await getBybitFuturesOrderStatus(ex, apiKey, apiSecret, t.symbol, { orderId: oo.id });
        } else {
          continue;
        }
      }
      
      // Only act on FILLED
      if ((st.status || '').toUpperCase() !== 'FILLED' || !(Number(st.executedQty) > 0)) {
        continue;
      }
      tradeLog.info('✅ Maker order has filled', { orderId: oo.id, avgPrice: st.avgPrice, qty: st.executedQty });

      const isShort = t.side === 'SELL';
      const rUsed = Number(extra?.r_target || STRICT_RRR);
      const entryPx = Number(st.avgPrice || oo.px);
      const qty = Number(st.executedQty || 0);

      const actualStopPrice = !isShort ? entryPx * (1 - t.stop_pct) : entryPx * (1 + t.stop_pct);
      const actualTpPrice = !isShort ? entryPx * (1 + t.stop_pct * rUsed) : entryPx * (1 - t.stop_pct * rUsed);

      const openedAtMs = Date.now();
      ensureTradeTtl(extra, openedAtMs, env);
      delete extra.open_order;

      // BRACKET FAILURE ROLLBACK FIX
      const useBrackets = (ex.kind === "binanceFuturesUSDT" || ex.kind === "bybitFuturesV5") ? true : (String(env?.USE_BRACKETS || "0") === "1");
      if (useBrackets) {
        tradeLog.info('📌 Placing bracket orders after maker fill');
        try {
          let br = null;
          const isLong = !isShort;
          if (ex.kind === "binanceLike") {
            br = await placeBinanceLikeExitOrders(ex, apiKey, apiSecret, t.symbol, isLong, qty, actualTpPrice, actualStopPrice, extra?.client_order_id || undefined);
          } else if (ex.kind === "binanceFuturesUSDT") {
            br = await placeBinanceFuturesBracketOrders(ex, apiKey, apiSecret, t.symbol, isLong, qty, actualTpPrice, actualStopPrice, extra?.client_order_id || undefined);
          } else if (ex.kind === "bybitFuturesV5") {
            br = await placeBybitFuturesBracketOrders(ex, apiKey, apiSecret, t.symbol, isLong, qty, actualTpPrice, actualStopPrice, extra?.client_order_id || undefined);
          }
          
          if (!br?.tp && !br?.sl) {
            tradeLog.critical('❌ CRITICAL: Brackets required but placement failed');
            throw new Error('Required bracket orders failed to place');
          }
          extra.open_brackets = { tp: br.tp ? { orderId: br.tp.orderId } : null, sl: br.sl ? { orderId: br.sl.orderId } : null };
          tradeLog.info('✅ Brackets placed successfully', { tp: br?.tp?.orderId, sl: br?.sl?.orderId });
        } catch (e) {
          tradeLog.error('❌ Bracket placement failed, reversing fill', { error: e.message });
          try {
            if (!isShort) {
              await placeMarketSell(env, userId, t.symbol, qty, false, { reduceOnly: true });
            } else {
              const currentPrice = await getCurrentPrice(t.symbol);
              await placeMarketBuy(env, userId, t.symbol, qty * currentPrice, { reduceOnly: true });
            }
            tradeLog.info('✅ Fill reversed successfully');
          } catch (rollbackErr) {
            tradeLog.critical('❌ ROLLBACK FAILED — MANUAL INTERVENTION REQUIRED', { symbol: t.symbol, qty: qty, error: rollbackErr.message });
          }
          
          await env.DB.prepare("UPDATE trades SET status='failed', close_type='BRACKET_FAIL', updated_at=? WHERE id=?")
            .bind(nowISO(), t.id).run();
          continue;
        }
      }

      const tcAtEntry = await getTotalCapital(env, userId);
      const notionalAtEntry = entryPx * qty;
      const pctOfTCAtEntry = tcAtEntry > 0 ? notionalAtEntry / tcAtEntry : 0;
      extra.metrics = { ...(extra.metrics || {}), tc_at_entry: tcAtEntry, notional_at_entry: notionalAtEntry, pct_of_tc_at_entry: pctOfTCAtEntry };

      await env.DB.prepare(
        `UPDATE trades SET status='open', qty=?, entry_price=?, stop_price=?, tp_price=?, extra_json=?, updated_at=? WHERE id=?`
      ).bind(qty, entryPx, actualStopPrice, actualTpPrice, JSON.stringify(extra), nowISO(), t.id).run();
      tradeLog.info('✅ Trade successfully marked as OPEN in DB');

      await gistMarkOpen(env, t, entryPx, qty);
      await logEvent(env, userId, 'trade_open', {
        symbol: t.symbol, side: t.side, qty, entry: entryPx, r: rUsed,
        stop_pct: t.stop_pct, stop_price: actualStopPrice, tp_price: actualTpPrice,
        ttl_ts_ms: extra?.ttl?.ttl_ts_ms, policy: 'post_only_fill'
      });
    } catch (e) {
      tradeLog.error('Error processing working order', { error: e.message, stack: e.stack });
    }
  }
}

/* ---------- Helpers for mgmt overlay ---------- */
function unrealizedR(trade, price) {
  const pnl = trade.side==='SELL' ? (trade.entry_price - price)*trade.qty : (price - trade.entry_price)*trade.qty;
  return trade.risk_usd > 0 ? pnl / trade.risk_usd : 0;
}
// SECURITY FIX: Replaced eval-based parser with a safe, simple one
function parseThreshold(expr, costBps) {
  if (expr == null) return null;
  if (typeof expr === 'number') return expr;
  
  const s = String(expr).trim().toUpperCase();
  if (!s) return null;
  
  const COST = Number(costBps || 0);
  
  if (s === 'COST') return COST;
  
  const match = s.match(/^COST\s*([+\-*/])\s*([\d.]+)$/);
  if (match) {
    const op = match[1];
    const num = Number(match[2]);
    if (!Number.isFinite(num)) return null;
    
    switch (op) {
      case '+': return COST + num;
      case '-': return COST - num;
      case '*': return COST * num;
      case '/': return num !== 0 ? COST / num : null;
      default: return null;
    }
  }
  
  const num = Number(s);
  return Number.isFinite(num) ? num : null;
}

/* ---------- Manual close & automated exits (with mgmt overlay) ---------- */
async function closeTradeNow(env, userId, tradeId) {
  const trade = await env.DB.prepare("SELECT * FROM trades WHERE id = ? AND user_id = ?").bind(tradeId, userId).first();
  if (!trade || trade.status !== "open") return { ok: false, msg: "Trade not open." };
  const protocol = await getProtocolState(env, userId);
  const isShort = trade.side === 'SELL';
  const commFromEx = String(env?.COMMISSIONS_FROM_EXCHANGE || "1") === "1";

  try {
    let exitPrice = 0;

    // If brackets were placed, cancel them first to avoid double exec on reduceOnly
    const session = await getSession(env, userId);
    const ex = SUPPORTED_EXCHANGES[session.exchange_name];
    const extra = JSON.parse(trade.extra_json || '{}');
    if (extra?.open_brackets) await cancelBracketsIfAny(env, userId, trade, session, ex, extra);

    if (!isShort) {
      const sellResult = await placeMarketSell(env, userId, trade.symbol, trade.qty, false, { reduceOnly: true, clientOrderId: extra?.client_order_id || null });
      exitPrice = sellResult.avgPrice || (await getCurrentPrice(trade.symbol));
    } else {
      if (session.exchange_name === 'crypto_parrot') {
        const buyResult = await placeDemoOrder(trade.symbol, "BUY", trade.qty, false, extra?.client_order_id || null);
        exitPrice = buyResult.avgPrice;
      } else {
        const requiredQuote = trade.qty * (await getCurrentPrice(trade.symbol));
        const buyResult = await placeMarketBuy(env, userId, trade.symbol, requiredQuote, { reduceOnly: true, clientOrderId: extra?.client_order_id || null });
        exitPrice = buyResult.avgPrice || (await getCurrentPrice(trade.symbol));
      }
    }

    const grossPnl = !isShort ? (exitPrice - trade.entry_price) * trade.qty : (trade.entry_price - exitPrice) * trade.qty;

    // Try to fetch true commissions + maker/taker + fingerprints
    let details = null;
    if (commFromEx) {
      try {
        const session2 = await getSession(env, userId);
        const ex2 = SUPPORTED_EXCHANGES[session2.exchange_name];
        const apiKey = await decrypt(session2.api_key_encrypted, env);
        const apiSecret = await decrypt(session2.api_secret_encrypted, env);

        // fetch fills in recent window
        const startMs = Date.parse(trade.created_at || trade.updated_at || "") - 30 * 60 * 1000;
        const fills = ex2.kind === "binanceLike"
          ? await getBinanceLikeMyTrades(ex2, apiKey, apiSecret, trade.symbol, { startTime: startMs, limit: 1000 })
          : ex2.kind === "binanceFuturesUSDT"
          ? await getBinanceFuturesUserTrades(ex2, apiKey, apiSecret, trade.symbol, { startTime: startMs, limit: 1000 })
          : ex2.kind === "bybitFuturesV5"
          ? await getBybitFuturesUserTrades(ex2, apiKey, apiSecret, trade.symbol, { startTime: startMs, limit: 1000 })
          : [];

        const entrySideIsBuy = !isShort;
        const exitSideIsBuy = isShort;

        let commissionEntryUSDT = 0, entryMakerVotes = 0, entryTakerVotes = 0;
        let commissionExitUSDT = 0, exitMakerVotes = 0, exitTakerVotes = 0;
        let fp_entry = null, fp_exit = null;

        for (const f of (fills || [])) {
          const isBuyer = f.isBuyer === true || String(f.side||"").toUpperCase() === "BUY";
          const qty = Number(f.qty || f.qty_filled || f.baseQty || 0);
          const price = Number(f.price || f.avgPrice || 0);
          if (!(qty > 0 && price > 0)) continue;

          const isEntry = (isBuyer && entrySideIsBuy) || (!isBuyer && !entrySideIsBuy);
          const isExit  = (isBuyer && exitSideIsBuy) || (!isBuyer && !exitSideIsBuy);

          if (isEntry) {
            if (f.isMaker === true) entryMakerVotes++; else entryTakerVotes++;
            const cu = await convertCommissionToUSDT((f.commissionAsset || "").toUpperCase(), Number(f.commission || 0));
            commissionEntryUSDT += cu;
            if (!fp_entry) fp_entry = fillFingerprint(trade.symbol, f);
          } else if (isExit) {
            if (f.isMaker === true) exitMakerVotes++; else exitTakerVotes++;
            const cu = await convertCommissionToUSDT((f.commissionAsset || "").toUpperCase(), Number(f.commission || 0));
            commissionExitUSDT += cu;
            if (!fp_exit) fp_exit = fillFingerprint(trade.symbol, f);
          }
        }

        const commissionQuoteUSDT = commissionEntryUSDT + commissionExitUSDT;
        const commissionBps = (trade.entry_price > 0 && trade.qty > 0) ? (commissionQuoteUSDT / (trade.entry_price * trade.qty)) * 10000 : 0;

        details = {
          fromExchange: true,
          commission_quote_usdt: commissionQuoteUSDT,
          commission_bps: commissionBps,
          maker_taker_entry: entryMakerVotes >= entryTakerVotes ? "M" : "T",
          maker_taker_exit: exitMakerVotes >= exitTakerVotes ? "M" : "T",
          commission_asset_entry: "USDT",
          commission_asset_exit: "USDT",
          fingerprint_entry: fp_entry,
          fingerprint_exit: fp_exit
        };
      } catch (_) {}
    }

    const fees = details?.commission_quote_usdt ?? (trade.entry_price * trade.qty + exitPrice * trade.qty) * (protocol?.fee_rate ?? 0.001);
    const netPnl = grossPnl - fees;
    const realizedR = trade.risk_usd > 0 ? netPnl / trade.risk_usd : 0;

    await env.DB.prepare(
      `UPDATE trades SET status='closed', price=?, realized_pnl=?, realized_r=?, fees=?, close_type='MANUAL', updated_at=? WHERE id=?`
    ).bind(exitPrice, netPnl, realizedR, fees, nowISO(), tradeId).run();

    await gistReportClosed(env, trade, exitPrice, 'MANUAL', protocol?.fee_rate, details || null);

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

/* ---------- Reconciliation helpers (optional; default OFF) ---------- */
function fillFingerprint(sym, fill) {
  try {
    const side = (fill.isBuyer === true ? "B" : (fill.isBuyer === false ? "S" : (String(fill.side||"").toUpperCase()==="BUY"?"B":"S")));
    const oid = fill.orderId != null ? String(fill.orderId) : (fill.orderID != null ? String(fill.orderID) : "");
    const tid = fill.id != null ? String(fill.id) : (fill.tradeId != null ? String(fill.tradeId) : "");
    const qty = Math.round((+fill.qty || +fill.executedQty || +fill.baseQty || 0) * 1e8);
    const price = Math.round((+fill.price || 0) * 1e8);
    const cma = (fill.commissionAsset || fill.cma || "").toUpperCase();
    const mk = (fill.isMaker === true) ? "M" : (fill.isMaker === false) ? "T" : "?";
    return [String(sym||"").toUpperCase()+"USDT", side, oid, tid, qty, price, cma, mk].join(":");
  } catch {
    return String(fill?.id || fill?.orderId || Math.random());
  }
}
async function convertCommissionToUSDT(asset, amount) {
  try {
    if (!asset || asset.toUpperCase() === "USDT") return Number(amount || 0);
    const px = await getCurrentPrice(String(asset || "").toUpperCase());
    return Number(amount || 0) * (isFinite(px) && px > 0 ? px : 0);
  } catch { return 0; }
}

/* ---------- Exchange reconciliation (detect bracket TP/SL fills) ---------- */
async function reconcileExchangeFills(env, userId) {
  if (String(env?.RECONCILE_FROM_EXCHANGE || "0") !== "1") return;

  const session = await getSession(env, userId);
  if (!session || !session.api_key_encrypted || !session.api_secret_encrypted) return;
  const ex = SUPPORTED_EXCHANGES[session.exchange_name];
  if (!ex || !ex.hasOrders) return;

  const apiKey = await decrypt(session.api_key_encrypted, env);
  const apiSecret = await decrypt(session.api_secret_encrypted, env);

  // Fetch open trades (only these need reconciliation for bracket exits)
  const open = await env.DB.prepare(
    "SELECT id, symbol, side, qty, entry_price, risk_usd, extra_json, created_at, updated_at FROM trades WHERE user_id = ? AND status = 'open'"
  ).bind(userId).all();

  const commFromEx = String(env?.COMMISSIONS_FROM_EXCHANGE || "1") === "1";

  for (const t of (open.results || [])) {
    try {
      const extra = JSON.parse(t.extra_json || '{}');
      if (!extra?.open_brackets) continue; // only reconcile bracket-driven

      const isShort = t.side === 'SELL';
      // Check bracket order statuses
      let tpFill = null, slFill = null;
      if (ex.kind === "binanceLike") {
        const s = await getBinanceLikeOrderStatus(ex, apiKey, apiSecret, t.symbol, extra.open_brackets?.tp?.orderId);
        if ((s?.status || "").toUpperCase() === "FILLED" && Number(s.executedQty||0) > 0) tpFill = s;
        const s2 = await getBinanceLikeOrderStatus(ex, apiKey, apiSecret, t.symbol, extra.open_brackets?.sl?.orderId);
        if ((s2?.status || "").toUpperCase() === "FILLED" && Number(s2.executedQty||0) > 0) slFill = s2;
      } else if (ex.kind === "binanceFuturesUSDT") {
        const s = await getBinanceFuturesOrderStatus(ex, apiKey, apiSecret, t.symbol, extra.open_brackets?.tp?.orderId);
        if ((s?.status || "").toUpperCase() === "FILLED" && Number(s.executedQty||0) > 0) tpFill = s;
        const s2 = await getBinanceFuturesOrderStatus(ex, apiKey, apiSecret, t.symbol, extra.open_brackets?.sl?.orderId);
        if ((s2?.status || "").toUpperCase() === "FILLED" && Number(s2.executedQty||0) > 0) slFill = s2;
      } else if (ex.kind === "bybitFuturesV5") {
        const s = await getBybitFuturesOrderStatus(ex, apiKey, apiSecret, t.symbol, { orderId: extra.open_brackets?.tp?.orderId });
        if ((s?.status || "").toUpperCase() === "FILLED" && Number(s.executedQty||0) > 0) tpFill = s;
        const s2 = await getBybitFuturesOrderStatus(ex, apiKey, apiSecret, t.symbol, { orderId: extra.open_brackets?.sl?.orderId });
        if ((s2?.status || "").toUpperCase() === "FILLED" && Number(s2.executedQty||0) > 0) slFill = s2;
      } else {
        continue;
      }

      if (!tpFill && !slFill) continue; // nothing to close

      const exitReason = tpFill ? "tp" : "sl";
      const st = tpFill || slFill;
      const exitQty = Number(st.executedQty || 0);
      const exitPrice = Number(st.avgPrice || 0);
      if (!(exitQty > 0 && exitPrice > 0)) continue;

      // Commission and maker/taker from userTrades (best-effort)
      let commissionQuoteUSDT = null;
      let commissionBps = null;
      let maker_taker_entry = null;
      let maker_taker_exit = null;
      let fp_entry = null;
      let fp_exit = null;
      try {
        // fetch fills in recent window
        const startMs = Date.parse(t.created_at || t.updated_at || "") - 30 * 60 * 1000;
        const fills = ex.kind === "binanceLike"
          ? await getBinanceLikeMyTrades(ex, apiKey, apiSecret, t.symbol, { startTime: startMs, limit: 1000 })
          : ex.kind === "binanceFuturesUSDT"
          ? await getBinanceFuturesUserTrades(ex, apiKey, apiSecret, t.symbol, { startTime: startMs, limit: 1000 })
          : ex.kind === "bybitFuturesV5"
          ? await getBybitFuturesUserTrades(ex, apiKey, apiSecret, t.symbol, { startTime: startMs, limit: 1000 })
          : [];

        // Separate entry vs exit by side
        // For long (BUY entry), exit is SELL; for short (SELL entry), exit is BUY
        const entrySideIsBuy = !isShort;
        const exitSideIsBuy = isShort;

        let commissionEntryUSDT = 0, entryMakerVotes = 0, entryTakerVotes = 0;
        let commissionExitUSDT = 0, exitMakerVotes = 0, exitTakerVotes = 0;

        for (const f of (fills || [])) {
          const isBuyer = f.isBuyer === true || String(f.side||"").toUpperCase() === "BUY";
          const qty = Number(f.qty || f.qty_filled || f.baseQty || 0);
          const price = Number(f.price || f.avgPrice || 0);

          // Derive entry/exit group
          const isEntry = (isBuyer && entrySideIsBuy) || (!isBuyer && !entrySideIsBuy);
          const isExit = (isBuyer && exitSideIsBuy) || (!isBuyer && !exitSideIsBuy);

          if (isEntry && qty > 0 && price > 0) {
            if (f.isMaker === true) entryMakerVotes++; else entryTakerVotes++;
            if (commFromEx && f.commission != null) {
              const ca = (f.commissionAsset || "").toUpperCase();
              const cu = await convertCommissionToUSDT(ca, Number(f.commission || 0));
              commissionEntryUSDT += cu;
            }
            if (!fp_entry) fp_entry = fillFingerprint(t.symbol, f);
          } else if (isExit && qty > 0 && price > 0) {
            if (f.isMaker === true) exitMakerVotes++; else exitTakerVotes++;
            if (commFromEx && f.commission != null) {
              const ca = (f.commissionAsset || "").toUpperCase();
              const cu = await convertCommissionToUSDT(ca, Number(f.commission || 0));
              commissionExitUSDT += cu;
            }
            if (!fp_exit) fp_exit = fillFingerprint(t.symbol, f);
          }
        }

        if (commFromEx) {
          commissionQuoteUSDT = commissionEntryUSDT + commissionExitUSDT;
          if (t.entry_price > 0 && t.qty > 0) {
            commissionBps = (commissionQuoteUSDT / (t.entry_price * t.qty)) * 10000;
          }
        }
        maker_taker_entry = entryMakerVotes >= entryTakerVotes ? "M" : "T";
        maker_taker_exit = exitMakerVotes >= exitTakerVotes ? "M" : "T";
      } catch {}

      // Close trade in DB
      const protocol = await getProtocolState(env, userId);
      const grossPnl = !isShort ? (exitPrice - t.entry_price) * t.qty : (t.entry_price - exitPrice) * t.qty;
      const feesEst = (t.entry_price * t.qty + exitPrice * t.qty) * (protocol?.fee_rate ?? 0.001);
      const fees = commFromEx && Number.isFinite(commissionQuoteUSDT) ? commissionQuoteUSDT : feesEst;
      const netPnl = grossPnl - fees;
      const realizedR = t.risk_usd > 0 ? netPnl / t.risk_usd : 0;

      await env.DB.prepare(
        `UPDATE trades SET status='closed', price=?, realized_pnl=?, realized_r=?, fees=?, close_type=?, updated_at=? WHERE id=?`
      ).bind(exitPrice, netPnl, realizedR, fees, exitReason.toUpperCase(), nowISO(), t.id).run();

      // Prepare details for gistReportClosed
      const details = {
        fromExchange: true,
        commission_quote_usdt: commissionQuoteUSDT ?? fees,
        commission_bps: commissionBps ?? ((fees / Math.max(1e-9, t.entry_price * t.qty)) * 10000),
        maker_taker_entry: maker_taker_entry,
        maker_taker_exit: maker_taker_exit,
        commission_asset_entry: "USDT",
        commission_asset_exit: "USDT",
        fingerprint_entry: fp_entry,
        fingerprint_exit: fp_exit
      };

      await gistReportClosed(env, t, exitPrice, exitReason, protocol?.fee_rate, details);
      await setSymbolCooldown(env, userId, t.symbol);
      await logEvent(env, userId, 'trade_close', {
        symbol: t.symbol, entry: t.entry_price, exit: exitPrice, realizedR,
        hit: exitReason, exit_source: 'reconcile',
        pnl: netPnl, fees
      });
    } catch (e) {
      console.warn("reconcileExchangeFills warn:", e?.message || e);
    }
  }
}

/* ---------- Robust auto-close with mgmt overlay + batched price fetching ---------- */
async function checkAndExitTrades(env, userId) {
  const log = new Logger(env, { fn: 'checkAndExitTrades', userId });
  log.info('🔍 Checking open trades for exits');
  const timer = log.time('checkAndExitTrades');
  
  const openTrades = await env.DB.prepare(
    "SELECT id, symbol, side, qty, entry_price, stop_price, tp_price, risk_usd, stop_pct, extra_json, created_at, updated_at FROM trades WHERE user_id = ? AND status = 'open'"
  ).bind(userId).all();
  
  if (!openTrades?.results?.length) {
    log.info('ℹ️ No open trades to check');
    timer.end({ trades_checked: 0 });
    return;
  }
  log.info(`✅ Loaded ${openTrades.results.length} open trades to check`);

  const protocol = await getProtocolState(env, userId);

  // Batch unique symbols for one-shot pricing
  const symbols = [...new Set(openTrades.results.map(t => t.symbol))];
  log.debug('Fetching prices for all symbols', { count: symbols.length });
  const priceMap = new Map();
  for (const s of symbols) {
    priceMap.set(s, await robustPriceFor(s));
  }
  log.info(`✅ Prices fetched for ${symbols.length} unique symbols`);

  // Fallback global time-stop (only if per-trade TTL not present)
  const AGE_MS_FALLBACK = getMaxTradeAgeMs(env);
  const overlayOn = envFlag(env, 'ENABLE_MGMT_OVERLAY', '1');

  for (const trade of (openTrades.results || [])) {
    const tradeLog = log.scope({ tradeId: trade.id, symbol: trade.symbol });
    try {
      let currentPrice = Number(priceMap.get(trade.symbol) || 0);
      if (!isFinite(currentPrice) || currentPrice <= 0) {
        tradeLog.error('Bad price for symbol, skipping check', { price: currentPrice });
        continue;
      }
      tradeLog.debug('Checking trade for exit conditions', { currentPrice, qty: trade.qty });

      const isShort = (trade.side === 'SELL');
      let shouldExit = false;
      let exitReason = '';

      let extra = {};
      try { extra = JSON.parse(trade.extra_json || '{}'); } catch { extra = {}; }
      const haveBrackets = !!(extra.open_brackets);
      const mgmt = extra?.mgmt || {};
      const rNow = unrealizedR(trade, currentPrice);
      const costBps = extra?.idea_fields?.cost_bps;

      let newStop = trade.stop_price;
      let stateChanged = false;

      if (overlayOn) {
        // 1) Break-even shift
        if (isFinite(mgmt.be_at_r) && rNow >= Number(mgmt.be_at_r) && !extra?.mgmt_state?.be_done) {
          tradeLog.info('📈 Mgmt: Applying break-even stop', { rNow, be_at_r: mgmt.be_at_r });
          newStop = trade.entry_price;
          extra.mgmt_state = { ...(extra.mgmt_state || {}), be_done: true };
          stateChanged = true;
        }

        // 2) Trailing stop
        if (isFinite(mgmt.trail_atr_mult) && Number(mgmt.trail_atr_mult) > 0) {
          const baseDist = trade.stop_pct * trade.entry_price;
          const trailDist = baseDist * Number(mgmt.trail_atr_mult);
          const trailed = isShort ? Math.min(newStop, currentPrice + trailDist) : Math.max(newStop, currentPrice - trailDist);
          const improved = isShort ? trailed < newStop : trailed > newStop;
          if (improved) {
            tradeLog.info('📈 Mgmt: Applying trailing stop', { oldStop: newStop, newStop: trailed });
            newStop = trailed;
            stateChanged = true;
          }
        }

        // 3) TTL rescue
        if (mgmt.extend_ttl_if_unrealized_bps_ge != null) {
          const thr = parseThreshold(mgmt.extend_ttl_if_unrealized_bps_ge, costBps);
          const bpsUnreal = ((currentPrice / trade.entry_price - 1) * (isShort ? -1 : 1)) * 10000;
          if (thr != null && bpsUnreal >= thr && isFinite(mgmt.extend_ttl_sec) && mgmt.extend_ttl_sec > 0) {
            const ttl = (extra.ttl || {});
            const newTtl = Math.max(Number(ttl.ttl_ts_ms || 0), Date.now()) + Number(mgmt.extend_ttl_sec) * 1000;
            tradeLog.info('📈 Mgmt: Extending TTL', { bpsUnreal, threshold: thr, oldTtl: ttl.ttl_ts_ms, newTtl });
            extra.ttl = { ...(ttl || {}), ttl_ts_ms: newTtl, source: 'mgmt_extend' };
            stateChanged = true;
          }
        }

        // 4) Early cut (soft SL) - OPTIMIZATION: Removed bracket cancel
        if (mgmt.early_cut_if_unrealized_bps_le != null) {
          const thrLe = parseThreshold(mgmt.early_cut_if_unrealized_bps_le, costBps);
          const bpsUnreal = ((currentPrice / trade.entry_price - 1) * (isShort ? -1 : 1)) * 10000;
          if (thrLe != null && bpsUnreal <= thrLe) {
            tradeLog.info('🎯 Mgmt: Early cut triggered', { bpsUnreal, threshold: thrLe });
            shouldExit = true;
            exitReason = 'MGMT_EARLY_CUT';
          }
        }
        
        // 5) Partial take - CRITICAL FIX: Update in-memory quantity
        if (isFinite(mgmt.partial_take_at_r) && rNow >= Number(mgmt.partial_take_at_r) && !extra?.mgmt_state?.pt_done) {
          const pct = clamp(Number(mgmt.partial_take_pct || 0.5), 0.05, 0.95);
          const qtyPart = trade.qty * pct;
          tradeLog.info('📈 Mgmt: Taking partial profit', { rNow, at_r: mgmt.partial_take_at_r, pct, qtyPart });
          try {
            if (trade.side === 'SELL') {
              const buy = await placeMarketBuy(env, userId, trade.symbol, qtyPart * currentPrice, { reduceOnly: true });
              const newQty = Math.max(0, trade.qty - (buy.executedQty || qtyPart));
              await env.DB.prepare("UPDATE trades SET qty = ?, updated_at = ? WHERE id = ?")
                .bind(newQty, nowISO(), trade.id).run();
              trade.qty = newQty; // Update in-memory object
            } else {
              const sell = await placeMarketSell(env, userId, trade.symbol, qtyPart, false, { reduceOnly: true });
              const newQty = Math.max(0, trade.qty - (sell.executedQty || qtyPart));
              await env.DB.prepare("UPDATE trades SET qty = ?, updated_at = ? WHERE id = ?")
                .bind(newQty, nowISO(), trade.id).run();
              trade.qty = newQty; // Update in-memory object
            }
            extra.mgmt_state = { ...(extra.mgmt_state || {}), pt_done: true };
            stateChanged = true;
            tradeLog.info('✅ Partial take successful', { newQty: trade.qty });
          } catch (e) {
            tradeLog.error('Partial take profit failed', { error: e.message });
          }
        }
      }

      if (stateChanged) {
        await env.DB.prepare("UPDATE trades SET stop_price = ?, extra_json = ?, updated_at = ? WHERE id = ?")
          .bind(newStop, JSON.stringify(extra), nowISO(), trade.id).run();
      }

      // ALWAYS check price-based exits as a backup (CRITICAL FIX)
      if (!shouldExit) {
        if (!isShort) {
          if (currentPrice <= newStop) { shouldExit = true; exitReason = 'SL'; }
          else if (currentPrice >= trade.tp_price) { shouldExit = true; exitReason = 'TP'; }
        } else {
          if (currentPrice >= newStop) { shouldExit = true; exitReason = 'SL'; }
          else if (currentPrice <= trade.tp_price) { shouldExit = true; exitReason = 'TP'; }
        }
        if (exitReason) tradeLog.debug('Price-based exit met', { exitReason, currentPrice, newStop, tp_price: trade.tp_price });
      }

      // TTL enforcement (per-trade)
      let ttlTs = null;
      try { ttlTs = Number(JSON.parse(trade.extra_json || '{}')?.ttl?.ttl_ts_ms ?? null); } catch {}
      if (!shouldExit && isFinite(ttlTs) && ttlTs > 0 && Date.now() >= ttlTs) {
        shouldExit = true;
        exitReason = 'TTL';
        tradeLog.debug('Per-trade TTL met');
      }

      // Fallback global TIME stop (only if no per-trade TTL)
      if (!shouldExit && !(isFinite(ttlTs) && ttlTs > 0)) {
        const openedAtMs = Date.parse(trade.updated_at || trade.created_at || "");
        if (isFinite(openedAtMs) && (Date.now() - openedAtMs >= AGE_MS_FALLBACK)) {
          shouldExit = true;
          exitReason = 'TIME';
          tradeLog.debug('Global time stop met');
        }
      }

      if (!shouldExit) continue;

      tradeLog.info('🎯 Exit triggered', { reason: exitReason });

      if (haveBrackets) {
        const session2 = await getSession(env, userId);
        const ex2 = SUPPORTED_EXCHANGES[session2.exchange_name];
        await cancelBracketsIfAny(env, userId, trade, session2, ex2, extra);
      }

      const cidBase = extra?.client_order_id || null;
      const exitCid = (exitReason === 'TTL' || exitReason === 'TIME') && cidBase ? `${cidBase}:ttl` : cidBase;

      let exitPrice = currentPrice;
      if (trade.qty > 0) { // Ensure there is remaining quantity to close
        if (!isShort) {
            const sellResult = await placeMarketSell(env, userId, trade.symbol, trade.qty, false, { reduceOnly: true, clientOrderId: exitCid });
            exitPrice = sellResult.avgPrice || currentPrice;
        } else {
            const session = await getSession(env, userId);
            if (session.exchange_name === 'crypto_parrot') {
            const buyResult = await placeDemoOrder(trade.symbol, "BUY", trade.qty, false, exitCid);
            exitPrice = buyResult.avgPrice || currentPrice;
            } else {
            const requiredQuote = trade.qty * currentPrice;
            const buyResult = await placeMarketBuy(env, userId, trade.symbol, requiredQuote, { reduceOnly: true, clientOrderId: exitCid });
            exitPrice = buyResult.avgPrice || currentPrice;
            }
        }
      } else {
        tradeLog.warn('Trade has zero quantity, marking as closed without exchange order');
      }

      const grossPnl = !isShort ? (exitPrice - trade.entry_price) * trade.qty : (trade.entry_price - exitPrice) * trade.qty;
      const fees = (trade.entry_price * trade.qty + exitPrice * trade.qty) * (protocol?.fee_rate ?? 0.001);
      const netPnl = grossPnl - fees;
      const realizedR = trade.risk_usd > 0 ? netPnl / trade.risk_usd : 0;

      await env.DB.prepare(
        `UPDATE trades SET status = 'closed', price = ?, realized_pnl = ?, realized_r = ?, fees = ?, close_type = ?, updated_at = ? WHERE id = ?`
      ).bind(exitPrice, netPnl, realizedR, fees, exitReason, nowISO(), trade.id).run();

      await gistReportClosed(env, trade, exitPrice, exitReason, protocol?.fee_rate);
      await setSymbolCooldown(env, userId, trade.symbol);

      const r_target = (() => { try { return Number(JSON.parse(trade.extra_json||"{}")?.r_target || STRICT_RRR); } catch { return STRICT_RRR; } })();
      await reconcileProtocol(env, userId, exitReason === 'TP', trade.risk_usd, r_target, fees);
      await logEvent(env, userId, 'trade_close', {
        symbol: trade.symbol, entry: trade.entry_price, exit: exitPrice, realizedR,
        hit: exitReason, exit_source: (exitReason === 'TTL' ? 'ttl_scan' : (exitReason === 'TIME' ? 'time_stop' : 'sl_tp_scan')),
        pnl: netPnl, fees, strict_rrr: true, r_target,
        ttl_ts_ms: ttlTs
      });
    } catch (e) {
      tradeLog.error('Error processing trade for exit', { error: e.message, stack: e.stack });
      await logEvent(env, userId, 'error', { where: 'checkAndExitTrades', message: e.message, tradeId: trade.id });
    }
  }
  timer.end({ trades_checked: openTrades.results.length });
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
  getWinStats,
  // new reconciliation export (wired in Section 7)
  reconcileExchangeFills
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
    "Futures (USDT-M): Binance Futures and Bybit Futures (Testnet) are integrated.",
    "",
    "Read-only (balances): Kraken, Gate, Huobi, (Bybit Spot wallet).",
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

  const ttlTs = Number(extra?.ttl?.ttl_ts_ms || 0);
  const ttlLeftMs = ttlTs > 0 ? (ttlTs - Date.now()) : 0;
  const ttlLine = ttlTs > 0 ? `TTL: ${formatDurationShort(Math.max(0, ttlLeftMs))}` : "TTL: -";

  const oo = extra?.open_order;
  const ooLine = oo?.id
    ? `Posted maker @ ${formatMoney(oo.px)} (CID: ${extra?.client_order_id || "-"})`
    : "Not posted yet";

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
    ttlLine,
    ooLine,
    "",
    "Approve to place the order now (market or maker intent). Reject/Cancel to discard or cancel a posted maker order."
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
    [{ text: "Bybit Futures (Testnet)", callback_data: "exchange_bybit_futures_testnet" }, { text: "Bybit (Wallet)", callback_data: "exchange_bybit" }],
    [{ text: "Kraken", callback_data: "exchange_kraken" }, { text: "Gate", callback_data: "exchange_gate" }],
    [{ text: "Huobi", callback_data: "exchange_huobi" }],
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
function kbPendingTrade(tradeId, fundsOk, hasOpenOrder = false) {
  const rows = [];
  if (hasOpenOrder) {
    rows.push([{ text: "Cancel order ❌", callback_data: `tr_cancel:${tradeId}` }, { text: "Refresh 🔄", callback_data: `tr_refresh:${tradeId}` }]);
  } else {
    if (fundsOk) rows.push([{ text: "Approve ✅", callback_data: `tr_appr:${tradeId}` }, { text: "Reject ❌", callback_data: `tr_rej:${tradeId}` }]);
    else rows.push([{ text: "Reject ❌", callback_data: `tr_rej:${tradeId}` }]);
  }
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
    text = "Bot is active (Manual).\n\nExits: dynamic RRR per idea when provided; fallback 2:1.\nACP V20 sizing. Futures 1x isolated; non-demo requires a futures venue (Bybit Futures Testnet/Binance Futures).\nUse Manage Trades to inspect suggestions and open/closed positions.";
  } else {
    text = `Bot is active (Auto).\nStatus: ${paused ? "Paused" : "Running"}.\n\nExits: dynamic RRR per idea when provided; fallback 2:1.\nACP V20 sizing. Futures 1x isolated; non-demo requires a futures venue (Bybit Futures Testnet/Binance Futures).\nUse Manage Trades to inspect positions.`;
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
      
      // Emoji: 🟢 for BUY (long), 🔴 for SELL (short)
      const emoji = r.side === 'BUY' ? '🟢' : '🔴';
      
      if (r.status === 'open') {
        return `${emoji} ${r.symbol} ${r.side} ${qty.toFixed(4)} — open | Entry ${formatMoney(r.entry_price)} [${intent}] | SL ${formatMoney(r.stop_price)}${stopPctTxt} | TP ${formatMoney(r.tp_price)} (R ${rTxt})`;
      } else if (r.status === 'pending') {
        return `${emoji} ${r.symbol} ${r.side} ${qty.toFixed(4)} — pending | Entry~ ${formatMoney(r.price)} [${intent}] | SL ${formatMoney(r.stop_price)}${stopPctTxt} | TP ${formatMoney(r.tp_price)} (R ${rTxt})`;
      } else if (r.status === 'rejected') {
        let reason = '';
        try { const exj = JSON.parse(r.extra_json || '{}'); if (exj.auto_skip && exj.skip_reason) reason = ` — skipped: ${String(exj.skip_reason).replace(/_/g,' ')}`; } catch (_) {}
        return `${emoji} ${r.symbol} ${r.side} ${qty.toFixed(4)} — rejected${reason} | Entry~ ${formatMoney(r.price)} [${intent}] | SL ${formatMoney(r.stop_price)}${stopPctTxt} | TP ${formatMoney(r.tp_price)} (R ${rTxt})`;
      } else if (r.status === 'closed') {
        const pnl = Number(r.realized_pnl || 0);
        const rVal = Number(r.realized_r || 0);
        const hit = r.close_type ? ` (${r.close_type})` : "";
        return `${emoji} ${r.symbol} ${r.side} ${qty.toFixed(4)} — closed${hit} | P&L ${pnl >= 0 ? "+" : ""}${formatMoney(pnl)} | R ${rVal >= 0 ? "+" : ""}${rVal.toFixed(2)}`;
      } else {
        return `${emoji} ${r.symbol} ${r.side} ${qty.toFixed(4)} — ${r.status}`;
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
    const hasOO = !!(extra?.open_order?.id);
    const buttons = kbPendingTrade(tradeId, extra.funds_ok, hasOO);
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

  if (trade.status === 'rejected' && !extra?.auto_skip) {
    const reason = trade.close_type || 'REJECTED';
    const t = [
      `Suggestion #${trade.id} (Rejected)`,
      "",
      `Symbol: ${trade.symbol}`,
      `Direction: ${trade.side === 'SELL' ? 'Short' : 'Long'}`,
      `Reason: ${reason}`
    ].join("\n");
    const b = [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }], [{ text: "Back to list", callback_data: "manage_trades" }]];
    if (messageId) await editMessage(userId, messageId, t, b, env);
    else await sendMessage(userId, t, b, env);
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
    case 'tr_cancel': {
      const row = await env.DB.prepare("SELECT * FROM trades WHERE id = ? AND user_id = ?").bind(tradeId, userId).first();
      if (!row || row.status !== 'pending') {
        await editMessage(userId, messageId, `Trade #${tradeId} not cancellable.`, [[{ text: "Continue ▶️", callback_data: "continue_dashboard" }]], env);
        break;
      }
      let extra = {};
      try { extra = JSON.parse(row.extra_json || '{}'); } catch {}
      const oo = extra?.open_order;
      if (!oo?.id) {
        // nothing on exchange; mark rejected
        await env.DB.prepare("UPDATE trades SET status='rejected', close_type='MANUAL_CANCEL', updated_at=? WHERE id=? AND user_id=?")
          .bind(nowISO(), tradeId, userId).run();
        await sendTradeDetailsUI(env, userId, tradeId, messageId);
        break;
      }
      try {
        const session = await getSession(env, userId);
        const ex = SUPPORTED_EXCHANGES[session.exchange_name];
        const apiKey = await decrypt(session.api_key_encrypted, env);
        const apiSecret = await decrypt(session.api_secret_encrypted, env);
        if (ex.kind === 'binanceLike')        await cancelBinanceLikeOrder(ex, apiKey, apiSecret, row.symbol, { orderId: oo.id });
        else if (ex.kind === 'binanceFuturesUSDT') await cancelBinanceFuturesOrder(ex, apiKey, apiSecret, row.symbol, { orderId: oo.id });
        else if (ex.kind === 'bybitFuturesV5') await cancelBybitFuturesOrder(ex, apiKey, apiSecret, row.symbol, { orderId: oo.id });
      } catch (_) {}
      // Update DB (reject) and remove gist pending
      delete extra.open_order;
      await env.DB.prepare("UPDATE trades SET status='rejected', close_type='MANUAL_CANCEL', extra_json=?, updated_at=? WHERE id=? AND user_id=?")
        .bind(JSON.stringify(extra), nowISO(), tradeId, userId).run();
      if (extra?.client_order_id) {
        await gistPatchState(env, (state) => {
          const idx = gistFindPendingIdxByCID(state, extra.client_order_id);
          if (idx >= 0) state.pending.splice(idx, 1);
          return state;
        }).catch(()=>{});
      }
      await sendTradeDetailsUI(env, userId, tradeId, messageId);
      break;
    }
    case 'tr_rej': {
      const row = await env.DB.prepare("SELECT * FROM trades WHERE id = ? AND user_id = ?").bind(tradeId, userId).first();
      if (row && row.status === 'pending') {
        let extra = {}; try { extra = JSON.parse(row.extra_json || '{}'); } catch {}
        const oo = extra?.open_order;
        if (oo?.id) {
          try {
            const session = await getSession(env, userId);
            const ex = SUPPORTED_EXCHANGES[session.exchange_name];
            const apiKey = await decrypt(session.api_key_encrypted, env);
            const apiSecret = await decrypt(session.api_secret_encrypted, env);
            if (ex.kind === 'binanceLike') await cancelBinanceLikeOrder(ex, apiKey, apiSecret, row.symbol, { orderId: oo.id });
            else if (ex.kind === 'binanceFuturesUSDT')await cancelBinanceFuturesOrder(ex, apiKey, apiSecret, row.symbol, { orderId: oo.id });
            else if (ex.kind === 'bybitFuturesV5') await cancelBybitFuturesOrder(ex, apiKey, apiSecret, row.symbol, { orderId: oo.id });
          } catch (_) {}
          delete extra.open_order;
          await env.DB.prepare("UPDATE trades SET extra_json=?, updated_at=? WHERE id=? AND user_id=?")
            .bind(JSON.stringify(extra), nowISO(), tradeId, userId).run();
        }
        // remove gist pending by CID (if any)
        if (extra?.client_order_id) {
          await gistPatchState(env, (state) => {
            const idx = gistFindPendingIdxByCID(state, extra.client_order_id);
            if (idx >= 0) state.pending.splice(idx, 1);
            return state;
          }).catch(()=>{});
        }
      }
      await env.DB.prepare("UPDATE trades SET status = 'rejected', updated_at = ? WHERE id = ? AND user_id = ?")
        .bind(nowISO(), tradeId, userId).run();
      await sendTradesListUI(env, userId, 1, messageId);
      break;
    }
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

/* ---------- Gist → ideas builder (planned → ideas array) ---------- */
async function buildIdeasFromGistPending(env) {
  // Retry logic for robust fetch (3 attempts on 401/403/412)
  let g = null;
  for (let attempt = 1; attempt <= 3; attempt++) {
    g = await gistGetState(env);
    if (g && g.state) break;  // Success
    console.log(`[Gist Retry ${attempt}/3] Failed, waiting 2s...`);
    await sleep(2000);  // Backoff
  }
  if (!g || !g.state) {
    console.warn(`[Gist Final Fail] All retries failed; using fallback.`);
    return null;
  }

  // UPDATED: Optional success log (for verification post-fix; remove after confirming no fallbacks).
  console.log("[Gist Success] Loaded pending:", g.state.pending?.length || 0);

  const pendAll = g.state?.pending;
  if (!Array.isArray(pendAll) || pendAll.length === 0) {
    return { ts: nowISO(), ideas: [], meta: { origin: "gist-empty" } };
  }

  const planned = pendAll.filter(p => (p?.status || "planned") === "planned");
  if (!planned.length) {
    return { ts: nowISO(), ideas: [], meta: { origin: "gist-no-planned" } };
  }

  // UPDATED: Optional log for planned count (remove after testing).
  console.log("[Gist Success] Loaded", planned.length, "planned ideas");

  // Best timestamp from plans (fallback now)
  const maxTs = planned.reduce((m, p) => Math.max(m, Number(p.ts_ms || 0)), 0);
  const ts = maxTs > 0 ? new Date(maxTs).toISOString() : nowISO();

  const ideas = planned.map(p => {
    const base = String(p.base || "").toUpperCase()
      || String((p.symbolFull || "").replace(/USDT$/i, "")).toUpperCase();
    // Normalize side to long/short
    let side = String(p.side || 'long').toLowerCase();
    if (side === 'buy') side = 'long';
    if (side === 'sell') side = 'short';

    return {
      // core
      symbol: base,
      side,
      ttl_sec: isFinite(Number(p.hold_sec)) ? Number(p.hold_sec) : undefined,

      // exits/sizing from plan
      entry_limit: isFinite(Number(p.entry_limit)) ? Number(p.entry_limit) : undefined,
      tp_bps: isFinite(Number(p.tp_bps)) ? Number(p.tp_bps) : undefined,
      sl_bps: isFinite(Number(p.sl_bps)) ? Number(p.sl_bps) : undefined,
      cost_bps: isFinite(Number(p.cost_bps)) ? Number(p.cost_bps) : undefined,
      notional_usd: isFinite(Number(p.notional_usd)) ? Number(p.notional_usd) : undefined,

      // probabilities and metadata linkages
      p_lcb: isFinite(Number(p.p_lcb)) ? Number(p.p_lcb) : undefined,
      p_win: isFinite(Number(p.p_lcb)) ? Number(p.p_lcb) : undefined,
      p_raw: isFinite(Number(p.p_raw)) ? Number(p.p_raw) : undefined,
      calib_key: p.calib_key,
      regime: p.regime,
      predicted: p.predicted,

      // stable IDs
      client_order_id: p.client_order_id,
      idea_id: p.idea_id
    };
  });

  return { ts, ideas, meta: { origin: "gist" } };
}

/* ---------- Per-user processing (flat-CPU: early exits, time guards) ---------- */
async function processUser(env, userId, budget = null) {
  const log = new Logger(env, { fn: 'processUser', userId });
  log.info('🔄 Starting user processing cycle');

  const cycleTimer = log.time('processUser_cycle');

  try {
    // Step 1: Budget check
    if (budget?.isExpired()) {
      log.warn('⏱️ CPU budget expired before start', {
        remaining_ms: budget.timeLeft()
      });
      return;
    }

    // Step 2: Session validation
    log.debug('Step 1: Loading user session');
    const session = await getSession(env, userId);
    if (!session) {
      log.info('ℹ️ No session found for user');
      return;
    }

    const ex = SUPPORTED_EXCHANGES[session.exchange_name];
    if (!ex) {
      log.warn('⚠️ Invalid exchange configuration', { 
        exchange: session.exchange_name 
      });
      return;
    }

    log.info('✅ Session loaded', {
      exchange: session.exchange_name,
      mode: session.bot_mode,
      status: session.status
    });

    // Step 3: Futures-only enforcement
    log.debug('Step 2: Checking exchange compatibility');
    if (session.exchange_name !== 'crypto_parrot' && 
        ["binanceLike","lbankV2","coinexV2"].includes(ex.kind)) {
      log.warn('🚫 Futures-only mode: Spot exchange blocked', {
        exchange: session.exchange_name,
        kind: ex.kind
      });
      await logEvent(env, userId, 'futures_only_block', { 
        exchange: session.exchange_name 
      });
      return;
    }

    // Step 4: Reconciliation
    log.debug('Step 3: Running exchange reconciliation');
    const reconTimer = log.time('reconciliation');
    await reconcileExchangeFills(env, userId);
    reconTimer.end();
    log.info('✅ Reconciliation completed');

    if (budget?.isExpired()) {
      log.warn('⏱️ CPU budget expired after reconciliation');
      return;
    }

    // Step 5: Working orders
    log.debug('Step 4: Checking working orders (post-only)');
    const workingTimer = log.time('working_orders');
    await checkWorkingOrders(env, userId);
    workingTimer.end();
    log.info('✅ Working orders processed');

    if (budget?.isExpired()) {
      log.warn('⏱️ CPU budget expired after working orders');
      return;
    }

    // Step 6: Auto exits
    log.debug('Step 5: Running auto exit checks');
    const exitTimer = log.time('auto_exits');
    await checkAndExitTrades(env, userId);
    exitTimer.end();
    log.info('✅ Auto exits processed');

    if (budget?.isExpired()) {
      log.warn('⏱️ CPU budget expired after exits');
      return;
    }

    // Step 7: Concurrency check
    log.debug('Step 6: Checking concurrency limits');
    const openPauseTh = Number(env.OPEN_PAUSE_THRESHOLD || 2);
    const openCountNow = await getOpenPositionsCount(env, userId);
    const overConcurrency = openCountNow >= openPauseTh;

    log.info('✅ Concurrency check', {
      open_positions: openCountNow,
      threshold: openPauseTh,
      over_limit: overConcurrency
    });

    // Step 8: Protocol validation
    log.debug('Step 7: Validating protocol state');
    const protocol = await getProtocolState(env, userId);
    if (session.status !== 'active' || !protocol || protocol.phase === 'terminated') {
      log.info('ℹ️ User not active or protocol terminated', {
        session_status: session.status,
        protocol_phase: protocol?.phase
      });
      return;
    }

    // Step 9: Load limits
    log.debug('Step 8: Loading position limits');
    const maxPos = getMaxConcurrent(env);
    const maxNew = getMaxNewPerCycle(env);

    log.info('✅ Limits loaded', {
      max_concurrent: maxPos,
      max_new_per_cycle: maxNew
    });

    // Step 10: Budget snapshot
    log.debug('Step 9: Calculating budgets');
    const budgetTimer = log.time('budget_calculation');

    const TC = await getTotalCapital(env, userId);
    const perTradeNotionalCapFrac = Number(env?.PER_TRADE_NOTIONAL_CAP_FRAC ?? 0.10);
    const dailyNotionalCapFrac = Number(env?.DAILY_OPEN_NOTIONAL_CAP_FRAC ?? 0.30);
    const dailyNotionalCap = dailyNotionalCapFrac * TC;
    const openNotional = await getOpenNotional(env, userId);
    let dailyNotionalLeft = Math.max(0, dailyNotionalCap - openNotional);

    const D = DAILY_OPEN_RISK_CAP_FRAC * TC;
    const openRisk = await getOpenPortfolioRisk(env, userId);
    let D_left = Math.max(0, D - openRisk);

    budgetTimer.end();

    log.info('✅ Budgets calculated', {
      TC: TC.toFixed(2),
      daily_notional_cap: dailyNotionalCap.toFixed(2),
      daily_notional_left: dailyNotionalLeft.toFixed(2),
      daily_risk_cap: D.toFixed(2),
      daily_risk_left: D_left.toFixed(2),
      open_notional: openNotional.toFixed(2),
      open_risk: openRisk.toFixed(2)
    });

    // Step 11: Load ideas
    log.debug('Step 10: Loading ideas');
    const ideasTimer = log.time('ideas_load');

    let ideas = await buildIdeasFromGistPending(env);

    if (ideas === null) {
      log.warn('⚠️ Gist fetch failed; falling back to DB snapshot');
      ideas = await getLatestIdeas(env);
    }

    ideasTimer.end({ 
      count: ideas?.ideas?.length || 0,
      origin: ideas?.meta?.origin 
    });

    if (!ideas || !ideas.ideas || !ideas.ideas.length) {
      log.info('ℹ️ No ideas available');
      return;
    }

    log.info('✅ Ideas loaded', {
      count: ideas.ideas.length,
      origin: ideas.meta?.origin,
      timestamp: ideas.ts
    });

    // Step 12: Freshness check
    log.debug('Step 11: Checking ideas freshness');
    try {
      const ts = new Date(ideas.ts || 0).getTime();
      const maxAgeMs = Number(env.IDEAS_MAX_AGE_MS || 20 * 60 * 1000);
      const age = Date.now() - ts;
      
      if (!ts || age > maxAgeMs) {
        log.warn('⚠️ Ideas too stale', {
          age_ms: age,
          max_age_ms: maxAgeMs,
          age_min: (age / 60000).toFixed(1)
        });
        return;
      }

      log.info('✅ Ideas freshness OK', {
        age_ms: age,
        age_min: (age / 60000).toFixed(1)
      });
    } catch (e) {
      log.error('❌ Freshness check failed', { error: e.message });
    }

    // Step 13: Snapshot deduplication
    log.debug('Step 12: Checking snapshot deduplication');
    const ideasTs = String(ideas.ts || "");
    const lastKey = `last_ideas_ts_user_${userId}`;
    const lastSeenTs = await kvGet(env, lastKey);

    if (openCountNow === 0 && lastSeenTs && lastSeenTs === ideasTs) {
      log.info('ℹ️ Snapshot already processed (no open positions)', {
        snapshot_ts: ideasTs
      });
      return;
    }

    log.info('✅ Snapshot dedup check passed', {
      current_snapshot: ideasTs,
      last_processed: lastSeenTs,
      is_new: lastSeenTs !== ideasTs
    });

    // Step 14: Duplicate symbol tracking
    log.debug('Step 13: Initializing duplicate symbol tracking');
    const counts = new Map();
    const cycleOnly = String(env?.CYCLE_ONLY_DUP_GUARD || '0') === '1';

    if (!cycleOnly) {
      const rowsCnt = await env.DB.prepare(
        "SELECT symbol, COUNT(*) AS c FROM trades WHERE user_id = ? AND status IN ('open','pending') GROUP BY symbol"
      ).bind(userId).all();

      for (const r of (rowsCnt.results || [])) {
        const s = String(r.symbol || '').toUpperCase();
        const c = Number(r.c || 0);
        if (s) counts.set(s, c);
      }

      log.info('✅ Symbol exposure loaded', {
        unique_symbols: counts.size,
        exposures: Array.from(counts.entries()).map(([sym, cnt]) => `${sym}:${cnt}`)
      });
    }

    const allowDup = String(env?.ALLOW_DUPLICATE_SYMBOLS || '0') === '1';
    const capPerSym = Math.max(1, Math.floor(Number(env?.MAX_POS_PER_SYMBOL || 1)));

    log.info('✅ Duplicate policy configured', {
      cycle_only: cycleOnly,
      allow_duplicates: allowDup,
      cap_per_symbol: capPerSym
    });

    // Step 15: Ideas processing loop
    log.info('🔄 Starting ideas processing loop', {
      total_ideas: ideas.ideas.length
    });

    let placed = 0;
    const stopReason = (!D_left ? 'daily_risk_budget_exhausted' 
      : (!dailyNotionalLeft ? 'daily_notional_exhausted' : null));

    if (stopReason) {
      log.warn('⚠️ Budget exhausted before processing ideas', {
        reason: stopReason,
        D_left,
        dailyNotionalLeft
      });
    }

    const ideasTsMs = new Date(ideas.ts || 0).getTime();

    for (let idx = 0; idx < ideas.ideas.length; idx++) {
      if (budget?.isExpired()) {
        log.warn('⏱️ CPU budget expired during ideas loop', {
          processed: idx,
          remaining: ideas.ideas.length - idx
        });
        break;
      }

      const idea = ideas.ideas[idx];
      const ideaLog = log.scope({ idea_idx: idx, symbol: idea.symbol });

      ideaLog.debug(`Processing idea ${idx + 1}/${ideas.ideas.length}`);

      // TTL check
      try {
        const ttlMs = Number(idea.ttl_sec || 0) * 1000;
        if (ttlMs > 0 && ideasTsMs > 0 && Date.now() - ideasTsMs > ttlMs) {
          ideaLog.info('⏱️ Idea expired (TTL)', {
            ttl_sec: idea.ttl_sec,
            age_ms: Date.now() - ideasTsMs
          });
          if (String(env?.NO_REJECTS || '0') !== '1') {
            await createAutoSkipRecord(env, userId, idea, protocol, 'stale_idea_ttl', { 
              ttl_sec: idea.ttl_sec 
            });
          }
          continue;
        }
      } catch (e) {
        ideaLog.warn('TTL check error', { error: e.message });
      }

      const symU = String(idea.symbol || '').toUpperCase();
      const curCount = counts.get(symU) || 0;

      // Duplicate check
      if (cycleOnly) {
        if (curCount > 0) {
          ideaLog.info('🚫 Duplicate symbol in cycle', { curCount });
          await createAutoSkipRecord(env, userId, idea, protocol, 'duplicate_symbol_cycle', { curCount });
          continue;
        }
      } else {
        if ((!allowDup && curCount > 0) || (allowDup && curCount >= capPerSym)) {
          ideaLog.info('🚫 Duplicate symbol cap reached', { 
            curCount, 
            capPerSym 
          });
          await createAutoSkipRecord(env, userId, idea, protocol, 'duplicate_symbol', { 
            curCount, 
            capPerSym 
          });
          continue;
        }
      }

      // Concurrency limit
      if (overConcurrency || (openCountNow + placed) >= maxPos) {
        ideaLog.info('🚫 Concurrency limit reached', {
          openCountNow,
          placed,
          maxPos
        });
        if (String(env?.NO_REJECTS || '0') !== '1') {
          await createAutoSkipRecord(env, userId, idea, protocol, 'concurrency_limit', { 
            openCountNow, 
            placed, 
            maxPos 
          });
        }
        continue;
      }

      // Budget exhaustion
      if (stopReason && String(env?.NO_REJECTS || '0') !== '1') {
        ideaLog.info('🚫 Budget stop', { reason: stopReason });
        await createAutoSkipRecord(env, userId, idea, protocol, stopReason, { 
          D_left, 
          dailyNotionalLeft 
        });
        continue;
      }

      // FIXED: Track budget before creating pending trade
      const beforeOpenRisk = await getOpenPortfolioRisk(env, userId);
      const beforeOpenNotional = await getOpenNotional(env, userId);

      ideaLog.debug('Creating pending trade');
      const createTimer = ideaLog.time('createPendingTrade');

      const { id, meta } = await createPendingTrade(env, userId, idea, protocol);

      createTimer.end({ 
        success: !!id, 
        skipReason: meta?.skipReason 
      });

      if (!id) {
        ideaLog.info('🚫 Trade creation rejected', { 
          reason: meta?.skipReason 
        });
        await createAutoSkipRecord(env, userId, idea, protocol, meta?.skipReason || 'unknown', meta);
        continue;
      }

      ideaLog.info('✅ Pending trade created', { tradeId: id });

      // Execute (Auto) or show card (Manual)
      if (session.bot_mode === 'auto' && session.auto_paused !== 'true') {
        ideaLog.debug('Executing trade (auto mode)');
        const execTimer = ideaLog.time('executeTrade');

        const result = await executeTrade(env, userId, id);

        execTimer.end({ 
          status: result.status,
          success: result.success 
        });

        // FIXED: Only treat as success if filled
        if (result.status !== 'filled') {
          ideaLog.warn('⚠️ Trade not filled', { 
            status: result.status 
          });
          
          if (result.status === 'posted') {
            ideaLog.info('📌 Order posted, waiting for fill');
            // Don't increment counts or decrement budgets yet
            continue;
          } else {
            // Failed execution
            await env.DB.prepare(
              "UPDATE trades SET status = 'failed', updated_at = ? WHERE id = ?"
            ).bind(nowISO(), id).run();
            
            ideaLog.error('❌ Trade execution failed', { 
              tradeId: id,
              error: result.error 
            });
            continue;
          }
        }

        // FIXED: Increment duplicate counter ONLY after successful fill
        counts.set(symU, (counts.get(symU) || 0) + 1);
        placed++;

        ideaLog.info('🎉 Trade executed successfully', { 
          tradeId: id,
          placed_this_cycle: placed 
        });

        // Cycle cap enforcement
        if (placed >= maxNew) {
          ideaLog.info('🛑 Cycle cap reached', { 
            placed, 
            maxNew 
          });
          
          if (String(env?.NO_REJECTS || '0') !== '1') {
            for (const rest of ideas.ideas.slice(idx + 1)) {
              await createAutoSkipRecord(env, userId, rest, protocol, 'cycle_cap_reached', { 
                placed, 
                maxNew 
              });
            }
          }
          break;
        }

        // FIXED: Update budgets ONLY after confirmed fill
        const afterOpenRisk = await getOpenPortfolioRisk(env, userId);
        const riskConsumed = Math.max(0, afterOpenRisk - beforeOpenRisk);
        D_left -= riskConsumed;

        const afterOpenNotional = await getOpenNotional(env, userId);
        const notionalConsumed = Math.max(0, afterOpenNotional - beforeOpenNotional);
        dailyNotionalLeft = Math.max(0, dailyNotionalCap - afterOpenNotional);

        ideaLog.info('📊 Budgets updated', {
          risk_consumed: riskConsumed.toFixed(2),
          D_left_remaining: D_left.toFixed(2),
          notional_consumed: notionalConsumed.toFixed(2),
          notional_left_remaining: dailyNotionalLeft.toFixed(2)
        });

      } else if (session.bot_mode === 'manual') {
        ideaLog.debug('Sending manual approval card');
        
        const trow = await env.DB.prepare(
          "SELECT * FROM trades WHERE id = ? AND user_id = ?"
        ).bind(id, userId).first();
        const extra = JSON.parse(trow.extra_json || '{}');
        
        await sendMessage(userId, pendingTradeCard(trow, extra), kbPendingTrade(id, extra.funds_ok), env);

        // FIXED: Don't decrement budgets in manual mode until approved
        counts.set(symU, (counts.get(symU) || 0) + 1);
        placed++;

        ideaLog.info('📧 Manual approval card sent', { 
          tradeId: id 
        });

        if (placed >= maxNew) {
          ideaLog.info('🛑 Cycle cap reached (manual)', { 
            placed, 
            maxNew 
          });
          
          if (String(env?.NO_REJECTS || '0') !== '1') {
            for (const rest of ideas.ideas.slice(idx + 1)) {
              await createAutoSkipRecord(env, userId, rest, protocol, 'cycle_cap_reached', { 
                placed, 
                maxNew 
              });
            }
          }
          break;
        }
      }

      // Budget exhaustion mid-loop
      if ((D_left <= 0 || dailyNotionalLeft <= 0) && String(env?.NO_REJECTS || '0') !== '1') {
        const reason = D_left <= 0 ? 'daily_risk_budget_exhausted' : 'daily_notional_exhausted';
        ideaLog.warn('⚠️ Budget exhausted mid-loop', { reason });
        
        for (const rest of ideas.ideas.slice(idx + 1)) {
          await createAutoSkipRecord(env, userId, rest, protocol, reason, { 
            D_left, 
            dailyNotionalLeft 
          });
        }
        break;
      }
    }

    // Mark snapshot as processed
    if (ideasTs) {
      await kvSet(env, lastKey, ideasTs);
      log.info('✅ Snapshot marked as processed', { 
        snapshot_ts: ideasTs 
      });
    }

    const totalDuration = cycleTimer.end({ 
      success: true,
      ideas_processed: ideas.ideas.length,
      trades_placed: placed
    });

    log.info('🎉 User processing cycle completed', {
      total_duration_ms: totalDuration.toFixed(2),
      ideas_total: ideas.ideas.length,
      trades_placed: placed,
      budgets_remaining: {
        D_left: D_left.toFixed(2),
        notional_left: dailyNotionalLeft.toFixed(2)
      }
    });

  } catch (e) {
    cycleTimer.end({ error: e.message });
    log.error('❌ processUser failed', { 
      error: e.message, 
      stack: e.stack 
    });
    await logEvent(env, userId, 'error', { 
      where: 'processUser', 
      message: e.message 
    });
  }
}


/* ---------- Telegram update handler (debounced exits + working orders + reconciliation) ---------- */
async function handleTelegramUpdate(update, env, ctx) {
  const isCb = !!update.callback_query;
  const msg = isCb ? update.callback_query.message : update.message;
  if (!msg) return;

  const userId = msg.chat.id;
  const text = isCb ? update.callback_query.data : (update.message.text || "");
  const messageId = isCb ? update.callback_query.message.message_id : (update.message?.message_id || 0);
  if (isCb) ctx.waitUntil(answerCallbackQuery(env, update.callback_query.id));

  // Debounced checks: reconciliation + working orders + exits
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
        ctx.waitUntil(reconcileExchangeFills(env, userId)); // optional (no-op if disabled)
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
  if (text === "action_refresh_balance") {
    await renderProtocolStatus(env, userId, isCb ? messageId : 0);
    return;
  }

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
    const cidsForPrune = [];
    // Cancel live pending entry orders
    try {
      const session2 = await getSession(env, userId);
      const ex2 = SUPPORTED_EXCHANGES[session2.exchange_name];
      if (ex2 && session2?.api_key_encrypted && session2?.api_secret_encrypted) {
        const apiKey = await decrypt(session2.api_key_encrypted, env);
        const apiSecret = await decrypt(session2.api_secret_encrypted, env);
        const pendLive = await env.DB.prepare(
          "SELECT id, symbol, extra_json FROM trades WHERE user_id = ? AND status = 'pending' AND json_extract(extra_json,'$.open_order.id') IS NOT NULL"
        ).bind(userId).all();
        for (const r of (pendLive.results || [])) {
          try {
            const exj = JSON.parse(r.extra_json || '{}');
            if (exj?.client_order_id) cidsForPrune.push(exj.client_order_id);
            const oid = exj?.open_order?.id;
            if (!oid) continue;
            if (ex2.kind === 'binanceLike')             await cancelBinanceLikeOrder(ex2, apiKey, apiSecret, r.symbol, { orderId: oid });
            else if (ex2.kind === 'binanceFuturesUSDT') await cancelBinanceFuturesOrder(ex2, apiKey, apiSecret, r.symbol, { orderId: oid });
            else if (ex2.kind === 'bybitFuturesV5')     await cancelBybitFuturesOrder(ex2, apiKey, apiSecret, r.symbol, { orderId: oid });
            // mark row rejected
            await env.DB.prepare("UPDATE trades SET status='rejected', close_type='MANUAL_CANCEL', updated_at=? WHERE id=?")
              .bind(nowISO(), r.id).run();
          } catch(_) {}
        }
      }
    } catch(_) {}
    
    if (cidsForPrune.length) {
      await gistPatchState(env, (state) => {
        const arr = Array.isArray(state?.pending) ? state.pending : [];
        state.pending = arr.filter(p => !cidsForPrune.includes(p?.client_order_id));
        state.lastReconcileTs = Date.now();
        return state;
      }).catch(()=>{});
    }
  
    // Close open positions
    const openTrades = await env.DB.prepare("SELECT id FROM trades WHERE user_id = ? AND status = 'open'").bind(userId).all();
    let closed = 0;
    for (const t of (openTrades.results || [])) { try { const res = await closeTradeNow(env, userId, t.id); if (res.ok) closed++; } catch(_){} }
    const t = `All positions closed (${closed}).`;
    if (isCb && messageId) await editMessage(userId, messageId, t, null, env); else await sendMessage(userId, t, null, env);
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
    // Cancel live pending entry orders first
    const session2 = await getSession(env, userId);
    const ex2 = SUPPORTED_EXCHANGES[session2.exchange_name];
    if (ex2 && session2?.api_key_encrypted && session2?.api_secret_encrypted) {
      const apiKey = await decrypt(session2.api_key_encrypted, env);
      const apiSecret = await decrypt(session2.api_secret_encrypted, env);
      const pendLive = await env.DB.prepare(
        "SELECT id, symbol, extra_json FROM trades WHERE user_id = ? AND status = 'pending' AND json_extract(extra_json,'$.open_order.id') IS NOT NULL"
      ).bind(userId).all();
      for (const r of (pendLive.results || [])) {
        try {
          const exj = JSON.parse(r.extra_json || '{}');
          const oid = exj?.open_order?.id;
          if (!oid) continue;
          if (ex2.kind === 'binanceLike')             await cancelBinanceLikeOrder(ex2, apiKey, apiSecret, r.symbol, { orderId: oid });
          else if (ex2.kind === 'binanceFuturesUSDT') await cancelBinanceFuturesOrder(ex2, apiKey, apiSecret, r.symbol, { orderId: oid });
          else if (ex2.kind === 'bybitFuturesV5')     await cancelBybitFuturesOrder(ex2, apiKey, apiSecret, r.symbol, { orderId: oid });
        } catch(_) {}
      }
    }

    const pendRows = await env.DB.prepare(
      "SELECT extra_json FROM trades WHERE user_id = ? AND status = 'pending'"
    ).bind(userId).all();
    const cids = [];
    for (const r of (pendRows.results || [])) {
      try { const exj = JSON.parse(r.extra_json || '{}'); if (exj?.client_order_id) cids.push(exj.client_order_id); } catch {}
    }
    if (cids.length) {
      await gistPatchState(env, (state) => {
        const arr = Array.isArray(state?.pending) ? state.pending : [];
        state.pending = arr.filter(p => !cids.includes(p?.client_order_id));
        state.lastReconcileTs = Date.now();
        return state;
      }).catch(()=>{});
    }
  
    // Remove pending/rejected and resync pacing counters
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
      // Cancel live pending entry orders first
      const session2 = await getSession(env, userId);
      const ex2 = SUPPORTED_EXCHANGES[session2.exchange_name];
      if (ex2 && session2?.api_key_encrypted && session2?.api_secret_encrypted) {
        const apiKey = await decrypt(session2.api_key_encrypted, env);
        const apiSecret = await decrypt(session2.api_secret_encrypted, env);
        const pendLive = await env.DB.prepare(
          "SELECT id, symbol, extra_json FROM trades WHERE user_id = ? AND status = 'pending' AND json_extract(extra_json,'$.open_order.id') IS NOT NULL"
        ).bind(userId).all();
        for (const r of (pendLive.results || [])) {
          try {
            const exj = JSON.parse(r.extra_json || '{}');
            const oid = exj?.open_order?.id;
            if (!oid) continue;
            if (ex2.kind === 'binanceLike')             await cancelBinanceLikeOrder(ex2, apiKey, apiSecret, r.symbol, { orderId: oid });
            else if (ex2.kind === 'binanceFuturesUSDT') await cancelBinanceFuturesOrder(ex2, apiKey, apiSecret, r.symbol, { orderId: oid });
            else if (ex2.kind === 'bybitFuturesV5')     await cancelBybitFuturesOrder(ex2, apiKey, apiSecret, r.symbol, { orderId: oid });
          } catch(_) {}
        }
      }
      
      const pendRows = await env.DB.prepare(
        "SELECT extra_json FROM trades WHERE user_id = ? AND status = 'pending'"
      ).bind(userId).all();
      const cids = [];
      for (const r of (pendRows.results || [])) {
        try { const exj = JSON.parse(r.extra_json || '{}'); if (exj?.client_order_id) cids.push(exj.client_order_id); } catch {}
      }
      if (cids.length) {
        await gistPatchState(env, (state) => {
          const arr = Array.isArray(state?.pending) ? state.pending : [];
          state.pending = arr.filter(p => !cids.includes(p?.client_order_id));
          state.lastReconcileTs = Date.now();
          return state;
        }).catch(()=>{});
      }

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
    const row = await env.DB.prepare(
      "SELECT * FROM trades WHERE user_id = ? AND status = 'pending' ORDER BY created_at DESC LIMIT 1"
    ).bind(userId).first();
    const tradeId = id || row?.id;
    if (!tradeId) { await sendMessage(userId, "No pending trade to reject.", null, env); return; }

    let extra = {};
    try { extra = JSON.parse(row.extra_json || '{}'); } catch {}
    const oo = extra?.open_order;

    if (oo?.id) {
      try {
        const session2 = await getSession(env, userId);
        const ex2 = SUPPORTED_EXCHANGES[session2.exchange_name];
        const apiKey = await decrypt(session2.api_key_encrypted, env);
        const apiSecret = await decrypt(session2.api_secret_encrypted, env);
        if (ex2.kind === 'binanceLike') await cancelBinanceLikeOrder(ex2, apiKey, apiSecret, row.symbol, { orderId: oo.id });
        else if (ex2.kind === 'binanceFuturesUSDT') await cancelBinanceFuturesOrder(ex2, apiKey, apiSecret, row.symbol, { orderId: oo.id });
        else if (ex2.kind === 'bybitFuturesV5') await cancelBybitFuturesOrder(ex2, apiKey, apiSecret, row.symbol, { orderId: oo.id });
      } catch (_) {}
    }

    await env.DB.prepare(
      "UPDATE trades SET status = 'rejected', close_type='MANUAL_CANCEL', updated_at = ? WHERE id = ? AND user_id = ?"
    ).bind(nowISO(), tradeId, userId).run();

    if (extra?.client_order_id) {
      await gistPatchState(env, (state) => {
        const idx = gistFindPendingIdxByCID(state, extra.client_order_id);
        if (idx >= 0) state.pending.splice(idx, 1);
        return state;
      }).catch(()=>{});
    }

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
    const detectedBal = (() => {
      const a = Number(res.data?.available);
      if (Number.isFinite(a) && a > 1e-6) return a; // prefer real available
      const e = Number(res.data?.equity);
      if (Number.isFinite(e)) return e; // fallback to equity (mirrors dashboard)
      const s = Number(String(res.data?.balance || "0").replace(/[^\d.]/g, ""));
      return Number.isFinite(s) ? s : 0; // legacy fallback
    })();
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

  // Detect refresh
  if (text === "action_refresh_detect") {
    const s = await getSession(env, userId);
    if (s?.api_key_encrypted && s?.api_secret_encrypted && s?.exchange_name) {
      try {
        const apiKey = await decrypt(s.api_key_encrypted, env);
        const apiSecret = await decrypt(s.api_secret_encrypted, env);
        const res = await verifyApiKeys(apiKey, apiSecret, s.exchange_name);
        if (res.success) {
          const detectedBal = (() => {
            const a = Number(res.data?.available);
            if (Number.isFinite(a) && a > 1e-6) return a; // prefer real available
            const e = Number(res.data?.equity);
            if (Number.isFinite(e)) return e; // fallback to equity (mirrors dashboard)
            const s = Number(String(res.data?.balance || "0").replace(/[^\d.]/g, ""));
            return Number.isFinite(s) ? s : 0; // legacy fallback
          })();
          const feeRate = res.data?.feeRate ?? 0.001;
          await saveSession(env, userId, { pending_action_data: JSON.stringify({ bal: detectedBal, feeRate }) });
        }
      } catch (_) {}
    }
    // fall-through to renderer
  }

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
              const detectedBal = (() => {
                const a = Number(result.data?.available);
                if (Number.isFinite(a) && a > 1e-6) return a; // prefer real available
                const e = Number(result.data?.equity);
                if (Number.isFinite(e)) return e; // fallback to equity (mirrors dashboard)
                const s = Number(String(result.data?.balance || "0").replace(/[^\d.]/g, ""));
                return Number.isFinite(s) ? s : 0; // legacy fallback
              })();
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
      session = await getSession(env, userId); // refresh session to get latest pending data
      const { bal, feeRate } = JSON.parse(updates.pending_action_data || session.pending_action_data || '{}');
      outText = confirmManualTextB(bal ?? 0, feeRate ?? 0.001);
      buttons = [[{ text: "Start Manual ▶️", callback_data: "action_start_manual" }], [{ text: "Refresh 🔄", callback_data: "action_refresh_detect" }], [{ text: "Wipe API Key & Go Back", callback_data: "action_wipe_keys" }]];
      break;
    }
    case "confirm_auto_funds": {
      session = await getSession(env, userId); // refresh session to get latest pending data
      const { bal, feeRate } = JSON.parse(updates.pending_action_data || session.pending_action_data || '{}');
      outText = confirmAutoTextB(bal ?? 0, feeRate ?? 0.001);
      buttons = [[{ text: "Yes, Start Auto 🚀", callback_data: "action_start_auto" }], [{ text: "Refresh 🔄", callback_data: "action_refresh_detect" }], [{ text: "Wipe API Key & Go Back", callback_data: "action_wipe_keys" }]];
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
    // NEW: detect and handle gaps
    const now = Date.now();
    const lastRunRaw = await kvGet(env, CRON_LAST_RUN_KEY);
    const lastRun = Number(lastRunRaw || 0);
    const expected = cronIntervalMs(env);
    const gap = lastRun ? (now - lastRun) : 0;
    const missed = lastRun && gap > expected ? Math.floor(gap / expected) : 0;
    const maxCatchup = cronCatchupMax(env);
    const cyclesToRun = 1 + Math.min(missed, maxCatchup);

    // NEW: Optional alert on large gap
    if (gap > cronGapAlertMs(env) && env.ADMIN_TELEGRAM_CHAT_ID) {
      try {
        await sendMessage(env.ADMIN_TELEGRAM_CHAT_ID, `Cron gap detected: ${(gap/1000).toFixed(0)}s. Catch-up cycles=${cyclesToRun-1}`, null, env);
      } catch (_) {}
    }

    for (let pass = 0; pass < cyclesToRun; pass++) {
      const activeUsers = await env.DB
        .prepare("SELECT user_id FROM user_sessions WHERE status IN ('active','halted') AND bot_mode IN ('manual','auto')")
        .all();

      const base = String(env.SELF_BASE_URL || "").trim().replace(/\/+$/, "");
      const token = String(env.TASK_TOKEN || "").trim();
      const haveTaskEndpoint = !!base && !!token;
      const forceInline = String(env.DEBUG_FORCE_INLINE || "0") === "1";

      // Slightly smaller CPU budget on catch-up passes
      for (const u of (activeUsers.results || [])) {
        const uid = u.user_id;
        const budgetMs = Number(pass === 0 ? (env.USER_CPU_BUDGET_MS || 20000) : (env.USER_CPU_BUDGET_MS_CATCHUP || 12000));

        if (haveTaskEndpoint && !forceInline) {
          try {
            const url = `${base}/task/process-user?uid=${encodeURIComponent(uid)}`;
            const r = await safeFetch(url, {
              method: "POST",
              headers: { "Authorization": `Bearer ${token}` }
            }, 6000);
            if (r) await r.text();
            if (!r || !r.ok) {
              console.warn("[cron] fanout non-OK; fallback inline", { uid, status: r?.status });
              await processUser(env, uid, createCpuBudget(budgetMs));
            }
          } catch (e) {
            console.error("[cron] fanout error; fallback inline", { uid, err: e?.message || e });
            await processUser(env, uid, createCpuBudget(budgetMs));
          }
        } else {
          await processUser(env, uid, createCpuBudget(budgetMs));
        }
      }
    }

    // NEW: record last-run timestamp
    await kvSet(env, CRON_LAST_RUN_KEY, Date.now());
  } catch (e) {
    console.error("runCron error:", e);
  } finally {
    await env.DB.prepare("DELETE FROM kv_state WHERE key = ? AND value = ?")
      .bind(CRON_LOCK_KEY, lockValue).run();
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

      if (url.pathname === "/cron/trigger" && request.method === "POST") {
        const auth = request.headers.get("Authorization") || "";
        const token = String(env.CRON_TOKEN || env.TASK_TOKEN || "").trim();
        const good = token && auth === `Bearer ${token}`;
        if (!good) return new Response("Unauthorized", { status: 401 });

        try {
          await runCron(env);
          return new Response(JSON.stringify({ ok: true, ran: "cron" }), {
            headers: { "Content-Type": "application/json" }
          });
        } catch (e) {
          console.error("cron/trigger error:", e);
          return new Response(JSON.stringify({ ok: false, error: e.message || "fail" }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }

      return new Response("Not found", { status: 404 });
    } catch (e) {
      console.error("fetch error:", e);
      return new Response("Error", { status: 500 });
    }
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(runCron(env));  // ensures non-blocking execution
  },
};
