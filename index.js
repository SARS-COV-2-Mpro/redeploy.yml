// =========================================================================================
// BINANCE FUTURES EXECUTOR - ASYNCHRONOUS MAKER MODE
//
// MODE CONFIGURATION (MAKER):
// - Places LIMIT orders that sit on the order book to earn maker rebates.
// - Does not wait for fill; saves pending order to state.
// - MONITOR job checks for fills and sets TP/SL, or cancels if expired.
// - PUSHER_TPSL_MODE = "on" → Use pusher-provided TP/SL percentages
// - PUSHER_TPSL_MODE = "off" → Use worker env TP_PERCENT/SL_PERCENT
//
// MONITORING: Checks every minute for:
// - Pending maker orders → If filled, set TP/SL; if expired (5 mins), cancel.
// - Active positions → Closed positions (TP/SL hit or manual close)
// - Active positions → Missing TP/SL orders → Re-arms with original percentages
//
// LATEST UPDATE:
// - STRICT FLUSH MODE: Triple-pass verification + Critical Fix for Hedge Mode closing.
// - CLEAR PHASE SEPARATION: Phase 1 (Close) must complete before Phase 2 (Open).
// =========================================================================================

const INTER_ORDER_SLEEP = 200;
const INTER_TPSL_SLEEP = 250;
const MAX_EXECUTION_TIME = 50000; // 50 seconds for Phase 1 (close) + Phase 2 (open)
const TPSL_SAFETY_BUFFER = 0.3;
const RECV_WINDOW = 60000;
const PLANNED_IDEA_TTL = 30 * 60 * 1000;
const MIN_REARM_INTERVAL = 3 * 60 * 1000; // 3 minutes between re-arm attempts

// === MAKER CHANGE START (Step 1: Add Constants) ===
const MAKER_OFFSET_BPS = 2; // 0.02% from market price
const MAKER_ORDER_TTL_MS = 5 * 60 * 1000; // 5 minutes before canceling unfilled orders
// === MAKER CHANGE END ===

let GIST_STATE_CACHE = null;
let GIST_STATE_DIRTY = false;
let TIME_OFFSET = 0;

export default {
    async fetch(request, env, ctx) {
        return await handleRequest(request, env, ctx);
    },
    async scheduled(event, env, ctx) {
        console.log('[scheduled] Monitor triggered at', new Date().toISOString());
        ctx.waitUntil(monitorPositions(env));
    }
};

// ---------------- Router ----------------
async function handleRequest(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === '/health') {
        return jsonResponse({
            status: 'ok',
            ts: Date.now(),
            tpsl_mode: env.PUSHER_TPSL_MODE || 'off',
            tp_percent: env.TP_PERCENT || '2.5',
            sl_percent: env.SL_PERCENT || '1.0'
        });
    }

    if (url.pathname === '/signals/push' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization') || '';
        const token = (env.PUSH_TOKEN || '').trim();
        const presented = authHeader.replace(/^Bearer\s+/i, '').trim();
        if (!token || presented !== token) return jsonResponse({ error: 'Unauthorized' }, 401);

        const payload = await request.json().catch(() => ({}));
        const ideas = Array.isArray(payload.ideas) ? payload.ideas : [];
        if (!ideas.length) {
            console.log('[push] empty ideas');
            return jsonResponse({ success: true, accepted: 0 }, 202);
        }

        console.log(`[push] accepted ${ideas.length} ideas (async)`);
        console.log(`[push] TP/SL MODE: ${env.PUSHER_TPSL_MODE || 'off'} | ENV TP=${env.TP_PERCENT || '2.5'}% SL=${env.SL_PERCENT || '1.0'}%`);

        ctx.waitUntil(
            Promise.race([
                processCycleWithTimeout(ideas, env),
                sleep(MAX_EXECUTION_TIME).then(() => {
                    console.error('[push] TIMEOUT - execution exceeded', MAX_EXECUTION_TIME, 'ms');
                })
            ])
        );

        return jsonResponse({ success: true, accepted: ideas.length }, 202);
    }

    if (url.pathname === '/monitor' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization') || '';
        const token = (env.PUSH_TOKEN || '').trim();
        const presented = authHeader.replace(/^Bearer\s+/i, '').trim();
        if (!token || presented !== token) return jsonResponse({ error: 'Unauthorized' }, 401);

        ctx.waitUntil(monitorPositions(env));
        return jsonResponse({ success: true, accepted: true }, 202);
    }

    return new Response('PS-Parity Worker - Async Maker Mode with Auto-Monitor', { status: 200 });
}

// ---------------- Time Sync ----------------
async function syncServerTime(env) {
    try {
        const serverTime = await binanceRequest('/fapi/v1/time', {}, false, 'GET', env);
        if (serverTime?.serverTime) {
            const localTime = Date.now();
            TIME_OFFSET = serverTime.serverTime - localTime;
            if (Math.abs(TIME_OFFSET) > 1000) {
                console.log('[sync] time offset detected:', TIME_OFFSET, 'ms');
            }
        }
    } catch (e) {
        console.error('[sync] failed to sync time', e?.message || e);
    }
}

function getTimestamp() {
    return Date.now() + TIME_OFFSET;
}

function shouldEnableCriticalAlerts(env) {
  const val = (env.ENABLE_CRITICAL_ALERTS || 'true').toLowerCase().trim();
  return val === 'true' || val === '1' || val === 'yes';
}

// ============================================================
// TRAILING STOP CONFIGURATION - NO DEFAULTS, STRICT ENV ONLY
// ============================================================
function resolveTrailingConfig(env) {
    const initial_sl = parseFloat(env.INITIAL_SL_PERCENT);
    const base_tp = parseFloat(env.BASE_TP_PERCENT);
    const minor_tp = parseFloat(env.MINOR_TP_PERCENT);
    const major_tp = parseFloat(env.MAJOR_TP_PERCENT);
    const trailing_offset = parseFloat(env.TRAILING_STOP_OFFSET_PERCENT);

    // STRICT VALIDATION - NO DEFAULTS ALLOWED
    if (!Number.isFinite(initial_sl) || initial_sl >= 0) {
        throw new Error('INITIAL_SL_PERCENT must be defined and negative (e.g., -0.6)');
    }
    if (!Number.isFinite(base_tp) || base_tp <= 0) {
        throw new Error('BASE_TP_PERCENT must be defined and positive (e.g., 1.0)');
    }
    if (!Number.isFinite(minor_tp) || minor_tp <= base_tp) {
        throw new Error('MINOR_TP_PERCENT must be defined and > BASE_TP_PERCENT (e.g., 1.5)');
    }
    if (!Number.isFinite(major_tp) || major_tp <= minor_tp) {
        throw new Error('MAJOR_TP_PERCENT must be defined and > MINOR_TP_PERCENT (e.g., 1.8)');
    }
    if (!Number.isFinite(trailing_offset) || trailing_offset <= 0) {
        throw new Error('TRAILING_STOP_OFFSET_PERCENT must be defined and positive (e.g., 0.2)');
    }

    console.log(`[trailing:config] ✓ LOADED FROM ENV:`);
    console.log(`[trailing:config]   INITIAL_SL: ${initial_sl}%`);
    console.log(`[trailing:config]   BASE_TP: ${base_tp}%`);
    console.log(`[trailing:config]   MINOR_TP: ${minor_tp}%`);
    console.log(`[trailing:config]   MAJOR_TP: ${major_tp}%`);
    console.log(`[trailing:config]   OFFSET: ${trailing_offset}%`);

    return {
        initial_sl_percent: initial_sl,
        base_tp_percent: base_tp,
        minor_tp_percent: minor_tp,
        major_tp_percent: major_tp,
        trailing_offset_percent: trailing_offset
    };
}


// ============================================================
// CALCULATE ALL TRAILING LEVELS FROM ENTRY PRICE
// ============================================================
function calculateTrailingLevels(entryPrice, side, market, trailing) {
    const isLong = (side === 'Buy' || side === 'long');
    
    console.log(`[trailing:calc] Entry=${entryPrice} Side=${side}`);
    console.log(`[trailing:calc] Config: SL=${trailing.initial_sl_percent}% Base=${trailing.base_tp_percent}% Minor=${trailing.minor_tp_percent}% Major=${trailing.major_tp_percent}%`);

    let initial_sl_price, major_tp_price;

    if (isLong) {
        // LONG: SL below entry, TP above entry
        initial_sl_price = entryPrice * (1 + trailing.initial_sl_percent / 100);
        major_tp_price = entryPrice * (1 + trailing.major_tp_percent / 100);
        
        initial_sl_price = floorToTick(initial_sl_price, market.tickSize);
        major_tp_price = ceilToTick(major_tp_price, market.tickSize);
    } else {
        // SHORT: SL above entry, TP below entry
        initial_sl_price = entryPrice * (1 - trailing.initial_sl_percent / 100);
        major_tp_price = entryPrice * (1 - trailing.major_tp_percent / 100);
        
        initial_sl_price = ceilToTick(initial_sl_price, market.tickSize);
        major_tp_price = floorToTick(major_tp_price, market.tickSize);
    }

    console.log(`[trailing:calc] ✓ INITIAL_SL=${initial_sl_price} MAJOR_TP=${major_tp_price}`);

    return {
        initial_sl_price,
        major_tp_price,
        // Virtual thresholds (not placed as orders, used for trailing logic)
        base_tp_threshold_percent: trailing.base_tp_percent,
        minor_tp_threshold_percent: trailing.minor_tp_percent
    };
}

// ============================================================
// CHECK IF POSITION SHOULD TRAIL STOP LOSS
// ============================================================
function shouldTrailStopLoss(position, currentPnL, env) {
    const trailing = position.trailing;
    if (!trailing) return { should: false, reason: 'no_trailing_config' };

    // Already at final stage
    if (trailing.minor_tp_hit) {
        return { should: false, reason: 'already_at_final_stage' };
    }

    // ✅ FIX: Check HIGHEST milestone first to prevent skipping
    
    // Check MINOR_TP milestone FIRST
    if (!trailing.minor_tp_hit && currentPnL >= trailing.minor_tp_percent) {
        console.log(`[trail:check] PnL ${currentPnL.toFixed(2)}% >= MINOR ${trailing.minor_tp_percent}%`);
        return { 
            should: true, 
            reason: 'minor_tp_reached', 
            new_stage: 'minor_hit',
            new_sl_percent: trailing.minor_tp_percent - trailing.trailing_offset_percent
        };
    }

    // Check BASE_TP milestone (only if minor didn't trigger)
    if (!trailing.base_tp_hit && currentPnL >= trailing.base_tp_percent) {
        console.log(`[trail:check] PnL ${currentPnL.toFixed(2)}% >= BASE ${trailing.base_tp_percent}%`);
        return { 
            should: true, 
            reason: 'base_tp_reached', 
            new_stage: 'base_hit',
            new_sl_percent: trailing.base_tp_percent - trailing.trailing_offset_percent
        };
    }

    return { should: false, reason: 'no_milestone_reached' };
}

// ============================================================
// TRAIL STOP LOSS TO NEW LEVEL
// ============================================================
async function trailStopLoss(symbol, position, trailAction, env) {
    console.log(`[trail] ========================================`);
    console.log(`[trail] ${symbol} TRAILING STOP LOSS`);
    console.log(`[trail] Side: ${position.side_exchange}`);
    console.log(`[trail] Reason: ${trailAction.reason}`);
    console.log(`[trail] New Stage: ${trailAction.new_stage}`);
    console.log(`[trail] New SL%: ${trailAction.new_sl_percent}%`);

    try {
        const market = await getMarketInfoCached(symbol, env);
        const livePos = await getPositionInfoWithRetry(symbol, env, position.side_exchange);
        
        // ✅ ADD: Verify we got the correct side
        if (!livePos || livePos.size === 0) {
            console.warn(`[trail] ${symbol} position not found, skipping trail`);
            return { success: false, reason: 'position_not_found' };
        }
        
        if (livePos.side !== position.side_exchange) {
            console.error(`[trail] ${symbol} SIDE MISMATCH - expected ${position.side_exchange}, got ${livePos.side}`);
            return { success: false, reason: 'side_mismatch' };
        }

        const isLong = (position.side_exchange === 'Buy');
        const entryPrice = position.entry_price;

        // Calculate new SL price
        let newSlPrice;
        if (isLong) {
            newSlPrice = entryPrice * (1 + trailAction.new_sl_percent / 100);
            newSlPrice = floorToTick(newSlPrice, market.tickSize);
        } else {
            newSlPrice = entryPrice * (1 - trailAction.new_sl_percent / 100);
            newSlPrice = ceilToTick(newSlPrice, market.tickSize);
        }

        const newSlFmt = formatPx(newSlPrice, market.priceDecimals);

        console.log(`[trail] Entry=${entryPrice} NewSL=${newSlFmt} (from ${trailAction.new_sl_percent}%)`);

        // ✅ STEP 12: Cancel SL orders for THIS side only
        const posSide = isLong ? 'LONG' : 'SHORT';

        try {
            const openOrders = await binanceRequest('/fapi/v1/openOrders', { symbol }, true, 'GET', env);
            await sleep(150);
            
            const slOrders = openOrders.filter(o => 
                (o.type === 'STOP_MARKET' || o.type === 'STOP' || o.type === 'STOP_LOSS') &&
                o.positionSide === posSide
            );

            for (const order of slOrders) {
                await binanceRequest('/fapi/v1/order', { 
                    symbol, 
                    orderId: order.orderId 
                }, true, 'DELETE', env);
                console.log(`[trail] ✓ Canceled old SL orderId=${order.orderId}`);
                await sleep(100);
            }
        } catch (e) {
            console.warn(`[trail] Cancel SL warning:`, e?.message || '');
        }

        await sleep(200);

        // Place new SL
        const orderSide = isLong ? 'SELL' : 'BUY';
        
        await binanceRequest('/fapi/v1/order', {
            symbol,
            side: orderSide,
            type: 'STOP_MARKET',
            stopPrice: newSlFmt,
            closePosition: 'true',
            workingType: 'MARK_PRICE',
            positionSide: posSide
        }, true, 'POST', env);

        console.log(`[trail] ✓✓✓ NEW SL PLACED at ${newSlFmt}`);
        console.log(`[trail] ========================================`);

        return { 
            success: true, 
            new_sl_price: newSlPrice,
            new_stage: trailAction.new_stage
        };

    } catch (e) {
        console.error(`[trail] ✗ ERROR:`, e?.message || e);
        return { success: false, reason: e?.message || 'unknown_error' };
    }
}

// ============================================================
// EMERGENCY GUARD: CLOSE UNPROTECTED POSITIONS BEYOND THRESHOLDS
// ============================================================
async function checkUnprotectedPositionEmergency(position, livePosition, env) {
    const symbol = position.symbol;
    
    try {
        // Step 1: Verify protection exists
        const openOrders = await binanceRequest('/fapi/v1/openOrders', { symbol }, true, 'GET', env);
        await sleep(150);

        // ✅ STEP 14: Verify we're checking the correct side
        const expectedSide = (position.side_exchange === 'Buy') ? 'Buy' : 'Sell';
        if (livePosition.side !== expectedSide) {
          console.warn(`[emergency:guard] ${symbol} SIDE MISMATCH - state=${expectedSide} exchange=${livePosition.side}`);
          return { action: 'none', reason: 'side_mismatch' };
        }

        const hasTP = openOrders.some(o => 
            o.type === 'TAKE_PROFIT_MARKET' || 
            o.type === 'TAKE_PROFIT'
        );
        const hasSL = openOrders.some(o => 
            o.type === 'STOP_MARKET' || 
            o.type === 'STOP' || 
            o.type === 'STOP_LOSS'
        );

        // Protected → skip
        if (hasTP && hasSL) {
            return { action: 'none', reason: 'fully_protected' };
        }

        console.warn(`[emergency:guard] ${symbol} UNPROTECTED - TP=${hasTP} SL=${hasSL}`);

        // ✅ FIX: IMMEDIATE RE-ARM (don't wait for later logic)
        try {
            console.log(`[emergency:guard] ${symbol} RE-ARMING IMMEDIATELY`);
            
            const market = await getMarketInfoCached(symbol, env);
            const isLong = (position.side_exchange === 'Buy');
            const posSide = isLong ? 'LONG' : 'SHORT';
            
            // Cancel any partial orders for this side
            await cancelSideOrders(symbol, posSide, env);
            await sleep(200);
            
            // Re-arm TP/SL immediately
            const tpslResult = await setTPSL({
                symbol,
                side: livePosition.side,
                market,
                position,
                currentPrice: livePosition.markPrice,
                env
            });
            
            if (tpslResult.tp_set && tpslResult.sl_set) {
                console.log(`[emergency:guard] ✓ ${symbol} RE-ARMED successfully`);
                
                // Update state
                const cached = await getGistStateCache(env);
                const idx = cached.pending.findIndex(x => 
                    x.symbol === symbol && 
                    x.status === 'active' &&
                    x.side_exchange === position.side_exchange
                );
                if (idx >= 0) {
                    cached.pending[idx].tp_set = true;
                    cached.pending[idx].sl_set = true;
                    cached.pending[idx].tpsl_armed = true;
                    cached.pending[idx].tpsl_last_attempt = Date.now();
                    GIST_STATE_DIRTY = true;
                }
            }
        } catch (e) {
            console.error(`[emergency:guard] ${symbol} RE-ARM FAILED:`, e?.message || e);
        }

        // Step 2: NOW check thresholds (position is protected or we tried)
        const trailing = position.trailing;
        if (!trailing) {
            console.warn(`[emergency:guard] ${symbol} no trailing config, cannot calculate thresholds`);
            return { action: 'none', reason: 'no_trailing_config' };
        }

        const entryPrice = position.entry_price;
        const currentPrice = livePosition.markPrice;
        const isLong = (position.side_exchange === 'Buy');

        let slThreshold, tpThreshold;

        if (isLong) {
            slThreshold = entryPrice * (1 + trailing.initial_sl_percent / 100);
            tpThreshold = entryPrice * (1 + trailing.major_tp_percent / 100);
        } else {
            slThreshold = entryPrice * (1 - trailing.initial_sl_percent / 100);
            tpThreshold = entryPrice * (1 - trailing.major_tp_percent / 100);
        }

        console.log(`[emergency:guard] ${symbol} Entry=${entryPrice} Current=${currentPrice}`);
        console.log(`[emergency:guard] ${symbol} SL_Threshold=${slThreshold} TP_Threshold=${tpThreshold}`);

        // Step 3: Check if price is beyond safe zone
        let beyondThreshold = false;
        let breachType = '';

        if (isLong) {
            if (currentPrice <= slThreshold) {
                beyondThreshold = true;
                breachType = 'below_sl_threshold';
            } else if (currentPrice >= tpThreshold) {
                beyondThreshold = true;
                breachType = 'above_tp_threshold';
            }
        } else {
            if (currentPrice >= slThreshold) {
                beyondThreshold = true;
                breachType = 'above_sl_threshold';
            } else if (currentPrice <= tpThreshold) {
                beyondThreshold = true;
                breachType = 'below_tp_threshold';
            }
        }

        // Step 4: EMERGENCY CLOSE if unprotected + beyond threshold
        if (beyondThreshold) {
            console.error(`[emergency:guard] 🚨🚨🚨 ${symbol} UNPROTECTED + ${breachType.toUpperCase()}`);
            console.error(`[emergency:guard] 🚨 EMERGENCY CLOSING POSITION`);

            // ✅ STEP 18: Cancel only THIS side's orders (don't unprotect the other hedge)
            const posSide = isLong ? 'LONG' : 'SHORT';

            try {
                await cancelSideOrders(symbol, posSide, env);
                await sleep(200);
            } catch (e) {
                console.error(`[emergency:guard] Cancel error:`, e?.message || '');
            }

            // Close position
            const market = await getMarketInfoCached(symbol, env);
            const orderSide = isLong ? 'SELL' : 'BUY';
            
            const closeResult = await placeMarketOrderWithRetry(
                symbol, 
                orderSide, 
                livePosition.size, 
                market, 
                env, 
                true
            );

            if (closeResult.success) {
                console.log(`[emergency:guard] ✓ ${symbol} EMERGENCY CLOSED`);
                await logCriticalFailure(symbol, 'emergency_close_success', position, env);
                
                // Move to closed in state
                await closePositionCached(position, {
                    exit_reason: 'emergency_unprotected_threshold_breach',
                    exit_detail: breachType,
                    exit_price: currentPrice,
                    ts_exit_ms: Date.now(),
                    emergency_close: true
                }, env);

                return { 
                    action: 'emergency_closed', 
                    reason: breachType,
                    exit_price: currentPrice 
                };
            } else {
                console.error(`[emergency:guard] ✗ ${symbol} EMERGENCY CLOSE FAILED`);
                await logCriticalFailure(symbol, 'emergency_close_failed', position, env);
                return { action: 'close_failed', reason: 'market_order_rejected' };
            }
        }

        // Unprotected but within safe zone → just warn
        console.warn(`[emergency:guard] ⚠ ${symbol} unprotected but within safe zone (normal re-arm will handle)`);
        return { action: 'none', reason: 'unprotected_but_safe' };

    } catch (e) {
        console.error(`[emergency:guard] ${symbol} error:`, e?.message || e);
        return { action: 'error', reason: e?.message || 'unknown' };
    }
}

// ---------------- TWO-PHASE CYCLE ----------------
async function processCycleWithTimeout(ideas, env) {
    const startTime = Date.now();
    try {
        await syncServerTime(env);
        await processCycleTwoPhase(ideas, env, startTime);
    } catch (e) {
        console.error('[cycle] critical error', e?.message || e);
        try {
            await flushGistState(env);
        } catch (e2) {
            console.error('[cycle] failed to save state on error', e2?.message || e2);
        }
    }
}

async function processCycleTwoPhase(ideas, env, startTime) {
    const batch_id = `batch_${Date.now()}`;
    
    GIST_STATE_CACHE = null;
    GIST_STATE_DIRTY = false;

    const state = await loadGistState(env);
    
    // Enable hedge mode for dual-side positions
    await ensureHedgeMode(env);
    
    // ============================================================
    // PHASE 1: CLOSE ALL EXISTING POSITIONS & ORDERS
    // ============================================================
    console.log('');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('  PHASE 1: CLOSING ALL POSITIONS & ORDERS');
    console.log('  Batch ID:', batch_id);
    console.log('═══════════════════════════════════════════════════════════');
    console.log('');
    
    await closeAllPositionsAndOrders(state, env, startTime);
    
    console.log('');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('  ✅ PHASE 1 COMPLETE - PROCEEDING TO PHASE 2');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('');
    
    // ============================================================
    // PHASE 2: OPEN NEW POSITIONS
    // ============================================================
    console.log('');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('  PHASE 2: OPENING NEW POSITIONS');
    console.log('  Ideas Received:', ideas.length);
    console.log('  TP/SL MODE:', env.PUSHER_TPSL_MODE || 'off');
    console.log('  Worker ENV: TP=' + (env.TP_PERCENT || '2.5') + '% SL=' + (env.SL_PERCENT || '1.0') + '%');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('');

    const ledger = {
        batch_id,
        received: ideas.map(i => i.idea_id || i.client_order_id || 'unknown'),
        executed: [],
        rejected: [],
        failed: [],
        tpsl_status: []
    };

    const availableBalance = await getAvailableBalanceWithRetry(env);
    const capPct = toNum(env.CAPITAL_PERCENTAGE, 0.10);

    console.log('[PHASE2:FILL] ========================================');
    console.log('[PHASE2:FILL] Available Balance:', availableBalance.toFixed(2), 'USDT');
    console.log('[PHASE2:FILL] ========================================');

    // ============================================================
    // STEP 1: FILL ALL POSITIONS
    // ============================================================
    const filledPositions = [];

    for (let i = 0; i < ideas.length; i++) {
        if (Date.now() - startTime > MAX_EXECUTION_TIME - 10000) {
            console.warn('[PHASE2:FILL] timeout approaching, stopping at', i, 'of', ideas.length);
            break;
        }

        const idea = ideas[i];
        try {
            const result = await fillPosition(idea, availableBalance, capPct, env, batch_id);

            if (result.success) {
                filledPositions.push(result.position);
                ledger.executed.push(idea.idea_id || idea.client_order_id || 'unknown');
                
                const pos = result.position;
                
                if (pos.status === 'pending_fill') {
                    console.log(`[PHASE2:FILL] ✓ MAKER ORDER PLACED ${pos.symbol} limit @ ${pos.limit_price} (orderId=${pos.order_id})`);
                } else if (pos.order_type === 'taker') {
                    const verified = pos.position_verified ? '✓ verified' : '⚠ estimated';
                    console.log(`[PHASE2:FILL] ✓ TAKER FILLED ${pos.symbol} ${pos.side} @ ${pos.entry_price} (${verified})`);
                } else {
                    console.log(`[PHASE2:FILL] ✓ FILLED ${pos.symbol} ${pos.side} @ ${pos.entry_price}`);
                }
            } else {
                ledger.rejected.push({
                    id: idea.idea_id || idea.client_order_id || 'unknown',
                    reason: result.error || 'unknown'
                });
                console.warn('[PHASE2:FILL] ✗ REJECTED', idea.symbol, result.error);
            }
        } catch (e) {
            console.error('[PHASE2:FILL] ✗ ERROR', idea.symbol, e?.message || e);
            ledger.failed.push({
                id: idea.idea_id || idea.client_order_id || 'unknown',
                reason: e?.message || String(e)
            });
        }

        if (i < ideas.length - 1) {
            await sleep(INTER_ORDER_SLEEP);
        }
    }

    console.log('[PHASE2:FILL] ========================================');
    console.log('[PHASE2:FILL] COMPLETE - filled/placed:', filledPositions.length);
    console.log('[PHASE2:FILL] ========================================');

    // ============================================================
    // STEP 2: SET ALL TP/SL (BATCH) - SKIP PENDING MAKER ORDERS
    // ============================================================
    const actuallyFilledPositions = filledPositions.filter(p => p.status !== 'pending_fill');
    const pendingMakerOrders = filledPositions.filter(p => p.status === 'pending_fill');

    console.log('[PHASE2:TPSL] ========================================');
    console.log('[PHASE2:TPSL] Setting TP/SL - filled:', actuallyFilledPositions.length, 'pending maker:', pendingMakerOrders.length);
    console.log('[PHASE2:TPSL] ========================================');

    if (pendingMakerOrders.length > 0) {
        console.log('[PHASE2:TPSL] Pending maker orders will be checked by monitor job');
        for (const p of pendingMakerOrders) {
            console.log(`[PHASE2:TPSL] - ${p.symbol} orderId=${p.order_id} limitPrice=${p.limit_price}`);
        }
    }

    for (let i = 0; i < actuallyFilledPositions.length; i++) {
        if (Date.now() - startTime > MAX_EXECUTION_TIME - 3000) {
            console.warn('[PHASE2:TPSL] timeout approaching, stopping TP/SL at', i, 'of', actuallyFilledPositions.length);
            break;
        }

        const pos = actuallyFilledPositions[i];
        // === MAKER CHANGE END ===
        try {
            const market = await getMarketInfoCached(pos.symbol, env);
            const livePos = await getPositionInfoWithRetry(pos.symbol, env);
            const currentPrice = livePos?.markPrice || pos.entry_price;

            console.log(`[PHASE2:TPSL] ${pos.symbol} entry=${pos.entry_price} current=${currentPrice}`);

            const tpslResult = await setTPSL({
                symbol: pos.symbol,
                side: pos.side_exchange,
                market,
                position: pos,  // Pass entire position object (contains trailing config)
                currentPrice,
                env
            });

            pos.tp_set = tpslResult.tp_set;
            pos.sl_set = tpslResult.sl_set;
            pos.tpsl_armed = tpslResult.tp_set && tpslResult.sl_set;
            pos.tpsl_last_attempt = Date.now();
            pos.protection_status = pos.tpsl_armed ? 'verified' : 'partial';

            ledger.tpsl_status.push({
                symbol: pos.symbol,
                tp_set: tpslResult.tp_set,
                sl_set: tpslResult.sl_set,
                status: tpslResult.status
            });

            console.log(`[PHASE2:TPSL] ${pos.symbol} RESULT: TP=${tpslResult.tp_set ? '✓' : '✗'} SL=${tpslResult.sl_set ? '✓' : '✗'} armed=${pos.tpsl_armed}`);
        } catch (e) {
            console.error('[PHASE2:TPSL] ✗ ERROR', pos.symbol, e?.message || e);
            pos.tp_set = false;
            pos.sl_set = false;
            pos.tpsl_armed = false;
        }

        if (i < actuallyFilledPositions.length - 1) {
            await sleep(INTER_TPSL_SLEEP);
        }
    }

    console.log('[PHASE2:TPSL] ========================================');
    console.log('[PHASE2:TPSL] COMPLETE');
    console.log('[PHASE2:TPSL] ========================================');

    // ============================================================
    // SAVE ALL POSITIONS TO STATE & CLEANUP ZOMBIES
    // ============================================================
    try {
        const finalState = await getGistStateCache(env);

        // === MAKER CHANGE START (Step 7: Fix State Saving) ===
        for (const pos of filledPositions) {
            const idx = finalState.pending.findIndex(p =>
                (p.idea_id && p.idea_id === pos.idea_id) ||
                (p.client_order_id && p.client_order_id === pos.client_order_id)
            );

            // Preserve the position's status (pending_fill or active)
            if (idx >= 0) {
                finalState.pending[idx] = { ...finalState.pending[idx], ...pos };
            } else {
                finalState.pending.push(pos);
            }
        }
        // === MAKER CHANGE END ===

        const now = Date.now();
        const rejectedIds = new Set(ledger.rejected.map(r => r.id));
        const failedIds = new Set(ledger.failed.map(f => f.id));
        const beforeCleanup = finalState.pending.length;

        finalState.pending = finalState.pending.filter(p => {
            if (p.status === 'active' || p.status === 'pending_fill') return true;

            const pId = p.idea_id || p.client_order_id || '';
            if (pId && (rejectedIds.has(pId) || failedIds.has(pId))) {
                console.log('[cleanup] removing rejected idea:', pId, p.symbol || p.symbolFull || p.base);
                return false;
            }

            if (p.status === 'planned' || p.status === null || p.status === undefined) {
                const ideaAge = now - (p.ts_ms || 0);
                const ttlExpired = p.ttl_ts_ms && p.ttl_ts_ms < now;
                const tooOld = ideaAge > PLANNED_IDEA_TTL;

                if (ttlExpired || tooOld) {
                    console.log('[cleanup] removing expired planned idea:', pId, p.symbol || p.symbolFull || p.base,
                        'age:', Math.round(ideaAge / 1000), 's');
                    return false;
                }
            }

            return true;
        });

        const afterCleanup = finalState.pending.length;
        const cleaned = beforeCleanup - afterCleanup;

        if (cleaned > 0) {
            console.log('[cleanup] removed', cleaned, 'zombie ideas, pending:', beforeCleanup, '→', afterCleanup);
        }

        GIST_STATE_DIRTY = true;

        finalState.last_batch = {
            ts: new Date().toISOString(),
            ts_ms: Date.now(),
            execution_time_ms: Date.now() - startTime,
            zombies_cleaned: cleaned,
            ...ledger
        };

        await flushGistState(env);
    } catch (e) {
        console.error('[cycle] failed to save final state', e?.message || e);
    }

    const execTime = ((Date.now() - startTime) / 1000).toFixed(2);
    
    console.log('');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('  ✅✅✅ PHASE 2 COMPLETE - ALL DONE');
    console.log('  Batch ID:', batch_id);
    console.log('  Executed:', ledger.executed.length);
    console.log('  Rejected:', ledger.rejected.length);
    console.log('  Failed:', ledger.failed.length);
    console.log('  Total Time:', execTime, 's');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('');
}

// ---------------- PHASE 1: Fill Position (FAST) ----------------
async function fillPosition(idea, availableBalance, capPct, env, batch_id) {
    const base0 = idea.symbol || idea.symbol_full || idea.symbolFull || idea.base || '';
    const sideIn = (idea.side || '').toLowerCase();

    if (!base0 || !sideIn) {
        return { success: false, error: 'bad_idea_payload' };
    }

    const resolvedSymbol = await resolveSymbolSmart(base0, env);
    if (!resolvedSymbol) {
        console.warn('[fill] symbol_not_found', base0);
        return { success: false, error: 'symbol_not_found' };
    }

    const target = availableBalance * capPct;
    if (!(target > 5)) {
        return { success: false, error: 'insufficient_balance' };
    }

    const market = await getMarketInfoCached(resolvedSymbol, env);
    if (!market.valid) {
        return { success: false, error: 'market_info_failed' };
    }

    const lev = toInt(env.DEFAULT_LEVERAGE, 1);
    await setLeverage(resolvedSymbol, lev, env);

    const qtyRaw = target / market.price;
    const qtyRounded = floorToStep(qtyRaw, market.stepSize);
    const finalQty = Math.max(qtyRounded, market.minQty);

    const orderSide = sideIn === 'long' ? 'BUY' : 'SELL';

    const order = await placeMarketOrderWithRetry(resolvedSymbol, orderSide, finalQty, market, env, false);

    if (!order.success) {
        return { success: false, error: order.reason || 'order_failed' };
    }

    const trailingConfig = resolveTrailingConfig(env);

    // ========================================
    // TAKER MODE: Immediate Fill with Robust Position Check
    // ========================================
    if (order.taker_mode) {
        console.log(`[fill:taker] ${resolvedSymbol} verifying position...`);
        
        let position = null;
        let positionFound = false;
        
        // ✅ STRATEGY 1: Try to get position with retries (up to 5 attempts, 4 seconds total)
        for (let attempt = 0; attempt < 5; attempt++) {
            const waitTime = 300 + (attempt * 200);  // 300ms, 500ms, 700ms, 900ms, 1100ms
            await sleep(waitTime);
            
            position = await getPositionInfoWithRetry(resolvedSymbol, env);
            
            if (position && position.size > 0) {
                console.log(`[fill:taker] ✓ ${resolvedSymbol} position verified (attempt ${attempt + 1}, qty=${position.size})`);
                positionFound = true;
                break;
            }
            
            if (attempt < 4) {
                console.warn(`[fill:taker] position not found yet, retrying... (${attempt + 1}/5)`);
            }
        }
        
        // ✅ STRATEGY 2: If still not found, check all positions (maybe symbol mismatch)
        if (!positionFound) {
            console.warn(`[fill:taker] ${resolvedSymbol} position not found by symbol, checking all positions...`);
            
            const allPositions = await getAllOpenPositionsWithRetry(env);
            const matchingPos = allPositions.find(p => 
                p.symbol === resolvedSymbol || 
                p.symbol.replace(/USDT$/, '') === resolvedSymbol.replace(/USDT$/, '')
            );
            
            if (matchingPos) {
                console.log(`[fill:taker] ✓ ${resolvedSymbol} found in all positions list`);
                position = matchingPos;
                positionFound = true;
            }
        }
        
        // ✅ STRATEGY 3: Use fill price from order response if available
        let entryPrice = null;
        let actualQty = finalQty;
        
        if (positionFound && position) {
            entryPrice = position.avgPrice;
            actualQty = position.size;
        } else if (order.fill_price && order.fill_price > 0) {
            // Order response included fill price
            console.log(`[fill:taker] ✓ using fill price from order response: ${order.fill_price}`);
            entryPrice = order.fill_price;
            actualQty = order.fill_qty || finalQty;
            positionFound = true;  // Consider it found
        } else {
            // ⚠️ LAST RESORT: Use current market price
            console.error(`[fill:taker] ⚠️ ${resolvedSymbol} CRITICAL: Position not found after all attempts`);
            console.error(`[fill:taker] ⚠️ Order succeeded (orderId=${order.orderId}) but cannot verify fill`);
            console.error(`[fill:taker] ⚠️ Creating state entry with ESTIMATED price - MONITOR WILL VERIFY`);
            
            entryPrice = market.price;
            actualQty = finalQty;
        }
        
        return {
            success: true,
            position: {
                idea_id: idea.idea_id,
                client_order_id: idea.client_order_id,
                batch_id,
                symbol: resolvedSymbol,
                base: resolvedSymbol.replace(/(USDT|USDC|USD)$/, ''),
                quote: 'USDT',
                side: sideIn,
                side_exchange: orderSide === 'BUY' ? 'Buy' : 'Sell',
                positionSide: orderSide === 'BUY' ? 'LONG' : 'SHORT',  // ✅ STEP 19
                status: 'active',
                entry_price: entryPrice,
                qty: actualQty,
                order_id: order.orderId,
                
                trailing: {
                    initial_sl_percent: trailingConfig.initial_sl_percent,
                    base_tp_percent: trailingConfig.base_tp_percent,
                    minor_tp_percent: trailingConfig.minor_tp_percent,
                    major_tp_percent: trailingConfig.major_tp_percent,
                    trailing_offset_percent: trailingConfig.trailing_offset_percent,
                    base_tp_hit: false,
                    minor_tp_hit: false,
                    trailing_stage: 'initial'
                },
                
                p_raw: idea.p_raw ?? idea.p_win ?? null,
                p_lcb: idea.p_lcb ?? null,
                regime: idea.regime ?? null,
                calib_key: idea.calib_key ?? null,
                predicted: idea.predicted ?? null,
                ts_entry_ms: Date.now(),
                order_type: 'taker',
                
                // ✅ Metadata for verification
                position_verified: positionFound,
                entry_price_source: positionFound && position ? 'exchange_position' : 
                                   order.fill_price ? 'order_response' : 'market_estimate',
                needs_verification: !positionFound  // Monitor will check
            }
        };
    }

    // ========================================
    // MAKER MODE: Pending Fill
    // ========================================
    if (order.pending_fill) {
        console.log(`[fill:maker] ${resolvedSymbol} LIMIT order pending fill (orderId=${order.orderId})`);
    
        return {
            success: true,
            position: {
                idea_id: idea.idea_id,
                client_order_id: idea.client_order_id,
                batch_id,
                symbol: resolvedSymbol,
                base: resolvedSymbol.replace(/(USDT|USDC|USD)$/, ''),
                quote: 'USDT',
                side: sideIn,
                side_exchange: orderSide === 'BUY' ? 'Buy' : 'Sell',
                positionSide: orderSide === 'BUY' ? 'LONG' : 'SHORT',  // ✅ STEP 19
                status: 'pending_fill',
                order_id: order.orderId,
                limit_price: order.limit_price,
                qty: finalQty,
                ts_order_placed: Date.now(),
                
                trailing: {
                    initial_sl_percent: trailingConfig.initial_sl_percent,
                    base_tp_percent: trailingConfig.base_tp_percent,
                    minor_tp_percent: trailingConfig.minor_tp_percent,
                    major_tp_percent: trailingConfig.major_tp_percent,
                    trailing_offset_percent: trailingConfig.trailing_offset_percent,
                    base_tp_hit: false,
                    minor_tp_hit: false,
                    trailing_stage: 'initial'
                },
                
                p_raw: idea.p_raw ?? idea.p_win ?? null,
                p_lcb: idea.p_lcb ?? null,
                regime: idea.regime ?? null,
                calib_key: idea.calib_key ?? null,
                predicted: idea.predicted ?? null,
                order_type: 'maker'
            }
        };
    }

    // ========================================
    // FALLBACK: Should not reach here
    // ========================================
    console.warn(`[fill] ${resolvedSymbol} unexpected: order succeeded but no mode flag`);
    
    await sleep(800);
    // ✅ STEP 17E: Pass side to get correct position
    const position = await getPositionInfoWithRetry(resolvedSymbol, env, orderSide);

    const basePosition = {
        idea_id: idea.idea_id,
        client_order_id: idea.client_order_id,
        batch_id,
        symbol: resolvedSymbol,
        base: resolvedSymbol.replace(/(USDT|USDC|USD)$/, ''),
        quote: 'USDT',
        side: sideIn,
        side_exchange: orderSide === 'BUY' ? 'Buy' : 'Sell',
        positionSide: orderSide === 'BUY' ? 'LONG' : 'SHORT',  // ✅ STEP 19
        status: 'active',
        
        trailing: {
            initial_sl_percent: trailingConfig.initial_sl_percent,
            base_tp_percent: trailingConfig.base_tp_percent,
            minor_tp_percent: trailingConfig.minor_tp_percent,
            major_tp_percent: trailingConfig.major_tp_percent,
            trailing_offset_percent: trailingConfig.trailing_offset_percent,
            base_tp_hit: false,
            minor_tp_hit: false,
            trailing_stage: 'initial'
        },
        
        p_raw: idea.p_raw ?? idea.p_win ?? null,
        p_lcb: idea.p_lcb ?? null,
        regime: idea.regime ?? null,
        calib_key: idea.calib_key ?? null,
        predicted: idea.predicted ?? null,
        ts_entry_ms: Date.now()
    };

    if (!position || !position.size) {
        console.warn('[fill] fallback: position not found after order', resolvedSymbol);
        return {
            success: true,
            position: {
                ...basePosition,
                entry_price: market.price,
                qty: finalQty,
                needs_verification: true
            }
        };
    }

    return {
        success: true,
        position: {
            ...basePosition,
            entry_price: position.avgPrice,
            qty: position.size
        }
    };
}

// ---------------- STRICT FLUSH: CLOSE EVERYTHING ----------------
async function closeAllPositionsAndOrders(state, env, startTime) {
    console.log('[PHASE1:CLOSE] ========================================');
    console.log('[PHASE1:CLOSE] 🔒 CLOSING ALL POSITIONS & ORDERS');
    console.log('[PHASE1:CLOSE] ========================================');

    let flushAttempt = 0;
    const MAX_FLUSH_ATTEMPTS = 3;

    // ============================================================
    // TRIPLE-PASS LOOP: Keep trying until BOTH orders AND positions are zero
    // ============================================================
    while (flushAttempt < MAX_FLUSH_ATTEMPTS) {
        flushAttempt++;
        console.log(`[PHASE1:CLOSE] --- ATTEMPT ${flushAttempt}/${MAX_FLUSH_ATTEMPTS} ---`);

        // ============================================================
        // PASS 1: CANCEL ALL ORDERS (GLOBAL)
        // ============================================================
        try {
            console.log('[PHASE1:CLOSE] Fetching all open orders...');
            const allOpenOrders = await binanceRequest('/fapi/v1/openOrders', {}, true, 'GET', env);
            await sleep(200);

            if (allOpenOrders.length > 0) {
                console.log(`[PHASE1:CLOSE] 🧹 Found ${allOpenOrders.length} open orders`);
                
                // Group by symbol
                const ordersBySymbol = {};
                for (const order of allOpenOrders) {
                    if (!ordersBySymbol[order.symbol]) ordersBySymbol[order.symbol] = [];
                    ordersBySymbol[order.symbol].push(order);
                }

                // Cancel per symbol
                for (const symbol of Object.keys(ordersBySymbol)) {
                    const count = ordersBySymbol[symbol].length;
                    console.log(`[PHASE1:CLOSE] Canceling ${count} orders for ${symbol}`);
                    
                    try {
                        await binanceRequest('/fapi/v1/allOpenOrders', { symbol }, true, 'DELETE', env);
                        console.log(`[PHASE1:CLOSE] ✓ ${symbol} orders canceled`);
                    } catch (e) {
                        if (!e.message?.includes('-2011')) {
                            console.error(`[PHASE1:CLOSE] ✗ ${symbol} cancel error:`, e?.message || e);
                        }
                    }
                    await sleep(150);
                }
                
                console.log('[PHASE1:CLOSE] Waiting for cancellations to settle...');
                await sleep(2000);  // ✅ INCREASED to 2s to prevent race conditions
            } else {
                console.log('[PHASE1:CLOSE] ✓ No open orders found');
            }
        } catch (e) {
            console.error('[PHASE1:CLOSE] Error fetching orders:', e?.message || e);
        }

        // ============================================================
        // PASS 2: CLOSE ALL POSITIONS
        // ============================================================
        try {
            console.log('[PHASE1:CLOSE] Fetching all open positions...');
            const positions = await getAllOpenPositionsWithRetry(env);
            await sleep(200);

            if (positions.length > 0) {
                console.log(`[PHASE1:CLOSE] 🧹 Found ${positions.length} open positions`);

                for (let i = 0; i < positions.length; i++) {
                    if (Date.now() - startTime > MAX_EXECUTION_TIME - 10000) {
                        console.warn('[PHASE1:CLOSE] ⚠️ Timeout approaching, stopping loop');
                        break;
                    }

                    const pos = positions[i];
                    const symbol = pos.symbol;
                    const isLong = (pos.side === 'Buy');
                    const posSide = isLong ? 'LONG' : 'SHORT';
                    
                    console.log(`[PHASE1:CLOSE] Closing ${symbol} ${posSide} size=${pos.size}`);

                    try {
                        // Cancel any lingering orders for this side (safety)
                        await cancelSideOrders(symbol, posSide, env);
                        await sleep(150);

                        // Verify position still exists
                        const currentPos = await getPositionInfoWithRetry(symbol, env, pos.side);
                        
                        if (!currentPos || currentPos.size === 0) {
                            console.log(`[PHASE1:CLOSE] ${symbol} ${posSide} already closed`);
                            continue;
                        }

                        // Verify side matches
                        if (currentPos.side !== pos.side) {
                            console.error(`[PHASE1:CLOSE] ${symbol} SIDE MISMATCH - expected ${pos.side}, got ${currentPos.side}`);
                            continue;
                        }

                        // Close position
                        const sideClose = isLong ? 'SELL' : 'BUY';
                        const market = await getMarketInfoCached(symbol, env);

                        let closed = false;

                        // Attempt 1: reduceOnly=true
                        const result1 = await placeMarketOrderWithRetry(symbol, sideClose, currentPos.size, market, env, true);
                        if (result1.success) {
                            closed = true;
                            console.log(`[PHASE1:CLOSE] ✓ ${symbol} ${posSide} closed (reduceOnly)`);
                        } else {
                            await sleep(200);
                            // Attempt 2: normal market order
                            const result2 = await placeMarketOrderWithRetry(symbol, sideClose, currentPos.size, market, env, false);
                            if (result2.success) {
                                closed = true;
                                console.log(`[PHASE1:CLOSE] ✓ ${symbol} ${posSide} closed (market)`);
                            }
                        }

                        if (closed) {
                            // Mark as closed in state
                            await closePositionCached(symbol, {
                                exit_reason: 'flush',
                                exit_price: currentPos.markPrice || currentPos.avgPrice,
                                ts_exit_ms: Date.now(),
                                side: pos.side
                            }, env);
                        } else {
                            console.error(`[PHASE1:CLOSE] ✗ ${symbol} ${posSide} FAILED TO CLOSE`);
                        }

                    } catch (e) {
                        console.error(`[PHASE1:CLOSE] ${symbol} ${posSide} error:`, e?.message || e);
                    }

                    await sleep(250);
                }

                console.log('[PHASE1:CLOSE] Waiting for closes to settle...');
                await sleep(1500);
            } else {
                console.log('[PHASE1:CLOSE] ✓ No open positions found');
            }
        } catch (e) {
            console.error('[PHASE1:CLOSE] Error fetching positions:', e?.message || e);
        }

        // ============================================================
        // PASS 3: VERIFY EMPTY STATE
        // ============================================================
        console.log('[PHASE1:CLOSE] Final verification...');
        await sleep(1000); // Give exchange time to process

        let remainingOrders = 0;
        let remainingPositions = 0;

        try {
            const checkOrders = await binanceRequest('/fapi/v1/openOrders', {}, true, 'GET', env);
            remainingOrders = checkOrders.length;
            
            if (remainingOrders > 0) {
                console.warn(`[PHASE1:CLOSE] ⚠️ ${remainingOrders} orders still open:`);
                for (const o of checkOrders) {
                    console.warn(`[PHASE1:CLOSE]   - ${o.symbol} ${o.side} ${o.type} orderId=${o.orderId}`);
                }
            }
        } catch (e) {
            console.error('[PHASE1:CLOSE] Could not check orders:', e?.message || e);
        }

        try {
            const checkPositions = await getAllOpenPositionsWithRetry(env);
            remainingPositions = checkPositions.length;
            
            if (remainingPositions > 0) {
                console.warn(`[PHASE1:CLOSE] ⚠️ ${remainingPositions} positions still open:`);
                for (const p of checkPositions) {
                    console.warn(`[PHASE1:CLOSE]   - ${p.symbol} ${p.side} size=${p.size}`);
                }
            }
        } catch (e) {
            console.error('[PHASE1:CLOSE] Could not check positions:', e?.message || e);
        }

        // Check if we're clean
        if (remainingOrders === 0 && remainingPositions === 0) {
            console.log('[PHASE1:CLOSE] ✅✅✅ VERIFICATION PASSED: COMPLETELY CLEAN');
            break; // Exit the loop - we're done!
        } else {
            console.warn(`[PHASE1:CLOSE] ⚠️ NOT CLEAN: ${remainingOrders} orders, ${remainingPositions} positions remain`);
            if (flushAttempt < MAX_FLUSH_ATTEMPTS) {
                console.log('[PHASE1:CLOSE] Retrying flush...');
                await sleep(1000);
            } else {
                console.error('[PHASE1:CLOSE] 🚨 CRITICAL: FLUSH FAILED AFTER 3 ATTEMPTS');
                console.error('[PHASE1:CLOSE] 🚨 Remaining: orders=' + remainingOrders + ' positions=' + remainingPositions);
                
                // ALWAYS throw error to stop cycle
                throw new Error(`FLUSH FAILED: ${remainingOrders} orders + ${remainingPositions} positions still open`);
            }
        }
    }

    // ============================================================
    // PASS 4: CLEAN STATE (pending + pending_fill)
    // ============================================================
    try {
        console.log('[PHASE1:CLOSE] Cleaning state entries...');
        const cached = await getGistStateCache(env);
        
        const beforeCount = cached.pending.length;
        
        // Remove ALL active and pending_fill entries
        cached.pending = cached.pending.filter(p => {
            if (p.status === 'active' || p.status === 'pending_fill') {
                const sym = symbolSafe(p);
                const side = p.side_exchange || p.side || 'unknown';
                console.log(`[PHASE1:CLOSE] Removing ${sym} ${side} status=${p.status}`);
                
                // Mark as flushed in closed history
                if (p.status === 'active') {
                    const closedEntry = {
                        ...p,
                        exit_reason: 'flush_state_cleanup',
                        exit_price: p.entry_price || 0,
                        ts_exit_ms: Date.now(),
                        status: 'closed'
                    };
                    cached.closed = [...(cached.closed || []), closedEntry];
                }
                
                return false; // Remove from pending
            }
            return true; // Keep planned/other statuses
        });
        
        const removedCount = beforeCount - cached.pending.length;
        if (removedCount > 0) {
            console.log(`[PHASE1:CLOSE] ✓ Cleaned ${removedCount} state entries`);
            GIST_STATE_DIRTY = true;
        }
    } catch (e) {
        console.error('[PHASE1:CLOSE] Error cleaning state:', e?.message || e);
    }

    console.log('[PHASE1:CLOSE] ========================================');
    console.log('[PHASE1:CLOSE] ✅ ALL POSITIONS CLOSED SUCCESSFULLY');
    console.log('[PHASE1:CLOSE] ========================================');
}

// ============================================================
// MONITOR POSITIONS
// ============================================================
async function monitorPositions(env) {
    const monitorStart = Date.now();
    console.log('[monitor] ========================================');
    console.log('[monitor] Starting position monitor at', new Date().toISOString());

    GIST_STATE_CACHE = null;
    GIST_STATE_DIRTY = false;

    try {
        await syncServerTime(env);
        
        // ✅ STEP 3: Ensure hedge mode is enabled
        await ensureHedgeMode(env);

        const state = await loadGistState(env);

        const activePending = (state.pending || []).filter(p => p.status === 'active');
        const makerPending = (state.pending || []).filter(p => p.status === 'pending_fill');

        if (!activePending.length && !makerPending.length) {
            console.log('[monitor] No active or pending positions to monitor');
            await cleanupZombieIdeas(state, env);
            await flushGistState(env); // Save cleanup changes
            return;
        }

        console.log(`[monitor] Monitoring ${activePending.length} active + ${makerPending.length} pending maker orders`);

        // ============================================================
        // CHECK PENDING MAKER ORDERS WITH 5-MINUTE CUTOFF
        // ============================================================
        for (const p of makerPending) {
            const symbol = p.symbol;
            const now = Date.now();
            const orderAge = now - (p.ts_order_placed || 0);

            try {
                console.log(`[monitor:maker] ${symbol} checking orderId=${p.order_id} age=${Math.round(orderAge/1000)}s`);

                // Check order status
                const order = await binanceRequest('/fapi/v1/order', { symbol, orderId: p.order_id }, true, 'GET', env);
                await sleep(200);

                // ✅ FIX: Verify order matches expected side
                const expectedSide = (p.side === 'long' || p.side === 'Buy') ? 'BUY' : 'SELL';
                if (order.side !== expectedSide) {
                  console.error(`[monitor:maker] ${symbol} CRITICAL: Order side mismatch! Expected ${expectedSide}, got ${order.side}`);
                  console.error(`[monitor:maker] ${symbol} This indicates state corruption - skipping this position`);
                  continue;
                }

                const executedQty = parseFloat(order.executedQty || '0');
                const originalQty = parseFloat(order.origQty || '0');
                const isFullyFilled = executedQty >= originalQty;
                const isPartiallyFilled = executedQty > 0 && executedQty < originalQty;
                const isNotFilled = executedQty === 0;

                // ============================================================
                // SCENARIO 1: FULLY FILLED - SET TP/SL IMMEDIATELY
                // ============================================================
                if (isFullyFilled) {
                    console.log(`[monitor:maker] ✓✓✓ ${symbol} FULLY FILLED! Setting TP/SL...`);

                    // ✅ STEP 17B: Pass side to get correct position
                    const position = await getPositionInfoWithRetry(symbol, env, p.side_exchange);
                    if (position && position.size > 0) {
                        const market = await getMarketInfoCached(symbol, env);

                        const tpslResult = await setTPSL({
                            symbol,
                            side: position.side,
                            market,
                            position: { ...p, entry_price: position.avgPrice },  // ✅ FIX: Add entry_price
                            currentPrice: position.markPrice,
                            env
                        });

                        // Update to ACTIVE status
                        const cached = await getGistStateCache(env);
                        const idx = cached.pending.findIndex(x => x.symbol === symbol && x.status === 'pending_fill');
                        if (idx >= 0) {
                            cached.pending[idx] = {
                                ...cached.pending[idx],
                                status: 'active',
                                entry_price: position.avgPrice,
                                qty: position.size,
                                executed_qty: executedQty,
                                original_qty: originalQty,
                                fill_percentage: 100,
                                tp_set: tpslResult.tp_set,
                                sl_set: tpslResult.sl_set,
                                tpsl_armed: tpslResult.tp_set && tpslResult.sl_set,
                                tpsl_last_attempt: now,
                                ts_entry_ms: now,
                                fill_type: 'full'
                            };
                            GIST_STATE_DIRTY = true;
                            console.log(`[monitor:maker] ✓ ${symbol} promoted to ACTIVE with TP/SL`);
                        }
                    }

                    // ============================================================
                    // SCENARIO 2: 5+ MINUTES PASSED - CANCEL ORDER & SET TP/SL IF PARTIALLY FILLED
                    // ============================================================
                } else if (orderAge > MAKER_ORDER_TTL_MS) {
                    const fillPct = originalQty > 0 ? ((executedQty / originalQty) * 100).toFixed(1) : 0;

                    console.log(`[monitor:maker] ⏱ ${symbol} 5-MIN CUTOFF REACHED - Filled: ${executedQty}/${originalQty} (${fillPct}%)`);

                    // Cancel the remaining order
                    try {
                        await binanceRequest('/fapi/v1/order', { symbol, orderId: p.order_id }, true, 'DELETE', env);
                        console.log(`[monitor:maker] ✓ ${symbol} remaining order canceled`);
                        await sleep(200);
                    } catch (e) {
                        // -2011 means order already filled/canceled - that's fine
                        if (!e.message?.includes('-2011')) {
                            console.error(`[monitor:maker] cancel error:`, e?.message || e);
                        }
                    }

                    // If we have ANY filled quantity, set TP/SL
                    if (isPartiallyFilled) {
                        console.log(`[monitor:maker] ✓ ${symbol} PARTIAL FILL (${fillPct}%) - Setting TP/SL for filled portion...`);

                        // ✅ STEP 17C: Pass side to get correct position
                        const position = await getPositionInfoWithRetry(symbol, env, p.side_exchange);
                        if (position && position.size > 0) {
                            const market = await getMarketInfoCached(symbol, env);

                            const tpslResult = await setTPSL({
                                symbol,
                                side: position.side,
                                market,
                                position: { ...p, entry_price: position.avgPrice },  // ✅ FIX: Add entry_price
                                currentPrice: position.markPrice,
                                env
                            });

                            // Update to ACTIVE (partial) status
                            const cached = await getGistStateCache(env);
                            const idx = cached.pending.findIndex(x => x.symbol === symbol && x.status === 'pending_fill');
                            if (idx >= 0) {
                                cached.pending[idx] = {
                                    ...cached.pending[idx],
                                    status: 'active',  // Still active, just partial fill
                                    entry_price: position.avgPrice,
                                    qty: position.size,
                                    executed_qty: executedQty,
                                    original_qty: originalQty,
                                    fill_percentage: parseFloat(fillPct),
                                    tp_set: tpslResult.tp_set,
                                    sl_set: tpslResult.sl_set,
                                    tpsl_armed: tpslResult.tp_set && tpslResult.sl_set,
                                    tpsl_last_attempt: now,
                                    ts_entry_ms: now,
                                    fill_type: 'partial',
                                    partial_fill_reason: '5min_cutoff'
                                };
                                GIST_STATE_DIRTY = true;
                                console.log(`[monitor:maker] ✓ ${symbol} partial position (${fillPct}%) promoted to ACTIVE with TP/SL`);
                            }
                        } else {
                            console.warn(`[monitor:maker] ⚠ ${symbol} partial fill detected but no position found on exchange`);
                            // Remove from state if no position exists
                            const cached = await getGistStateCache(env);
                            cached.pending = cached.pending.filter(x => !(x.symbol === symbol && x.status === 'pending_fill'));
                            GIST_STATE_DIRTY = true;
                        }

                        // If nothing was filled, just remove from state
                    } else if (isNotFilled) {
                        console.log(`[monitor:maker] ✗ ${symbol} NOT FILLED after 5 minutes - Removing from state`);
                        const cached = await getGistStateCache(env);
                        cached.pending = cached.pending.filter(x => !(x.symbol === symbol && x.status === 'pending_fill'));
                        GIST_STATE_DIRTY = true;
                    }
                // ============================================================
                // SCENARIO 3: STILL PENDING (< 5 MINUTES)
                // ============================================================
                } else {
                    // SCENARIO 3: STILL PENDING (< 5 MINUTES)
                    const fillPct = originalQty > 0 ? ((executedQty / originalQty) * 100).toFixed(1) : 0;
                    const timeLeft = Math.round((MAKER_ORDER_TTL_MS - orderAge) / 1000);
                    
                    console.log(`[monitor:maker] ${symbol} still PENDING - Filled: ${fillPct}% | Time left: ${timeLeft}s`);
                    
                    // ========================================
                    // 🛡️ PARTIAL FILL EMERGENCY GUARD
                    // ========================================
                    if (isPartiallyFilled) {
                        // ✅ STEP 17D: Pass side to get correct position
                        const position = await getPositionInfoWithRetry(symbol, env, p.side_exchange);
                        
                        if (position && position.size > 0) {
                            console.log(`[monitor:maker:guard] ${symbol} checking partial fill protection...`);
                            
                            const trailing = p.trailing;
                            if (trailing) {
                                const entryPrice = p.limit_price; // Use limit price as entry
                                const currentPrice = position.markPrice;
                                const isLong = (p.side_exchange === 'Buy');
                                
                                // Calculate thresholds
                                let slThreshold, tpThreshold;
                                if (isLong) {
                                    slThreshold = entryPrice * (1 + trailing.initial_sl_percent / 100);
                                    tpThreshold = entryPrice * (1 + trailing.major_tp_percent / 100);
                                } else {
                                    slThreshold = entryPrice * (1 - trailing.initial_sl_percent / 100);
                                    tpThreshold = entryPrice * (1 - trailing.major_tp_percent / 100);
                                }
                                
                                // Check if beyond thresholds
                                let beyondThreshold = false;
                                let breachType = '';
                                
                                if (isLong) {
                                    if (currentPrice <= slThreshold) {
                                        beyondThreshold = true;
                                        breachType = 'partial_fill_sl_breach';
                                    } else if (currentPrice >= tpThreshold) {
                                        beyondThreshold = true;
                                        breachType = 'partial_fill_tp_breach';
                                    }
                                } else {
                                    if (currentPrice >= slThreshold) {
                                        beyondThreshold = true;
                                        breachType = 'partial_fill_sl_breach';
                                    } else if (currentPrice <= tpThreshold) {
                                        beyondThreshold = true;
                                        breachType = 'partial_fill_tp_breach';
                                    }
                                }
                                
                                // Emergency close if beyond thresholds
                                if (beyondThreshold) {
                                    console.error(`[monitor:maker:guard] 🚨🚨🚨 ${symbol} PARTIAL FILL EMERGENCY - ${breachType.toUpperCase()}`);
                                    console.error(`[monitor:maker:guard] 🚨 Entry=${entryPrice} Current=${currentPrice} Threshold=${breachType.includes('sl') ? slThreshold : tpThreshold}`);
                                    console.error(`[monitor:maker:guard] 🚨 EMERGENCY CLOSING PARTIAL POSITION`);
                                    
                                    try {
                                        // Cancel pending order
                                        await binanceRequest('/fapi/v1/order', { 
                                            symbol, 
                                            orderId: p.order_id 
                                        }, true, 'DELETE', env);
                                        await sleep(200);
                                        
                                        // Close position
                                        const market = await getMarketInfoCached(symbol, env);
                                        const orderSide = isLong ? 'SELL' : 'BUY';
                                        
                                        const closeResult = await placeMarketOrderWithRetry(
                                            symbol, 
                                            orderSide, 
                                            position.size, 
                                            market, 
                                            env, 
                                            true
                                        );
                                        
                                        if (closeResult.success) {
                                            console.log(`[monitor:maker:guard] ✓ ${symbol} PARTIAL FILL EMERGENCY CLOSED`);
                                            
                                            // Remove from state
                                            const cached = await getGistStateCache(env);
                                            cached.pending = cached.pending.filter(x => 
                                                !(x.symbol === symbol && x.status === 'pending_fill')
                                            );
                                            
                                            // Add to closed
                                            await closePositionCached(p, {
                                                exit_reason: 'emergency_partial_fill_threshold_breach',
                                                exit_detail: breachType,
                                                exit_price: currentPrice,
                                                ts_exit_ms: Date.now(),
                                                fill_percentage: parseFloat(fillPct),
                                                emergency_close: true,
                                                partial_fill_emergency: true
                                            }, env);
                                            
                                            GIST_STATE_DIRTY = true;
                                            
                                            // Skip to next position
                                            await sleep(300);
                                            continue;
                                        } else {
                                            console.error(`[monitor:maker:guard] ✗ ${symbol} EMERGENCY CLOSE FAILED`);
                                        }
                                    } catch (e) {
                                        console.error(`[monitor:maker:guard] ${symbol} emergency error:`, e?.message || e);
                                    }
                                } else {
                                    console.log(`[monitor:maker:guard] ${symbol} partial fill within safe zone (${fillPct}% filled, ${timeLeft}s left)`);
                                }
                            }
                        }
                    }
                    // ========================================
                    // END PARTIAL FILL GUARD
                    // ========================================
                }

            } catch (e) {
                console.error(`[monitor:maker] ${symbol} error:`, e?.message || e);
            }

            await sleep(300);
        }

        // Now get the updated list of active positions
        const pending = (await getGistStateCache(env)).pending.filter(p => p.status === 'active');
        
        const allPositions = await getAllOpenPositionsWithRetry(env);
// Map by symbol+side instead of symbol only
        const posMap = new Map(allPositions.map(p => [
          getPositionKeyFromLive(p.symbol, p.side),
          p
        ]));

        let closedCount = 0;
        let rearmedCount = 0;
        let activeCount = 0;

        for (const p of pending) {
            const symbol = p.symbol || p.symbol_full || p.symbolFull || (p.base ? `${p.base}USDT` : null);
            if (!symbol) continue;
        
            try {
                // ✅ STEP 5: Look up by symbol+side
                const side = p.side_exchange || p.side || 'unknown';
                const positionKey = getPositionKeyFromLive(symbol, side);
                const position = posMap.get(positionKey);
                const now = Date.now();
        
                // ============================================================
                // 🚨 EMERGENCY GUARD: Check unprotected positions
                // ============================================================
                if (position && position.size > 0) {
                    const emergencyResult = await checkUnprotectedPositionEmergency(p, position, env);
                    
                    if (emergencyResult.action === 'emergency_closed') {
                        console.log(`[monitor] 🚨 ${symbol} EMERGENCY CLOSED - skipping further checks`);
                        closedCount++;
                        await sleep(300);
                        continue; // Skip to next position
                    }
                }
        
                // ============================================================
                // 🔍 POSITION VERIFICATION: Update entry price if needed
                // ============================================================
                if (p.needs_verification === true && position && position.size > 0) {
                    console.log(`[monitor:verify] ${symbol} verifying position with needs_verification flag`);
                    
                    const expectedSide = (p.side_exchange === 'Buy') ? 'Buy' : 'Sell';
                    if (position.side !== expectedSide) {
                      console.error(`[monitor:verify] ${symbol} SIDE MISMATCH - state=${expectedSide} exchange=${position.side}`);
                      console.error(`[monitor:verify] ${symbol} Skipping verification - likely wrong position`);
                      continue;
                    }
                    
                    const stateEntryPrice = p.entry_price;
                    const actualEntryPrice = position.avgPrice;
                    const priceDiff = Math.abs(actualEntryPrice - stateEntryPrice);
                    const diffPercent = (priceDiff / stateEntryPrice) * 100;
                    
                    if (diffPercent > 0.1) {
                        console.warn(`[monitor:verify] ${symbol} entry price mismatch: state=${stateEntryPrice} actual=${actualEntryPrice} (${diffPercent.toFixed(2)}% diff)`);
                        console.log(`[monitor:verify] ${symbol} updating state with actual entry price`);
                        
                        // Update state
                        const cached = await getGistStateCache(env);
                        const idx = cached.pending.findIndex(x => x.symbol === symbol && x.status === 'active');
                        
                        if (idx >= 0) {
                            cached.pending[idx].entry_price = actualEntryPrice;
                            cached.pending[idx].qty = position.size;
                            cached.pending[idx].needs_verification = false;
                            cached.pending[idx].position_verified = true;
                            cached.pending[idx].entry_price_source = 'exchange_position';
                            GIST_STATE_DIRTY = true;
                            
                            // Force re-arm to update TP/SL with correct price
                            cached.pending[idx].tp_set = false;
                            cached.pending[idx].sl_set = false;
                            cached.pending[idx].tpsl_armed = false;
                            
                            console.log(`[monitor:verify] ✓ ${symbol} state corrected, will re-arm TP/SL with correct price`);
                        }
                    } else {
                        console.log(`[monitor:verify] ${symbol} entry price OK (diff: ${diffPercent.toFixed(3)}%)`);
                        
                        // Mark as verified
                        const cached = await getGistStateCache(env);
                        const idx = cached.pending.findIndex(x => x.symbol === symbol && x.status === 'active');
                        if (idx >= 0) {
                            cached.pending[idx].needs_verification = false;
                            cached.pending[idx].position_verified = true;
                            GIST_STATE_DIRTY = true;
                        }
                    }
                }

                // ============================================================
                // 📈 TRAILING CHECK: Move stop loss at milestones
                // ============================================================
                if (position && position.size > 0 && p.trailing) {
                    const entryPrice = p.entry_price;
                    const currentPrice = position.markPrice;
                    const isLong = (p.side_exchange === 'Buy');
        
                    // Calculate current PnL%
                    let currentPnL = 0;
                    if (isLong) {
                        currentPnL = ((currentPrice / entryPrice) - 1) * 100;
                    } else {
                        currentPnL = ((entryPrice / currentPrice) - 1) * 100;
                    }
        
                    console.log(`[monitor:trail] ${symbol} PnL=${currentPnL.toFixed(2)}% (entry=${entryPrice} current=${currentPrice})`);
        
                    const trailAction = shouldTrailStopLoss(p, currentPnL, env);
        
                    if (trailAction.should) {
                        console.log(`[monitor:trail] 🎯 ${symbol} MILESTONE HIT: ${trailAction.reason}`);
        
                        const trailResult = await trailStopLoss(symbol, p, trailAction, env);
        
                        if (trailResult.success) {
                            // Update state
                            const cached = await getGistStateCache(env);
                            const idx = cached.pending.findIndex(x => x.symbol === symbol && x.status === 'active');
                            
                            if (idx >= 0) {
                                // ✅ FIX: Mark ALL milestones we passed (not just the latest)
                                if (trailAction.new_stage === 'minor_hit') {
                                    cached.pending[idx].trailing.base_tp_hit = true;   // ✅ Mark both!
                                    cached.pending[idx].trailing.minor_tp_hit = true;
                                    console.log(`[trail:state] ${symbol} marked BOTH base + minor as hit`);
                                } else if (trailAction.new_stage === 'base_hit') {
                                    cached.pending[idx].trailing.base_tp_hit = true;
                                    console.log(`[trail:state] ${symbol} marked base as hit`);
                                }
                                cached.pending[idx].trailing.trailing_stage = trailAction.new_stage;
                                cached.pending[idx].sl_abs = trailResult.new_sl_price;
                                GIST_STATE_DIRTY = true;
        
                                console.log(`[monitor:trail] ✓ ${symbol} state updated: stage=${trailAction.new_stage}`);
                            }
        
                            rearmedCount++; // Count as re-arm action
                        }
        
                        await sleep(300);
                    }
                }
        
                // ==== CHECK IF POSITION CLOSED ====
                if (!position || position.size === 0) {
                    console.log(`[monitor:closed] ${symbol} position closed, analyzing exit...`);

                    const allOrders = await getAllOrdersWithRetry(symbol, p.ts_entry_ms || 0, env);
                    await sleep(150);

                    const tpTypes = new Set(['TAKE_PROFIT_MARKET', 'TAKE_PROFIT']);
                    const slTypes = new Set(['STOP_MARKET', 'STOP', 'STOP_LOSS', 'STOP_LOSS_LIMIT', 'TRAILING_STOP_MARKET']);

                    // Find the actual filled TP or SL order
                    const tpFill = Array.isArray(allOrders) ? allOrders.find(o => o.status === 'FILLED' && tpTypes.has(o.type)) : null;
                    const slFill = Array.isArray(allOrders) ? allOrders.find(o => o.status === 'FILLED' && slTypes.has(o.type)) : null;

                    let exitReason = 'unknown';
                    let exitPx = p.entry_price; // Start with entry price as a fallback

                    if (tpFill) {
                        exitReason = 'tp';
                        // Use avgPrice first, then stopPrice, then the fallback
                        exitPx = parseFloat(tpFill.avgPrice || tpFill.stopPrice || exitPx);
                        console.log(`[monitor:closed] ${symbol} TP HIT at ${exitPx} (from filled order)`);
                    } else if (slFill) {
                        exitReason = 'sl';
                        // Use avgPrice first, then stopPrice, then the fallback
                        exitPx = parseFloat(slFill.avgPrice || slFill.stopPrice || exitPx);
                        console.log(`[monitor:closed] ${symbol} SL HIT at ${exitPx} (from filled order)`);
                    } else {
                        exitReason = 'manual_or_liquidation';
                        // Find the last MARKET order that closed the position
                        const filledMarketOrders = Array.isArray(allOrders) ? allOrders.filter(o => o.status === 'FILLED' && o.type === 'MARKET') : [];
                        if (filledMarketOrders.length > 0) {
                            const lastMarketOrder = filledMarketOrders[filledMarketOrders.length - 1];
                            // If the market order was a closing order, its side will be opposite
                            const isLong = (p.side === 'long' || p.side === 'Buy');
                            const expectedCloseSide = isLong ? 'SELL' : 'BUY';
                            if (lastMarketOrder.side === expectedCloseSide) {
                                exitPx = parseFloat(lastMarketOrder.avgPrice || lastMarketOrder.price || exitPx);
                                 console.log(`[monitor:closed] ${symbol} closed manually/liquidation at ${exitPx} (from market order)`);
                            } else {
                                 console.log(`[monitor:closed] ${symbol} closed manually/liquidation (no clear closing order found)`);
                            }
                        } else {
                            console.log(`[monitor:closed] ${symbol} closed manually/liquidation (no closing market order found)`);
                        }
                    }

                    // ✅ FIX: Pass client_order_id and side to closePositionCached
                    await closePositionCached(symbol, {
                        exit_reason: exitReason,
                        exit_price: exitPx,
                        ts_exit_ms: now,
                        position_data: p,
                        client_order_id: p.client_order_id,  // ✅ ADD THIS
                        side: p.side || p.side_exchange       // ✅ ADD THIS
                    }, env);

                    closedCount++;

                } else {
                    // ==== POSITION STILL OPEN - CHECK TP/SL ====
                    activeCount++;

                    const needsRearm = shouldRearmTPSL(p, position, now);

                    if (needsRearm.should) {
                        console.log(`[monitor:rearm] ${symbol} NEEDS TP/SL RE-ARM - reason: ${needsRearm.reason}`);

                        try {
                            // ✅ STEP 11: Cancel only THIS side's orders
                            const isLong = (p.side_exchange === 'Buy');
                            const posSide = isLong ? 'LONG' : 'SHORT';
                            await cancelSideOrders(symbol, posSide, env);
                            await sleep(200);

                            const market = await getMarketInfoCached(symbol, env);
                            
                            const result = await setTPSL({
                                symbol,
                                side: position.side,
                                market,
                                position: p,
                                currentPrice: position.markPrice,
                                env
                            });

                            // Update state with re-arm results
                            const cached = await getGistStateCache(env);
                            const idx = cached.pending.findIndex(x => x.symbol === symbol && x.status === 'active');
                            if (idx >= 0) {
                                cached.pending[idx].tp_set = result.tp_set;
                                cached.pending[idx].sl_set = result.sl_set;
                                cached.pending[idx].tpsl_armed = result.tp_set && result.sl_set;
                                cached.pending[idx].tpsl_last_attempt = now;
                                cached.pending[idx].rearm_count = (cached.pending[idx].rearm_count || 0) + 1;
                                GIST_STATE_DIRTY = true;

                                console.log(`[monitor:rearm] ${symbol} RE-ARM RESULT: TP=${result.tp_set ? '✓' : '✗'} SL=${result.sl_set ? '✓' : '✗'} (attempt #${cached.pending[idx].rearm_count})`);

                                if (result.tp_set && result.sl_set) {
                                    rearmedCount++;
                                }
                            }
                        } catch (e) {
                            console.error(`[monitor:rearm] ${symbol} RE-ARM FAILED:`, e?.message || e);
                        }
                    } else {
                        console.log(`[monitor:active] ${symbol} position active, TP/SL status OK (armed=${p.tpsl_armed})`);
                    }
                }

            } catch (e) {
                console.error('[monitor] symbol error', symbolSafe(p), e?.message || e);
            }

            await sleep(300);
        }

        // Clean up stale planned ideas
        await cleanupZombieIdeas(state, env);

        // Save state
        await flushGistState(env);

        const monitorTime = ((Date.now() - monitorStart) / 1000).toFixed(2);
        console.log('[monitor] ========================================');
        console.log(`[monitor] COMPLETE in ${monitorTime}s`);
        console.log(`[monitor] Active: ${activeCount} | Closed: ${closedCount} | Re-armed: ${rearmedCount}`);
        console.log('[monitor] ========================================');
    } catch (e) {
        console.error('[monitor] error', e?.message || e);
        await flushGistState(env);
    }
}

// ============================================================
// ENHANCED SHOULD RE-ARM CHECK
// ============================================================
function shouldRearmTPSL(position, livePosition, now) {
    const lastAttempt = position.tpsl_last_attempt || position.ts_entry_ms || 0;
    const timeSinceLastAttempt = now - lastAttempt;

    // ✅ FIX: IMMEDIATE re-arm for critical conditions (no throttle)
    
    // Missing orders → IMMEDIATE
    if (position.tp_set !== true || position.sl_set !== true) {
        console.log(`[rearm:check] ${position.symbol} MISSING ORDERS - immediate re-arm`);
        return { should: true, reason: 'missing_orders_immediate' };
    }

    // Not armed → IMMEDIATE
    if (position.tpsl_armed !== true) {
        console.log(`[rearm:check] ${position.symbol} NOT ARMED - immediate re-arm`);
        return { should: true, reason: 'not_armed_immediate' };
    }

    // Needs verification → IMMEDIATE
    if (position.needs_verification === true) {
        console.log(`[rearm:check] ${position.symbol} NEEDS VERIFICATION - immediate re-arm`);
        return { should: true, reason: 'needs_verification_immediate' };
    }

    // ✅ Only apply throttle to PERIODIC checks
    // Already fully armed and recent → check throttle
    if (position.tpsl_armed === true) {
        if (timeSinceLastAttempt < MIN_REARM_INTERVAL) {
            return { should: false, reason: 'periodic_throttled' };
        }
        
        // Periodic check (every 30 min)
        if (timeSinceLastAttempt > 30 * 60 * 1000) {
            return { should: true, reason: 'periodic_check' };
        }
    }

    return { should: false, reason: 'no_need' };
}


async function cleanupZombieIdeas(state, env) {
    try {
        const now = Date.now();
        const cached = await getGistStateCache(env);
        const beforeCleanup = cached.pending.length;

        cached.pending = cached.pending.filter(p => {
            // === MAKER CHANGE START (Bonus: Safety Net Cleanup) ===
            // Clean up expired pending_fill orders (safety net)
            if (p.status === 'pending_fill') {
                const orderAge = now - (p.ts_order_placed || 0);
                if (orderAge > MAKER_ORDER_TTL_MS + (60 * 1000) /* add 1 min buffer */) {
                    console.log('[cleanup] removing expired pending_fill order:', p.symbol, 'age:', Math.round(orderAge / 1000), 's');
                    return false;
                }
            }
            // === MAKER CHANGE END ===

            if (p.status === 'active' || p.status === 'pending_fill') return true;

            if (p.status === 'planned' || p.status === null || p.status === undefined) {
                const ideaAge = now - (p.ts_ms || 0);
                const ttlExpired = p.ttl_ts_ms && p.ttl_ts_ms < now;
                const tooOld = ideaAge > PLANNED_IDEA_TTL;

                if (ttlExpired || tooOld) {
                    console.log('[monitor:cleanup] removing stale planned idea:',
                        p.idea_id || p.client_order_id, p.symbol || p.symbolFull || p.base,
                        'age:', Math.round(ideaAge / 1000), 's');
                    return false;
                }
            }

            return true;
        });

        const afterCleanup = cached.pending.length;
        const cleaned = beforeCleanup - afterCleanup;

        if (cleaned > 0) {
            console.log('[monitor:cleanup] removed', cleaned, 'stale ideas');
            GIST_STATE_DIRTY = true;
        }
    } catch (e) {
        console.error('[monitor:cleanup] error', e?.message || e);
    }
}

// ---------------- TP/SL Order Placement ----------------
async function setTPSL({ symbol, side, market, position, currentPrice, env }) {
    const result = { tp_set: false, sl_set: false, status: 'pending' };
    const trailing = position.trailing;
    
    if (!trailing) {
        console.error(`[tpsl:set] ${symbol} NO TRAILING CONFIG`);
        return { ...result, status: 'error_no_config' };
    }
    
    const isLong = (side === 'Buy');
    
    // ✅ STEP 9A: Calculate positionSide for hedge mode
    const posSide = isLong ? 'LONG' : 'SHORT';
    
    const entryPrice = position.entry_price;
    
    console.log(`[tpsl:set] ========================================`);
    console.log(`[tpsl:set] ${symbol} side=${side} Entry=${entryPrice} Stage=${trailing.trailing_stage}`);
    
    // Calculate MAJOR_TP (always same)
    const levels = calculateTrailingLevels(entryPrice, side, market, trailing);
    const majorTpFmt = formatPx(levels.major_tp_price, market.priceDecimals);
    
    // ✅ FIX: Calculate CURRENT SL based on trailing stage
    let currentSlPercent;
    if (trailing.minor_tp_hit) {
        currentSlPercent = trailing.minor_tp_percent - trailing.trailing_offset_percent;
        console.log(`[tpsl:set] Re-arming at MINOR stage → SL=${currentSlPercent}%`);
    } else if (trailing.base_tp_hit) {
        currentSlPercent = trailing.base_tp_percent - trailing.trailing_offset_percent;
        console.log(`[tpsl:set] Re-arming at BASE stage → SL=${currentSlPercent}%`);
    } else {
        currentSlPercent = trailing.initial_sl_percent;
        console.log(`[tpsl:set] Re-arming at INITIAL stage → SL=${currentSlPercent}%`);
    }
    
    // Calculate actual SL price
    let currentSlPrice;
    if (isLong) {
        currentSlPrice = entryPrice * (1 + currentSlPercent / 100);
        currentSlPrice = floorToTick(currentSlPrice, market.tickSize);
    } else {
        currentSlPrice = entryPrice * (1 - currentSlPercent / 100);
        currentSlPrice = ceilToTick(currentSlPrice, market.tickSize);
    }
    
    const currentSlFmt = formatPx(currentSlPrice, market.priceDecimals);
    
    console.log(`[tpsl:set] CURRENT_SL=${currentSlFmt} (${currentSlPercent}%) MAJOR_TP=${majorTpFmt}`);

    try {
        // ✅ STEP 10: Cancel only THIS side's orders (not both)
        await cancelSideOrders(symbol, posSide, env);
        await sleep(200);
        const orderSide = isLong ? 'SELL' : 'BUY';

        // Place MAJOR_TP
        for (let attempt = 0; attempt < 2; attempt++) {
            try {
                await binanceRequest('/fapi/v1/order', { 
                    symbol, 
                    side: orderSide, 
                    type: 'TAKE_PROFIT_MARKET', 
                    stopPrice: majorTpFmt, 
                    closePosition: 'true', 
                    workingType: 'MARK_PRICE', 
                    positionSide: posSide  // ✅ STEP 9B
                }, true, 'POST', env);
                result.tp_set = true; 
                console.log(`[tpsl:tp] ${symbol} ✓ MAJOR_TP placed`);
                break;
            } catch (e) { 
                if (attempt === 1) console.error(`[tpsl:tp] ${symbol} failed:`, e?.message || e); 
                else await sleep(300); 
            }
        }
        
        await sleep(150);
        
        // Place CURRENT_SL (respects trailing stage) ✅
        for (let attempt = 0; attempt < 2; attempt++) {
            try {
                await binanceRequest('/fapi/v1/order', { 
                    symbol, 
                    side: orderSide, 
                    type: 'STOP_MARKET', 
                    stopPrice: currentSlFmt, 
                    closePosition: 'true', 
                    workingType: 'MARK_PRICE',
                    positionSide: posSide  // ✅ STEP 9C
                }, true, 'POST', env);
                result.sl_set = true;
                console.log(`[tpsl:sl] ${symbol} ✓ CURRENT_SL placed at ${currentSlFmt}`);
                break;
            } catch (e) { 
                if (attempt === 1) console.error(`[tpsl:sl] ${symbol} failed:`, e?.message || e); 
                else await sleep(300); 
            }
        }
        
        result.status = (result.tp_set && result.sl_set) ? 'complete' : 'partial';
        console.log(`[tpsl:set] ${symbol} RESULT: status=${result.status}`);
        console.log(`[tpsl:set] ========================================`);
    } catch (e) {
        console.error('[tpsl:set] ✗ CRITICAL ERROR', symbol, e?.message || e);
        await logCriticalFailure(symbol, 'tpsl_critical_error', position, env);
        result.status = 'error';
    }

    return result;
}


function ceilToTick(px, tick) { if (!(tick > 0)) return px; return Math.ceil(px / tick) * tick; }
function floorToTick(px, tick) { if (!(tick > 0)) return px; return Math.floor(px / tick) * tick; }

// === MAKER CHANGE START (Step 2: Add Maker Price Calculator) ===
function calculateMakerPrice(currentPrice, side, tickSize, priceDecimals) {
    const offsetBps = MAKER_OFFSET_BPS;
    const offsetPct = offsetBps / 10000; // 2 bps = 0.0002 = 0.02%

    let limitPrice;

    if (side === 'BUY') {
        // For LONG: place bid BELOW current price (passive)
        limitPrice = currentPrice * (1 - offsetPct);
        limitPrice = floorToTick(limitPrice, tickSize); // Round down
    } else {
        // For SHORT: place ask ABOVE current price (passive)
        limitPrice = currentPrice * (1 + offsetPct);
        limitPrice = ceilToTick(limitPrice, tickSize); // Round up
    }

    const formattedPrice = formatPx(limitPrice, priceDecimals);

    console.log(`[maker:price] side=${side} current=${currentPrice} offset=${offsetBps}bps → limit=${formattedPrice}`);

    return formattedPrice;
}
// === MAKER CHANGE END ===

async function placeMarketOrderWithRetry(symbol, side, quantity, market, env, reduceOnly = false) {
    const qtyRounded = floorToStep(quantity, market.stepSize);
    const qtyFmt = formatQty(qtyRounded, market.qtyDecimals);

    // ============================================================
    // ENTRY ORDERS: Check ORDER_TYPE mode
    // ============================================================
    if (!reduceOnly) {
        const orderType = (env.ORDER_TYPE || 'maker').toLowerCase().trim();
        
        // ========================================
        // TAKER MODE: MARKET ORDERS (Immediate Fill)
        // ========================================
        if (orderType === 'taker') {
            console.log(`[order:taker] ${symbol} placing MARKET order ${side} ${qtyFmt}`);

            for (let attempt = 0; attempt < 3; attempt++) {
                try {
                    // ✅ STEP 8A: Add positionSide for hedge mode
                    const posSide = (side === 'BUY') ? 'LONG' : 'SHORT';

                    const takerOrder = await binanceRequest('/fapi/v1/order', {
                        symbol,
                        side,
                        type: 'MARKET',
                        quantity: qtyFmt,
                        newOrderRespType: 'FULL',
                        positionSide: posSide
                    }, true, 'POST', env);

                    if (takerOrder?.orderId) {
                        const avgPrice = parseFloat(takerOrder.avgPrice || '0');
                        const executedQty = parseFloat(takerOrder.executedQty || '0');
                        
                        console.log(`[order:taker] ✓ ${symbol} MARKET filled orderId=${takerOrder.orderId} qty=${executedQty} price=${avgPrice}`);

                        return {
                            success: true,
                            orderId: takerOrder.orderId,
                            maker: false,
                            pending_fill: false,
                            taker_mode: true,
                            fill_price: avgPrice > 0 ? avgPrice : null,  // ✅ Capture fill price if available
                            fill_qty: executedQty > 0 ? executedQty : null
                        };
                    }

                } catch (e) {
                    const errMsg = e?.message || '';

                    // Insufficient margin/balance
                    if (errMsg.includes('-2019') || errMsg.includes('Margin is insufficient')) {
                        console.error(`[order:taker] ✗ ${symbol} INSUFFICIENT MARGIN - cannot retry`);
                        return { success: false, reason: 'insufficient_margin' };
                    }

                    // Invalid quantity
                    if (errMsg.includes('-1111') || errMsg.includes('Precision')) {
                        console.error(`[order:taker] ✗ ${symbol} INVALID QUANTITY - cannot retry`);
                        return { success: false, reason: 'invalid_quantity' };
                    }

                    // Time sync error
                    if (errMsg.includes('-1021') && attempt < 2) {
                        console.log('[order:taker] timestamp error, resyncing');
                        await syncServerTime(env);
                        await sleep(300);
                        continue;
                    }

                    // Server errors
                    if ((errMsg.includes('502') || errMsg.includes('503') || errMsg.includes('504')) && attempt < 2) {
                        console.log(`[order:taker] retry on ${errMsg.includes('502') ? '502' : errMsg.includes('503') ? '503' : '504'}`, symbol);
                        await sleep(500 * (attempt + 1));  // Exponential backoff
                        continue;
                    }

                    // Rate limit
                    if (errMsg.includes('-1003') && attempt < 2) {
                        console.log('[order:taker] rate limit, backing off');
                        await sleep(1000);
                        continue;
                    }

                    console.error(`[order:taker] ✗ error:`, symbol, errMsg.slice(0, 200));
                    
                    if (attempt === 2) {
                        return { success: false, reason: 'taker_error', error: errMsg.slice(0, 200) };
                    }
                }
            }

            return { success: false, reason: 'taker_failed_after_retries' };
        }
        
        // ========================================
        // MAKER MODE: LIMIT ORDERS (Current Logic)
        // ========================================
        console.log(`[order:maker] ${symbol} placing MAKER order ${side} ${qtyFmt}`);

        for (let attempt = 0; attempt < 3; attempt++) {  // ✅ Increased to 3 attempts
            try {
                // Calculate limit price for maker order
                const limitPrice = calculateMakerPrice(market.price, side, market.tickSize, market.priceDecimals);

                // ✅ STEP 8B: Add positionSide for hedge mode
                const posSide = (side === 'BUY') ? 'LONG' : 'SHORT';

                // Place LIMIT order with GTX (post-only)
                const makerOrder = await binanceRequest('/fapi/v1/order', {
                    symbol,
                    side,
                    type: 'LIMIT',
                    quantity: qtyFmt,
                    price: limitPrice,
                    timeInForce: 'GTX',
                    positionSide: posSide
                }, true, 'POST', env);

                if (makerOrder?.orderId) {
                    console.log(`[order:maker] ✓ ${symbol} LIMIT order placed, orderId=${makerOrder.orderId} @ ${limitPrice}`);

                    return {
                        success: true,
                        orderId: makerOrder.orderId,
                        maker: true,
                        pending_fill: true,
                        limit_price: limitPrice
                    };
                }

            } catch (e) {
                const errMsg = e?.message || '';

                // GTX rejection means order would cross (take liquidity)
                if (errMsg.includes('-2021') || errMsg.includes('Post-only')) {
                    console.log(`[order:maker] ${symbol} GTX rejected (would cross), recalculating price...`);
                    // ✅ Refresh market price and retry
                    const freshMarket = await getMarketInfoCached(symbol, env);
                    market.price = freshMarket.price;
                    await sleep(200);
                    continue;
                }

                // Insufficient margin
                if (errMsg.includes('-2019') || errMsg.includes('Margin is insufficient')) {
                    console.error(`[order:maker] ✗ ${symbol} INSUFFICIENT MARGIN - cannot retry`);
                    return { success: false, reason: 'insufficient_margin' };
                }

                // Invalid quantity
                if (errMsg.includes('-1111') || errMsg.includes('Precision')) {
                    console.error(`[order:maker] ✗ ${symbol} INVALID QUANTITY - cannot retry`);
                    return { success: false, reason: 'invalid_quantity' };
                }

                // Time sync error
                if (errMsg.includes('-1021') && attempt < 2) {
                    console.log('[order:maker] timestamp error, resyncing');
                    await syncServerTime(env);
                    await sleep(300);
                    continue;
                }

                // Server errors
                if ((errMsg.includes('502') || errMsg.includes('503') || errMsg.includes('504')) && attempt < 2) {
                    console.log(`[order:maker] retry on server error`, symbol);
                    await sleep(500 * (attempt + 1));
                    continue;
                }

                // Rate limit
                if (errMsg.includes('-1003') && attempt < 2) {
                    console.log('[order:maker] rate limit, backing off');
                    await sleep(1000);
                    continue;
                }

                console.error(`[order:maker] ✗ error:`, symbol, errMsg.slice(0, 200));
                
                if (attempt === 2) {
                    return { success: false, reason: 'maker_error', error: errMsg.slice(0, 200) };
                }
            }
        }

        return { success: false, reason: 'maker_failed_after_retries' };
    }

    // ============================================================
    // CLOSE ORDERS: ALWAYS MARKET (in hedge mode, positionSide is enough - no reduceOnly needed)
    // ============================================================
    console.log(`[order:market] ${symbol} closing position with MARKET order`);

    for (let attempt = 0; attempt < 3; attempt++) {
        try {
            // ✅ FIX: For close orders, positionSide is OPPOSITE of order side
            // To close LONG: SELL order with positionSide='LONG'
            // To close SHORT: BUY order with positionSide='SHORT'
            const posSide = (side === 'BUY') ? 'SHORT' : 'LONG';  // ✅ INVERTED for close
            
            console.log(`[order:market] ${symbol} ${side} order to close ${posSide} position`);

            // ✅ In hedge mode, Binance rejects reduceOnly when positionSide is specified
            const params = { 
                symbol, 
                side, 
                type: 'MARKET', 
                quantity: qtyFmt,
                positionSide: posSide
                // ✅ NO reduceOnly parameter - positionSide is sufficient in hedge mode
            };

            const result = await binanceRequest('/fapi/v1/order', params, true, 'POST', env);

            if (result?.orderId) {
                console.log('[order:market] ✓ filled', symbol, side, qtyFmt, 'id=', result.orderId);
                return { success: true, orderId: result.orderId, maker: false };
            }
        } catch (e) {
            const errMsg = e?.message || '';
            
            // Position doesn't exist
            if (errMsg.includes('-2011') || errMsg.includes('Unknown order')) {
                console.warn('[order:market] position already closed');
                return { success: true, orderId: null, already_closed: true };
            }
            
            if (errMsg.includes('-1021') && attempt < 2) {
                console.log('[order:market] timestamp error, resyncing');
                await syncServerTime(env);
                await sleep(300);
                continue;
            }
            if ((errMsg.includes('502') || errMsg.includes('503') || errMsg.includes('504')) && attempt < 2) {
                console.log('[order:market] retry on server error', symbol);
                await sleep(500);
                continue;
            }
            console.error('[order:market] ✗ error:', symbol, errMsg.slice(0, 100));
            
            if (attempt === 2) {
                return { success: false, error: errMsg };
            }
        }
    }
    return { success: false };
}


// ---------------- API Wrappers with Retry ----------------
async function getAllOpenPositionsWithRetry(env) {
    for (let attempt = 0; attempt < 3; attempt++) {
        try {
            return await getAllOpenPositions(env);
        } catch (e) {
            if (e.message?.includes('-1021') && attempt < 2) {
                await syncServerTime(env);
                await sleep(300);
                continue;
            }
            if (attempt === 2) throw e;
            await sleep(500);
        }
    }
    return [];
}

// ✅ STEP 17F: Add expectedSide parameter
async function getPositionInfoWithRetry(symbol, env, expectedSide = null) {
    for (let attempt = 0; attempt < 3; attempt++) {
        try {
            return await getPositionInfo(symbol, env, expectedSide);
        } catch (e) {
            if (e.message?.includes('-1021') && attempt < 2) {
                await syncServerTime(env);
                await sleep(300);
                continue;
            }
            if (attempt === 2) return null;
            await sleep(500);
        }
    }
    return null;
}

async function getAvailableBalanceWithRetry(env) {
    for (let attempt = 0; attempt < 3; attempt++) {
        try {
            return await getAvailableBalance(env);
        } catch (e) {
            if (e.message?.includes('-1021') && attempt < 2) {
                await syncServerTime(env);
                await sleep(300);
                continue;
            }
            if (attempt === 2) return 0;
            await sleep(500);
        }
    }
    return 0;
}

async function getAllOrdersWithRetry(symbol, startTime, env) {
    for (let attempt = 0; attempt < 3; attempt++) {
        try {
            return await getAllOrders(symbol, startTime, env);
        } catch (e) {
            if (e.message?.includes('-1021') && attempt < 2) {
                await syncServerTime(env);
                await sleep(300);
                continue;
            }
            if (attempt === 2) return [];
            await sleep(500);
        }
    }
    return [];
}

// ---------------- Gist state ----------------
async function loadGistState(env) {
  const init = {
    v: 'ps-parity-worker-trailing-stop-enhanced-3.0',  // ✅ Match code c version
    pending: [],
    closed: [],
    equity: [],
    sym_stats_real: {},  // ✅ Add code c field
    critical_failures: [],  // ✅ Add code c field
    last_batch: null,
    last_monitor: null,
    lastReconcileTs: 0,  // ✅ Add code c field
    reconciled_at_startup: null  // ✅ Add code c field
  };

  if (!env.GIST_TOKEN || !env.GIST_ID) return init;

  try {
    const r = await fetch(`https://api.github.com/gists/${env.GIST_ID}`, {
      headers: {
        'Authorization': `Bearer ${env.GIST_TOKEN}`,
        'Accept': 'application/vnd.github+json',
        'User-Agent': 'cf-worker-async-maker/1.0'  // Keep our user-agent
      }
    });
    if (!r.ok) return init;
    const g = await r.json();
    const c = g.files?.['state.json']?.content;
    if (!c) return init;
    const s = JSON.parse(c);
    
    // ✅ Match code c: Initialize missing arrays/objects
    s.pending = Array.isArray(s.pending) ? s.pending : [];
    s.closed = Array.isArray(s.closed) ? s.closed : [];
    s.equity = Array.isArray(s.equity) ? s.equity : [];
    s.sym_stats_real = s.sym_stats_real || {};  // ✅ Code c default
    s.critical_failures = Array.isArray(s.critical_failures) ? s.critical_failures : [];  // ✅ Code c default

    GIST_STATE_CACHE = { ...init, ...s };
    return GIST_STATE_CACHE;
  } catch (e) {
    console.error('[gist] load error', e?.message || e);
    return init;
  }
}

async function getGistStateCache(env) {
    if (GIST_STATE_CACHE) return GIST_STATE_CACHE;
    return await loadGistState(env);
}

async function flushGistState(env) {
  if (!GIST_STATE_DIRTY || !GIST_STATE_CACHE) return;
  if (!env.GIST_TOKEN || !env.GIST_ID) return;

  try {
    // ✅ Code c: Add last_monitor timestamp
    GIST_STATE_CACHE.last_monitor = {
      ts: new Date().toISOString(),
      ts_ms: Date.now()
    };

    const r = await fetch(`https://api.github.com/gists/${env.GIST_ID}`, {
      method: 'PATCH',
      headers: {
        'Authorization': `Bearer ${env.GIST_TOKEN}`,
        'Accept': 'application/vnd.github+json',
        'Content-Type': 'application/json',
        'User-Agent': 'cf-worker-async-maker/1.0'  // Keep our user-agent
      },
      body: JSON.stringify({
        files: { 'state.json': { content: JSON.stringify(GIST_STATE_CACHE, null, 2) } }
      })
    });
    if (!r.ok) {
      const t = await r.text().catch(() => '');
      console.error('[gist] save failed:', r.status, t.slice(0, 200));
    } else {
      GIST_STATE_DIRTY = false;
    }
  } catch (e) {
    console.error('[gist] save error:', e?.message || e);
  }
}

async function logCriticalFailure(symbol, reason, position, env) {
  try {
    const state = await getGistStateCache(env);
    
    const failure = {
      ts: new Date().toISOString(),
      ts_ms: Date.now(),
      symbol: symbol,
      reason: reason,
      position: {
        side: position?.side || 'unknown',
        size: position?.size || 0,
        avgPrice: position?.avgPrice || position?.markPrice || 0,
        idea_id: position?.idea_id || null
      },
      alert: '🚨 MANUAL INTERVENTION REQUIRED'  // ✅ Code c exact
    };
    
    state.critical_failures = state.critical_failures || [];
    state.critical_failures.push(failure);
    state.critical_failures = state.critical_failures.slice(-20);  // ✅ Code c: Limit to last 20
    GIST_STATE_DIRTY = true;
    
    if (shouldEnableCriticalAlerts(env)) {  // ✅ Use the helper from Step 6A
      console.error('[critical] 🚨🚨🚨', symbol, reason, failure);
    }
  } catch (e) {
    console.error('[critical] failed to log', e?.message || e);
  }
}

// ============================================================
// BIDIRECTIONAL SUPPORT HELPERS
// ============================================================

// Generate unique position key (client_order_id or symbol+side)
function getPositionKey(p) {
  if (p.client_order_id) return `cid:${p.client_order_id}`;
  const symbol = symbolSafe(p);
  const side = p.side_exchange || p.side || 'unknown';
  return `${symbol}:${side}`;
}

function getPositionKeyFromLive(symbol, side) {
  return `${symbol}:${side}`;
}

// Enable hedge mode (dual-side positions)
async function ensureHedgeMode(env) {
  try {
    const r = await binanceRequest('/fapi/v1/positionSide/dual', {}, true, 'GET', env);
    const dual = r?.dualSidePosition === true || r?.dualSidePosition === 'true';
    
    if (!dual) {
      console.log('[hedge] Enabling hedge mode...');
      await binanceRequest('/fapi/v1/positionSide/dual', { dualSidePosition: 'true' }, true, 'POST', env);
      
      // ✅ VERIFY it worked
      await sleep(500);
      const verify = await binanceRequest('/fapi/v1/positionSide/dual', {}, true, 'GET', env);
      const verifyDual = verify?.dualSidePosition === true || verify?.dualSidePosition === 'true';
      
      if (!verifyDual) {
        throw new Error('CRITICAL: Cannot enable hedge mode - dual positions impossible');
      }
      console.log('[hedge] ✓ Enabled and verified dualSidePosition=true');
    } else {
      console.log('[hedge] ✓ Hedge mode already enabled');
    }
  } catch (e) {
    console.error('[hedge] ✗ Failed to enable hedge mode:', e?.message || e);
    // ✅ RE-THROW to stop execution
    throw new Error(`HEDGE MODE REQUIRED: ${e?.message || e}`);
  }
}

// Cancel orders for ONE side only (not both)
async function cancelSideOrders(symbol, posSide, env) {
  try {
    const openOrders = await binanceRequest('/fapi/v1/openOrders', { symbol }, true, 'GET', env);
    await sleep(150);
    const toCancel = (openOrders || []).filter(o => o.positionSide === posSide);
    
    for (const o of toCancel) {
      await binanceRequest('/fapi/v1/order', { symbol, orderId: o.orderId }, true, 'DELETE', env);
      await sleep(120);
    }
    
    console.log(`[cancel:side] ${symbol} canceled ${toCancel.length} orders for ${posSide}`);
  } catch (e) {
    if (!e.message?.includes('-2011')) {
      console.error('[cancel:side] error:', symbol, e?.message || e);
    }
  }
}

async function closePositionCached(symbolOrPos, exitData, env) {
  const state = await getGistStateCache(env);
  GIST_STATE_DIRTY = true;

  let position;
  if (typeof symbolOrPos === 'string') {
    const symbol = symbolOrPos;
    
    // ✅ STEP 6: Try client_order_id first, then symbol+side
    const cid = exitData.client_order_id || exitData.position_data?.client_order_id;
    let idx = -1;
    
    if (cid) {
      idx = state.pending.findIndex(p => p.client_order_id === cid && p.status === 'active');
      console.log(`[close] Looking for client_order_id=${cid}, found=${idx >= 0}`);
    }
    
    if (idx < 0) {
      const side = exitData.side || exitData.position_data?.side || exitData.position_data?.side_exchange;
      idx = state.pending.findIndex(p => 
        symbolSafe(p) === symbol && 
        p.status === 'active' &&
        (p.side === side || p.side_exchange === side)
      );
      console.log(`[close] Fallback lookup: symbol=${symbol} side=${side}, found=${idx >= 0}`);
    }
    
    if (idx >= 0) {
      position = state.pending.splice(idx, 1)[0];
    } else if (exitData.position_data) {
      position = exitData.position_data;
    } else {
      position = {
        symbol, side: 'unknown', entry_price: exitData.exit_price || 0,
        qty: 0, status: 'closed', ts_entry_ms: Date.now() - 1000,
        p_raw: null, p_lcb: null, regime: null
      };
    }
  } else {
    position = symbolOrPos;
    
    // ✅ STEP 7: Use client_order_id as primary key
    const cid = position.client_order_id;
    if (cid) {
      state.pending = state.pending.filter(p => p.client_order_id !== cid);
      console.log(`[close] Removed by client_order_id=${cid}`);
    } else {
      const id = position.idea_id;
      if (id) {
        state.pending = state.pending.filter(p => p.idea_id !== id);
        console.log(`[close] Removed by idea_id=${id}`);
      } else {
        const sym = symbolSafe(position);
        const side = position.side || position.side_exchange;
        state.pending = state.pending.filter(p => 
          !(symbolSafe(p) === sym && (p.side === side || p.side_exchange === side))
        );
        console.log(`[close] Removed by symbol+side: ${sym} ${side}`);
      }
    }
  }

  const symStr = symbolSafe(position);
  const base = symStr.replace(/(USDT|USDC|USD)$/, '');
  const entryPx = Number(position.entry_price || 0);
  const exitPx = Number(exitData.exit_price || entryPx || 0);
  const qty = Number(position.qty || 0);

  let pnl_bps = 0;
  let pnl_percent = 0;
  if (entryPx > 0 && exitPx > 0) {
    const isLong = (position.side === 'long' || position.side === 'Buy');
    const ret = isLong ? (exitPx / entryPx - 1) : (entryPx / exitPx - 1);
    pnl_bps = Math.round(ret * 10000);
    pnl_percent = ret * 100;
  }

  // Commission in bps (optional fields; default if missing)
  let commission_bps = 0;
  const entryComm = position.commission_quote_usdt || 0;
  const exitComm = exitData.commission_quote_usdt_exit || 0;
  const totalComm = entryComm + exitComm;
  if (entryPx > 0 && qty > 0) {
    const positionValue = entryPx * qty;
    if (positionValue > 0) commission_bps = Math.round((totalComm / positionValue) * 10000);
  }

  const closedEntry = {
    ...position,
    ...exitData,
    symbol: base,            // code c: base (no USDT)
    symbolFull: symStr,      // full symbol
    price_entry: entryPx,    // code c naming
    price_exit: exitPx,      // code c naming
    pnl_bps,
    pnl_percent,
    reconciliation: 'exchange_trade_history',
    status: 'closed',        // ensure closed status
    learned: false,
    trade_details: {         // code c trade details
      commission_bps,
      commission_quote_usdt: totalComm,
      commission_asset_entry: position.commission_asset_entry || 'USDT',
      commission_asset_exit: exitData.commission_asset_exit || 'USDT',
      maker_taker_entry: position.maker_taker_entry || 'unknown',
      maker_taker_exit: exitData.maker_taker_exit || 'unknown',
      slip_realized_bps: position.slip_realized_bps || 0,
      fingerprint_entry: position.fingerprint_entry || '',
      fingerprint_exit: exitData.fingerprint_exit || ''
    }
  };

  // code c: append without slice
  state.closed = [...state.closed, closedEntry];

  // code c: equity only has ts_ms and pnl_bps
  state.equity = [...state.equity, {
    ts_ms: exitData.ts_exit_ms,
    pnl_bps
  }];

  // code c: update sym_stats_real
  if (!state.sym_stats_real[base]) {
    state.sym_stats_real[base] = { n: 0, wins: 0, pnl_sum: 0 };
  }
  state.sym_stats_real[base].n += 1;
  state.sym_stats_real[base].pnl_sum += pnl_bps;
  if (pnl_bps > 0) state.sym_stats_real[base].wins += 1;

  state.lastReconcileTs = Date.now();

  console.log(`[close] ${symStr} ${position.side} → ${exitData.exit_reason} = ${pnl_bps} bps (${pnl_percent.toFixed(2)}%)`);
}


function symbolSafe(p) {
    return (p.symbol || p.symbol_full || p.symbolFull || (p.base ? `${p.base}USDT` : '') || '').toString();
}

// ---------------- Binance API ----------------
let EXINFO_CACHE = { ts: 0, symbols: [], bySymbol: {} };
let MARKET_INFO_CACHE = new Map();

function canonicalBase(b) {
    return (b || '').toString().toUpperCase().replace(/USDT$/, '').trim();
}

function parseAliasMap(env) {
    try {
        const raw = env.SYMBOL_ALIASES || '{}';
        const m = JSON.parse(raw);
        const out = {};
        for (const [k, arr] of Object.entries(m || {})) {
            out[k.toUpperCase()] = (arr || []).map(x => canonicalBase(x));
        }
        return out;
    } catch { return {}; }
}

const DEFAULT_ALIAS_MAP = {
    FLOKI: ['FLOKI', '1000FLOKI'],
    PEPE: ['PEPE', '1000PEPE'],
    SHIB: ['1000SHIB', 'SHIB'],
    BONK: ['BONK', '1000BONK'],
    SATS: ['SATS', '1000SATS'],
    CAT: ['CAT', '1000CAT'],
    RATS: ['RATS', '1000RATS']
};

async function getExSymbols(env) {
    const FRESH_MS = 10 * 60 * 1000;
    if (EXINFO_CACHE.symbols.length && (Date.now() - EXINFO_CACHE.ts < FRESH_MS)) {
        return EXINFO_CACHE.symbols;
    }
    try {
        const ex = await binanceRequest('/fapi/v1/exchangeInfo', {}, false, 'GET', env);
        const symbols = (ex.symbols || [])
            .filter(s => s.status === 'TRADING' && s.quoteAsset === 'USDT')
            .map(s => s.symbol);

        const bySymbol = {};
        for (const s of ex.symbols || []) {
            bySymbol[s.symbol] = s;
        }

        EXINFO_CACHE = { ts: Date.now(), symbols, bySymbol };
        return symbols;
    } catch (e) {
        console.error('[exsymbols] error', e?.message || e);
        return EXINFO_CACHE.symbols || [];
    }
}

async function resolveSymbolSmart(baseOrFull, env) {
    const base0 = canonicalBase(baseOrFull);
    if (!base0) return null;

    const aliasMap = { ...DEFAULT_ALIAS_MAP, ...parseAliasMap(env) };
    const candidates = [];
    const pushUnique = s => { if (s && !candidates.includes(s)) candidates.push(s); };

    const noNum = base0.replace(/^\d+/, '');

    pushUnique(`${base0}USDT`);
    if (noNum !== base0) {
        pushUnique(`${noNum}USDT`);
        pushUnique(`1000${noNum}USDT`);
        pushUnique(`100${noNum}USDT`);
    } else {
        pushUnique(`1000${base0}USDT`);
        pushUnique(`100${base0}USDT`);
    }

    const al = aliasMap[base0] || [];
    for (const a of al) {
        const canonical = canonicalBase(a);
        pushUnique(`${canonical}USDT`);
        pushUnique(`1000${canonical}USDT`);
    }

    const exSyms = await getExSymbols(env);
    const found = candidates.find(s => exSyms.includes(s));
    if (found) return found;

    const variants = new Set([base0, noNum, `1000${noNum}`, `100${noNum}`].map(x => x.toUpperCase()));
    const fuzzy = exSyms.find(s => {
        if (!s.endsWith('USDT')) return false;
        const b = s.slice(0, -4);
        const bNoNum = b.replace(/^\d+/, '');
        return variants.has(b.toUpperCase()) || variants.has(bNoNum.toUpperCase());
    });

    return fuzzy || null;
}

async function getMarketInfoCached(symbol, env) {
    const CACHE_MS = 5 * 60 * 1000;
    const cached = MARKET_INFO_CACHE.get(symbol);
    if (cached && (Date.now() - cached.ts < CACHE_MS)) {
        return cached.data;
    }

    const data = { valid: false };
    try {
        let symbolInfo = EXINFO_CACHE.bySymbol[symbol];

        if (!symbolInfo) {
            const ticker = await binanceRequest('/fapi/v1/ticker/price', { symbol }, false, 'GET', env);
            data.price = parseFloat(ticker.price);

            const exchangeInfo = await binanceRequest('/fapi/v1/exchangeInfo', {}, false, 'GET', env);
            symbolInfo = (exchangeInfo.symbols || []).find(s => s.symbol === symbol);
            if (!symbolInfo) return data;
        } else {
            const ticker = await binanceRequest('/fapi/v1/ticker/price', { symbol }, false, 'GET', env);
            data.price = parseFloat(ticker.price);
        }

        const lotFilter = (symbolInfo.filters || []).find(f => f.filterType === 'LOT_SIZE') || {};
        data.stepSize = parseFloat(lotFilter.stepSize || '0.0001');
        data.minQty = parseFloat(lotFilter.minQty || '0');

        const stepStr = (lotFilter.stepSize || '0.0001').toString();
        data.qtyDecimals = stepStr.includes('.') ? stepStr.split('.')[1].replace(/0+$/, '').length : 0;

        const priceFilter = (symbolInfo.filters || []).find(f => f.filterType === 'PRICE_FILTER') || {};
        data.tickSize = parseFloat(priceFilter.tickSize || '0.0001');
        const tickStr = (priceFilter.tickSize || '0.0001').toString();
        data.priceDecimals = tickStr.includes('.') ? tickStr.split('.')[1].replace(/0+$/, '').length : 2;

        data.valid = true;
        MARKET_INFO_CACHE.set(symbol, { ts: Date.now(), data });
    } catch (e) {
        console.error('[market] error:', symbol, e?.message || e);
    }

    return data;
}

async function getAllOpenPositions(env) {
    try {
        const positions = await binanceRequest('/fapi/v2/positionRisk', {}, true, 'GET', env);
        return (positions || [])
            .filter(p => Math.abs(parseFloat(p.positionAmt || '0')) > 0)
            .map(p => {
                // ✅ STEP 15: Use positionSide from Binance (LONG/SHORT/BOTH)
                const ps = (p.positionSide || '').toUpperCase();
                let side;
                
                if (ps === 'LONG') {
                    side = 'Buy';
                } else if (ps === 'SHORT') {
                    side = 'Sell';
                } else {
                    // Fallback for one-way mode
                    side = parseFloat(p.positionAmt) > 0 ? 'Buy' : 'Sell';
                }
                
                return {
                    symbol: p.symbol,
                    size: Math.abs(parseFloat(p.positionAmt)),
                    avgPrice: parseFloat(p.entryPrice),
                    markPrice: parseFloat(p.markPrice),
                    side,
                    positionSide: ps
                };
            });
    } catch (e) {
        console.error('[positions] error', e?.message || e);
        throw e;
    }
}

async function getAvailableBalance(env) {
    try {
        const balances = await binanceRequest('/fapi/v2/balance', {}, true, 'GET', env);
        const usdt = (balances || []).find(b => b.asset === 'USDT');
        return parseFloat(usdt?.availableBalance || 0);
    } catch (e) {
        console.error('[balance] error', e?.message || e);
        throw e;
    }
}

async function setLeverage(symbol, leverage, env) {
    try {
        await binanceRequest('/fapi/v1/leverage', { symbol, leverage }, true, 'POST', env);
    } catch (e) {
        if (!e.message?.includes('-4028')) {
            console.error('[leverage] error:', symbol, e?.message || e);
        }
    }
}

// ✅ STEP 16: Add expectedSide parameter to get correct position in hedge mode
async function getPositionInfo(symbol, env, expectedSide = null) {
    try {
        const positions = await binanceRequest('/fapi/v2/positionRisk', {}, true, 'GET', env);
        const symbolPositions = (positions || []).filter(p => 
            p.symbol === symbol && 
            Math.abs(parseFloat(p.positionAmt)) !== 0
        );
        
        if (!symbolPositions.length) return null;
        
        // If expectedSide provided, match by positionSide
        let pos = null;
        if (expectedSide) {
            const expectedPosSide = (expectedSide === 'Buy' || expectedSide === 'long') ? 'LONG' : 'SHORT';
            pos = symbolPositions.find(p => 
                (p.positionSide || '').toUpperCase() === expectedPosSide
            );
        }
        
        // Fallback: take first position
        if (!pos) pos = symbolPositions[0];
        
        const ps = (pos.positionSide || '').toUpperCase();
        let side;
        if (ps === 'LONG') {
            side = 'Buy';
        } else if (ps === 'SHORT') {
            side = 'Sell';
        } else {
            side = parseFloat(pos.positionAmt) > 0 ? 'Buy' : 'Sell';
        }
        
        return {
            size: Math.abs(parseFloat(pos.positionAmt)),
            avgPrice: parseFloat(pos.entryPrice),
            markPrice: parseFloat(pos.markPrice),
            side,
            positionSide: ps
        };
    } catch (e) {
        console.error('[position] error:', symbol, e?.message || e);
        throw e;
    }
    return null;
}

async function getAllOrders(symbol, startTime, env) {
    try {
        return await binanceRequest('/fapi/v1/allOrders', { symbol, startTime }, true, 'GET', env);
    }
    catch (e) {
        console.error('[allorders] error', symbol, e?.message || e);
        throw e;
    }
}

async function cancelAllOrders(symbol, env) {
    try {
        await binanceRequest('/fapi/v1/allOpenOrders', { symbol }, true, 'DELETE', env);
    } catch (e) {
        if (!e.message?.includes('-2011')) {
            console.error('[cancel] error:', symbol, e?.message || e);
        }
    }
}

async function binanceRequest(endpoint, params = {}, signed = false, method = 'GET', env) {
    const baseUrl = (env.BINANCE_BASE_URL || '').trim();
    const apiKey = (env.BINANCE_API_KEY || '').trim();
    const secretKey = (env.BINANCE_SECRET_KEY || '').trim();

    if (!baseUrl) throw new Error('Missing BINANCE_BASE_URL');

    let url = `${baseUrl}${endpoint}`;
    const qp = new URLSearchParams();
    for (const [k, v] of Object.entries(params || {})) {
        if (v !== undefined && v !== null) qp.set(k, String(v));
    }

    if (signed) {
        qp.set('timestamp', String(getTimestamp()));
        qp.set('recvWindow', String(RECV_WINDOW));
        const qs = qp.toString();
        const sig = await createSignature(qs, secretKey);
        url = `${url}?${qs}&signature=${sig}`;
    } else {
        const q = qp.toString();
        url = q ? `${url}?${q}` : url;
    }

    const headers = { 'X-MBX-APIKEY': apiKey, 'Content-Type': 'application/json' };
    const res = await fetch(url, { method, headers });

    if (!res.ok) {
        const text = await res.text().catch(() => '');
        throw new Error(`Binance ${res.status}: ${text.slice(0, 220)}`);
    }
    return await res.json();
}

async function createSignature(queryString, secret) {
    const enc = new TextEncoder();
    const key = await crypto.subtle.importKey('raw', enc.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
    const sig = await crypto.subtle.sign('HMAC', key, enc.encode(queryString));
    return [...new Uint8Array(sig)].map(b => b.toString(16).padStart(2, '0')).join('');
}

// ---------------- Utils ----------------
function jsonResponse(data, status = 200) {
    return new Response(JSON.stringify(data), { status, headers: { 'Content-Type': 'application/json' } });
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function toNum(x, d) { const n = Number(x); return Number.isFinite(n) ? n : d; }
function toInt(x, d) { const n = parseInt(x, 10); return Number.isFinite(n) ? n : d; }
function floorToStep(qty, step) { if (!(step > 0)) return qty; return Math.floor(qty / step) * step; }
function formatQty(qty, dec) { return (qty ?? 0).toFixed(Math.max(0, dec || 0)).replace(/\.?0+$/, ''); }
function formatPx(px, dec) { return (px ?? 0).toFixed(Math.max(0, dec || 2)).replace(/\.?0+$/, ''); }
