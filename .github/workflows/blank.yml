name: Ideas Pusher (MEXC Ultimate 7.7.7)

on:
  repository_dispatch:
    types: [run_B]
  workflow_dispatch: {}

permissions:
  contents: read

concurrency:
  group: ideas-mexc-ultimate
  cancel-in-progress: true

jobs:
  push:
    runs-on: ubuntu-latest
    timeout-minutes: 12

    env:
      # Required secrets
      WORKER_PUSH_URL: ${{ secrets.WORKER_PUSH_URL }}
      PUSH_TOKEN: ${{ secrets.PUSH_TOKEN }}
      WORKER_PUSH_URL_V2: ${{ secrets.WORKER_PUSH_URL_V2 }}

      # Required for trade reconciliation
      MEXC_API_KEY: ${{ secrets.MEXC_API_KEY }}
      MEXC_SECRET_KEY: ${{ secrets.MEXC_SECRET_KEY }}

      # Optional (safe defaults inside the script)
      GIST_TOKEN: ${{ secrets.GIST_TOKEN }}
      GIST_ID: ${{ secrets.GIST_ID }}

      # Exchange + tuning knobs (via Repo/Org Variables)
      EXCHANGE: mexc
      MIN_QV_USD: ${{ vars.MIN_QV_USD }}
      TOP_N: ${{ vars.TOP_N }}
      MAX_SPREAD_BPS: ${{ vars.MAX_SPREAD_BPS }}
      EXP_LCB_MIN_BPS: ${{ vars.EXP_LCB_MIN_BPS }}
      FEES_BPS: ${{ vars.FEES_BPS }}
      NOTIONAL_USD: ${{ vars.NOTIONAL_USD }}
      MEXC_DEPTH_LIMIT: ${{ vars.MEXC_DEPTH_LIMIT }}
      OBI_TOPN: ${{ vars.OBI_TOPN }}
      ACTIVE_UTC_START: ${{ vars.ACTIVE_UTC_START }}
      ACTIVE_UTC_END: ${{ vars.ACTIVE_UTC_END }}
      MEXC_BASE: ${{ vars.MEXC_BASE }}
      FORCE_SIZE_BPS: ${{ vars.FORCE_SIZE_BPS }}

      # New Confidence Model Knobs
      DIRECTION: ${{ vars.DIRECTION }}
      MR_SLOPE_MAX_ATR50: ${{ vars.MR_SLOPE_MAX_ATR50 }}
      MR_BREAKOUT_DC_N: ${{ vars.MR_BREAKOUT_DC_N }}
      MR_BREAKOUT_RET15_ATR: ${{ vars.MR_BREAKOUT_RET15_ATR }}
      MR_LOCKOUT_SEC: ${{ vars.MR_LOCKOUT_SEC }}
      MAX_COST_BPS_HARD: ${{ vars.MAX_COST_BPS_HARD }}
      MIN_VOL_1H_USD: ${{ vars.MIN_VOL_1H_USD }}
      DEPTH_1P_MIN_USD: ${{ vars.DEPTH_1P_MIN_USD }}
      NO_LONG_DOWN_ENABLE: ${{ vars.NO_LONG_DOWN_ENABLE }}
      LONG_DOWN_RSI_CUTOFF: ${{ vars.LONG_DOWN_RSI_CUTOFF }}
      LONG_MR_DIVERGENCE_REQ: ${{ vars.LONG_MR_DIVERGENCE_REQ }}
      LONG_EDGE_MULT_MIN: ${{ vars.LONG_EDGE_MULT_MIN }}
      LONG_BOUNCE_SL_ATR: ${{ vars.LONG_BOUNCE_SL_ATR }}
      LONG_BOUNCE_TP_ATR: ${{ vars.LONG_BOUNCE_TP_ATR }}
      LONG_SIMILARITY_RHO: ${{ vars.LONG_SIMILARITY_RHO }}

      # New (MTF + Provenance + Align)
      MIN_TF_ALIGN: ${{ vars.MIN_TF_ALIGN }} # default handled in script
      TF_WEIGHTS: ${{ vars.TF_WEIGHTS }}     # e.g. {"1m":0.1,"5m":0.3,"15m":0.25,"1h":0.2,"4h":0.15}
      STRATEGY_NAME: ${{ vars.STRATEGY_NAME }}
      MODEL_VERSION: ${{ vars.MODEL_VERSION }}
      SCHEMA_VERSION: ${{ vars.SCHEMA_VERSION }}
      GIT_SHA: ${{ github.sha }}

    steps:
      - name: Setup Node 20
        uses: actions/setup-node@v4
        with:
          node-version: 20

      - name: Run pusher (always-push + clear logs)
        shell: bash
        run: |
          set -euo pipefail

          # Hard fail only on true misconfig (so setup problems aren't hidden)
          if [[ -z "${WORKER_PUSH_URL:-}" || -z "${PUSH_TOKEN:-}" ]]; then
            echo "[gha] Missing WORKER_PUSH_URL or PUSH_TOKEN"
            exit 1
          fi

          set +e
          node - <<'NODE'
          (async ()=>{
          'use strict';
          const crypto = require('crypto');
          const MODEL_VERSION=(process.env.MODEL_VERSION||"6.4");
          const UA = `gh-actions-ideas-mexc-ultimate/${MODEL_VERSION} (+https://github.com/)`;

          // ---------------- Utils ----------------
          const clamp=(x,a,b)=>Math.max(a,Math.min(b,x));
          const tanh=(x)=>Math.tanh(x);
          const sum=(a)=>a.reduce((x,y)=>x+y,0);
          const mean=(a)=>a.length?sum(a)/a.length:0;
          const std=(a)=>{ const m=mean(a); let v=0; for(const x of a) v+=(x-m)*(x-m); return a.length>1?Math.sqrt(v/(a.length-1)):0; };
          const ema=(arr,p)=>{ if(!arr||arr.length<p) return null; const k=2/(p+1); let e=arr.slice(0,p).reduce((a,b)=>a+b,0)/p; for(let i=p;i<arr.length;i++) e=arr[i]*k+e*(1-k); return e; };
          const rsi=(cl,p=14)=>{ if(!cl||cl.length<=p) return null; let g=0,l=0; for(let i=1;i<=p;i++){ const d=cl[i]-cl[i-1]; g+=Math.max(0,d); l+=Math.max(0,-d); } let ag=g/p, al=l/p; for(let i=p+1;i<cl.length;i++){ const d=cl[i]-cl[i-1]; ag=(ag*(p-1)+Math.max(0,d))/p; al=(al*(p-1)+Math.max(0,-d))/p; } const rs=al===0?100:ag/al; return 100-100/(1+rs); };
          const computeADX_ATR=(h,l,c,p=14)=>{ const n=c.length; if(n<p+2) return null; const TR=[],plusDM=[],minusDM=[]; for(let i=1;i<n;i++){ const up=h[i]-h[i-1], down=l[i-1]-l[i]; plusDM.push((up>down&&up>0)?up:0); minusDM.push((down>up&&down>0)?down:0); TR.push(Math.max(h[i]-l[i], Math.abs(h[i]-c[i-1]), Math.abs(l[i]-c[i-1]))); } let trN=0,pdmN=0,ndmN=0; for(let i=0;i<p;i++){ trN+=TR[i]; pdmN+=plusDM[i]; ndmN+=minusDM[i]; } let pDI=100*(pdmN/(trN||1)), nDI=100*(ndmN/(trN||1)); let dx=100*Math.abs(pDI-nDI)/((pDI+nDI)||1), adx=dx; for(let i=p;i<TR.length;i++){ trN=trN-(trN/p)+TR[i]; pdmN=pdmN-(pdmN/p)+plusDM[i]; ndmN=ndmN-(ndmN/p)+minusDM[i]; pDI=100*(pdmN/(trN||1)); nDI=100*(ndmN/(trN||1)); dx=100*Math.abs(pDI-nDI)/((pDI+nDI)||1); adx=((adx*(p-1))+dx)/p; } const atr=trN/p; return { adx, atr, trLast: TR.at(-1) }; };
          const vwapAnchored=(h,l,c,v,win)=>{ if(!c?.length) return null; const n=c.length,s=Math.max(0,n-win); let pv=0,vv=0; for(let i=s;i<n;i++){ const tp=(h[i]+l[i]+c[i])/3; const vol=+v[i]||0; pv+=tp*vol; vv+=vol; } return vv>0?pv/vv:c.at(-1); };
          const corr=(a,b)=>{ const n=Math.min(a?.length||0,b?.length||0); if(n<5) return 0; const as=a.slice(-n), bs=b.slice(-n); const ma=mean(as), mb=mean(bs); let num=0,da=0,db=0; for(let i=0;i<n;i++){ const xa=as[i]-ma, xb=bs[i]-mb; num+=xa*xb; da+=xa*xa; db+=xb*xb; } const den=Math.sqrt(da*db)||1; return num/den; };
          const wilsonLCB=(p,n,z=1.34)=>{ if(n<=0) return p; const z2=z*z; const a=p + z2/(2*n); const b=z*Math.sqrt((p*(1-p)+z2/(4*n))/n); const c=1+z2/n; return clamp((a-b)/c, 0, 1); };
          const sleep=(ms)=>new Promise(r=>setTimeout(r,ms));
          const jitter=(ms)=>ms + Math.floor(Math.random()*ms*0.25);
          const log=(...a)=>console.log("[gha]",...a);
          const softmax=(arr,t=20)=>{ const m=Math.max(...arr,0); const ex=arr.map(x=>Math.exp((x-m)/t)); const s=ex.reduce((a,b)=>a+b,0)||1; return ex.map(x=>x/s); };
          const sigmoid=(z)=>1/(1+Math.exp(-z));
          const logit=(p)=>{ const eps=1e-6; const pp=Math.min(1-eps,Math.max(eps,p)); return Math.log(pp/(1-pp)); };
          let bmSpare=null;
          const randn=()=>{ if(bmSpare!=null){ const v=bmSpare; bmSpare=null; return v; } let u=0,v=0; while(u===0) u=Math.random(); while(v===0) v=Math.random(); const r=Math.sqrt(-2*Math.log(u)); const th=2*Math.PI*v; bmSpare=r*Math.sin(th); return r*Math.cos(th); };
          const randt=(df)=>{ if(df<=2) df=2.01; const z=randn(); let x2=0; for(let i=0;i<Math.floor(df);i++){ const z2=randn(); x2+=z2*z2; } const frac=df-Math.floor(df); if(frac>0){ const zf=randn(); x2+=frac*zf*zf; } const chi=x2||1e-6; return z / Math.sqrt(chi/df); };
          const kalman1D=(obs,{q=1e-5,r=1e-3,x0=null,p0=1e-2}={})=>{ if(!obs?.length) return []; let x=(x0==null?obs[0]:x0), p=p0; const out=[]; for(const z of obs){ p+=q; const K=p/(p+r); x=x+K*(z-x); p=(1-K)*p; out.push(x); } return out; };
          const pathEVHeavy=({entry,tp_bps,sl_bps,side,retSeries,mu,sigma,steps,N=192,cost_bps=10,t_df=5,boot_block=3})=>{ const up=(side==="long"), tpF=tp_bps/10000, slF=sl_bps/10000; let ev=0, wins=0; const rets=retSeries||[]; const boot1=()=>{ if(rets.length<8) return Array(steps).fill(0).map(()=> sigma*Math.sqrt(1/steps)*randn()); const out=[]; while(out.length<steps){ const start=Math.floor(Math.random()*(rets.length-boot_block)); for(let k=0;k<boot_block && out.length<steps;k++) out.push(rets[start+k]||0); } return out.slice(0,steps); }; for(let n=0;n<N;n++){ const mode=(n%2===0)?"t":"boot"; let S=entry, tp=up? entry*(1+tpF):entry*(1-tpF), sl=up? entry*(1-slF):entry*(1+slF); let pnl_bps=0,win=0; const rs=(mode==="t")? Array(steps).fill(0).map(()=> mu + sigma*randt(t_df)/Math.sqrt(steps)) : boot1(); for(let t=0;t<steps;t++){ S*=(1+rs[t]); if(up){ if(S>=tp){ pnl_bps=tp_bps-cost_bps; win=1; break;} if(S<=sl){ pnl_bps=-sl_bps-cost_bps; win=0; break;} } else { if(S<=tp){ pnl_bps=tp_bps-cost_bps; win=1; break;} if(S>=sl){ pnl_bps=-sl_bps-cost_bps; win=0; break;} } if(t===steps-1){ const ret=up? (S/entry-1):(entry/S-1); pnl_bps=Math.round(ret*10000)-cost_bps; win=pnl_bps>0?1:0; } } ev+=pnl_bps; wins+=win; } const p=wins/N, ev_mean=ev/N; return { p, ev_bps:Math.round(ev_mean) }; };

          // ---------- New Helpers ----------
          const uuidv4=()=>{ try{ return crypto.randomUUID(); }catch{ const b=crypto.randomBytes(16); b[6]=(b[6]&0x0f)|0x40; b[8]=(b[8]&0x3f)|0x80; const h=b.toString("hex"); return [h.slice(0,8),h.slice(8,12),h.slice(12,16),h.slice(16,20),h.slice(20)].join("-"); } };
          const mkClientOrderId = (prefix="mxu") => {
            const raw = (crypto.randomUUID ? crypto.randomUUID() : uuidv4()).replace(/-/g, "");
            const base = (prefix + "_" + raw).replace(/[^A-Za-z0-9_-]/g, "").slice(0, 30);
            return base.padEnd(Math.max(24, base.length), "0");
          };
          const hashObj=(o)=>{ const norm=(x)=>{ if(x&&typeof x==="object"&&!Array.isArray(x)){ const ks=Object.keys(x).sort(); const y={}; for(const k of ks) y[k]=norm(x[k]); return y; } if(Array.isArray(x)) return x.map(norm); return x; }; const s=JSON.stringify(norm(o)); return crypto.createHash("sha256").update(s).digest("hex"); };
          const toCSVLine=(row,keys)=> keys.map(k=>{ let v=row?.[k]; if(v==null) return ""; if(typeof v==="object") v=JSON.stringify(v); const s=String(v); return s.includes(",")||s.includes("\n")||s.includes('"')? `"${s.replace(/"/g,'""')}"`: s; }).join(",");

          const slopeBps=(arr,len=21)=>{ if(!arr?.length||arr.length<Math.max(5,len)) return 0; const s=arr.slice(-len); const n=s.length; const last=s[n-1]||1; const mx=(n-1)/2; let num=0,den=0; for(let i=0;i<n;i++){ const x=i, y=((s[i]||last)-last)/last*10000; num+=(x-mx)*y; den+=(x-mx)*(x-mx); } return den>0? num/den : 0; };

          const computeMTFIndicators=(km,weights)=>{ const TFs=["1m","3m","5m","15m","30m","1h","2h","4h","12h","1d"]; const wNorm=(()=>{ let out={}; let sw=0; for(const tf of TFs){ const w=+((weights&&weights[tf])||0); if(w>0){ out[tf]=w; sw+=w; } } if(sw<=0){ out={"1m":0.08,"3m":0.08,"5m":0.12,"15m":0.15,"30m":0.12,"1h":0.12,"2h":0.10,"4h":0.08,"12h":0.07,"1d":0.08}; sw=1; } for(const k of Object.keys(out)) out[k]=out[k]/sw; return out; })();
            const perTF={}; const agg={ rsi:0, adx:0, atr_bps:0, roc1:0, roc3:0 }; let ww=0; const retSeries={};
            for(const tf of Object.keys(wNorm)){
              const k=km?.[tf]; if(!k?.length) continue;
              const c=k.map(x=>+x[4]), h=k.map(x=>+x[2]), l=k.map(x=>+x[3]);
              const rsi14=rsi(c,14)||50;
              const adx14=(computeADX_ATR(h,l,c,14)?.adx)||0;
              const atr=(computeADX_ATR(h,l,c,14)?.atr)||0;
              const atr_bps=Math.round((atr/(c.at(-1)||1))*10000);
              const roc1=(c.at(-1)/(c.at(-2)||c.at(-1)) - 1);
              const roc3=(c.at(-1)/(c.at(-4)||c.at(-1)) - 1);
              perTF[tf]={ rsi:rsi14, adx:adx14, atr_bps, roc1, roc3, close:c.at(-1) };
              const w=wNorm[tf]; agg.rsi+=w*rsi14; agg.adx+=w*adx14; agg.atr_bps+=w*atr_bps; agg.roc1+=w*roc1; agg.roc3+=w*roc3; ww+=w;
              const rets=[]; for(let i=1;i<c.length;i++) rets.push(Math.log(c[i]/c[i-1])); retSeries[tf]=rets.slice(-120);
            }
            if(ww<=0) return { agg:{ rsi:50, adx:0, atr_bps:0, roc1:0, roc3:0 }, tfAlign:0, perTF:{} };
            const tfs=Object.keys(perTF); let pairs=0, acc=0;
            for(let i=0;i<tfs.length;i++) for(let j=i+1;j<tfs.length;j++){ const a=retSeries[tfs[i]]||[], b=retSeries[tfs[j]]||[]; const r=corr(a,b); acc+=r; pairs++; }
            const tfAlign=pairs>0? acc/pairs : 0;
            return { agg, tfAlign, perTF, weights:wNorm };
          };

          const getTradeFingerprint=(t)=>{ try{
            const sym=(t.symbol||"").toUpperCase();
            const side=(t.isBuyer===true||t.side==="BUY")?"B":"S";
            const oid=t.orderId!=null?t.orderId:(t.orderID!=null?t.orderID:"");
            const tid=t.id!=null?t.id:(t.tradeId!=null?t.tradeId:"");
            const q=Math.round((+t.qty||+t.executedQty||0)*1e8);
            const p=Math.round((+t.price||0)*1e8);
            const cmA=(t.commissionAsset||t.cma||"").toUpperCase();
            const mk=(t.isMaker===true)?"M":(t.isMaker===false)?"T":"?";
            return [sym,side,oid,tid,q,p,cmA,mk].join(":");
          }catch{ return String(t?.id||t?.orderId||Math.random()); } };

          const DROPPABLE_STATUSES = new Set(["planned","placed","pending","queued","ready"]);

          async function cleanupPendingBeforePlanning(state, heartbeat) {
            // Fetch open orders once
            let openAll = null;
            try { openAll = await fetchOpenOrders(); } catch (e) { log("openOrders err", e?.message || e); }

            // If we can’t confirm open orders, skip cleanup (don’t risk deleting live orders)
            if (!Array.isArray(openAll)) {
              log("cleanup: openOrders unavailable; skipping this cycle");
              return { dropped: 0, dropCIDs: [], scanned: state.pending?.length || 0, droppable: 0, orphans: 0 };
            }

            const byClient = new Map(openAll.map(o => [String(o.clientOrderId || o.clientOrderID || "").trim(), o]));
            const zombieSet = new Set((heartbeat?.details || [])
              .filter(d => d.type === "zombie")
              .map(d => String(d.client_order_id || "").trim())
              .filter(Boolean)
            );

            const keep = [];
            const dropCIDs = [];
            let scanned = 0, droppable = 0, orphans = 0;

            const idOf = (p)=> String(p?.client_order_id || p?.clientOrderId || p?.idea_id || "").trim();

            for (const p of (state.pending || [])) {
              scanned++;
              const cid = idOf(p);
              const status = (p.status || "planned").toLowerCase();
              const noEntryYet = !p.entry_ts_ms;
              const onEx = cid && byClient.has(cid);
              const orphan = !onEx || zombieSet.has(cid);
              const canDrop = DROPPABLE_STATUSES.has(status) && noEntryYet;

              if (canDrop) droppable++;
              if (canDrop && orphan) { orphans++; dropCIDs.push(cid || p.idea_id || ""); continue; }
              keep.push(p);
            }

            if (dropCIDs.length) {
              state.pending = keep;
              state.__drop_pending_cids = [...new Set([...(state.__drop_pending_cids || []), ...dropCIDs])];
              log(`cleanup: scanned=${scanned} droppable=${droppable} orphans=${orphans} dropped=${dropCIDs.length}`);
            } else {
              log(`cleanup: scanned=${scanned} droppable=${droppable} orphans=${orphans} dropped=0`);
            }
            return { dropped: dropCIDs.length, dropCIDs, scanned, droppable, orphans };
          }
          
          // ---------- Microstructure / Heavy Features helpers ----------
          const computeOBI=(depth, topN=12)=>{ if(!depth?.asks?.length || !depth?.bids?.length) return 0; const w=(i)=>Math.exp(-i*0.18); let bidNot=0, askNot=0; for(let i=0;i<Math.min(topN, depth.bids.length); i++){ const p=+depth.bids[i][0], q=+depth.bids[i][1]; if(p>0&&q>0) bidNot += p*q*w(i); } for(let i=0;i<Math.min(topN, depth.asks.length); i++){ const p=+depth.asks[i][0], q=+depth.asks[i][1]; if(p>0&&q>0) askNot += p*q*w(i); } return (bidNot+askNot>0) ? (bidNot-askNot)/(bidNot+askNot) : 0; };
          const vwapFillLevels=(levels,targetUSD)=>{ let remain=targetUSD,val=0,qty=0; for(const [ps,qs] of levels){ const p=+ps, q=+qs; if(!(p>0&&q>0)) continue; const can=p*q, take=Math.min(remain,can), tq=take/p; val+=p*tq; qty+=tq; remain-=take; if(remain<=1e-6) break; } return { px: qty>0? val/qty : null, filledUSD: (targetUSD-remain) }; };
          const slipFromDepth=(depth,mid,side,notionalUSD)=>{ if(!depth?.asks?.length || !depth?.bids?.length || !(mid>0)) return { slip_bps:null, fill_prob:null }; if(side==="long"){ const buy=vwapFillLevels(depth.asks, Math.max(50,notionalUSD)); const slip = buy.px? Math.max(0, Math.round((buy.px - mid)/mid*10000)) : null; const fill = Math.min(1, (buy.filledUSD||0)/Math.max(1,notionalUSD)); return { slip_bps: slip, fill_prob: +fill.toFixed(4) }; }else{ const sell=vwapFillLevels(depth.bids, Math.max(50,notionalUSD)); const slip = sell.px? Math.max(0, Math.round((mid - sell.px)/mid*10000)) : null; const fill = Math.min(1, (sell.filledUSD||0)/Math.max(1,notionalUSD)); return { slip_bps: slip, fill_prob: +fill.toFixed(4) }; } };
          const depthWithinPctUSD=(depth, mid, pct=0.01)=>{ if(!depth?.asks?.length || !depth?.bids?.length || !(mid>0)) return { bidsUSD:0, asksUSD:0 }; const askLim=mid*(1+pct), bidLim=mid*(1-pct); let asksUSD=0, bidsUSD=0; for(const [ps,qs] of depth.asks){ const p=+ps, q=+qs; if(!(p>0&&q>0)) continue; if(p<=askLim) asksUSD += p*q; else break; } for(const [ps,qs] of depth.bids){ const p=+ps, q=+qs; if(!(p>0&&q>0)) continue; if(p>=bidLim) bidsUSD += p*q; else break; } return { bidsUSD, asksUSD }; };
          const confidenceScore=({ p_cal, ev_bps, atr_bps, micro, regime, reliability, riskAdj })=>{ const ev_norm = atr_bps>0? ev_bps/atr_bps : 0; const ev_sig = 1/(1+Math.exp(-ev_norm)); const base = 0.6*p_cal + 0.4*ev_sig; const mult = (micro||1)*(regime||1)*(reliability||1)*(riskAdj||1); return Math.round(100*base*mult); };
          const microprice=(bid,ask,qb,qa)=>{ const d=(qb||1)+(qa||1); return (ask*qb + bid*qa)/d; };
          const ofiProxy=(d0,d1)=>{ const lev=(side,d)=>d?.[side]?.slice(0,3).map(x=>+x[1]||0)||[]; const sum=a=>a.reduce((x,y)=>x+y,0); if(!d0||!d1) return 0; return (sum(lev("bids",d1))-sum(lev("bids",d0))) - (sum(lev("asks",d1))-sum(lev("asks",d0))); };
          const bookSlope=(depth, mid)=>{ const toPts=(levels)=>{ let cum=0; const pts=[]; for(const [ps,qs] of (levels||[])){ const p=+ps,q=+qs; if(!(p>0&&q>0)) continue; cum+=p*q; const x=Math.log(Math.abs(p-mid)/mid + 1e-6), y=Math.log(cum+1); pts.push([x,y]); if(pts.length>=10) break; } return pts; }; if(!depth?.asks?.length||!depth?.bids?.length||!(mid>0)) return { slope:0, r2:0 }; const pts=toPts(depth.asks).concat(toPts(depth.bids)); if(pts.length<6) return { slope:0, r2:0 }; const xs=pts.map(p=>p[0]), ys=pts.map(p=>p[1]), mx=mean(xs), my=mean(ys); let num=0,den=0,vy=0; for(let i=0;i<xs.length;i++){ const dx=xs[i]-mx, dy=ys[i]-my; num+=dx*dy; den+=dx*dx; vy+=dy*dy; } return { slope: den>1e-9? num/den : 0, r2: den>1e-9 && vy>1e-9? (num*num)/(den*vy):0 }; };
          const reasonBuilder=(ctx)=>{ const out=[]; if((ctx.adx1h||0)>22) out.push({ factor:"ADX_1h", value:+(ctx.adx1h||0).toFixed(1), contribution:+Math.min(12,(ctx.adx1h-22)*0.6).toFixed(1) }); if((ctx.ofi30s||0)>0) out.push({ factor:"OFI_30s", value:+(ctx.ofi30s||0).toFixed(3), contribution:+Math.min(10, ctx.ofi30s*10).toFixed(1) }); if((ctx.spread_bps||0)>8) out.push({ factor:"Spread_bps", value:ctx.spread_bps, contribution:-Math.min(8,(ctx.spread_bps-8)) }); if((ctx.slip_bps_est||0)>4) out.push({ factor:"Slip_est_bps", value:ctx.slip_bps_est, contribution:-Math.min(6,(ctx.slip_bps_est-4)) }); if((ctx.tfAlign||0)>=0.7) out.push({ factor:"TF_alignment", value:+(ctx.tfAlign||0).toFixed(2), contribution:+6.0 }); if((ctx.mtf_rsi||0)>55) out.push({ factor:"MTF_RSI", value:+(ctx.mtf_rsi||0).toFixed(1), contribution:+3.0 }); return out.sort((a,b)=>Math.abs(b.contribution)-Math.abs(a.contribution)).slice(0,5); };
          const cholesky=(A)=>{ const n=A.length; const L=Array.from({length:n},()=>Array(n).fill(0)); for(let i=0;i<n;i++){ for(let j=0;j<=i;j++){ let s=0; for(let k=0;k<j;k++) s+=L[i][k]*L[j][k]; const v=A[i][j]-s; if(i===j){ if(v<=1e-12) return null; L[i][j]=Math.sqrt(Math.max(v,1e-12)); } else L[i][j]=v/(L[j][j]||1e-12); } } return L; };
          const choleskySafe=(S)=>{ const L=cholesky(S); if(L) return L; const n=S.length; const Sc=S.map((r,i)=> r.map((v,j)=> v + (i===j?1e-6:0))); return cholesky(Sc); };
          const sampleMultiT=(muVec,Sigma,df=6,M=1000)=>{ const n=muVec.length; const L=choleskySafe(Sigma); if(!L) return []; const out=[]; for(let m=0;m<M;m++){ const z=Array(n).fill(0).map(()=>randn()); const y=Array(n).fill(0); for(let i=0;i<n;i++){ let s=0; for(let k=0;k<=i;k++) s+=L[i][k]*z[k]; y[i]=s; } let chi=0; for(let i=0;i<df;i++){ const zc=randn(); chi+=zc*zc; } const scale=Math.sqrt(df/Math.max(1e-9,chi)); out.push(y.map((yi,i)=> (muVec[i]||0) + yi*scale)); } return out; };
          const portfolioVaR_ES_tCopula=(picks, df=6)=>{ if(!picks.length) return { VaR95_bps:0, ES95_bps:0 }; const n=picks.length, mu=Array(n).fill(0), sd=Array(n).fill(0); for(let i=0;i<n;i++){ const r5=picks[i].ret5||[]; const rMean=mean(r5), rStd=std(r5)||1e-4; const steps=Math.max(1, Math.round((picks[i].ttl_sec||600)/300)); mu[i]=rMean*steps*10000; sd[i]=rStd*Math.sqrt(steps)*10000; } const R=Array.from({length:n},()=>Array(n).fill(0)); for(let i=0;i<n;i++){ for(let j=i;j<n;j++) R[i][j]=corr(picks[i].ret5||[], picks[j].ret5||[]); R[i][i]=1; } const Sigma=Array.from({length:n},()=>Array(n).fill(0)); for(let i=0;i<n;i++) for(let j=0;j<n;j++) Sigma[i][j]=R[i][j]*sd[i]*sd[j]; const draws=sampleMultiT(mu,Sigma,df,1200); if(!draws.length) return { VaR95_bps:0, ES95_bps:0 }; const w0=picks.map(p=> p.size_bps||0); const ws=sum(w0)||1; const w=w0.map(x=> x/ws); const port=draws.map(d=> d.reduce((acc,di,ii)=> acc + w[ii]*di, 0)); port.sort((a,b)=>a-b); const idx=Math.floor(0.05*port.length); const VaR95=port[idx]||0; const ES=mean(port.slice(0,idx+1)); return { VaR95_bps:Math.round(VaR95), ES95_bps:Math.round(ES) }; };

          // ---------- HTTP + Concurrency ----------
          async function fetchWithTimeout(url,opts={},ms=12000){ const ac=new AbortController(); const t=setTimeout(()=>ac.abort(),ms); try{ return await fetch(url,{...opts,signal:ac.signal,headers:{"User-Agent":UA,...(opts?.headers||{})}});} finally{ clearTimeout(t);} }
          async function getJSON(url,ms=12000,tries=2){ for(let a=0;a<tries;a++){ try{ const r=await fetchWithTimeout(url,{},ms); if(r?.ok){ return await r.json(); } }catch{} await sleep(jitter(220)); } return null; }
          const cache={ k:new Map(), depth:new Map(), bt:new Map(), conv:new Map() };
          const TTL={ k: 60_000, depth: 8_000, bt: 5_000, conv: 60_000 };

          const getCached=(map,key,ttl)=>{ const e=map.get(key); return e && (Date.now()-e.ts<ttl)? e.v : null; };
          const setCached=(map,key,v)=>map.set(key,{ v, ts: Date.now() });

          // ---------- Config / ENV ----------
          const TARGET_EXCHANGE = "bybit_futures_testnet";
          const MARKET_TYPE     = "futures";
          const MARGIN_MODE     = "isolated";
          const LEVERAGE        = 1;
          const EXCHANGE=(process.env.EXCHANGE||"mexc").toLowerCase().replace("mexci","mexc");
          if(EXCHANGE!=="mexc"){ log("This build targets MEXC. Set EXCHANGE=mexc"); process.exit(1); }
          const MEXC_API_KEY = process.env.MEXC_API_KEY || "";
          const MEXC_SECRET_KEY = process.env.MEXC_SECRET_KEY || "";
          const MEXC_BASES=[process.env.MEXC_BASE,"https://api.mexc.com","https://www.mexc.com"].filter(Boolean);
          const MIN_TF_ALIGN=(Number(process.env.MIN_TF_ALIGN||"0.60"));
          let TF_WEIGHTS={}; try{ TF_WEIGHTS=JSON.parse(process.env.TF_WEIGHTS||"{}"); }catch{ TF_WEIGHTS={}; }
          const STRATEGY_NAME=(process.env.STRATEGY_NAME||"mexc-ultimate");
          const SCHEMA_VERSION=(process.env.SCHEMA_VERSION||"1");
          const GIT_SHA=(process.env.GIT_SHA||"unknown");
          
          // Calibration tuning
          const ADAPTIVE_LEARNING = true;
          const MIN_LEARNING_RATE = 0.001;
          const MAX_LEARNING_RATE = 0.08;
          const MOMENTUM_RATE = 0.9;
          const LEARNING_DECAY = 0.001;
          const CALIBRATION_RESET_THRESHOLD = 0.49;
          const COEFF_BOUND_A = 2.5;
          const COEFF_BOUND_B = 3.5;
          const CALIB_MIN_SAMPLES = 10;
          const CALIB_ERROR_WINDOW = 20;
          const CALIB_MIN_RESET_N = 30;

          const PUSHER_MODE=(process.env.PUSHER_MODE||"plan_only").toLowerCase();

          const MIN_QV_ENV = Number(process.env.MIN_QV_USD||"0") || 0;
          const UNIV_TARGET_MIN=100, UNIV_TARGET_MAX=120, DYN_QV_MIN=Number(process.env.DYN_QV_MIN||"10000000"), DYN_QV_STEP=5_000_000;
          const TOP_N=Number(process.env.TOP_N||"3"), MAX_SPREAD_BPS=Number(process.env.MAX_SPREAD_BPS||"12");
          const EXP_LCB_MIN_BPS_BASE=Number(process.env.EXP_LCB_MIN_BPS||"12");
          const EMA_FAST=21, EMA_SLOW=50, ADX_P=14, ATR_P=14, K1M=240, K5M=300, K15M=300, K1H=300, K4H=300;
          const VWAP_5M_WIN=36, COST_BPS=Number(process.env.FEES_BPS||"10"), NOTIONAL=Number(process.env.NOTIONAL_USD||"300");
          const DEPTH_LIMIT=Number(process.env.MEXC_DEPTH_LIMIT||"50"), OBI_TOPN=Number(process.env.OBI_TOPN||"12");
          const COOLDOWN_MS=3*60*60*1000, FLIP_GUARD_MS=30*60*1000;
          const DD_24H_LIMIT_BPS=-150, DD_PEAK_LIMIT_BPS=-300;
          const WILSON_Z=1.34, TARGET_PORT_RISK_BPS=52, TTL_MIN=540, TTL_MAX=1200;
          const S_H=process.env.ACTIVE_UTC_START?Number(process.env.ACTIVE_UTC_START):null;
          const E_H=process.env.ACTIVE_UTC_END?Number(process.env.ACTIVE_UTC_END):null;
          const nowH=new Date().getUTCHours();
          const todOK=(S_H==null||E_H==null)?true:(S_H<=E_H?(nowH>=S_H&&nowH<=E_H):(nowH>=S_H||nowH<=E_H));
          const STABLES=new Set(["USDT","USDC","USD","USDE","USDD","BUSD","FDUSD","TUSD","DRKUSD","DAI","USDP","PAX","USTC"]);
          const DIRECTION=(process.env.DIRECTION||"both").trim().toLowerCase();
          const MR_ADX_MAX_DEFAULT=20, MR_BREAKOUT_DC_N=Number(process.env.MR_BREAKOUT_DC_N||"120");
          const MR_BREAKOUT_RET15_ATR=Number(process.env.MR_BREAKOUT_RET15_ATR||"1.5"), MR_LOCKOUT_SEC=Number(process.env.MR_LOCKOUT_SEC||"1800");
          const MAX_COST_BPS_HARD=Number(process.env.MAX_COST_BPS_HARD||"15");
          const MIN_VOL_1H_USD=Number(process.env.MIN_VOL_1H_USD||"5000000"), DEPTH_1P_MIN_USD=Number(process.env.DEPTH_1P_MIN_USD||"200000");
          const NO_LONG_DOWN_ENABLE=(process.env.NO_LONG_DOWN_ENABLE||"true").toLowerCase()==="true";
          const LONG_DOWN_RSI_CUTOFF=Number(process.env.LONG_DOWN_RSI_CUTOFF||"40");
          const LONG_MR_DIVERGENCE_REQ=(process.env.LONG_MR_DIVERGENCE_REQ||"true").toLowerCase()==="true";
          const LONG_EDGE_MULT_MIN=Number(process.env.LONG_EDGE_MULT_MIN||"4");
          const LONG_BOUNCE_SL_ATR=Number(process.env.LONG_BOUNCE_SL_ATR||"0.50"), LONG_BOUNCE_TP_ATR=Number(process.env.LONG_BOUNCE_TP_ATR||"0.90");
          const LONG_SIMILARITY_RHO=Number(process.env.LONG_SIMILARITY_RHO||"0.84");

          const CONFIG_HASH=(()=>{ try{
            const knobs={
              DIRECTION, MR_BREAKOUT_DC_N, MR_BREAKOUT_RET15_ATR, MR_LOCKOUT_SEC,
              MAX_COST_BPS_HARD, MIN_VOL_1H_USD, DEPTH_1P_MIN_USD, NO_LONG_DOWN_ENABLE,
              LONG_DOWN_RSI_CUTOFF, LONG_MR_DIVERGENCE_REQ, LONG_EDGE_MULT_MIN,
              LONG_BOUNCE_SL_ATR, LONG_BOUNCE_TP_ATR, LONG_SIMILARITY_RHO, TOP_N, MAX_SPREAD_BPS,
              EXP_LCB_MIN_BPS_BASE, COST_BPS, NOTIONAL, DEPTH_LIMIT, OBI_TOPN, MIN_TF_ALIGN, TF_WEIGHTS
            };
            return hashObj(knobs);
          }catch{ return "cfg-unknown"; } })();

          // ---------- Worker endpoints ----------
          const PUSH_URL=process.env.WORKER_PUSH_URL||"", PUSH_TOKEN=process.env.PUSH_TOKEN||"";
          const PUSH_URL_V2 = process.env.WORKER_PUSH_URL_V2 || "";
          const HEALTH_URL=(()=>{ try{ const u=new URL(PUSH_URL); return `${u.origin}${u.pathname.replace(/\/signals\/push(\?.*)?$/,"/health")}`;}catch{return PUSH_URL.replace(/\/signals\/push(\?.*)?$/,"/health");}})();
          log("health GET", HEALTH_URL);
          try{ const r=await fetchWithTimeout(HEALTH_URL,{ headers:{ "Authorization":`Bearer ${PUSH_TOKEN}`,"User-Agent":UA }},5000); let t=""; try{ t=await r.text(); }catch{} log("health status", r?.status||"ERR", (t||"").slice(0,160)); }catch(e){ log("health error", e?.message||e); }

          // ---------- MEXC Adapter ----------
          async function pickMexcBase(){ for(const b of MEXC_BASES){ try{ const r=await fetchWithTimeout(`${b}/api/v3/time`,{},4000); if(r?.ok) return b; }catch{} } throw new Error("No healthy MEXC base"); }
          const BASE=await pickMexcBase();
          const api=(path,params={})=>{ const u=new URL(path,BASE); for(const [k,v] of Object.entries(params)){ if(v!==undefined&&v!==null) u.searchParams.set(k,String(v)); } return u.toString(); };
          async function mexcSignedRequest(path, params = {}, method = 'GET') {
              if (!MEXC_API_KEY || !MEXC_SECRET_KEY) return null;
              const timestamp = Date.now();
              const queryString = new URLSearchParams({ ...params, timestamp }).toString();
              const signature = crypto.createHmac('sha256', MEXC_SECRET_KEY).update(queryString).digest('hex');
              const url = new URL(path, BASE);
              url.search = queryString + `&signature=${signature}`;
              const options = { method, headers: { 'X-MEXC-APIKEY': MEXC_API_KEY, 'Content-Type': 'application/json', 'User-Agent': UA } };
              try {
                  const r = await fetchWithTimeout(url.toString(), options, 15000);
                  if (r.ok) return await r.json();
                  log(`MEXC signed request failed: ${r.status}`, (await r.text()||'').slice(0,100));
                  return null;
              } catch (e) {
                  log(`MEXC signed request error: ${e?.message||e}`);
                  return null;
              }
          }
          async function fetchMyTrades(symbol, startTime) { return await mexcSignedRequest('/api/v3/myTrades', { symbol, startTime, limit: 1000 }); }
          async function fetchOpenOrders(symbol) {
            const r = await mexcSignedRequest('/api/v3/openOrders', symbol?{symbol}:{});
            return Array.isArray(r) ? r : null; // null => unavailable
          }
          async function fetchK(sym,interval,limit,startTime,endTime){ const u=api("/api/v3/klines",{symbol:sym,interval,limit,startTime,endTime}); const r=await getJSON(u,10000,2); return Array.isArray(r)?r:null; }
          async function fetchDepth(symbol){ return await getJSON(api("/api/v3/depth",{symbol,limit:DEPTH_LIMIT}), 10000, 2); }
          async function fetchAll24hr(){
            let all=await getJSON(api("/api/v3/ticker/24hr"),10000,2); if(Array.isArray(all)&&all.length) return all;
            const exi=await getJSON(api("/api/v3/exchangeInfo"),10000,2); const syms=(exi?.symbols||[]).filter(s=>s.status==="TRADING").map(s=>s.symbol).slice(0,300);
            const out=[]; for(let i=0;i<syms.length;i+=24){ const chunk=syms.slice(i,i+24); const got=await Promise.all(chunk.map(s=>getJSON(api("/api/v3/ticker/24hr",{symbol:s}),6000,1))); for(const x of got) if(x) out.push(x); await sleep(180); } return out;
          }
          async function fetchBookTicker(symbol){ return await getJSON(api("/api/v3/ticker/bookTicker",{symbol}), 8000, 1); }

          async function getKCached(symbol,interval,limit,startTime,endTime){ const key=[symbol,interval,limit,startTime||"",endTime||""].join("|"); const c=getCached(cache.k,key,TTL.k); if(c) return c; const v=await fetchK(symbol,interval,limit,startTime,endTime); if(v) setCached(cache.k,key,v); return v; }
          async function getDepthCached(symbol){ const c=getCached(cache.depth,symbol,TTL.depth); if(c) return c; const v=await fetchDepth(symbol); if(v) setCached(cache.depth,symbol,v); return v; }
          async function getBookTickerCached(symbol){ const c=getCached(cache.bt,symbol,TTL.bt); if(c) return c; const v=await fetchBookTicker(symbol); if(v) setCached(cache.bt,symbol,v); return v; }
          async function getKMultiTF(symbol){ const pairs=[["1m",300],["3m",300],["5m",K5M],["15m",K15M],["30m",300],["1h",K1H],["2h",200],["4h",K4H],["12h",120],["1d",90]]; const out={}; for(const [tf,lim] of pairs){ out[tf]=await getKCached(symbol, tf, lim); } return out; }
          const tfInd=(k)=>{ if(!k?.length) return null; const h=k.map(x=>+x[2]), l=k.map(x=>+x[3]), c=k.map(x=>+x[4]); const adxatr=computeADX_ATR(h,l,c,14)||{adx:0,atr:0}; const up=c.at(-1)>=ema(c,21)&&c.at(-1)>=ema(c,50); return {adx:adxatr.adx||0,atr:adxatr.atr||0,up}; };

          // ---------- Conversion rate helper for commissions ----------
          async function getConversionRate(asset, quote="USDT"){
            try{
              if(!asset||asset.toUpperCase()===quote.toUpperCase()) return 1;
              const key=asset.toUpperCase()+"_"+quote.toUpperCase();
              const c=getCached(cache.conv,key,TTL.conv); if(c) return c;
              const sym1=asset.toUpperCase()+quote.toUpperCase();
              const sym2=quote.toUpperCase()+asset.toUpperCase();
              let px=null;
              let bt=await getBookTickerCached(sym1);
              if(bt?.askPrice) px=+bt.askPrice;
              if(px==null){ bt=await getBookTickerCached(sym2); if(bt?.bidPrice) px=1/(+bt.bidPrice||1); }
              if(px==null) return 1;
              setCached(cache.conv,key,px);
              return px;
            }catch{ return 1; }
          }

          // ---------- Secondary store stub (no-op) ----------
          async function pushAuditSecondary(path, payload){
            try{
              // Placeholder for an external blob store (S3, GCS, KV, etc.).
              // Intentionally a no-op to avoid failures impacting main loop.
              return { ok:true, path };
            }catch(e){ log("secondary store stub error", e?.message||e); return { ok:false, error:String(e) }; }
          }

          // ---------- Heartbeat & Zombie detection ----------
          async function heartbeatCheck(state){
            const res={ mismatches:0, zombies:0, details:[] };
            if(!MEXC_API_KEY||!MEXC_SECRET_KEY) return res;
            try{
              const openAll=await fetchOpenOrders();
              if (!Array.isArray(openAll)) return res; // don’t mark zombies on unavailable data

              const byClient=new Map();
              for(const o of openAll){ const cid=(o.clientOrderId||o.clientOrderID||"").trim(); if(cid) byClient.set(cid,o); }
              for(const p of state.pending||[]){
                const cid=(p.client_order_id||p.clientOrderId||"").trim();
                if(!cid) continue;
                const onEx=byClient.has(cid);
                if(!onEx){
                  res.zombies++; res.details.push({ type:"zombie", symbol:p.symbolFull, client_order_id:cid, ts_ms:p.ts_ms });
                }
              }
              res.mismatches=res.zombies;
            }catch(e){ log("heartbeat warn", e?.message||e); }
            return res;
          }
          // ---------- State (Gist) + Calibration + Audit ----------
          async function loadState(){
            const token=process.env.GIST_TOKEN, id=process.env.GIST_ID;
            const init={ v:"mexc-ultimate-6.4-audit-mtf",
              cooldown:{}, cooldown_side:{}, pending:[], equity:[], closed:[],
              mr_lockout:{}, sym_stats:{}, sym_stats_real:{}, calibCoeffs:{}, lastReconcileTs:0,
              calibration_holdout:[]
            };
            if(!token||!id) return { state:init, persist:null };
            try{
              const r=await fetchWithTimeout(`https://api.github.com/gists/${id}`,{
                headers:{Authorization:`Bearer ${token}`,"Accept":"application/vnd.github+json","User-Agent":UA}
              });
              if(!r.ok) return { state:init, persist:null };
              const etag=r.headers.get("etag");
              const g=await r.json();
              const c=g.files?.["state.json"]?.content;
              const s=c?JSON.parse(c):init;

              // ensure defaults
              if(!s.cooldown)s.cooldown={};
              if(!s.cooldown_side)s.cooldown_side={};
              if(!Array.isArray(s.pending))s.pending=[];
              if(!Array.isArray(s.closed))s.closed=[];
              if(!Array.isArray(s.equity))s.equity=[];
              if(!s.mr_lockout)s.mr_lockout={};
              if(!s.sym_stats)s.sym_stats={};
              if(!s.sym_stats_real)s.sym_stats_real={};
              if(!s.calibCoeffs)s.calibCoeffs={};
              if(!s.calibration_holdout)s.calibration_holdout=[];
              if(!s.lastReconcileTs)s.lastReconcileTs=0;

              return { state:s, persist:{ id, token, etag } };
            }catch(e){ log("loadState warn", e?.message||e); return { state:init, persist:null }; }
          }
          
          function mergeStates(remote, local) {
            const merged = { ...remote };

            // Honor Pusher-requested drops so remote.pending doesn't reintroduce them
            const dropSet = new Set((local.__drop_pending_cids || []).map(x => String(x).trim()).filter(Boolean));
            const id = (p) => String(p?.client_order_id || p?.clientOrderId || p?.idea_id || "").trim();

            // Pending: remote truth minus drops, plus any truly new local
            const remotePending = (remote.pending || []).filter(p => !dropSet.has(id(p)));
            const remoteIds = new Set(remotePending.map(id));
            const newLocalPending = (local.pending || []).filter(p => {
              const k = id(p);
              return k && !remoteIds.has(k);
            });
            merged.pending = [...remotePending, ...newLocalPending];

            // Worker-owned: keep remote as truth
            merged.equity = Array.isArray(remote.equity) ? remote.equity : [];
            merged.sym_stats_real = remote.sym_stats_real || {};
            merged.sym_stats = remote.sym_stats || {};
            merged.lastReconcileTs = remote.lastReconcileTs || 0;
            if ('fees' in remote) merged.fees = remote.fees;

            // Pusher-owned: keep local
            merged.cooldown = local.cooldown || {};
            merged.cooldown_side = local.cooldown_side || {};
            merged.mr_lockout = local.mr_lockout || {};
            merged.calibCoeffs = local.calibCoeffs || remote.calibCoeffs || {};

            // Closed: remote truth + overlay learned flags only
            const keyClosed = (c) => {
              const cid = c?.trade_details?.client_order_id || c.client_order_id || c.idea_id || "";
              const tx  = c?.ts_exit_ms || c?.exit_ts_ms || "";
              return `${cid}|${tx}`;
            };
            const localClosedMap = new Map((local.closed||[]).map(c => [keyClosed(c), c]));
            merged.closed = (remote.closed || []).map(rc => {
              const lc = localClosedMap.get(keyClosed(rc));
              if (!lc) return rc;
              const learned = Boolean(rc.learned || lc.learned);
              const learned_at_ts = learned ? (rc.learned_at_ts || lc.learned_at_ts || Date.now()) : undefined;
              return { ...rc, learned, learned_at_ts };
            });

            merged.version_ts = Date.now();

            log("[gha] mergeStates: remote_pending_after_drops", remotePending.length,
                        "appended_new_local", newLocalPending.length,
                        "merged_pending", merged.pending.length);

            return merged;
          }

          async function saveState(persist, state, auditOut, retries=2, blindFallback=true) {
            if (!persist) { log("WARN: No persist—skipping Gist save."); return; }

            const gistUrl = `https://api.github.com/gists/${persist.id}`;
            const mkFiles = (s) => ({ "state.json": { content: JSON.stringify(s) } });

            async function patch(files, etag) {
              const headers = {
                Authorization: `Bearer ${persist.token}`,
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "User-Agent": UA
              };
              if (etag) headers["If-Match"] = etag;
              return await fetchWithTimeout(gistUrl, {
                method: "PATCH",
                headers,
                body: JSON.stringify({ files })
              }, 15000);
            }

            let toSave = state;
            let etag = persist.etag || null;

            for (let attempt = 0; attempt <= retries; attempt++) {
              log("DEBUG: saveState attempt", attempt, "etag?", !!etag, "pending=", (toSave.pending||[]).length);
              const r = await patch(mkFiles(toSave), etag);
              if (r.ok) {
                const newEtag = r.headers.get("etag");
                persist.etag = newEtag || persist.etag;
                log("SUCCESS: Gist updated! Status=", r.status, "pending now=", (toSave.pending||[]).length);
                return;
              }

              const body = (await r.text().catch(()=> ""))?.slice(0, 300) || "";
              log("WARN: saveState failed status=", r.status, "body=", body);

              if (r.status === 429) {
                const ra = Number(r.headers.get("retry-after") || 2);
                log("WARN: Rate limited. Backing off", ra, "s");
                await sleep(ra * 1000);
                continue;
              }

              if (r.status === 412) {
                // Conflict: re-fetch + merge + retry
                const r2 = await fetchWithTimeout(gistUrl, {
                  headers: {
                    Authorization: `Bearer ${persist.token}`,
                    "Accept": "application/vnd.github+json",
                    "User-Agent": UA
                  }
                }, 12000);
                if (!r2.ok) {
                  log("ERROR: Refetch for merge failed status=", r2.status);
                  break;
                }
                const remoteEtag = r2.headers.get("etag");
                const remoteGist = await r2.json();
                const remoteContent = remoteGist.files?.["state.json"]?.content;
                const remoteState = remoteContent ? JSON.parse(remoteContent) : {};
                toSave = mergeStates(remoteState, state);
                etag = remoteEtag || null;
                await sleep(250 + Math.floor(Math.random()*250)); // small jitter
                continue;
              }

              // Other non-retryable errors: break and optionally blind-patch
              break;
            }

            if (blindFallback) {
              log("WARN: Falling back to blind PATCH (no If-Match).");
              
              // Pre-merge before blind patch to avoid clobbering Worker updates
              try {
                  const r2 = await fetchWithTimeout(gistUrl, {
                    headers: {
                      Authorization: `Bearer ${persist.token}`,
                      "Accept": "application/vnd.github+json",
                      "User-Agent": UA
                    }
                  }, 12000);

                  if (r2.ok) {
                    const remoteGist = await r2.json();
                    const remoteContent = remoteGist.files?.["state.json"]?.content;
                    const remoteState = remoteContent ? JSON.parse(remoteContent) : {};
                    toSave = mergeStates(remoteState, state);
                    log("DEBUG: Pre-blind-patch merge successful.");
                  } else {
                    log("WARN: Pre-blind-patch re-fetch failed status=", r2.status, ". Will save local state.");
                  }
              } catch (e) {
                  log("ERROR: Exception during pre-blind-patch re-fetch:", e?.message || String(e));
              }
              
              const r3 = await patch(mkFiles(toSave), null);
              if (r3.ok) {
                const newEtag = r3.headers.get("etag");
                persist.etag = newEtag || persist.etag;
                log("SUCCESS: Blind Gist update ok. pending=", (toSave.pending||[]).length);
              } else {
                const body = (await r3.text().catch(()=> ""))?.slice(0, 300) || "";
                log("ERROR: Blind save failed status=", r3.status, "body=", body);
              }
            }
          }
          
          const regFromKey = (k) => k?.includes("regime:trend") ? "trend" : k?.includes("regime:meanrevert") ? "meanrevert" : null;
          function learnFromGistRealFills(state){
            const learnable = (state.closed || []).filter(c =>
              c.reconciliation === "exchange_trade_history" &&
              !c.learned &&
              c.p_raw != null
            );
            
            if (learnable.length === 0) {
              log("📚 No new fills to learn from this cycle.");
              return;
            }

            log(`📚 Learning from ${learnable.length} real fills...`);
            
            let learnedCount = 0;
            for (const c of learnable) {
              const reg = c.regime || regFromKey(c.calib_key);
              if (!reg || !c.side) continue;

              const outcome = (c.pnl_bps || 0) > 0 ? 1 : 0;
              const result = outcome ? "WIN" : "LOSS";
              
              log(`📊 Trade: ${c.symbolFull || c.symbol} ${c.side} ${result} pnl=${c.pnl_bps}bps p_raw=${c.p_raw?.toFixed(3)}`);
              
              try { 
                updateCalibration(state, c.side, reg, c.p_raw, outcome); 
              }
              catch(e){ 
                log("⚠️  learnFromGistRealFills error:", e?.message||e); 
              }
              
              c.learned = true;
              c.learned_at_ts = Date.now();
              learnedCount++;
            }
            
            log(`✅ Learning complete: ${learnedCount} fills processed.`);
          }

          const getCoeffs = (state, side, regime) => {
            const key = `${side}_${regime}`;
            if (!state.calibCoeffs) state.calibCoeffs = {};
            const def = {
              a: 0,
              b: 1,
              n: 0,
              momentum_a: 0,
              momentum_b: 0,
              recent_errors: []
            };
            state.calibCoeffs[key] = { ...def, ...(state.calibCoeffs[key] || {}) };
            return state.calibCoeffs[key];
          };
          function validateCalibration(state) {
            const report = {};
            for (const key of Object.keys(state.calibCoeffs || {})) {
              const coeff = state.calibCoeffs[key];
              if (coeff.n < CALIB_MIN_SAMPLES) continue;

              const avg_error = mean(coeff.recent_errors || []);
              const is_healthy =
                avg_error < 0.4 &&
                Math.abs(coeff.a) < COEFF_BOUND_A * 0.75 &&
                Math.abs(coeff.b) < COEFF_BOUND_B * 0.75;

              report[key] = {
                n: coeff.n,
                a: coeff.a.toFixed(3),
                b: coeff.b.toFixed(3),
                avg_error: avg_error.toFixed(3),
                status: is_healthy ? "healthy" : "degraded"
              };
            }

            if (Object.keys(report).length > 0) {
              log("Calibration health:", JSON.stringify(report));
            }
            return report;
          }
          function updateCalibration(state, side, regime, pRaw, outcome) {
            if (pRaw == null || !isFinite(pRaw)) return;

            const coeff = getCoeffs(state, side, regime);

            // Store before state
            const before_a = coeff.a;
            const before_b = coeff.b;
            const before_n = coeff.n;

            // 1. Adaptive learning rate
            const lr = ADAPTIVE_LEARNING
              ? Math.max(
                  MIN_LEARNING_RATE,
                  MIN_LEARNING_RATE +
                    (MAX_LEARNING_RATE - MIN_LEARNING_RATE) / Math.sqrt(1 + coeff.n / 10)
                )
              : 0.05;

            // 2. Logit transform for better numerical properties
            const x = Math.max(-5, Math.min(5, logit(pRaw)));

            // 3. Compute prediction and error
            const z = coeff.a + coeff.b * x;
            const pHat = sigmoid(z);
            const err = outcome - pHat;

            // 4. Update momentum
            coeff.momentum_a = MOMENTUM_RATE * coeff.momentum_a + lr * err;
            coeff.momentum_b = MOMENTUM_RATE * coeff.momentum_b + lr * err * x;

            // 5. Clamp momentum to prevent explosion
            const MOMENTUM_CLAMP = 0.2;
            coeff.momentum_a = clamp(coeff.momentum_a, -MOMENTUM_CLAMP, MOMENTUM_CLAMP);
            coeff.momentum_b = clamp(coeff.momentum_b, -MOMENTUM_CLAMP, MOMENTUM_CLAMP);

            // 6. Apply momentum and weight decay to coefficients
            const a_next = (1 - LEARNING_DECAY) * coeff.a + coeff.momentum_a;
            const b_next = (1 - LEARNING_DECAY) * coeff.b + coeff.momentum_b;

            // 7. Clamp coefficients to bounds
            coeff.a = clamp(a_next, -COEFF_BOUND_A, COEFF_BOUND_A);
            coeff.b = clamp(b_next, -COEFF_BOUND_B, COEFF_BOUND_B);

            // 8. Reset momentum if bounds were hit
            const hit_bound_a = (a_next !== coeff.a);
            const hit_bound_b = (b_next !== coeff.b);
            if (hit_bound_a) coeff.momentum_a = 0;
            if (hit_bound_b) coeff.momentum_b = 0;

            // 9. Track recent errors for validation
            if (!coeff.recent_errors) coeff.recent_errors = [];
            coeff.recent_errors.push(Math.abs(err));
            if (coeff.recent_errors.length > CALIB_ERROR_WINDOW) {
              coeff.recent_errors.shift();
            }

            // 10. Auto-reset if performing worse than random
            const avg_error = mean(coeff.recent_errors);
            const will_reset = (avg_error > CALIBRATION_RESET_THRESHOLD && coeff.n > CALIB_MIN_RESET_N);

            // ========== VERBOSE LOGGING ==========
            log(`📚 LEARN [${side}_${regime}] n=${before_n}→${before_n+1} lr=${lr.toFixed(4)}`);
            log(`   Input: p_raw=${pRaw.toFixed(3)} outcome=${outcome} | Predicted: p_hat=${pHat.toFixed(3)} error=${err.toFixed(3)}`);
            log(`   Coeffs: a=${before_a.toFixed(4)}→${coeff.a.toFixed(4)} b=${before_b.toFixed(4)}→${coeff.b.toFixed(4)}`);
            if (hit_bound_a || hit_bound_b) {
              log(`   ⚠️  Hit bounds! a=${hit_bound_a} b=${hit_bound_b} (momentum reset)`);
            }
            log(`   Momentum: a=${coeff.momentum_a.toFixed(4)} b=${coeff.momentum_b.toFixed(4)}`);
            log(`   Avg_error=${avg_error.toFixed(3)} (window=${coeff.recent_errors.length})`);

            if (will_reset) {
              log(`   🔄 AUTO-RESET triggered! avg_error=${avg_error.toFixed(3)} > threshold=${CALIBRATION_RESET_THRESHOLD}`);
              coeff.a = 0;
              coeff.b = 1;
              coeff.momentum_a = 0;
              coeff.momentum_b = 0;
              coeff.recent_errors = [];
              coeff.n = 0; // restart learning
            } else {
              coeff.n++;
            }

            state.calibCoeffs[`${side}_${regime}`] = coeff;
          }
          const calibrateP=(state, side, regime, pRaw)=>{
            const coeff = getCoeffs(state, side, regime);
            if(pRaw==null || !isFinite(pRaw)) return 0.5; // safe fallback
            const x = Math.max(-5, Math.min(5, logit(pRaw)));
            return sigmoid(coeff.a + coeff.b * x);
          };
          const calibratorInfo=(state,side,regime)=>{ const coeff=getCoeffs(state,side,regime); return { type:"logistic", n:coeff.n, a:coeff.a, b:coeff.b }; };
          function getSymbolAdj(state, base) {
            const real = state.sym_stats_real?.[base];
            const s = real || state.sym_stats?.[base];
            if (!s || s.n < 6) return 1.0;
            const winRate = s.wins / s.n;
            const avgPnL = s.pnl_sum / s.n;
            let m = 1.0;
            if (winRate < 0.4) m *= 0.7;
            if (avgPnL < 0) m *= 0.8;
            return clamp(m, 0.5, 1.2);
          }

          // Build audit outputs from state (kept for diagnostics)
          const TRADE_CSV_KEYS=["ts_close_iso","idea_id","client_order_id","symbol","side","pnl_bps","entry_ts_ms","exit_ts_ms","entry_price","exit_price","qty","commission_bps","commission_quote_usdt","commission_asset_entry","commission_asset_exit","maker_taker_entry","maker_taker_exit","slip_realized_bps","reconciliation","fingerprint_entry","fingerprint_exit","calib_key","p_raw","p_pred","regime","source"];
          function buildAuditOutputs(state){
            const lastNClosed=(state.closed||[]).slice(-5000);
            const closedAudit=lastNClosed.map(x=>JSON.stringify(x)).join("\n");
            const lastHoldout=(state.calibration_holdout||[]).slice(-500);
            const disagreements=lastHoldout.map(x=>JSON.stringify(x)).join("\n");
            const rows=[];
            for(const c of lastNClosed){
              const td=c.trade_details||{};
              const row={
                ts_close_iso: new Date(c.ts_exit_ms||c.predicted_snapshot?.ts_ms||Date.now()).toISOString(),
                idea_id: td.idea_id||"",
                client_order_id: td.client_order_id||"",
                symbol: c.symbolFull||"",
                side: c.side||"",
                pnl_bps: c.pnl_bps,
                entry_ts_ms: c.ts_entry_ms||"",
                exit_ts_ms: c.ts_exit_ms||"",
                entry_price: c.price_entry||"",
                exit_price: c.price_exit||"",
                qty: c.qty||"",
                commission_bps: td.commission_bps!=null?td.commission_bps:"",
                commission_quote_usdt: td.commission_quote_usdt!=null?td.commission_quote_usdt:"",
                commission_asset_entry: td.commission_asset_entry||"",
                commission_asset_exit: td.commission_asset_exit||"",
                maker_taker_entry: td.maker_taker_entry||"",
                maker_taker_exit: td.maker_taker_exit||"",
                slip_realized_bps: td.slip_realized_bps!=null?td.slip_realized_bps:"",
                reconciliation: c.reconciliation||"",
                fingerprint_entry: td.fingerprint_entry||"",
                fingerprint_exit: td.fingerprint_exit||"",
                calib_key: c.calib_key||"",
                p_raw: c.p_raw!=null?c.p_raw:"",
                p_pred: c.p_pred!=null?c.p_pred:"",
                regime: c.regime||"",
                source: c.reconciliation||""
              };
              rows.push(toCSVLine(row, TRADE_CSV_KEYS));
            }
            const header=TRADE_CSV_KEYS.join(",");
            const tradesCsv=[header,...rows].join("\n");
            return { closedAudit, disagreements, tradesCsv };
          }

          // Candle-sim close helper (used both for compare and fallback)
          async function candleSimForIdea(p){
            try{
              const ttl_ts_ms=p.ttl_ts_ms || (p.ts_ms + (p.hold_sec||0)*1000);
              const k = await fetchK(p.symbolFull,"1m",K1M, p.ts_ms-60*1000, ttl_ts_ms+60*1000);
              if(!k) return { status:"no_klines", closed:false };
              const ts=k.map(x=>+x[0]), highs=k.map(x=>+x[2]), lows=k.map(x=>+x[3]), closes=k.map(x=>+x[4]);
              const long=p.side==="long";
              const entry=Number.isFinite(p.entry_limit)?p.entry_limit:p.entry_price;
              const tpB=(p.tp_bps||0)/10000, slB=(p.sl_bps||0)/10000;
              const tp_abs=Number.isFinite(p.tp_abs)?p.tp_abs:(long?entry*(1+tpB):entry*(1-tpB));
              const sl_abs=Number.isFinite(p.sl_abs)?p.sl_abs:(long?entry*(1-slB):entry*(1+slB));

              let filled=false,iFill=-1;
              for(let i=0;i<k.length;i++){
                if(ts[i]<p.ts_ms-1000) continue;
                if(ts[i]>ttl_ts_ms) break;
                if(long?(lows[i]<=entry):(highs[i]>=entry)){ iFill=i; filled=true; break; }
              }
              if(!filled) return { status:"still_pending", closed:false };

              let exit_px=closes.at(-1), exit_reason="ttl", iExit=k.length-1;
              let mfe=0, mae=0;
              for(let i=iFill;i<k.length;i++){
                if(ts[i]>ttl_ts_ms){ iExit=i; break; }
                const hi=highs[i], lo=lows[i];
                const rHi=long?(hi/entry-1):(entry/hi-1), rLo=long?(lo/entry-1):(entry/lo-1);
                mfe=Math.max(mfe,rHi*10000);
                mae=Math.min(mae,rLo*10000);
                if(long){
                  if(hi>=tp_abs){ exit_px=tp_abs; exit_reason="tp"; iExit=i; break; }
                  if(lo<=sl_abs){ exit_px=sl_abs; exit_reason="sl"; iExit=i; break; }
                }else{
                  if(lo<=tp_abs){ exit_px=tp_abs; exit_reason="tp"; iExit=i; break; }
                  if(hi>=sl_abs){ exit_px=sl_abs; exit_reason="sl"; iExit=i; break; }
                }
              }
              const ret=long?(exit_px/entry-1):(entry/exit_px-1);
              const pnl_bps=Math.round(ret*10000)-(p.cost_bps||0);
              return {
                status:"closed_sim",
                closed:true,
                pnl_bps,
                ts_entry_ms: ts[iFill],
                ts_exit_ms: ts[Math.min(iExit,ts.length-1)],
                price_entry: entry, price_exit: exit_px, exit_reason,
                realized:{ tp_hit:exit_reason==="tp", sl_hit:exit_reason==="sl", ttl_exit:exit_reason==="ttl", max_favorable_excursion_bps:mfe, max_adverse_excursion_bps:mae }
              };
            }catch(e){ return { status:"sim_error", error:e?.message||String(e), closed:false }; }
          }
          
          // ---------- Main ----------
          let reason="ok", selectionTier="confidence";
          try{
            const { state, persist } = await loadState();
            learnFromGistRealFills(state);
            const calibHealth = validateCalibration(state);

            // Heartbeat existing pending
            const heartbeat = await heartbeatCheck(state);
            if((heartbeat?.zombies||0)>0) log("heartbeat zombies", heartbeat.zombies, (heartbeat.details||[]).slice(0,5));

            // Strict pre-clean of orphan pendings
            const cleaned = await cleanupPendingBeforePlanning(state, heartbeat);
            if (cleaned.dropped > 0) {
              const auditOut = buildAuditOutputs(state);
              await saveState(persist, state, auditOut, 2, true);
              log("cleanup saved to Gist before planning.");
            }

            const ALL24 = await fetchAll24hr();
            const booksRaw=await getJSON(api("/api/v3/ticker/bookTicker"),10000,2) || [];
            const bookMap=new Map(booksRaw.map(b=>[b.symbol,{ bid:+b.bidPrice, ask:+b.askPrice }]));
            if(!todOK) reason="tod_gate";

            // BTC regimes
            const BTC="BTCUSDT", ETH="ETHUSDT", SOL="SOLUSDT";
            const kBTC1H=await getKCached(BTC,"1h",K1H);
            const kBTC5=await getKCached(BTC,"5m",K5M);
            const btcUp1h = (() => {
              if (!kBTC1H?.length) return false;
              const c = kBTC1H.map(x => +x[4]);
              return c.at(-1) >= ema(c, EMA_FAST) && c.at(-1) >= ema(c, EMA_SLOW);
            })();
            const btcR5=(()=>{ const c=(kBTC5||[]).map(x=>+x[4]); const r=[]; for(let i=1;i<c.length;i++) r.push(Math.log(c[i]/c[i-1])); return r.slice(-120); })();

            // Universe build
            const QUOTES=["USDT","USDC","USD"];
            const split=(sym)=>{ for(const q of QUOTES) if(sym.endsWith(q)) return { base:sym.slice(0,-q.length), quote:q }; return null; };
            const all=[]; for(const t of ALL24||[]){ const sym=t.symbol||t.s; if(!sym) continue; const sq=split(sym); if(!sq) continue; if(STABLES.has(sq.base)) continue; const qv=+(t.quoteVolume||t.q||0); if(!isFinite(qv)||qv<=0) continue; all.push({ symbol:sym, base:sq.base, quote:sq.quote, qv }); }
            all.sort((a,b)=>b.qv-a.qv);
            let dynMinQV=Math.max(MIN_QV_ENV||0,DYN_QV_MIN), filt=all.filter(x=>x.qv>=dynMinQV);
            while(filt.length<UNIV_TARGET_MIN && dynMinQV>1_000_000){ dynMinQV=Math.max(1_000_000, dynMinQV-DYN_QV_STEP); filt=all.filter(x=>x.qv>=dynMinQV); }
            const universe=filt.slice(0,UNIV_TARGET_MAX);
            log("universe", universe.length, "dyn_min_qv", dynMinQV);

            // DD/throttle
            const equityStats=(eq)=>{ const day=Date.now()-24*3600*1000; let pnl24=0,cum=0,peak=0,dd=0; for(const e of eq||[]){ if(e.ts_ms>=day) pnl24+=e.pnl_bps; cum+=e.pnl_bps; if(cum>peak) peak=cum; dd=Math.min(dd,cum-peak); } return { pnl24_bps:Math.round(pnl24), peak_dd_bps:Math.round(dd) }; };
            const { pnl24_bps, peak_dd_bps } = equityStats(state.equity);
            const throttle=(pnl24_bps<=DD_24H_LIMIT_BPS) || (peak_dd_bps<=DD_PEAK_LIMIT_BPS);

            // Liquidity rank
            const liqPct=new Map(); for(let i=0;i<universe.length;i++) liqPct.set(universe[i].symbol,(universe.length===1)?1:1-i/(universe.length-1));

            // Pre-scan candidates
            const picksRaw=[]; const B=10;
            for(let i=0;i<(universe||[]).length;i+=B){
              const batch=await Promise.all(universe.slice(i,i+B).map(async c=>{ try{
                const lastTs=state.cooldown?.[c.base]||0; if(lastTs && (Date.now()-lastTs)<COOLDOWN_MS) return null;
                const book=bookMap.get(c.symbol) || await getBookTickerCached(c.symbol); if(!book?.bid||!book?.ask) return null;
                const mid=(+book.bid + +book.ask)/2; if(!(mid>0)) return null;
                const spreadBps=Math.round(((book.ask-book.bid)/mid)*10000); // soft gate: keep, penalize in confidence

                const k5=await getKCached(c.symbol,"5m",K5M); if(!k5||k5.length<Math.max(EMA_SLOW+5,ATR_P+5)) return null;
                const c5_raw=k5.map(x=>+x[4]);
                const c5=kalman1D(c5_raw,{q:1e-4,r:5e-4});
                const h5=k5.map(x=>+x[2]), l5=k5.map(x=>+x[3]), v5=k5.map(x=>+x[5]);
                const adxatr=computeADX_ATR(h5,l5,c5,ADX_P)||{};
                const adx5=adxatr.adx||0, atr5=adxatr.atr||0;
                const atr_bps=Math.round((atr5/(c5.at(-1)||1))*10000);
                if(atr_bps<4||atr_bps>260) return null;

                const vwap5=vwapAnchored(h5,l5,c5,v5,VWAP_5M_WIN);
                const last=c5.at(-1), roc1=(last/(c5.at(-2)||last))-1, roc3=(last/(c5.at(-4)||last))-1;
                const z_vwap=(last-vwap5)/(atr5||1);
                const rsi14=rsi(c5,14);

                const highs=h5, lows=l5;
                const win=MR_BREAKOUT_DC_N;
                const dcHi=Math.max(...highs.slice(-win)), dcLo=Math.min(...lows.slice(-win));
                const atrPct=(atr5/last), ret15abs=Math.abs((last/(c5.at(-4)||last))-1);
                const breakout=(last>dcHi)||(last<dcLo)||(ret15abs>=MR_BREAKOUT_RET15_ATR*atrPct);
                if(breakout) state.mr_lockout[c.symbol]=Date.now()+MR_LOCKOUT_SEC*1000;
                const mrLocked=(Date.now()<(state.mr_lockout?.[c.symbol]||0));
                const noLongDown=NO_LONG_DOWN_ENABLE&&((rsi14||100)<LONG_DOWN_RSI_CUTOFF);

                const adxF=clamp((adx5-16)/14,0,1);
                const pTrend=clamp(0.5+0.27*(0.6*tanh(roc1/0.003)+0.4*tanh(roc3/0.0065))*adxF,0.32,0.93);
                const pMR=clamp(0.5+0.23*(0.7*tanh(Math.abs(z_vwap))*Math.sign(-z_vwap)+0.3*(-(rsi14-50)/50))*(1-adxF),0.35,0.90);
                const pLong0=clamp(0.45*pTrend+0.55*pMR,0.3,0.97);
                const pShort0=clamp(0.45*(1-pTrend)+0.55*(1-pMR),0.3,0.97);

                // 5m trend snapshot for provenance
                const ema21=ema(c5,21), ema50=ema(c5,50);
                const roc5m=(c5.at(-1)/(c5.at(-6)||c5.at(-1)) - 1);
                const roc15m=(c5.at(-1)/(c5.at(-16)||c5.at(-1)) - 1);
                const slope=slopeBps(c5,21);
                const dir=(!ema21||!ema50)?"flat":(ema21>ema50? "up" : (ema21<ema50? "down":"flat"));

                return {c,k5,c5,h5,l5,v5,adx5,atr_bps,spreadBps,pLong0,pShort0,mrLocked,noLongDown,vol1h_est_usd:c.qv/24, trend5m:{ ema21, ema50, rsi:rsi14, roc5:roc5m, roc15:roc15m, adx:adx5, ema_slope_bps:slope, dir } };
              }catch{ return null; } }));
              for(const x of batch) if(x) picksRaw.push(x);
            }

            // Prelim top N by quick score
            const MAX_DEEP=30;
            const prelim=picksRaw.map(p=>{
              const pW=Math.max(p.pLong0||0,p.pShort0||0), adxN=clamp((p.adx5-16)/14,0,1), sP=Math.max(0,(p.spreadBps||0)-6)/12;
              const score=0.6*pW+0.3*adxN-0.1*sP;
              return {p,score};
            }).sort((a,b)=>b.score-a.score).slice(0,MAX_DEEP).map(x=>x.p);

            // Heavy refine with MTF, TF alignment gate, microstructure and EV sims
            const refined=[];
            for(let idx=0; idx<prelim.length; idx++){
              try{
                const p=prelim[idx], c=p.c;
                const book=bookMap.get(c.symbol)||await getBookTickerCached(c.symbol); if(!book?.bid||!book?.ask) continue;
                const mid=(+book.bid + +book.ask)/2; if(!(mid>0)) continue;

                const depthA=await getDepthCached(c.symbol); await sleep(180+Math.floor(Math.random()*120)); const depthB=await fetchDepth(c.symbol);
                const depth=depthB||depthA; if(!depth?.bids?.length || !depth?.asks?.length) continue;
                const obi=computeOBI(depth,OBI_TOPN);
                const ofi30s=ofiProxy(depthA,depthB);
                const slopeObj=bookSlope(depth,mid);
                const bestBid=+depth?.bids?.[0]?.[0], bestAsk=+depth?.asks?.[0]?.[0], bidQ=+depth?.bids?.[0]?.[1], askQ=+depth?.asks?.[0]?.[1];
                const mp=(bestBid&&bestAsk&&bidQ&&askQ)?microprice(bestBid,bestAsk,bidQ,askQ):null;
                const micro_bias_bps=(mp&&mid)?Math.round((mp-mid)/mid*10000):null;

                const km=await getKMultiTF(c.symbol);
                const mtf=computeMTFIndicators(km, TF_WEIGHTS);
                const tfAlign=mtf?.tfAlign||0;
                const mtfAgg=mtf?.agg||{ rsi:50, adx:0, atr_bps:0, roc1:0, roc3:0 };

                const ind5=tfInd(km["5m"]),ind15=tfInd(km["15m"]),ind1h=tfInd(km["1h"]),ind4h=tfInd(km["4h"]);
                const sideSlipL=slipFromDepth(depth,mid,"long",NOTIONAL), sideSlipS=slipFromDepth(depth,mid,"short",NOTIONAL);
                const slipL=sideSlipL?.slip_bps??Math.round(p.spreadBps/2), slipS=sideSlipS?.slip_bps??Math.round(p.spreadBps/2);
                const costLong=COST_BPS+slipL, costShort=COST_BPS+slipS;

                const d1=depthWithinPctUSD(depth,mid,0.01);
                const depth1pUSD=(d1.asksUSD||0)+(d1.bidsUSD||0);

                const regime=p.mrLocked?"trend":"meanrevert";
                const coeffL = getCoeffs(state, "long", regime);
                const coeffS = getCoeffs(state, "short", regime);
                const nL = Math.max(1, Math.min(500, coeffL.n || 0));
                const nS = Math.max(1, Math.min(500, coeffS.n || 0));
                const pLong=calibrateP(state,"long",regime,p.pLong0);
                const pShort=calibrateP(state,"short",regime,p.pShort0);
                const pLong_lcb=wilsonLCB(pLong, nL, WILSON_Z);
                const pShort_lcb=wilsonLCB(pShort, nS, WILSON_Z);

                const yR=[]; for(let i=1;i<p.c5.length;i++) yR.push(Math.log(p.c5[i]/p.c5[i-1]));
                const mu5=mean(yR)||0, sig5=Math.max(1e-4,std(yR));

                const MAX_SL_BPS = 80;
                let candLong=null, candShort=null;

                if(DIRECTION!=="short"){
                  let tp=Math.round(LONG_BOUNCE_TP_ATR*p.atr_bps);
                  let sl=Math.round(LONG_BOUNCE_SL_ATR*p.atr_bps);
                  sl = Math.min(sl, MAX_SL_BPS);
                  tp = Math.max(tp, Math.round(sl * 1.5));
                  const sim=pathEVHeavy({entry:mid,tp_bps:tp,sl_bps:sl,side:"long",retSeries:yR,mu:mu5,sigma:sig5,steps:4,N:192,cost_bps:costLong});
                  const evLCB=Math.round(pLong_lcb*tp-(1-pLong_lcb)*sl-costLong);
                  const edgeL = tp/Math.max(1, LONG_EDGE_MULT_MIN*costLong);
                  const evScoreL = evLCB/Math.max(8, EXP_LCB_MIN_BPS_BASE);
                  const viabMultL = clamp(0.5*clamp(edgeL,0.3,1.4)+0.5*clamp(evScoreL,0.3,1.4), 0.3, 1.3);
                  candLong={ side:"long", p_lcb:pLong_lcb,p_raw:p.pLong0, ev_bps:sim.ev_bps, exp_lcb_bps:evLCB, tp_bps:tp, sl_bps:sl, cost_bps:costLong, style:"mr", regime, viab_mult:viabMultL };
                }
                if(DIRECTION!=="long"){
                  let tp=Math.round(p.atr_bps*1.0);
                  let sl=Math.round(p.atr_bps*0.5);
                  sl = Math.min(sl, MAX_SL_BPS);
                  tp = Math.max(tp, Math.round(sl * 1.5));
                  const sim=pathEVHeavy({entry:mid,tp_bps:tp,sl_bps:sl,side:"short",retSeries:yR,mu:mu5,sigma:sig5,steps:4,N:192,cost_bps:costShort});
                  const evLCB=Math.round(pShort_lcb*tp-(1-pShort_lcb)*sl-costShort);
                  const edgeS = tp/Math.max(1, 4*costShort);
                  const evScoreS = evLCB/Math.max(8, EXP_LCB_MIN_BPS_BASE);
                  const viabMultS = clamp(0.5*clamp(edgeS,0.3,1.4)+0.5*clamp(evScoreS,0.3,1.4), 0.3, 1.3);
                  candShort={ side:"short", p_lcb:pShort_lcb,p_raw:p.pShort0, ev_bps:sim.ev_bps, exp_lcb_bps:evLCB, tp_bps:tp, sl_bps:sl, cost_bps:costShort, style:"mr", regime, viab_mult:viabMultS };
                }

                const chosen=candLong&&candShort?(candLong.exp_lcb_bps>=candShort.exp_lcb_bps?candLong:candShort):(candLong||candShort||null);
                if(!chosen) continue;

                const microQ=(Math.max(0,1-(p.spreadBps||0)/20))*(1-Math.min(0.6,(chosen.cost_bps-COST_BPS)/20))*(1+clamp(obi,-0.2,0.2));
                // Soft TF alignment (no hard gate)
                const gTF = clamp(0.4 + 0.8*tfAlign, 0.2, 1.2);
                const regimeQ=0.7+0.4*tfAlign;

                const gSpread = clamp(1 - Math.max(0,(p.spreadBps - MAX_SPREAD_BPS))/MAX_SPREAD_BPS, 0.2, 1.1);
                const gCost   = clamp(1 - Math.max(0,(chosen.cost_bps - COST_BPS))/20, 0.2, 1.1);
                const gVol    = clamp((p.vol1h_est_usd||0)/Math.max(1, MIN_VOL_1H_USD), 0.2, 1.1);
                const gDepth  = clamp(depth1pUSD/Math.max(1, DEPTH_1P_MIN_USD), 0.2, 1.1);
                const gMR     = p.mrLocked ? 0.7 : 1.05;
                const gDir    = (chosen.side==="long" && p.noLongDown) ? 0.8 : 1.0;
                const gBTC    = (chosen.side==="long" && !btcUp1h) ? 0.85 : 1.0;
                const gEV     = clamp(chosen.viab_mult||1, 0.3, 1.3);

                const gateMult= clamp(gSpread * gCost * gVol * gDepth * gMR * gDir * gBTC * gTF * gEV, 0.2, 1.4);

                const conf = confidenceScore({
                  p_cal: chosen.p_lcb,
                  ev_bps: chosen.ev_bps,
                  atr_bps: p.atr_bps,
                  micro: microQ,
                  regime: regimeQ,
                  reliability: 1,
                  riskAdj: 1
                }) * gateMult;

                const slipEst=chosen.side==="long"?slipL:slipS;
                const btcCorr=corr((()=>{const r=[];for(let i=1;i<p.c5.length;i++) r.push(Math.log(p.c5[i]/p.c5[i-1])); return r.slice(-120);})(), btcR5);
                const reasons=reasonBuilder({adx1h:ind1h?.adx||0,ofi30s,tfAlign,spread_bps:p.spreadBps,slip_bps_est:slipEst,mtf_rsi:mtfAgg.rsi});

                refined.push({
                  symbol:c.symbol, base:c.base, quote:c.quote, qv:c.qv,
                  side:chosen.side,
                  p_lcb:+chosen.p_lcb.toFixed(3), p_raw:+chosen.p_raw.toFixed(3),
                  exp_bps:chosen.ev_bps, exp_lcb_bps:chosen.exp_lcb_bps,
                  tp_bps:chosen.tp_bps, sl_bps:chosen.sl_bps,
                  rrr:+(chosen.tp_bps/Math.max(1,chosen.sl_bps)).toFixed(2),
                  spread_bps:p.spreadBps,
                  cost_bps:chosen.cost_bps,
                  adx:+(p.adx5||0).toFixed(1), atr_bps:p.atr_bps,
                  regime:chosen.regime, style:chosen.style,
                  hold_sec:clamp(720,TTL_MIN,TTL_MAX),
                  liq_pct:+(liqPct.get(c.symbol)||0.5).toFixed(3),
                  ret5:(()=>{const r=[];for(let i=1;i<p.c5.length;i++)r.push(Math.log(p.c5[i]/p.c5[i-1]));return r.slice(-36);})(),
                  confidence: Math.round(conf),
                  slip_est:slipEst, reasons, ofi30s, micro_bias_bps, tfAlign, obi,
                  vol1h_est_usd: p.vol1h_est_usd,
                  depth1p_usd: depth1pUSD,
                  mr_locked: p.mrLocked || false,
                  mtf_agg: mtfAgg,
                  mtf_weights: mtf?.weights||{},
                  btc_corr: +(+btcCorr||0).toFixed(3),
                  trend5m: p.trend5m
                });
              }catch(e){ log("heavy refine warn",e?.message||e); }
            }

            // Uniqueness adjustment
            for (let i=0;i<refined.length;i++){
              let mx=0;
              for (let j=0;j<refined.length;j++){
                if(i===j) continue;
                const r=corr(refined[i].ret5||[], refined[j].ret5||[]);
                mx=Math.max(mx, r||0);
              }
              refined[i].uniqMult = clamp(1 - Math.max(0, mx - 0.6)/0.4, 0.6, 1.1);
            }
            for (const x of refined) {
              const uniqMult = x.uniqMult || 1;
              const symMult = getSymbolAdj(state, x.base);
              x.confidence = Math.round(x.confidence * uniqMult * symMult);
            }

            refined.sort((a,b)=>b.confidence-a.confidence);

            const TARGET_TOP_N = Math.max(1, TOP_N || 3);

            const selected=[];
            for (const cand of refined) {
              if (selected.length >= TARGET_TOP_N) break;
              let ok=true;
              for (const s of selected) {
                const r=corr(cand.ret5||[], s.ret5||[]);
                if (r>0.98 && cand.side===s.side) { ok=false; break; }
              }
              if (ok) selected.push(cand);
            }
            // If still fewer than target, fill ignoring correlation guard by descending confidence
            if (selected.length < TARGET_TOP_N) {
              const have=new Set(selected.map(s=>s.symbol+":"+s.side));
              for (const c of refined) {
                if (selected.length >= TARGET_TOP_N) break;
                const k=c.symbol+":"+c.side;
                if (!have.has(k)) { selected.push(c); have.add(k); }
              }
            }
            // Just in case refined was tiny, ensure we never exceed target
            if (selected.length > TARGET_TOP_N) selected.splice(TARGET_TOP_N);

            // Risk weights via softmax of EV (unchanged)
            const wEv=softmax(selected.map(x=>x.exp_lcb_bps),20);

            // Build full v2 ideas (all code2 semantics kept)
            let picksV2 = selected.map((x,i)=>{
              const bk = bookMap.get(x.symbol);
              const mid = (bk?.bid && bk?.ask) ? ((+bk.bid + +bk.ask)/2) : null;

              // compute abs exits (also needed for v1 compat)
              let entry_limit = null, tp_abs = null, sl_abs = null;
              if (mid != null) {
                const slF = x.sl_bps/10000, tpF = x.tp_bps/10000;
                if (x.side === "long") {
                  entry_limit = mid * (1 - Math.max(0.001, slF*0.5));
                  tp_abs = entry_limit * (1 + tpF);
                  sl_abs = entry_limit * (1 - slF);
                } else {
                  entry_limit = mid * (1 + slF);
                  tp_abs = entry_limit * (1 - tpF);
                  sl_abs = entry_limit * (1 + slF);
                }
              }

              const idea_id = uuidv4();
              const client_order_id = mkClientOrderId("mxu");

              const ttl = clamp(Math.round((x.hold_sec || 720)+i*12), TTL_MIN, TTL_MAX);
              const size_bps = (Number(process.env.FORCE_SIZE_BPS||"0")>0)
                  ? Math.round(Number(process.env.FORCE_SIZE_BPS))
                  : (x.sl_bps>0?Math.min(220,Math.round((wEv[i]*TARGET_PORT_RISK_BPS/x.sl_bps)*100)):0);

              return {
                idea_id, client_order_id,

                symbol: x.base,
                symbol_full: x.symbol,
                symbolFull: x.symbol,
                quote: x.quote,
                side: x.side,
                rank: i+1,

                // v2/futures intent (kept, but hidden from v1 Worker)
                target_exchange: TARGET_EXCHANGE,   // e.g. "bybit_futures_testnet"
                market_type: MARKET_TYPE,           // "futures"
                margin_mode: MARGIN_MODE,           // "isolated"
                leverage: LEVERAGE,                 // 1

                // code2 entry intent
                entry_policy: "maker_join",
                exec: { exec: "post_only" },

                // entry + exits
                entry_mid: mid,
                entry_limit,
                tp_abs,
                sl_abs,

                tp_bps: x.tp_bps,
                sl_bps: x.sl_bps,
                rrr: x.rrr,
                exp_lcb_bps: x.exp_lcb_bps,
                ev_bps: x.exp_bps,
                cost_bps: x.cost_bps,

                p_win: x.p_lcb,
                p_lcb: x.p_lcb,
                p_raw: x.p_raw,
                calib_key: `side:${x.side}|regime:${x.regime}`,
                regime: x.regime,

                ttl_sec: ttl,
                size_bps,

                predicted: {
                  ts_ms: Date.now(),
                  p_cal: x.p_lcb,
                  ev_bps: x.exp_lcb_bps ?? x.exp_bps,
                  confidence_score: x.confidence
                },

                confidence: x.confidence,
                reasons: x.reasons,
                reasons_text: (x.reasons||[]).slice(0,3).map(r=>`${r.factor}:${r.value}(${r.contribution>=0?"+":""}${r.contribution})`),

                // keep for state/debug parity
                trend5m: x.trend5m
              };
            });

            // Convert v2 ideas to a v1‑compatible shape for the current Worker
            const toV1Compat = (pv2) => ({
              idea_id: pv2.idea_id,
              client_order_id: pv2.client_order_id,

              symbol: pv2.symbol,
              symbol_full: pv2.symbol_full,
              quote: pv2.quote,
              side: pv2.side,
              rank: pv2.rank,

              // v1 entry shape (what the current MEXC Worker expects)
              entry_policy: "smart_swing_limit",
              entry_type: "limit",
              activation: "on_fill",

              entry_mid: pv2.entry_mid,
              entry_limit: pv2.entry_limit,
              tp_abs: pv2.tp_abs,
              sl_abs: pv2.sl_abs,

              tp_bps: pv2.tp_bps,
              sl_bps: pv2.sl_bps,
              rrr: pv2.rrr,
              exp_lcb_bps: pv2.exp_lcb_bps,
              ev_bps: pv2.ev_bps,
              cost_bps: pv2.cost_bps,

              p_win: pv2.p_lcb,
              p_lcb: pv2.p_lcb,
              p_raw: pv2.p_raw,
              calib_key: pv2.calib_key,
              regime: pv2.regime,

              ttl_sec: pv2.ttl_sec,
              size_bps: pv2.size_bps,

              // keep all new info inside meta extras (preserved for your separate Worker)
              predicted: {
                ...pv2.predicted,
                meta: {
                  strategy: STRATEGY_NAME, model_version: MODEL_VERSION, schema_version: SCHEMA_VERSION,
                  git_sha: GIT_SHA, config_hash: CONFIG_HASH,
                  futures_hint: {
                    target_exchange: pv2.target_exchange,
                    market_type: pv2.market_type,
                    margin_mode: pv2.margin_mode,
                    leverage: pv2.leverage,
                    entry_policy_override: pv2.entry_policy,
                    exec: pv2.exec
                  }
                }
              },

              confidence: pv2.confidence,
              reasons: pv2.reasons,
              reasons_text: pv2.reasons_text,
              trend5m: pv2.trend5m
            });

            let picks = picksV2.map(toV1Compat); // this is what you POST to the current Worker

            // Portfolio VaR/ES throttle on sizes
            try {
              const varStats = portfolioVaR_ES_tCopula(picks, 6);
              if (varStats.ES95_bps < -220) {
                const scale = clamp(-220 / Math.min(-1e-6, varStats.ES95_bps), 0.35, 1);
                picks = picks.map(p => ({ ...p, size_bps: Math.max(4, Math.round(p.size_bps * scale)) }));
                console.log("[gha] VaR/ES throttle", varStats, "scale", scale);
              } else {
                console.log("[gha] VaR/ES ok", varStats);
              }
            } catch (e) { console.log("[gha] VaR/ES warn", e?.message||e); }

            // Update state with pending ideas + cooldowns (v1-safe)
            try{
              const nowMs = Date.now();
              const ideasTs = new Date(nowMs).toISOString();
              for (const p of picks) {
                state.cooldown[p.symbol] = nowMs;
                state.cooldown_side[p.symbol] = { side: p.side, ts_ms: nowMs };
                state.pending.push({
                  ts: ideasTs,
                  ts_ms: nowMs,
                  ttl_ts_ms: nowMs + p.ttl_sec*1000,

                  symbolFull: p.symbol_full,   // keep this key as in code1/state
                  base: p.symbol,
                  quote: p.quote,
                  side: p.side,

                  entry_limit: p.entry_limit,
                  tp_abs: p.tp_abs,
                  sl_abs: p.sl_abs,

                  hold_sec: p.ttl_sec,
                  tp_bps: p.tp_bps,
                  sl_bps: p.sl_bps,
                  cost_bps: p.cost_bps,
                  tier: p.tier || "confidence",

                  p_lcb: p.p_lcb,
                  p_raw: p.p_raw,
                  calib_key: p.calib_key,
                  predicted: p.predicted,
                  regime: p.regime,

                  decision_context: { tfs:["1m","5m","15m","1h","4h"], mtf_weights: TF_WEIGHTS },

                  notional_usd: NOTIONAL,
                  size_bps: p.size_bps,

                  idea_id: p.idea_id,
                  client_order_id: p.client_order_id,
                  trend5m: p.trend5m,

                  status: "planned",
                  entry_ts_ms: null,
                  entry_price: null,
                  qty: null
                });
              }
              if (state.pending.length > 550) state.pending = state.pending.slice(-550);
              log("DEBUG: Appended", picks.length, "plans. New pending length=", (state.pending || []).length, "example ID:", state.pending.slice(-1)[0]?.client_order_id || "none");
            }catch(e){ log("state update warn",e?.message||e); }

            // Persist state + audit artifacts
            const auditOut=buildAuditOutputs(state);
            log("DEBUG: Pre-save: pending length=", (state.pending || []).length);
            await saveState(persist, state, auditOut, 2, true);
            log("DEBUG: saveState call complete.");

            // Build meta and payload — push v1 ideas to the existing Worker
            for (const p of picks) { log(`idea ${p.symbol_full} ${p.side} conf=${p.confidence} reasons=${(p.reasons_text||[]).join("; ")}`); }
            const ideaLogs = picks.map(p=>({symbol:p.symbol_full,side:p.side,confidence:p.confidence,reasons:p.reasons_text,p_cal:+p.p_lcb.toFixed(3),ev_bps:p.ev_bps,idea_id:p.idea_id}));

            const meta = {
              origin:"github_actions",
              reason,
              exchange:"mexc", exchange_base: BASE,
              strategy: STRATEGY_NAME, model_version: MODEL_VERSION, schema_version: SCHEMA_VERSION,
              git_sha: GIT_SHA, config_hash: CONFIG_HASH,
              counts:{universe:universe.length,refined:refined.length,selected:picks.length},
              dd_gate:{pnl24_bps,peak_dd_bps,throttle},
              tod:{active:S_H!=null&&E_H!=null,start:S_H,end:E_H},
              heartbeat:{ mismatches:heartbeat?.mismatches||0, zombies:heartbeat?.zombies||0 },
              tier: selectionTier,
              idea_logs: ideaLogs,
              tf_weights: TF_WEIGHTS,
              calibration_health: calibHealth,

              // keep all code2 fields here so nothing is lost
              extras: {
                v2_ideas: picksV2,
                features_requested: ["maker_join","post_only","futures"]
              }
            };

            const payload = { ts:new Date().toISOString(), mode:"normal", source:"external_pusher", meta, top_n:picks.length||0, ideas:picks||[] };
            log(`pushing ${picks.length} ideas (${selectionTier}) to ${PUSH_URL}`);
            try{
              const r = await fetchWithTimeout(PUSH_URL,{
                method:"POST",
                headers:{ "Content-Type":"application/json", "Authorization":"Bearer " + PUSH_TOKEN },
                body: JSON.stringify(payload)
              },15000);
              let txt=""; try{ txt=await r.text(); }catch{}
              log("push status", r?.status||"ERR", (txt||"").slice(0,400));
            }catch(e){ log("push failed",e?.message||e); }

            if (PUSH_URL_V2) {
              const payloadV2 = {
                ts: new Date().toISOString(),
                mode: "normal",
                source: "external_pusher",
                meta: { origin: "github_actions_v2" },
                top_n: picksV2.length,
                ideas: picksV2
              };
              log(`pushing ${picksV2.length} v2 ideas to ${PUSH_URL_V2}`);
              try {
                const r2 = await fetchWithTimeout(PUSH_URL_V2, {
                  method:"POST",
                  headers:{ "Content-Type":"application/json", "Authorization":"Bearer " + PUSH_TOKEN },
                  body: JSON.stringify(payloadV2)
                }, 15000);
                let txt2=""; try{ txt2=await r2.text(); }catch{}
                log("push v2 status", r2?.status||"ERR", (txt2||"").slice(0,400));
              } catch (e) { log("push v2 failed", e?.message||e); }
            }
          }catch(e){
            reason=reason!=="ok"?reason:"data_error";
            const payload={ ts:new Date().toISOString(),mode:"normal",source:"external_pusher",meta:{origin:"github_actions",reason,error:(e?.message||String(e)).slice(0,220)},top_n:0,ideas:[] };
            log("pushing 0 ideas to",PUSH_URL);
            try{
              const r=await fetchWithTimeout(PUSH_URL,{
                method:"POST",
                headers:{ "Content-Type":"application/json", "Authorization":"Bearer " + PUSH_TOKEN },
                body:JSON.stringify(payload)
              },15000);
              let txt=""; try{ txt=await r.text(); }catch{}
              log("push status",r?.status||"ERR",(txt||"").slice(0,200));
            }catch(e2){ log("push failed",e2?.message||e2); }
          }
          })();
          NODE

          EXIT_CODE=$?

          # Shell-level fallback (in case node crashed before posting)
          set -e
          if [[ $EXIT_CODE -ne 0 ]]; then
            echo "[gha] pusher exited with code $EXIT_CODE — sending minimal payload so tail shows a [push] line"

            # Sanitize and derive URLs
            raw_push_url="$(printf '%s' "${WORKER_PUSH_URL}" | tr -d '\r\n')"
            health_url="$(printf '%s' "${raw_push_url}" | sed -E 's#/signals/push(\?.*)?$#/health#')"

            # 1) Fallback health GET (guaranteed Worker hit)
            echo "[gha] fallback health GET ${health_url}"
            http_code_h=$(curl -g -sS -o /dev/null -w "%{http_code}" \
              -H "Authorization: Bearer ${PUSH_TOKEN}" \
              --max-time 8 \
              "${health_url}" || true)
            echo "[gha] fallback health status ${http_code_h}"

            # 2) Minimal POST with 0 ideas
            now=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
            payload='{"ts":"'"${now}"'","mode":"normal","source":"external_pusher","meta":{"origin":"github_actions","reason":"fallback_node_error"},"top_n":0,"ideas":[]}'
            echo "[gha] pushing 0 ideas to ${raw_push_url}"
            mkdir -p /tmp
            : > /tmp/push_resp.txt
            http_code=$(curl -g -sS -o /tmp/push_resp.txt -w "%{http_code}" -X POST \
              -H "Content-Type: application/json" \
              -H "Authorization: Bearer ${PUSH_TOKEN}" \
              --data-raw "${payload}" \
              --max-time 12 \
              "${raw_push_url}" || true)
            body="$(head -c 400 /tmp/push_resp.txt || true)"
            echo "[gha] push status ${http_code} ${body}"
            exit 0
          fi
