// enrich.js — обогащение токенов + запись снапшотов (p0, ATH 5m, start MC)

import 'dotenv/config';
import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 3,
  idleTimeoutMillis: 30_000,
});

const PAUSE_MS = 3000;           // 3 сек между токенами (чтобы не ловить 429)
const SNAP_DELAY_MIN = 5;        // ждать 5 мин после старта токена
const CURRENCY = 'USD';
const INTERVAL = '1m';
const CANDLE_LIMIT = 1000;
const TRADES_LIMIT = 100;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const ceilToMinute = (ts) => Math.ceil(ts / 60000) * 60000;

async function getJSON(url, init) {
  const r = await fetch(url, init);
  if (!r.ok) {
    const txt = await r.text().catch(()=> '');
    throw new Error(`${url} -> ${r.status} ${txt}`);
  }
  return r.json();
}

// ==== Pump.fun endpoints ====
const repoUrl = (mint) =>
  `https://frontend-api-v3.pump.fun/coins/${mint}`;
const candlesUrl = (mint, createdTs, beforeTs=null) => {
  const u = new URL(`https://swap-api.pump.fun/v2/coins/${mint}/candles`);
  u.searchParams.set('interval', INTERVAL);
  u.searchParams.set('limit', String(CANDLE_LIMIT));
  u.searchParams.set('currency', CURRENCY);
  u.searchParams.set('createdTs', String(createdTs));
  if (beforeTs) u.searchParams.set('beforeTs', String(beforeTs));
  return u.toString();
};
async function tradesBatch(mint, creator, afterTs, beforeTs, limit=TRADES_LIMIT){
  const r = await fetch(`https://swap-api.pump.fun/v1/coins/${mint}/trades/batch`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      userAddresses: [creator],
      limit: Math.min(limit, TRADES_LIMIT),
      afterTs, beforeTs
    })
  });
  if(!r.ok){
    const txt = await r.text().catch(()=> '');
    throw new Error(`trades/batch ${r.status}: ${txt}`);
  }
  const j = await r.json();
  return Array.isArray(j?.[creator]) ? j[creator] : [];
}

// ==== DB helpers ====
async function pickTokenForSnapshot() {
  const sql = `
    WITH pick AS (
      SELECT ca
      FROM pump_tokens t
      WHERE t.enrich_status = 'ok'
        AND t.created_timestamp <= now() - interval '${SNAP_DELAY_MIN} minutes'
        AND NOT EXISTS (SELECT 1 FROM token_snapshots s WHERE s.ca = t.ca)
      ORDER BY t.inserted_at
      LIMIT 1
    )
    SELECT p.ca, t.creator, t.created_timestamp
    FROM pick p
    JOIN pump_tokens t USING (ca)
  `;
  const { rows } = await pool.query(sql);
  return rows[0];
}

async function insertSnapshot(row) {
  const cols = [
    'ca','snap_ts','price_usd','fdv_usd','usd_market_cap','supply_display',
    'p0_price_usd','start_mc_usd','ath_5m_usd','dev'
  ];
  const sql = `
    INSERT INTO token_snapshots (${cols.join(', ')})
    VALUES ($1, now(), $2, $3, $4, $5, $6, $7, $8, $9)
  `;
  const vals = [
    row.ca,
    row.price_usd ?? null,
    row.fdv_usd ?? null,
    row.usd_market_cap ?? null,
    row.supply_display ?? null,
    row.p0_price_usd ?? null,
    row.start_mc_usd ?? null,
    row.ath_5m_usd ?? null,
    row.dev ?? null,
  ];
  await pool.query(sql, vals);
}

// ==== расчёт снапшота ====
async function buildSnapshot(ca, creator, createdTsMs){
  // 1) repo
  const repo = await getJSON(repoUrl(ca));
  const decimals = Number.isFinite(repo.decimals) ? repo.decimals : 6;
  const totalSupply = Number(repo.total_supply ?? 0);
  const supplyDisplay = totalSupply > 0 ? totalSupply / 10 ** decimals : 1e9; // запасной

  // 2) dev-buy p0
  let t0 = createdTsMs;
  let p0 = null;
  try{
    if (creator) {
      const rows = await tradesBatch(ca, creator, createdTsMs - 30_000, createdTsMs + 5*60_000);
      const buys = rows
        .filter(t => t.type === 'buy')
        .map(t => ({ ts: Date.parse(t.timestamp), price: Number(t.priceUSD) }))
        .filter(o => Number.isFinite(o.ts) && Number.isFinite(o.price))
        .sort((a,b)=> a.ts - b.ts);
      if (buys.length){
        t0 = buys[0].ts;
        p0 = buys[0].price;
      }
    }
  }catch(e){
    console.warn('dev-buy lookup failed:', e.message);
  }

  // 3) свечи (для ATH 0–5 минут и "последняя цена")
  let all = [];
  let beforeTs = null;
  for (let page=0; page<200; page++){
    const batch = await getJSON(candlesUrl(ca, createdTsMs, beforeTs));
    if (!Array.isArray(batch) || batch.length === 0) break;
    batch.sort((a,b)=> a.timestamp - b.timestamp);
    // toNumber
    for (const c of batch){
      c.open = Number(c.open);
      c.high = Number(c.high);
      c.low  = Number(c.low);
      c.close= Number(c.close);
    }
    all.push(...batch);
    const earliest = batch[0].timestamp;
    if (earliest <= createdTsMs) break;
    beforeTs = earliest - 1;
  }
  // дедуп
  const m = new Map();
  for (const c of all) m.set(c.timestamp, c);
  const candles = Array.from(m.values()).sort((a,b)=> a.timestamp - b.timestamp);

  // 4) текущая цена / MC
  let priceNow = null;
  if (Number.isFinite(repo.usd_market_cap) && supplyDisplay > 0){
    priceNow = repo.usd_market_cap / supplyDisplay;
  } else if (candles.length){
    priceNow = Number(candles.at(-1)?.close) || null;
  }
  const usdMarketCap = (priceNow != null && supplyDisplay > 0)
    ? priceNow * supplyDisplay
    : null;

  // 5) ATH за 0–5 минут
  const minStart = ceilToMinute(t0);
  const fiveEnd  = minStart + 5*60*1000;
  let ath5 = -Infinity;
  for (const c of candles){
    const price = Number.isFinite(c.close) ? c.close : Number(c.open);
    if (!Number.isFinite(price)) continue;
    const mc = price * supplyDisplay;
    if (c.timestamp >= minStart && c.timestamp <= fiveEnd){
      if (mc > ath5) ath5 = mc;
    }
  }
  const ath5Val = Number.isFinite(ath5) ? ath5 : null;

  // 6) p0 → start MC
  const startMc = (p0 != null) ? p0 * supplyDisplay : null;

  return {
    ca,
    price_usd: priceNow,
    fdv_usd: null,                 // при необходимости можно посчитать отдельно
    usd_market_cap: usdMarketCap,
    supply_display: supplyDisplay,
    p0_price_usd: p0,
    start_mc_usd: startMc,
    ath_5m_usd: ath5Val,
    dev: repo.creator ?? creator ?? null,
  };
}

// ==== основной цикл ====
async function loop(){
  console.log(`enricher started | wait=${SNAP_DELAY_MIN}m | pause=${PAUSE_MS}ms`);
  while(true){
    try{
      const pick = await pickTokenForSnapshot();
      if (!pick){
        await sleep(5000);
        continue;
      }
      const ca = pick.ca;
      const creator = pick.creator || null;
      const createdTsMs = Date.parse(pick.created_timestamp);

      const snap = await buildSnapshot(ca, creator, createdTsMs);
      await insertSnapshot(snap);

      console.log(`snapshot saved ${ca} | p0=${snap.p0_price_usd ?? '-'} | ath5=${snap.ath_5m_usd ?? '-'}`);
      await sleep(PAUSE_MS);
    }catch(e){
      console.error('loop error:', e.message);
      await sleep(4000);
    }
  }
}

loop().catch(e => {
  console.error('fatal', e);
  process.exit(1);
});
