// enrich.js — считаем только первые 10 минут свечей от dev-buy (S1)
// sequential + 5s pause + retry/backoff for 429/403/503

import 'dotenv/config';
import fetch from 'node-fetch';
import { Pool } from 'pg';

// ---------- настройки ----------
const DB_URL           = process.env.DATABASE_URL || process.env.NEON_DATABASE_URL;
const PAUSE_MS         = 3000;       // пауза между токенами
const WAIT_MINUTES     = 10;         // берём токены старше 10 минут
const CANDLE_LIMIT     = 20;         // возьмём ~30 минут 1m-свечей (запас)
const INTERVAL         = '1m';
const CURRENCY         = 'USD';
const RETRY_IN_MIN     = 15;         // при фолбэке повторить через 15 минут
const MAX_FETCH_RETRY  = 4;          // ретраи для API

// ---------- PG ----------
const pool = new Pool({ connectionString: DB_URL });

// ---------- helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const ceilToMinute = (ts) => Math.ceil(ts / 60000) * 60000;
const round2 = (v) => (v == null ? null : Math.round(v * 100) / 100);

function mc(price, supplyDisplay) {
  const p = Number(price);
  if (!Number.isFinite(p)) return null;
  return p * Number(supplyDisplay);
}

async function fetchJson(url, options = {}, retry = 0) {
  const r = await fetch(url, options);
  if ([429, 403, 503].includes(r.status)) {
    if (retry < MAX_FETCH_RETRY) {
      const backoff = 2000 * Math.pow(2, retry); // 2s,4s,8s,16s
      await sleep(backoff);
      return fetchJson(url, options, retry + 1);
    }
  }
  if (!r.ok) {
    const text = await r.text().catch(() => '');
    throw new Error(`${url} -> ${r.status} ${text.slice(0, 200)}`);
  }
  return r.json();
}

async function postJson(url, body, retry = 0) {
  const r = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  if ([429, 403, 503].includes(r.status)) {
    if (retry < MAX_FETCH_RETRY) {
      const backoff = 2000 * Math.pow(2, retry);
      await sleep(backoff);
      return postJson(url, body, retry + 1);
    }
  }
  if (!r.ok) {
    const text = await r.text().catch(() => '');
    throw new Error(`${url} -> ${r.status} ${text.slice(0, 200)}`);
  }
  return r.json();
}

// ---------- API ----------
async function getRepo(mint) {
  const url = `https://frontend-api-v3.pump.fun/coins/${mint}`;
  return fetchJson(url);
}

async function getCandles(mint, createdTs) {
  const url = new URL(`https://swap-api.pump.fun/v2/coins/${mint}/candles`);
  url.searchParams.set('interval', INTERVAL);
  url.searchParams.set('limit', String(CANDLE_LIMIT));
  url.searchParams.set('currency', CURRENCY);
  url.searchParams.set('createdTs', String(createdTs));
  return fetchJson(url.toString());
}

async function getDevBuys(mint, creator, createdTs) {
  // окно поиска первой покупки дева: [createdTs-30s; createdTs+5m]
  const afterTs  = createdTs - 30_000;
  const beforeTs = createdTs + 5 * 60_000;
  const payload = {
    userAddresses: [creator],
    limit: 100,
    afterTs,
    beforeTs,
  };
  const url = `https://swap-api.pump.fun/v1/coins/${mint}/trades/batch`;
  const j = await postJson(url, payload);
  const arr = Array.isArray(j?.[creator]) ? j[creator] : [];
  const buys = arr
    .filter(t => t.type === 'buy')
    .map(t => ({ ts: Date.parse(t.timestamp), price: Number(t.priceUSD) }))
    .filter(o => Number.isFinite(o.ts) && Number.isFinite(o.price))
    .sort((a, b) => a.ts - b.ts);
  return buys;
}

// ---------- БД ----------
async function pickQueue(client) {
  const q = `
    SELECT ca
    FROM pump_tokens
    WHERE (enrich_status IN ('new','retry') OR enrich_status IS NULL)
      AND now() - inserted_at >= interval '${WAIT_MINUTES} minutes'
      AND (next_enrich_at IS NULL OR next_enrich_at <= now())
    ORDER BY inserted_at ASC
    LIMIT 25;
  `;
  const { rows } = await client.query(q);
  return rows.map(r => r.ca);
}

async function markRetry(client, ca, msg) {
  await client.query(
    `UPDATE pump_tokens
       SET enrich_status = 'retry',
           next_enrich_at = now() + interval '${RETRY_IN_MIN} minutes',
           last_error = $2,
           error_count = coalesce(error_count,0)+1
     WHERE ca = $1`,
    [ca, String(msg).slice(0, 500)]
  );
}

async function markOk(client, ca, payload) {
  const {
    t0, p0_price_usd, start_mc_usd,
    ath1, ath1Ts, ath5, ath5Ts, ath10, ath10Ts,
    supplyDisplay, creator,
    ath1x, ath1pct, ath5x, ath5pct, ath10x, ath10pct,
  } = payload;

  // ПОРЯДОК ПАРАМЕТРОВ ВАЖЕН!
  const sql = `
    UPDATE pump_tokens
       SET t0              = $2::timestamptz,
           p0_price_usd    = $3::numeric,
           start_mc_usd    = $4::numeric,
           ath_1m_usd      = $5::numeric,
           ath_1m_ts       = $6::timestamptz,
           ath_5m_usd      = $7::numeric,
           ath_5m_ts       = $8::timestamptz,
           ath_10m_usd     = $9::numeric,
           ath_10m_ts      = $10::timestamptz,
           supply_display  = COALESCE(supply_display, $11::numeric),
           creator         = COALESCE(creator, $12::text),
           ath_1m_x        = $13::numeric,
           ath_1m_pct      = $14::numeric,
           ath_5m_x        = $15::numeric,
           ath_5m_pct      = $16::numeric,
           ath_10m_x       = $17::numeric,
           ath_10m_pct     = $18::numeric,
           enrich_status   = 'ok',
           enriched_at     = now(),
           next_enrich_at  = NULL,
           last_error      = NULL
     WHERE ca = $1;
  `;

  const params = [
    ca,                  // $1
    t0,                  // $2
    p0_price_usd,        // $3
    start_mc_usd,        // $4
    ath1,                // $5
    ath1Ts,              // $6
    ath5,                // $7
    ath5Ts,              // $8
    ath10,               // $9
    ath10Ts,             // $10
    supplyDisplay,       // $11
    creator,             // $12
    ath1x,               // $13
    ath1pct,             // $14
    ath5x,               // $15
    ath5pct,             // $16
    ath10x,              // $17
    ath10pct,            // $18
  ];

  await client.query(sql, params);
}

// ---------- основной расчёт ----------
async function processOne(client, ca) {
  try {
    const repo = await getRepo(ca);
    const createdTs = +repo.created_timestamp;
    const creator   = repo.creator;
    const decimals  = (repo.decimals ?? 6);
    const total     = Number(repo.total_supply);
    const supplyDisplay = Number.isFinite(total) ? total / Math.pow(10, decimals) : null;

    if (!Number.isFinite(supplyDisplay) || supplyDisplay <= 0) {
      await markRetry(client, ca, 'bad supply_display');
      return { ca, status: 'retry(supply)' };
    }

    // dev-buy из batch
    const devBuys = await getDevBuys(ca, creator, createdTs);
    if (!devBuys.length) {
      await markRetry(client, ca, 'dev-buy not found yet');
      return { ca, status: 'retry(dev)' };
    }

    const t0ms = devBuys[0].ts;
    const p0   = devBuys[0].price;
    const startMc = mc(p0, supplyDisplay);

    // окно
    const winStart = ceilToMinute(t0ms);
    const end1  = winStart + 1 * 60 * 1000;
    const end5  = winStart + 5 * 60 * 1000;
    const end10 = winStart + 10 * 60 * 1000;

    // свечи
    let candles = await getCandles(ca, createdTs);
    if (!Array.isArray(candles)) candles = [];
    candles.sort((a,b) => a.timestamp - b.timestamp);

    let ath1 = null, ath1Ts = null;
    let ath5 = null, ath5Ts = null;
    let ath10= null, ath10Ts = null;

    for (const c of candles) {
      const ts = Number(c.timestamp);
      if (!Number.isFinite(ts)) continue;
      if (ts < winStart || ts > end10) continue;

      const price = Number(c.close ?? c.open);
      const mcap  = mc(price, supplyDisplay);
      if (mcap == null) continue;

      if (ts <= end1  && (ath1  == null || mcap > ath1 )) { ath1  = mcap; ath1Ts  = new Date(ts); }
      if (ts <= end5  && (ath5  == null || mcap > ath5 )) { ath5  = mcap; ath5Ts  = new Date(ts); }
      if (ts <= end10 && (ath10 == null || mcap > ath10)) { ath10 = mcap; ath10Ts = new Date(ts); }
    }

    // фолбэки: если в окне не было сделок — ставим стартовую капу
    if (ath1  == null) { ath1  = startMc; ath1Ts  = null; }
    if (ath5  == null) { ath5  = startMc; ath5Ts  = null; }
    if (ath10 == null) { ath10 = startMc; ath10Ts = null; }

    // множители и проценты (проценты округляем до 2 знаков, допускаем отрицательные)
    const ath1x   = Number.isFinite(startMc) && startMc > 0 ? ath1  / startMc : null;
    const ath5x   = Number.isFinite(startMc) && startMc > 0 ? ath5  / startMc : null;
    const ath10x  = Number.isFinite(startMc) && startMc > 0 ? ath10 / startMc : null;

    const ath1pct  = ath1x  == null ? null : round2((ath1x  - 1) * 100);
    const ath5pct  = ath5x  == null ? null : round2((ath5x  - 1) * 100);
    const ath10pct = ath10x == null ? null : round2((ath10x - 1) * 100);

    await markOk(client, ca, {
      t0: new Date(t0ms),
      p0_price_usd: p0,
      start_mc_usd: startMc,
      ath1, ath1Ts, ath5, ath5Ts, ath10, ath10Ts,
      supplyDisplay, creator,
      ath1x, ath1pct, ath5x, ath5pct, ath10x, ath10pct,
    });

    console.log('enriched', ca);
    return { ca, status: 'ok' };

  } catch (e) {
    await markRetry(client, ca, e.message || String(e));
    console.warn('loop error:', e.message || e);
    return { ca, status: 'retry(err)' };
  }
}

// ---------- цикл ----------
async function loop() {
  console.log(`enricher started | wait=${WAIT_MINUTES}m | pause=${PAUSE_MS}ms`);
  const client = await pool.connect();
  try {
    while (true) {
      const cas = await pickQueue(client);
      if (!cas.length) {
        await sleep(10_000);
        continue;
      }
      for (const ca of cas) {
        await processOne(client, ca);
        await sleep(PAUSE_MS);
      }
    }
  } finally {
    client.release();
  }
}

loop().catch(e => {
  console.error('fatal', e);
  process.exit(1);
});
