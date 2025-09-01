// enrich.js — S1: считаем только первые 10 минут свечей от dev-buy
// sequential + 5s pause + retry/backoff for 429

import 'dotenv/config';
import fetch from 'node-fetch';
import { Pool } from 'pg';

// ---------- настройки ----------
const DB_URL         = process.env.DATABASE_URL || process.env.NEON_DATABASE_URL;
const PAUSE_MS       = 5000;                      // пауза между токенами
const WAIT_MINUTES   = 10;                        // считаем только, когда токену >= 10 мин
const CANDLE_LIMIT   = 30;                        // запросим ~30 баров 1m
const INTERVAL       = '1m';
const CURRENCY       = 'USD';
const RETRY_IN_MIN   = 15;                        // повторная попытка через 15 минут при фолбэке
const MAX_FETCH_RETRY = 4;                        // ретраи на 429/сеть

// ---------- PG ----------
const pool = new Pool({ connectionString: DB_URL });

// ---------- helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const ceilToMinute = (ts) => Math.ceil(ts / 60000) * 60000;

function mc(price, supplyDisplay) {
  const p = Number(price);
  if (!Number.isFinite(p)) return null;
  return p * Number(supplyDisplay);
}

async function fetchJson(url, options = {}, retry = 0) {
  const r = await fetch(url, options);
  if (r.status === 429 || r.status === 403 || r.status === 503) {
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
  if (r.status === 429 || r.status === 403 || r.status === 503) {
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
  // ищем первые покупки дева в окне [createdTs-30s; createdTs+5m]
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
  // берём токены только старше 10 минут и нужного статуса
  const q = `
    select ca
    from pump_tokens
    where (enrich_status in ('new','retry') or enrich_status is null)
      and now() - inserted_at >= interval '${WAIT_MINUTES} minutes'
      and (next_enrich_at is null or next_enrich_at <= now())
    order by inserted_at asc
    limit 25
  `;
  const { rows } = await client.query(q);
  return rows.map(r => r.ca);
}

async function markRetry(client, ca, msg) {
  await client.query(
    `update pump_tokens
       set enrich_status = 'retry',
           next_enrich_at = now() + interval '${RETRY_IN_MIN} minutes',
           last_error = $2,
           error_count = coalesce(error_count,0)+1
     where ca = $1`,
    [ca, String(msg).slice(0, 500)]
  );
}

async function markOk(client, ca, payload) {
  const {
    t0, p0_price_usd, start_mc_usd,
    ath1, ath1Ts, ath5, ath5Ts, ath10, ath10Ts,
    supplyDisplay
  } = payload;

  await client.query(
    `update pump_tokens
       set t0 = $2,
           p0_price_usd = $3,
           start_mc_usd = $4,
           ath_1m_usd = $5,
           ath_1m_ts  = $6,
           ath_5m_usd = $7,
           ath_5m_ts  = $8,
           ath_10m_usd= $9,
           ath_10m_ts = $10,
           supply_display = coalesce(supply_display, $11),
           enrich_status = 'ok',
           enriched_at = now(),
           next_enrich_at = null,
           last_error = null
     where ca = $1`,
    [ca, t0, p0_price_usd, start_mc_usd, ath1, ath1Ts, ath5, ath5Ts, ath10, ath10Ts, supplyDisplay]
  );
}

// ---------- основной расчёт ----------
async function processOne(client, ca) {
  try {
    const repo = await getRepo(ca);
    const createdTs = +repo.created_timestamp;
    const creator   = repo.creator;
    const decimals  = (repo.decimals ?? 6);
    const supplyDisplay = Number(repo.total_supply) / Math.pow(10, decimals);

    // dev-buy
    const devBuys = await getDevBuys(ca, creator, createdTs);
    if (!devBuys.length) {
      // строго от dev-buy, поэтому переносим
      await markRetry(client, ca, 'dev-buy not found yet');
      return { ca, status: 'retry(dev)' };
    }
    const t0 = devBuys[0].ts;
    const p0 = devBuys[0].price;
    const startMc = mc(p0, supplyDisplay);

    // окна
    const winStart = ceilToMinute(t0);
    const end1  = winStart + 1*60*1000;
    const end5  = winStart + 5*60*1000;
    const end10 = winStart +10*60*1000;

    // свечи: один запрос, обрежем по времени
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

    // фолбэки — чтобы поля не были NULL
    if (ath1  == null) { ath1  = startMc; ath1Ts  = null; }
    if (ath5  == null) { ath5  = startMc; ath5Ts  = null; }
    if (ath10 == null) { ath10 = startMc; ath10Ts = null; }

    await markOk(client, ca, {
      t0: new Date(t0),
      p0_price_usd: p0,
      start_mc_usd: startMc,
      ath1, ath1Ts, ath5, ath5Ts, ath10, ath10Ts,
      supplyDisplay
    });

    console.log('enriched', ca);
    return { ca, status: 'ok' };
  } catch (e) {
    // 429 и пр. — переносим
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
