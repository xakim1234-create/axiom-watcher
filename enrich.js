// enrich.js — S1: первые 10 минут от dev-buy, с 2м запасом (12м фильтр).
import 'dotenv/config';
import fetch from 'node-fetch';
import { Pool } from 'pg';

// ---------- настройки ----------
const DB_URL           = process.env.DATABASE_URL || process.env.NEON_DATABASE_URL;
const PAUSE_MS         = 5000;   // пауза между токенами
const WAIT_MINUTES     = 10;     // считаем только, когда токену >= 10 мин
const CANDLE_LIMIT     = 30;     // берём ~30 баров 1m, но дальше фильтруем окном 12м
const INTERVAL         = '1m';
const CURRENCY         = 'USD';
const RETRY_IN_MIN     = 15;     // повтор при сбое/дыре
const MAX_FETCH_RETRY  = 4;      // ретраи на 429/403/503

if (!DB_URL) {
  console.error('DATABASE_URL is not set');
  process.exit(1);
}

// ---------- PG ----------
const pool = new Pool({ connectionString: DB_URL, ssl: { rejectUnauthorized: false } });

// ---------- helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const ceilToMinute = (ts) => Math.ceil(ts / 60000) * 60000;

const round = (v, dp) => (v == null || !Number.isFinite(v)) ? null
  : Math.round(v * 10**dp) / 10**dp;

function mc(price, supplyDisplay) {
  const p = Number(price);
  if (!Number.isFinite(p)) return null;
  return p * Number(supplyDisplay);
}

function isRate(r, text='') {
  return r && (r.status === 429 || r.status === 403 || r.status === 503) ||
         /rate limited|cloudflare|cf-error/i.test(text);
}

async function fetchJson(url, options = {}, retry = 0) {
  const r = await fetch(url, options);
  let text = '';
  try { text = await r.text(); } catch {}
  if (!r.ok) {
    if (isRate(r, text) && retry < MAX_FETCH_RETRY) {
      const backoff = 2000 * Math.pow(2, retry); // 2s,4s,8s,16s
      await sleep(backoff);
      return fetchJson(url, options, retry + 1);
    }
    throw new Error(`${url} -> ${r.status} ${text.slice(0,200)}`);
  }
  try { return JSON.parse(text); }
  catch { throw new Error(`bad json from ${url}: ${text.slice(0,200)}`); }
}

async function postJson(url, body, retry = 0) {
  const r = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  let text = '';
  try { text = await r.text(); } catch {}
  if (!r.ok) {
    if (isRate(r, text) && retry < MAX_FETCH_RETRY) {
      const backoff = 2000 * Math.pow(2, retry);
      await sleep(backoff);
      return postJson(url, body, retry + 1);
    }
    throw new Error(`${url} -> ${r.status} ${text.slice(0,200)}`);
  }
  try { return JSON.parse(text); }
  catch { throw new Error(`bad json from ${url}: ${text.slice(0,200)}`); }
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
  const arr = await fetchJson(url.toString());
  if (!Array.isArray(arr)) return [];
  arr.sort((a,b) => Number(a.timestamp) - Number(b.timestamp));
  // привести к числам
  for (const c of arr) {
    c.open  = Number(c.open);
    c.high  = Number(c.high);
    c.low   = Number(c.low);
    c.close = Number(c.close);
  }
  return arr;
}

async function getDevBuys(mint, creator, createdTs) {
  const afterTs  = createdTs - 30_000;
  const beforeTs = createdTs + 5*60_000;
  const url = `https://swap-api.pump.fun/v1/coins/${mint}/trades/batch`;
  const j = await postJson(url, {
    userAddresses: [creator],
    limit: 100,
    afterTs, beforeTs,
  });
  const arr = Array.isArray(j?.[creator]) ? j[creator] : [];
  const buys = arr
    .filter(t => t.type === 'buy')
    .map(t => ({ ts: Date.parse(t.timestamp), price: Number(t.priceUSD) }))
    .filter(o => Number.isFinite(o.ts) && Number.isFinite(o.price))
    .sort((a,b) => a.ts - b.ts);
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
    LIMIT 25
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
           error_count = COALESCE(error_count,0)+1
     WHERE ca = $1`,
    [ca, String(msg).slice(0, 500)]
  );
}

async function markOk(client, ca, payload) {
  const {
    creator, t0Ms, p0PriceUsd, startMcUsd, supplyDisplay,
    ath1mUsd, ath1mTsMs, ath5mUsd, ath5mTsMs, ath10mUsd, ath10mTsMs,
    ath1mX, ath1mPct, ath5mX, ath5mPct, ath10mX, ath10mPct
  } = payload;

  // ЯВНЫЕ КАСТЫ: никаких "could not determine data type of parameter $N"
  const UPDATE_SQL = `
  UPDATE pump_tokens SET
    creator        = COALESCE(creator, $2::text),

    t0             = CASE WHEN $3 IS NULL THEN NULL
                          ELSE to_timestamp($3::double precision/1000.0) END,
    p0_price_usd   = $4::numeric,
    start_mc_usd   = $5::numeric,

    ath_1m_usd     = $6::numeric,
    ath_1m_ts      = CASE WHEN $7 IS NULL THEN NULL
                          ELSE to_timestamp($7::double precision/1000.0) END,

    ath_5m_usd     = $8::numeric,
    ath_5m_ts      = CASE WHEN $9 IS NULL THEN NULL
                          ELSE to_timestamp($9::double precision/1000.0) END,

    ath_10m_usd    = $10::numeric,
    ath_10m_ts     = CASE WHEN $11 IS NULL THEN NULL
                          ELSE to_timestamp($11::double precision/1000.0) END,

    ath_1m_x       = $12::numeric,
    ath_1m_pct     = $13::numeric,
    ath_5m_x       = $14::numeric,
    ath_5m_pct     = $15::numeric,
    ath_10m_x      = $16::numeric,
    ath_10m_pct    = $17::numeric,

    supply_display = COALESCE(supply_display, $18::numeric),

    enrich_status  = 'ok',
    enriched_at    = now(),
    next_enrich_at = NULL,
    last_error     = NULL
  WHERE ca = $1::text
  `;

  const params = [
    ca,                // $1
    creator ?? null,   // $2
    t0Ms ?? null,      // $3  (ms)
    p0PriceUsd ?? null,// $4
    startMcUsd ?? null,// $5

    ath1mUsd ?? null,  // $6
    ath1mTsMs ?? null, // $7
    ath5mUsd ?? null,  // $8
    ath5mTsMs ?? null, // $9
    ath10mUsd ?? null, // $10
    ath10mTsMs ?? null,// $11

    ath1mX ?? null,    // $12
    ath1mPct ?? null,  // $13
    ath5mX ?? null,    // $14
    ath5mPct ?? null,  // $15
    ath10mX ?? null,   // $16
    ath10mPct ?? null, // $17

    supplyDisplay ?? null // $18
  ];
  await client.query(UPDATE_SQL, params);

  // token_snapshots — одна фиксация, без дублей (NOT EXISTS)
  await client.query(
    `
    INSERT INTO token_snapshots
      (ca, snap_ts, usd_market_cap, supply_display,
       dev, p0_price_usd, start_mc_usd,
       ath_5m_usd, ath_5m_ts)
    SELECT
      $1, now(), NULL, $2,
      $3, $4, $5,
      $6, CASE WHEN $7 IS NULL THEN NULL ELSE to_timestamp($7::double precision/1000.0) END
    WHERE NOT EXISTS (SELECT 1 FROM token_snapshots WHERE ca = $1)
    `,
    [
      ca,
      supplyDisplay ?? null,
      creator ?? null,
      p0PriceUsd ?? null,
      startMcUsd ?? null,
      ath5mUsd ?? null,
      ath5mTsMs ?? null,
    ]
  );
}

// ---------- основной расчёт ----------
async function processOne(client, ca) {
  try {
    // repo → creator, supply, createdTs
    const repo = await getRepo(ca);
    const createdTs = +repo.created_timestamp;
    const creator   = repo.creator;
    const decimals  = Number.isFinite(repo.decimals) ? repo.decimals : 6;
    const supply    = Number(repo.total_supply) || 0;
    const supplyDisplay = supply / Math.pow(10, decimals);

    if (!creator || !Number.isFinite(createdTs) || supplyDisplay <= 0) {
      await markRetry(client, ca, 'missing creator/createdTs/supply');
      return;
    }

    // dev-buy (первая покупка дева)
    const devBuys = await getDevBuys(ca, creator, createdTs);
    if (!devBuys.length) {
      await markRetry(client, ca, 'dev-buy not found yet');
      return;
    }
    const t0Ms = devBuys[0].ts;
    const p0   = devBuys[0].price;
    if (!Number.isFinite(t0Ms) || !Number.isFinite(p0)) {
      await markRetry(client, ca, 'bad t0/p0');
      return;
    }

    const startMc = mc(p0, supplyDisplay);

    // свечи → фильтруем только окно [ceil(t0) .. +12m]
    const candles = await getCandles(ca, createdTs);
    const winStart = ceilToMinute(t0Ms);
    const end1  = winStart + 1*60*1000;
    const end5  = winStart + 5*60*1000;
    const end10 = winStart +10*60*1000;
    const end12 = winStart +12*60*1000;

    let ath1 = null, ath1Ts = null;
    let ath5 = null, ath5Ts = null;
    let ath10= null, ath10Ts = null;

    for (const c of candles) {
      const ts = Number(c.timestamp);
      if (!Number.isFinite(ts)) continue;
      if (ts < winStart || ts > end12) continue; // за пределы 12 минут не выходим

      const price = Number.isFinite(c.close) ? c.close : c.open;
      const mcap  = mc(price, supplyDisplay);
      if (mcap == null) continue;

      if (ts <= end1  && (ath1  == null || mcap > ath1 )) { ath1  = mcap; ath1Ts  = ts; }
      if (ts <= end5  && (ath5  == null || mcap > ath5 )) { ath5  = mcap; ath5Ts  = ts; }
      if (ts <= end10 && (ath10 == null || mcap > ath10)) { ath10 = mcap; ath10Ts = ts; }
    }

    // фолбэки к старту (даст 0%)
    if (ath1  == null) { ath1  = startMc; ath1Ts  = null; }
    if (ath5  == null) { ath5  = startMc; ath5Ts  = null; }
    if (ath10 == null) { ath10 = startMc; ath10Ts = null; }

    // X и %
    const x1  = (Number.isFinite(ath1)  && Number.isFinite(startMc) && startMc>0) ? (ath1 / startMc)  : null;
    const x5  = (Number.isFinite(ath5)  && Number.isFinite(startMc) && startMc>0) ? (ath5 / startMc)  : null;
    const x10 = (Number.isFinite(ath10) && Number.isFinite(startMc) && startMc>0) ? (ath10 / startMc) : null;

    const pct1  = (x1  == null) ? null : (x1  - 1) * 100;
    const pct5  = (x5  == null) ? null : (x5  - 1) * 100;
    const pct10 = (x10 == null) ? null : (x10 - 1) * 100;

    // округления: X до 3 знаков, % до 2 знаков
    const payload = {
      creator,
      t0Ms,
      p0PriceUsd: p0,
      startMcUsd: startMc,
      supplyDisplay,

      ath1mUsd: ath1,  ath1mTsMs: ath1Ts,
      ath5mUsd: ath5,  ath5mTsMs: ath5Ts,
      ath10mUsd: ath10,ath10mTsMs: ath10Ts,

      ath1mX:  round(x1, 3),
      ath1mPct: round(pct1, 2),
      ath5mX:  round(x5, 3),
      ath5mPct: round(pct5, 2),
      ath10mX: round(x10,3),
      ath10mPct: round(pct10,2),
    };

    await markOk(client, ca, payload);
    console.log('enriched', ca);
  } catch (e) {
    await markRetry(client, ca, e?.message || String(e));
    console.warn('loop error:', e?.message || e);
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

loop().catch(e => { console.error('fatal', e); process.exit(1); });
