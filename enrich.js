// enrich.js
import { Pool } from 'pg';

// ---------- CONFIG ----------
const DB_URL = process.env.DATABASE_URL;
if (!DB_URL) {
  console.error('DATABASE_URL is not set');
  process.exit(1);
}

const MIN_AGE_MIN = 10;          // берём токены, старше 10 минут
const PAUSE_MS = 5_000;          // пауза между токенами
const SELECT_LIMIT = 1;          // по одному токену
const TRADES_LIMIT = 100;        // лимит /trades/batch
const CANDLES_LIMIT = 30;        // берём ~30 минутных свечей
const CURRENCY = 'USD';
const INTERVAL = '1m';

// ---------- DB ----------
const pool = new Pool({
  connectionString: DB_URL,
  ssl: { rejectUnauthorized: false },
});

// ---------- HELPERS ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const ceilToMinute = (tsMs) => Math.ceil(tsMs / 60_000) * 60_000;

const safeNum = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

function athInWindow(candles, supplyDisplay, startMs, windowMinutes) {
  const end = startMs + windowMinutes * 60_000;
  let top = null;
  let topTs = null;

  for (const c of candles) {
    const t = Number(c.timestamp);
    if (!Number.isFinite(t)) continue;
    if (t < startMs || t > end) continue;

    const price = safeNum(c.close ?? c.open);
    if (price == null) continue;
    const mc = price * supplyDisplay;
    if (top == null || mc > top) {
      top = mc;
      topTs = t;
    }
  }
  return { usd: top, ts: topTs };
}

function globalAthFromCandles(candles, supplyDisplay) {
  let top = null;
  let topTs = null;
  for (const c of candles) {
    const t = Number(c.timestamp);
    if (!Number.isFinite(t)) continue;
    const price = safeNum(c.close ?? c.open);
    if (price == null) continue;
    const mc = price * supplyDisplay;
    if (top == null || mc > top) {
      top = mc;
      topTs = t;
    }
  }
  return { usd: top, ts: topTs };
}

function deriveXandPct(startMc, athUsd) {
  if (!Number.isFinite(startMc) || startMc <= 0 || !Number.isFinite(athUsd)) {
    return { x: null, pct: null };
  }
  const x = athUsd / startMc;
  const pct = (x - 1) * 100;
  return { x, pct };
}

function isRateLimit(res, text) {
  if (res && (res.status === 429 || res.status === 403)) return true;
  // Cloudflare html page often includes 'You are being rate limited' or 'cf-error-details'
  return text && /rate limited|cloudflare|cf-error/i.test(text);
}

// ---------- API ----------
async function fetchJson(url, opts = {}) {
  const r = await fetch(url, opts);
  let bodyText = '';
  try { bodyText = await r.text(); } catch {}
  if (!r.ok) {
    if (isRateLimit(r, bodyText)) {
      const e = new Error('rate-limit');
      e.isRate = true;
      throw e;
    }
    throw new Error(`${r.status} ${url} -> ${bodyText.slice(0, 200)}`);
  }
  try {
    return JSON.parse(bodyText);
  } catch (e) {
    throw new Error(`bad json from ${url}: ${bodyText.slice(0, 200)}`);
  }
}

async function getRepo(ca) {
  const url = `https://frontend-api-v3.pump.fun/coins/${ca}`;
  return fetchJson(url);
}

async function getCandles(ca, createdTs) {
  const u = new URL(`https://swap-api.pump.fun/v2/coins/${ca}/candles`);
  u.searchParams.set('interval', INTERVAL);
  u.searchParams.set('limit', String(CANDLES_LIMIT));
  u.searchParams.set('currency', CURRENCY);
  u.searchParams.set('createdTs', String(createdTs));
  const arr = await fetchJson(u.toString());
  if (!Array.isArray(arr)) return [];
  arr.sort((a, b) => Number(a.timestamp) - Number(b.timestamp));
  for (const c of arr) {
    c.open  = safeNum(c.open);
    c.high  = safeNum(c.high);
    c.low   = safeNum(c.low);
    c.close = safeNum(c.close);
  }
  return arr;
}

async function devBuyBatch(ca, creator, afterTs, beforeTs, limit = TRADES_LIMIT) {
  const url = `https://swap-api.pump.fun/v1/coins/${ca}/trades/batch`;
  const body = {
    userAddresses: [creator],
    limit: Math.min(limit, TRADES_LIMIT),
    afterTs,
    beforeTs,
  };
  const r = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const t = await r.text();
  if (!r.ok) {
    if (isRateLimit(r, t)) {
      const e = new Error('rate-limit');
      e.isRate = true;
      throw e;
    }
    throw new Error(`trades/batch ${r.status}: ${t.slice(0, 200)}`);
  }
  const j = JSON.parse(t);
  const rows = Array.isArray(j?.[creator]) ? j[creator] : [];
  // first buy
  const buys = rows
    .filter(x => x?.type === 'buy')
    .map(x => ({
      ts: Date.parse(x.timestamp),
      price: safeNum(x.priceUSD),
    }))
    .filter(o => Number.isFinite(o.ts) && Number.isFinite(o.price))
    .sort((a, b) => a.ts - b.ts);
  return buys[0] || null;
}

// ---------- DB helpers ----------
async function selectNextToken(client) {
  const sql = `
    SELECT ca
    FROM pump_tokens
    WHERE (enrich_status IS NULL OR enrich_status IN ('new','retry'))
      AND inserted_at <= now() - interval '${MIN_AGE_MIN} minutes'
      AND (next_enrich_at IS NULL OR next_enrich_at <= now())
    ORDER BY inserted_at
    LIMIT ${SELECT_LIMIT};
  `;
  const { rows } = await client.query(sql);
  return rows.map(r => r.ca);
}

async function markRetry(client, ca, msg, minutes = 30) {
  await client.query(
    `
    UPDATE pump_tokens
    SET enrich_status = 'retry',
        next_enrich_at = now() + interval '${minutes} minutes',
        last_error = $2,
        error_count = COALESCE(error_count, 0) + 1
    WHERE ca = $1
    `,
    [ca, String(msg).slice(0, 500)],
  );
}

async function markError(client, ca, msg) {
  await client.query(
    `
    UPDATE pump_tokens
    SET enrich_status = 'err',
        next_enrich_at = now() + interval '60 minutes',
        last_error = $2,
        error_count = COALESCE(error_count, 0) + 1
    WHERE ca = $1
    `,
    [ca, String(msg).slice(0, 500)],
  );
}

async function markOk(client, data) {
  // ВАЖНО: явные приведения типов — никаких "could not determine data type …"
  const sql = `
  UPDATE pump_tokens
  SET
    creator          = $1,
    t0               = to_timestamp($2::double precision / 1000.0),
    p0_price_usd     = $3::numeric,
    start_mc_usd     = $4::numeric,

    ath_1m_usd       = $5::numeric,
    ath_1m_ts        = CASE WHEN $6 IS NULL  THEN NULL
                            ELSE to_timestamp($6::double precision / 1000.0) END,

    ath_5m_usd       = $7::numeric,
    ath_5m_ts        = CASE WHEN $8 IS NULL  THEN NULL
                            ELSE to_timestamp($8::double precision / 1000.0) END,

    ath_10m_usd      = $9::numeric,
    ath_10m_ts       = CASE WHEN $10 IS NULL THEN NULL
                            ELSE to_timestamp($10::double precision / 1000.0) END,

    ath_all_usd      = $11::numeric,
    ath_all_ts       = CASE WHEN $12 IS NULL THEN NULL
                            ELSE to_timestamp($12::double precision / 1000.0) END,

    ath_1m_x         = $13::numeric,
    ath_5m_x         = $14::numeric,
    ath_10m_x        = $15::numeric,

    ath_1m_pct       = $16::numeric,
    ath_5m_pct       = $17::numeric,
    ath_10m_pct      = $18::numeric,

    supply_display   = $19::numeric,

    enrich_status    = 'ok',
    enriched_at      = now(),
    next_enrich_at   = NULL,
    last_error       = NULL,
    error_count      = 0
  WHERE ca = $20
  `;
  const params = [
    data.creator,          // $1
    data.t0Ms,             // $2
    data.p0PriceUsd,       // $3
    data.startMcUsd,       // $4
    data.ath1mUsd,         // $5
    data.ath1mTsMs,        // $6
    data.ath5mUsd,         // $7
    data.ath5mTsMs,        // $8
    data.ath10mUsd,        // $9
    data.ath10mTsMs,       // $10
    data.athAllUsd,        // $11
    data.athAllTsMs,       // $12
    data.ath1mX,           // $13
    data.ath5mX,           // $14
    data.ath10mX,          // $15
    data.ath1mPct,         // $16
    data.ath5mPct,         // $17
    data.ath10mPct,        // $18
    data.supplyDisplay,    // $19
    data.ca,               // $20
  ];
  await pool.query(sql, params);
}

// ---------- CORE ----------
async function processOne(client, ca) {
  try {
    // 1) repo
    const repo = await getRepo(ca);
    const createdTs = +repo.created_timestamp; // ms
    const creator = repo.creator || null;
    const decimals = Number.isFinite(repo.decimals) ? repo.decimals : 6;
    const totalSupply = safeNum(repo.total_supply) ?? 0;
    const supplyDisplay = totalSupply / Math.pow(10, decimals);

    // 2) dev-buy
    if (!creator || !Number.isFinite(createdTs)) {
      await markRetry(client, ca, 'missing creator or createdTs', 30);
      return;
    }
    const dev = await devBuyBatch(
      ca,
      creator,
      createdTs - 30_000,
      createdTs + 5 * 60_000,
      TRADES_LIMIT
    );
    if (!dev) {
      await markRetry(client, ca, 'dev-buy not found', 30);
      return;
    }
    const t0Ms = dev.ts;
    const p0PriceUsd = dev.price;
    if (!Number.isFinite(t0Ms) || !Number.isFinite(p0PriceUsd) || supplyDisplay <= 0) {
      await markRetry(client, ca, 'bad t0/p0/supply', 30);
      return;
    }
    const startMcUsd = p0PriceUsd * supplyDisplay;

    // 3) свечи
    const candles = await getCandles(ca, createdTs);
    const minStart = ceilToMinute(t0Ms);

    const a1 = athInWindow(candles, supplyDisplay, minStart, 1);
    const a5 = athInWindow(candles, supplyDisplay, minStart, 5);
    const a10 = athInWindow(candles, supplyDisplay, minStart, 10);
    const aall = globalAthFromCandles(candles, supplyDisplay);

    // fallback'и
    const ath1mUsd = a1.usd ?? startMcUsd;
    const ath1mTsMs = a1.ts ?? null;

    const ath5mUsd = a5.usd ?? startMcUsd;
    const ath5mTsMs = a5.ts ?? null;

    const ath10mUsd = a10.usd ?? startMcUsd;
    const ath10mTsMs = a10.ts ?? null;

    // глобальный ATH — если в свечах нет, возьмём repo.usd_market_cap
    let athAllUsd = aall.usd ?? safeNum(repo.usd_market_cap) ?? null;
    let athAllTsMs = aall.ts ?? null;

    // 4) производные (X и %)
    const { x: x1, pct: p1 } = deriveXandPct(startMcUsd, ath1mUsd);
    const { x: x5, pct: p5 } = deriveXandPct(startMcUsd, ath5mUsd);
    const { x: x10, pct: p10 } = deriveXandPct(startMcUsd, ath10mUsd);

    // 5) save
    await markOk(client, {
      ca,
      creator,
      t0Ms,
      p0PriceUsd,
      startMcUsd,
      supplyDisplay,

      ath1mUsd,  ath1mTsMs,
      ath5mUsd,  ath5mTsMs,
      ath10mUsd, ath10mTsMs,

      athAllUsd, athAllTsMs,

      ath1mX:  x1,  ath1mPct:  p1,
      ath5mX:  x5,  ath5mPct:  p5,
      ath10mX: x10, ath10mPct: p10,
    });

    console.log('enriched', ca);
  } catch (e) {
    // rate-limit → переназначаем next_enrich_at и выходим
    if (e?.isRate || /rate-limit/i.test(String(e?.message))) {
      await markRetry(client, ca, 'rate-limit', 60);
      console.warn('rate-limit, delayed', ca);
      return;
    }
    // прочие ошибки — ставим err (но продолжаем цикл)
    await markError(client, ca, e?.message || e);
    console.error('fatal error:', e?.message || e);
  }
}

async function loop() {
  console.log(`enricher started | wait=${MIN_AGE_MIN}m | pause=${PAUSE_MS}ms`);
  while (true) {
    const client = await pool.connect();
    try {
      const cas = await selectNextToken(client);
      client.release();

      if (!cas.length) {
        await sleep(PAUSE_MS);
        continue;
      }

      for (const ca of cas) {
        await processOne(pool, ca);
        await sleep(PAUSE_MS);
      }
    } catch (e) {
      client.release();
      console.error('loop error:', e?.message || e);
      await sleep(10_000);
    }
  }
}

loop().catch(e => {
  console.error(e);
  process.exit(1);
});
