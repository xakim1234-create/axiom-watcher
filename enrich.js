// enrich.js
import pg from "pg";

// --- настройки очереди/лимитов ---
const WAIT_MINUTES = 10;        // ждём, пока токену исполнится 10 минут
const PAUSE_MS     = 5000;      // пауза между токенами
const RL_BASE_MS   = 120000;    // базовая пауза при 429 (2 мин), растёт экспоненциально
const MAX_429_TRIES= 5;

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL || process.env.POSTGRES_URL,
  ssl: { rejectUnauthorized: false },
});

// утилиты
const sleep = (ms)=> new Promise(r=>setTimeout(r, ms));
const ceilToMinute = (tsMs)=> Math.ceil(tsMs/60000)*60000;

// API helpers
async function getRepo(ca){
  const url = `https://frontend-api-v3.pump.fun/coins/${ca}`;
  const r = await fetch(url);
  if (!r.ok) throw new Error(`repo ${r.status}`);
  return r.json();
}
async function getCandles10m(ca, createdTs){
  // тянем мало свечей, только что нужно для 10 минут, без пагинации назад
  const params = new URLSearchParams({
    interval: "1m",
    limit: "30",
    currency: "USD",
    createdTs: String(createdTs),
  });
  const url = `https://swap-api.pump.fun/v2/coins/${ca}/candles?${params.toString()}`;
  const r = await fetch(url);
  if (r.status === 429) throw Object.assign(new Error("429"), {code:429});
  if (!r.ok) throw new Error(`candles ${r.status}`);
  const a = await r.json();
  if (!Array.isArray(a)) return [];
  a.sort((x,y)=>x.timestamp-y.timestamp);
  // привести к числам
  for (const c of a) {
    c.open  = Number(c.open);
    c.high  = Number(c.high);
    c.low   = Number(c.low);
    c.close = Number(c.close);
  }
  return a;
}
async function devBatchFirstBuy(ca, creator, t0min, t0max){
  // первый buy от создателя в окне [-30с ; +5м]
  const url = `https://swap-api.pump.fun/v1/coins/${ca}/trades/batch`;
  const body = {
    userAddresses: [creator],
    limit: 100,
    afterTs:  t0min,
    beforeTs: t0max
  };
  const r = await fetch(url, {
    method: "POST",
    headers: {"content-type":"application/json"},
    body: JSON.stringify(body)
  });
  if (r.status === 429) throw Object.assign(new Error("429"), {code:429});
  if (!r.ok) throw new Error(`trades/batch ${r.status}: ${await r.text()}`);
  const j = await r.json();
  const arr = Array.isArray(j?.[creator]) ? j[creator] : [];
  const buys = arr
    .filter(t=>t.type==="buy")
    .map(t => ({ ts: Date.parse(t.timestamp), priceUSD: Number(t.priceUSD) }))
    .filter(o => Number.isFinite(o.ts) && Number.isFinite(o.priceUSD))
    .sort((a,b)=>a.ts-b.ts);
  return buys[0] || null;
}

// DB helpers
async function markRetry(client, ca, message, addMinutes = 10){
  await client.query(
    `
    UPDATE pump_tokens
    SET enrich_status='retry',
        next_enrich_at = now() + ($2::text || ' minutes')::interval,
        last_error     = $3,
        error_count    = COALESCE(error_count,0)+1
    WHERE ca=$1;
    `,[ca, String(addMinutes), message?.toString()?.slice(0,4000) || null]
  );
}
async function markErr(client, ca, message){
  await client.query(
    `
    UPDATE pump_tokens
    SET enrich_status='err',
        next_enrich_at = NULL,
        last_error     = $2,
        error_count    = COALESCE(error_count,0)+1
    WHERE ca=$1;
    `,[ca, message?.toString()?.slice(0,4000) || null]
  );
}

function pickPrice(c){ // безопасно берём цену свечи
  if (!c) return null;
  const v = Number.isFinite(c.close) ? c.close :
            Number.isFinite(c.open)  ? c.open  : null;
  return Number.isFinite(v) ? v : null;
}

// основной расчёт для S1
function calcAthWindows(candles, t0ms, supplyDisplay){
  const startBar = ceilToMinute(t0ms);
  const w1End  = startBar + 1*60*1000;
  const w5End  = startBar + 5*60*1000;
  const w10End = startBar +10*60*1000;

  let ath1 = {usd:null, ts:null};
  let ath5 = {usd:null, ts:null};
  let ath10= {usd:null, ts:null};

  for (const c of candles){
    const p = pickPrice(c);
    if (!Number.isFinite(p)) continue;
    const mc = p * supplyDisplay;
    const ts = c.timestamp;

    if (ts >= startBar && ts <= w1End){
      if (ath1.usd==null || mc>ath1.usd){ ath1.usd=mc; ath1.ts=ts; }
    }
    if (ts >= startBar && ts <= w5End){
      if (ath5.usd==null || mc>ath5.usd){ ath5.usd=mc; ath5.ts=ts; }
    }
    if (ts >= startBar && ts <= w10End){
      if (ath10.usd==null || mc>ath10.usd){ ath10.usd=mc; ath10.ts=ts; }
    }
  }
  return {ath1, ath5, ath10, startBar};
}

function ratioOrNull(numer, denom){
  if (!Number.isFinite(numer) || !Number.isFinite(denom) || denom===0) return null;
  return numer / denom;
}

async function processOne(client, row){
  const ca = row.ca;

  // если токен моложе 10 минут — перенесём позже
  const tooYoung = await client.query(
    `SELECT (now() - inserted_at) < interval '${WAIT_MINUTES} minutes' AS young
       FROM pump_tokens WHERE ca=$1`, [ca]
  );
  if (tooYoung.rows[0]?.young){
    await markRetry(client, ca, 'too-young', WAIT_MINUTES);
    return;
  }

  let repo;
  try {
    repo = await getRepo(ca);
  } catch(e) {
    if (e.code===429){ await markRetry(client, ca, 'repo-429', 10); throw e; }
    await markRetry(client, ca, `repo-error ${e.message}`, 15);
    return;
  }

  const creator = repo.creator || null;
  const createdTs = Number(repo.created_timestamp) || Date.now();   // ms
  const decimals  = Number.isFinite(repo.decimals) ? Number(repo.decimals) : 6;
  const supplyDisplay = Number(repo.total_supply) ? (Number(repo.total_supply) / 10**decimals) : 1e9;

  // первая покупка девелопера — база
  let dev;
  try {
    dev = await devBatchFirstBuy(
      ca, creator,
      createdTs - 30_000,
      createdTs + 5*60_000
    );
  } catch(e){
    if (e.code===429){ await markRetry(client, ca, 'batch-429', 10); throw e; }
    await markRetry(client, ca, `batch-error ${e.message}`, 15);
    return;
  }
  if (!dev){
    await markRetry(client, ca, 'no-dev-buy', 20);
    return;
  }

  const t0ms = dev.ts;
  const p0   = dev.priceUSD;
  const startMc = p0 * supplyDisplay;

  // свечи только для первых 10 минут
  let candles=[];
  for (let i=0;i<MAX_429_TRIES;i++){
    try{
      candles = await getCandles10m(ca, createdTs);
      break;
    }catch(e){
      if (e.code===429){
        await sleep(RL_BASE_MS * Math.pow(1.6, i));
        continue;
      }
      await markRetry(client, ca, `candles-error ${e.message}`, 15);
      return;
    }
  }

  const {ath1, ath5, ath10, startBar} = calcAthWindows(candles, t0ms, supplyDisplay);

  // фолбэк: если нет данных по окну — берём стартовую MC и время старта бара
  const ath1_usd = Number.isFinite(ath1.usd) ? ath1.usd : startMc;
  const ath1_ts  = Number.isFinite(ath1.ts)  ? ath1.ts  : startBar;

  const ath5_usd = Number.isFinite(ath5.usd) ? ath5.usd : startMc;
  const ath5_ts  = Number.isFinite(ath5.ts)  ? ath5.ts  : startBar;

  const ath10_usd= Number.isFinite(ath10.usd)? ath10.usd: startMc;
  const ath10_ts = Number.isFinite(ath10.ts) ? ath10.ts : startBar;

  // глобальный ATH как справка (из repo, не по свечам)
  const athAll_usd = Number(repo.usd_market_cap) || null;
  const athAll_ts  = Number(repo.ath_market_cap_timestamp) || null;

  // мультипликаторы
  const ath1x  = ratioOrNull(ath1_usd,  startMc);
  const ath5x  = ratioOrNull(ath5_usd,  startMc);
  const ath10x = ratioOrNull(ath10_usd, startMc);

  // пишем в БД (creator только если пуст)
  await client.query(
    `
    UPDATE pump_tokens SET
      creator        = COALESCE(creator, $1),
      t0             = to_timestamp($2/1000.0),
      p0_price_usd   = $3,
      start_mc_usd   = $4,

      ath_1m_usd     = $5,
      ath_1m_ts      = to_timestamp($6/1000.0),
      ath_5m_usd     = $7,
      ath_5m_ts      = to_timestamp($8/1000.0),
      ath_10m_usd    = $9,
      ath_10m_ts     = to_timestamp($10/1000.0),

      ath_all_usd    = $11,
      ath_all_ts     = CASE WHEN $12 IS NULL THEN NULL ELSE to_timestamp($12/1000.0) END,

      ath_1m_x       = $13,
      ath_5m_x       = $14,
      ath_10m_x      = $15,

      enrich_status  = 'ok',
      enriched_at    = now(),
      next_enrich_at = NULL,
      last_error     = NULL
    WHERE ca=$16;
    `,
    [
      creator,                // $1
      t0ms,                   // $2
      p0,                     // $3
      startMc,                // $4
      ath1_usd,               // $5
      ath1_ts,                // $6
      ath5_usd,               // $7
      ath5_ts,                // $8
      ath10_usd,              // $9
      ath10_ts,               // $10
      athAll_usd,             // $11
      athAll_ts,              // $12 (ms)
      ath1x,                  // $13
      ath5x,                  // $14
      ath10x,                 // $15
      ca                      // $16
    ]
  );

  // снимок (опционально; храним supply_display, базу и ATH)
  await client.query(
    `
    INSERT INTO token_snapshots
      (ca, snap_ts, price_usd, fdv_usd, usd_market_cap, supply_display,
       dev, p0_price_usd, start_mc_usd, ath_5m_usd, ath_5m_ts, ath_all_usd, ath_all_ts)
    VALUES
      ($1, now(), NULL, NULL, $2, $3,
       $4, $5, $6, $7, to_timestamp($8/1000.0), $9,
       CASE WHEN $10 IS NULL THEN NULL ELSE to_timestamp($10/1000.0) END)
    `,
    [
      ca,
      athAll_usd,            // текущую usd_market_cap берём из repo как справку
      supplyDisplay,
      creator,
      p0, startMc,
      ath5_usd, ath5_ts,
      athAll_usd, athAll_ts
    ]
  );
}

async function loop(){
  console.log(`enricher started | wait=${WAIT_MINUTES}m | pause=${PAUSE_MS}ms`);
  while(true){
    const client = await pool.connect();
    try{
      const {rows} = await client.query(
        `
        SELECT ca
        FROM pump_tokens
        WHERE COALESCE(enrich_status,'new') IN ('new','retry')
          AND inserted_at <= now() - interval '${WAIT_MINUTES} minutes'
          AND (next_enrich_at IS NULL OR next_enrich_at <= now())
        ORDER BY COALESCE(next_enrich_at, inserted_at), inserted_at
        LIMIT 1;
        `
      );

      if (rows.length===0){
        await client.release();
        await sleep(PAUSE_MS);
        continue;
      }

      const ca = rows[0].ca;
      try{
        await processOne(client, {ca});
        console.log("enriched", ca);
      }catch(e){
        // уже проставили retry выше (markRetry), тут просто лог
        if (e.code===429) {
          console.warn("429 rate-limit; backing off");
        } else {
          console.warn("loop error:", e.message || e);
        }
      }finally{
        client.release();
      }

      await sleep(PAUSE_MS);
    }catch(e){
      client.release();
      console.error("fatal loop:", e);
      await sleep(10_000);
    }
  }
}

loop().catch(e=>{ console.error(e); process.exit(1); });
