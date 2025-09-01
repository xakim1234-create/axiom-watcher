import 'dotenv/config';
import { Pool } from 'pg';

// ==== ENV ====
const DB_URL = process.env.DATABASE_URL;
if (!DB_URL) {
  console.error('DATABASE_URL is not set');
  process.exit(1);
}

const ENRICH_MINUTES = +(process.env.ENRICH_MINUTES ?? 5);
const ENRICH_INTERVAL_MS = +(process.env.ENRICH_INTERVAL_MS ?? 3000);
const ENRICH_COOLDOWN_429_MS = +(process.env.ENRICH_COOLDOWN_429_MS ?? 30000);
const ENRICH_COOLDOWN_MAX_MS = +(process.env.ENRICH_COOLDOWN_MAX_MS ?? 300000);
const MAX_PAGES_CANDLES = +(process.env.MAX_PAGES_CANDLES ?? 200);
const TRADES_LIMIT = +(process.env.TRADES_LIMIT ?? 100);

const pool = new Pool({ connectionString: DB_URL });
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ---------- pump.fun helpers ----------
async function getJSON(url, init) {
  const r = await fetch(url, init);
  if (!r.ok) {
    const t = await r.text().catch(() => '');
    const err = new Error(`HTTP ${r.status} ${r.statusText} @ ${url} ${t}`);
    err.status = r.status;
    throw err;
  }
  return r.json();
}

async function getRepo(mint) {
  return getJSON(`https://frontend-api-v3.pump.fun/coins/${mint}`);
}
async function getCandlesPage(mint, createdTs, beforeTs=null) {
  const u = new URL(`https://swap-api.pump.fun/v2/coins/${mint}/candles`);
  u.searchParams.set('interval','1m');
  u.searchParams.set('limit','1000');
  u.searchParams.set('currency','USD');
  u.searchParams.set('createdTs', String(createdTs));
  if (beforeTs) u.searchParams.set('beforeTs', String(beforeTs));
  return getJSON(u);
}
async function tradesPage(mint, cursor=null, limit=TRADES_LIMIT) {
  const u = new URL(`https://swap-api.pump.fun/v1/coins/${mint}/trades`);
  u.searchParams.set('limit', String(Math.min(limit, TRADES_LIMIT)));
  if (cursor) u.searchParams.set('cursor', cursor);
  return getJSON(u);
}
async function devBatch(mint, creator, afterTs, beforeTs, limit=TRADES_LIMIT) {
  return getJSON(
    `https://swap-api.pump.fun/v1/coins/${mint}/trades/batch`,
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        userAddresses:[creator],
        limit: Math.min(limit, TRADES_LIMIT),
        afterTs, beforeTs
      })
    }
  );
}
const ceilToMinute = (ts) => Math.ceil(ts / 60000) * 60000;

// ---------- вычисление снэпшота ----------
async function computeSnapshot(mint) {
  const repo = await getRepo(mint);

  const createdTs = +repo.created_timestamp;
  const creator   = repo.creator ?? null;
  const decimals  = Number.isFinite(repo.decimals) ? repo.decimals : 6;
  const supplyDisplay = Number(repo.total_supply) / 10**decimals;

  let t0 = createdTs;
  let p0 = null;
  if (creator) {
    try {
      const rows = await devBatch(mint, creator, createdTs - 30_000, createdTs + 5*60_000);
      const list = Array.isArray(rows?.[creator]) ? rows[creator] : [];
      const devBuys = list
        .filter(t => t.type === 'buy')
        .map(t => ({ ts: Date.parse(t.timestamp), price: Number(t.priceUSD) }))
        .filter(o => Number.isFinite(o.ts) && Number.isFinite(o.price))
        .sort((a,b) => a.ts - b.ts);
      if (devBuys.length) { t0 = devBuys[0].ts; p0 = devBuys[0].price; }
    } catch(e) { console.warn(`[${mint}] trades/batch failed:`, e.message || e); }
  }

  const minStart = ceilToMinute(t0);
  const fiveEnd  = minStart + 5*60*1000;

  let pages = 0, all = [], beforeTs = null;
  while (pages < MAX_PAGES_CANDLES) {
    pages++;
    const batch = await getCandlesPage(mint, createdTs, beforeTs);
    if (!Array.isArray(batch) || batch.length === 0) break;
    batch.sort((a,b) => a.timestamp - b.timestamp);
    for (const c of batch) {
      c.open  = Number(c.open);
      c.high  = Number(c.high);
      c.low   = Number(c.low);
      c.close = Number(c.close);
    }
    all.push(...batch);
    const earliest = batch[0].timestamp;
    if (earliest <= createdTs) break;
    beforeTs = earliest - 1;
  }

  const map = new Map();
  for (const c of all) map.set(c.timestamp, c);
  const candles = Array.from(map.values()).sort((a,b) => a.timestamp - b.timestamp);

  let ath5=-Infinity, ath5Ts=null, athAll=-Infinity, athAllTs=null;
  for (const c of candles) {
    const price = Number(c.close ?? c.open);
    if (!Number.isFinite(price)) continue;
    const mc = price * supplyDisplay;
    if (c.timestamp >= minStart && c.timestamp <= fiveEnd) {
      if (mc > ath5) { ath5=mc; ath5Ts=c.timestamp; }
    }
    if (mc > athAll) { athAll=mc; athAllTs=c.timestamp; }
  }
  const lastPrice = Number(candles.at(-1)?.close) || 0;
  const currentMc = Number.isFinite(repo.usd_market_cap) ? repo.usd_market_cap : lastPrice * supplyDisplay;

  async function countTradesBetween(startTs, endTs) {
    let buys=0, sells=0, cursor=null;
    while (true) {
      const { trades, pagination } = await tradesPage(mint, cursor, TRADES_LIMIT);
      if (!trades?.length) break;
      let below=false;
      for (const t of trades) {
        const ts = Date.parse(t.timestamp);
        if (!Number.isFinite(ts)) continue;
        if (ts < startTs) { below=true; break; }
        if (ts <= endTs && ts >= startTs) {
          if (t.type==='buy') buys++; else if (t.type==='sell') sells++;
        }
      }
      if (below || !pagination?.hasMore) break;
      cursor = pagination.nextCursor;
    }
    return { buys, sells, total: buys + sells };
  }

  const win1 =  await countTradesBetween(t0, t0 + 60_000);
  const win5 =  await countTradesBetween(t0, t0 + 5*60_000);
  const winAll = await countTradesBetween(t0, Date.now());

  return {
    repo, createdTs, creator, decimals, supplyDisplay,
    t0, p0, minStart,
    ath5: Number.isFinite(ath5) ? ath5 : null, ath5Ts,
    athAll: Number.isFinite(athAll) ? athAll : null, athAllTs,
    currentMc,
    trades: { win1, win5, winAll }
  };
}

// ---------- DB work ----------
async function pickOneClient(client) {
  const q = `
    select ca
    from pump_tokens
    where coalesce(enrich_status,'new') in ('new','err')
      and coalesce(next_enrich_at, now()) <= now()
      and coalesce(inserted_at, now() - interval '10 minutes') <= now() - ($1 || ' minutes')::interval
    order by next_enrich_at nulls first, inserted_at
    limit 1
    for update skip locked
  `;
  const r = await client.query(q, [ENRICH_MINUTES]);
  return r.rows[0]?.ca || null;
}

async function setStatus(client, ca, status, extra={}) {
  const cols=['enrich_status'], vals=[status];
  if (extra.enriched_at)        { cols.push('enriched_at');       vals.push(extra.enriched_at); }
  if ('last_enrich_error' in extra){ cols.push('last_enrich_error'); vals.push(extra.last_enrich_error); }
  if ('next_enrich_at' in extra)   { cols.push('next_enrich_at');    vals.push(extra.next_enrich_at); }
  const sets = cols.map((c,i)=>`${c} = $${i+2}`).join(', ');
  await client.query(`update pump_tokens set ${sets} where ca=$1`, [ca, ...vals]);
}

async function saveSnapshot(client, ca, s) {
  const q = `
    insert into token_snapshots (
      ca, snap_ts,
      price_usd, fdv_usd, usd_market_cap, supply_display,
      dev, p0_price_usd, start_mc_usd,
      ath_5m_usd, ath_5m_ts, ath_all_usd, ath_all_ts,
      max_txs_per_min,
      buys_0_1, sells_0_1, total_0_1,
      buys_0_5, sells_0_5, total_0_5,
      buys_all, sells_all, total_all,
      raw
    ) values (
      $1, now(),
      null, null, $2, $3,
      $4, $5, $6,
      $7, to_timestamp($8/1000.0), $9, to_timestamp($10/1000.0),
      null,
      $11, $12, $13,
      $14, $15, $16,
      $17, $18, $19,
      $20
    )
    returning id
  `;
  const params = [
    ca,
    s.currentMc,
    s.supplyDisplay,
    s.creator,
    s.p0,
    s.p0 != null ? s.p0 * s.supplyDisplay : null,
    s.ath5,
    s.ath5Ts,
    s.athAll,
    s.athAllTs,
    s.trades.win1.buys, s.trades.win1.sells, s.trades.win1.total,
    s.trades.win5.buys, s.trades.win5.sells, s.trades.win5.total,
    s.trades.winAll.buys, s.trades.winAll.sells, s.trades.winAll.total,
    s.repo
  ];
  const r = await client.query(q, params);
  return r.rows[0]?.id;
}

async function updateTokenMeta(client, ca, s) {
  const q = `
    update pump_tokens
    set creator = coalesce(creator, $2),
        created_timestamp = coalesce(created_timestamp, to_timestamp($3/1000.0)),
        decimals = coalesce(decimals, $4)
    where ca = $1
  `;
  await client.query(q, [ca, s.creator, s.createdTs, s.decimals]);
}

let global429Cooldown = 0;

async function processOne(ca) {
  const client = await pool.connect();
  try {
    await client.query('begin');
    const row = await client.query(`select enrich_status from pump_tokens where ca=$1 for update`, [ca]);
    if (row.rowCount === 0) { await client.query('rollback'); return; }
    await setStatus(client, ca, 'busy');
    await client.query('commit');

    const snap = await computeSnapshot(ca);

    await client.query('begin');
    await updateTokenMeta(client, ca, snap);
    const id = await saveSnapshot(client, ca, snap);
    await setStatus(client, ca, 'ok', { enriched_at: new Date(), last_enrich_error: null, next_enrich_at: null });
    await client.query('commit');

    console.log(`[ok] ${ca} -> snapshot ${id}`);
    global429Cooldown = 0;
  } catch (e) {
    await client.query('rollback').catch(()=>{});
    const msg = String(e?.message || e);

    if (e.status === 429 || /HTTP 429/.test(msg)) {
      global429Cooldown = Math.min(global429Cooldown ? global429Cooldown * 2 : ENRICH_COOLDOWN_429_MS, ENRICH_COOLDOWN_MAX_MS);
      const retryAt = new Date(Date.now() + global429Cooldown);
      await setStatus(client, ca, 'err', { last_enrich_error: '429 rate limit', next_enrich_at: retryAt });
      console.warn(`[429] ${ca} cooldown=${global429Cooldown}ms`);
      return;
    }

    const retryAt = new Date(Date.now() + 60_000);
    await setStatus(client, ca, 'err', { last_enrich_error: msg.slice(0,400), next_enrich_at: retryAt });
    console.warn(`[err] ${ca} ${msg}`);
  } finally {
    client.release();
  }
}

async function loop() {
  console.log(`enricher started | wait=${ENRICH_MINUTES}m | pause=${ENRICH_INTERVAL_MS}ms`);
  while (true) {
    const client = await pool.connect();
    try {
      await client.query('begin');
      const ca = await pickOneClient(client);
      await client.query('commit');
      client.release();

      if (!ca) { await sleep(2000); continue; }

      await processOne(ca);

      if (global429Cooldown) await sleep(global429Cooldown);
      else await sleep(ENRICH_INTERVAL_MS);
    } catch (e) {
      client.release();
      console.error('loop error:', e?.message || e);
      await sleep(3000);
    }
  }
}

loop().catch(e => { console.error('fatal', e); process.exit(1); });
