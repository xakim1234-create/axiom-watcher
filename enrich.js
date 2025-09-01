// enrich.js
import pg from "pg";
const { Pool } = pg;

// --- ENV ---
const DB_URL = process.env.DATABASE_URL;
if (!DB_URL) {
  console.error("DATABASE_URL is not set");
  process.exit(1);
}
const pool = new Pool({ connectionString: DB_URL, max: 1 });

// --- Тюнинг цикла ---
const PAUSE_MS = 3000;         // пауза между задачами
const WAIT_BEFORE_SNAPSHOT = 5 * 60 * 1000; // 5 минут

// --- Утилиты времени/форматов ---
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ceilToMinute = (tsMs) => Math.ceil(tsMs / 60000) * 60000;

// --- API helpers ---
async function getJSON(url, body) {
  const opts = body
    ? { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) }
    : undefined;
  const r = await fetch(url, opts);
  if (!r.ok) throw new Error(`${url} → ${r.status}`);
  return r.json();
}

async function getRepo(mint) {
  return getJSON(`https://frontend-api-v3.pump.fun/coins/${mint}`);
}

async function getCandles(mint, createdTs, beforeTs = null) {
  // берём побольше, заодно дотягиваемся назад до старта
  const u = new URL(`https://swap-api.pump.fun/v2/coins/${mint}/candles`);
  u.searchParams.set("interval", "1m");
  u.searchParams.set("limit", "1000");
  u.searchParams.set("currency", "USD");
  u.searchParams.set("createdTs", String(createdTs));
  if (beforeTs) u.searchParams.set("beforeTs", String(beforeTs));
  return getJSON(u.toString());
}

async function tradesBatch(mint, creator, afterTs, beforeTs) {
  const url = `https://swap-api.pump.fun/v1/coins/${mint}/trades/batch`;
  const body = {
    userAddresses: [creator],
    limit: 100,
    afterTs,
    beforeTs,
  };
  const j = await getJSON(url, body);
  return Array.isArray(j?.[creator]) ? j[creator] : [];
}

// --- БД helpers ---
async function fetchOneNewToken(client) {
  // 1) взять токен, который ещё не "обогащён"
  const q = `
    select *
    from pump_tokens
    where enrich_status in ('new','retry')
    order by inserted_at
    limit 1
    for update skip locked
  `;
  const { rows } = await client.query(q);
  return rows[0] || null;
}

async function markEnrichOK(client, ca, payload) {
  const {
    created_timestamp, decimals, creator, name, symbol,
  } = payload;

  await client.query(
    `
    update pump_tokens
    set
      created_timestamp = $2,
      decimals = $3,
      creator = $4,
      name = coalesce($5, name),
      symbol = coalesce($6, symbol),
      enrich_status = 'ok',
      enriched_at = now()
    where ca = $1
    `,
    [ca, created_timestamp ? new Date(created_timestamp) : null, decimals ?? null, creator ?? null, name ?? null, symbol ?? null]
  );
}

async function markEnrichErr(client, ca, msg) {
  await client.query(
    `update pump_tokens set enrich_status = 'err', next_enrich_at = now() + interval '10 minutes' where ca = $1`,
    [ca]
  );
  console.warn("enrich error:", ca, msg);
}

async function fetchOneReadyForSnapshot(client) {
  // 2) взять токен, у которого уже OK, прошло >=5 мин с created_timestamp,
  //    и в token_snapshots ещё нет записи
  const q = `
    select p.*
    from pump_tokens p
    where p.enrich_status = 'ok'
      and p.created_timestamp is not null
      and p.created_timestamp <= now() - interval '5 minutes'
      and not exists (select 1 from token_snapshots s where s.ca = p.ca)
    order by p.created_timestamp
    limit 1
    for update skip locked
  `;
  const { rows } = await client.query(q);
  return rows[0] || null;
}

async function saveSnapshot(client, row, snapshot) {
  // простая idempotent вставка (уникальный индекс по ca)
  await client.query(
    `
    insert into token_snapshots
      (ca, snap_ts, price_usd, fdv_usd, usd_market_cap, supply_display,
       dev, p0_price_usd, start_mc_usd, ath_5m_usd)
    values
      ($1, $2, $3, $4, $5, $6,
       $7, $8, $9, $10)
    on conflict (ca) do nothing
    `,
    [
      row.ca,
      new Date(snapshot.snap_ts),
      snapshot.price_usd,
      snapshot.fdv_usd,
      snapshot.usd_market_cap,
      snapshot.supply_display,
      snapshot.dev,
      snapshot.p0_price_usd,
      snapshot.start_mc_usd,
      snapshot.ath_5m_usd,
    ]
  );
}

// --- Бизнес-логика ---
async function enrichOne(client, row) {
  // row.ca = mint с суффиксом pump (mint…pump)
  const mint = row.ca;
  const repo = await getRepo(mint);

  const decimals = repo.decimals ?? 6;
  const supplyDisplay = repo.total_supply ? repo.total_supply / 10 ** decimals : null;

  await markEnrichOK(client, mint, {
    created_timestamp: repo.created_timestamp, // ms
    decimals,
    creator: repo.creator,
    name: repo.name,
    symbol: repo.symbol,
  });

  console.log("enriched", mint);
}

async function snapshotOne(client, row) {
  const mint = row.ca;
  const repo = await getRepo(mint);

  const createdTs = +repo.created_timestamp;           // ms
  const minStart = ceilToMinute(createdTs);
  const fiveEnd  = minStart + 5 * 60 * 1000;

  const decimals = repo.decimals ?? 6;
  const supplyDisplay = repo.total_supply ? repo.total_supply / 10 ** decimals : null;

  // Dev-buy (p0) через batch
  let p0 = null, t0 = createdTs;
  try {
    const devRows = await tradesBatch(mint, repo.creator, createdTs - 30_000, createdTs + 5 * 60_000);
    const devBuys = devRows
      .filter(t => t.type === "buy")
      .map(t => ({ ts: Date.parse(t.timestamp), price: Number(t.priceUSD) }))
      .filter(t => Number.isFinite(t.ts) && Number.isFinite(t.price))
      .sort((a,b) => a.ts - b.ts);
    if (devBuys.length) {
      p0 = devBuys[0].price;
      t0 = devBuys[0].ts;
    }
  } catch (e) {
    console.warn("trades/batch", mint, e.message || e);
  }

  // Свечи → ATH за 0–5 минут
  let ath5 = null;
  try {
    const batch = await getCandles(mint, createdTs);
    const map = new Map();
    for (const c of batch || []) {
      if (!c) continue;
      map.set(c.timestamp, {
        ...c,
        open:  Number(c.open),
        high:  Number(c.high),
        low:   Number(c.low),
        close: Number(c.close),
      });
    }
    const candles = Array.from(map.values()).sort((a,b) => a.timestamp - b.timestamp);

    for (const c of candles) {
      const ts = c.timestamp;
      if (ts < minStart || ts > fiveEnd) continue;
      const price = Number.isFinite(c.close) ? c.close : c.open;
      if (!Number.isFinite(price) || !Number.isFinite(supplyDisplay)) continue;
      const mc = price * supplyDisplay;
      if (!Number.isFinite(ath5) || mc > ath5) ath5 = mc;
    }
  } catch (e) {
    console.warn("candles", mint, e.message || e);
  }

  const snap = {
    snap_ts: fiveEnd,                                    // точка "t+5m"
    price_usd: repo.usd_price ?? null,                   // если есть в repo
    fdv_usd: (repo.usd_price && supplyDisplay) ? repo.usd_price * supplyDisplay : null,
    usd_market_cap: repo.usd_market_cap ?? null,
    supply_display: supplyDisplay ?? null,
    dev: repo.creator ?? null,
    p0_price_usd: p0 ?? null,
    start_mc_usd: (p0 && supplyDisplay) ? p0 * supplyDisplay : null,
    ath_5m_usd: Number.isFinite(ath5) ? ath5 : null,
  };

  await saveSnapshot(client, row, snap);
  console.log("snapshot saved", mint, "| ath5:", snap.ath_5m_usd ?? "null");
}

// --- Основной цикл ---
async function loop() {
  console.log(`enricher started | wait=5m | pause=${PAUSE_MS}ms`);

  while (true) {
    const client = await pool.connect();
    try {
      await client.query("begin");

      // 1) обогатить свежий токен
      let row = await fetchOneNewToken(client);
      if (row) {
        try {
          await enrichOne(client, row);
          await client.query("commit");
        } catch (e) {
          await markEnrichErr(client, row.ca, e.message || String(e));
          await client.query("commit");
        } finally {
          client.release();
        }
        await sleep(PAUSE_MS);
        continue;
      }

      // 2) сделать снапшот t+5m для токена без снапшота
      await client.query("begin");
      row = await fetchOneReadyForSnapshot(client);
      if (row) {
        try {
          await snapshotOne(client, row);
          await client.query("commit");
        } catch (e) {
          console.warn("snapshot error:", row.ca, e.message || e);
          await client.query("rollback");
        } finally {
          client.release();
        }
        await sleep(PAUSE_MS);
        continue;
      }

      // 3) ничего не нашли — просто подождать
      await client.query("commit");
      client.release();
      await sleep(PAUSE_MS);
    } catch (e) {
      try { await client.query("rollback"); } catch {}
      client.release();
      console.warn("loop error:", e.message || e);
      await sleep(PAUSE_MS);
    }
  }
}

// run
loop().catch(err => {
  console.error("fatal", err);
  process.exit(1);
});
