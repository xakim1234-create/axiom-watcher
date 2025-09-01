// enrich.js
// Node 18+/22+ (ESM). Требуются пакеты: pg, dotenv (опционально).
// Таблицы: pump_tokens (с колонками enrich_status, enriched_at, decimals, created_timestamp, ...)
//          token_snapshots (ca, snap_ts, price_usd, fdv_usd, usd_market_cap, supply_display, dev, p0_price_usd, start_mc_usd, ath_5m_usd)

import 'dotenv/config';
import { Pool } from 'pg';

// ---------- Конфиг ----------

// БД
const DATABASE_URL =
  process.env.DATABASE_URL ||
  process.env.PGURI ||
  process.env.PG_URL ||
  process.env.POSTGRES_URL;

// Сколько токенов за один «проход» (не параллель, а последовательно)
const BATCH_SIZE = Number(process.env.BATCH_SIZE ?? 1);

// Пауза между токенами (мс) + случайный джиттер до 1000мс
const PAUSE_MS = Number(process.env.PAUSE_MS ?? 8000);

// Сколько минут «выдерживать» токен перед обогащением
const WAIT_MINUTES = Number(process.env.WAIT_MINUTES ?? 5);

// Свечи: только ОДНА страница по 1m, чтобы не ловить rate-limit
const CANDLES_INTERVAL = '1m';
const CANDLES_LIMIT = 120;         // 2 часа по минуте — более чем достаточно
const MAX_CANDLE_PAGES = 1;        // НЕ трогаем

// Сколько попыток при 429/403 и какая базовая задержка (сек)
const RL_MAX_ATTEMPTS = 6;
const RL_BASE_DELAY_S = 5;

// ---------- Клиент БД ----------

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl:
    process.env.PGSSL === 'disable'
      ? false
      : { rejectUnauthorized: false }, // для Neon/Render
});

// Утилита-сон
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Общий «User-Agent/Accept», чтобы Cloudflare относился лояльнее
const DEFAULT_HEADERS = {
  'user-agent':
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/124.0 Safari/537.36 pfan-watcher/1.0',
  accept: 'application/json, */*;q=0.1',
};

// Универсальный fetch с экспоненциальным бэк-оффом
async function fetchJSON(url, opts = {}, attempt = 1) {
  const res = await fetch(url, {
    ...opts,
    headers: { ...(opts.headers ?? {}), ...DEFAULT_HEADERS },
    redirect: 'follow',
  });

  if (res.status === 429 || res.status === 403) {
    const retryAfter =
      Number(res.headers.get('retry-after')) ||
      Math.min(120, Math.pow(2, attempt - 1) * RL_BASE_DELAY_S);

    console.warn(
      `[429/403] ${url} -> wait ${retryAfter}s (attempt ${attempt}/${RL_MAX_ATTEMPTS})`
    );
    await sleep(retryAfter * 1000);

    if (attempt < RL_MAX_ATTEMPTS) {
      return fetchJSON(url, opts, attempt + 1);
    }
    throw new Error(`Too many 429/403 for ${url}`);
  }

  if (!res.ok) {
    const text = (await res.text()).slice(0, 300);
    throw new Error(`${url} -> ${res.status} ${text}`);
  }

  // Пытаемся распарсить JSON (иногда Cloudflare отдаёт HTML — мы это словили выше)
  const ct = res.headers.get('content-type') || '';
  if (!ct.includes('application/json')) {
    const txt = (await res.text()).slice(0, 400);
    throw new Error(`${url} -> non-JSON response: ${txt}`);
  }

  return res.json();
}

// Округление вверх до минуты
const ceilToMinute = (ts) => Math.ceil(ts / 60000) * 60000;

// ---------- API Pump.fun helpers ----------

async function getRepo(mint) {
  const url = `https://frontend-api-v3.pump.fun/coins/${mint}`;
  return fetchJSON(url);
}

async function getCandlesOnce(mint, createdTs) {
  const u = new URL(`https://swap-api.pump.fun/v2/coins/${mint}/candles`);
  u.searchParams.set('interval', CANDLES_INTERVAL);
  u.searchParams.set('limit', String(CANDLES_LIMIT));
  u.searchParams.set('currency', 'USD');
  u.searchParams.set('createdTs', String(createdTs));
  // Без пагинации, строго 1 запрос, чтобы не ловить Cloudflare
  return fetchJSON(u.toString());
}

async function devBatch(mint, creator, afterTs, beforeTs, limit = 100) {
  const url = `https://swap-api.pump.fun/v1/coins/${mint}/trades/batch`;
  const body = {
    userAddresses: [creator],
    limit: Math.min(limit, 100),
    afterTs,
    beforeTs,
  };

  return fetchJSON(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
}

// ---------- Бизнес-логика расчёта метрик ----------

function safeNum(n) {
  const v = Number(n);
  return Number.isFinite(v) ? v : null;
}

// Возвращает снапшот: { price_usd, usd_mc, fdv_usd, supply_display, dev, p0_price_usd, start_mc_usd, ath_5m_usd }
async function buildSnapshot(mint) {
  const repo = await getRepo(mint);

  const createdTs = Number(repo.created_timestamp ?? 0);
  const creator = repo.creator ?? null;
  const decimals = Number.isFinite(repo.decimals) ? repo.decimals : 6;
  const supplyDisplay = safeNum(repo.total_supply) / Math.pow(10, decimals);

  // Попытка найти цену dev-buy (p0)
  let t0 = createdTs;
  let p0_price_usd = null;

  if (creator && createdTs) {
    try {
      const rows = await devBatch(
        mint,
        creator,
        createdTs - 30_000,
        createdTs + 5 * 60_000,
        100
      );
      const list = Array.isArray(rows?.[creator]) ? rows[creator] : [];
      const devBuys = list
        .filter((t) => t?.type === 'buy')
        .map((t) => ({
          ts: Date.parse(t.timestamp),
          price: safeNum(t.priceUSD),
        }))
        .filter((x) => x.ts && x.price)
        .sort((a, b) => a.ts - b.ts);

      if (devBuys.length) {
        t0 = devBuys[0].ts;
        p0_price_usd = devBuys[0].price;
      }
    } catch (e) {
      console.warn(`devBatch fallback for ${mint}: ${e.message || e}`);
    }
  }

  // Свечи (1 запрос)
  let lastClose = null;
  let ath_5m_usd = null;
  let start_mc_usd = null;

  const minStart = ceilToMinute(t0 || createdTs);
  const fiveEnd = minStart + 5 * 60 * 1000;

  try {
    const candles = await getCandlesOnce(mint, createdTs);
    // Приводим к числам и сортируем
    const sorted = (Array.isArray(candles) ? candles : [])
      .map((c) => ({
        ts: Number(c.timestamp),
        open: safeNum(c.open),
        high: safeNum(c.high),
        low: safeNum(c.low),
        close: safeNum(c.close),
      }))
      .filter((c) => c.ts && (c.close || c.open))
      .sort((a, b) => a.ts - b.ts);

    if (sorted.length) {
      lastClose = sorted.at(-1).close ?? sorted.at(-1).open ?? null;

      if (supplyDisplay && (p0_price_usd || lastClose)) {
        if (p0_price_usd) start_mc_usd = p0_price_usd * supplyDisplay;

        // ATH в окне первых 5 минут
        let maxMC = -Infinity;
        for (const c of sorted) {
          if (c.ts < minStart) continue;
          if (c.ts > fiveEnd) break;
          const price = c.close ?? c.open ?? null;
          if (!price) continue;
          const mc = price * supplyDisplay;
          if (mc > maxMC) maxMC = mc;
        }
        if (Number.isFinite(maxMC)) ath_5m_usd = maxMC;
      }
    }
  } catch (e) {
    console.warn(`candles error for ${mint}: ${e.message || e}`);
  }

  // Текущий MC и цена
  // repo.usd_market_cap часто есть — используем её приоритетно
  let usd_market_cap =
    (typeof repo.usd_market_cap === 'number' && isFinite(repo.usd_market_cap))
      ? repo.usd_market_cap
      : null;

  let price_usd = null;
  if (usd_market_cap && supplyDisplay) {
    price_usd = usd_market_cap / supplyDisplay;
  } else if (lastClose && supplyDisplay) {
    usd_market_cap = lastClose * supplyDisplay;
    price_usd = lastClose;
  } else if (lastClose) {
    price_usd = lastClose;
  }

  // FDV = price * total_supply_display (если она равна circulating — = MC)
  const fdv_usd = price_usd && supplyDisplay ? price_usd * supplyDisplay : null;

  return {
    price_usd: safeNum(price_usd),
    usd_mc: safeNum(usd_market_cap),
    fdv_usd: safeNum(fdv_usd),
    supply_display: safeNum(supplyDisplay),
    dev: creator || null,
    p0_price_usd: safeNum(p0_price_usd),
    start_mc_usd: safeNum(start_mc_usd),
    ath_5m_usd: safeNum(ath_5m_usd),
    created_ts: createdTs ? new Date(createdTs) : null,
    decimals,
  };
}

// ---------- Работа с БД: выбор и отметки ----------

async function pickBatch() {
  // Берём токены со статусом 'new', старше WAIT_MINUTES, и ещё не снапшотнутые
  const q = `
    SELECT t.ca
    FROM pump_tokens t
    LEFT JOIN token_snapshots s USING (ca)
    WHERE t.enrich_status = 'new'
      AND t.inserted_at <= now() - INTERVAL '${WAIT_MINUTES} minutes'
      AND s.ca IS NULL
    ORDER BY t.inserted_at
    LIMIT $1
  `;
  const { rows } = await pool.query(q, [BATCH_SIZE]);
  return rows.map((r) => r.ca);
}

async function markOK(ca, created_ts, decimals) {
  const q = `
    UPDATE pump_tokens
    SET enrich_status = 'ok',
        enriched_at   = now(),
        created_timestamp = COALESCE(created_timestamp, $2),
        decimals = COALESCE(decimals, $3)
    WHERE ca = $1
  `;
  await pool.query(q, [ca, created_ts, decimals]);
}

async function markERR(ca, minutes = 30) {
  const q = `
    UPDATE pump_tokens
    SET enrich_status = 'err',
        enriched_at   = now(),
        next_enrich_at = now() + INTERVAL '${minutes} minutes'
    WHERE ca = $1
  `;
  await pool.query(q, [ca]);
}

async function insertSnapshot(ca, snap) {
  const q = `
    INSERT INTO token_snapshots
      (ca, snap_ts, price_usd, fdv_usd, usd_market_cap, supply_display,
       dev, p0_price_usd, start_mc_usd, ath_5m_usd)
    VALUES
      ($1, now(), $2, $3, $4, $5, $6, $7, $8, $9)
  `;
  await pool.query(q, [
    ca,
    snap.price_usd,
    snap.fdv_usd,
    snap.usd_mc,
    snap.supply_display,
    snap.dev,
    snap.p0_price_usd,
    snap.start_mc_usd,
    snap.ath_5m_usd,
  ]);
}

// ---------- Основной цикл ----------

async function processOne(ca) {
  try {
    const snap = await buildSnapshot(ca);

    await insertSnapshot(ca, snap);
    await markOK(ca, snap.created_ts, snap.decimals);

    console.log(`enriched ${ca}`);
  } catch (e) {
    console.warn(`enrich failed ${ca}: ${e.message || e}`);
    // Если это перманентный фейл после бэк-оффов — переносим на потом
    await markERR(ca, 60);
  }
}

async function loop() {
  console.log(
    `enricher started | wait=${WAIT_MINUTES}m | pause=${PAUSE_MS}ms`
  );

  // Бесконечный цикл: берём порцию токенов, обрабатываем их последовательно
  for (;;) {
    try {
      const batch = await pickBatch();
      if (!batch.length) {
        // Ничего нет — подождать немного и снова
        await sleep(10_000);
        continue;
      }

      for (const ca of batch) {
        await processOne(ca);

        // Пауза с небольшим джиттером
        const jitter = Math.floor(Math.random() * 1000);
        await sleep(PAUSE_MS + jitter);
      }
    } catch (e) {
      console.error('loop error:', e.message || e);
      // Маленькая пауза, чтобы не крутить цикл как вентилятор
      await sleep(5_000);
    }
  }
}

loop().catch((e) => {
  console.error('fatal', e);
  process.exit(1);
});
