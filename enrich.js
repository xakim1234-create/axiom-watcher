// enrich.js
// Node 18+ (в Render у вас Node 22) – fetch встроен
import pg from 'pg';

const {
  DATABASE_URL,
  WAIT_MINUTES = '5',      // сколько ждать от first_seen/inserted прежде чем обогащать
  RETRY_MINUTES = '15',    // через сколько минут повторять после ошибки
  PAUSE_MS = '3000',       // пауза между обработками токенов
  IDLE_MS = '15000',       // пауза, когда очередь пуста
} = process.env;

if (!DATABASE_URL) {
  console.error('ENV DATABASE_URL is required');
  process.exit(1);
}

const pool = new pg.Pool({
  connectionString: DATABASE_URL,
  max: 6,
  idleTimeoutMillis: 30_000,
});

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

async function claimOne() {
  // КОРОТКАЯ ТРАНЗАКЦИЯ: только забрать и пометить
  const client = await pool.connect();
  let inTx = false;
  try {
    await client.query('BEGIN');
    inTx = true;

    // выбираем один токен, у которого пришло время
    const sel = await client.query(
      `
      SELECT id, ca
      FROM pump_tokens
      WHERE enrich_status IN ('new')
        AND inserted_at <= now() - ($1::int || ' minutes')::interval
        AND (next_enrich_at IS NULL OR next_enrich_at <= now())
      ORDER BY inserted_at ASC
      FOR UPDATE SKIP LOCKED
      LIMIT 1
      `,
      [Number(WAIT_MINUTES)]
    );

    if (sel.rowCount === 0) {
      await client.query('ROLLBACK');
      inTx = false;
      return null;
    }

    const row = sel.rows[0];

    // помечаем processing
    await client.query(
      `UPDATE pump_tokens
         SET enrich_status = 'processing'
       WHERE id = $1`,
      [row.id]
    );

    await client.query('COMMIT');
    inTx = false;
    return row;
  } catch (err) {
    if (inTx) {
      try { await client.query('ROLLBACK'); } catch {}
      inTx = false;
    }
    throw err;
  } finally {
    client.release();
  }
}

async function fetchRepo(mint) {
  const url = `https://frontend-api-v3.pump.fun/coins/${encodeURIComponent(mint)}`;
  const r = await fetch(url, { headers: { 'accept': 'application/json' } });
  if (!r.ok) {
    const t = await r.text().catch(() => '');
    throw new Error(`repo ${r.status}: ${t.slice(0, 200)}`);
  }
  return r.json();
}

async function saveOk(id, patch) {
  // КОРОТКАЯ ТРАНЗАКЦИЯ записи результата
  const client = await pool.connect();
  let inTx = false;
  try {
    await client.query('BEGIN');
    inTx = true;

    const { creator, decimals, createdTs, repoJson } = patch;

    // обновляем поля + raw->repo
    await client.query(
      `
      UPDATE pump_tokens
         SET enrich_status     = 'ok',
             enriched_at       = now(),
             creator           = COALESCE($1, creator),
             decimals          = COALESCE($2, decimals),
             created_timestamp = COALESCE($3, created_timestamp),
             raw               = COALESCE(raw, '{}'::jsonb) || jsonb_build_object('repo', $4::jsonb)
       WHERE id = $5
      `,
      [
        creator ?? null,
        Number.isFinite(decimals) ? decimals : null,
        createdTs ? new Date(createdTs) : null,
        JSON.stringify(repoJson ?? {}),
        id,
      ]
    );

    await client.query('COMMIT');
    inTx = false;
  } catch (err) {
    if (inTx) {
      try { await client.query('ROLLBACK'); } catch {}
      inTx = false;
    }
    throw err;
  } finally {
    client.release();
  }
}

async function saveErr(id, minutes, errMsg) {
  const client = await pool.connect();
  let inTx = false;
  try {
    await client.query('BEGIN');
    inTx = true;

    await client.query(
      `
      UPDATE pump_tokens
         SET enrich_status = 'err',
             next_enrich_at = now() + ($1::int || ' minutes')::interval
       WHERE id = $2
      `,
      [Number(minutes), id]
    );

    // При желании можно логировать ошибку в отдельную таблицу:
    // await client.query('INSERT INTO enrich_log(token_id, err, created_at) VALUES ($1, $2, now())', [id, String(errMsg).slice(0, 500)]);

    await client.query('COMMIT');
    inTx = false;
  } catch (err) {
    if (inTx) {
      try { await client.query('ROLLBACK'); } catch {}
      inTx = false;
    }
    // проглатываем — это «побочная» запись ошибки
  } finally {
    client.release();
  }
}

async function processOne({ id, ca }) {
  // 1) тащим repo
  const repo = await fetchRepo(ca);

  // 2) выделяем нужные поля
  const createdTs = typeof repo?.created_timestamp === 'number' ? repo.created_timestamp : null; // ms
  const decimals  = Number.isFinite(repo?.decimals) ? Number(repo.decimals) : 6;
  const creator   = repo?.creator ?? null;

  // 3) сохраняем успех
  await saveOk(id, {
    creator,
    decimals,
    createdTs,
    repoJson: repo,
  });
}

async function loop() {
  console.log(
    `enricher started | wait=${WAIT_MINUTES}m | pause=${PAUSE_MS}ms`
  );

  // бесконечный цикл
  while (true) {
    let job = null;
    try {
      job = await claimOne();
    } catch (e) {
      console.error('claim error:', e?.message || e);
      // если проблемы с БД — немного подождать
      await sleep(5000);
      continue;
    }

    if (!job) {
      // очередь пуста
      await sleep(Number(IDLE_MS));
      continue;
    }

    try {
      await processOne(job);
    } catch (e) {
      console.error(`process error for ${job.ca}:`, e?.message || e);
      await saveErr(job.id, RETRY_MINUTES, e?.message || String(e));
    }

    // троттлинг между задачами
    await sleep(Number(PAUSE_MS));
  }
}

loop().catch(err => {
  console.error('fatal:', err);
  process.exit(1);
});
