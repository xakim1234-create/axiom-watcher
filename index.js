// index.js — v2 с резервным WS и health-checks
import 'dotenv/config';
import WebSocket from 'ws';
import { Pool } from 'pg';

const DB = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

async function initDb() {
  await DB.query(`
    create table if not exists pump_tokens (
      id bigserial primary key,
      ca text unique,
      name text,
      symbol text,
      image_url text,
      creator text,
      first_seen_ts timestamptz,
      raw jsonb,
      inserted_at timestamptz default now()
    );
    create unique index if not exists pump_tokens_ca_uidx on pump_tokens(ca);
    create index if not exists pump_tokens_inserted_at_idx on pump_tokens(inserted_at desc);
  `);
  console.log('DB ready');
}

// ——— извлечение CA из payload ———
function extractCA(obj) {
  if (!obj) return null;

  // Явные поля
  for (const k of ['ca','contract','mint','mintAddress','tokenAddress','address','publicKey']) {
    const v = obj?.[k];
    if (typeof v === 'string' && v.endsWith('pump')) return v;
  }

  // Из ссылки coin-image/<CA>/
  const collect = [];
  for (const k of ['image','image_url','imageUri','imageURI','img','thumb','logo','pic','icon']) {
    if (typeof obj?.[k] === 'string') collect.push(obj[k]);
  }
  if (typeof obj?.metadata?.image === 'string') collect.push(obj.metadata.image);
  for (const url of collect) {
    const m = /\/coin-image\/([^/]+)\//.exec(url);
    if (m) return m[1];
  }

  // Фолбэк: по сырому тексту
  const s = JSON.stringify(obj);
  let m = /\/coin-image\/([^/]+)\//.exec(s);
  if (m) return m[1];
  m = /\b[1-9A-HJ-NP-Za-km-z]{25,}pump\b/.exec(s);
  if (m) return m[0];

  return null;
}

async function saveToken(ca, payload) {
  const name = payload?.name ?? payload?.tokenName ?? null;
  const symbol = payload?.symbol ?? payload?.ticker ?? null;
  const image_url = payload?.image ?? payload?.image_url ?? null;
  const creator = payload?.creator ?? payload?.creatorAddress ?? payload?.owner ?? null;
  const ts = payload?.createdAt ?? payload?.timestamp ?? null;

  await DB.query(
    `insert into pump_tokens (ca,name,symbol,image_url,creator,first_seen_ts,raw)
     values ($1,$2,$3,$4,$5,$6,$7)
     on conflict (ca) do update set
       name = coalesce(EXCLUDED.name, pump_tokens.name),
       symbol = coalesce(EXCLUDED.symbol, pump_tokens.symbol),
       image_url = coalesce(EXCLUDED.image_url, pump_tokens.image_url),
       creator = coalesce(EXCLUDED.creator, pump_tokens.creator),
       first_seen_ts = coalesce(EXCLUDED.first_seen_ts, pump_tokens.first_seen_ts),
       raw = coalesce(EXCLUDED.raw, pump_tokens.raw)`,
    [ca, name, symbol, image_url, creator, ts ? new Date(ts) : null, payload]
  );
  stats.saved++;
  stats.lastSavedAt = Date.now();
  console.log('saved', ca, name || '', symbol || '');
}

// ——— общие штуки для WS ———
const stats = { saved: 0, reconnects: 0, lastSavedAt: 0 };
setInterval(() => {
  const idleSec = ((Date.now() - (stats.lastSavedAt || Date.now()))/1000)|0;
  console.log(`[stats] saved/min≈${stats.saved}  reconnects=${stats.reconnects}  idle=${idleSec}s`);
  stats.saved = 0;
}, 60_000);

// Сет локальных "увиденных" за последнюю сессию, чтобы не долбить БД при дублях от разных источников
const seenInProcess = new Set();
setInterval(() => { seenInProcess.clear(); }, 5 * 60_000); // каждые 5 минут очищаем

async function handleMessage(raw, tag) {
  let data = raw;
  if (Buffer.isBuffer(data)) data = data.toString();
  if (typeof data === 'string') {
    try { data = JSON.parse(data); } catch { data = { text: data }; }
  }
  const ca = extractCA(data);
  if (!ca) return;
  if (seenInProcess.has(ca)) return;
  seenInProcess.add(ca);
  try { await saveToken(ca, data); }
  catch (e) { console.error(`[${tag}] DB error:`, e.message); }
}

function connectWS({ url, subscribeMsg, headers, tag }) {
  let backoff = 1000; // 1s → 30s
  let ws;

  const open = () => {
    ws = new WebSocket(url, { headers });
    let heartbeat;

    ws.on('open', () => {
      console.log(`[${tag}] open`);
      backoff = 1000;
      try { ws.send(JSON.stringify(subscribeMsg)); } catch {}
      heartbeat = setInterval(() => { try { ws.ping(); } catch {} }, 20_000);
    });

    ws.on('message', (chunk) => handleMessage(chunk, tag));
    ws.on('error', (e) => console.error(`[${tag}] error:`, e.message));

    const scheduleReconnect = () => {
      clearInterval(heartbeat);
      const wait = Math.min(backoff, 30_000);
      console.log(`[${tag}] closed → reconnect in ${wait}ms`);
      stats.reconnects++;
      setTimeout(open, wait);
      backoff *= 2;
    };

    ws.on('close', scheduleReconnect);

    // сторожок: если 2 минуты нет новых save — форсим перезапуск сокета
    const guard = setInterval(() => {
      const idleMs = Date.now() - (stats.lastSavedAt || 0);
      if (idleMs > 120_000) {
        console.log(`[${tag}] idle ${Math.round(idleMs/1000)}s → force reconnect`);
        try { ws.terminate(); } catch {}
      }
    }, 30_000);
    ws.once('close', () => clearInterval(guard));
  };

  open();
}

// ——— запуск ———
(async () => {
  await initDb();

  // 1) основной поток: PumpPortal (subscribeNewToken)
  connectWS({
    url: 'wss://pumpportal.fun/api/data',                 // офиц. точка PumpPortal
    subscribeMsg: { method: 'subscribeNewToken' },        // новые токены
    headers: undefined,
    tag: 'pumpportal',
  });

  // 2) резервный поток (опционально): bloXroute или любой другой WS
  // включается, если есть переменная окружения SECONDARY_WS_URL
  if (process.env.SECONDARY_WS_URL) {
    const hdr = {};
    if (process.env.SECONDARY_WS_AUTH_HEADER && process.env.SECONDARY_WS_AUTH_VALUE) {
      hdr[process.env.SECONDARY_WS_AUTH_HEADER] = process.env.SECONDARY_WS_AUTH_VALUE;
    }
    // для bloXroute метод обычно называется GetPumpFunNewTokensStream
    connectWS({
      url: process.env.SECONDARY_WS_URL,
      subscribeMsg: { method: 'GetPumpFunNewTokensStream' }, // не страшно, если игнорится
      headers: hdr,
      tag: 'secondary',
    });
  }
})();
