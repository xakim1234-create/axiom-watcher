import 'dotenv/config';
import WebSocket from 'ws';
import { Pool } from 'pg';

const DB = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
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
    create index if not exists pump_tokens_inserted_at_idx on pump_tokens(inserted_at desc);
  `);
  console.log('DB ready');
}

/** Достаём CA из payload */
function extractCA(obj) {
  if (!obj) return null;

  // 1) Явные поля
  const keys = ['ca','contract','mint','mintAddress','tokenAddress','address','publicKey'];
  for (const k of keys) {
    const v = obj?.[k];
    if (typeof v === 'string' && v.endsWith('pump')) return v;
  }

  // 2) Картинка coin-image/<CA>/
  const urls = [];
  const urlKeys = ['image','image_url','imageUri','imageURI','img','thumb','logo','pic','icon'];
  for (const k of urlKeys) if (typeof obj?.[k] === 'string') urls.push(obj[k]);
  if (typeof obj?.metadata?.image === 'string') urls.push(obj.metadata.image);

  for (const u of urls) {
    const m = /\/coin-image\/([^/]+)\//.exec(u);
    if (m) return m[1];
  }

  // 3) Фолбэк: ищем по сырой строке
  const s = JSON.stringify(obj);
  let m = /\/coin-image\/([^/]+)\//.exec(s);
  if (m) return m[1];
  m = /\b[1-9A-HJ-NP-Za-km-z]{25,}pump\b/.exec(s);  // base58 + "pump"
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
  console.log('saved', ca, name || '', symbol || '');
}

function runWs() {
  const URL = 'wss://pumpportal.fun/api/data';
  let backoff = 1000; // 1s → 30s

  const connect = () => {
    const ws = new WebSocket(URL);

    ws.on('open', () => {
      console.log('ws open');
      backoff = 1000;
      ws.send(JSON.stringify({ method: 'subscribeNewToken' }));
    });

    ws.on('message', async (chunk) => {
      let data = chunk.toString();
      try { data = JSON.parse(data); } catch {}
      const payload = typeof data === 'string' ? { text: data } : data;
      const ca = extractCA(payload);
      if (ca) {
        try { await saveToken(ca, payload); }
        catch (e) { console.error('DB error:', e.message); }
      }
    });

    ws.on('close', () => {
      const wait = Math.min(backoff, 30000);
      console.log('ws closed, reconnect in', wait, 'ms');
      setTimeout(connect, wait);
      backoff *= 2;
    });

    ws.on('error', (e) => {
      console.error('ws error:', e.message);
      try { ws.close(); } catch {}
    });

    // heartbeat
    const t = setInterval(() => { try { ws.ping(); } catch {} }, 20000);
    ws.once('close', () => clearInterval(t));
  };

  connect();
}

(async () => {
  await initDb();
  runWs();
})();
