// index.js
import puppeteer from "puppeteer";
import fetch from "node-fetch";

// ====== CONFIG ======
const PAGE_URL  = process.env.PAGE_URL  || "https://axiom.trade/pulse";
const CDN_HOST  = process.env.CDN_HOST  || "axiomtrading.sfo3.cdn.digitaloceanspaces.com";
const API_URL   = process.env.API_URL;                 // ваш Vercel /api/mints
const BATCH_MS  = +(process.env.BATCH_MS || 5000);     // период отправки
const CHROME    = "/opt/render/.cache/puppeteer/chrome/linux-131.0.6778.204/chrome-linux64/chrome";

// ====== GUARDS ======
if (!API_URL) {
  console.error("❌ Set API_URL env (your Vercel endpoint)");
  process.exit(1);
}

// ====== STATE ======
const queue   = new Set();
const seenReq = new Set();

// ====== UTILS ======
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function extractMintFromUrl(u) {
  try {
    const last = u.split("/").pop().split("?")[0].split("#")[0];
    const dot  = last.indexOf(".");
    return dot === -1 ? last : last.slice(0, dot);
  } catch {
    return null;
  }
}

async function flush() {
  if (queue.size === 0) return;
  const mints = Array.from(queue);
  queue.clear();
  try {
    const r = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mints }),
    });
    console.log(`✅ Flushed mints: ${mints.length} → status=${r.status}`);
  } catch (e) {
    console.error("❌ Failed to flush:", e);
    // вернём назад, чтобы не потерять
    mints.forEach(m => queue.add(m));
  }
}

async function gotoWithRetries(page, url, tries = 3) {
  for (let i = 1; i <= tries; i++) {
    try {
      console.log(`🌐 goto attempt ${i}/${tries}: ${url}`);
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 120_000 });
      await sleep(1500); // заменили page.waitForTimeout
      return;
    } catch (e) {
      console.warn(`⚠️ goto failed (${i}/${tries}):`, e?.message || e);
      if (i === tries) throw e;
      await sleep(1500);
    }
  }
}

function normalizeCookies(raw) {
  // гарантируем .axiom.trade, secure и sane значения
  return raw.map(c => {
    const domain = (c.domain || "axiom.trade").replace(/^https?:\/\//, "");
    const needsDot = !domain.startsWith(".");
    return {
      path: "/",
      sameSite: c.sameSite || "Lax",
      secure: true,
      httpOnly: !!c.httpOnly,
      ...c,
      domain: needsDot ? `.${domain}` : domain, // 👈 важная точка перед axiom.trade
    };
  });
}

// ====== MAIN ======
async function run() {
  console.log("🚀 Launching watcher...");

  const browser = await puppeteer.launch({
    headless: true,
    executablePath: CHROME,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage", // важно для Render
      "--no-zygote",
      "--single-process",
    ],
  });

  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120_000);
  page.setDefaultTimeout(120_000);
  await page.setCacheEnabled(false);

  // логи страницы
  page.on("console", (msg) => {
    try { console.log(`🖥️ page: ${msg.type()} ${msg.text()}`); } catch {}
  });
  page.on("pageerror", (err) => console.error("🖥️ pageerror:", err));

  // user agent и размер
  await page.setUserAgent(
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
  );
  await page.setViewport({ width: 1366, height: 768 });

  // ⬇️ КУКИ ИЗ ENV (+ Authorization заголовок)
  let accessToken = "";
  try {
    if (process.env.AXIOM_COOKIES) {
      const cookiesRaw = JSON.parse(process.env.AXIOM_COOKIES);
      const cookies = normalizeCookies(cookiesRaw);
      await page.setCookie(...cookies);
      console.log(`🍪 Injected ${cookies.length} cookies for .axiom.trade`);

      const access = cookies.find(c => c.name === "auth-access-token");
      if (access?.value) accessToken = access.value;
    } else {
      console.warn("⚠️ No AXIOM_COOKIES provided, login may fail");
    }
  } catch (err) {
    console.error("❌ Failed to load cookies:", err);
  }

  if (accessToken) {
    await page.setExtraHTTPHeaders({
      "Authorization": `Bearer ${accessToken}`
    });
  }

  // перехват запросов → выдёргиваем CA из URL
  await page.setRequestInterception(true);
  page.on("request", (req) => {
    const url = req.url();
    if (seenReq.has(url)) return req.continue();
    seenReq.add(url);

    // лог WS для отладки
    if (url.startsWith("wss://") || url.startsWith("ws://")) {
      console.log("🔌 WS:", url);
    }

    if (url.includes(CDN_HOST) && url.endsWith("pump.webp")) {
      const mint = extractMintFromUrl(url);
      if (mint) {
        console.log("👀 NEW CA:", mint);
        queue.add(mint);
      }
    }

    req.continue();
  });

  await gotoWithRetries(page, PAGE_URL, 3);

  try {
    await page.waitForSelector("body", { timeout: 30_000 });
    console.log("🟢 Page ready (body present). Watching…");
  } catch {
    console.warn("⚠️ body selector not confirmed — продолжаем наблюдать");
  }

  // лёгкий автоскролл, чтобы триггерить lazy/обновления
  setInterval(async () => {
    try {
      await page.evaluate(() => {
        window.scrollBy(0, 500);
        setTimeout(() => window.scrollTo(0, 0), 400);
      });
    } catch {}
  }, 5_000);

  // периодическая отправка
  setInterval(flush, BATCH_MS);
}

// автоперезапуск на ошибках
run().catch((err) => {
  console.error("Watcher error, restart in 5s:", err);
  setTimeout(run, 5000);
});
