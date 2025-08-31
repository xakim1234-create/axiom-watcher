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
    await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mints }),
    });
    console.log("✅ Flushed mints:", mints);
  } catch (e) {
    console.error("❌ Failed to flush:", e);
  }
}

async function gotoWithRetries(page, url, tries = 3) {
  for (let i = 1; i <= tries; i++) {
    try {
      console.log(`🌐 goto attempt ${i}/${tries}: ${url}`);
      // быстрее стартует на Render, чем networkidle2
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 120_000 });
      // небольшой догон, если сеть дергается
      await page.waitForTimeout(2000);
      return;
    } catch (e) {
      console.warn(`⚠️ goto failed (${i}/${tries}):`, e?.message || e);
      if (i === tries) throw e;
    }
  }
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

  // логируем всё, что происходит на странице — помогает дебажить
  page.on("console", (msg) => {
    try {
      console.log(`🖥️ page: ${msg.type()} ${msg.text()}`);
    } catch {}
  });
  page.on("pageerror", (err) => console.error("🖥️ pageerror:", err));

  // подменим UA — ближе к обычному браузеру
  await page.setUserAgent(
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
  );
  await page.setViewport({ width: 1366, height: 768 });

  // ⬇️ КУКИ ИЗ ENV
  try {
    if (process.env.AXIOM_COOKIES) {
      const cookies = JSON.parse(process.env.AXIOM_COOKIES);
      await page.setCookie(...cookies);
      console.log(`🍪 Injected ${cookies.length} cookies for axiom.trade`);
    } else {
      console.warn("⚠️ No AXIOM_COOKIES provided, login may fail");
    }
  } catch (err) {
    console.error("❌ Failed to load cookies:", err);
  }

  // перехват запросов для выдёргивания mint’ов
  await page.setRequestInterception(true);
  page.on("request", (req) => {
    const url = req.url();
    if (seenReq.has(url)) return req.continue();
    seenReq.add(url);

    if (url.includes(CDN_HOST)) {
      const mint = extractMintFromUrl(url);
      if (mint) {
        console.log("👀 Found mint:", mint);
        queue.add(mint);
      }
    }

    req.continue();
  });

  // заходим на страницу с ретраями
  await gotoWithRetries(page, PAGE_URL, 3);

  // пробуем убедиться, что мы «внутри» (есть контент)
  try {
    // если у них есть какой-то «пульс»-список — подстрой при желании
    await page.waitForSelector("body", { timeout: 30_000 });
    console.log("🟢 Page ready (body present). Watching…");
  } catch {
    console.warn("⚠️ body selector not confirmed — продолжаем наблюдать");
  }

  // периодическая отправка
  setInterval(flush, BATCH_MS);
}

// автоперезапуск на ошибках
run().catch((err) => {
  console.error("Watcher error, restart in 5s:", err);
  setTimeout(run, 5000);
});
