import puppeteer from "puppeteer";
import fetch from "node-fetch";

/** ====== CONFIG ====== */
const PAGE_URL = process.env.PAGE_URL || "https://axiom.trade/pulse";
const CDN_HOST =
  process.env.CDN_HOST || "axiomtrading.sfo3.cdn.digitaloceanspaces.com";
const API_URL = process.env.API_URL;
const BATCH_MS = +(process.env.BATCH_MS || 5000);

// Хром, установленный на Render (пусть остаётся дефолт, можно переопределять env-ом)
const EXEC_PATH =
  process.env.PUPPETEER_EXECUTABLE_PATH ||
  "/opt/render/.cache/puppeteer/chrome/linux-131.0.6778.204/chrome-linux64/chrome";

/** ====== GUARDS ====== */
if (!API_URL) {
  console.error("❌ Set API_URL env (your Vercel endpoint)");
  process.exit(1);
}

/** ====== UTILS ====== */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Достаём mint из URL webp-картинки на CDN */
function extractMintFromCdnPath(u) {
  try {
    const url = new URL(u);
    if (!url.host.includes(CDN_HOST)) return null;
    if (!url.pathname.endsWith(".webp")) return null;

    const base = url.pathname.split("/").pop(); // пример: 24tBK...pump.webp  или  O_pfp.webp

    // 1) <mint>pump.webp
    const m1 = base.match(/^([A-Za-z0-9]{32,})pump\.webp$/);
    if (m1) return m1[1];

    // 2) <mint>npump.webp
    const m2 = base.match(/^([A-Za-z0-9]{32,})npump\.webp$/);
    if (m2) return m2[1];

    // 3) *_pfp.webp — игнорим (аватарки)
    if (/_pfp\.webp$/.test(base)) return null;

    // На всякий случай: всё, что перед "pump.webp"
    const i = base.indexOf("pump.webp");
    if (i > 0) {
      const maybe = base.slice(0, i);
      if (/^[A-Za-z0-9]{32,}$/.test(maybe)) return maybe;
    }

    return null;
  } catch {
    return null;
  }
}

/** ====== QUEUE & FLUSH ====== */
const queue = new Set();
const seenReq = new Set();

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

/** ====== ROBUST GOTO ====== */
async function gotoWithRetries(page, url, tries = 3) {
  for (let i = 1; i <= tries; i++) {
    try {
      console.log(`🌐 goto attempt ${i}/${tries}: ${url}`);
      await page.goto(url, {
        waitUntil: ["domcontentloaded", "networkidle2"],
        timeout: 45_000,
      });
      await sleep(1500); // даём подгрузиться картинкам
      return;
    } catch (e) {
      console.warn(`⚠️ goto failed (${i}/${tries}): ${e.message}`);
      if (i === tries) throw e;
      await sleep(2000);
    }
  }
}

/** ====== MAIN ====== */
async function run() {
  console.log("🚀 Launching watcher...");

  const browser = await puppeteer.launch({
    headless: true,
    executablePath: EXEC_PATH,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  const page = await browser.newPage();

  // user-agent/viewport ближе к твоим локальным условиям
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131 Safari/537.36"
  );
  await page.setViewport({ width: 1366, height: 900, deviceScaleFactor: 1 });

  // Куки с ENV (AXIOM_COOKIES) — JSON-массив
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

  // Ловим все запросы, но извлекаем только .webp с CDN
  await page.setRequestInterception(true);
  page.on("request", (req) => {
    const url = req.url();
    if (seenReq.has(url)) return req.continue();
    seenReq.add(url);

    const mint = extractMintFromCdnPath(url);
    if (mint) {
      if (!queue.has(mint)) {
        queue.add(mint);
        console.log("👀 Found mint:", mint, "from", url);
      }
    }
    req.continue();
  });

  // немножко логов со страницы — полезно в трейбле
  page.on("console", (msg) =>
    console.log("🖥️ page:", msg.type(), msg.text?.() ?? msg.text())
  );
  page.on("pageerror", (err) => console.log("🖥️ pageerror:", err.message));

  await gotoWithRetries(page, PAGE_URL, 3);

  // Автоскролл, чтобы подгружались новые карточки и их картинки
  setInterval(async () => {
    try {
      await page.evaluate(() => window.scrollBy(0, 900));
      await sleep(800);
    } catch (e) {
      console.warn("scroll error:", e.message);
    }
  }, 3500);

  // Периодическая отправка
  setInterval(flush, BATCH_MS);
}

run().catch((err) => {
  console.error("Watcher error, restart in 5s:", err);
  setTimeout(run, 5000);
});
