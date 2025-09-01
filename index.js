// index.js
import puppeteer from "puppeteer";
import fetch from "node-fetch";

/* ====== ENV ====== */
const PAGE_URL  = process.env.PAGE_URL  || "https://axiom.trade/pulse";
const CDN_HOST  = process.env.CDN_HOST  || "axiomtrading.sfo3.cdn.digitaloceanspaces.com";
const API_URL   = process.env.API_URL;                 // твой Vercel endpoint
const BATCH_MS  = +(process.env.BATCH_MS || 5000);

// Путь к установленному Chrome (Render). Можно переопределить через ENV CHROME_PATH.
const EXEC_PATH = process.env.CHROME_PATH
  || "/opt/render/.cache/puppeteer/chrome/linux-131.0.6778.204/chrome-linux64/chrome";

if (!API_URL) {
  console.error("❌ Set API_URL env (your Vercel endpoint)");
  process.exit(1);
}

/* ====== STATE ====== */
const queue   = new Set();
const seenReq = new Set();

/* ====== UTILS ====== */
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
  if (!queue.size) return;
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

/** надёжная навигация с ретраями */
async function gotoWithRetries(page, url, retries = 3, timeout = 45000) {
  let lastErr;
  for (let i = 1; i <= retries; i++) {
    try {
      console.log(`🌐 goto attempt ${i}/${retries}: ${url}`);
      await page.goto(url, { waitUntil: ["networkidle2", "domcontentloaded"], timeout });
      return;
    } catch (err) {
      lastErr = err;
      console.warn(`⚠️ goto failed (${i}/${retries}): ${err.message}`);
      await page.waitForTimeout(1500);
    }
  }
  throw lastErr;
}

/** подготавливаем куки (главное — домен .axiom.trade для поддоменов) */
function normalizeAxiomCookies() {
  if (!process.env.AXIOM_COOKIES) return null;
  try {
    let cookies = JSON.parse(process.env.AXIOM_COOKIES);
    cookies = cookies.map((c) => {
      if (c.domain === "axiom.trade") c.domain = ".axiom.trade";
      return c;
    });
    return cookies;
  } catch (e) {
    console.error("❌ Failed to parse AXIOM_COOKIES:", e);
    return null;
  }
}

/* ====== MAIN ====== */
async function run() {
  console.log("🚀 Launching watcher...");

  const launchArgs = ["--no-sandbox", "--disable-setuid-sandbox"];
  if (process.env.PROXY_URL) launchArgs.push(`--proxy-server=${process.env.PROXY_URL}`);

  const browser = await puppeteer.launch({
    headless: true,
    executablePath: EXEC_PATH,
    args: launchArgs,
  });

  const page = await browser.newPage();

  // Вкалываем куки для авторизации
  const cookies = normalizeAxiomCookies();
  if (cookies && cookies.length) {
    await page.setCookie(...cookies);
    console.log(`🍪 Injected ${cookies.length} cookies for axiom.trade`);
  } else {
    console.warn("⚠️ No AXIOM_COOKIES provided, login may fail");
  }

  // Чуть более тихий лог ошибок страницы (опционально)
  page.on("console", (msg) => {
    const t = msg.type();
    if (t === "error" || t === "warning") {
      console.log(`🖥️ page: ${t}`, msg.text());
    }
  });
  page.on("pageerror", (err) => console.log("🖥️ pageerror:", err.message));
  page.on("requestfailed", (req) =>
    console.log("🖥️ requestfailed:", req.url(), req.failure()?.errorText || "")
  );

  // Перехват запросов и вытягивание минтов по CDN
  await page.setRequestInterception(true);
  page.on("request", (req) => {
    try {
      const url = req.url();
      if (seenReq.has(url)) return req.continue();
      seenReq.add(url);

      // Ищем картинки токенов на CDN (webp/png/jpg, чтобы отсечь мусор)
      if (
        url.includes(CDN_HOST) &&
        (url.endsWith(".webp") || url.endsWith(".png") || url.endsWith(".jpg"))
      ) {
        const mint = extractMintFromUrl(url);
        if (mint) {
          console.log("👀 Found mint:", mint, "from", url.slice(0, 140));
          queue.add(mint);
        }
      }
      req.continue();
    } catch {
      req.continue();
    }
  });

  // Сначала корень (прогреть сессию), затем /pulse
  await gotoWithRetries(page, "https://axiom.trade", 3);
  await gotoWithRetries(page, PAGE_URL, 3);

  // Периодическая отправка
  setInterval(flush, BATCH_MS);
}

run().catch((err) => {
  console.error("Watcher error, restart in 5s:", err);
  setTimeout(run, 5000);
});
