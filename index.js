// axiom-watcher: Pump.fun listener (imagedelivery.net/…/coin-image/<MINT>/…)
import puppeteer from "puppeteer-core";
import fetch from "node-fetch";

const PAGE_URL = process.env.PAGE_URL || "https://pump.fun/advanced/scan";
const API_URL = process.env.API_URL;                 // твой Vercel endpoint
const BATCH_MS = +(process.env.BATCH_MS || 5000);

// где лежит Chrome на Render (мы ставим его в Start Command)
const CHROME_PATH =
  process.env.PUPPETEER_EXEC_PATH ||
  "/opt/render/.cache/puppeteer/chrome/linux-131.0.6778.204/chrome-linux64/chrome";

if (!API_URL) {
  console.error("❌ Set API_URL env (your Vercel endpoint)");
  process.exit(1);
}

// ===== helpers =====
const queue = new Set();
const seenReq = new Set();

function extractMintFromDeliveryUrl(u) {
  try {
    // ожидаем: https://imagedelivery.net/.../coin-image/<MINT>/72x72?... или 128x128...
    const i = u.indexOf("/coin-image/");
    if (i === -1) return null;
    const tail = u.slice(i + "/coin-image/".length);
    // <MINT>/72x72?alpha=true
    const mint = tail.split("/")[0];
    if (!mint || mint.length < 8) return null;
    return mint;
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
    console.log("✅ Flushed mints:", mints.length, mints.slice(0, 5));
  } catch (e) {
    console.error("❌ Failed to flush:", e?.message || e);
  }
}

async function gotoWithRetries(page, url, tries = 3) {
  for (let i = 1; i <= tries; i++) {
    try {
      console.log(`🌐 goto attempt ${i}/${tries}: ${url}`);
      await page.goto(url, { waitUntil: "networkidle2", timeout: 45_000 });
      return;
    } catch (err) {
      console.warn(`⚠️ goto failed (${i}/${tries}): ${err?.message || err}`);
      // маленькая пауза между попытками
      await page.waitForTimeout?.(1500).catch(() => {});
    }
  }
  throw new Error("goto failed after retries");
}

// мягкое гашение модалок + жёсткий css-фолбэк
async function dismissOverlays(page) {
  // 1) на всякий: кнопки «Next / Done / Accept …»
  const clickers = [
    'button:has-text("Next")',
    'button:has-text("Done")',
    'button:has-text("Понятно")',
    'button:has-text("Accept all")',
    'button:has-text("Accept All")',
    'button:has-text("Accept")',
    'button:has-text("Принять")',
    '[aria-label="Close"]',
    'button[aria-label="Close"]',
  ];

  for (const sel of clickers) {
    try {
      const btn = await page.$(sel);
      if (btn) {
        await btn.click({ delay: 50 });
        await page.waitForTimeout?.(200).catch(() => {});
      }
    } catch {}
  }

  // send ESC несколько раз
  try {
    await page.keyboard.press("Escape");
    await page.waitForTimeout?.(200).catch(() => {});
    await page.keyboard.press("Escape");
  } catch {}

  // 2) жёсткий CSS-фолбэк (скроет любые overlay/role=dialog)
  await page.addStyleTag({
    content: `
      [role="dialog"], [role="alertdialog"], .modal, .Modal, .DialogOverlay,
      .overlay, .Overlay, .backdrop, .Backdrop, .cookie, .Cookie {
        display: none !important;
        visibility: hidden !important;
        pointer-events: none !important;
      }
      html, body { overflow: auto !important; }
    `,
  }).catch(() => {});
}

async function antiOnboarding(page) {
  // ставим флажки ДО загрузки страницы (на всякий)
  await page.evaluateOnNewDocument(() => {
    try {
      const set = (k, v) => localStorage.setItem(k, typeof v === "string" ? v : JSON.stringify(v));
      // разные варианты ключей – с запасом
      set("scanSettingsOnboardingSeen", "true");
      set("scan-onboarding", "done");
      set("cookie_consent", "true");
      set("cookie-consent", "true");
      document.addEventListener("DOMContentLoaded", () => {
        const style = document.createElement("style");
        style.textContent = `
          [role="dialog"], .modal, .overlay, .backdrop { display:none !important; }
          html, body { overflow:auto !important; }
        `;
        document.documentElement.appendChild(style);
      });
    } catch {}
  });
}

async function keepPageAlive(page) {
  // небольшой «джанитор»: раз в 5 сек гасим внезапные модалки
  setInterval(() => {
    dismissOverlays(page).catch(() => {});
  }, 5000);
}

// ===== main =====
async function run() {
  console.log("🚀 Launching watcher (Pump.fun)…");

  const browser = await puppeteer.launch({
    headless: true,
    executablePath: CHROME_PATH,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
    ],
  });

  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(60_000);

  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131 Safari/537.36"
  );

  await antiOnboarding(page);

  // слушаем только imagedelivery
  await page.setRequestInterception(true);
  page.on("request", (req) => {
    const url = req.url();
    // пропускаем сразу всё — нам важно только «услышать» URL
    req.continue().catch(() => {});
    if (seenReq.has(url)) return;
    seenReq.add(url);

    if (
      url.startsWith("https://imagedelivery.net/") &&
      url.includes("/coin-image/") &&
      /\/(32|64|72|128|256)x(32|64|72|128|256)/.test(url) // иконки разных размеров
    ) {
      const mint = extractMintFromDeliveryUrl(url);
      if (mint) {
        queue.add(mint);
        // для отладки можно включить:
        // console.log("👀 mint:", mint);
      }
    }
  });

  await gotoWithRetries(page, PAGE_URL, 3);
  await dismissOverlays(page);
  await keepPageAlive(page);

  // периодический «пинок», чтобы лента обновлялась
  setInterval(async () => {
    try {
      await page.evaluate(() => window.scrollTo(0, Math.random() * 1000));
    } catch {}
  }, 10_000);

  setInterval(flush, BATCH_MS);
}

run().catch((err) => {
  console.error("Watcher error, restart in 5s:", err);
  setTimeout(run, 5000);
});
