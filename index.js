// axiom-watcher (pump.fun): слушаем только
//   https://imagedelivery.net/.../coin-image/<MINT>/<SIZE>?...
// и шлём батчами на API_URL

import puppeteer from "puppeteer-core";
import fetch from "node-fetch";
import fs from "fs";
import { execFile } from "child_process";
import { promisify } from "util";

const execFileAsync = promisify(execFile);

// -----------------------------
// CONFIG
// -----------------------------
const DEBUG = process.env.DEBUG === "1" || process.env.DEBUG === "true";

const PAGE_URL = process.env.PAGE_URL || "https://pump.fun/advanced/scan";
const API_URL = process.env.API_URL;                     // твой Vercel endpoint
const BATCH_MS = +(process.env.BATCH_MS || 5000);

// где лежит Chrome на Render (мы ставим его в Start Command)
const CHROME_PATH =
  process.env.PUPPETEER_EXEC_PATH ||
  "/opt/render/.cache/puppeteer/chrome/linux-131.0.6778.204/chrome-linux64/chrome";

// -----------------------------
// LOG helpers
// -----------------------------
const t0 = process.hrtime.bigint();
const fmtMs = ns => `${Number(ns) / 1e6 | 0}ms`;
const nowMs = () => fmtMs(process.hrtime.bigint() - t0);

function L(phase, msg, extra = {}) {
  const base = { t: nowMs(), phase, msg };
  const line = { ...base, ...extra };
  // делаем лёгкую компактную строку
  console.log(
    `${line.t} [${line.phase}] ${line.msg}` +
    (Object.keys(extra).length ? ` ${JSON.stringify(extra)}` : "")
  );
}
function LE(phase, err, extra = {}) {
  const m = (err && err.message) ? err.message : String(err);
  L(phase, `ERROR: ${m}`, extra);
}

// -----------------------------
// QUEUE / counters
// -----------------------------
const queue = new Set();
const seenReq = new Set();

const counters = {
  reqStart: 0,
  reqFinish: 0,
  reqFail: 0,
  inflight: 0,
  wsOpen: 0,
  wsErr: 0,
  wsClose: 0
};

// -----------------------------
// helpers
// -----------------------------
function extractMintFromDeliveryUrl(u) {
  try {
    // ожидаем: .../coin-image/<MINT>/<SIZE>?...
    const i = u.indexOf("/coin-image/");
    if (i === -1) return null;
    const tail = u.slice(i + "/coin-image/".length);
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
  const started = process.hrtime.bigint();
  try {
    const res = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mints }),
    });
    const txt = await res.text().catch(() => "");
    L("flush", `sent ${mints.length} mints`, {
      status: res.status, dur: fmtMs(process.hrtime.bigint() - started),
      sample: mints.slice(0, 5)
    });
    if (!res.ok && DEBUG) {
      L("flush", "non-200 response body", { body: txt.slice(0, 200) });
    }
  } catch (e) {
    LE("flush", e, { dur: fmtMs(process.hrtime.bigint() - started) });
  }
}

async function gotoWithRetries(page, url, tries = 3) {
  for (let i = 1; i <= tries; i++) {
    const started = process.hrtime.bigint();
    try {
      L("goto", `attempt ${i}/${tries}`, { url });
      // ждём только domcontentloaded — networkidle может не наступить
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60_000 });
      L("goto", "success", { dur: fmtMs(process.hrtime.bigint() - started) });
      return;
    } catch (err) {
      LE("goto", err, { attempt: i, dur: fmtMs(process.hrtime.bigint() - started) });
      // маленькая пауза между попытками
      await page.waitForTimeout?.(1500).catch(() => {});
    }
  }
  throw new Error("goto failed after retries");
}

async function dismissOverlays(page) {
  const started = process.hrtime.bigint();
  try {
    // Мягко: клики по «Next/Done/Accept/Close»
    const selectors = [
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
    let clicks = 0;
    for (const sel of selectors) {
      const btn = await page.$(sel).catch(() => null);
      if (btn) {
        await btn.click({ delay: 50 }).catch(() => {});
        clicks++;
        await page.waitForTimeout?.(200).catch(() => {});
      }
    }
    // ESC
    try { await page.keyboard.press("Escape"); } catch {}
    try { await page.keyboard.press("Escape"); } catch {}

    // Жёсткий CSS-фолбэк
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

    L("overlays", "dismissed", { clicks, dur: fmtMs(process.hrtime.bigint() - started) });
  } catch (e) {
    LE("overlays", e);
  }
}

async function antiOnboarding(page) {
  await page.evaluateOnNewDocument(() => {
    try {
      const set = (k, v) => localStorage.setItem(k, typeof v === "string" ? v : JSON.stringify(v));
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
  L("antiOnboarding", "installed");
}

async function keepPageAlive(page) {
  setInterval(() => {
    dismissOverlays(page).catch(() => {});
  }, 5000);
}

// -----------------------------
// PROBE: проверить бинарь Chrome
// -----------------------------
async function probeChrome() {
  L("boot", "config", {
    node: process.version,
    page: PAGE_URL,
    api: API_URL ? "<set>" : "<missing>",
    batchMs: BATCH_MS,
    chromePath: CHROME_PATH,
  });

  const ok = fs.existsSync(CHROME_PATH);
  L("probe", ok ? "stat ok" : "stat missing", { exists: ok });

  if (ok) {
    try {
      const { stdout } = await execFileAsync(CHROME_PATH, ["--version"], { timeout: 5000 });
      L("probe", "chrome --version", { v: stdout.trim() });
    } catch (e) {
      LE("probe", e, { step: "--version" });
    }
    try {
      const st = fs.statSync(CHROME_PATH);
      L("probe", "file", { size: st.size, mode: (st.mode & 0o777).toString(8) });
    } catch (e) {
      LE("probe", e, { step: "statSync" });
    }
  }
}

// -----------------------------
// MAIN
// -----------------------------
async function run() {
  if (!API_URL) {
    L("boot", "API_URL is required. Exit.");
    process.exit(1);
  }

  await probeChrome();

  // запускаем Chrome
  const launchStarted = process.hrtime.bigint();
  L("launch", "starting", {
    headless: true,
    dumpio: !!DEBUG,
  });

  const browser = await puppeteer.launch({
    headless: true,
    executablePath: CHROME_PATH,
    dumpio: !!DEBUG, // в DEBUG льём stderr Chromium в наши логи
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--no-first-run",
      "--no-default-browser-check",
      "--disable-features=Translate,BackForwardCache",
    ],
  });

  L("launch", "started", { dur: fmtMs(process.hrtime.bigint() - launchStarted) });

  const page = await browser.newPage();
  L("page", "newPage created");

  // настройки страницы
  await page.setDefaultNavigationTimeout(60_000);
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131 Safari/537.36"
  );
  await page.setViewport({ width: 1280, height: 900 });
  L("page", "UA/viewport set");

  // вешаем диагностические события (в DEBUG более многословно)
  page.on("request", () => { counters.reqStart++; counters.inflight++; });
  page.on("requestfinished", () => { counters.reqFinish++; counters.inflight = Math.max(0, counters.inflight - 1); });
  page.on("requestfailed", req => {
    counters.reqFail++; counters.inflight = Math.max(0, counters.inflight - 1);
    if (DEBUG) L("requestfailed", req.url(), { err: req.failure()?.errorText });
  });
  page.on("console", msg => { if (DEBUG) L("page.console", msg.text()); });
  page.on("pageerror", err => L("pageerror", err.message));

  // ws диагностика (если страница открывает WS)
  page.on("framenavigated", f => { if (DEBUG) L("frame", "navigated", { url: f.url().slice(0, 120) }); });

  await antiOnboarding(page);

  // interception: слушаем только imagedelivery.net/…/coin-image/…
  await page.setRequestInterception(true);
  page.on("request", req => {
    const url = req.url();
    // пропускаем сразу всё — нам важно только «услышать» URL
    req.continue().catch(() => {});
    if (seenReq.has(url)) return;
    seenReq.add(url);

    if (
      url.startsWith("https://imagedelivery.net/") &&
      url.includes("/coin-image/") &&
      /\/(32|64|72|96|128|256)x(32|64|72|96|128|256)/.test(url)
    ) {
      const mint = extractMintFromDeliveryUrl(url);
      if (mint) queue.add(mint);
    }
  });
  L("interception", "on (imagedelivery.net/coin-image)");

  // навигация
  await gotoWithRetries(page, PAGE_URL, 3);

  await dismissOverlays(page);
  await keepPageAlive(page);

  // периодический «пинок», чтобы лента шевелилась
  setInterval(async () => {
    try {
      await page.evaluate(() => window.scrollTo(0, Math.random() * 2000));
    } catch {}
  }, 10_000);

  // heartbeat: раз в 30с печатаем состояние
  setInterval(() => {
    L("hb", "state", {
      queue: queue.size,
      req: { start: counters.reqStart, fin: counters.reqFinish, fail: counters.reqFail, inflight: counters.inflight },
      seen: seenReq.size
    });
  }, 30_000);

  // батч-отправка
  setInterval(flush, BATCH_MS);
}

// watchdog
run().catch(err => {
  LE("watchdog", err);
  L("watchdog", "restart in 5s");
  setTimeout(run, 5000);
});
