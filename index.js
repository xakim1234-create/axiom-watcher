import puppeteer, { executablePath as ppExecPath } from "puppeteer";
import fetch from "node-fetch";

const PAGE_URL = process.env.PAGE_URL || "https://axiom.trade/pulse";
const CDN_HOST = process.env.CDN_HOST || "axiomtrading.sfo3.cdn.digitaloceanspaces.com";
const API_URL  = process.env.API_URL;
const BATCH_MS = +(process.env.BATCH_MS || 5000);
const RELOAD_MS = +(process.env.RELOAD_MS || 60_000);   // раз в 60 сек перегружаем
const SCROLL_MS = +(process.env.SCROLL_MS || 5_000);    // раз в 5 сек подскролл

if (!API_URL) {
  console.error("❌ Set API_URL env (your Vercel endpoint)");
  process.exit(1);
}

const queue = new Set();
const seenUrl = new Set();

function extractMintFromUrl(u) {
  try {
    const last = u.split("/").pop().split("?")[0].split("#")[0];
    const dot = last.indexOf(".");
    return dot === -1 ? last : last.slice(0, dot); // между / и первой точкой
  } catch { return null; }
}

async function flush() {
  if (!queue.size) return;
  const mints = Array.from(queue);
  queue.clear();
  try {
    const r = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mints }),
    });
    console.log(`✅ FLUSH sent=${mints.length} status=${r.status}`);
  } catch (e) {
    console.error("❌ FLUSH failed, requeue:", e);
    mints.forEach(m => queue.add(m));
  }
}

function considerUrl(url) {
  try {
    const u = new URL(url);
    if (u.hostname !== CDN_HOST) return;
    if (!u.pathname.endsWith("pump.webp")) return;
    if (seenUrl.has(url)) return;
    seenUrl.add(url);
    const mint = extractMintFromUrl(url);
    if (mint) {
      queue.add(mint);
      console.log("👀 NEW CA:", mint, " ← ", url);
    }
  } catch {}
}

async function runOnce() {
  const execPathEnv = (process.env.PUPPETEER_EXECUTABLE_PATH || "").trim();
  const execPath = execPathEnv || ppExecPath();
  console.log("🧭 Using Chrome at:", execPath);

  const browser = await puppeteer.launch({
    headless: true,
    executablePath: execPath,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
    ],
  });

  const page = await browser.newPage();

  // Без кеша, чтобы новые запросы не съедал SW/кеш
  await page.setCacheEnabled(false);

  // Дружелюбный UA и размер окна
  await page.setUserAgent(
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
  );
  await page.setViewport({ width: 1366, height: 768 });

  // Логи страницы — если там ошибки, мы их увидим
  page.on("console", msg => {
    try { console.log("🖥️ page:", msg.type(), msg.text()); } catch {}
  });

  // 1) Слушаем сразу три источника
  page.on("request", req => considerUrl(req.url()));
  page.on("requestfinished", req => considerUrl(req.url()));
  page.on("response", res => { try { considerUrl(res.url()); } catch {} });

  // 2) Идём на страницу
  await page.goto(PAGE_URL, { waitUntil: "domcontentloaded" });
  console.log("🟢 Watcher opened:", PAGE_URL);

  // 3) Периодически скроллим — триггерим lazy/обновления
  const scrollTimer = setInterval(async () => {
    try {
      await page.evaluate(() => {
        window.scrollBy(0, 400);
        setTimeout(() => window.scrollTo(0, 0), 500);
      });
    } catch {}
  }, SCROLL_MS);

  // 4) Периодически перезагружаем страницу — чтобы не залипало
  const reloadTimer = setInterval(async () => {
    try {
      console.log("🔄 Reloading page...");
      await page.reload({ waitUntil: "domcontentloaded" });
    } catch (e) {
      console.error("Reload failed:", e);
    }
  }, RELOAD_MS);

  // 5) Периодический FLUSH
  const flushTimer = setInterval(flush, BATCH_MS);

  // держим процесс «вечно»
  await new Promise(() => {});

  // на случай выхода (обычно не дойдём)
  clearInterval(scrollTimer);
  clearInterval(reloadTimer);
  clearInterval(flushTimer);
  await browser.close();
}

async function main() {
  console.log("🚀 Launching watcher...");
  while (true) {
    try {
      await runOnce();
    } catch (e) {
      console.error("⚠️ Watcher error, restart in 5s:", e);
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

process.on("uncaughtException", err => console.error("UNCAUGHT", err));
process.on("unhandledRejection", err => console.error("UNHANDLED", err));

main();
