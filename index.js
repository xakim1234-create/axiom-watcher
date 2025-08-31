import puppeteer, { executablePath as ppExecPath } from "puppeteer";
import fetch from "node-fetch";

const PAGE_URL = process.env.PAGE_URL || "https://axiom.trade/pulse";
const CDN_HOST = process.env.CDN_HOST || "axiomtrading.sfo3.cdn.digitaloceanspaces.com";
const API_URL  = process.env.API_URL;
const BATCH_MS = +(process.env.BATCH_MS || 5000);

if (!API_URL) {
  console.error("❌ Set API_URL env (your Vercel endpoint)"); 
  process.exit(1);
}

const queue = new Set();
const seenReq = new Set();

function extractMintFromUrl(u) {
  try {
    const last = u.split("/").pop().split("?")[0].split("#")[0];
    const dot = last.indexOf(".");
    return dot === -1 ? last : last.slice(0, dot); // всё между / и первой .
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
    console.log(`✅ FLUSH sent=${mints.length} status=${r.status}`);
  } catch (e) {
    console.error("❌ FLUSH failed, requeue:", e);
    mints.forEach(m => queue.add(m)); // вернём обратно — не теряем
  }
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

  // ловим завершённые запросы и фильтруем CDN + mask pump.webp
  page.on("requestfinished", req => {
    try {
      const url = req.url();
      if (!url.includes(CDN_HOST)) return;
      if (!url.endsWith("pump.webp")) return;
      if (seenReq.has(url)) return;
      seenReq.add(url);

      const mint = extractMintFromUrl(url);
      if (mint && !queue.has(mint)) {
        queue.add(mint);
        console.log("👀 NEW CA:", mint);
      }
    } catch (e) {
      // просто игнор
    }
  });

  await page.goto(PAGE_URL, { waitUntil: "domcontentloaded" });
  console.log("🟢 Watcher opened:", PAGE_URL);

  const timer = setInterval(flush, BATCH_MS);

  // держим процесс живым, пока всё ок
  await new Promise(() => {}); // никогда не resolve

  // (теоретически не дойдём сюда, но на всякий случай)
  clearInterval(timer);
  await browser.close();
}

async function main() {
  console.log("🚀 Launching watcher...");
  while (true) {
    try {
      await runOnce();
    } catch (e) {
      console.error("⚠️ Watcher error, restart in 5s:", e);
      // небольшая пауза, чтобы не долбить перезапусками
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

process.on("uncaughtException", err => {
  console.error("UNCAUGHT", err);
});
process.on("unhandledRejection", err => {
  console.error("UNHANDLED", err);
});

main();
