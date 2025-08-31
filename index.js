import puppeteer from "puppeteer";
import fetch from "node-fetch";

const PAGE_URL = process.env.PAGE_URL || "https://axiom.trade/pulse";
const CDN_HOST = process.env.CDN_HOST || "axiomtrading.sfo3.cdn.digitaloceanspaces.com";
const API_URL  = process.env.API_URL;
const BATCH_MS = +(process.env.BATCH_MS || 5000);

if (!API_URL) { console.error("Set API_URL env"); process.exit(1); }

const queue = new Set();
const seenReq = new Set();

const execPath = puppeteer.executablePath(); // <-- важное

function extractMintFromUrl(u) {
  const last = u.split("/").pop().split("?")[0].split("#")[0];
  const dot = last.indexOf(".");
  return dot === -1 ? last : last.slice(0, dot);
}

async function flush() {
  if (!queue.size) return;
  const mints = [...queue]; queue.clear();
  try {
    const r = await fetch(API_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ mints })
    });
    const js = await r.json().catch(()=>({}));
    console.log(`[FLUSH] sent=${mints.length} status=${r.status} ok=${js.ok}`);
  } catch (e) {
    console.error("[FLUSH] failed:", e);
    mints.forEach(m => queue.add(m));
  }
}

async function run() {
  while (true) {
    let browser;
    try {
      browser = await puppeteer.launch({
        headless: "new",
        executablePath: execPath,              // <-- важное
        args: ["--no-sandbox", "--disable-setuid-sandbox"]
      });
      const page = await browser.newPage();

      page.on("requestfinished", req => {
        try {
          const u = new URL(req.url());
          if (u.hostname !== CDN_HOST) return;
          if (!u.pathname.endsWith("pump.webp")) return;
          const full = u.toString();
          if (seenReq.has(full)) return;
          seenReq.add(full);

          const mint = extractMintFromUrl(full);
          if (mint && !queue.has(mint)) {
            queue.add(mint);
            console.log("[NEW CA]", mint);
          }
        } catch {}
      });

      await page.goto(PAGE_URL, { waitUntil: "domcontentloaded" });
      console.log("Watcher opened:", PAGE_URL);

      setInterval(flush, BATCH_MS);
      await new Promise(()=>{});
    } catch (e) {
      console.error("Watcher error, restart in 5s:", e);
      await new Promise(r => setTimeout(r, 5000));
    } finally {
      try { await flush(); } catch {}
      try { await browser?.close(); } catch {}
    }
  }
}

run();
