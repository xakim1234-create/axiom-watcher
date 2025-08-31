import puppeteer from "puppeteer";
import fetch from "node-fetch";

const PAGE_URL = process.env.PAGE_URL || "https://axiom.trade/pulse";
const CDN_HOST = process.env.CDN_HOST || "axiomtrading.sfo3.cdn.digitaloceanspaces.com";
const API_URL = process.env.API_URL;
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
      body: JSON.stringify({ mints })
    });
    console.log("✅ Flushed mints:", mints);
  } catch (e) {
    console.error("❌ Failed to flush:", e);
  }
}

async function run() {
  console.log("🚀 Launching watcher...");

  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"]
  });

  const page = await browser.newPage();
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

  await page.goto(PAGE_URL, { waitUntil: "networkidle2" });

  setInterval(flush, BATCH_MS);
}

run().catch((err) => {
  console.error("Watcher error, restart in 5s:", err);
  setTimeout(run, 5000);
});
