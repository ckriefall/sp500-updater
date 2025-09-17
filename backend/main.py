# --- stdlib ---
import json
import logging
import os
import re
import unicodedata
from datetime import datetime
from io import StringIO
from typing import List, Dict, Any

# --- third-party ---
import pandas as pd
import requests
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- app constants ---
DATA_FILE = "companies.json"
LOG_FILE = "changes.log"
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
FIN_FILE = "financials.json"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return []

def save_companies(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

def load_financials() -> Dict[str, Any]:
    if os.path.exists(FIN_FILE):
        with open(FIN_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_financials(data: Dict[str, Any]) -> None:
    with open(FIN_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def append_log(message: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} - {message}\n")

def log_changes(old_data: List[Dict[str, Any]],
                new_data: List[Dict[str, Any]],
                key: str = "symbol") -> Dict[str, Any]:
    old_map = {r[key]: r for r in old_data if key in r}
    new_map = {r[key]: r for r in new_data if key in r}
    old_keys = set(old_map)
    new_keys = set(new_map)

    added = sorted(new_keys - old_keys)
    removed = sorted(old_keys - new_keys)

    updated = []
    updated_detail = {}
    for k in (old_keys & new_keys):
        o, n = old_map[k], new_map[k]
        if o != n:
            updated.append(k)
            fields = sorted(set(o.keys()) | set(n.keys()))
            changes = {
                f: {"old": o.get(f), "new": n.get(f)}
                for f in fields
                if o.get(f) != n.get(f)
            }
            updated_detail[k] = changes

    entry = {
        "ts": datetime.now().isoformat(),
        "counts": {"old": len(old_data), "new": len(new_data)},
        "added": added,
        "removed": removed,
        "updated": [{"symbol": s, "changes": updated_detail[s]} for s in updated],
    }

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")

    return {"added": added, "removed": removed, "updated": updated}

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; sp500-updater/1.0; +https://example.org/contact)",
    "Accept-Language": "en-US,en;q=0.9",
}

def _http_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(DEFAULT_HEADERS)
    return s

def _norm_col(s: str) -> str:
    s = unicodedata.normalize("NFKC", str(s))
    s = s.replace("\u2011", "-").replace("\u2010", "-")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def _fetch_wikipedia_html() -> str:
    sess = _http_session()
    r = sess.get(WIKI_URL, timeout=15)
    if r.status_code in (403, 429):
        alt = "https://en.wikipedia.org/w/index.php?title=List_of_S%26P_500_companies&printable=yes"
        r = sess.get(alt, timeout=15)
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Failed to fetch S&P 500 list: HTTP {r.status_code}")
    return r.text

def fetch_sp500_list() -> List[Dict[str, Any]]:
    try:
        html = _fetch_wikipedia_html()
        tables = pd.read_html(StringIO(html), match="Symbol")
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Failed to parse S&P 500 table")
        raise HTTPException(status_code=502, detail=f"Failed to parse S&P 500 list: {e}")

    if not tables:
        raise HTTPException(status_code=502, detail="No tables found at source")

    df = tables[0]
    col_map = {_norm_col(c): c for c in df.columns}
    want = {
        "symbol": ("symbol",),
        "name": ("security", "company", "name"),
        "sector": ("gics sector", "sector"),
        "sub_sector": ("gics sub-industry", "gics sub industry", "sub-industry", "sub industry"),
        "headquarters": ("headquarters location", "headquarters"),
        "date_added": ("date first added", "date added"),
    }

    chosen = {}
    for out_key, candidates in want.items():
        chosen[out_key] = next((col_map[c] for c in candidates if c in col_map), None)

    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        rec = {}
        for out_key, src_col in chosen.items():
            val = row[src_col] if src_col in df.columns else None
            if pd.isna(val):
                val = None
            if isinstance(val, str):
                val = val.strip()
            rec[out_key] = val
        if rec["symbol"] is not None:
            rec["symbol"] = str(rec["symbol"]).upper()
        records.append(rec)

    seen = set()
    uniq = []
    for r in records:
        k = r.get("symbol")
        if k and k not in seen:
            uniq.append(r)
            seen.add(k)
    uniq.sort(key=lambda r: r.get("symbol") or "")
    logging.info("fetch_sp500_list: %d symbols", len(uniq))
    return uniq

@app.get("/companies")
def get_companies():
    return load_data()

@app.get("/companies/{symbol}")
def get_company(symbol: str):
    for c in load_data():
        if c.get("symbol", "").upper() == symbol.upper():
            return c
    raise HTTPException(status_code=404, detail="Company not found")

@app.post("/refresh")
def refresh_companies():
    try:
        logging.info("Starting refresh...")
        old_data = load_data()
        new_data = fetch_sp500_list()
        save_companies(new_data)
        changes = log_changes(old_data, new_data, key="symbol")
        return {"status": "success", "count": len(new_data), **changes}
    except Exception as e:
        logging.exception("Refresh failed")
        raise HTTPException(status_code=500, detail=f"Refresh failed: {e}")

# ---- Financials endpoints ----
from financials_provider import fetch_financials

@app.post("/financials/refresh")
def refresh_financials(symbols: List[str] | None = Body(default=None)):
    companies = load_data()
    if not companies:
        raise HTTPException(status_code=400, detail="No companies loaded. Run /refresh first.")

    all_symbols = [c.get("symbol", "").upper() for c in companies if c.get("symbol")]
    target = [s.upper() for s in symbols] if symbols else all_symbols

    existing = load_financials()
    fetched = fetch_financials(target)

    updated = 0
    for sym, rec in fetched.items():
        if rec:
            existing[sym] = rec
            updated += 1

    save_financials(existing)
    append_log(f"Financials refreshed: {updated}/{len(target)} symbols")
    return {"status": "success", "requested": len(target), "updated": updated, "skipped": len(target) - updated}

@app.get("/financials/{symbol}")
def get_financials(symbol: str):
    fin = load_financials()
    rec = fin.get(symbol.upper())
    if not rec:
        raise HTTPException(status_code=404, detail="No financial data for symbol")
    return rec

@app.get("/", response_class=HTMLResponse)
def companies_ui():
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>S&P 500 Updater</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-50 text-gray-900">
  <header class="border-b bg-white">
    <div class="mx-auto max-w-6xl px-4 py-4 flex items-center justify-between gap-4">
      <div class="flex items-center gap-3">
        <div class="w-9 h-9 rounded-2xl bg-gray-900 text-white grid place-items-center font-semibold">S&P</div>
        <div>
          <h1 class="text-xl font-semibold leading-tight">S&amp;P 500 Updater</h1>
          <p class="text-sm text-gray-500">Browse companies • View details • Refresh from source</p>
        </div>
      </div>
      <div class="flex items-center gap-3">
        <button id="refreshBtn" onclick="refreshData()" class="px-4 py-2 bg-gray-900 text-white rounded-xl">Refresh Data</button>
        <button id="finBtn" onclick="refreshFinancials()" class="px-4 py-2 bg-blue-600 text-white rounded-xl">Refresh Financials</button>
        <a href="/docs" class="text-sm text-gray-500 hover:text-gray-900 underline">API Docs</a>
      </div>
    </div>
  </header>
  <main class="mx-auto max-w-6xl px-4 py-6">
    <div id="notice" class="hidden mb-4 rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-emerald-900"></div>
    <div id="error" class="hidden mb-4 rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-rose-900"></div>
    <div id="listView" class="rounded-2xl border bg-white shadow-sm overflow-hidden">
      <table class="min-w-full text-sm"><thead><tr>
        <th class="px-4 py-3">Symbol</th><th>Name</th><th>Sector</th><th>Sub-Sector</th><th>HQ</th><th>Date Added</th>
      </tr></thead><tbody id="companiesTbody"></tbody></table>
    </div>
    <div id="detailView" class="hidden"><div id="detailCard" class="p-6"></div></div>
  </main>
<script>
  async function loadCompanies() {
    const res = await fetch('/companies');
    if (!res.ok) return alert('Error loading companies');
    const companies = await res.json();
    document.getElementById('companiesTbody').innerHTML = companies.map(c =>
      `<tr><td><button onclick="loadCompany('${c.symbol}')">${c.symbol}</button></td>
        <td>${c.name||''}</td><td>${c.sector||''}</td><td>${c.sub_sector||''}</td><td>${c.headquarters||''}</td><td>${c.date_added||''}</td></tr>`
    ).join('');
  }

  async function loadCompany(symbol) {
    const res = await fetch('/companies/' + encodeURIComponent(symbol));
    const c = await res.json();
    let f = null;
    try {
      const fres = await fetch('/financials/' + encodeURIComponent(symbol));
      if (fres.ok) f = await fres.json();
    } catch {}
    document.getElementById('detailCard').innerHTML = `
      <h2 class="text-xl font-bold">${c.symbol}</h2>
      <p>${c.name||''}</p>
      ${f ? `<div><h3>Financials (as of ${f.as_of})</h3>
        <p>Price: $${f.price}</p>
        <p>Market Cap: ${f.market_cap}</p>
        <p>P/E: ${f.trailing_pe ?? '—'}</p>
        </div>` : ''}`;
    document.getElementById('detailView').classList.remove('hidden');
  }

  async function refreshData() {
    await fetch('/refresh', {method:'POST'});
    await loadCompanies();
  }

  async function refreshFinancials() {
    await fetch('/financials/refresh', {method:'POST', headers: {"Content-Type":"application/json"}, body:"null"});
    const params = new URLSearchParams(location.search);
    if (params.get('symbol')) loadCompany(params.get('symbol'));
  }

  loadCompanies();
</script>
</body>
</html>"""
