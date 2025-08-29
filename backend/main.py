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
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# --- app constants ---
DATA_FILE = "companies.json"
LOG_FILE = "changes.log"
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# (optional) basic logging setup
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ---- end of imports --------

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_FILE = "companies.json"
LOG_FILE = "changes.log"
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return []

def save_companies(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

def append_log(message: str) -> None:
    """Keep your original single-line logger for arbitrary messages."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} - {message}\n")

def log_changes(old_data: List[Dict[str, Any]],
                new_data: List[Dict[str, Any]],
                key: str = "symbol") -> Dict[str, Any]:
    """
    Compute set-level diffs and field-level updates, write one JSON line to LOG_FILE,
    and return a concise summary for the API response.
    """
    old_map = {r[key]: r for r in old_data if key in r}
    new_map = {r[key]: r for r in new_data if key in r}

    old_keys = set(old_map)
    new_keys = set(new_map)

    added   = sorted(new_keys - old_keys)
    removed = sorted(old_keys - new_keys)

    # Detect updated: present in both, but any field differs
    updated = []
    updated_detail = {}  # symbol -> {field: {"old":..., "new":...}}
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
            # If you want to ignore noisy fields, drop them here, e.g.:
            # for noisy in ("last_updated",): changes.pop(noisy, None)
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

    # Return a compact summary you can surface in the response
    return {"added": added, "removed": removed, "updated": updated}

# polite, non-default UA + retries
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
    # primary request
    r = sess.get(WIKI_URL, timeout=15)
    # fallback to "printable" view if blocked
    if r.status_code in (403, 429):
        alt = "https://en.wikipedia.org/w/index.php?title=List_of_S%26P_500_companies&printable=yes"
        r = sess.get(alt, timeout=15)
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Failed to fetch S&P 500 list: HTTP {r.status_code}")
    return r.text

def fetch_sp500_list() -> List[Dict[str, Any]]:
    """
    Return records with fields:
    symbol, name, sector, sub_sector, headquarters, date_added
    """
    try:
        html = _fetch_wikipedia_html()
        tables = pd.read_html(StringIO(html), match="Symbol")  # <-- wrap in StringIO
        # tables = pd.read_html(html, match="Symbol")
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Failed to parse S&P 500 table")
        raise HTTPException(status_code=502, detail=f"Failed to parse S&P 500 list: {e}")

    if not tables:
        raise HTTPException(status_code=502, detail="No tables found at source")

    df = tables[0]
    col_map = { _norm_col(c): c for c in df.columns }
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

    # dedupe & sort
    seen = set()
    uniq = []
    for r in records:
        k = r.get("symbol")
        if k and k not in seen:
            uniq.append(r); seen.add(k)
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

        old_data = load_data()  # was get_companies()
        logging.info("Loaded %d old companies", len(old_data))

        # ⬇️ use live S&P 500 list
        new_data = fetch_sp500_list()
        logging.info("Fetched %d new companies", len(new_data))

        save_companies(new_data)
        logging.info("Saved companies.json")

        changes = log_changes(old_data, new_data, key="symbol")

        return {"status": "success", "count": len(new_data), **changes}

    except Exception as e:
        logging.exception("Refresh failed")
        raise HTTPException(status_code=500, detail=f"Refresh failed: {e}")

@app.get("/", response_class=HTMLResponse)
def companies_ui():
    return """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>S&P 500 Updater</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    /* Simple table scroll shadow */
    .scroll-shadow {
      mask-image: linear-gradient(to bottom, transparent, black 16px, black calc(100% - 16px), transparent);
    }
  </style>
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
        <button id="refreshBtn" class="inline-flex items-center gap-2 rounded-xl bg-gray-900 px-4 py-2 text-white shadow hover:bg-gray-800 active:scale-[0.99] transition disabled:opacity-60 disabled:cursor-not-allowed" onclick="refreshData()">
          <span id="refreshSpinner" class="hidden h-4 w-4 border-2 border-white/30 border-t-white rounded-full animate-spin"></span>
          <span>Refresh Data</span>
        </button>
        <a href="/docs" class="text-sm text-gray-500 hover:text-gray-900 underline decoration-dotted">API Docs</a>
      </div>
    </div>
  </header>

  <main class="mx-auto max-w-6xl px-4 py-6">
    <div id="notice" class="hidden mb-4 rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-emerald-900"></div>
    <div id="error" class="hidden mb-4 rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-rose-900"></div>

    <!-- Toolbar -->
    <div class="mb-4 flex flex-col md:flex-row items-stretch md:items-center justify-between gap-3">
      <div class="relative w-full md:w-96">
        <input id="searchInput" type="search" placeholder="Search by symbol, name, sector…" class="w-full rounded-xl border border-gray-300 bg-white px-4 py-2 pl-10 outline-none focus:ring-2 focus:ring-gray-900/10">
        <svg class="absolute left-3 top-2.5 h-5 w-5 text-gray-400" viewBox="0 0 24 24" fill="none"><path d="M21 21l-4.3-4.3M10.5 18a7.5 7.5 0 1 1 0-15 7.5 7.5 0 0 1 0 15Z" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>
      </div>
      <div class="text-sm text-gray-500">
        <span id="countLabel">0</span> companies
      </div>
    </div>

    <!-- Views -->
    <div id="listView" class="rounded-2xl border bg-white shadow-sm overflow-hidden">
      <div class="scroll-shadow max-h-[65vh] overflow-auto">
        <table class="min-w-full text-sm">
          <thead class="sticky top-0 bg-gray-50 text-gray-600">
            <tr>
              <th class="px-4 py-3 text-left font-medium">Symbol</th>
              <th class="px-4 py-3 text-left font-medium">Name</th>
              <th class="px-4 py-3 text-left font-medium">Sector</th>
              <th class="px-4 py-3 text-left font-medium">Sub-Sector</th>
              <th class="px-4 py-3 text-left font-medium">HQ</th>
              <th class="px-4 py-3 text-left font-medium">Date Added</th>
            </tr>
          </thead>
          <tbody id="companiesTbody" class="divide-y">
            <!-- rows injected here -->
          </tbody>
        </table>
      </div>
    </div>

    <div id="detailView" class="hidden">
      <div class="mb-4">
        <button class="text-sm text-gray-600 hover:text-gray-900 underline decoration-dotted" onclick="showList()">← Back to list</button>
      </div>
      <div id="detailCard" class="rounded-2xl border bg-white shadow-sm p-6">
        <!-- detail injected here -->
      </div>
    </div>
  </main>

  <footer class="mx-auto max-w-6xl px-4 pb-8 pt-2 text-xs text-gray-500">
    Served by FastAPI • <code>/companies</code>, <code>/companies/{symbol}</code>, <code>/refresh</code>
  </footer>

<script>
  const els = {
    listView: document.getElementById('listView'),
    detailView: document.getElementById('detailView'),
    tbody: document.getElementById('companiesTbody'),
    count: document.getElementById('countLabel'),
    search: document.getElementById('searchInput'),
    refreshBtn: document.getElementById('refreshBtn'),
    refreshSpinner: document.getElementById('refreshSpinner'),
    notice: document.getElementById('notice'),
    error: document.getElementById('error'),
    detailCard: document.getElementById('detailCard'),
  };

  let companies = [];

  function setNotice(msg) {
    els.notice.textContent = msg;
    els.notice.classList.remove('hidden');
    setTimeout(() => els.notice.classList.add('hidden'), 6000);
  }

  function setError(msg) {
    els.error.textContent = msg;
    els.error.classList.remove('hidden');
    setTimeout(() => els.error.classList.add('hidden'), 8000);
  }

  function showList() {
    els.detailView.classList.add('hidden');
    els.listView.classList.remove('hidden');
    history.replaceState(null, '', location.pathname);
  }

  function showDetail(symbol) {
    els.listView.classList.add('hidden');
    els.detailView.classList.remove('hidden');
    history.replaceState(null, '', `?symbol=${encodeURIComponent(symbol)}`);
  }

  function renderRows(rows) {
    els.tbody.innerHTML = rows.map(c => `
      <tr class="hover:bg-gray-50">
        <td class="px-4 py-3">
          <button class="text-gray-900 font-semibold hover:underline" onclick="loadCompany('${c.symbol}')">${c.symbol || ''}</button>
        </td>
        <td class="px-4 py-3">${escapeHtml(c.name || '')}</td>
        <td class="px-4 py-3">${escapeHtml(c.sector || '')}</td>
        <td class="px-4 py-3">${escapeHtml(c.sub_sector || '')}</td>
        <td class="px-4 py-3">${escapeHtml(c.headquarters || '')}</td>
        <td class="px-4 py-3">${escapeHtml(c.date_added || '')}</td>
      </tr>
    `).join('');
    els.count.textContent = rows.length.toLocaleString();
  }

  function filterRows() {
    const q = els.search.value.trim().toLowerCase();
    if (!q) return renderRows(companies);
    const filtered = companies.filter(c =>
      (c.symbol || '').toLowerCase().includes(q) ||
      (c.name || '').toLowerCase().includes(q) ||
      (c.sector || '').toLowerCase().includes(q) ||
      (c.sub_sector || '').toLowerCase().includes(q) ||
      (c.headquarters || '').toLowerCase().includes(q)
    );
    renderRows(filtered);
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[m]));
  }

  async function loadCompanies() {
    try {
      const res = await fetch('/companies');
      if (!res.ok) throw new Error('Failed to load companies');
      companies = await res.json();
      renderRows(companies);
    } catch (e) {
      setError(e.message || 'Error loading companies');
    }
  }

  async function loadCompany(symbol) {
    try {
      const res = await fetch('/companies/' + encodeURIComponent(symbol));
      if (!res.ok) throw new Error('Company not found');
      const c = await res.json();
      showDetail(symbol);
      els.detailCard.innerHTML = `
        <div class="flex items-center justify-between">
          <div>
            <div class="text-2xl font-semibold">${escapeHtml(c.symbol || '')}</div>
            <div class="text-gray-500">${escapeHtml(c.name || '')}</div>
          </div>
          <div>
            <span class="rounded-full bg-gray-100 px-3 py-1 text-xs text-gray-700">${escapeHtml(c.sector || '—')}</span>
          </div>
        </div>
        <dl class="mt-6 grid grid-cols-1 md:grid-cols-2 gap-4">
          <div><dt class="text-xs uppercase tracking-wide text-gray-500">Sub-Sector</dt><dd class="text-sm">${escapeHtml(c.sub_sector || '—')}</dd></div>
          <div><dt class="text-xs uppercase tracking-wide text-gray-500">HQ</dt><dd class="text-sm">${escapeHtml(c.headquarters || '—')}</dd></div>
          <div><dt class="text-xs uppercase tracking-wide text-gray-500">Date Added</dt><dd class="text-sm">${escapeHtml(c.date_added || '—')}</dd></div>
        </dl>
      `;
    } catch (e) {
      setError(e.message || 'Error loading company');
      showList();
    }
  }

  async function refreshData() {
    els.refreshBtn.disabled = true;
    els.refreshSpinner.classList.remove('hidden');
    try {
      const res = await fetch('/refresh', { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.detail || 'Refresh failed');

      // Show a compact summary of changes
      const added = (data.added || []).slice(0, 10);
      const removed = (data.removed || []).slice(0, 10);
      const updated = (data.updated || []).slice(0, 10);

      let msg = `Refreshed ${data.count?.toLocaleString?.() ?? data.count} companies.`;
      if (added.length)  msg += ` Added: ${added.join(', ')}${data.added.length > 10 ? '…' : ''}.`;
      if (removed.length) msg += ` Removed: ${removed.join(', ')}${data.removed.length > 10 ? '…' : ''}.`;
      if (updated.length) msg += ` Updated: ${updated.join(', ')}${data.updated.length > 10 ? '…' : ''}.`;

      setNotice(msg);

      // Reload list
      await loadCompanies();
      showList();
    } catch (e) {
      setError(e.message || 'Refresh failed');
    } finally {
      els.refreshSpinner.classList.add('hidden');
      els.refreshBtn.disabled = false;
    }
  }

  // Search as you type (debounced a bit)
  let t;
  els.search.addEventListener('input', () => {
    clearTimeout(t);
    t = setTimeout(filterRows, 120);
  });

  // Support deep link like /?symbol=MSFT
  function checkQuery() {
    const params = new URLSearchParams(location.search);
    const sym = params.get('symbol');
    if (sym) loadCompany(sym);
  }

  // Boot
  loadCompanies().then(checkQuery);
</script>
</body>
</html>
"""
