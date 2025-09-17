import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import yfinance as yf


def _now_iso() -> str:
    """Return current UTC time as ISO 8601 string (no microseconds)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fetch_financials(symbols: List[str], pause_sec: float = 0.2) -> Dict[str, Optional[dict]]:
    """
    Fetch a compact set of financial fields from Yahoo Finance via yfinance.
    Returns a dict: {SYMBOL: financial_data_dict or None on failure}.
    """
    out: Dict[str, Optional[dict]] = {}
    if not symbols:
        return out

    CHUNK = 50  # yfinance can batch multiple symbols, keep chunk size safe
    for i in range(0, len(symbols), CHUNK):
        chunk = [s.upper() for s in symbols[i:i + CHUNK]]
        tickers = yf.Tickers(" ".join(chunk))
        for sym in chunk:
            try:
                t = tickers.tickers[sym]
                fi = getattr(t, "fast_info", None)
                info = t.info  # richer but slower fallback

                rec = {
                    "as_of": _now_iso(),
                    "price": (getattr(fi, "last_price", None) if fi else None) or info.get("currentPrice"),
                    "market_cap": (getattr(fi, "market_cap", None) if fi else None) or info.get("marketCap"),
                    "trailing_pe": info.get("trailingPE"),
                    "forward_pe": info.get("forwardPE"),
                    "dividend_yield": info.get("dividendYield"),  # decimal e.g. 0.0123 = 1.23%
                    "beta": info.get("beta"),
                    "high_52w": info.get("fiftyTwoWeekHigh"),
                    "low_52w": info.get("fiftyTwoWeekLow"),
                    "shares_outstanding": info.get("sharesOutstanding"),
                }

                # If both price and market_cap are missing, consider this a failed fetch
                if rec["price"] is None and rec["market_cap"] is None:
                    out[sym] = None
                else:
                    out[sym] = rec

            except Exception:
                out[sym] = None

        # Pause a bit between chunks to avoid hammering Yahoo's servers
        time.sleep(pause_sec)

    return out
