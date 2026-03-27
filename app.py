import os
import logging
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from threading import Lock
from html import unescape
import re

import requests
from flask import Flask, render_template, jsonify
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()
FRED_API_KEY = os.getenv("FRED_API_KEY")

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FRED series definitions
# ---------------------------------------------------------------------------
SERIES = {
    # Central bank policy rates
    "central_banks": {
        "FEDFUNDS":          "Fed Funds Rate",
        "IUDSOIA":           "Bank of England (SONIA Proxy)",
        "ECBMLFR":           "ECB Main Refinancing Rate",
        "INTDSRJPM193N":     "Bank of Japan Rate",
    },
    # US Treasury yields
    "us_treasuries": {
        "DGS1MO":  "1 Month",
        "DGS3MO":  "3 Month",
        "DGS6MO":  "6 Month",
        "DGS1":    "1 Year",
        "DGS2":    "2 Year",
        "DGS5":    "5 Year",
        "DGS7":    "7 Year",
        "DGS10":   "10 Year",
        "DGS20":   "20 Year",
        "DGS30":   "30 Year",
    },
    # UK & global bonds
    "bonds": {
        "IRLTLT01GBM156N": "UK Long-Term Gilt",
        "MORTGAGE30US":    "US 30-Year Mortgage",
    },
    # Spreads & money markets
    "spreads": {
        "T10Y2Y":          "2s10s Spread",
        "SOFR":            "SOFR",
        "BAMLC0A0CM":      "IG Corporate Spread",
        "BAMLH0A0HYM2":    "HY Corporate Spread",
        "TEDRATE":         "TED Spread",
        "DPRIME":          "US Prime Rate",
    },
    # UK-specific rates
    "uk_rates": {
        "IR3TIB01GBM156N": "UK 3-Month Interbank",
        "INTDSRGBM193N":   "UK Short-Term Rate",
        "IRLTLT01GBQ156N": "UK Long-Term Govt Bond (Quarterly)",
    },
    # FX rates
    "fx_rates": {
        "DEXUSUK":  "GBP/USD",
        "DEXUSEU":  "EUR/USD",
        "DEXJPUS":  "USD/JPY",
    },
    # Inflation
    "inflation": {
        "CPIAUCSL":        "US CPI (Index)",
        "GBRCPIALLMINMEI": "UK CPI (Index)",
        "T10YIE":          "US 10Y Breakeven Inflation",
    },
}

ALL_SERIES: dict[str, str] = {}
for group in SERIES.values():
    ALL_SERIES.update(group)

# ---------------------------------------------------------------------------
# RSS News Feed definitions
# ---------------------------------------------------------------------------
NEWS_FEEDS = {
    "uk_finance": [
        {
            "name": "FT — UK Finance",
            "url": "https://www.ft.com/rss/home/uk",
            "icon": "📰",
        },
        {
            "name": "BBC — Business",
            "url": "http://feeds.bbci.co.uk/news/business/rss.xml",
            "icon": "🔵",
        },
        {
            "name": "The Guardian — Business",
            "url": "https://www.theguardian.com/uk/business/rss",
            "icon": "📊",
        },
    ],
    "central_banking": [
        {
            "name": "Bank of England — News",
            "url": "https://www.bankofengland.co.uk/rss/news",
            "icon": "🏛️",
        },
        {
            "name": "FCA — News",
            "url": "https://www.fca.org.uk/news/rss.xml",
            "icon": "⚖️",
        },
    ],
    "global_markets": [
        {
            "name": "CNBC — Finance",
            "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
            "icon": "📈",
        },
        {
            "name": "Bloomberg — Markets",
            "url": "https://feeds.bloomberg.com/markets/news.rss",
            "icon": "💹",
        },
    ],
}

# Search queries for UBL UK mentions
UBL_SEARCH_QUERIES = [
    "United Bank Limited UK",
    "UBL UK",
    "ubluk.com",
]

# ---------------------------------------------------------------------------
# Data cache
# ---------------------------------------------------------------------------
_cache: dict = {
    "rates": {},
    "news": {},
    "ubl_mentions": [],
    "last_updated": None,
    "news_last_updated": None,
}
_lock = Lock()


def _fetch_fred_series(series_id: str) -> dict:
    """Fetch the latest observation for a single FRED series."""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 2,
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        observations = resp.json().get("observations", [])
        if not observations:
            return {"value": None, "date": None, "direction": None}

        latest = observations[0]
        value = latest.get("value", ".")
        date = latest.get("date", "")

        try:
            value_float = float(value)
        except (ValueError, TypeError):
            value_float = None

        direction = None
        if len(observations) > 1 and value_float is not None:
            try:
                prev = float(observations[1]["value"])
                if value_float < prev:
                    direction = "down"
                elif value_float > prev:
                    direction = "up"
                else:
                    direction = "flat"
            except (ValueError, TypeError, KeyError):
                pass

        return {"value": value_float, "date": date, "direction": direction}
    except Exception as exc:
        logger.warning("Failed to fetch %s: %s", series_id, exc)
        return {"value": None, "date": None, "direction": None}


def _strip_html(text: str) -> str:
    """Remove HTML tags from a string."""
    clean = re.sub(r"<[^>]+>", "", text)
    return unescape(clean).strip()


def _parse_rss(url: str, max_items: int = 8) -> list[dict]:
    """Fetch and parse an RSS feed, returning a list of articles."""
    try:
        headers = {
            "User-Agent": "CEOMonitor/1.0 (Financial Dashboard)",
            "Accept": "application/rss+xml, application/xml, text/xml",
        }
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)

        # Handle both RSS and Atom formats
        items = []
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        # RSS 2.0
        for item in root.findall(".//item")[:max_items]:
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            pub_date = item.findtext("pubDate", "").strip()
            desc = _strip_html(item.findtext("description", ""))
            if len(desc) > 200:
                desc = desc[:200] + "…"
            if title:
                items.append({
                    "title": title,
                    "link": link,
                    "date": pub_date,
                    "summary": desc,
                })

        # Atom format fallback
        if not items:
            for entry in root.findall("atom:entry", ns)[:max_items]:
                title = entry.findtext("atom:title", "", ns).strip()
                link_el = entry.find("atom:link", ns)
                link = link_el.get("href", "") if link_el is not None else ""
                pub_date = entry.findtext("atom:published", "", ns) or entry.findtext("atom:updated", "", ns)
                desc = _strip_html(entry.findtext("atom:summary", "", ns) or entry.findtext("atom:content", "", ns) or "")
                if len(desc) > 200:
                    desc = desc[:200] + "…"
                if title:
                    items.append({
                        "title": title,
                        "link": link,
                        "date": pub_date or "",
                        "summary": desc,
                    })

        return items
    except Exception as exc:
        logger.warning("Failed to fetch RSS %s: %s", url, exc)
        return []


def _search_google_news(query: str, max_items: int = 5) -> list[dict]:
    """Search Google News RSS for mentions of a query."""
    try:
        encoded = requests.utils.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded}&hl=en-GB&gl=GB&ceid=GB:en"
        return _parse_rss(url, max_items)
    except Exception as exc:
        logger.warning("Failed Google News search for %s: %s", query, exc)
        return []


def refresh_data():
    """Fetch all FRED series and update cache."""
    logger.info("Refreshing FRED data…")
    rates: dict = {}
    for series_id, label in ALL_SERIES.items():
        data = _fetch_fred_series(series_id)
        data["label"] = label
        rates[series_id] = data

    with _lock:
        _cache["rates"] = rates
        _cache["last_updated"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    logger.info("FRED data refresh complete.")


def refresh_news():
    """Fetch all news feeds and UBL mentions."""
    logger.info("Refreshing news feeds…")
    news: dict = {}
    for category, feeds in NEWS_FEEDS.items():
        category_items = []
        for feed in feeds:
            articles = _parse_rss(feed["url"])
            for article in articles:
                article["source"] = feed["name"]
                article["icon"] = feed["icon"]
            category_items.extend(articles)
        news[category] = category_items

    # UBL UK mentions from Google News
    ubl_mentions = []
    seen_titles = set()
    for query in UBL_SEARCH_QUERIES:
        results = _search_google_news(query)
        for item in results:
            title_hash = hashlib.md5(item["title"].encode()).hexdigest()
            if title_hash not in seen_titles:
                seen_titles.add(title_hash)
                item["query"] = query
                ubl_mentions.append(item)

    with _lock:
        _cache["news"] = news
        _cache["ubl_mentions"] = ubl_mentions
        _cache["news_last_updated"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    logger.info("News refresh complete — %d UBL mentions found.", len(ubl_mentions))


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------
scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(refresh_data, "interval", minutes=30, id="refresh_rates")
scheduler.add_job(refresh_news, "interval", minutes=15, id="refresh_news")
scheduler.start()

# Initial fetch
refresh_data()
refresh_news()

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def dashboard():
    with _lock:
        rates = dict(_cache["rates"])
        news = dict(_cache["news"])
        ubl_mentions = list(_cache["ubl_mentions"])
        last_updated = _cache["last_updated"]
        news_last_updated = _cache["news_last_updated"]

    # Build grouped rate data
    grouped: dict[str, list[dict]] = {}
    for group_key, series_map in SERIES.items():
        items = []
        for sid, label in series_map.items():
            entry = rates.get(sid, {"value": None, "date": None, "direction": None})
            items.append({"id": sid, "label": label, **entry})
        grouped[group_key] = items

    # Treasury yield-curve data for chart
    treasury_order = list(SERIES["us_treasuries"].keys())
    yc_labels = [SERIES["us_treasuries"][s] for s in treasury_order]
    yc_values = []
    for sid in treasury_order:
        v = rates.get(sid, {}).get("value")
        yc_values.append(v if v is not None else "null")

    # Health indicators
    spread_2s10s = rates.get("T10Y2Y", {}).get("value")
    hy_spread = rates.get("BAMLH0A0HYM2", {}).get("value")
    ted_spread = rates.get("TEDRATE", {}).get("value")

    health = {
        "yield_curve": _traffic(spread_2s10s, [(0, "red"), (None, "green")]),
        "credit_stress": _traffic(hy_spread, [(4, "green"), (5, "yellow"), (None, "red")]),
        "liquidity": _traffic(ted_spread, [(0.5, "green"), (1, "yellow"), (None, "red")]),
        "spread_2s10s": spread_2s10s,
        "hy_spread": hy_spread,
        "ted_spread": ted_spread,
    }

    return render_template(
        "dashboard.html",
        grouped=grouped,
        yc_labels=yc_labels,
        yc_values=yc_values,
        health=health,
        news=news,
        ubl_mentions=ubl_mentions,
        last_updated=last_updated,
        news_last_updated=news_last_updated,
    )


@app.route("/api/rates")
def api_rates():
    with _lock:
        return jsonify({
            "rates": _cache["rates"],
            "last_updated": _cache["last_updated"],
        })


@app.route("/api/news")
def api_news():
    with _lock:
        return jsonify({
            "news": _cache["news"],
            "ubl_mentions": _cache["ubl_mentions"],
            "news_last_updated": _cache["news_last_updated"],
        })


def _traffic(value, thresholds):
    if value is None:
        return "grey"
    for bound, colour in thresholds:
        if bound is None:
            return colour
        if value < bound:
            return colour
    return "grey"


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, port=5000, use_reloader=False)
