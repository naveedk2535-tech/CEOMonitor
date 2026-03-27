import os
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from threading import Lock
from html import unescape
from functools import wraps
import re
import time

import requests
from flask import Flask, render_template, jsonify, request, redirect, url_for, session

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

FRED_API_KEY = os.getenv("FRED_API_KEY")

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "banking-exec-dashboard-2026-secret-key")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Auth credentials
# ---------------------------------------------------------------------------
AUTH_USERNAME = "admin"
AUTH_PASSWORD = "admin1"

# ---------------------------------------------------------------------------
# FRED series definitions
# ---------------------------------------------------------------------------
SERIES = {
    "central_banks": {
        "DFEDTARU":          "Fed Funds (Target Upper)",
        "IUDSOIA":           "Bank of England (SONIA)",
        "ECB_DFR":           "ECB Deposit Rate",
        "IR3TIB01JPM156N":   "Japan Interbank Rate",
        "IR3TIB01CHM156N":   "Swiss Interbank Rate",
        "IR3TIB01AUM156N":   "Australia Interbank Rate",
        "IRSTCI01CAM156N":   "Bank of Canada Rate",
        "IR3TIB01CNM156N":   "China Interbank Rate",
    },
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
    "global_bonds_10y": {
        "DGS10":             "US 10Y",
        "IRLTLT01GBM156N":   "UK 10Y",
        "IRLTLT01DEM156N":   "Germany 10Y",
        "IRLTLT01FRM156N":   "France 10Y",
        "IRLTLT01JPM156N":   "Japan 10Y",
        "IRLTLT01CAM156N":   "Canada 10Y",
        "IRLTLT01AUM156N":   "Australia 10Y",
        "IRLTLT01ITM156N":   "Italy 10Y",
        "IRLTLT01ESM156N":   "Spain 10Y",
        "IRLTLT01CHM156N":   "Switzerland 10Y",
        "IRLTLT01KRM156N":   "South Korea 10Y",
        "IRLTLT01NZM156N":   "New Zealand 10Y",
    },
    "spreads": {
        "T10Y2Y":          "2s10s Spread",
        "SOFR":            "SOFR",
        "BAMLC0A0CM":      "IG Corporate Spread",
        "BAMLH0A0HYM2":    "HY Corporate Spread",
        "TEDRATE":         "TED Spread (Discontinued)",
        "DPRIME":          "US Prime Rate",
        "MORTGAGE30US":    "US 30-Year Mortgage",
    },
    "uk_rates": {
        "IR3TIB01GBM156N": "UK 3-Month Interbank",
        "INTDSRGBM193N":   "UK Short-Term Rate",
        "IRLTLT01GBQ156N": "UK Long-Term Govt Bond (Quarterly)",
    },
    "fx_rates": {
        "DEXUSUK":  "GBP/USD",
        "DEXUSEU":  "EUR/USD",
        "DEXJPUS":  "USD/JPY",
    },
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
        {"name": "BBC — Business", "url": "http://feeds.bbci.co.uk/news/business/rss.xml", "icon": "📰"},
        {"name": "The Guardian — Business", "url": "https://www.theguardian.com/uk/business/rss", "icon": "📊"},
    ],
    "central_banking": [
        {"name": "Bank of England — News", "url": "https://www.bankofengland.co.uk/rss/news", "icon": "🏛️"},
        {"name": "FCA — News", "url": "https://www.fca.org.uk/news/rss.xml", "icon": "⚖️"},
    ],
    "global_markets": [
        {"name": "CNBC — Finance", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664", "icon": "📈"},
        {"name": "Bloomberg — Markets", "url": "https://feeds.bloomberg.com/markets/news.rss", "icon": "💹"},
    ],
}

# ---------------------------------------------------------------------------
# Time-based cache (no APScheduler needed)
# ---------------------------------------------------------------------------
_cache: dict = {
    "rates": {},
    "news": {},
    "last_updated": None,
    "news_last_updated": None,
    "rates_fetched_at": 0,
    "news_fetched_at": 0,
}
_lock = Lock()

RATES_CACHE_SECONDS = 1800   # 30 minutes
NEWS_CACHE_SECONDS = 900     # 15 minutes


def _fetch_fred_series(series_id: str) -> dict:
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


def _fetch_ecb_rate(rate_type: str = "DFR") -> dict:
    """Fetch live ECB rate from ECB Statistical Data Warehouse.
    rate_type: DFR (deposit facility), MRR_FR (main refinancing), MLF_FR (marginal lending)
    """
    url = f"https://data-api.ecb.europa.eu/service/data/FM/D.U2.EUR.4F.KR.{rate_type}.LEV?lastNObservations=1&format=csvdata"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        lines = resp.text.strip().split("\n")
        if len(lines) >= 2:
            fields = lines[-1].split(",")
            value = float(fields[9])
            date = fields[8]
            return {"value": value, "date": date, "direction": None, "source": "ECB SDW"}
    except Exception as exc:
        logger.warning("Failed to fetch ECB %s: %s", rate_type, exc)
    return {"value": None, "date": None, "direction": None}


# Manual overrides for rates where FRED is hopelessly stale.
# These are updated when central banks announce changes.
# Format: series_id -> {value, date, label, source}
MANUAL_OVERRIDES = {
    # BoJ raised to 0.50% on Jan 24, 2025
    "BOJ_POLICY": {"value": 0.50, "date": "2025-01-24", "label": "Bank of Japan Policy Rate", "source": "BoJ (manual)"},
    # SNB cut to 0.25% on March 20, 2025
    "SNB_POLICY": {"value": 0.25, "date": "2025-03-20", "label": "Swiss National Bank Rate", "source": "SNB (manual)"},
}

# Staleness threshold: FRED dates older than this many days get flagged
STALE_THRESHOLD_DAYS = 60


def _check_staleness(date_str: str) -> str:
    """Return 'fresh', 'aging', or 'stale' based on date age."""
    if not date_str:
        return "stale"
    try:
        obs_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        age_days = (datetime.now(timezone.utc) - obs_date).days
        if age_days <= 30:
            return "fresh"
        elif age_days <= STALE_THRESHOLD_DAYS:
            return "aging"
        else:
            return "stale"
    except ValueError:
        return "stale"


def _fetch_fred_history(series_id: str, months: int = 12) -> list[dict]:
    """Fetch FRED observations for a series over the past N months."""
    url = "https://api.stlouisfed.org/fred/series/observations"
    start_date = (datetime.now(timezone.utc) - timedelta(days=months * 30)).strftime("%Y-%m-%d")
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "asc",
        "observation_start": start_date,
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        observations = resp.json().get("observations", [])
        result = []
        for obs in observations:
            try:
                val = float(obs["value"])
            except (ValueError, TypeError):
                continue
            result.append({"date": obs["date"], "value": val})
        return result
    except Exception as exc:
        logger.warning("Failed to fetch history for %s: %s", series_id, exc)
        return []


def _strip_html(text: str) -> str:
    clean = re.sub(r"<[^>]+>", "", text)
    return unescape(clean).strip()


def _parse_rss(url: str, max_items: int = 8) -> list[dict]:
    try:
        headers = {
            "User-Agent": "BankingDashboard/1.0 (Financial Dashboard)",
            "Accept": "application/rss+xml, application/xml, text/xml",
        }
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        items = []
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        for item in root.findall(".//item")[:max_items]:
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            pub_date = item.findtext("pubDate", "").strip()
            desc = _strip_html(item.findtext("description", ""))
            if len(desc) > 200:
                desc = desc[:200] + "…"
            if title:
                items.append({"title": title, "link": link, "date": pub_date, "summary": desc})

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
                    items.append({"title": title, "link": link, "date": pub_date or "", "summary": desc})

        return items
    except Exception as exc:
        logger.warning("Failed to fetch RSS %s: %s", url, exc)
        return []


def _ensure_rates():
    """Refresh rates if cache is stale."""
    now = time.time()
    if now - _cache["rates_fetched_at"] < RATES_CACHE_SECONDS and _cache["rates"]:
        return
    logger.info("Refreshing rate data…")
    rates = {}
    for series_id, label in ALL_SERIES.items():
        if series_id == "ECB_DFR":
            # Fetch live from ECB Statistical Data Warehouse
            data = _fetch_ecb_rate("DFR")
            if data["value"] is None:
                # Fallback to FRED ECBDFR
                data = _fetch_fred_series("ECBDFR")
            data["label"] = label
            data["source"] = data.get("source", "FRED")
        else:
            data = _fetch_fred_series(series_id)
            data["label"] = label
            data["source"] = "FRED"

        # Add staleness flag
        data["freshness"] = _check_staleness(data.get("date"))
        rates[series_id] = data

    # Apply manual overrides for hopelessly stale FRED series
    # Map interbank proxies to their policy rate overrides
    override_map = {
        "IR3TIB01JPM156N": "BOJ_POLICY",
        "IR3TIB01CHM156N": "SNB_POLICY",
    }
    for fred_id, override_key in override_map.items():
        if fred_id in rates and override_key in MANUAL_OVERRIDES:
            fred_data = rates[fred_id]
            override = MANUAL_OVERRIDES[override_key]
            # Add the official policy rate as a separate display field
            fred_data["official_rate"] = override["value"]
            fred_data["official_date"] = override["date"]
            fred_data["official_source"] = override["source"]
            fred_data["official_label"] = override["label"]

    with _lock:
        _cache["rates"] = rates
        _cache["rates_fetched_at"] = now
        _cache["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info("Rate data refresh complete.")


def _ensure_news():
    """Refresh news if cache is stale."""
    now = time.time()
    if now - _cache["news_fetched_at"] < NEWS_CACHE_SECONDS and _cache["news"]:
        return
    logger.info("Refreshing news feeds…")
    news = {}
    for category, feeds in NEWS_FEEDS.items():
        category_items = []
        for feed in feeds:
            articles = _parse_rss(feed["url"])
            for article in articles:
                article["source"] = feed["name"]
                article["icon"] = feed["icon"]
            category_items.extend(articles)
        news[category] = category_items

    with _lock:
        _cache["news"] = news
        _cache["news_fetched_at"] = now
        _cache["news_last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info("News refresh complete.")


# ---------------------------------------------------------------------------
# Auth decorator
# ---------------------------------------------------------------------------
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if username == AUTH_USERNAME and password == AUTH_PASSWORD:
            session["logged_in"] = True
            return redirect(url_for("dashboard"))
        else:
            error = "Invalid credentials. Please try again."
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.pop("logged_in", None)
    return redirect(url_for("login"))


@app.route("/")
@login_required
def dashboard():
    _ensure_rates()
    _ensure_news()

    with _lock:
        rates = dict(_cache["rates"])
        news = dict(_cache["news"])
        last_updated = _cache["last_updated"]
        news_last_updated = _cache["news_last_updated"]

    grouped: dict[str, list[dict]] = {}
    for group_key, series_map in SERIES.items():
        items = []
        for sid, label in series_map.items():
            entry = rates.get(sid, {"value": None, "date": None, "direction": None})
            items.append({"id": sid, "label": label, **entry})
        grouped[group_key] = items

    treasury_order = list(SERIES["us_treasuries"].keys())
    yc_labels = [SERIES["us_treasuries"][s] for s in treasury_order]
    yc_values = []
    for sid in treasury_order:
        v = rates.get(sid, {}).get("value")
        yc_values.append(v if v is not None else "null")

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

    # Compute Bund-BTP spread (Italy 10Y minus Germany 10Y)
    italy_10y = rates.get("IRLTLT01ITM156N", {}).get("value")
    germany_10y = rates.get("IRLTLT01DEM156N", {}).get("value")
    if italy_10y is not None and germany_10y is not None:
        bund_btp_spread = round(italy_10y - germany_10y, 4)
    else:
        bund_btp_spread = None

    return render_template(
        "dashboard.html",
        grouped=grouped,
        yc_labels=yc_labels,
        yc_values=yc_values,
        health=health,
        news=news,
        bund_btp_spread=bund_btp_spread,
        last_updated=last_updated,
        news_last_updated=news_last_updated,
    )


@app.route("/api/rates")
@login_required
def api_rates():
    _ensure_rates()
    with _lock:
        return jsonify({"rates": _cache["rates"], "last_updated": _cache["last_updated"]})


@app.route("/api/news")
@login_required
def api_news():
    _ensure_news()
    with _lock:
        return jsonify({"news": _cache["news"], "news_last_updated": _cache["news_last_updated"]})


@app.route("/api/history/<series_id>")
@login_required
def api_history(series_id):
    """Return historical FRED observations for a series over N months."""
    allowed_months = {3, 6, 12}
    try:
        months = int(request.args.get("months", 12))
    except (ValueError, TypeError):
        months = 12
    if months not in allowed_months:
        months = 12

    label = ALL_SERIES.get(series_id, series_id)
    # ECB_DFR is not a real FRED series — use ECBDFR as fallback for history
    fetch_id = "ECBDFR" if series_id == "ECB_DFR" else series_id
    data = _fetch_fred_history(fetch_id, months)

    return jsonify({
        "series_id": series_id,
        "label": label,
        "months": months,
        "data": data,
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
