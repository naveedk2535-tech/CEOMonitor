import os
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from threading import Lock
from html import unescape
from functools import wraps
import re
import time
import hashlib

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
    # central_banks is now rendered via CENTRAL_BANK_CARDS (multi-rate per bank)
    # These FRED series are still fetched individually for the cache
    "central_banks_fred": {
        "DFEDTARU":          "Fed Target Upper",
        "DFEDTARL":          "Fed Target Lower",
        "IUDSOIA":           "BoE SONIA",
        "ECBMRRFR":          "ECB Main Refi (FRED)",
        "IR3TIB01JPM156N":   "Japan 3M Interbank",
        "IR3TIB01CHM156N":   "Swiss 3M Interbank",
        "IR3TIB01AUM156N":   "Australia 3M Interbank",
        "IRSTCI01CAM156N":   "Bank of Canada Rate",
        "IR3TIB01CNM156N":   "China 3M Interbank",
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
    "commodities": {
        "DCOILBRENTEU":    "Brent Crude Oil ($/bbl)",
    },
    "uk_rates": {
        "IUDSOIA":           "UK SONIA (Overnight)",
        "IRSTCI01GBM156N":   "UK Short-Term Rate (Monthly)",
        "IRLTLT01GBM156N":   "UK Long-Term Gilt (Monthly)",
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

# Central bank cards: each bank shows multiple rates from multiple sources
CENTRAL_BANK_CARDS = [
    {
        "bank": "Federal Reserve (US)",
        "flag": "🇺🇸",
        "rates": [
            {"id": "DFEDTARU", "label": "Target Upper Bound", "source": "FRED (daily)"},
            {"id": "DFEDTARL", "label": "Target Lower Bound", "source": "FRED (daily)"},
        ],
        "history_id": "DFEDTARU",
    },
    {
        "bank": "Bank of England",
        "flag": "🇬🇧",
        "rates": [
            {"id": "IUDSOIA", "label": "SONIA (Overnight)", "source": "FRED (daily)"},
        ],
        "note": "BoE Base Rate is closely tracked by SONIA. Check bankofengland.co.uk for official rate.",
        "history_id": "IUDSOIA",
    },
    {
        "bank": "European Central Bank",
        "flag": "🇪🇺",
        "rates": [
            {"id": "ECB_DFR", "label": "Deposit Facility Rate", "source": "ECB SDW (live)"},
            {"id": "ECB_MRR", "label": "Main Refinancing Rate", "source": "ECB SDW (live)"},
            {"id": "ECB_MLF", "label": "Marginal Lending Rate", "source": "ECB SDW (live)"},
        ],
        "history_id": "ECBDFR",
    },
    {
        "bank": "Bank of Japan",
        "flag": "🇯🇵",
        "rates": [
            {"id": "IR3TIB01JPM156N", "label": "3-Month Interbank", "source": "FRED (monthly)"},
        ],
        "note": "BoJ Policy Rate = 0.50% (raised Jan 24, 2025).",
        "history_id": "IR3TIB01JPM156N",
    },
    {
        "bank": "Swiss National Bank",
        "flag": "🇨🇭",
        "rates": [
            {"id": "IR3TIB01CHM156N", "label": "3-Month Interbank", "source": "FRED (monthly)"},
        ],
        "note": "SNB Policy Rate = 0.25% (cut Mar 20, 2025).",
        "history_id": "IR3TIB01CHM156N",
    },
    {
        "bank": "Reserve Bank of Australia",
        "flag": "🇦🇺",
        "rates": [
            {"id": "IR3TIB01AUM156N", "label": "3-Month Interbank", "source": "FRED (monthly)"},
        ],
        "note": "RBA Cash Rate = 4.10% (cut Feb 18, 2025).",
        "history_id": "IR3TIB01AUM156N",
    },
    {
        "bank": "Bank of Canada",
        "flag": "🇨🇦",
        "rates": [
            {"id": "IRSTCI01CAM156N", "label": "Short-Term Rate", "source": "FRED (monthly)"},
        ],
        "note": "BoC Policy Rate = 2.75% (cut Mar 12, 2025).",
        "history_id": "IRSTCI01CAM156N",
    },
    {
        "bank": "People's Bank of China",
        "flag": "🇨🇳",
        "rates": [
            {"id": "IR3TIB01CNM156N", "label": "3-Month Interbank", "source": "FRED (monthly)"},
        ],
        "note": "PBoC 1Y LPR = 3.10%, 7-day reverse repo = 1.50%.",
        "history_id": "IR3TIB01CNM156N",
    },
]

ALL_SERIES: dict[str, str] = {}
for group in SERIES.values():
    ALL_SERIES.update(group)

# Add ECB live series IDs to ALL_SERIES for label lookups
ALL_SERIES["ECB_DFR"] = "ECB Deposit Facility Rate"
ALL_SERIES["ECB_MRR"] = "ECB Main Refinancing Rate"
ALL_SERIES["ECB_MLF"] = "ECB Marginal Lending Rate"

# ---------------------------------------------------------------------------
# RSS News Feed definitions
# ---------------------------------------------------------------------------
NEWS_FEEDS = {
    "uk_finance": [
        {"name": "Google News — UK Finance", "url": "https://news.google.com/rss/search?q=UK+finance+economy+banking+interest+rates&hl=en-GB&gl=GB&ceid=GB:en", "icon": "🇬🇧"},
        {"name": "Google News — UK Economy", "url": "https://news.google.com/rss/search?q=UK+economy+inflation+Bank+of+England&hl=en-GB&gl=GB&ceid=GB:en", "icon": "📊"},
        {"name": "BBC — Business", "url": "http://feeds.bbci.co.uk/news/business/rss.xml", "icon": "📰"},
    ],
    "central_banking": [
        {"name": "Google News — UK Banking Regulation", "url": "https://news.google.com/rss/search?q=UK+bank+regulation+FCA+PRA+%22Bank+of+England%22&hl=en-GB&gl=GB&ceid=GB:en", "icon": "⚖️"},
        {"name": "Google News — UK Banking News", "url": "https://news.google.com/rss/search?q=UK+banking+sector+high+street+banks&hl=en-GB&gl=GB&ceid=GB:en", "icon": "🏦"},
        {"name": "Bank of England — News", "url": "https://www.bankofengland.co.uk/rss/news", "icon": "🏛️"},
        {"name": "FCA — News", "url": "https://www.fca.org.uk/news/rss.xml", "icon": "⚖️"},
    ],
    "global_markets": [
        {"name": "Google News — Global Markets", "url": "https://news.google.com/rss/search?q=global+markets+stocks+bonds+finance&hl=en-GB&gl=GB&ceid=GB:en", "icon": "🌍"},
        {"name": "Google News — Central Banks", "url": "https://news.google.com/rss/search?q=Federal+Reserve+ECB+central+bank+interest+rate&hl=en-GB&gl=GB&ceid=GB:en", "icon": "🏛️"},
        {"name": "CNBC — Finance", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664", "icon": "📈"},
    ],
}

# ---------------------------------------------------------------------------
# UBL UK Monitoring — Google News searches for bank mentions
# ---------------------------------------------------------------------------
UBL_SEARCH_QUERIES = [
    '"United Bank Limited" UK',
    '"UBL UK"',
    "site:ubluk.com",
    '"United Bank Limited" London',
    '"UBL" bank UK remittance',
]

# ---------------------------------------------------------------------------
# Executive Brief — structured intelligence sections
# ---------------------------------------------------------------------------
EXEC_BRIEF_MARKET_SERIES = {
    "DCOILBRENTEU": {"label": "Brent Crude Oil", "unit": "$", "suffix": "/bbl", "decimals": 2},
    "DEXUSUK":      {"label": "GBP / USD", "unit": "", "suffix": "", "decimals": 4},
}

# Calendar & upcoming events (manually maintained)
EXEC_CALENDAR = [
    {"date": "2026-04-06", "event": "FCA 'Targeted Support' gateway goes live; new suggestions model for retail financial decisions."},
    {"date": "2026-04-09", "event": "Deadline for HM Treasury consultation on the Appointed Representatives (AR) regime."},
    {"date": "2026-05-14", "event": "Next Bank of England Monetary Policy Report and rate decision."},
    {"date": "2026-06-01", "event": "Katharine Braddick to formally succeed Sam Woods as PRA Chief Executive."},
    {"date": "2026-07-15", "event": "FCA final rules for Buy Now Pay Later (BNPL) regulation take effect."},
]

# London BTL & Property data (manually updated)
EXEC_PROPERTY = [
    {"metric": "London House Price", "value": "508,400", "unit": "£", "change": "-0.5% MoM", "note": "6th consecutive annual fall at -1.7%"},
    {"metric": "London Rent (New)", "value": "2,140", "unit": "£", "change": "+1.7% YoY", "note": "Rental growth slowing but still positive"},
    {"metric": "BTL Incorporation", "value": "74%", "unit": "", "change": "", "note": "Of new purchases — driven by Section 24 tax friction"},
    {"metric": "EPC 'C' Gap", "value": "42%", "unit": "", "change": "", "note": "Of London private rental stock — MEES compliance liability"},
    {"metric": "ICR Stress Test", "value": "8.23%", "unit": "", "change": "", "note": "SONIA 3.73% + 4.5% stressed buffer for higher-rate taxpayers"},
]

# Regulatory & Strategic updates (manually maintained)
EXEC_REGULATORY = [
    "FCA published 2026 Regulatory Priorities — replacing portfolio letters with outcomes-focused sector reports for Mortgages.",
    "FCA confirmed final rules for Buy Now Pay Later (BNPL) regulation, scheduled for implementation July 15, 2026.",
    "HM Treasury proposed a new 'Permission for acting as a Principal,' curbing unregulated expansion of Appointed Representatives.",
    "PRA issued a 'Limited Appetite' statement on capital requirement compromises, prioritizing sector resilience over aggressive growth.",
    "Financial Ombudsman Service (FOS) scope expanded to allow direct complaints against Appointed Representatives, increasing Principal liability.",
]

# Major Banking & M&A News (manually maintained)
EXEC_BANKING_NEWS = [
    "Allegro Finance Limited completed a landmark securitised warehouse facility to establish a global media credit platform.",
    "Arcus Infrastructure Partners acquired 100% of WCCTV, supported by a major UK lender consortium debt package.",
    "Barclays strategic policy lead Katharine Braddick confirmed as next BoE Deputy Governor, signaling a shift in prudential supervision.",
    "Charles Russell Speechlys reports a surge in mid-market M&A activity following £38bn of UK financial services deals in the prior year.",
    "Travers Smith advised on a major real estate finance restructuring involving new 'contractual control' registers under the Levelling Up Act.",
]

# Leadership tip
EXEC_LEADERSHIP_TIP = "Strategic Subtraction: Audit your lending policy for 'legacy criteria' that no longer contribute to risk-adjusted returns. If a rule hasn't caught a default in 24 months but slows down 20% of applications, remove it."


def _fetch_exchange_rates() -> dict:
    """Fetch GBP-based exchange rates from free API (GBP/PKR, Gold via XAU)."""
    try:
        resp = requests.get("https://open.er-api.com/v6/latest/GBP", timeout=15)
        resp.raise_for_status()
        data = resp.json()
        rates = data.get("rates", {})
        return {
            "GBP_PKR": {"value": rates.get("PKR"), "label": "GBP / PKR", "source": "open.er-api.com"},
            "GBP_USD": {"value": rates.get("USD"), "label": "GBP / USD", "source": "open.er-api.com"},
            "GBP_EUR": {"value": rates.get("EUR"), "label": "GBP / EUR", "source": "open.er-api.com"},
            "USD_per_XAU": {"value": round(1 / rates["XAU"], 2) if rates.get("XAU") else None, "label": "Gold (USD/oz)", "source": "open.er-api.com"},
        }
    except Exception as exc:
        logger.warning("Failed to fetch exchange rates: %s", exc)
        return {}


# ---------------------------------------------------------------------------
# Time-based cache (no APScheduler needed)
# ---------------------------------------------------------------------------
_cache: dict = {
    "rates": {},
    "news": {},
    "ubl_mentions": [],
    "exec_fx": {},
    "last_updated": None,
    "news_last_updated": None,
    "rates_fetched_at": 0,
    "news_fetched_at": 0,
    "exec_fx_fetched_at": 0,
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


def _search_google_news(query: str, max_items: int = 5) -> list[dict]:
    """Search Google News RSS for mentions of a query."""
    try:
        encoded = requests.utils.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded}&hl=en-GB&gl=GB&ceid=GB:en"
        return _parse_rss(url, max_items)
    except Exception as exc:
        logger.warning("Failed Google News search for %s: %s", query, exc)
        return []


def _ensure_rates():
    """Refresh rates if cache is stale."""
    now = time.time()
    if now - _cache["rates_fetched_at"] < RATES_CACHE_SECONDS and _cache["rates"]:
        return
    logger.info("Refreshing rate data…")
    rates = {}
    # Map ECB live IDs to their ECB SDW rate codes and FRED fallbacks
    ecb_live_map = {
        "ECB_DFR": ("DFR", "ECBDFR"),
        "ECB_MRR": ("MRR_FR", "ECBMRRFR"),
        "ECB_MLF": ("MLFR", "ECBMLFR"),
    }

    for series_id, label in ALL_SERIES.items():
        if series_id in ecb_live_map:
            ecb_code, fred_fallback = ecb_live_map[series_id]
            data = _fetch_ecb_rate(ecb_code)
            if data["value"] is None:
                data = _fetch_fred_series(fred_fallback)
                data["source"] = "FRED (fallback)"
            data["label"] = label
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
        _cache["news_fetched_at"] = now
        _cache["news_last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info("News refresh complete — %d UBL mentions found.", len(ubl_mentions))


# ---------------------------------------------------------------------------
# AI Executive Summary Generator
# ---------------------------------------------------------------------------
def _generate_executive_summary(rates: dict, exec_fx: dict, health: dict) -> list[dict]:
    """Analyze all data and generate executive summary bullet points.
    Each item: {icon, category, text, severity: 'info'|'positive'|'warning'|'alert'}
    """
    summary = []
    today = datetime.now(timezone.utc).strftime("%A, %B %d, %Y")

    # --- Monetary Policy Stance ---
    fed_upper = rates.get("DFEDTARU", {}).get("value")
    fed_lower = rates.get("DFEDTARL", {}).get("value")
    ecb_dfr = rates.get("ECB_DFR", {}).get("value")
    sonia = rates.get("IUDSOIA", {}).get("value")

    if fed_upper is not None and fed_lower is not None:
        summary.append({
            "icon": "🏛️", "category": "Fed Policy",
            "text": f"Federal Reserve target range holds at {fed_lower:.2f}%–{fed_upper:.2f}%. "
                    f"{'Markets pricing in potential cuts.' if fed_upper <= 4.0 else 'Elevated rates signal continued tightening bias.'}",
            "severity": "info"
        })

    if ecb_dfr is not None:
        summary.append({
            "icon": "🇪🇺", "category": "ECB",
            "text": f"ECB deposit facility rate at {ecb_dfr:.2f}%. "
                    f"{'Accommodative stance — watch for further easing signals.' if ecb_dfr < 2.5 else 'Restrictive territory — monitoring inflation trajectory.'}",
            "severity": "info"
        })

    if sonia is not None:
        boe_approx = round(sonia * 4) / 4  # Round to nearest 0.25
        summary.append({
            "icon": "🇬🇧", "category": "Bank of England",
            "text": f"SONIA at {sonia:.2f}%, implying BoE Base Rate near {boe_approx:.2f}%. "
                    f"{'Rate-sensitive mortgage products remain under pressure.' if sonia > 4.0 else 'Rate environment stabilizing — positive for lending margins.'}",
            "severity": "info"
        })

    # --- Yield Curve Health ---
    spread_2s10s = rates.get("T10Y2Y", {}).get("value")
    if spread_2s10s is not None:
        if spread_2s10s < 0:
            summary.append({
                "icon": "⚠️", "category": "Yield Curve",
                "text": f"ALERT: Yield curve inverted at {spread_2s10s:.2f}%. Historically signals recession risk within 12–18 months. Review credit exposure.",
                "severity": "alert"
            })
        elif spread_2s10s < 0.25:
            summary.append({
                "icon": "🔶", "category": "Yield Curve",
                "text": f"Yield curve nearly flat at {spread_2s10s:.2f}%. Monitor for potential inversion — tighten risk appetite on longer-duration lending.",
                "severity": "warning"
            })
        else:
            summary.append({
                "icon": "✅", "category": "Yield Curve",
                "text": f"Yield curve positive at {spread_2s10s:.2f}%. Normal shape supports traditional banking margins.",
                "severity": "positive"
            })

    # --- Credit Stress ---
    hy_spread = rates.get("BAMLH0A0HYM2", {}).get("value")
    if hy_spread is not None:
        if hy_spread > 5:
            summary.append({
                "icon": "🚨", "category": "Credit Markets",
                "text": f"High-yield spread at {hy_spread:.2f}% — elevated stress. Corporate default risk rising. Review counterparty exposures.",
                "severity": "alert"
            })
        elif hy_spread > 4:
            summary.append({
                "icon": "🔶", "category": "Credit Markets",
                "text": f"High-yield spread at {hy_spread:.2f}% — moderate stress. Credit conditions tightening.",
                "severity": "warning"
            })
        else:
            summary.append({
                "icon": "✅", "category": "Credit Markets",
                "text": f"High-yield spread at {hy_spread:.2f}% — benign conditions. Credit markets calm.",
                "severity": "positive"
            })

    # --- Oil & Inflation ---
    brent = rates.get("DCOILBRENTEU", {}).get("value")
    if brent is not None:
        if brent > 100:
            summary.append({
                "icon": "🛢️", "category": "Energy & Inflation",
                "text": f"Brent crude at ${brent:.2f}/bbl — elevated. Energy-driven inflation pressure remains a headwind for rate cuts.",
                "severity": "warning"
            })
        elif brent > 80:
            summary.append({
                "icon": "🛢️", "category": "Energy",
                "text": f"Brent crude at ${brent:.2f}/bbl — stable range. Limited inflation pass-through expected.",
                "severity": "info"
            })
        else:
            summary.append({
                "icon": "🛢️", "category": "Energy",
                "text": f"Brent crude at ${brent:.2f}/bbl — supportive of disinflation narrative.",
                "severity": "positive"
            })

    # --- Sterling ---
    gbpusd = rates.get("DEXUSUK", {}).get("value")
    gbp_pkr = exec_fx.get("GBP_PKR", {}).get("value") if exec_fx else None

    if gbpusd is not None:
        if gbpusd > 1.35:
            sev = "positive"
            note = "Strong sterling — favourable for UK importers and overseas asset holders."
        elif gbpusd < 1.25:
            sev = "warning"
            note = "Weak sterling — increases import costs and inflation risk."
        else:
            sev = "info"
            note = "Sterling trading in normal range."
        summary.append({"icon": "💱", "category": "FX", "text": f"GBP/USD at {gbpusd:.4f}. {note}", "severity": sev})

    if gbp_pkr is not None:
        summary.append({
            "icon": "💱", "category": "Remittance Corridor",
            "text": f"GBP/PKR at {gbp_pkr:.2f} — {'stable corridor supports remittance flows.' if gbp_pkr > 350 else 'monitor for corridor pressure.'}",
            "severity": "info"
        })

    # --- Gold ---
    gold = exec_fx.get("USD_per_XAU", {}).get("value") if exec_fx else None
    if gold is not None:
        if gold > 2500:
            summary.append({"icon": "🥇", "category": "Safe Haven", "text": f"Gold at ${gold:,.0f}/oz — strong safe-haven demand. Risk-off sentiment elevated.", "severity": "warning"})
        elif gold > 2000:
            summary.append({"icon": "🥇", "category": "Safe Haven", "text": f"Gold at ${gold:,.0f}/oz — elevated but stable. Uncertainty premium persists.", "severity": "info"})

    # --- UK Gilt ---
    gilt_10y = rates.get("IRLTLT01GBM156N", {}).get("value")
    if gilt_10y is not None:
        if gilt_10y > 4.5:
            summary.append({
                "icon": "🇬🇧", "category": "UK Gilts",
                "text": f"UK 10Y Gilt yield at {gilt_10y:.2f}% — approaching 5%. 'Higher for longer' fiscal environment pressuring fixed-rate mortgage pricing.",
                "severity": "warning"
            })
        else:
            summary.append({
                "icon": "🇬🇧", "category": "UK Gilts",
                "text": f"UK 10Y Gilt yield at {gilt_10y:.2f}% — manageable range for mortgage-backed products.",
                "severity": "info"
            })

    # --- Upcoming Calendar ---
    upcoming = []
    now_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for event in EXEC_CALENDAR:
        if event["date"] >= now_date:
            upcoming.append(event)
    if upcoming:
        next_event = upcoming[0]
        summary.append({
            "icon": "📅", "category": "Next Key Date",
            "text": f"{next_event['date']}: {next_event['event']}",
            "severity": "info"
        })

    # --- Mortgage Rate Indicator ---
    mortgage = rates.get("MORTGAGE30US", {}).get("value")
    if mortgage is not None:
        summary.append({
            "icon": "🏠", "category": "US Mortgage",
            "text": f"US 30Y mortgage rate at {mortgage:.2f}%. {'Affordability under strain.' if mortgage > 7 else 'Relatively stable for borrowers.'}",
            "severity": "warning" if mortgage > 7 else "info"
        })

    return summary


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

    # Fetch executive brief FX data
    now = time.time()
    if now - _cache["exec_fx_fetched_at"] > RATES_CACHE_SECONDS or not _cache["exec_fx"]:
        _cache["exec_fx"] = _fetch_exchange_rates()
        _cache["exec_fx_fetched_at"] = now

    with _lock:
        rates = dict(_cache["rates"])
        news = dict(_cache["news"])
        ubl_mentions = list(_cache["ubl_mentions"])
        exec_fx = dict(_cache["exec_fx"])
        last_updated = _cache["last_updated"]
        news_last_updated = _cache["news_last_updated"]

    grouped: dict[str, list[dict]] = {}
    for group_key, series_map in SERIES.items():
        if group_key == "central_banks_fred":
            continue  # Rendered via cb_cards instead
        items = []
        for sid, label in series_map.items():
            entry = rates.get(sid, {"value": None, "date": None, "direction": None})
            items.append({"id": sid, "label": label, **entry})
        grouped[group_key] = items

    # Build central bank multi-rate cards
    cb_cards = []
    for card_def in CENTRAL_BANK_CARDS:
        card = {
            "bank": card_def["bank"],
            "flag": card_def["flag"],
            "note": card_def.get("note"),
            "history_id": card_def["history_id"],
            "rates": [],
        }
        for rate_def in card_def["rates"]:
            r = rates.get(rate_def["id"], {})
            card["rates"].append({
                "id": rate_def["id"],
                "label": rate_def["label"],
                "value": r.get("value"),
                "date": r.get("date"),
                "direction": r.get("direction"),
                "freshness": r.get("freshness", "stale"),
                "source": rate_def["source"],
            })
        cb_cards.append(card)

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

    # Generate AI executive summary
    ai_summary = _generate_executive_summary(rates, exec_fx, health)

    return render_template(
        "dashboard.html",
        grouped=grouped,
        cb_cards=cb_cards,
        yc_labels=yc_labels,
        yc_values=yc_values,
        health=health,
        news=news,
        ubl_mentions=ubl_mentions,
        us_10y_value=rates.get("DGS10", {}).get("value"),
        bund_btp_spread=bund_btp_spread,
        ai_summary=ai_summary,
        exec_fx=exec_fx,
        exec_calendar=EXEC_CALENDAR,
        exec_property=EXEC_PROPERTY,
        exec_regulatory=EXEC_REGULATORY,
        exec_banking_news=EXEC_BANKING_NEWS,
        exec_leadership_tip=EXEC_LEADERSHIP_TIP,
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
    # ECB live IDs are not real FRED series — map to FRED equivalents for history
    ecb_history_map = {"ECB_DFR": "ECBDFR", "ECB_MRR": "ECBMRRFR", "ECB_MLF": "ECBMLFR"}
    fetch_id = ecb_history_map.get(series_id, series_id)
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
