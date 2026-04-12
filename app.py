import os
import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from threading import Lock
from html import unescape
from functools import wraps
import re
import time
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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
EIA_API_KEY = os.getenv("EIA_API_KEY")

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
    # ===== US REGION =====
    "us_rates": {
        "DFEDTARU":        "Fed Target Upper",
        "DFEDTARL":        "Fed Target Lower",
        "SOFR":            "SOFR",
        "DPRIME":          "US Prime Rate",
        "MORTGAGE30US":    "US 30-Year Mortgage",
        "T10Y2Y":          "2s10s Spread",
    },
    "us_treasuries": {
        "DGS1MO":  "1 Month", "DGS3MO":  "3 Month", "DGS6MO":  "6 Month",
        "DGS1":    "1 Year", "DGS2":    "2 Year", "DGS5":    "5 Year",
        "DGS7":    "7 Year", "DGS10":   "10 Year", "DGS20":   "20 Year", "DGS30":   "30 Year",
    },
    "us_macro": {
        "UNRATE":           "US Unemployment Rate",
        "CPIAUCSL":         "US CPI (Index)",
        "T10YIE":           "US 10Y Breakeven Inflation",
        "A191RL1Q225SBEA":  "US Real GDP Growth %",
        "INDPRO":           "US Industrial Production",
        "SP500":            "S&P 500",
        "NASDAQCOM":        "NASDAQ Composite",
        "DTWEXBGS":         "US Dollar Index",
    },
    "us_property": {
        "CSUSHPISA":       "Case-Shiller Home Price Index",
        "MSPUS":           "US Median Home Sale Price",
        "HOUST":           "US Housing Starts (000s)",
    },
    "us_credit": {
        "BAMLC0A0CM":      "IG Corporate Spread",
        "BAMLH0A0HYM2":    "HY Corporate Spread",
    },
    # ===== UK REGION =====
    "uk_rates": {
        "IUDSOIA":           "UK SONIA (Overnight)",
        "IRSTCI01GBM156N":   "UK Short-Term Rate",
        "IRLTLT01GBM156N":   "UK Long-Term Gilt",
    },
    "uk_macro": {
        "GBRCPIALLMINMEI":   "UK CPI (Index)",
        "LRHUTTTTGBM156S":   "UK Unemployment Rate",
    },
    # ===== GLOBAL / REST OF WORLD =====
    "fx_rates": {
        "DEXUSUK":  "GBP/USD", "DEXUSEU":  "EUR/USD", "DEXJPUS":  "USD/JPY",
    },
    "global_bonds_10y": {
        "DGS10": "US 10Y", "IRLTLT01GBM156N": "UK 10Y",
        "IRLTLT01DEM156N": "Germany 10Y", "IRLTLT01FRM156N": "France 10Y",
        "IRLTLT01JPM156N": "Japan 10Y", "IRLTLT01CAM156N": "Canada 10Y",
        "IRLTLT01AUM156N": "Australia 10Y", "IRLTLT01ITM156N": "Italy 10Y",
        "IRLTLT01ESM156N": "Spain 10Y", "IRLTLT01CHM156N": "Switzerland 10Y",
        "IRLTLT01KRM156N": "South Korea 10Y", "IRLTLT01NZM156N": "New Zealand 10Y",
    },
    "global_rates_fred": {
        "ECBMRRFR":          "ECB Main Refi (FRED)",
        "IR3TIB01JPM156N":   "Japan 3M Interbank",
        "IR3TIB01CHM156N":   "Swiss 3M Interbank",
        "IR3TIB01AUM156N":   "Australia 3M Interbank",
        "IRSTCI01CAM156N":   "Bank of Canada Rate",
        "IR3TIB01CNM156N":   "China 3M Interbank",
    },
    "commodities": {
        "DCOILBRENTEU":    "Brent Crude Oil ($/bbl)",
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
# City Monitor definitions
# ---------------------------------------------------------------------------
CITY_MONITOR = {
    "chicago": {
        "name": "Chicago",
        "flag": "🏙️",
        "fred_series": {
            "CHXRSA": "Case-Shiller Home Price Index",
            "CHIC917URN": "Chicago Metro Unemployment Rate",
        },
        "news_query": "Chicago+economy+finance+business",
        "property_query": "Chicago+housing+property+real+estate+prices",
    },
    "new_york": {
        "name": "New York",
        "flag": "🗽",
        "fred_series": {
            "NYXRSA": "Case-Shiller Home Price Index",
            "NEWY636URN": "NYC Metro Unemployment Rate",
        },
        "news_query": "New+York+City+economy+finance+Wall+Street+business",
        "property_query": "New+York+City+housing+property+real+estate+prices",
    },
    "los_angeles": {
        "name": "Los Angeles",
        "flag": "🌴",
        "fred_series": {
            "LXXRSA": "Case-Shiller Home Price Index",
            "LOSA706URN": "LA Metro Unemployment Rate",
        },
        "news_query": "Los+Angeles+economy+finance+business",
        "property_query": "Los+Angeles+housing+property+real+estate+prices",
    },
    "miami": {
        "name": "Miami",
        "flag": "🌊",
        "fred_series": {
            "MIXRSA": "Case-Shiller Home Price Index",
            "MIAM112URN": "Miami Metro Unemployment Rate",
        },
        "news_query": "Miami+economy+finance+business",
        "property_query": "Miami+housing+property+real+estate+prices",
    },
    "san_francisco": {
        "name": "San Francisco",
        "flag": "🌉",
        "fred_series": {
            "SFXRSA": "Case-Shiller Home Price Index",
            "SANF806URN": "SF Metro Unemployment Rate",
        },
        "news_query": "San+Francisco+economy+finance+tech+business",
        "property_query": "San+Francisco+housing+property+real+estate+prices",
    },
    "philadelphia": {
        "name": "Philadelphia",
        "flag": "🔔",
        "fred_series": {
            "PHXRSA": "Case-Shiller Home Price Index",
            "PHIL942URN": "Philadelphia Metro Unemployment Rate",
        },
        "news_query": "Philadelphia+economy+finance+business",
        "property_query": "Philadelphia+housing+property+real+estate+prices",
    },
    "london": {
        "name": "London",
        "flag": "🇬🇧",
        "fred_series": {},
        "news_query": "London+economy+finance+City+business",
        "property_query": "London+housing+property+house+prices",
        "use_land_registry": True,
    },
    "manchester": {
        "name": "Manchester",
        "flag": "🇬🇧",
        "fred_series": {},
        "news_query": "Manchester+economy+business+finance",
        "property_query": "Manchester+housing+property+house+prices",
    },
}

# Add city FRED series to ALL_SERIES for label lookups & fetching
for _city in CITY_MONITOR.values():
    for _sid, _label in _city.get("fred_series", {}).items():
        ALL_SERIES[_sid] = _label

# ---------------------------------------------------------------------------
# RSS News Feed definitions
# ---------------------------------------------------------------------------
NEWS_FEEDS = {
    "us_finance": [
        {"name": "Google News — US Economy & Finance", "url": "https://news.google.com/rss/search?q=US+economy+finance+Federal+Reserve+Wall+Street+when:7d&hl=en-US&gl=US&ceid=US:en", "icon": "🇺🇸"},
    ],
    "uk_finance": [
        {"name": "Google News — UK Finance", "url": "https://news.google.com/rss/search?q=UK+finance+economy+banking+interest+rates+when:7d&hl=en-GB&gl=GB&ceid=GB:en", "icon": "🇬🇧"},
        {"name": "Google News — UK Economy", "url": "https://news.google.com/rss/search?q=UK+economy+inflation+Bank+of+England+when:7d&hl=en-GB&gl=GB&ceid=GB:en", "icon": "📊"},
        {"name": "BBC — Business", "url": "http://feeds.bbci.co.uk/news/business/rss.xml", "icon": "📰"},
    ],
    "central_banking": [
        {"name": "Google News — UK Banking Regulation", "url": "https://news.google.com/rss/search?q=UK+bank+regulation+FCA+PRA+%22Bank+of+England%22+when:7d&hl=en-GB&gl=GB&ceid=GB:en", "icon": "⚖️"},
        {"name": "Google News — UK Banking News", "url": "https://news.google.com/rss/search?q=UK+banking+sector+high+street+banks+when:7d&hl=en-GB&gl=GB&ceid=GB:en", "icon": "🏦"},
        {"name": "Google News — BoE Speeches", "url": "https://news.google.com/rss/search?q=%22Bank+of+England%22+speech+OR+announcement+OR+policy+when:7d&hl=en-GB&gl=GB&ceid=GB:en", "icon": "🏛️"},
    ],
    "global_markets": [
        {"name": "Google News — Global Markets", "url": "https://news.google.com/rss/search?q=global+markets+stocks+bonds+finance+when:7d&hl=en-GB&gl=GB&ceid=GB:en", "icon": "🌍"},
        {"name": "Google News — Central Banks", "url": "https://news.google.com/rss/search?q=Federal+Reserve+ECB+central+bank+interest+rate+when:7d&hl=en-GB&gl=GB&ceid=GB:en", "icon": "🏛️"},
        {"name": "CNBC — Finance", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664", "icon": "📈"},
    ],
    "global_finance": [
        {"name": "Google News — Global Economy & Trade", "url": "https://news.google.com/rss/search?q=global+economy+emerging+markets+trade+when:7d&hl=en-US&gl=US&ceid=US:en", "icon": "🌐"},
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

# Leadership tips — rotating pool (changes daily based on day of year)
LEADERSHIP_TIPS = [
    "Strategic Subtraction: Audit your lending policy for 'legacy criteria' that no longer contribute to risk-adjusted returns. If a rule hasn't caught a default in 24 months but slows down 20% of applications, remove it.",
    "Decision Velocity: The cost of a delayed decision often exceeds the cost of a wrong one. Set a 48-hour rule for any decision that's reversible.",
    "Margin Discipline: Revenue growth without margin expansion is just burning fuel faster. Every new product should pass the 'would I invest my own money?' test.",
    "Talent Density: One exceptional performer creates more value than three average ones. Invest disproportionately in your top 10%.",
    "Customer Friction Mapping: Walk through your customer onboarding process quarterly. Every unnecessary click, form field, or wait time is a silent revenue leak.",
    "Risk Culture: The best risk management isn't about stopping things — it's about enabling the right things faster. Measure how quickly good deals get approved, not just how many bad ones you catch.",
    "Data Over Intuition: If two executives disagree, ask 'what data would change your mind?' If neither can answer, the discussion isn't ready for a decision.",
    "Regulatory Advantage: Don't just comply with regulation — find the competitive advantage in it. The bank that implements Consumer Duty best wins the market, not just the audit.",
    "Communication Clarity: If your strategy can't be explained in three sentences to a branch manager, it's not a strategy — it's a wish list.",
    "Operational Leverage: Every manual process is technical debt. Automate one workflow per quarter and reinvest the saved hours into customer-facing innovation.",
    "Board Readiness: Before every board meeting, ask yourself: 'What's the one question I hope they don't ask?' — then prepare the answer.",
    "Balance Sheet Thinking: Assets are easy to accumulate, hard to exit. For every acquisition, document the exit thesis on day one.",
    "Stakeholder Mapping: Your regulators, auditors, and rating agencies are stakeholders too. A 15-minute quarterly call prevents a 15-day crisis response.",
    "Cultural Capital: How your team behaves when you're not in the room is your actual culture. Measure it through 360 feedback, not town halls.",
    "Strategic Patience: The best deals come to those who can walk away. Never let urgency override discipline — the market always gives you another chance.",
]

# Google News RSS queries for live exec brief sections
EXEC_NEWS_FEEDS = {
    "banking_ma": {
        "label": "Major Banking & M&A News",
        "queries": [
            "https://news.google.com/rss/search?q=UK+banking+M%26A+acquisition+deal+when:7d&hl=en-GB&gl=GB&ceid=GB:en",
            "https://news.google.com/rss/search?q=UK+bank+merger+finance+deal+when:7d&hl=en-GB&gl=GB&ceid=GB:en",
        ],
        "max_items": 5,
    },
    "regulatory": {
        "label": "Regulatory & Strategic Updates",
        "queries": [
            "https://news.google.com/rss/search?q=FCA+regulation+UK+banking+when:7d&hl=en-GB&gl=GB&ceid=GB:en",
            "https://news.google.com/rss/search?q=PRA+%22Bank+of+England%22+prudential+regulation+when:7d&hl=en-GB&gl=GB&ceid=GB:en",
        ],
        "max_items": 5,
    },
    "property": {
        "label": "London Property & BTL News",
        "queries": [
            "https://news.google.com/rss/search?q=London+house+prices+property+market+BTL+when:7d&hl=en-GB&gl=GB&ceid=GB:en",
        ],
        "max_items": 5,
    },
    "calendar": {
        "label": "UK Financial Calendar & Events",
        "queries": [
            "https://news.google.com/rss/search?q=%22Bank+of+England%22+rate+decision+MPC+when:7d&hl=en-GB&gl=GB&ceid=GB:en",
            "https://news.google.com/rss/search?q=FCA+deadline+consultation+UK+finance+when:7d&hl=en-GB&gl=GB&ceid=GB:en",
        ],
        "max_items": 5,
    },
    "us_property": {
        "label": "US Housing & Property Market",
        "queries": [
            "https://news.google.com/rss/search?q=US+housing+market+property+prices+mortgage+rates+when:7d&hl=en-US&gl=US&ceid=US:en",
        ],
        "max_items": 5,
    },
}


def _fetch_london_property() -> list[dict]:
    """Fetch London property data from HM Land Registry SPARQL API."""
    query = '''
PREFIX ukhpi: <http://landregistry.data.gov.uk/def/ukhpi/>
SELECT ?month ?avgPrice ?annualChange ?monthlyChange WHERE {
  ?item ukhpi:refRegion <http://landregistry.data.gov.uk/id/region/london> ;
        ukhpi:refMonth ?month ;
        ukhpi:averagePrice ?avgPrice .
  OPTIONAL { ?item ukhpi:percentageAnnualChange ?annualChange }
  OPTIONAL { ?item ukhpi:percentageChange ?monthlyChange }
} ORDER BY DESC(?month) LIMIT 6
'''
    try:
        resp = requests.post(
            "https://landregistry.data.gov.uk/landregistry/query",
            data={"query": query},
            timeout=15,
            headers={"Accept": "application/sparql-results+json"},
        )
        resp.raise_for_status()
        results = resp.json().get("results", {}).get("bindings", [])
        data = []
        for row in results:
            data.append({
                "month": row["month"]["value"],
                "avgPrice": float(row["avgPrice"]["value"]),
                "annualChange": row.get("annualChange", {}).get("value"),
                "monthlyChange": row.get("monthlyChange", {}).get("value"),
            })
        return data
    except Exception as exc:
        logger.warning("Failed to fetch London property data: %s", exc)
        return []


def _fetch_ons_hpi() -> dict:
    """Fetch UK and London HPI annual % change from ONS."""
    result = {}
    for code, name in [("l55o", "UK HPI"), ("l55p", "London HPI")]:
        try:
            url = f"https://www.ons.gov.uk/economy/inflationandpriceindices/timeseries/{code}/mm23/data"
            resp = requests.get(url, timeout=15, headers={"User-Agent": "CEOMonitor/1.0"})
            resp.raise_for_status()
            months = resp.json().get("months", [])
            if months:
                latest = months[-1]
                result[name] = {"date": latest["date"], "value": latest["value"]}
        except Exception as exc:
            logger.warning("Failed to fetch ONS %s: %s", code, exc)
    return result


def _fetch_boe_events() -> list[dict]:
    """Fetch BoE events — try official RSS first, fallback to Google News."""
    articles = _parse_rss("https://www.bankofengland.co.uk/rss/events", max_items=10)
    if not articles:
        # Fallback: Google News for BoE events and calendar
        articles = _parse_rss(
            "https://news.google.com/rss/search?q=%22Bank+of+England%22+event+OR+meeting+OR+speech+OR+decision+when:7d&hl=en-GB&gl=GB&ceid=GB:en",
            max_items=10,
        )
        for a in articles:
            a["source"] = "Google News"
            a["icon"] = "🏛️"
    return articles


def _fetch_exec_news() -> dict:
    """Fetch all executive brief news sections from Google News RSS."""
    result = {}
    for key, config in EXEC_NEWS_FEEDS.items():
        items = []
        seen = set()
        for url in config["queries"]:
            articles = _parse_rss(url, max_items=config["max_items"])
            for a in articles:
                title_hash = hashlib.md5(a["title"].encode()).hexdigest()
                if title_hash not in seen:
                    seen.add(title_hash)
                    items.append(a)
        result[key] = items[:config["max_items"]]
    return result


def _get_leadership_tip() -> str:
    """Return a leadership tip that rotates daily."""
    day_of_year = datetime.now(timezone.utc).timetuple().tm_yday
    return LEADERSHIP_TIPS[day_of_year % len(LEADERSHIP_TIPS)]


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
# Oil Tracker — EIA + FRED data for WTI/USO analysis
# ---------------------------------------------------------------------------

def _fetch_eia_series(series_id: str, num: int = 52) -> list[dict]:
    """Fetch weekly EIA petroleum data via v2 API."""
    if not EIA_API_KEY:
        logger.warning("EIA_API_KEY not set — skipping EIA fetch for %s", series_id)
        return []
    url = "https://api.eia.gov/v2/petroleum/stoc/wstk/data/"
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "weekly",
        "data[0]": "value",
        "facets[series][]": series_id,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": num,
    }
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        raw = resp.json().get("response", {}).get("data", [])
        result = []
        for row in raw:
            try:
                result.append({"date": row["period"], "value": float(row["value"])})
            except (ValueError, TypeError, KeyError):
                continue
        return result
    except Exception as exc:
        logger.warning("EIA fetch failed for %s: %s", series_id, exc)
        return []


def _fetch_eia_weekly_supply(series_id: str, num: int = 52) -> list[dict]:
    """Fetch weekly EIA petroleum supply data via v2 API."""
    if not EIA_API_KEY:
        return []
    url = "https://api.eia.gov/v2/petroleum/sum/sndw/data/"
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "weekly",
        "data[0]": "value",
        "facets[series][]": series_id,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": num,
    }
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        raw = resp.json().get("response", {}).get("data", [])
        result = []
        for row in raw:
            try:
                result.append({"date": row["period"], "value": float(row["value"])})
            except (ValueError, TypeError, KeyError):
                continue
        return result
    except Exception as exc:
        logger.warning("EIA supply fetch failed for %s: %s", series_id, exc)
        return []


def _fetch_eia_spot_price(series_id: str, num: int = 90) -> list[dict]:
    """Fetch daily EIA spot prices (WTI, gasoline, etc) via v2 API."""
    if not EIA_API_KEY:
        return []
    url = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "daily",
        "data[0]": "value",
        "facets[series][]": series_id,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": num,
    }
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        raw = resp.json().get("response", {}).get("data", [])
        result = []
        for row in raw:
            try:
                result.append({"date": row["period"], "value": float(row["value"])})
            except (ValueError, TypeError, KeyError):
                continue
        return result
    except Exception as exc:
        logger.warning("EIA spot price fetch failed for %s: %s", series_id, exc)
        return []


def _fetch_yahoo_chart(symbol: str, range_str: str = "3mo", interval: str = "1d") -> list[dict]:
    """Fetch daily price data from Yahoo Finance chart API (free, no key)."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": range_str, "interval": interval}
    headers = {"User-Agent": "CEOMonitor/1.0"}
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        result = data.get("chart", {}).get("result", [{}])[0]
        timestamps = result.get("timestamp", [])
        closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
        out = []
        for ts, close in zip(timestamps, closes):
            if close is not None:
                date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                out.append({"date": date_str, "value": round(close, 2)})
        return out
    except Exception as exc:
        logger.warning("Yahoo Finance fetch failed for %s: %s", symbol, exc)
        return []


def _fetch_oil_news() -> list[dict]:
    """Fetch oil-related news headlines from Google News RSS."""
    urls = [
        "https://news.google.com/rss/search?q=oil+price+WTI+crude+OPEC+when:7d&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=oil+market+petroleum+energy+prices+when:7d&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=Iran+oil+sanctions+Hormuz+Middle+East+oil+when:7d&hl=en-US&gl=US&ceid=US:en",
    ]
    all_items = []
    seen = set()
    for url in urls:
        articles = _parse_rss(url, max_items=8)
        for a in articles:
            title_hash = hashlib.md5(a["title"].encode()).hexdigest()
            if title_hash not in seen:
                seen.add(title_hash)
                all_items.append(a)
    return all_items[:15]


# ---------------------------------------------------------------------------
# Geopolitical Layer — Polymarket odds + News sentiment
# ---------------------------------------------------------------------------

# Polymarket event slugs and markets that affect oil prices
POLYMARKET_OIL_EVENTS = [
    # Each: (slug_search_term, question_keywords, bullish_if_yes, weight)
    # bullish_if_yes=True means "Yes" outcome = oil price UP (e.g., war/conflict)
    # bullish_if_yes=False means "Yes" outcome = oil price DOWN (e.g., ceasefire/peace)
    {"keywords": ["ceasefire", "russia", "ukraine"], "bullish_if_yes": False, "label": "Russia-Ukraine Ceasefire"},
    {"keywords": ["china", "invade", "taiwan"], "bullish_if_yes": True, "label": "China-Taiwan Conflict"},
    {"keywords": ["israel", "annex"], "bullish_if_yes": True, "label": "Israel Annexation"},
    {"keywords": ["nato", "troops", "ukraine"], "bullish_if_yes": True, "label": "NATO in Ukraine"},
    {"keywords": ["hamas", "disarm"], "bullish_if_yes": False, "label": "Hamas Disarmament"},
    {"keywords": ["netanyahu", "out"], "bullish_if_yes": False, "label": "Netanyahu Exit"},
    {"keywords": ["us", "russia", "military", "clash"], "bullish_if_yes": True, "label": "US-Russia Clash"},
    {"keywords": ["iran"], "bullish_if_yes": True, "label": "Iran Conflict"},
]


def _fetch_polymarket_geopolitical() -> dict:
    """Fetch Polymarket prediction odds for geopolitical events affecting oil."""
    result = {"markets": [], "risk_score": 0, "risk_label": "Unknown"}
    try:
        resp = requests.get(
            "https://gamma-api.polymarket.com/events",
            params={"closed": "false", "limit": 100, "active": "true"},
            timeout=15,
        )
        resp.raise_for_status()
        events = resp.json()
    except Exception as exc:
        logger.warning("Polymarket fetch failed: %s", exc)
        return result

    # Score each relevant market
    war_risk_total = 0
    peace_signal_total = 0
    matched = 0

    for event in events:
        title = event.get("title", "").lower()
        for pm_def in POLYMARKET_OIL_EVENTS:
            if all(kw in title for kw in pm_def["keywords"]):
                # Find the most relevant (latest expiry) active market
                markets = event.get("markets", [])
                best_market = None
                for m in markets:
                    prices = m.get("outcomePrices", "")
                    if prices and prices != "[]":
                        try:
                            price_list = json.loads(prices) if isinstance(prices, str) else prices
                            yes_prob = float(price_list[0])
                            if yes_prob > 0 and yes_prob < 1:
                                best_market = {"question": m.get("question", ""), "yes_prob": yes_prob}
                        except (ValueError, IndexError, TypeError):
                            continue

                if best_market:
                    matched += 1
                    prob = best_market["yes_prob"]

                    # Determine oil impact
                    if pm_def["bullish_if_yes"]:
                        # Conflict/escalation market — higher prob = more oil bullish (supply risk)
                        oil_impact = prob  # 0 to 1, higher = more bullish for oil
                    else:
                        # Peace/de-escalation market — higher prob = bearish for oil
                        oil_impact = 1 - prob  # invert: low peace prob = bullish oil

                    result["markets"].append({
                        "label": pm_def["label"],
                        "question": best_market["question"],
                        "yes_prob": round(prob * 100, 1),
                        "oil_impact": round(oil_impact, 3),
                        "bullish_for_oil": pm_def["bullish_if_yes"],
                    })

                    if pm_def["bullish_if_yes"]:
                        war_risk_total += prob
                    else:
                        peace_signal_total += (1 - prob)
                break  # only match first event per definition

    # Compute overall geopolitical risk score
    if matched > 0:
        # Average oil-bullish probability across all matched markets
        avg_risk = (war_risk_total + peace_signal_total) / matched
        result["risk_score"] = round(avg_risk, 3)

        if avg_risk > 0.5:
            result["risk_label"] = "HIGH — Elevated conflict risk, bullish for oil"
        elif avg_risk > 0.3:
            result["risk_label"] = "MODERATE — Some geopolitical tension"
        else:
            result["risk_label"] = "LOW — Relative calm, bearish for oil"

    return result


def _score_news_sentiment(headlines: list[dict]) -> dict:
    """Score oil news sentiment using keyword analysis (no NLP library needed).

    Returns: {score: -2 to +2, bullish_count, bearish_count, details: [...]}
    """
    # Keywords that signal BULLISH for oil prices (supply disruption, demand strength)
    BULLISH_KEYWORDS = [
        "surge", "soar", "spike", "rally", "jump", "rise", "gain", "climb",
        "shortage", "disruption", "outage", "cut", "embargo", "sanction",
        "attack", "strike", "war", "conflict", "tension", "escalat",
        "iran", "hormuz", "houthi", "hezbollah", "missile",
        "opec cut", "production cut", "supply tight", "demand strong",
        "refinery fire", "pipeline", "hurricane", "storm",
        "record high", "multi-year high", "highest since",
        "draw", "decline in stockpile", "inventory drop",
    ]

    # Keywords that signal BEARISH for oil prices (oversupply, demand weakness)
    BEARISH_KEYWORDS = [
        "plunge", "crash", "tumble", "drop", "fall", "decline", "slump", "sink",
        "glut", "oversupply", "surplus", "excess", "weak demand", "slowdown",
        "recession", "ceasefire", "peace", "deal", "agreement", "truce",
        "opec increase", "production increase", "output boost", "ramp up",
        "record low", "lowest since", "multi-year low",
        "build", "stockpile increase", "inventory rise",
        "demand weak", "consumption drop", "china slowdown",
        "trade war", "tariff", "strong dollar",
    ]

    bullish_count = 0
    bearish_count = 0
    details = []

    for article in headlines[:15]:
        title = article.get("title", "").lower()
        summary = article.get("summary", "").lower()
        text = title + " " + summary

        article_bull = sum(1 for kw in BULLISH_KEYWORDS if kw in text)
        article_bear = sum(1 for kw in BEARISH_KEYWORDS if kw in text)

        if article_bull > article_bear:
            bullish_count += 1
            sentiment = "bullish"
        elif article_bear > article_bull:
            bearish_count += 1
            sentiment = "bearish"
        else:
            sentiment = "neutral"

        details.append({
            "title": article.get("title", ""),
            "sentiment": sentiment,
            "bull_hits": article_bull,
            "bear_hits": article_bear,
        })

    total = bullish_count + bearish_count
    if total == 0:
        score = 0
    else:
        ratio = (bullish_count - bearish_count) / total
        if ratio > 0.5:
            score = 2
        elif ratio > 0.15:
            score = 1
        elif ratio < -0.5:
            score = -2
        elif ratio < -0.15:
            score = -1
        else:
            score = 0

    return {
        "score": score,
        "bullish_count": bullish_count,
        "bearish_count": bearish_count,
        "neutral_count": len(details) - bullish_count - bearish_count,
        "details": details,
    }


def _build_oil_analysis(wti_prices, inventory_data, gasoline_demand, dxy_data, brent_value,
                         polymarket_data=None, news_sentiment=None):
    """Build step-by-step oil price analysis with scoring.

    Scoring is tuned via backtest over 57 weeks of EIA data (Apr 2025–Mar 2026).
    Key design choices:
      - Momentum (price trend) uses both weekly and monthly signals to capture
        both short-term swings and sustained moves.
      - Inventory uses a 3-week trend (not just 1-week change) to reduce noise
        from single-week revisions.
      - NEUTRAL is eliminated as a prediction — the model always leans directional,
        since oil rarely stays flat week-to-week.
      - Spread thresholds are relaxed ($3/$6 instead of $2/$8) so the factor
        actually contributes signal.
    """
    analysis = {"steps": [], "signal": "NEUTRAL", "score": 0, "factors": []}

    total_score = 0

    # Step 1: Price Trend (WTI) — dual timeframe momentum
    if wti_prices and len(wti_prices) >= 5:
        latest = wti_prices[0]["value"]
        week_ago = wti_prices[min(4, len(wti_prices)-1)]["value"]
        month_ago = wti_prices[min(20, len(wti_prices)-1)]["value"] if len(wti_prices) > 20 else week_ago
        weekly_chg = ((latest - week_ago) / week_ago * 100) if week_ago else 0
        monthly_chg = ((latest - month_ago) / month_ago * 100) if month_ago else 0

        # Weekly momentum score
        trend_score = 0
        if weekly_chg > 4:
            trend_score = 2
        elif weekly_chg > 1.5:
            trend_score = 1
        elif weekly_chg < -4:
            trend_score = -2
        elif weekly_chg < -1.5:
            trend_score = -1

        # Monthly trend adds conviction when aligned
        if monthly_chg > 5 and trend_score >= 0:
            trend_score += 1
        elif monthly_chg < -5 and trend_score <= 0:
            trend_score -= 1

        # Clamp to [-2, +2]
        trend_score = max(-2, min(2, trend_score))

        total_score += trend_score
        analysis["steps"].append({
            "title": "WTI Price Trend",
            "icon": "chart-line",
            "value": f"${latest:.2f}/bbl",
            "detail": f"Weekly: {weekly_chg:+.1f}% | Monthly: {monthly_chg:+.1f}%",
            "score": trend_score,
            "explanation": f"WTI at ${latest:.2f}. {'Strong upward momentum — prices accelerating.' if trend_score == 2 else 'Rising prices signal bullish momentum.' if trend_score > 0 else 'Sharp decline — prices under heavy selling pressure.' if trend_score == -2 else 'Falling prices signal bearish pressure.' if trend_score < 0 else 'Prices stable in range.'}",
        })
        analysis["factors"].append({"name": "Price Trend", "score": trend_score, "weight": "25%"})

    # Step 2: Inventory Analysis — 3-week trend for noise reduction
    # Note: EIA WCRSTUS1 values are in THOUSANDS of barrels
    if inventory_data and len(inventory_data) >= 3:
        latest_inv = inventory_data[0]["value"] / 1000  # convert to millions
        prev_inv = inventory_data[1]["value"] / 1000
        two_back = inventory_data[2]["value"] / 1000
        change_1w = latest_inv - prev_inv
        change_3w = latest_inv - two_back  # 3-week cumulative in millions

        inv_score = 0
        # Use 3-week cumulative trend (in millions of barrels)
        if change_3w < -4:
            inv_score = 2  # sustained draws = strongly bullish
        elif change_3w < -1:
            inv_score = 1  # moderate draws
        elif change_3w > 4:
            inv_score = -2  # sustained builds = strongly bearish
        elif change_3w > 1:
            inv_score = -1  # moderate builds

        total_score += inv_score
        analysis["steps"].append({
            "title": "US Crude Inventory",
            "icon": "database",
            "value": f"{latest_inv:,.1f}M bbl",
            "detail": f"1-week: {change_1w:+.1f}M | 3-week trend: {change_3w:+.1f}M bbl",
            "score": inv_score,
            "explanation": f"{'Sustained draws of {:.1f}M over 3 weeks — supply tightening, bullish.'.format(abs(change_3w)) if inv_score > 0 else 'Sustained builds of {:.1f}M over 3 weeks — supply growing, bearish.'.format(change_3w) if inv_score < 0 else '3-week inventory trend is flat — no supply signal.'}",
        })
        analysis["factors"].append({"name": "Inventory", "score": inv_score, "weight": "20%"})
    elif inventory_data and len(inventory_data) >= 2:
        latest_inv = inventory_data[0]["value"] / 1000
        prev_inv = inventory_data[1]["value"] / 1000
        change = latest_inv - prev_inv
        inv_score = 0
        if change < -3:
            inv_score = 2
        elif change < -0.5:
            inv_score = 1
        elif change > 3:
            inv_score = -2
        elif change > 0.5:
            inv_score = -1
        total_score += inv_score
        analysis["steps"].append({
            "title": "US Crude Inventory",
            "icon": "database",
            "value": f"{latest_inv:,.1f}M bbl",
            "detail": f"Change: {change:+.1f}M bbl",
            "score": inv_score,
            "explanation": f"{'Draw of {:.1f}M barrels — supply tightening.'.format(abs(change)) if change < 0 else 'Build of {:.1f}M barrels — supply growing.'.format(change)}",
        })
        analysis["factors"].append({"name": "Inventory", "score": inv_score, "weight": "20%"})

    # Step 3: Gasoline Demand — relaxed thresholds
    if gasoline_demand and len(gasoline_demand) >= 2:
        latest_gas = gasoline_demand[0]["value"]
        prev_gas = gasoline_demand[1]["value"]
        gas_chg = ((latest_gas - prev_gas) / prev_gas * 100) if prev_gas else 0

        gas_score = 0
        if gas_chg > 1.5:
            gas_score = 1
        elif gas_chg < -1.5:
            gas_score = -1

        total_score += gas_score
        analysis["steps"].append({
            "title": "US Gasoline Demand",
            "icon": "gas-pump",
            "value": f"{latest_gas:,.0f}K bbl/day",
            "detail": f"Week-over-week: {gas_chg:+.1f}%",
            "score": gas_score,
            "explanation": f"{'Rising demand supports oil prices.' if gas_score > 0 else 'Weakening demand pressures prices lower.' if gas_score < 0 else 'Demand holding steady.'}",
        })
        analysis["factors"].append({"name": "Demand", "score": gas_score, "weight": "15%"})

    # Step 4: US Dollar Impact
    if dxy_data:
        dxy_val = dxy_data.get("value")
        dxy_dir = dxy_data.get("direction")
        if dxy_val is not None:
            dxy_score = 0
            if dxy_dir == "up":
                dxy_score = -1
            elif dxy_dir == "down":
                dxy_score = 1

            total_score += dxy_score
            analysis["steps"].append({
                "title": "US Dollar Index (DXY)",
                "icon": "dollar-sign",
                "value": f"{dxy_val:.2f}",
                "detail": f"Trend: {'Rising' if dxy_dir == 'up' else 'Falling' if dxy_dir == 'down' else 'Flat'}",
                "score": dxy_score,
                "explanation": f"{'Strong dollar makes oil more expensive globally, reducing demand.' if dxy_score < 0 else 'Weak dollar makes oil cheaper globally, boosting demand.' if dxy_score > 0 else 'Dollar neutral — no impact on oil pricing.'}",
            })
            analysis["factors"].append({"name": "Dollar", "score": dxy_score, "weight": "15%"})

    # Step 5: WTI vs Brent Spread — relaxed thresholds
    if wti_prices and brent_value is not None:
        wti_latest = wti_prices[0]["value"]
        spread = brent_value - wti_latest
        spread_score = 0
        if spread > 6:
            spread_score = 1   # wide spread = global tightness
        elif spread > 3:
            spread_score = 0   # normal
        elif spread < 1:
            spread_score = -1  # very narrow = global weakness / US oversupply

        total_score += spread_score
        analysis["steps"].append({
            "title": "Brent-WTI Spread",
            "icon": "exchange-alt",
            "value": f"${spread:.2f}/bbl",
            "detail": f"Brent ${brent_value:.2f} vs WTI ${wti_latest:.2f}",
            "score": spread_score,
            "explanation": f"Wide spread (${spread:.1f}) — global supply tighter than US, bullish for WTI catch-up." if spread_score > 0 else ("Very narrow spread — global demand weak or US oversupplied." if spread_score < 0 else "Spread in normal range ($3-$6)."),
        })
        analysis["factors"].append({"name": "Brent-WTI Spread", "score": spread_score, "weight": "10%"})

    # Step 6: Mean Reversion — oil bounces after sharp weekly moves
    if wti_prices and len(wti_prices) >= 5:
        latest = wti_prices[0]["value"]
        week_ago = wti_prices[min(4, len(wti_prices)-1)]["value"]
        weekly_chg = ((latest - week_ago) / week_ago * 100) if week_ago else 0

        revert_score = 0
        if weekly_chg < -6:
            revert_score = 2   # oversold — expect bounce
        elif weekly_chg < -3:
            revert_score = 1
        elif weekly_chg > 6:
            revert_score = -2  # overbought — expect pullback
        elif weekly_chg > 3:
            revert_score = -1

        total_score += revert_score
        analysis["steps"].append({
            "title": "Mean Reversion Check",
            "icon": "sync-alt",
            "value": f"{weekly_chg:+.1f}% this week",
            "detail": f"{'Oversold bounce likely' if revert_score > 0 else 'Overbought pullback likely' if revert_score < 0 else 'No extreme — no reversion signal'}",
            "score": revert_score,
            "explanation": f"{'Oil dropped sharply — historical pattern shows strong bounce-back probability.' if revert_score > 0 else 'Oil surged sharply — pullback risk elevated. Take profits or wait.' if revert_score < 0 else 'Weekly move within normal range. No mean-reversion signal.'}",
        })
        analysis["factors"].append({"name": "Mean Reversion", "score": revert_score, "weight": "15%"})

    # Step 7: News Sentiment (keyword-based)
    sent_score = 0
    if news_sentiment:
        sent_score = news_sentiment.get("score", 0)
        bull_n = news_sentiment.get("bullish_count", 0)
        bear_n = news_sentiment.get("bearish_count", 0)
        neut_n = news_sentiment.get("neutral_count", 0)
        total_score += sent_score
        analysis["steps"].append({
            "title": "News Sentiment",
            "icon": "newspaper",
            "value": f"{bull_n} bullish / {bear_n} bearish / {neut_n} neutral",
            "detail": f"Scanned {bull_n + bear_n + neut_n} oil headlines for supply/demand/conflict keywords",
            "score": sent_score,
            "explanation": f"{'Headlines skew bullish — supply disruption or demand strength dominating news.' if sent_score > 0 else 'Headlines skew bearish — oversupply, demand weakness, or peace signals.' if sent_score < 0 else 'Mixed headlines — no clear directional bias from news.'}",
        })
        analysis["factors"].append({"name": "News Sentiment", "score": sent_score, "weight": "10%"})

    # Step 8: Polymarket Geopolitical Risk
    geo_score = 0
    if polymarket_data and polymarket_data.get("markets"):
        risk = polymarket_data["risk_score"]
        n_markets = len(polymarket_data["markets"])

        if risk > 0.5:
            geo_score = 2
        elif risk > 0.35:
            geo_score = 1
        elif risk < 0.15:
            geo_score = -1

        total_score += geo_score
        top_risks = sorted(polymarket_data["markets"], key=lambda m: m["oil_impact"], reverse=True)[:3]
        top_str = " | ".join(f"{m['label']} {m['yes_prob']}%" for m in top_risks)

        analysis["steps"].append({
            "title": "Geopolitical Risk (Polymarket)",
            "icon": "globe",
            "value": polymarket_data["risk_label"],
            "detail": f"Tracking {n_markets} prediction markets: {top_str}",
            "score": geo_score,
            "explanation": f"{'HIGH geopolitical risk — conflict probabilities elevated. Supply disruption risk supports oil prices.' if geo_score >= 2 else 'Moderate geopolitical tension — some risk premium in oil.' if geo_score > 0 else 'Low geopolitical risk — peace/stability reduces oil risk premium.' if geo_score < 0 else 'Geopolitical risk at baseline levels — no abnormal risk premium.'}",
        })
        analysis["factors"].append({"name": "Geopolitical", "score": geo_score, "weight": "10%"})
    else:
        # Fallback when Polymarket data unavailable
        analysis["steps"].append({
            "title": "Geopolitical Risk",
            "icon": "globe",
            "value": "Data pending",
            "detail": "Polymarket data loading — check news feed for live developments",
            "score": 0,
            "explanation": "Geopolitical risk data not yet available. Check oil news below.",
        })
        analysis["factors"].append({"name": "Geopolitical", "score": 0, "weight": "10%"})

    # Final signal — no NEUTRAL prediction (oil rarely stays flat)
    if total_score >= 3:
        analysis["signal"] = "STRONG BULLISH"
    elif total_score >= 1:
        analysis["signal"] = "BULLISH"
    elif total_score <= -3:
        analysis["signal"] = "STRONG BEARISH"
    elif total_score <= -1:
        analysis["signal"] = "BEARISH"
    else:
        # Score exactly 0 — lean toward recent momentum
        if wti_prices and len(wti_prices) >= 5:
            recent = wti_prices[0]["value"] - wti_prices[4]["value"]
            analysis["signal"] = "LEAN BULLISH" if recent > 0 else "LEAN BEARISH"
        else:
            analysis["signal"] = "NEUTRAL"

    analysis["score"] = total_score
    return analysis


def _fetch_all_oil_data() -> dict:
    """Fetch all oil-related data for the Oil Tracker tab."""
    oil_data = {
        "wti_prices": [],
        "brent_prices": [],
        "uso_prices": [],
        "inventory": [],
        "gasoline_demand": [],
        "oil_news": [],
        "polymarket": {},
        "news_sentiment": {},
        "analysis": {},
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(_fetch_eia_spot_price, "RWTC", 90): "wti_prices",
            executor.submit(_fetch_eia_spot_price, "RBRTE", 90): "brent_prices",
            executor.submit(_fetch_yahoo_chart, "USO", "3mo", "1d"): "uso_prices",
            executor.submit(_fetch_eia_series, "WCRSTUS1", 52): "inventory",
            executor.submit(_fetch_eia_weekly_supply, "WGFUPUS2", 26): "gasoline_demand",
            executor.submit(_fetch_oil_news): "oil_news",
            executor.submit(_fetch_polymarket_geopolitical): "polymarket",
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                oil_data[key] = future.result()
            except Exception as exc:
                logger.warning("Oil data fetch failed for %s: %s", key, exc)

    # Score news sentiment from fetched headlines
    if oil_data["oil_news"]:
        oil_data["news_sentiment"] = _score_news_sentiment(oil_data["oil_news"])

    # Get DXY and Brent from FRED cache, with fallback fetch
    with _lock:
        rates = _cache.get("rates", {})
    dxy_data = rates.get("DTWEXBGS", {})
    brent_value = rates.get("DCOILBRENTEU", {}).get("value")

    # Fallback: if DXY not in cache, fetch it directly
    if not dxy_data.get("value") and FRED_API_KEY:
        dxy_data = _fetch_fred_series("DTWEXBGS")

    # Fallback: use EIA Brent if FRED Brent not cached
    if brent_value is None and oil_data["brent_prices"]:
        brent_value = oil_data["brent_prices"][0]["value"]

    oil_data["analysis"] = _build_oil_analysis(
        oil_data["wti_prices"],
        oil_data["inventory"],
        oil_data["gasoline_demand"],
        dxy_data,
        brent_value,
        polymarket_data=oil_data["polymarket"],
        news_sentiment=oil_data["news_sentiment"],
    )

    return oil_data


# ---------------------------------------------------------------------------
# Time-based cache with disk persistence for fast cold starts
# ---------------------------------------------------------------------------
CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache.json")
CACHE_SECONDS = 3600  # 60 minutes — all data refreshes on same cycle

_EMPTY_CACHE: dict = {
    "rates": {},
    "news": {},
    "ubl_mentions": [],
    "exec_fx": {},
    "exec_property": [],
    "exec_ons_hpi": {},
    "exec_boe_events": [],
    "exec_news": {},
    "last_updated": None,
    "news_last_updated": None,
    "rates_fetched_at": 0,
    "news_fetched_at": 0,
    "exec_fetched_at": 0,
    "oil_data": {},
    "oil_fetched_at": 0,
}


def _load_cache_from_disk() -> dict:
    """Load cached data from disk on startup — instant page loads after restarts."""
    try:
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)
            logger.info("Loaded cache from disk (last updated: %s)", data.get("last_updated"))
            return data
    except (FileNotFoundError, json.JSONDecodeError, Exception) as exc:
        logger.info("No disk cache found (%s), starting fresh.", exc)
        return dict(_EMPTY_CACHE)


def _save_cache_to_disk():
    """Persist current cache to disk so next cold start is instant."""
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(_cache, f)
        logger.info("Cache saved to disk.")
    except Exception as exc:
        logger.warning("Failed to save cache to disk: %s", exc)


_cache: dict = _load_cache_from_disk()
_lock = Lock()


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
        url = f"https://news.google.com/rss/search?q={encoded}+when:7d&hl=en-GB&gl=GB&ceid=GB:en"
        return _parse_rss(url, max_items)
    except Exception as exc:
        logger.warning("Failed Google News search for %s: %s", query, exc)
        return []


def _fetch_single_rate(series_id: str, label: str) -> tuple:
    """Fetch a single rate — used by thread pool."""
    ecb_live_map = {
        "ECB_DFR": ("DFR", "ECBDFR"),
        "ECB_MRR": ("MRR_FR", "ECBMRRFR"),
        "ECB_MLF": ("MLFR", "ECBMLFR"),
    }
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
    data["freshness"] = _check_staleness(data.get("date"))
    return (series_id, data)


def _do_refresh_rates():
    """Fetch all rates in parallel using thread pool."""
    logger.info("Refreshing rate data (parallel)…")
    rates = {}

    # Fetch all series in parallel (10 threads)
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(_fetch_single_rate, sid, label): sid
            for sid, label in ALL_SERIES.items()
        }
        for future in as_completed(futures):
            try:
                series_id, data = future.result()
                rates[series_id] = data
            except Exception as exc:
                sid = futures[future]
                logger.warning("Failed to fetch %s: %s", sid, exc)

    # Apply manual overrides
    override_map = {
        "IR3TIB01JPM156N": "BOJ_POLICY",
        "IR3TIB01CHM156N": "SNB_POLICY",
    }
    for fred_id, override_key in override_map.items():
        if fred_id in rates and override_key in MANUAL_OVERRIDES:
            fred_data = rates[fred_id]
            override = MANUAL_OVERRIDES[override_key]
            fred_data["official_rate"] = override["value"]
            fred_data["official_date"] = override["date"]
            fred_data["official_source"] = override["source"]
            fred_data["official_label"] = override["label"]

    with _lock:
        _cache["rates"] = rates
        _cache["rates_fetched_at"] = time.time()
        _cache["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info("Rate data refresh complete.")


def _do_refresh_all():
    """Refresh all data sources — rates, news, exec data. Runs in background thread."""
    _do_refresh_rates()
    _ensure_news()
    # Exec brief data
    _cache["exec_fx"] = _fetch_exchange_rates()
    _cache["exec_property"] = _fetch_london_property()
    _cache["exec_ons_hpi"] = _fetch_ons_hpi()
    _cache["exec_boe_events"] = _fetch_boe_events()
    _cache["exec_news"] = _fetch_exec_news()
    _cache["exec_fetched_at"] = time.time()
    logger.info("All data refresh complete.")
    _save_cache_to_disk()


_bg_refresh_running = False


def _ensure_data_background():
    """Kick off a background refresh if cache is empty/stale. Non-blocking."""
    global _bg_refresh_running
    now = time.time()
    rates_stale = now - _cache["rates_fetched_at"] > CACHE_SECONDS or not _cache["rates"]
    news_stale = now - _cache["news_fetched_at"] > CACHE_SECONDS or not _cache["news"]
    exec_stale = now - _cache.get("exec_fetched_at", 0) > CACHE_SECONDS or not _cache.get("exec_fx")

    if (rates_stale or news_stale or exec_stale) and not _bg_refresh_running:
        _bg_refresh_running = True

        def _bg_worker():
            global _bg_refresh_running
            try:
                _do_refresh_all()
            finally:
                _bg_refresh_running = False

        t = threading.Thread(target=_bg_worker, daemon=True)
        t.start()


def _ensure_rates():
    """Refresh rates if cache is stale — now parallel."""
    now = time.time()
    if now - _cache["rates_fetched_at"] < CACHE_SECONDS and _cache["rates"]:
        return
    _do_refresh_rates()


def _ensure_news():
    """Refresh news if cache is stale."""
    now = time.time()
    if now - _cache["news_fetched_at"] < CACHE_SECONDS and _cache["news"]:
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

    # --- Upcoming Calendar (removed — now live from BoE RSS) ---

    # --- Mortgage Rate Indicator ---
    mortgage = rates.get("MORTGAGE30US", {}).get("value")
    if mortgage is not None:
        summary.append({
            "icon": "🏠", "category": "US Mortgage",
            "text": f"US 30Y mortgage rate at {mortgage:.2f}%. {'Affordability under strain.' if mortgage > 7 else 'Relatively stable for borrowers.'}",
            "severity": "warning" if mortgage > 7 else "info"
        })

    # --- US Unemployment ---
    unrate = rates.get("UNRATE", {}).get("value")
    if unrate is not None:
        if unrate > 5:
            summary.append({
                "icon": "⚠️", "category": "US Labor Market",
                "text": f"US unemployment at {unrate:.1f}% — rising above 5% threshold. Labour market softening may signal broader economic slowdown. Review credit provisioning.",
                "severity": "warning"
            })
        else:
            summary.append({
                "icon": "👷", "category": "US Labor Market",
                "text": f"US unemployment at {unrate:.1f}% — labour market remains resilient.",
                "severity": "positive" if unrate < 4 else "info"
            })

    # --- S&P 500 ---
    sp500 = rates.get("SP500", {}).get("value")
    if sp500 is not None:
        sp_direction = rates.get("SP500", {}).get("direction")
        arrow = "up" if sp_direction == "up" else ("down" if sp_direction == "down" else "flat")
        summary.append({
            "icon": "📈", "category": "US Equities",
            "text": f"S&P 500 at {sp500:,.2f} (trending {arrow}). {'Risk-on sentiment prevails.' if sp_direction == 'up' else 'Monitor for sustained weakness.' if sp_direction == 'down' else 'Range-bound trading.'}",
            "severity": "info"
        })

    # --- US Property (Case-Shiller) ---
    case_shiller = rates.get("CSUSHPISA", {}).get("value")
    if case_shiller is not None:
        cs_direction = rates.get("CSUSHPISA", {}).get("direction")
        summary.append({
            "icon": "🏘️", "category": "US Property",
            "text": f"Case-Shiller Home Price Index at {case_shiller:,.2f} — {'prices rising, watch for overheating.' if cs_direction == 'up' else 'price momentum cooling.' if cs_direction == 'down' else 'prices stable.'}",
            "severity": "info"
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
    # Non-blocking: kick off background refresh, render with whatever we have
    _ensure_data_background()

    with _lock:
        rates = dict(_cache["rates"])
        news = dict(_cache["news"])
        ubl_mentions = list(_cache["ubl_mentions"])
        exec_fx = dict(_cache["exec_fx"])
        exec_property = list(_cache["exec_property"])
        exec_ons_hpi = dict(_cache["exec_ons_hpi"])
        exec_boe_events = list(_cache["exec_boe_events"])
        exec_news = dict(_cache["exec_news"])
        last_updated = _cache["last_updated"]
        news_last_updated = _cache["news_last_updated"]

    grouped: dict[str, list[dict]] = {}
    for group_key, series_map in SERIES.items():
        if group_key == "global_rates_fred":
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
        exec_property=exec_property,
        exec_ons_hpi=exec_ons_hpi,
        exec_boe_events=exec_boe_events,
        exec_news=exec_news,
        exec_leadership_tip=_get_leadership_tip(),
        last_updated=last_updated,
        news_last_updated=news_last_updated,
        city_monitor=CITY_MONITOR,
    )


@app.route("/api/status")
@login_required
def api_status():
    """Return data loading status — used by frontend to know when to refresh."""
    return jsonify({
        "has_rates": bool(_cache["rates"]),
        "has_news": bool(_cache["news"]),
        "has_exec": bool(_cache.get("exec_fx")),
        "loading": _bg_refresh_running,
        "last_updated": _cache["last_updated"],
    })


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


@app.route("/api/city/<city_id>")
@login_required
def api_city(city_id):
    """Return city-specific data — metrics + news. Fetched on demand, cached."""
    city = CITY_MONITOR.get(city_id)
    if not city:
        return jsonify({"error": "Unknown city"}), 404

    cache_key = f"city_{city_id}"
    cached = _cache.get(cache_key)
    now = time.time()
    if cached and now - cached.get("fetched_at", 0) < CACHE_SECONDS:
        return jsonify(cached["data"])

    # Fetch FRED metrics in parallel
    metrics = []
    if city.get("fred_series"):
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(_fetch_fred_series, sid): (sid, label)
                for sid, label in city["fred_series"].items()
            }
            for future in as_completed(futures):
                sid, label = futures[future]
                try:
                    data = future.result()
                    data["id"] = sid
                    data["label"] = label
                    metrics.append(data)
                except Exception:
                    metrics.append({"id": sid, "label": label, "value": None, "date": None, "direction": None})

    # Fetch London Land Registry if applicable
    property_data = []
    if city.get("use_land_registry"):
        property_data = _fetch_london_property()

    # Fetch city news
    hl = "en-GB&gl=GB&ceid=GB:en" if city_id in ("london", "manchester") else "en-US&gl=US&ceid=US:en"
    news_url = f"https://news.google.com/rss/search?q={city['news_query']}+when:7d&hl={hl}"
    property_url = f"https://news.google.com/rss/search?q={city['property_query']}+when:7d&hl={hl}"

    news_items = _parse_rss(news_url, max_items=8)
    property_news = _parse_rss(property_url, max_items=6)

    result = {
        "city": city["name"],
        "flag": city["flag"],
        "metrics": metrics,
        "property_data": property_data,
        "news": news_items,
        "property_news": property_news,
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }

    _cache[cache_key] = {"data": result, "fetched_at": now}
    return jsonify(result)


@app.route("/api/oil")
@login_required
def api_oil():
    """Return oil tracker data — WTI prices, inventory, analysis, news."""
    now = time.time()
    cached = _cache.get("oil_data")
    if cached and now - _cache.get("oil_fetched_at", 0) < CACHE_SECONDS:
        return jsonify(cached)

    oil_data = _fetch_all_oil_data()
    with _lock:
        _cache["oil_data"] = oil_data
        _cache["oil_fetched_at"] = now
    return jsonify(oil_data)


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
