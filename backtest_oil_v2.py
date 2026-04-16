#!/usr/bin/env python3
"""
Oil Prediction Model v2 — Full 9-Factor Backtest
=================================================
Uses Yahoo Finance futures (CL=F, BZ=F) + EIA spot + all 9 scoring
factors including Spot-Futures Divergence, News Sentiment (simulated),
and Geopolitical (constant baseline).

Tests weekly predictions over 12 months of data.
"""

import os
import sys
import requests
import json
from datetime import datetime, timedelta, timezone
from collections import defaultdict

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

EIA_API_KEY = os.getenv("EIA_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")

if not EIA_API_KEY:
    print("ERROR: EIA_API_KEY not set"); sys.exit(1)


# ---------------------------------------------------------------------------
# Data Fetchers
# ---------------------------------------------------------------------------
def fetch_yahoo(symbol, range_str="1y"):
    r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
        params={"range": range_str, "interval": "1d"},
        headers={"User-Agent": "CEOMonitor/1.0"}, timeout=20)
    r.raise_for_status()
    result = r.json()["chart"]["result"][0]
    ts = result.get("timestamp", [])
    closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
    out = []
    for t, c in zip(ts, closes):
        if c is not None:
            out.append({"date": datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d"), "value": round(c, 2)})
    return out  # oldest first


def fetch_eia_spot(series_id, num=300):
    r = requests.get("https://api.eia.gov/v2/petroleum/pri/spt/data/", params={
        "api_key": EIA_API_KEY, "frequency": "daily", "data[0]": "value",
        "facets[series][]": series_id, "sort[0][column]": "period",
        "sort[0][direction]": "desc", "length": num,
    }, timeout=20)
    r.raise_for_status()
    raw = r.json().get("response", {}).get("data", [])
    return [{"date": row["period"], "value": float(row["value"])} for row in raw
            if row.get("value") is not None]  # newest first


def fetch_eia_weekly(endpoint, series_id, num=60):
    url = f"https://api.eia.gov/v2/petroleum/{endpoint}/data/"
    r = requests.get(url, params={
        "api_key": EIA_API_KEY, "frequency": "weekly", "data[0]": "value",
        "facets[series][]": series_id, "sort[0][column]": "period",
        "sort[0][direction]": "desc", "length": num,
    }, timeout=20)
    r.raise_for_status()
    raw = r.json().get("response", {}).get("data", [])
    return [{"date": row["period"], "value": float(row["value"])} for row in raw
            if row.get("value") is not None]  # newest first


def fetch_fred(series_id, num=300):
    if not FRED_API_KEY: return []
    start = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
    r = requests.get("https://api.stlouisfed.org/fred/series/observations", params={
        "series_id": series_id, "api_key": FRED_API_KEY, "file_type": "json",
        "sort_order": "desc", "observation_start": start, "limit": num,
    }, timeout=20)
    r.raise_for_status()
    return [{"date": o["date"], "value": float(o["value"])}
            for o in r.json().get("observations", [])
            if o.get("value") not in (None, ".")]  # newest first


# ---------------------------------------------------------------------------
# Scoring Functions — exact match to app.py v2 (9 factors)
# ---------------------------------------------------------------------------
def score_price_trend(prices, idx, lookback_week=5, lookback_month=20):
    """Score from prices list (oldest-first), looking back from idx."""
    if idx < lookback_week:
        return 0
    latest = prices[idx]
    week_ago = prices[max(0, idx - lookback_week)]
    month_ago = prices[max(0, idx - lookback_month)]
    weekly_chg = ((latest - week_ago) / week_ago * 100) if week_ago else 0
    monthly_chg = ((latest - month_ago) / month_ago * 100) if month_ago else 0

    score = 0
    if weekly_chg > 4: score = 2
    elif weekly_chg > 1.5: score = 1
    elif weekly_chg < -4: score = -2
    elif weekly_chg < -1.5: score = -1

    if monthly_chg > 5 and score >= 0: score += 1
    elif monthly_chg < -5 and score <= 0: score -= 1
    score = max(-2, min(2, score))
    return score


def score_inventory(inv_values):
    """inv_values = last 3 values (newest first, in thousands of barrels)."""
    if len(inv_values) < 3:
        return 0
    latest = inv_values[0] / 1000
    prev = inv_values[1] / 1000
    two_back = inv_values[2] / 1000
    change_3w = latest - two_back
    if change_3w < -4: return 2
    elif change_3w < -1: return 1
    elif change_3w > 4: return -2
    elif change_3w > 1: return -1
    return 0


def score_gasoline(gas_values):
    """gas_values = last 2 values (newest first)."""
    if len(gas_values) < 2:
        return 0
    chg = ((gas_values[0] - gas_values[1]) / gas_values[1] * 100) if gas_values[1] else 0
    if chg > 1: return 1
    elif chg < -1: return -1
    return 0


def score_dollar(dxy_values):
    """dxy_values = last 2 values (newest first)."""
    if len(dxy_values) < 2:
        return 0
    if dxy_values[0] > dxy_values[1]: return -1  # rising = bearish for oil
    elif dxy_values[0] < dxy_values[1]: return 1
    return 0


def score_spread(wti_price, brent_price):
    if wti_price is None or brent_price is None:
        return 0
    spread = brent_price - wti_price
    if spread > 6: return 1
    elif spread < 1: return -1
    return 0


def score_spot_futures(eia_spot, futures_price):
    """Score EIA spot vs Yahoo futures divergence."""
    if eia_spot is None or futures_price is None or futures_price == 0:
        return 0
    div_pct = ((eia_spot - futures_price) / futures_price * 100)
    if div_pct > 10: return 2
    elif div_pct > 4: return 1
    elif div_pct < -10: return -2
    elif div_pct < -4: return -1
    return 0


def score_mean_reversion(prices, idx, lookback=5):
    if idx < lookback:
        return 0
    latest = prices[idx]
    prev = prices[max(0, idx - lookback)]
    chg = ((latest - prev) / prev * 100) if prev else 0
    if chg < -6: return 2
    elif chg < -3: return 1
    elif chg > 6: return -2
    elif chg > 3: return -1
    return 0


def compute_signal(total_score, prices=None, idx=None):
    if total_score >= 3: return "STRONG BULLISH"
    elif total_score >= 1: return "BULLISH"
    elif total_score <= -3: return "STRONG BEARISH"
    elif total_score <= -1: return "BEARISH"
    else:
        if prices and idx and idx >= 5:
            return "LEAN BULLISH" if prices[idx] > prices[idx-5] else "LEAN BEARISH"
        return "NEUTRAL"


# ---------------------------------------------------------------------------
# Backtest
# ---------------------------------------------------------------------------
def get_nearest_before(date_idx, target_date, lookback=7):
    """Find value on or before target_date."""
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    for offset in range(lookback + 1):
        key = (dt - timedelta(days=offset)).strftime("%Y-%m-%d")
        if key in date_idx:
            return date_idx[key]
    return None


def run_backtest():
    print("=" * 70)
    print("OIL PREDICTION MODEL v2 — 9-FACTOR BACKTEST")
    print("Yahoo Futures + EIA Spot + Full Scoring Engine")
    print("=" * 70)
    print()

    # Fetch all data
    print("Fetching 12 months of historical data...")
    print("  Yahoo WTI futures (CL=F)...", end=" ", flush=True)
    wti_yahoo = fetch_yahoo("CL=F", "1y")
    print(f"{len(wti_yahoo)} days")

    print("  Yahoo Brent futures (BZ=F)...", end=" ", flush=True)
    brent_yahoo = fetch_yahoo("BZ=F", "1y")
    print(f"{len(brent_yahoo)} days")

    print("  EIA WTI spot (RWTC)...", end=" ", flush=True)
    eia_wti = fetch_eia_spot("RWTC", 300)
    print(f"{len(eia_wti)} days")

    print("  EIA inventory (WCRSTUS1)...", end=" ", flush=True)
    inv_raw = fetch_eia_weekly("stoc/wstk", "WCRSTUS1", 60)
    print(f"{len(inv_raw)} weeks")

    print("  EIA gasoline demand (WGFUPUS2)...", end=" ", flush=True)
    gas_raw = fetch_eia_weekly("sum/sndw", "WGFUPUS2", 60)
    print(f"{len(gas_raw)} weeks")

    print("  FRED DXY (DTWEXBGS)...", end=" ", flush=True)
    dxy_raw = fetch_fred("DTWEXBGS", 300)
    print(f"{len(dxy_raw)} days")
    print()

    # Build date indexes (all newest-first from APIs, convert to dict)
    wti_prices_of = wti_yahoo  # oldest first from Yahoo
    brent_idx = {d["date"]: d["value"] for d in brent_yahoo}
    eia_wti_idx = {d["date"]: d["value"] for d in eia_wti}
    inv_idx = {d["date"]: d["value"] for d in inv_raw}
    gas_idx = {d["date"]: d["value"] for d in gas_raw}
    dxy_idx = {d["date"]: d["value"] for d in dxy_raw}

    # Inventory dates sorted (for getting last N before a date)
    inv_dates = sorted(inv_idx.keys())
    gas_dates = sorted(gas_idx.keys())

    wti_dates = [d["date"] for d in wti_prices_of]
    wti_values = [d["value"] for d in wti_prices_of]

    # Test on every Friday (weekly) — need at least 25 days warmup
    results = []
    test_count = 0

    for i in range(25, len(wti_dates) - 5):
        test_date = wti_dates[i]
        dt = datetime.strptime(test_date, "%Y-%m-%d")

        # Only test on Fridays (or nearest weekday)
        if dt.weekday() != 4:  # 4 = Friday
            continue

        test_count += 1
        wti_price = wti_values[i]

        # 1. Price Trend
        trend_score = score_price_trend(wti_values, i)

        # 2. Inventory (get last 3 weekly values before test_date)
        inv_before = [inv_idx[d] for d in inv_dates if d <= test_date]
        inv_score = score_inventory(inv_before[-3:] if len(inv_before) >= 3 else inv_before[-2:] if len(inv_before) >= 2 else [])

        # 3. Gasoline demand
        gas_before = [gas_idx[d] for d in gas_dates if d <= test_date]
        gas_score = score_gasoline(gas_before[-2:] if len(gas_before) >= 2 else [])

        # 4. Dollar
        dxy_val = get_nearest_before(dxy_idx, test_date)
        dxy_prev = get_nearest_before(dxy_idx, (dt - timedelta(days=7)).strftime("%Y-%m-%d"))
        dxy_score = 0
        if dxy_val and dxy_prev:
            dxy_score = score_dollar([dxy_val, dxy_prev])

        # 5. Brent-WTI spread
        brent_price = get_nearest_before(brent_idx, test_date)
        spread_score = score_spread(wti_price, brent_price)

        # 6. Spot-Futures Divergence
        eia_spot = get_nearest_before(eia_wti_idx, test_date)
        sf_score = score_spot_futures(eia_spot, wti_price)

        # 7. Mean Reversion
        revert_score = score_mean_reversion(wti_values, i)

        # 8. News Sentiment — we can't backtest this (no historical headlines)
        # Use 0 (neutral) as baseline — conservative
        news_score = 0

        # 9. Geopolitical — constant baseline (can't backtest Polymarket history)
        geo_score = 0

        total = trend_score + inv_score + gas_score + dxy_score + spread_score + sf_score + revert_score + news_score + geo_score
        signal = compute_signal(total, wti_values, i)

        # Actual outcome: price 5 trading days later
        future_idx = min(i + 5, len(wti_values) - 1)
        future_price = wti_values[future_idx]
        actual_chg = ((future_price - wti_price) / wti_price * 100)
        actual_dir = "UP" if future_price > wti_price else ("DOWN" if future_price < wti_price else "FLAT")

        if "BULLISH" in signal:
            predicted_dir = "UP"
        elif "BEARISH" in signal:
            predicted_dir = "DOWN"
        else:
            predicted_dir = "FLAT"

        correct = (predicted_dir == actual_dir)

        results.append({
            "date": test_date, "price": wti_price, "future_price": future_price,
            "pct_chg": actual_chg, "total": total, "signal": signal,
            "predicted": predicted_dir, "actual": actual_dir, "correct": correct,
            "trend": trend_score, "inv": inv_score, "gas": gas_score,
            "dxy": dxy_score, "spread": spread_score, "sf": sf_score,
            "revert": revert_score, "news": news_score, "geo": geo_score,
        })

    # Print results
    print(f"Tested {len(results)} weekly periods (Fridays)")
    print(f"Date range: {results[0]['date']} to {results[-1]['date']}")
    print("-" * 100)
    print(f"{'Date':<12} {'WTI':>8} {'NxtWk':>8} {'Chg%':>7} {'Score':>6} {'Signal':<18} {'Pred':>5} {'Act':>5} {'':>6} | Trnd Inv Gas DXY Sprd S-F  Rev")
    print("-" * 100)

    for r in results:
        tick = " OK " if r["correct"] else "MISS"
        c = "\033[92m" if r["correct"] else "\033[91m"
        x = "\033[0m"
        print(f"{r['date']:<12} ${r['price']:>7.2f} ${r['future_price']:>7.2f} {r['pct_chg']:>+6.1f}% {r['total']:>+5d}  {r['signal']:<18} {r['predicted']:>5} {r['actual']:>5}  {c}{tick}{x} | {r['trend']:>+2d}  {r['inv']:>+2d}  {r['gas']:>+2d}  {r['dxy']:>+2d}  {r['spread']:>+2d}  {r['sf']:>+2d}  {r['revert']:>+2d}")

    # Summary
    print()
    print("=" * 70)
    print("BACKTEST SUMMARY")
    print("=" * 70)

    total_n = len(results)
    correct_n = sum(1 for r in results if r["correct"])
    accuracy = (correct_n / total_n * 100) if total_n else 0

    print(f"Total weeks:          {total_n}")
    print(f"Correct:              {correct_n}")
    print(f"Accuracy:             {accuracy:.1f}%")
    print()

    # By signal
    print("By Signal:")
    print(f"  {'Signal':<20} {'Count':>6} {'Correct':>8} {'Accuracy':>9}")
    print(f"  {'-'*48}")
    sig_groups = defaultdict(lambda: {"n": 0, "ok": 0})
    for r in results:
        sg = sig_groups[r["signal"]]
        sg["n"] += 1
        if r["correct"]: sg["ok"] += 1
    for sig in ["STRONG BULLISH", "BULLISH", "LEAN BULLISH", "LEAN BEARISH", "BEARISH", "STRONG BEARISH"]:
        sg = sig_groups.get(sig, {"n": 0, "ok": 0})
        if sg["n"] > 0:
            print(f"  {sig:<20} {sg['n']:>6} {sg['ok']:>8} {sg['ok']/sg['n']*100:>8.1f}%")
    print()

    # By direction
    print("By Predicted Direction:")
    dir_groups = defaultdict(lambda: {"n": 0, "ok": 0})
    for r in results:
        dg = dir_groups[r["predicted"]]
        dg["n"] += 1
        if r["correct"]: dg["ok"] += 1
    for d in ["UP", "DOWN"]:
        dg = dir_groups.get(d, {"n": 0, "ok": 0})
        if dg["n"] > 0:
            print(f"  Predicted {d:<5}: {dg['ok']}/{dg['n']} = {dg['ok']/dg['n']*100:.1f}%")
    print()

    # Factor contribution
    print("Factor Activity & Contribution:")
    factors = [("Price Trend", "trend"), ("Inventory", "inv"), ("Gasoline", "gas"),
               ("Dollar", "dxy"), ("Brent-WTI Spread", "spread"),
               ("Spot-Futures", "sf"), ("Mean Reversion", "revert"),
               ("News (baseline)", "news"), ("Geopolitical (baseline)", "geo")]
    for name, key in factors:
        vals = [r[key] for r in results]
        avg = sum(vals) / len(vals)
        active = sum(1 for v in vals if v != 0)
        # Accuracy when this factor is bullish vs bearish
        bull_correct = sum(1 for r in results if r[key] > 0 and r["actual"] == "UP")
        bull_total = sum(1 for r in results if r[key] > 0)
        bear_correct = sum(1 for r in results if r[key] < 0 and r["actual"] == "DOWN")
        bear_total = sum(1 for r in results if r[key] < 0)
        bull_acc = f"{bull_correct/bull_total*100:.0f}%" if bull_total else "N/A"
        bear_acc = f"{bear_correct/bear_total*100:.0f}%" if bear_total else "N/A"
        print(f"  {name:<20} avg={avg:>+5.2f}  active={active:>2}/{total_n}  bull_acc={bull_acc:>4}  bear_acc={bear_acc:>4}")
    print()

    # P&L
    print("Hypothetical P&L (following signals):")
    total_pnl = 0
    wins = 0
    losses = 0
    trades = 0
    for r in results:
        if "NEUTRAL" in r["signal"]:
            continue
        trades += 1
        if "BULLISH" in r["signal"]:
            pnl = r["pct_chg"]
        else:
            pnl = -r["pct_chg"]
        total_pnl += pnl
        if pnl > 0: wins += 1
        else: losses += 1
    if trades > 0:
        print(f"  Trades: {trades} (skipped {total_n - trades} neutral)")
        print(f"  Wins: {wins} ({wins/trades*100:.1f}%)")
        print(f"  Losses: {losses}")
        print(f"  Cumulative return: {total_pnl:+.1f}%")
        print(f"  Avg per trade: {total_pnl/trades:+.2f}%")
    print()

    # Score distribution
    print("Score Distribution:")
    score_hist = defaultdict(int)
    for r in results:
        score_hist[r["total"]] += 1
    for s in sorted(score_hist.keys()):
        bar = "#" * score_hist[s]
        print(f"  {s:>+3d}: {bar} ({score_hist[s]})")
    print()

    # Spot-Futures Divergence deep dive
    print("=" * 70)
    print("SPOT-FUTURES DIVERGENCE — DEEP DIVE")
    print("=" * 70)
    sf_weeks = [(r["date"], r["sf"], r["price"], r["pct_chg"], r["correct"]) for r in results if r["sf"] != 0]
    print(f"Active in {len(sf_weeks)}/{total_n} weeks")
    if sf_weeks:
        sf_bull = [r for r in results if r["sf"] > 0]
        sf_bear = [r for r in results if r["sf"] < 0]
        if sf_bull:
            sf_bull_up = sum(1 for r in sf_bull if r["actual"] == "UP")
            sf_bull_avg = sum(r["pct_chg"] for r in sf_bull) / len(sf_bull)
            print(f"  When SF bullish (+1/+2): {sf_bull_up}/{len(sf_bull)} went UP ({sf_bull_up/len(sf_bull)*100:.1f}%), avg next-week chg: {sf_bull_avg:+.2f}%")
        if sf_bear:
            sf_bear_dn = sum(1 for r in sf_bear if r["actual"] == "DOWN")
            sf_bear_avg = sum(r["pct_chg"] for r in sf_bear) / len(sf_bear)
            print(f"  When SF bearish (-1/-2): {sf_bear_dn}/{len(sf_bear)} went DOWN ({sf_bear_dn/len(sf_bear)*100:.1f}%), avg next-week chg: {sf_bear_avg:+.2f}%")
    print()

    # Mean Reversion deep dive
    print("=" * 70)
    print("MEAN REVERSION — DEEP DIVE")
    print("=" * 70)
    rev_bull = [r for r in results if r["revert"] > 0]
    rev_bear = [r for r in results if r["revert"] < 0]
    if rev_bull:
        rev_bull_up = sum(1 for r in rev_bull if r["actual"] == "UP")
        rev_bull_avg = sum(r["pct_chg"] for r in rev_bull) / len(rev_bull)
        print(f"  Oversold bounces (+1/+2): {rev_bull_up}/{len(rev_bull)} went UP ({rev_bull_up/len(rev_bull)*100:.1f}%), avg: {rev_bull_avg:+.2f}%")
    if rev_bear:
        rev_bear_dn = sum(1 for r in rev_bear if r["actual"] == "DOWN")
        rev_bear_avg = sum(r["pct_chg"] for r in rev_bear) / len(rev_bear)
        print(f"  Overbought pullbacks (-1/-2): {rev_bear_dn}/{len(rev_bear)} went DOWN ({rev_bear_dn/len(rev_bear)*100:.1f}%), avg: {rev_bear_avg:+.2f}%")
    print()

    print("=" * 70)
    print("Backtest complete.")


if __name__ == "__main__":
    run_backtest()
