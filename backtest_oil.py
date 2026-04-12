#!/usr/bin/env python3
"""
Oil Prediction Model — Backtest & Validation
=============================================
Fetches 12 months of historical EIA data, replays the scoring engine
week-by-week, and checks if the signal predicted the correct price
direction for the following week.

Run:  python3 backtest_oil.py
"""

import os
import sys
import requests
from datetime import datetime, timedelta
from collections import defaultdict

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

EIA_API_KEY = os.getenv("EIA_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")

if not EIA_API_KEY:
    print("ERROR: EIA_API_KEY not set in .env")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Data Fetchers (standalone — no Flask dependency)
# ---------------------------------------------------------------------------
def fetch_eia_spot(series_id, num=300):
    """Fetch daily EIA spot prices."""
    url = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
    params = {
        "api_key": EIA_API_KEY, "frequency": "daily",
        "data[0]": "value", "facets[series][]": series_id,
        "sort[0][column]": "period", "sort[0][direction]": "desc",
        "length": num,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    raw = resp.json().get("response", {}).get("data", [])
    result = []
    for row in raw:
        try:
            result.append({"date": row["period"], "value": float(row["value"])})
        except (ValueError, TypeError, KeyError):
            continue
    return result  # newest first


def fetch_eia_inventory(series_id, num=60):
    """Fetch weekly EIA inventory."""
    url = "https://api.eia.gov/v2/petroleum/stoc/wstk/data/"
    params = {
        "api_key": EIA_API_KEY, "frequency": "weekly",
        "data[0]": "value", "facets[series][]": series_id,
        "sort[0][column]": "period", "sort[0][direction]": "desc",
        "length": num,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    raw = resp.json().get("response", {}).get("data", [])
    result = []
    for row in raw:
        try:
            result.append({"date": row["period"], "value": float(row["value"])})
        except (ValueError, TypeError, KeyError):
            continue
    return result


def fetch_eia_supply(series_id, num=60):
    """Fetch weekly EIA supply/demand."""
    url = "https://api.eia.gov/v2/petroleum/sum/sndw/data/"
    params = {
        "api_key": EIA_API_KEY, "frequency": "weekly",
        "data[0]": "value", "facets[series][]": series_id,
        "sort[0][column]": "period", "sort[0][direction]": "desc",
        "length": num,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    raw = resp.json().get("response", {}).get("data", [])
    result = []
    for row in raw:
        try:
            result.append({"date": row["period"], "value": float(row["value"])})
        except (ValueError, TypeError, KeyError):
            continue
    return result


def fetch_fred(series_id, num=300):
    """Fetch FRED observations."""
    if not FRED_API_KEY:
        return []
    url = "https://api.stlouisfed.org/fred/series/observations"
    start = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
    params = {
        "series_id": series_id, "api_key": FRED_API_KEY,
        "file_type": "json", "sort_order": "desc",
        "observation_start": start, "limit": num,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    obs = resp.json().get("observations", [])
    result = []
    for o in obs:
        try:
            result.append({"date": o["date"], "value": float(o["value"])})
        except (ValueError, TypeError):
            continue
    return result


# ---------------------------------------------------------------------------
# Scoring Engine — exact copy from app.py for consistency
# ---------------------------------------------------------------------------
def score_price_trend(wti_slice):
    """Score WTI price momentum — dual timeframe (weekly + monthly)."""
    if len(wti_slice) < 5:
        return 0, {}
    latest = wti_slice[0]["value"]
    week_ago = wti_slice[min(4, len(wti_slice)-1)]["value"]
    month_ago = wti_slice[min(20, len(wti_slice)-1)]["value"] if len(wti_slice) > 20 else week_ago
    weekly_chg = ((latest - week_ago) / week_ago * 100) if week_ago else 0
    monthly_chg = ((latest - month_ago) / month_ago * 100) if month_ago else 0

    score = 0
    if weekly_chg > 4:
        score = 2
    elif weekly_chg > 1.5:
        score = 1
    elif weekly_chg < -4:
        score = -2
    elif weekly_chg < -1.5:
        score = -1

    # Monthly trend adds conviction when aligned
    if monthly_chg > 5 and score >= 0:
        score += 1
    elif monthly_chg < -5 and score <= 0:
        score -= 1

    score = max(-2, min(2, score))

    return score, {"weekly_chg": weekly_chg, "monthly_chg": monthly_chg, "price": latest}


def score_inventory(inv_slice):
    """Score inventory — uses 3-week cumulative trend for noise reduction."""
    if len(inv_slice) < 3:
        if len(inv_slice) >= 2:
            change = inv_slice[0]["value"] - inv_slice[1]["value"]
            score = 0
            if change < -3:
                score = 2
            elif change < -0.5:
                score = 1
            elif change > 3:
                score = -2
            elif change > 0.5:
                score = -1
            return score, {"change_1w": change, "change_3w": None, "level": inv_slice[0]["value"]}
        return 0, {}

    change_1w = inv_slice[0]["value"] - inv_slice[1]["value"]
    change_3w = inv_slice[0]["value"] - inv_slice[2]["value"]

    score = 0
    if change_3w < -4:
        score = 2
    elif change_3w < -1:
        score = 1
    elif change_3w > 4:
        score = -2
    elif change_3w > 1:
        score = -1

    return score, {"change_1w": change_1w, "change_3w": change_3w, "level": inv_slice[0]["value"]}


def score_gasoline(gas_slice):
    """Score gasoline demand — relaxed thresholds."""
    if len(gas_slice) < 2:
        return 0, {}
    chg = ((gas_slice[0]["value"] - gas_slice[1]["value"]) / gas_slice[1]["value"] * 100) if gas_slice[1]["value"] else 0
    score = 0
    if chg > 1.5:
        score = 1
    elif chg < -1.5:
        score = -1
    return score, {"chg_pct": chg, "demand": gas_slice[0]["value"]}


def score_dollar(dxy_slice):
    """Score DXY direction."""
    if len(dxy_slice) < 2:
        return 0, {}
    latest = dxy_slice[0]["value"]
    prev = dxy_slice[1]["value"]
    direction = "up" if latest > prev else ("down" if latest < prev else "flat")
    score = 0
    if direction == "up":
        score = -1
    elif direction == "down":
        score = 1
    return score, {"value": latest, "direction": direction}


def score_spread(wti_price, brent_price):
    """Score Brent-WTI spread — relaxed thresholds."""
    if wti_price is None or brent_price is None:
        return 0, {}
    spread = brent_price - wti_price
    score = 0
    if spread > 6:
        score = 1
    elif spread < 1:
        score = -1
    return score, {"spread": spread}


def compute_signal(total_score, wti_slice=None):
    if total_score >= 3:
        return "STRONG BULLISH"
    elif total_score >= 1:
        return "BULLISH"
    elif total_score <= -3:
        return "STRONG BEARISH"
    elif total_score <= -1:
        return "BEARISH"
    else:
        # Score exactly 0 — lean toward recent momentum
        if wti_slice and len(wti_slice) >= 5:
            recent = wti_slice[0]["value"] - wti_slice[4]["value"]
            return "LEAN BULLISH" if recent > 0 else "LEAN BEARISH"
        return "NEUTRAL"


# ---------------------------------------------------------------------------
# Backtest Engine
# ---------------------------------------------------------------------------
def build_date_index(data_list):
    """Convert list of {date, value} to dict keyed by date string."""
    return {d["date"]: d["value"] for d in data_list}


def get_nearest(date_idx, target_date, lookback_days=5):
    """Find the nearest value on or before target_date."""
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    for offset in range(lookback_days + 1):
        key = (dt - timedelta(days=offset)).strftime("%Y-%m-%d")
        if key in date_idx:
            return date_idx[key]
    return None


def get_slice_before(data_list_oldest_first, target_date, num):
    """Get the last `num` entries on or before target_date, returned newest-first."""
    cutoff = target_date
    filtered = [d for d in data_list_oldest_first if d["date"] <= cutoff]
    return list(reversed(filtered[-num:]))  # newest first


def run_backtest():
    print("=" * 70)
    print("OIL PREDICTION MODEL — BACKTEST")
    print("=" * 70)
    print()

    # --- Fetch Data ---
    print("Fetching historical data from EIA + FRED...")
    print("  - WTI daily prices (300 days)...", end=" ", flush=True)
    wti_raw = fetch_eia_spot("RWTC", 300)
    print(f"got {len(wti_raw)} points")

    print("  - Brent daily prices (300 days)...", end=" ", flush=True)
    brent_raw = fetch_eia_spot("RBRTE", 300)
    print(f"got {len(brent_raw)} points")

    print("  - US crude inventory (60 weeks)...", end=" ", flush=True)
    inv_raw = fetch_eia_inventory("WCRSTUS1", 60)
    print(f"got {len(inv_raw)} points")

    print("  - Gasoline demand (60 weeks)...", end=" ", flush=True)
    gas_raw = fetch_eia_supply("WGFUPUS2", 60)
    print(f"got {len(gas_raw)} points")

    print("  - US Dollar Index from FRED...", end=" ", flush=True)
    dxy_raw = fetch_fred("DTWEXBGS", 300)
    print(f"got {len(dxy_raw)} points")
    print()

    if len(wti_raw) < 30:
        print("ERROR: Not enough WTI data for backtest")
        return

    # Convert to oldest-first for slicing
    wti_of = list(reversed(wti_raw))
    brent_of = list(reversed(brent_raw))
    inv_of = list(reversed(inv_raw))
    gas_of = list(reversed(gas_raw))
    dxy_of = list(reversed(dxy_raw))

    # Build date indexes for quick lookup
    wti_idx = build_date_index(wti_raw)
    brent_idx = build_date_index(brent_raw)

    # --- Generate weekly test dates ---
    # Use Fridays from the inventory dates (which are weekly)
    # We need at least 2 weeks of lookahead for each test
    test_dates = []
    if len(inv_of) > 4:
        for i in range(2, len(inv_of) - 1):  # skip first 2 for warm-up
            test_dates.append(inv_of[i]["date"])

    print(f"Running backtest on {len(test_dates)} weekly periods...")
    print(f"Date range: {test_dates[0]} to {test_dates[-1]}")
    print("-" * 70)

    # --- Run scoring for each week ---
    results = []
    for i, test_date in enumerate(test_dates):
        # Get data slices up to this date
        wti_slice = get_slice_before(wti_of, test_date, 25)
        brent_slice = get_slice_before(brent_of, test_date, 25)
        inv_slice = get_slice_before(inv_of, test_date, 4)
        gas_slice = get_slice_before(gas_of, test_date, 4)
        dxy_slice = get_slice_before(dxy_of, test_date, 5)

        if not wti_slice or len(wti_slice) < 5:
            continue

        # Score each factor
        trend_score, trend_info = score_price_trend(wti_slice)
        inv_score, inv_info = score_inventory(inv_slice)
        gas_score, gas_info = score_gasoline(gas_slice)
        dxy_score, dxy_info = score_dollar(dxy_slice)

        brent_price = brent_slice[0]["value"] if brent_slice else None
        wti_price = wti_slice[0]["value"]
        spread_score, spread_info = score_spread(wti_price, brent_price)

        # Mean reversion — bounces after sharp moves
        revert_score = 0
        if len(wti_slice) >= 5:
            wk_chg = ((wti_slice[0]["value"] - wti_slice[4]["value"]) / wti_slice[4]["value"] * 100)
            if wk_chg < -6:
                revert_score = 2
            elif wk_chg < -3:
                revert_score = 1
            elif wk_chg > 6:
                revert_score = -2
            elif wk_chg > 3:
                revert_score = -1

        total_score = trend_score + inv_score + gas_score + dxy_score + spread_score + revert_score
        signal = compute_signal(total_score, wti_slice)

        # Get actual price 5 trading days later for validation
        future_date = (datetime.strptime(test_date, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")
        future_price = get_nearest(wti_idx, future_date, lookback_days=5)

        if future_price is None:
            continue

        actual_direction = "UP" if future_price > wti_price else ("DOWN" if future_price < wti_price else "FLAT")
        # With the new model, signal always leans directional
        if "BULLISH" in signal:
            predicted_direction = "UP"
        elif "BEARISH" in signal:
            predicted_direction = "DOWN"
        else:
            predicted_direction = "FLAT"

        # Did we get it right?
        correct = (predicted_direction == actual_direction)

        results.append({
            "date": test_date,
            "wti_price": wti_price,
            "future_price": future_price,
            "price_change": future_price - wti_price,
            "pct_change": (future_price - wti_price) / wti_price * 100,
            "total_score": total_score,
            "signal": signal,
            "predicted": predicted_direction,
            "actual": actual_direction,
            "correct": correct,
            "trend_score": trend_score,
            "inv_score": inv_score,
            "gas_score": gas_score,
            "dxy_score": dxy_score,
            "spread_score": spread_score,
            "revert_score": revert_score,
        })

    # --- Print Results ---
    print()
    print(f"{'Date':<12} {'WTI':>8} {'Next Wk':>8} {'Chg%':>7} {'Score':>6} {'Signal':<16} {'Pred':>5} {'Actual':>6} {'Result':>8}")
    print("-" * 90)

    for r in results:
        tick = "  OK" if r["correct"] else "MISS"
        color = ""
        reset = ""
        if r["correct"]:
            color = "\033[92m"  # green
            reset = "\033[0m"
        else:
            color = "\033[91m"  # red
            reset = "\033[0m"

        print(f"{r['date']:<12} ${r['wti_price']:>7.2f} ${r['future_price']:>7.2f} {r['pct_change']:>+6.1f}% {r['total_score']:>+5d}  {r['signal']:<16} {r['predicted']:>5} {r['actual']:>6}  {color}{tick}{reset}")

    # --- Summary Statistics ---
    print()
    print("=" * 70)
    print("BACKTEST SUMMARY")
    print("=" * 70)

    total = len(results)
    correct_count = sum(1 for r in results if r["correct"])
    accuracy = (correct_count / total * 100) if total else 0

    print(f"Total weeks tested:     {total}")
    print(f"Correct predictions:    {correct_count}")
    print(f"Overall accuracy:       {accuracy:.1f}%")
    print()

    # Accuracy by signal type
    signal_groups = defaultdict(lambda: {"total": 0, "correct": 0})
    for r in results:
        sg = signal_groups[r["signal"]]
        sg["total"] += 1
        if r["correct"]:
            sg["correct"] += 1

    print("Accuracy by Signal:")
    print(f"  {'Signal':<20} {'Count':>6} {'Correct':>8} {'Accuracy':>9}")
    print(f"  {'-'*45}")
    for sig in ["STRONG BULLISH", "BULLISH", "NEUTRAL", "BEARISH", "STRONG BEARISH"]:
        sg = signal_groups.get(sig, {"total": 0, "correct": 0})
        if sg["total"] > 0:
            acc = sg["correct"] / sg["total"] * 100
            print(f"  {sig:<20} {sg['total']:>6} {sg['correct']:>8} {acc:>8.1f}%")
    print()

    # Accuracy by direction
    dir_groups = defaultdict(lambda: {"total": 0, "correct": 0})
    for r in results:
        dg = dir_groups[r["predicted"]]
        dg["total"] += 1
        if r["correct"]:
            dg["correct"] += 1

    print("Accuracy by Predicted Direction:")
    for d in ["UP", "DOWN", "FLAT"]:
        dg = dir_groups.get(d, {"total": 0, "correct": 0})
        if dg["total"] > 0:
            acc = dg["correct"] / dg["total"] * 100
            print(f"  Predicted {d:<5}: {dg['correct']}/{dg['total']} = {acc:.1f}%")
    print()

    # Factor contribution analysis
    print("Factor Score Distribution:")
    for factor_name, factor_key in [("Price Trend", "trend_score"), ("Inventory", "inv_score"),
                                     ("Gasoline Demand", "gas_score"), ("Dollar (DXY)", "dxy_score"),
                                     ("Brent-WTI Spread", "spread_score"), ("Mean Reversion", "revert_score")]:
        scores = [r[factor_key] for r in results]
        avg = sum(scores) / len(scores) if scores else 0
        nonzero = sum(1 for s in scores if s != 0)
        print(f"  {factor_name:<20} avg={avg:>+5.2f}  active={nonzero}/{len(scores)}")

    print()

    # Profitability analysis — if you followed the signals
    print("Hypothetical P&L (following signals):")
    total_pnl = 0
    winning_trades = 0
    losing_trades = 0
    trades_taken = 0
    for r in results:
        if r["signal"] == "NEUTRAL":
            continue  # stay flat
        trades_taken += 1
        if r["total_score"] > 0:  # bullish — long
            pnl = r["pct_change"]
        else:  # bearish — short
            pnl = -r["pct_change"]
        total_pnl += pnl
        if pnl > 0:
            winning_trades += 1
        else:
            losing_trades += 1

    if trades_taken > 0:
        win_rate = winning_trades / trades_taken * 100
        print(f"  Trades taken:         {trades_taken} (skipped {total - trades_taken} NEUTRAL weeks)")
        print(f"  Winning trades:       {winning_trades} ({win_rate:.1f}%)")
        print(f"  Losing trades:        {losing_trades}")
        print(f"  Cumulative return:    {total_pnl:+.1f}% (sum of weekly %)")
        print(f"  Avg return per trade: {total_pnl/trades_taken:+.2f}%")
    else:
        print("  No trades taken (all NEUTRAL)")

    print()

    # Model validation checks
    print("=" * 70)
    print("MODEL VALIDATION CHECKS")
    print("=" * 70)

    issues = []

    # Check 1: Score range
    scores = [r["total_score"] for r in results]
    min_s, max_s = min(scores), max(scores)
    print(f"  Score range:        [{min_s}, {max_s}] (expected: within [-10, +10])")
    if min_s < -10 or max_s > 10:
        issues.append("Score out of expected range [-10, +10]")

    # Check 2: Score distribution — not always the same
    unique_scores = len(set(scores))
    print(f"  Unique scores:      {unique_scores} (want >3 for meaningful variation)")
    if unique_scores <= 2:
        issues.append("Score has very low variance — model may not be discriminating")

    # Check 3: Signal distribution — not always neutral
    signal_dist = defaultdict(int)
    for r in results:
        signal_dist[r["signal"]] += 1
    print(f"  Signal distribution: {dict(signal_dist)}")
    if signal_dist.get("NEUTRAL", 0) > total * 0.8:
        issues.append("More than 80% signals are NEUTRAL — thresholds may be too tight")

    # Check 4: Accuracy should be meaningfully above random (33% for 3-way)
    print(f"  Accuracy vs random: {accuracy:.1f}% (random baseline ~50% for binary)")
    if accuracy < 45:
        issues.append(f"Accuracy ({accuracy:.1f}%) is below 45% — model may need tuning")

    # Check 5: Ensure all factors are contributing
    for factor_name, factor_key in [("trend_score", "Price Trend"), ("inv_score", "Inventory"),
                                     ("gas_score", "Gasoline"), ("dxy_score", "Dollar"),
                                     ("spread_score", "Spread"), ("revert_score", "Mean Reversion")]:
        nonzero = sum(1 for r in results if r[factor_name] != 0)
        if nonzero == 0:
            issues.append(f"Factor '{factor_key}' is always 0 — never triggers")
        elif nonzero < total * 0.1:
            issues.append(f"Factor '{factor_key}' triggers only {nonzero}/{total} times — may be too strict")

    print()
    if issues:
        print("  ISSUES FOUND:")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print("  All validation checks PASSED")

    print()
    print("=" * 70)
    print("Backtest complete.")


if __name__ == "__main__":
    run_backtest()
