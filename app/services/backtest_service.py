# app/services/backtest_service.py

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Tuple
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class STMBacktestRequest:
    ticker:        str
    starting_cash: float
    k:             float
    stepsize:      float
    bars:          List[Dict[str, Any]]


REQUIRED_COLUMNS = ["Date", "Time", "Open", "High", "Low", "Close"]


def bars_to_dataframe(bars: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert frontend OHLC bars into a clean pandas DataFrame.
    """
    if not bars:
        raise ValueError("bars cannot be empty")

    df = pd.DataFrame(bars).copy()

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    if df["Date"].isna().any():
        raise ValueError("Invalid Date values found in bars")

    for col in ["Open", "High", "Low", "Close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        if df[col].isna().any():
            raise ValueError(f"Invalid numeric values found in column: {col}")

    df = df[df["Close"] != 0].reset_index(drop=True)
    if df.empty:
        raise ValueError("No valid bars remain after filtering Close != 0")

    return df


def initialize_state(
    df: pd.DataFrame,
    starting_cash: float,
    stepsize: float,
    k: float,
) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """
    Initialize STM state using the first bar.
    """
    if starting_cash <= 0:
        raise ValueError("starting_cash must be > 0")
    if stepsize <= 0:
        raise ValueError("stepsize must be > 0")
    if k <= 1:
        raise ValueError("k must be > 1")

    start_price = float(df.loc[0, "Open"])
    if start_price <= 0:
        raise ValueError("First bar Open must be > 0")

    target = start_price * k
    last_price = start_price
    steps = (target - start_price) / stepsize
    if steps <= 0:
        raise ValueError("Derived steps must be > 0")

    initial_shares = (starting_cash / start_price) * (target - start_price) / (target - start_price / 2)
    tradesize = (initial_shares * stepsize / (target - start_price))
    cgained = -initial_shares * last_price
    acctvalue = (start_price ** 2) * tradesize / (2 * stepsize) + initial_shares * start_price
    rofsell = tradesize / stepsize

    cleft = acctvalue + cgained
    cash_needed = (last_price / 2) * (last_price / stepsize) * tradesize
    sowned = initial_shares

    date0 = df.loc[0, "Date"]
    time0 = df.loc[0, "Time"]
    extra_cash = cleft - cash_needed
    cumulative_extracash = extra_cash

    df["cleft"] = cleft
    df["sowned"] = sowned
    df["tacctvalue"] = acctvalue

    initial_trade_row = {
        "tnum":                  0,
        "Date":                  date0,
        "Time":                  time0,
        "tpt":                   0,
        "target":                target,
        "steps":                 steps,
        "stepsize":              stepsize,
        "tradesize":             tradesize,
        "Lprice":                last_price,
        "cgained":               cgained,
        "sbought":               initial_shares,
        "sowned":                sowned,
        "cleft":                 cleft,
        "sufficiency":           "",
        "resetpt":               True,
        "tacctvalue":            acctvalue,
        "cneeded":               cash_needed,
        "extra_cash":            extra_cash,
        "cumulative_extracash":  cumulative_extracash,
        "Price":                 start_price,
    }

    state = {
        "last_price":              last_price,
        "target":                  target,
        "stepsize":                stepsize,
        "tradesize":               tradesize,
        "cgained":                 cgained,
        "acctvalue":               acctvalue,
        "cleft":                   cleft,
        "sowned":                  sowned,
        "sbought":                 initial_shares,
        "rofsell":                 rofsell,
        "old_cumulative_extracash": cumulative_extracash,
    }

    return state, initial_trade_row


def run_stm(
    df: pd.DataFrame,
    k: float,
    stepsize: float,
    starting_cash: float,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Run STM on OHLC data.
    """
    n = len(df)
    if n == 0:
        empty = pd.DataFrame()
        return df, empty, empty

    open_arr  = df["Open"].to_numpy(dtype=float)
    high_arr  = df["High"].to_numpy(dtype=float)
    low_arr   = df["Low"].to_numpy(dtype=float)
    close_arr = df["Close"].to_numpy(dtype=float)
    date_arr  = df["Date"].to_numpy()
    time_arr  = df["Time"].to_numpy()

    acctvalue_arr  = np.full(n, np.nan, dtype=float)
    cleft_arr      = np.full(n, np.nan, dtype=float)
    sowned_arr     = np.full(n, np.nan, dtype=float)
    cashneeded_arr = np.full(n, np.nan, dtype=float)
    pdiff_arr      = np.zeros(n, dtype=float)
    nsteps_arr     = np.zeros(n, dtype=float)

    state, first_trade_row = initialize_state(df, starting_cash, stepsize, k)
    stepsize = state["stepsize"]

    acctvalue_arr[0]  = state["acctvalue"]
    cleft_arr[0]      = state["cleft"]
    sowned_arr[0]     = state["sowned"]
    cashneeded_arr[0] = (state["last_price"] / 2) * (state["last_price"] / stepsize) * state["tradesize"]

    trade_log: List[Dict[str, Any]] = [first_trade_row]

    starting_price        = open_arr[0]
    cumulative_extracash  = state["cleft"] - (starting_price ** 2 / 2) * state["tradesize"] / stepsize

    for i in range(1, n):
        price       = close_arr[i]
        date_value  = date_arr[i]
        time_value  = time_arr[i]

        expected_sell = state["last_price"] + stepsize
        expected_buy  = state["last_price"] - stepsize

        if low_arr[i] <= expected_buy:
            price = low_arr[i]
        if expected_sell <= high_arr[i]:
            price = high_arr[i]

        pdiff         = price - state["last_price"]
        pdiff_arr[i]  = pdiff

        if price >= state["target"] and state["sowned"] <= 0:
            sufficiency       = ""
            state["target"]   = price * k
            steps             = (state["target"] - price) / stepsize
            trade_qty         = (state["acctvalue"] / price) * (state["target"] - price) / (state["target"] - price / 2)
            state["tradesize"] = trade_qty * stepsize / (state["target"] - price)
            state["cgained"]  = trade_qty * price
            state["cleft"]    = state["acctvalue"] - trade_qty * price
            state["sowned"]   = trade_qty
            state["last_price"] = price

            cash_needed = (price / 2) * (price / stepsize) * state["tradesize"]

            acctvalue_arr[i]  = state["acctvalue"]
            cleft_arr[i]      = state["cleft"]
            sowned_arr[i]     = state["sowned"]
            cashneeded_arr[i] = cash_needed

            trade_log.append({
                "tnum":                 len(trade_log),
                "Date":                 date_value,
                "Time":                 time_value,
                "tpt":                  i,
                "target":               state["target"],
                "steps":                steps,
                "stepsize":             stepsize,
                "tradesize":            state["tradesize"],
                "Lprice":               state["last_price"],
                "cgained":              state["cgained"],
                "sbought":              trade_qty,
                "sowned":               state["sowned"],
                "cleft":                state["cleft"],
                "sufficiency":          sufficiency,
                "resetpt":              True,
                "tacctvalue":           state["acctvalue"],
                "cneeded":              cash_needed,
                "extra_cash":           state["cleft"] - cash_needed,
                "cumulative_extracash": state["cleft"] - cash_needed,
                "Price":                price,
            })
            cumulative_extracash = state["cleft"] - cash_needed
            continue

        if abs(pdiff) < stepsize:
            acctvalue_arr[i]  = state["sowned"] * price + state["cleft"]
            cleft_arr[i]      = state["cleft"]
            sowned_arr[i]     = state["sowned"]
            cashneeded_arr[i] = (state["last_price"] / 2) * (state["last_price"] / stepsize) * state["tradesize"]
            continue

        nsteps        = np.floor(abs(pdiff) / stepsize)
        nsteps_arr[i] = nsteps

        direction_price = np.sign(pdiff)
        if direction_price == 0:
            continue

        max_trade_qty = -nsteps * state["tradesize"] * direction_price
        trade_qty = max(
            min(state["cleft"] / price, max_trade_qty),
            -state["sowned"],
        )

        state["last_price"] = min(
            state["last_price"] + stepsize * nsteps * direction_price,
            state["target"],
        )

        state["cgained"] = -trade_qty * state["last_price"]

        sufficiency = ""
        if state["cleft"] / price < max_trade_qty:
            sufficiency = "insufficient cash"
        if state["sowned"] < max_trade_qty:
            sufficiency = f"insufficient stocks={state['sowned']}|{nsteps}"

        state["cleft"]     += state["cgained"]
        state["sowned"]    += trade_qty
        state["acctvalue"]  = state["sowned"] * price + state["cleft"]

        cash_needed       = (state["last_price"] / 2) * (state["last_price"] / stepsize) * state["tradesize"]
        acctvalue_arr[i]  = state["acctvalue"]
        cleft_arr[i]      = state["cleft"]
        sowned_arr[i]     = state["sowned"]
        cashneeded_arr[i] = cash_needed

        if abs(state["cgained"]) > 0.01:
            trade_log.append({
                "tnum":                 len(trade_log),
                "Date":                 date_value,
                "Time":                 time_value,
                "tpt":                  i,
                "target":               state["target"],
                "steps":                (state["target"] - price) / stepsize,
                "stepsize":             stepsize,
                "tradesize":            state["tradesize"],
                "Lprice":               state["last_price"],
                "cgained":              state["cgained"],
                "sbought":              trade_qty,
                "sowned":               state["sowned"],
                "cleft":                state["cleft"],
                "sufficiency":          sufficiency,
                "resetpt":              False,
                "tacctvalue":           state["acctvalue"],
                "cneeded":              cash_needed,
                "extra_cash":           (state["cleft"] - cash_needed) - cumulative_extracash,
                "cumulative_extracash": cumulative_extracash,
                "Price":                price,
            })
            cumulative_extracash = state["cleft"] - cash_needed

    df["pdiff"]          = pdiff_arr
    df["nsteps"]         = nsteps_arr
    df["acctvalue"]      = acctvalue_arr
    df["cleft"]          = cleft_arr
    df["sowned"]         = sowned_arr
    df["cash_needed"]    = cashneeded_arr
    df["extra_cash_mtm"] = df["cleft"] - df["cash_needed"]

    trade_log_df      = pd.DataFrame(trade_log)
    trade_log_df["Cum_EC"] = trade_log_df["extra_cash"].cumsum()

    daily_ec = (
        trade_log_df.dropna(subset=["Cum_EC"])
        .groupby("Date", as_index=False)
        .last()[["Date", "Cum_EC"]]
        .rename(columns={"Cum_EC": "eod_Cum_EC"})
    )

    eod_account_value = (
        df.dropna(subset=["acctvalue"])
        .groupby("Date", as_index=False)
        .last()[["Date", "Close", "acctvalue"]]
        .rename(columns={"Close": "eod_close", "acctvalue": "eod_acctvalue"})
    )

    eod_account_value = eod_account_value.merge(daily_ec, on="Date", how="left")
    eod_account_value["eod_Cum_EC"] = eod_account_value["eod_Cum_EC"].ffill().fillna(0.0)

    return df, trade_log_df, eod_account_value


def build_equity_curve(
    eod_account_value: pd.DataFrame,
    starting_cash: float,
    initial_price: float,
) -> List[Dict[str, Any]]:
    """
    Convert EOD account value output into frontend chart format.
    """
    if eod_account_value.empty:
        return []

    curve = eod_account_value.copy()
    curve["bh"]      = starting_cash * (curve["eod_close"] / initial_price)
    curve["ec"]      = curve["eod_Cum_EC"]
    curve["av_peak"] = curve["eod_acctvalue"].cummax()
    curve["av_dd"]   = curve["eod_acctvalue"] - curve["av_peak"]
    curve["av_dd_pct"] = np.where(
        curve["av_peak"] != 0,
        curve["av_dd"] / curve["av_peak"] * 100.0,
        0.0,
    )
    curve["bh_peak"]   = curve["bh"].cummax()
    curve["bh_dd"]     = curve["bh"] - curve["bh_peak"]
    curve["bh_dd_pct"] = np.where(
        curve["bh_peak"] != 0,
        curve["bh_dd"] / curve["bh_peak"] * 100.0,
        0.0,
    )

    records: List[Dict[str, Any]] = []
    for _, row in curve.iterrows():
        records.append({
            "date":        pd.Timestamp(row["Date"]).strftime("%Y-%m-%d"),
            "close":       round(float(row["eod_close"]),    6),
            "av":          round(float(row["eod_acctvalue"]), 6),
            "bh":          round(float(row["bh"]),           6),
            "ec":          round(float(row["ec"]),           6),
            "av_dd":       round(float(row["av_dd"]),        6),
            "av_dd_pct":   round(float(row["av_dd_pct"]),    6),
            "bh_dd":       round(float(row["bh_dd"]),        6),
            "bh_dd_pct":   round(float(row["bh_dd_pct"]),    6),
        })
    return records


def build_summary(
    ticker: str,
    equity_curve: List[Dict[str, Any]],
    trade_log_df: pd.DataFrame,
    initial_price: float,
) -> Dict[str, Any]:
    """
    Build summary cards for frontend.
    """
    if not equity_curve:
        return {
            "ticker":        ticker,
            "initial_price": round(initial_price, 6),
            "final_close":   round(initial_price, 6),
            "final_av":      0.0,
            "final_bh":      0.0,
            "total_ec":      0.0,
            "av_return_pct": 0.0,
            "bh_return_pct": 0.0,
            "ec_pct":        0.0,
            "max_av_dd":     0.0,
            "max_av_dd_pct": 0.0,
            "max_bh_dd":     0.0,
            "max_bh_dd_pct": 0.0,
            "total_trades":  int(len(trade_log_df)),
        }

    first      = equity_curve[0]
    last       = equity_curve[-1]
    initial_av = float(first["av"])
    final_av   = float(last["av"])
    final_bh   = float(last["bh"])
    total_ec   = float(last["ec"])

    av_return_pct = ((final_av - initial_av) / initial_av * 100.0) if initial_av else 0.0
    bh_return_pct = ((final_bh - initial_av) / initial_av * 100.0) if initial_av else 0.0
    ec_pct        = (total_ec / initial_av * 100.0)                 if initial_av else 0.0

    return {
        "ticker":        ticker,
        "initial_price": round(initial_price, 6),
        "final_close":   round(float(last["close"]), 6),
        "final_av":      round(final_av,   6),
        "final_bh":      round(final_bh,   6),
        "total_ec":      round(total_ec,   6),
        "av_return_pct": round(av_return_pct, 6),
        "bh_return_pct": round(bh_return_pct, 6),
        "ec_pct":        round(ec_pct,     6),
        "max_av_dd":     round(min(float(x["av_dd"])     for x in equity_curve), 6),
        "max_av_dd_pct": round(min(float(x["av_dd_pct"]) for x in equity_curve), 6),
        "max_bh_dd":     round(min(float(x["bh_dd"])     for x in equity_curve), 6),
        "max_bh_dd_pct": round(min(float(x["bh_dd_pct"]) for x in equity_curve), 6),
        "total_trades":  int(len(trade_log_df)),
    }


def serialize_trade_log(trade_log_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Convert trade log dataframe into JSON-safe records.
    """
    if trade_log_df.empty:
        return []

    safe_df = trade_log_df.copy()
    if "Date" in safe_df.columns:
        safe_df["Date"] = pd.to_datetime(safe_df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    safe_df = safe_df.replace({np.nan: None})
    return safe_df.to_dict(orient="records")


def _run_backtest_sync(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sync core logic — runs in thread pool to avoid blocking event loop.
    numpy + pandas are CPU-bound, so thread pool is the right approach.
    """
    request = STMBacktestRequest(
        ticker        = str(payload["ticker"]),
        starting_cash = float(payload["starting_cash"]),
        k             = float(payload["k"]),
        stepsize      = float(payload["stepsize"]),
        bars          = list(payload["bars"]),
    )

    df            = bars_to_dataframe(request.bars)
    initial_price = float(df.iloc[0]["Open"])

    _, trade_log_df, eod_account_value = run_stm(
        df            = df,
        k             = request.k,
        stepsize      = request.stepsize,
        starting_cash = request.starting_cash,
    )

    equity_curve = build_equity_curve(
        eod_account_value = eod_account_value,
        starting_cash     = request.starting_cash,
        initial_price     = initial_price,
    )
    summary = build_summary(
        ticker        = request.ticker,
        equity_curve  = equity_curve,
        trade_log_df  = trade_log_df,
        initial_price = initial_price,
    )
    trade_log = serialize_trade_log(trade_log_df)

    return {
        "ticker":       request.ticker,
        "summary":      summary,
        "equity_curve": equity_curve,
        "trade_log":    trade_log,
    }


async def run_backtest_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Async entry point — runs CPU-heavy backtest in thread pool.
    Event loop will not be blocked.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _run_backtest_sync, payload)
# ```

# ---

# ## Changes Jo Kiye
# ```
# PEHLE:                          AB:
# def run_backtest_payload()  →   async def run_backtest_payload()

# Kaise async kiya:
# numpy + pandas = CPU-bound      CPU work thread pool mein chala
#                                 Event loop block nahi hoga

# run_in_executor use kiya:
# _run_backtest_sync()  ← Actual logic (sync — same as before)
# run_backtest_payload() ← Async wrapper (thread pool mein chalata hai)

# Functionality: SAME ✅
# Return value:  SAME ✅
# All calculations: SAME ✅