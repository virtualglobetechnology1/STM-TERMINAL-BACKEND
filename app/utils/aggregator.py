from typing import Any, Dict, List


def aggregate_portfolio_curves(ticker_results: List[Dict[str, Any]]) -> List[Dict[str, float | str]]:
    """
    Aggregate individual ticker equity curves into portfolio-level curves.
    
    Args:
        ticker_results: List of ticker backtest results, each containing 'results' key with equity curve data
    
    Returns:
        List of aggregated portfolio curves by date
    """
    by_date: Dict[str, Dict[str, float]] = {}

    for item in ticker_results:
        # Skip items with errors
        if "error" in item:
            continue
            
        for row in item.get("equity_curve", []):
            date = row["date"]
            if date not in by_date:
                by_date[date] = {"av": 0.0, "bh": 0.0, "ec": 0.0 ,"av_dd": 0.0,"av_dd_pct": 0.0,"bh_dd": 0.0,"bh_dd_pct": 0.0}
            
            bucket = by_date[date]
            bucket["av"] += float(row.get("av", 0.0))
            bucket["bh"] += float(row.get("bh", 0.0))
            bucket["ec"] += float(row.get("ec", 0.0))
            bucket["av_dd"] += float(row.get("av_dd", 0.0))
            bucket["av_dd_pct"] += float(row.get("av_dd_pct", 0.0))
            bucket["bh_dd"] += float(row.get("bh_dd", 0.0))
            bucket["bh_dd_pct"] += float(row.get("bh_dd_pct", 0.0))



    # Sort by date and format response
    return [
        {
            "date": date,
            "av": round(vals["av"], 6),
            "bh": round(vals["bh"], 6),
            "ec": round(vals["ec"], 6),
            "av_dd": round(vals["av_dd"], 6),
            "av_dd_pct": round(vals["av_dd_pct"], 6),
            "bh_dd": round(vals["bh_dd"], 6),
            "bh_dd_pct": round(vals["bh_dd_pct"], 6),
        }
        for date, vals in sorted(by_date.items(), key=lambda x: x[0])
    ]