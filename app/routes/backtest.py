# app/routes/backtest.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Any, Dict, List

from app.db.database import SessionLocal
from app.services.backtest_service import run_backtest_payload
from app.schemas.backtest_schema import PortfolioBacktestRequest
from app.utils.aggregator import aggregate_portfolio_curves
from app.utils.response import success_response, error_response

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/backtest")
async def backtest_portfolio(
    request: PortfolioBacktestRequest,
    db: Session = Depends(get_db)
):
    try:
        if not request.stocks:
            return error_response("stocks list is empty")

        ticker_results: List[Dict[str, Any]] = []
        errors: List[Dict[str, str]] = []

        for stock in request.stocks:
            try:
                payload = {
                    "ticker":        stock.ticker,
                    "starting_cash": stock.starting_cash,
                    "k":             stock.k,
                    "stepsize":      stock.stepsize,
                    "bars":          [bar.model_dump() for bar in stock.bars]
                }

                # Async call — runs in thread pool
                result = await run_backtest_payload(payload)

                summary = result["summary"]

                ticker_results.append({
                    "ticker":       result["ticker"],
                    "equity_curve": result["equity_curve"],
                    "tradeLog":     result["trade_log"],
                    "summary": {
                        "finalAV":     summary["final_av"],
                        "finalBH":     summary["final_bh"],
                        "totalEC":     summary["total_ec"],
                        "ecPct":       str(round(summary["ec_pct"], 6)),
                        "avReturn":    str(round(summary["av_return_pct"], 6)),
                        "bhReturn":    str(round(summary["bh_return_pct"], 6)),
                        "totalTrades": summary["total_trades"]
                    },
                })

            except Exception as e:
                errors.append({
                    "ticker": stock.ticker,
                    "error":  str(e)
                })

        if not ticker_results and errors:
            return error_response(
                message="All backtests failed",
                data={"errors": errors}
            )

        portfolio_results = aggregate_portfolio_curves(ticker_results)

        return success_response(
            message="Backtest executed successfully",
            data={
                "portfolioResults": portfolio_results,
                "tickerResults":    ticker_results,
                "errors":           errors if errors else None
            }
        )

    except Exception as e:
        return error_response("Internal server error", str(e))


@router.get("/backtest/health")
async def health_check():
    return success_response(message="Backtest service is healthy")