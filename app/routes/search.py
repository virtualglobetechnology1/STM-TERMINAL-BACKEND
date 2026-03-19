# app/routes/search.py

from fastapi import APIRouter, Query
from typing import Optional
from app.schemas.search_schema import SearchRequest
from app.services.search_service import (
    search_tickers,
    clear_search_cache
)

router = APIRouter()


# ─────────────────────────────────────────────────────
# GET — Query params se search
# ─────────────────────────────────────────────────────
@router.get("/search/ticker")
async def search_ticker_get(
    ticker_name: str           = Query(...,  description="Ticker or company name"),
    exchange:    Optional[str] = Query(None, description="NSE or BSE"),
    page:        int           = Query(1,    ge=1,        description="Page number"),
    page_size:   int           = Query(10,   ge=1, le=50, description="Results per page")
):
    """
    Search tickers by name or company name with pagination.

    Examples:
    GET /api/search/ticker?ticker_name=RELIANCE
    GET /api/search/ticker?ticker_name=TATA&exchange=NSE&page=1&page_size=10
    GET /api/search/ticker?ticker_name=Reliance Industries&page=2
    """

    result = await search_tickers(
        ticker_name=ticker_name,
        exchange=exchange,
        page=page,
        page_size=page_size
    )

    if not result["success"]:
        return {
            "status":  False,
            "message": result.get("error", "Search failed")
        }

    return {
        "status":      True,
        "message":     "Success",
        "source":      result["source"],
        "pagination":  result["pagination"],
        "data":        result["results"],
        "suggestions": result["suggestions"]
    }


# ─────────────────────────────────────────────────────
# POST — Body se search
# ─────────────────────────────────────────────────────
@router.post("/search/ticker")
async def search_ticker_post(request: SearchRequest):
    """
    Search tickers by name or company name with pagination.

    Body:
    {
        "ticker_name": "RELIANCE",
        "exchange":    "NSE",
        "page":        1,
        "page_size":   10
    }
    """

    result = await search_tickers(
        ticker_name=request.ticker_name,
        exchange=request.exchange,
        page=request.page,
        page_size=request.page_size
    )

    if not result["success"]:
        return {
            "status":  False,
            "message": result.get("error", "Search failed")
        }

    return {
        "status":      True,
        "message":     "Success",
        "source":      result["source"],
        "pagination":  result["pagination"],
        "data":        result["results"],
        "suggestions": result["suggestions"]
    }


# ─────────────────────────────────────────────────────
# DELETE — Cache clear
# ─────────────────────────────────────────────────────
@router.delete("/search/cache")
async def clear_cache():
    """Clear search cache manually."""
    clear_search_cache()
    return {
        "status":  True,
        "message": "Search cache cleared successfully"
    }