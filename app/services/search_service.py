# app/services/search_service.py

import aiomysql
import math
from app.core.config import (
    DB_HOST, DB_PORT,
    DB_USER, DB_PASSWORD, DB_NAME
)

# ─────────────────────────────────────────────────────
# CONNECTION POOL
# One pool for all requests — no new connection each time
# ─────────────────────────────────────────────────────
_pool = None

async def get_pool():
    global _pool
    if _pool is None:
        _pool = await aiomysql.create_pool(
            host=DB_HOST,
            port=int(DB_PORT),
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            minsize=3,
            maxsize=10,
            autocommit=True
        )
        print("Connection pool created!")
    return _pool


# ─────────────────────────────────────────────────────
# SUGGESTIONS
# Auto-complete suggestions based on search results
# ─────────────────────────────────────────────────────
def _generate_suggestions(results: list, ticker_name: str) -> list:
    suggestions = []
    seen        = set()
    query_upper = ticker_name.upper()

    for row in results:
        ticker = row.get("ticker", "")
        issuer = row.get("issuer_name", "") or ""

        # Ticker suggestion
        if ticker and ticker not in seen:
            suggestions.append(ticker)
            seen.add(ticker)

        # Company name first word suggestion
        if issuer:
            first_word = issuer.split()[0].upper()
            if (
                len(first_word) > 2
                and first_word not in seen
                and query_upper in first_word
            ):
                suggestions.append(first_word)
                seen.add(first_word)

        if len(suggestions) >= 8:
            break

    return suggestions[:8]


# ─────────────────────────────────────────────────────
# MAIN SEARCH FUNCTION
# Table: dd_new_ticker_list
# Filter: NSE and BSE only
# Async with connection pool
# ─────────────────────────────────────────────────────
async def search_tickers(
    ticker_name: str,
    exchange:    str = None,
    page:        int = 1,
    page_size:   int = 10
):
    ticker_name = ticker_name.strip()

    # Input validation
    if not ticker_name:
        return {
            "success":     False,
            "error":       "ticker_name is required",
            "results":     [],
            "suggestions": [],
            "total":       0
        }

    if page < 1:
        return {
            "success":     False,
            "error":       "page must be greater than 0",
            "results":     [],
            "suggestions": [],
            "total":       0
        }

    if page_size < 1 or page_size > 50:
        return {
            "success":     False,
            "error":       "page_size must be between 1 and 50",
            "results":     [],
            "suggestions": [],
            "total":       0
        }

    try:
        pool              = await get_pool()
        offset            = (page - 1) * page_size
        search_term       = f"%{ticker_name}%"
        search_term_start = f"{ticker_name.upper()}%"
        ticker_upper      = ticker_name.upper()

        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:

                # ── With exchange filter (NSE or BSE only) ────────────────────
                if exchange:

                    # Count query
                    await cursor.execute("""
                        SELECT COUNT(*) as total
                        FROM dd_new_ticker_list
                        WHERE ticker_exchange = %s
                        AND   ticker_exchange IN ('NSE', 'BSE')
                        AND (
                            ticker            LIKE %s
                            OR ticker_issuer_name LIKE %s
                        )
                    """, (exchange.upper(), search_term, search_term))

                    total = (await cursor.fetchone())["total"]

                    # Data query
                    await cursor.execute("""
                        SELECT
                            ticker_id,
                            ticker,
                            ticker_exchange,
                            ticker_issuer_name,
                            ticker_isin_no,
                            ticker_status
                        FROM dd_new_ticker_list
                        WHERE ticker_exchange = %s
                        AND   ticker_exchange IN ('NSE', 'BSE')
                        AND (
                            ticker            LIKE %s
                            OR ticker_issuer_name LIKE %s
                        )
                        ORDER BY
                            CASE
                                WHEN ticker = %s    THEN 1
                                WHEN ticker LIKE %s THEN 2
                                ELSE 3
                            END,
                            ticker ASC
                        LIMIT %s OFFSET %s
                    """, (
                        exchange.upper(),
                        search_term,    search_term,
                        ticker_upper,   search_term_start,
                        page_size,      offset
                    ))

                # ── Without exchange filter (NSE + BSE both) ─────────────────
                else:

                    # Count query
                    await cursor.execute("""
                        SELECT COUNT(*) as total
                        FROM dd_new_ticker_list
                        WHERE ticker_exchange IN ('NSE', 'BSE')
                        AND (
                            ticker            LIKE %s
                            OR ticker_issuer_name LIKE %s
                        )
                    """, (search_term, search_term))

                    total = (await cursor.fetchone())["total"]

                    # Data query
                    await cursor.execute("""
                        SELECT
                            ticker_id,
                            ticker,
                            ticker_exchange,
                            ticker_issuer_name,
                            ticker_isin_no,
                            ticker_status
                        FROM dd_new_ticker_list
                        WHERE ticker_exchange IN ('NSE', 'BSE')
                        AND (
                            ticker            LIKE %s
                            OR ticker_issuer_name LIKE %s
                        )
                        ORDER BY
                            CASE
                                WHEN ticker = %s    THEN 1
                                WHEN ticker LIKE %s THEN 2
                                ELSE 3
                            END,
                            ticker ASC
                        LIMIT %s OFFSET %s
                    """, (
                        search_term,    search_term,
                        ticker_upper,   search_term_start,
                        page_size,      offset
                    ))

                rows = await cursor.fetchall()

        # Format results
        results = [
            {
                "ticker_id":   row["ticker_id"],
                "ticker":      row["ticker"],
                "exchange":    row["ticker_exchange"],
                "issuer_name": row["ticker_issuer_name"],
                "isin":        row["ticker_isin_no"],
                "status":      row["ticker_status"]
            }
            for row in rows
        ]

        # Pagination meta
        total_pages = math.ceil(total / page_size) if total > 0 else 1

        return {
            "success":     True,
            "results":     results,
            "suggestions": _generate_suggestions(results, ticker_name),
            "total":       total,
            "pagination": {
                "total":       total,
                "page":        page,
                "page_size":   page_size,
                "total_pages": total_pages,
                "has_next":    page < total_pages,
                "has_prev":    page > 1
            }
        }

    except Exception as e:
        print(f"Search error: {e}")
        return {
            "success":     False,
            "error":       f"Database error: {str(e)}",
            "results":     [],
            "suggestions": [],
            "total":       0
        }
