# subscribe.py

import asyncio
import aiohttp
import aiomysql
import pyotp
import os
from dotenv import load_dotenv
from SmartApi import SmartConnect

load_dotenv()

API_KEY     = os.getenv("ANGEL_API_KEY")
CLIENT_CODE = os.getenv("ANGEL_CLIENT_CODE")
PIN         = os.getenv("ANGEL_PIN")
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "3306"))
DB_USER     = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME     = os.getenv("DB_NAME", "dev_dozen_diamonds")

EXCHANGE_TYPE_MAP = {
    "NSE": 1, "BSE": 3, "NSE_FO": 2, "BSE_FO": 4, "MCX": 5,
}
EXCHANGE_NAME_MAP = {
    "1": "NSE", "2": "NSE_FO", "3": "BSE", "4": "BSE_FO", "5": "MCX",
}


# ── Async DB lookup ───────────────────────────────────────────────────────────

async def lookup_ticker(ticker_input: str):
    conn = await aiomysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME
    )
    async with conn.cursor(aiomysql.DictCursor) as cursor:
        await cursor.execute("""
            SELECT ticker_id, ticker, ticker_security_code,
                   ticker_exchange, ticker_security_name
            FROM dd_ticker_list
            WHERE ticker = %s
            AND ticker_security_code IS NOT NULL
            AND ticker_status = 'Active'
        """, (ticker_input.upper(),))
        results = await cursor.fetchall()
    conn.close()
    return results


async def pick_ticker(ticker_input: str):
    """Lookup ticker in DB and let user pick exchange."""
    results = await lookup_ticker(ticker_input)

    if not results:
        print(f"  '{ticker_input}' not found in database. Skipping.")
        return None

    print(f"\n  Found '{ticker_input}' on:")
    for i, r in enumerate(results):
        print(f"    {i+1}. {r['ticker_exchange']} | "
              f"Code: {r['ticker_security_code']} | "
              f"{r['ticker_security_name']}")

    if len(results) > 1:
        pick = int(input("  Choose exchange number: ").strip()) - 1
        chosen       = results[pick]
        exchange_type = EXCHANGE_TYPE_MAP.get(
            chosen["ticker_exchange"].upper(), 1
        )
    else:
        chosen       = results[0]
        default_et   = EXCHANGE_TYPE_MAP.get(
            chosen["ticker_exchange"].upper(), 1
        )
        print(
            f"  Only on {chosen['ticker_exchange']}. "
            f"Override? 1=NSE | 2=NSE_FO | 3=BSE | 4=BSE_FO "
            f"(Enter={default_et}): ",
            end=""
        )
        override      = input().strip()
        exchange_type = int(override) if override else default_et

    return str(chosen["ticker_security_code"]), exchange_type, chosen["ticker"]


# ── Async subscribe ───────────────────────────────────────────────────────────

async def subscribe_to_server(
    jwt_token:     str,
    feed_token:    str,
    security_codes: list,
    exchange_type: int,
    mode:          int
):
    """Send subscribe request to FastAPI server — async."""
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "http://localhost:8000/api/live-price/subscribe",
            json={
                "tokens":        security_codes,
                "exchange_type": exchange_type,
                "mode":          mode,
            }
        ) as response:
            return await response.json()


# ── Main flow ─────────────────────────────────────────────────────────────────

async def main():
    print("=" * 50)
    print("  AngelOne Live Price Subscriber")
    print("=" * 50)

    # Step 1: Ask tickers
    raw = input(
        "\nEnter ticker name(s) comma-separated "
        "(e.g. RELIANCE, INFY, 5PAISA): "
    ).strip()
    ticker_inputs = [t.strip().upper() for t in raw.split(",") if t.strip()]

    # Step 2: Ask mode
    print("\nMode options:")
    print("  1 = LTP only")
    print("  2 = Quote (price + OHLCV)  <- recommended")
    print("  3 = SnapQuote (full depth)")
    mode = int(input("Select mode (default=2): ").strip() or "2")

    # Step 3: Lookup tickers — all in parallel
    print("\nLooking up tickers in database...")

    tasks   = [pick_ticker(t) for t in ticker_inputs]
    results = await asyncio.gather(*tasks)

    exchange_groups: dict[int, list[str]] = {}
    ticker_summary  = []

    for result in results:
        if result is None:
            continue
        security_code, exchange_type, ticker_name = result
        exchange_groups.setdefault(exchange_type, []).append(security_code)
        ticker_summary.append((ticker_name, security_code, exchange_type))

    if not exchange_groups:
        print("\nNo valid tickers found. Exiting.")
        return

    print("\nSummary of subscriptions:")
    for ticker_name, security_code, exchange_type in ticker_summary:
        print(
            f"  {ticker_name} | Code: {security_code} | "
            f"Exchange: {EXCHANGE_NAME_MAP.get(str(exchange_type))} | "
            f"Mode: {mode}"
        )

    # Step 4: Get fresh AngelOne tokens — runs in thread pool (SmartApi is sync)
    print("\nGenerating AngelOne tokens...")
    try:
        loop = asyncio.get_event_loop()

        def _get_tokens():
            totp       = pyotp.TOTP(TOTP_SECRET).now()
            smart      = SmartConnect(API_KEY)
            data       = smart.generateSession(CLIENT_CODE, PIN, totp)
            jwt_token  = data["data"]["jwtToken"]
            feed_token = smart.getfeedToken()
            return jwt_token, feed_token

        jwt_token, feed_token = await loop.run_in_executor(None, _get_tokens)
        print("Tokens generated")

    except Exception as e:
        print(f"Login failed: {e}")
        return

    # Step 5: Subscribe each exchange group — all in parallel
    all_security_codes = []

    subscribe_tasks = []
    for exchange_type, security_codes in exchange_groups.items():
        all_security_codes.extend(security_codes)
        print(
            f"\nSubscribing {security_codes} on "
            f"{EXCHANGE_NAME_MAP.get(str(exchange_type))}..."
        )
        subscribe_tasks.append(
            subscribe_to_server(
                jwt_token, feed_token,
                security_codes, exchange_type, mode
            )
        )

    responses = await asyncio.gather(*subscribe_tasks, return_exceptions=True)

    for i, resp in enumerate(responses):
        if isinstance(resp, Exception):
            print(f"  Subscribe failed: {resp}")
        else:
            print(f"  Response: {resp}")

    # Step 6: Print WebSocket URL
    print(f"\nWebSocket URL to stream all tickers:")
    print(f"ws://localhost:8000/api/live-price/stream")
    print(f"\nRun: python test_stream.py")


if __name__ == "__main__":
    asyncio.run(main())