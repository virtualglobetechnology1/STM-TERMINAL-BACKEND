# test_stream.py

import asyncio
import aiomysql
import websockets
import json
import os
from dotenv import load_dotenv

load_dotenv()

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
    """Lookup ticker and let user pick exchange."""
    results = await lookup_ticker(ticker_input)

    if not results:
        print(f"  '{ticker_input}' not found in database. Skipping.")
        return None

    print(f"\n  Found '{ticker_input}' on:")
    for i, r in enumerate(results):
        print(
            f"    {i+1}. {r['ticker_exchange']} | "
            f"Code: {r['ticker_security_code']} | "
            f"{r['ticker_security_name']}"
        )

    if len(results) > 1:
        pick          = int(input("  Choose exchange number: ").strip()) - 1
        chosen        = results[pick]
        exchange_type = EXCHANGE_TYPE_MAP.get(
            chosen["ticker_exchange"].upper(), 1
        )
    else:
        chosen      = results[0]
        default_et  = EXCHANGE_TYPE_MAP.get(
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


async def listen(subscriptions: list):
    """Connect once and receive ticks for ALL subscribed tickers."""
    code_to_ticker = {s[0]: s[2] for s in subscriptions}

    uri = "ws://localhost:8000/api/live-price/stream"
    print(f"\nConnecting to live stream...")

    async with websockets.connect(uri) as ws:
        print("Connected! Waiting for ticks...\n")
        async for message in ws:
            tick = json.loads(message)

            if "status" in tick:
                print(f"  {tick}")
                continue

            if "error" in tick:
                print(f"Error: {tick['error']}")
                continue

            token       = tick.get("token", "")
            ticker_name = code_to_ticker.get(token, token)

            print(
                f"{ticker_name} | {tick.get('exchange')} | "
                f"LTP: {tick.get('ltp')} | "
                f"High: {tick.get('high')} | "
                f"Low: {tick.get('low')} | "
                f"Volume: {tick.get('volume')}"
            )


async def main():
    print("=" * 50)
    print("  AngelOne Live Price Stream")
    print("=" * 50)

    # Step 1: Ask tickers
    raw = input(
        "\nEnter ticker name(s) comma-separated "
        "(e.g. RELIANCE, INFY, 5PAISA): "
    ).strip()
    ticker_inputs = [
        t.strip().upper() for t in raw.split(",") if t.strip()
    ]

    # Step 2: Lookup all tickers in parallel
    print("\nLooking up tickers in database...")
    tasks         = [pick_ticker(t) for t in ticker_inputs]
    results       = await asyncio.gather(*tasks)
    subscriptions = [r for r in results if r is not None]

    if not subscriptions:
        print("\nNo valid tickers found. Exiting.")
        return

    # Step 3: Ask mode
    print("\nMode options:")
    print("  1 = LTP only")
    print("  2 = Quote (price + OHLCV)  <- recommended")
    print("  3 = SnapQuote (full depth)")
    mode = int(input("Select mode (default=2): ").strip() or "2")

    # Step 4: Summary
    print("\nSubscribed tickers:")
    for security_code, exchange_type, ticker_name in subscriptions:
        print(
            f"  {ticker_name} | Code: {security_code} | "
            f"Exchange: {EXCHANGE_NAME_MAP.get(str(exchange_type))} | "
            f"Mode: {mode}"
        )

    print("\nMake sure you ran python subscribe.py first.")
    print("All ticks will appear below:\n")

    # Step 5: Listen
    await listen(subscriptions)


if __name__ == "__main__":
    asyncio.run(main())
