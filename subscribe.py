import requests
import pyotp
from dotenv import load_dotenv
import os
import mysql.connector
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

def lookup_ticker(ticker_input: str):
    conn = mysql.connector.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT ticker_id, ticker, ticker_security_code, ticker_exchange, ticker_security_name
        FROM dd_ticker_list
        WHERE ticker = %s
        AND ticker_security_code IS NOT NULL
        AND ticker_status = 'Active'
    """, (ticker_input.upper(),))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def pick_ticker(ticker_input: str):
    """Lookup ticker in DB and let user pick exchange. Returns (security_code, exchange_type, ticker_name)."""
    results = lookup_ticker(ticker_input)

    if not results:
        print(f"  ❌ '{ticker_input}' not found in database. Skipping.")
        return None

    print(f"\n  Found '{ticker_input}' on:")
    for i, r in enumerate(results):
        print(f"    {i+1}. {r['ticker_exchange']} | Code: {r['ticker_security_code']} | {r['ticker_security_name']}")

    if len(results) > 1:
        pick = int(input("  Choose exchange number: ").strip()) - 1
        chosen = results[pick]
        exchange_type = EXCHANGE_TYPE_MAP.get(chosen["ticker_exchange"].upper(), 1)
    else:
        chosen = results[0]
        default_et = EXCHANGE_TYPE_MAP.get(chosen["ticker_exchange"].upper(), 1)
        print(f"  Only on {chosen['ticker_exchange']}. Override? 1=NSE | 2=NSE_FO | 3=BSE | 4=BSE_FO (Enter={default_et}): ", end="")
        override = input().strip()
        exchange_type = int(override) if override else default_et

    return str(chosen["ticker_security_code"]), exchange_type, chosen["ticker"]


# ── Step 1: Ask how many tickers ─────────────────────────────────────────────
print("=" * 50)
print("  AngelOne Live Price Subscriber")
print("=" * 50)

raw = input("\nEnter ticker name(s) separated by comma (e.g. RELIANCE, INFY, 5PAISA): ").strip()
ticker_inputs = [t.strip().upper() for t in raw.split(",") if t.strip()]

# ── Step 2: Ask mode once for all ────────────────────────────────────────────
print("\nMode options:")
print("  1 = LTP only (just the price)")
print("  2 = Quote (price + OHLCV + volume)  ← recommended")
print("  3 = SnapQuote (full order book depth)")
mode = int(input("Select mode for all tickers (default=2): ").strip() or "2")

# ── Step 3: Lookup each ticker ────────────────────────────────────────────────
print("\nLooking up tickers in database...")

# Group by exchange_type: {exchange_type: [security_codes]}
exchange_groups: dict[int, list[str]] = {}
ticker_summary = []

for ticker_input in ticker_inputs:
    result = pick_ticker(ticker_input)
    if result is None:
        continue
    security_code, exchange_type, ticker_name = result
    exchange_groups.setdefault(exchange_type, []).append(security_code)
    ticker_summary.append((ticker_name, security_code, exchange_type))

if not exchange_groups:
    print("\n❌ No valid tickers found. Exiting.")
    exit(1)

print("\nSummary of subscriptions:")
for ticker_name, security_code, exchange_type in ticker_summary:
    print(f"  {ticker_name} | Code: {security_code} | Exchange: {EXCHANGE_NAME_MAP.get(str(exchange_type))} | Mode: {mode}")

# ── Step 4: Get fresh AngelOne tokens ────────────────────────────────────────
print("\nGenerating AngelOne tokens...")
try:
    totp       = pyotp.TOTP(TOTP_SECRET).now()
    smart      = SmartConnect(API_KEY)
    data       = smart.generateSession(CLIENT_CODE, PIN, totp)
    jwt_token  = data["data"]["jwtToken"]
    feed_token = smart.getfeedToken()
    print("✅ Tokens generated")
except Exception as e:
    print(f"❌ Login failed: {e}")
    exit(1)

# ── Step 5: Subscribe each exchange group separately ─────────────────────────
all_security_codes = []
for exchange_type, security_codes in exchange_groups.items():
    all_security_codes.extend(security_codes)
    print(f"\nSubscribing {security_codes} on {EXCHANGE_NAME_MAP.get(str(exchange_type))}...")
    try:
        response = requests.post(
            "http://localhost:8000/api/live-price/subscribe",
            json={
                "jwt_token":     jwt_token,
                "api_key":       API_KEY,
                "client_code":   CLIENT_CODE,
                "feed_token":    feed_token,
                "tokens":        security_codes,
                "exchange_type": exchange_type,
                "mode":          mode,
            }
        )
        print(f"  Response: {response.json()}")
    except Exception as e:
        print(f"  ❌ Subscribe failed: {e}")

# ── Step 6: Print WebSocket URL ───────────────────────────────────────────────
token_str = ",".join(all_security_codes)
print(f"\n🔗 WebSocket URL to stream all tickers (use in test_stream.py or frontend):")
print(f"ws://localhost:8000/api/live-price/stream")
print(f"\nNote: All subscribed tickers will stream on the same WebSocket connection.")
print(f"Run: python test_stream.py")