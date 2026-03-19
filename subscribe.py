import requests
import pyotp
from dotenv import load_dotenv
import os
from SmartApi import SmartConnect


load_dotenv()

API_KEY     = os.getenv("ANGEL_API_KEY")
CLIENT_CODE = os.getenv("ANGEL_CLIENT_CODE")
PIN         = os.getenv("ANGEL_PIN")
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

# Get fresh tokens
totp      = pyotp.TOTP(TOTP_SECRET).now()
smart     = SmartConnect(API_KEY)
data      = smart.generateSession(CLIENT_CODE, PIN, totp)
jwt_token  = data["data"]["jwtToken"]
feed_token = smart.getfeedToken()

print("✅ Tokens generated")

# Subscribe
response = requests.post(
    "http://localhost:8000/api/live-price/subscribe",
    json={
        "jwt_token":    jwt_token,
        "api_key":      API_KEY,
        "client_code":  CLIENT_CODE,
        "feed_token":   feed_token,
        "tokens":       ["500312"],
        "exchange_type": 3,
        "mode": 1
    }
)

print("Subscribe response:", response.json())