import pyotp
from dotenv import load_dotenv
import os
from SmartApi import SmartConnect

load_dotenv()

API_KEY     = os.getenv("ANGEL_API_KEY")
CLIENT_CODE = os.getenv("ANGEL_CLIENT_CODE")
PIN         = os.getenv("ANGEL_PIN")
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

totp = pyotp.TOTP(TOTP_SECRET).now()

smart = SmartConnect(API_KEY)
data  = smart.generateSession(CLIENT_CODE, PIN, totp)

jwt_token  = data["data"]["jwtToken"]
feed_token = smart.getfeedToken()

print("\n✅ Credentials ready!\n")
print(f"jwt_token  : {jwt_token}")
print(f"feed_token : {feed_token}")
print(f"client_code: {CLIENT_CODE}")
print(f"api_key    : {API_KEY}")