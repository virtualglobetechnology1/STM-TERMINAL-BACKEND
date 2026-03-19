# # get_token.py
# import requests

# # Yahan apni details daalo
# API_KEY     = "TotqUGJp"
# CLIENT_CODE = "AABZ076636"  # A123456
# PASSWORD    = "Dozen@7007"
# TOTP        = "858080"  # Authenticator app se

# url = "https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword"

# headers = {
#     "Content-Type": "application/json",
#     "Accept": "application/json",
#     "X-UserType": "USER",
#     "X-SourceID": "WEB",
#     "X-ClientLocalIP": "CLIENT_LOCAL_IP",
#     "X-ClientPublicIP": "CLIENT_PUBLIC_IP",
#     "X-MACAddress": "MAC_ADDRESS",
#     "X-PrivateKey": API_KEY
# }

# body = {
#     "clientcode": CLIENT_CODE,
#     "password": PASSWORD,
#     "totp": TOTP
# }

# response = requests.post(url, json=body, headers=headers)
# data = response.json()

# print("Full Response:", data)
# print("JWT Token:", data["data"]["jwtToken"])
# print("Feed Token:", data["data"]["feedToken"])
# print("Refresh Token:", data["data"]["refreshToken"])
# # ```

# # ---

# # ## TOTP Kya Hai?
# # ```
# # TOTP = Time based One Time Password
# # 6 digit code jo har 30 sec mein change hota hai

# # Kaise setup karo:
# # 1. Angel One app kholo
# # 2. Profile → Security → TOTP Enable karo
# # 3. Google Authenticator ya similar app se scan karo
# # 4. Har baar login ke waqt woh 6 digit daalna hoga









import requests
import pyotp

API_KEY     = "TotqUGJp"
CLIENT_CODE = "AABZ076636"
MPIN        = "7007"
TOTP_SECRET = "UZPWRDEXK2HWLF5EL4MQSQDM6I"

print("Script is Running...")

totp = pyotp.TOTP(TOTP_SECRET)
current_totp = totp.now()
print("TOTP:", current_totp)

url = "https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword"

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-UserType": "USER",
    "X-SourceID": "WEB",
    "X-ClientLocalIP": "127.0.0.1",
    "X-ClientPublicIP": "127.0.0.1",
    "X-MACAddress": "00:00:00:00:00:00",
    "X-PrivateKey": API_KEY
}

body = {
    "clientcode": CLIENT_CODE,
    "password": MPIN,
    "totp": current_totp
}

print("Login is on going...")
response = requests.post(url, json=body, headers=headers)
data = response.json()

print("Response:", data)

if data["status"]:
    jwt = data["data"]["jwtToken"]
    feed = data["data"]["feedToken"]
    
    print("\n✅ LOGIN SUCCESSFUL!")
    print("JWT Token:", jwt)
    print("Feed Token:", feed)
    
    # .env mein save karo
    with open(".env", "a") as f:
        f.write(f"\nANGEL_JWT_TOKEN={jwt}")
        f.write(f"\nANGEL_FEED_TOKEN={feed}")
    
    print("\n.env file update successful!")
    
else:
    print("\n❌ Login Failed:", data["message"])
    print("Error Code:", data["errorcode"])