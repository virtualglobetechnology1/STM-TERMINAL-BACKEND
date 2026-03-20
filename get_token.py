import requests
import pyotp
import os
from dotenv import load_dotenv

load_dotenv()

# .env se credentials lo — hardcode nahi
API_KEY      = os.getenv("ANGEL_API_KEY")
CLIENT_CODE  = os.getenv("ANGEL_CLIENT_CODE")
MPIN         = os.getenv("ANGEL_MPIN")
TOTP_SECRET  = os.getenv("ANGEL_TOTP_SECRET")

def get_angel_tokens():

    # Validate karo
    if not all([API_KEY, CLIENT_CODE, MPIN, TOTP_SECRET]):
        print(".env credentials missing !")
        print(f"API_KEY: {'✅' if API_KEY else 'Error'}")
        print(f"CLIENT_CODE: {'✅' if CLIENT_CODE else 'client code error'}")
        print(f"MPIN: {'✅' if MPIN else 'MPIN error'}")
        print(f"TOTP_SECRET: {'✅' if TOTP_SECRET else 'totp error'}")
        return None

    # TOTP generate karo
    try:
        totp = pyotp.TOTP(TOTP_SECRET)
        current_totp = totp.now()
        print(f" TOTP Generated: {current_totp}")
    except Exception as e:
        print(f" TOTP Error: {e}")
        return None

    # Login API
    url = "https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword"

    headers = {
        "Content-Type":        "application/json",
        "Accept":              "application/json",
        "X-UserType":          "USER",
        "X-SourceID":          "WEB",
        "X-ClientLocalIP":     "127.0.0.1",
        "X-ClientPublicIP":    "127.0.0.1",
        "X-MACAddress":        "00:00:00:00:00:00",
        "X-PrivateKey":        API_KEY
    }

    body = {
        "clientcode": CLIENT_CODE,
        "password":   MPIN,
        "totp":       current_totp
    }

    try:
        print("AngelOne is running...")
        response = requests.post(url, json=body, headers=headers)
        data = response.json()

        if not data.get("status"):
            print(f" Login Failed: {data.get('message')}")
            print(f"Error Code: {data.get('errorcode')}")
            return None

        jwt_token  = data["data"]["jwtToken"]
        feed_token = data["data"]["feedToken"]

        print(" Login Successful!")

        # .env mein save karo
        update_env_file(jwt_token, feed_token)

        return {
            "jwt_token":  jwt_token,
            "feed_token": feed_token
        }

    except Exception as e:
        print(f" Error: {e}")
        return None


def update_env_file(jwt_token, feed_token):
    """
    .env file present only JWT aur Feed token update automatically
    
    """
    env_path = ".env"

    # Existing .env read karo
    lines = []
    try:
        with open(env_path, "r") as f:
            lines = f.readlines()
    except FileNotFoundError:
        pass

    # JWT aur Feed token update karo
    updated = {
        "ANGEL_JWT_TOKEN":  jwt_token,
        "ANGEL_FEED_TOKEN": feed_token
    }

    new_lines = []
    updated_keys = set()

    for line in lines:
        key = line.split("=")[0].strip()
        if key in updated:
            new_lines.append(f"{key}={updated[key]}\n")
            updated_keys.add(key)
        else:
            new_lines.append(line)

    # Jo keys nahi thi unhe add karo
    for key, val in updated.items():
        if key not in updated_keys:
            new_lines.append(f"{key}={val}\n")

    with open(env_path, "w") as f:
        f.writelines(new_lines)

    print(".env file updated!")
    print(f"JWT Token: {jwt_token[:30]}...")
    print(f"Feed Token: {feed_token}")


if __name__ == "__main__":
    get_angel_tokens()