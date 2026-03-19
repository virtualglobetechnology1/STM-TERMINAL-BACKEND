# import os
# from dotenv import load_dotenv

# load_dotenv()

# class Settings:
#     DB_HOST = os.getenv("DB_HOST")
#     DB_PORT = os.getenv("DB_PORT")
#     DB_USER = os.getenv("DB_USER")
#     DB_PASSWORD = os.getenv("DB_PASSWORD")
#     DB_NAME = os.getenv("DB_NAME")

# settings = Settings()




# # import os                    added on 18/03/2026
# # from dotenv import load_dotenv

# load_dotenv()

# ANGEL_API_KEY    = os.getenv("ANGEL_API_KEY")
# ANGEL_CLIENT_CODE = os.getenv("ANGEL_CLIENT_CODE")
# ANGEL_JWT_TOKEN  = os.getenv("ANGEL_JWT_TOKEN")
# ANGEL_LTP_URL    = "https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/"









# app/core/config.py

import os
from dotenv import load_dotenv

load_dotenv()

# Database Config
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "3306")
DB_USER     = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME     = os.getenv("DB_NAME", "dev_dozen_diamonds")

# Angel One Config
ANGEL_API_KEY     = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_CODE = os.getenv("ANGEL_CLIENT_CODE")
ANGEL_JWT_TOKEN   = os.getenv("ANGEL_JWT_TOKEN")
ANGEL_LTP_URL     = "https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/"