import uvicorn
import logging

# Only show DEBUG for your own service, keep others at INFO
logging.basicConfig(level=logging.INFO)
logging.getLogger("app.services.live_price_service").setLevel(logging.DEBUG)

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)