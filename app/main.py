from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from app.routes import live_price

from app.routes import backtest
from app.routes import historical

app = FastAPI(title="STM Backtest API", version="1.0.0")

# ✅ GZIP compression (BIG WIN)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(backtest.router, prefix="/api", tags=["backtest"])
app.include_router(historical.router, prefix="/api", tags=["historical"])
app.include_router(live_price.router, prefix="/api", tags=["live-price"])

@app.get("/health")
def health_check():
    return {"status": "ok"}