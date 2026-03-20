# app/main.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from app.routes import live_price
from app.routes import backtest
from app.routes import historical
from app.routes import search

app = FastAPI(title="STM Backtest API", version="1.0.0")

app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(backtest.router,    prefix="/api", tags=["backtest"])
app.include_router(historical.router,  prefix="/api", tags=["historical"])
app.include_router(live_price.router,  prefix="/api", tags=["live-price"])
app.include_router(search.router,      prefix="/api", tags=["search"])


@app.get("/health")
async def health_check():
    return {"status": "ok"}