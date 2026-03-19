
import asyncio
import websockets
import json

async def listen():
    uri = "ws://localhost:8000/api/live-price/stream"
    print("Connecting to live stream...")
    async with websockets.connect(uri) as ws:
        print("✅ Connected! Waiting for ticks...\n")
        async for message in ws:
            tick = json.loads(message)
            print(f"📈 {tick['token']} | LTP: ₹{tick['ltp']} | High: ₹{tick.get('high')} | Low: ₹{tick.get('low')} | Volume: {tick.get('volume')}")

asyncio.run(listen())