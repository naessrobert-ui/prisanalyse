#!/usr/bin/env python3
"""Debug: tester ulike bbox-størrelser mot AISstream."""
import asyncio, json, os, sys
from dotenv import load_dotenv
load_dotenv()

try:
    import websockets
except ImportError:
    print("pip install websockets", file=sys.stderr); sys.exit(1)

API_KEY = os.getenv("AISTREAM_API_KEY", "").strip()
if not API_KEY:
    print("Sett AISTREAM_API_KEY", file=sys.stderr); sys.exit(1)

TESTS = [
    ("Trang Hormuz-kjerne", [[[26.0, 55.5], [27.0, 57.0]]]),
    ("Bred Hormuz", [[[23.5, 55.5], [27.5, 60.5]]]),
    ("Hele verden (ingen filter)", [[[-90, -180], [90, 180]]]),
]

async def test_bbox(name, bbox, seconds=20):
    sub = {"APIKey": API_KEY, "BoundingBoxes": bbox}
    print(f"\n=== Test: {name} | bbox={bbox} ===")
    try:
        async with websockets.connect(
            "wss://stream.aisstream.io/v0/stream",
            open_timeout=15,
        ) as ws:
            await ws.send(json.dumps(sub))
            print(f"Subscription sendt. Venter {seconds}s ...")
            count = 0
            try:
                async with asyncio.timeout(seconds):
                    async for raw in ws:
                        data = json.loads(raw)
                        if data.get("error"):
                            print(f"FEIL: {data['error']}")
                            return 0
                        count += 1
                        meta = data.get("MetaData", {})
                        print(f"  [{count}] {data.get('MessageType')} | "
                              f"skip={meta.get('ShipName','?')} | "
                              f"lat={meta.get('latitude')} lon={meta.get('longitude')}")
                        if count >= 5:
                            break
            except asyncio.TimeoutError:
                print(f"  Timeout etter {seconds}s — mottatt {count} meldinger")
            return count
    except Exception as exc:
        print(f"  Feil: {exc}")
        return 0

async def main():
    for name, bbox in TESTS:
        count = await test_bbox(name, bbox)
        if count > 0:
            print(f"✓ Fungerer med '{name}'!")
            break
        await asyncio.sleep(2)

asyncio.run(main())
