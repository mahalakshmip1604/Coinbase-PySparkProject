import json, websocket

ws = websocket.create_connection("wss://ws-feed.exchange.coinbase.com", timeout=15)
ws.send(json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}))
for _ in range(5):
    print(ws.recv())
ws.close()