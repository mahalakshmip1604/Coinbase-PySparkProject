import argparse, json, socket, time
import websocket

URL = "wss://ws-feed.exchange.coinbase.com"

def main(host, port, products, channels):
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port)); srv.listen(1)
    print(f"Waiting for Spark on {host}:{port} ...")
    conn, _ = srv.accept()
    print("Spark connected, streaming.")
    state = {"stop": False, "n": 0}

    def on_open(ws):
        ws.send(json.dumps({"type": "subscribe", "product_ids": products, "channels": channels}))

    def on_message(ws, msg):
        try:
            conn.sendall((msg.replace("\n", " ") + "\n").encode())   # one JSON message per line
            state["n"] += 1
            if state["n"] % 500 == 0:
                print(f"forwarded {state['n']} messages")
        except OSError:                                              # Spark disconnected
            state["stop"] = True
            ws.close()

    while not state["stop"]:
        app = websocket.WebSocketApp(URL, on_open=on_open, on_message=on_message,
                                     on_error=lambda ws, e: print("ws error:", e))
        app.run_forever(ping_interval=20, ping_timeout=10)
        if not state["stop"]:
            print("Disconnected, reconnecting in 2s"); time.sleep(2)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9999)
    p.add_argument("--products", nargs="+", default=["BTC-USD", "ETH-USD", "SOL-USD"])
    p.add_argument("--channels", nargs="+", default=["ticker"])
    a = p.parse_args()
    main(a.host, a.port, a.products, a.channels)