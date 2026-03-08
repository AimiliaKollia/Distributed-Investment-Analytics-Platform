import socket
import random
import json

from clock import wait_for_tick, ack
from src.helpers import config

PORT = 9999


def run_server():
    current_prices = {ticker: price for ticker, price in config.ALL_STOCKS[:12]}

    ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ssocket.bind(('', PORT))
    ssocket.listen(1)
    ssocket.setblocking(False)
    print(f"SE1 Server ready on port {PORT}")

    client = None
    tick = 0

    try:
        while True:
            # ── Wait for coordinator to publish this tick ──────────────────
            date_str = wait_for_tick(tick)
            print(f"[SE1] Tick {tick} → {date_str}")

            # Try to accept a new connection without blocking
            try:
                c, addr = ssocket.accept()
                client = c
                client.setblocking(False)
                print(f"[SE1] Client connected from {addr}")
            except BlockingIOError:
                pass

            # Evolve prices and emit
            for ticker in current_prices:
                current_prices[ticker] *= (1 + random.uniform(-0.03, 0.03))
                msg = json.dumps({
                    "date": date_str,
                    "ticker": ticker,
                    "price": round(current_prices[ticker], 2)
                })

                if client:
                    try:
                        client.send((msg + '\n').encode())
                    except (ConnectionResetError, BrokenPipeError, BlockingIOError):
                        print("[SE1] Client disconnected.")
                        client.close()
                        client = None

            # ── Signal coordinator that SE1 is done with this tick ─────────
            ack("se1")
            tick += 1

    except KeyboardInterrupt:
        print("\n[SE1] Server shutting down.")
    finally:
        if client:
            client.close()
        ssocket.close()


if __name__ == "__main__":
    run_server()
