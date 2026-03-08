import socket
import random
import json
import threading

from clock import wait_for_tick, ack
from src.helpers import config

PORT = 9999


def run_server():
    current_prices = {ticker: price for ticker, price in config.ALL_STOCKS[:12]}

    ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ssocket.bind(('', PORT))
    ssocket.listen(10)
    ssocket.setblocking(False)
    print(f"SE1 Server ready on port {PORT}")

    clients = []
    clients_lock = threading.Lock()
    tick = 0

    def accept_loop():
        """Continuously accepts new investor connections in a background thread."""
        while True:
            try:
                ssocket.setblocking(True)
                # block here until someone connects
                c, addr = ssocket.accept()
                c.setblocking(True)
                with clients_lock:
                    clients.append(c)
                print(f"[SE1] Client connected from {addr} "
                      f"(total: {len(clients)})")
            except OSError:
                break           # server socket was closed — time to exit

    threading.Thread(target=accept_loop, daemon=True, name='SE1-accept').start()

    def broadcast(msg_bytes):
        """Send msg_bytes to every connected client; drop the ones that fail."""
        dead = []
        with clients_lock:
            for c in clients:
                try:
                    c.sendall(msg_bytes)
                except (ConnectionResetError, BrokenPipeError, OSError):
                    print("[SE1] A client disconnected — removing.")
                    dead.append(c)
            for c in dead:
                c.close()
                clients.remove(c)

    try:
        while True:
            date_str = wait_for_tick(tick)
            print(f"[SE1] Tick {tick} → {date_str}")

            # Evolve prices and broadcast to ALL connected investors
            for ticker in current_prices:
                current_prices[ticker] *= (1 + random.uniform(-0.03, 0.03))
                msg = json.dumps({
                    "date":   date_str,
                    "ticker": ticker,
                    "price":  round(current_prices[ticker], 2)
                }) + '\n'
                broadcast(msg.encode())

            ack("se1")
            tick += 1

    except KeyboardInterrupt:
        print("\n[SE1] Server shutting down.")
    finally:
        with clients_lock:
            for c in clients:
                c.close()
        ssocket.close()


if __name__ == "__main__":
    run_server()
