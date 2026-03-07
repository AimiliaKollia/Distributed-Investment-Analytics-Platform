import socket
import random
import time
import json
from datetime import datetime
from src.helpers.date import get_next_trading_day
from src.helpers import config

PORT = 9999

def run_server():
    # Convert to dict for easier daily price updates
    current_prices = {ticker: price for ticker, price in config.ALL_STOCKS[:12]}
    current_date = datetime(2020, 1, 1)

    # Create server socket once — reuse across client reconnections
    ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ssocket.bind(('', PORT))
    ssocket.listen(1)
    print(f"SE1 Server ready on port {PORT}")

    try:
        while True:  # Outer loop: accept new clients
            print("Waiting for client connection...")
            c, addr = ssocket.accept()
            print(f"Connection established with {addr}")

            try:
                while True:  # Inner loop: stream prices to connected client
                    valid_date = get_next_trading_day(current_date)
                    date_str = valid_date.strftime('%Y-%m-%d')

                    for ticker in current_prices:
                        current_prices[ticker] *= (1 + random.uniform(-0.03, 0.03))
                        msg = json.dumps({
                            "date": date_str,
                            "ticker": ticker,
                            "price": round(current_prices[ticker], 2)
                        })
                        c.send((msg + '\n').encode())

                    current_date = valid_date
                    time.sleep(5)

            except (ConnectionResetError, BrokenPipeError):
                print("Client disconnected. Waiting for new connection...")
            finally:
                c.close()  # Always clean up client socket

    except KeyboardInterrupt:
        print("\nServer shutting down.")
    finally:
        ssocket.close()  # Clean up server socket on exit

if __name__ == "__main__":
    run_server()