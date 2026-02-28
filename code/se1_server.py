import socket
import random

PORT = 9999

stock = [
    ('IBM', 223.35), ('AAPL', 266.18), ('META', 637.25), ('AMZN', 205.27),
    ('GOOG', 311.69), ('MU', 420.97), ('MSI', 465.03), ('INTC', 43.63),
    ('AMD', 196.60), ('MSFT', 384.47), ('DELL', 119.14), ('ORKL', 141.31),
    ('HPQ', 18.35), ('CSCO', 77.74), ('ZM', 86.06), ('QCOM', 140.41),
    ('ADBE', 246.68), ('VZ', 49.68), ('TXN', 219.86), ('CRM', 178.16),
    ('AVGO', 330.34), ('NVDA', 191.55), ('MSTR', 123.71), ('PLTR', 130.60)
]

ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# ssocket.bind((socket.gethostname(), PORT))
ssocket.bind(('', PORT))
ssocket.listen()
print(f"Server ready: listening to port {PORT} for connections.\n")
(c, addr) = ssocket.accept()

for s in stock:
    ticker, price = s
    msg = <create a message to emit>
    print(msg)
    c.send((msg + '\n').encode())

while True:
    <sleep for at least 3 seconds>
    ticker = <Pick a ticker symbol>
    price = <Calculate new stock price for ticker>
    msg = '<create a message to emit>
    print(msg)
    c.send((msg + '\n').encode())

