# The original list of 24 stocks as tuples
ALL_STOCKS = [
    ('IBM', 223.35), ('AAPL', 266.18), ('META', 637.25), ('AMZN', 205.27),
    ('GOOG', 311.69), ('MU', 420.97), ('MSI', 465.03), ('INTC', 43.63),
    ('AMD', 196.60), ('MSFT', 384.47), ('DELL', 119.14), ('ORCL', 141.31),
    ('HPQ', 18.35), ('CSCO', 77.74), ('ZM', 86.06), ('QCOM', 140.41),
    ('ADBE', 246.68), ('VZ', 49.68), ('TXN', 219.86), ('CRM', 178.16),
    ('AVGO', 330.34), ('NVDA', 191.55), ('MSTR', 123.71), ('PLTR', 130.60)
]

# Portfolio definitions remain the same for the investors [cite: 17]
PORTFOLIOS = {
    "P11": {"IBM": 13000, "AAPL": 22000, "META": 19000, "AMZN": 25000, "GOOG": 19000, "AVGO": 24000},
    "P12": {"VZ": 29000, "INTC": 26000, "AMD": 21000, "MSFT": 12000, "PLTR": 27000, "ORCL": 12000},
    "P21": {"HPQ": 16000, "CSCO": 17000, "ZM": 19000, "QCOM": 21000, "ADBE": 28000, "VZ": 17000},
    "P22": {"TXN": 14000, "CRM": 26000, "AVGO": 17000, "NVDA": 18000, "MSTR": 26000, "MSI": 18000},
    "P31": {"HPQ": 22000, "ZM": 18000, "DELL": 24000, "NVDA": 12000, "IBM": 19000, "INTC": 16000},
    "P32": {"VZ": 18000, "AVGO": 29000, "NVDA": 16000, "AAPL": 22000, "DELL": 25000, "ORCL": 20000}
}