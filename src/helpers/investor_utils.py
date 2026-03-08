import socket
import json
import threading
import random
from kafka import KafkaConsumer, KafkaProducer


class InvestorEngine:
    def __init__(self, portfolios, investor_name):
        self.portfolios = portfolios
        self.investor_name = investor_name
        self.all_watched_stocks = {s for p in portfolios.values() for s in p}
        self.daily_cache = {}
        self.prev_navs = {p_name: None for p_name in portfolios}
        self.lock = threading.Lock()

        self.producer = KafkaProducer(
            bootstrap_servers=['127.0.0.1:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000,
            metadata_max_age_ms=30000,
            acks='all'
        )

    def process_message(self, data):
        with self.lock:
            date, ticker, price = data['date'], data['ticker'], data['price']

            if ticker not in self.all_watched_stocks:
                return

            self.daily_cache.setdefault(date, {})[ticker] = price

            received       = set(self.daily_cache[date].keys())
            missing        = self.all_watched_stocks - received
            current_count  = len(received)
            required_count = len(self.all_watched_stocks)

            print(f"[{self.investor_name}] Date: {date} | "
                  f"Collected: {current_count}/{required_count} | "
                  f"Just added: {ticker} | "
                  f"Still missing: {missing if missing else 'none'}")

            if not missing:
                print(f"--- [OK] Gate opened for {date}. Calculating NAV... ---")
                self.calculate_daily_metrics(date)

    def calculate_daily_metrics(self, date):
        """Calculates NAV, Daily_Change, and Daily_Change_Percent for each portfolio."""
        prices = self.daily_cache[date]

        for p_name, p_qty in self.portfolios.items():
            # Total evaluation of assets at close of day
            total_assets = sum(prices[s] * qty for s, qty in p_qty.items())

            # Liabilities: simulated as 65%–70% of total assets
            liabilities = total_assets * random.uniform(0.65, 0.70)

            current_nav = total_assets - liabilities

            change     = 0.0
            pct_change = 0.0
            if self.prev_navs[p_name] is not None:
                change     = current_nav - self.prev_navs[p_name]
                pct_change = (change / self.prev_navs[p_name]) * 100

            self.prev_navs[p_name] = current_nav

            payload = {
                "Date":                 date,
                "NAV":                  round(current_nav, 2),
                "Daily_Change":         round(change, 2),
                "Daily_Change_Percent": round(pct_change, 2),
            }

            self.producer.send('portfolios', key=p_name.encode('utf-8'), value=payload)
            print(f"[{self.investor_name}] Published {p_name} → {payload}")

        self.producer.flush()
        del self.daily_cache[date]

    def start(self):
        """Runs the TCP (SE1) and Kafka (SE2) listeners concurrently."""

        def tcp_listener():
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(('localhost', 9999))
                    print(f"[{self.investor_name}] TCP listener connected to SE1.")
                    for line in s.makefile():
                        if line.strip():
                            self.process_message(json.loads(line))
            except ConnectionRefusedError:
                print(f"[{self.investor_name}] WARNING: SE1 not reachable on port 9999.")

        def kafka_listener():
            consumer = KafkaConsumer(
                'StockExchange',
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                # No group_id: every restart gets a fresh anonymous consumer.
                # Kafka will never have committed offsets for this instance,
                # so auto_offset_reset='latest' always applies cleanly.
                group_id=None,
                # Only receive messages produced from this moment forward.
                # History is ignored — each restart is a new event.
                auto_offset_reset='latest',
                # No offset commits — stateless by design.
                enable_auto_commit=False,
            )
            print(f"[{self.investor_name}] Kafka listener subscribed to StockExchange (latest only).")
            for msg in consumer:
                self.process_message(msg.value)

        threading.Thread(target=tcp_listener, daemon=True, name=f'{self.investor_name}-tcp').start()
        kafka_listener()
