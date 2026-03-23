import json
import threading
import random
import socket
import time
from kafka import KafkaConsumer, KafkaProducer


class InvestorEngine:
    def __init__(self, portfolios, investor_name):
        self.portfolios = portfolios
        self.investor_name = investor_name

        # All distinct stocks this investor cares about
        self.all_watched_stocks = {s for p in portfolios.values() for s in p}

        # date -> {ticker -> price}
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

        # Filled when start() runs
        self.se1_watched = set()
        self.se2_watched = set()

    def _set_source_splits(self, se1_tickers, se2_tickers):
        self.se1_watched = self.all_watched_stocks & se1_tickers
        self.se2_watched = self.all_watched_stocks & se2_tickers
        print(f"[{self.investor_name}] SE1 watched: {self.se1_watched}")
        print(f"[{self.investor_name}] SE2 watched: {self.se2_watched}")

    def process_message(self, data):
        with self.lock:
            date = data['date']
            ticker = data['ticker']
            price = data['price']

            if ticker not in self.all_watched_stocks:
                return

            self.daily_cache.setdefault(date, {})[ticker] = price

            received = set(self.daily_cache[date].keys())
            missing = self.all_watched_stocks - received
            current_count = len(received)
            required_count = len(self.all_watched_stocks)

            print(
                f"[{self.investor_name}] Date: {date} | "
                f"Collected: {current_count}/{required_count} | "
                f"Just added: {ticker} | "
                f"Still missing: {missing if missing else 'none'}"
            )

            se1_complete = self.se1_watched.issubset(received)
            se2_complete = self.se2_watched.issubset(received)

            if se1_complete and se2_complete:
                print(f"--- [OK] Gate opened for {date} (SE1 ✓  SE2 ✓). Calculating NAV... ---")
                self.calculate_daily_metrics(date)
            else:
                waiting = []
                if not se1_complete:
                    waiting.append(f"SE1 missing {self.se1_watched - received}")
                if not se2_complete:
                    waiting.append(f"SE2 missing {self.se2_watched - received}")
                print(f"[{self.investor_name}] Waiting: {' | '.join(waiting)}")
    def calculate_daily_metrics(self, date):
        prices = self.daily_cache[date]

        for p_name, p_qty in self.portfolios.items():
            total_assets = sum(prices[s] * qty for s, qty in p_qty.items())
            liabilities = total_assets * random.uniform(0.65, 0.70)
            net_assets = total_assets - liabilities

            total_portfolio_shares = sum(p_qty.values())
            nav = net_assets / total_portfolio_shares

            change = 0.0
            pct_change = 0.0
            if self.prev_navs[p_name] is not None:
                change = nav - self.prev_navs[p_name]
                pct_change = (change / self.prev_navs[p_name]) * 100

            self.prev_navs[p_name] = nav

            payload = {
                "Date": date,
                "NAV": round(nav, 2),
                "Daily_NAV_Change": round(change, 2),
                "Daily_NAV_Change_Pct": round(pct_change, 2),
            }

            self.producer.send("portfolios", key=p_name.encode("utf-8"), value=payload)
            print(f"[{self.investor_name}] Published {p_name} -> key='{p_name}' | {payload}")

        self.producer.flush()
        del self.daily_cache[date]

    def start(self):
        from src.helpers import config

        se1_tickers = {ticker for ticker, _ in config.ALL_STOCKS[:12]}
        se2_tickers = {ticker for ticker, _ in config.ALL_STOCKS[12:]}
        self._set_source_splits(se1_tickers, se2_tickers)

        def se1_listener():
            while True:
                sock = None
                file_obj = None
                try:
                    print(f"[{self.investor_name}] Connecting to SE1 on port 9999...")
                    sock = socket.create_connection(("127.0.0.1", 9999), timeout=10)
                    file_obj = sock.makefile("r")
                    print(f"[{self.investor_name}] TCP listener connected to SE1 on port 9999.")

                    for line in file_obj:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            self.process_message(json.loads(line))
                        except json.JSONDecodeError as e:
                            print(f"[{self.investor_name}] Bad JSON from SE1: {e}")

                    print(f"[{self.investor_name}] SE1 connection closed. Retrying...")

                except Exception as e:
                    print(f"[{self.investor_name}] SE1 connection error: {e}. Retrying in 2 seconds...")
                    time.sleep(2)

                finally:
                    try:
                        if file_obj:
                            file_obj.close()
                    except Exception:
                        pass
                    try:
                        if sock:
                            sock.close()
                    except Exception:
                        pass

        def kafka_listener():
            consumer = KafkaConsumer(
                'StockExchange',
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id=None,
                auto_offset_reset='latest',
                enable_auto_commit=False,
            )
            print(f"[{self.investor_name}] Kafka listener subscribed to StockExchange.")
            for msg in consumer:
                self.process_message(msg.value)

        threading.Thread(
            target=se1_listener,
            daemon=True,
            name=f'{self.investor_name}-tcp'
        ).start()

        kafka_listener()
