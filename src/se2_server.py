import random
import time
import json
from datetime import datetime, timedelta
import holidays
from kafka import KafkaProducer

from src.helpers.date import get_next_trading_day
from src.helpers import config


def run_server():
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    current_prices = {ticker: price for ticker, price in config.ALL_STOCKS[12:]}

    topic = 'StockExchange'
    # Start date is 1/1/2020
    current_date = datetime(2020, 1, 1)

    print(f"SE2 Server ready: emitting to Kafka topic '{topic}'...")

    try:
        while True:
            valid_date = get_next_trading_day(current_date)
            date_str = valid_date.strftime('%Y-%m-%d')
            time.sleep(3)  # [cite: 10]

            for ticker in current_prices:
                current_prices[ticker] *= (1 + random.uniform(-0.03, 0.03))

                msg = {
                    "date": date_str,
                    "ticker": ticker,
                    "price": round(current_prices[ticker], 2)
                }
                producer.send('StockExchange', value=msg)

            producer.flush()
            current_date = valid_date
    except KeyboardInterrupt:
        print("\nSE2 Server shutting down.")
    finally:
        producer.close()


if __name__ == "__main__":
    run_server()