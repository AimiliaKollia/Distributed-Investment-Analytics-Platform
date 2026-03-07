import random
import time
import json
from datetime import datetime, timedelta
import holidays
from kafka import KafkaProducer

from src.helpers.date import get_next_trading_day
from src.helpers import config

def on_send_success(record_metadata):
    print(f"Successfully sent to {record_metadata.topic} "
          f"partition {record_metadata.partition} "
          f"offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Error sending message: {excp}")
def run_server():
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['127.0.0.1:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Force a version that doesn't trigger the metadata version error
        api_version=(0, 10, 1),
        # Ensure the producer doesn't wait indefinitely if the broker is unreachable
        request_timeout_ms=5000,
        metadata_max_age_ms=30000
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
            time.sleep(5)

            for ticker in current_prices:
                msg = {"date": date_str, "ticker": ticker, "price": round(current_prices[ticker], 2)}
                producer.send('StockExchange', value=msg).add_callback(on_send_success).add_errback(on_send_error)
            print("before flush")
            producer.flush()
            print("after flush")
            current_date = valid_date
    except KeyboardInterrupt:
        print("\nSE2 Server shutting down.")
    finally:
        producer.close()


if __name__ == "__main__":
    run_server()