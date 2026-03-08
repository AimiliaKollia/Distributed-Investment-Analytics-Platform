import json

from kafka import KafkaProducer

from clock import wait_for_tick, ack
from src.helpers import config


def on_send_success(record_metadata):
    print(f"[SE2] Sent → topic={record_metadata.topic} "
          f"partition={record_metadata.partition} "
          f"offset={record_metadata.offset}")

def on_send_error(excp):
    print(f"[SE2] Kafka send error: {excp}")


def run_server():
    producer = KafkaProducer(
        bootstrap_servers=['127.0.0.1:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=5000,
        metadata_max_age_ms=30000,
        acks='all'
    )

    current_prices = {ticker: price for ticker, price in config.ALL_STOCKS[12:]}
    topic = 'StockExchange'
    tick = 0

    print(f"SE2 Server ready: emitting to Kafka topic '{topic}'...")

    try:
        while True:
            # ── Wait for coordinator to publish this tick ──────────────────
            date_str = wait_for_tick(tick)
            print(f"[SE2] Tick {tick} → {date_str}")

            for ticker in current_prices:
                msg = {
                    "date": date_str,
                    "ticker": ticker,
                    "price": round(current_prices[ticker], 2)
                }
                print(f"[SE2] Sending: {msg}")
                (producer
                    .send(topic, value=msg)
                    .add_callback(on_send_success)
                    .add_errback(on_send_error))

            producer.flush()

            # ── Signal coordinator that SE2 is done with this tick ─────────
            ack("se2")
            tick += 1

    except KeyboardInterrupt:
        print("\n[SE2] Server shutting down.")
    finally:
        producer.close()


if __name__ == "__main__":
    run_server()
