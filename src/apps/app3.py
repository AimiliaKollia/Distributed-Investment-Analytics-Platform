import json
from kafka import KafkaConsumer
from pymongo import MongoClient
 
 
def get_mongo_connection():
    client = MongoClient("mongodb://localhost:27017/")
    return client["Portfolios"]
 
 
def main():
    # Kafka consumer (same style as app1)
    consumer = KafkaConsumer(
        "portfolios",
        bootstrap_servers=["localhost:9092"],
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="app3-mongo-writer",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
 
    db = get_mongo_connection()
 
    print("[APP3] Listening to Kafka topic 'portfolios'...")
 
    try:
        for msg in consumer:
            portfolio_key = msg.key
            payload = msg.value
 
            # Safety checks (same logic as app1)
            if not portfolio_key:
                print("[APP3] Skipping message with missing portfolio key.")
                continue
 
            if not payload:
                print(f"[APP3] Empty payload for {portfolio_key}")
                continue
 
            # Choose collection dynamically
            collection = db[portfolio_key]
 
            # Optional: avoid duplicates (same date)
            existing = collection.find_one({"Date": payload.get("Date")})
            if existing:
                print(f"[APP3] Duplicate date for {portfolio_key}, updating instead.")
                collection.update_one(
                    {"Date": payload.get("Date")},
                    {"$set": payload}
                )
            else:
                collection.insert_one(payload)
 
            print(f"[APP3] Inserted into {portfolio_key}: {payload}")
 
    except KeyboardInterrupt:
        print("\n[APP3] Stopping consumer...")
 
    finally:
        consumer.close()
        print("[APP3] Shutdown complete.")
 
 
if __name__ == "__main__":
    main()
