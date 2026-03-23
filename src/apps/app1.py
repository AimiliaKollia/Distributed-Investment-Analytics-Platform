import json
from kafka import KafkaConsumer
import mysql.connector


PORTFOLIO_TABLE_MAP = {
    "P11": "Inv1_P11",
    "P12": "Inv1_P12",
    "P21": "Inv2_P21",
    "P22": "Inv2_P22",
    "P31": "Inv3_P31",
    "P32": "Inv3_P32",
}


def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="itc6107",
        password="itc6107",
        database="InvestorsDB",
    )


def main():
    consumer = KafkaConsumer(
        "portfolios",
        bootstrap_servers=["localhost:9092"],
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="app1-mysql-writer",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    conn = get_db_connection()
    cursor = conn.cursor()

    print("[APP1] Listening to Kafka topic 'portfolios'...")

    try:
        for msg in consumer:
            portfolio_key = msg.key
            payload = msg.value

            if not portfolio_key:
                print("[APP1] Skipping message with missing portfolio key.")
                continue

            table_name = PORTFOLIO_TABLE_MAP.get(portfolio_key)
            if not table_name:
                print(f"[APP1] Unknown portfolio key '{portfolio_key}' - skipping.")
                continue

            date_val = payload.get("Date")
            nav_val = payload.get("NAV")
            change_val = payload.get("Daily_NAV_Change")
            pct_val = payload.get("Daily_NAV_Change_Pct")

            if None in (date_val, nav_val, change_val, pct_val):
                print(f"[APP1] Incomplete payload for {portfolio_key}: {payload}")
                continue

            sql = f"""
                INSERT INTO `{table_name}`
                (Date, NAV, Daily_NAV_Change, Daily_NAV_Change_Pct)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    NAV = VALUES(NAV),
                    Daily_NAV_Change = VALUES(Daily_NAV_Change),
                    Daily_NAV_Change_Pct = VALUES(Daily_NAV_Change_Pct)
            """

            values = (date_val, nav_val, change_val, pct_val)

            try:
                cursor.execute(sql, values)
                conn.commit()
                print(f"[APP1] Inserted {portfolio_key} -> {table_name}: {payload}")
            except mysql.connector.Error as e:
                conn.rollback()
                print(f"[APP1] MySQL insert error for {portfolio_key}: {e}")

    except KeyboardInterrupt:
        print("\n[APP1] Stopping consumer...")

    finally:
        cursor.close()
        conn.close()
        consumer.close()
        print("[APP1] Shutdown complete.")


if __name__ == "__main__":
    main()

