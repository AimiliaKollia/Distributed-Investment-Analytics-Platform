import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, avg, stddev, min as spark_min, max as spark_max
)

INVESTOR_PORTFOLIOS = {
    "Inv1": ["P11", "P12"],
    "Inv2": ["P21", "P22"],
    "Inv3": ["P31", "P32"],
}

MYSQL_URL = "jdbc:mysql://localhost:3306/InvestorsDB"
MYSQL_PROPERTIES = {
    "user": "bigdatauser",
    "password": "bigdatapass",
    "driver": "com.mysql.cj.jdbc.Driver"
}


def load_portfolio_table(spark, table_name):
    return spark.read.jdbc(
        url=MYSQL_URL,
        table=table_name,
        properties=MYSQL_PROPERTIES
    )


def analyze_portfolio(df, table_name):
    df = df.withColumn("Date", col("Date").cast("date"))

    # Overall stats
    overall_stats = df.select(
        spark_min("Daily_NAV_Change").alias("min_daily_change"),
        spark_max("Daily_NAV_Change").alias("max_daily_change"),
        spark_min("Daily_NAV_Change_Pct").alias("min_daily_change_pct"),
        spark_max("Daily_NAV_Change_Pct").alias("max_daily_change_pct"),
        avg("NAV_per_Share").alias("avg_nav_per_share"),
        stddev("NAV_per_Share").alias("std_nav_per_share")
    ).collect()[0]

    # Yearly stats
    yearly_stats = (
        df.withColumn("Year", year("Date"))
          .groupBy("Year")
          .agg(
              spark_min("Daily_NAV_Change").alias("min_daily_change"),
              spark_max("Daily_NAV_Change").alias("max_daily_change"),
              spark_min("Daily_NAV_Change_Pct").alias("min_daily_change_pct"),
              spark_max("Daily_NAV_Change_Pct").alias("max_daily_change_pct")
          )
          .orderBy("Year")
          .collect()
    )

    # Monthly averages
    monthly_avg = (
        df.withColumn("Year", year("Date"))
          .withColumn("Month", month("Date"))
          .groupBy("Year", "Month")
          .agg(avg("NAV_per_Share").alias("avg_nav_per_share"))
          .orderBy(col("Year").desc(), col("Month").desc())
          .collect()
    )

    return overall_stats, yearly_stats, monthly_avg


def save_report(table_name, overall_stats, yearly_stats, monthly_avg, output_dir="output"):
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{table_name}_stats.txt")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"Portfolio: {table_name}\n")
        f.write("=" * 50 + "\n\n")

        f.write("OVERALL STATISTICS\n")
        f.write(f"Min Daily NAV Change: {overall_stats['min_daily_change']}\n")
        f.write(f"Max Daily NAV Change: {overall_stats['max_daily_change']}\n")
        f.write(f"Min Daily NAV Change %: {overall_stats['min_daily_change_pct']}\n")
        f.write(f"Max Daily NAV Change %: {overall_stats['max_daily_change_pct']}\n")
        f.write(f"Average NAV per Share: {overall_stats['avg_nav_per_share']}\n")
        f.write(f"Std Dev NAV per Share: {overall_stats['std_nav_per_share']}\n\n")

        f.write("YEARLY STATISTICS\n")
        for row in yearly_stats:
            f.write(
                f"Year {row['Year']}: "
                f"Min Change={row['min_daily_change']}, "
                f"Max Change={row['max_daily_change']}, "
                f"Min %={row['min_daily_change_pct']}, "
                f"Max %={row['max_daily_change_pct']}\n"
            )

        f.write("\nMONTHLY AVERAGE NAV PER SHARE\n")
        for row in monthly_avg:
            f.write(
                f"{row['Year']}-{row['Month']:02d}: "
                f"{row['avg_nav_per_share']}\n"
            )

    print(f"[APP2] Report saved: {output_path}")


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 src/apps/app2.py <InvestorName>")
        sys.exit(1)

    investor_name = sys.argv[1]

    if investor_name not in INVESTOR_PORTFOLIOS:
        print(f"Unknown investor '{investor_name}'. Choose from: {list(INVESTOR_PORTFOLIOS.keys())}")
        sys.exit(1)

    spark = (
        SparkSession.builder
        .appName(f"App2_{investor_name}")
        .master("local[*]")
        .config("spark.jars", "./src/apps/mysql-connector-java-8.0.28.jar")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        for portfolio in INVESTOR_PORTFOLIOS[investor_name]:
            table_name = f"{investor_name}_{portfolio}"
            print(f"[APP2] Reading table {table_name}...")

            df = load_portfolio_table(spark, table_name)

            if df.limit(1).count() == 0:
                print(f"[APP2] Table {table_name} is empty. Skipping.")
                continue

            overall_stats, yearly_stats, monthly_avg = analyze_portfolio(df, table_name)
            save_report(table_name, overall_stats, yearly_stats, monthly_avg)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

