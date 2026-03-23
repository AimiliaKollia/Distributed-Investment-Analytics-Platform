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

MYSQL_URL = "jdbc:mysql://127.0.0.1:3306/InvestorsDB"
MYSQL_PROPERTIES = {
    "user": "itc6107",
    "password": "itc6107",
    "driver": "com.mysql.cj.jdbc.Driver"
}

PERIOD_START_YEAR = 2020
PERIOD_END_YEAR = 2024


def load_portfolio_table(spark, table_name):
    return spark.read.jdbc(
        url=MYSQL_URL,
        table=table_name,
        properties=MYSQL_PROPERTIES
    )


def analyze_portfolio(df):
    df = df.withColumn("Date", col("Date").cast("date"))

    # Full-history overall stats
    overall_stats = df.select(
        spark_min("Daily_NAV_Change").alias("min_daily_change"),
        spark_max("Daily_NAV_Change").alias("max_daily_change"),
        spark_min("Daily_NAV_Change_Pct").alias("min_daily_change_pct"),
        spark_max("Daily_NAV_Change_Pct").alias("max_daily_change_pct"),
        avg("NAV").alias("avg_nav"),
        stddev("NAV").alias("std_nav")
    ).collect()[0]

    # Full-history yearly stats
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

    # Given-period stats
    period_df = df.filter(
        (year("Date") >= PERIOD_START_YEAR) &
        (year("Date") <= PERIOD_END_YEAR)
    )

    if period_df.count() > 0:
        period_stats = period_df.select(
            avg("NAV").alias("avg_nav_period"),
            stddev("NAV").alias("std_nav")
        ).collect()[0]
    else:
        period_stats = None

    # Full-history monthly averages, most recent month first
    monthly_avg = (
        df.withColumn("Year", year("Date"))
        .withColumn("Month", month("Date"))
        .groupBy("Year", "Month")
        .agg(avg("NAV").alias("avg_nav"))
        .orderBy(col("Year").desc(), col("Month").desc())
        .collect()
    )

    return overall_stats, yearly_stats, period_stats, monthly_avg


def save_report(table_name, overall_stats, yearly_stats, period_stats, monthly_avg, output_dir="output"):
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{table_name}_stats.txt")

    std_full = overall_stats["std_nav_per_share"]
    std_full = std_full if std_full is not None else "N/A"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"Portfolio: {table_name}\n")
        f.write("=" * 60 + "\n\n")

        f.write("FULL HISTORY STATISTICS\n")
        f.write(f"Min Daily NAV Change: {overall_stats['min_daily_change']}\n")
        f.write(f"Max Daily NAV Change: {overall_stats['max_daily_change']}\n")
        f.write(f"Min Daily NAV Change %: {overall_stats['min_daily_change_pct']}\n")
        f.write(f"Max Daily NAV Change %: {overall_stats['max_daily_change_pct']}\n")
        f.write(f"Average NAV : {overall_stats['avg_nav']}\n")
        f.write(f"Std Dev NAV : {std_full}\n\n")

        f.write("YEARLY STATISTICS\n")
        for row in yearly_stats:
            f.write(
                f"Year {row['Year']}: "
                f"Min Change={row['min_daily_change']}, "
                f"Max Change={row['max_daily_change']}, "
                f"Min %={row['min_daily_change_pct']}, "
                f"Max %={row['max_daily_change_pct']}\n"
            )

        f.write(f"\nPERIOD STATISTICS ({PERIOD_START_YEAR}-{PERIOD_END_YEAR})\n")
        if period_stats is None:
            f.write("No data available for this period.\n")
        else:
            std_period = period_stats["std_nav_period"]
            std_period = std_period if std_period is not None else "N/A"
            f.write(f"Average NAV : {period_stats['avg_nav_period']}\n")
            f.write(f"Std Dev NAV : {std_period}\n")

        f.write("\nMONTHLY AVERAGE NAV (MOST RECENT FIRST)\n")
        for row in monthly_avg:
            f.write(
                f"{row['Year']}-{row['Month']:02d}: "
                f"{row['avg_nav']}\n"
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

            overall_stats, yearly_stats, period_stats, monthly_avg = analyze_portfolio(df)
            save_report(table_name, overall_stats, yearly_stats, period_stats, monthly_avg)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
