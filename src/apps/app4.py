import sys
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
 
def main():
    if len(sys.argv) != 4:
        print("Usage: python3 app4.py <PortfolioName> <StartDate> <EndDate>")
        print("Date format: YYYY-MM-DD")
        sys.exit(1)
 
    portfolio_name = sys.argv[1]
    start_date_str = sys.argv[2]
    end_date_str = sys.argv[3]
 
    # Parse dates
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD")
        sys.exit(1)
 
    # Connect to MongoDB
    try:
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        client.server_info()  # Force connection check
    except ConnectionFailure:
        print("Could not connect to MongoDB. Is it running?")
        sys.exit(1)
 
    db = client["Portfolios"]
 
    if portfolio_name not in db.list_collection_names():
        print(f"Portfolio '{portfolio_name}' does not exist in MongoDB")
        sys.exit(1)
 
    collection = db[portfolio_name]
 
    # Query documents in the period
    query = {"Date": {"$gte": start_date_str, "$lte": end_date_str}}
    docs = list(collection.find(query))
 
    if not docs:
        print(f"No NAV data found for {portfolio_name} in the period {start_date_str} to {end_date_str}")
        sys.exit(0)
 
    nav_values = [doc["NAV_per_Share"] for doc in docs]
    num_values = len(nav_values)
    min_nav = min(nav_values)
    max_nav = max(nav_values)
    avg_nav = sum(nav_values) / num_values
 
    # Output report
    print(f"\nPortfolio Report: {portfolio_name}")
    print("="*50)
    print(f"Period: {start_date_str} → {end_date_str}")
    print(f"Number of NAV values: {num_values}")
    print(f"Minimum NAV: {min_nav:.2f}")
    print(f"Maximum NAV: {max_nav:.2f}")
    print(f"Average NAV: {avg_nav:.2f}")
    print("="*50)
 
if __name__ == "__main__":
    main()
