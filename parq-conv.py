import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
token = os.getenv('MOTHERDUCK_TOKEN')

# Connect to MotherDuck
con = duckdb.connect(f"md:skyeco_dev?motherduck_token={token}")

# Export your main table to a compressed Parquet file
con.execute("COPY (SELECT * FROM skyeco_dev.main.stg_flights) TO 'flights_snapshot.parquet' (FORMAT PARQUET)")
print("Snapshot created: flights_snapshot.parquet")