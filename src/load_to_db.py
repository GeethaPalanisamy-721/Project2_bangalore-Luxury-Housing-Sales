import os
import pymysql
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Paths from .env
CLEAN_PATH = os.getenv("CLEAN_DATA_PATH")
if not CLEAN_PATH or not os.path.exists(CLEAN_PATH):
    raise FileNotFoundError(f"Cleaned CSV not found at: {CLEAN_PATH}")

# MySQL connection details from .env
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "db_luxury_housing")

# Read cleaned CSV
df = pd.read_csv(CLEAN_PATH)
print(f"Loaded cleaned data: {df.shape} rows, {df.shape[1]} columns")

# Connect to DB
conn = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    autocommit=True
)
cursor = conn.cursor()

# Prepare insert statement
insert_sql = """
INSERT INTO luxury_housing (
    Property_ID, Micro_Market, Project_Name, Developer_Name,
    Unit_Size_Sqft, Configuration, Ticket_Price_Cr,
    Transaction_Type, Buyer_Type, Purchase_Quarter,
    Connectivity_Score, Amenity_Score, Possession_Status,
    Sales_Channel, NRI_Buyer, Locality_Infra_Score,
    Avg_Traffic_Time_Min, Buyer_Comments, Bedrooms,
    Purchase_Year, Quarter, Price_Per_Sqft, Booking_Flag
) VALUES (
    %(Property_ID)s, %(Micro_Market)s, %(Project_Name)s, %(Developer_Name)s,
    %(Unit_Size_Sqft)s, %(Configuration)s, %(Ticket_Price_Cr)s,
    %(Transaction_Type)s, %(Buyer_Type)s, %(Purchase_Quarter)s,
    %(Connectivity_Score)s, %(Amenity_Score)s, %(Possession_Status)s,
    %(Sales_Channel)s, %(NRI_Buyer)s, %(Locality_Infra_Score)s,
    %(Avg_Traffic_Time_Min)s, %(Buyer_Comments)s, %(Bedrooms)s,
    %(Purchase_Year)s, %(Quarter)s, %(Price_Per_Sqft)s, %(Booking_Flag)s
)
"""

# Convert DataFrame to list of dicts for executemany
records = df.to_dict(orient="records")

# Bulk insert
try:
    cursor.executemany(insert_sql, records)
    print(f"Inserted {cursor.rowcount} rows into luxury_housing")
except Exception as e:
    print("Error inserting data:", e)
finally:
    cursor.close()
    conn.close()
