# src/load_data.py
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv

# Load env vars
load_dotenv()
raw = os.getenv("RAW_DATA_PATH")
RAW_PATH = Path(raw) if raw else None

if not RAW_PATH or not RAW_PATH.exists():
    raise FileNotFoundError(f"Data file not found at: {RAW_PATH}\n"
                            f"Tip: Check RAW_DATA_PATH in your .env")

# Load raw data
df = pd.read_csv(RAW_PATH)

print("Raw data loaded:", RAW_PATH)
print("Shape:", df.shape)
print("\nColumns and dtypes:\n", df.dtypes)
print("\nNull counts:\n", df.isnull().sum())
print("\nSample rows:\n", df.head(10))
print("\nNumeric describe:\n", df.describe(include='number').T)
