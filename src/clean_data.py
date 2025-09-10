# src/clean_data.py
from pathlib import Path
import pandas as pd
import numpy as np
import re
import os
from dotenv import load_dotenv

# ---------------- Load Paths from .env ----------------
load_dotenv()

raw = os.getenv("RAW_DATA_PATH")
clean = os.getenv("CLEAN_DATA_PATH")

RAW_PATH = Path(raw) if raw else None
CLEAN_PATH = Path(clean) if clean else None

if not RAW_PATH or not RAW_PATH.exists():
    raise FileNotFoundError(f"Raw data not found at: {RAW_PATH}\n"
                            f"Tip: Check RAW_DATA_PATH in your .env")

if not CLEAN_PATH:
    raise ValueError("CLEAN_DATA_PATH not set in .env file.")

# ---------------- Helper Functions for data Cleaning ----------------
def standardize_configuration(val):
    """Convert 3bhk, 3-BHK, 3 BHK → 3BHK"""
    if pd.isna(val): 
        return np.nan
    s1 = str(val).lower().strip()
    s1 = re.sub(r'[^\w]', '', s1)
    m = re.search(r'(\d+)', s1)
    return f"{int(m.group(1))}BHK" if m else s1.upper()

def extract_bedrooms(val):
    """Extract number from '3BHK' → 3"""
    if pd.isna(val): 
        return np.nan
    m = re.search(r'(\d+)', str(val))
    return int(m.group(1)) if m else np.nan

def parse_ticket_price_to_crore(s1):
    """Convert Ticket Price like '₹1.5 Cr', '85 Lakh', '1,20,00,000' → float in Crore"""
    if pd.isna(s1): 
        return np.nan
    s2 = str(s1).lower().replace(',', '').strip()
    s2 = re.sub(r'[₹$]', '', s2)
    if s2 == '' or s2 in ['nan', 'none']:
        return np.nan
    m_num = re.search(r'([0-9]+(?:\.[0-9]+)?)', s2)
    if not m_num:
        return np.nan
    val = float(m_num.group(1))
    if 'cr' in s2 or 'crore' in s2:
        return val
    if 'lakh' in s2 or 'lac' in s2:
        return val / 100.0
    if val > 1000:  # if raw number like 12000000
        return val / 1e7
    return val

# ---------------- Main Cleaning ----------------
def main():
    df = pd.read_csv(RAW_PATH)
    print("Raw data loaded:", RAW_PATH)
    print("Shape before cleaning:", df.shape)

    # --- Step 1: Clean Columns ---
    df['Configuration'] = df['Configuration'].apply(standardize_configuration)
    df['Bedrooms'] = df['Configuration'].apply(extract_bedrooms).astype('Int64')
    df['Ticket_Price_Cr'] = df['Ticket_Price_Cr'].apply(parse_ticket_price_to_crore)

    df['NRI_Buyer'] = df['NRI_Buyer'].str.lower().map({'yes': True, 'no': False})

    # --- Step 2: Handle Missing Values ---
    for col in df.columns:
        if df[col].isnull().sum() == 0:
            continue
        if df[col].dtype == "object":
            mode_val = df[col].mode().iloc[0] if not df[col].mode().empty else "Unknown"
            df[col] = df[col].fillna(mode_val)
            print(f"Filled '{col}' with mode → {mode_val}")
        elif pd.api.types.is_numeric_dtype(df[col]):
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            print(f"Filled '{col}' with median → {median_val}")
        else:
            df[col] = df[col].fillna("Unknown")
            print(f"Filled '{col}' with 'Unknown'")

    # --- Step 3: Handle Outliers ---
    before = df.shape[0]
    # Bedrooms filter: keep only 1 to 6
    df = df[(df['Bedrooms'] >= 1) & (df['Bedrooms'] <= 6)]
    # Ticket Price filter: keep 0.2 Cr to 30 Cr
    df = df[(df['Ticket_Price_Cr'] >= 0.2) & (df['Ticket_Price_Cr'] <= 30)]
    after = df.shape[0]
    print(f"\nOutlier handling removed {before - after} rows (kept {after}).")
    

    # --- Step 4: Fix Purchase_Quarter ---
    if 'Purchase_Quarter' in df.columns:
        df['Purchase_Quarter'] = pd.to_datetime(df['Purchase_Quarter'], errors='coerce')
        df['Purchase_Quarter'] = df['Purchase_Quarter'].dt.to_period('Q').astype(str)
        df['Purchase_Year'] = df['Purchase_Quarter'].str[:4]
        df['Quarter'] = df['Purchase_Quarter'].str[-2:]
        print("\nDEBUG: Purchase Quarter transformation preview:")
        print(df[['Purchase_Quarter', 'Purchase_Year', 'Quarter']].head(10))

    # --- Step 5: Derived Column1:Price_per_Sqft ---
    if 'Unit_Size_Sqft' in df.columns:
        df['Price_Per_Sqft'] = (df['Ticket_Price_Cr'] * 1e7 / df['Unit_Size_Sqft']).round(2)
        # to check outlier as Error inserting data: (1264, "Out of range value for column 'Price_Per_Sqft' at row 237")
        print("Max Price_Per_Sqft:", df['Price_Per_Sqft'].max())
        print(df[df['Price_Per_Sqft'] > 100000].head(10))
 
    # --- Step 6: Derived Column2:Booking Flag ---
    if 'Possession_Status' in df.columns:
        df['Booking_Flag'] = df['Possession_Status'].str.lower().map({
            'ready to move': 1,
            'under construction': 0,
            'launch': 0
        }).fillna(0).astype(int)
        print("\nDEBUG: Booking_Flag transformation preview:")
        print(df[['Possession_Status', 'Booking_Flag']].head(10))

     # --- Step 7: Ensure Unique Property_IDs ---
    if 'Property_ID' in df.columns:
        before_dupes = df.shape[0]
        df = df.drop_duplicates(subset=['Property_ID'], keep='first')
        after_dupes = df.shape[0]
        print(f"\nDuplicate handling removed {before_dupes - after_dupes} rows. Kept {after_dupes} unique Property_IDs.")
    
    # --- Step 8: Save Cleaned Data ---
    if not CLEAN_PATH.parent.exists():
        CLEAN_PATH.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(CLEAN_PATH, index=False)
    print("Cleaned data saved at:", CLEAN_PATH)
    print("Final shape:", df.shape)
    print("\nPreview:\n", df.head())

# ---------------- Run Script ----------------
if __name__ == "__main__":
    main()
