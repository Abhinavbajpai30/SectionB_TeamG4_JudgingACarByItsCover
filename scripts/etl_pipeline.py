"""
etl_pipeline.py
===============
Used Vehicle Pricing Intelligence — Full ETL Pipeline
DVA Capstone 2 | Newton School of Technology

This script consolidates the complete ETL workflow from raw data extraction
through feature engineering to the final cleaned export. It is designed to be
run as a standalone script or imported as a module.

Pipeline stages:
  1. Load raw data from data/raw/car_prices.csv
  2. Standardise categorical columns (strip whitespace, normalise case)
  3. Handle missing values (median/mode imputation per group)
  4. Apply IQR-based winsorisation to numeric outliers
  5. Filter invalid / out-of-scope rows
  6. Engineer 13 analytical features
  7. Export cleaned dataset to data/processed/car_prices_cleaned.csv
  8. Print before/after summary statistics

Usage:
  python scripts/etl_pipeline.py
"""

import pandas as pd
import numpy as np
import os
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

# === SECTION: Path Configuration ===

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
RAW_PATH     = os.path.join(PROJECT_ROOT, 'data', 'raw', 'car_prices.csv')
PROCESSED_DIR = os.path.join(PROJECT_ROOT, 'data', 'processed')
OUTPUT_PATH  = os.path.join(PROCESSED_DIR, 'car_prices_cleaned.csv')


# === SECTION: Helper Functions ===

def print_separator(title: str = '', width: int = 65) -> None:
    """Print a formatted section separator."""
    if title:
        print(f"\n{'=' * width}")
        print(f"  {title}")
        print(f"{'=' * width}")
    else:
        print(f"{'=' * width}")


def iqr_winsorise(series: pd.Series, multiplier: float = 2.5) -> pd.Series:
    """
    Apply IQR-based winsorisation to a numeric Series.
    Values beyond Q1 - multiplier*IQR and Q3 + multiplier*IQR are clipped.
    """
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - multiplier * iqr
    upper = q3 + multiplier * iqr
    clipped = series.clip(lower=lower, upper=upper)
    n_clipped = (series < lower).sum() + (series > upper).sum()
    return clipped, lower, upper, int(n_clipped)


def print_summary_stats(df: pd.DataFrame, label: str = 'Dataset') -> None:
    """Print concise summary statistics for the pipeline QA log."""
    print(f"\n  {label}:")
    print(f"    Rows          : {len(df):,}")
    print(f"    Columns       : {df.shape[1]}")
    if 'sellingprice' in df.columns:
        print(f"    Avg sell price: ${df['sellingprice'].mean():,.2f}")
        print(f"    Total volume  : ${df['sellingprice'].sum():,.0f}")
    if 'mmr' in df.columns:
        print(f"    Avg MMR       : ${df['mmr'].mean():,.2f}")
    missing_total = df.isnull().sum().sum()
    print(f"    Missing values: {missing_total:,}")


# === SECTION: Main Pipeline ===

def run_pipeline() -> pd.DataFrame:
    """
    Execute the full ETL pipeline and return the cleaned DataFrame.
    Also exports the cleaned data to data/processed/car_prices_cleaned.csv.
    """

    start_time = datetime.now()
    print_separator("USED VEHICLE PRICING INTELLIGENCE — ETL PIPELINE")
    print(f"  Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # ------------------------------------------------------------------
    # STAGE 1: Load Raw Data
    # ------------------------------------------------------------------
    print_separator("STAGE 1: Load Raw Data")

    if not os.path.exists(RAW_PATH):
        raise FileNotFoundError(
            f"Raw data not found at: {RAW_PATH}\n"
            "Ensure car_prices.csv is placed in data/raw/ before running."
        )

    df = pd.read_csv(RAW_PATH, low_memory=False)
    print(f"  Loaded: {RAW_PATH}")
    print_summary_stats(df, label="Raw Dataset")

    raw_rows = len(df)

    # ------------------------------------------------------------------
    # STAGE 2: Standardise Categorical Columns
    # ------------------------------------------------------------------
    print_separator("STAGE 2: Standardise Categorical Columns")

    # Strip whitespace from all string columns
    str_cols = df.select_dtypes(include='object').columns.tolist()
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({'nan': np.nan, 'NaN': np.nan, '': np.nan})

    # Title-case standardisation for key categorical fields
    title_case_cols = ['make', 'model', 'trim', 'body', 'color', 'interior', 'seller']
    for col in title_case_cols:
        if col in df.columns:
            df[col] = df[col].str.title()

    # Lowercase for transmission and state
    for col in ['transmission', 'state']:
        if col in df.columns:
            df[col] = df[col].str.lower()

    # Normalise transmission values
    if 'transmission' in df.columns:
        df['transmission'] = df['transmission'].replace({
            'auto': 'automatic',
            'manual': 'manual',
            'automatic': 'automatic'
        })

    print("  Categorical columns standardised (Title Case / lowercase).")

    # ------------------------------------------------------------------
    # STAGE 3: Handle Missing Values
    # ------------------------------------------------------------------
    print_separator("STAGE 3: Handle Missing Values")

    before_missing = df.isnull().sum().sum()

    # (a) Impute condition — median per make/body group
    if 'condition' in df.columns:
        df['condition'] = df.groupby(['make', 'body'])['condition'].transform(
            lambda x: x.fillna(x.median())
        )
        # Fallback: global median
        df['condition'] = df['condition'].fillna(df['condition'].median())
        print("  condition: imputed with group median (make + body).")

    # (b) Impute mmr — median per make/model/year group
    if 'mmr' in df.columns:
        df['mmr'] = df.groupby(['make', 'model', 'year'])['mmr'].transform(
            lambda x: x.fillna(x.median())
        )
        df['mmr'] = df['mmr'].fillna(df['mmr'].median())
        print("  mmr: imputed with group median (make + model + year).")

    # (c) Impute transmission — mode per make/body group
    if 'transmission' in df.columns:
        def fill_mode(x):
            mode = x.mode()
            return x.fillna(mode[0] if not mode.empty else 'automatic')
        df['transmission'] = df.groupby(['make', 'body'])['transmission'].transform(fill_mode)
        df['transmission'] = df['transmission'].fillna('automatic')
        print("  transmission: imputed with group mode (make + body).")

    # (d) Drop rows missing sellingprice (only 12 rows — not worth imputing)
    if 'sellingprice' in df.columns:
        before = len(df)
        df = df.dropna(subset=['sellingprice'])
        print(f"  sellingprice: dropped {before - len(df)} rows with missing values.")

    # (e) Impute odometer — median per make/year
    if 'odometer' in df.columns:
        df['odometer'] = df.groupby(['make', 'year'])['odometer'].transform(
            lambda x: x.fillna(x.median())
        )
        df['odometer'] = df['odometer'].fillna(df['odometer'].median())
        print("  odometer: imputed with group median (make + year).")

    after_missing = df.isnull().sum().sum()
    print(f"\n  Missing values reduced: {before_missing:,} → {after_missing:,}")

    # ------------------------------------------------------------------
    # STAGE 4: IQR Winsorisation (outlier treatment)
    # ------------------------------------------------------------------
    print_separator("STAGE 4: IQR Winsorisation (2.5x multiplier)")

    winsorise_cols = ['sellingprice', 'mmr', 'odometer']
    for col in winsorise_cols:
        if col in df.columns:
            df[col], lo, hi, n = iqr_winsorise(df[col], multiplier=2.5)
            print(f"  {col}: clipped {n:,} values to [{lo:,.0f}, {hi:,.0f}]")

    # ------------------------------------------------------------------
    # STAGE 5: Filter Invalid Rows
    # ------------------------------------------------------------------
    print_separator("STAGE 5: Filter Invalid Rows")

    before_filter = len(df)

    # Remove pre-1990 vehicles
    if 'year' in df.columns:
        df = df[df['year'] >= 1990]
        print(f"  Removed {before_filter - len(df):,} rows with year < 1990.")

    before_price = len(df)

    # Remove non-positive selling prices
    if 'sellingprice' in df.columns:
        df = df[df['sellingprice'] > 0]
        print(f"  Removed {before_price - len(df):,} rows with sellingprice <= 0.")

    # Remove extreme odometer readings (> 300,000 miles after winsorisation)
    if 'odometer' in df.columns:
        before_odo = len(df)
        df = df[df['odometer'] <= 300000]
        print(f"  Removed {before_odo - len(df):,} rows with odometer > 300,000 miles.")

    print(f"\n  Rows retained: {len(df):,} / {raw_rows:,} ({len(df)/raw_rows*100:.1f}%)")

    # ------------------------------------------------------------------
    # STAGE 6: Feature Engineering
    # ------------------------------------------------------------------
    print_separator("STAGE 6: Feature Engineering (13 features)")

    # Parse saledate to datetime
    if 'saledate' in df.columns:
        df['saledate_parsed'] = pd.to_datetime(df['saledate'], errors='coerce', utc=True)
        df['sale_year']      = df['saledate_parsed'].dt.year
        df['sale_month']     = df['saledate_parsed'].dt.month
        df['sale_month_name']= df['saledate_parsed'].dt.strftime('%B')
        df['sale_dow']       = df['saledate_parsed'].dt.dayofweek   # 0=Monday
        df['sale_dow_name']  = df['saledate_parsed'].dt.strftime('%A')
        df.drop(columns=['saledate_parsed'], inplace=True)
        print("  sale_year, sale_month, sale_month_name, sale_dow, sale_dow_name: parsed from saledate.")

    # Vehicle age at sale
    if 'sale_year' in df.columns and 'year' in df.columns:
        df['vehicle_age'] = df['sale_year'] - df['year']
        df['vehicle_age'] = df['vehicle_age'].clip(lower=0)
        print("  vehicle_age: sale_year - manufacture year.")

    # Price deviation metrics
    if 'sellingprice' in df.columns and 'mmr' in df.columns:
        df['price_deviation']      = df['sellingprice'] - df['mmr']
        df['price_deviation_pct']  = ((df['sellingprice'] - df['mmr']) / df['mmr']) * 100
        df['price_realization_rate'] = (df['sellingprice'] / df['mmr']) * 100
        df['sold_above_mmr']       = (df['sellingprice'] > df['mmr']).astype(int)
        print("  price_deviation, price_deviation_pct, price_realization_rate, sold_above_mmr: computed.")

    # Price per age year
    if 'sellingprice' in df.columns and 'vehicle_age' in df.columns:
        df['price_per_age_year'] = df.apply(
            lambda r: r['sellingprice'] / r['vehicle_age'] if r['vehicle_age'] > 0 else np.nan,
            axis=1
        )
        print("  price_per_age_year: sellingprice / vehicle_age.")

    # Odometer bucket
    if 'odometer' in df.columns:
        df['odometer_bucket'] = pd.cut(
            df['odometer'],
            bins=[0, 30000, 60000, 100000, 150000, float('inf')],
            labels=['0-30k', '30-60k', '60-100k', '100-150k', '150k+'],
            right=True
        ).astype(str)
        print("  odometer_bucket: binned into 5 bands.")

    # Condition tier
    if 'condition' in df.columns:
        quartiles = df['condition'].quantile([0.25, 0.5, 0.75])
        q25, q50, q75 = quartiles[0.25], quartiles[0.50], quartiles[0.75]

        def assign_tier(c):
            if pd.isna(c):
                return np.nan
            if c <= q25:
                return 'Poor'
            elif c <= q50:
                return 'Fair'
            elif c <= q75:
                return 'Good'
            else:
                return 'Excellent'

        df['condition_tier'] = df['condition'].apply(assign_tier)
        print(f"  condition_tier: Poor/Fair/Good/Excellent (quartile breakpoints: {q25:.1f}/{q50:.1f}/{q75:.1f}).")

    print(f"\n  Final column count: {df.shape[1]}")

    # ------------------------------------------------------------------
    # STAGE 7: Export Cleaned Dataset
    # ------------------------------------------------------------------
    print_separator("STAGE 7: Export Cleaned Dataset")

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    file_size_mb = os.path.getsize(OUTPUT_PATH) / 1024**2
    print(f"  Exported: {OUTPUT_PATH}")
    print(f"  File size: {file_size_mb:.1f} MB")

    # ------------------------------------------------------------------
    # STAGE 8: Before / After Summary
    # ------------------------------------------------------------------
    print_separator("STAGE 8: Pipeline Summary")

    print_summary_stats(pd.read_csv(RAW_PATH, nrows=5).pipe(lambda _: df), label="Cleaned Dataset")

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n  Rows removed   : {raw_rows - len(df):,} ({(raw_rows - len(df))/raw_rows*100:.2f}%)")
    print(f"  Rows retained  : {len(df):,}")
    print(f"  Features added : 13")
    print(f"  Final shape    : {df.shape}")
    print(f"  Elapsed time   : {elapsed:.1f}s")

    print_separator()
    print("  Pipeline complete. Cleaned dataset ready for analysis and Tableau.")
    print_separator()

    return df


# === SECTION: Entry Point ===

if __name__ == '__main__':
    run_pipeline()
