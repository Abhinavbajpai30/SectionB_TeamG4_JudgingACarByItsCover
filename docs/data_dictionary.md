# Data Dictionary — Used Vehicle Pricing Intelligence

## Dataset Overview

- **Source:** Manheim Used Vehicle Sales Dataset (Kaggle)
- **Raw file:** data/raw/car_prices.csv
- **Cleaned file:** data/processed/car_prices_cleaned.csv
- **Raw shape:** 558,837 rows × 16 columns
- **Cleaned shape:** 558,675 rows × 29 columns
- **Sector:** Automotive / Used Vehicle Market

---

## Column Definitions

### Original Columns (16)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| year | int | Vehicle manufacture year | 2015 |
| make | str | Vehicle brand / manufacturer | Ford |
| model | str | Vehicle model name | Focus |
| trim | str | Trim level / variant of the model | SE |
| body | str | Body style / type | Sedan |
| transmission | str | Transmission type (automatic or manual) | automatic |
| vin | str | Vehicle Identification Number — unique 17-character ID | 1FADP3F21FL264541 |
| state | str | US state code where the vehicle was sold | fl |
| condition | float | Vehicle condition score on a 1–49 scale (higher = better) | 3.5 |
| odometer | float | Recorded odometer reading at time of sale (miles) | 52,354 |
| color | str | Exterior paint colour | Black |
| interior | str | Interior trim colour | Black |
| seller | str | Name of the selling entity (dealer / auction house) | Nissan Infiniti Lt |
| mmr | float | Manheim Market Report price — wholesale market benchmark ($) | 13,500.00 |
| sellingprice | float | Actual transaction selling price ($) | 13,250.00 |
| saledate | str | Sale timestamp string (parsed to datetime during cleaning) | Tue Dec 16 2014 12:30:00 GMT-0800 |

---

### Engineered Features (13)

| Column | Type | Formula / Description | Values |
|--------|------|-----------------------|--------|
| sale_year | int | Calendar year extracted from saledate | 2014, 2015 |
| sale_month | int | Month number extracted from saledate | 1–12 |
| sale_month_name | str | Full month name extracted from saledate | January … December |
| sale_dow | int | Day of week number extracted from saledate (0 = Monday) | 0–6 |
| sale_dow_name | str | Day of week name extracted from saledate | Monday … Sunday |
| vehicle_age | int | Age of the vehicle at time of sale in years: sale_year − year | 0–35 |
| price_deviation | float | Absolute difference between selling price and MMR: sellingprice − mmr | −5,000 … +5,000 |
| price_deviation_pct | float | Price deviation as a percentage of MMR: (sellingprice − mmr) / mmr × 100 | −50.0 … +50.0 |
| price_realization_rate | float | Selling price as a percentage of MMR: sellingprice / mmr × 100 | 50.0 … 150.0 |
| sold_above_mmr | int | Binary flag: 1 if sellingprice > mmr, else 0 | 0 or 1 |
| price_per_age_year | float | Selling price normalised by vehicle age: sellingprice / vehicle_age | varies; NaN when vehicle_age = 0 |
| odometer_bucket | str | Odometer reading grouped into five mileage bands | 0-30k, 30-60k, 60-100k, 100-150k, 150k+ |
| condition_tier | str | Condition score assigned to quartile-based tier labels | Poor, Fair, Good, Excellent |

---

## Data Quality Notes

- **Transmission missingness (11.69%)** was the most significant quality issue. Missing values were imputed using the statistical mode of the transmission type within each make/body group, reflecting realistic fleet-level patterns rather than arbitrary defaults.
- **IQR-based winsorisation** (2.5× multiplier) was applied to `sellingprice`, `mmr`, and `odometer` to suppress the influence of extreme outliers without removing records. This preserves dataset size while protecting aggregations.
- **162 rows were filtered out** during cleaning: records with `year < 1990` (fringe antique vehicles outside the scope of modern used-car pricing) and rows with `sellingprice <= 0` (likely data entry errors or administrative placeholders).
- **Condition tier boundaries** are data-driven (quartile-based), not fixed: Poor ≤ Q1, Fair ≤ Q2, Good ≤ Q3, Excellent > Q3. This ensures balanced class distribution across all downstream analyses.
