"""
data_generator.py — Food Waste Optimization 360
================================================
Generates five referentially consistent synthetic CSV files using Faker.
Output matches the schema of the real macro_*.csv source files so the
entire downstream pipeline (bronze_loader → silver → gold) works unchanged.

Files generated (saved to DATA_DIR, default: data/raw/):
  macro_location_data.csv      ~20 rows
  macro_menu_data.csv          ~200 rows
  macro_supplier_data.csv      ~50 supplier-item records (with SCD2 history)
  macro_production_logs.csv    ~10,000 rows
  macro_waste_logs.csv         ~6,000 rows

Referential integrity guarantees (enforced at generation time):
  - All location_id values in production_logs exist in location_data
  - All menu_item_id values in production_logs exist in menu_data
  - All supplier records map to valid menu_item_id values in menu_data
  - waste_quantity <= quantity_prepared for every matching production row
  - waste_logs reference valid (date, location_id, menu_item_id, meal_period)
    combinations that exist in production_logs

Usage:
  python ingestion/data_generator.py
  python ingestion/data_generator.py --rows-production 10000 --rows-waste 6000
"""

import argparse
import os
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker("en_IN")   # Indian locale for realistic city/name data
random.seed(42)
Faker.seed(42)

DATA_DIR = Path(os.environ.get("DATA_DIR", "data/raw"))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATE_START = date(2025, 1, 1)
DATE_END   = date(2025, 12, 31)

MEAL_PERIODS   = ["Breakfast", "Lunch", "Dinner", "Snack"]
WASTE_REASONS  = ["overproduction", "spoilage", "low demand",
                  "forecast miss", "prep error", "plate waste"]
WASTE_STAGES   = ["Serving", "Storage", "Preparation", "Portioning"]
CATEGORIES     = ["Main Course", "Starter", "Dessert", "Beverage",
                  "Salads", "Dairy", "Fresh Juice", "Snacks", "Breads"]
SUB_CATEGORIES = {
    "Main Course": ["Rice", "Curry", "Pasta", "Noodles"],
    "Starter":     ["Soup", "Salad", "Tikka"],
    "Dessert":     ["Mithai", "Ice Cream", "Cake"],
    "Beverage":    ["Hot Drinks", "Cold Drinks", "Juices"],
    "Salads":      ["Green Salad", "Fruit Salad"],
    "Dairy":       ["Curd", "Paneer", "Milk Products"],
    "Fresh Juice": ["Citrus", "Mixed Fruit", "Vegetable"],
    "Snacks":      ["Fried", "Baked", "Steamed"],
    "Breads":      ["Indian", "Continental"],
}
PREP_COMPLEXITIES = ["Low", "Medium", "High"]
LOCATION_TYPES    = ["Campus", "Corporate", "Hospital", "Hostel"]
STORAGE_RATINGS   = ["A", "B", "C"]
REGIONS           = ["North", "South", "East", "West"]
CITIES = [
    "Bangalore", "Hyderabad", "Chennai", "Mumbai", "Pune",
    "Delhi", "Kolkata", "Ahmedabad", "Jaipur", "Kochi",
]


def _date_range(start: date, end: date) -> list[date]:
    days = (end - start).days + 1
    return [start + timedelta(days=i) for i in range(days)]


ALL_DATES = _date_range(DATE_START, DATE_END)


# ---------------------------------------------------------------------------
# 1. Location data
# ---------------------------------------------------------------------------
def generate_location_data(n: int = 20) -> pd.DataFrame:
    rows = []
    used_names = set()
    for i in range(1, n + 1):
        loc_type = random.choice(LOCATION_TYPES)
        city = random.choice(CITIES)
        name = f"{city} {loc_type} {i}"
        while name in used_names:
            name = f"{city} {loc_type} {i}-{random.randint(2,9)}"
        used_names.add(name)

        open_dt = fake.date_between(start_date=date(2020, 1, 1), end_date=date(2023, 12, 31))
        rows.append({
            "location_id":    f"LOC{i:03d}",
            "location_name":  name,
            "city":           city,
            "region":         random.choice(REGIONS),
            "location_type":  loc_type,
            "capacity":       random.randint(200, 1200),
            "open_date":      open_dt.isoformat(),
            "manager_name":   fake.name(),
            "storage_rating": random.choice(STORAGE_RATINGS),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 2. Menu data
# ---------------------------------------------------------------------------
def generate_menu_data(n: int = 200) -> pd.DataFrame:
    rows = []
    used_names = set()
    for i in range(1, n + 1):
        category = random.choice(CATEGORIES)
        sub_cat  = random.choice(SUB_CATEGORIES[category])
        name_base = fake.word().capitalize() + " " + sub_cat
        name = name_base
        suffix = 1
        while name in used_names:
            suffix += 1
            name = f"{name_base} {suffix}"
        used_names.add(name)

        rows.append({
            "menu_item_id":         f"MI{i:04d}",
            "menu_item_name":       name,
            "category":             category,
            "sub_category":         sub_cat,
            "veg_flag":             random.choice(["Yes", "No"]),
            "standard_portion_size":f"{random.choice([1,2,3])} {random.choice(['pcs','bowl','cup','plate'])}",
            "cost_per_unit":        round(random.uniform(15.0, 120.0), 2),
            "shelf_life_hours":     random.choice([4, 8, 12, 24, 36, 48, 72]),
            "prep_complexity":      random.choice(PREP_COMPLEXITIES),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 3. Supplier data  (SCD2-style — current + some historical records)
# ---------------------------------------------------------------------------
def generate_supplier_data(menu_df: pd.DataFrame, n_suppliers: int = 10) -> pd.DataFrame:
    """
    Each supplier covers multiple menu items.
    Some suppliers have historical records (simulated attribute change).
    """
    menu_ids  = menu_df["menu_item_id"].tolist()
    supplier_names = [fake.company() for _ in range(n_suppliers)]
    rows = []
    record_id = 1

    for s_idx in range(n_suppliers):
        sid = f"SUP{s_idx + 1:03d}"
        sname = supplier_names[s_idx]
        scity = random.choice(CITIES)

        # Each supplier covers 3–8 menu items
        covered = random.sample(menu_ids, k=random.randint(3, min(8, len(menu_ids))))
        for mid in covered:
            # Optionally add a historical record (attribute change ~30% of items)
            if random.random() < 0.3:
                hist_start = fake.date_between(start_date=date(2024, 1, 1),
                                               end_date=date(2024, 9, 30))
                hist_end   = hist_start + timedelta(days=random.randint(30, 120))
                rows.append({
                    "supplier_record_id": f"SPR{record_id:05d}",
                    "supplier_id":        sid,
                    "menu_item_id":       mid,
                    "supplier_name":      sname,
                    "supplier_city":      scity,
                    "supplier_cost":      round(random.uniform(10.0, 80.0), 2),
                    "lead_time_days":     random.randint(1, 7),
                    "quality_score":      round(random.uniform(2.0, 4.0), 2),
                    "effective_from":     hist_start.isoformat(),
                    "effective_to":       hist_end.isoformat(),
                    "is_current":         0,
                })
                record_id += 1
                current_start = hist_end + timedelta(days=1)
            else:
                current_start = fake.date_between(start_date=date(2024, 10, 1),
                                                  end_date=date(2025, 1, 1))

            rows.append({
                "supplier_record_id": f"SPR{record_id:05d}",
                "supplier_id":        sid,
                "menu_item_id":       mid,
                "supplier_name":      sname,
                "supplier_city":      scity,
                "supplier_cost":      round(random.uniform(10.0, 80.0), 2),
                "lead_time_days":     random.randint(1, 7),
                "quality_score":      round(random.uniform(3.0, 5.0), 2),
                "effective_from":     current_start.isoformat(),
                "effective_to":       "",
                "is_current":         1,
            })
            record_id += 1

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 4. Production logs
# ---------------------------------------------------------------------------
def generate_production_logs(
    location_df: pd.DataFrame,
    menu_df: pd.DataFrame,
    n_rows: int = 10_000,
) -> pd.DataFrame:
    loc_ids   = location_df["location_id"].tolist()
    loc_map   = location_df.set_index("location_id")["location_name"].to_dict()
    menu_ids  = menu_df["menu_item_id"].tolist()
    menu_map  = menu_df.set_index("menu_item_id")[["menu_item_name", "category", "cost_per_unit"]].to_dict("index")

    rows = []
    for _ in range(n_rows):
        dt    = random.choice(ALL_DATES)
        lid   = random.choice(loc_ids)
        mid   = random.choice(menu_ids)
        meal  = random.choice(MEAL_PERIODS)
        qty   = round(random.uniform(30.0, 400.0), 1)
        plan  = int(qty * random.uniform(0.9, 1.2))
        served = int(qty * random.uniform(0.6, 0.95))
        cost  = menu_map[mid]["cost_per_unit"]

        rows.append({
            "date":                  dt.isoformat(),
            "location_id":           lid,
            "location_name":         loc_map[lid],
            "meal_period":           meal,
            "menu_item_id":          mid,
            "menu_item_name":        menu_map[mid]["menu_item_name"],
            "category":              menu_map[mid]["category"],
            "quantity_prepared":     qty,
            "unit":                  "portions",
            "cost_per_unit":         cost,
            "planned_quantity":      plan,
            "actual_served_estimate":served,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 5. Waste logs
# ---------------------------------------------------------------------------
def generate_waste_logs(
    prod_df: pd.DataFrame,
    n_rows: int = 6_000,
) -> pd.DataFrame:
    """
    Sample production rows and generate a waste event for each.
    Guarantees: waste_quantity <= quantity_prepared for every row.
    """
    # Sample production rows to attach waste to
    sampled = prod_df.sample(n=min(n_rows, len(prod_df)), replace=False).reset_index(drop=True)

    rows = []
    for _, prod_row in sampled.iterrows():
        max_waste = prod_row["quantity_prepared"]
        waste_qty = round(random.uniform(1.0, max_waste * 0.45), 1)   # max 45% waste

        rows.append({
            "date":          prod_row["date"],
            "location_id":   prod_row["location_id"],
            "location_name": prod_row["location_name"],
            "meal_period":   prod_row["meal_period"],
            "menu_item_id":  prod_row["menu_item_id"],
            "menu_item_name":prod_row["menu_item_name"],
            "category":      prod_row["category"],
            "quantity_wasted":waste_qty,
            "unit":          "portions",
            "waste_reason":  random.choice(WASTE_REASONS),
            "waste_stage":   random.choice(WASTE_STAGES),
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(rows_production: int = 10_000, rows_waste: int = 6_000) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("Generating synthetic data...\n")

    # Generate in dependency order
    loc_df  = generate_location_data(n=20)
    menu_df = generate_menu_data(n=200)
    sup_df  = generate_supplier_data(menu_df)
    prod_df = generate_production_logs(loc_df, menu_df, n_rows=rows_production)

    # Enforce composite PK uniqueness — keep first occurrence of each (date, location_id, menu_item_id, meal_period)
    pk_cols = ["date", "location_id", "menu_item_id", "meal_period"]
    prod_df = prod_df.drop_duplicates(subset=pk_cols).reset_index(drop=True)

    waste_df = generate_waste_logs(prod_df, n_rows=rows_waste)

    files = {
        "macro_location_data.csv":    loc_df,
        "macro_menu_data.csv":        menu_df,
        "macro_supplier_data.csv":    sup_df,
        "macro_production_logs.csv":  prod_df,
        "macro_waste_logs.csv":       waste_df,
    }

    for filename, df in files.items():
        path = DATA_DIR / filename
        df.to_csv(path, index=False)
        print(f"  {filename:40s}  {len(df):>7,} rows  →  {path}")

    # Quick integrity check
    print("\nIntegrity checks:")
    prod_keys = set(
        zip(prod_df["date"], prod_df["location_id"],
            prod_df["menu_item_id"], prod_df["meal_period"])
    )
    waste_keys = set(
        zip(waste_df["date"], waste_df["location_id"],
            waste_df["menu_item_id"], waste_df["meal_period"])
    )
    orphan_waste = waste_keys - prod_keys
    print(f"  Orphan waste rows (no matching production): {len(orphan_waste)}")

    # Validate waste <= prepared for all matching rows
    merged = waste_df.merge(
        prod_df[["date", "location_id", "menu_item_id", "meal_period", "quantity_prepared"]],
        on=["date", "location_id", "menu_item_id", "meal_period"],
        how="left",
    )
    violations = (merged["quantity_wasted"] > merged["quantity_prepared"]).sum()
    print(f"  waste_quantity > quantity_prepared violations: {violations}")

    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic food waste data")
    parser.add_argument("--rows-production", type=int, default=10_000)
    parser.add_argument("--rows-waste",      type=int, default=6_000)
    args = parser.parse_args()
    run(rows_production=args.rows_production, rows_waste=args.rows_waste)
