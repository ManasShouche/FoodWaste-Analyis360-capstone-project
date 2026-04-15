# Data Dictionary — Food Waste Optimization 360

All column names, types, and definitions across every layer of the pipeline.

---

## Source Layer (macro_*.csv)

### macro_production_logs.csv
One row per meal-period production batch at a location.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Production date (YYYY-MM-DD) |
| `location_id` | STRING | FK → location_data.location_id |
| `location_name` | STRING | Denormalised location name |
| `meal_period` | STRING | Meal service period: Breakfast / Lunch / Dinner / Snack |
| `menu_item_id` | STRING | FK → menu_data.menu_item_id |
| `menu_item_name` | STRING | Denormalised menu item name |
| `category` | STRING | Denormalised category name |
| `quantity_prepared` | FLOAT | Units prepared for this batch |
| `unit` | STRING | Unit of measure (e.g. "portions") |
| `cost_per_unit` | FLOAT | Cost in INR per unit |
| `planned_quantity` | INT | Units planned based on forecast |
| `actual_served_estimate` | INT | Estimated units actually served |

**Composite key:** (date, location_id, menu_item_id, meal_period)

---

### macro_waste_logs.csv
One row per waste event at a location.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Waste date (YYYY-MM-DD) |
| `location_id` | STRING | FK → location_data.location_id |
| `location_name` | STRING | Denormalised location name |
| `meal_period` | STRING | Meal service period |
| `menu_item_id` | STRING | FK → menu_data.menu_item_id |
| `menu_item_name` | STRING | Denormalised menu item name |
| `category` | STRING | Denormalised category |
| `quantity_wasted` | FLOAT | Units wasted (always ≤ quantity_prepared) |
| `unit` | STRING | Unit of measure |
| `waste_reason` | STRING | Normalised reason: overproduction / spoilage / low demand / forecast miss / prep error / plate waste |
| `waste_stage` | STRING | Stage where waste occurred: Serving / Storage / Preparation / Portioning |

**Constraint:** `quantity_wasted <= quantity_prepared` for any matching (date, location_id, menu_item_id, meal_period) row.

---

### macro_menu_data.csv
One row per menu item (reference/dimension data).

| Column | Type | Description |
|--------|------|-------------|
| `menu_item_id` | STRING | Primary key |
| `menu_item_name` | STRING | Display name |
| `category` | STRING | High-level category (Main Course, Salads, Dairy, etc.) |
| `sub_category` | STRING | More specific sub-grouping |
| `veg_flag` | STRING | "Yes" / "No" — vegetarian indicator |
| `standard_portion_size` | STRING | Typical serving size (e.g. "2 pcs", "1 bowl") |
| `cost_per_unit` | FLOAT | Standard cost per unit in INR |
| `shelf_life_hours` | INT | Hours before item must be discarded |
| `prep_complexity` | STRING | Low / Medium / High |

---

### macro_location_data.csv
One row per kitchen location (reference/dimension data).

| Column | Type | Description |
|--------|------|-------------|
| `location_id` | STRING | Primary key (e.g. LOC001) |
| `location_name` | STRING | Display name |
| `city` | STRING | City name |
| `region` | STRING | North / South / East / West |
| `location_type` | STRING | Campus / Corporate / Hospital / Hostel |
| `capacity` | INT | Seating / serving capacity |
| `open_date` | DATE | Date location opened (YYYY-MM-DD) |
| `manager_name` | STRING | Current manager name |
| `storage_rating` | STRING | Cold storage quality: A / B / C |

---

### macro_supplier_data.csv
SCD2-style supplier records — multiple rows per supplier_id when attributes change.

| Column | Type | Description |
|--------|------|-------------|
| `supplier_record_id` | STRING | Primary key of this physical row (e.g. SPR00001) |
| `supplier_id` | STRING | Natural key of the supplier entity |
| `menu_item_id` | STRING | FK → menu_data.menu_item_id |
| `supplier_name` | STRING | Supplier company name |
| `supplier_city` | STRING | City of supplier |
| `supplier_cost` | FLOAT | Cost charged by supplier per unit in INR |
| `lead_time_days` | INT | Days from order to delivery |
| `quality_score` | FLOAT | Quality rating 1–5 |
| `effective_from` | DATE | Date this record version became active |
| `effective_to` | DATE | Date this version ended (blank = still current) |
| `is_current` | INT | 1 = current active record, 0 = historical |

---

## Silver Layer

Produced by `transforms/silver_transform.py`. Joins production + waste + menu + location and adds derived analytical columns.

**Write path:** `s3://bucket/silver/year={Y}/month={M}/`

All source columns are retained. Key additions and transformations:

### Type-cast columns

| Column | Bronze type | Silver type | Notes |
|--------|-------------|-------------|-------|
| `date` | STRING | DATE | Parsed with DateType cast |
| `quantity_prepared` | STRING | DOUBLE | |
| `quantity_wasted` | STRING | DOUBLE | Coalesced to 0.0 where no waste record |
| `cost_per_unit` | STRING | DOUBLE | |
| `planned_quantity` | STRING | DOUBLE | |
| `actual_served_estimate` | STRING | DOUBLE | |
| `lead_time_days` | STRING | INT | |
| `quality_score` | STRING | DOUBLE | |
| `shelf_life_hours` | STRING | INT | |
| `meal_period` | STRING | STRING | Lowercased and stripped |
| `waste_reason` | STRING | STRING | Stripped + lowercased |
| `category` | STRING | STRING | Uppercased |

### Derived columns (computed in Silver)

| Column | Formula | Notes |
|--------|---------|-------|
| `waste_percentage` | `(quantity_wasted / quantity_prepared) * 100` | 0.0 when quantity_prepared = 0 |
| `waste_cost` | `quantity_wasted * cost_per_unit` | INR |
| `quantity_consumed` | `quantity_prepared - quantity_wasted` | Always ≥ 0 (enforced by DQ gate) |
| `demand_gap` | `planned_quantity - actual_served_estimate` | Positive = over-planned |
| `year` | `YEAR(date)` | Partition column |
| `month` | `MONTH(date)` | Partition column |

### DQ columns (metadata from Bronze)

| Column | Description |
|--------|-------------|
| `ingestion_timestamp` | UTC ISO string of when Bronze ingestion ran |
| `source_file` | Original filename (e.g. macro_production_logs.csv) |
| `batch_id` | UUID identifying the Bronze run that produced this row |

---

## Gold Layer — Dimensions

### DIM_DATE
Generated programmatically for 2025-01-01 to 2025-12-31 (365 rows).

| Column | Type | Description |
|--------|------|-------------|
| `date_sk` | BIGINT | Surrogate key in YYYYMMDD format |
| `full_date` | STRING | ISO date string (YYYY-MM-DD) |
| `day_of_week` | INT | 1=Monday … 7=Sunday |
| `day_name` | STRING | e.g. "Monday" |
| `month` | INT | 1–12 |
| `month_name` | STRING | e.g. "January" |
| `quarter` | INT | 1–4 |
| `year` | INT | e.g. 2025 |
| `is_weekend` | BOOLEAN | True for Saturday and Sunday |

---

### DIM_LOCATION (SCD1)
One row per location. Updated in place on re-load (SCD1 — no history).

| Column | Type | Description |
|--------|------|-------------|
| `location_sk` | STRING | UUID surrogate key |
| `location_id` | STRING | Natural key |
| `location_name` | STRING | |
| `city` | STRING | |
| `region` | STRING | |
| `location_type` | STRING | Campus / Corporate / Hospital / Hostel |
| `capacity` | STRING | Serving capacity |
| `storage_rating` | STRING | A / B / C |

---

### DIM_MENU (SCD1)
One row per menu item. Updated in place on re-load.

| Column | Type | Description |
|--------|------|-------------|
| `menu_sk` | STRING | UUID surrogate key |
| `menu_item_id` | STRING | Natural key |
| `menu_item_name` | STRING | |
| `category` | STRING | Uppercased category |
| `sub_category` | STRING | |
| `veg_flag` | STRING | Yes / No |
| `shelf_life_hours` | INT | |
| `prep_complexity` | STRING | Low / Medium / High |

---

### DIM_CATEGORY (static)
Derived from distinct category values in Silver.

| Column | Type | Description |
|--------|------|-------------|
| `category_sk` | STRING | UUID surrogate key |
| `category_name` | STRING | Uppercased category name |

---

### DIM_MEAL_PERIOD (static)
Four hardcoded values.

| Column | Type | Description |
|--------|------|-------------|
| `meal_period_sk` | STRING | UUID surrogate key |
| `meal_period_name` | STRING | breakfast / lunch / dinner / snack |

---

### DIM_WASTE_REASON (static)
Six normalised waste reason values.

| Column | Type | Description |
|--------|------|-------------|
| `waste_reason_sk` | STRING | UUID surrogate key |
| `waste_reason_name` | STRING | overproduction / spoilage / low demand / plate waste / forecast miss / prep error |

---

### DIM_SUPPLIER (SCD2)
Full version history. Each change to tracked columns creates a new row.

| Column | Type | Description |
|--------|------|-------------|
| `supplier_sk` | STRING | UUID surrogate key — new UUID per version |
| `supplier_id` | STRING | Natural key — stable across versions |
| `supplier_name` | STRING | **Tracked** — change triggers new SCD2 version |
| `menu_item_id` | STRING | Menu item this supplier covers |
| `supplier_city` | STRING | |
| `lead_time_days` | INT | **Tracked** — change triggers new SCD2 version |
| `quality_score` | DOUBLE | **Tracked** — change triggers new SCD2 version |
| `effective_date` | STRING | ISO date this version became active |
| `expiry_date` | STRING | ISO date this version ended; "9999-12-31" for current |
| `is_current` | BOOLEAN | True for the active version only |

**Constraint enforced by DQ gate:** Exactly one `is_current=True` per `supplier_id` at all times.

**Source:** Bronze `supplier_data` parquet — not Silver.

---

## Gold Layer — Facts

### FACT_PRODUCTION
One row per (date, location, menu item, meal period) production batch.

| Column | Type | Description |
|--------|------|-------------|
| `production_sk` | STRING | UUID surrogate key |
| `date_sk` | BIGINT | FK → DIM_DATE.date_sk |
| `location_sk` | STRING | FK → DIM_LOCATION.location_sk |
| `menu_sk` | STRING | FK → DIM_MENU.menu_sk |
| `meal_period_sk` | STRING | FK → DIM_MEAL_PERIOD.meal_period_sk |
| `quantity_prepared` | DOUBLE | Units prepared |
| `cost_per_unit` | DOUBLE | INR per unit |
| `batch_id` | STRING | Bronze run UUID |
| `year` | INT | Partition column |
| `month` | INT | Partition column |

---

### FACT_WASTE
One row per waste event (same grain as FACT_PRODUCTION — one per batch).

| Column | Type | Description |
|--------|------|-------------|
| `waste_sk` | STRING | UUID surrogate key |
| `date_sk` | BIGINT | FK → DIM_DATE.date_sk |
| `location_sk` | STRING | FK → DIM_LOCATION.location_sk |
| `menu_sk` | STRING | FK → DIM_MENU.menu_sk |
| `meal_period_sk` | STRING | FK → DIM_MEAL_PERIOD.meal_period_sk |
| `waste_reason_sk` | STRING | FK → DIM_WASTE_REASON.waste_reason_sk |
| `supplier_sk` | STRING | FK → DIM_SUPPLIER.supplier_sk (current record) |
| `quantity_wasted` | DOUBLE | Units wasted |
| `waste_percentage` | DOUBLE | Percentage of prepared quantity wasted |
| `waste_cost` | DOUBLE | INR cost of waste |
| `year` | INT | Partition column |
| `month` | INT | Partition column |

---

### FACT_CONSUMPTION
Derived fact — production minus waste.

| Column | Type | Description |
|--------|------|-------------|
| `consumption_sk` | STRING | UUID surrogate key |
| `date_sk` | BIGINT | FK → DIM_DATE.date_sk |
| `location_sk` | STRING | FK → DIM_LOCATION.location_sk |
| `menu_sk` | STRING | FK → DIM_MENU.menu_sk |
| `quantity_prepared` | DOUBLE | From production |
| `quantity_wasted` | DOUBLE | From waste |
| `quantity_consumed` | DOUBLE | quantity_prepared − quantity_wasted (always ≥ 0) |
| `demand_gap` | DOUBLE | planned_quantity − actual_served_estimate |
| `year` | INT | Partition column |
| `month` | INT | Partition column |

---

### FACT_WASTE_SUMMARY
Pre-aggregated by location + category + year + month. Used by all dashboard queries.

| Column | Type | Description |
|--------|------|-------------|
| `location_sk` | STRING | FK → DIM_LOCATION.location_sk |
| `category_sk` | STRING | FK → DIM_CATEGORY.category_sk |
| `category` | STRING | Category name string (denormalised for query convenience) |
| `year` | INT | |
| `month` | INT | |
| `total_waste_quantity` | DOUBLE | Sum of quantity_wasted for this group |
| `total_waste_cost` | DOUBLE | Sum of waste_cost in INR |
| `avg_waste_percentage` | DOUBLE | Mean waste_percentage across events |
| `waste_event_count` | BIGINT | Number of waste events aggregated |

**Why this table exists:** Dashboard queries hit this table rather than `FACT_WASTE` to minimise Athena data scanned per query (pre-aggregated → orders of magnitude smaller).
