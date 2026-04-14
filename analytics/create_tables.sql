-- ============================================================
-- Athena DDL — Food Waste Optimization 360
-- Creates food_waste_db and registers all Gold tables as
-- external Parquet tables pointing to S3.
-- Replace ${S3_BUCKET} with your actual bucket name before running.
-- ============================================================

CREATE DATABASE IF NOT EXISTS food_waste_db;

-- ============================================================
-- DIMENSIONS
-- ============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS food_waste_db.dim_date (
    date_sk     BIGINT,
    full_date   STRING,
    day_of_week INT,
    day_name    STRING,
    month       INT,
    month_name  STRING,
    quarter     INT,
    year        INT,
    is_weekend  BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/gold/dims/dim_date/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS food_waste_db.dim_location (
    location_sk    STRING,
    location_id    STRING,
    location_name  STRING,
    city           STRING,
    region         STRING,
    location_type  STRING,
    capacity       STRING,
    storage_rating STRING
)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/gold/dims/dim_location/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS food_waste_db.dim_menu (
    menu_sk          STRING,
    menu_item_id     STRING,
    menu_item_name   STRING,
    category         STRING,
    sub_category     STRING,
    veg_flag         STRING,
    shelf_life_hours INT,
    prep_complexity  STRING
)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/gold/dims/dim_menu/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS food_waste_db.dim_category (
    category_sk   STRING,
    category_name STRING
)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/gold/dims/dim_category/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS food_waste_db.dim_meal_period (
    meal_period_sk   STRING,
    meal_period_name STRING
)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/gold/dims/dim_meal_period/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS food_waste_db.dim_waste_reason (
    waste_reason_sk   STRING,
    waste_reason_name STRING
)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/gold/dims/dim_waste_reason/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS food_waste_db.dim_supplier (
    supplier_sk    STRING,
    supplier_id    STRING,
    supplier_name  STRING,
    menu_item_id   STRING,
    supplier_city  STRING,
    lead_time_days INT,
    quality_score  DOUBLE,
    effective_date STRING,
    expiry_date    STRING,
    is_current     BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/gold/dims/dim_supplier/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ============================================================
-- FACTS
-- ============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS food_waste_db.fact_production (
    production_sk   STRING,
    date_sk         BIGINT,
    location_sk     STRING,
    menu_sk         STRING,
    meal_period_sk  STRING,
    quantity_prepared DOUBLE,
    cost_per_unit   DOUBLE,
    batch_id        STRING
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/gold/facts/fact_production/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');
MSCK REPAIR TABLE food_waste_db.fact_production;

-- ------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS food_waste_db.fact_waste (
    waste_sk          STRING,
    date_sk           BIGINT,
    location_sk       STRING,
    menu_sk           STRING,
    meal_period_sk    STRING,
    waste_reason_sk   STRING,
    supplier_sk       STRING,
    quantity_wasted   DOUBLE,
    waste_percentage  DOUBLE,
    waste_cost        DOUBLE
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/gold/facts/fact_waste/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');
MSCK REPAIR TABLE food_waste_db.fact_waste;

-- ------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS food_waste_db.fact_consumption (
    consumption_sk     STRING,
    date_sk            BIGINT,
    location_sk        STRING,
    menu_sk            STRING,
    quantity_prepared  DOUBLE,
    quantity_wasted    DOUBLE,
    quantity_consumed  DOUBLE,
    demand_gap         DOUBLE
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/gold/facts/fact_consumption/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');
MSCK REPAIR TABLE food_waste_db.fact_consumption;

-- ------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS food_waste_db.fact_waste_summary (
    location_sk          STRING,
    category_sk          STRING,
    category             STRING,
    total_waste_quantity DOUBLE,
    total_waste_cost     DOUBLE,
    avg_waste_percentage DOUBLE,
    waste_event_count    BIGINT
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/gold/facts/fact_waste_summary/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');
MSCK REPAIR TABLE food_waste_db.fact_waste_summary;
