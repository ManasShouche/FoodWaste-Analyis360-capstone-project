-- ============================================================
-- Athena Analytical Queries — Food Waste Optimization 360
-- All queries filter on partition columns (year, month) first.
-- No SELECT * used anywhere.
-- Required techniques: CTE, Window, LAG, RANK/DENSE_RANK
-- ============================================================

-- ============================================================
-- Query 1: Monthly waste trend by location with MoM % change (LAG)
-- Answers: Which locations are improving or worsening each month?
-- Technique: LAG window function
-- ============================================================
SELECT
    dl.location_name,
    fws.year,
    fws.month,
    fws.total_waste_cost,
    LAG(fws.total_waste_cost) OVER (
        PARTITION BY fws.location_sk
        ORDER BY fws.year, fws.month
    ) AS prev_month_cost,
    ROUND(
        (fws.total_waste_cost - LAG(fws.total_waste_cost) OVER (
            PARTITION BY fws.location_sk
            ORDER BY fws.year, fws.month
        )) * 100.0
        / NULLIF(LAG(fws.total_waste_cost) OVER (
            PARTITION BY fws.location_sk
            ORDER BY fws.year, fws.month
        ), 0),
        2
    ) AS mom_pct_change
FROM food_waste_db.fact_waste_summary fws
JOIN food_waste_db.dim_location dl
    ON fws.location_sk = dl.location_sk
WHERE fws.year = 2025
  AND fws.month BETWEEN 1 AND 12
ORDER BY dl.location_name, fws.year, fws.month;


-- ============================================================
-- Query 2: Top 10 menu items by total waste cost (DENSE_RANK)
-- Answers: Which menu items are costing the most in waste?
-- Technique: DENSE_RANK
-- ============================================================
SELECT
    ranked.menu_item_name,
    ranked.category,
    ranked.total_waste_cost,
    ranked.waste_rank
FROM (
    SELECT
        dm.menu_item_name,
        dm.category,
        SUM(fw.waste_cost)   AS total_waste_cost,
        DENSE_RANK() OVER (ORDER BY SUM(fw.waste_cost) DESC) AS waste_rank
    FROM food_waste_db.fact_waste fw
    JOIN food_waste_db.dim_menu dm
        ON fw.menu_sk = dm.menu_sk
    WHERE fw.year = 2025
      AND fw.month BETWEEN 1 AND 12
    GROUP BY dm.menu_item_name, dm.category
) ranked
WHERE ranked.waste_rank <= 10
ORDER BY ranked.waste_rank;


-- ============================================================
-- Query 3: Demand vs supply gap by category (CTE)
-- Answers: Which categories consistently over-prepare?
-- Technique: CTE
-- ============================================================
WITH category_supply AS (
    SELECT
        dm.category,
        SUM(fp.quantity_prepared)    AS total_prepared,
        SUM(fc.quantity_consumed)    AS total_consumed,
        SUM(fc.demand_gap)           AS total_demand_gap,
        COUNT(fp.production_sk)      AS production_events
    FROM food_waste_db.fact_production fp
    JOIN food_waste_db.dim_menu dm
        ON fp.menu_sk = dm.menu_sk
    JOIN food_waste_db.fact_consumption fc
        ON fp.date_sk = fc.date_sk
       AND fp.location_sk = fc.location_sk
       AND fp.menu_sk = fc.menu_sk
    WHERE fp.year = 2025
      AND fp.month BETWEEN 1 AND 12
    GROUP BY dm.category
),
category_waste AS (
    SELECT
        fws.category,
        SUM(fws.total_waste_cost)        AS total_waste_cost,
        AVG(fws.avg_waste_percentage)    AS avg_waste_pct
    FROM food_waste_db.fact_waste_summary fws
    WHERE fws.year = 2025
      AND fws.month BETWEEN 1 AND 12
    GROUP BY fws.category
)
SELECT
    cs.category,
    cs.total_prepared,
    cs.total_consumed,
    cs.total_demand_gap,
    cw.total_waste_cost,
    ROUND(cw.avg_waste_pct, 2)            AS avg_waste_pct,
    ROUND(cs.total_demand_gap * 100.0
          / NULLIF(cs.total_prepared, 0), 2) AS gap_pct
FROM category_supply cs
JOIN category_waste cw
    ON cs.category = cw.category
ORDER BY cs.total_demand_gap DESC;


-- ============================================================
-- Query 4: Supplier quality score vs average waste percentage
-- Answers: Do lower-quality suppliers correlate with higher waste?
-- Technique: Window function (AVG OVER PARTITION BY supplier)
-- ============================================================
SELECT
    ds.supplier_name,
    ds.supplier_city,
    ds.quality_score,
    dm.menu_item_name,
    dm.category,
    fw.waste_percentage,
    AVG(fw.waste_percentage) OVER (
        PARTITION BY fw.supplier_sk
    ) AS supplier_avg_waste_pct,
    AVG(fw.waste_percentage) OVER () AS overall_avg_waste_pct
FROM food_waste_db.fact_waste fw
JOIN food_waste_db.dim_supplier ds
    ON fw.supplier_sk = ds.supplier_sk
JOIN food_waste_db.dim_menu dm
    ON fw.menu_sk = dm.menu_sk
WHERE fw.year = 2025
  AND fw.month BETWEEN 1 AND 12
  AND ds.is_current = true
ORDER BY ds.quality_score ASC, fw.waste_percentage DESC;


-- ============================================================
-- Query 5: Waste reason breakdown by meal period (window % of total)
-- Answers: Which meal periods have the worst waste patterns?
-- Technique: Window function (SUM OVER PARTITION BY)
-- ============================================================
SELECT
    dmp.meal_period_name,
    dwr.waste_reason_name,
    COUNT(fw.waste_sk)         AS event_count,
    SUM(fw.waste_cost)         AS total_cost,
    ROUND(
        SUM(fw.waste_cost) * 100.0
        / NULLIF(SUM(SUM(fw.waste_cost)) OVER (PARTITION BY fw.year, fw.month), 0),
        2
    ) AS pct_of_monthly_total
FROM food_waste_db.fact_waste fw
JOIN food_waste_db.dim_waste_reason dwr
    ON fw.waste_reason_sk = dwr.waste_reason_sk
JOIN food_waste_db.dim_meal_period dmp
    ON fw.year = 2025   -- join via fact; meal_period_sk not in fact_waste, use silver for this
-- Note: if meal_period_sk is added to fact_waste in a future iteration,
-- join directly: fw.meal_period_sk = dmp.meal_period_sk
WHERE fw.year = 2025
  AND fw.month BETWEEN 1 AND 12
GROUP BY dmp.meal_period_name, dwr.waste_reason_name, fw.year, fw.month
ORDER BY dmp.meal_period_name, total_cost DESC;
