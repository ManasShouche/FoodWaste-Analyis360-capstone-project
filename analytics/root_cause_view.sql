-- ============================================================
-- Root Cause Classification View — Food Waste Optimization 360
--
-- Implements the four-bucket rule-based classifier from the
-- project overview (Section 10) using columns available in
-- FACT_WASTE_SUMMARY joined to DIM_LOCATION and DIM_CATEGORY.
--
-- FACT_WASTE_SUMMARY columns used:
--   avg_waste_percentage  — pre-aggregated waste %
--   total_waste_quantity  — total units wasted
--   waste_event_count     — number of waste events in the group
--   category              — category name (string, already in summary)
--
-- demand_gap and waste_reason are NOT in FACT_WASTE_SUMMARY (aggregated table).
-- demand_gap proxy: high waste_quantity relative to event count signals overproduction.
-- waste_reason = 'spoilage' proxy: perishable categories (Salads, Dairy, Fresh Juice, Fruits).
-- ============================================================

CREATE OR REPLACE VIEW food_waste_db.waste_root_cause AS
SELECT
    dl.location_name,
    dl.city,
    dl.region,
    fws.category                                   AS category_name,
    fws.year,
    fws.month,
    ROUND(fws.avg_waste_percentage, 2)             AS waste_percentage,
    fws.total_waste_cost,
    fws.total_waste_quantity,
    fws.waste_event_count,

    CASE
        -- Overproduction: high waste % AND high volume per event (demand_gap > 0 proxy)
        WHEN fws.avg_waste_percentage > 40
             AND (fws.total_waste_quantity / NULLIF(fws.waste_event_count, 0)) > 5
            THEN 'Overproduction'

        -- Storage / Spoilage: perishable categories with elevated waste (spoilage proxy)
        WHEN fws.category IN ('SALADS', 'DAIRY', 'FRESH JUICE', 'FRUITS',
                              'Salads', 'Dairy', 'Fresh Juice', 'Fruits')
             AND fws.avg_waste_percentage > 20
            THEN 'Storage / Spoilage'

        -- Portion Mismatch: high waste % but low volume per event (demand_gap <= 0 proxy)
        WHEN fws.avg_waste_percentage > 30
             AND (fws.total_waste_quantity / NULLIF(fws.waste_event_count, 0)) <= 5
            THEN 'Portion Mismatch'

        -- Default: insufficient demand for this item
        ELSE 'Low Demand'
    END AS root_cause,

    CASE
        WHEN fws.avg_waste_percentage > 40
             AND (fws.total_waste_quantity / NULLIF(fws.waste_event_count, 0)) > 5
            THEN 'Reduce batch size by 20-30% for this category at this location.'

        WHEN fws.category IN ('SALADS', 'DAIRY', 'FRESH JUICE', 'FRUITS',
                              'Salads', 'Dairy', 'Fresh Juice', 'Fruits')
             AND fws.avg_waste_percentage > 20
            THEN 'Review storage conditions and supplier lead time.'

        WHEN fws.avg_waste_percentage > 30
             AND (fws.total_waste_quantity / NULLIF(fws.waste_event_count, 0)) <= 5
            THEN 'Adjust portion sizes for this menu item.'

        ELSE 'Review menu item demand — consider removal or rotation.'
    END AS recommendation

FROM food_waste_db.fact_waste_summary fws
JOIN food_waste_db.dim_location dl
    ON fws.location_sk = dl.location_sk
WHERE fws.year = 2025
  AND fws.month BETWEEN 1 AND 12
ORDER BY fws.avg_waste_percentage DESC;
