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
            THEN 'You are cooking way more than guests are eating. Cut prep volume by 25% for the next 2 weeks and track whether plates run out — if they don''t, lock in the lower batch size permanently.'

        WHEN fws.category IN ('SALADS', 'DAIRY', 'FRESH JUICE', 'FRUITS',
                              'Salads', 'Dairy', 'Fresh Juice', 'Fruits')
             AND fws.avg_waste_percentage > 20
            THEN 'This is a fridge or timing problem, not a demand problem. Check how long items sit before service and when the supplier delivers — if it''s arriving less than 12 hours before a meal, push for an earlier drop.'

        WHEN fws.avg_waste_percentage > 30
             AND (fws.total_waste_quantity / NULLIF(fws.waste_event_count, 0)) <= 5
            THEN 'Portions are too generous for what guests actually want. Try a smaller default serving and let guests ask for more — most won''t, and waste will drop fast.'

        ELSE 'Guests are consistently skipping this item. Either pull it from the menu on low-traffic days or swap it out for something in the same category that actually moves.'
    END AS recommendation

FROM food_waste_db.fact_waste_summary fws
JOIN food_waste_db.dim_location dl
    ON fws.location_sk = dl.location_sk
WHERE fws.year = 2025
  AND fws.month BETWEEN 1 AND 12
ORDER BY fws.avg_waste_percentage DESC;
