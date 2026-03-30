-- =============================================================================
-- Section B: Generation Mix by Fuel Type (per region)
-- One row per region per fuel category for the desired month
--
-- Wind, Solar_Utility, and Hydro are rolled up into a single "Renewables"
-- category. All other fuel types are reported individually.
-- Energy (MWh) is calculated as dispatch_mw * (5/60) since each interval
-- represents 5 minutes of output.
-- =============================================================================

WITH dispatch_with_category AS (
    SELECT
        region_id,
        CASE
            WHEN fuel_type IN ('WIND', 'SOLAR_UTILITY', 'HYDRO') THEN 'Renewables'
            ELSE fuel_type
        END                                                         AS fuel_category,
        -- Convert 5-minute MW dispatch to MWh energy
        dispatch_mw * (5.0 / 60.0)                                 AS dispatch_mwh
    FROM gold.generation_mix
    WHERE report_month = '2024-08'
),

-- Total dispatch for every region
regional_totals AS (
    SELECT
        region_id,
        SUM(dispatch_mwh)                                           AS total_region_mwh
    FROM dispatch_with_category
    GROUP BY region_id
),

-- Total dispatch for every region and every fuel type
category_totals AS (
    SELECT
        region_id,
        fuel_category,
        SUM(dispatch_mwh)                                         AS category_mwh
    FROM dispatch_with_category
    GROUP BY region_id, fuel_category
)


SELECT
    c.region_id,
    c.fuel_category,
    ROUND(c.category_mwh, 2)                                        AS total_dispatch_mwh,
    ROUND(c.category_mwh / r.total_region_mwh * 100, 2)             AS percentage_of_total_dispatch
FROM category_totals c
JOIN regional_totals r
    ON c.region_id = r.region_id
ORDER BY c.region_id, pct_of_regional_dispatch DESC;





