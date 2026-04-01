-- =============================================================================
-- Section B: Generation Mix by Fuel Type (per region)
-- One row per region per fuel category for the desired month
--
-- Wind, Solar_Utility, and Hydro are rolled up into a single "Renewables"
-- category. All other fuel types are reported individually.

-- total_dispatch_mwh is already pre-aggregated in the Gold table as explained below:
-- SUM of dispatch_mwh from "silver.unit_dispatch" across all intervals in the month,
-- where dispatch_mwh = dispatch_mw * (5.0 / 60.0)
-- =============================================================================

WITH dispatch_by_category AS (
    SELECT
        region_id,
        CASE
            WHEN fuel_type IN ('WIND', 'SOLAR_UTILITY', 'HYDRO') THEN 'Renewables'
            ELSE fuel_type
        END                             AS fuel_category,
        SUM(total_dispatch_mwh)         AS category_mwh
    FROM gold.generation_mix
    WHERE report_month = '2024-08'
    GROUP BY
        region_id,
        fuel_category
),

regional_totals AS (
    SELECT
        region_id,
        SUM(category_mwh)               AS total_region_mwh
    FROM dispatch_by_category
    GROUP BY region_id
)

SELECT
    f.region_id,
    f.fuel_category,
    ROUND(f.category_mwh, 2)                                AS total_dispatch_mwh,
    ROUND(f.category_mwh / r.total_region_mwh * 100, 2)     AS percentage_of_total_dispatch
FROM dispatch_by_category f
JOIN regional_totals r
    ON f.region_id = r.region_id
ORDER BY f.region_id, percentage_of_total_dispatch DESC;