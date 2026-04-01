-- =============================================================================
-- Section A: Regional Price Summary
-- One row per region for the desired month
-- 
-- Assumption: NEM price cap is $17,500/MWh and floor price is -$1,000/MWh as per
-- AEMO market price limits effective July 2024. 

-- Counts are pre-aggregated in the Gold table during transformation from silver to gold.
-- We report intervals at or above the price cap and intervals at or below the floor price.
-- Floor price flag is used only (no general negative price count was added to the query).
-- =============================================================================

SELECT
    region_id,
    avg_rrp,
    min_rrp,
    max_rrp,
    intervals_count_above_price_cap,
    intervals_count_below_floor_price
FROM gold.regional_price_summary
WHERE report_month = '2024-08'
ORDER BY region_id;


