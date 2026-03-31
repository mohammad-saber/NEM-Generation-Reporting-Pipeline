-- =============================================================================
-- Section A: Regional Price Summary
-- One row per region for the desired month
-- 
-- Assumption: NEM price cap is $17,500/MWh and floor price is -$1,000/MWh as per
-- AEMO market price limits effective July 2024. 
-- We count intervals at or above the price cap and intervals at or below the floor price.
-- Floor price flag was used only (no general negative price count was added to the query).
-- =============================================================================

SELECT
    region_id,
    ROUND(AVG(rrp), 2)                  AS avg_rrp,
    ROUND(MIN(rrp), 2)                  AS min_rrp,
    ROUND(MAX(rrp), 2)                  AS max_rrp,
    COUNT_IF(rrp >= 17500)              AS intervals_count_above_price_cap,
    COUNT_IF(rrp <= -1000)              AS intervals_count_below_floor_price
FROM gold.regional_price_summary
WHERE report_month = '2024-08'
GROUP BY region_id
ORDER BY region_id;



