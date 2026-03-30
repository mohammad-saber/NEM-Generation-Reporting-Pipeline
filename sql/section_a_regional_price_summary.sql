-- =============================================================================
-- Section A: Regional Price Summary
-- One row per region for the desired month
-- 
-- Assumption: NEM price cap is $17,500/MWh and floor is -$1,000/MWh as per
-- AEMO market price limits effective August 2024. We count intervals AT OR ABOVE
-- the cap and intervals strictly below zero (<0) as negative price.
-- =============================================================================

SELECT
    region_id,
    ROUND(AVG(rrp), 2)                                          AS avg_rrp,
    ROUND(MIN(rrp), 2)                                          AS min_rrp,
    ROUND(MAX(rrp), 2)                                          AS max_rrp,
    COUNT_IF(rrp >= 17500)                                      AS intervals_count_at_price_cap,
    COUNT_IF(rrp < 0)                                           AS intervals_count_negative_price
FROM gold.regional_price_summary
WHERE report_month = '2024-08'
GROUP BY region_id
ORDER BY region_id;



