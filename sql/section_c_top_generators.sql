-- =============================================================================
-- Section C: Top 10 Generators by Dispatch Volume (optional)
--
-- Capacity factor = actual energy dispatched (MWh) divided by the maximum
-- possible energy the unit could have produced at its registered nameplate
-- capacity over the same period (registered_capacity_mw × total hours in month).
--
-- Assumption: August 2024 has 31 days = 744 hours. Capacity factor is expressed
-- as a percentage. 
-- =============================================================================

SELECT
    ROW_NUMBER() OVER (ORDER BY total_dispatch_mwh DESC)    AS rank,
    station_name,
    owner,
    fuel_type,
    region_id,
    ROUND(total_dispatch_mwh, 2)                            AS total_dispatch_mwh,
    -- Capacity factor: actual MWh / (nameplate MW × 744 hours in August 2024) × 100
    ROUND(
        total_dispatch_mwh / (registered_capacity_mw * 744) * 100,
        1
    )                                                       AS capacity_factor_pct
FROM gold.top_generators
WHERE report_month = '2024-08'
ORDER BY total_dispatch_mwh DESC
LIMIT 10;
