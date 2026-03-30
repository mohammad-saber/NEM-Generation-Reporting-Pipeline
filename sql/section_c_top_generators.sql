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

WITH unit_energy AS (
    SELECT
        duid,
        -- Convert 5-minute intervals to MWh: MW × (5 min / 60 min)
        SUM(dispatch_mw * (5.0 / 60.0))                             AS total_dispatch_mwh
    FROM gold.top_generators
    WHERE report_month = '2024-08'
    GROUP BY duid
)

SELECT
    ROW_NUMBER() OVER (ORDER BY ue.total_dispatch_mwh DESC)         AS rank,
    g.station_name,
    g.owner,
    g.fuel_type,
    g.region_id,
    ROUND(ue.total_dispatch_mwh, 2)                                  AS total_dispatch_mwh,
    -- Capacity factor: actual MWh / (nameplate MW × 744 hours in August 2024) × 100
    ROUND(
        ue.total_dispatch_mwh / (g.registered_capacity_mw * 744) * 100,
        1
    )                                                                AS capacity_factor_pct
FROM unit_energy ue
JOIN gold.top_generators g
    ON ue.duid = g.duid
ORDER BY ue.total_dispatch_mwh DESC
LIMIT 10;

