
-- =============================================================================
-- NEM Generation Reporting Pipeline — DDL / Schema Definitions
-- All tables are Delta tables on ADLS2, managed in Unity Catalog.
-- Partitioning rationale is noted inline.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- BRONZE LAYER — raw ingested data
-- ---------------------------------------------------------------------------

-- Grain: one row per region per 5-minute interval
-- Partitioned by interval_date for efficient time-range queries and
-- incremental daily loads without full-table scans.
CREATE TABLE IF NOT EXISTS bronze.dispatch_intervals (
    interval_datetime   TIMESTAMP,
    region_id           STRING,
    rrp                 DOUBLE,
    total_demand_mw     DOUBLE,
    scheduled_generation_mw DOUBLE,
    -- ingestion metadata
    source_file_name    STRING,
    source_file_path    STRING,
    ingestion_timestamp TIMESTAMP       
)
USING DELTA
PARTITIONED BY (interval_date DATE GENERATED ALWAYS AS CAST(interval_datetime AS DATE))
COMMENT 'Bronze: raw AEMO 5-minute regional pricing and demand intervals';


-- Grain: one row per DUID per 5-minute interval
-- Partitioned by interval_date for daily incremental loads from email files.
-- Each morning's file covers the previous day; partitioning prevents re-scanning
-- historical data on each load.
CREATE TABLE IF NOT EXISTS bronze.unit_dispatch (
    interval_datetime   TIMESTAMP,
    duid                STRING,
    region_id           STRING,
    dispatch_mw         DOUBLE,
    availability_mw     DOUBLE,
    fuel_type           STRING,
    -- ingestion metadata
    source_file_name    STRING,
    source_file_path    STRING,
    ingestion_timestamp TIMESTAMP  
)
USING DELTA
PARTITIONED BY (interval_date DATE GENERATED ALWAYS AS CAST(interval_datetime AS DATE))
COMMENT 'Bronze: raw unit dispatch from daily email file (external data provider)';


-- Grain: one row per DUID (slowly changing reference data)
-- Not partitioned as it is a small table.
CREATE TABLE IF NOT EXISTS bronze.reference_generators (
    duid                    STRING          NOT NULL,
    station_name            STRING,
    fuel_type               STRING,
    region_id               STRING,
    registered_capacity_mw  DOUBLE,
    owner                   STRING,
    -- ingestion metadata
    source_file_name        STRING,
    source_file_path        STRING,
    ingestion_timestamp     TIMESTAMP  
)
USING DELTA
COMMENT 'Bronze: generator reference data (SCD, internal system)';


-- ---------------------------------------------------------------------------
-- SILVER LAYER 
-- These tables represent cleaned, conformed, and enriched data, 
-- stored in Delta format for ACID properties and schema enforcement. 
-- Data types are explicitly defined, and basic data quality checks are applied.
-- ---------------------------------------------------------------------------

-- Grain: one row per region per 5-minute interval (deduped)
-- Partitioned by interval_date for incremental silver processing.
CREATE TABLE IF NOT EXISTS silver.dispatch_intervals (
    interval_datetime       TIMESTAMP,
    interval_date           DATE,
    region_id               STRING,
    rrp                     DOUBLE,
    total_demand_mw         DOUBLE,
    scheduled_generation_mw DOUBLE,
    is_price_cap            BOOLEAN,   -- 1 if it is >= price cap         
    is_negative_price       BOOLEAN,   -- 1 if it negative       
    -- metadata
    source_file_name        STRING,
    source_file_path        STRING,
    ingestion_timestamp     TIMESTAMP, 
    transformation_timestamp  TIMESTAMP 
)
USING DELTA
PARTITIONED BY (interval_date)
COMMENT 'Silver: cleaned dispatch intervals with derived flags';


-- Grain: one row per DUID per 5-minute interval (deduped, enriched with station metadata)
-- Partitioned by interval_date.
CREATE TABLE IF NOT EXISTS silver.unit_dispatch (
    interval_datetime       TIMESTAMP,
    interval_date           DATE,
    duid                    STRING,
    region_id               STRING,
    dispatch_mw             DOUBLE,
    dispatch_mwh            DOUBLE,
    availability_mw         DOUBLE,
    fuel_type               STRING,
    -- enriched from reference_generators
    station_name            STRING,
    registered_capacity_mw  DOUBLE,
    owner                   STRING,
    -- metadata
    source_file_name        STRING,
    source_file_path        STRING,
    ingestion_timestamp     TIMESTAMP, 
    transformation_timestamp  TIMESTAMP 
)
USING DELTA
PARTITIONED BY (interval_date)
COMMENT 'Silver: unit dispatch enriched with generator reference attributes';


-- Grain: one row per DUID (current snapshot)
-- Not partitioned as it is a small table.
CREATE TABLE IF NOT EXISTS silver.reference_generators (
    duid                    STRING          NOT NULL,
    station_name            STRING,
    fuel_type               STRING,
    region_id               STRING,
    registered_capacity_mw  DOUBLE,
    owner                   STRING,
    effective_from          TIMESTAMP,
    effective_until         TIMESTAMP,
    is_current              BOOLEAN,
    created_timestamp       TIMESTAMP,
    updated_timestamp       TIMESTAMP,
    -- metadata
    source_file_name        STRING,
    source_file_path        STRING,
    ingestion_timestamp     TIMESTAMP, 
    transformation_timestamp  TIMESTAMP 
    CONSTRAINT pk_reference_generators PRIMARY KEY (duid)
)
USING DELTA
COMMENT 'Silver: current-state generator reference, deduplicated';


-- ---------------------------------------------------------------------------
-- GOLD LAYER — pre-aggregated, report-ready
-- These tables are optimized for reporting and analytics, pre-aggregating data 
-- to meet the specific requirements of the reporting dashboard.
-- ---------------------------------------------------------------------------

-- Grain: one row per region per report_month
-- Partitioned by report_month for BI tool filter push-down.
CREATE TABLE IF NOT EXISTS gold.regional_price_summary (
    report_month                STRING,   -- e.g. '2024-08'  
    region_id                   STRING,
    avg_rrp                     DOUBLE,
    min_rrp                     DOUBLE,
    max_rrp                     DOUBLE,
    intervals_count_at_price_cap      BIGINT,
    intervals_count_negative_price    BIGINT,
    transformation_timestamp    TIMESTAMP
)
USING DELTA
PARTITIONED BY (report_month)
COMMENT 'Gold: Section A — regional RRP statistics per month';


-- Grain: one row per region per fuel_category per report_month
-- Partitioned by: report_month
CREATE TABLE IF NOT EXISTS gold.generation_mix (
    report_month            STRING,
    region_id               STRING,
    fuel_type               STRING,
    dispatch_mw             DOUBLE,
    transformation_timestamp      TIMESTAMP   
)
USING DELTA
PARTITIONED BY (report_month)
COMMENT 'Gold: Section B — dispatch energy by region and fuel category (Renewables rolled up)';


-- Grain: one row per DUID per report_month (all generators)
-- Partitioned by: report_month
CREATE TABLE IF NOT EXISTS gold.top_generators (
    report_month                STRING,
    duid                        STRING      NOT NULL,
    station_name                STRING,
    owner                       STRING,
    fuel_type                   STRING,
    region_id                   STRING,
    registered_capacity_mw      DOUBLE,
    dispatch_mw                 DOUBLE,
    transformation_timestamp    TIMESTAMP
)
USING DELTA
PARTITIONED BY (report_month)
COMMENT 'Gold: Section C — per-unit dispatch totals, join with generators for capacity factor';



