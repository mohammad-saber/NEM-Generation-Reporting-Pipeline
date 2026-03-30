# NEM Generation Reporting Pipeline Assessment

This repository contains my proposed solution for the NEM generation reporting pipeline assessment.

## 1) Proposed Azure pipeline architecture

### Design summary
I use a medallion-style layout on Azure Data Lake Storage Gen2 (bronze/silver/gold) because it keeps raw source fidelity, makes transformations auditable, and gives the BI layer clean semantic tables rather than forcing dashboard logic onto raw files. The daily `raw_unit_dispatch.csv` email feed is handled by **Azure Logic Apps**, which watches a dedicated mailbox, validates the sender/attachment pattern, writes the attachment and email metadata to a landing container in ADLS, and triggers orchestration. **Azure Data Factory (ADF)** orchestrates all ingestions and dependency ordering, while **Azure Databricks** performs parsing, deduplication, enrichment, unit conversion from MW to MWh, and publication of curated Delta tables. I keep the 5‑minute fact tables at interval grain so the model stays reusable, and the three report queries remain very short and BI-friendly.

### Reliable handling of the emailed unit-dispatch file
The email-delivered source is the riskiest part of the pipeline, so I would make it explicitly idempotent and observable. Logic Apps writes each attachment with a deterministic file path (for example by provider date), captures sender / subject / received timestamp / checksum, and stores a control record so the same file is not processed twice. ADF then validates the expected delivery date, row count > 0, schema, and attachment naming; failed files are quarantined and notified via Teams/email. The original attachment is retained unchanged in bronze so the pipeline is replayable and auditable.

### End-to-end flow
See the diagram in `diagram/azure_nem_pipeline.svg`.

## 2) Data model

I keep three layers:

- **Bronze**: immutable raw landing tables/files
- **Silver**: cleaned and standardized tables, including an SCD2 generator dimension
- **Gold**: analytics-ready interval facts used directly by BI

The model is intentionally narrow:
- one regional 5-minute fact table for prices/demand
- one generator-unit 5-minute fact table for dispatch
- one generator dimension for descriptive attributes and slowly changing ownership/capacity

This avoids unnecessary duplication while keeping all three report sections straightforward:
- Section A reads from `gold.fact_region_interval`
- Section B reads from `gold.fact_generator_interval`
- Section C reads from `gold.fact_generator_interval`

### Grain and partitioning choices
I partition the large interval tables by `market_date` because the unit-dispatch feed arrives daily and the natural incremental load unit is one market day. This gives efficient append/reprocess behavior without rewriting an entire month. I do **not** partition the small generator dimension because it is tiny and better accessed through point lookups / joins on `duid`.

### Produced tables

#### Bronze
1. **bronze.raw_dispatch_intervals**
   - Grain: one raw region row per 5-minute interval from AEMO
   - Partition: `market_date`

2. **bronze.raw_unit_dispatch**
   - Grain: one raw DUID row per 5-minute interval from the email attachment
   - Partition: `market_date`

3. **bronze.raw_reference_generators**
   - Grain: one raw generator reference row per file snapshot
   - Partition: `snapshot_date`

#### Silver
4. **silver.dispatch_intervals_clean**
   - Grain: one deduplicated region row per 5-minute interval
   - Notes: standardised timestamps, data types, and data-quality flags
   - Partition: `market_date`

5. **silver.dim_generator_scd**
   - Grain: one versioned generator record per DUID per effective period
   - Notes: SCD2 structure for owner/capacity/retirement changes
   - Partition: none

#### Gold
6. **gold.fact_region_interval**
   - Grain: one region row per 5-minute interval
   - Notes: includes report-friendly flags such as `is_price_cap_interval` and `is_floor_price_interval`
   - Partition: `market_date`

7. **gold.fact_generator_interval**
   - Grain: one DUID row per 5-minute interval, enriched with generator attributes
   - Notes: includes `dispatch_mwh` and `availability_mwh_equiv` derived from 5-minute MW values
   - Partition: `market_date`

## 3) DDL
The DDL for the proposed tables is in `sql/00_data_model.sql`.

## 4) Report queries

### Section A — Regional price summary
Query file: `sql/01_section_a_regional_price_summary.sql`

Assumptions / choices:
- The report is monthly, so the query filters by a month window using `market_date`.
- I count negative-price intervals as `rrp < 0`, while also keeping a separate floor-price flag (`rrp <= -1000`) in the gold table for auditability / future reporting.

### Section B — Generation mix by fuel type
Query file: `sql/02_section_b_generation_mix.sql`

Assumptions / choices:
- Dispatch is converted from MW to MWh at 5-minute interval grain before aggregation (`dispatch_mwh = dispatch_mw * 5/60`).
- `WIND`, `SOLAR_UTILITY`, and `HYDRO` are rolled into `RENEWABLES` exactly as requested.

### Section C — Top 10 generators by dispatch volume
Query file: `sql/03_section_c_top10_generators.sql`

Assumptions / choices:
- Capacity factor is calculated over the selected reporting period as total dispatched MWh divided by (`registered_capacity_mw × total hours in period`).
- In the provided sample data, station is effectively one-to-one with DUID; in production I would keep DUID as the technical key and expose station attributes in the BI layer.

## 5) Validation notes against the supplied sample files
I validated the SQL logic against the supplied August 2024 CSV files. One duplicate row exists in `raw_dispatch_intervals.csv`, so the silver step explicitly deduplicates on `(interval_datetime, region_id)` before publishing the gold fact table. This is a good example of why the bronze/silver separation is useful: raw fidelity is preserved, but downstream reporting remains clean and deterministic.

## 6) Azure services used in the proposed solution
- Azure Logic Apps
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks (Delta Lake / Spark SQL)
- Azure Key Vault (for mailbox / source credentials)
- Power BI or another BI tool on top of the gold tables

## 7) AI usage disclosure
I used ChatGPT to help structure the written solution and review SQL / architecture wording. The final design, assumptions, and modelling decisions were reviewed and tailored for this assessment.
