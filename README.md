# NEM Generation Reporting Pipeline — Technical Assessment

## Repository Structure

```
├── diagram/
│   └── pipeline_architecture.drawio   # End-to-end Azure pipeline diagram
├── sql/
│   ├── schema_ddl.sql                 # Bronze / Silver / Gold DDL
│   ├── section_a_regional_price_summary.sql
│   ├── section_b_generation_mix.sql
│   └── section_c_top_generators.sql
└── README.md
```

---

## Part 1 — Pipeline Architecture

### Diagram

See `diagram/pipeline_architecture.drawio`.

### Description

The pipeline follows a **Medallion architecture** (Bronze → Silver → Gold) on Microsoft Azure, using ADLS2 as the storage backbone and Databricks as the transformation engine.

**Layering decisions:**

The Bronze layer ingests raw data with no transformation. It preserves the source exactly as received, which is critical for auditability and reprocessing. The Silver layer applies validation, deduplication, and enriches `unit_dispatch` with generator metadata from the reference table. The Gold layer materialises pre-aggregated tables built for the three report sections, therefore the BI tool runs against aggregated values rather than hundreds of thousands of raw interval rows.

**Handling the daily email delivery of `raw_unit_dispatch.csv`:**

An Azure Logic App monitors the data provider's email inbox. When a new email arrives from the expected sender with a CSV attachment, the Logic App extracts the attachment and writes it to a dedicated `raw/unit_dispatch/` path in ADLS2, naming the file with the delivery date (e.g. `unit_dispatch_2024-08-02.csv`). Azure Data Factory then detects the new file via an event-based trigger and loads it into the `bronze.unit_dispatch` Delta table. 

The key reliability considerations are: (1) a Logic App dead-letter queue and alert if no file arrives by 09:00 AEST; and (2) file archiving after successful ingestion so re-runs can replay from the raw file rather than re-requesting the email.

**Orchestration:**

ADF pipelines orchestrate ingestion and trigger Databricks notebook jobs for Bronze, Silver and Gold transformations. The Gold refresh runs after Silver completes, using ADF dependency chaining. For monitoring and alerting, Azure Monitor alerts are configured on pipeline failure, and a Data Quality notebook in Databricks validates row counts and null rates before loading to Silver tables.

---

## Part 2 — Data Model

### DDL

See `sql/schema_ddl.sql` for full `CREATE TABLE` statements.

### Description

The model contains **three Bronze tables** (raw), **three Silver tables** (cleaned), and **three Gold tables** (one per report section).

The central design decision is to pre-join `unit_dispatch` with `reference_generators` at the Silver layer, so that station name, owner, registered capacity, and region are carried forward on every dispatch row. This eliminates repeated joins in Gold and in BI queries, and means the Gold tables are self-contained for reporting.

The `silver.unit_dispatch` table also includes a generated column `dispatch_mwh = dispatch_mw * (5.0 / 60.0)`, making the MW-to-MWh conversion explicit and consistent across all downstream queries. This is important for energy domain correctness: MW is instantaneous power; MWh is the energy produced over the 5-minute interval, which is what the report's generation mix and capacity factor calculations require.

Partitioning by `interval_date` in Bronze and Silver enables efficient incremental daily loads — each day's ADF run only writes to one partition, and historical partitions are never rewritten. Gold tables are partitioned by `report_month`, which matches how the BI tool filters (monthly reports) and keeps each month's aggregation isolated for easy re-runs.

---

## Part 3 — Report Queries

### Section A — Regional Price Summary

See `sql/section_a_regional_price_summary.sql`.

**Assumptions and choices:**

The NEM market price cap is **$17,500/MWh** and the market floor price is **−$1,000/MWh** as per AEMO's market price limits effective from 1 July 2024. 

Intervals with `rrp >= price cap` and `rrp <= floor price` are counted and reported.

The Gold table is queried directly since it already holds the per-region aggregates.

---

### Section B — Generation Mix by Fuel Type

See `sql/section_b_generation_mix.sql`.

**Assumptions and choices:**

Wind (`WIND`), utility-scale solar (`SOLAR_UTILITY`), and hydro (`HYDRO`) are consolidated into a single `Renewables` category using a `CASE` expression before aggregation. All other fuel types (Black Coal, Brown Coal, Gas OCGT, Gas CCGT, Battery Storage) are reported as their own categories. 
The percentage is calculated per region against the region's total dispatched energy.

---

### Section C — Top 10 Generators by Dispatch Volume *(optional)*

See `sql/section_c_top_generators.sql`.

**Capacity factor definition:**
> Capacity factor = (actual energy output over a specific period) ÷ (maximum potential output if it operated at full nameplate capacity continuously).

For August 2024, it is calculated as:
> Capacity factor in percentage = (actual MWh dispatched in August 2024) ÷ (registered nameplate capacity MW × 744 hours in August 2024) × 100.

**Assumptions and choices:**
744 hours is the correct value for August 2024 (31 days × 24 hours). 

---

## Energy Domain Notes

- **MW vs MWh:** All dispatch data is in MW (instantaneous power). Energy (MWh) is derived by multiplying by the interval duration in hours (5 min = 5/60 hours). This distinction is applied consistently across Silver and Gold layers.
- **NEM regions:** NSW1, VIC1, QLD1, SA1, TAS1 are the five interconnected regions of the Australian National Electricity Market.
- **RRP:** The Regional Reference Price is the settlement price for each 5-minute dispatch interval. The market cap and floor are set by AEMO.
- **DUID:** Dispatchable Unit Identifier — the unique identifier for each generating unit registered with AEMO.

---

**AI Tool used:** I used ChatGPT to check SQL queries and revise the README file. 

---
