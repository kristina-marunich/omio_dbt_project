# Omio Bookings Data Pipeline: Complete Walkthrough

Hey team, here is the complete summary of the bookings data project. We built a robust Star Schema to transform the nested daily export into reliable, analyst-ready data.

## 1. Project Architecture and Setup

I implemented a comprehensive transformation pipeline centered on dbt for the entire workflow.

### Project Steps Taken

1.  Created the dbt project, linked it to BigQuery, and set up version control on GitHub.
2.  Used an artificial file to simulate the semi-structured booking export, loading it to BigQuery to act as our core base source (added to the repo)
4.  Defined all transformations in dbt (SQL/YAML).
5.  Included a functional example of an Airflow DAG to demonstrate how orchestration, scheduling, and failure logic would work in production.

### Data Flow Diagram Notes


#### Initial ERD of Raw Data: Shows the complexity of the 6 intertwined raw tables before transformation.
<img width="476" height="663" alt="image" src="https://github.com/user-attachments/assets/12d4139a-4a46-43d6-9d75-44c21003853d" />

#### Star Schema of Core Marts: Illustrates the final, simplified analytical structure with the central Fact and surrounding Dimensions/Bridges.
<img width="890" height="442" alt="image" src="https://github.com/user-attachments/assets/ef8f48c1-5033-4486-84aa-dcb929565378" />

#### Lineage from Raw to Reporting: Provides the end-to-end lineage, showing how raw columns flow through Staging, Intermediate, and Marts to become final KPIs.
<img width="1349" height="601" alt="image" src="https://github.com/user-attachments/assets/8a85b2a7-e0b6-464e-a281-04c1ccbe995a" />


---

## 2. Data Model Design (Pure Star Schema)

We chose the Pure Star Schema centered on the Ticket Transaction for maximum performance and stability.

### Key Modeling Decisions

* Fact Grain is the Ticket: We chose the Ticket (`fact_ticket_transaction`) as the Fact grain because the primary measure, `ticket_price_eur`, lives on the ticket level. Booking is correctly modeled as a Dimension (the transaction header).
* Master IDs: We assume `passenger_id` and `segment_id` are Master Keys (persistent). This makes our `dim_passenger` and `dim_segment` tables small, efficient, and robust.
* M:M Resolution (Bridge Tables): We use Bridge Tables (`bridge_ticket_passenger` and `bridge_ticket_segment`) to resolve the Many-to-Many (M:M) relationship between the Fact and the Master Dimensions. This ensures we correctly attribute ticket revenue across multiple linked people and segments.

### Data Model Structure

| Layer | Materialization | Key Example |
| :--- | :--- | :--- |
| Staging (`stg_*`) | View | `stg_backend__booking` (Flattening and cleansing of raw JSON.) |
| Intermediate (`int_*`) | View | `int_ticket_base` (Calculates EUR Conversion and handles Master ID deduplication.) |
| Marts: Core (`fact_`, `dim_`, `bridge_`) |  Incremental | `fact_ticket_transaction` (Set to Incremental using `uploaded_at_timestamp` for efficiency.) |
| Marts: Reporting (`Reporting_`) |  Table | `reporting_tickets_monthly_summary` (Ready for BI data mart) |

### Snapshots (Slowly Changing Data)

We would implement snapshots on the `stg_backend___booking` model, targeting the `updated_at_timestamp` column using the `check` strategy.

---


## 3. Data Quality (DQ) Enforcement

We enforce data quality across the pipeline using a structured framework to guarantee stakeholder trust.

### DQ Testing Philosophy and Placement

Our testing strategy covers the entire data lifecycle, recognizing that each dbt layer introduces different risks.

| Layer | When to Test | Test Focus | Implementation |
| :--- | :--- | :--- | :--- |
| Pre-Transform (Staging) | Immediately upon ingestion. | Initial Validity & Cleansing: We run quick checks to ensure type conformance and basic validity (e.g., price $\ge 0$). Goal is to fail fast. | YAML (`accepted_values`)  |
| Post-Transform (Marts) | After all joins and aggregations are complete. | Final Integrity & Business Logic: We confirm structural integrity (PKs, FKs) and run complex anomaly checks. | YAML (`unique`, `relationships`) and `tests//` SQL files. |

---

### DQ Question Answers

#### Business vs. Technical Tests, what's the difference?

The core difference lies in the purpose and origin of the requirement:

* Technical tests check the quiality and schema constraints. They ensure the pipeline worked correctly. *Example:* Ensuring `ticket_id` is always unique (defined in YAML).
* Business tests check the data logic and enforce domain rules. They validate that the data makes sense based on real-world constraints. *Example:* "Every booking has $\ge 1$ passenger" (defined in a custom SQL file in `tests//`).

#### How to test for seasonality anomalies like Train strikes?

Anomalies caused by external factors require sophisticated time-aware comparisons.

1.  Year-over-Year (YoY) Baseline: We start by comparing current volume/revenue to the same period last year. This naturally controls for recurring seasonality (e.g., holiday travel spikes).
2.  Z-Score Anomaly Detection: We use the Z-score method to flag statistically significant deviations.
3.  Exclusion Window: For non-recurring events like strikes, the Z-score logic is refined using an Exclusion Window. We write the test logic to exclude all days flagged as "strike days" from the historical mean calculation. This prevents a false comparison that would otherwise trigger an alert against a stable period.

### DQ Implementation Summary

| Test Type | Focus | Location | Implementation Style |
| :--- | :--- | :--- | :--- |
| Technical Tests | Structural Health (PKs, FKs) | YAML (`schema.yml`) in Marts folders. | `unique`, `not_null`, `relationships` (Native dbt tests). |
| Business Logic Tests | Domain Rules (e.g., $\ge 1$ passenger) | `tests/` folder. | Custom `.sql` file that returns rows on failure. |
| Time-Aware Tests | Anomalies (Volume/Price Spikes) | `tests/` folder. | Custom `.sql` file using Z-Score and window functions. |

---


## 4. Pipeline Orchestration Walkthrough (Airflow)

This section explains the provided Airflow DAG, which orchestrates the daily process of transforming raw booking data into final reporting marts.

### Orchestration Context

This DAG is configured to work specifically with dbt Cloud, utilizing the `DbtCloudRunJobOperator`. This approach is common in production because dbt Cloud handles the compute and environment management, letting Airflow focus purely on scheduling and dependency management.

Note: This DAG file is included here as an example of my approach. In a true production environment, the Airflow DAG files would reside in a separate, dedicated Git repository from the dbt project for better separation of concerns and security. If the dbt project were run using dbt Core (CLI), I would replace the `DbtCloudRunJobOperator` with a generic `BashOperator` or a specialized provider (like `dbt-common`) to run commands like `dbt run --target prod`.

### DAG Flow and Logic

The DAG implements the essential "Test Then Deploy" pipeline methodology, ensuring data quality is always verified before production tables are updated.

1.  Ingest Raw Export (`ingest_raw_export`):
    * Action: This task simulates the initial Extract and Load (E&L) step—a separate service (like Fivetran or a custom loader) dumping the latest semi-structured booking data into the raw BigQuery schema.
    * Operator: Uses a simple `BashOperator` as a mock.

2.  Run Staging, Intermediate & Test (`run_staging_build_and_test`):
    * Action: Triggers the first dbt Cloud Job. This job is configured to run all transformations up through the Marts layer and execute all data quality checks (`dbt test`).
    * Failure Logic: This task is the Data Quality Gate. If any model or test fails, the pipeline stops immediately.

3.  Promote to Production (`run_prod_deploy`):
    * Action: Triggers the final dbt Cloud Job, configured to run only the Marts layer models (`dbt run --target production`).
    * Dependency: Uses `trigger_rule="all_success"`. This task only runs if Step 2 successfully passed every data quality check.

4.  Notifications (`default_args` / `notify_success`):
    * Scheduling: The DAG runs daily at 6 AM UTC (`schedule_interval="0 6 * * *"`).
    * Failure: `email_on_failure=True` in `default_args` ensures immediate alerts are sent to the data team if any test or task fails.
    * Retries: The pipeline is configured with 2 retries and a 5-minute delay to gracefully handle transient cloud or network errors.

---

---

## 5. Stakeholder Communication (One-Pager for PMs)


[Booking Reporting Overview.docx](https://github.com/user-attachments/files/23052723/Booking.Reporting.Overview.docx)

## 6. Metrics and KPIs

The KPIs in the final report are chosen not only for strategic importance but also to demonstrate how your fact_ticket_transaction and Bridge Tables enable powerful, complex business insights.
These might be 5 most important KPIs for Omio (some of them are included to the final reporting):
1. Total Net Revenue (EUR): The core measure of financial performance.
2. Average Booking Value (ABV): Measures the average value per order, indicating efficiency and upsell success.
3. Revenue per Passenger: Tracks the total economic value generated by the average individual customer.
4. Booking Volume: The fundamental measure of market activity and total orders placed.
5. Cancellation Rate: A critical health metric tracking post-purchase satisfaction and operational stability.

