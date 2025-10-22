# Omio Bookings Data Pipeline: Complete Walkthrough

Hey team, here is the complete summary of the bookings data project. We built a robust **Star Schema** to transform the nested daily export into reliable, analyst-ready data.

## 1. Project Architecture and Setup

I implemented a comprehensive transformation pipeline centered on dbt for the entire workflow.

### Project Steps Taken

1.  Created the dbt project, linked it to BigQuery, and set up version control on GitHub.
2.  Used an artificial file to simulate the semi-structured booking export, loading it to BigQuery to act as our core base source.
3.  Defined all transformations in dbt (SQL/YAML).
4.  Included a functional example of an Airflow DAG to demonstrate how orchestration, scheduling, and failure logic would work in production.

### Data Flow Diagram Notes


#### Initial ERD of Raw Data: Shows the complexity of the 6 intertwined raw tables before transformation.
<img width="476" height="663" alt="image" src="https://github.com/user-attachments/assets/12d4139a-4a46-43d6-9d75-44c21003853d" />

#### Star Schema of Core Marts: Illustrates the final, simplified analytical structure with the central Fact and surrounding Dimensions/Bridges.
<img width="890" height="442" alt="image" src="https://github.com/user-attachments/assets/ef8f48c1-5033-4486-84aa-dcb929565378" />

#### Lineage from Raw to Reporting: Provides the end-to-end lineage, showing how raw columns flow through Staging, Intermediate, and Marts to become final KPIs.
<img width="1349" height="601" alt="image" src="https://github.com/user-attachments/assets/8a85b2a7-e0b6-464e-a281-04c1ccbe995a" />


---

## 2. Data Model Design (Pure Star Schema)

We chose the **Pure Star Schema** centered on the **Ticket Transaction** for maximum performance and stability.

### Key Modeling Decisions

* **Fact Grain is the Ticket:** We chose the **Ticket** (`fact_ticket_transaction`) as the Fact grain because the primary measure, **`ticket_price_eur`**, lives on the ticket level. **Booking** is correctly modeled as a **Dimension** (the transaction header).
* **Master IDs:** We assume `passenger_id` and `segment_id` are **Master Keys** (persistent). This makes our `dim_passenger` and `dim_segment` tables small, efficient, and robust.
* **M:M Resolution (Bridge Tables):** We use **Bridge Tables** (`bridge_ticket_passenger` and `bridge_ticket_segment`) to resolve the **Many-to-Many (M:M)** relationship between the Fact and the Master Dimensions. This ensures we correctly attribute ticket revenue across multiple linked people and segments.

### Data Model Structure

| Layer | Materialization | Key Example |
| :--- | :--- | :--- |
| **Staging** (`stg_*`) | **View** | `stg_backend__booking` (Flattening and cleansing of raw JSON.) |
| **Intermediate** (`int_*`) | **View** | `int_ticket_base` (Calculates **EUR Conversion** and handles Master ID deduplication.) |
| **Marts: Core** (`fact_`, `dim_`, `bridge_`) | ** Incremental** | `fact_ticket_transaction` (Set to **Incremental** using `uploaded_at_timestamp` for efficiency.) |
| **Marts: Reporting** (`Reporting_`) | ** Table** | `reporting_tickets_monthly_summary` (Ready for BI data mart) |

### Snapshots (Slowly Changing Data)

We would implement snapshots on the **`dim_booking`** model, targeting the **`updated_at_timestamp`** column using the `check` strategy.

* **Answers Historical Questions:** By tracking changes to the booking header (like `partner_id`), we can accurately answer:
    * **"What was the offer partner when the user first booked?"** (Query the earliest record for that `booking_id` in the snapshot.)

---

## 3. Data Quality (DQ) and Governance

Data quality is enforced at every step to build trust.

### DQ Testing Strategy

| Test Type | Focus | Location | Implementation Style |
| :--- | :--- | :--- | :--- |
| **Technical Tests** | **Structural Health** (PKs, FKs) | **YAML** (`schema.yml`) in Marts folders. | `unique`, `not_null`, `relationships`. |
| **Business Logic Tests** | **Domain Rules** (e.g., $\ge 1$ passenger) | **`tests/singular/`** folder. | Custom `.sql` file that returns rows on failure. |
| **Time-Aware Tests** | **Anomalies** (Volume/Price Spikes) | **`tests/singular/`** folder. | Custom `.sql` file using Z-Score and window functions. |

### DQ Question Answers

* **Business vs. Technical Tests, what's the difference?**
    * **Technical tests** check the plumbing (Is the FK valid?).
    * **Business tests** check the data logic (Does every order have a customer?). They validate domain knowledge.

* **How to test for seasonality anomalies like Train strikes?**
    We use **Year-over-Year (YoY)** comparisons as a baseline. For anomalous events like strikes, the **Z-score** logic should use an **Exclusion Window** to exclude the strike period from the historical mean calculation, preventing false positives when comparing the current run to a stable period.

---

## 4. Pipeline Orchestration (Airflow)

The pipeline is scheduled daily and built to be resilient, using the "Test and then Build" methodology.

### Airflow DAG Logic & Dependencies

1.  **Ingest $\rightarrow$ Run Staging/Intermediate:** Loads the newest raw data and runs the initial, inexpensive transformations.
2.  **Run Tests:** Executes `dbt test` across the whole project.
3.  **Run Production:** The `run_marts` task is set with `trigger_rule='all_success'`. **If the tests fail, the production build is BLOCKED.**
4.  **Failure Logic:** Retry logic (3 retries with backoff) handles transient errors, and a failure alert is sent immediately to the engineering team if any test or task fails.

---

## 5. Stakeholder Communication (One-Pager for PMs)

**TO:** Product Managers & Business Leads
**FROM:** Analytics Engineering
**DATE:** October 2025

### **New Data Source: Reliable Booking Transaction Data**

This new system converts our complex daily booking exports into clear, analytical tables, ensuring we are all looking at one source of truth for revenue and customer metrics.

| What's New? | What does it fix? | How to use it? |
| :--- | :--- | :--- |
| **Normalized Revenue (EUR)** | Currency fluctuations are neutralized. All revenue metrics (`ticket_price_eur`) are automatically converted to a single base currency (EUR) at the time of transaction. | All revenue metrics are reliable for YOY comparisons. |
| **Master ID Tracking** | We can track the same person (`Master Passenger ID`) across multiple orders. | You can accurately measure **Average Bookings per User** and analyze customer lifetime value. |
| **Data Quality Assurance** | Unexpected errors (missing data, price spikes) are caught *before* they hit your dashboard. | If you see a major dip in the ticket volume metric, trust that the pipeline flagged a problem, and the team is already investigating. |

### Core Data KPIs to Monitor (Bonus)

