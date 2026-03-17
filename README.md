# NorthWave Telecom — Genie Space Capstone Project

A 5-day capstone project for building, tuning, and documenting a production-grade [Databricks Genie Space](https://docs.databricks.com/en/genie/index.html) using a realistic telecom subscriber dataset.

## Objective

Build a Genie Space that enables a **Customer Insights Analyst** at the fictional **NorthWave Telecom** to explore subscriber status, usage trends, payment performance, and support ticket resolution using natural language — then benchmark it to **85%+ accuracy** across 15 gold-standard questions.

## Repository Structure


| File                                    | Description                                                                                     |
| --------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `genie_capstone_instructions.py`        | Main instructions notebook — project brief, schedule, deliverables, assessment rubric           |
| `genie_space_capstone_dataset_setup.py` | Data generation notebook — creates all 5 tables with synthetic data                             |
| `benchmark_sqls.py`                     | 15 gold-standard SQL queries for benchmarking, with copy-paste-ready output for the Genie Space |


All three files are Databricks notebooks in Python source format. Import them into a Databricks workspace to run.

## Dataset

Five tables modelling a telecom subscriber business:


| Table          | Rows   | Description                                              |
| -------------- | ------ | -------------------------------------------------------- |
| `tc_plans`     | 8      | Service plan catalogue — tier, allowances, monthly fee   |
| `tc_customers` | 600    | Subscriber master data — plan, status, churn risk score  |
| `tc_usage`     | 7,000  | Usage events — voice, data, SMS, roaming                 |
| `tc_tickets`   | 2,000  | Support cases — type, priority, resolution, satisfaction |
| `tc_payments`  | ~3,200 | Billing records — MRC, OTC, adjustments, payment status  |


The dataset includes intentional ambiguities (coded columns, implicit business rules) that must be resolved through Knowledge Store configuration.

## Getting Started

1. **Import** the three `.py` notebooks into your Databricks workspace
2. **Open** `genie_space_capstone_dataset_setup` — set the `catalog` and `schema` widgets, then **Run All** to provision the tables
3. **Open** `genie_capstone_instructions` for the full project brief and day-by-day schedule
4. **Open** `benchmark_sqls` — set the same `catalog` and `schema` widgets, then **Run All** to execute all 15 benchmark queries and generate copy-paste-ready SQL for your Genie Space

## 5-Day Schedule


| Days | Phase                | Focus                                                                                     |
| ---- | -------------------- | ----------------------------------------------------------------------------------------- |
| 1–2  | **Build**            | Create the Genie Space, populate Knowledge Store (UC metadata, instructions, example SQL) |
| 3–4  | **Benchmark & Tune** | Run 15 benchmark questions, identify failures, iterate on Knowledge Store                 |
| 5    | **Monitor & Report** | Build a monitoring dashboard, write the delivery report                                   |


## Benchmark Suite

15 questions spanning Basic → Intermediate → Advanced difficulty:


| #   | Question                                                                                     | Difficulty   |
| --- | -------------------------------------------------------------------------------------------- | ------------ |
| 1   | How many active subscribers do we currently have?                                            | Basic        |
| 2   | What is total revenue collected in the current fiscal year?                                  | Basic        |
| 3   | How many support tickets are currently open?                                                 | Basic        |
| 4   | List all current service plans and their monthly fees                                        | Basic        |
| 5   | What is the average monthly fee per subscriber for each plan tier?                           | Intermediate |
| 6   | Which region has the highest number of subscribers at risk of churning?                      | Intermediate |
| 7   | What is the payment failure rate by payment method?                                          | Intermediate |
| 8   | How does total mobile data consumption compare across plan tiers?                            | Intermediate |
| 9   | What percentage of support tickets were resolved within their SLA target?                    | Intermediate |
| 10  | How many new subscribers were acquired through each channel in the last 12 months?           | Intermediate |
| 11  | What is the monthly churn trend over the last 6 months?                                      | Advanced     |
| 12  | What is the net monthly revenue trend after credits and adjustments?                         | Advanced     |
| 13  | Which active subscribers have both a high churn risk score and a high total payment amount?  | Advanced     |
| 14  | What is the average ticket resolution time (in days) by type and priority?                   | Advanced     |
| 15  | What is the total monthly revenue at risk if all high-churn-risk active subscribers churned? | Advanced     |


**Target:** 13/15 passing (87%)

## Key Domain Rules

These business rules are central to achieving benchmark accuracy:

- **Revenue** = successful (`S`) MRC + OTC payments, minus refunded (`R`) amounts. ADJ excluded from gross revenue.
- **Active subscriber** = `status = 'A'`
- **High churn risk** = `churn_risk_score > 70`
- **Open ticket** = `resolved_date IS NULL`
- **Fiscal year** = April – March
- **SLA targets** = priority 1 → 1 day, 2 → 2 days, 3 → 3 days, 4 → 5 days
- **Unlimited allowance** = `data_gb = 0` or `voice_min = 0` means unlimited, not zero

## Deliverables

1. Configured Genie Space with all 5 tables
2. Knowledge Store — table descriptions, column glossary, 5+ example SQLs
3. Benchmark results — 15 questions at 85%+ accuracy
4. Monitoring dashboard (Genie audit logs)
5. 1-page delivery report

## Assessment Rubric


| Area                 | Weight | Standard                                           |
| -------------------- | ------ | -------------------------------------------------- |
| Genie Space setup    | 10%    | All 5 tables, title, description, sample questions |
| Knowledge Store      | 20%    | Table descriptions, glossary, 5+ example SQLs      |
| Benchmark score      | 15%    | ≥ 85% (13+ of 15 passing)                          |
| Tuning documentation | 45%    | Failure modes documented with root cause and fix   |
| Monitoring + report  | 10%    | Dashboard and 4-section delivery report            |


**Passing threshold:** 80% weighted score

## References

- [Curate an effective Genie Space (best practices)](https://docs.databricks.com/en/genie/best-practices.html)
- [Genie Space overview](https://docs.databricks.com/en/genie/index.html)
- [AI/BI for Data Analysts course](https://partner-academy.databricks.com/learn/courses/3707/aibi-for-data-analysts)
- [Explore Genie via dbdemos](https://www.databricks.com/resources/demos/tutorials)

