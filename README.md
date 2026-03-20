# NorthWave Telecom — Genie Space Capstone Project

A 5-day capstone at the end of your residency for building, tuning, and documenting a production-grade [Databricks Genie Space](https://docs.databricks.com/en/genie/index.html) using a realistic telecom subscriber dataset.

## Objective

Build a Genie Space that enables a **Customer Insights Analyst** at the fictional **NorthWave Telecom** to explore subscriber status, usage trends, payment performance, and support ticket resolution using natural language — then benchmark it to **85%+ accuracy** across 15 gold-standard questions.

## Repository Structure


| File                                    | Description                                                                                                                                      |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `genie_capstone_instructions.py`        | Main instructions notebook — project brief, column reference & decode tables, schedule, deliverables, assessment rubric                          |
| `genie_space_capstone_dataset_setup.py` | Data generation notebook — creates all 5 tables with synthetic data                                                                              |
| `benchmark_sqls.py`                     | 15 gold-standard SQL queries for benchmarking, with copy-paste-ready output for the Genie Space                                                  |
| `assessment_sqls.py`                    | 15 harder assessment questions + expected SQL — use after tuning to stress-test Genie accuracy                                                   |
| `sample_report.py`                      | Sample delivery report — benchmark score progression per tuning step, Genie link placeholders, Discovery / Design / Tuning / Monitoring sections |


All notebooks are Databricks `.py` source format. Import them into a Databricks workspace to run. Use `sample_report` as a **template** for the delivery report and tuning log (replace sample scores and URLs with your own).

The **full column glossary and coded-value decode table** live in `genie_capstone_instructions.py` — use that as the source of truth when configuring Unity Catalog and the Knowledge Store.

## Dataset

Five tables modelling a telecom subscriber business (provisioned via `genie_space_capstone_dataset_setup` into `{CATALOG}.{SCHEMA}`):


| Table          | Rows   | Description                                              |
| -------------- | ------ | -------------------------------------------------------- |
| `tc_plans`     | 8      | Service plan catalogue — tier, allowances, monthly fee   |
| `tc_customers` | 600    | Subscriber master data — plan, status, churn risk score  |
| `tc_usage`     | 7,000  | Usage events — voice, data, SMS, roaming                 |
| `tc_tickets`   | 2,000  | Support cases — type, priority, resolution, satisfaction |
| `tc_payments`  | ~3,200 | Billing records — MRC, OTC, adjustments, payment status  |


The dataset includes intentional ambiguities (coded columns, implicit business rules) that must be resolved through Unity Catalog descriptions, Genie instructions, and example SQL.

## Getting Started

1. **Import** the `.py` notebooks into your Databricks workspace
2. **Open** `genie_space_capstone_dataset_setup` — set the `catalog` and `schema` widgets, then **Run All** to provision the tables
3. **Open** `genie_capstone_instructions` for the full brief, column reference, decode table, and day-by-day guidance
4. **Open** `benchmark_sqls` — set the same `catalog` and `schema` widgets, then **Run All** to execute all 15 benchmark queries and generate copy-paste-ready SQL for your Genie Space
5. **Open** `assessment_sqls` (optional) — same widgets; 15 harder questions with ground-truth SQL to validate a tuned Space
6. **Open** `sample_report` when you are ready to structure your tuning log and delivery report

## 5-Day Schedule


| Days | Phase                | Focus                                                                                                                       |
| ---- | -------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| 1–2  | **Build**            | Define persona and benchmark questions; create the Space; populate Knowledge Store (UC metadata, instructions, example SQL) |
| 3–4  | **Benchmark & Tune** | Run the 15 benchmark questions, identify failures, iterate on Knowledge Store; log results after each tuning round          |
| 5    | **Monitor & Report** | Use the Genie Space **Monitoring** tab; write the delivery report                                                           |


### Genie Space defaults (from instructions)

- **Title:** NorthWave Telecom — Subscriber Analytics  
- **Description:** Enables customer insights analysts to explore subscriber status, usage trends, payment performance, and support ticket resolution for the NorthWave telecom business  
- **Sample questions:** Add 3–5 from the benchmark list below

### Knowledge Store (order of reliability)

1. **Table & column descriptions** — In Unity Catalog, describe each table and annotate every coded column using the column reference in the instructions notebook.
2. **Instructions** — In Genie Space settings, define revenue calculation, active subscriber rules, open vs resolved tickets, high churn risk, priority/SLA mapping, zero allowances, fiscal year, and how ADJ payments affect financial logic.
3. **Example SQL** — Add the **5** most critical queries as certified examples (especially multi-code joins and non-obvious aggregation). Test each against the tables before certifying.

### Tuning (Days 3–4)

- Follow [Curate an effective Genie Space (best practices)](https://docs.databricks.com/aws/en/genie/best-practices).
- Iteratively run the benchmark questions; refine instructions, catalog descriptions, and entity mappings.  
- After each tuning round, re-run the suite and **log** scores and observations.  
- Document changes and decisions for reproducibility.  
- Ground truth SQL is in `benchmark_sqls`.

## Deliverables

Submit the following **by Day 10**:

1. Configured Genie Space with all 5 tables
2. Knowledge Store — table descriptions, column glossary, 5+ example SQLs
3. Benchmark results — 15 questions at 85%+ accuracy
4. Monitoring review — explore the **Monitoring** tab; note feedback, failures, low-confidence queries, and patterns
5. One-page delivery report covering **Discovery** (persona questions; which coded columns failed first and why), **Design decisions** (what went into UC vs instructions vs example SQL), and **Tuning** (iterations; top 3 failure patterns, root cause, and the exact fix each time) — align with the delivery-report prompts in `genie_capstone_instructions`

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

These business rules are central to achieving benchmark accuracy (full coded mappings are in the instructions notebook):

- **Revenue** = successful (`S`) MRC + OTC payments, minus refunded (`R`) amounts. ADJ excluded from gross revenue; handle adjustments explicitly where questions ask for net trends.
- **Active subscriber** = `status = 'A'`
- **High churn risk** = `churn_risk_score > 70`
- **Open ticket** = `resolved_date IS NULL`
- **Fiscal year** = April – March
- **SLA targets** = priority 1 → 1 day, 2 → 2 days, 3 → 3 days, 4 → 5 days
- **Unlimited allowance** = `data_gb = 0` or `voice_min = 0` means unlimited, not zero

## Assessment Rubric


| Area                 | Weight | Standard                                              |
| -------------------- | ------ | ----------------------------------------------------- |
| Genie Space setup    | 10%    | All 5 tables, title, description, sample questions    |
| Knowledge Store      | 20%    | Table descriptions, glossary, 5+ example SQLs         |
| Benchmark score      | 15%    | ≥ 85% (13+ of 15 passing)                             |
| Tuning documentation | 45%    | Failure modes documented with root cause and fix      |
| Monitoring + report  | 10%    | Monitoring observations and 3-section delivery report |


**Passing threshold:** 80% weighted score

## References


| Resource                                            | Link                                                                                                |
| --------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| AI/BI for Data Analysts course (includes lab setup) | [Partner Academy](https://partner-academy.databricks.com/learn/courses/3707/aibi-for-data-analysts) |
| Curate an effective Genie Space (best practices)    | [Databricks Docs](https://docs.databricks.com/en/genie/best-practices.html)                         |
| Genie Space overview                                | [Databricks Docs](https://docs.databricks.com/en/genie/index.html)                                  |
| Explore Genie via dbdemos                           | [dbdemos](https://www.databricks.com/resources/demos/tutorials)                                     |


