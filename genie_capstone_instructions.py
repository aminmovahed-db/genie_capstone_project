# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Space — Capstone Project
# MAGIC
# MAGIC **Companion Notebooks:**
# MAGIC - <a href="$./genie_space_capstone_dataset_setup">genie_space_capstone_dataset_setup</a> — Dataset setup and table provisioning
# MAGIC - <a href="$./benchmark_sqls">benchmark_sqls</a> — Ground truth SQL queries for benchmarking
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Build, tune, and document a production-grade Genie Space using a realistic telecom subscriber dataset. You will complete this over 5 days at the end of your residency program.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset
# MAGIC
# MAGIC The capstone uses five tables provisioned by the <a href="$./genie_space_capstone_dataset_setup">genie_space_capstone_dataset_setup</a> notebook provided by your instructor.
# MAGIC
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `tc_plans` | Service plan catalogue — includes plan tier, data and voice allowances, and monthly fee |
# MAGIC | `tc_customers` | Subscriber master data — includes current plan, subscription status, and churn risk score |
# MAGIC | `tc_usage` | Individual usage events — voice calls, mobile data, SMS, and roaming |
# MAGIC | `tc_tickets` | Customer support cases — includes type, priority, resolution code, and satisfaction score |
# MAGIC | `tc_payments` | Billing and payment records — includes charge type, payment method, and payment status |
# MAGIC
# MAGIC Run the setup notebook provided to you to provision these tables into `{CATALOG}.{SCHEMA}` on your workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Reference
# MAGIC
# MAGIC Several columns use coded values. The full mapping is provided below — use this as your source of truth when configuring the Knowledge Store.
# MAGIC
# MAGIC | Table | Column | Coded Values | Notes |
# MAGIC |-------|--------|-------------|-------|
# MAGIC | tc_plans | plan_tier | 1, 2, 3, 4 | Tier names are not stored in the table |
# MAGIC | tc_plans | contract_type | M, A, B | Single-letter codes only |
# MAGIC | tc_plans | data_gb | 0–unlimited | 0 does NOT mean zero; see notes below |
# MAGIC | tc_plans | voice_min | 0–unlimited | 0 does NOT mean zero; see notes below |
# MAGIC | tc_customers | status | A, S, C, T | Subscriber lifecycle state |
# MAGIC | tc_customers | acq_channel | ONL, RET, REF, TEL | How the subscriber was acquired |
# MAGIC | tc_customers | region | NW, SW, NE, SE, MW | Geographic region codes |
# MAGIC | tc_customers | churn_risk_score | 0–100 integer | Propensity model output |
# MAGIC | tc_usage | usage_type | V, D, S, R | Type of usage event |
# MAGIC | tc_usage | unit | MIN, MB, CNT | Unit of measure for quantity |
# MAGIC | tc_usage | direction | I, O, NA | Call/SMS direction; not applicable for data |
# MAGIC | tc_tickets | priority | 1, 2, 3, 4 | Note: lower number = higher urgency |
# MAGIC | tc_tickets | ticket_type | TEC, BIL, SVC, GEN | Support category |
# MAGIC | tc_tickets | resolution_code | R1, R2, R3, R4 | Outcome of resolved ticket |
# MAGIC | tc_payments | payment_type | MRC, OTC, ADJ | Type of charge; ADJ amounts may be negative |
# MAGIC | tc_payments | pmt_method | CC, DD, BT, WT | Payment instrument |
# MAGIC | tc_payments | pmt_status | S, F, P, R | Transaction outcome |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decode Table
# MAGIC
# MAGIC | Code | Meaning | Code | Meaning |
# MAGIC |------|---------|------|---------|
# MAGIC | plan_tier 1 | Basic | plan_tier 2 | Standard |
# MAGIC | plan_tier 3 | Premium | plan_tier 4 | Enterprise |
# MAGIC | contract_type M | Monthly | contract_type A | Annual |
# MAGIC | contract_type B | Biennial (2-year) | data_gb / voice_min = 0 | Unlimited |
# MAGIC | status A | Active | status S | Suspended |
# MAGIC | status C | Churned | status T | Trial |
# MAGIC | acq_channel ONL | Online | acq_channel RET | Retail store |
# MAGIC | acq_channel REF | Referral | acq_channel TEL | Telesales |
# MAGIC | region NW | Northwest | region SW | Southwest |
# MAGIC | region NE | Northeast | region SE | Southeast |
# MAGIC | region MW | Midwest | churn_risk_score > 70 | High risk |
# MAGIC | usage_type V | Voice call | usage_type D | Mobile data |
# MAGIC | usage_type S | SMS | usage_type R | Roaming data |
# MAGIC | direction I | Inbound | direction O | Outbound |
# MAGIC | priority 1 | Critical | priority 2 | High |
# MAGIC | priority 3 | Medium | priority 4 | Low |
# MAGIC | ticket_type TEC | Technical | ticket_type BIL | Billing |
# MAGIC | ticket_type SVC | Service change | ticket_type GEN | General inquiry |
# MAGIC | resolution_code R1 | Issue Fixed | resolution_code R2 | Workaround Provided |
# MAGIC | resolution_code R3 | No Issue Found | resolution_code R4 | Escalated to Engineering |
# MAGIC | payment_type MRC | Monthly Recurring Charge | payment_type OTC | One-Time Charge |
# MAGIC | payment_type ADJ | Adjustment / credit | pmt_method CC | Credit Card |
# MAGIC | pmt_method DD | Direct Debit | pmt_method BT | Bank Transfer |
# MAGIC | pmt_method WT | Wallet / prepaid | pmt_status S | Successful |
# MAGIC | pmt_status F | Failed | pmt_status P | Pending |
# MAGIC | pmt_status R | Refunded | SLA priority 1 | 1 day |
# MAGIC | SLA priority 2 | 2 days | SLA priority 3 | 3 days |
# MAGIC | SLA priority 4 | 5 days | Fiscal year | April – March |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deliverables
# MAGIC
# MAGIC Submit the following by Day 10:
# MAGIC
# MAGIC 1. A configured Genie Space with all 5 tables
# MAGIC 2. Knowledge Store — table descriptions, column glossary, 5+ example SQLs
# MAGIC 3. Benchmark results — 15 questions at 85%+ accuracy
# MAGIC 4. A simple monitoring dashboard (Genie audit logs)
# MAGIC 5. A 1-page delivery report

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 1-2: Build the Genie Space
# MAGIC
# MAGIC ### 1. Define the business context first
# MAGIC
# MAGIC - **Persona:** Customer Insights Analyst at NorthWave Telecom
# MAGIC - **Domain:** subscriber management, usage analytics, revenue performance, and support operations
# MAGIC - Identify the 15 questions (from the benchmark suite below) your user needs answered
# MAGIC
# MAGIC ### 2. Create the Space
# MAGIC
# MAGIC In Genie, click **+ New**, add all 5 tables, then configure:
# MAGIC
# MAGIC - **Title:** NorthWave Telecom — Subscriber Analytics
# MAGIC - **Description:** Enables customer insights analysts to explore subscriber status, usage trends, payment performance, and support ticket resolution for the NorthWave telecom business
# MAGIC - **Sample questions:** Add 3–5 from the benchmark list below
# MAGIC
# MAGIC ### 3. Populate the Knowledge Store
# MAGIC
# MAGIC Work through each layer — in order of reliability:
# MAGIC
# MAGIC #### Table & column descriptions (edit in Unity Catalog)
# MAGIC
# MAGIC For each table, write a clear business description in Unity Catalog and annotate every coded column using the Column Reference table below. Without these descriptions, Genie has no way to interpret the single-letter and numeric codes stored in the dataset.
# MAGIC
# MAGIC #### Instructions (add in Genie Space settings)
# MAGIC
# MAGIC Using the Column Reference table, write precise Genie instructions that define:
# MAGIC
# MAGIC - How "revenue" should be calculated (consider which payment types and statuses count)
# MAGIC - What subscriber statuses qualify as "active"
# MAGIC - What makes a ticket "open" vs resolved
# MAGIC - What threshold defines "high churn risk"
# MAGIC - How ticket priority maps to urgency and SLA targets
# MAGIC - How to handle zero values in allowance columns
# MAGIC - The fiscal year definition for NorthWave Telecom
# MAGIC - How adjustment payments (ADJ) should be treated in financial calculations
# MAGIC
# MAGIC #### Example SQL (add the 5 most critical queries as certified examples)
# MAGIC
# MAGIC Identify the queries most likely to return wrong results without proper context — these are your best candidates for certified examples. Focus on queries that combine multiple coded columns or involve non-obvious aggregation logic. Write and test each query against the actual tables before adding it to the Knowledge Store.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 3-4: Benchmark & Tune
# MAGIC
# MAGIC # **Tuning Instructions:**
# MAGIC #
# MAGIC ### To maximize Genie Space accuracy and performance:
# MAGIC
# MAGIC 1. Review and implement best practices outlined here: https://docs.databricks.com/aws/en/genie/best-practices
# MAGIC 2. Iteratively test Genie with the benchmark questions below to identify gaps or failure modes.
# MAGIC 3. Tune your Space by refining instructions, updating column/table descriptions, and validating entity mappings.
# MAGIC 4. After each round of tuning, re-run the benchmark suite and log the results.
# MAGIC 5. Focus especially on ambiguous logic (coded columns, revenue calculations, status flags, etc.)—clarify rules in instructions and catalog.
# MAGIC 6. Document all changes and decisions made during tuning for transparency and reproducibility.
# MAGIC
# MAGIC ### Benchmark Suite
# MAGIC
# MAGIC | # | Question | Difficulty |
# MAGIC |---|----------|------------|
# MAGIC | 1 | How many active subscribers do we currently have? | Basic |
# MAGIC | 2 | What is total revenue collected in the current fiscal year? | Basic |
# MAGIC | 3 | How many support tickets are currently open? | Basic |
# MAGIC | 4 | List all current service plans and their monthly fees | Basic |
# MAGIC | 5 | What is the average monthly fee per subscriber for each plan tier? | Intermediate |
# MAGIC | 6 | Which region has the highest number of subscribers at risk of churning? | Intermediate |
# MAGIC | 7 | What is the payment failure rate by payment method? | Intermediate |
# MAGIC | 8 | How does total mobile data consumption compare across plan tiers? | Intermediate |
# MAGIC | 9 | What percentage of support tickets were resolved within their SLA target? | Intermediate |
# MAGIC | 10 | How many new subscribers were acquired through each channel in the last 12 months? | Intermediate |
# MAGIC | 11 | What is the monthly churn trend over the last 6 months? | Advanced |
# MAGIC | 12 | What is the net monthly revenue trend after credits and adjustments? | Advanced |
# MAGIC | 13 | Which active subscribers have both a high churn risk score and a high total payment amount? | Advanced |
# MAGIC | 14 | What is the average ticket resolution time (in days) by type and priority? | Advanced |
# MAGIC | 15 | What is the total monthly revenue at risk if all high-churn-risk active subscribers churned? | Advanced |
# MAGIC
# MAGIC **Target:** 13/15 passing (87%)
# MAGIC
# MAGIC **NOTE:** You can find the ground truth SQL queries for benchmarking in the <a href="$./benchmark_sqls">benchmark_sqls</a> notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Failure Modes
# MAGIC
# MAGIC | Failure | Likely Cause | Fix |
# MAGIC |---------|-------------|-----|
# MAGIC | Revenue inflated — includes failed payments | pmt_status codes not defined | Define which pmt_status values count as collected revenue |
# MAGIC | Active count includes churned subscribers | status codes not defined | Define which status code means Active |
# MAGIC | Ticket priority ordering inverted | priority 1 assumed = least urgent | Clarify priority direction in instructions |
# MAGIC | Data usage double-counts roaming | usage_type R not separated from D | Define each usage_type code in UC description |
# MAGIC | Unlimited plans excluded or shown as zero | data_gb = 0 treated literally | Annotate what zero means for allowance columns |
# MAGIC | SLA % calculated over all tickets | Open tickets included in denominator | Define what "open" means and how to identify resolved tickets |
# MAGIC
# MAGIC Log all your workings and observations with respect to tuning your Genie space to improve accuracy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 5: Monitoring & Delivery Report
# MAGIC
# MAGIC ### Monitoring
# MAGIC
# MAGIC - Open the **Monitoring** tab in your Genie Space — review thumbs up/down feedback and failed queries
# MAGIC - Build a simple AI/BI dashboard with: queries per day, % positive feedback, top 10 questions asked, error rate
# MAGIC - Set a Databricks Alert for when daily error rate > 10%
# MAGIC
# MAGIC ### Delivery Report
# MAGIC
# MAGIC Cover these four sections:
# MAGIC
# MAGIC 1. **Discovery** — what questions did your persona need? Which coded columns caused the most initial failures and why?
# MAGIC 2. **Design decisions** — what did you add to UC metadata vs. Genie instructions vs. example SQL, and why?
# MAGIC 3. **Tuning** — how many iterations? Top 3 failure patterns, root cause analysis, and the exact fix applied each time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assessment Rubric
# MAGIC
# MAGIC | Area | Weight | Standard |
# MAGIC |------|--------|----------|
# MAGIC | Genie Space setup | 10% | All 5 tables, title, description, sample questions |
# MAGIC | Knowledge Store | 20% | Table descriptions, glossary, 5+ example SQLs |
# MAGIC | Benchmark score | 15% | ≥ 85% (13+ of 15 passing) |
# MAGIC | Tuning documentation | 45% | Failure modes documented with root cause and fix |
# MAGIC | Monitoring + report | 10% | Dashboard and 4-section delivery report |
# MAGIC
# MAGIC **Passing threshold:** 80% weighted score

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference
# MAGIC
# MAGIC | Resource | Link |
# MAGIC |----------|------|
# MAGIC | AI/BI for Data Analysts course (includes lab setup) | [Partner Academy](https://partner-academy.databricks.com/learn/courses/3707/aibi-for-data-analysts) |
# MAGIC | Curate an effective Genie Space (best practices) | [Databricks Docs](https://docs.databricks.com/en/genie/best-practices.html) |
# MAGIC | Genie Space overview | [Databricks Docs](https://docs.databricks.com/en/genie/index.html) |
# MAGIC | Explore Genie via dbdemos | [dbdemos.ai](https://www.databricks.com/resources/demos/tutorials) |
