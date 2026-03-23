# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Space — Capstone Project Instructions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objective
# MAGIC
# MAGIC Build, tune, and document a production-grade Genie Space using a realistic telecom subscriber dataset. You can complete this over 5 days ( < 10 hrs in total).
# MAGIC
# MAGIC By the end of the capstone project, the learner should be able to effectively set up and tune Genie space to achieve production-grade accuracy following best practices. 

# COMMAND ----------

# MAGIC %md ##Business Context & Dataset
# MAGIC
# MAGIC - **Persona:** Customer Insights Analyst at NorthWave Telecom
# MAGIC - **Domain:** subscriber management, usage analytics, revenue performance, and support operations
# MAGIC - **Critical User Journey:** Enables customer insights analysts to explore subscriber status, usage trends, payment performance, and support ticket resolution for the NorthWave telecom business
# MAGIC

# COMMAND ----------

# MAGIC %md ### Tables
# MAGIC
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `tc_plans` | Service plan catalogue — includes plan tier, data and voice allowances, and monthly fee |
# MAGIC | `tc_customers` | Subscriber master data — includes current plan, subscription status, and churn risk score |
# MAGIC | `tc_usage` | Individual usage events — voice calls, mobile data, SMS, and roaming |
# MAGIC | `tc_tickets` | Customer support cases — includes type, priority, resolution code, and satisfaction score |
# MAGIC | `tc_payments` | Billing and payment records — includes charge type, payment method, and payment status |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Coded Column
# MAGIC
# MAGIC The tables contain columns storing only coded values. The full mapping is provided in next cell.
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
# MAGIC | tc_usage | unit | MIN, MB, CNT | Unit of measure for quantity; use 1 GB = 1024 MB for conversions |
# MAGIC | tc_usage | direction | I, O, NA | Call/SMS direction; not applicable for data |
# MAGIC | tc_tickets | priority | 1, 2, 3, 4 | Note: lower number = higher urgency |
# MAGIC | tc_tickets | ticket_type | TEC, BIL, SVC, GEN | Support category |
# MAGIC | tc_tickets | resolution_code | R1, R2, R3, R4 | Outcome of resolved ticket |
# MAGIC | tc_payments | payment_type | MRC, OTC, ADJ | Type of charge; ADJ amounts may be negative |
# MAGIC | tc_payments | pmt_method | CC, DD, BT, WT | Payment instrument |
# MAGIC | tc_payments | pmt_status | S, F, P, R | Transaction outcome |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decoded Values
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
# MAGIC | direction NA | Not applicable (data/roaming) | | |
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

# MAGIC %md ##Implementation Steps
# MAGIC
# MAGIC Step 1: Run <a href="$./2.genie_space_capstone_dataset_setup">genie_space_capstone_dataset_setup</a> to create tables and data in your own workspace, catalog and schema
# MAGIC
# MAGIC Step 2: Create empty Genie Space, add <a href="$./3.benchmark_sqls">benchmark_sqls</a> to the space — the notebook contains benchmark questions with ground truth SQLs for training/tuning Genie space - MUST score >85%
# MAGIC
# MAGIC Step 3: Run benchmark Evaluation to generate baseline accuracy
# MAGIC
# MAGIC Step 4: Iteratively tune Genie space following <a href="https://docs.google.com/document/d/1HTUbxnO9y5NQQI3ZyBY9nR6V0TKuRZ0nRhHBD2alPkI/edit?tab=t.0">Best Practices For Building A Genie Space</a> and <a href="https://docs.databricks.com/aws/en/genie/best-practices">Curate an effective Genie space</a>
# MAGIC
# MAGIC Step 5: Frequently run Benchmark Evaluations on all benchmark questions to show progress - Go back to Step 4 until target >85% is achieved
# MAGIC
# MAGIC Step 6: Once you have achieved accuracy score, add <a href="$./4.assessment_sqls">assessment_sqls</a> to the space and run Benmark evaluations on these queries. - These queries are only for cross validating tuned Genie space - DO NOT tune with these queries. **NOTE**: You are not marked based on accuracy of assessment queries, but please record your the result in the delivery report
# MAGIC
# MAGIC Step 7: Capture your tuning logs, key findings, lessons learnt in delivery report following this template  <a href="$./5.sample_report">sample_report</a> and sumbit for assessment

# COMMAND ----------

# MAGIC %md ## Deliverables
# MAGIC - Confirm the catalog and schema name you have used
# MAGIC - Export and share Delivery Report created in Step 7
# MAGIC - Open the Genie space in the UI, go to Settings, and copy the space ID from there.
# MAGIC - Export and share Genie Space as JSON using Export API below:
# MAGIC
# MAGIC ```
# MAGIC export DATABRICKS_HOST="https://<your-workspace-url>"
# MAGIC export DATABRICKS_TOKEN="<your_pat>"
# MAGIC export SPACE_ID="<your_genie_space_id>"
# MAGIC curl -X GET \
# MAGIC   "$DATABRICKS_HOST/api/2.0/genie/spaces/$SPACE_ID?include_serialized_space=true" \
# MAGIC   -H "Authorization: Bearer $DATABRICKS_TOKEN" \
# MAGIC   -H "Content-Type: application/json" 
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assessment Rubric
# MAGIC
# MAGIC | Area | Weight | Standard |
# MAGIC |------|--------|----------|
# MAGIC | Genie Space setup with knowledge store | 30% | A fully configured Genie Space with source tables, knowledge Store with example SQL, table relationships, SQL expressions, and general instructions |
# MAGIC | Benchmark score | 20% | ≥ 85% (13+ of 15 passing) |
# MAGIC | Tuning documentation | 40% | Tuning log, discovery findings, lessons learnt, and handover recommendations |
# MAGIC | AI/BI Dashboard | 10% | Pairing Genie with a sample AI/BI Dashboards |
# MAGIC
# MAGIC **Passing threshold:** 80% weighted score
