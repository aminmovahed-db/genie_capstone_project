# Databricks notebook source

# COMMAND ----------

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

# MAGIC %md
# MAGIC ##Business Context & Dataset
# MAGIC 
# MAGIC - **Persona:** Customer Insights Analyst at NorthWave Telecom
# MAGIC - **Domain:** subscriber management, usage analytics, revenue performance, and support operations
# MAGIC - **Critical User Journey:** Enables customer insights analysts to explore subscriber status, usage trends, payment performance, and support ticket resolution for the NorthWave telecom business

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables
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
# MAGIC ### Coded Columns & Decoded Values
# MAGIC 
# MAGIC The tables contain columns storing only coded values. The full mapping is below.
# MAGIC 
# MAGIC | Table | Column | Code | Meaning | Notes |
# MAGIC |-------|--------|------|---------|-------|
# MAGIC | tc_plans | plan_tier | 1 | Basic | Tier names are not stored in the table |
# MAGIC | | | 2 | Standard | |
# MAGIC | | | 3 | Premium | |
# MAGIC | | | 4 | Enterprise | |
# MAGIC | | data_gb | 0 | Unlimited | 0 does NOT mean zero — it means unlimited |
# MAGIC | | voice_min | 0 | Unlimited | 0 does NOT mean zero — it means unlimited |
# MAGIC | | | | | |
# MAGIC | tc_customers | status | A | Active | Subscriber lifecycle state |
# MAGIC | | | S | Suspended | |
# MAGIC | | | C | Churned | |
# MAGIC | | | T | Trial | |
# MAGIC | | acq_channel | ONL | Online | How the subscriber was acquired |
# MAGIC | | | RET | Retail store | |
# MAGIC | | | REF | Referral | |
# MAGIC | | | TEL | Telesales | |
# MAGIC | | region | NW | Northwest | Geographic region codes |
# MAGIC | | | SW | Southwest | |
# MAGIC | | | NE | Northeast | |
# MAGIC | | | SE | Southeast | |
# MAGIC | | | MW | Midwest | |
# MAGIC | | churn_risk_score | > 70 | High risk | 0–100 integer; propensity model output |
# MAGIC | | | | | |
# MAGIC | tc_usage | usage_type | V | Voice call | Type of usage event |
# MAGIC | | | D | Mobile data | |
# MAGIC | | | S | SMS | |
# MAGIC | | | R | Roaming data | |
# MAGIC | | unit | MIN | Minutes | Unit of measure; use 1 GB = 1024 MB for conversions |
# MAGIC | | | MB | Megabytes | |
# MAGIC | | | CNT | Count | |
# MAGIC | | | | | |
# MAGIC | tc_tickets | priority | 1 | Critical | Lower number = higher urgency; SLA: 1 day |
# MAGIC | | | 2 | High | SLA: 2 days |
# MAGIC | | | 3 | Medium | SLA: 3 days |
# MAGIC | | | 4 | Low | SLA: 5 days |
# MAGIC | | ticket_type | TEC | Technical | Support category |
# MAGIC | | | BIL | Billing | |
# MAGIC | | | SVC | Service change | |
# MAGIC | | | GEN | General inquiry | |
# MAGIC | | resolution_code | R1 | Issue Fixed | Outcome of resolved ticket |
# MAGIC | | | R2 | Workaround Provided | |
# MAGIC | | | R3 | No Issue Found | |
# MAGIC | | | R4 | Escalated to Engineering | |
# MAGIC | | | | | |
# MAGIC | tc_payments | payment_type | MRC | Monthly Recurring Charge | Type of charge; ADJ amounts may be negative |
# MAGIC | | | OTC | One-Time Charge | |
# MAGIC | | | ADJ | Adjustment / credit | |
# MAGIC | | pmt_method | CC | Credit Card | Payment instrument |
# MAGIC | | | DD | Direct Debit | |
# MAGIC | | | BT | Bank Transfer | |
# MAGIC | | | WT | Wallet / prepaid | |
# MAGIC | | | | | |
# MAGIC | — | Fiscal year | — | April – March | |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Derived subscriber metrics (not table columns)
# MAGIC 
# MAGIC `tc_payments` has no **total payment amount** column — each row stores **`amount`**. For questions about **high total payment**, compute each subscriber's **total paid** by aggregating `amount` with the same rules as benchmark **Q8** (net successful and refunded MRC/OTC; adjustments do not contribute to that total). **High** means that per-subscriber total is **greater than** the average of those totals across all subscribers.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Implementation Steps
# MAGIC 
# MAGIC Step 1: Review the <a href="$./sample_report">sample_report</a> template — start capturing your tuning logs, key findings, and lessons learnt from the very beginning so you don't miss any details for the final delivery report
# MAGIC 
# MAGIC Step 2: Run <a href="$./2.genie_space_capstone_dataset_setup">genie_space_capstone_dataset_setup</a> to create tables and data in your own workspace, catalog and schema
# MAGIC 
# MAGIC Step 3: Create empty Genie Space, add <a href="$./3.benchmark_sqls">benchmark_sqls</a> to the space — the notebook contains benchmark questions with ground truth SQLs for training/tuning Genie space - MUST score >85%
# MAGIC 
# MAGIC Step 4: Run benchmark Evaluation to generate baseline accuracy
# MAGIC 
# MAGIC Step 5: Iteratively tune Genie space following the reference material below:
# MAGIC - <a href="https://docs.databricks.com/aws/en/genie/best-practices">Curate an effective Genie space</a>
# MAGIC - <a href="https://docs.databricks.com/aws/en/genie/set-up">Set up and manage a Genie space</a>
# MAGIC - <a href="https://docs.databricks.com/aws/en/genie/knowledge-store">Build a knowledge store for more reliable Genie spaces</a>
# MAGIC - <a href="https://docs.google.com/document/d/1HTUbxnO9y5NQQI3ZyBY9nR6V0TKuRZ0nRhHBD2alPkI/edit?tab=t.0">Best Practices For Building A Genie Space</a> (internal Databricks document; not publicly accessible)
# MAGIC 
# MAGIC Step 6: Frequently run Benchmark Evaluations on all benchmark questions to show progress - Go back to Step 5 until target >85% is achieved
# MAGIC 
# MAGIC Step 7: Once you have achieved accuracy score, add <a href="$./4.assessment_sqls">assessment_sqls</a> to the space and run Benchmark evaluations on these queries. - These queries are only for cross validating tuned Genie space - DO NOT tune with these queries. **NOTE**: You are not marked based on accuracy of assessment queries, but please record your the result in the delivery report
# MAGIC 
# MAGIC Step 8: Finalise your delivery report and submit for assessment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference material
# MAGIC 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deliverables
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
# MAGIC | Benchmark score | 20% | ≥ 85% (9+ of 10 passing) |
# MAGIC | Tuning documentation | 40% | Tuning log, discovery findings, lessons learnt, and handover recommendations |
# MAGIC | AI/BI Dashboard | 10% | Pairing Genie with a sample AI/BI Dashboards |
# MAGIC 
# MAGIC **Passing threshold:** 80% weighted score
