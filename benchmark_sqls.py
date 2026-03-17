# Databricks notebook source
# MAGIC %md
# MAGIC # NorthWave Telecom — Benchmark SQL Answers
# MAGIC
# MAGIC Set the **Catalog** and **Schema** widgets at the top of the notebook before running.
# MAGIC
# MAGIC ---

# COMMAND ----------

dbutils.widgets.text("catalog", "your_catalog", "Catalog")
dbutils.widgets.text("schema", "genie_capston", "Schema")

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** When adding benchmark queries in Genie space, replace `IDENTIFIER(:catalog || '.' || :schema || '.<table>')` with `<catalog>.<schema>.<table>` using the actual catalog and schema values from your data environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1 — How many active subscribers do we currently have?

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT COUNT(*) AS active_subscribers
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_customers')
# MAGIC WHERE status = 'A'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2 — What is total revenue collected in the current fiscal year?
# MAGIC
# MAGIC > Fiscal year = April – March. Revenue = successful MRC and OTC payments, minus refunded amounts. ADJ (billing credits) excluded from this gross figure.

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT ROUND(SUM(
# MAGIC     CASE
# MAGIC         WHEN pmt_status = 'S' AND payment_type IN ('MRC', 'OTC') THEN  amount
# MAGIC         WHEN pmt_status = 'R' AND payment_type IN ('MRC', 'OTC') THEN -amount
# MAGIC         ELSE 0
# MAGIC     END), 2) AS total_revenue_collected
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_payments')
# MAGIC WHERE payment_date >= '2025-04-01'
# MAGIC   AND payment_date <  '2026-04-01'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3 — How many support tickets are currently open?
# MAGIC
# MAGIC > Open = `resolved_date IS NULL`.

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT COUNT(*) AS open_tickets
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_tickets')
# MAGIC WHERE resolved_date IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4 — List all current service plans and their monthly fees

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT plan_id,
# MAGIC        plan_name,
# MAGIC        CASE plan_tier
# MAGIC            WHEN 1 THEN 'Basic'
# MAGIC            WHEN 2 THEN 'Standard'
# MAGIC            WHEN 3 THEN 'Premium'
# MAGIC            WHEN 4 THEN 'Enterprise'
# MAGIC        END      AS tier_name,
# MAGIC        monthly_fee
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_plans')
# MAGIC WHERE is_active = TRUE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q5 — What is the average monthly fee per subscriber for each plan tier?

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT p.plan_tier,
# MAGIC        CASE p.plan_tier
# MAGIC            WHEN 1 THEN 'Basic'
# MAGIC            WHEN 2 THEN 'Standard'
# MAGIC            WHEN 3 THEN 'Premium'
# MAGIC            WHEN 4 THEN 'Enterprise'
# MAGIC        END                   AS tier_name,
# MAGIC        ROUND(AVG(p.monthly_fee), 2) AS avg_monthly_fee
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_customers') c
# MAGIC JOIN IDENTIFIER(:catalog || '.' || :schema || '.tc_plans') p ON c.plan_id = p.plan_id
# MAGIC WHERE c.status = 'A'
# MAGIC GROUP BY p.plan_tier
# MAGIC ORDER BY p.plan_tier

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q6 — Which region has the highest number of subscribers at risk of churning?
# MAGIC
# MAGIC > High churn risk = `churn_risk_score > 70`. Active subscribers only.

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT region,
# MAGIC        COUNT(*) AS high_risk_count
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_customers')
# MAGIC WHERE churn_risk_score > 70
# MAGIC   AND status = 'A'
# MAGIC GROUP BY region
# MAGIC ORDER BY high_risk_count DESC
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q7 — What is the payment failure rate by payment method?

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT pmt_method,
# MAGIC        ROUND(100.0 * SUM(CASE WHEN pmt_status = 'F' THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_rate_pct
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_payments')
# MAGIC GROUP BY pmt_method

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q8 — How does total mobile data consumption compare across plan tiers?
# MAGIC
# MAGIC > Mobile data = `usage_type 'D'` only. Roaming (`usage_type 'R'`) is excluded.

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT p.plan_tier,
# MAGIC        CASE p.plan_tier
# MAGIC            WHEN 1 THEN 'Basic'
# MAGIC            WHEN 2 THEN 'Standard'
# MAGIC            WHEN 3 THEN 'Premium'
# MAGIC            WHEN 4 THEN 'Enterprise'
# MAGIC        END                              AS tier_name,
# MAGIC        ROUND(SUM(u.quantity), 0)        AS total_mb,
# MAGIC        ROUND(SUM(u.quantity) / 1024, 2) AS total_gb
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_usage') u
# MAGIC JOIN IDENTIFIER(:catalog || '.' || :schema || '.tc_customers') c ON u.customer_id = c.customer_id
# MAGIC JOIN IDENTIFIER(:catalog || '.' || :schema || '.tc_plans') p ON c.plan_id     = p.plan_id
# MAGIC WHERE u.usage_type = 'D'
# MAGIC GROUP BY p.plan_tier
# MAGIC ORDER BY p.plan_tier

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q9 — What percentage of support tickets were resolved within their SLA target?
# MAGIC
# MAGIC > SLA targets: priority 1 = 1 day, 2 = 2 days, 3 = 3 days, 4 = 5 days. Open tickets excluded.

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT ROUND(100.0 * SUM(CASE
# MAGIC         WHEN DATEDIFF(resolved_date, created_date) <=
# MAGIC              CASE priority WHEN 1 THEN 1 WHEN 2 THEN 2 WHEN 3 THEN 3 WHEN 4 THEN 5 END
# MAGIC         THEN 1 ELSE 0 END) / COUNT(*), 2) AS sla_compliance_pct
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_tickets')
# MAGIC WHERE resolved_date IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q10 — How many new subscribers were acquired through each channel in the last 12 months?

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT CASE acq_channel
# MAGIC            WHEN 'ONL' THEN 'Online'
# MAGIC            WHEN 'RET' THEN 'Retail'
# MAGIC            WHEN 'REF' THEN 'Referral'
# MAGIC            WHEN 'TEL' THEN 'Telesales'
# MAGIC        END      AS channel_name,
# MAGIC        COUNT(*) AS new_subscribers
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_customers')
# MAGIC WHERE acq_date >= ADD_MONTHS(CURRENT_DATE(), -12)
# MAGIC GROUP BY acq_channel

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q11 — What is the monthly churn trend over the last 6 months?
# MAGIC
# MAGIC > Churn date = `last_status_change` for customers with `status = 'C'`.

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT DATE_FORMAT(last_status_change, 'yyyy-MM') AS churn_month,
# MAGIC        COUNT(*) AS churned_count
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_customers')
# MAGIC WHERE status = 'C'
# MAGIC   AND last_status_change >= ADD_MONTHS(CURRENT_DATE(), -6)
# MAGIC GROUP BY DATE_FORMAT(last_status_change, 'yyyy-MM')
# MAGIC ORDER BY churn_month

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q12 — What is the net monthly revenue trend after credits and adjustments?
# MAGIC
# MAGIC > Gross = successful MRC + OTC minus refunded amounts. Net = gross plus ADJ (billing credits, always negative). Refunds reduce gross before adjustments are applied.

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT billing_month,
# MAGIC        ROUND(SUM(CASE
# MAGIC                WHEN payment_type IN ('MRC','OTC') AND pmt_status = 'S' THEN  amount
# MAGIC                WHEN payment_type IN ('MRC','OTC') AND pmt_status = 'R' THEN -amount
# MAGIC                ELSE 0 END), 2)                                         AS gross_revenue,
# MAGIC        ROUND(SUM(CASE WHEN payment_type = 'ADJ'
# MAGIC                       THEN amount ELSE 0 END), 2)                      AS adjustments,
# MAGIC        ROUND(SUM(CASE
# MAGIC                WHEN payment_type IN ('MRC','OTC') AND pmt_status = 'S' THEN  amount
# MAGIC                WHEN payment_type IN ('MRC','OTC') AND pmt_status = 'R' THEN -amount
# MAGIC                WHEN payment_type = 'ADJ'                               THEN  amount
# MAGIC                ELSE 0 END), 2)                                         AS net_revenue
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_payments')
# MAGIC GROUP BY billing_month
# MAGIC ORDER BY billing_month

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q13 — Which active subscribers have both a high churn risk score and a high total payment amount?
# MAGIC
# MAGIC > High churn risk = `churn_risk_score > 70`. High total payment = above the subscriber average (successful payments minus refunds).

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC WITH subscriber_payments AS (
# MAGIC     SELECT customer_id,
# MAGIC            ROUND(SUM(
# MAGIC                CASE
# MAGIC                    WHEN pmt_status = 'S' AND payment_type IN ('MRC','OTC') THEN  amount
# MAGIC                    WHEN pmt_status = 'R' AND payment_type IN ('MRC','OTC') THEN -amount
# MAGIC                    ELSE 0
# MAGIC                END
# MAGIC            ), 2) AS total_paid
# MAGIC     FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_payments')
# MAGIC     GROUP BY customer_id
# MAGIC ),
# MAGIC avg_paid AS (
# MAGIC     SELECT AVG(total_paid) AS avg_total_paid FROM subscriber_payments
# MAGIC )
# MAGIC SELECT c.customer_id,
# MAGIC        c.full_name,
# MAGIC        c.region,
# MAGIC        c.churn_risk_score,
# MAGIC        sp.total_paid
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_customers') c
# MAGIC JOIN subscriber_payments sp ON c.customer_id = sp.customer_id
# MAGIC CROSS JOIN avg_paid ap
# MAGIC WHERE c.status = 'A'
# MAGIC   AND c.churn_risk_score > 70
# MAGIC   AND sp.total_paid      > ap.avg_total_paid
# MAGIC ORDER BY c.churn_risk_score DESC, sp.total_paid DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q14 — What is the average ticket resolution time (in days) by type and priority?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ticket_type,
# MAGIC        priority,
# MAGIC        ROUND(AVG(DATEDIFF(resolved_date, created_date)), 2) AS avg_days_to_resolve
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_tickets')
# MAGIC WHERE resolved_date IS NOT NULL
# MAGIC GROUP BY ticket_type, priority
# MAGIC ORDER BY ticket_type, priority

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q15 — What is the total monthly revenue at risk if all high-churn-risk active subscribers churned?
# MAGIC
# MAGIC > High churn risk active = `status = 'A'` AND `churn_risk_score > 70`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)                        AS high_risk_active_subscribers,
# MAGIC        ROUND(SUM(monthly_spend), 2)    AS monthly_revenue_at_risk
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.tc_customers')
# MAGIC WHERE status = 'A'
# MAGIC   AND churn_risk_score > 70
