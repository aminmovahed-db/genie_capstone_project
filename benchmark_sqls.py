# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # NorthWave Telecom — Benchmark SQL Answers
# MAGIC
# MAGIC Set the **Catalog** and **Schema** widgets at the top of the notebook, then copy and paste the SQL (with your catalog/schema substituted) for the Genie Space
# MAGIC
# MAGIC ---

# COMMAND ----------

dbutils.widgets.text("catalog", "your_catalog", "Catalog")
dbutils.widgets.text("schema", "genie_capston", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Genie Space — Copy-Paste Ready SQL
# MAGIC
# MAGIC The cell below prints all 15 in a clean format you can copy directly into your Genie Space as example SQL.

# COMMAND ----------

GENIE_QUERIES = {
    "Q1 — How many active subscribers do we currently have?": f"""
SELECT COUNT(*) AS active_subscribers
FROM {catalog}.{schema}.tc_customers
WHERE status = 'A'
""",

    "Q2 — What is total revenue collected in the current fiscal year?": f"""
SELECT ROUND(SUM(
    CASE
        WHEN pmt_status = 'S' AND payment_type IN ('MRC', 'OTC') THEN  amount
        WHEN pmt_status = 'R' AND payment_type IN ('MRC', 'OTC') THEN -amount
        ELSE 0
    END), 2) AS total_revenue_collected
FROM {catalog}.{schema}.tc_payments
WHERE payment_date >= '2025-04-01'
  AND payment_date <  '2026-04-01'
""",

    "Q3 — How many support tickets are currently open?": f"""
SELECT COUNT(*) AS open_tickets
FROM {catalog}.{schema}.tc_tickets
WHERE resolved_date IS NULL
""",

    "Q4 — List all current service plans and tiers and their monthly fees": f"""
SELECT plan_name,
       CASE plan_tier
           WHEN 1 THEN 'Basic'
           WHEN 2 THEN 'Standard'
           WHEN 3 THEN 'Premium'
           WHEN 4 THEN 'Enterprise'
       END      AS tier_name,
       monthly_fee
FROM {catalog}.{schema}.tc_plans
WHERE is_active = TRUE
""",

    "Q5 — What is the average monthly fee per active subscriber for each plan tier?": f"""
SELECT 
       CASE p.plan_tier
           WHEN 1 THEN 'Basic'
           WHEN 2 THEN 'Standard'
           WHEN 3 THEN 'Premium'
           WHEN 4 THEN 'Enterprise'
       END                   AS tier_name,
       ROUND(AVG(p.monthly_fee), 2) AS avg_monthly_fee
FROM {catalog}.{schema}.tc_customers c
JOIN {catalog}.{schema}.tc_plans p ON c.plan_id = p.plan_id
WHERE c.status = 'A'
GROUP BY p.plan_tier
ORDER BY p.plan_tier
""",

    "Q6 — Which region has the highest number of subscribers at risk of churning?": f"""
SELECT CASE tc_customers.region WHEN 'NW' THEN 'Northwest' WHEN 'SW' THEN 'Southwest' WHEN 'NE' THEN 'Northeast' WHEN 'SE' THEN 'Southeast' WHEN 'MW' THEN 'Midwest' END,
       COUNT(*) AS high_risk_count
FROM {catalog}.{schema}.tc_customers
WHERE churn_risk_score > 70
  AND status = 'A'
GROUP BY CASE tc_customers.region WHEN 'NW' THEN 'Northwest' WHEN 'SW' THEN 'Southwest' WHEN 'NE' THEN 'Northeast' WHEN 'SE' THEN 'Southeast' WHEN 'MW' THEN 'Midwest' END
ORDER BY high_risk_count DESC
LIMIT 1
""",

    "Q7 — What is the payment failure rate by payment method?": f"""
SELECT CASE pmt_method WHEN 'CC' THEN 'Credit Card' WHEN 'DD' THEN 'Direct Debit' WHEN 'EW' THEN 'E-Wallet' WHEN 'BT' THEN 'Bank Transfer' END as payment_method,
       ROUND(100.0 * SUM(CASE WHEN pmt_status = 'F' THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_rate_pct
FROM {catalog}.{schema}.tc_payments
GROUP BY payment_method
""",

    "Q8 — How does total mobile data consumption compare across plan tiers?": f"""
SELECT 
       CASE p.plan_tier
           WHEN 1 THEN 'Basic'
           WHEN 2 THEN 'Standard'
           WHEN 3 THEN 'Premium'
           WHEN 4 THEN 'Enterprise'
       END                              AS tier_name,
       ROUND(SUM(u.quantity) / 1024, 2) AS total_gb
FROM {catalog}.{schema}.tc_usage u
JOIN {catalog}.{schema}.tc_customers c ON u.customer_id = c.customer_id
JOIN {catalog}.{schema}.tc_plans p ON c.plan_id     = p.plan_id
WHERE u.usage_type = 'D'
GROUP BY tier_name
ORDER BY tier_name
""",

    "Q9 — What percentage of support tickets were resolved within their SLA target?": f"""
SELECT ROUND(100.0 * SUM(CASE
        WHEN DATEDIFF(resolved_date, created_date) <=
             CASE priority WHEN 1 THEN 1 WHEN 2 THEN 2 WHEN 3 THEN 3 WHEN 4 THEN 5 END
        THEN 1 ELSE 0 END) / COUNT(*), 2) AS sla_compliance_pct
FROM {catalog}.{schema}.tc_tickets
WHERE resolved_date IS NOT NULL
""",

    "Q10 — How many new subscribers were acquired through each channel in the last 12 months?": f"""
SELECT CASE acq_channel
           WHEN 'ONL' THEN 'Online'
           WHEN 'RET' THEN 'Retail'
           WHEN 'REF' THEN 'Referral'
           WHEN 'TEL' THEN 'Telesales'
       END      AS channel_name,
       COUNT(*) AS new_subscribers
FROM {catalog}.{schema}.tc_customers
WHERE acq_date >= ADD_MONTHS(CURRENT_DATE(), -12)
GROUP BY acq_channel
""",

    "Q11 — What is the monthly churn trend over the last 6 months?": f"""
SELECT DATE_FORMAT(last_status_change, 'yyyy-MM') AS churn_month,
       COUNT(*) AS churned_count
FROM {catalog}.{schema}.tc_customers
WHERE status = 'C'
  AND last_status_change >= ADD_MONTHS(CURRENT_DATE(), -6)
GROUP BY DATE_FORMAT(last_status_change, 'yyyy-MM')
ORDER BY churn_month
""",

    "Q12 — What is the net monthly revenue trend after credits and adjustments?": f"""
SELECT billing_month,
       ROUND(SUM(CASE
               WHEN payment_type IN ('MRC','OTC') AND pmt_status = 'S' THEN  amount
               WHEN payment_type IN ('MRC','OTC') AND pmt_status = 'R' THEN -amount
               WHEN payment_type = 'ADJ'                               THEN  amount
               ELSE 0 END), 2)                                         AS net_revenue
FROM {catalog}.{schema}.tc_payments
GROUP BY billing_month
ORDER BY billing_month
""",

    "Q13 — Which active subscribers have both a high churn risk score and a high total payment amount?": f"""
WITH subscriber_payments AS (
    SELECT customer_id,
           ROUND(SUM(
               CASE
                   WHEN pmt_status = 'S' AND payment_type IN ('MRC','OTC') THEN  amount
                   WHEN pmt_status = 'R' AND payment_type IN ('MRC','OTC') THEN -amount
                   ELSE 0
               END
           ), 2) AS total_paid
    FROM {catalog}.{schema}.tc_payments
    GROUP BY customer_id
),
avg_paid AS (
    SELECT AVG(total_paid) AS avg_total_paid FROM subscriber_payments
)
SELECT c.customer_id,
       c.full_name,
       c.churn_risk_score,
       sp.total_paid
FROM {catalog}.{schema}.tc_customers c
JOIN subscriber_payments sp ON c.customer_id = sp.customer_id
CROSS JOIN avg_paid ap
WHERE c.status = 'A'
  AND c.churn_risk_score > 70
  AND sp.total_paid      > ap.avg_total_paid
""",

    "Q14 — What is the average ticket resolution time (in days) by type and priority?": f"""
SELECT
  CASE tc_tickets.ticket_type
    WHEN 'TEC' THEN 'Technical'
    WHEN 'BIL' THEN 'Billing'
    WHEN 'SVC' THEN 'Service Change'
    WHEN 'GEN' THEN 'General Inquiry'
  END AS ticket_type_name,
  CASE tc_tickets.priority
    WHEN 1 THEN 'Critical'
    WHEN 2 THEN 'High'
    WHEN 3 THEN 'Medium'
    WHEN 4 THEN 'Low'
  END AS priority_name,
  ROUND(AVG(DATEDIFF(tc_tickets.resolved_date, tc_tickets.created_date)), 2) AS avg_resolution_days
FROM
  {catalog}.{schema}.`tc_tickets`
WHERE
  tc_tickets.resolved_date IS NOT NULL
  AND tc_tickets.created_date IS NOT NULL
GROUP BY
  ticket_type_name,
  priority_name
ORDER BY
  ticket_type_name,
  priority_name;
""",

    "Q15 — What is the total monthly revenue at risk if all high-churn-risk active subscribers churned?": f"""
SELECT COUNT(*)                        AS high_risk_active_subscribers,
       ROUND(SUM(monthly_spend), 2)    AS monthly_revenue_at_risk
FROM {catalog}.{schema}.tc_customers
WHERE status = 'A'
  AND churn_risk_score > 70
""",
}

for title, sql in GENIE_QUERIES.items():
    print(f"{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}")
    print(sql.strip())
    print()
