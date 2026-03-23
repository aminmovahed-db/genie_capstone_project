# Databricks notebook source
# MAGIC %md
# MAGIC # NorthWave Telecom — **Assessment** SQL (harder benchmark)
# MAGIC
# MAGIC Use this notebook **after** you have tuned your Genie Space on the standard [benchmark_sqls]($./benchmark_sqls) suite. The questions below are deliberately more difficult: they combine multiple tables, coded fields, implicit business rules (fiscal year, revenue definitions, SLA days, unlimited allowances), window functions, and `HAVING` / subquery logic.
# MAGIC

# COMMAND ----------

# MAGIC %md ####Execute next cell to generate widgets

# COMMAND ----------

dbutils.widgets.text("catalog", "your_catalog", "Catalog")
dbutils.widgets.text("schema", "genie_capstone", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Assessment suite — questions and ground-truth SQL
# MAGIC
# MAGIC Set **Catalog** and **Schema** widgets, run the then copy and paste the SQL (with your catalog/schema substituted) for the Genie Space
# MAGIC
# MAGIC Execute cell below tp print all 15 in a clean format you can add directly into your Genie Space as benchmark questions

# COMMAND ----------

ASSESSMENT_QUERIES = {
    "A1 — For active subscribers only, which acquisition channel has the highest median churn risk score?": f"""
SELECT CASE acq_channel
           WHEN 'ONL' THEN 'Online'
           WHEN 'RET' THEN 'Retail'
           WHEN 'REF' THEN 'Referral'
           WHEN 'TEL' THEN 'Telesales'
       END      AS channel_name,
       percentile_approx(churn_risk_score, 0.5) AS median_risk
FROM {catalog}.{schema}.tc_customers
WHERE status = 'A'
GROUP BY acq_channel
ORDER BY median_risk DESC, acq_channel
LIMIT 1
""",

    "A2 — How many subscribers are on annual or biennial contract plans, broken down by plan tier (Basic through Enterprise)?": f"""
SELECT CASE p.plan_tier
           WHEN 1 THEN 'Basic'
           WHEN 2 THEN 'Standard'
           WHEN 3 THEN 'Premium'
           WHEN 4 THEN 'Enterprise'
       END      AS tier_name,
       COUNT(*) AS subscriber_count
FROM {catalog}.{schema}.tc_customers c
JOIN {catalog}.{schema}.tc_plans p ON c.plan_id = p.plan_id
WHERE p.contract_type IN ('A', 'B')
GROUP BY p.plan_tier
ORDER BY p.plan_tier
""",

    "A3 — What is total roaming data usage in gigabytes (roaming usage events only)?": f"""
SELECT ROUND(SUM(quantity) / 1024, 2) AS total_roaming_gb
FROM {catalog}.{schema}.tc_usage
WHERE usage_type = 'R'
""",

    "A4 — How many active subscribers have monthly_spend more than 20 percent above their plan catalogue monthly_fee?": f"""
SELECT COUNT(*) AS active_subscribers_over_plan_fee
FROM {catalog}.{schema}.tc_customers c
JOIN {catalog}.{schema}.tc_plans p ON c.plan_id = p.plan_id
WHERE c.status = 'A'
  AND c.monthly_spend > p.monthly_fee * 1.2
""",

    "A5 — For open support tickets with critical priority only, how many are there per customer region?": f"""
SELECT  CASE c.`region`
      WHEN 'NW' THEN 'Northwest'
      WHEN 'SW' THEN 'Southwest'
      WHEN 'NE' THEN 'Northeast'
      WHEN 'SE' THEN 'Southeast'
      WHEN 'MW' THEN 'Midwest'
      ELSE 'Other'
      END AS region_name,
       COUNT(*) AS open_critical_tickets
FROM {catalog}.{schema}.tc_tickets t
JOIN {catalog}.{schema}.tc_customers c ON t.customer_id = c.customer_id
WHERE t.resolved_date IS NULL
  AND t.priority = 1
GROUP BY c.region
ORDER BY open_critical_tickets DESC, c.region
""",

    "A6 — Among resolved tickets only, what is the average customer satisfaction score when the ticket missed its SLA target (resolution took longer than allowed for that priority)?": f"""
SELECT ROUND(AVG(t.satisfaction_score), 2) AS avg_satisfaction_sla_missed
FROM {catalog}.{schema}.tc_tickets t
WHERE t.resolved_date IS NOT NULL
  AND t.satisfaction_score IS NOT NULL
  AND DATEDIFF(t.resolved_date, t.created_date) >
      CASE t.priority
          WHEN 1 THEN 1
          WHEN 2 THEN 2
          WHEN 3 THEN 3
          WHEN 4 THEN 5
      END
""",

    "A7 — What are the top three regions by count of suspended subscribers?": f"""
SELECT CASE c.`region`
      WHEN 'NW' THEN 'Northwest'
      WHEN 'SW' THEN 'Southwest'
      WHEN 'NE' THEN 'Northeast'
      WHEN 'SE' THEN 'Southeast'
      WHEN 'MW' THEN 'Midwest'
      ELSE 'Other'
      END AS region_name,
       COUNT(*) AS suspended_subscribers
FROM {catalog}.{schema}.tc_customers
WHERE status = 'S'
GROUP BY region_name
ORDER BY suspended_subscribers DESC, region
LIMIT 3
""",

    "A8 — How many active subscribers opened at least three billing tickets in the last 12 months?": f"""
SELECT COUNT(*) AS active_subscribers_with_3plus_billing_tickets
FROM (
    SELECT c.customer_id
    FROM {catalog}.{schema}.tc_customers c
    JOIN {catalog}.{schema}.tc_tickets t ON c.customer_id = t.customer_id
    WHERE c.status = 'A'
      AND t.ticket_type = 'BIL'
      AND t.created_date >= ADD_MONTHS(CURRENT_DATE(), -12)
    GROUP BY c.customer_id
    HAVING COUNT(*) >= 3
) x
""",

    "A9 — For voice usage only, what are total minutes by region and by call direction (inbound vs outbound)?": f"""
SELECT CASE c.`region`
      WHEN 'NW' THEN 'Northwest'
      WHEN 'SW' THEN 'Southwest'
      WHEN 'NE' THEN 'Northeast'
      WHEN 'SE' THEN 'Southeast'
      WHEN 'MW' THEN 'Midwest'
      ELSE 'Other'
      END AS region_name,
       CASE u.direction
           WHEN 'I' THEN 'Inbound'
           WHEN 'O' THEN 'Outbound'
       END      AS call_direction,
       ROUND(SUM(u.quantity), 0) AS total_minutes
FROM {catalog}.{schema}.tc_usage u
JOIN {catalog}.{schema}.tc_customers c ON u.customer_id = c.customer_id
WHERE u.usage_type = 'V'
  AND c.region IS NOT NULL
  AND u.direction IS NOT NULL
GROUP BY c.region, call_direction
ORDER BY c.region, call_direction
""",

    "A10 — For plans with unlimited mobile data (data allowance flag is zero meaning unlimited), how many active subscribers are on each plan?": f"""
SELECT 
       p.plan_name,
       COUNT(c.customer_id) AS active_subscribers
FROM {catalog}.{schema}.tc_plans p
LEFT JOIN {catalog}.{schema}.tc_customers c
       ON p.plan_id = c.plan_id AND c.status = 'A'
WHERE p.data_gb = 0
GROUP BY  p.plan_name
ORDER BY p.plan_name
""",

    "A11 — What percentage of all payment rows are successful (pmt_status S), counting every payment type?": f"""
SELECT ROUND(100.0 * SUM(CASE WHEN pmt_status = 'S' THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate_pct
FROM {catalog}.{schema}.tc_payments
""",

    "A12 — Among churned customers only, rank plans by average lifetime_months (highest average tenure before churn first).": f"""
SELECT p.plan_name,
       ROUND(AVG(c.lifetime_months), 2) AS avg_lifetime_months_before_churn
FROM {catalog}.{schema}.tc_customers c
JOIN {catalog}.{schema}.tc_plans p ON c.plan_id = p.plan_id
WHERE c.status = 'C'
GROUP BY p.plan_id, p.plan_name
ORDER BY avg_lifetime_months_before_churn DESC
""",

    "A13 — For active subscribers with churn risk score at least 80 and lifetime at least 24 months, how many are in each plan tier?": f"""
SELECT CASE p.plan_tier
           WHEN 1 THEN 'Basic'
           WHEN 2 THEN 'Standard'
           WHEN 3 THEN 'Premium'
           WHEN 4 THEN 'Enterprise'
       END      AS tier_name,
       COUNT(*) AS subscriber_count
FROM {catalog}.{schema}.tc_customers c
JOIN {catalog}.{schema}.tc_plans p ON c.plan_id = p.plan_id
WHERE c.status = 'A'
  AND c.churn_risk_score >= 80
  AND c.lifetime_months >= 24
GROUP BY p.plan_tier
ORDER BY p.plan_tier
""",

    "A14 — Among active subscribers, how many are the top monthly spender (rank 1) within their own region?": f"""
WITH ranked AS (
    SELECT customer_id,
           region,
           ROW_NUMBER() OVER (PARTITION BY region ORDER BY monthly_spend DESC) AS rnk
    FROM {catalog}.{schema}.tc_customers
    WHERE status = 'A'
)
SELECT COUNT(*) AS regional_top_spenders
FROM ranked
WHERE rnk = 1
""",

    "A15 — In the current fiscal year, what is the sum of all adjustment payment amounts (payment_type ADJ, successful status only)?": f"""
SELECT ROUND(SUM(amount), 2) AS total_adjustment_amount_fy
FROM {catalog}.{schema}.tc_payments
WHERE payment_type = 'ADJ'
  AND pmt_status = 'S'
  AND payment_date >= '2025-04-01'
  AND payment_date < '2026-04-01'
""",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Copy-paste block (all assessment SQL)

# COMMAND ----------

for title, sql in ASSESSMENT_QUERIES.items():
    print(f"{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}")
    print(sql.strip())
    print()
