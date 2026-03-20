# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NorthWave Telecom — Sample Delivery Report (Capstone)
# MAGIC 
# MAGIC **Purpose:** This notebook is an **illustrative example** of how to document your capstone: reasoning, tuning steps, benchmark scores after each iteration, and where to paste **Genie conversation / run links** from your workspace. Replace all placeholders (`<…>`, fictional IDs, and example URLs) with your real Space, links, and scores.
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Genie Space (sample metadata)
# MAGIC 
# MAGIC | Field | Sample value |
# MAGIC |-------|----------------|
# MAGIC | **Title** | NorthWave Telecom — Subscriber Analytics |
# MAGIC | **Description** | Enables customer insights analysts to explore subscriber status, usage trends, payment performance, and support ticket resolution for the NorthWave telecom business. |
# MAGIC | **Tables** | `tc_plans`, `tc_customers`, `tc_usage`, `tc_tickets`, `tc_payments` (all 5 in `{catalog}.{schema}`) |
# MAGIC | **Persona** | Customer Insights Analyst at NorthWave Telecom |
# MAGIC 
# MAGIC **How to capture Genie links:** In the Databricks UI, open your Genie Space → run a benchmark question → open the conversation (or Monitoring / history, depending on workspace version) → copy the URL from the browser. Paste one representative link per tuning round below so instructors can reproduce your journey.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Benchmark progress after each tuning step (sample scores)
# MAGIC 
# MAGIC Target from the capstone brief: **≥ 13 / 15 (87%)** correct vs. ground truth in [benchmark_sqls]($./benchmark_sqls).
# MAGIC 
# MAGIC The table below shows a **plausible learning curve** (not your actual data). Your rows should reflect real pass/fail checks after each change.
# MAGIC 
# MAGIC | Step | What changed | Score (pass / 15) | % | Notes (sample) |
# MAGIC |------|----------------|------------------:|--:|----------------|
# MAGIC | **0 — Baseline** | Space created, tables attached, minimal / empty Knowledge Store | 7 | 47% | Failures on revenue, fiscal year, `status` codes, open tickets, tier labels |
# MAGIC | **1** | Unity Catalog table + column comments (coded columns, unlimited = 0) | 10 | 67% | Revenue and SLA still wrong; churn “high risk” threshold vague |
# MAGIC | **2** | Genie **Instructions**: active = `A`, revenue rules, FY Apr–Mar, open ticket = `resolved_date IS NULL`, churn > 70, SLA days by priority | 12 | 80% | Q12 net revenue (ADJ), Q13 “high payment” definition, Q14 grouping edge cases |
# MAGIC | **3** | **5+ certified example SQLs** (from benchmark_sqls, edited for your catalog/schema) | 13 | 87% | Still one miss on Q8 phrasing / MB→GB until usage_type called out in instructions |
# MAGIC | **4** | Instruction tweak (e.g. “mobile data” = `usage_type = 'D'`) + optional entity / synonym | **14** | **93%** | Meets **≥ 85%** bar; optional push to 15/15 with one more certified example |
# MAGIC 
# MAGIC **Sample Genie conversation URLs (fictional — replace with yours):**
# MAGIC 
# MAGIC | Step | Representative run / conversation (placeholder) |
# MAGIC |------|---------------------------------------------------|
# MAGIC | After step 0 | `https://<your-workspace-host>/genie/spaces/01JG8SAMPLE0/conversations/01JG8RUNBASE` |
# MAGIC | After step 1 | `https://<your-workspace-host>/genie/spaces/01JG8SAMPLE0/conversations/01JG8RUNUCMETA` |
# MAGIC | After step 2 | `https://<your-workspace-host>/genie/spaces/01JG8SAMPLE0/conversations/01JG8RUNINSTR` |
# MAGIC | After step 3 | `https://<your-workspace-host>/genie/spaces/01JG8SAMPLE0/conversations/01JG8RUNEXSQL` |
# MAGIC | Final verification | `https://<your-workspace-host>/genie/spaces/01JG8SAMPLE0/conversations/01JG8RUNFINAL` |
# MAGIC 
# MAGIC *If your workspace uses a different URL pattern (e.g. thread id query parameters), paste whatever the UI provides — the goal is reproducibility for the grader.*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Discovery
# MAGIC 
# MAGIC ### Questions the persona needs (benchmark suite)
# MAGIC 
# MAGIC The 15 questions in [genie_capstone_instructions]($./genie_capstone_instructions) define the analyst workflow: headcount and segments, revenue and payments, support SLAs, usage by tier, acquisition and churn trends, and combined “risk + value” views.
# MAGIC 
# MAGIC ### Coded columns that caused the most initial failures (sample reasoning)
# MAGIC 
# MAGIC | Area | Why Genie struggled | What the analyst actually means |
# MAGIC |------|---------------------|----------------------------------|
# MAGIC | **Subscriber status** | Single-letter codes (`A`, `S`, `C`, `T`) without UC context | “Active subscribers” = `status = 'A'` only |
# MAGIC | **Revenue** | Summing all `amount` or including ADJ in “collected” revenue | Per README / benchmark: successful MRC+OTC net of refunds; fiscal year **Apr–Mar**; ADJ handled separately for “net” trends |
# MAGIC | **Plans** | `plan_tier` as integers; `data_gb` / `voice_min` = 0 meaning **unlimited** | Need tier names and explicit unlimited rule in catalog or instructions |
# MAGIC | **Tickets** | “Open” interpreted as status field | Ground truth: **open = `resolved_date IS NULL`** |
# MAGIC | **SLA** | Priority treated as ordinal without day targets | Map priority 1→1d, 2→2d, 3→3d, 4→5d |
# MAGIC | **Churn risk** | Fuzzy “high risk” | Align to **`churn_risk_score > 70`** for benchmark consistency |
# MAGIC 
# MAGIC This discovery phase maps directly to Knowledge Store work: **metadata** for codes, **instructions** for business rules, **example SQL** for non-obvious SQL shape (joins, CTEs, `CASE` revenue logic).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Design decisions
# MAGIC 
# MAGIC | Layer | What we added (sample) | Rationale |
# MAGIC |-------|-------------------------|-----------|
# MAGIC | **Unity Catalog** | Table comments describing domain; column comments on every coded field using the instruction notebook’s **Column Reference** and **Decode Table** | UC is durable, travels with the data, and is the first line of defense for codes (`usage_type`, `pmt_status`, regions, etc.) |
# MAGIC | **Genie Instructions** | Single source of truth for: revenue definition, active definition, open ticket, churn threshold, SLA calendar days, FY definition, unlimited allowances, treatment of ADJ in “net” vs “collected” revenue | Short, imperative rules reduce hallucination when questions span multiple tables |
# MAGIC | **Certified example SQL** | Five patterns prioritized: (1) active count, (2) FY revenue with `CASE` + date window, (3) open tickets, (4) SLA % with `DATEDIFF` + priority map, (5) net monthly trend with ADJ | Matches the most failure-prone benchmark questions; copied from [benchmark_sqls]($./benchmark_sqls) and validated in SQL editor first |
# MAGIC 
# MAGIC We avoided duplicating long prose in both UC and Instructions: **codes and meanings → UC**; **calculations and filters → Instructions**; **query shape → examples**.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Tuning — iterations, failure patterns, fixes
# MAGIC 
# MAGIC **Iterations (sample narrative):** four documented rounds (baseline → UC → instructions → examples → small polish), aligned with the score table above.
# MAGIC 
# MAGIC ### Top 3 failure patterns (sample)
# MAGIC 
# MAGIC | # | Pattern | Root cause (sample) | Fix applied |
# MAGIC |---|---------|---------------------|-------------|
# MAGIC | 1 | **Revenue / fiscal year wrong** | Model defaulted to calendar year and summed all payment rows | Instructions: define FY Apr–Mar; revenue = `pmt_status = 'S'` MRC+OTC minus refunds on MRC+OTC; date filter on `payment_date`. Optional: paste Q2 SQL as certified example. |
# MAGIC | 2 | **“Open tickets” over/under-counted** | Assumed a status column or “not closed” without matching schema | Instructions: open = `resolved_date IS NULL`. UC comment on `resolved_date`. |
# MAGIC | 3 | **Tier / usage questions mis-joined** | Weak join path customer → plan; `usage_type` for data not set to `'D'` | UC on `usage_type`; example SQL for Q8-style join and `WHERE usage_type = 'D'`; confirm `plan_id` on `tc_customers`. |
# MAGIC 
# MAGIC ### Per-question log — final sample outcome (illustrative)
# MAGIC 
# MAGIC | Q# | Question (short) | Final result (sample) |
# MAGIC |----|------------------|------------------------|
# MAGIC | 1 | Active subscribers | Pass |
# MAGIC | 2 | FY revenue | Pass |
# MAGIC | 3 | Open tickets | Pass |
# MAGIC | 4 | Plans and fees | Pass |
# MAGIC | 5 | Avg fee by tier | Pass |
# MAGIC | 6 | Region / churn risk | Pass |
# MAGIC | 7 | Payment failure rate | Pass |
# MAGIC | 8 | Data by tier | Pass |
# MAGIC | 9 | SLA % | Pass |
# MAGIC | 10 | Acq channel 12 mo | Pass |
# MAGIC | 11 | Churn trend 6 mo | Pass |
# MAGIC | 12 | Net monthly revenue | Pass |
# MAGIC | 13 | High risk + high pay | Pass |
# MAGIC | 14 | Avg resolution by type/priority | **Fail (sample)** — e.g. wrong handling of ticket date filters until a Q14-style example is certified; your run may Pass |
# MAGIC | 15 | Revenue at risk | Pass (after clarifying high-churn + active + `monthly_spend` in UC comments) |
# MAGIC 
# MAGIC *Your notebook should mark each question Pass/Fail after each run, not only at the end.*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC Link example for a monitored conversation: `https://<your-workspace-host>/genie/spaces/01JG8SAMPLE0/conversations/01JG8RUNMON`
# MAGIC 
# MAGIC *This sample report is a template for the **1-page delivery report** + **tuning documentation** described in the assessment rubric in [genie_capstone_instructions]($./genie_capstone_instructions). Condense the prose into a one-pager for submission if required; keep the full notebook as your working log.*
