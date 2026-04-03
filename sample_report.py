# Databricks notebook source
# MAGIC %md
# MAGIC # NorthWave Telecom — Sample Delivery Report (Capstone)
# MAGIC
# MAGIC **Purpose:** This notebook is an **illustrative example** tuning log, key findings, lessons learnt, and handover recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC ___
# MAGIC ## Tuning Log
# MAGIC The table below shows a **plausible learning curve** (not your actual data). Benchmark progress after major tuning steps (sample scores)
# MAGIC
# MAGIC | Run | What changed | Score (pass / 10) | % | Notes (sample) |
# MAGIC |------|----------------|------------------:|--:|----------------|
# MAGIC | **0 — Baseline** | Space created, tables attached, minimal / empty Knowledge Store | 4 | 40% | Failures on revenue, fiscal year, `status` codes, SLA, tier labels |
# MAGIC | **1** | Unity Catalog table + column comments (coded columns, unlimited = 0) | 6 | 60% | Revenue and SLA still wrong; churn “high risk” threshold vague |
# MAGIC | **2** | Genie **Instructions**: active = `A`, revenue rules, FY Apr–Mar, open ticket = `resolved_date IS NULL`, churn > 70, SLA days by priority | 7 | 70% | Q7 net revenue (ADJ), Q8 “high payment” definition, Q9 grouping edge cases |
# MAGIC | **3** | **5+ certified example SQLs** (from benchmark_sqls, edited for your catalog/schema) | 9 | 90% | meets **≥ 85%** bar; optional push to 10/10 with one more certified example  |
# MAGIC | **4 - Cross-validation** | validation on assessment sqls | **5** | **71%** | a lower score indicates either new patterns in assessment sql, space not tuned optimally or overfitting |
# MAGIC
# MAGIC ### Final Benchmark Question Result - run 3
# MAGIC
# MAGIC | Q# | Question (short) | Final result (sample) |
# MAGIC |----|------------------|------------------------|
# MAGIC | 1 | Region / churn risk | Pass |
# MAGIC | 2 | Payment failure rate | Pass |
# MAGIC | 3 | Data by tier | Pass |
# MAGIC | 4 | SLA % | Pass |
# MAGIC | 5 | Acq channel 12 mo | Pass |
# MAGIC | 6 | Churn trend 6 mo | Pass |
# MAGIC | 7 | Net monthly revenue | Pass |
# MAGIC | 8 | High risk + high pay | Pass |
# MAGIC | 9 | Avg resolution by type/priority | **Fail (sample)** — e.g. wrong handling of ticket date filters until a Q9-style example is certified; your run may Pass |
# MAGIC | 10 | Revenue at risk | Pass (after clarifying high-churn + active + `monthly_spend` in UC comments) |
# MAGIC
# MAGIC ### Assessment Question Result - run 4
# MAGIC
# MAGIC | A# | Question (short) | Result | Root cause of failure (if any) | Suggested fix (not applied) |
# MAGIC |----|------------------|--------|-------------------------------|----------------------------|
# MAGIC | A1 | Channel with highest median churn risk | Pass | — | — |
# MAGIC | A2 | Subscribers spending 20%+ above plan fee | **Fail** | Genie compared `monthly_spend` to `monthly_fee` directly instead of multiplying by 1.2 | Add an instruction: “spending above plan fee” means `monthly_spend > monthly_fee * 1.2` when a percentage is stated |
# MAGIC | A3 | Voice minutes by region and direction | Pass | — | — |
# MAGIC | A4 | Active subscribers with 3+ billing tickets in 12 mo | Pass | — | — |
# MAGIC | A5 | Unlimited data plans — active subscriber count | **Fail** | Genie filtered `data_gb IS NULL` instead of `data_gb = 0` | Reinforce in UC column comment and instructions that `data_gb = 0` means **unlimited**, not zero or NULL |
# MAGIC | A6 | Regional top spenders (rank 1 per region) | Pass | — | — |
# MAGIC | A7 | FY adjustment amount (ADJ, successful only) | Pass | — | — |
# MAGIC
# MAGIC **Sample score:** 5 / 7 (71%)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Key Findings
# MAGIC
# MAGIC - Which configuration can lead to the biggest improvement of score?
# MAGIC - When do you use column comments, entity matching, sql expression and why?
# MAGIC - When do you use text instruction?
# MAGIC - etc
# MAGIC
# MAGIC An example below:
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
# MAGIC
# MAGIC | Layer | What we added (sample) | Rationale |
# MAGIC |-------|-------------------------|-----------|
# MAGIC | **Unity Catalog** | Table comments describing domain; column comments on every coded field using the instruction notebook’s **Column Reference** and **Decode Table** | UC is durable, travels with the data, and is the first line of defense for codes (`usage_type`, regions, etc.) |
# MAGIC | **Genie Instructions** | Single source of truth for: revenue definition, active definition, open ticket, churn threshold, SLA calendar days, FY definition, unlimited allowances, treatment of ADJ in “net” vs “collected” revenue | Short, imperative rules reduce hallucination when questions span multiple tables |
# MAGIC | **Certified example SQL** | Five patterns prioritized: (1) active count, (2) FY revenue with `CASE` + date window, (3) open tickets, (4) SLA % with `DATEDIFF` + priority map, (5) net monthly trend with ADJ | Matches the most failure-prone benchmark questions; copied from [benchmark_sqls]($./benchmark_sqls) and validated in SQL editor first |
# MAGIC
# MAGIC We avoided duplicating long prose in both UC and Instructions: **codes and meanings → UC**; **calculations and filters → Instructions**; **query shape → examples**.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Lesson Learnt
# MAGIC
# MAGIC - Do you follow the best practice doc? What works and what doesn't work?
# MAGIC - What do you do when it doesn't work?
# MAGIC - What are the different types of errors you encountered, and what is the best way to address them?
# MAGIC
# MAGIC An example below:
# MAGIC
# MAGIC ### Top 3 failure patterns (sample)
# MAGIC
# MAGIC | # | Pattern | Root cause (sample) | Fix applied |
# MAGIC |---|---------|---------------------|-------------|
# MAGIC | 1 | **Revenue / fiscal year wrong** | Model defaulted to calendar year and summed all payment rows | Instructions: define FY Apr–Mar; revenue = `pmt_status = 'successful'` MRC+OTC minus refunds on MRC+OTC; date filter on `payment_date`. Optional: paste the revenue SQL as a certified example. |
# MAGIC | 2 | **“Open tickets” over/under-counted** | Assumed a status column or “not closed” without matching schema | Instructions: open = `resolved_date IS NULL`. UC comment on `resolved_date`. |
# MAGIC | 3 | **Tier / usage questions mis-joined** | Weak join path customer → plan; `usage_type` for data not set to `'D'` | UC on `usage_type`; example SQL for Q3-style join and `WHERE usage_type = 'D'`; confirm `plan_id` on `tc_customers`. |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Handover Recommendations
# MAGIC
# MAGIC - What are the remaining open items?
# MAGIC - How would you productionize the solution?
