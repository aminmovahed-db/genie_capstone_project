# NorthWave Telecom — Genie Space Capstone Project

A 5-day capstone for building, tuning, and documenting a production-grade [Databricks Genie Space](https://docs.databricks.com/en/genie/index.html) using a realistic telecom subscriber dataset.

## Repository Files

| File | Description |
| --- | --- |
| `genie_capstone_instructions.py` | Main instructions — project brief, column reference, decode tables, schedule, deliverables, and assessment rubric |
| `genie_space_capstone_dataset_setup.py` | Data-generation notebook — creates all 5 tables with synthetic data |
| `benchmark_sqls.py` | 15 gold-standard SQL queries for benchmarking Genie accuracy |
| `assessment_sqls.py` | 15 harder assessment questions + expected SQL for stress-testing a tuned Space |
| `sample_report.py` | Delivery-report template — benchmark progression, tuning log, and monitoring sections |

All files are Databricks `.py` source format. Import them into a Databricks workspace to run.

## Getting Started

1. **Import** the `.py` notebooks into your Databricks workspace.
2. **Run** `genie_space_capstone_dataset_setup` — set the `catalog` and `schema` widgets, then Run All to provision the tables.
3. **Read** `genie_capstone_instructions` for the full brief, column glossary, decode tables, and day-by-day guidance.
4. **Run** `benchmark_sqls` (same `catalog`/`schema` widgets) to execute the 15 benchmark queries.
5. **Run** `assessment_sqls` (optional, same widgets) — 15 harder questions to stress-test your tuned Space.
6. **Use** `sample_report` as a template for your delivery report and tuning log.

## References

| Resource | Link |
| --- | --- |
| AI/BI for Data Analysts course | [Partner Academy](https://partner-academy.databricks.com/learn/courses/3707/aibi-for-data-analysts) |
| Curate an effective Genie Space | [Databricks Docs](https://docs.databricks.com/en/genie/best-practices.html) |
| Genie Space overview | [Databricks Docs](https://docs.databricks.com/en/genie/index.html) |
