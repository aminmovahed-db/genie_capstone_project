# NorthWave Telecom — Genie Space Capstone Project

A capstone project (est. ~8-10 hrs) for building, tuning, and documenting a production-grade [Databricks Genie Space](https://docs.databricks.com/en/genie/index.html) using a realistic telecom subscriber dataset.

## Repository Files

| File | Description |
| --- | --- |
| `genie_capstone_instructions.py` | Main instructions — project brief, column reference, decode tables, schedule, deliverables, and assessment rubric |
| `genie_space_capstone_dataset_setup.py` | Data-generation notebook — creates all 5 tables with synthetic data |
| `benchmark_sqls.py` | 15 ground-truth benchmark SQL queries for training/tuning Genie accuracy |
| `assessment_sqls.py` | 15 cross-validation queries for testing the tuned Space |
| `sample_report.py` | Delivery-report template — benchmark progression, tuning log, and lessons learnt sections |

All files are Databricks `.py` source format. Import them into a Databricks workspace to run.

## Getting Started

1. **Import** the `.py` notebooks into your Databricks workspace
2. **Read** `genie_capstone_instructions` for the full brief, column glossary, decode tables, and day-by-day guidance.
