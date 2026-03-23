# NorthWave Telecom — Genie Space Capstone Project

Capstone project for building, tuning, and documenting a production-grade [Databricks Genie Space](https://docs.databricks.com/en/genie/index.html) using a realistic telecom subscriber dataset.

## Repository Files

| File | Description |
| --- | --- |
| `genie_capstone_instructions.py` | Main instructions — project brief, column reference, decode tables, implementation steps, deliverables, and assessment rubric |
| `genie_space_capstone_dataset_setup.py` | Data-generation notebook — creates all 5 tables with synthetic data |
| `benchmark_sqls.py` | the notebook contains benchmark questions with ground truth SQLs for training/tuning Genie space - MUST score >85% |
| `assessment_sqls.py` | 15 harder assessment questions that are only for cross validating tuned Genie space - DO NOT tune with these queries. |
| `sample_report.py` | Delivery-report template — Tuning log, discovery findings, lessons learnt, and handover recommendations |

All files are Databricks `.py` source format. Import them into a Databricks workspace to run.

## Getting Started

1. **Import** the `.py` notebooks into your Databricks workspace.
2. **Follow** `genie_capstone_instructions` for the full brief and step-by-step guidance.

## References

| Resource | Link |
| --- | --- |
| AI/BI for Data Analysts course | [Partner Academy](https://partner-academy.databricks.com/learn/courses/3707/aibi-for-data-analysts) |
| Curate an effective Genie Space | [Databricks Docs](https://docs.databricks.com/en/genie/best-practices.html) |
| Genie Space overview | [Databricks Docs](https://docs.databricks.com/en/genie/index.html) |
