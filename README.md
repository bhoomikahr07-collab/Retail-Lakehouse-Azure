# Retail Data Lakehouse — Databricks & Azure

A medallion-architecture (bronze/silver/gold) lakehouse pipeline that turns
raw retail transactional data into analytics-ready gold tables on
Databricks, orchestrated by Azure Data Factory, stored in Delta Lake, and
modeled as a snowflake schema in Azure Synapse Analytics for merchandising
and supply-chain reporting.

```
┌──────────────┐   ┌─────────────────────────────────────────────┐   ┌───────────────┐
│ Raw sources   │   │              Databricks (PySpark)             │   │ Azure Synapse │
│ POS/ERP CSV   │──▶│  Bronze ─▶ Silver (DQ gate) ─▶ Gold           │──▶│  Snowflake    │
│ orchestrated  │   │  (Delta Lake, ACID, versioned at every layer) │   │  schema       │
│ by Azure Data │   └─────────────────────────────────────────────┘   └───────────────┘
│ Factory       │                                                              │
└──────────────┘                                                              ▼
                                                                        Power BI / reporting
```

## What this project demonstrates

- **Medallion architecture on Databricks** — bronze (raw, append-only) → silver (cleaned, conformed, DQ-gated) → gold (analytics-ready, aggregated) using PySpark (`src/bronze_ingest.py`, `src/silver_transform.py`, `src/gold_transform.py`)
- **Delta Lake** — every layer is written as Delta so it's ACID-compliant and versioned; silver/gold writes use `MERGE` for idempotent upserts, and bad rows are quarantined rather than silently dropped
- **Azure Data Factory orchestration** — a pipeline definition chaining the three Databricks notebook activities with dependency conditions (`adf/pipeline_retail_lakehouse.json`)
- **Snowflake schema in Azure Synapse Analytics** — normalized dimensions (`dim_product → dim_category → dim_department`, `dim_store → dim_region`, `dim_supplier`) to support merchandising and supply-chain reporting (`synapse/schema.sql`)
- **Automated data quality checks with logging/monitoring** — null checks, schema enforcement, and duplicate detection run at the silver gate, with structured logging to a run log (`src/data_quality.py`)

## Repo structure

```
retail-lakehouse-azure/
├── config/
│   └── config.yaml
├── data/
│   └── generate_sample_data.py
├── src/
│   ├── schema.py
│   ├── utils.py
│   ├── data_quality.py
│   ├── bronze_ingest.py
│   ├── silver_transform.py
│   └── gold_transform.py
├── databricks/
│   └── bronze_to_gold_job.json
├── adf/
│   └── pipeline_retail_lakehouse.json
├── synapse/
│   ├── schema.sql
│   └── load_gold_to_synapse.sql
├── reports/
│   ├── simulate_pipeline_pandas.py
│   ├── pipeline_run_report.md
│   └── README.md
└── tests/
    └── test_pipeline.py
```

## Running it locally

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python data/generate_sample_data.py
python src/bronze_ingest.py --env local
python src/silver_transform.py --env local
python src/gold_transform.py --env local
pytest tests/
```

## Deploying to Databricks + Azure

1. Upload source extracts to ADLS Gen2.
2. Import `src/*.py` as Databricks notebooks or workflow tasks.
3. Create a Databricks Job from `databricks/bronze_to_gold_job.json`.
4. Deploy `adf/pipeline_retail_lakehouse.json` in Azure Data Factory.
5. Run `synapse/schema.sql`, then schedule `synapse/load_gold_to_synapse.sql`.
6. Point Power BI at Synapse for reporting.

Bad rows are written to a quarantine Delta table with failure reasons rather than silently dropped. Bronze preserves raw history, silver performs cleansing and DQ gating, and gold provides reporting-ready fact and dimension tables.
