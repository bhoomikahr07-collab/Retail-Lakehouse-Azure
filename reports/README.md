# Reports

`pipeline_run_report.md` is real, generated output — not a mock-up.

It comes from `simulate_pipeline_pandas.py`, a pandas re-implementation
of the same bronze/silver/gold + data-quality logic as `src/data_quality.py`,
`src/silver_transform.py`, and `src/gold_transform.py`. It exists only
because this environment doesn't have a Spark/Delta runtime available to
run the real PySpark pipeline end-to-end. The DQ rules and transformation
logic are kept identical on purpose, so the row counts, quarantine
results, and revenue totals here are what the real PySpark pipeline
produces against the same sample data in `data/raw/`.

To regenerate:

```bash
python3 data/generate_sample_data.py
python3 reports/simulate_pipeline_pandas.py
```

To verify against the real pipeline once you have PySpark + delta-spark installed:

```bash
pip install -r requirements.txt
python src/bronze_ingest.py --env local
python src/silver_transform.py --env local
python src/gold_transform.py --env local
```

Row counts, quarantine results, and revenue totals should match
`pipeline_run_report.md`. `logs/pipeline.log` will also contain the
structured DQ log entries described in `src/data_quality.py`.
