# From Messy CSVs to Queryable Parquet

Companion code for an AWS Glue / PySpark / S3 / Athena reference architecture article.
The local demo is runnable without AWS. **No AWS Glue deployment or benchmark is claimed.**

## Requirements
- Python 3.11
- Java 17 (JAVA_HOME configured)

## Run locally

```bash
python -m venv .venv
# macOS/Linux:
source .venv/bin/activate
# Windows PowerShell:
# .venv\Scripts\Activate.ps1
pip install -r requirements.txt
python src/run_local.py
pytest -q
```

Expected logical results from the supplied synthetic input:

| Metric | Expected |
|---|---:|
| Raw records | 5 |
| Invalid records | 0 |
| Duplicate records | 1 |
| Curated records | 4 |
| Completed payment total | GBP 150.00 |

These are **expected** values, not recorded execution results.

## Files
- `data/transactions.csv`: synthetic input
- `src/pipeline.py`: reusable PySpark validation and transformation
- `src/run_local.py`: local execution and Parquet output
- `src/glue_job.py`: example AWS Glue entry point (not deployed/tested)
- `tests/test_pipeline.py`: local tests

## AWS deployment outline
Upload `transactions.csv` to the S3 raw prefix. Package `pipeline.py` as a Glue extra Python file (`--extra-py-files`) and upload `glue_job.py` as the job script. Set job parameters `--SOURCE_PATH` and `--TARGET_PATH` to the respective S3 prefixes. Configure the Glue job role with appropriate S3 permissions, run the job, then catalog the Parquet output using a crawler. This outline is not a tested deployment guide.

## Scope and caveats
This is a full-refresh demonstration. A production pipeline needs deterministic handling of conflicting duplicates, quarantine of rejected rows, monitoring, access controls, and safe publication of curated data. The sample rejects conflicting duplicates and fails when invalid rows exceed 5%, but drops invalid rows below that threshold. The sample dataset has no invalid rows.
