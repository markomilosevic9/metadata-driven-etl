<a href="#"><p align="left">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/python-dark.svg" width="50">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/postgressql-dark.svg" width="50">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/apachespark-dark.svg" width="50">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/docker-dark.svg" width="50">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/apacheairflow-dark.svg" width="50">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/pytest-dark.svg" width="50">

</p></a>

# Goal

A local batch data pipeline for fictional insurance policy records. It generates a synthetic sample data and uploads it to MinIO, validates records with PySpark using metadata-driven rules, builds a Kimball star schema in PostgreSQL with SCD1/SCD2 dimension handling, and persists data quality metrics that are displayed in a Grafana dashboard. Metabase is included for analytics, and the entire stack runs on Docker Compose.

<details>
<summary>Tree diagram of project structure</summary>
<pre>
├── airflow/
│   └── dags/
│       └── motor_policy_pipeline_dag.py      - Airflow DAG
├── config/
│   ├── pipeline_config.yaml                  - Spark, MinIO, and DB connection settings
│   ├── pipeline_metadata.json                - Source schema, validation rules, dimension registry, SCD config, derived field expressions, fact definition
│   └── spark-defaults.conf                   - Spark config defaults
├── grafana/
│   └── provisioning/
│       ├── dashboards/                       - DQ metrics dashboard definition
│       └── datasources/                      - Grafana datasource configuration
├── pipeline/
│   ├── core/
│   │   └── config_loader.py                  - YAML config loader with env variable resolution
│   ├── jobs/
│   │   ├── ingestion_runner.py               - Batch discovery, schema enforcement, field validation, valid/invalid split, MinIO write
│   │   └── run_pipeline.py                   - Spark job entry point; handles ingestion and dimension/fact pipeline stages
│   └── stages/
│       ├── dq_metrics.py                     - DQ metrics aggregation and persistence
│       ├── field_validator.py                - Metadata-driven Spark SQL validation expression generator
│       ├── schema_enforcer.py                - Applies source schema from metadata file to Spark DataFrames
│       ├── scd_and_warehouse.py              - SCD1/SCD2 processing, Parquet snapshots, JDBC upserts
│       └── warehouse_loader.py               - calendar dimension generation and fact surrogate key resolution and load
├── sql/
│   ├── metabase_db_init.sql                  - Metabase internal database initialisation
│   └── schema_init.sql                       - DDL: dimension/fact tables, ETL state tables, DQ metrics table, constraints, mart views
├── tests/
│   ├── conftest.py                           - Shared fixtures and database/MinIO connections
│   ├── test_post_pipeline_data_integrity.py  - Record structure, lineage columns, and error format validation checks
│   ├── test_post_pipeline_scd2.py            - SCD1/SCD2 versioning and correctness assertions
│   ├── test_post_pipeline_star_schema.py     - DWH state, table existence, and Parquet-PostgreSQL equivalence checks
│   ├── test_pre_infrastructure.py            - Input dataset checks and JSONL format validation
│   └── test_pre_metadata_integrity.py        - Metadata file structural validation
├── utils/
│   ├── logging_setup.py                      - Logging configuration
│   ├── minio_helper.py                       - MinIO client helpers
│   └── postgres_helper.py                    - PostgreSQL connection helpers
├── .env                                      - Env variables
├── docker-compose.yml                        - Local stack: MinIO, Spark, PostgreSQL, Airflow, Grafana, Metabase
├── Dockerfile.airflow                        - Custom Airflow image with Python dependencies
├── Dockerfile.spark                          - Custom Spark image with Hadoop S3A and JDBC JARs
├── generate_sample_data.py                   - Synthetic sample dataset generation and upload
├── Makefile                                  - Convenience commands for setup, execution, and testing
├── pytest.ini                                - Test marker configuration (pre/post-pipeline)
└── requirements.txt                          - Python dependencies
</pre>
</details>

## Important aspects of pipeline implementation

- Metadata-driven batch validation using Spark SQL expressions. Validation rules, SCD field classifications, deduplication keys, derived fields, and the fact table and measure definition live in metadata file, so many pipeline behavior changes require a edit of `config/pipeline_metadata.json`. 
- Kimball dimensional model with SCD1/SCD2 dimensions, natural-to-surrogate key resolution at fact load, and analytics-ready star schema mart views.
- Incremental and idempotent ETL behavior.
- Per-run data quality metrics with input/output reconciliation and a per-field error breakdown.
- Airflow orchestration with pre-pipeline data and configuration checks and post-pipeline assertions.


# Pipeline overview



Before triggering the DAG, running a `generate_sample_data.py` generates sample data and upload it to MinIO. It contains ~100K records (for demo purposes) spanning 2 batches: the 1st carries intentional field-level validation failures (~5% injected per failure category), and the 2nd simulates SCD2 change records, records that reappear across both batches, and duplicates within a batch.

## DAG tasks

<img width="1142" height="277" alt="s1" src="https://github.com/user-attachments/assets/b07e490a-795b-4da8-a8c2-64b07c1ba0a3" />

1. Task - input data & metadata file verification/checks: verifies that the input JSONL files are present and parseable in MinIO and that the metadata file is structurally sound (dimension registry keys, fact measure source column, validation field references etc. are present). The DAG does not proceed if either check fails.

2. Task - PySpark ETL job (the core run):
   - Registers the run in the run-audit table.
   - Scans MinIO for input batches and skips any already ingested.
   - Applies schema enforcement and metadata-driven validation, splitting records into valid and invalid buckets.
   - For each dimension: deduplicates, applies SCD logic, writes a Parquet snapshot, stages the records in PostgreSQL, and merges them into the target table.
   - Generates the calendar dimension and assembles the fact table.
   - Marks the run as successful or failed.

3. Task - DQ metrics aggregation: reads the valid/invalid outputs from MinIO, queries dimension and fact row counts from PostgreSQL, and writes reconciliation totals and per-field error breakdowns to the metrics table.

4. Task - post-load verification/checks: assertions covering SCD1/SCD2 correctness, Parquet-to-PostgreSQL equivalence, and data integrity.

## End-to-end data flow and storage zones

Data moves through 4 MinIO buckets before landing in the PostgreSQL DWH:

- `input-data/` - raw (source) batches uploaded by `generate_sample_data.py`.
- `data-valid/` and `data-invalid/` - records split by validation, written as JSON per batch.
- `motor-policy-dimensions/` - per-dimension Parquet snapshots holding the full SCD dimension state.

The Spark job reads `input-data/`, writes the valid/invalid split, and builds each dimension's Parquet snapshot from the valid records. Those snapshots are the source of truth for each dimension's state; PostgreSQL is upserted from them and then serves the star schema for analytics. The DWH also holds the ETL state and DQ metrics tables.

# Pipeline workflow

## Data processing

### Ingestion and validation

`field_validator.py` compiles each field's metadata rules into Spark SQL expressions, evaluated in a single `selectExpr` pass.

Supported rule types are: `notNull` (value must not be NULL), `notEmpty` (string must not be empty after trimming), `regex` (value must match a declared pattern), `minValue` / `maxValue` (numeric bounds). A field absent from the input is flagged as `fieldMissing`.

Records that fail any rule are routed to `data-invalid/` with a `validation_errors` column describing each failure; the rest go to `data-valid/`.

### SCD processing

For each dimension, the pipeline:

- Deduplicates the pending batch, keeping the most recent, deterministically ordered record per entity
- Applies the dimension's SCD logic: under SCD2, a current row whose tracked fields changed is expired and a new version is inserted (with proper start/end date management); under SCD1, tracked attributes are updated in place.

### DWH loading

Dimensions are upserted into PostgreSQL, the calendar dimension (`dim_date`) is generated, and fact table is assembled by resolving each insurance policy (`dim_policy`) version's natural keys to dimension surrogate keys (grain: 1 row per policy version). The mart views are defined in the DDL (`sql/schema_init.sql`) and created at database initialization.


## Data quality & testing

Quality checks are written as `pytest` tests, split by `pre_pipeline` / `post_pipeline` markers and reading from MinIO and PostgreSQL through shared fixtures. Both groups run inside the DAG as 2 tasks.

### Pre-pipeline checks
- Confirm the input data exists in MinIO and is valid, parseable JSON.
- Confirm the metadata file is structurally valid: the dimension registry is non-empty and carries the required keys, the fact measure source field exists in the source schema, and every validation rule references a declared source column.

### Post-pipeline checks
- SCD correctness - for SCD2, 1 current row per policy with aligned start/end dates; for SCD1, an attribute change updates the existing row in-place.
- Star schema integrity - expected tables and marts exist, the DWH dimension state matches the Parquet snapshots, fact measures are populated, and the fact table stays time-aligned with the source data.
- Data lineage and error formatting - valid records carry all source columns and no errors; invalid records carry their validation errors, and the reconciliation metrics are present and consistent.

### DQ metrics

Computed after each run and persisted per run:
- Per-batch valid and invalid record counts.
- Input-to-output reconciliation (input records = valid + invalid, so the difference should be zero).
- Duplicate rate.
- Per-field error breakdown.
- Dimension and fact row counts, including the SCD2 current-vs-historical split for `dim_policy`.

### Observability

A single Grafana dashboard and its PostgreSQL datasource are provisioned on startup. It reads the per-run DQ metrics and the run-audit table, presenting those metrics scoped to a selected run, plus cross-run rate trend and a "Recent runs" table of run status, timing, and errors. 

<img width="1918" height="683" alt="s3" src="https://github.com/user-attachments/assets/60b8caa9-75c8-4c7b-8be3-daf41f7ae129" />

<img width="1912" height="741" alt="s4" src="https://github.com/user-attachments/assets/c6ab5401-7fa3-4277-ab6e-73c14911fcd1" />


The run-audit table has 1 record per pipeline run (marked `running`/`success`/`failed`. The pipeline emits structured JSON logs to stdout, captured in Airflow's task logs. Beyond that, there is no centralized log aggregation or real-time monitoring.

### Data analytics/BI

Alongside the Grafana DQ dashboard, a Metabase instance is provisioned for ad-hoc BI, so the DWH can be queried and explored directly.

<img width="1918" height="871" alt="s5" src="https://github.com/user-attachments/assets/23d3f362-8e92-4c2a-9bd9-0c395a1b9911" />

<img width="1918" height="957" alt="s6" src="https://github.com/user-attachments/assets/a050ff26-b475-495e-88ea-47c180eb0352" />

## Running the pipeline

Local setup:

```bash
make build          # build all Docker images
make up             # start all services
make generate-data  # generate dataset and upload to MinIO
```

Once generated, the synthetic dataset is uploaded to MinIO at `http://localhost:9001` (credentials: `minioadmin` / `minioadmin`). Airflow is accessible at `http://localhost:8080` (credentials: `admin` / `admin`) for triggering the DAG manually. After the run completes, DQ metrics are shown in the Grafana dashboard at `http://localhost:3000` (credentials: `admin` / `admin`). Metabase is available at `http://localhost:3001` (create an admin user and connect it to the analytics database on first login).

The following commands are also available:
```bash
make test-pre      # run pre-pipeline checks without triggering the DAG
make test-post     # run post-pipeline assertions against the current DWH state
make clean         # stop all services and remove all volumes
```
