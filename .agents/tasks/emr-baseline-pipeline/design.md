# Technical Design — Baseline EMR Serverless Medallion Pipeline (POC)

## Overview

This design turns the approved requirements into a concrete, implementable blueprint for a
POC-scale banking data ingestion pipeline on **AWS EMR Serverless + PySpark**, provisioned
with **Terraform**, orchestrated by **AWS Step Functions**, with **lightweight custom JSON
lineage** written to S3 and registered in the Glue Data Catalog. The pipeline moves four
banking tables through a five-stage medallion-plus-canonical flow
(`landing → bronze → silver → canonical → gold`), runs in-Spark data-quality assertions with
a hard FAIL gate, masks PII in the canonical layer, emits structured logs and a run-level
audit record, and exposes the gold layer to Athena.

The whole system is authored and verified on a **local Windows host with no AWS access**.
Nothing deploys or runs against AWS in this task; verification is limited to Python byte
compilation, running the pure-Python data generator, and (if the tools happen to be present)
Terraform `validate`/`fmt` and an optional local Spark+Iceberg smoke test. Those last two are
**optional and skip-if-absent** because the host confirmed `terraform`, `java`, and `pyspark`
are all unavailable.

The technology stack is **locked** by this design: **Python language level 3.11** — code targets
EMR Serverless' Spark 3.5 / Python 3.11 runtime and MUST NOT use any syntax or stdlib API newer
than 3.11, even though the local authoring interpreter is 3.14. Byte-compiling on 3.14 is a smoke
check only; it does **not** prove 3.11 compatibility and can pass 3.12–3.14-only syntax (e.g. the
new `type` statement generics, `except*` refinements) that would fail on EMR. The 3.11 ceiling is
enforced by tooling (`pyproject.toml` `[tool.black] target-version = ["py311"]`, ruff
`target-version = "py311"`) and stated as an authoring rule in §11/§13. Spark 3.5 with
**Apache Iceberg 1.5.x via the Glue Data Catalog as the
Iceberg catalog**, Terraform ≥ 1.6 with the AWS provider ~> 5.x, Step Functions (ASL JSON
rendered by Terraform), and Athena engine v3. No OpenLineage/Marquez, no external Great
Expectations dependency, no alternative compute.

---

## 1. Repository / Directory Layout

All paths are relative to the workspace root `c:\Upal\Projects\OCBC-Sample-project`.

```
OCBC-Sample-project/
├── README.md                          # top-level architecture + runbook (see §12)
├── requirements.txt                   # Python deps (see §11)
├── pyproject.toml                     # project metadata + tool config (black/py version)
├── config/
│   └── pipeline.yaml                  # single source of truth for names/prefixes/DQ rules
├── data/
│   └── generator/
│       ├── __init__.py
│       ├── generate.py                # synthetic data generator (pure Python, stdlib only)
│       └── schema_defs.py             # the 4 table column defs + PII flags (shared w/ docs)
├── scripts/
│   ├── upload_landing.py              # upload generated data to landing S3 prefix (boto3)
│   ├── run_local_checks.py            # local verification orchestrator (see §13)
│   └── smoke_local_spark.py           # OPTIONAL local Spark+Iceberg smoke test (skip-if-absent)
├── src/
│   ├── common/
│   │   ├── __init__.py
│   │   ├── logging_util.py            # structured JSON logging
│   │   ├── masking.py                 # PII masking (pure fns + Spark expr wrappers)
│   │   ├── spark_session.py           # Spark + Iceberg/Glue session helper
│   │   ├── lineage.py                 # custom JSON lineage emitter + Glue register
│   │   ├── quality.py                 # expectations helper + FAIL gate
│   │   ├── audit.py                   # run-level audit/metrics writer
│   │   ├── config_loader.py           # loads config/pipeline.yaml, resolves per-env names
│   │   └── io_paths.py                # central S3 path/table-name builder
│   └── jobs/
│       ├── __init__.py
│       ├── landing_to_bronze.py       # typed Parquet
│       ├── bronze_to_silver.py        # cleansed + DQ → Iceberg (FAIL gate owner)
│       ├── silver_to_canonical.py     # standardized + PII-masked → Iceberg
│       └── canonical_to_gold.py       # aggregate marts → Iceberg
├── sql/
│   └── athena/
│       ├── 00_setup_notes.md          # how Iceberg tables appear in Athena
│       ├── 01_gold_customer_360.sql
│       ├── 02_gold_daily_txn_summary.sql
│       ├── 03_gold_account_balances.sql
│       └── 04_audit_run_history.sql
├── infra/
│   └── terraform/
│       ├── versions.tf                # terraform + provider version pins
│       ├── providers.tf               # aws provider + default_tags
│       ├── variables.tf
│       ├── locals.tf                  # name prefixes, bucket names, tag map
│       ├── kms.tf                     # CMK + key policy
│       ├── s3.tf                      # buckets, BPA, SSE-KMS, lifecycle
│       ├── glue.tf                    # 5 layer DBs + audit DB
│       ├── iam.tf                     # EMR Serverless + Step Functions + execution roles
│       ├── emr_serverless.tf          # EMR Serverless application (auto-stop, max capacity)
│       ├── cloudwatch.tf              # SFN + EMR CloudWatch log groups (KMS-encrypted)
│       ├── stepfunctions.tf           # state machine + ASL definition
│       ├── athena.tf                  # workgroup + results config
│       ├── outputs.tf
│       ├── terraform.tfvars.example
│       └── README.md                  # deploy / teardown runbook
└── docs/
    ├── architecture.md                # mermaid + ASCII diagram, flow narrative
    ├── finops.md                      # cost story
    ├── security.md                    # KMS/BPA/IAM story
    ├── lineage_quality_observability.md
    └── pii_catalog.md                 # PII columns + masking method per column
```

A single `config/pipeline.yaml` holds project name, environment, region, bucket base name,
and the DQ rule set, so both the Spark jobs (via `config_loader.py`) and documentation read
from one place. Terraform does **not** read this YAML; instead the same logical names are
mirrored as Terraform variables/locals. The naming scheme (below) is deterministic from
`project` + `env` so both sides stay in sync without a shared state file.

---

## 2. Naming Scheme

### 2.1 Deterministic base

Two inputs drive every name: `project = "ocbc-poc"` and `env = "dev"` (both Terraform
variables and YAML keys). A `name_prefix = "${project}-${env}"` → `ocbc-poc-dev`.

### 2.2 S3 buckets and prefixes

To keep the POC simple and cheap, **one data bucket** holds all medallion layers as prefixes,
and **one results bucket** is dedicated to Athena query output (Athena requires a writable
results location and mixing it with data lifecycle rules is messy). Bucket names include the
AWS account id to stay globally unique:

| Purpose            | Bucket name                                   |
| ------------------ | --------------------------------------------- |
| Data lake (layers) | `ocbc-poc-dev-datalake-<account_id>`          |
| Athena results     | `ocbc-poc-dev-athena-results-<account_id>`    |

Prefixes inside the data-lake bucket (one per layer plus the cross-cutting operational
prefixes):

| Logical layer / stream | S3 prefix                              | Format            |
| ---------------------- | -------------------------------------- | ----------------- |
| landing                | `s3://.../landing/<table>/`            | CSV / JSON        |
| bronze                 | `s3://.../bronze/<table>/`             | Parquet           |
| silver                 | `s3://.../silver/<table>/`             | Iceberg           |
| canonical              | `s3://.../canonical/<table>/`          | Iceberg           |
| gold                   | `s3://.../gold/<mart>/`                | Iceberg           |
| lineage                | `s3://.../lineage/<run_id>/<job>/<table>.json` | JSON      |
| quality                | `s3://.../quality/<run_id>/<job>/<table>.json` | JSON      |
| audit                  | `s3://.../audit/`                      | Iceberg (+ JSON)  |

**Per-table keying (resolves review Finding 1).** Both lineage and quality JSON are emitted
**one file per table**, so a 4-table `--all` run of a stage writes four distinct lineage files
and four distinct quality files — they never collide on a single `<job>.json`. The `<table>`
segment is the final path component; `<job>` (e.g. `bronze_to_silver`) is a directory under the
run id. The `io_paths` helpers (§5.8) therefore take a `table` argument:
`lineage_path(run_id, job, table)` and `quality_path(run_id, job, table)`. The single-table JSON
shapes in §7.1/§7.2 are kept as-is (each file is one table's record); no array/map aggregation is
used. The `audit/` JSON copies are keyed similarly (`audit/<run_id>/<job>/<table>.json`) so the
durable JSON mirror is also per table, matching the per-table Iceberg audit grain (§7.3, Finding 3).

Iceberg table data physically lives under a `warehouse/` sub-prefix that the Glue/Iceberg
catalog manages (`s3://.../warehouse/<db>/<table>/`); the human-facing prefixes above are the
logical contract. The `io_paths.py` helper is the single place that builds every one of these
strings, so there is no string duplication across jobs.

### 2.3 Glue databases (one per layer + audit)

| Layer     | Glue database name       |
| --------- | ------------------------ |
| bronze    | `ocbc_poc_dev_bronze`    |
| silver    | `ocbc_poc_dev_silver`    |
| canonical | `ocbc_poc_dev_canonical` |
| gold      | `ocbc_poc_dev_gold`      |
| audit     | `ocbc_poc_dev_audit`     |

Landing is raw files only and is **not** cataloged as a managed DB in the baseline (it is
unmanaged CSV/JSON); a `ocbc_poc_dev_landing` database is still created by Terraform to satisfy
"each layer maps to its own Glue database" (AC4) and left available for optional crawler/
external-table use, but the jobs do not write Iceberg/Parquet tables into it. Glue DB names use
underscores because Athena/Hive identifiers disallow hyphens.

### 2.4 EMR Serverless / Step Functions / Athena

| Resource                    | Name                                 |
| --------------------------- | ------------------------------------ |
| EMR Serverless application  | `ocbc-poc-dev-emr-serverless`        |
| Step Functions state machine| `ocbc-poc-dev-medallion-pipeline`    |
| Athena workgroup            | `ocbc-poc-dev-wg`                     |
| KMS key alias               | `alias/ocbc-poc-dev-datalake`        |

---

## 3. Table Schemas (landing contract) and PII Flags

Schemas are declared once in `data/generator/schema_defs.py` as plain Python structures and
mirrored into `docs/pii_catalog.md`. Landing types are strings-as-delivered (CSV/JSON);
the typed column shows the target type the `landing_to_bronze` job casts to.

### 3.1 `customers`  (format: CSV)

| Column            | Bronze type | Nullable | PII | Notes                          |
| ----------------- | ----------- | -------- | --- | ------------------------------ |
| `customer_id`     | string (PK) | no       |     | `CUST%06d`                     |
| `first_name`      | string      | no       | PII | mask → first char + `***`      |
| `last_name`       | string      | no       | PII | mask → first char + `***`      |
| `email`           | string      | no       | PII | partial-mask local part        |
| `phone`           | string      | yes      | PII | keep last 2, mask rest         |
| `national_id`     | string      | no       | PII | SHA-256 + salt → token         |
| `date_of_birth`   | date        | no       | PII | generalize → birth year only   |
| `country`         | string      | no       |     | ISO-2 code, one of `[SG,US,MY,GB,IN,AU]` |
| `created_at`      | timestamp   | no       |     | ingestion-independent          |

### 3.2 `accounts`  (format: CSV)

| Column            | Bronze type     | Nullable | PII | Notes                       |
| ----------------- | --------------- | -------- | --- | --------------------------- |
| `account_id`      | string (PK)     | no       |     | `ACC%08d`                   |
| `customer_id`     | string (FK)     | no       |     | → `customers.customer_id`   |
| `account_type`    | string          | no       |     | SAVINGS/CHECKING/CREDIT     |
| `currency`        | string          | no       |     | ISO-3 (SGD/USD/…)           |
| `balance`         | decimal(15,2)   | no       |     | range check ≥ -10000        |
| `status`          | string          | no       |     | ACTIVE/DORMANT/CLOSED       |
| `opened_date`     | date            | no       |     |                             |

### 3.3 `transactions`  (format: JSON)

| Column             | Bronze type    | Nullable | PII | Notes                        |
| ------------------ | -------------- | -------- | --- | ---------------------------- |
| `transaction_id`   | string (PK)    | no       |     | `TXN%010d`                   |
| `account_id`       | string (FK)    | no       |     | → `accounts.account_id`      |
| `card_id`          | string (FK)    | yes      |     | → `cards.card_id` (nullable) |
| `amount`           | decimal(15,2)  | no       |     | range check, non-zero        |
| `currency`         | string         | no       |     | ISO-3                        |
| `txn_type`         | string         | no       |     | DEBIT/CREDIT                 |
| `merchant`         | string         | yes      |     | allowed-set-ish, free text   |
| `txn_timestamp`    | timestamp      | no       |     | used for gold daily rollups  |

### 3.4 `cards`  (format: CSV)

| Column            | Bronze type | Nullable | PII | Notes                               |
| ----------------- | ----------- | -------- | --- | ----------------------------------- |
| `card_id`         | string (PK) | no       |     | `CARD%08d`                          |
| `account_id`      | string (FK) | no       |     | → `accounts.account_id`             |
| `card_number`     | string      | no       | PII | keep last 4, mask rest (`****1234`) |
| `card_type`       | string      | no       |     | VISA/MASTERCARD/AMEX                |
| `expiry_month`    | int         | no       |     | 1–12                                |
| `expiry_year`     | int         | no       |     | ≥ current year                      |
| `cardholder_name` | string      | no       | PII | first char + `***`                  |
| `status`          | string      | no       |     | ACTIVE/BLOCKED/EXPIRED              |

**Row counts (config-driven — resolves Finding 8).** Target counts are **not** hardcoded in the
generator or the check; they are declared once in `config/pipeline.yaml` under
`generator.row_counts` as `{table: [min, max]}` ranges and read by **both** the generator and the
§13 step-3 assertion, so the assertion is deterministic:

| Table        | `generator.row_counts` range | Note                                        |
| ------------ | ---------------------------- | ------------------------------------------- |
| customers    | `[90, 110]`                  | ~100                                        |
| accounts     | `[140, 160]`                 | ~150 (≥ 1 per customer, some customers 2)   |
| cards        | `[110, 130]`                 | ~120 (not every account has a card)         |
| transactions | `[700, 900]`                 | ~800; largest fact table but still POC-tiny |

The generator draws a count uniformly within each table's range; §13 step 3 asserts each produced
count falls inside its configured `[min, max]`. Referential integrity is guaranteed by the
generator (child FKs drawn from already-generated parents). The `--inject-dq-failure` flag adds
exactly one extra `transactions` row beyond the range (the planted duplicate, §6.5), which the
step-3 check tolerates when the flag is set.

---

## 4. PII Catalog and Masking Approach

Masking happens **only in the `silver → canonical` job** (AC8). Silver keeps cleansed-but-
unmasked data (so DQ/lineage see real keys); canonical is the first masked layer; gold
inherits masked values. Each method is deterministic where a join key must survive masking.

| Table      | Column            | PII type        | Masking method                                              |
| ---------- | ----------------- | --------------- | ----------------------------------------------------------- |
| customers  | first_name        | name            | first char + `***` (e.g. `A***`)                            |
| customers  | last_name         | name            | first char + `***`                                          |
| customers  | email             | email           | partial: first char of local + `***@` + domain (`a***@x.com`)|
| customers  | phone             | phone           | keep last 2 digits, `****` prefix                           |
| customers  | national_id       | government id   | `SHA-256(salt + value)` UTF-8 → lowercase hex, first 16 chars; `salt` from `config masking.salt`, overridable by env `PIPELINE_MASK_SALT` |
| customers  | date_of_birth     | quasi-identifier| generalize to birth **year** (drop month/day)               |
| cards      | card_number       | PAN             | keep last 4, mask rest → `**** **** **** 1234`              |
| cards      | cardholder_name   | name            | first char + `***`                                          |

Design decision — **salt source and hash contract:** `national_id` is hashed with a **fixed
project salt**. `tokenize_id(value, salt)` computes `sha256((salt + value).encode("utf-8"))`,
takes the lowercase hex digest, and returns its **first 16 characters**. The concatenation order
is **salt-prefix** (`salt + value`, not `value + salt`) and the encoding is **UTF-8**; both are
fixed so the token is deterministic and matches the §14 unit test. The salt is resolved with
precedence **`PIPELINE_MASK_SALT` env var > `config.masking.salt` > hard startup fail**
(`event=config_fail`; see §9/§10) — it is never defaulted and never deferred to canonical-job
runtime. A fixed salt is chosen over a per-run random salt because tokens must be **stable
across runs** for the demo to be coherent (same customer → same token). This is a POC;
`security.md` notes that a production system would use a KMS-backed key or a tokenization vault
(explicitly out of scope). The `national_id` is a quasi-join-key only within `customers`, so
stable tokenization is sufficient.

Design decision — **masking is one-way** (no reversible tokenization), consistent with the
requirement's "hash or partial-mask" and the out-of-scope note on reversible vaults.

---

## 5. Common Modules (public functions)

Each module is importable on the local host with **no Spark import at module top-level** except
`spark_session.py`; Spark-dependent functions import `pyspark` lazily inside the function body
so `py_compile`/import checks pass on a host without pyspark. This is a deliberate testability
decision (see §13).

### 5.1 `logging_util.py`
- `get_logger(run_id: str, layer: str, table: str | None = None) -> logging.Logger`
  Returns a stdlib `logging.Logger` with a JSON formatter writing single-line JSON to stdout
  (EMR Serverless ships stdout to CloudWatch/S3). Every record carries
  `run_id, layer, table, event, message, ts`.
- `log_event(logger, event: str, **fields)` — structured helper; `event` is a stable enum-ish
  string (`job_start`, `read_done`, `dq_start`, `dq_pass`, `dq_fail`, `write_done`,
  `job_end`).
- `timed(logger, event)` — context manager that logs start/end and a `duration_ms` field.

### 5.2 `spark_session.py`
- `build_spark(app_name: str, catalog: str = "glue_catalog", warehouse: str | None = None,
  local: bool = False) -> SparkSession`
  Configures Iceberg with the Glue catalog:
  `spark.sql.catalog.glue_catalog = org.apache.iceberg.spark.SparkCatalog`,
  `...catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog`,
  `...io-impl = org.apache.iceberg.aws.s3.S3FileIO`,
  `...warehouse = s3://.../warehouse/`,
  plus `spark.sql.extensions = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`.
  When `local=True` it swaps to a `hadoop` catalog pointed at a local `warehouse/` dir and
  skips Glue (used only by the optional smoke test). `pyspark` is imported inside the function.

### 5.3 `lineage.py`
- `start_lineage(run_id, job_name, layer_from, layer_to, table) -> LineageBuilder` — the
  builder is **per table**: it carries the `table` so `emit` can derive its own path (resolves
  Finding 1). `layer_from`/`layer_to` and `table` are set here, never inferred later.
- `LineageBuilder.add_input(dataset, fmt, row_count=None, columns=None)`
- `LineageBuilder.add_output(dataset, fmt, row_count=None, columns=None)`
- `LineageBuilder.add_column_mapping(output_col, inputs: list[str], transform: str)`
- `emit(builder, s3_client=None, local_dir=None) -> str` — serializes JSON (§7.1), writes to
  `lineage/<run_id>/<job>/<table>.json` via `io_paths.lineage_path(run_id, job, builder.table)`
  (S3 via boto3 when available, else `local_dir`), returns the path. Because the path includes
  `<table>`, the four per-table files in a stage never overwrite one another.
- `register_glue_table(catalog, database, table, location, fmt, columns=None)` — where
  `columns: list[tuple[str, str]] | None` is the ordered list of `(name, glue_type)` pairs.
  For the **bronze Parquet** path `columns` is **required**: the Glue `create_table`/
  `update_table` (via boto3) populates `StorageDescriptor.Columns` from it, otherwise Athena
  cannot read the Parquet table. For **Iceberg** tables this function is a no-op
  create-if-not-exists (the Spark `CREATE TABLE ... USING iceberg` already registered schema in
  Glue), so `columns` is ignored on that path. If `fmt="parquet"` and `columns` is `None` the
  function raises `ValueError` rather than registering a schema-less table. Lazy boto3 import.

### 5.4 `quality.py`
- `Expectation` dataclass: `name, kind, column(s), params, severity ("critical"|"warn")`.
- `expect_not_null(col)`, `expect_unique(cols)`, `expect_in_set(col, allowed)`,
  `expect_range(col, min, max)`, `expect_referential(col, ref_table_name, ref_col)`,
  `expect_row_count(min, max=None)` — each returns an `Expectation`.
- `run_expectations(df, expectations, run_id, job, table, ref_dfs: dict | None) -> QualityResult`
  evaluates all expectations in Spark (count-based, no row-by-row UDFs), builds the result
  JSON (§7.2). The `table` argument is the table being validated; it is stored on the returned
  `QualityResult` so `emit_quality` can derive the per-table path (Finding 1).

**Referential wiring (normative — resolves Finding 5).** `expect_referential` stores the parent
**table NAME** (a string, `ref_table_name`), not a DataFrame. The **caller** (`bronze_to_silver`,
§6.2) is responsible for reading each needed silver parent and passing it in a dict keyed by that
same table name:

```python
ref_dfs = {"customers": spark.table(io_paths.iceberg_table("silver", "customers"))}
result = quality.run_expectations(df, expectations, run_id, job, table="accounts", ref_dfs=ref_dfs)
```

`run_expectations` resolves each referential expectation via `ref_dfs[exp.ref_table_name]`. If
the key is **absent** (e.g. a child validated before its parent's silver table was written, or a
`--table` run of a child alone), `run_expectations` raises a **hard error** (`event=dq_fail`,
non-zero exit) — it never silently treats a missing parent as a pass. The dict is keyed by parent
**table name**, matching the string stored on the expectation; `expect_referential` never holds a
DataFrame, so §5.4 and §6.2 agree on name-vs-DataFrame.
- `QualityResult.passed -> bool` (True only if no **critical** expectation failed).
- `emit_quality(result, s3_client=None, local_dir=None) -> str` — writes the per-table result
  JSON (§7.2) to `quality/<run_id>/<job>/<table>.json` via
  `io_paths.quality_path(result.run_id, result.job, result.table)`. `QualityResult` carries the
  scalar `table` it was built for (§5.4 `run_expectations` sets it), so the path is derived from
  the result and the four per-table files never collide (resolves Finding 1).
- `enforce_gate(result)` — **raises `QualityGateError`** (custom exception) if a critical
  expectation failed. It takes **only** `result` and performs **no I/O**; it does not write the
  quality JSON itself. The "quality JSON is always persisted before any raise" invariant is a
  **caller-side contract** owned by the fixed sequence in **§6.2a**
  (`run_expectations → emit_quality → enforce_gate`), not by this function. This keeps
  `enforce_gate` trivially unit-testable (pass a `QualityResult`, assert raise/no-raise) while
  the caller guarantees ordering. The resulting non-zero exit is what the Step Functions task
  catches to drive the FAIL path (§8).

### 5.5 `audit.py`
- `AuditRecord` dataclass (§7.3 fields). The per-table field is named **`table_name`** (never
  `table`, which is a reserved keyword in Spark SQL and Athena/Trino); this spelling is used
  consistently in the dataclass attribute, the audit JSON key (§7.3), the Iceberg DDL (§7.3),
  and `sql/athena/04_audit_run_history.sql`. The `status` field is a string restricted to the
  enum **`{"SUCCESS", "DQ_FAILED", "FAILED"}`** — `SUCCESS` when the table completed and passed
  its gate, `DQ_FAILED` when a critical DQ expectation tripped the gate (§6.2a), `FAILED` for any
  other fatal error (read/write/config) that aborted that table. This is the single canonical
  enumeration; no other status value is emitted.
- **Optional-vs-required fields (resolves Finding 5).** The post-write fields `output_rows`,
  `rejected_rows`, `lineage_path`, `emr_application_id`, and `spark_app_id` are declared
  **`Optional` with a default of `None`** on the `AuditRecord` dataclass. On the DQ-failure path
  (§6.2a) and any pre-write-fatal path (read/config error before the silver write), the silver
  write never happens and lineage is not yet emitted, so these fields stay `None`; `input_rows`
  **is** populated because the bronze read succeeded. On the success path all fields are populated.
  Marking them `Optional` ensures the audit write on the exact failure path it exists to record
  never crashes on a missing required field.
- `write_audit(spark, record, catalog, database, table="pipeline_runs",
  s3_json_dir=None)` — appends **one row for one table** (the `record`'s `table_name`) to the
  `ocbc_poc_dev_audit.pipeline_runs` Iceberg table (create-if-not-exists) and also writes a JSON
  copy to `audit/<run_id>/<job>/<table>.json` for durability/observability outside Athena. The
  caller invokes `write_audit` **once per table** (§6, §6.2a), so a 4-table run produces up to
  four rows; the function itself never writes more than one row per call.

### 5.6 `config_loader.py`
- `load_config(path="config/pipeline.yaml") -> Config` (uses `yaml` if installed, else a tiny
  bundled parser is **not** attempted — `requirements.txt` pins PyYAML; on the no-AWS host the
  loader is still import-safe because parsing happens at call time, not import time).
- `Config` exposes `project, env, region, buckets, databases, masking, dq_rules, generator`
  (where `generator.row_counts` holds the per-table `[min, max]` ranges read by the data
  generator and the §13 step-3 assertion — Finding 8).

### 5.7 `masking.py`
- Pure-Python functions (no Spark, unit-testable on the bare host):
  `mask_name(v) -> str` (first char + `***`), `mask_email(v) -> str` (partial local part),
  `mask_phone(v) -> str` (keep last 2), `tokenize_id(v, salt) -> str` (SHA-256 hex[:16]),
  `generalize_dob(v) -> int | None` (birth year), `mask_pan(v) -> str` (keep last 4).
- Spark wrappers: `apply_masking(df, table, salt) -> DataFrame` maps the per-table PII column →
  method table (§4) onto the DataFrame using `F.udf`/`F.expr`. `pyspark` imported lazily.

### 5.8 `io_paths.py`
- `s3_layer_prefix(layer, table) -> str`, `warehouse_uri() -> str`,
  `lineage_path(run_id, job, table) -> str` (→ `s3://.../lineage/<run_id>/<job>/<table>.json`),
  `quality_path(run_id, job, table) -> str` (→ `s3://.../quality/<run_id>/<job>/<table>.json`),
  `audit_json_path(run_id, job, table) -> str` (→ `s3://.../audit/<run_id>/<job>/<table>.json`),
  `glue_db(layer) -> str`, `iceberg_table(layer, table) -> str`
  (returns `glue_catalog.<db>.<table>`). All three operational-artifact paths take a `table`
  argument so per-table files never collide (Finding 1). Pure string functions, no AWS/Spark —
  fully unit testable on the local host.

---

## 6. Transition Jobs (responsibilities, I/O, errors)

**CLI contract (single, authoritative — §6, §8, §10 all agree on this).** There are **four
separate entry-point scripts** (`landing_to_bronze.py`, `bronze_to_silver.py`,
`silver_to_canonical.py`, `canonical_to_gold.py`), one per stage. Each is invoked with exactly:

```
--run-id <id>   (required, non-empty string)
--env <env>     (required, allowed set {dev} for the POC)
--all           (process all four tables in the fixed order; see §6.5) — DEFAULT behavior
--table <name>  (optional, local/dev convenience: process a single table; mutually exclusive
                 with --all; NOT passed by Step Functions)
--local         (optional flag, used only by the local Spark smoke test)
```

The **stage/job name is derived from the script filename** (e.g. `bronze_to_silver`), so there
is **no `--job` argument** anywhere. Step Functions always passes
`["--run-id", <id>, "--env", "dev", "--all"]` (see §8). Every job loads config, builds
logger + Spark session, then processes each table in turn: starts a **per-table** lineage
builder, does the work, emits that table's lineage JSON, and writes exactly **one audit row per
table** (§6.2a). The job exits `0` when all tables succeed / non-zero on the first failure.

**Audit grain (normative — resolves Finding 3): one audit row per table, per job.** A 4-table
`--all` run of a stage writes up to four audit rows, each with a scalar `table_name`. There is no
job-level "ALL" row. `status` for each row is one of **`{SUCCESS, DQ_FAILED, FAILED}`** (enum
defined in §5.5/§7.3). Success and failure paths share this grain: a passing table → `SUCCESS`, a
table that trips the critical DQ gate → `DQ_FAILED` (§6.2a), a table aborted by a non-DQ fatal
error (read/write/config) → `FAILED`.

### 6.1 `landing_to_bronze.py`  → Parquet
- **Reads** landing CSV/JSON per table (schema-on-read with the explicit schema from
  `schema_defs`), casts string columns to target bronze types, adds audit columns
  `_ingested_at`, `_run_id`, `_source_file`.
- **Writes** Parquet to `bronze/<table>/` partitioned by nothing (POC size) and registers the
  Parquet table in Glue via `register_glue_table(..., fmt="parquet", columns=<bronze schema>)`,
  passing the ordered `(name, glue_type)` pairs derived from `schema_defs` so
  `StorageDescriptor.Columns` is populated and Athena/Glue can read the table.
  - **Glue type mapping (pinned — resolves Finding 6):** `string→string`, `int→int`,
    `decimal(p,s)→decimal(p,s)`, `date→date`, `timestamp→timestamp`. `schema_defs` stores the
    **Glue-compatible type string directly** (the §3 spellings already match Glue/Hive), so
    `register_glue_table` needs **no translation layer** — it passes the stored type string
    straight into `StorageDescriptor.Columns`.
- **Lineage:** input = landing dataset; output = bronze dataset; column mapping = 1:1 cast.
- **No DQ gate here** (bronze is "land it as typed, reject nothing") — malformed rows go to a
  `bronze/<table>/_rejected/` side path with a reason column, counted in lineage.

**Bronze reject contract (normative — resolves Finding 2).** The reject side-path and its counts
are pinned so two coders implement it identically:
- **Detection rule:** for a typed column, a reject is a **non-empty/non-null source value that
  casts to `null`** (Spark `cast` yields `null` rather than raising on a bad value). A genuinely
  empty/null source value in a nullable column is **not** a reject (it is a legitimate null and is
  distinguishable from a cast failure precisely because the source string was empty/null to begin
  with).
- **Row rule:** a row is rejected if any such cast failure occurs on any of its typed columns.
- **Reject format:** rejects are written to `bronze/<table>/_rejected/` as **Parquet** carrying
  **all original string columns** (as delivered, uncast) plus one extra `_reject_reason` string
  column. The reason value is `"<column>:uncastable"`, and when multiple columns fail on the same
  row the per-column reasons are **semicolon-joined** (e.g. `"amount:uncastable;balance:uncastable"`).
- **Count contract:** `rejected_rows` = rows written to `_rejected/`; `output_rows` = rows written
  to the main `bronze/<table>/` path; and `input_rows = output_rows + rejected_rows`. These three
  feed the lineage `metrics` (§7.1) and the audit record (§7.3).

### 6.2 `bronze_to_silver.py`  → Iceberg (DQ + FAIL gate owner)
- **Reads** bronze Parquet.
- **Cleanses:** trims strings, normalizes case for coded columns, drops exact duplicates,
  deduplicates on PK keeping latest `_ingested_at`.
- **Processing order (fixed):** in an `--all` run the stage processes tables in the order
  **`customers → accounts → cards → transactions`** (parents before children). This order is
  what makes referential checks well-defined: by the time a child is validated, its parent's
  **silver** table already exists for this run.
- **Referential-check source (pinned):** a child's FK is validated against the **just-written
  silver parent**. The job reads each parent with
  `spark.table(io_paths.iceberg_table("silver", parent))` (the parent written earlier in the same
  `--all` run) and passes it as `ref_dfs={parent_name: parent_df}` to `run_expectations`; the
  `expect_referential` expectation stores only the parent **name**, and `run_expectations`
  resolves `ref_dfs[name]` (§5.4). The source is the silver parent, **not** the in-flight
  bronze/cleansed DataFrame. `accounts.customer_id` → silver `customers`; `cards.account_id` and
  `transactions.account_id` → silver `accounts`. If the required parent key is absent from
  `ref_dfs` (e.g. a `--table` run of a child alone, or an ordering bug), `run_expectations`
  raises a **hard error** (`event=dq_fail`, non-zero exit) — it never silently passes.
- **Runs DQ** via `quality.run_expectations` **per table, in the order above** (rules from
  config, see §6.5). For each table the required sequence is
  `run_expectations → emit_quality → enforce_gate` (§6.2a) so the quality JSON is always
  persisted before any raise; on critical failure `enforce_gate` raises `QualityGateError` and
  the job exits non-zero **before** writing that table's silver output.
- **Writes** Iceberg table `glue_catalog.ocbc_poc_dev_silver.<table>` (create-if-not-exists,
  then overwrite-by-table; POC uses overwrite rather than MERGE/upsert — documented in §14).
- **Lineage + audit per table:** after each table's silver write succeeds, emit that table's
  lineage JSON (§7.1) and write exactly one `status="SUCCESS"` audit row for it (§6.2a). The
  DQ-failure path writes a `DQ_FAILED` row for the failing table instead (§6.2a).

#### 6.2a Required DQ call sequence (hard contract)
Per table, `bronze_to_silver.py` MUST call, in this exact order:

```python
result = quality.run_expectations(df, expectations, run_id, job, table, ref_dfs)
quality.emit_quality(result, s3_client=..., local_dir=...)   # ALWAYS before the gate
try:
    quality.enforce_gate(result)                             # raises QualityGateError if critical failed
except quality.QualityGateError:
    # Finding 2: the DQ-failure audit row is written HERE, on the failure path, before re-raising.
    # Without this, enforce_gate raises and control never reaches the end-of-job success audit,
    # so no audit row would ever exist for a gated run — breaking the out-of-band DQ diagnosability
    # the §8 single-Fail-state design depends on.
    audit.write_audit(
        spark,
        audit.AuditRecord(
            run_id=run_id, job=job, layer_from="bronze", layer_to="silver",
            table_name=result.table, status="DQ_FAILED",
            dq_passed=False, dq_critical_failed=result.critical_failed,
            input_rows=input_rows,          # populated (the bronze read succeeded)
            quality_path=quality_path,
            # Optional post-write fields are left at their None default on the gate-failure path:
            # output_rows=None, rejected_rows=None, lineage_path=None,
            # emr_application_id=None, spark_app_id=None
            started_at=started_at, ended_at=ended_at, duration_ms=duration_ms),
        catalog, database)
    raise                                                    # non-zero exit → SFN Catch [States.ALL]
```

`enforce_gate` (§5.4) deliberately takes only `result` and performs **no I/O**, so both the
"quality JSON written before raise" invariant **and** the "DQ_FAILED audit row written before
re-raise" invariant are owned entirely by this caller sequence. This sequence is the single
normative statement of both invariants (referenced by §8, §10's error table, and §15).

**Audit write grain and paths (normative):**
- **Failure path (above):** exactly **one** audit row with `status="DQ_FAILED"` is written for
  the **failing table only**, before the re-raise. Tables processed and passing earlier in the
  same `--all` run have already written their own `SUCCESS` rows (see below); tables that would
  have been processed after the failing one are never reached, so they get no row.
- **Success path:** each table that passes its gate writes exactly **one** audit row with
  `status="SUCCESS"` at the end of that table's processing (immediately after its silver write
  and lineage emit), not once at job end. This keeps the per-table grain uniform across success
  and failure paths (resolves Finding 3). The §6.2 "Lineage + audit as usual" phrasing means this
  per-table, per-success write.
- Every job other than `bronze_to_silver` has no DQ gate, so it only ever takes the success path:
  one `SUCCESS` audit row per table, or — if a non-DQ fatal error (read/write/config) aborts the
  table — one `FAILED` row is written by the job's top-level `except` handler before the non-zero
  exit. `status` is therefore one of the three values enumerated in §5.5/§7.3.

### 6.3 `silver_to_canonical.py`  → Iceberg (PII masking)
- **Reads** silver Iceberg.
- **Standardizes:** canonical column names (snake_case already), canonical code values
  (e.g. map currency synonyms), ISO date/timestamp normalization.
- **Masks PII** per §4 using `common/masking.py` (§5.7): card_number last-4, email partial,
  national_id SHA-256+salt, names first-char, DOB→year, phone last-2. Pure functions are
  unit-testable without Spark; `apply_masking` wraps them for the DataFrame.
- **Writes** Iceberg `glue_catalog.ocbc_poc_dev_canonical.<table>`.
- **Lineage** records column mapping including `transform="mask:<method>"` on each PII column so
  masking is visible in lineage.

**Canonical output schemas (normative — resolves Finding 4).** Masked columns **keep their
original names and types** (masked in place); **no `_masked` suffix** is added at the canonical
layer — the suffix only appears later in the gold marts (§6.4), which read these unsuffixed
canonical columns and rename on output. The one structural change is `date_of_birth` (a `date`)
being **replaced** by `birth_year` (an `int`, the generalized year) — the `date_of_birth` column
does not exist in canonical. `national_id` is **retained under its own name** holding the 16-char
hex token (a `string`). This makes the §6.4 "already masked" assumption and its `*_masked`
renames well-defined.

**`customers` canonical schema:**

| Column          | Type   | Note                                                     |
| --------------- | ------ | -------------------------------------------------------- |
| `customer_id`   | string | PK, unchanged                                            |
| `first_name`    | string | masked in place (`A***`)                                 |
| `last_name`     | string | masked in place                                          |
| `email`         | string | masked in place (`a***@x.com`)                           |
| `phone`         | string | masked in place (last-2)                                 |
| `national_id`   | string | holds the 16-char SHA-256 hex token (used as join token) |
| `birth_year`    | int    | **replaces** `date_of_birth`; generalized year          |
| `country`       | string | unchanged                                                |
| `created_at`    | timestamp | unchanged                                             |

**`accounts` canonical schema:** `account_id` string PK, `customer_id` string FK, `account_type`
string, `currency` string, `balance` decimal(15,2), `status` string, `opened_date` date — all
unchanged (no PII in `accounts`), standardized code values only.

**`cards` canonical schema:** `card_id` string PK, `account_id` string FK, `card_number` string
(masked in place → `**** **** **** 1234`), `card_type` string, `expiry_month` int, `expiry_year`
int, `cardholder_name` string (masked in place → `A***`), `status` string.

**`transactions` canonical schema:** `transaction_id` string PK, `account_id` string FK,
`card_id` string FK (nullable), `amount` decimal(15,2), `currency` string, `txn_type` string,
`merchant` string (nullable), `txn_timestamp` timestamp — all unchanged (no PII), standardized
code/format values only.

### 6.4 `canonical_to_gold.py`  → Iceberg marts
- **Reads** canonical Iceberg (joins across tables).
- **Currency rule (resolved):** accounts/transactions are **multi-currency** and the POC does
  **not** convert currencies. Therefore any balance/amount aggregate is kept **per currency**:
  marts that sum money carry a `currency` column in their grain and never sum across currencies.
  This avoids an FX-rate dependency that is out of scope for the baseline.
- **Builds 3 marts** (one Iceberg table each in `ocbc_poc_dev_gold`), with the exact schemas and
  grain below. All PII-derived columns are already masked (read from canonical, never silver).

**Mart 1 — `customer_360`** (grain: **one row per `customer_id`**; cross-currency balance is
intentionally NOT produced — see `total_balance_by_currency` note):

| Column                 | Type           | Source / transform                                  |
| ---------------------- | -------------- | --------------------------------------------------- |
| `customer_id`          | string         | canonical customers PK                              |
| `first_name_masked`    | string         | canonical `first_name` (already masked)             |
| `last_name_masked`     | string         | canonical `last_name` (already masked)              |
| `email_masked`         | string         | canonical `email` (already masked)                  |
| `country`              | string         | canonical customers                                 |
| `account_count`        | int            | COUNT(DISTINCT accounts.account_id) per customer    |
| `card_count`           | int            | COUNT(DISTINCT cards.card_id) per customer (via accounts) |
| `active_account_count` | int            | COUNT accounts WHERE status='ACTIVE'                |
| `txn_count`            | bigint         | COUNT(transactions) across the customer's accounts  |
| `last_txn_at`          | timestamp      | MAX(transactions.txn_timestamp)                     |

Per-currency balances are exposed via **Mart 3** (`account_balances`) rather than collapsed into
`customer_360`, precisely because balances cannot be summed across currencies. `customer_360`
therefore carries counts and recency, not a single blended balance.

**Build strategy (normative — resolves Finding 3): separate per-customer aggregations joined on
`customer_id`, NOT one flat join.** `customer_360` is assembled from independent per-customer
aggregates, each keyed by `customer_id`, then joined on `customer_id`:
- **account metrics** (`account_count`, `active_account_count`) from `accounts` grouped by
  `customer_id`;
- **card-count** (`card_count`) from `accounts ⋈ cards` (on `account_id`) grouped by
  `customer_id` — `cards` has no `customer_id`, so it reaches the customer only through accounts;
- **txn metrics** (`txn_count`, `last_txn_at`) from `accounts ⋈ transactions` (on `account_id`)
  grouped by `customer_id`.

Do **not** flat-join `customers ⋈ accounts ⋈ cards ⋈ transactions` before aggregating: that
fans out rows (a customer with 2 accounts, 3 cards, and 50 transactions yields 300 joined rows),
which corrupts `txn_count` (plain `COUNT`) and `last_txn_at` (`MAX`) and forces fragile
`COUNT(DISTINCT ...)` workarounds. Computing each metric independently at its correct grain and
joining the per-metric results on `customer_id` keeps the one-row-per-customer grain exact.

**Mart 2 — `daily_txn_summary`** (grain: **one row per `account_id` × `txn_date` × `currency`**):

| Column         | Type          | Source / transform                                       |
| -------------- | ------------- | -------------------------------------------------------- |
| `account_id`   | string        | transactions FK                                          |
| `txn_date`     | date          | `date(txn_timestamp)`                                    |
| `currency`     | string        | transactions.currency (in grain — no cross-ccy sum)      |
| `txn_count`    | bigint        | COUNT(*)                                                 |
| `debit_total`  | decimal(18,2) | SUM(amount) WHERE txn_type='DEBIT'                       |
| `credit_total` | decimal(18,2) | SUM(amount) WHERE txn_type='CREDIT'                      |
| `net_amount`   | decimal(18,2) | `credit_total - debit_total`                             |

**Mart 3 — `account_balances`** (grain: **one row per `account_id`**; this is the authoritative
per-account current balance, kept in the account's native currency):

| Column           | Type          | Source / transform                                 |
| ---------------- | ------------- | -------------------------------------------------- |
| `account_id`     | string        | canonical accounts PK                              |
| `customer_id`    | string        | canonical accounts FK                              |
| `customer_token` | string        | canonical `customers.national_id` (the 16-char token) joined via `customer_id`; renamed to `customer_token` in gold |
| `account_type`   | string        | canonical accounts                                 |
| `currency`       | string        | canonical accounts (native currency of the balance)|
| `balance`        | decimal(15,2) | canonical accounts.balance (native currency)       |
| `status`         | string        | canonical accounts                                 |

- **Lineage** captures multi-input → single-output mappings and aggregation transforms
  (e.g. `net_amount` ← `[transactions.amount, transactions.txn_type]`, `transform="agg:sum"`).
- **Audit** written as with every job. The four `sql/athena/*.sql` files (§1) are authored to
  match these exact columns: `01_gold_customer_360.sql`, `02_gold_daily_txn_summary.sql`,
  `03_gold_account_balances.sql`, and `04_audit_run_history.sql` (which selects `table_name`,
  never `table`).

### 6.5 DQ rule set (config-driven, per table)
Declared in `config/pipeline.yaml` and loaded by the silver job. The `country` allowed set is
**not** hardcoded in the job: it is read from `config/pipeline.yaml` key
`dq_rules.customers.country_allowed` with the explicit value **`["SG","US","MY","GB","IN","AU"]`**
(matching exactly what the generator emits — see §3/§14). Baseline rules:
- **customers:** `customer_id` not-null + unique (critical); `email` not-null (critical);
  `country` in allowed set `[SG,US,MY,GB,IN,AU]` (warn); `row_count ≥ 1` (**warn**).
- **accounts:** `account_id` unique (critical); `customer_id` referential → silver customers
  (critical); `balance` range ≥ -10000 (warn); `status` in set (warn); `row_count ≥ 1` (**warn**).
- **transactions:** `transaction_id` unique (critical); `account_id` referential → silver
  accounts (critical); `amount` non-zero range (warn); `txn_type` in {DEBIT,CREDIT} (critical);
  `row_count ≥ 1` (**critical** — an empty transactions fact table makes the gold marts
  meaningless, so emptiness here is genuinely invalid).
- **cards:** `card_id` unique (critical); `account_id` referential → silver accounts (critical);
  `card_type` in set (warn); `expiry_month` range 1–12 (warn); `row_count ≥ 1` (**warn**).

**Design decision — row-count severity (resolves the empty-table footgun).** `row_count ≥ 1` is
**critical only for `transactions`** and **warn for the other three tables**. Rationale: a legit
or accidentally-empty `customers`/`accounts`/`cards` landing file should produce an empty-but-
valid silver table with a recorded warning, not halt the whole run; but an empty `transactions`
table makes every gold mart trivial, so it is a real failure worth gating on. This is the single
authoritative statement of row-count severity and §14 is aligned to it. Every expectation's
`severity` is declared in `config/pipeline.yaml` so a demo can retune it without code changes.

A **critical** failure in any rule above halts the pipeline at the silver stage. To let the demo
show the gate firing **deterministically**, the generator supports a `--inject-dq-failure` flag.

**Design decision — injection must survive silver dedupe (resolves Finding 7).** Silver dedupes
on PK keeping the latest `_ingested_at` **before** the uniqueness check runs, so a plain duplicate
`transaction_id` with differing timestamps would collapse to one row and the gate would **not**
fire. The flag therefore injects a violation that dedupe cannot erase: it writes **two
`transactions` rows with the same `transaction_id` AND the same `_ingested_at`**. Dedupe cannot
pick a winner between two rows identical on both the PK and the tie-break column, so it keeps both
(or, equivalently, the implementation keeps the first and the second survives as a genuine
duplicate); either way two rows share `transaction_id` after dedupe and the critical
`transaction_id_unique` expectation fails, tripping the gate. This is the single authoritative
statement of the injected violation; §14 is aligned to it.

The two injected rows are identical on `transaction_id` and `_ingested_at` but **differ in at
least one non-key column** (e.g. `amount` 100.00 vs 100.01) so the full-row `distinct()`
exact-duplicate drop in §6.2 retains both rows; only the PK-uniqueness expectation catches the
collision. Pinning this is required because §6.2 runs a full-row `distinct()` **before** the PK
dedupe: if the two rows were identical on every column, `distinct()` would collapse them first and
the gate would **not** fire. Differing a non-key column keeps the injection deterministic.

---

## 7. JSON Shapes

### 7.1 Lineage (`lineage/<run_id>/<job>/<table>.json`, one file per table)
The document carries an explicit scalar `table` field (added below) so a reader can identify the
dataset from the payload as well as the path.

**`glue_table` is optional per input/output (resolves Finding 4).** The `glue_table` key is
**present for every cataloged dataset** (bronze/silver/canonical/gold) but **omitted (or set to
`null`) for uncataloged datasets** — specifically the **landing input** of the
`landing_to_bronze` record, since §2.3 keeps landing uncataloged (the jobs write no Glue table
into the landing DB). A reader must treat a missing `glue_table` key as "uncataloged", not as an
error. Example `landing_to_bronze` input with **no** `glue_table` key:

```json
{ "dataset": "s3://.../landing/customers/", "format": "csv", "row_count": 100 }
```

The `bronze_to_silver` example below shows the cataloged case (input bronze carries
`glue_table`).
```json
{
  "schema_version": "1.0",
  "run_id": "2025-01-01T10-00-00Z_ab12cd",
  "job": "bronze_to_silver",
  "table": "customers",
  "layer_from": "bronze",
  "layer_to": "silver",
  "started_at": "2025-01-01T10:00:00Z",
  "ended_at": "2025-01-01T10:00:42Z",
  "status": "SUCCESS",
  "inputs": [
    { "dataset": "s3://.../bronze/customers/", "format": "parquet", "row_count": 100,
      "glue_table": "ocbc_poc_dev_bronze.customers" }
  ],
  "outputs": [
    { "dataset": "glue_catalog.ocbc_poc_dev_silver.customers", "format": "iceberg",
      "row_count": 100, "glue_table": "ocbc_poc_dev_silver.customers" }
  ],
  "column_mappings": [
    { "output": "email", "inputs": ["email"], "transform": "trim" }
  ],
  "metrics": { "input_rows": 100, "output_rows": 100, "rejected_rows": 0 }
}
```

### 7.2 Quality result (`quality/<run_id>/<job>/<table>.json`, one file per table)
```json
{
  "schema_version": "1.0",
  "run_id": "...",
  "job": "bronze_to_silver",
  "table": "transactions",
  "evaluated_at": "2025-01-01T10:00:30Z",
  "passed": false,
  "critical_failed": 1,
  "warn_failed": 0,
  "expectations": [
    { "name": "transaction_id_unique", "kind": "unique", "columns": ["transaction_id"],
      "severity": "critical", "passed": false,
      "observed": { "duplicate_count": 1 }, "params": {} },
    { "name": "account_id_referential", "kind": "referential", "columns": ["account_id"],
      "severity": "critical", "passed": true, "observed": { "orphans": 0 } }
  ]
}
```

### 7.3 Audit record (Iceberg `ocbc_poc_dev_audit.pipeline_runs` + JSON copy)
```json
{
  "run_id": "...",
  "job": "bronze_to_silver",
  "layer_from": "bronze",
  "layer_to": "silver",
  "table_name": "transactions",
  "status": "SUCCESS",
  "input_rows": 800,
  "output_rows": 798,
  "rejected_rows": 2,
  "dq_passed": true,
  "dq_critical_failed": 0,
  "started_at": "2025-01-01T10:00:00Z",
  "ended_at": "2025-01-01T10:00:42Z",
  "duration_ms": 42000,
  "lineage_path": "s3://.../lineage/<run_id>/bronze_to_silver/transactions.json",
  "quality_path": "s3://.../quality/<run_id>/bronze_to_silver/transactions.json",
  "emr_application_id": "00f...",
  "spark_app_id": "app-..."
}
```
Iceberg table DDL: `pipeline_runs(run_id string, job string, layer_from string, layer_to string,
table_name string, status string, input_rows bigint, output_rows bigint, rejected_rows bigint,
dq_passed boolean, dq_critical_failed int, started_at timestamp, ended_at timestamp,
duration_ms bigint, lineage_path string, quality_path string, emr_application_id string,
spark_app_id string)` — **unpartitioned** (no partition spec).

**Design decision — unpartitioned audit table (resolves Finding 6).** The earlier draft used the
Iceberg hidden-partition transform `days(started_at)`, but `pipeline_runs` is written by Spark
and also read by `04_audit_run_history.sql` through **Athena v3**; partition-transform/spec
semantics can differ across engines and this is an engine-compatibility surface the design
otherwise minimizes. At POC scale the table holds a handful of rows per run, so partitioning buys
nothing. The baseline therefore ships it **unpartitioned**. (If a later demo wants the partition
for narrative, re-add `days(started_at)` and note it is Spark-written only plus add it to the §16
deploy-time verification list — but the baseline does not.)

---

## 8. Step Functions State Machine

`ocbc-poc-dev-medallion-pipeline` is a sequential state machine whose definition (ASL JSON) is
rendered by Terraform (`templatefile`) with the EMR Serverless application id and the job role
ARN interpolated in. Each stage is a `Task` using the EMR Serverless integration
(`arn:aws:states:::emr-serverless:startJobRun.sync`) so each state blocks until the job run
completes and surfaces its exit code.

Shape:

```
StartAt: LandingToBronze
States:
  LandingToBronze     (Task: emr-serverless startJobRun.sync, entryPoint=landing_to_bronze.py)
     Catch: [States.ALL] → PipelineFailed
     → Next: BronzeToSilver
  BronzeToSilver      (Task: ... bronze_to_silver.py)   ← DQ gate owner
     Catch: [States.ALL] → PipelineFailed
     → Next: SilverToCanonical
  SilverToCanonical   (Task: ... silver_to_canonical.py)
     Catch: [States.ALL] → PipelineFailed
     → Next: CanonicalToGold
  CanonicalToGold     (Task: ... canonical_to_gold.py)
     Catch: [States.ALL] → PipelineFailed
     → Next: PipelineSucceeded
  PipelineFailed      (Fail: error="PipelineFailed",
                       cause="a pipeline stage failed; inspect the stage's CloudWatch logs and,
                       for DQ, the quality/<run_id>/ JSON")
  PipelineSucceeded   (Succeed)
```

**Design decision — single shared Fail state (no DQ-vs-other discrimination in ASL).** An
earlier draft used a `Choice` state to route a `QualityGateError` differently from other
failures. That is **not implementable** with the `emr-serverless:startJobRun.sync` integration:
a non-zero Spark exit surfaces to Step Functions as a generic EMR job-failed payload
(`EmrServerless.JobFailed` / a `States`-level error with a `Cause` describing the EMR job-run
state), **not** the in-process Python exception class name, and a `Choice` cannot substring-match
an arbitrary `Cause` string anyway. So every stage's `Catch [States.ALL]` routes to **one shared
`PipelineFailed` Fail state**. This fully satisfies **AC6**, which only requires that a critical
DQ failure **reaches a FAIL state and does not advance** to the next layer — it does not require
ASL-level discrimination of failure type.

**Where the DQ gate halts, and how DQ failures stay diagnosable without the Choice:** the gate
lives inside `bronze_to_silver.py`. On critical DQ failure the job writes the quality JSON
(always before the gate — see §6.2a and §10); `enforce_gate` raises `QualityGateError`, the
caller's `except QualityGateError` block writes a `status="DQ_FAILED"` audit row for the failing
table (§6.2a) and re-raises, and the process exits non-zero. The `startJobRun.sync`
task fails, its `Catch [States.ALL]` routes to `PipelineFailed`, and the execution stops
**before** `SilverToCanonical` runs. The fact that it was a DQ gate (not an infra error) is
recovered **out-of-band** from the durable artifacts — `quality/<run_id>/bronze_to_silver.json`
(`passed=false`, which critical expectations failed) and the audit row (`status="DQ_FAILED"`,
`dq_critical_failed>0`) — not from the ASL. This keeps the state machine trivially correct for a
POC while preserving full diagnosability. (Richer in-ASL classification via an S3 sentinel +
Lambda classifier is noted as a deferred enhancement; no Lambda is in the baseline §9 inventory.)

Run id is generated at execution start (`$$.Execution.Name` plus a timestamp) and threaded into
every job's `--run-id` so lineage, quality, and audit records for one execution share a key.

**Per-stage invocation (CLI contract, see §6):** each stage is its **own** entry-point script;
Step Functions sets `entryPoint = s3://.../src/jobs/<script>.py` and
`entryPointArguments = ["--run-id", <id>, "--env", "dev", "--all"]`. The stage/job name is
**derived from the script name**, so **no `--job` argument is passed**.

---

## 9. Terraform Resource Inventory

Provider: `hashicorp/aws ~> 5.x`, Terraform `>= 1.6`. `providers.tf` sets `default_tags` on the
AWS provider so every taggable resource inherits the FinOps tag map from `locals.tf`:

```hcl
default_tags { tags = {
  Project = "ocbc-poc", Environment = "dev", CostCenter = "data-platform-poc",
  Owner = "solutions-eng", ManagedBy = "terraform", DataClassification = "confidential-pii"
}}
```

Resources:

- **KMS** (`kms.tf`): one customer-managed `aws_kms_key` (rotation enabled) + `aws_kms_alias`
  `alias/ocbc-poc-dev-datalake`; key policy grants the account root admin and the EMR/SFN roles
  encrypt/decrypt.
- **S3** (`s3.tf`): two buckets (datalake, athena-results). For each:
  `aws_s3_bucket_public_access_block` (all four flags `true`),
  `aws_s3_bucket_server_side_encryption_configuration` with `sse-kms` + the CMK,
  `aws_s3_bucket_versioning` (enabled on datalake), and
  `aws_s3_bucket_lifecycle_configuration`:
  - `landing/` → expire after 30 days (transient raw).
  - `bronze/` → transition to STANDARD_IA after 30 days, expire after 90.
  - `lineage/`, `quality/` → expire after 90 days.
  - athena-results bucket → expire all objects after 14 days.
  - abort incomplete multipart uploads after 7 days (both buckets).
  - **`audit/` is intentionally exempt from any expiry rule** — it holds the durable
    pipeline-run tracking record (FR6/NFR4), so unlike the transient `lineage/`/`quality/`
    prefixes it is never aged out. This omission is deliberate, not an oversight.
- **Glue** (`glue.tf`): six `aws_glue_catalog_database` — landing, bronze, silver, canonical,
  gold, audit (names per §2.3).
- **IAM** (`iam.tf`):
  - `emr_serverless_job_role` — assumed by EMR Serverless; least-privilege policy scoped to the
    two bucket ARNs (+`/*`), the six Glue DBs/tables, KMS encrypt/decrypt on the CMK, and
    `logs:CreateLogStream`/`logs:PutLogEvents` on `aws_cloudwatch_log_group.emr.arn` (+`:*`).
    No `s3:*`, no `logs:*` — resources enumerated.
  - `step_functions_role` — assume EMR Serverless `StartJobRun`/`GetJobRun`/`CancelJobRun` on
    the specific application ARN, plus `iam:PassRole` of the job role, plus the CloudWatch Logs
    delivery actions SFN needs for `logging_configuration`
    (`logs:CreateLogDelivery`, `logs:GetLogDelivery`, `logs:UpdateLogDelivery`,
    `logs:DeleteLogDelivery`, `logs:ListLogDeliveries`, `logs:PutResourcePolicy`,
    `logs:DescribeResourcePolicies`, `logs:DescribeLogGroups` — these must be `Resource = "*"`
    per the SFN vended-logs requirement, which is a documented exception to least-privilege and
    noted as such in `security.md`).
  - Trust policies restrict `Principal` to the respective service and (for the job role)
    condition on the EMR Serverless application via `aws:SourceArn` where practical.
- **CloudWatch Logs** (`cloudwatch.tf`): two explicit log groups so nothing is implied:
  - `aws_cloudwatch_log_group.sfn` — name
    `/aws/vendedlogs/states/ocbc-poc-dev-medallion-pipeline`, `retention_in_days = 14`,
    `kms_key_id = aws_kms_key.datalake.arn`. Its ARN (with the `:*` suffix) is wired into the
    Step Functions `logging_configuration` (below). The `/aws/vendedlogs/states/` prefix is used
    because Step Functions log delivery requires the vended-logs resource-policy path.
  - `aws_cloudwatch_log_group.emr` — name `/aws/emr-serverless/ocbc-poc-dev`,
    `retention_in_days = 14`, `kms_key_id = aws_kms_key.datalake.arn`. This is the destination
    for the EMR Serverless application's driver/executor logs (below).
- **EMR Serverless** (`emr_serverless.tf`): one `aws_emrserverless_application` type `SPARK`,
  release `emr-7.x` (Spark 3.5), with:
  - `auto_start_configuration { enabled = true }`,
  - `auto_stop_configuration { enabled = true, idle_timeout_minutes = 15 }` (FinOps auto-stop),
  - `maximum_capacity { cpu = "8 vCPU", memory = "32 GB", disk = "200 GB" }` (cost cap),
  - `initial_capacity` kept minimal/omitted so cold workers scale from zero.
  - **Log destination (decided explicitly):** job runs use a
    `cloud_watch_logging_configuration { enabled = true, log_group_name =
    aws_cloudwatch_log_group.emr.name }` set **on the Step Functions `startJobRun` payload's
    `configurationOverrides.monitoringConfiguration`** (EMR Serverless attaches monitoring at the
    job-run level, not on the application resource). This routes Spark driver/executor stdout
    (where the §5.1 structured JSON logs land) to the `emr` log group. The alternative —
    `managedPersistenceMonitoringConfiguration` (EMR-managed S3) — is **not** chosen because the
    demo's observability story reads logs in CloudWatch alongside the SFN execution history.
    The job role needs `logs:CreateLogStream`/`logs:PutLogEvents` on the `emr` log group ARN
    (see IAM below).
- **Step Functions** (`stepfunctions.tf`): `aws_sfn_state_machine` with `definition =
  templatefile(...)` (§8), and an explicit
  `logging_configuration { level = "ALL", include_execution_data = true, log_destination =
  "${aws_cloudwatch_log_group.sfn.arn}:*" }`, role = SFN role. The `startJobRun` task payload
  sets `configurationOverrides.monitoringConfiguration.cloudWatchLoggingConfiguration` pointing
  at the `emr` log group (see above).
- **Athena** (`athena.tf`): `aws_athena_workgroup` `ocbc-poc-dev-wg` with
  `result_configuration` → athena-results bucket + `encryption_configuration` SSE_KMS,
  `enforce_workgroup_configuration = true`, `publish_cloudwatch_metrics_enabled = true`,
  engine version Athena v3. No separate Athena DB resource needed — gold Glue DB is the schema.
- **Outputs** (`outputs.tf`): bucket names, KMS key arn/alias, EMR application id, state machine
  arn, Athena workgroup name, Glue DB names — everything the scripts/runbook need.
- **Variables** (`variables.tf`): `project`, `env`, `region`, `idle_timeout_minutes`,
  `max_cpu/mem/disk`, lifecycle day counts, tag overrides. `terraform.tfvars.example` ships
  sane defaults.

FinOps notes (`finops.md`) explain: pay-per-use serverless + auto-stop (no idle cost), capacity
ceiling prevents runaway spend, lifecycle expiry caps storage growth, cost-allocation tags let
the customer slice spend in Cost Explorer, and Athena result expiry + workgroup bytes-scanned
limits control query cost.

---

## 10. Error Handling (per fallible operation)

| Operation | Failure condition | Recoverable? | Caller receives | Logged |
| --- | --- | --- | --- | --- |
| Read landing files | path missing / empty | fatal for that table | job exits non-zero; SFN `Catch`→`PipelineFailed` | ERROR + `event=read_fail` |
| Schema cast (bronze) | uncastable value | recoverable per-row | bad rows → `_rejected/` with reason; job continues | WARN per batch w/ counts |
| DQ critical expectation | not-null/unique/ref/range/set/count fails | **fatal (gate)** | `QualityGateError`, non-zero exit → SFN `Catch [States.ALL]` → shared `PipelineFailed` Fail state (§8) | ERROR `event=dq_fail`; quality JSON persisted first per §6.2a + `status="DQ_FAILED"` audit row |
| DQ warn expectation | warn rule fails | recoverable | recorded in quality JSON, pipeline continues | WARN `event=dq_warn` |
| Iceberg write | catalog/commit/S3 error | fatal | exception propagates, non-zero exit → `PipelineFailed` | ERROR `event=write_fail` |
| Glue register (bronze/audit) | Glue API error | recoverable (data already on S3) | logged; job still succeeds if write done | WARN `event=glue_register_fail` |
| Lineage emit | S3 put error | recoverable | logged; does not fail the job (observability, not data) | WARN `event=lineage_emit_fail` |
| Audit write | Iceberg/S3 error | recoverable | logged; JSON copy still attempted | WARN `event=audit_write_fail` |
| Config load | missing/invalid YAML | fatal (startup) | exits non-zero before Spark starts | ERROR `event=config_fail` |
| Masking (canonical) | null/odd PII value | recoverable | masks what it can; null stays null; never throws | DEBUG count of masked |

Principle: **data-integrity operations are fatal** (reads, DQ-critical, writes, config);
**observability side-effects are best-effort** (lineage, audit, Glue register) so a telemetry
hiccup never corrupts or blocks the data path. The DQ gate is the one place a "successful"
compute deliberately turns into a non-zero exit, and it always persists the quality JSON before
raising so the failure is diagnosable.

### Input validation (external inputs)
- **CLI args** (`--run-id`, `--env`, and `--all`/`--table`; **no `--job`** — the job name is
  derived from the script filename, see §6/§8): `--run-id` required, must be non-empty string;
  `--env` required, must be in allowed set `{dev}` for the POC; `--all` and `--table` are
  mutually exclusive and default to `--all`; invalid args → exit non-zero with `config_fail`.
- **Landing files**: schema enforced on read (explicit `StructType`); extra columns ignored,
  missing required columns → table-level fatal; type violations → per-row reject (not fatal).
- **Config YAML**: validated by `config_loader` for required keys
  (`project, env, region, buckets, databases, masking, dq_rules`) — **`masking` is required**,
  with a required sub-key `salt`, because `national_id` tokenization depends on it; a missing
  key → fatal startup error (`event=config_fail`).
- **Masking salt** resolution precedence (enforced at **startup**, not deferred to the canonical
  job): env var `PIPELINE_MASK_SALT` (must be non-empty if present) > `config.masking.salt` >
  **hard startup fail** (`event=config_fail`). The salt is never silently defaulted, so a config
  lacking `masking.salt` with no env override fails fast before any Spark work.

### Invariant ownership
- **Referential integrity** (FK validity) is owned by the **silver DQ layer** (enforced as
  critical expectations), not bronze — bronze lands everything, silver is the gatekeeper.
- **PK uniqueness** owned by silver (dedupe + unique expectation).
- **PII never reaches gold unmasked** — owned by the **canonical job**; gold reads only from
  canonical, so the invariant holds structurally (gold never reads silver). This structural
  guarantee is the reason masking sits at canonical and not gold.
- **Schema/type correctness** owned by the **bronze job** (explicit cast + reject path).

---

## 11. Python Dependencies (`requirements.txt`)

Runtime (on EMR; not installed locally): `pyspark==3.5.*` (provided by EMR runtime, pinned for
local smoke only), `boto3`, `PyYAML`. Dev/local: `boto3`, `PyYAML` (both pure-Python, install
fine on Windows), plus optional `pyspark` only if someone runs the smoke test. The generator and
`io_paths`/lineage/quality JSON builders use **stdlib only** so `generate.py` and the local
checks run on a bare Python 3.14 with nothing installed.

**Authoring language level (hard constraint):** target **Python 3.11**. Do not use any syntax or
stdlib API introduced after 3.11, even though the local interpreter is 3.14 — code ships to the
EMR 3.11 runtime. `pyproject.toml` pins `requires-python = ">=3.11,<3.12"` in intent for the
runtime, sets `[tool.black] target-version = ["py311"]`, and (if ruff is used)
`[tool.ruff] target-version = "py311"` so version-inappropriate syntax is flagged. Local
`py_compile` on 3.14 is explicitly a **smoke check, not a 3.11 compatibility guarantee** (see
§13 step 1).

---

## 12. Documentation Deliverables

`README.md` (top level): one-paragraph pitch, architecture diagram (mermaid + ASCII fallback),
prerequisites, the local-only verification steps (§13), and a "deploy to AWS" pointer to
`infra/terraform/README.md`. `docs/architecture.md` holds the full flow narrative; `finops.md`,
`security.md`, `lineage_quality_observability.md`, and `pii_catalog.md` carry the four customer-
priority narratives (FinOps, security, lineage/quality/observability, PII). The mermaid diagram
shows `landing → bronze → silver → canonical → gold` with the DQ gate branch to a FAIL state and
side-arrows to lineage/quality/audit prefixes and Athena on gold.

---

## 13. Local Verification Strategy (Windows, no AWS)

Confirmed on this host: **no `terraform`, no `java`, no `pyspark`**. Verification is therefore
layered so the always-available checks are mandatory and the tool-dependent ones are
skip-if-absent. `scripts/run_local_checks.py` orchestrates and prints a summary:

1. **Byte-compile all Python** — `python -m compileall src data scripts` and
   `python -m py_compile` on each entry point. Must pass. This is why Spark/boto3 imports are
   lazy (inside functions): modules import-compile cleanly without the heavy deps.
   **Caveat (Finding 4):** the local interpreter is 3.14 but the EMR runtime is 3.11, so a green
   `py_compile` here is a **smoke check only** and does **not** prove 3.11 compatibility — it can
   pass 3.12–3.14-only syntax. The authoring rule "target language level 3.11; no syntax/stdlib
   newer than 3.11" (§11, Overview) is the real guard, enforced via black/ruff `target-version =
   py311`. A later-iteration CI step on a true 3.11 interpreter is the authoritative check.
2. **Import-safe check** — import every `src/common` module and assert public functions exist
   (no Spark session constructed). Must pass with only stdlib (+ PyYAML/boto3 if installed;
   loader/emitter tolerate their absence at import time).
3. **Run the generator** — `python -m data.generator.generate --out ./_local_out` produces the
   four tables; assert each table's row count falls inside its configured
   `generator.row_counts[table]` `[min, max]` range (read from `config/pipeline.yaml`, the same
   source the generator used — Finding 8) and that PII columns are present and populated. Must
   pass (stdlib only; PyYAML if the config is YAML, else the ranges are mirrored in
   `schema_defs.py`).
4. **Unit tests (pure Python)** — pytest over `io_paths`, masking pure functions, lineage/
   quality/audit JSON builders (feed dicts, assert JSON shape matches §7). Must pass; these need
   no Spark.
5. **Terraform (optional, skip-if-absent)** — if `terraform` is on PATH:
   `terraform -chdir=infra/terraform init -backend=false`, then `validate`, then `fmt -check`.
   If not on PATH, print `SKIPPED: terraform not installed` and do not fail the run.
6. **Local Spark+Iceberg smoke (optional, skip-if-absent)** — if `pyspark` **and** Java are
   present, `scripts/smoke_local_spark.py` runs landing→bronze→silver→canonical→gold against a
   local hadoop Iceberg catalog (`--local`) on the generated tiny data and asserts gold tables
   materialize and the DQ gate fires under `--inject-dq-failure`. Otherwise print `SKIPPED` and
   do not fail.

Testability summary: `io_paths`, masking (pure variants), and all three JSON builders are **unit
testable** with zero infra. The transition jobs' Spark logic and the full medallion flow are
**integration testable** only via the optional local Spark smoke or a real EMR deploy. Terraform
is statically checkable via `validate`/`fmt` but not applied. CI in a later iteration would run
steps 1–5 on a Linux runner with Terraform installed; the baseline's bar is steps 1–4 green on
the bare Windows host.

---

## 14. Edge Cases

- **Empty table / zero rows** — per §6.5, `row_count ≥ 1` is **critical only for
  `transactions`** and **warn** for `customers`/`accounts`/`cards`. So an empty `transactions`
  landing file halts the gate (empty facts make gold meaningless), while an empty
  customers/accounts/cards file produces a valid empty silver table with a recorded DQ **warning**
  and the run continues. Both behaviors are demoable via empty landing files; this entry and
  §6.5 agree.
- **Nullable FKs** (`transactions.card_id`) — referential check allows nulls, only orphan non-
  null values fail.
- **Duplicate PKs in landing** — silver dedupe keeps latest `_ingested_at`, so a legitimate
  re-delivery (same PK, newer timestamp) collapses to one row and the unique expectation passes.
  The `--inject-dq-failure` flag (§6.5) deliberately defeats this by planting two `transactions`
  rows with the **same `transaction_id` and the same `_ingested_at`** but **differing in at least
  one non-key column** (e.g. `amount` 100.00 vs 100.01): the differing non-key column makes the
  full-row `distinct()` in §6.2 retain both rows, and dedupe cannot disambiguate two rows identical
  on PK and tie-break column, so both survive and the post-dedupe `transaction_id_unique` critical
  expectation fails, firing the gate deterministically. This is the exact injected violation pinned
  in §6.5.
- **Idempotent reruns** — same `run_id` overwrites its own lineage/quality JSON; Iceberg writes
  use overwrite-by-table for the POC so a rerun is clean (documented as POC simplification vs
  production MERGE/upsert).
- **Masking stability** — fixed salt guarantees the same `national_id` token across reruns and
  across tables. The §14 unit test pins the exact contract: with a known salt and a known input,
  `tokenize_id(value, salt)` equals the first 16 chars of the lowercase hex SHA-256 of the
  UTF-8 bytes of `salt + value` (salt-prefix order), asserted against a precomputed literal so
  any drift in order/encoding/truncation is caught.
- **Athena + Iceberg** — Athena v3 reads Glue-registered Iceberg tables natively; `sql/athena/
  00_setup_notes.md` documents that no manual `CREATE TABLE` is needed because the Spark jobs
  registered them in Glue.

---

## 15. Traceability to Acceptance Criteria

AC1 §3 generator; AC2 §1 `upload_landing.py`; AC3 §6 four jobs + formats; AC4 §2.2/§2.3
prefixes+DBs; AC5 §6.2/§6.2a/§6.5 DQ + §7.2 JSON; AC6 §8 DQ gate → shared `PipelineFailed` Fail
state (does not advance); AC7 §5.3/§7.1 lineage
+ Glue register; AC8 §4 masking + `pii_catalog.md`; AC9 §5.1/§5.5 logging + audit §7.3; AC10
§9 Athena WG+results+gold DB + §1 `sql/athena/`; AC11 §9 KMS/BPA/least-priv IAM; AC12 §9
default_tags + lifecycle + auto-stop/max-capacity + `finops.md`; AC13 §9 variables/outputs +
`infra/terraform/README.md`, no apply; AC14 §1/§12 structure + docs + diagram; AC15 §13 local
checks.

---

## 16. Responses to Design Review (iteration 3)

**Iteration-3 review fixes applied (this revision).** The six residual findings from the latest
`design-review.json` / `design-review.md` (iteration-3 review: 4 MEDIUM, 2 NIT) were applied in
place with no scope added and no other design questions reopened:
1. (MEDIUM) `--inject-dq-failure` rows now pinned to differ in a non-key column (e.g. `amount`
   100.00 vs 100.01) so the §6.2 full-row `distinct()` keeps both — added to §6.5 and §14.
2. (MEDIUM) Added a normative **Bronze reject contract** to §6.1 (detection = non-empty value
   casting to null; Parquet rejects with a `_reject_reason` string column; `input_rows =
   output_rows + rejected_rows`).
3. (MEDIUM) §6.4 `customer_360` now specified as **separate per-customer aggregations joined on
   `customer_id`** (account metrics, card-count via `accounts ⋈ cards`, txn metrics via
   `accounts ⋈ transactions`), not one flat join.
4. (MEDIUM) §7.1 now states `glue_table` is **optional per input/output**, omitted/null for the
   uncataloged landing input, with a `landing_to_bronze` example input that has no `glue_table`.
5. (NIT) §5.5 marks `output_rows`, `rejected_rows`, `lineage_path`, `emr_application_id`,
   `spark_app_id` as `Optional` defaulting to `None`; §6.2a's trailing `...` completed to show
   `input_rows` populated while post-write fields stay `None` on the DQ-failure path.
6. (NIT) §6.1 now pins the Glue type mapping (`string→string`, `int→int`,
   `decimal(p,s)→decimal(p,s)`, `date→date`, `timestamp→timestamp`) and states `schema_defs`
   stores the Glue-compatible type string directly (no translation needed).

This revision addresses every finding in the latest `design-review.json` /
`design-review.md` (iteration 2 review, 8 findings: 2 HIGH, 4 MEDIUM, 2 NIT). All 8 were
**addressed** (none backlogged or ignored) — each is implementable within POC scope and each
change aligns with the locked requirements.

- **Finding 1 (HIGH) — lineage/quality JSON paths lack a `<table>` segment; a 4-table `--all`
  run collides on one file. ADDRESSED.** Chose the reviewer's recommended option (a): per-table
  files. Paths are now `lineage/<run_id>/<job>/<table>.json` and
  `quality/<run_id>/<job>/<table>.json` (§2.2). `io_paths.lineage_path`/`quality_path` gained a
  `table` argument and a new `audit_json_path(run_id, job, table)` (§5.8). The lineage builder is
  now per-table (`start_lineage(..., table)`, §5.3) and `QualityResult` carries its `table`
  (`run_expectations(..., table, ...)`, §5.4); `emit`/`emit_quality` derive the path from
  builder/result. The §7.1 shape gained an explicit scalar `table` field; §7.2 already had one.
  The four per-table files never overwrite one another.

- **Finding 2 (HIGH) — the `DQ_FAILED` audit row is never written by the specified sequence.
  ADDRESSED.** §6.2a now wraps `enforce_gate` in `try/except QualityGateError` that calls
  `audit.write_audit(..., status="DQ_FAILED", dq_passed=False,
  dq_critical_failed=result.critical_failed)` for the failing table **before** re-raising, so the
  out-of-band DQ-vs-infra diagnosability story (§8/§10/§15) actually produces its artifact. The
  success-path row (`SUCCESS`) is written per table at the end of that table's processing. §6.2
  and §6 opening updated to match.

- **Finding 3 (MEDIUM) — audit granularity (job-level vs table-level) contradictory.
  ADDRESSED.** Chose per-table grain (reviewer's recommendation): **one audit row per table per
  job**, stated normatively in §6 opening, §5.5, §6.2a, and §7.3. The `status` enum is now
  explicitly enumerated as **`{SUCCESS, DQ_FAILED, FAILED}`** in §5.5/§7.3. Success and failure
  paths share the per-table grain, so no grain-mixing.

- **Finding 4 (MEDIUM) — canonical layer output schema never tabulated. ADDRESSED.** §6.3 now has
  per-table canonical output schema sub-tables for all four tables. Pinned: masked columns keep
  their **original names and types** (no `_masked` suffix at canonical); `national_id` is retained
  under its own name holding the 16-char token; `date_of_birth` is **replaced** by `birth_year:
  int`. §6.4's gold "already masked" assumption and its `*_masked` renames are now well-defined,
  and the `customer_token` source cell names canonical `customers.national_id` explicitly.

- **Finding 5 (MEDIUM) — `expect_referential` ref wiring ambiguous. ADDRESSED.** §5.4 changed the
  signature to `expect_referential(col, ref_table_name, ref_col)` (stores the parent **name**,
  not a DataFrame) and `run_expectations(df, expectations, run_id, job, table, ref_dfs)`; stated
  that the caller reads each silver parent via
  `spark.table(io_paths.iceberg_table("silver", parent))` and passes `ref_dfs={parent_name: df}`,
  that `run_expectations` resolves `ref_dfs[ref_table_name]`, and that a missing key raises a hard
  `event=dq_fail` error. §6.2 and §5.4 now agree on name-vs-DataFrame.

- **Finding 6 (MEDIUM) — `days(started_at)` partition transform on the audit table is an
  unverified cross-engine assumption. ADDRESSED.** Dropped the partition spec; `pipeline_runs` is
  now **unpartitioned** (§7.3), with a note that it is POC-tiny and that re-adding the transform
  for a demo would require the Spark-written caveat and a §16 deploy-time check.

- **Finding 7 (NIT) — `--inject-dq-failure` plants a dup that silver dedupe erases. ADDRESSED.**
  §6.5 now pins a deterministic injection: two `transactions` rows with the **same
  `transaction_id` AND the same `_ingested_at`**, which dedupe cannot disambiguate, so both
  survive and the post-dedupe uniqueness check fails and the gate fires. §14 aligned.

- **Finding 8 (NIT) — generated row counts not asserted deterministically. ADDRESSED.** Added
  `generator.row_counts` `{table: [min, max]}` ranges to `config/pipeline.yaml`, read by **both**
  the generator and the §13 step-3 assertion; §3 tabulates the ranges
  (customers `[90,110]`, accounts `[140,160]`, cards `[110,130]`, transactions `[700,900]`).

### Prior-iteration note

The following is retained from the iteration-2 responses for traceability; those 14 findings
remain addressed by the design as it now stands.

- **Finding 1 (HIGH) — SFN cannot discriminate `QualityGateError`. ADDRESSED.** Chose the
  reviewer's recommended option (b): dropped the `DQFailOrError` Choice and `DQGateFailed`
  state; every stage's `Catch [States.ALL]` now routes to a single shared `PipelineFailed` Fail
  state (§8). AC6 is satisfied (reaches FAIL, does not advance). DQ-vs-infra classification is
  recovered out-of-band from the quality JSON + a `status="DQ_FAILED"` audit row, so no Lambda is
  added. §8 rewritten; §10 error table and §15 updated to match.

- **Finding 2 (HIGH) — missing CloudWatch log groups / unspecified EMR log destination.
  ADDRESSED.** Added a new `cloudwatch.tf` with `aws_cloudwatch_log_group.sfn`
  (`/aws/vendedlogs/states/ocbc-poc-dev-medallion-pipeline`, 14-day retention, KMS) wired into
  SFN `logging_configuration` (`level=ALL`, `include_execution_data=true`, `${arn}:*`), and
  `aws_cloudwatch_log_group.emr` (`/aws/emr-serverless/ocbc-poc-dev`, 14-day, KMS). EMR log
  destination explicitly decided: job-run `cloudWatchLoggingConfiguration` on the SFN
  `startJobRun` payload (not managed-persistence), with the exact job-role IAM ARNs listed. §1,
  §9 updated.

- **Finding 3 (HIGH) — `table` reserved word in audit schema. ADDRESSED.** Renamed to
  `table_name` consistently across the `AuditRecord` dataclass (§5.5), the audit JSON key (§7.3),
  the Iceberg DDL (§7.3), and `sql/athena/04` (§6.4). The §7.2 quality-result JSON keeps its
  plain `table` field (it is a JSON document key, never a SQL/Iceberg column, so no reserved-word
  risk).

- **Finding 4 (MEDIUM) — 3.14-vs-3.11 py_compile false confidence. ADDRESSED.** Added an explicit
  "target language level 3.11" authoring constraint in the Overview and §11, enforced via black
  `target-version=["py311"]` and ruff `target-version="py311"`; §13 step 1 now states py_compile
  on 3.14 is a smoke check only and names a true-3.11 CI step as the authoritative check.

- **Finding 5 (MEDIUM) — referential-check ordering/source unspecified. ADDRESSED.** §6.2 now
  pins the processing order `customers → accounts → cards → transactions` and specifies the ref
  source as the **just-written silver parent** (`glue_catalog.ocbc_poc_dev_silver.<parent>`),
  with a hard error (never silent pass) if a child is validated before its parent's silver exists.

- **Finding 6 (MEDIUM) — inconsistent `--job` vs `--table`/`--all` contract. ADDRESSED.** Defined
  one contract (reviewer's recommendation): four per-stage scripts, Step Functions passes
  `--run-id/--env/--all`, job name derived from the script filename, **no `--job`**. §6 opening,
  §8, and §10 now agree.

- **Finding 7 (MEDIUM) — `enforce_gate` ordering invariant not in the contract. ADDRESSED.** Kept
  `enforce_gate(result)` I/O-free and made the ordering a hard caller contract in new §6.2a
  (`run_expectations → emit_quality → enforce_gate`), referenced from §5.4 and §10.

- **Finding 8 (MEDIUM) — garbled salt note in the PII table. ADDRESSED.** Replaced the residue
  cell with the resolved rule `SHA-256(salt + value)` UTF-8 → lowercase hex, first 16 chars; salt
  from `masking.salt`, overridable by `PIPELINE_MASK_SALT`. Concatenation order (salt-prefix) and
  encoding (UTF-8) pinned in §4 prose and the §14 unit-test contract.

- **Finding 9 (MEDIUM) — `masking` missing from required config keys. ADDRESSED.** §10 now lists
  `masking` (required sub-key `salt`) and states the precedence `PIPELINE_MASK_SALT > config
  masking.salt > hard startup fail (config_fail)`, enforced at startup not at canonical time.

- **Finding 10 (MEDIUM) — gold mart schemas underspecified. ADDRESSED.** §6.4 now has a
  per-mart column+type+grain table for `customer_360`, `daily_txn_summary`, and
  `account_balances`, and resolves the multi-currency question (no FX conversion; money aggregates
  are kept per `currency` in the grain). The four Athena SQL files are enumerated to match.

- **Finding 11 (MEDIUM) — `row_count ≥ 1` critical footgun. ADDRESSED.** Decided per-table
  severity: `row_count ≥ 1` is **critical only for `transactions`**, **warn** for the other
  three, declared in config. §6.5 and §14 reconciled.

- **Finding 12 (NIT) — audit lifecycle omission unexplained. ADDRESSED.** Added a §9 note that
  `audit/` is intentionally exempt from expiry (durable tracking record, FR6/NFR4).

- **Finding 13 (NIT) — `register_glue_table` lacks a columns arg. ADDRESSED.** Added
  `columns: list[tuple[str,str]] | None` (required for the Parquet path, raises `ValueError` if
  absent; ignored on the Iceberg no-op path) in §5.3; §6.1 passes the bronze schema.

- **Finding 14 (NIT) — vague "ISO-2-ish" country set. ADDRESSED.** Pinned the allowed set to
  `[SG,US,MY,GB,IN,AU]` in `config/pipeline.yaml` (`dq_rules.customers.country_allowed`),
  referenced from §3.1 and §6.5; dropped "-ish".

Reviewer "UNVERIFIED" items (EMR Serverless TF attribute names, Athena↔Iceberg round-trip,
EMR→CloudWatch delivery, Iceberg MERGE/partition-transform) remain runtime assumptions that
cannot be checked on the no-AWS Windows host; they are flagged for confirmation at deploy time
and do not block the baseline, consistent with the requirements' no-AWS-access constraint.
