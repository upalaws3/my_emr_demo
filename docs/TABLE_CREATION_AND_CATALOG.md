# Table Creation & Catalog — where the DDL lives and how tables are created

*How every table in the pipeline gets created, registered in the Glue Data Catalog, and made queryable in Athena. Short version: there is almost NO hand-written DDL — Spark/Iceberg create the tables implicitly at write time.*

---

## TL;DR

| Layer | Format | How the table is created | Hand-written DDL? | Registered in Glue by |
| --- | --- | --- | --- | --- |
| **bronze** | Parquet | `df.write.mode("overwrite").parquet(path)` writes files; then a Glue **API** call registers the schema | No (a Glue `TableInput` dict, not SQL) | `lineage.register_glue_table()` (boto3 Glue `create_table`) |
| **silver** | Iceberg | `df.writeTo(fqtable).using("iceberg").createOrReplace()` | **No** — schema inferred from the DataFrame | Iceberg via the Glue catalog, automatically |
| **canonical** | Iceberg | same `createOrReplace()` | **No** | Iceberg, automatically |
| **gold** | Iceberg | same `createOrReplace()` | **No** | Iceberg, automatically |
| **audit** (`pipeline_runs`) | Iceberg | `spark.sql("CREATE TABLE IF NOT EXISTS ...")` then `.append()` | **Yes** — the ONLY explicit DDL in the project | the Spark SQL DDL, via the Glue catalog |

**Everything runs inside the PySpark job, on the EMR Serverless driver, at write time.** No DDL is run in Athena, and (except for the audit table) none is hand-written anywhere.

---

## 1. Gold / Silver / Canonical — implicit DDL via Spark `createOrReplace()`

Example, gold (`src/jobs/canonical_to_gold.py`):

```python
fq_table = io_paths.iceberg_table("gold", mart, ctx)   # "glue_catalog.ocbc_poc_dev_gold.customer_360"
df.writeTo(fq_table).using("iceberg").createOrReplace()
```

That one line **is** the table creation. Spark's DataFrameV2 `writeTo(...).createOrReplace()`:

1. **Infers the schema from the DataFrame** `df` — whatever columns the aggregation produced (`customer_id`, `account_count`, `txn_count`, `last_txn_at`, …) with their Spark types become the table columns. There is no separate column list to maintain.
2. **Creates the table if absent, or fully replaces it if present** — so a re-run rewrites the table cleanly (no duplicate rows, no schema drift).
3. **Registers the table in the Glue Data Catalog automatically**, because the Spark session's Iceberg catalog is the Glue catalog (configured in `src/common/spark_session.py`):
   ```python
   spark.sql.catalog.glue_catalog                = org.apache.iceberg.spark.SparkCatalog
   spark.sql.catalog.glue_catalog.catalog-impl   = org.apache.iceberg.aws.glue.GlueCatalog
   spark.sql.catalog.glue_catalog.io-impl        = org.apache.iceberg.aws.s3.S3FileIO
   spark.sql.catalog.glue_catalog.warehouse      = s3://<datalake>/warehouse/
   ```
4. **Writes physical data + Iceberg metadata to S3** under `s3://<datalake>/warehouse/ocbc_poc_dev_gold.db/customer_360/`.

After that, **Athena reads the Glue table directly** — no `CREATE TABLE` in Athena. Silver and canonical work identically (same `createOrReplace()`), just different databases.

> **Why `warehouse` matters:** if the Iceberg catalog has no `warehouse` set, this call fails with *"Cannot derive default warehouse location."* That is exactly the bug that once blocked silver — fixed by threading `io_paths.warehouse_uri(ctx)` into `build_spark()` in `_job_common.bootstrap`.

---

## 2. Bronze — files + a Glue API registration (no SQL DDL)

Bronze is plain Parquet, not Iceberg, so it is created in two steps (`src/jobs/landing_to_bronze.py`):

```python
# 1. write the Parquet files
bronze.write.mode("overwrite").parquet(bronze_prefix)

# 2. register the schema in Glue so Athena can read it
lineage.register_glue_table(glue_client, database, table, location, fmt="parquet", columns=...)
```

`register_glue_table` (in `src/common/lineage.py`) is **not SQL** — it calls the **boto3 Glue API** (`create_table`, falling back to `update_table` on AlreadyExists) with a `StorageDescriptor` carrying the ordered `(name, glue_type)` columns (from `schema_defs.py`), the Parquet InputFormat/OutputFormat, and the SerDe. Athena needs this explicit column list because a bare Parquet table has no catalog schema on its own.

---

## 3. Audit (`pipeline_runs`) — the ONLY explicit SQL DDL

The audit table is the single place a hand-written `CREATE TABLE` exists (`src/common/audit.py`):

```python
spark.sql(f"CREATE TABLE IF NOT EXISTS {fq_table} ({_PIPELINE_RUNS_DDL_COLUMNS}) USING iceberg")
row_df.writeTo(fq_table).append()
```

**Why this one is different:** every data layer uses `createOrReplace()` (full overwrite each run), but the audit table uses `append()` to *accumulate run history*. To append, the table must already exist with a fixed, stable schema — so it is declared once via `CREATE TABLE IF NOT EXISTS` with the explicit `_PIPELINE_RUNS_DDL_COLUMNS` string, then appended to.

**Gotcha this creates:** because it is `IF NOT EXISTS`, changing the DDL does **not** alter an already-created table. If the schema is wrong (as it once was — `started_at`/`ended_at` declared `timestamp` while the code writes ISO-8601 strings, which silently failed every append), you must **drop the Glue table** so the corrected DDL recreates it:

```bash
aws glue delete-table --database-name ocbc_poc_dev_audit --name pipeline_runs --region us-east-1
aws s3 rm "s3://<datalake>/warehouse/ocbc_poc_dev_audit.db/pipeline_runs" --recursive --region us-east-1
# next pipeline run recreates it with the current DDL
```

The data layers never need this because `createOrReplace()` rewrites the schema on every run.

---

## 4. Mental model

```
Spark DataFrame  --writeTo(...).createOrReplace()-->  Iceberg table in Glue  -->  Athena reads it
    (schema inferred)        (silver/canonical/gold)      (+ data in S3 warehouse)

Parquet files    --Glue create_table API-->  Glue table (explicit columns)  -->  Athena reads it
    (bronze)

spark.sql(CREATE TABLE IF NOT EXISTS ...) + append()  -->  audit pipeline_runs  (the one explicit DDL)
```

- **Catalog:** AWS Glue Data Catalog, one database per layer (`ocbc_poc_dev_<layer>`).
- **Where it runs:** inside the PySpark job on the EMR Serverless driver, at write time. Never in Athena.
- **DDL you can hand-edit:** only `_PIPELINE_RUNS_DDL_COLUMNS` in `src/common/audit.py`. All other schemas follow the DataFrame (Iceberg) or `schema_defs.py` (bronze).

---
*Related: `docs/architecture.md`, `docs/KT_ARCHITECT_BRIEFING.md` (§3 flow, §7 gold marts), and `sql/athena/` for the read-side queries.*
