# Athena setup notes (gold DB + audit)

These queries run in the **`ocbc-poc-dev-wg`** Athena workgroup (Terraform
`athena.tf`) against the Glue databases that the Spark jobs populate.

## No manual `CREATE TABLE` needed

The Spark jobs register every Iceberg table in the Glue Data Catalog as they
write (silver, canonical, gold, audit). **Athena engine version 3** reads
Glue-registered Iceberg tables natively, so there is **nothing to create by
hand** — once a pipeline run has produced the gold marts and the audit table,
the queries below work as-is. (The bronze Parquet table is likewise registered,
but the demo queries target gold + audit.)

If a table is "not found", it simply means the corresponding Spark stage has
not run yet in this environment — run the Step Functions pipeline first.

## Databases used by these queries

| Query file | Database | Table |
| --- | --- | --- |
| `01_gold_customer_360.sql` | `ocbc_poc_dev_gold` | `customer_360` |
| `02_gold_daily_txn_summary.sql` | `ocbc_poc_dev_gold` | `daily_txn_summary` |
| `03_gold_account_balances.sql` | `ocbc_poc_dev_gold` | `account_balances` |
| `04_audit_run_history.sql` | `ocbc_poc_dev_audit` | `pipeline_runs` |

## Workgroup settings (reminder)

- Query results are written to the `ocbc-poc-dev-athena-results-<account_id>`
  bucket and encrypted with the data-lake CMK (SSE-KMS).
- The workgroup config is enforced, so the result location/encryption cannot be
  overridden per query.

## Column naming reminder

The audit table's per-table column is **`table_name`** (never `table`, which is
a reserved word in Athena/Trino). All gold columns match the §6.4 mart schemas
exactly (see each query's header comment).
