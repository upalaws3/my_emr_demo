# Design Review — Baseline EMR Serverless Medallion Pipeline (iteration 3)

**Reviewer role:** Fresh-eyes design review subagent. Reviewed `design.md` against
`requirements.md` with no access to the context that produced the design.

**Scope of review:** Can a coder implement each subsystem without guessing? Specifically: the
4 table schemas and PII flags, S3 prefixes and Glue DB names, common-module function
signatures, lineage/quality/audit JSON shapes, Step Functions FAIL-gate behavior, the
Terraform resource inventory, and the local (no-AWS) verification strategy on a Windows host.

**What I verified on the host (not taken on faith):**

- `python --version` → **Python 3.14.7**. Matches the design's claim that the local authoring
  interpreter is 3.14.
- `terraform --version` → **not installed** (command not found). Matches §13's "no terraform".
- `java -version` → **not installed**. Matches §13's "no java".
- `import pyspark` → **ModuleNotFoundError**. Matches §13's "no pyspark".
- File searches for `io_paths`, `pipeline.yaml`, `emr_serverless.tf`, `pyproject.toml` → **no
  files found**. This is a greenfield design; nothing is implemented yet. There is therefore no
  existing helper, API, or module to contradict the design — the usual "the existing code
  already handles this" class of unverified assumption does not arise here. The design builds
  everything from scratch.

Because the project is greenfield and all four tooling claims verify, the local verification
strategy (§13) is realistic and self-consistent: steps 1–4 (byte-compile, import-safe, run
generator, pure-Python unit tests) are achievable on this bare Windows + Python 3.14 host, and
steps 5–6 (terraform, Spark smoke) are correctly marked skip-if-absent since the tools are
genuinely absent.

The design is unusually thorough and internally cross-referenced; iterations 1–3 have closed
most of the ambiguity a coder would otherwise hit. The findings below are the residual gaps a
coder would still have to guess at, plus one correctness bug in the DQ-injection logic.

---

## Findings

### 1. MEDIUM — `--inject-dq-failure` can be erased by the "drop exact duplicates" step, contradicting §6.5/§14

**Where:** §6.2 ("Cleanses: trims strings, ... **drops exact duplicates**, deduplicates on PK
keeping latest `_ingested_at`") vs §6.5 and §14 (injection = "two `transactions` rows with the
**same `transaction_id` AND the same `_ingested_at`**").

**Problem:** §6.2 specifies two distinct dedupe operations in order: (a) drop **exact**
duplicates (a full-row `distinct()`), then (b) dedupe on PK keeping latest `_ingested_at`. The
injected failure is two rows sharing `transaction_id` and `_ingested_at`. The design argues the
PK dedupe (b) cannot disambiguate them so both survive. That is true for step (b) — but it says
nothing about step (a). If the two injected rows are identical on **every** column (which the
design never rules out, because it only pins two of the columns), the earlier `distinct()` in
step (a) collapses them to a single row **before** the uniqueness check ever runs, and the gate
does **not** fire. The injection is then non-deterministic depending on what the generator puts
in the other columns — exactly the footgun §6.5 claims to have resolved.

**Concrete fix:** Pin that the two injected rows must be **identical on `transaction_id` and
`_ingested_at` but differ in at least one non-key column** (e.g. a different `amount` or
`merchant`), so the full-row `distinct()` in §6.2 step (a) keeps both rows and only the PK-level
uniqueness check can catch them. Add one sentence to §6.5 and §14:

> The two injected rows share `transaction_id` and `_ingested_at` but differ in `amount`
> (e.g. 100.00 vs 100.01) so the `distinct()` exact-duplicate drop in §6.2 retains both rows;
> only the PK-uniqueness expectation catches the collision.

Alternatively, pin the ordering so the uniqueness check runs against the pre-`distinct()`
DataFrame — but differing a non-key column is simpler and matches the §14 unit-test intent.

### 2. MEDIUM — Bronze `_rejected/` side-path and reject-count thread into lineage/audit is referenced but never specified

**Where:** §6.1 ("malformed rows go to a `bronze/<table>/_rejected/` side path with a reason
column, counted in lineage"), §7.1 (`metrics.rejected_rows`), §7.3 (audit `rejected_rows`), and
§10 ("Schema cast ... bad rows → `_rejected/` with reason").

**Problem:** The reject path is named and its counts flow into two JSON shapes, but the coder is
left to guess: (a) what format `_rejected/` is written in (Parquet? the original CSV/JSON? a
JSON-with-reason?); (b) the exact schema of the "reason column" (column name, is it one reason
string or a struct of per-column failures?); (c) how a type-cast failure is detected per row in
Spark (a cast to a typed column yields `null`, not an exception, so "uncastable value" needs a
concrete detection rule — e.g. "non-null source string that casts to null"); (d) whether a
null produced by a legitimately-null source value is distinguishable from a cast failure. Two
different coders would implement reject detection incompatibly, and the `rejected_rows` metric
would mean different things.

**Concrete fix:** Add a short "Bronze reject contract" paragraph to §6.1:

- Detection rule: for each non-nullable typed column, a source value that is non-empty/non-null
  but casts to `null` is a reject; a genuinely empty/null source value in a nullable column is
  not.
- A row is rejected if any such cast failure occurs; it is written to `bronze/<table>/_rejected/`
  as **Parquet** with all original string columns plus a `_reject_reason` string column
  (format: `"<column>:uncastable"`, semicolon-joined for multiple).
- `rejected_rows` = count written to `_rejected/`; `output_rows` = rows written to the main
  bronze path; `input_rows = output_rows + rejected_rows`.

### 3. MEDIUM — Gold `card_count` join path is under-specified and may not match the stated grain

**Where:** §6.4 Mart 1 `customer_360`, column `card_count` — "COUNT(DISTINCT cards.card_id) per
customer (via accounts)".

**Problem:** `cards` has no `customer_id`; it joins to `customers` only through `accounts`
(`cards.account_id → accounts.account_id → accounts.customer_id`). The mart grain is one row per
`customer_id`. The design does not state the join, and a naive implementation that joins
customers → accounts → cards → transactions in a single flat join before aggregating will
**fan out** rows (a customer with 2 accounts, 3 cards, and 50 transactions produces 300 joined
rows), making `account_count`, `card_count`, `active_account_count`, and `txn_count` all
wrong unless each is computed with `COUNT(DISTINCT ...)` over the correct key or via separate
pre-aggregated subqueries. The design says `COUNT(DISTINCT ...)` for accounts and cards but uses
plain `COUNT` for `txn_count` and `MAX` for `last_txn_at`, which are incorrect under a fanned-out
join.

**Concrete fix:** Specify that `customer_360` is built from **separate per-customer
aggregations joined on `customer_id`**, not one flat join — e.g.:

> Build `customer_360` by computing each metric as an independent aggregate keyed by
> `customer_id` (accounts-count and active-count from `accounts`; card-count from
> `accounts ⋈ cards`; txn-count and last-txn from `accounts ⋈ transactions`), then join the
> per-metric results on `customer_id`. Do **not** flat-join all tables before aggregating, which
> fans out and corrupts the counts.

### 4. MEDIUM — Lineage/audit `glue_table` naming vs the `landing` DB decision is internally inconsistent for the bronze stage

**Where:** §7.1 lineage example (input `"glue_table": "ocbc_poc_dev_bronze.customers"`) and
§6.1 (bronze registers a Parquet table in Glue) vs §2.3 (landing is "not cataloged as a managed
DB ... the jobs do not write Iceberg/Parquet tables into it").

**Problem:** The `landing_to_bronze` lineage record's **input** is the landing dataset, which per
§2.3 has no Glue table. The §7.1 example only shows the `bronze_to_silver` record (input =
bronze, which does have a Glue table). It is unspecified whether the `landing_to_bronze` lineage
input carries a `glue_table` field at all (landing is uncataloged), and if the field is required
in the shape, what value it takes for landing (null? omitted? the optional
`ocbc_poc_dev_landing` DB that §2.3 says exists but is unused?). A coder serializing the
landing→bronze lineage will guess.

**Concrete fix:** State in §7.1 that `glue_table` is **optional per input/output** and is
**omitted** for uncataloged datasets (the landing input), present for every bronze/silver/
canonical/gold dataset. Add a landing→bronze example input without a `glue_table` key, or
explicitly: `"glue_table": null` for landing.

### 5. NIT — `AuditRecord` has required-vs-optional fields that the DQ-failure path cannot populate, but the dataclass contract doesn't say so

**Where:** §5.5 `AuditRecord` / §7.3 fields (`input_rows`, `output_rows`, `rejected_rows`,
`emr_application_id`, `spark_app_id`) vs §6.2a DQ-failure write (populates `run_id`, `job`,
`layer_from/to`, `table_name`, `status`, `dq_passed`, `dq_critical_failed`, `lineage_path`,
`quality_path`, and `...`).

**Problem:** On the DQ-failure path the silver write never happens, so `output_rows` is unknown
and the gate fires before `lineage_path` is produced (lineage is emitted after a successful
write per §6.2). The §6.2a snippet passes `lineage_path=lineage_path` and trails off with `...`,
implying the other numeric fields are populated, but several of them have no value at
gate-failure time. Which audit fields are nullable/optional is not stated, so a coder may mark
them required and crash on the failure path (the exact path this row exists to record).

**Concrete fix:** In §5.5 mark `output_rows`, `rejected_rows`, `lineage_path`,
`emr_application_id`, `spark_app_id` as `Optional` with a documented default of `None` for the
DQ-failure (and any pre-write fatal) path, and note that `input_rows` is populated (the read
succeeded) while post-write fields are `None`.

### 6. NIT — Glue type mapping for the bronze `register_glue_table` columns is never pinned

**Where:** §5.3 `register_glue_table(..., columns: list[tuple[str, glue_type]])` and §6.1
("ordered `(name, glue_type)` pairs derived from `schema_defs`").

**Problem:** §3 bronze types are Spark/SQL spellings (`decimal(15,2)`, `timestamp`, `date`,
`int`, `string`). Glue `StorageDescriptor.Columns` requires Hive/Glue type names, which mostly
overlap but differ in spots (e.g. Glue uses `decimal(15,2)` and `timestamp` fine, but the
mapping from `schema_defs` to the Glue string is not stated, and `int` vs `integer` can bite).
A coder must invent the §3-type → Glue-type map.

**Concrete fix:** Add a one-line mapping note to §6.1 or `schema_defs.py`: `string→string`,
`int→int`, `decimal(p,s)→decimal(p,s)`, `date→date`, `timestamp→timestamp`, and state that
`schema_defs` stores the Glue-compatible type string directly so no translation is needed.

---

## Verified Assumptions

These design claims were checked against the actual host/repo and hold:

1. **Local interpreter is Python 3.14** — confirmed `Python 3.14.7`. The design's repeated
   caveat that `py_compile` on 3.14 does not prove 3.11 compatibility is well-founded.
2. **`terraform` is not on PATH** — confirmed command-not-found. §13 step 5 (skip-if-absent) is
   correct.
3. **`java` is not installed** — confirmed command-not-found. §13 step 6 (skip-if-absent) is
   correct.
4. **`pyspark` is not importable** — confirmed `ModuleNotFoundError`. The "lazy Spark import
   inside functions so import/compile checks pass without pyspark" design (§5, §13) is both
   necessary and correctly motivated.
5. **Greenfield project** — no `io_paths`, `pipeline.yaml`, `emr_serverless.tf`, or
   `pyproject.toml` exist. No prior code contradicts the design; every module is net-new.
6. **Python 3.11-compatibility of the stated signatures** — the public signatures use
   `str | None`, `list[tuple[str, str]] | None`, `dict | None`, and dataclasses, all of which are
   valid Python 3.11 syntax. No signature in §5 uses 3.12+-only syntax (no `type` statement, no
   PEP 695 generics). The authoring target (3.11) is internally consistent with the API surface.
7. **Internal cross-reference integrity** — the per-table keying fix (Finding 1 of prior
   iterations) is applied consistently across §2.2, §5.3, §5.4, §5.8, §7.1, §7.2; the
   `table_name` rename is consistent across §5.5, §7.3, §6.4; the single-Fail-state SFN model is
   consistent across §8, §10, §15. These do not contradict one another.

## Unverified / Wrong Assumptions

**Wrong (now a finding):**

- The claim that the `--inject-dq-failure` injection "deterministically" fires the gate is
  **not guaranteed** by the design as written, because the earlier full-row `distinct()` in §6.2
  can erase two fully-identical rows before the uniqueness check. See **Finding 1**.

**Unverified (cannot be checked on a no-AWS Windows host; correctly flagged by the design as
deploy-time risks, listed here for completeness — these are NOT counted as findings because the
no-AWS constraint is a locked requirement, not a design defect):**

- EMR Serverless Terraform attribute names (`auto_stop_configuration`,
  `maximum_capacity { cpu/memory/disk }`, `cloud_watch_logging_configuration`,
  `managedPersistenceMonitoringConfiguration`) match the `~> 5.x` AWS provider schema.
- The `startJobRun.sync` payload shape for
  `configurationOverrides.monitoringConfiguration.cloudWatchLoggingConfiguration` is accepted by
  EMR Serverless and actually routes driver/executor stdout to the named CloudWatch group.
- Athena engine v3 reads Glue-registered Iceberg tables (silver/canonical/gold and the audit
  `pipeline_runs`) with no manual `CREATE TABLE`, including the unpartitioned audit table written
  by Spark.
- Iceberg 1.5.x via `GlueCatalog` + `S3FileIO` overwrite-by-table semantics behave as described
  on the EMR 7.x / Spark 3.5 runtime.
- The SFN vended-logs IAM actions on `Resource = "*"` are sufficient and necessary for
  `logging_configuration` delivery.

These remain genuine runtime unknowns. The design acknowledges each (§16 closing paragraph) and
the requirements explicitly block AWS execution, so they are appropriately deferred to deploy
time rather than treated as design defects.

---

## Verdict

Count of blocking findings: **4 MEDIUM, 0 HIGH** (plus 2 NIT). Mechanical rule: HIGH + MEDIUM
> 0 → **CHANGES_REQUESTED**.

The design is close and the four MEDIUM items are each a small, well-scoped clarification or a
single-sentence correctness fix — a quick loop-back, not a redesign. Finding 1 is a genuine
logic bug in the headline "watch the gate fire" demo path; Findings 2–4 are specification gaps
where two coders would implement incompatibly; Findings 5–6 are NITs that would merely cause a
crash on an edge path or a guessed type map.
