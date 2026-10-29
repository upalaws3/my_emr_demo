# Verification — Cleanup + Config-driven + INSTALL doc (iteration 1)

This iteration was the FIRST pass (no `review.json` present). Implemented the
full plan: region switch to `us-east-1` (GOAL 1), config-driven externalization
(GOAL 2), and `INSTALL.md` + README pointer (GOAL 3), plus new tests.

Host constraints: Windows, Python 3.14; terraform / java / pyspark NOT installed,
no AWS access. Terraform `fmt/validate` and the local Spark smoke therefore stay
SKIPPED (tools absent) — expected, not part of the local gate on this host. No
heavy toolchains were installed; no AWS calls were made.

## Commands run and results

All commands run from the repo root `c:\Upal\Projects\OCBC-Sample-project`.

### 1. Byte-compile — clean

```
python -m compileall -q src data scripts config
```
Result: clean (exit 0, no output).

### 2. Generator produces the 4 files with counts in range + PII populated

```
python -m data.generator.generate --out ./_local_out
```
Output:
```
generated landing data into ./_local_out:
  customers: 110 rows      # configured range [90,110]
  accounts: 143 rows       # configured range [140,160]
  cards: 110 rows          # configured range [110,130]
  transactions: 889 rows   # configured range [700,900]
```
Files produced: `customers.csv`, `accounts.csv`, `cards.csv`,
`transactions.json`. First customers.csv row confirms PII populated:
`CUST000001,Isabel,Wong,isabel.wong1@example.com,+6598299464,S4744854E,1962-10-19,IN,...`
`country` and `currency` values confirmed drawn from `reference_data` sets.

### 3. Unit tests — all green (incl. new tests)

```
python -m pytest tests/ -q
```
Result: `44 passed` (was 34 before; +10 new/updated assertions across
`test_config_loader.py`, `test_masking.py`, and the two new files
`test_generator_reference_data.py`, `test_gold_marts_config.py`).

### 4. Local gate — RESULT: OK (mandatory steps passed)

```
python scripts/run_local_checks.py
```
Summary:
```
  1. compileall + py_compile   PASS
  2. import-safe common        PASS
  3. generator in-range + PII  PASS   (customers=110, accounts=143, cards=110, transactions=889)
  4. pytest                    PASS   (44 passed)
  5. terraform                 SKIPPED  terraform not installed
  6. spark smoke               SKIPPED  pyspark and/or java not available
RESULT: OK (mandatory steps passed)   (exit 0)
```

### 5. Region grep — no stale references

```
grep ap-southeast-1  (excluding .agents/)
```
Result: ZERO matches outside `.agents/` (the only hits are in
`.agents/tasks/.../FEAT-001.json` and `plan.md`, which are historical task docs,
out of scope). `config/pipeline.yaml`, `infra/terraform/variables.tf`, and
`infra/terraform/terraform.tfvars.example` now all use `us-east-1`.
`INSTALL.md` uses `us-east-1` in every example and has zero `ap-southeast` hits.

## Cleanup

`./_local_out`, all `__pycache__`, and `.pytest_cache` removed after the run.

## Changes summary

- GOAL 1: `region: us-east-1` in config + Terraform default + tfvars example.
- GOAL 2 (config-driven, kept simple):
  - `config/pipeline.yaml`: added `environments`, `reference_data`, `gold_marts`,
    `masking.columns`; removed duplicated allowed-value lists from `dq_rules`
    (`customers.country_allowed` and inline `params.allowed` for status/card_type/
    txn_type now resolve from `reference_data`).
  - `config_loader.py`: new required keys + fail-fast validation
    (`environments` non-empty, `reference_data.countries/currencies` non-empty,
    `masking.columns` present) in the existing `event=config_fail` style.
  - `_job_common.py` / `canonical_to_gold.py`: `--env` choices from config;
    gold mart list + input tables from config; builder bodies LEFT in Python.
  - `generate.py` / `schema_defs.py`: generator reads coded sets from config via
    a stdlib-only scanner; `COUNTRIES`/`CURRENCIES` tuples removed from
    `schema_defs.py` (schema contract columns kept in code).
  - `masking.py` / `silver_to_canonical.py`: PII catalog moved to
    `config.masking.columns`, read via `pii_columns_for`; pure masking primitives
    LEFT in code. `run_local_checks._COMMON_PUBLIC` updated to expect
    `pii_columns_for` (keeps masking.py import-safe with no config dependency).
  - LEFT IN CODE (readability): schema_defs column defs, gold Spark aggregation
    bodies + representative lineage calls, bronze `_CODED_COLUMNS`/`_PRIMARY_KEY`/
    `_REFERENTIAL` wiring, generator cosmetic value pools, masking primitives.
- GOAL 3: new top-level `INSTALL.md`; one-line pointer + layout row in README.md.
- Tests: updated `test_config_loader.py`, `test_masking.py`; added
  `test_generator_reference_data.py`, `test_gold_marts_config.py`.
