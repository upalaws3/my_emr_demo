# Region switch to us-east-1, config-driven cleanup, and INSTALL.md

This iteration is a cleanup/config/docs pass on the already-approved EMR medallion baseline, with
no new pipeline behavior. It switches the demo region to `us-east-1` in the three knob locations
(config, Terraform variable default, tfvars example) while leaving region fully config-driven; it
externalizes four demo-tunable literal sets into `config/pipeline.yaml` (an `environments`
allow-list, a single-source-of-truth `reference_data` block, declarative `gold_marts` metadata, and
a `masking.columns` PII catalog) with fail-fast validation in `config_loader.py`; and it adds a
numbered copy-pasteable `INSTALL.md` at the repo root plus a README pointer. The schema contract in
`schema_defs.py` and the Spark aggregation bodies stay in code, as required.

Watch for: nothing blocking. Two minor non-blocking observations — the gold argparse path reloads
config two to three times per invocation (confirmed), and `scripts/upload_landing.py` keeps its
`--env` choices hardcoded while the two Spark job parsers now read from config (confirmed,
defensible since the plan scoped only the job parsers and the uploader is deliberately config-free).

**Verdict**: APPROVED

## High-level view

Region is switched correctly and completely. The literal `ap-southeast-1` existed in exactly three
places plus a test assertion; all three now read `us-east-1`, and a repo-wide grep (excluding
`.agents/`) returns zero stale references. Region was already read from `cfg.region` /
`var.region` everywhere else, so nothing new was hardcoded and the config-driven property holds.

The config externalizations are genuinely simple and match the "single source of truth" intent.
Coded-value sets that were previously duplicated (config `dq_rules` plus Python tuples in
`schema_defs.py` and the generator) now live once under `reference_data`; the generator reads them
with its existing narrow stdlib scanner and the DQ job resolves `in_set` allowed values through a
small explicit column→key map rather than a generic engine. The PII catalog moved out of a module
constant into `masking.columns`, with `masking.py` kept import-safe and config-free by passing the
resolved per-table map in as an argument. Gold marts expose only the declarative bits (names and
lineage input tables); the aggregation builders remain plain Python functions. None of these hurt
readability, and the explicit LEAVE-IN-CODE decisions (schema columns, aggregation bodies, transform
wiring maps, cosmetic value pools, masking primitives) were respected.

`config_loader.py` promotes the three new sections to first-class `Config` fields and validates them
fail-fast in the existing `event=config_fail` style: `environments` non-empty list, `reference_data`
a mapping with non-empty `countries`/`currencies`, and `masking.columns` present as a dict. Tests
cover every new path, including negative cases that break one section at a time.

`INSTALL.md` is complete and correctly ordered (prereqs → code → local Python → configure → deploy →
run → Athena → teardown → troubleshooting), uses `us-east-1` in every region/ARN/CLI example, and
cross-links the existing `docs/`, README, Terraform README, and Athena notes instead of duplicating
them. Its three local commands and the `upload_landing.py` flags match the real entry points. The
README gained a one-line pointer and a layout-table row without being restructured.

<details>
<summary>Issues (2)</summary>

1. **Repeated config loads in gold argparse** (non-blocking) — `_parse_gold_args` calls
   `gold_marts()`, `allowed_envs()`, and `_mart_inputs()` each independently reload the full config
   from disk via `config_loader.load_config()`. Harmless for a POC CLI, but a single load threaded
   through would be marginally cleaner if ever revisited.
2. **Uploader `--env` not config-driven** (non-blocking) — `scripts/upload_landing.py` keeps
   `choices=["dev"]` hardcoded while the two Spark job parsers now read `environments` from config.
   Defensible (the plan scoped only the job parsers, and the uploader is intentionally boto3/stdlib
   with no config dependency), but worth noting as an asymmetry if env-list consistency matters
   later.

</details>

<details>
<summary>Details</summary>

### reference_data as single source of truth

The coded-value sets were genuinely de-duplicated, not just copied. `schema_defs.py` deleted its
`COUNTRIES`/`CURRENCIES` tuples (keeping only the column contract) and the generator dropped its
local `_ACCOUNT_TYPES`/`_CARD_TYPES`/etc. tuples, both now sourced from `reference_data`. The
generator reads the block with `read_reference_data()`, a narrow stdlib scanner that mirrors the
existing `read_row_count_ranges()` approach and keeps the generator PyYAML-free — the right call
given the generator's stdlib-only constraint. It validates all seven required keys are present and
non-empty, failing with a clear message otherwise.

On the DQ side, the duplicated allowed lists were removed from `dq_rules`
(`customers.country_allowed` and the inline `params.allowed` for status/card_type/txn_type), and
`bronze_to_silver._build_expectations` resolves them through `_IN_SET_REFERENCE_KEY`, a five-entry
column→key dict (a small lookup, not a generic rules engine), with an inline `params.allowed` still
able to override it. The `_CODED_COLUMNS`, `_PRIMARY_KEY`, and `_REFERENTIAL` transform-wiring maps
correctly stayed in code.

### PII catalog moved to config while masking.py stays import-safe

`masking.py` dropped the `PII_COLUMNS` module constant and now exposes
`pii_columns_for(table, masking_cfg)`, with `apply_masking` taking the resolved per-table map as an
argument. This keeps the module free of any config/PyYAML dependency at import time, preserving the
bare-host import-safety contract — the pure masking primitives and `_SIMPLE_METHODS` dispatch are
unchanged. `silver_to_canonical.process_table` resolves the map once via
`pii_columns_for(table, cfg.masking)` and reuses it for both the masking call and the lineage
`mask:<method>` mappings, so the two can't drift. The compatibility follow-through landed too:
`run_local_checks._COMMON_PUBLIC` was updated to expect `pii_columns_for` instead of `PII_COLUMNS`,
which is why step 2 stays green.

### Gold marts: declarative metadata only

`canonical_to_gold` replaced the `GOLD_MARTS` tuple and the input-table lists embedded in
`_MART_BUILDERS` with config-derived `gold_marts()` / `_mart_inputs()`, while `_MART_BUILDERS` became
a name→function map only. The aggregation bodies (`build_customer_360` etc.) stay in Python with an
explicit comment recording why, satisfying the guardrail against moving aggregation logic into YAML.
`test_gold_marts_config.py` asserts the configured mart set equals the builder set, guarding against
a config/code mismatch.

One minor inefficiency: `_parse_gold_args` triggers several independent `load_config()` calls
(through `gold_marts()` and `allowed_envs()`), and `process_mart` calls `_mart_inputs(mart)` which
reloads again. For a POC CLI invoked once per stage this is immaterial, but it is a small readability
wart if the file is ever revisited — a single resolved config threaded through would read cleaner.

### config_loader validates the new keys fail-fast

`environments`, `reference_data`, and `gold_marts` were added to `_REQUIRED_TOP_KEYS` and the frozen
`Config` dataclass. Validation follows the established `event=config_fail` style: `environments`
must be a non-empty list, `reference_data` a mapping with non-empty `countries` and `currencies`,
and `masking.columns` a dict sitting next to the existing `masking.salt` check. A broken config
surfaces a specific message naming the offending key.

### Test coverage for the new paths

The suite grew from 34 to 44 tests (per the verification note). `test_config_loader.py` adds
positive assertions for region, `environments`, `reference_data`, `gold_marts`, and
`masking.columns`, plus four negative tests that each break one section and assert a `config_fail`
`ConfigError`; the stale `country_allowed` assertion was removed. `test_generator_reference_data.py`
checks the scanner reads the real config and that generated `customers.csv`/`accounts.csv` only emit
values within the configured sets. `test_masking.py` covers `pii_columns_for` for a table with PII,
a table without, and an empty config. `test_gold_marts_config.py` asserts mart names and inputs come
from config and that every configured mart has a builder. Not tested on this host: Terraform
validate/fmt and the Spark smoke, both correctly skip-if-absent and out of scope for the local gate.

### INSTALL.md and README pointer

`INSTALL.md` runs prereqs → get code → local Python (no AWS) → configure → Terraform deploy → load
and run → Athena → teardown → troubleshooting, which is the correct install order. Every region,
ARN, and CLI example uses `us-east-1`, and the troubleshooting table even includes a "wrong region"
row pointing back to the three region knobs. The local commands (`generate`, `pytest`,
`run_local_checks.py`) and the `upload_landing.py --src/--env/--account-id` flags match the actual
entry points (confirmed against the script). It cross-links the existing docs rather than restating
them. README gained the `INSTALL.md` pointer line and a layout-table row without any restructuring.

</details>

<details>
<summary>File map</summary>

- `config/pipeline.yaml` — region → us-east-1; added `environments`, `reference_data`, `gold_marts`,
  `masking.columns`; removed duplicated dq_rules allowed lists.
- `src/common/config_loader.py` — new required keys + fail-fast validation and `Config` fields.
- `src/common/masking.py` — `PII_COLUMNS` constant replaced by `pii_columns_for`; `apply_masking`
  takes the column map as an argument.
- `src/jobs/_job_common.py` — `--env` choices from `config.environments` via `allowed_envs()`.
- `src/jobs/canonical_to_gold.py` — mart list + input tables from config; builders stay in code.
- `src/jobs/bronze_to_silver.py` — `in_set` allowed values resolved from `reference_data`.
- `src/jobs/silver_to_canonical.py` — masking map resolved from config and reused for lineage.
- `data/generator/generate.py` — `read_reference_data()` stdlib scanner; gen functions take `ref`.
- `data/generator/schema_defs.py` — removed `COUNTRIES`/`CURRENCIES`; schema contract kept.
- `infra/terraform/variables.tf`, `terraform.tfvars.example` — region default → us-east-1.
- `scripts/run_local_checks.py` — public-attr list now expects `pii_columns_for`.
- `tests/test_config_loader.py`, `tests/test_masking.py` — updated for new config paths.
- `tests/test_generator_reference_data.py`, `tests/test_gold_marts_config.py` — new.
- `INSTALL.md` — new step-by-step setup guide.
- `README.md` — INSTALL.md pointer + layout row.

Full diff: `git diff 910bfa5 9ff253e`.

</details>
