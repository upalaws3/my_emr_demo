# Implementation Plan — Cleanup + Config-driven + INSTALL doc (iteration 2)

Scope: a CLEANUP + CONFIG + DOCUMENTATION pass on the already-complete, reviewer-approved
baseline. NO new pipeline features. Simplicity and readability beat cleverness and
completeness. Three user goals: (1) switch demo region to `us-east-1`, (2) make the code more
config-driven / remove demo-tunable hardcoding, (3) write a step-by-step `INSTALL.md`.

Verification commands discovered during exploration (the project's real local gate):
- `python scripts/run_local_checks.py` — the canonical one-command gate (compileall + import-safe
  + generator in-range/PII + pytest; terraform/spark steps skip-if-absent on this host).
- `python -m pytest tests/ -q` — the pure-Python unit suite.
- `python -m data.generator.generate --out ./_local_out` — runs the stdlib generator.
- `python -m compileall src data scripts` — byte-compile.
Terraform `validate/fmt` and the Spark smoke CANNOT run here (terraform/java/pyspark absent); they
stay skip-if-absent and are NOT part of the gate on this host.

Design decisions recorded inline per item (this is a plan, not a design doc — 1-2 sentences each).

---

## GOAL 1 — Switch demo region to us-east-1

Exploration result: region is ALREADY config-driven everywhere it matters — Terraform's
`providers.tf` uses `var.region`, and Python reads `cfg.region`. There is no scattered region
literal in job code, SQL, or README command examples. Only THREE files hold the literal default,
plus the test that asserts region is non-empty. Full grep for `ap-southeast-1` / region literals:
- `config/pipeline.yaml` line 15: `region: ap-southeast-1`
- `infra/terraform/variables.tf` line 33: `default = "ap-southeast-1"`
- `infra/terraform/terraform.tfvars.example` line 15: `region  = "ap-southeast-1"`
- (no region literal in any `sql/athena/*.sql`, `src/**/*.py`, `docs/*.md`, `README.md`, or
  `infra/terraform/README.md` — those reference the workgroup name `ocbc-poc-dev-wg`, not a region)

- [ ] 1. Switch the demo region default to `us-east-1` in the three knob locations.
      Change `region: ap-southeast-1` → `region: us-east-1` in `config/pipeline.yaml` (keep the
      surrounding comment style). Change the `variable "region"` `default` in
      `infra/terraform/variables.tf` to `us-east-1`. Change `region  = "ap-southeast-1"` to
      `region  = "us-east-1"` in `infra/terraform/terraform.tfvars.example`. Do NOT introduce any
      new region literal elsewhere — region stays read from config/variable.
      Files: config/pipeline.yaml, infra/terraform/variables.tf, infra/terraform/terraform.tfvars.example
      Verify: `python -m pytest tests/test_config_loader.py -q` passes (test asserts `cfg.region`
      is truthy); after item 9 the stronger `assert cfg.region == "us-east-1"` also passes. Also
      re-grep the repo for `ap-southeast-1` and confirm ZERO matches outside `.agents/`.

---

## GOAL 2 — Make the code config-driven, remove demo-tunable hardcoding (keep it SIMPLE)

New/extended `config/pipeline.yaml` sections (all heavily commented in the existing style so the
file keeps doubling as documentation). Shapes below are authoritative for the items that follow.

```yaml
# --- Allowed environments (design S6/S10) ---------------------------------
# The POC only ships "dev". Listing it here (instead of a tuple in argparse)
# lets a demo add an env WITHOUT editing job code. Jobs read this for the
# argparse --env choices.
environments: [dev]

# --- Reference data (single source of truth for code sets) ----------------
# These sets were previously declared TWICE (config dq_rules + Python tuples in
# schema_defs.py / the generator). They now live ONCE here. The stdlib-only
# generator reads them with its narrow YAML scanner (no PyYAML); the DQ job and
# config_loader read them via PyYAML. Keep every entry a simple inline list of
# bare tokens on ONE line so the generator's scanner can parse it.
reference_data:
  countries:    [SG, US, MY, GB, IN, AU]     # customers.country
  currencies:   [SGD, USD, MYR, GBP, INR, AUD] # accounts/transactions.currency
  account_types: [SAVINGS, CHECKING, CREDIT]
  account_statuses: [ACTIVE, DORMANT, CLOSED]
  card_types:   [VISA, MASTERCARD, AMEX]
  card_statuses: [ACTIVE, BLOCKED, EXPIRED]
  txn_types:    [DEBIT, CREDIT]

# --- Gold marts (declarative bits only — design S6.4) ---------------------
# ONLY the simple declarative metadata is config: the list of marts and each
# mart's lineage input tables. The Spark aggregation bodies (build_customer_360
# etc.) STAY in Python on purpose — turning joins/aggregations into config would
# make the POC LESS simple (explicit non-goal).
gold_marts:
  customer_360:      { inputs: [customers, accounts, cards, transactions] }
  daily_txn_summary: { inputs: [transactions] }
  account_balances:  { inputs: [accounts, customers] }

# --- PII masking catalog (what/how to mask — design S4) -------------------
# WHICH columns get masked and by WHICH method, per table. The schema contract
# (column names/types/pii flags) stays in schema_defs.py; only this what/how
# POLICY is config. Method is one of: name|email|phone|pan|token|dob.
masking:
  salt: ocbc-poc-demo-salt-2025   # (existing key — unchanged)
  columns:
    customers:
      first_name: name
      last_name: name
      email: email
      phone: phone
      national_id: token
      date_of_birth: dob
    cards:
      card_number: pan
      cardholder_name: name
```

Also fold the inline allowed-value sets in `dq_rules` params onto the reference_data sets where
they duplicate (accounts.status → account_statuses, cards.card_type → card_types,
transactions.txn_type → txn_types). See item 5 for exactly how, and the LEAVE-IN-CODE notes.

### config_loader.py changes

- [ ] 2. Promote the new config sections to first-class `Config` attributes and validate them
      fail-fast, consistent with the existing `masking.salt` check.
      In `src/common/config_loader.py`: add `environments`, `reference_data`, and `gold_marts` to
      `_REQUIRED_TOP_KEYS`; add fields `environments: list`, `reference_data: dict`,
      `gold_marts: dict` to the frozen `Config` dataclass and populate them in `load_config`.
      Add targeted sub-key validation (same `event=config_fail` message style): `environments`
      must be a non-empty list; `reference_data` must contain at least `countries` and
      `currencies` as non-empty lists; `masking.columns` must be a dict (may be empty but present)
      — fail hard with a clear message if `masking` lacks `columns`. Keep PyYAML import lazy /
      call-time (import-safe contract unchanged). `masking` stays required; add the `columns`
      sub-key check next to the existing `salt` check.
      Files: src/common/config_loader.py
      Verify: `python -m pytest tests/test_config_loader.py -q` — existing + new tests (item 10)
      pass; `python scripts/run_local_checks.py` step 2 (import-safe) stays PASS.

### env allow-list → config

- [ ] 3. Drive the argparse `--env` choices from `config.environments` instead of the hardcoded
      tuple, in BOTH parsers.
      `src/jobs/_job_common.py`: replace the module constant `_ALLOWED_ENVS = ("dev",)` by loading
      the allowed envs from config at parse time. Add a small helper that calls
      `config_loader.load_config()` and returns `tuple(cfg.environments)`; use it for the
      `--env ... choices=` in `parse_job_args`. `src/jobs/canonical_to_gold.py` `_parse_gold_args`:
      replace its inline `choices=("dev",)` the same way. Keep config import lazy (inside the
      function, as the module already does) so modules stay import-safe. Preserve the existing
      `config_fail` exit behavior and messages.
      Design note to add as a one-line comment: allowed envs come from config so a demo can add an
      env without touching code.
      Files: src/jobs/_job_common.py, src/jobs/canonical_to_gold.py
      Verify: `python -m pytest tests/ -q` passes; `python scripts/run_local_checks.py` step 1
      (compileall + py_compile each job) stays PASS.

### reference data (countries/currencies/etc.) → single source in config

- [ ] 4. Make the generator read reference-data sets from config (stdlib-only) and drop the
      duplicate Python tuples from `schema_defs.py`.
      `data/generator/generate.py`: generalize the existing narrow YAML scanner. Add a stdlib-only
      reader `read_reference_data() -> dict[str, list[str]]` that parses the `reference_data:`
      block the SAME way `read_row_count_ranges` parses `generator.row_counts` (find the key, read
      indented `name: [tok, tok, ...]` lines with a regex, stop at dedent). Replace the
      generator's use of `schema_defs.COUNTRIES` / `schema_defs.CURRENCIES` and the local tuples
      `_ACCOUNT_TYPES`, `_ACCOUNT_STATUSES`, `_CARD_TYPES`, `_CARD_STATUSES`, `_TXN_TYPES` with
      values drawn from `read_reference_data()` (load once in `generate()` and thread into the
      per-table gen functions, mirroring how `ranges` is threaded today). Keep `_FIRST_NAMES`,
      `_LAST_NAMES`, `_EMAIL_DOMAINS`, `_MERCHANTS` as code — those are cosmetic value pools, not
      a shared contract; LEAVE IN CODE (note the reason in a comment). In
      `data/generator/schema_defs.py`: DELETE the `COUNTRIES` and `CURRENCIES` module constants and
      their comment block (they move to config). Keep every `Column` definition and the
      `TABLE_*`/helpers unchanged — the schema contract stays in code.
      Design note: the generator stays STDLIB-ONLY (no PyYAML/boto3) — this is why reference data is
      parsed with the narrow scanner, not `config_loader`. Keep each reference list on ONE line of
      bare tokens so the scanner stays simple.
      Files: data/generator/generate.py, data/generator/schema_defs.py
      Verify: `python -m data.generator.generate --out ./_local_out` prints the 4 per-table counts;
      `python scripts/run_local_checks.py` step 3 (generator in-range + PII populated) stays PASS.

- [ ] 5. Consolidate the DQ allowed-value sets so reference data is declared ONCE, and have the
      job read country/currency from `reference_data`.
      In `config/pipeline.yaml`: keep each `dq_rules` expectation's STRUCTURE (name/kind/column/
      severity) but remove the duplicated literal allowed lists where they now live in
      `reference_data` — specifically drop `dq_rules.customers.country_allowed` and the inline
      `params.allowed` lists for `accounts.status` (→ account_statuses), `cards.card_type` (→
      card_types), and `transactions.txn_type` (→ txn_types). LEAVE the `account_type` set only if
      a DQ rule references it (there is none today — do NOT add one). In
      `src/jobs/bronze_to_silver.py` `_build_expectations`: for `in_set`, resolve `allowed` from
      `cfg.reference_data` by a small, explicit column→reference-key map
      (`country`→countries, `currency`→currencies, `status`→account_statuses,
      `card_type`→card_types, `txn_type`→txn_types) when `params.allowed` is absent, replacing the
      current special-case that reads `rules.get("country_allowed")`. Keep it a tiny readable dict,
      not a generic engine.
      Design note / LEAVE-IN-CODE: the per-table coded-column case-normalization map `_CODED_COLUMNS`
      and `_PRIMARY_KEY`/`_REFERENTIAL` maps STAY in code — they are transform wiring, not
      demo-tunable reference data; moving them to YAML would reduce clarity (non-goal).
      Files: config/pipeline.yaml, src/jobs/bronze_to_silver.py
      Verify: `python -m pytest tests/ -q` passes (incl. the updated config_loader test that no
      longer asserts `country_allowed`, see item 10); `python scripts/run_local_checks.py` all
      mandatory steps PASS.

### gold marts (names + input tables only) → config

- [ ] 6. Externalize ONLY the gold mart names and each mart's lineage input tables; keep every
      Spark builder body in Python.
      `src/jobs/canonical_to_gold.py`: derive the mart list and each mart's `input_tables` from
      `cfg.gold_marts` instead of the module `GOLD_MARTS` tuple and the input-table lists embedded
      in `_MART_BUILDERS`. Keep a code-level `_MART_BUILDERS` mapping of mart name → builder
      FUNCTION only (`build_customer_360` etc. unchanged), and look up `input_tables` from config
      in `process_mart` and `_marts_to_build`. `GOLD_MARTS` becomes a helper that returns
      `tuple(cfg.gold_marts)` (or is replaced by a cfg-derived local); `_parse_gold_args` validates
      `--table` against the configured mart names (load config in that function, lazy import).
      Add the required one-line comment explaining WHY the aggregation logic stays in code.
      Design note: the representative `add_column_mapping` lineage calls per mart stay in code — they
      are illustrative lineage, tightly coupled to the transform; LEAVE IN CODE.
      Files: src/jobs/canonical_to_gold.py
      Verify: `python scripts/run_local_checks.py` step 1 (py_compile canonical_to_gold) stays PASS;
      `python -m pytest tests/ -q` passes.

### PII mask catalog → config

- [ ] 7. Move the PII mask catalog (which columns, which method, per table) from code into config
      and have `apply_masking` read it; keep the pure masking primitives in code.
      `src/common/masking.py`: remove the hardcoded `PII_COLUMNS` dict literal; instead expose a
      function `pii_columns_for(table, masking_cfg) -> dict[str, str]` that returns
      `masking_cfg["columns"].get(table, {})`, and change `apply_masking(df, table, salt, columns)`
      to accept the per-table column→method mapping as an argument (resolved by the caller from
      config). Keep the pure functions (`mask_name`, `mask_email`, `mask_phone`, `tokenize_id`,
      `generalize_dob`, `mask_pan`) and the `_SIMPLE_METHODS` dispatch UNCHANGED. Preserve the
      exact masking contract (dob→birth_year drop, token in place, etc.).
      `src/jobs/silver_to_canonical.py`: pass the per-table mask columns from `cfg.masking["columns"]`
      into `apply_masking`, and build the lineage `mask:<method>` mappings from that same config
      mapping (replacing `masking.PII_COLUMNS.get(table, {})`).
      Compatibility note for run_local_checks: step 2 asserts `src.common.masking` exposes
      `PII_COLUMNS`. Either (a) keep a module-level `PII_COLUMNS` loaded from config at import —
      REJECTED because masking.py must stay import-safe with no config/PyYAML dependency at import
      time — or (b) update `run_local_checks._COMMON_PUBLIC` for `src.common.masking` to expect
      `pii_columns_for` instead of `PII_COLUMNS`. CHOOSE (b): update the public-attr list in
      `scripts/run_local_checks.py` to `["mask_name", "tokenize_id", "apply_masking",
      "pii_columns_for"]`. Rationale: keeping masking.py free of a config/import-time dependency
      preserves the bare-host import-safety contract; the local-check attr list is the right place
      to reflect the new API.
      Files: src/common/masking.py, src/jobs/silver_to_canonical.py, scripts/run_local_checks.py
      Verify: `python -m pytest tests/test_masking.py -q` (update/extend per item 10) passes;
      `python scripts/run_local_checks.py` steps 2 + 4 PASS.

### LEAVE-IN-CODE decisions (explicit, for the reviewer)

These were evaluated and deliberately NOT moved to config, because externalizing them would hurt
readability — simplicity beats completeness:
- `schema_defs.py` column definitions (name/glue_type/nullable/pii) — SCHEMA CONTRACT, stays in code
  (also a guardrail in the task).
- Gold Spark aggregation bodies (`build_customer_360`, `build_daily_txn_summary`,
  `build_account_balances`) and their representative lineage `add_column_mapping` calls.
- `bronze_to_silver._CODED_COLUMNS`, `_PRIMARY_KEY`, `_REFERENTIAL` — transform wiring, not
  demo-tunable reference data.
- Generator cosmetic value pools (`_FIRST_NAMES`, `_LAST_NAMES`, `_EMAIL_DOMAINS`, `_MERCHANTS`) —
  realism-only, not a shared contract.
- `masking.py` pure masking functions and `_SIMPLE_METHODS` dispatch — the HOW primitives; only the
  what/how CATALOG (which column → which method) becomes config.

---

## GOAL 3 — INSTALL.md + README pointer

- [ ] 8. Write `INSTALL.md` at the repo root — plain, numbered, copy-pasteable, `us-east-1` in
      every example. Audience: new to the project and possibly new to AWS/Spark. Cross-link to the
      existing `docs/` and `README.md` instead of duplicating narrative.
      Section outline (in this order):
      1. Prerequisites — a table with versions + official install links: Python 3.11+ (dev host
         has 3.14; note 3.11 is the EMR target), Terraform (~>1.5 / latest), AWS CLI v2, Java 11/17
         (optional, only for the local Spark smoke), an AWS account with `aws configure` credentials
         and IAM permissions to deploy (S3, Glue, EMR Serverless, Step Functions, Athena, KMS, IAM,
         CloudWatch).
      2. Get the code — clone/unzip, then a short repo-layout table (mirror README's layout table).
      3. Local Python setup with NO AWS — create venv, `pip install -r requirements.txt`, then the
         local checks with EXPECTED success output lines: `python -m data.generator.generate --out
         ./_local_out` (produces customers.csv, accounts.csv, cards.csv, transactions.json),
         `python -m pytest tests/ -q`, `python scripts/run_local_checks.py` (show the summary table
         + `RESULT: OK (mandatory steps passed)`).
      4. Configure the pipeline — point at `config/pipeline.yaml`; explain the key knobs:
         project/env/region (`region: us-east-1`), bucket base names, masking salt (and the
         `PIPELINE_MASK_SALT` env override), DQ severities, the new `reference_data` sets, the new
         `environments` list, `gold_marts`, and `masking.columns`. Then
         `infra/terraform/terraform.tfvars.example` → copy to `terraform.tfvars` (account id,
         `region = "us-east-1"`, tags).
      5. Deploy to AWS with Terraform — `cd infra/terraform`; `terraform init`; `terraform plan`;
         `terraform apply`; list what gets created (KMS, buckets, Glue DBs, EMR Serverless app,
         Step Functions state machine, Athena workgroup, IAM roles). Note it CANNOT run on the
         current dev host (no terraform / no credentials).
      6. Load data + run — `python scripts/upload_landing.py` to push the landing files; start the
         state machine via console or `aws stepfunctions start-execution` using the ARN from
         `terraform output` with `--region us-east-1`; how to watch (Step Functions console +
         CloudWatch log groups).
      7. Query results in Athena — select the `ocbc-poc-dev-wg` workgroup, point at the
         `ocbc_poc_dev_gold` database, run `sql/athena/01`–`04`.
      8. Teardown / cost hygiene — `terraform destroy` (empty S3 buckets first if needed) + a short
         FinOps note (EMR auto-stop, S3 lifecycle, cost-allocation `default_tags`).
      9. Troubleshooting table — bucket name already taken, KMS/IAM AccessDenied, DQ gate halting on
         purpose via `--inject-dq-failure`, Athena results location not set, wrong region.
      Cross-links: `docs/architecture.md`, `docs/finops.md`, `docs/security.md`,
      `docs/lineage_quality_observability.md`, `docs/pii_catalog.md`, `README.md`,
      `infra/terraform/README.md`, `sql/athena/00_setup_notes.md`.
      Files: INSTALL.md (new)
      Verify: manual read-through; confirm every region/ARN/command example uses `us-east-1`
      (grep `INSTALL.md` for `ap-southeast` → zero hits, for `us-east-1` → present). Confirm the
      three local commands in section 3 match the real entry points in the repo.

- [ ] 9. Add a one-line pointer to `INSTALL.md` near the top of `README.md`.
      Insert a short line just under the title/intro, e.g. "New here? Start with
      [`INSTALL.md`](INSTALL.md) for step-by-step setup." Do not restructure the README.
      Files: README.md
      Verify: `README.md` renders with a working relative link to `INSTALL.md`.

---

## Tests covering the new config-driven paths (under tests/)

- [ ] 10. Extend/add unit tests for every new config-driven path. Keep them pure-Python (no Spark).
      `tests/test_config_loader.py`: update `test_loads_real_config` to assert
      `cfg.region == "us-east-1"`, `cfg.environments == ["dev"]`,
      `cfg.reference_data["countries"] == ["SG","US","MY","GB","IN","AU"]` and
      `cfg.reference_data["currencies"]` present, `cfg.gold_marts` has the three mart keys with
      `inputs`, and `cfg.masking["columns"]["customers"]["national_id"] == "token"`. REMOVE the old
      `country_allowed` assertion (that key is deleted in item 5). Add negative tests: a config
      missing `environments` / `reference_data` / `masking.columns` raises a `config_fail`-tagged
      `ConfigError` (mirror the existing `masking.salt` negative test style).
      `tests/test_masking.py`: add a test that `pii_columns_for("customers", masking_cfg)` returns
      the expected column→method mapping from a small in-test masking dict, and that
      `pii_columns_for("accounts", masking_cfg)` is empty. Keep the existing pure-function
      determinism tests unchanged.
      New `tests/test_generator_reference_data.py`: assert `read_reference_data()` reads the real
      `config/pipeline.yaml` and returns non-empty `countries`/`currencies`/`txn_types`, and that a
      `generate(...)` run emits only `country` values within `reference_data.countries` and
      `currency` values within `reference_data.currencies` (read the produced `customers.csv` /
      `accounts.csv`). Clean up the temp output dir in the test.
      New `tests/test_gold_marts_config.py`: assert the mart names and `inputs` the gold job uses
      come from `cfg.gold_marts` — e.g. import `canonical_to_gold`, load the real config, and check
      the configured mart set equals `{"customer_360","daily_txn_summary","account_balances"}` and
      `customer_360` inputs are the four source tables. (Pure config/metadata assertions — do NOT
      invoke Spark builders.)
      Files: tests/test_config_loader.py, tests/test_masking.py, tests/test_generator_reference_data.py,
      tests/test_gold_marts_config.py
      Verify: `python -m pytest tests/ -q` — all tests pass.

---

## Final gate

- [ ] 11. Run the full local gate and confirm green; re-grep for the old region.
      Files: (none — verification only)
      Verify: `python scripts/run_local_checks.py` prints `RESULT: OK (mandatory steps passed)`
      with steps 1-4 PASS (5-6 SKIPPED on this host); `grep -r ap-southeast-1` outside `.agents/`
      returns nothing; `INSTALL.md` exists and uses `us-east-1` throughout. Remove any stray
      `_local_out/` the generator produced (run_local_checks already cleans it up).

---

## Ordering / dependency notes

- Item 2 (config_loader) comes before items 3-7 because they read the new `Config` attributes.
- Item 4 (generator reads reference_data) and item 5 (dq rules consolidation) both touch
  `config/pipeline.yaml`'s reference sets; do item 4 first (adds `reference_data`) then item 5
  (removes the now-duplicate dq literals). Each leaves the repo buildable.
- Item 1 (region) is independent and can be done first; the config_loader test assertion for
  `us-east-1` is tightened in item 10.
- Item 7's change to `run_local_checks._COMMON_PUBLIC` must land WITH item 7 so step 2 stays green.
- Items 8-9 (docs) depend on the final config shape (so the INSTALL knob list is accurate); do them
  after items 2-7. Item 11 is the final green gate.
```
