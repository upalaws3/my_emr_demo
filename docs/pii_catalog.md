# PII Catalog & Masking

This catalog lists every personal-data column across the four landing tables and
the masking method applied to it. It mirrors design §4 and the `pii` flags in
`data/generator/schema_defs.py` (the single source of truth for which columns are
PII). **Masking happens only in the `silver → canonical` job**
(`src/common/masking.py` + `silver_to_canonical.py`): silver keeps
cleansed-but-unmasked data so DQ and lineage see real keys; canonical is the
first masked layer; gold inherits masked values.

## PII columns and masking methods

| Table | Column | PII type | Masking method |
| --- | --- | --- | --- |
| customers | `first_name` | name | first char + `***` (e.g. `A***`) |
| customers | `last_name` | name | first char + `***` |
| customers | `email` | email | partial: first char of local part + `***@` + domain (`a***@x.com`) |
| customers | `phone` | phone | keep last 2 digits, `****` prefix |
| customers | `national_id` | government id | SHA-256(`salt` + value) UTF-8 → lowercase hex → first 16 chars (tokenized, in place under `national_id`) |
| customers | `date_of_birth` | quasi-identifier | generalize to birth **year** — `date_of_birth` is **replaced by** `birth_year` (int) |
| cards | `card_number` | PAN | keep last 4, mask the rest → `**** **** **** 1234` |
| cards | `cardholder_name` | name | first char + `***` |

(`accounts` and `transactions` carry no PII columns.)

## Naming after masking

- In **canonical**, masked columns keep their **original name and type** — there
  is no `_masked` suffix (design §6.3). `national_id` retains its name and holds
  the 16-char token; `date_of_birth` is replaced by `birth_year:int`.
- The `_masked` suffix appears **only in the gold `customer_360` mart**
  (`first_name_masked`, `last_name_masked`, `email_masked`), which simply renames
  the already-masked canonical columns for a self-describing analytics schema.
- In gold `account_balances`, the tokenized `national_id` surfaces as
  `customer_token`.

## Tokenization contract (`national_id`)

`tokenize_id(value, salt)` computes `sha256((salt + value).encode("utf-8"))`,
takes the lowercase hex digest, and returns its **first 16 characters**. Key
properties:

- **salt-prefix order** (`salt + value`, not `value + salt`) and **UTF-8**
  encoding are fixed, so the token is deterministic and pinned by a unit test
  against a precomputed literal (design §14).
- **Fixed salt** means the same customer maps to the same token across runs and
  across tables, so the token works as a stable join key (gold
  `account_balances.customer_token`) without exposing the raw id.
- **Salt precedence:** env `PIPELINE_MASK_SALT` > `config.masking.salt` > **hard
  startup fail** (never defaulted). The salt lives in `config/pipeline.yaml`
  under `masking.salt`.

## Masking is one-way

All methods are one-way (hash or partial-mask); there is no reversible
tokenization. A production system would use a KMS-backed key or a dedicated
tokenization vault — explicitly **out of scope** for this baseline (see
`security.md`). The `national_id` token is a quasi-join-key within `customers`
only, so stable one-way tokenization is sufficient for the demo.
