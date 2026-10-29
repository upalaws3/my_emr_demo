"""schema_defs.py -- the 4 landing tables' column definitions (design S3).

This is the SINGLE place that declares, for each of the four banking tables,
the ordered column list with:

  * ``name``       -- the column name (snake_case),
  * ``glue_type``  -- a Glue/Hive-compatible type STRING that goes straight into
                      ``register_glue_table``'s ``StorageDescriptor.Columns`` with
                      NO translation layer (design S6.1 Finding 6). The spellings
                      here already match Glue/Hive: ``string``, ``int``,
                      ``decimal(p,s)``, ``date``, ``timestamp``.
  * ``nullable``   -- whether the landing contract allows a null/empty value,
  * ``pii``        -- ``True`` when the column is personal data (mirrored into
                      ``docs/pii_catalog.md`` and used by the masking catalog).

These are plain Python structures (stdlib only) so this module imports on a bare
host and is reused by:
  * ``data/generator/generate.py``   -- to know what columns to synthesize,
  * ``src/jobs/landing_to_bronze.py``-- to build the explicit read schema and the
                                        ordered ``(name, glue_type)`` pairs for Glue,
  * ``docs/pii_catalog.md``          -- to list PII columns (authored by hand).

Nothing here imports Spark or boto3.
"""

from __future__ import annotations

from dataclasses import dataclass

# NOTE: the coded-value sets (countries, currencies, account/card/txn enums) used
# to live here as COUNTRIES/CURRENCIES tuples. They are a shared contract with the
# DQ job, so they now live ONCE in config/pipeline.yaml under reference_data and
# are read by data/generator/generate.read_reference_data(). This module keeps
# only the SCHEMA CONTRACT (column names/types/nullable/pii) below.


@dataclass(frozen=True)
class Column:
    """One column of a landing table.

    ``glue_type`` is stored EXACTLY as Glue/Hive expects it so there is no
    translation layer downstream (design S6.1 Finding 6).
    """

    name: str
    glue_type: str
    nullable: bool = False
    pii: bool = False


# --- customers (CSV) -------------------------------------------------------
CUSTOMERS = (
    Column("customer_id", "string", nullable=False, pii=False),
    Column("first_name", "string", nullable=False, pii=True),
    Column("last_name", "string", nullable=False, pii=True),
    Column("email", "string", nullable=False, pii=True),
    Column("phone", "string", nullable=True, pii=True),
    Column("national_id", "string", nullable=False, pii=True),
    Column("date_of_birth", "date", nullable=False, pii=True),
    Column("country", "string", nullable=False, pii=False),
    Column("created_at", "timestamp", nullable=False, pii=False),
)

# --- accounts (CSV) --------------------------------------------------------
ACCOUNTS = (
    Column("account_id", "string", nullable=False, pii=False),
    Column("customer_id", "string", nullable=False, pii=False),
    Column("account_type", "string", nullable=False, pii=False),
    Column("currency", "string", nullable=False, pii=False),
    Column("balance", "decimal(15,2)", nullable=False, pii=False),
    Column("status", "string", nullable=False, pii=False),
    Column("opened_date", "date", nullable=False, pii=False),
)

# --- cards (CSV) -----------------------------------------------------------
CARDS = (
    Column("card_id", "string", nullable=False, pii=False),
    Column("account_id", "string", nullable=False, pii=False),
    Column("card_number", "string", nullable=False, pii=True),
    Column("card_type", "string", nullable=False, pii=False),
    Column("expiry_month", "int", nullable=False, pii=False),
    Column("expiry_year", "int", nullable=False, pii=False),
    Column("cardholder_name", "string", nullable=False, pii=True),
    Column("status", "string", nullable=False, pii=False),
)

# --- transactions (JSON) ---------------------------------------------------
TRANSACTIONS = (
    Column("transaction_id", "string", nullable=False, pii=False),
    Column("account_id", "string", nullable=False, pii=False),
    Column("card_id", "string", nullable=True, pii=False),
    Column("amount", "decimal(15,2)", nullable=False, pii=False),
    Column("currency", "string", nullable=False, pii=False),
    Column("txn_type", "string", nullable=False, pii=False),
    Column("merchant", "string", nullable=True, pii=False),
    Column("txn_timestamp", "timestamp", nullable=False, pii=False),
)


# Landing file FORMAT per table (design S3): customers/accounts/cards are CSV,
# transactions is JSON. landing_to_bronze reads with the right reader per table.
TABLE_FORMATS = {
    "customers": "csv",
    "accounts": "csv",
    "cards": "csv",
    "transactions": "json",
}

# Master registry: table name -> ordered column tuple. The order is the FIXED
# medallion processing order (parents before children) used by the silver job
# and by the generator (parents generated first to guarantee referential FKs).
TABLES: dict[str, tuple[Column, ...]] = {
    "customers": CUSTOMERS,
    "accounts": ACCOUNTS,
    "cards": CARDS,
    "transactions": TRANSACTIONS,
}

# Ordered list of table names (customers -> accounts -> cards -> transactions).
TABLE_ORDER = ("customers", "accounts", "cards", "transactions")


def columns_for(table: str) -> tuple[Column, ...]:
    """Return the ordered column tuple for ``table`` (raises on unknown)."""
    try:
        return TABLES[table]
    except KeyError as exc:
        raise ValueError(f"unknown table {table!r}; expected one of {TABLE_ORDER}") from exc


def glue_columns(table: str) -> list[tuple[str, str]]:
    """Return ordered ``(name, glue_type)`` pairs for ``register_glue_table``.

    These go straight into ``StorageDescriptor.Columns`` with no translation
    (design S6.1 Finding 6).
    """
    return [(c.name, c.glue_type) for c in columns_for(table)]


def pii_columns(table: str) -> list[str]:
    """Return the names of the PII columns for ``table`` (for the PII catalog)."""
    return [c.name for c in columns_for(table) if c.pii]
