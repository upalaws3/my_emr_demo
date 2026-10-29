"""generate.py -- synthetic landing-layer data generator (STDLIB ONLY).

Produces the four banking tables as landing files for the POC demo:

    customers.csv   accounts.csv   cards.csv   transactions.json

Usage::

    python -m data.generator.generate --out ./_local_out [--inject-dq-failure]

Design references: S3 (schemas + PII), S6.5/S14 (the --inject-dq-failure
contract), Finding 8 (row-count ranges live in config/pipeline.yaml and are read
by BOTH this generator and the S13 step-3 assertion).

Hard constraints:
  * STDLIB ONLY -- no PyYAML, no boto3, no Spark. This runs on a bare Python 3.11+
    host. The config row-count ranges are read with a tiny hand-rolled YAML
    scan (just the ``generator.row_counts`` block) so we do NOT depend on PyYAML.
  * Referential integrity is GUARANTEED by construction: every child FK is drawn
    from an already-generated parent (accounts.customer_id from customers;
    cards.account_id and transactions.account_id from accounts;
    transactions.card_id -- nullable -- from cards).
  * All PII columns are populated with realistic-looking values.

Determinism: a fixed RNG seed makes a run reproducible, which keeps the demo and
the local-check assertions stable. ``--seed`` overrides it.
"""

from __future__ import annotations

import argparse
import csv
import json
import pathlib
import random
import re
import sys
from datetime import date, datetime, timedelta

from . import schema_defs

# --- Small fixed vocabularies for realistic-looking synthetic values -------
_FIRST_NAMES = (
    "Alice", "Bob", "Chen", "Divya", "Ethan", "Farah", "Grace", "Hiroshi",
    "Isabel", "Jamal", "Keiko", "Liam", "Mei", "Nadia", "Omar", "Priya",
    "Quinn", "Ravi", "Sofia", "Tariq", "Uma", "Viktor", "Wei", "Xenia",
    "Yusuf", "Zara",
)
_LAST_NAMES = (
    "Tan", "Lim", "Kumar", "Smith", "Nguyen", "Garcia", "Khan", "Wong",
    "Patel", "Johnson", "Suzuki", "Ahmed", "Lee", "Brown", "Singh", "Chan",
)
_EMAIL_DOMAINS = ("example.com", "mail.test", "bankdemo.io", "sample.org")
_MERCHANTS = (
    "FairPrice", "Amazon", "Shell", "Starbucks", "Grab", "Netflix", "Uber",
    "Apple", "Lazada", "IKEA", None,  # None -> nullable merchant left blank
)
_ACCOUNT_TYPES = ("SAVINGS", "CHECKING", "CREDIT")
_ACCOUNT_STATUSES = ("ACTIVE", "DORMANT", "CLOSED")
_CARD_TYPES = ("VISA", "MASTERCARD", "AMEX")
_CARD_STATUSES = ("ACTIVE", "BLOCKED", "EXPIRED")
_TXN_TYPES = ("DEBIT", "CREDIT")


# ---------------------------------------------------------------------------
# Config: read generator.row_counts ranges WITHOUT PyYAML (stdlib only).
# ---------------------------------------------------------------------------
def _config_path() -> pathlib.Path:
    """Return the repo-relative path to config/pipeline.yaml.

    This file lives at ``<repo>/data/generator/generate.py``; the config is at
    ``<repo>/config/pipeline.yaml``. We resolve it relative to this module so the
    generator works regardless of the current working directory.
    """
    here = pathlib.Path(__file__).resolve()
    repo_root = here.parents[2]  # data/generator/ -> data/ -> <repo>
    return repo_root / "config" / "pipeline.yaml"


def read_row_count_ranges() -> dict[str, tuple[int, int]]:
    """Parse ONLY the ``generator.row_counts`` block from config/pipeline.yaml.

    We deliberately avoid PyYAML (keeping the generator stdlib-only) with a
    narrow, forgiving scan: find the ``row_counts:`` line, then read the indented
    ``<table>: [min, max]`` entries beneath it. This is the SAME source the S13
    step-3 assertion uses, so the assertion stays deterministic (Finding 8).
    """
    text = _config_path().read_text(encoding="utf-8")
    lines = text.splitlines()

    ranges: dict[str, tuple[int, int]] = {}
    in_row_counts = False
    row_counts_indent = 0

    # Matches e.g. "    customers: [90, 110]" capturing name, min, max.
    entry_re = re.compile(r"^(\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:\s*\[\s*(\d+)\s*,\s*(\d+)\s*\]")

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if not in_row_counts:
            # Look for the "row_counts:" key (under "generator:").
            if re.match(r"^\s*row_counts\s*:\s*$", line):
                in_row_counts = True
                row_counts_indent = len(line) - len(line.lstrip())
            continue

        # We are inside the row_counts block. An entry must be MORE indented than
        # the "row_counts:" key; a line at or below that indent ends the block.
        indent = len(line) - len(line.lstrip())
        if indent <= row_counts_indent:
            break

        m = entry_re.match(line)
        if m:
            name = m.group(2)
            lo, hi = int(m.group(3)), int(m.group(4))
            ranges[name] = (lo, hi)

    # Sanity: every table we generate must have a configured range.
    for table in schema_defs.TABLE_ORDER:
        if table not in ranges:
            raise ValueError(
                f"generator.row_counts missing range for {table!r} in {_config_path()}"
            )
    return ranges


# ---------------------------------------------------------------------------
# Per-table row generators. Each returns a list of dicts keyed by column name.
# Parents are generated first so children can draw FKs from them.
# ---------------------------------------------------------------------------
def _rand_date(rng: random.Random, start_year: int, end_year: int) -> date:
    """Random date between Jan 1 start_year and Dec 31 end_year (inclusive-ish)."""
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    delta_days = (end - start).days
    return start + timedelta(days=rng.randint(0, delta_days))


def _rand_timestamp(rng: random.Random, start_year: int, end_year: int) -> datetime:
    """Random timestamp between start_year and end_year (second precision)."""
    d = _rand_date(rng, start_year, end_year)
    return datetime(d.year, d.month, d.day, rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59))


def gen_customers(rng: random.Random, n: int) -> list[dict]:
    """Generate ``n`` customers with all PII columns populated (design S3.1)."""
    rows = []
    for i in range(1, n + 1):
        first = rng.choice(_FIRST_NAMES)
        last = rng.choice(_LAST_NAMES)
        # national_id looks like a Singapore-style NRIC: letter + 7 digits + letter.
        national_id = f"S{rng.randint(1000000, 9999999)}{rng.choice('ABCDEFGHJKLMNPQRTUWXZ')}"
        rows.append(
            {
                "customer_id": f"CUST{i:06d}",
                "first_name": first,
                "last_name": last,
                # Unique-ish email so email-not-null + realism both hold.
                "email": f"{first.lower()}.{last.lower()}{i}@{rng.choice(_EMAIL_DOMAINS)}",
                "phone": f"+65{rng.randint(80000000, 99999999)}",
                "national_id": national_id,
                "date_of_birth": _rand_date(rng, 1955, 2004).isoformat(),
                "country": rng.choice(schema_defs.COUNTRIES),
                "created_at": _rand_timestamp(rng, 2020, 2023).isoformat(sep=" "),
            }
        )
    return rows


def gen_accounts(rng: random.Random, n: int, customers: list[dict]) -> list[dict]:
    """Generate ``n`` accounts; each customer_id drawn from existing customers.

    To satisfy "at least 1 account per customer" we first give every customer one
    account, then distribute the remaining accounts randomly across customers.
    """
    customer_ids = [c["customer_id"] for c in customers]
    rows = []
    # Guarantee >= 1 account per customer where possible.
    assigned = list(customer_ids)
    # Fill the rest (n - len(customers)) with random customers.
    while len(assigned) < n:
        assigned.append(rng.choice(customer_ids))
    # If n < number of customers (shouldn't happen per ranges), trim.
    assigned = assigned[:n]
    rng.shuffle(assigned)

    for i, cust_id in enumerate(assigned, start=1):
        rows.append(
            {
                "account_id": f"ACC{i:08d}",
                "customer_id": cust_id,
                "account_type": rng.choice(_ACCOUNT_TYPES),
                "currency": rng.choice(schema_defs.CURRENCIES),
                # balance as a 2dp decimal string (decimal(15,2)).
                "balance": f"{rng.uniform(-5000, 250000):.2f}",
                "status": rng.choice(_ACCOUNT_STATUSES),
                "opened_date": _rand_date(rng, 2015, 2023).isoformat(),
            }
        )
    return rows


def gen_cards(rng: random.Random, n: int, accounts: list[dict]) -> list[dict]:
    """Generate ``n`` cards; each account_id drawn from existing accounts.

    Not every account has a card (ranges keep cards < accounts), so we simply
    draw a random account per card.
    """
    account_ids = [a["account_id"] for a in accounts]
    rows = []
    for i in range(1, n + 1):
        # 16-digit PAN as a string; masking later keeps only the last 4.
        pan = "".join(str(rng.randint(0, 9)) for _ in range(16))
        rows.append(
            {
                "card_id": f"CARD{i:08d}",
                "account_id": rng.choice(account_ids),
                "card_number": pan,
                "card_type": rng.choice(_CARD_TYPES),
                "expiry_month": rng.randint(1, 12),
                "expiry_year": rng.randint(2026, 2031),
                "cardholder_name": f"{rng.choice(_FIRST_NAMES)} {rng.choice(_LAST_NAMES)}",
                "status": rng.choice(_CARD_STATUSES),
            }
        )
    return rows


def gen_transactions(
    rng: random.Random, n: int, accounts: list[dict], cards: list[dict]
) -> list[dict]:
    """Generate ``n`` transactions; account_id from accounts, card_id from cards.

    ``card_id`` is NULLABLE (design S3.3): ~30% of transactions carry no card.
    An ``_ingested_at`` is NOT part of the landing contract -- the bronze job adds
    it -- so the generator does not emit it here. The --inject-dq-failure path
    (handled by the caller) plants the duplicate using the landing-visible
    ``transaction_id`` + a fixed amount difference; the matching ``_ingested_at``
    collision is produced deterministically in bronze because both planted rows
    share the same source file + ingest run (documented in S6.5/S14).
    """
    account_ids = [a["account_id"] for a in accounts]
    card_ids = [c["card_id"] for c in cards]
    rows = []
    for i in range(1, n + 1):
        has_card = bool(card_ids) and rng.random() > 0.3
        amount = rng.uniform(-2000, 5000)
        rows.append(
            {
                "transaction_id": f"TXN{i:010d}",
                "account_id": rng.choice(account_ids),
                "card_id": rng.choice(card_ids) if has_card else None,
                "amount": f"{amount:.2f}",
                "currency": rng.choice(schema_defs.CURRENCIES),
                "txn_type": rng.choice(_TXN_TYPES),
                "merchant": rng.choice(_MERCHANTS),
                "txn_timestamp": _rand_timestamp(rng, 2023, 2024).isoformat(sep=" "),
            }
        )
    return rows


def _inject_transactions_dq_failure(transactions: list[dict]) -> list[dict]:
    """Plant the deterministic DQ violation (design S6.5/S14 Finding 7).

    Appends ONE extra transactions row that DUPLICATES the first row's
    ``transaction_id`` but differs in a single non-key column (``amount``:
    100.00 vs 100.01). Because the two rows differ on a non-key column, the
    silver job's full-row ``distinct()`` keeps BOTH, and because they share the
    PK (and -- once bronze stamps it -- the same ``_ingested_at``) the PK dedupe
    cannot disambiguate them. The surviving duplicate trips the critical
    ``transaction_id_unique`` expectation, firing the gate deterministically.

    This is the ONE extra row beyond the configured range that the S13 step-3
    count check tolerates when the flag is set.
    """
    if not transactions:
        return transactions
    original = transactions[0]
    # Force the pair to the pinned amounts so the non-key difference is explicit.
    original["amount"] = "100.00"
    planted = dict(original)
    planted["amount"] = "100.01"  # differs only in this non-key column
    transactions.append(planted)
    return transactions


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------
def _write_csv(path: pathlib.Path, table: str, rows: list[dict]) -> None:
    """Write ``rows`` to ``path`` as CSV with a header in schema column order."""
    header = [c.name for c in schema_defs.columns_for(table)]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_json(path: pathlib.Path, rows: list[dict]) -> None:
    """Write ``rows`` to ``path`` as a JSON array (transactions)."""
    path.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def generate(out_dir: str, inject_dq_failure: bool = False, seed: int = 42) -> dict[str, int]:
    """Generate all four tables into ``out_dir``; return the row count per table.

    The counts returned EXCLUDE nothing -- for transactions with
    ``inject_dq_failure`` the returned count is range+1 (the planted row), which
    the S13 step-3 assertion tolerates only when the flag is set.
    """
    rng = random.Random(seed)
    ranges = read_row_count_ranges()

    out = pathlib.Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Draw a count uniformly within each configured [min, max] range.
    n_customers = rng.randint(*ranges["customers"])
    n_accounts = rng.randint(*ranges["accounts"])
    n_cards = rng.randint(*ranges["cards"])
    n_transactions = rng.randint(*ranges["transactions"])

    # Generate parents BEFORE children to guarantee referential integrity.
    customers = gen_customers(rng, n_customers)
    accounts = gen_accounts(rng, n_accounts, customers)
    cards = gen_cards(rng, n_cards, accounts)
    transactions = gen_transactions(rng, n_transactions, accounts, cards)

    if inject_dq_failure:
        transactions = _inject_transactions_dq_failure(transactions)

    # Write landing files in their contracted formats (design S3).
    _write_csv(out / "customers.csv", "customers", customers)
    _write_csv(out / "accounts.csv", "accounts", accounts)
    _write_csv(out / "cards.csv", "cards", cards)
    _write_json(out / "transactions.json", transactions)

    return {
        "customers": len(customers),
        "accounts": len(accounts),
        "cards": len(cards),
        "transactions": len(transactions),
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="data.generator.generate",
        description="Generate synthetic banking landing data (stdlib only).",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output directory for the generated landing files.",
    )
    parser.add_argument(
        "--inject-dq-failure",
        action="store_true",
        help="Plant a duplicate transactions row so the silver DQ gate fires (S6.5).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for reproducible output (default: 42).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Prints the per-table counts and returns exit code 0."""
    args = _parse_args(argv)
    counts = generate(args.out, inject_dq_failure=args.inject_dq_failure, seed=args.seed)
    # Print a short, parseable summary so run_local_checks / humans can read it.
    print(f"generated landing data into {args.out}:")
    for table in schema_defs.TABLE_ORDER:
        print(f"  {table}: {counts[table]} rows")
    if args.inject_dq_failure:
        print("  (--inject-dq-failure: transactions carries one planted duplicate)")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised via CLI
    sys.exit(main())
