"""Unit tests for the generator's config-driven reference-data path.

The coded-value sets (countries/currencies/etc.) live ONCE in
config/pipeline.yaml under reference_data. The stdlib-only generator reads them
with its narrow YAML scanner (no PyYAML) via read_reference_data(), and every
generated row must only use values from those sets.
"""

import csv
import shutil

from data.generator.generate import generate, read_reference_data


def test_read_reference_data_returns_expected_sets():
    ref = read_reference_data()
    # countries + currencies are the two sets every table depends on.
    assert ref["countries"]
    assert ref["currencies"]
    # the enum sets the generator draws from are present too.
    assert ref["txn_types"]
    assert set(ref["txn_types"]) == {"DEBIT", "CREDIT"}


def test_generated_rows_only_use_configured_reference_values(tmp_path):
    ref = read_reference_data()
    out = tmp_path / "gen"
    try:
        generate(str(out), inject_dq_failure=False, seed=7)

        with (out / "customers.csv").open(encoding="utf-8") as fh:
            countries = {row["country"] for row in csv.DictReader(fh)}
        assert countries
        assert countries <= set(ref["countries"])

        with (out / "accounts.csv").open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        currencies = {row["currency"] for row in rows}
        statuses = {row["status"] for row in rows}
        assert currencies <= set(ref["currencies"])
        assert statuses <= set(ref["account_statuses"])
    finally:
        shutil.rmtree(out, ignore_errors=True)
