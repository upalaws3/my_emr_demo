"""Unit tests for the config-driven gold-mart metadata.

Only the DECLARATIVE bits are config-driven (design S6.4): the list of marts and
each mart's lineage input tables. The Spark aggregation bodies stay in Python
and are NOT exercised here (no Spark on the bare host).
"""

from src.common import config_loader
from src.jobs import canonical_to_gold

REAL_CONFIG = "config/pipeline.yaml"


def test_gold_marts_come_from_config():
    assert set(canonical_to_gold.gold_marts()) == {
        "customer_360",
        "daily_txn_summary",
        "account_balances",
    }


def test_mart_inputs_come_from_config():
    # customer_360 reads all four source tables for lineage.
    assert set(canonical_to_gold._mart_inputs("customer_360")) == {
        "customers",
        "accounts",
        "cards",
        "transactions",
    }
    assert canonical_to_gold._mart_inputs("daily_txn_summary") == ["transactions"]


def test_mart_builders_cover_every_configured_mart():
    # Every configured mart name must have a Python builder function.
    cfg = config_loader.load_config(REAL_CONFIG)
    assert set(cfg.gold_marts) == set(canonical_to_gold._MART_BUILDERS)
