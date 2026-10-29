"""Unit tests for src/common/config_loader.py.

Loads the real config/pipeline.yaml and checks required-key validation
(a config missing masking.salt raises a config_fail-tagged ConfigError).
"""

import pathlib

import pytest

from src.common import config_loader

REAL_CONFIG = "config/pipeline.yaml"


# A minimal-but-complete config body (every required top-level key present) that
# individual negative tests selectively break by removing one section.
def _good_config_lines() -> dict[str, str]:
    return {
        "project": "project: p",
        "env": "env: dev",
        "region": "region: us-east-1",
        "environments": "environments: [dev]",
        "buckets": "buckets: {datalake: d, athena_results: a}",
        "databases": "databases: {bronze: b}",
        "masking": "masking: {salt: s, columns: {customers: {email: email}}}",
        "reference_data": "reference_data: {countries: [SG], currencies: [SGD]}",
        "gold_marts": "gold_marts: {customer_360: {inputs: [customers]}}",
        "dq_rules": "dq_rules: {}",
    }


def _write_config(tmp_path, sections: dict[str, str]):
    body = "\n".join(sections.values()) + "\n"
    path = tmp_path / "cfg.yaml"
    path.write_text(body, encoding="utf-8")
    return str(path)


def test_loads_real_config():
    cfg = config_loader.load_config(REAL_CONFIG)
    assert cfg.project == "ocbc-poc"
    assert cfg.env == "dev"
    # Region is config-driven and now pinned to us-east-1 for the demo.
    assert cfg.region == "us-east-1"
    # Allowed envs drive the job --env choices.
    assert cfg.environments == ["dev"]
    assert cfg.masking["salt"]
    # generator.row_counts is read by both the generator and the local check.
    assert cfg.generator["row_counts"]["transactions"] == [700, 900]
    # reference_data is the single source of truth for coded-value sets.
    assert cfg.reference_data["countries"] == ["SG", "US", "MY", "GB", "IN", "AU"]
    assert cfg.reference_data["currencies"]
    # gold_marts carries the three marts + their lineage input tables.
    assert set(cfg.gold_marts) == {"customer_360", "daily_txn_summary", "account_balances"}
    assert cfg.gold_marts["customer_360"]["inputs"]
    # masking.columns is the config-driven PII catalog.
    assert cfg.masking["columns"]["customers"]["national_id"] == "token"


def test_missing_masking_salt_raises_config_fail(tmp_path):
    # Every top-level key present, but masking has no salt -> hard fail.
    sections = _good_config_lines()
    sections["masking"] = "masking: {columns: {}}"  # <-- no salt
    with pytest.raises(config_loader.ConfigError) as exc:
        config_loader.load_config(_write_config(tmp_path, sections))
    assert "config_fail" in str(exc.value)
    assert "masking.salt" in str(exc.value)


def test_missing_masking_columns_raises_config_fail(tmp_path):
    sections = _good_config_lines()
    sections["masking"] = "masking: {salt: s}"  # <-- no columns
    with pytest.raises(config_loader.ConfigError) as exc:
        config_loader.load_config(_write_config(tmp_path, sections))
    assert "config_fail" in str(exc.value)
    assert "masking.columns" in str(exc.value)


def test_missing_environments_raises_config_fail(tmp_path):
    sections = _good_config_lines()
    sections["environments"] = "environments: []"  # <-- empty list
    with pytest.raises(config_loader.ConfigError) as exc:
        config_loader.load_config(_write_config(tmp_path, sections))
    assert "config_fail" in str(exc.value)
    assert "environments" in str(exc.value)


def test_missing_reference_data_raises_config_fail(tmp_path):
    sections = _good_config_lines()
    sections["reference_data"] = "reference_data: {currencies: [SGD]}"  # <-- no countries
    with pytest.raises(config_loader.ConfigError) as exc:
        config_loader.load_config(_write_config(tmp_path, sections))
    assert "config_fail" in str(exc.value)
    assert "reference_data.countries" in str(exc.value)


def test_missing_top_level_key_raises_config_fail(tmp_path):
    bad = tmp_path / "bad2.yaml"
    bad.write_text("project: p\nenv: dev\n", encoding="utf-8")  # almost everything missing
    with pytest.raises(config_loader.ConfigError) as exc:
        config_loader.load_config(str(bad))
    assert "config_fail" in str(exc.value)


def test_missing_file_raises_config_fail():
    with pytest.raises(config_loader.ConfigError) as exc:
        config_loader.load_config(str(pathlib.Path("does-not-exist.yaml")))
    assert "config_fail" in str(exc.value)
