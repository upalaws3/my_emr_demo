"""Unit tests for src/common/config_loader.py.

Loads the real config/pipeline.yaml and checks required-key validation
(a config missing masking.salt raises a config_fail-tagged ConfigError).
"""

import pathlib

import pytest

from src.common import config_loader

REAL_CONFIG = "config/pipeline.yaml"


def test_loads_real_config():
    cfg = config_loader.load_config(REAL_CONFIG)
    assert cfg.project == "ocbc-poc"
    assert cfg.env == "dev"
    assert cfg.region
    assert cfg.masking["salt"]
    # generator.row_counts is read by both the generator and the local check.
    assert cfg.generator["row_counts"]["transactions"] == [700, 900]
    # country_allowed lives in config, not the job.
    assert cfg.dq_rules["customers"]["country_allowed"] == ["SG", "US", "MY", "GB", "IN", "AU"]


def test_missing_masking_salt_raises_config_fail(tmp_path):
    # A config that has every top-level key but no masking.salt must fail hard.
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "project: p\n"
        "env: dev\n"
        "region: r\n"
        "buckets: {datalake: d, athena_results: a}\n"
        "databases: {bronze: b}\n"
        "masking: {}\n"  # <-- no salt
        "dq_rules: {}\n",
        encoding="utf-8",
    )
    with pytest.raises(config_loader.ConfigError) as exc:
        config_loader.load_config(str(bad))
    assert "config_fail" in str(exc.value)
    assert "masking.salt" in str(exc.value)


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
