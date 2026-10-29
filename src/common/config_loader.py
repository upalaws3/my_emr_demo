"""config_loader.py -- load and validate ``config/pipeline.yaml``.

``config/pipeline.yaml`` (design S2/S3/S6.5) is the single source of truth for
names, buckets, the masking salt, generator row-count ranges, and DQ rules.
This module turns that YAML into a small typed-ish ``Config`` object and fails
loudly (``event=config_fail``) when a required key is missing.

IMPORT-SAFE CONTRACT: ``import yaml`` happens INSIDE ``load_config`` (call
time), never at module import time, so this module byte-compiles and imports on
a bare host where PyYAML may be absent.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Top-level keys every config MUST provide. A missing one is a fatal startup
# error -- there is no sensible default for any of them.
_REQUIRED_TOP_KEYS = (
    "project",
    "env",
    "region",
    "buckets",
    "databases",
    "masking",
    "dq_rules",
)


class ConfigError(Exception):
    """Raised when the pipeline config is missing a required key.

    The string always carries ``event=config_fail`` so structured-log scrapers
    and the design's S10 error table can key on a single stable token.
    """


@dataclass(frozen=True)
class Config:
    """Parsed, validated view of ``config/pipeline.yaml``.

    Attributes mirror the YAML top-level keys. ``generator`` is a plain dict and
    exposes ``generator["row_counts"]`` ({table: [min, max]}), read by both the
    data generator and the local-check assertion.
    """

    project: str
    env: str
    region: str
    buckets: dict[str, Any]
    databases: dict[str, Any]
    masking: dict[str, Any]
    dq_rules: dict[str, Any]
    generator: dict[str, Any]
    # The whole parsed mapping, kept so callers can reach anything not promoted
    # to a first-class attribute without re-reading the file.
    raw: dict[str, Any]


def load_config(path: str = "config/pipeline.yaml") -> Config:
    """Read, parse, and validate the pipeline config.

    Parameters
    ----------
    path:
        Path to the YAML file. Defaults to the repo-relative canonical location.

    Returns
    -------
    Config
        A validated config object.

    Raises
    ------
    ConfigError
        If the file cannot be read/parsed, a required top-level key is missing,
        or the required ``masking.salt`` sub-key is absent. Every message is
        tagged ``event=config_fail``.
    """
    # Lazy imports: keep the module importable on a host without PyYAML and keep
    # file I/O out of import time.
    import pathlib

    import yaml

    try:
        text = pathlib.Path(path).read_text(encoding="utf-8")
    except OSError as exc:  # file missing / unreadable
        raise ConfigError(f"event=config_fail could not read config at {path!r}: {exc}") from exc

    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"event=config_fail invalid YAML in {path!r}: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError(f"event=config_fail config root in {path!r} is not a mapping")

    # Validate required top-level keys.
    missing = [key for key in _REQUIRED_TOP_KEYS if key not in data or data[key] is None]
    if missing:
        raise ConfigError(
            f"event=config_fail missing required config key(s): {', '.join(sorted(missing))}"
        )

    # Validate the required masking.salt sub-key specifically (design S4): the
    # salt is never defaulted; an empty/absent salt is a hard startup failure.
    masking = data["masking"]
    if not isinstance(masking, dict) or not masking.get("salt"):
        raise ConfigError("event=config_fail missing required config key: masking.salt")

    return Config(
        project=data["project"],
        env=data["env"],
        region=data["region"],
        buckets=data["buckets"],
        databases=data["databases"],
        masking=masking,
        dq_rules=data["dq_rules"],
        # generator is optional at the schema level but present in the baseline;
        # default to an empty dict so .get("row_counts") stays safe.
        generator=data.get("generator") or {},
        raw=data,
    )
