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
    "environments",
    "buckets",
    "databases",
    "masking",
    "reference_data",
    "gold_marts",
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
    # Allowed --env values (jobs read this for their argparse choices).
    environments: list
    buckets: dict[str, Any]
    databases: dict[str, Any]
    masking: dict[str, Any]
    # Single source of truth for coded-value sets (countries/currencies/etc.).
    reference_data: dict[str, Any]
    # Declarative gold-mart metadata (mart name -> {inputs: [...]}).
    gold_marts: dict[str, Any]
    dq_rules: dict[str, Any]
    generator: dict[str, Any]
    # The whole parsed mapping, kept so callers can reach anything not promoted
    # to a first-class attribute without re-reading the file.
    raw: dict[str, Any]


def _resolve_config_path(path: str) -> str:
    """Resolve the config file location across local + EMR Serverless runtimes.

    Resolution order (first hit wins):
      1. The ``PIPELINE_CONFIG_PATH`` env var, if set (explicit override).
      2. ``path`` as given, if it exists relative to the current working dir
         (the local/dev case: run from the repo root).
      3. ``config/pipeline.yaml`` discovered RELATIVE TO THIS PACKAGE. On EMR
         Serverless the job runs from a staging dir (no repo-relative
         ``config/``), but the deploy zips ``config/`` alongside ``src/`` and
         ships it via spark.submit.pyFiles, so the file lands next to the
         unpacked ``src`` package. We walk up from this module to find it.

    Returns the first path that exists; otherwise returns ``path`` unchanged so
    the caller's existing "could not read" error still fires with a clear msg.
    """
    import os
    import pathlib

    override = os.environ.get("PIPELINE_CONFIG_PATH")
    if override:
        return override

    if pathlib.Path(path).exists():
        return path

    # Walk up from src/common/config_loader.py looking for config/pipeline.yaml.
    # Covers both the repo layout and the EMR-unzipped layout (where src/ and
    # config/ are unpacked as siblings into the job's working dir).
    here = pathlib.Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "config" / "pipeline.yaml"
        if candidate.exists():
            return str(candidate)

    return path


def load_config(path: str = "config/pipeline.yaml") -> Config:
    """Read, parse, and validate the pipeline config.

    Parameters
    ----------
    path:
        Path to the YAML file. Defaults to the repo-relative canonical location.
        At runtime the actual file is located via ``_resolve_config_path`` so it
        works both locally (run from repo root) and on EMR Serverless (where
        ``config/`` is shipped in the pyFiles zip). Set ``PIPELINE_CONFIG_PATH``
        to override explicitly.

    Returns
    -------
    Config
        A validated config object.

    Raises
    ------
    ConfigError
        If the file cannot be read/parsed, a required top-level key is missing,
        the required ``masking.salt`` / ``masking.columns`` sub-keys are absent,
        ``environments`` is not a non-empty list, or ``reference_data`` is
        missing a non-empty ``countries`` / ``currencies`` list. Every message
        is tagged ``event=config_fail``.
    """
    # Lazy imports: keep the module importable on a host without PyYAML and keep
    # file I/O out of import time.
    import pathlib

    import yaml

    resolved = _resolve_config_path(path)

    try:
        text = pathlib.Path(resolved).read_text(encoding="utf-8")
    except OSError as exc:  # file missing / unreadable
        raise ConfigError(
            f"event=config_fail could not read config at {resolved!r}: {exc}"
        ) from exc

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
    # masking.columns is the what/how-to-mask catalog (design S4). It must be a
    # mapping (may be empty) so apply_masking can look up a per-table policy.
    if not isinstance(masking.get("columns"), dict):
        raise ConfigError("event=config_fail missing required config key: masking.columns")

    # environments drives the job --env allow-list; it must be a non-empty list.
    environments = data["environments"]
    if not isinstance(environments, list) or not environments:
        raise ConfigError("event=config_fail config key environments must be a non-empty list")

    # reference_data is the single source of truth for coded-value sets. The two
    # sets every table depends on (countries + currencies) must be non-empty lists.
    reference_data = data["reference_data"]
    if not isinstance(reference_data, dict):
        raise ConfigError("event=config_fail config key reference_data must be a mapping")
    for ref_key in ("countries", "currencies"):
        value = reference_data.get(ref_key)
        if not isinstance(value, list) or not value:
            raise ConfigError(
                f"event=config_fail reference_data.{ref_key} must be a non-empty list"
            )

    return Config(
        project=data["project"],
        env=data["env"],
        region=data["region"],
        environments=environments,
        buckets=data["buckets"],
        databases=data["databases"],
        masking=masking,
        reference_data=reference_data,
        gold_marts=data["gold_marts"],
        dq_rules=data["dq_rules"],
        # generator is optional at the schema level but present in the baseline;
        # default to an empty dict so .get("row_counts") stays safe.
        generator=data.get("generator") or {},
        raw=data,
    )
