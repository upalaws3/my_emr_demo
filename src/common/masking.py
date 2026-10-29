"""masking.py -- PII masking primitives + a Spark DataFrame wrapper.

The pure-Python functions here implement each masking method from design S4 and
are fully unit-testable on the bare host (no Spark). ``apply_masking`` is the
only Spark-touching helper and imports ``pyspark`` lazily so this module
compiles/imports without pyspark present.

Masking happens ONLY in the silver -> canonical job (design S4/S6.3). Methods
that protect a join key (``tokenize_id``) are deterministic so the same input
always yields the same token across runs.
"""

from __future__ import annotations

import hashlib


def pii_columns_for(table: str, masking_cfg: dict) -> dict[str, str]:
    """Return the per-table column -> masking-method map from config (design S4).

    The what/how-to-mask POLICY lives ONCE in config/pipeline.yaml under
    ``masking.columns`` (the schema contract stays in schema_defs.py). This
    module intentionally holds NO hardcoded catalog so it stays import-safe with
    no config/PyYAML dependency at import time; the caller passes ``masking_cfg``
    (i.e. ``cfg.masking``) resolved from config. Tables with no PII return ``{}``.
    """
    return dict((masking_cfg or {}).get("columns", {}).get(table, {}) or {})


def mask_name(v: str | None) -> str | None:
    """First character + ``***`` (e.g. ``"Alice" -> "A***"``).

    ``None``/empty passes through unchanged (nothing to mask).
    """
    if not v:
        return v
    return f"{v[0]}***"


def mask_email(v: str | None) -> str | None:
    """Partial-mask the local part: ``"alice@x.com" -> "a***@x.com"``.

    A value without ``@`` is treated as a bare local part and masked like a name.
    """
    if not v:
        return v
    if "@" not in v:
        return f"{v[0]}***"
    local, domain = v.split("@", 1)
    local_masked = f"{local[0]}***" if local else "***"
    return f"{local_masked}@{domain}"


def mask_phone(v: str | None) -> str | None:
    """Keep the last 2 characters, mask the rest with ``****``.

    ``"+6591234567" -> "****67"``. Values of length <= 2 are returned as-is.
    """
    if not v:
        return v
    if len(v) <= 2:
        return v
    return f"****{v[-2:]}"


def tokenize_id(v: str | None, salt: str) -> str | None:
    """Deterministic token = first 16 chars of lowercase-hex SHA-256(salt+value).

    Contract (design S4, pinned for the unit test): concatenation is
    SALT-PREFIX (``salt + value``), encoding is UTF-8, digest is lowercase hex,
    result is truncated to the first 16 characters. ``None`` passes through.
    """
    if v is None:
        return None
    digest = hashlib.sha256((salt + v).encode("utf-8")).hexdigest()
    return digest[:16]


def generalize_dob(v: str | None) -> int | None:
    """Generalize a date of birth to its birth YEAR (int), dropping month/day.

    Accepts an ISO date string (``"1988-07-15"``) or anything whose first 4
    characters are the year. Returns ``None`` when the year cannot be parsed.
    """
    if not v:
        return None
    # Take the leading 4 chars; robust to "YYYY-MM-DD" and "YYYY/MM/DD".
    head = str(v)[:4]
    try:
        return int(head)
    except ValueError:
        return None


def mask_pan(v: str | None) -> str | None:
    """Keep the last 4 digits of a PAN: ``-> "**** **** **** 1234"``.

    Non-digits in the source are ignored when extracting the last 4 digits.
    ``None``/empty passes through.
    """
    if not v:
        return v
    digits = "".join(ch for ch in v if ch.isdigit())
    last4 = digits[-4:] if len(digits) >= 4 else digits
    return f"**** **** **** {last4}"


# Dispatch table mapping a method name to a callable. tokenize_id needs the salt
# so it is handled specially inside apply_masking rather than stored here.
_SIMPLE_METHODS = {
    "name": mask_name,
    "email": mask_email,
    "phone": mask_phone,
    "pan": mask_pan,
}


def apply_masking(df, table: str, salt: str, columns: dict[str, str]):
    """Return a new DataFrame with the table's PII columns masked (design S4).

    This is the only Spark-touching function; ``pyspark`` is imported LAZILY so
    the module stays import-safe on a host without Spark.

    ``columns`` is the per-table column -> method map resolved by the caller from
    config (``pii_columns_for(table, cfg.masking)``). Rules applied per column:
      * name/email/phone/pan -> the matching pure function via a UDF
      * token (national_id)  -> ``tokenize_id`` with the resolved salt (in place)
      * dob (date_of_birth)  -> REPLACED by a new ``birth_year`` int column, the
        original ``date_of_birth`` dropped (design S6.3 canonical schema)
    """
    # Lazy import -- only needed when actually running on Spark.
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, StringType

    masked = df

    for column, method in (columns or {}).items():
        if method in _SIMPLE_METHODS:
            fn = _SIMPLE_METHODS[method]
            udf = F.udf(fn, StringType())
            masked = masked.withColumn(column, udf(F.col(column)))
        elif method == "token":
            # Bind the salt so the UDF matches tokenize_id(value, salt) exactly.
            token_udf = F.udf(lambda value, _salt=salt: tokenize_id(value, _salt), StringType())
            masked = masked.withColumn(column, token_udf(F.col(column)))
        elif method == "dob":
            # date_of_birth -> birth_year (int); drop the original date column.
            dob_udf = F.udf(lambda value: generalize_dob(value), IntegerType())
            masked = masked.withColumn("birth_year", dob_udf(F.col(column).cast("string")))
            masked = masked.drop(column)
        else:  # pragma: no cover - guards against a config/typo mismatch
            raise ValueError(f"unknown masking method {method!r} for {table}.{column}")

    return masked
