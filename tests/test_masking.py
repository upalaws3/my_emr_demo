"""Unit tests for src/common/masking.py (pure functions, no Spark)."""

from src.common import masking

# Salt matches config/pipeline.yaml so the precomputed token literal is coherent.
SALT = "ocbc-poc-demo-salt-2025"


def test_tokenize_id_matches_precomputed_literal():
    # Pins order (salt-prefix), encoding (UTF-8), and truncation (first 16 chars).
    # Literal recomputed with: sha256((SALT + "S1234567A").encode()).hexdigest()[:16]
    assert masking.tokenize_id("S1234567A", SALT) == "e9b1164aa8b635be"


def test_tokenize_id_deterministic_and_salt_prefix():
    # Same input -> same token (stable across calls).
    assert masking.tokenize_id("abc", SALT) == masking.tokenize_id("abc", SALT)
    # salt-prefix (salt+value) differs from value+salt ordering.
    import hashlib

    wrong_order = hashlib.sha256(("abc" + SALT).encode("utf-8")).hexdigest()[:16]
    assert masking.tokenize_id("abc", SALT) != wrong_order


def test_tokenize_id_none_passthrough():
    assert masking.tokenize_id(None, SALT) is None


def test_mask_name():
    assert masking.mask_name("Alice") == "A***"
    assert masking.mask_name("A***") == "A***"  # idempotent (first char preserved)
    assert masking.mask_name("") == ""
    assert masking.mask_name(None) is None


def test_mask_email():
    assert masking.mask_email("alice@example.com") == "a***@example.com"
    # Idempotent: re-masking keeps the same first char + domain.
    assert masking.mask_email("a***@example.com") == "a***@example.com"
    assert masking.mask_email(None) is None


def test_mask_phone():
    assert masking.mask_phone("+6591234567") == "****67"
    assert masking.mask_phone("12") == "12"  # too short to mask
    assert masking.mask_phone(None) is None


def test_generalize_dob():
    assert masking.generalize_dob("1988-07-15") == 1988
    assert masking.generalize_dob("1990/01/01") == 1990
    assert masking.generalize_dob(None) is None
    assert masking.generalize_dob("garbage") is None


def test_mask_pan():
    assert masking.mask_pan("4111111111111234") == "**** **** **** 1234"
    # Idempotent: last 4 survive re-masking.
    assert masking.mask_pan("**** **** **** 1234") == "**** **** **** 1234"
    assert masking.mask_pan(None) is None


# The PII catalog is config-driven: pii_columns_for reads masking.columns.
_MASKING_CFG = {
    "salt": "s",
    "columns": {
        "customers": {
            "first_name": "name",
            "email": "email",
            "national_id": "token",
            "date_of_birth": "dob",
        },
        "cards": {"card_number": "pan"},
    },
}


def test_pii_columns_for_reads_config():
    cols = masking.pii_columns_for("customers", _MASKING_CFG)
    assert cols["national_id"] == "token"
    assert cols["date_of_birth"] == "dob"
    assert cols["email"] == "email"


def test_pii_columns_for_table_without_pii_is_empty():
    # accounts has no masking.columns entry -> empty mapping.
    assert masking.pii_columns_for("accounts", _MASKING_CFG) == {}
    # A missing/None config is tolerated too.
    assert masking.pii_columns_for("customers", {}) == {}
