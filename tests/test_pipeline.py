"""
tests/test_pipeline.py — Unit tests for customer transformation logic.

Tests are intentionally independent of any database connection.
Each test constructs a dummy pandas DataFrame and passes it directly
into the transform function via `.fn()` (Prefect's underlying callable),
with `get_run_logger` patched out so no Prefect run context is required.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipeline import transform_customers

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

PATCH_LOGGER = "pipeline.get_run_logger"


def _mock_logger():
    """Return a MagicMock that satisfies get_run_logger() calls."""
    logger = MagicMock()
    return logger


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def duplicate_customers_df() -> pd.DataFrame:
    """DataFrame with two customer_id=1 rows and different signup_dates."""
    return pd.DataFrame(
        {
            "customer_id": [1, 1, 2],
            "full_name": ["Alice Old", "Alice New", "Bob"],
            "email": ["alice.old@example.com", "alice.new@example.com", "bob@example.com"],
            "phone": ["5551111111", "5552222222", "5553333333"],
            "signup_date": ["2023-01-01", "2023-06-01", "2023-03-15"],
        }
    )


@pytest.fixture()
def phone_customers_df() -> pd.DataFrame:
    """DataFrame with various raw phone formats and one NULL phone."""
    return pd.DataFrame(
        {
            "customer_id": [1, 2, 3, 4, 5],
            "full_name": ["A", "B", "C", "D", "E"],
            "email": ["a@x.com", "b@x.com", "c@x.com", "d@x.com", "e@x.com"],
            "phone": [
                "+1 (555) 123-4567",   # international format
                "555-987-6543",         # dashes
                "1234567890",           # already digits
                "1-800-555-DINO",       # letters in phone
                None,                   # missing — should stay None/NaN
            ],
            "signup_date": ["2023-01-01"] * 5,
        }
    )


@pytest.fixture()
def missing_email_df() -> pd.DataFrame:
    """DataFrame where some customers have NULL emails."""
    return pd.DataFrame(
        {
            "customer_id": [1, 2, 3],
            "full_name": ["Has Email", "No Email 1", "No Email 2"],
            "email": ["valid@example.com", None, None],
            "phone": ["111", "222", "333"],
            "signup_date": ["2023-01-01", "2023-02-01", "2023-03-01"],
        }
    )


# ---------------------------------------------------------------------------
# Tests — Deduplication
# ---------------------------------------------------------------------------


class TestDeduplication:
    """transform_customers should keep the most recent signup_date per customer_id."""

    def test_duplicate_rows_are_removed(self, duplicate_customers_df):
        """After transformation, each customer_id should appear exactly once."""
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_customers.fn(duplicate_customers_df)

        assert result["customer_id"].duplicated().sum() == 0, (
            "Duplicate customer_id rows were not removed."
        )

    def test_most_recent_signup_date_is_kept(self, duplicate_customers_df):
        """For customer_id=1, the row with signup_date='2023-06-01' must be kept."""
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_customers.fn(duplicate_customers_df)

        alice_row = result[result["customer_id"] == 1]
        assert len(alice_row) == 1, "Expected exactly one row for customer_id=1."

        kept_date = pd.to_datetime(alice_row["signup_date"].iloc[0])
        assert kept_date == pd.Timestamp("2023-06-01"), (
            f"Expected 2023-06-01 (most recent), got {kept_date}."
        )

    def test_older_record_is_discarded(self, duplicate_customers_df):
        """The email from the older record ('alice.old@example.com') must not appear."""
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_customers.fn(duplicate_customers_df)

        emails = result["email"].tolist()
        assert "alice.old@example.com" not in emails, (
            "The older duplicate record was kept instead of the newer one."
        )

    def test_non_duplicate_rows_are_preserved(self, duplicate_customers_df):
        """customer_id=2 (Bob) has no duplicate and must be retained."""
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_customers.fn(duplicate_customers_df)

        assert 2 in result["customer_id"].values, (
            "customer_id=2 was incorrectly dropped."
        )

    def test_output_row_count(self, duplicate_customers_df):
        """Input has 3 rows with 2 distinct customer_ids → output should have 2 rows."""
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_customers.fn(duplicate_customers_df)

        assert len(result) == 2, f"Expected 2 rows after dedup, got {len(result)}."


# ---------------------------------------------------------------------------
# Tests — Phone Standardisation
# ---------------------------------------------------------------------------


class TestPhoneStandardisation:
    """transform_customers should strip all non-numeric chars from phone."""

    @pytest.mark.parametrize(
        "raw_phone, expected_digits",
        [
            ("+1 (555) 123-4567", "15551234567"),
            ("555-987-6543", "5559876543"),
            ("1234567890", "1234567890"),   # already clean → unchanged
            ("1-800-555-DINO", "1800555"),  # letters stripped
        ],
    )
    def test_phone_digits_only(self, raw_phone, expected_digits):
        """Each raw phone format should be reduced to digits only."""
        df = pd.DataFrame(
            {
                "customer_id": [1],
                "full_name": ["Test"],
                "email": ["test@example.com"],
                "phone": [raw_phone],
                "signup_date": ["2023-01-01"],
            }
        )
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_customers.fn(df)

        assert result["phone"].iloc[0] == expected_digits, (
            f"Phone '{raw_phone}' → expected '{expected_digits}', "
            f"got '{result['phone'].iloc[0]}'."
        )

    def test_null_phone_remains_null(self, phone_customers_df):
        """A NULL phone value should stay NULL (not become the string 'nan')."""
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_customers.fn(phone_customers_df)

        # customer_id=5 has the NULL phone
        null_phone_row = result[result["customer_id"] == 5]
        assert null_phone_row["phone"].isna().iloc[0], (
            "NULL phone was incorrectly converted to a non-null value."
        )


# ---------------------------------------------------------------------------
# Tests — Missing Email Fill
# ---------------------------------------------------------------------------


class TestMissingEmailFill:
    """transform_customers should replace NULL emails with 'unknown@domain.com'."""

    PLACEHOLDER = "unknown@domain.com"

    def test_null_emails_are_filled(self, missing_email_df):
        """All NULL emails must be replaced with the placeholder."""
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_customers.fn(missing_email_df)

        assert result["email"].isna().sum() == 0, (
            "There are still NULL emails after transformation."
        )

    def test_correct_placeholder_value(self, missing_email_df):
        """The fill value must be exactly 'unknown@domain.com'."""
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_customers.fn(missing_email_df)

        filled = result[result["full_name"].isin(["No Email 1", "No Email 2"])]["email"]
        assert (filled == self.PLACEHOLDER).all(), (
            f"Expected all filled emails to be '{self.PLACEHOLDER}', "
            f"got: {filled.tolist()}."
        )

    def test_existing_email_is_not_overwritten(self, missing_email_df):
        """A customer who already has an email must keep their original value."""
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_customers.fn(missing_email_df)

        has_email_row = result[result["full_name"] == "Has Email"]
        assert has_email_row["email"].iloc[0] == "valid@example.com", (
            "Existing valid email was unexpectedly overwritten."
        )

    def test_no_emails_filled_when_none_missing(self):
        """When no emails are NULL, the DataFrame should be unchanged."""
        df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "full_name": ["Alice", "Bob"],
                "email": ["alice@example.com", "bob@example.com"],
                "phone": ["111", "222"],
                "signup_date": ["2023-01-01", "2023-02-01"],
            }
        )
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_customers.fn(df)

        # Use set comparison — transform_customers sorts by signup_date so
        # the row order may differ from the input, but the values must be intact.
        assert set(result["email"]) == {"alice@example.com", "bob@example.com"}, (
            "Emails were unexpectedly modified when no NULLs were present."
        )
