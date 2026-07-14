"""
tests/test_pipeline.py — Unit tests for customer and order transformation logic.

Tests are intentionally independent of any database connection.
Each test constructs a dummy pandas DataFrame and passes it directly
into the transform function via `.fn()` (Prefect's underlying callable),
with `get_run_logger` patched out so no Prefect run context is required.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipeline import transform_customers, transform_orders

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


# ---------------------------------------------------------------------------
# Fixtures — Orders
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_rates_df() -> pd.DataFrame:
    """Minimal exchange-rates table covering EUR and JPY on a known date."""
    return pd.DataFrame(
        {
            "currency": ["EUR", "JPY", "GBP"],
            "rate_to_usd": [1.10, 0.007, 1.25],
            "date": ["2023-05-01", "2023-05-01", "2023-05-01"],
        }
    )


@pytest.fixture()
def mixed_amount_orders_df() -> pd.DataFrame:
    """Orders with positive, zero, and negative total_amount values."""
    return pd.DataFrame(
        {
            "order_id":     [1,    2,     3,      4,    5],
            "customer_id":  [1,    2,     3,      4,    5],
            "order_date":   ["2023-05-01"] * 5,
            "total_amount": [150.0, -50.0, 0.0, 200.0, -1.0],
            "currency":     ["USD", "USD", "USD", "EUR", "USD"],
            "status":       ["COMPLETED", "SYSTEM_ERROR", "COMPLETED",
                             "COMPLETED", "SYSTEM_ERROR"],
        }
    )


# ---------------------------------------------------------------------------
# Tests — Order Filtering
# ---------------------------------------------------------------------------


class TestOrderFiltering:
    """transform_orders must remove all rows where total_amount <= 0."""

    def test_negative_amounts_are_removed(self, mixed_amount_orders_df, sample_rates_df):
        """Orders with a negative total_amount must be excluded from the output."""
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_orders.fn(mixed_amount_orders_df, sample_rates_df)

        assert (result["total_amount"] > 0).all(), (
            "Negative total_amount rows were not removed."
        )

    def test_zero_amount_is_removed(self, mixed_amount_orders_df, sample_rates_df):
        """An order with total_amount == 0 must also be excluded."""
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_orders.fn(mixed_amount_orders_df, sample_rates_df)

        assert 0.0 not in result["total_amount"].values, (
            "Zero total_amount order was not removed."
        )

    def test_positive_amounts_are_retained(self, mixed_amount_orders_df, sample_rates_df):
        """Orders with a positive total_amount must all remain in the output."""
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_orders.fn(mixed_amount_orders_df, sample_rates_df)

        # original positive order_ids are 1 (150.0) and 4 (200.0)
        assert set(result["order_id"]) == {1, 4}, (
            f"Expected order_ids {{1, 4}} to survive filtering, got {set(result['order_id'])}."
        )

    def test_output_row_count(self, mixed_amount_orders_df, sample_rates_df):
        """5 input rows (2 positive, 1 zero, 2 negative) → 2 output rows."""
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_orders.fn(mixed_amount_orders_df, sample_rates_df)

        assert len(result) == 2, f"Expected 2 rows after filtering, got {len(result)}."

    def test_all_invalid_amounts_filtered(self):
        """If every order has total_amount <= 0, the result should be empty."""
        df = pd.DataFrame(
            {
                "order_id":     [1, 2],
                "customer_id":  [1, 2],
                "order_date":   ["2023-05-01", "2023-05-01"],
                "total_amount": [-10.0, 0.0],
                "currency":     ["USD", "USD"],
                "status":       ["SYSTEM_ERROR", "COMPLETED"],
            }
        )
        rates = pd.DataFrame(
            {
                "currency":    pd.Series([], dtype="object"),
                "rate_to_usd": pd.Series([], dtype="float64"),
                "date":        pd.Series([], dtype="object"),
            }
        )
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_orders.fn(df, rates)

        assert len(result) == 0, (
            "Expected an empty DataFrame when all amounts are invalid."
        )


# ---------------------------------------------------------------------------
# Tests — Currency Conversion
# ---------------------------------------------------------------------------


class TestCurrencyConversion:
    """transform_orders must compute usd_amount = total_amount * rate_to_usd."""

    def _make_order(self, currency: str, amount: float, date: str = "2023-05-01"):
        """Helper: build a single-row orders DataFrame."""
        return pd.DataFrame(
            {
                "order_id":     [1],
                "customer_id":  [1],
                "order_date":   [date],
                "total_amount": [amount],
                "currency":     [currency],
                "status":       ["COMPLETED"],
            }
        )

    def test_known_currency_is_converted_correctly(self, sample_rates_df):
        """EUR order on 2023-05-01 at rate 1.10 → usd_amount = amount * 1.10."""
        df = self._make_order("EUR", 200.0)
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_orders.fn(df, sample_rates_df)

        expected = round(200.0 * 1.10, 4)
        assert result["usd_amount"].iloc[0] == expected, (
            f"EUR conversion: expected {expected}, got {result['usd_amount'].iloc[0]}."
        )

    def test_jpy_conversion(self, sample_rates_df):
        """JPY order at rate 0.007 → usd_amount = amount * 0.007."""
        df = self._make_order("JPY", 10000.0)
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_orders.fn(df, sample_rates_df)

        expected = round(10000.0 * 0.007, 4)
        assert result["usd_amount"].iloc[0] == expected, (
            f"JPY conversion: expected {expected}, got {result['usd_amount'].iloc[0]}."
        )

    def test_usd_order_not_in_rates_defaults_to_one(self, sample_rates_df):
        """USD orders have no entry in the rates table → rate defaults to 1.0,
        so usd_amount equals total_amount unchanged."""
        df = self._make_order("USD", 150.0)
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_orders.fn(df, sample_rates_df)

        assert result["usd_amount"].iloc[0] == 150.0, (
            "USD order: usd_amount should equal total_amount when no rate is found."
        )

    def test_null_currency_defaults_to_rate_one(self, sample_rates_df):
        """A NULL currency cannot be matched → rate defaults to 1.0."""
        df = self._make_order(None, 120.0)
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_orders.fn(df, sample_rates_df)

        assert result["usd_amount"].iloc[0] == 120.0, (
            "NULL currency: usd_amount should equal total_amount (rate=1.0 fallback)."
        )

    def test_missing_date_in_rates_defaults_to_rate_one(self, sample_rates_df):
        """A currency that exists in the rates table but on a different date
        has no match → rate defaults to 1.0."""
        df = self._make_order("EUR", 100.0, date="2024-01-01")  # date not in rates
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_orders.fn(df, sample_rates_df)

        assert result["usd_amount"].iloc[0] == 100.0, (
            "No rate for this date: usd_amount should equal total_amount (rate=1.0 fallback)."
        )

    def test_usd_amount_column_is_added(self, sample_rates_df):
        """The output DataFrame must always contain the 'usd_amount' column."""
        df = self._make_order("EUR", 50.0)
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_orders.fn(df, sample_rates_df)

        assert "usd_amount" in result.columns, (
            "'usd_amount' column is missing from transform_orders output."
        )

    def test_rate_to_usd_column_is_removed(self, sample_rates_df):
        """The intermediate 'rate_to_usd' column must not be present in the output."""
        df = self._make_order("EUR", 50.0)
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_orders.fn(df, sample_rates_df)

        assert "rate_to_usd" not in result.columns, (
            "'rate_to_usd' column was not dropped from transform_orders output."
        )

    @pytest.mark.parametrize(
        "currency, amount, rate, expected_usd",
        [
            ("EUR", 200.0,   1.10,  220.0),
            ("JPY", 10000.0, 0.007, 70.0),
            ("GBP", 100.0,   1.25,  125.0),
        ],
    )
    def test_conversion_accuracy_parametrized(
        self, currency, amount, rate, expected_usd, sample_rates_df
    ):
        """Parametrized check: usd_amount = round(total_amount * rate, 4)."""
        df = self._make_order(currency, amount)
        with patch(PATCH_LOGGER, return_value=_mock_logger()):
            result = transform_orders.fn(df, sample_rates_df)

        assert result["usd_amount"].iloc[0] == round(expected_usd, 4), (
            f"{currency} {amount} * {rate}: expected {round(expected_usd,4)}, "
            f"got {result['usd_amount'].iloc[0]}."
        )
