"""
Data quality checks for each medallion layer using Great Expectations.
"""

import structlog
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from great_expectations.data_context import FileDataContext

logger = structlog.get_logger()


def build_bronze_suite() -> ExpectationSuite:
    """Bronze layer: minimal checks — data exists, metadata columns present."""
    suite = ExpectationSuite(expectation_suite_name="bronze_orders")
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 1},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "_ingested_at"},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "_batch_id"},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "_ingested_at"},
        )
    )
    return suite


def build_silver_orders_suite() -> ExpectationSuite:
    """Silver layer: strict schema and value checks."""
    suite = ExpectationSuite(expectation_suite_name="silver_orders")

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "order_id"},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "order_id"},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "customer_id"},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "total_amount", "min_value": 0, "max_value": 100000},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "status",
                "value_set": [
                    "pending", "processing", "completed",
                    "shipped", "refunded", "cancelled",
                ],
            },
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "payment_method",
                "value_set": [
                    "credit_card", "debit_card", "paypal",
                    "bank_transfer", "crypto",
                ],
            },
        )
    )
    return suite


def build_gold_rfm_suite() -> ExpectationSuite:
    """Gold layer: business logic validation."""
    suite = ExpectationSuite(expectation_suite_name="gold_rfm")

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "customer_id"},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "r_score", "min_value": 1, "max_value": 5},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "f_score", "min_value": 1, "max_value": 5},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "m_score", "min_value": 1, "max_value": 5},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "rfm_segment",
                "value_set": [
                    "champion", "loyal", "new_customer",
                    "at_risk", "lost", "regular",
                ],
            },
        )
    )
    return suite
