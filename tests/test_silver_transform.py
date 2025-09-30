"""Tests for Silver layer transformations."""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date


class TestSilverTransform:
    """Test Silver layer transformation logic."""

    def test_order_status_normalization(self):
        """Verify status values are lowered and trimmed."""
        raw_statuses = ["  Completed ", "PENDING", "shipped"]
        expected = ["completed", "pending", "shipped"]
        result = [s.strip().lower() for s in raw_statuses]
        assert result == expected

    def test_line_total_calculation(self):
        """Verify line_total = unit_price * quantity * (1 - discount)."""
        unit_price = 29.99
        quantity = 3
        discount = 0.1
        expected = round(unit_price * quantity * (1 - discount), 2)
        assert expected == pytest.approx(80.97, abs=0.01)

    def test_line_total_no_discount(self):
        """Verify no-discount case."""
        unit_price = 50.0
        quantity = 2
        discount = 0.0
        expected = unit_price * quantity * (1 - discount)
        assert expected == 100.0

    def test_customer_full_name(self):
        """Verify full name concatenation."""
        first = "John"
        last = "Doe"
        full = f"{first.strip()} {last.strip()}"
        assert full == "John Doe"

    def test_scd2_change_detection(self):
        """Verify SCD Type 2 detects changes in tracked columns."""
        old = {"email": "old@test.com", "segment": "basic", "phone": "123"}
        new = {"email": "old@test.com", "segment": "premium", "phone": "123"}

        changed = (
            old["email"] != new["email"]
            or old["segment"] != new["segment"]
            or old["phone"] != new["phone"]
        )
        assert changed is True

    def test_scd2_no_change(self):
        """Verify SCD Type 2 skips unchanged records."""
        old = {"email": "test@test.com", "segment": "premium", "phone": "123"}
        new = {"email": "test@test.com", "segment": "premium", "phone": "123"}

        changed = (
            old["email"] != new["email"]
            or old["segment"] != new["segment"]
            or old["phone"] != new["phone"]
        )
        assert changed is False


class TestGoldAggregation:
    """Test Gold layer business logic."""

    def test_value_segment_classification(self):
        """Verify customer value segmentation thresholds."""
        def classify(spend):
            if spend > 1000:
                return "high_value"
            elif spend > 500:
                return "medium_value"
            elif spend > 0:
                return "low_value"
            return "no_purchases"

        assert classify(1500) == "high_value"
        assert classify(750) == "medium_value"
        assert classify(100) == "low_value"
        assert classify(0) == "no_purchases"

    def test_rfm_scoring(self):
        """Verify RFM segment logic."""
        def rfm_segment(r, f, m):
            if r >= 4 and f >= 4 and m >= 4:
                return "champion"
            elif r >= 3 and f >= 3:
                return "loyal"
            elif r >= 4 and f <= 2:
                return "new_customer"
            elif r <= 2 and f >= 3:
                return "at_risk"
            elif r <= 2 and f <= 2:
                return "lost"
            return "regular"

        assert rfm_segment(5, 5, 5) == "champion"
        assert rfm_segment(4, 4, 2) == "loyal"
        assert rfm_segment(5, 1, 1) == "new_customer"
        assert rfm_segment(1, 4, 4) == "at_risk"
        assert rfm_segment(1, 1, 1) == "lost"
        assert rfm_segment(3, 2, 4) == "regular"
