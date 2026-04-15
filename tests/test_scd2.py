"""
Unit tests for warehouse/scd2_supplier.py

Uses pandas DataFrames — no PySpark, no AWS calls.
"""

import pytest
import pandas as pd
from datetime import date

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from warehouse.scd2_supplier import apply_scd2, run_scd2_dq

TODAY = date(2025, 6, 1)
YESTERDAY = date(2025, 5, 31)
FAR_FUTURE = date(9999, 12, 31)


def make_incoming(**kwargs):
    defaults = {
        "supplier_id":   "SUP001",
        "supplier_name": "Fresh Farms",
        "menu_item_id":  "MI001",
        "supplier_city": "Pune",
        "lead_time_days": 3,
        "quality_score": 4.5,
    }
    defaults.update(kwargs)
    return pd.DataFrame([defaults])


def make_existing(is_current=True, quality_score=4.5, expiry=None):
    return pd.DataFrame([{
        "supplier_sk":   "SK-EXISTING",
        "supplier_id":   "SUP001",
        "supplier_name": "Fresh Farms",
        "menu_item_id":  "MI001",
        "supplier_city": "Pune",
        "lead_time_days": 3,
        "quality_score": quality_score,
        "effective_date": date(2025, 1, 1).isoformat(),
        "expiry_date":   (expiry or FAR_FUTURE).isoformat(),
        "is_current":    is_current,
    }])


# ---------------------------------------------------------------------------
# New supplier is inserted with is_current=True
# ---------------------------------------------------------------------------
def test_new_supplier_inserted_as_current():
    result = apply_scd2(None, make_incoming(), TODAY)
    assert len(result) == 1
    row = result.iloc[0]
    assert row["is_current"] is True or row["is_current"] == True
    assert row["supplier_id"] == "SUP001"
    assert row["expiry_date"] == FAR_FUTURE.isoformat()
    assert row["effective_date"] == TODAY.isoformat()


# ---------------------------------------------------------------------------
# Changed quality_score creates new row and closes old row
# ---------------------------------------------------------------------------
def test_changed_attribute_creates_new_version():
    existing = make_existing(quality_score=4.5)
    incoming = make_incoming(quality_score=3.8)  # changed
    result = apply_scd2(existing, incoming, TODAY)

    assert len(result) == 2

    old = result[result["supplier_sk"] == "SK-EXISTING"].iloc[0]
    assert old["is_current"] is False or old["is_current"] == False
    assert old["expiry_date"] == YESTERDAY.isoformat()

    new = result[result["supplier_sk"] != "SK-EXISTING"].iloc[0]
    assert new["is_current"] is True or new["is_current"] == True
    assert float(new["quality_score"]) == pytest.approx(3.8)
    assert new["effective_date"] == TODAY.isoformat()


# ---------------------------------------------------------------------------
# No change — no new row created
# ---------------------------------------------------------------------------
def test_no_change_produces_no_new_row():
    existing = make_existing(quality_score=4.5)
    incoming = make_incoming(quality_score=4.5)  # identical
    result = apply_scd2(existing, incoming, TODAY)
    assert len(result) == 1
    assert result.iloc[0]["supplier_sk"] == "SK-EXISTING"


# ---------------------------------------------------------------------------
# Only one is_current=True per supplier_id after any operation
# ---------------------------------------------------------------------------
def test_only_one_current_per_supplier_after_change():
    existing = make_existing(quality_score=4.5)
    incoming = make_incoming(quality_score=2.0)
    result = apply_scd2(existing, incoming, TODAY)

    current_count = result[result["is_current"] == True].shape[0]
    assert current_count == 1


def test_only_one_current_per_supplier_no_change():
    existing = make_existing(quality_score=4.5)
    incoming = make_incoming(quality_score=4.5)
    result = apply_scd2(existing, incoming, TODAY)

    current_count = result[result["is_current"] == True].shape[0]
    assert current_count == 1


# ---------------------------------------------------------------------------
# DQ check: duplicate is_current should raise
# ---------------------------------------------------------------------------
def test_dq_raises_on_multiple_current_records():
    bad = pd.DataFrame([
        {
            "supplier_sk": "SK1", "supplier_id": "SUP001", "supplier_name": "A",
            "menu_item_id": "MI001", "supplier_city": "X", "lead_time_days": 1,
            "quality_score": 4.0, "effective_date": "2025-01-01",
            "expiry_date": "9999-12-31", "is_current": True,
        },
        {
            "supplier_sk": "SK2", "supplier_id": "SUP001", "supplier_name": "A",
            "menu_item_id": "MI001", "supplier_city": "X", "lead_time_days": 1,
            "quality_score": 4.0, "effective_date": "2025-03-01",
            "expiry_date": "9999-12-31", "is_current": True,
        },
    ])
    with pytest.raises(ValueError, match="DQ GATE FAILED"):
        run_scd2_dq(bad)


# ---------------------------------------------------------------------------
# DQ check: duplicate supplier_sk should raise
# ---------------------------------------------------------------------------
def test_dq_raises_on_duplicate_supplier_sk():
    bad = pd.DataFrame([
        {
            "supplier_sk": "SK1", "supplier_id": "SUP001", "supplier_name": "A",
            "menu_item_id": "MI001", "supplier_city": "X", "lead_time_days": 1,
            "quality_score": 4.0, "effective_date": "2025-01-01",
            "expiry_date": "2025-05-31", "is_current": False,
        },
        {
            "supplier_sk": "SK1", "supplier_id": "SUP001", "supplier_name": "A",
            "menu_item_id": "MI001", "supplier_city": "X", "lead_time_days": 1,
            "quality_score": 4.5, "effective_date": "2025-06-01",
            "expiry_date": "9999-12-31", "is_current": True,
        },
    ])
    with pytest.raises(ValueError, match="DQ GATE FAILED"):
        run_scd2_dq(bad)
