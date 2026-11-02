"""Run: pytest -v backend/tests"""
import os, sys
import pytest
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "backend", "spark"))
sys.path.insert(0, os.path.join(ROOT, "backend", "dataset"))
from spark_pipeline import get_spark, clean, rfm_segments, analyse, SCHEMA  # noqa
from generate_synthetic import generate  # noqa


@pytest.fixture(scope="module")
def spark():
    s = get_spark("tests"); yield s; s.stop()


ROWS = [  # order, cust, prod, name, cat, qty, price, ts, region
    ("O1", 1, "P1", "Phone", "Electronics", 2, 100.0, "2024-01-05 10:00:00", "UK"),
    ("O1", 1, "P1", "Phone", "Electronics", 2, 100.0, "2024-01-05 10:00:00", "UK"),   # duplicate
    ("O2", None, "P2", "Book", "Books", 1, 10.0, "2024-01-06 11:00:00", "UK"),        # missing customer
    ("O3", 2, "P2", "Book", "Books", -1, 10.0, "2024-02-07 12:00:00", "USA"),         # invalid qty
    ("O4", 2, "P3", "Shirt", None, 3, 20.0, "2024-02-08 09:00:00", None),             # missing cat/region
    ("O5", 3, "P3", "Shirt", "Fashion", 1, 0.0, "2024-03-01 09:00:00", "USA"),        # zero price
    ("O6", 3, "P1", "Phone", "Electronics", 1, 100.0, "2024-03-02 09:00:00", "USA"),
]


@pytest.fixture(scope="module")
def cleaned(spark):
    df = spark.createDataFrame(ROWS, SCHEMA)
    return clean(df)


def test_duplicates_removed(cleaned):
    assert cleaned[1]["duplicates_removed"] == 1


def test_missing_and_invalid_dropped(cleaned):
    assert cleaned[1]["rows_dropped_missing"] == 1 and cleaned[1]["rows_dropped_invalid"] == 2
    assert cleaned[1]["final_clean_rows"] == 3


def test_missing_category_filled(cleaned):
    vals = {r[0] for r in cleaned[0].select("category").distinct().collect()}
    assert "Unknown" in vals


def test_revenue_total(cleaned):
    res = analyse(cleaned[0])
    assert res["kpis"]["total_revenue"] == 200 + 60 + 100


def test_monthly_trend(cleaned):
    m = {r.year_month: r.revenue for r in analyse(cleaned[0])["monthly"].collect()}
    assert m == {"2024-01": 200.0, "2024-02": 60.0, "2024-03": 100.0}


def test_rfm_segments_valid(cleaned):
    segs = {r.segment for r in rfm_segments(cleaned[0]).collect()}
    assert segs <= {"Champions", "Loyal", "Potential", "At Risk"}


def test_generator_schema_and_dirty_rows():
    df = generate(rows=3000, seed=1)
    assert list(df.columns) == [f.name for f in SCHEMA.fields]
    assert df.duplicated().sum() > 0 and df.customer_id.isna().sum() > 0
