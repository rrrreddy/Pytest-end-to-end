# Pytest-end-to-end

1. What to unit test in data engineering
You mainly unit‑test pure transformation logic and small helpers, not entire Spark clusters or warehouses.

Typical targets:

Functions that transform DataFrames (filtering, joins, aggregations).

Parsing/validation functions.

SQL‑generation logic.

Small IO wrappers (with external systems mocked).

2. Basic PyTest structure
Directory example:

text
project/
  src/
    transform.py
  tests/
    test_transform.py
    conftest.py
Install: pip install pytest.

A minimal test:

python
# src/transform.py
def add_total(row):
    return {**row, "total": row["qty"] * row["price"]}
python
# tests/test_transform.py
from src.transform import add_total

def test_add_total_basic():
    row = {"qty": 2, "price": 10}
    result = add_total(row)
    assert result["total"] == 20
Run: pytest -q.

3. Fixtures for reusable data
Fixtures create shared objects (Spark session, sample DataFrames, temp dirs, configs).

Example: Spark session + input DataFrame.

python
# tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local[2]")
            .appName("tests")
            .getOrCreate())

@pytest.fixture
def sales_df(spark):
    return spark.createDataFrame(
        [
            (1, "A", 100.0),
            (2, "B", 200.0),
            (3, "A", 150.0),
        ],
        ["id", "category", "amount"],
    )
Using fixture in test:

python
# tests/test_agg.py
from src.transform import total_by_category

def test_total_by_category(sales_df):
    result = total_by_category(sales_df)
    rows = { (r["category"], r["total"]) for r in result.collect() }
    assert rows == {("A", 250.0), ("B", 200.0)}
Benefits: no repeated setup; you can change test data in one place.

4. Parametrized tests
Use @pytest.mark.parametrize to test many input/output cases in one function.

Example: validating a parse function.

python
import pytest
from src.transform import parse_amount

@pytest.mark.parametrize("raw, expected", [
    ("10", 10.0),
    ("10.50", 10.5),
    ("  5 ", 5.0),
])
def test_parse_amount_valid(raw, expected):
    assert parse_amount(raw) == expected

@pytest.mark.parametrize("raw", ["", "abc", None])
def test_parse_amount_invalid(raw):
    with pytest.raises(ValueError):
        parse_amount(raw)
This avoids copy‑paste and is expected in production‑quality suites.

5. Mocking external systems (DB, S3, APIs)
For unit tests, never hit real Postgres, S3, or REST APIs.
​

Use:

unittest.mock (built‑in) or

pytest-mock plugin (pip install pytest-mock).
​

Example: mock S3 client used by a function.

python
# src/io.py
import boto3

def list_keys(bucket):
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket=bucket)
    return [o["Key"] for o in resp.get("Contents", [])]
python
# tests/test_io.py
def test_list_keys(mocker):
    fake_client = mocker.Mock()
    fake_client.list_objects_v2.return_value = {
        "Contents": [{"Key": "a.csv"}, {"Key": "b.csv"}]
    }
    mocker.patch("src.io.boto3.client", return_value=fake_client)

    from src.io import list_keys
    keys = list_keys("any-bucket")

    assert keys == ["a.csv", "b.csv"]
    fake_client.list_objects_v2.assert_called_once()
Key interview point: “We mock external services so unit tests are fast, deterministic, and network‑independent.”
​

6. Testing Spark / Pandas transformations
Pattern: given small input → call transformation → assert on resulting rows/columns.

Example transformation:

python
# src/transform.py
from pyspark.sql import functions as F

def total_by_category(df):
    return (df.groupBy("category")
              .agg(F.sum("amount").alias("total")))
Test (we already saw with fixture); just ensure:

Use tiny in‑memory DataFrames.

Avoid writing to disk in unit tests; if needed, write to tmp dirs via fixture.

7. Testing data quality rules
You can encode rules like “no NULLs in key columns” or “amount >= 0” as functions and test them.

Example:

python
# src/quality.py
def check_no_nulls(df, cols):
    return all(df.filter(df[c].isNull()).count() == 0 for c in cols)
python
# tests/test_quality.py
def test_check_no_nulls_pass(spark):
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "category"])
    from src.quality import check_no_nulls
    assert check_no_nulls(df, ["id", "category"])

def test_check_no_nulls_fail(spark):
    df = spark.createDataFrame([(1, None)], ["id", "category"])
    from src.quality import check_no_nulls
    assert not check_no_nulls(df, ["category"])
In production you’d call these checks inside your pipeline and fail early if they return false.

8. Using markers, skipping, slow tests
PyTest markers let you tag tests as slow, integration, spark, etc.

python
import pytest

@pytest.mark.slow
def test_big_dataset():
    ...
Run only unit tests in CI fast path: pytest -m "not slow".

This pattern is important when Spark tests are heavier than pure Python tests.

9. Best practices you can quote in interviews
From engineering‑focused articles and guides:

Prefer pure functions for transformations; they’re easy to unit test.

One logical check per test; keep tests small and readable.
​

Use fixtures for common setup, parametrize for many cases.

Mock IO and external services; never depend on real infra in unit tests.
​

Separate unit vs integration tests via markers.

Run tests in CI on every commit; fail the build if tests fail.

Track coverage (e.g., pytest --cov=src) but prioritize meaningful tests over chasing 100%.

10. Short “tell me how you test pipelines” answer
You can adapt this as an interview script:

“For data pipelines I write PyTest unit tests around my transformation functions, using small Spark DataFrames and fixtures for setup. I parametrize tests to cover edge cases and use mocks to isolate external systems like S3 or Postgres, so tests are fast and deterministic. I separate pure unit tests from integration tests with markers, run them in CI on each commit, and add data quality checks (nulls, referential integrity, range checks) as standalone functions with their own tests.”
