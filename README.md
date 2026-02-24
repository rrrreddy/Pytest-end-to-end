# Pytest End-to-End Testing for Data Engineers

## Overview

This guide provides a comprehensive approach to end-to-end testing for data engineering projects using **pytest**. It covers unit, integration, and system testing, with practical examples and best practices tailored for data pipelines, transformation logic, and external system interactions.

---

## Why Testing Matters in Data Engineering

Data pipelines are complex, often integrating multiple sources, transformations, and destinations. Testing ensures:

- **Correctness**: Data is processed as expected.
- **Reliability**: Pipelines are robust against changes and failures.
- **Maintainability**: Code is easier to refactor and extend.
- **Early Bug Detection**: Issues are caught before reaching production.

---

## Types of Tests for Data Engineering

1. **Unit Tests**: Test individual functions (e.g., transformation logic, parsing, validation).
2. **Integration Tests**: Validate interactions between components (e.g., database, APIs, file systems).
3. **System/End-to-End Tests**: Simulate real workflows, ensuring the pipeline works from source to destination.
4. **Data Quality Tests**: Assert data integrity, null checks, referential integrity, and business rules.
5. **Performance & Regression Tests**: Ensure speed and stability, and that new changes don’t break existing functionality.

---

## Pytest Basics

**Pytest** is a powerful testing framework for Python, ideal for data engineering due to its:

- Simple syntax
- Fixture support
- Parametrization
- Mocking capabilities
- Plugins for coverage, mocking, and more

**Install:**

```bash
pip install pytest
```

**Directory Structure Example:**

```
project/
  src/
    transform.py
  tests/
    test_transform.py
    conftest.py
```

---

## Unit Testing Transformation Logic

Focus on pure functions that transform data, parse values, or generate SQL. Avoid testing entire clusters or external systems.

**Example:**

```python
# src/transform.py
def add_total(row):
    return {**row, "total": row["qty"] * row["price"]}

# tests/test_transform.py
from src.transform import add_total

def test_add_total_basic():
    row = {"qty": 2, "price": 10}
    result = add_total(row)
    assert result["total"] == 20
```

---

## Using Fixtures for Reusable Setup

Fixtures help create shared objects (e.g., Spark session, sample DataFrames, configs).

**Example:**

```python
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
```

**Usage in Test:**

```python
def test_total_by_category(sales_df):
    result = total_by_category(sales_df)
    rows = { (r["category"], r["total"]) for r in result.collect() }
    assert rows == {("A", 250.0), ("B", 200.0)}
```

---

## Parametrized Testing

Use `@pytest.mark.parametrize` to test multiple input/output cases efficiently.

**Example:**

```python
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
```

---

## Mocking External Systems

Never hit real databases, S3, or APIs in unit tests. Use mocks to simulate external interactions.

**Tools:**
- `unittest.mock` (built-in)
- `pytest-mock` plugin

**Example:**

```python
# src/io.py
import boto3

def list_keys(bucket):
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket=bucket)
    return [o["Key"] for o in resp.get("Contents", [])]

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
```

**Key Point:** Mocking makes tests fast, deterministic, and network-independent.

---

## Testing Data Transformations (Spark/Pandas)

Pattern: Provide small input → call transformation → assert on output.

**Example:**

```python
# src/transform.py
from pyspark.sql import functions as F

def total_by_category(df):
    return (df.groupBy("category")
              .agg(F.sum("amount").alias("total")))
```

**Test:**

```python
def test_total_by_category(sales_df):
    result = total_by_category(sales_df)
    rows = { (r["category"], r["total"]) for r in result.collect() }
    assert rows == {("A", 250.0), ("B", 200.0)}
```

---

## Data Quality Testing

Encode rules like "no NULLs in key columns" or "amount >= 0" as functions and test them.

**Example:**

```python
# src/quality.py
def check_no_nulls(df, cols):
    return all(df.filter(df[c].isNull()).count() == 0 for c in cols)

# tests/test_quality.py
def test_check_no_nulls_pass(spark):
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "category"])
    from src.quality import check_no_nulls
    assert check_no_nulls(df, ["id", "category"])

def test_check_no_nulls_fail(spark):
    df = spark.createDataFrame([(1, None)], ["id", "category"])
    from src.quality import check_no_nulls
    assert not check_no_nulls(df, ["category"])
```

---

## Markers, Skipping, and Slow Tests

Pytest markers let you tag tests (e.g., slow, integration, spark).

**Example:**

```python
import pytest

@pytest.mark.slow
def test_big_dataset():
    ...
```

**Run only unit tests:**

```bash
pytest -m "not slow"
```

---

## Best Practices for Data Engineering Testing

- Prefer pure functions for transformations.
- One logical check per test; keep tests small and readable.
- Use fixtures for common setup, parametrize for many cases.
- Mock IO and external services; never depend on real infrastructure in unit tests.
- Separate unit vs integration tests via markers.
- Run tests in CI on every commit; fail the build if tests fail.
- Track coverage (e.g., `pytest --cov=src`) but prioritize meaningful tests over chasing 100%.

---

## Example Interview Script: "How Do You Test Data Pipelines?"

> "For data pipelines, I write Pytest unit tests around transformation functions, using small Spark DataFrames and fixtures for setup. I parametrize tests to cover edge cases and use mocks to isolate external systems like S3 or Postgres, so tests are fast and deterministic. I separate pure unit tests from integration tests with markers, run them in CI on each commit, and add data quality checks (nulls, referential integrity, range checks) as standalone functions with their own tests."

---

## Additional Resources

- [Pytest Documentation](https://docs.pytest.org/en/latest/)
- [pytest-mock Plugin](https://pytest-mock.readthedocs.io/en/latest/)
- [Testing Spark with Pytest](https://medium.com/@achillesrasquinha/pytest-for-pyspark-6b7b2d4a6cdd)

---

**End-to-end testing is essential for robust, reliable, and maintainable data engineering workflows. Pytest provides the flexibility and power to test every layer of your pipeline, from transformation logic to integration and quality checks.**