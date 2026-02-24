import pytest
import sqlite3
import pandas as pd
from main import fetch_data, transform_data, setup_db, load_bronze, load_silver, load_gold

@pytest.fixture(scope="module")
def db_conn():
    conn = setup_db()
    yield conn
    conn.close()

@pytest.fixture
def sample_raw():
    return fetch_data()

@pytest.fixture
def sample_df(sample_raw):
    return transform_data(sample_raw)

def test_fetch_data(sample_raw):
    assert "bpi" in sample_raw

def test_transform_data(sample_df):
    assert not sample_df.empty
    assert "currency" in sample_df.columns
    assert "rate" in sample_df.columns

def test_load_bronze(db_conn, sample_raw):
    load_bronze(db_conn, sample_raw)
    cur = db_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM bronze")
    assert cur.fetchone()[0] > 0

def test_load_silver(db_conn, sample_df):
    load_silver(db_conn, sample_df)
    cur = db_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM silver")
    assert cur.fetchone()[0] > 0

def test_load_gold(db_conn, sample_df):
    load_gold(db_conn, sample_df)
    cur = db_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM gold")
    assert cur.fetchone()[0] > 0
