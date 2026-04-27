import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sqlite3
import pytest
from consumer.metadata_store import init_db, insert_lineage, query_by_run_id, query_by_delta_version

TEST_DB = "./test_metadata.db"

@pytest.fixture(autouse=True)
def setup_db(monkeypatch):
    monkeypatch.setattr("consumer.metadata_store.DB_PATH", TEST_DB)
    init_db()
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

def test_insert_and_query_by_run_id():
    insert_lineage("0", "run_abc123", "a3f9c12", 10, 0, "2026-04-26 10:00:00")
    rows = query_by_run_id("run_abc123")
    assert len(rows) == 1
    assert rows[0][0] == "run_abc123"
    assert rows[0][1] == "a3f9c12"
    assert rows[0][2] == 0

def test_insert_and_query_by_delta_version():
    insert_lineage("0", "run_abc123", "a3f9c12", 10, 0, "2026-04-26 10:00:00")
    insert_lineage("0", "run_def456", "a3f9c12", 10, 0, "2026-04-26 10:00:00")
    rows = query_by_delta_version(0)
    assert len(rows) == 2

def test_query_missing_run_id():
    rows = query_by_run_id("run_doesnotexist")
    assert rows == []