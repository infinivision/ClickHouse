import os.path
import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance', stay_alive=True)
q = instance.query
path_to_data = '/var/lib/clickhouse/'


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        q('CREATE DATABASE test')

        yield cluster

    finally:
        cluster.shutdown()


def test_insertion_deduplication(started_cluster):
    q("DROP TABLE IF EXISTS test.tbl")
    q("CREATE TABLE test.tbl (p Date, k Int8) ENGINE = MergeTree PARTITION BY toYYYYMM(p) ORDER BY p SETTINGS enable_insertion_deduplication=1")

    q("INSERT INTO test.tbl (p, k) VALUES (toDate(1), 1)")
    q("INSERT INTO test.tbl (p, k) VALUES (toDate(2), 2), (toDate(3), 3)")
    expected = TSV('1970-01-02\t1\n1970-01-03\t2\n1970-01-04\t3')
    res = q("SELECT * FROM test.tbl ORDER BY p")
    assert(TSV(res) == expected)

    q("INSERT INTO test.tbl (p, k) VALUES (toDate(2), 2), (toDate(3), 3)")
    res = q("SELECT * FROM test.tbl ORDER BY p")
    assert(TSV(res) == expected)

    q("INSERT INTO test.tbl (p, k) VALUES (toDate(2), 2), (toDate(3), 3), (toDate(60), 60)")
    expected = TSV('1970-01-02\t1\n1970-01-03\t2\n1970-01-04\t3\n1970-03-02\t60')
    res = q("SELECT * FROM test.tbl ORDER BY p")
    assert(TSV(res) == expected)

    q("INSERT INTO test.tbl (p, k) VALUES (toDate(1), 1), (toDate(2), 2), (toDate(3), 3), (toDate(60), 60)")
    expected = TSV('1970-01-02\t1\n1970-01-02\t1\n1970-01-03\t2\n1970-01-03\t2\n1970-01-04\t3\n1970-01-04\t3\n1970-03-02\t60')
    res = q("SELECT * FROM test.tbl ORDER BY p")
    assert(TSV(res) == expected)

    q("ALTER TABLE test.tbl DROP PARTITION '197001'")
    expected = TSV('1970-03-02\t60')
    res = q("SELECT * FROM test.tbl ORDER BY p")
    assert(TSV(res) == expected)

    q("INSERT INTO test.tbl (p, k) VALUES (toDate(1), 1), (toDate(2), 2), (toDate(3), 3), (toDate(60), 60)")
    expected = TSV('1970-01-02\t1\n1970-01-03\t2\n1970-01-04\t3\n1970-03-02\t60')
    res = q("SELECT * FROM test.tbl ORDER BY p")
    assert(TSV(res) == expected)


    q("DROP TABLE IF EXISTS test.tbl2")
    q("RENAME TABLE test.tbl TO test.tbl2")
    q("INSERT INTO test.tbl2 (p, k) VALUES (toDate(2), 2), (toDate(3), 3)")
    expected = TSV('1970-01-02\t1\n1970-01-02\t1\n1970-01-03\t2\n1970-01-03\t2\n1970-01-04\t3\n1970-01-04\t3\n1970-03-02\t60')
    res = q("SELECT * FROM test.tbl2 ORDER BY p")
    assert(TSV(res) == expected)

    instance.restart_clickhouse()
    time.sleep(5)

    q("INSERT INTO test.tbl2 (p, k) VALUES (toDate(2), 2), (toDate(3), 3), (toDate(60), 60), (toDate(120), 120)")
    expected = TSV('1970-01-02\t1\n1970-01-02\t1\n1970-01-03\t2\n1970-01-03\t2\n1970-01-04\t3\n1970-01-04\t3\n1970-03-02\t60\n1970-05-01\t120')
    res = q("SELECT * FROM test.tbl2 ORDER BY p")
    assert(TSV(res) == expected)

    q("DROP TABLE IF EXISTS test.tbl")
    q("DROP TABLE IF EXISTS test.tbl2")
