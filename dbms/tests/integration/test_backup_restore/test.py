import os.path
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')
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


def exec_bash(cmd):
    cmd = '/bin/bash -c "{}"'.format(cmd.replace('"', '\\"'))
    return instance.exec_in_container(cmd)


def copy_backup_to_detached(database, table):
    fp_increment = os.path.join(path_to_data, 'shadow/increment.txt')
    increment = exec_bash('cat ' + fp_increment).strip()
    fp_backup = os.path.join(path_to_data, 'shadow', increment, 'data', database, table)
    fp_detached = os.path.join(path_to_data, 'data', database, table, 'detached')
    exec_bash('cp -r {}/* {}/'.format(fp_backup, fp_detached))


@pytest.fixture
def backup_restore(started_cluster):
    q("DROP TABLE IF EXISTS test.partition")
    q("CREATE TABLE test.partition (p Date, k Int8) ENGINE = MergeTree PARTITION BY toYYYYMM(p) ORDER BY p")
    q("INSERT INTO test.partition (p, k) VALUES(toDate(1), 1)")
    q("INSERT INTO test.partition (p, k) VALUES(toDate(50), 50)")

    yield

    q("DROP TABLE test.partition")


def test_backup_restore(backup_restore):
    q("ALTER TABLE test.partition UPDATE k=100 WHERE 1")
    q("SELECT sleep(2)")

    expected = TSV('1970-01-02\t100\n1970-02-20\t100')
    res = q("SELECT * FROM test.partition ORDER BY p")
    assert(TSV(res) == expected)

    q("ALTER TABLE test.partition FREEZE")
    q("DROP TABLE test.partition")
    q("CREATE TABLE test.partition (p Date, k Int8) ENGINE = MergeTree PARTITION BY toYYYYMM(p) ORDER BY p")

    copy_backup_to_detached('test', 'partition')

    q("ALTER TABLE test.partition ATTACH PARTITION 197001")
    q("ALTER TABLE test.partition ATTACH PARTITION 197002")
    q("SELECT sleep(2)")

    expected = TSV('1970-01-02\t100\n1970-02-20\t100')
    res = q("SELECT * FROM test.partition ORDER BY p")
    assert(TSV(res) == expected)

    q("ALTER TABLE test.partition UPDATE k=10 WHERE 1")
    q("SELECT sleep(2)")

    expected = TSV('1970-01-02\t10\n1970-02-20\t10')
    res = q("SELECT * FROM test.partition ORDER BY p")
    assert(TSV(res) == expected)
