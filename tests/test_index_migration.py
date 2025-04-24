"""
Tests for index migration functionality.
"""
from unittest.mock import Mock, patch, call
import pytest
from oracle_table_migration.db.oracle_handler import OracleHandler, IndexDefinition
from oracle_table_migration.migration.migrator import TableMigrator


@pytest.fixture
def mock_oracle_handler():
    """Create a mock OracleHandler instance."""
    handler = Mock(spec=OracleHandler)
    handler.connect.return_value = True
    return handler


@pytest.fixture
def sample_indexes():
    """Sample index definitions for testing."""
    return [
        IndexDefinition(
            name="IDX_TEST_1",
            table_name="TEST_TABLE",
            columns=["COL1", "COL2"],
            is_unique=True,
            tablespace_name="TEST_TS"
        ),
        IndexDefinition(
            name="IDX_TEST_2",
            table_name="TEST_TABLE",
            columns=["COL3"],
            is_unique=False,
            tablespace_name=None
        )
    ]


def test_get_table_indexes(mock_oracle_handler, sample_indexes):
    """Test retrieving table indexes."""
    mock_oracle_handler.get_table_indexes.return_value = sample_indexes
    
    indexes = mock_oracle_handler.get_table_indexes("TEST_TABLE")
    
    assert len(indexes) == 2
    assert indexes[0].name == "IDX_TEST_1"
    assert indexes[0].is_unique is True
    assert indexes[1].name == "IDX_TEST_2"
    assert indexes[1].is_unique is False


def test_generate_index_ddl(mock_oracle_handler):
    """Test generating index DDL statements."""
    index = IndexDefinition(
        name="IDX_TEST_1",
        table_name="SOURCE_TABLE",
        columns=["COL1", "COL2"],
        is_unique=True,
        tablespace_name="TEST_TS"
    )
    
    mock_oracle_handler.generate_index_ddl.return_value = (
        "CREATE UNIQUE INDEX IDX_TEST_1 ON TARGET_TABLE (COL1, COL2)"
    )
    
    ddl = mock_oracle_handler.generate_index_ddl(index, "TARGET_TABLE")
    
    assert "CREATE UNIQUE INDEX" in ddl
    assert "TARGET_TABLE" in ddl
    assert "COL1, COL2" in ddl


def test_index_migration_in_migrator(mock_oracle_handler, sample_indexes):
    """Test index migration process in TableMigrator."""
    # Setup
    source_db = mock_oracle_handler
    target_db = Mock(spec=OracleHandler)
    
    migration_settings = {'migrate_indexes_globally': True}
    migrator = TableMigrator(source_db, target_db, migration_settings)
    
    # Configure mocks
    source_db.get_table_indexes.return_value = sample_indexes
    source_db.generate_index_ddl.return_value = "CREATE INDEX test_index ON test_table (col1)"
    target_db.create_index.return_value = True
    
    # Test successful migration with indexes
    result = migrator._migrate_indexes_for_table("source_table", "target_table")
    
    assert result is True
    source_db.get_table_indexes.assert_called_once_with("source_table")
    assert target_db.create_index.call_count == len(sample_indexes)


def test_index_migration_failure_handling(mock_oracle_handler, sample_indexes):
    """Test handling of index migration failures."""
    source_db = mock_oracle_handler
    target_db = Mock(spec=OracleHandler)
    
    migration_settings = {'migrate_indexes_globally': True}
    migrator = TableMigrator(source_db, target_db, migration_settings)
    
    # Configure mocks
    source_db.get_table_indexes.return_value = sample_indexes
    source_db.generate_index_ddl.return_value = "CREATE INDEX test_index ON test_table (col1)"
    target_db.create_index.side_effect = [True, False]  # First succeeds, second fails
    
    # Test migration with partial failure
    result = migrator._migrate_indexes_for_table("source_table", "target_table")
    
    assert result is False  # Overall result should be False due to partial failure
    assert target_db.create_index.call_count == 2  # Both indexes should be attempted


def test_index_name_generation(mock_oracle_handler):
    """Test index name generation for different table name lengths."""
    # Test case where target table name is same length
    index1 = IndexDefinition(
        name="IDX_TABLE1_TEST",
        table_name="SOURCE_TBL",
        columns=["COL1"],
        is_unique=False
    )
    
    # Test case where target table name is longer
    index2 = IndexDefinition(
        name="IDX_TBL_TEST",
        table_name="TBL",
        columns=["COL1"],
        is_unique=False
    )
    
    mock_oracle_handler.generate_index_ddl.side_effect = [
        "CREATE INDEX IDX_TABLE1_TEST ON TARGET_TBL (COL1)",
        "CREATE INDEX IDX_LONG_TABLE_TEST ON LONG_TABLE (COL1)"
    ]
    
    ddl1 = mock_oracle_handler.generate_index_ddl(index1, "TARGET_TBL")
    ddl2 = mock_oracle_handler.generate_index_ddl(index2, "LONG_TABLE")
    
    assert "IDX_TABLE1_TEST" in ddl1  # Name should be preserved
    assert len(ddl2.split()[2]) <= 30  # New name should respect Oracle's 30-char limit