"""
Tests for the TableMigrator class.
"""
import re
import pytest
import oracledb
from unittest.mock import MagicMock, patch, call
from oracle_table_migration.migration.migrator import TableMigrator

@pytest.fixture
def mock_connections():
    """Fixture for mock database connections."""
    # Create source connection mock
    source_conn = MagicMock()
    # Create a fake cursor for source connection if needed
    source_cursor = MagicMock()
    source_conn.connection.cursor.return_value = source_cursor

    # Create target connection mock with a cursor attribute
    target_conn = MagicMock()
    target_cursor = MagicMock()
    target_conn.cursor = target_cursor
    target_conn.connection.commit = MagicMock()

    return source_conn, target_conn

@pytest.fixture
def table_migrator(mock_connections):
    """Fixture to create a TableMigrator instance."""
    source_conn, target_conn = mock_connections
    # Default migration settings for testing
    migration_settings = {
        'migrate_indexes_globally': False,
        'default_chunk_size': 10000
    }
    migrator = TableMigrator(source_conn, target_conn, migration_settings)
    # Mock schema_validator to ensure it's a MagicMock object that supports assertion methods
    migrator.schema_validator = MagicMock()
    return migrator

def test_get_column_type(table_migrator):
    """Test the mapping of Oracle data types to python-oracledb types."""
    # Test standard types
    assert table_migrator.get_column_type("NUMBER") == oracledb.DB_TYPE_NUMBER
    assert table_migrator.get_column_type("VARCHAR2") == oracledb.DB_TYPE_VARCHAR
    assert table_migrator.get_column_type("CHAR") == oracledb.DB_TYPE_CHAR
    assert table_migrator.get_column_type("DATE") == oracledb.DB_TYPE_DATE
    assert table_migrator.get_column_type("TIMESTAMP") == oracledb.DB_TYPE_TIMESTAMP
    assert table_migrator.get_column_type("CLOB") == oracledb.DB_TYPE_CLOB
    assert table_migrator.get_column_type("BLOB") == oracledb.DB_TYPE_BLOB
    assert table_migrator.get_column_type("RAW") == oracledb.DB_TYPE_RAW
    assert table_migrator.get_column_type("LONG") == oracledb.DB_TYPE_LONG

    # Test unknown type (should default to VARCHAR)
    assert table_migrator.get_column_type("UNKNOWN_TYPE") == oracledb.DB_TYPE_VARCHAR

    # Test type with precision/scale
    assert table_migrator.get_column_type("NUMBER(10,2)") == oracledb.DB_TYPE_NUMBER
    assert table_migrator.get_column_type("VARCHAR2(4000)") == oracledb.DB_TYPE_VARCHAR

    # Test TIMESTAMP with precision
    assert table_migrator.get_column_type("TIMESTAMP(6)") == oracledb.DB_TYPE_TIMESTAMP

def test_convert_value(table_migrator):
    """Test conversion of values based on target data types."""
    # For None, should return None regardless of target type
    assert table_migrator.convert_value(None, "NUMBER") is None
    assert table_migrator.convert_value(None, "VARCHAR2") is None
    assert table_migrator.convert_value(None, "DATE") is None

    # For VARCHAR2, non-string values should be converted to string
    assert table_migrator.convert_value(123, "VARCHAR2") == "123"
    assert table_migrator.convert_value(3.14, "VARCHAR2") == "3.14"
    assert table_migrator.convert_value(True, "VARCHAR2") == "True"

    # For other types, values should be passed through unchanged
    assert table_migrator.convert_value(123, "NUMBER") == 123
    assert table_migrator.convert_value("2023-01-01", "DATE") == "2023-01-01"

def test_convert_row_values(table_migrator):
    """Test conversion of all values in a row."""
    # Define a sample row and column types
    row = (1, 123, None, "test")
    column_types = [
        {"data_type": "NUMBER"},
        {"data_type": "VARCHAR2"},
        {"data_type": "DATE"},
        {"data_type": "VARCHAR2"}
    ]

    # Mock convert_value to track calls and return predictable values
    with patch.object(table_migrator, 'convert_value') as mock_convert:
        mock_convert.side_effect = lambda val, type_: f"{val}-{type_}" if val is not None else None
        converted = table_migrator.convert_row_values(row, column_types)

    # Verify convert_value was called for each column with correct parameters
    assert mock_convert.call_count == len(row)
    expected_calls = [
        call(1, "NUMBER"),
        call(123, "VARCHAR2"),
        call(None, "DATE"),
        call("test", "VARCHAR2")
    ]
    mock_convert.assert_has_calls(expected_calls)

    # Without mocking, test actual conversion
    converted = table_migrator.convert_row_values(row, column_types)
    assert converted == (1, "123", None, "test")

def test_get_row_count_default_query(table_migrator, mock_connections):
    """Test getting row count with default query."""
    source_conn, _ = mock_connections
    # Prepare count query return value
    source_conn.execute_query.return_value = [(10,)]
    
    # Without custom_query, method should build a default query
    count = table_migrator.get_row_count("TEST_TABLE")
    
    # Verify that the row count is correctly returned
    assert count == 10
    source_conn.execute_query.assert_called_once_with("SELECT COUNT(*) FROM TEST_TABLE")

def test_get_row_count_custom_query(table_migrator, mock_connections):
    """Test getting row count with custom query."""
    source_conn, _ = mock_connections
    # Prepare return value
    source_conn.execute_query.return_value = [(5,)]
    
    # With custom_query, method should use the provided query
    custom_query = "SELECT * FROM TEST_TABLE WHERE ID > 5"
    count = table_migrator.get_row_count("TEST_TABLE", custom_query=custom_query)
    
    # Verify that the row count is correctly returned
    assert count == 5
    source_conn.execute_query.assert_called_once_with(f"SELECT COUNT(*) FROM ({custom_query})")

def test_get_row_count_empty_result(table_migrator, mock_connections):
    """Test getting row count when query returns empty result."""
    source_conn, _ = mock_connections
    # Simulate empty result
    source_conn.execute_query.return_value = []
    
    # Method should handle empty result
    count = table_migrator.get_row_count("TEST_TABLE")
    
    # Should return 0 for empty result
    assert count == 0

def test_get_data_without_chunking(table_migrator, mock_connections):
    """Test retrieving data without chunking."""
    source_conn, _ = mock_connections
    # Simulate return value
    expected_data = [(1, "a"), (2, "b"), (3, "c")]
    source_conn.execute_query.return_value = expected_data
    
    # Call get_data without chunking
    data = table_migrator.get_data("TEST_TABLE", chunk_size=None)
    
    # Verify that data is correctly returned
    assert data == expected_data
    source_conn.execute_query.assert_called_once_with("SELECT * FROM TEST_TABLE")

def test_get_data_with_custom_query(table_migrator, mock_connections):
    """Test retrieving data with custom query."""
    source_conn, _ = mock_connections
    # Simulate return value
    expected_data = [(4, "d"), (5, "e")]
    source_conn.execute_query.return_value = expected_data
    
    # Call get_data with custom query
    custom_query = "SELECT * FROM TEST_TABLE WHERE ID > 3"
    data = table_migrator.get_data("TEST_TABLE", custom_query=custom_query, chunk_size=None)
    
    # Verify that data is correctly returned
    assert data == expected_data
    source_conn.execute_query.assert_called_once_with(custom_query)

def test_get_data_with_chunking(table_migrator):
    """Test retrieving data with chunking."""
    # Mock _get_data_in_chunks to return predetermined chunks
    test_chunks = [[(1, "a"), (2, "b")], [(3, "c")]]
    
    with patch.object(table_migrator, '_get_data_in_chunks') as mock_get_chunks:
        mock_get_chunks.return_value = (chunk for chunk in test_chunks)
        
        # Call get_data with chunking
        data_gen = table_migrator.get_data("TEST_TABLE", chunk_size=2)
        
        # Collect chunks from generator
        chunks = list(data_gen)
    
    # Verify that chunks are correctly returned
    assert chunks == test_chunks
    mock_get_chunks.assert_called_once_with("SELECT * FROM TEST_TABLE", 2)

def test_get_data_in_chunks(table_migrator, mock_connections):
    """Test the _get_data_in_chunks generator."""
    source_conn, _ = mock_connections
    cursor = source_conn.connection.cursor.return_value
    
    # Mock cursor behavior
    cursor.fetchmany.side_effect = [[(1, "a"), (2, "b")], [(3, "c")], []]
    
    # Call the generator
    query = "SELECT * FROM TEST_TABLE"
    chunk_size = 2
    chunks = list(table_migrator._get_data_in_chunks(query, chunk_size))
    
    # Verify behavior
    assert chunks == [[(1, "a"), (2, "b")], [(3, "c")]]
    cursor.execute.assert_called_once_with(query)
    assert cursor.arraysize == chunk_size
    assert cursor.fetchmany.call_count == 3
    cursor.close.assert_called_once()

def test_get_column_names(table_migrator):
    """Test retrieving column names."""
    # Mock schema_validator.get_table_schema
    sample_schema = [
        {"name": "ID", "data_type": "NUMBER"},
        {"name": "NAME", "data_type": "VARCHAR2"},
        {"name": "CREATED_DATE", "data_type": "DATE"}
    ]
    
    table_migrator.schema_validator.get_table_schema.return_value = sample_schema
    
    # Call get_column_names
    column_names = table_migrator.get_column_names("TEST_TABLE")
    
    # Verify that column names are correctly returned
    assert column_names == ["ID", "NAME", "CREATED_DATE"]
    table_migrator.schema_validator.get_table_schema.assert_called_once_with(table_migrator.source_conn, "TEST_TABLE")

def test_prepare_insert_statement(table_migrator):
    """Test preparing INSERT statement."""
    # Call prepare_insert_statement
    column_names = ["ID", "NAME", "CREATED_DATE"]
    insert_stmt = table_migrator.prepare_insert_statement("TEST_TABLE", column_names)
    
    # Verify that statement is correctly formatted
    expected_stmt = "INSERT INTO TEST_TABLE (ID, NAME, CREATED_DATE) VALUES (:1, :2, :3)"
    assert insert_stmt == expected_stmt

def test_migrate_table_append_to_compatible_table(table_migrator):
    """Test migrating data to an existing compatible table."""
    # Mock dependencies
    table_migrator.schema_validator.table_exists = MagicMock(return_value=True)
    table_migrator.schema_validator.schemas_match = MagicMock(return_value=True)
    table_migrator.get_row_count = MagicMock(return_value=3)
    
    # Mock schema for column names and types
    sample_schema = [
        {"name": "ID", "data_type": "NUMBER"},
        {"name": "NAME", "data_type": "VARCHAR2"}
    ]
    table_migrator.schema_validator.get_table_schema = MagicMock(return_value=sample_schema)
    
    # Mock data retrieval
    test_data = [[(1, "Test 1"), (2, "Test 2"), (3, "Test 3")]]
    with patch.object(table_migrator, 'get_data', return_value=test_data):
        # Mock progress bar
        mock_progress = MagicMock()
        mock_progress.__enter__.return_value = mock_progress
        mock_progress.add_task.return_value = "task1"
        
        with patch('oracle_table_migration.migration.migrator.create_progress_bar', return_value=mock_progress):
            # Call migrate_table
            result = table_migrator.migrate_table(
                "TEST_TABLE",
                mode="full",
                existing_table_behavior="append_if_compatible"
            )
    
    # Verify that migration was successful
    assert result is True
    
    # Verify that table was not dropped or created
    table_migrator.schema_validator.drop_table.assert_not_called() if hasattr(table_migrator.schema_validator, 'drop_table') else None
    table_migrator.schema_validator.create_table.assert_not_called() if hasattr(table_migrator.schema_validator, 'create_table') else None

def test_migrate_table_drop_and_recreate(table_migrator):
    """Test migrating data with drop_and_recreate behavior."""
    # Mock dependencies
    table_migrator.schema_validator.table_exists = MagicMock(return_value=True)
    table_migrator.schema_validator.schemas_match = MagicMock(return_value=False)
    table_migrator.schema_validator.drop_table = MagicMock(return_value=True)
    table_migrator.schema_validator.create_table = MagicMock(return_value=True)
    table_migrator.get_row_count = MagicMock(return_value=3)
    
    # Mock schema for column names and types
    sample_schema = [
        {"name": "ID", "data_type": "NUMBER"},
        {"name": "NAME", "data_type": "VARCHAR2"}
    ]
    table_migrator.schema_validator.get_table_schema = MagicMock(return_value=sample_schema)
    
    # Mock data retrieval
    test_data = [[(1, "Test 1"), (2, "Test 2"), (3, "Test 3")]]
    with patch.object(table_migrator, 'get_data', return_value=test_data):
        # Mock progress bar
        mock_progress = MagicMock()
        mock_progress.__enter__.return_value = mock_progress
        mock_progress.add_task.return_value = "task1"
        
        with patch('oracle_table_migration.migration.migrator.create_progress_bar', return_value=mock_progress):
            # Call migrate_table
            result = table_migrator.migrate_table(
                "TEST_TABLE",
                mode="full",
                existing_table_behavior="drop_and_recreate"
            )
    
    # Verify that migration was successful
    assert result is True
    
    # Verify that table was dropped and created
    table_migrator.schema_validator.drop_table.assert_called_once_with("TEST_TABLE")
    table_migrator.schema_validator.create_table.assert_called_once_with("TEST_TABLE")
def test_migrate_table_custom_query(table_migrator):
    """Test migrating data using custom query."""
    # Mock dependencies
    table_migrator.schema_validator.table_exists = MagicMock(return_value=False)
    table_migrator.schema_validator.create_table = MagicMock(return_value=True)
    
    custom_query = "SELECT * FROM TEST_TABLE WHERE ID > 5"
    table_migrator.get_row_count = MagicMock(return_value=2)
    
    # Mock schema for column names and types
    sample_schema = [
        {"name": "ID", "data_type": "NUMBER"},
        {"name": "NAME", "data_type": "VARCHAR2"}
    ]
    table_migrator.schema_validator.get_table_schema = MagicMock(return_value=sample_schema)
    
    # Mock data retrieval
    test_data = [[(6, "Test 6"), (7, "Test 7")]]
    mock_get_data = MagicMock(return_value=test_data)
    
    with patch.object(table_migrator, 'get_data', mock_get_data):
        # Mock progress bar
        mock_progress = MagicMock()
        mock_progress.__enter__.return_value = mock_progress
        mock_progress.add_task.return_value = "task1"
        
        with patch('oracle_table_migration.migration.migrator.create_progress_bar', return_value=mock_progress):
            # Call migrate_table
            result = table_migrator.migrate_table(
                "TEST_TABLE",
                mode="custom",
                custom_query=custom_query
            )
    
    # Verify that migration was successful
    assert result is True
    
    # Verify that row count used the custom query
    table_migrator.get_row_count.assert_called_once_with("TEST_TABLE", custom_query)
    
    # Verify that get_data was called with the custom query
def test_migrate_table_no_data(table_migrator):
    """Test migrating table with no data."""
    # Mock dependencies
    table_migrator.schema_validator.table_exists = MagicMock(return_value=False)
    table_migrator.schema_validator.create_table = MagicMock(return_value=True)
    table_migrator.get_row_count = MagicMock(return_value=0)
    
    # Mock get_data
    mock_get_data = MagicMock()
    
    with patch.object(table_migrator, 'get_data', mock_get_data):
        # Call migrate_table
        result = table_migrator.migrate_table("EMPTY_TABLE")
        
        # Verify that migration was successful
        assert result is True
        
        # Verify that get_data was not called (no data to migrate)
        mock_get_data.assert_not_called()
    
    # No need to check outside the with block as the mock is no longer valid

def test_migrate_table_failed_to_drop(table_migrator):
    """Test migration failure when dropping table fails."""
    # Mock dependencies
    table_migrator.schema_validator.table_exists = MagicMock(return_value=True)
    table_migrator.schema_validator.drop_table = MagicMock(return_value=False)
    
    # Call migrate_table
    result = table_migrator.migrate_table(
        "TEST_TABLE",
        existing_table_behavior="drop_and_recreate"
    )
    
    # Verify that migration failed
    assert result is False

def test_migrate_table_failed_to_create(table_migrator):
    """Test migration failure when creating table fails."""
    # Mock dependencies
    table_migrator.schema_validator.table_exists = MagicMock(return_value=False)
    table_migrator.schema_validator.create_table = MagicMock(return_value=False)
    
    # Call migrate_table
    result = table_migrator.migrate_table("TEST_TABLE")
    
    # Verify that migration failed
    assert result is False

def test_migrate_table_incompatible_schema(table_migrator):
    """Test migration failure with incompatible schema."""
    # Mock dependencies
    table_migrator.schema_validator.table_exists = MagicMock(return_value=True)
    table_migrator.schema_validator.schemas_match = MagicMock(return_value=False)
    
    # Call migrate_table
    result = table_migrator.migrate_table(
        "TEST_TABLE",
        existing_table_behavior="append_if_compatible"
    )
    
    # Verify that migration failed
    assert result is False

def test_migrate_table_invalid_behavior(table_migrator):
    """Test migration failure with invalid existing_table_behavior."""
    # Mock dependencies
    table_migrator.schema_validator.table_exists = MagicMock(return_value=True)
    
    # Call migrate_table
    result = table_migrator.migrate_table(
        "TEST_TABLE",
        existing_table_behavior="invalid_behavior"
    )
    
    # Verify that migration failed
    assert result is False