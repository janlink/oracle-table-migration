"""
Tests for the schema validator module.
"""
import pytest
from unittest.mock import patch, MagicMock
from oracle_table_migration.db.schema_validator import SchemaValidator
from oracle_table_migration.db.connection import DatabaseConnection

@pytest.fixture
def mock_connections():
    """Setup mock database connections."""
    source_conn = MagicMock(spec=DatabaseConnection)
    target_conn = MagicMock(spec=DatabaseConnection)
    return source_conn, target_conn

@pytest.fixture
def schema_validator(mock_connections):
    """Create a schema validator with mock connections."""
    source_conn, target_conn = mock_connections
    return SchemaValidator(source_conn, target_conn)

def test_table_exists(schema_validator, mock_connections):
    """Test checking if a table exists."""
    source_conn, _ = mock_connections
    
    # Table exists
    source_conn.execute_query.return_value = [(1,)]
    assert schema_validator.table_exists(source_conn, "TEST_TABLE") is True
    
    # Table does not exist
    source_conn.execute_query.return_value = [(0,)]
    assert schema_validator.table_exists(source_conn, "TEST_TABLE") is False
    
    # Error case
    source_conn.execute_query.return_value = []
    assert schema_validator.table_exists(source_conn, "TEST_TABLE") is False

def test_get_table_schema(schema_validator, mock_connections):
    """Test retrieving a table schema."""
    source_conn, _ = mock_connections
    
    # Mock the query result
    mock_columns = [
        ("ID", "NUMBER", None, 10, 0, "N"),
        ("NAME", "VARCHAR2", 100, None, None, "Y"),
        ("CREATED_DATE", "DATE", None, None, None, "Y")
    ]
    source_conn.execute_query.return_value = mock_columns
    
    schema = schema_validator.get_table_schema(source_conn, "TEST_TABLE")
    
    # Verify the result
    assert len(schema) == 3
    assert schema[0]["name"] == "ID"
    assert schema[0]["data_type"] == "NUMBER"
    assert schema[0]["data_precision"] == 10
    assert schema[0]["nullable"] == "N"
    
    assert schema[1]["name"] == "NAME"
    assert schema[1]["data_type"] == "VARCHAR2"
    assert schema[1]["data_length"] == 100
    
    assert schema[2]["name"] == "CREATED_DATE"
    assert schema[2]["data_type"] == "DATE"

def test_schemas_match_equal(schema_validator, mock_connections):
    """Test schema comparison when schemas match."""
    source_conn, target_conn = mock_connections
    
    # Both tables exist
    source_conn.execute_query.side_effect = [
        [(1,)],  # table_exists for source
        [        # get_table_schema for source
            ("ID", "NUMBER", None, 10, 0, "N"),
            ("NAME", "VARCHAR2", 100, None, None, "Y")
        ]
    ]
    
    target_conn.execute_query.side_effect = [
        [(1,)],  # table_exists for target
        [        # get_table_schema for target
            ("ID", "NUMBER", None, 10, 0, "N"),
            ("NAME", "VARCHAR2", 100, None, None, "Y")
        ]
    ]
    
    assert schema_validator.schemas_match("TEST_TABLE") is True

def test_schemas_match_different(schema_validator, mock_connections):
    """Test schema comparison when schemas don't match."""
    source_conn, target_conn = mock_connections
    
    # Both tables exist but have different schemas
    source_conn.execute_query.side_effect = [
        [(1,)],  # table_exists for source
        [        # get_table_schema for source
            ("ID", "NUMBER", None, 10, 0, "N"),
            ("NAME", "VARCHAR2", 100, None, None, "Y")
        ]
    ]
    
    target_conn.execute_query.side_effect = [
        [(1,)],  # table_exists for target
        [        # get_table_schema for target - different data length
            ("ID", "NUMBER", None, 10, 0, "N"),
            ("NAME", "VARCHAR2", 200, None, None, "Y")
        ]
    ]
    
    assert schema_validator.schemas_match("TEST_TABLE") is False

def test_schemas_match_target_missing(schema_validator, mock_connections):
    """Test schema comparison when target table doesn't exist."""
    source_conn, target_conn = mock_connections
    
    # Source table exists, target doesn't
    source_conn.execute_query.return_value = [(1,)]  # table_exists for source
    target_conn.execute_query.return_value = [(0,)]  # table_exists for target
    
    assert schema_validator.schemas_match("TEST_TABLE") is False

def test_generate_create_table_sql(schema_validator, mock_connections):
    """Test SQL generation for creating a table."""
    source_conn, _ = mock_connections
    
    # Mock the query result
    mock_columns = [
        ("ID", "NUMBER", None, 10, 0, "N"),
        ("NAME", "VARCHAR2", 100, None, None, "Y"),
        ("SALARY", "NUMBER", None, 10, 2, "Y"),
        ("CREATED_DATE", "DATE", None, None, None, "Y")
    ]
    source_conn.execute_query.return_value = mock_columns
    
    sql = schema_validator.generate_create_table_sql("TEST_TABLE")
    
    # Verify SQL contains expected elements
    assert "CREATE TABLE TEST_TABLE" in sql
    assert "ID NUMBER(10) NOT NULL" in sql
    assert "NAME VARCHAR2(100)" in sql
    assert "SALARY NUMBER(10,2)" in sql
    assert "CREATED_DATE DATE" in sql

def test_create_table(schema_validator, mock_connections):
    """Test creating a table in the target database."""
    _, target_conn = mock_connections
    
    # Mock the behavior
    with patch.object(schema_validator, 'generate_create_table_sql') as mock_generate_sql:
        mock_generate_sql.return_value = "CREATE TABLE TEST_TABLE (ID NUMBER(10) NOT NULL)"
        target_conn.execute_non_query.return_value = True
        
        result = schema_validator.create_table("TEST_TABLE")
        
        assert result is True
        mock_generate_sql.assert_called_once_with("TEST_TABLE")
        target_conn.execute_non_query.assert_called_once_with("CREATE TABLE TEST_TABLE (ID NUMBER(10) NOT NULL)")

def test_drop_table(schema_validator, mock_connections):
    """Test dropping a table in the target database."""
    _, target_conn = mock_connections
    
    # Mock successful drop
    target_conn.execute_non_query.return_value = True
    
    result = schema_validator.drop_table("TEST_TABLE")
    
    assert result is True
    target_conn.execute_non_query.assert_called_once_with("DROP TABLE TEST_TABLE")