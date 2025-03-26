"""
Tests for the database connection module.
"""
import pytest
from unittest.mock import patch, MagicMock
from oracle_table_migration.db.connection import DatabaseConnection
import oracledb

@pytest.fixture
def db_config():
    """Sample database configuration."""
    return {
        'username': 'test_user',
        'password': 'test_password',
        'dsn': 'test_host:1521/test_service'
    }

@patch('oracle_table_migration.db.connection.oracledb.connect')
def test_connect_success(mock_connect, db_config):
    """Test successful database connection."""
    # Setup mock
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    
    # Create connection
    db_conn = DatabaseConnection(db_config)
    result = db_conn.connect()
    
    # Assertions
    assert result is True
    mock_connect.assert_called_once_with(
        user=db_config['username'],
        password=db_config['password'],
        dsn=db_config['dsn']
    )
    assert db_conn.connection == mock_connection
    assert db_conn.cursor == mock_cursor

@patch('oracle_table_migration.db.connection.oracledb.connect')
def test_connect_failure(mock_connect, db_config):
    """Test database connection failure."""
    # Setup mock to raise an exception
    mock_connect.side_effect = oracledb.Error("Connection error")
    
    # Create connection
    db_conn = DatabaseConnection(db_config)
    result = db_conn.connect()
    
    # Assertions
    assert result is False
    assert db_conn.connection is None
    assert db_conn.cursor is None

@patch('oracle_table_migration.db.connection.oracledb.connect')
def test_execute_query(mock_connect, db_config):
    """Test query execution."""
    # Setup mocks
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    
    # Setup mock cursor to return test data
    mock_cursor.fetchall.return_value = [(1, 'test'), (2, 'test2')]
    
    # Create connection and execute query
    db_conn = DatabaseConnection(db_config)
    db_conn.connect()
    results = db_conn.execute_query("SELECT * FROM test_table")
    
    # Assertions
    assert results == [(1, 'test'), (2, 'test2')]
    mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")
    mock_cursor.fetchall.assert_called_once()

@patch('oracle_table_migration.db.connection.oracledb.connect')
def test_execute_non_query(mock_connect, db_config):
    """Test non-query execution (INSERT, UPDATE, etc.)."""
    # Setup mocks
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    
    # Create connection and execute non-query
    db_conn = DatabaseConnection(db_config)
    db_conn.connect()
    result = db_conn.execute_non_query("INSERT INTO test_table VALUES (1, 'test')")
    
    # Assertions
    assert result is True
    mock_cursor.execute.assert_called_once_with("INSERT INTO test_table VALUES (1, 'test')")
    mock_connection.commit.assert_called_once()

@patch('oracle_table_migration.db.connection.oracledb.connect')
def test_disconnect(mock_connect, db_config):
    """Test database disconnection."""
    # Setup mocks
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    
    # Create connection and disconnect
    db_conn = DatabaseConnection(db_config)
    db_conn.connect()
    db_conn.disconnect()
    
    # Assertions
    mock_cursor.close.assert_called_once()
    mock_connection.close.assert_called_once()