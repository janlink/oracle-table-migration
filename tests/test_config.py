"""
Tests for the configuration loader module.
"""
import os
import pytest
from pathlib import Path
from oracle_table_migration.config.config_loader import ConfigLoader

@pytest.fixture
def sample_config_file(tmp_path):
    """Create a sample configuration file for testing."""
    config_content = """
    tables:
      - name: TEST_TABLE1
        mode: full
      
      - name: TEST_TABLE2
        mode: custom
        query: |
          SELECT * FROM TEST_TABLE2 
          WHERE id < 100
    """
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(config_content)
    return str(config_file)

@pytest.fixture
def setup_env_vars():
    """Set up environment variables for testing."""
    os.environ["SOURCE_DB_USERNAME"] = "test_source_user"
    os.environ["SOURCE_DB_PASSWORD"] = "test_source_pass"
    os.environ["SOURCE_DB_DSN"] = "test_source_dsn"
    
    os.environ["TARGET_DB_USERNAME"] = "test_target_user"
    os.environ["TARGET_DB_PASSWORD"] = "test_target_pass"
    os.environ["TARGET_DB_DSN"] = "test_target_dsn"
    
    yield
    
    # Clean up environment variables
    del os.environ["SOURCE_DB_USERNAME"]
    del os.environ["SOURCE_DB_PASSWORD"]
    del os.environ["SOURCE_DB_DSN"]
    del os.environ["TARGET_DB_USERNAME"]
    del os.environ["TARGET_DB_PASSWORD"]
    del os.environ["TARGET_DB_DSN"]

def test_load_config(sample_config_file):
    """Test loading configuration from a file."""
    config_loader = ConfigLoader(sample_config_file)
    tables_config = config_loader.get_tables_config()
    
    assert len(tables_config) == 2
    assert tables_config[0]["name"] == "TEST_TABLE1"
    assert tables_config[0]["mode"] == "full"
    assert tables_config[1]["name"] == "TEST_TABLE2"
    assert tables_config[1]["mode"] == "custom"
    assert "SELECT * FROM TEST_TABLE2" in tables_config[1]["query"]

def test_get_table_config(sample_config_file):
    """Test retrieving configuration for a specific table."""
    config_loader = ConfigLoader(sample_config_file)
    
    table_config = config_loader.get_table_config("TEST_TABLE1")
    assert table_config is not None
    assert table_config["mode"] == "full"
    
    table_config = config_loader.get_table_config("NONEXISTENT_TABLE")
    assert table_config is None

def test_db_config(sample_config_file, setup_env_vars):
    """Test retrieving database configuration from environment variables."""
    config_loader = ConfigLoader(sample_config_file)
    
    source_config = config_loader.get_source_db_config()
    assert source_config["username"] == "test_source_user"
    assert source_config["password"] == "test_source_pass"
    assert source_config["dsn"] == "test_source_dsn"
    
    target_config = config_loader.get_target_db_config()
    assert target_config["username"] == "test_target_user"
    assert target_config["password"] == "test_target_pass"
    assert target_config["dsn"] == "test_target_dsn"