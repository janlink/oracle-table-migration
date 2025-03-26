"""
Configuration loader module for Oracle Table Migration Tool.
"""
import os
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

class ConfigLoader:
    """Class for loading and accessing configuration."""
    
    def __init__(self, config_path: str):
        """
        Initialize the config loader.
        
        Args:
            config_path (str): Path to the YAML configuration file
        """
        self.config_path = Path(config_path)
        
        # Load environment variables from .env file
        load_dotenv()
        
        # Load YAML configuration
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load YAML configuration file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
            
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def get_source_db_config(self) -> Dict[str, str]:
        """Return source database configuration from environment variables."""
        return {
            'username': os.getenv('SOURCE_DB_USERNAME'),
            'password': os.getenv('SOURCE_DB_PASSWORD'),
            'dsn': os.getenv('SOURCE_DB_DSN')
        }
    
    def get_target_db_config(self) -> Dict[str, str]:
        """Return target database configuration from environment variables."""
        return {
            'username': os.getenv('TARGET_DB_USERNAME'),
            'password': os.getenv('TARGET_DB_PASSWORD'),
            'dsn': os.getenv('TARGET_DB_DSN')
        }
    
    def get_tables_config(self) -> List[Dict[str, Any]]:
        """Return tables configuration from YAML file."""
        return self.config.get('tables', [])

    def get_default_chunk_size(self) -> int:
        """Return the default chunk size from the settings."""
        settings = self.config.get('settings', {})
        return settings.get('default_chunk_size', 10000)  # Default to 10000 if not specified

    def get_table_config(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a specific table.
        
        Args:
            table_name (str): Name of the table to get config for
            
        Returns:
            Optional[Dict[str, Any]]: Table configuration or None if not found
        """
        tables = self.get_tables_config()
        for table in tables:
            if table.get('name') == table_name:
                return table
        return None
        
    def get_table_chunk_size(self, table_name: str) -> int:
        """
        Get configured chunk size for a specific table.
        
        Args:
            table_name (str): Name of the table to get chunk size for
            
        Returns:
            int: Configured chunk size or default if not specified
        """
        default_size = self.get_default_chunk_size()
        table_config = self.get_table_config(table_name)
        
        if table_config and 'chunk_size' in table_config:
            return table_config.get('chunk_size')
        
        return default_size