"""
Database connection module for Oracle Table Migration Tool.
"""
import oracledb
from typing import Dict, Any, List, Tuple
from oracle_table_migration.utils.logger import logger

class DatabaseConnection:
    """Class for managing Oracle database connections."""

    def __init__(self, config: Dict[str, str]):
        """
        Initialize database connection.

        Args:
            config (Dict[str, str]): Database connection configuration
        """
        self.username = config.get('username')
        self.password = config.get('password')
        self.dsn = config.get('dsn')
        self.schema = config.get('schema')
        self.connection = None
        self.cursor = None

    def connect(self):
        """
        Establish database connection.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            logger.info(f"Connecting to database {self.dsn}")
            self.connection = oracledb.connect(
                user=self.username,
                password=self.password,
                dsn=self.dsn
            )
            self.cursor = self.connection.cursor()
            
            if self.schema:
                self.cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {self.schema}")
                logger.info(f"Set current schema to {self.schema}")
                
            logger.info(f"Connected successfully to {self.dsn}")
            return True
        except oracledb.Error as e:
            logger.error(f"Database connection error: {e}")
            return False

    def disconnect(self):
        """Close database connection and cursor."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.info(f"Disconnected from database {self.dsn}")
        except oracledb.Error as e:
            logger.error(f"Error disconnecting from database: {e}")

    def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Tuple]:
        """
        Execute a SQL query and return results.

        Args:
            query (str): SQL query to execute
            params (Dict[str, Any], optional): Query parameters

        Returns:
            List[Tuple]: Query results
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            results = self.cursor.fetchall()
            return results
        except oracledb.Error as e:
            logger.error(f"Query execution error: {e}")
            logger.error(f"Query: {query}")
            return []

    def execute_non_query(self, query: str, params: Dict[str, Any] = None) -> bool:
        """
        Execute a non-query SQL statement (INSERT, UPDATE, etc.).

        Args:
            query (str): SQL statement to execute
            params (Dict[str, Any], optional): Query parameters

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            self.connection.commit()
            return True
        except oracledb.Error as e:
            logger.error(f"Statement execution error: {e}")
            logger.error(f"Statement: {query}")
            return False