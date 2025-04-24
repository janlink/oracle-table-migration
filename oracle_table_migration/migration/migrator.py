"""
Table migrator module for Oracle Table Migration Tool.
"""
from typing import Dict, List, Any, Tuple, Optional
import re
import oracledb
from oracle_table_migration.db.oracle_handler import OracleHandler
from oracle_table_migration.db.schema_validator import SchemaValidator
from oracle_table_migration.utils.logger import logger, create_progress_bar

class TableMigrator:
    """Class for migrating tables between Oracle databases."""
    
    def __init__(self, source_conn: OracleHandler, target_conn: OracleHandler,
                migration_settings: Dict[str, Any]):
        """
        Initialize table migrator.
        
        Args:
            source_conn (DatabaseConnection): Source database connection
            target_conn (DatabaseConnection): Target database connection
        """
        self.source_conn = source_conn
        self.target_conn = target_conn
        self.schema_validator = SchemaValidator(source_conn, target_conn)
        self.migrate_indexes_globally = migration_settings.get('migrate_indexes_globally', False)

    def get_column_type(self, data_type: str) -> Any:
        """
        Map Oracle data type to python-oracledb type.
        
        Args:
            data_type: Oracle data type name
            
        Returns:
            python-oracledb type constant
        """
        # Extract base type and precision for TIMESTAMP
        timestamp_match = re.match(r'TIMESTAMP\((\d+)\)', data_type)
        
        if timestamp_match:
            # For TIMESTAMP types, we use DB_TYPE_TIMESTAMP regardless of precision
            # The precision is handled by Oracle
            return oracledb.DB_TYPE_TIMESTAMP
            
        type_map = {
            'NUMBER': oracledb.DB_TYPE_NUMBER,
            'VARCHAR2': oracledb.DB_TYPE_VARCHAR,
            'CHAR': oracledb.DB_TYPE_CHAR,
            'DATE': oracledb.DB_TYPE_DATE,
            'TIMESTAMP': oracledb.DB_TYPE_TIMESTAMP,
            'CLOB': oracledb.DB_TYPE_CLOB,
            'BLOB': oracledb.DB_TYPE_BLOB,
            'RAW': oracledb.DB_TYPE_RAW,
            'LONG': oracledb.DB_TYPE_LONG
        }
        
        # Get the base type, removing any precision/scale information
        base_type = data_type.split('(')[0].strip()
        
        return type_map.get(base_type, oracledb.DB_TYPE_VARCHAR)

    def convert_value(self, value: Any, target_type: str) -> Any:
        """
        Convert a value to the appropriate type for Oracle.
        
        Args:
            value: The value to convert
            target_type: The Oracle target data type
            
        Returns:
            Converted value suitable for Oracle
        """
        if value is None:
            return None
            
        if target_type == 'VARCHAR2' and not isinstance(value, str):
            return str(value)
            
        return value

    def convert_row_values(self, row: Tuple, column_types: List[Dict[str, Any]]) -> Tuple:
        """
        Convert values in a row according to target column types.
        
        Args:
            row: The row of data to convert
            column_types: List of column type information
            
        Returns:
            Tuple of converted values
        """
        return tuple(
            self.convert_value(val, col["data_type"])
            for val, col in zip(row, column_types)
        )
    
    def get_row_count(self, table_name: str, custom_query: Optional[str] = None) -> int:
        """
        Get the number of rows to be migrated.
        
        Args:
            table_name (str): Table name
            custom_query (Optional[str]): Custom SQL query
            
        Returns:
            int: Row count
            
        Raises:
            Exception: If query execution fails
        """
        try:
            if custom_query:
                count_query = f"SELECT COUNT(*) FROM ({custom_query})"
                logger.info(f"Executing row count query for custom query on table {table_name}")
            else:
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                logger.info(f"Executing row count query for table {table_name}")
                
            result = self.source_conn.execute_query(count_query)
            if not result:
                raise Exception("No results returned from row count query")
            return result[0][0]
        except Exception as e:
            logger.error(f"Failed to get row count for table {table_name}: {e}")
            raise Exception(f"Row count query failed for table {table_name}") from e
        
    def get_data(self, table_name: str, custom_query: Optional[str] = None, chunk_size: Optional[int] = None) -> List[Tuple]:
        """
        Retrieve data from source table.
        
        Args:
            table_name (str): Table name
            custom_query (Optional[str]): Custom SQL query
            chunk_size (Optional[int]): If provided, return a generator yielding chunks instead of all data
            
        Returns:
            List[Tuple] or Generator: Table data or generator of data chunks
        """
        if custom_query:
            query = custom_query
        else:
            query = f"SELECT * FROM {table_name}"
            
        if chunk_size is None:
            return self.source_conn.execute_query(query)
        else:
            # Return a generator that fetches data in chunks
            return self._get_data_in_chunks(query, chunk_size)
            
    def _get_data_in_chunks(self, query: str, chunk_size: int):
        """
        Generator that fetches data in chunks.
        
        Args:
            query (str): SQL query to execute
            chunk_size (int): Number of rows to fetch in each chunk
            
        Yields:
            List[Tuple]: Chunks of data
        """
        cursor = self.source_conn.connection.cursor()
        cursor.arraysize = chunk_size
        cursor.execute(query)
        
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            yield rows
        
        cursor.close()
        
    def get_column_names(self, table_name: str) -> List[str]:
        """
        Get column names for a table.
        
        Args:
            table_name (str): Table name
            
        Returns:
            List[str]: Column names
        """
        schema = self.schema_validator.get_table_schema(self.source_conn, table_name)
        return [col["name"] for col in schema]
        
    def prepare_insert_statement(self, table_name: str, column_names: List[str]) -> str:
        """
        Prepare a parameterized INSERT statement.
        
        Args:
            table_name (str): Table name
            column_names (List[str]): Column names
            
        Returns:
            str: INSERT statement
        """
        placeholders = ", ".join([f":{i+1}" for i in range(len(column_names))])
        columns = ", ".join(column_names)
        
        return f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
    def migrate_table(self, table_name: str, mode: str = "full", custom_query: Optional[str] = None, 
                     existing_table_behavior: str = "drop_and_recreate", chunk_size: int = 10000) -> bool:
        """
        Migrate a table from source to target database.
        
        Args:
            table_name (str): Table name
            mode (str): Migration mode ('full' or 'custom')
            custom_query (Optional[str]): Custom SQL query for 'custom' mode
            existing_table_behavior (str): How to handle existing tables ('drop_and_recreate' or 'append_if_compatible')
            chunk_size (int): Number of rows to process at once (default: 10000)
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Starting migration for table {table_name}")
        
        table_exists = self.schema_validator.table_exists(self.target_conn, table_name)
        schemas_match = self.schema_validator.schemas_match(table_name) if table_exists else False
        
        if table_exists:
            if existing_table_behavior == "drop_and_recreate":
                logger.info(f"Dropping existing table {table_name} as per configuration")
                if not self.schema_validator.drop_table(table_name):
                    logger.error(f"Failed to drop table {table_name}")
                    return False
                table_exists = False
                schemas_match = False
            elif existing_table_behavior == "append_if_compatible":
                if not schemas_match:
                    logger.error(f"Cannot append to {table_name}: schemas are incompatible")
                    return False
                logger.info(f"Appending to existing table {table_name} as schemas are compatible")
            else:
                logger.error(f"Invalid existing_table_behavior: {existing_table_behavior}")
                return False
        
        if not table_exists:
            logger.info(f"Creating table {table_name} in target database")
            if not self.schema_validator.create_table(table_name):
                logger.error(f"Failed to create table {table_name}")
                return False
        
        # Get row count for progress tracking
        try:
            row_count = self.get_row_count(table_name, custom_query) if mode == "custom" and custom_query else self.get_row_count(table_name)
            if row_count == 0:
                logger.warning(f"No data found for table {table_name}")
                return True
        except Exception as e:
            logger.error(f"Failed to migrate table {table_name}: {e}")
            return False
            
        # Get column information for type conversion
        column_types = self.schema_validator.get_table_schema(self.source_conn, table_name)
        column_names = [col["name"] for col in column_types]
        insert_stmt = self.prepare_insert_statement(table_name, column_names)
        
        # Set input sizes based on column types to handle NULL values correctly
        input_types = [self.get_column_type(col["data_type"]) for col in column_types]
        self.target_conn.cursor.setinputsizes(*input_types)
        
        # Initialize data retrieval generator with chunking
        query = custom_query if mode == "custom" and custom_query else None
        data_chunks = self.get_data(table_name, query, chunk_size=chunk_size)
        
        # Insert data with progress tracking and type conversion
        logger.info(f"Migrating {row_count} rows for table {table_name} in chunks of {chunk_size}")
        
        with create_progress_bar() as progress:
            task = progress.add_task(f"Migrating {table_name}", total=row_count)
            
            rows_processed = 0
            for chunk in data_chunks:
                try:
                    # Convert values in the chunk
                    converted_chunk = [
                        self.convert_row_values(row, column_types)
                        for row in chunk
                    ]
                    self.target_conn.cursor.executemany(insert_stmt, converted_chunk)
                    self.target_conn.connection.commit()
                    
                    chunk_size_actual = len(chunk)
                    rows_processed += chunk_size_actual
                    progress.update(task, advance=chunk_size_actual)
                    
                except Exception as e:
                    logger.error(f"Error inserting data: {e}")
                    self.target_conn.connection.rollback()
                    return False
                    
        logger.info(f"Successfully migrated {rows_processed} rows for table {table_name}")

        # Handle index migration if configured and data migration was successful
        if self.migrate_indexes_globally:
            logger.info(f"Global index migration enabled. Discovering indexes for {table_name}...")
            if not self._migrate_indexes_for_table(table_name, table_name):
                logger.warning(f"Index migration failed for {table_name}, but data was migrated successfully")

        return True

    def _migrate_indexes_for_table(self, source_table: str, target_table: str) -> bool:
        """
        Migrate indexes from source table to target table.

        Args:
            source_table (str): Source table name
            target_table (str): Target table name

        Returns:
            bool: True if all indexes were migrated successfully, False if any failed
        """
        try:
            # Get source table indexes
            indexes = self.source_conn.get_table_indexes(source_table)
            if not indexes:
                logger.info(f"No indexes found for table {source_table}")
                return True

            success = True
            for index in indexes:
                try:
                    # Generate DDL for the index
                    ddl = self.source_conn.generate_index_ddl(index, target_table)
                    if not ddl:
                        logger.error(f"Failed to generate DDL for index {index.name}")
                        success = False
                        continue

                    # Create the index
                    logger.info(f"Creating index {index.name} on {target_table}...")
                    if not self.target_conn.create_index(ddl):
                        logger.error(f"Failed to create index {index.name}")
                        success = False
                    else:
                        logger.info(f"Successfully created index {index.name}")

                except Exception as e:
                    logger.error(f"Error creating index {index.name}: {e}")
                    success = False
                    continue

            return success

        except Exception as e:
            logger.error(f"Error during index migration for table {source_table}: {e}")
            return False