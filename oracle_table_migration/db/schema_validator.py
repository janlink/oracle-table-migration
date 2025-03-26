"""
Schema validator module for Oracle Table Migration Tool.
"""
from typing import Dict, List, Any, Tuple
from oracle_table_migration.db.connection import DatabaseConnection
from oracle_table_migration.utils.logger import logger

class SchemaValidator:
    """Class for validating and managing database schemas."""
    
    def __init__(self, source_conn: DatabaseConnection, target_conn: DatabaseConnection):
        """
        Initialize schema validator.
        
        Args:
            source_conn (DatabaseConnection): Source database connection
            target_conn (DatabaseConnection): Target database connection
        """
        self.source_conn = source_conn
        self.target_conn = target_conn

    def get_table_schema(self, conn: DatabaseConnection, table_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve column definitions for a table.
        
        Args:
            conn (DatabaseConnection): Database connection
            table_name (str): Table name
            
        Returns:
            List[Dict[str, Any]]: Column definitions
        """
        query = """
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM user_tab_columns
            WHERE table_name = :table_name
            ORDER BY column_id
        """
        
        results = conn.execute_query(query, {"table_name": table_name.upper()})
        
        columns = []
        for row in results:
            columns.append({
                "name": row[0],
                "data_type": row[1],
                "data_length": row[2],
                "data_precision": row[3],
                "data_scale": row[4],
                "nullable": row[5]
            })
            
        return columns
    
    def table_exists(self, conn: DatabaseConnection, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            conn (DatabaseConnection): Database connection
            table_name (str): Table name
            
        Returns:
            bool: True if table exists, False otherwise
        """
        query = """
            SELECT COUNT(*) FROM user_tables WHERE table_name = :table_name
        """
        
        result = conn.execute_query(query, {"table_name": table_name.upper()})
        return result[0][0] > 0 if result else False
    
    def schemas_match(self, table_name: str) -> bool:
        """
        Compare schemas between source and target databases.
        
        Args:
            table_name (str): Table name
            
        Returns:
            bool: True if schemas match, False otherwise
        """
        if not self.table_exists(self.source_conn, table_name):
            logger.error(f"Source table {table_name} does not exist")
            return False
            
        if not self.table_exists(self.target_conn, table_name):
            logger.info(f"Target table {table_name} does not exist, will be created")
            return False
            
        source_schema = self.get_table_schema(self.source_conn, table_name)
        target_schema = self.get_table_schema(self.target_conn, table_name)
        
        # Simple schema comparison - could be enhanced for more detailed comparison
        if len(source_schema) != len(target_schema):
            return False
            
        for s_col, t_col in zip(source_schema, target_schema):
            if (s_col["name"] != t_col["name"] or
                s_col["data_type"] != t_col["data_type"] or
                s_col["data_length"] != t_col["data_length"] or
                s_col["data_precision"] != t_col["data_precision"] or
                s_col["data_scale"] != t_col["data_scale"]):
                return False
                
        return True
    
    def generate_create_table_sql(self, table_name: str) -> str:
        """
        Generate CREATE TABLE SQL from source schema.
        
        Args:
            table_name (str): Table name
            
        Returns:
            str: CREATE TABLE SQL statement
        """
        columns = self.get_table_schema(self.source_conn, table_name)
        
        if not columns:
            logger.error(f"Could not retrieve schema for {table_name}")
            return ""
            
        sql_parts = [f"CREATE TABLE {table_name} ("]
        
        for col in columns:
            col_def = f"{col['name']} {col['data_type']}"
            
            # Handle different data types
            if col['data_type'] == 'NUMBER' and col['data_precision'] is not None:
                if col['data_scale'] and col['data_scale'] > 0:
                    col_def += f"({col['data_precision']},{col['data_scale']})"
                else:
                    col_def += f"({col['data_precision']})"
            elif col['data_type'] in ('VARCHAR2', 'CHAR'):
                col_def += f"({col['data_length']})"
            elif col['data_type'] == 'RAW':
                # RAW type requires a size - using data_length if available, otherwise default to 2000
                size = col['data_length'] if col['data_length'] is not None else 2000
                col_def += f"({size})"
                
            if col['nullable'] == 'N':
                col_def += " NOT NULL"
                
            sql_parts.append(f"    {col_def},")
            
        # Remove trailing comma from last column
        sql_parts[-1] = sql_parts[-1].rstrip(',')
        
        sql_parts.append(")")
        return "\n".join(sql_parts)
        
    def drop_table(self, table_name: str) -> bool:
        """
        Drop table in target database.
        
        Args:
            table_name (str): Table name
            
        Returns:
            bool: True if successful, False otherwise
        """
        drop_sql = f"DROP TABLE {table_name}"
        return self.target_conn.execute_non_query(drop_sql)
        
    def create_table(self, table_name: str) -> bool:
        """
        Create table in target database based on source schema.
        
        Args:
            table_name (str): Table name
            
        Returns:
            bool: True if successful, False otherwise
        """
        create_sql = self.generate_create_table_sql(table_name)
        if not create_sql:
            return False
            
        return self.target_conn.execute_non_query(create_sql)