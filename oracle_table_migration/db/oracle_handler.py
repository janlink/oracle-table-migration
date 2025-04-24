"""
Oracle-specific database handler with index management capabilities.
"""
from dataclasses import dataclass
from typing import Dict, Any, List, Tuple, Optional

from oracle_table_migration.db.connection import DatabaseConnection
from oracle_table_migration.utils.logger import logger


@dataclass
class IndexDefinition:
    """Represents an Oracle index definition."""
    name: str
    table_name: str
    columns: List[str]
    is_unique: bool
    tablespace_name: Optional[str] = None


class OracleHandler(DatabaseConnection):
    """Oracle-specific database handler with extended functionality."""

    def get_table_indexes(self, table_name: str) -> List[IndexDefinition]:
        """
        Retrieve all indexes for a given table using Oracle dictionary views.

        Args:
            table_name (str): Name of the table to get indexes for

        Returns:
            List[IndexDefinition]: List of index definitions
        """
        try:
            # Query to get index information
            index_query = """
                SELECT i.index_name,
                       i.table_name,
                       i.uniqueness,
                       i.tablespace_name,
                       LISTAGG(c.column_name, ',') WITHIN GROUP (ORDER BY c.column_position) as columns
                FROM user_indexes i
                JOIN user_ind_columns c ON i.index_name = c.index_name
                WHERE i.table_name = :table_name
                GROUP BY i.index_name, i.table_name, i.uniqueness, i.tablespace_name
            """
            
            results = self.execute_query(index_query, {"table_name": table_name.upper()})
            
            indexes: List[IndexDefinition] = []
            for row in results:
                index = IndexDefinition(
                    name=row[0],
                    table_name=row[1],
                    is_unique=(row[2] == "UNIQUE"),
                    tablespace_name=row[3],
                    columns=row[4].split(",")
                )
                indexes.append(index)
                
            logger.info(f"Retrieved {len(indexes)} indexes for table {table_name}")
            return indexes
            
        except Exception as e:
            logger.error(f"Error retrieving indexes for table {table_name}: {e}")
            return []

    def generate_index_ddl(self, index: IndexDefinition, target_table: str) -> str:
        """
        Generate CREATE INDEX DDL statement for a given index definition.

        Args:
            index (IndexDefinition): Index definition to generate DDL for
            target_table (str): Name of the target table for the index

        Returns:
            str: CREATE INDEX DDL statement
        """
        try:
            # Format column list
            columns = ", ".join(index.columns)
            
            # Generate new index name for target table
            # Keep original name if target table name is same length, otherwise adjust
            if len(target_table) == len(index.table_name):
                new_index_name = index.name
            else:
                # Create new index name, ensuring it stays under Oracle's 30 char limit
                base_name = index.name.replace(index.table_name, target_table)
                new_index_name = base_name[:30]
            
            # Build the DDL statement
            ddl = f"CREATE {'UNIQUE ' if index.is_unique else ''}INDEX {new_index_name} "
            ddl += f"ON {target_table} ({columns})"
            logger.debug(f"Generated index DDL: {ddl}")
            return ddl
            
        except Exception as e:
            logger.error(f"Error generating index DDL: {e}")
            return ""

    def create_index(self, index_ddl: str) -> bool:
        """
        Execute a CREATE INDEX DDL statement.

        Args:
            index_ddl (str): The CREATE INDEX DDL statement to execute

        Returns:
            bool: True if successful, False otherwise
        """
        if not index_ddl:
            logger.error("Cannot create index: empty DDL statement")
            return False
            
        try:
            result = self.execute_non_query(index_ddl)
            return result
            
        except Exception as e:
            logger.error(f"Error creating index: {e}")
            return False