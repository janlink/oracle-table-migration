"""
Main module for Oracle Table Migration Tool.
"""
import argparse
import sys
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

from oracle_table_migration.config.config_loader import ConfigLoader
from oracle_table_migration.db.connection import DatabaseConnection
from oracle_table_migration.migration.migrator import TableMigrator
from oracle_table_migration.utils.logger import logger

console = Console()

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Oracle Table Migration Tool')
    parser.add_argument('-c', '--config', type=str, default='config/migration_config.yaml',
                        help='Path to configuration file')
    return parser.parse_args()

def main():
    """Main entry point for the application."""
    # Display welcome banner
    console.print(Panel.fit(
        "[bold blue]Oracle Table Migration Tool[/bold blue]\n"
        "[italic]Transfer tables between Oracle databases[/italic]",
        border_style="green"
    ))
    
    # Parse command line arguments
    args = parse_arguments()
    config_path = args.config
    
    try:
        # Load configuration
        logger.info(f"Loading configuration from {config_path}")
        config = ConfigLoader(config_path)
        
        # Setup database connections
        source_config = config.get_source_db_config()
        target_config = config.get_target_db_config()
        
        source_db = DatabaseConnection(source_config)
        target_db = DatabaseConnection(target_config)
        
        # Connect to databases
        if not source_db.connect():
            logger.error("Failed to connect to source database")
            return 1
            
        if not target_db.connect():
            logger.error("Failed to connect to target database")
            return 1
        
        # Create migrator and process tables
        migrator = TableMigrator(source_db, target_db)
        tables_config = config.get_tables_config()
        
        if not tables_config:
            logger.warning("No tables configured for migration")
            return 0
            
        success_count = 0
        fail_count = 0
        
        for table_config in tables_config:
            table_name = table_config.get('name')
            mode = table_config.get('mode', 'full')
            custom_query = table_config.get('query') if mode == 'custom' else None
            existing_table_behavior = table_config.get('existing_table_behavior', 'drop_and_recreate')
            chunk_size = config.get_table_chunk_size(table_name)
            
            logger.info(f"Processing table {table_name} in {mode} mode with {existing_table_behavior} behavior and chunk size {chunk_size}")
            
            if migrator.migrate_table(table_name, mode, custom_query, existing_table_behavior, chunk_size):
                success_count += 1
            else:
                fail_count += 1
                
        # Print summary
        console.print(Panel.fit(
            f"[bold green]Migration Complete[/bold green]\n"
            f"Successfully migrated: {success_count} tables\n"
            f"Failed: {fail_count} tables",
            border_style="blue"
        ))
        
        return 0 if fail_count == 0 else 1
        
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1
    finally:
        # Ensure connections are closed
        if 'source_db' in locals():
            source_db.disconnect()
        if 'target_db' in locals():
            target_db.disconnect()

if __name__ == "__main__":
    sys.exit(main())