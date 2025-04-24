# Oracle Table Migration Tool 🚀

✨ Seamlessly migrate tables and their indexes between Oracle databases with this powerful and flexible Python tool! ✨

Whether you need to move entire tables, specific subsets of data using custom queries, or include associated indexes, this tool simplifies the process, handling schema validation and creation automatically.

## 🌟 Features

*   **Flexible Configuration:** Define your migration tasks easily using a clear YAML configuration file. ⚙️
*   **Multiple Migration Modes:**
    *   `full`: Copy entire tables effortlessly.
    *   `custom`: Migrate specific data using your own SQL queries. 🎯
*   **Index Migration:** Optionally migrate table indexes along with the data. Migration can be controlled globally or on a per-table basis. 🔑
*   **Smart Schema Handling:** Automatically validates table schemas between source and target. If the target table doesn't exist or schemas don't match, it can create the table for you! 🏗️
*   **Efficient Data Transfer:** Fetches and inserts data in configurable chunks to handle large tables gracefully. 📊
*   **Progress Tracking:** Stay informed with visual progress bars during migration. ⏳
*   **Comprehensive Logging:** Detailed logs capture every step of the migration process for easy monitoring and debugging. 📝

## 🛠️ Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/janlink/oracle-table-migration.git
    cd oracle-table-migration
    ```
2.  Install the package (preferably in a virtual environment using `uv`):
    ```bash
    # Example using uv (recommended)
    uv venv
    uv pip install -e .

    # Or using pip
    # python -m venv venv
    # source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    # pip install -e .
    ```

## ⚙️ Configuration

1.  **Database Credentials:**
    *   Create a `.env` file in the project root (or ensure your environment variables are set).
    *   Add your source and target database connection details:
        ```dotenv
        SOURCE_DB_USERNAME=your_source_username
        SOURCE_DB_PASSWORD=your_source_password
        SOURCE_DB_DSN=your_source_host:1521/service_name
        # Optional: Specify the source schema to validate against
        # If omitted, the user's default schema will be used.
        SOURCE_DB_SCHEMA=your_source_schema

        TARGET_DB_USERNAME=your_target_username
        TARGET_DB_PASSWORD=your_target_password
        TARGET_DB_DSN=your_target_host:1521/service_name
        # Optional: Specify the target schema where tables should be created/migrated
        TARGET_DB_SCHEMA=your_target_schema
        ```
    *(Note: The tool uses `python-dotenv` to load these variables. If `TARGET_DB_SCHEMA` is omitted, the user's default schema will be used.)*

2.  **Migration Tasks:**
    *   Copy the template `config/migration_config.yaml.template` to `config/migration_config.yaml`.
    *   Edit `config/migration_config.yaml` to define the tables, migration modes, and index migration settings.

    **Example `migration_config.yaml`:**
    ```yaml
    # Global migration settings (optional)
    migration_settings:
      # Default chunk size for fetching data (can be overridden per table)
      default_chunk_size: 10000
      # Globally enable/disable index migration (default: false)
      # Can be overridden per table. Indexes are created *after* data migration.
      migrate_indexes_globally: true

    tables:
      # Example 1: Full table migration with global index setting (true in this case)
      - name: EMPLOYEES
        mode: full
        # Optional: Override default chunk size for this table
        chunk_size: 5000
        # migrate_indexes: true # Not needed, inherits global setting

      # Example 2: Custom query migration, explicitly disabling index migration for this table
      - name: DEPARTMENTS_ACTIVE
        # Target table name can be different if needed
        target_table: DEPARTMENTS
        mode: custom
        query: |
          SELECT department_id, department_name, manager_id, location_id
          FROM DEPARTMENTS
          WHERE status = 'ACTIVE'
          ORDER BY department_id
        # Optional: Chunk size for this specific query
        chunk_size: 1000
        # Override global setting: Do not migrate indexes for this specific table
        migrate_indexes: false

      # Example 3: Full table migration, explicitly enabling index migration (overrides global if it were false)
      - name: JOB_HISTORY
        mode: full
        migrate_indexes: true # Explicitly enable index migration for this table
    ```

## ▶️ Usage

Run the migration tool from your terminal:

```bash
# Ensure your virtual environment is activated
# e.g., source venv/bin/activate or venv\Scripts\activate

# Run using the installed script (if using uv or pip install -e .)
oracle-migrate

# Or run directly via Python
# python -m oracle_table_migration.main
```

By default, it looks for `config/migration_config.yaml`. You can specify a different configuration file using the `--config` option:

```bash
oracle-migrate --config /path/to/your/custom_config.yaml
```

The tool will then connect to the databases, validate schemas, migrate the data, and optionally create indexes for each table defined in your configuration file.

## 📜 License

This project is licensed under [The Unlicense](https://unlicense.org/).

This means:
- You can use it for private or commercial purposes
- You can modify and distribute it freely
- No warranty or liability is provided
- The work is dedicated to the public domain

See the [LICENSE](LICENSE) file for more details.