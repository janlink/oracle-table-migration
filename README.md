# Oracle Table Migration Tool 🚀

✨ Seamlessly migrate tables between Oracle databases with this powerful and flexible Python tool! ✨

Whether you need to move entire tables or just specific subsets of data using custom queries, this tool simplifies the process, handling schema validation and creation automatically.

## 🌟 Features

*   **Flexible Configuration:** Define your migration tasks easily using a clear YAML configuration file. ⚙️
*   **Multiple Migration Modes:**
    *   `full`: Copy entire tables effortlessly.
    *   `custom`: Migrate specific data using your own SQL queries. 🎯
*   **Smart Schema Handling:** Automatically validates table schemas between source and target. If the target table doesn't exist or schemas don't match, it can create the table for you! 🏗️
*   **Efficient Data Transfer:** Fetches and inserts data in configurable chunks to handle large tables gracefully. 📊
*   **Progress Tracking:** Stay informed with visual progress bars during migration. ⏳
*   **Comprehensive Logging:** Detailed logs capture every step of the migration process for easy monitoring and debugging. 📝

## 🛠️ Installation

1.  Clone the repository:
    ```bash
    git clone <your-repo-url>
    cd oracle-table-migration
    ```
2.  Install the package (preferably in a virtual environment):
    ```bash
    pip install -e .
    ```

## ⚙️ Configuration

1.  **Database Credentials:**
    *   Create a `.env` file in the project root (or ensure your environment variables are set).
    *   Add your source and target database connection details:
        ```dotenv
        SOURCE_DB_USERNAME=your_source_username
        SOURCE_DB_PASSWORD=your_source_password
        SOURCE_DB_DSN=your_source_host:1521/service_name

        TARGET_DB_USERNAME=your_target_username
        TARGET_DB_PASSWORD=your_target_password
        TARGET_DB_DSN=your_target_host:1521/service_name
        ```
    *(Note: The tool uses `python-dotenv` to load these variables)*

2.  **Migration Tasks:**
    *   Copy the template `config/migration_config.yaml.template` to `config/migration_config.yaml`.
    *   Edit `config/migration_config.yaml` to define the tables and migration modes.

    **Example `migration_config.yaml`:**
    ```yaml
    # Default chunk size for fetching data (optional)
    default_chunk_size: 10000 

    tables:
      # Example 1: Full table migration
      - name: EMPLOYEES
        mode: full
        # Optional: Override default chunk size for this table
        chunk_size: 5000 

      # Example 2: Custom query migration
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
    ```

## ▶️ Usage

Run the migration tool from your terminal:

```bash
oracle-migrate
```

By default, it looks for `config/migration_config.yaml`. You can specify a different configuration file using the `--config` option:

```bash
oracle-migrate --config /path/to/your/custom_config.yaml
```

The tool will then connect to the databases, validate schemas, and migrate the data for each table defined in your configuration file.

## 📜 License

[Specify your license here, e.g., MIT License]