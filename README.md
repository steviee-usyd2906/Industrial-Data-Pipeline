# EPS Project - PI Data Extraction and PostgreSQL Database

Tools for extracting PI (Plant Information) data, organizing element/attribute hierarchies, loading into PostgreSQL, and exporting via a Flask web UI. Includes support for derived attributes with formula-based calculations.

Service name: EPS_Ingest_Service

**Note**: Configuration files containing credentials and sensitive information are not included in this repository for security reasons. You will need to create and populate them with your own credentials as described in the setup instructions.

## Table of Contents

1. [Project Overview](#project-overview)
2. [File Structure](#file-structure)
3. [Code Files Documentation](#code-files-documentation)
4. [Database Schema & Functions](#database-schema--functions)
5. [Configuration Variables](#configuration-variables)
6. [Installation and Setup](#installation-and-setup)
7. [Usage Instructions](#usage-instructions)

---

## Project Overview

This project consists of multiple components:

- **Tree Structure Generation**: Crawls PI Web API to build hierarchical element/attribute structures
- **Data Extraction**: Extracts time-series data from PI Web API
- **Database Population**: Populates PostgreSQL databases with element hierarchies, attributes, and archive data
- **Derived Attributes**: Create computed attributes using formulas referencing actual attribute IDs
- **Web Interface**: Flask-based web application for database querying, element/attribute management, and export

---

## File Structure (current scope)

```
EPS-project/
├── credentials/
│   ├── db_credentials.ini                 # PostgreSQL credentials (sections per DB)
│   └── pi_credentials.ini                 # PI Web API credentials (sections per site)
├── data/
├── src/
│   ├── database/
│   │   ├── populate.py                    # Build element/attribute/archive tables
│   │   └── extract_leaf_nodes.py          # Extract leaf nodes / selected webids
│   └── pi/
│       ├── tree_generator.py              # PI Web API crawler for tree caches
│       └── extraction/
│           └── ingest.py                  # PI Web API batch ingestion -> PostgreSQL
├── Database/
│   ├── config.ini                         # Flask & PostgreSQL connection config
│   ├── database.py                        # PostgreSQL helper functions
│   ├── web_app.py                         # Flask web application
│   ├── routes.py                          # Additional Flask routes (if used)
│   ├── templates/                         # HTML templates
│   ├── static/                            # CSS/JS assets
│   └── exports/                           # Generated export files
└── README.md
```

---

## Code Files Documentation

### 1. `Database/database.py`

**Purpose**: PostgreSQL connection management and data operations.

**Key Functions**:
- `get_connection(config_section)`: Connects to PostgreSQL using credentials
- `list_available_databases()`: Lists all configured database sections
- `get_leaf_elements(conn)`: Retrieves all leaf elements
- `get_element_attributes(conn, element_id)`: Gets attributes for an element
- `get_timeseries_data(conn, attribute_ids, start_ts, end_ts)`: Fetches and pivots archive data
- `insert_element(conn, element_data)`: Creates new element
- `insert_attribute(conn, attribute_data)`: Creates new attribute (optionally with formula for backfilling)
- `backfill_derived_attribute(conn, new_attribute_id, formula)`: Computes derived attribute values using formula
- `update_json_cache_files(database_name, conn)`: Updates attribute mappings from PostgreSQL
- `delete_element(conn, element_id)`: Deletes element and cascades to attributes/archive
- `delete_attribute(conn, attribute_id)`: Deletes attribute and its archive data

**Database Schema**:
```
element:
  - element_id (serial PK)
  - name (text)
  - level (integer)
  - parent_id (integer, FK to element.element_id)

attribute:
  - attribute_id (serial PK)
  - element_id (integer, FK to element)
  - name (text)
  - kks (text, optional)

archive:
  - archive_id (serial PK)
  - attribute_id (integer, FK to attribute)
  - timestamp (timestamp)
  - value (numeric, optional)
```

---

### 2. `Database/web_app.py`

**Purpose**: Flask web application for database interaction and data export.

**API Endpoints**:
- `GET /`: Home page with database selection
- `GET /api/databases`: List available databases
- `GET /api/elements/<database_name>`: Get leaf elements
- `GET /api/attributes/<database_name>/<element_id>`: Get attributes and timestamp range
- `POST /api/export`: Export selected attributes to CSV/Parquet
- `POST /api/insert_element`: Create new element
- `POST /api/insert_attribute`: Create new attribute (with optional formula)
- `POST /api/delete_element/<element_id>`: Delete element
- `POST /api/delete_attribute/<attribute_id>`: Delete attribute

**Configuration**: Uses `Database/config.ini` for database connections and Flask port settings.

**Run**: `cd Database && python web_app.py` then open `http://127.0.0.1:<port>/`

---

### 3. `src/database/populate.py`

**Purpose**: Build the PostgreSQL schema and load element/attribute hierarchy from PI tree cache JSON.

**Key Functions**:
- `pgconnect(...)`: Connects to PostgreSQL using credentials
- `create_tables(engine)`: Creates element, attribute, archive tables with indexes
- `process_tree_node(...)`: Recursively inserts elements and attributes
- `populate_database(engine, json_file_path)`: Loads tree cache and populates tables

**Usage**: Configure section name in script, then run `python src/database/populate.py`

---

### 4. `src/pi/tree_generator.py`

**Purpose**: Crawls PI Web API to generate hierarchical element/attribute caches.

**Key Functions**:
- `get_api(endpoint)`: Makes authenticated GET requests
- `get_attributes(webid)`: Retrieves attributes for an element
- `build_node(name, webid, is_db=False)`: Recursively builds tree structure
- `main()`: Entry point that crawls asset servers

**Configuration Variables** (customize in script):
```python
BASE_URL = "https://10.156.8.181/piwebapi"
USERNAME = "your_username"
PASSWORD = "your_password"
```

**Output**: JSON cache files under `data/mong_duong` or `data/vinh_tan`

---

### 5. `src/pi/extraction/ingest.py`

**Purpose**: Batch PI Web API data ingestion directly to PostgreSQL `archive` table.

**Features**:
- Uses parallel threads per site
- Interpolated data pulls with configurable intervals
- Retry logic for network resilience
- Direct insertion into PostgreSQL

**Configuration**: Edit `webids_file`, `attribute_mapping`, interval, and time ranges in script

---

## Database Schema & Functions

### Creating Derived Attributes (Formulas)

Derived attributes allow you to create computed values based on existing attribute data. They are useful for calculated metrics, ratios, or aggregations.

**Formula Syntax**:
- Use `$N` where N is the actual **attribute ID** (not index)
- Plain numbers are treated as numeric constants
- Examples:
  - `$7 + $8` - Sum of attribute 7 and attribute 8
  - `($7 * 2) - $9` - Calculation using attributes 7, 9 with constant 2
  - `($7 + $8) / 2` - Average of two attributes

**Creating a Derived Attribute**:

Via Flask web app:
1. Select database and element
2. Click "Create Attribute"
3. Fill in: name, element_id, optional kks
4. Optionally add formula (e.g., `$7 + $8`)
5. Submit - formula will automatically backfill archive data

Via Python:
```python
from database import get_connection, insert_attribute

conn = get_connection('MONGDUONG1')
attribute_data = {
    'name': 'Calculated_Temp',
    'element_id': 50,
    'kks': 'CALC.TEMP',
    'formula': '($7 + $8) / 2'  # Average of attributes 7 and 8
}
new_attr_id = insert_attribute(conn, attribute_data)
conn.close()
```

**How Formulas Work**:
1. `$7`, `$8`, etc. are replaced with attribute_id at matching timestamps
2. Non-matching timestamps are excluded (WHERE clause filters NULLs)
3. Results are inserted into the `archive` table for the new attribute_id
4. **Real-time computation**: PostgreSQL triggers automatically compute new values when source data arrives

---

### Real-Time Derived Attributes with Triggers

**Automatic Trigger Creation**:

When creating a derived attribute with a formula, a PostgreSQL trigger is automatically created to compute values in real-time as new data is inserted.

```python
from database import get_connection, insert_attribute

conn = get_connection('MONGDUONG1')
attribute_data = {
    'name': 'Live_Sum',
    'element_id': 50,
    'formula': '$10 + $11',
    'create_trigger': True  # Default: True, creates trigger automatically
}
derived_attr_id = insert_attribute(conn, attribute_data)
conn.close()
```

**How Triggers Work**:

1. When new data is inserted for any source attribute (e.g., attribute 10 or 11)
2. The trigger checks if all required source values exist at that timestamp
3. If all values exist, the formula is computed and inserted/updated automatically
4. Uses `ON CONFLICT` to handle duplicate timestamps gracefully

**Updating Derived Attributes**:

You can update a derived attribute's formula and automatically regenerate its trigger. **Only derived attributes** (those with formulas/triggers) can be updated - source attributes from PI systems are protected from modification:

```python
from database import get_connection, update_attribute

conn = get_connection('MONGDUONG1')
update_result = update_attribute(conn, attribute_id=99, update_data={
    'name': 'Updated_Name',        # Optional: change name
    'formula': '$10 * $11',         # New formula
    'recompute_archive': True,      # Default: True, recalculates historical data
    'recreate_trigger': True        # Default: True, regenerates trigger
})
print(f"Updated: {update_result['updated_fields']}")
print(f"Records recomputed: {update_result['archive_records_inserted']}")
conn.close()
```

**Important**: Attempting to update a non-derived attribute will raise an error to prevent accidental modification of source data from PI systems.

Options for `update_attribute`:
- **recompute_archive**: If `True`, deletes old values and recalculates using new formula
- **recreate_trigger**: If `True`, drops old trigger and creates new one for real-time computation
- Set `recompute_archive=False` to keep existing values but update trigger for new data only

**Trigger Function Example**:

The system generates PostgreSQL trigger functions like this:

```sql
CREATE OR REPLACE FUNCTION compute_derived_attr_99()
RETURNS trigger AS $$
DECLARE
    v_10 DOUBLE PRECISION;
    v_11 DOUBLE PRECISION;
BEGIN
    -- Only react to source attributes
    IF NEW.attribute_id NOT IN (10, 11) THEN
        RETURN NEW;
    END IF;

    -- Fetch both values for this timestamp
    SELECT value INTO v_10
    FROM archive
    WHERE attribute_id = 10
      AND "timestamp" = NEW."timestamp";

    SELECT value INTO v_11
    FROM archive
    WHERE attribute_id = 11
      AND "timestamp" = NEW."timestamp";

    -- Only compute when both exist
    IF v_10 IS NOT NULL AND v_11 IS NOT NULL THEN
        INSERT INTO archive (attribute_id, "timestamp", value)
        VALUES (99, NEW."timestamp", v_10 + v_11)
        ON CONFLICT (attribute_id, "timestamp")
        DO UPDATE SET value = EXCLUDED.value;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

**Manual Trigger Management**:

```python
from database import (
    get_connection,
    create_derived_attribute_trigger,
    drop_derived_attribute_trigger,
    update_attribute
)

conn = get_connection('MONGDUONG1')

# Update formula and regenerate trigger
update_attribute(conn, 99, {
    'formula': '$10 * $11',
    'recompute_archive': True,
    'recreate_trigger': True
})

# Or manually drop/create triggers if needed
drop_derived_attribute_trigger(conn, 99)
trigger_name = create_derived_attribute_trigger(conn, 99, '$10 + $11')
print(f"Created trigger: {trigger_name}")

conn.close()
```

**Testing Triggers**:

Run the test script to verify trigger functionality:
```bash
cd Database
python test_trigger.py
```

**Important Notes**:
- Archive table must have unique constraint on `(attribute_id, timestamp)`
- The system automatically creates this constraint if it doesn't exist
- Triggers are automatically dropped when deleting derived attributes
- Formula validation happens before trigger creation

---

### Updating Attribute Mappings

The attribute mapping files (`attribute_mapping_*.json`) are generated from the PostgreSQL database and map full element paths to attribute IDs.

**Format**:
```json
{
    "\\\\Element1|Attribute1": 1,
    "\\\\Element2|Child|Attribute2": 5,
    ...
}
```

**Update Mappings**:

Via Python:
```python
from database import get_connection, update_json_cache_files

conn = get_connection('MONGDUONG1')
result = update_json_cache_files('MONGDUONG1', conn)
print(f"Updated {result['attribute_count']} attributes in {result['attribute_mapping']}")
conn.close()
```

This is useful after:
- Inserting new attributes
- Modifying element hierarchy
- Syncing changes from PI Web API

---

## Configuration Variables Summary

### Critical Files to Update

1. **Database Credentials** (`credentials/db_credentials.ini`): sections for each target DB
2. **PI API Credentials** (`credentials/pi_credentials.ini`): sections per PI server
3. **Web App Config** (`Database/config.ini`): database sections and Flask port
4. **PI Server URLs**: Embedded in `src/pi/tree_generator.py` and `src/pi/extraction/ingest.py`

### Example `Database/config.ini`

```ini
[Early Warning System MD1]
host = 10.144.20.67
user = postgres
database = Early Warning System MD1
password = YourPassword
port = 5432

[MONGDUONG1]
host = 10.144.20.67
user = postgres
database = MONGDUONG1
password = YourPassword
port = 5432

[FLASK]
port = 10429
```

---

## Installation and Setup

```bash
pip install pandas sqlalchemy psycopg2-binary requests requests-ntlm pg8000 flask
```

### Database Setup

1. Ensure PostgreSQL is running
2. Create a database for your project
3. Fill in the placeholders in `credentials/db_credentials.ini` with your actual database connection details (host, user, database name, password, port)
4. Fill in the placeholders in `Database/config.ini` with matching database connection details

### PI System Access

1. Verify network access to PI Web API servers
2. Fill in the placeholders in `credentials/pi_credentials.ini` with your PI Web API credentials
3. Update the PI server URLs in `src/pi/tree_generator.py` and `src/pi/extraction/ingest.py` by replacing the placeholder URLs with your actual PI Web API endpoints

---

## Usage Instructions

### 1. Generate PI Tree Structure (Optional)

```bash
python src/pi/tree_generator.py
```
- Connects to PI Web API
- Crawls element hierarchy
- Saves to `data/<site>/*_pi_tree_cache.json`

### 2. Populate Database Schema

```bash
python src/database/populate.py
```
- Builds `element`, `attribute`, `archive` tables
- Loads tree cache JSON from `data/<site>/`
- Creates initial database structure

### 3. Batch PI Data Ingestion

```bash
python src/pi/extraction/ingest.py
```
- Pulls time-series data from PI Web API
- Inserts into PostgreSQL `archive` table
- Configure time ranges and intervals in script

### 4. Launch Web Interface

```bash
cd Database
python web_app.py
```
- Access at `http://127.0.0.1:<port>/`
- Select database, choose elements/attributes
- Export data to CSV or Parquet
- Create/manage derived attributes

### 5. Update Attribute Mappings (After schema changes)

After inserting new elements or attributes, update the cache files:
```bash
python -c "from Database.database import *; conn = get_connection('MONGDUONG1'); update_json_cache_files('MONGDUONG1', conn); conn.close()"
```

### 6. Test Derived Attribute Updates

Test updating formulas and regenerating triggers:
```bash
cd Database
python test_update_attribute.py
```

---

## Notes

- **Derived Attributes**: Use formulas like `$7 + $8` to reference actual attribute IDs (not ordinal positions)
- **Archive Data**: Backfilling is automatic when creating attributes with formulas
- **Real-Time Computation**: PostgreSQL triggers automatically compute derived values as source data arrives
- **Formula Updates**: Use `update_attribute()` to change formulas and regenerate triggers/data
- **JSON Mappings**: Updated from PostgreSQL, not from PI caches directly
- **Element Hierarchy**: Maintained via `parent_id` relationships in the database
- **Error Handling**: All functions include exception handling and connection cleanup
- **Trigger Management**: Triggers are auto-created with formulas and auto-dropped on attribute deletion

---

## Troubleshooting

### Connection Issues
- Verify credentials in `db_credentials.ini` and matching section names
- Check PostgreSQL is running: `psql -U postgres -h localhost`
- Test PI server connectivity

### Formula Errors
- Verify attribute IDs exist in database: check `SELECT attribute_id FROM attribute`
- Ensure timestamps overlap between referenced attributes
- Use valid SQL operators: `+`, `-`, `*`, `/`, `(`, `)`
- Check formula syntax uses `$N` where N is the actual attribute ID

### Update Issues
- **Cannot update attribute**: Error "not a derived attribute" means you're trying to update a source attribute from PI. Only derived attributes (with formulas) can be updated
- **Old values persist**: Set `recompute_archive=True` when calling `update_attribute()`
- **Trigger not updating**: Set `recreate_trigger=True` to regenerate trigger function
- **Permission errors**: Ensure database user has CREATE FUNCTION and CREATE TRIGGER privileges

### Trigger Issues
- **Trigger not firing**: Check if unique constraint exists on archive table
  ```sql
  SELECT constraint_name FROM information_schema.table_constraints 
  WHERE table_name = 'archive' AND constraint_type = 'UNIQUE';
  ```
- **Duplicate key errors**: Run `ensure_archive_unique_constraint(conn)` to add constraint
- **Missing computed values**: Verify all source attributes have data at the same timestamp
- **Trigger conflicts**: Check existing triggers with:
  ```sql
  SELECT trigger_name FROM information_schema.triggers WHERE event_object_table = 'archive';
  ```
- **Drop stuck trigger**: Manually drop with:
  ```sql
  DROP TRIGGER IF EXISTS trigger_compute_derived_attr_N ON archive;
  DROP FUNCTION IF EXISTS compute_derived_attr_N();
  ```

### Export Issues
- Check `Database/exports/` directory permissions
- Verify timestamp format in database (should be ISO 8601)
- Ensure at least one attribute has data in selected time range

### Performance Issues
- Reduce batch size for large time ranges
- Increase PostgreSQL connection pool size in config
- Use smaller time intervals for ingest scripts
- Monitor trigger performance: too many triggers can slow inserts
- Consider disabling triggers during bulk data loads

---

## License and Contact

This project is for internal use. For questions or issues, contact the development team.

