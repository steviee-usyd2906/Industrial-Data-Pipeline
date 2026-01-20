#!/usr/bin/env python3
"""
Database module for PI data extraction and archival system.
Handles connections, queries, and data transformations aligned with PostgreSQL schema.
"""

import configparser
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import psycopg2
import psycopg2.extras
import pandas as pd
import json
from datetime import datetime

# Resolve config.ini relative to this file for predictable loading
CONFIG_PATH = Path(__file__).resolve().parent / "config.ini"
CONFIG_SECTION_DB = "DATABASE"


def load_db_config(config_section: str = CONFIG_SECTION_DB) -> configparser.SectionProxy:
    """Load database configuration from config.ini."""
    config = configparser.ConfigParser()
    read_files = config.read(CONFIG_PATH)
    
    if not read_files:
        raise FileNotFoundError(f"Could not read config file at {CONFIG_PATH}")
    
    if config_section not in config:
        raise KeyError(f"Missing [{config_section}] section in {CONFIG_PATH}")
    
    return config[config_section]


def get_connection(config_section: str = CONFIG_SECTION_DB):
    """Create a PostgreSQL connection using credentials from config.ini."""
    cfg = load_db_config(config_section)
    
    try:
        conn = psycopg2.connect(
            host=cfg.get('host', 'localhost'),
            port=int(cfg.get('port', 5432)),
            user=cfg.get('user'),
            password=cfg.get('password'),
            database=cfg.get('database'),
            connect_timeout=10
        )
        return conn
    except psycopg2.Error as e:
        raise ConnectionError(f"Failed to connect to database: {e}")


def list_available_databases(config_file: str = str(CONFIG_PATH)) -> List[str]:
    """List all database sections available in config.ini (except DEFAULT)."""
    config = configparser.ConfigParser()
    config.read(config_file)
    
    sections = [s for s in config.sections() if s != CONFIG_SECTION_DB]
    return sections


def get_database_config_section(database_name: str, config_file: str = str(CONFIG_PATH)) -> str:
    """
    Get the config section name for a given database name.
    Handles both database names and config section names.
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    
    # If it's already a valid section, return it
    if database_name in config.sections():
        return database_name
    
    # Otherwise, search for a section where database field matches
    for section in config.sections():
        if section != CONFIG_SECTION_DB:
            if config[section].get('database') == database_name:
                return section
    
    # If not found, raise error
    raise ValueError(f"Database '{database_name}' not found in config")


def get_leaf_elements(conn) -> Dict[str, str]:
    """
    Get all leaf elements from the element table.
    Returns dict: {element_path: element_id}
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        sql = """
            SELECT element_id, name, level, parent_id
            FROM element
            WHERE parent_id IS NULL OR level = (SELECT MAX(level) FROM element)
            ORDER BY name
        """
        cur.execute(sql)
        results = cur.fetchall()
        cur.close()
        
        return {row['name']: str(row['element_id']) for row in results}
    except psycopg2.Error as e:
        raise Exception(f"Failed to fetch leaf elements: {e}")


def lookup_element_id_by_name(conn, element_name: str) -> Optional[int]:
    """
    Look up an element ID by its name.
    
    Args:
        conn: Database connection
        element_name: Name of the element to find
    
    Returns:
        element_id if found, None otherwise
    """
    try:
        cur = conn.cursor()
        sql = """
            SELECT element_id FROM element
            WHERE name = %s
            LIMIT 1
        """
        cur.execute(sql, (element_name,))
        result = cur.fetchone()
        cur.close()
        
        return result[0] if result else None
    except psycopg2.Error as e:
        raise Exception(f"Failed to lookup element ID: {e}")


def lookup_attribute_id_by_name(conn, attribute_name: str, element_id: Optional[int] = None) -> Optional[int]:
    """
    Look up an attribute ID by its name.
    Optionally filter by element_id if multiple attributes have the same name.
    
    Args:
        conn: Database connection
        attribute_name: Name of the attribute to find
        element_id: Optional element ID to narrow search
    
    Returns:
        attribute_id if found, None otherwise
    """
    try:
        cur = conn.cursor()
        
        if element_id:
            sql = """
                SELECT attribute_id FROM attribute
                WHERE name = %s AND element_id = %s
                LIMIT 1
            """
            cur.execute(sql, (attribute_name, element_id))
        else:
            sql = """
                SELECT attribute_id FROM attribute
                WHERE name = %s
                LIMIT 1
            """
            cur.execute(sql, (attribute_name,))
        
        result = cur.fetchone()
        cur.close()
        
        return result[0] if result else None
    except psycopg2.Error as e:
        raise Exception(f"Failed to lookup attribute ID: {e}")


def find_element_by_name(conn, element_name: str) -> Optional[Dict]:
    """
    Find element details by name.
    
    Args:
        conn: Database connection
        element_name: Name of the element to find
    
    Returns:
        Dict with element_id, name, level, parent_id if found, None otherwise
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        sql = """
            SELECT element_id, name, level, parent_id
            FROM element
            WHERE name = %s
            LIMIT 1
        """
        cur.execute(sql, (element_name,))
        result = cur.fetchone()
        cur.close()
        
        return dict(result) if result else None
    except psycopg2.Error as e:
        raise Exception(f"Failed to find element: {e}")


def find_attribute_by_name(conn, attribute_name: str, element_id: Optional[int] = None) -> Optional[Dict]:
    """
    Find attribute details by name.
    Optionally filter by element_id if multiple attributes have the same name.
    
    Args:
        conn: Database connection
        attribute_name: Name of the attribute to find
        element_id: Optional element ID to narrow search
    
    Returns:
        Dict with attribute_id, name, element_id, kks if found, None otherwise
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        if element_id:
            sql = """
                SELECT attribute_id, name, element_id, kks
                FROM attribute
                WHERE name = %s AND element_id = %s
                LIMIT 1
            """
            cur.execute(sql, (attribute_name, element_id))
        else:
            sql = """
                SELECT attribute_id, name, element_id, kks
                FROM attribute
                WHERE name = %s
                LIMIT 1
            """
            cur.execute(sql, (attribute_name,))
        
        result = cur.fetchone()
        cur.close()
        
        return dict(result) if result else None
    except psycopg2.Error as e:
        raise Exception(f"Failed to find attribute: {e}")


def search_elements_by_name(conn, element_name_pattern: str) -> List[Dict]:
    """
    Search for elements by name pattern (case-insensitive).
    Uses SQL LIKE operator with wildcards.
    
    Args:
        conn: Database connection
        element_name_pattern: Pattern to search (e.g., "Fan%", "%Motor%")
    
    Returns:
        List of matching element dicts
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        sql = """
            SELECT element_id, name, level, parent_id
            FROM element
            WHERE LOWER(name) LIKE LOWER(%s)
            ORDER BY name
        """
        cur.execute(sql, (element_name_pattern,))
        results = cur.fetchall()
        cur.close()
        
        return [dict(row) for row in results]
    except psycopg2.Error as e:
        raise Exception(f"Failed to search elements: {e}")


def search_attributes_by_name(conn, attribute_name_pattern: str, element_id: Optional[int] = None) -> List[Dict]:
    """
    Search for attributes by name pattern (case-insensitive).
    Uses SQL LIKE operator with wildcards.
    
    Args:
        conn: Database connection
        attribute_name_pattern: Pattern to search (e.g., "Temp%", "%Speed%")
        element_id: Optional element ID to narrow search
    
    Returns:
        List of matching attribute dicts with element_name
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        if element_id:
            sql = """
                SELECT a.attribute_id, a.name, a.element_id, a.kks,
                       e.name as element_name
                FROM attribute a
                JOIN element e ON a.element_id = e.element_id
                WHERE LOWER(a.name) LIKE LOWER(%s) AND a.element_id = %s
                ORDER BY a.name
            """
            cur.execute(sql, (attribute_name_pattern, element_id))
        else:
            sql = """
                SELECT a.attribute_id, a.name, a.element_id, a.kks,
                       e.name as element_name
                FROM attribute a
                JOIN element e ON a.element_id = e.element_id
                WHERE LOWER(a.name) LIKE LOWER(%s)
                ORDER BY e.name, a.name
            """
            cur.execute(sql, (attribute_name_pattern,))
        
        results = cur.fetchall()
        cur.close()
        
        return [dict(row) for row in results]
    except psycopg2.Error as e:
        raise Exception(f"Failed to search attributes: {e}")



def get_element_details(conn, element_id: str) -> Dict:
    """Get details of a specific element."""
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        sql = """
            SELECT *
            FROM element
            WHERE element_id = %s
        """
        cur.execute(sql, (int(element_id),))
        result = cur.fetchone()
        cur.close()
        
        return dict(result) if result else None
    except psycopg2.Error as e:
        raise Exception(f"Failed to fetch element details: {e}")


def get_element_attributes(conn, element_id: str) -> List[Dict]:
    """
    Get all attributes (children) of a specific element.
    Returns list of attribute dicts with: attribute_id, name, kks, etc.
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        sql = """
            SELECT attribute_id, attribute_id as attribute_id, name, kks
            FROM attribute
            WHERE element_id = %s
            ORDER BY name
        """
        cur.execute(sql, (int(element_id),))
        results = cur.fetchall()
        cur.close()
        
        return [dict(row) for row in results]
    except psycopg2.Error as e:
        raise Exception(f"Failed to fetch element attributes: {e}")


def get_timeseries_data(
    conn,
    attribute_ids: List[str],
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    element_ids: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Fetch time series data for given attributes and format as required.
    
    Args:
        conn: Database connection
        attribute_ids: List of attribute IDs to fetch
        start_timestamp: Optional start time filter
        end_timestamp: Optional end time filter
        element_ids: Optional list of element_ids (for multi-element queries)
    
    Returns:
        DataFrame with columns: timestamp, [attribute_names], [element_name if multiple elements]
    """
    if not attribute_ids:
        return pd.DataFrame()
    
    try:
        placeholders = ','.join(['%s'] * len(attribute_ids))
        
        where_clause = f"WHERE arch.attribute_id IN ({placeholders})"
        params = [int(aid) for aid in attribute_ids]
        
        if start_timestamp:
            where_clause += " AND arch.timestamp >= %s"
            params.append(start_timestamp)
        
        if end_timestamp:
            where_clause += " AND arch.timestamp <= %s"
            params.append(end_timestamp)
        
        sql = f"""
            SELECT 
                arch.timestamp,
                attr.name as attribute_name,
                arch.value,
                elem.name as element_name,
                attr.element_id
            FROM archive arch
            JOIN attribute attr ON arch.attribute_id = attr.attribute_id
            JOIN element elem ON attr.element_id = elem.element_id
            {where_clause}
            ORDER BY arch.timestamp, attr.name
        """
        
        df = pd.read_sql(sql, conn, params=params)
        
        if df.empty:
            return df
        

        
        # Create pivot table with timestamp as index and attribute names as columns
        pivot_df = df.pivot_table(
            index='timestamp',
            columns='attribute_name',
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Add element_name column if multiple elements are queried
        if element_ids and len(element_ids) > 1:
            # Create element_name mapping from original dataframe
            element_mapping = df.groupby('element_id')['element_name'].first()
            pivot_df.insert(1, 'element_name', df.groupby('timestamp')['element_name'].first().values)
        
        return pivot_df
    
    except psycopg2.Error as e:
        raise Exception(f"Failed to fetch timeseries data: {e}")


def get_timestamp_range(conn, attribute_id: str) -> Tuple[Optional[str], Optional[str]]:
    """Get available timestamp range for an attribute."""
    try:
        cur = conn.cursor()
        sql = """
            SELECT MIN(timestamp), MAX(timestamp)
            FROM archive
            WHERE attribute_id = %s
        """
        cur.execute(sql, (int(attribute_id),))
        result = cur.fetchone()
        cur.close()
        
        return result if result else (None, None)
    except psycopg2.Error as e:
        raise Exception(f"Failed to fetch timestamp range: {e}")


def export_to_csv(df: pd.DataFrame, filepath: str):
    """Export DataFrame to CSV file."""
    df.to_csv(filepath, index=False)
    return filepath


def export_to_parquet(df: pd.DataFrame, filepath: str):
    """Export DataFrame to Parquet file."""
    df.to_parquet(filepath, index=False)
    return filepath


def insert_element(conn, element_data: Dict) -> int:
    """
    Insert a new element into the database.
    Returns the newly created element_id.
    
    Args:
        element_data: Dict with keys: name, level, parent_id (optional)
    """
    try:
        cur = conn.cursor()
        sql = """
            INSERT INTO element (name, level, parent_id)
            VALUES (%s, %s, %s)
            RETURNING element_id
        """
        cur.execute(sql, (
            element_data.get('name'),
            element_data.get('level', 0),
            element_data.get('parent_id')
        ))
        element_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        return element_id
    except psycopg2.Error as e:
        conn.rollback()
        raise Exception(f"Failed to insert element: {e}")


def insert_attribute(conn, attribute_data: Dict) -> int:
    """
    Insert a new attribute into the database.
    Returns the newly created attribute_id.
    
    Args:
        attribute_data: Dict with keys: 
            - name: attribute name
            - element_id: parent element ID
            - kks (optional): KKS identifier
            - formula (optional): formula for derived attributes (e.g., "$7 + $8")
            - create_trigger (optional, default True): create trigger for live computation
    """
    try:
        cur = conn.cursor()
        sql = """
            INSERT INTO attribute (name, element_id, kks)
            VALUES (%s, %s, %s)
            RETURNING attribute_id
        """
        cur.execute(sql, (
            attribute_data.get('name'),
            int(attribute_data.get('element_id')),
            attribute_data.get('kks')
        ))
        attribute_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        
        # If formula provided, backfill archive data and create trigger
        formula = attribute_data.get('formula')
        if formula:
            backfill_derived_attribute(conn, attribute_id, formula)
            
            # Create trigger for live computation (default: True)
            if attribute_data.get('create_trigger', True):
                trigger_name = create_derived_attribute_trigger(conn, attribute_id, formula)
                print(f"Created trigger: {trigger_name}")
        
        return attribute_id
    except psycopg2.Error as e:
        conn.rollback()
        raise Exception(f"Failed to insert attribute: {e}")


def backfill_derived_attribute(conn, new_attribute_id: int, formula: str) -> int:
    """
    Backfill archive data for a derived attribute using a formula.
    Formula uses $N where N is the actual attribute_id to reference.
    Plain numbers are treated as constants.
    
    Args:
        conn: Database connection
        new_attribute_id: ID of the newly created derived attribute
        formula: Formula string like "$7 + $8" or "($7 * 2) - $9"
                 where $7, $8, $9 are actual attribute IDs (not indices)
                 and 2 is a numeric constant
    
    Returns:
        Number of records inserted
    """
    try:
        cur = conn.cursor()
        
        # Extract referenced attribute IDs from formula ($7, $8, etc.)
        import re
        attr_id_strs = set(re.findall(r'\$(\d+)', formula))
        if not attr_id_strs:
            return 0
        
        # Convert to integers - these are the actual attribute IDs
        attr_ids = [int(aid) for aid in attr_id_strs]
        
        # Verify all referenced attributes exist
        placeholders = ','.join(['%s'] * len(attr_ids))
        cur.execute(f"""
            SELECT attribute_id FROM attribute 
            WHERE attribute_id IN ({placeholders})
        """, attr_ids)
        existing_attrs = {row[0] for row in cur.fetchall()}
        
        missing = set(attr_ids) - existing_attrs
        if missing:
            raise ValueError(f"Formula references non-existent attribute IDs: {sorted(missing)}")
        
        # Replace $N with actual attribute ID lookups in formula
        sql_formula = formula
        for attr_id in attr_ids:
            placeholder = f'${attr_id}'
            sql_formula = sql_formula.replace(placeholder, f"(SELECT value FROM archive WHERE attribute_id = {attr_id} AND timestamp = a.timestamp LIMIT 1)")
        
        # Insert derived values from archive data
        sql = f"""
            INSERT INTO archive (attribute_id, timestamp, value)
            SELECT %s, a.timestamp, {sql_formula}
            FROM (
                SELECT DISTINCT timestamp FROM archive
                WHERE attribute_id IN ({','.join(str(aid) for aid in attr_ids)})
            ) a
            WHERE {sql_formula} IS NOT NULL
            ON CONFLICT DO NOTHING
        """
        
        cur.execute(sql, (new_attribute_id,))
        inserted_count = cur.rowcount
        conn.commit()
        cur.close()
        
        return inserted_count
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to backfill derived attribute: {e}")


def ensure_archive_unique_constraint(conn) -> None:
    """
    Ensure the archive table has a unique constraint on (attribute_id, timestamp)
    for ON CONFLICT clauses in triggers to work properly.
    """
    try:
        cur = conn.cursor()
        
        # Check if constraint already exists
        cur.execute("""
            SELECT constraint_name 
            FROM information_schema.table_constraints 
            WHERE table_name = 'archive' 
            AND constraint_type = 'UNIQUE'
            AND constraint_name = 'archive_attribute_timestamp_unique'
        """)
        
        if cur.fetchone() is None:
            # Create the unique constraint
            cur.execute("""
                ALTER TABLE archive 
                ADD CONSTRAINT archive_attribute_timestamp_unique 
                UNIQUE (attribute_id, "timestamp")
            """)
            conn.commit()
            print("✓ Created unique constraint on archive (attribute_id, timestamp)")
        
        cur.close()
        
    except Exception as e:
        conn.rollback()
        # If constraint already exists through other means, ignore
        if "already exists" not in str(e).lower():
            raise Exception(f"Failed to create archive unique constraint: {e}")


def create_derived_attribute_trigger(conn, derived_attribute_id: int, formula: str) -> str:
    """
    Create a PostgreSQL trigger to automatically compute derived attribute values
    when new data is inserted into the archive table.
    
    Args:
        conn: Database connection
        derived_attribute_id: ID of the derived attribute
        formula: Formula string like "$7 + $8" where $N references attribute IDs
    
    Returns:
        Name of the created trigger function
    """
    try:
        import re
        
        # Ensure archive table has unique constraint for ON CONFLICT
        ensure_archive_unique_constraint(conn)
        
        cur = conn.cursor()
        
        # Extract referenced attribute IDs from formula
        attr_id_strs = set(re.findall(r'\$(\d+)', formula))
        if not attr_id_strs:
            raise ValueError("Formula must reference at least one attribute using $N syntax")
        
        source_attr_ids = [int(aid) for aid in attr_id_strs]
        
        # Convert formula to PostgreSQL expression
        pg_formula = formula
        var_declarations = []
        fetch_statements = []
        null_checks = []
        
        for attr_id in source_attr_ids:
            var_name = f"v_{attr_id}"
            var_declarations.append(f"    {var_name} DOUBLE PRECISION;")
            fetch_statements.append(f"""
    SELECT value INTO {var_name}
    FROM archive
    WHERE attribute_id = {attr_id}
      AND "timestamp" = NEW."timestamp";""")
            null_checks.append(f"{var_name} IS NOT NULL")
            pg_formula = pg_formula.replace(f"${attr_id}", var_name)
        
        # Create unique function name
        function_name = f"compute_derived_attr_{derived_attribute_id}"
        trigger_name = f"trigger_{function_name}"
        
        # Build the trigger function
        trigger_function_sql = f"""
CREATE OR REPLACE FUNCTION {function_name}()
RETURNS trigger AS $$
DECLARE
{chr(10).join(var_declarations)}
BEGIN
    -- Only react to source attributes
    IF NEW.attribute_id NOT IN ({', '.join(str(aid) for aid in source_attr_ids)}) THEN
        RETURN NEW;
    END IF;

    -- Fetch all required values for this timestamp
{chr(10).join(fetch_statements)}

    -- Only compute when all source values exist
    IF {' AND '.join(null_checks)} THEN
        INSERT INTO archive (attribute_id, "timestamp", value)
        VALUES ({derived_attribute_id}, NEW."timestamp", {pg_formula})
        ON CONFLICT (attribute_id, "timestamp")
        DO UPDATE SET value = EXCLUDED.value;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""
        
        # Drop existing trigger if it exists
        cur.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON archive;")
        
        # Create the trigger function
        cur.execute(trigger_function_sql)
        
        # Create the trigger
        trigger_sql = f"""
CREATE TRIGGER {trigger_name}
AFTER INSERT ON archive
FOR EACH ROW
EXECUTE FUNCTION {function_name}();
"""
        cur.execute(trigger_sql)
        
        conn.commit()
        cur.close()
        
        return trigger_name
        
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to create derived attribute trigger: {e}")


def drop_derived_attribute_trigger(conn, derived_attribute_id: int) -> None:
    """
    Drop the trigger and function for a derived attribute.
    
    Args:
        conn: Database connection
        derived_attribute_id: ID of the derived attribute
    """
    try:
        cur = conn.cursor()
        
        function_name = f"compute_derived_attr_{derived_attribute_id}"
        trigger_name = f"trigger_{function_name}"
        
        # Drop trigger
        cur.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON archive;")
        
        # Drop function
        cur.execute(f"DROP FUNCTION IF EXISTS {function_name}();")
        
        conn.commit()
        cur.close()
        
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to drop derived attribute trigger: {e}")


def update_json_cache_files(database_name: str, conn) -> Dict[str, str]:
    """
    Update JSON cache files in the data folder based on database content.
    Creates/updates:
    - attribute_mapping_{database}.json: maps full attribute paths to attribute IDs
    
    Returns dict with paths to updated files.
    """
    try:
        # Determine data folder based on database name
        project_root = Path(__file__).resolve().parent.parent
        
        # Map database names to folder names
        folder_map = {
            'Early Warning System MD1': 'mong_duong',
            'MONGDUONG1': 'mong_duong',
            'Early Warning System VT2': 'vinh_tan',
            'VINHTAN2': 'vinh_tan'
        }
        
        folder_name = folder_map.get(database_name, 'mong_duong')
        data_folder = project_root / 'data' / folder_name
        data_folder.mkdir(parents=True, exist_ok=True)
        
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Fetch all elements with hierarchy info
        sql = """
            SELECT element_id, name, level, parent_id
            FROM element
            ORDER BY level, element_id
        """
        cur.execute(sql)
        elements_rows = cur.fetchall()
        elements_dict = {row['element_id']: dict(row) for row in elements_rows}
        
        # Build element paths (to get full hierarchy like "Root|Child|Grandchild")
        def get_element_path(element_id):
            """Build the full path from root to this element."""
            path_parts = []
            current_id = element_id
            while current_id:
                elem = elements_dict.get(current_id)
                if not elem:
                    break
                path_parts.insert(0, elem['name'])
                current_id = elem['parent_id']
            return '|'.join(path_parts)
        
        # Generate attribute mapping file with full paths
        sql = """
            SELECT a.attribute_id, a.name, a.element_id, e.name as element_name
            FROM attribute a
            JOIN element e ON a.element_id = e.element_id
            ORDER BY a.attribute_id
        """
        cur.execute(sql)
        attributes = cur.fetchall()
        
        # Create mapping using full path format
        attribute_mapping = {}
        for row in attributes:
            element_path = get_element_path(row['element_id'])
            # Use backslash path format like in the cache files
            full_path = f"\\\\{element_path}|{row['name']}"
            attribute_mapping[full_path] = row['attribute_id']
        
        attr_map_filename = f"attribute_mapping_{database_name.replace(' ', '_')}.json"
        attr_map_path = data_folder / attr_map_filename
        
        with open(attr_map_path, 'w', encoding='utf-8') as f:
            json.dump(attribute_mapping, f, indent=4, ensure_ascii=False)
        
        cur.close()
        
        return {
            'attribute_mapping': str(attr_map_path),
            'attribute_count': len(attribute_mapping)
        }
        
    except Exception as e:
        raise Exception(f"Failed to update JSON cache files: {e}")


def get_all_elements(conn) -> List[Dict]:
    """
    Get all elements from the database with their hierarchy.
    Returns list of element dicts.
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        sql = """
            SELECT element_id, name, level, parent_id
            FROM element
            ORDER BY level, name
        """
        cur.execute(sql)
        results = cur.fetchall()
        cur.close()
        
        return [dict(row) for row in results]
    except psycopg2.Error as e:
        raise Exception(f"Failed to fetch all elements: {e}")


def get_all_attributes(conn, element_id: Optional[str] = None) -> List[Dict]:
    """
    Get all attributes from the database, optionally filtered by element_id.
    Returns list of attribute dicts.
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        if element_id:
            sql = """
                SELECT a.attribute_id, a.name, a.element_id, a.kks,
                       e.name as element_name
                FROM attribute a
                JOIN element e ON a.element_id = e.element_id
                WHERE a.element_id = %s
                ORDER BY a.name
            """
            cur.execute(sql, (int(element_id),))
        else:
            sql = """
                SELECT a.attribute_id, a.name, a.element_id, a.kks,
                       e.name as element_name
                FROM attribute a
                JOIN element e ON a.element_id = e.element_id
                ORDER BY e.name, a.name
            """
            cur.execute(sql)
        
        results = cur.fetchall()
        cur.close()
        
        return [dict(row) for row in results]
    except psycopg2.Error as e:
        raise Exception(f"Failed to fetch attributes: {e}")


def delete_element(conn, element_id: int) -> Dict[str, int]:
    """
    Delete an element from the database.
    Also deletes associated attributes and archive data (cascade).
    
    Returns dict with counts of deleted records.
    """
    try:
        cur = conn.cursor()
        
        # Count attributes before deletion
        cur.execute("SELECT COUNT(*) FROM attribute WHERE element_id = %s", (element_id,))
        attr_count = cur.fetchone()[0]
        
        # Count archive records before deletion
        cur.execute("""
            SELECT COUNT(*) FROM archive 
            WHERE attribute_id IN (SELECT attribute_id FROM attribute WHERE element_id = %s)
        """, (element_id,))
        archive_count = cur.fetchone()[0]
        
        # Delete archive data first
        cur.execute("""
            DELETE FROM archive 
            WHERE attribute_id IN (SELECT attribute_id FROM attribute WHERE element_id = %s)
        """, (element_id,))
        
        # Delete attributes
        cur.execute("DELETE FROM attribute WHERE element_id = %s", (element_id,))
        
        # Delete element
        cur.execute("DELETE FROM element WHERE element_id = %s", (element_id,))
        
        conn.commit()
        cur.close()
        
        return {
            'elements_deleted': 1,
            'attributes_deleted': attr_count,
            'archive_records_deleted': archive_count
        }
    except psycopg2.Error as e:
        conn.rollback()
        raise Exception(f"Failed to delete element: {e}")


def update_attribute(conn, attribute_id: int, update_data: Dict) -> Dict[str, any]:
    """
    Update a derived attribute's properties and formula.
    Only derived attributes (those with triggers) can be updated.
    
    Args:
        conn: Database connection
        attribute_id: ID of the derived attribute to update
        update_data: Dict with optional keys:
            - name: New attribute name
            - kks: New KKS identifier
            - formula: New formula (optional, only if changing formula)
            - recreate_trigger: Whether to recreate trigger (default True if formula provided)
            - recompute_archive: Whether to recompute archive data (default True if formula changed)
    
    Returns:
        Dict with update results including records affected
    
    Raises:
        ValueError: If attribute is not a derived attribute (no trigger exists)
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get current attribute info
        cur.execute("SELECT * FROM attribute WHERE attribute_id = %s", (attribute_id,))
        current_attr = cur.fetchone()
        if not current_attr:
            raise ValueError(f"Attribute {attribute_id} not found")
        
        # Check if this is a derived attribute by checking for trigger existence
        function_name = f"compute_derived_attr_{attribute_id}"
        cur.execute("""
            SELECT COUNT(*) as count
            FROM pg_proc
            WHERE proname = %s
        """, (function_name,))
        has_trigger = cur.fetchone()['count'] > 0
        
        if not has_trigger:
            raise ValueError(
                f"Attribute {attribute_id} is not a derived attribute. "
                "Only derived attributes (with formulas/triggers) can be updated. "
                "Source attributes from PI systems should not be modified."
            )
        
        cur.close()
        
        results = {'attribute_id': attribute_id, 'updated_fields': [], 'is_derived': True}
        
        # Update basic fields (name, kks) - these can be updated independently
        update_fields = []
        update_values = []
        
        if 'name' in update_data and update_data['name']:
            update_fields.append("name = %s")
            update_values.append(update_data['name'])
            results['updated_fields'].append('name')
        
        if 'kks' in update_data and update_data['kks']:
            update_fields.append("kks = %s")
            update_values.append(update_data['kks'])
            results['updated_fields'].append('kks')
        
        if update_fields:
            cur = conn.cursor()
            sql = f"UPDATE attribute SET {', '.join(update_fields)} WHERE attribute_id = %s"
            update_values.append(attribute_id)
            cur.execute(sql, update_values)
            conn.commit()
            cur.close()
        
        # Handle formula update for derived attributes (optional)
        if 'formula' in update_data and update_data['formula']:
            formula = update_data['formula']
            results['updated_fields'].append('formula')
            
            # Drop existing trigger
            try:
                drop_derived_attribute_trigger(conn, attribute_id)
                results['trigger_dropped'] = True
            except Exception as e:
                results['trigger_dropped'] = False
                results['trigger_drop_error'] = str(e)
            
            # Recompute archive data if requested (default: True)
            if update_data.get('recompute_archive', True):
                # Delete old derived values
                cur = conn.cursor()
                cur.execute("DELETE FROM archive WHERE attribute_id = %s", (attribute_id,))
                deleted_count = cur.rowcount
                conn.commit()
                cur.close()
                results['archive_records_deleted'] = deleted_count
                
                # Backfill with new formula
                inserted_count = backfill_derived_attribute(conn, attribute_id, formula)
                results['archive_records_inserted'] = inserted_count
            
            # Recreate trigger if requested (default: True)
            if update_data.get('recreate_trigger', True):
                trigger_name = create_derived_attribute_trigger(conn, attribute_id, formula)
                results['trigger_created'] = trigger_name
        
        if not results['updated_fields']:
            raise ValueError("At least one field (name, kks, or formula) must be provided for update")
        
        return results
        
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to update attribute: {e}")


def delete_attribute(conn, attribute_id: int) -> Dict[str, int]:
    """
    Delete an attribute from the database.
    Also deletes associated archive data and any triggers.
    
    Returns dict with counts of deleted records.
    """
    try:
        cur = conn.cursor()
        
        # Try to drop trigger if it exists (for derived attributes)
        try:
            drop_derived_attribute_trigger(conn, attribute_id)
        except Exception:
            # Trigger might not exist, continue with deletion
            pass
        
        # Count archive records before deletion
        cur.execute("SELECT COUNT(*) FROM archive WHERE attribute_id = %s", (attribute_id,))
        archive_count = cur.fetchone()[0]
        
        # Delete archive data first
        cur.execute("DELETE FROM archive WHERE attribute_id = %s", (attribute_id,))
        
        # Delete attribute
        cur.execute("DELETE FROM attribute WHERE attribute_id = %s", (attribute_id,))
        
        conn.commit()
        cur.close()
        
        return {
            'attributes_deleted': 1,
            'archive_records_deleted': archive_count
        }
    except psycopg2.Error as e:
        conn.rollback()
        raise Exception(f"Failed to delete attribute: {e}")

