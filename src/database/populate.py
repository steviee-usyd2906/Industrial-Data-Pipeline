"""
Database Population Script for PI Tree Structure

This script reads the pi_tree_cache.json file and populates a PostgreSQL database
with three tables:
1. element: Stores hierarchical element structure (level, element_id, name, parent_id)
2. attribute: Stores attributes linked to elements (element_id, attribute_id, name, kks)
3. archive: Stores time series data for attributes (archive_id, attribute_id, timestamp, value)

Usage:
    python populate_database.py
    
Configuration:
    - Update 'config/db_credentials.ini' with your database credentials
    - Update 'db_section' variable to specify which database section to use
    - Update 'json_file_path' variable to point to your pi_tree_cache.json file
"""

import json
import os
import configparser
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.sql import func
from typing import Optional, Dict, List
import sys


def pgconnect(credential_filepath, section='MONGDUONG1'):
    """
    Connect to PostgreSQL database using credentials from INI file.
    
    Args:
        credential_filepath: Path to INI file containing database credentials
        section: Section name in INI file (default: 'MONGDUONG1')
            Available sections: 'Early Warning System MD1', 'Early Warning System VT2', 'VINHTAN2', 'MONGDUONG1'
    
    Returns:
        SQLAlchemy Engine object or None if connection fails
    """
    config = configparser.ConfigParser()
    config.read(credential_filepath)
    
    if section not in config:
        print(f"✗ Section '{section}' not found in {credential_filepath}")
        return None
    
    host = config[section]['host']
    db_user = config[section]['user']
    db_pw = config[section]['password']
    default_db = config[section]['database']
    port = config[section].getint('port')
    
    try:
        db = create_engine(
            f'postgresql+psycopg2://{db_user}:{db_pw}@{host}:{port}/{default_db}',
            echo=False,
            future=True,
            pool_size=20,
            max_overflow=40,
            pool_pre_ping=True
        )
        print('✓ Connected to database successfully.')
        return db
    except Exception as e:
        print(f"✗ Unable to connect to the database: {e}")
        return None


def create_tables(engine, derived_attributes_backup: List[Dict] = None):
    """
    Create the three tables: element, attribute, and archive.
    Preserves archive table and data if it already exists.
    
    Tables created:
    1. element: level, element_id (PK), name, parent_id (FK)
    2. attribute: element_id (FK), attribute_id (PK), name, kks
    3. archive: archive_id (PK), attribute_id (FK), timestamp, value
    
    Args:
        engine: SQLAlchemy Engine object
        derived_attributes_backup: List of derived attribute dicts to preserve (already backed up)
    """
    metadata = MetaData()
    
    # Element table - stores hierarchical structure (4 columns only)
    element_table = Table(
        'element', metadata,
        Column('level', Integer, nullable=False),
        Column('element_id', Integer, primary_key=True, autoincrement=True),
        Column('name', String(500), nullable=False),
        Column('parent_id', Integer, ForeignKey('element.element_id'), nullable=True)
    )
    
    # Attribute table - stores attributes linked to elements (4 columns only)
    attribute_table = Table(
        'attribute', metadata,
        Column('element_id', Integer, ForeignKey('element.element_id', ondelete='CASCADE'), nullable=False),
        Column('attribute_id', Integer, primary_key=True, autoincrement=True),
        Column('name', String(500), nullable=False),
        Column('kks', String(500), nullable=True)
    )
    
    # Archive table - stores time series data (PRESERVED - not dropped)
    archive_table = Table(
        'archive', metadata,
        Column('archive_id', Integer, primary_key=True, autoincrement=True),
        Column('attribute_id', Integer, ForeignKey('attribute.attribute_id', ondelete='CASCADE'), nullable=False),
        Column('timestamp', DateTime, nullable=False),
        Column('value', Float, nullable=True)
    )
    
    # Create indexes for better query performance
    from sqlalchemy import Index
    Index('idx_element_parent', element_table.c.parent_id)
    Index('idx_element_level', element_table.c.level)
    Index('idx_attribute_element', attribute_table.c.element_id)
    Index('idx_archive_attribute', archive_table.c.attribute_id)
    Index('idx_archive_timestamp', archive_table.c.timestamp)
    
    try:
        with engine.connect() as conn:
            # Check if archive table exists
            archive_exists = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'archive'
                )
            """)).scalar()
            
            if archive_exists:
                print('ℹ️  Archive table exists - will be preserved')
                # Disable foreign key constraint temporarily
                conn.execute(text("ALTER TABLE archive DISABLE TRIGGER ALL"))
                # Drop the foreign key constraint on archive
                conn.execute(text("""
                    ALTER TABLE archive 
                    DROP CONSTRAINT IF EXISTS archive_attribute_id_fkey
                """))
                # Now drop element and attribute tables
                conn.execute(text("DROP TABLE IF EXISTS attribute CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS element CASCADE"))
                conn.commit()
                # Re-enable triggers on archive after dropping dependent tables
                conn.execute(text("ALTER TABLE archive ENABLE TRIGGER ALL"))
                conn.commit()
            else:
                print('ℹ️  No existing archive table found')
        
        # Create element and attribute tables (archive will be created if doesn't exist)
        metadata.create_all(engine)
        
        print('✓ Tables created successfully.')
        print('  - element table')
        print('  - attribute table')
        print('  - archive table (preserved if existed)')
        print('  - Indexes created')
        return True
    except Exception as e:
        print(f"✗ Error creating tables: {e}")
        return False


def backup_derived_attributes(engine) -> tuple[List[Dict], Dict[str, int]]:
    """
    Backup derived attributes and old attribute mapping before dropping tables.
    Identifies derived attributes by checking for trigger functions.
    
    Args:
        engine: SQLAlchemy Engine object
    
    Returns:
        Tuple of (derived_attributes list, old_attribute_mapping dict)
    """
    derived_attributes = []
    old_attribute_mapping = {}
    
    with engine.connect() as conn:
        # Check if tables exist
        tables_exist = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'attribute'
            )
        """)).scalar()
        
        if not tables_exist:
            return derived_attributes, old_attribute_mapping
        
        # Check if archive exists - if so, backup old attribute mapping
        archive_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'archive'
            )
        """)).scalar()
        
        if archive_exists:
            print("\n🔄 Backing up attribute mapping for archive data...")
            result = conn.execute(text("""
                WITH RECURSIVE element_paths AS (
                    SELECT 
                        e.element_id,
                        e.name,
                        e.parent_id,
                        ARRAY[e.name::VARCHAR] as path_array
                    FROM element e
                    WHERE e.parent_id IS NULL
                    
                    UNION ALL
                    
                    SELECT 
                        e.element_id,
                        e.name,
                        e.parent_id,
                        ep.path_array || e.name
                    FROM element e
                    INNER JOIN element_paths ep ON e.parent_id = ep.element_id
                )
                SELECT 
                    a.attribute_id,
                    array_to_string(ep.path_array, '|') || '|' || a.name as full_path
                FROM attribute a
                INNER JOIN element_paths ep ON a.element_id = ep.element_id
            """))
            
            for row in result:
                old_attr_id = row[0]
                full_path = row[1]
                old_attribute_mapping[full_path] = old_attr_id
            
            print(f"  ✓ Backed up {len(old_attribute_mapping)} attribute mappings")
        
        # Check for derived attributes (those with triggers)
        print("\n🔍 Checking for derived attributes...")
        result = conn.execute(text("""
            SELECT DISTINCT
                p.proname,
                CAST(SUBSTRING(p.proname FROM 'compute_derived_attr_([0-9]+)') AS INTEGER) as attribute_id
            FROM pg_proc p
            WHERE p.proname LIKE 'compute_derived_attr_%'
        """))
        
        trigger_attr_ids = {row[1] for row in result if row[1] is not None}
        
        if trigger_attr_ids:
            print(f"  ℹ️  Found {len(trigger_attr_ids)} derived attributes with triggers")
            
            # Get full details of derived attributes
            result = conn.execute(text("""
                WITH RECURSIVE element_paths AS (
                    SELECT 
                        e.element_id,
                        e.name,
                        e.parent_id,
                        ARRAY[e.name::VARCHAR] as path_array
                    FROM element e
                    WHERE e.parent_id IS NULL
                    
                    UNION ALL
                    
                    SELECT 
                        e.element_id,
                        e.name,
                        e.parent_id,
                        ep.path_array || e.name
                    FROM element e
                    INNER JOIN element_paths ep ON e.parent_id = ep.element_id
                )
                SELECT 
                    a.attribute_id,
                    a.name,
                    a.kks,
                    a.element_id,
                    array_to_string(ep.path_array, '|') as element_path,
                    e.name as element_name
                FROM attribute a
                INNER JOIN element_paths ep ON a.element_id = ep.element_id
                INNER JOIN element e ON a.element_id = e.element_id
                WHERE a.attribute_id = ANY(:attr_ids)
            """), {'attr_ids': list(trigger_attr_ids)})
            
            for row in result:
                derived_attributes.append({
                    'attribute_id': row[0],
                    'name': row[1],
                    'kks': row[2],
                    'old_element_id': row[3],
                    'element_path': row[4],
                    'element_name': row[5]
                })
            
            print(f"  ✓ Backed up {len(derived_attributes)} derived attributes")
        else:
            print("  ℹ️  No derived attributes found")
    
    return derived_attributes, old_attribute_mapping


def process_tree_node(engine, node: Dict, parent_id: Optional[int] = None, level: int = 0) -> Optional[int]:
    """
    Recursively process a tree node and insert it into the database.
    
    Args:
        engine: SQLAlchemy Engine object
        node: Dictionary containing node data (name, children, attributes)
        parent_id: ID of parent element in database (None for root nodes)
        level: Hierarchy level (0 for root)
    
    Returns:
        element_id of the inserted element, or None if insertion failed
    """
    try:
        with engine.connect() as conn:
            # Insert the element (only 4 columns: level, element_id, name, parent_id)
            insert_element = text("""
                INSERT INTO element (level, name, parent_id)
                VALUES (:level, :name, :parent_id)
                RETURNING element_id
            """)
            
            result = conn.execute(insert_element, {
                'level': level,
                'name': node.get('name', ''),
                'parent_id': parent_id
            })
            conn.commit()
            
            element_id = result.scalar()
            
            # Insert attributes if this element has any (only 4 columns: element_id, attribute_id, name, kks)
            attributes = node.get('attributes', [])
            if attributes:
                for attr in attributes:
                    insert_attribute = text("""
                        INSERT INTO attribute (element_id, name, kks)
                        VALUES (:element_id, :name, :kks)
                    """)
                    conn.execute(insert_attribute, {
                        'element_id': element_id,
                        'name': attr.get('name', ''),
                        'kks': attr.get('kks')
                    })
                conn.commit()
            
            # Recursively process children
            children = node.get('children', [])
            for child in children:
                process_tree_node(engine, child, parent_id=element_id, level=level + 1)
            
            return element_id
            
    except Exception as e:
        print(f"✗ Error processing node '{node.get('name', 'Unknown')}': {e}")
        return None


def populate_database(engine, json_file_path: str, derived_attributes: List[Dict] = None, old_attribute_mapping: Dict[str, int] = None):
    """
    Main function to populate database from JSON tree structure.
    Preserves archive data and updates attribute_id mappings.
    
    Args:
        engine: SQLAlchemy Engine object
        json_file_path: Path to the pi_tree_cache.json file
        derived_attributes: List of derived attributes backed up before dropping tables
        old_attribute_mapping: Dict mapping full_path -> old_attribute_id for archive updates
    """
    # Check if file exists
    if not os.path.exists(json_file_path):
        print(f"✗ Error: File not found: {json_file_path}")
        return
    
    print(f"\n📖 Reading JSON file: {json_file_path}")
    
    # Read JSON file
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            tree_data = json.load(f)
        print(f"✓ JSON file loaded successfully.")
    except Exception as e:
        print(f"✗ Error reading JSON file: {e}")
        return
    
    # Process the tree data
    print("\n🔄 Processing tree structure and inserting into database...")
    
    # Handle both list and single object formats
    if isinstance(tree_data, list):
        print(f"Found {len(tree_data)} root node(s)")
        for root_node in tree_data:
            process_tree_node(engine, root_node, parent_id=None, level=0)
    else:
        print("Found single root node")
        process_tree_node(engine, tree_data, parent_id=None, level=0)
    
    # Restore derived attributes
    if derived_attributes:
        print("\n🔄 Restoring derived attributes...")
        restore_derived_attributes(engine, derived_attributes)
    
    # Update archive table attribute IDs if we have old mappings
    if old_attribute_mapping:
        print("\n🔄 Updating archive table attribute IDs...")
        update_archive_attribute_ids(engine, old_attribute_mapping)
    
    # After updates, restore foreign key constraint if it was removed
    print("\n🔗 Verifying foreign key constraints...")
    with engine.connect() as conn:
        # First, clean up orphaned archive records (pointing to non-existent attributes)
        result = conn.execute(text("""
            DELETE FROM archive 
            WHERE attribute_id NOT IN (SELECT attribute_id FROM attribute)
        """))
        deleted_count = result.rowcount
        if deleted_count > 0:
            print(f"  🧹 Cleaned up {deleted_count} orphaned archive records")
            conn.commit()
        
        # Check if constraint exists
        constraint_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.table_constraints 
                WHERE table_name = 'archive' 
                AND constraint_name = 'archive_attribute_id_fkey'
            )
        """)).scalar()
        
        if not constraint_exists:
            try:
                conn.execute(text("""
                    ALTER TABLE archive 
                    ADD CONSTRAINT archive_attribute_id_fkey 
                    FOREIGN KEY (attribute_id) REFERENCES attribute(attribute_id) ON DELETE CASCADE
                """))
                conn.commit()
                print("  ✓ Foreign key constraint restored")
            except Exception as e:
                print(f"  ⚠️  Could not restore FK constraint: {e}")
        else:
            print("  ✓ Foreign key constraint already exists")
    
    # Get statistics
    print("\n📊 Database Statistics:")
    with engine.connect() as conn:
        element_count = conn.execute(text("SELECT COUNT(*) FROM element")).scalar()
        attribute_count = conn.execute(text("SELECT COUNT(*) FROM attribute")).scalar()
        archive_count = conn.execute(text("SELECT COUNT(*) FROM archive")).scalar()
        
        print(f"  Elements: {element_count}")
        print(f"  Attributes: {attribute_count}")
        print(f"  Archive records: {archive_count}")
    
    print("\n✓ Database population completed successfully!")


def restore_derived_attributes(engine, derived_attributes: List[Dict]):
    """
    Restore derived attributes that were backed up before repopulation.
    
    Args:
        engine: SQLAlchemy Engine object
        derived_attributes: List of derived attribute dicts with element_path
    """
    restored_count = 0
    skipped_count = 0
    
    with engine.connect() as conn:
        for attr in derived_attributes:
            # Find the new element_id for this derived attribute's parent element
            result = conn.execute(text("""
                WITH RECURSIVE element_paths AS (
                    SELECT 
                        e.element_id,
                        e.name,
                        e.parent_id,
                        ARRAY[e.name::VARCHAR] as path_array
                    FROM element e
                    WHERE e.parent_id IS NULL
                    
                    UNION ALL
                    
                    SELECT 
                        e.element_id,
                        e.name,
                        e.parent_id,
                        ep.path_array || e.name
                    FROM element e
                    INNER JOIN element_paths ep ON e.parent_id = ep.element_id
                )
                SELECT element_id
                FROM element_paths
                WHERE array_to_string(path_array, '|') = :element_path
            """), {'element_path': attr['element_path']})
            
            row = result.fetchone()
            if row:
                new_element_id = row[0]
                
                # Check if this derived attribute already exists
                check_result = conn.execute(text("""
                    SELECT attribute_id 
                    FROM attribute 
                    WHERE element_id = :element_id AND name = :name
                """), {'element_id': new_element_id, 'name': attr['name']})
                
                if check_result.fetchone() is None:
                    # Insert the derived attribute
                    conn.execute(text("""
                        INSERT INTO attribute (element_id, name, kks)
                        VALUES (:element_id, :name, :kks)
                    """), {
                        'element_id': new_element_id,
                        'name': attr['name'],
                        'kks': attr['kks']
                    })
                    restored_count += 1
                else:
                    skipped_count += 1
            else:
                print(f"  ⚠️  Could not find parent element for derived attribute: {attr['name']}")
                skipped_count += 1
        
        conn.commit()
    
    print(f"  ✓ Restored {restored_count} derived attributes")
    if skipped_count > 0:
        print(f"  ℹ️  Skipped {skipped_count} derived attributes (already exist or parent not found)")


def update_archive_attribute_ids(engine, old_mapping: Dict[str, int]):
    """
    Update archive table to use new attribute IDs after repopulation.
    
    Args:
        engine: SQLAlchemy Engine object
        old_mapping: Dict mapping full_path -> old_attribute_id
    """
    with engine.connect() as conn:
        # Get new attribute mapping
        result = conn.execute(text("""
            WITH RECURSIVE element_paths AS (
                SELECT 
                    e.element_id,
                    e.name,
                    e.parent_id,
                    ARRAY[e.name::VARCHAR] as path_array
                FROM element e
                WHERE e.parent_id IS NULL
                
                UNION ALL
                
                SELECT 
                    e.element_id,
                    e.name,
                    e.parent_id,
                    ep.path_array || e.name
                FROM element e
                INNER JOIN element_paths ep ON e.parent_id = ep.element_id
            )
            SELECT 
                a.attribute_id,
                array_to_string(ep.path_array, '|') || '|' || a.name as full_path
            FROM attribute a
            INNER JOIN element_paths ep ON a.element_id = ep.element_id
        """))
        
        new_mapping = {}
        for row in result:
            new_attr_id = row[0]
            full_path = row[1]
            new_mapping[full_path] = new_attr_id
        
        # Create mapping: old_id -> new_id
        id_mapping = {}
        for full_path, old_id in old_mapping.items():
            if full_path in new_mapping:
                new_id = new_mapping[full_path]
                if old_id != new_id:
                    id_mapping[old_id] = new_id
        
        if id_mapping:
            print(f"  ℹ️  Found {len(id_mapping)} attribute ID changes to update")
            
            # Update archive records in batches
            updated_count = 0
            for old_id, new_id in id_mapping.items():
                result = conn.execute(text("""
                    UPDATE archive 
                    SET attribute_id = :new_id 
                    WHERE attribute_id = :old_id
                """), {'old_id': old_id, 'new_id': new_id})
                updated_count += result.rowcount
            
            conn.commit()
            print(f"  ✓ Updated {updated_count} archive records with new attribute IDs")
        else:
            print("  ℹ️  No attribute ID changes needed")


def create_indexes(db, json_file_path):
    attribute_map = {}
    if json_file_path=='data\\vinh_tan\\VT2_pi_tree_cache.json':
        table_path = 'data\\vinh_tan\\attribute_mapping_VT2.json'
    elif json_file_path=='data\\mong_duong\\MD1_pi_tree_cache.json':
        table_path = 'data\\mong_duong\\attribute_mapping_MD1.json'
    elif json_file_path=='data\\vinh_tan\\Early_Warning_System_VT2_pi_tree_cache.json':
        table_path = 'data\\vinh_tan\\attribute_mapping_Early_Warning_System_VT2.json'
    elif json_file_path=='data\\mong_duong\\Early_Warning_System_MD1_pi_tree_cache.json':
        table_path = 'data\\mong_duong\\attribute_mapping_Early_Warning_System_MD1.json'
    with db.connect() as conn:
        # Query to get attribute_id, element name path, and attribute name
        # We need to reconstruct the path from element hierarchy to match webids format
        result = conn.execute(text("""
            WITH RECURSIVE element_paths AS (
                -- Base case: root elements (parent_id IS NULL)
                SELECT 
                    e.element_id,
                    e.name,
                    e.parent_id,
                    e.level,
                    ARRAY[e.name::VARCHAR] as path_array
                FROM element e
                WHERE e.parent_id IS NULL
                
                UNION ALL
                
                -- Recursive case: child elements
                SELECT 
                    e.element_id,
                    e.name,
                    e.parent_id,
                    e.level,
                    ep.path_array || e.name
                FROM element e
                INNER JOIN element_paths ep ON e.parent_id = ep.element_id
            )
            SELECT 
                a.attribute_id,
                array_to_string(ep.path_array, '|') as element_path,
                a.name as attribute_name
            FROM attribute a
            INNER JOIN element_paths ep ON a.element_id = ep.element_id
        """))
        
        for row in result:
            element_path = row[1]
            attribute_name = row[2]
            attribute_id = row[0]
            # Store mapping - use element path + attribute name as key
            # This matches the format: "Element1|Element2|Element3" + "AttributeName"
            attribute_map[(element_path, attribute_name)] = attribute_id
    json_ready_map = {
        f"{path}|{attr}": attr_id 
        for (path, attr), attr_id in attribute_map.items()
    }

    with open(table_path, 'w', encoding='utf-8') as f:
        json.dump(json_ready_map, f, indent=4, ensure_ascii=False)
    print(f"✓ Attribute mapping saved to {table_path}")

def convert_mappings():
    mapping_files = [
        "data/mong_duong/attribute_mapping_Early_Warning_System_MD1.json",
        "data/mong_duong/attribute_mapping_MD1.json",
        "data/vinh_tan/attribute_mapping_Early_Warning_System_VT2.json",
        "data/vinh_tan/attribute_mapping_VT2.json"
    ]

    # This prefix matches the start of the Path returned by PI Web API
    # Adjust if your AF Server names are different
    prefixes = {
        "MD1": "\\\\RMS-MD1-PIAF\\",
        "VT2": "\\\\RMS-VT2-PIAF\\"
    }

    for file_path in mapping_files:
        if not os.path.exists(file_path):
            continue
            
        with open(file_path, 'r', encoding='utf-8') as f:
            old_map = json.load(f)
        
        prefix = prefixes["MD1"] if "MD1" in file_path else prefixes["VT2"]
        
        # Convert "Name|Sub|Attr" -> "\\AFSERVER\Name\Sub|Attr"
        # Note: PI Web API paths use backslashes for hierarchy but 
        # keep the pipe | for the attribute name at the end.
        new_map = {}
        for pi_name, attr_id in old_map.items():
            # Split by pipe, convert all but last to backslashes
            parts = pi_name.split('|')
            if len(parts) > 1:
                # Join all hierarchy parts with backslash, keep last part with pipe
                hierarchy = '\\'.join(parts[:-1])
                raw_path = prefix + hierarchy + '|' + parts[-1]
            else:
                # No attribute separator, just add prefix
                raw_path = prefix + pi_name
            new_map[raw_path] = attr_id
            
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(new_map, f, indent=4, ensure_ascii=False)
        print(f"Created: {file_path}")
def main():
    """
    Main entry point for the script.
    """
    config = {'VINHTAN2': ['data\\vinh_tan\\VT2_pi_tree_cache.json','credentials\\db_credentials.ini'], 'MONGDUONG1': ['data\\mong_duong\\MD1_pi_tree_cache.json', 'credentials\\db_credentials.ini'], 'Early Warning System VT2': ['data\\vinh_tan\\Early_Warning_System_VT2_pi_tree_cache.json', 'credentials\\db_credentials.ini'], 'Early Warning System MD1': ['data\\mong_duong\\Early_Warning_System_MD1_pi_tree_cache.json', 'credentials\\db_credentials.ini']}
    for db_section, data in config.items():
        json_file_path = data[0]
        credential_filepath = data[1]
        engine = pgconnect(credential_filepath, section=db_section)
        if engine is None:
            print(f"\n✗ Failed to connect to database {db_section}. Exiting.")
            sys.exit(1)
        
        # 1. Backup derived attributes BEFORE dropping tables
        print("\n🔍 Backing up derived attributes...")
        derived_attrs, old_mapping = backup_derived_attributes(engine)
        
        # 2. Create tables (drops element/attribute, preserves archive)
        print("\n📋 Creating database tables...")
        if not create_tables(engine):
            print("\n✗ Failed to create tables. Exiting.")
            sys.exit(1)
        
        # 3. Populate database (with derived attributes restoration)
        populate_database(engine, json_file_path, derived_attrs, old_mapping)
        
        # 4. Create indexes
        create_indexes(engine, json_file_path)
        print("\n" + "=" * 70)
        print("Script completed!")
        print("=" * 70)
    convert_mappings()



if __name__ == "__main__":
    main()
