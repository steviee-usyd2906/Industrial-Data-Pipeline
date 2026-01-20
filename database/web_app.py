"""
Flask web application for PI data extraction and download.
Users can select database, leaf elements, and time ranges.
"""

from flask import Flask, render_template, request, jsonify, send_file
import json
import os
from datetime import datetime
from pathlib import Path
import traceback

from database import (
    list_available_databases,
    get_connection,
    get_database_config_section,
    get_leaf_elements,
    get_element_details,
    get_element_attributes,
    get_timeseries_data,
    get_timestamp_range,
    export_to_csv,
    export_to_parquet,
    insert_element,
    insert_attribute,
    update_attribute,
    update_json_cache_files,
    get_all_elements,
    get_all_attributes,
    delete_element,
    delete_attribute,
    lookup_element_id_by_name,
    lookup_attribute_id_by_name,
    find_element_by_name,
    find_attribute_by_name,
    search_elements_by_name,
    search_attributes_by_name
)

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB max

EXPORT_DIR = Path(__file__).resolve().parent / "exports"
EXPORT_DIR.mkdir(exist_ok=True)


@app.route('/')
def index():
    """Home page - database and element selection."""
    try:
        databases = list_available_databases()
        return render_template('index.html', databases=databases)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/databases')
def get_databases():
    """API endpoint to list all available databases."""
    try:
        databases = list_available_databases()
        return jsonify({
            'success': True,
            'databases': databases
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/elements/<database_name>')
def get_elements(database_name):
    """API endpoint to get all leaf elements for a database."""
    try:
        config_section = get_database_config_section(database_name)
        conn = get_connection(config_section)
        leaf_elements = get_leaf_elements(conn)
        conn.close()
        
        return jsonify({
            'success': True,
            'elements': leaf_elements
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/attributes/<database_name>/<element_id>')
def get_attributes(database_name, element_id):
    """API endpoint to get attributes for a selected leaf element."""
    try:
        config_section = get_database_config_section(database_name)
        conn = get_connection(config_section)
        element_details = get_element_details(conn, element_id)
        attributes = get_element_attributes(conn, element_id)
        
        # Get timestamp range for the first attribute
        if attributes:
            min_ts, max_ts = get_timestamp_range(conn, attributes[0]['attribute_id'])
        else:
            min_ts, max_ts = None, None
        
        conn.close()
        
        return jsonify({
            'success': True,
            'element': element_details,
            'attributes': attributes,
            'min_timestamp': str(min_ts) if min_ts else None,
            'max_timestamp': str(max_ts) if max_ts else None
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/download', methods=['POST'])
def download_data():
    """API endpoint to download time series data."""
    try:
        data = request.json
        database_name = data.get('database')
        selected_elements = data.get('elements')  # List of element_ids
        selected_elements = [int(x) for x in selected_elements]
        start_timestamp = data.get('start_timestamp')
        end_timestamp = data.get('end_timestamp')
        format_type = data.get('format', 'csv')  # 'csv' or 'parquet'
        
        if not database_name or not selected_elements:
            return jsonify({'success': False, 'error': 'Database and elements are required'}), 400
        
        # Fetch attributes for all selected elements
        config_section = get_database_config_section(database_name)
        conn = get_connection(config_section)
        all_attribute_ids = []
        
        for element_id in selected_elements:
            attributes = get_element_attributes(conn, element_id)
            all_attribute_ids.extend([attr['attribute_id'] for attr in attributes])
        
        if not all_attribute_ids:
            conn.close()
            return jsonify({'success': False, 'error': 'No attributes found for selected elements'}), 400
        
        # --- DEBUG: print request time range ---
        print("REQ start_timestamp:", start_timestamp)
        print("REQ end_timestamp  :", end_timestamp)
        print("ATTR_COUNT:", len(all_attribute_ids))

        # --- DEBUG: get available range for a representative attribute (first one) ---
        min_ts, max_ts = get_timestamp_range(conn, all_attribute_ids[0])
        print("AVAILABLE RANGE (first attr):", min_ts, "->", max_ts)
        
        # Fetch time series data
        df = get_timeseries_data(
            conn,
            all_attribute_ids,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            element_ids=selected_elements
        )
        
        conn.close()
        
        if df.empty:
            return jsonify({'success': False, 'error': 'No data found for the selected criteria'}), 400
        
        # Generate export filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_filename = f"pi_data_{timestamp}"
        
        if format_type == 'parquet':
            filepath = EXPORT_DIR / f"{base_filename}.parquet"
            export_to_parquet(df, str(filepath))
        else:
            filepath = EXPORT_DIR / f"{base_filename}.csv"
            export_to_csv(df, str(filepath))
        
        return jsonify({
            'success': True,
            'filename': filepath.name,
            'rows': len(df),
            'columns': list(df.columns)
        })
    
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/download/<filename>')
def download_file(filename):
    """Download exported file."""
    try:
        filepath = EXPORT_DIR / filename
        
        if not filepath.exists():
            return jsonify({'error': 'File not found'}), 404
        
        return send_file(
            str(filepath),
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/cleanup', methods=['POST'])
def cleanup_old_files():
    """Remove old exported files (older than 24 hours)."""
    try:
        import time
        cutoff_time = time.time() - (24 * 3600)  # 24 hours ago
        
        deleted_count = 0
        for filepath in EXPORT_DIR.glob('pi_data_*'):
            if filepath.stat().st_mtime < cutoff_time:
                filepath.unlink()
                deleted_count += 1
        
        return jsonify({'success': True, 'deleted': deleted_count})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/elements/all/<database_name>')
def get_all_elements_api(database_name):
    """API endpoint to get all elements (not just leaf nodes)."""
    try:
        config_section = get_database_config_section(database_name)
        conn = get_connection(config_section)
        elements = get_all_elements(conn)
        conn.close()
        
        return jsonify({
            'success': True,
            'elements': elements
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/attributes/all/<database_name>', methods=['GET'])
def get_all_attributes_api(database_name):
    """API endpoint to get all attributes, optionally filtered by element_id."""
    try:
        element_id = request.args.get('element_id')
        config_section = get_database_config_section(database_name)
        conn = get_connection(config_section)
        attributes = get_all_attributes(conn, element_id)
        conn.close()
        
        return jsonify({
            'success': True,
            'attributes': attributes
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/element/insert', methods=['POST'])
def insert_element_api():
    """API endpoint to insert a new element."""
    try:
        data = request.json
        database_name = data.get('database')
        element_data = data.get('element')
        
        if not database_name or not element_data:
            return jsonify({'success': False, 'error': 'Database and element data are required'}), 400
        
        config_section = get_database_config_section(database_name)
        conn = get_connection(config_section)
        element_id = insert_element(conn, element_data)
        conn.close()
        
        return jsonify({
            'success': True,
            'element_id': element_id,
            'message': f'Element created successfully with ID: {element_id}'
        })
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/attribute/insert', methods=['POST'])
def insert_attribute_api():
    """API endpoint to insert a new attribute."""
    try:
        data = request.json
        database_name = data.get('database')
        attribute_data = data.get('attribute')
        
        if not database_name or not attribute_data:
            return jsonify({'success': False, 'error': 'Database and attribute data are required'}), 400
        
        config_section = get_database_config_section(database_name)
        conn = get_connection(config_section)
        attribute_id = insert_attribute(conn, attribute_data)
        conn.close()
        
        message = f'Attribute created successfully with ID: {attribute_id}'
        if attribute_data.get('formula'):
            message += ' (formula provided - archive data backfilled)'
        
        return jsonify({
            'success': True,
            'attribute_id': attribute_id,
            'message': message
        })
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/update-cache/<database_name>', methods=['POST'])
def update_cache_files(database_name):
    """API endpoint to update JSON cache files in data folder."""
    try:
        config_section = get_database_config_section(database_name)
        conn = get_connection(config_section)
        result = update_json_cache_files(database_name, conn)
        conn.close()
        
        return jsonify({
            'success': True,
            'result': result,
            'message': f'Cache files updated successfully. {result["attribute_count"]} attributes, {result["element_count"]} elements.'
        })
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/element/delete', methods=['POST'])
def delete_element_api():
    """API endpoint to delete an element and its associated data."""
    try:
        data = request.json
        database_name = data.get('database')
        element_id = data.get('element_id')
        
        if not database_name or not element_id:
            return jsonify({'success': False, 'error': 'Database and element_id are required'}), 400
        
        config_section = get_database_config_section(database_name)
        conn = get_connection(config_section)
        result = delete_element(conn, int(element_id))
        conn.close()
        
        return jsonify({
            'success': True,
            'result': result,
            'message': f'Element deleted successfully. {result["attributes_deleted"]} attributes and {result["archive_records_deleted"]} archive records removed.'
        })
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/attribute/delete', methods=['POST'])
def delete_attribute_api():
    """API endpoint to delete an attribute and its associated data."""
    try:
        data = request.json
        database_name = data.get('database')
        attribute_id = data.get('attribute_id')
        
        if not database_name or not attribute_id:
            return jsonify({'success': False, 'error': 'Database and attribute_id are required'}), 400
        
        config_section = get_database_config_section(database_name)
        conn = get_connection(config_section)
        result = delete_attribute(conn, int(attribute_id))
        conn.close()
        
        return jsonify({
            'success': True,
            'result': result,
            'message': f'Attribute deleted successfully. {result["archive_records_deleted"]} archive records removed.'
        })
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/attribute/update', methods=['POST'])
def update_attribute_api():
    """API endpoint to update a derived attribute's properties and formula."""
    try:
        data = request.json
        database_name = data.get('database')
        attribute_id = data.get('attribute_id')
        update_data = data.get('update_data')
        
        if not database_name or not attribute_id or not update_data:
            return jsonify({'success': False, 'error': 'Database, attribute_id, and update_data are required'}), 400
        
        config_section = get_database_config_section(database_name)
        conn = get_connection(config_section)
        result = update_attribute(conn, int(attribute_id), update_data)
        conn.close()
        
        message = f'Derived attribute updated successfully. Fields updated: {", ".join(result["updated_fields"])}'
        if result.get('archive_records_inserted'):
            message += f'. {result["archive_records_inserted"]} archive records recomputed.'
        
        return jsonify({
            'success': True,
            'result': result,
            'message': message
        })
    except ValueError as e:
        # Handle non-derived attribute error or validation error
        error_msg = str(e)
        status_code = 400 if "not a derived attribute" in error_msg or "field" in error_msg else 400
        return jsonify({'success': False, 'error': error_msg}), status_code
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/lookup', methods=['POST'])
def lookup_id():
    """
    Look up element or attribute ID by name.
    Supports exact name lookup and pattern-based search.
    
    Expected JSON:
    {
        "database": "database_name",
        "type": "element" or "attribute",
        "name": "exact name or pattern with %",
        "element_id": (optional, for attribute lookup)
    }
    
    Returns:
    {
        "success": true/false,
        "results": [list of matching records]
    }
    """
    try:
        data = request.get_json()
        database = data.get('database')
        lookup_type = data.get('type')  # 'element' or 'attribute'
        name = data.get('name')
        element_id = data.get('element_id')
        
        if not database or not lookup_type or not name:
            return jsonify({'success': False, 'error': 'Missing required fields: database, type, name'}), 400
        
        if lookup_type not in ['element', 'attribute']:
            return jsonify({'success': False, 'error': 'Type must be "element" or "attribute"'}), 400
        
        conn = get_connection(database)
        results = []
        
        if lookup_type == 'element':
            if '%' in name:
                # Pattern search
                results = search_elements_by_name(conn, name)
            else:
                # Exact lookup
                result = find_element_by_name(conn, name)
                if result:
                    results = [result]
        else:  # attribute
            if '%' in name:
                # Pattern search
                results = search_attributes_by_name(conn, name, element_id)
            else:
                # Exact lookup
                result = find_attribute_by_name(conn, name, element_id)
                if result:
                    results = [result]
        
        conn.close()
        
        return jsonify({
            'success': True,
            'results': results,
            'count': len(results)
        })
        
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


def get_port():
    """Get port from environment or use default."""
    return int(os.environ.get('FLASK_PORT', 5000))


if __name__ == '__main__':
    port = get_port()
    print("-" * 70)
    print(f"Open your browser to: http://127.0.0.1:{port}/")
    print("-" * 70)
    app.run(debug=True, host='0.0.0.0', port=port)

