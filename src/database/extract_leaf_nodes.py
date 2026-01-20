import json
import os
from pathlib import Path

def extract_leaf_nodes(node, path_parts=[]):
    """
    Recursively traverse the tree and extract leaf nodes.
    A node is a leaf if it has no children or empty children list.
    Returns a dict with path as key and webid as value.
    """
    results = {}
    
    # Build current path
    current_path = "|".join(path_parts + [node['name']])
    
    # Check if this is a leaf node (no children or empty children)
    if not node.get('children'):
        # This is a leaf node
        results[current_path] = node['webid']
    else:
        # Recursively process children
        for child in node['children']:
            child_results = extract_leaf_nodes(child, path_parts + [node['name']])
            results.update(child_results)
    
    return results


def process_json_file(input_path):
    """
    Process a single JSON file and extract leaf nodes.
    
    Args:
        input_path: Path to the input JSON file
    
    Returns:
        dict: Dictionary of leaf nodes (path -> webid)
    """
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            tree_data = json.load(f)
        
        all_leaf_nodes = {}
        
        # Process each root node in the tree
        if isinstance(tree_data, list):
            for root in tree_data:
                leaf_nodes = extract_leaf_nodes(root)
                all_leaf_nodes.update(leaf_nodes)
        else:
            leaf_nodes = extract_leaf_nodes(tree_data)
            all_leaf_nodes.update(leaf_nodes)
        
        return all_leaf_nodes
    except Exception as e:
        print(f"  ✗ Error processing {input_path}: {e}")
        return None


def main():
    """
    Extract leaf nodes from all JSON files in the data directory.
    Processes all JSON files recursively and saves output in the same directory.
    """
    # Get project root (3 levels up from this file)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    data_dir = os.path.join(project_root, 'data')
    
    if not os.path.exists(data_dir):
        print(f"✗ Error: Data directory not found: {data_dir}")
        return
    
    print("=" * 70)
    print("Leaf Node Extraction from Data Directory")
    print("=" * 70)
    print(f"Scanning directory: {data_dir}\n")
    
    # Find all JSON files in data directory (recursively)
    json_files = []
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith('.json'):
                # Skip output files (files that contain 'selected' or 'webids' in name)
                if 'selected' not in file.lower() and 'webids' not in file.lower():
                    json_files.append(os.path.join(root, file))
    
    if not json_files:
        print("✗ No JSON files found in data directory")
        return
    
    print(f"Found {len(json_files)} JSON file(s) to process:\n")
    for json_file in json_files:
        print(f"  - {os.path.relpath(json_file, project_root)}")
    print()
    
    # Process each JSON file
    total_leaf_nodes = 0
    processed_count = 0
    
    for json_file in json_files:
        print(f"Processing: {os.path.basename(json_file)}")
        
        leaf_nodes = process_json_file(json_file)
        
        if leaf_nodes is not None:
            # Generate output filename (same directory, with _selected_webids suffix)
            file_dir = os.path.dirname(json_file)
            file_name = os.path.basename(json_file)
            file_base = os.path.splitext(file_name)[0]
            output_file = os.path.join(file_dir, f"{file_base}_selected_webids.json")
            
            # Write output file
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(leaf_nodes, f, indent=2)
                
                print(f"  ✓ Extracted {len(leaf_nodes)} leaf nodes")
                print(f"  ✓ Output saved to: {os.path.relpath(output_file, project_root)}")
                total_leaf_nodes += len(leaf_nodes)
                processed_count += 1
            except Exception as e:
                print(f"  ✗ Error writing output file: {e}")
        print()
    
    # Summary
    print("=" * 70)
    print("Summary:")
    print(f"  Files processed: {processed_count}/{len(json_files)}")
    print(f"  Total leaf nodes extracted: {total_leaf_nodes}")
    print("=" * 70)


if __name__ == "__main__":
    main()
