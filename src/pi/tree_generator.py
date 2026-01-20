import json
import requests
import urllib3
import configparser
from requests_ntlm import HttpNtlmAuth

# Constants
BASE_URL = "https://YOUR_PI_WEBAPI_URL/piwebapi"
import os
config = configparser.ConfigParser()
credential_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'credentials', 'pi_credentials.ini')
config.read(credential_path)
creds_section = 'MONGDUONG1'  # Use 'MONGDUONG1' for Mong Duong, 'VINHTAN2' for Vinh Tan
USERNAME = config[creds_section]['USERNAME']
PASSWORD = config[creds_section]['PASSWORD']

def get_api(endpoint):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session = requests.Session()
    session.verify = False
    session.auth = HttpNtlmAuth(USERNAME, PASSWORD)
    url = BASE_URL + endpoint
    r = session.get(url)
    r.raise_for_status()
    return r.json()

def get(endpoint):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session = requests.Session()
    session.verify = False
    session.auth = HttpNtlmAuth(USERNAME, PASSWORD)
    url =endpoint
    r = session.get(url)
    r.raise_for_status()
    return r.json()


def get_attributes(webid):
    """Get attributes for a given element."""
    try:
        endpoint = f"/elements/{webid}/attributes"
        data = get_api(endpoint)
        attributes = []
        for attr in data.get("Items", []):
            try:
                kks=(get(attr['Links'].get('Point')))['Name']
                print(kks)
            except Exception:
                kks = None
            attributes.append({
                "name": attr['Name'],
                "webid": attr['WebId'],
                "type": attr.get('Type', 'Unknown'),
                "path": attr.get('Path', ''),
                "kks": kks
            })
        return attributes
    except Exception as e:
        print(f"Error getting attributes: {e}")
        return []

def build_node(name, webid, is_db=False):
    """Recursive function to build the JSON tree."""
    print(f"Crawling: {name}")
    endpoint = f"/assetdatabases/{webid}/elements" if is_db else f"/elements/{webid}/elements"
    
    try:
        data = get_api(endpoint)
        items = data.get("Items", [])
        
        children = []
        attributes = []
        
        # If no child elements, this is a leaf - get its attributes
        if not items and not is_db:
            print(f"  → Leaf node detected. Fetching attributes...")
            attributes = get_attributes(webid)
            print(f"  → Found {len(attributes)} attributes")
        
        # Process child elements
        for item in items:
            children.append(build_node(item['Name'], item['WebId']))


        
        return {
            "name": name,
            "webid": webid,
            "children": children,
            "attributes": attributes,
            "is_leaf": len(children) == 0 and not is_db
        }
    except Exception as e:
        print(f"Error at {name}: {e}")
        return {
            "name": name,
            "webid": webid,
            "children": [],
            "attributes": [],
            "is_leaf": True
        }

def main():
    try:
        servers = get_api("/assetservers")
        server_webid = servers["Items"][0]["WebId"]
        server_name = servers["Items"][0]["Name"]
        
        print(f"Connected to {server_name}. Starting full crawl...")
        
        dbs = get_api(f"/assetservers/{server_webid}/assetdatabases")
        tree_data = []
        for db in dbs.get("Items", []):
            prompt = input(f"Do you want to crawl this database {db['Name']}: ")
            if prompt.upper() == "YES":
                tree_data.append(build_node(db['Name'], db['WebId'], is_db=True))
            else:
                continue
            
        output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'mong_duong', 'Early_Warning_System_MD1_pi_tree_cache.json')
        with open(output_path, "w") as f:
            json.dump(tree_data, f, indent=4)
            
        print(f"\nSuccess! Tree saved to {output_path}")
    except Exception as e:
        print(f"Failed to generate tree: {e}")

if __name__ == "__main__":
    main()