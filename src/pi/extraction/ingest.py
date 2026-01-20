import ujson as json_lib
import pandas as pd
import requests
from sqlalchemy import create_engine, text
import urllib3
from requests_ntlm import HttpNtlmAuth
import io
import os
import sys
from exchangelib import Credentials, Account, Message, Mailbox, Configuration, FileAttachment, DELEGATE
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import configparser

# ----------------------------------------------------
# INITIALIZATION & ENCODING
# ----------------------------------------------------
os.environ["PYTHONIOENCODING"] = "utf-8"

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ----------------------------------------------------
# 1. DATABASE UTILITY FUNCTIONS
# ----------------------------------------------------

def pgconnect(credential_filepath, db_schema="public", section="DEFAULT", pi_section="DEFAULT"):
    pi_config = configparser.ConfigParser()
    pi_config.read('credentials\\pi_credentials.ini')
    USERNAME = pi_config[pi_section]['USERNAME']
    PASSWORD = pi_config[pi_section]['PASSWORD']
    config = configparser.ConfigParser()
    config.read(credential_filepath)
    
    if section not in config:
        raise ValueError(f"Section [{section}] not found in {credential_filepath}")
    
    creds = config[section]
    host = creds['host']
    db_user = creds['user']
    db_pw = creds['password']
    default_db = creds['database']
    port = creds['port']
    
    try:
        db = create_engine(f'postgresql+psycopg2://{db_user}:{db_pw}@{host}:{port}/{default_db}', echo=False, future=True)
        print('Connected successfully.')
    except Exception as e:
        print("Unable to connect to the database.")
        print(e)
    return db, USERNAME, PASSWORD

# ----------------------------------------------------
# 2. EMAIL UTILITY
# ----------------------------------------------------

def send_email(account, to_email_list, subject, body, attachments=None):
    try:
        if isinstance(to_email_list, str):
            to_email_list = [to_email_list]

        message = Message(
            account=account,
            folder=account.sent,
            subject=subject,
            body=body,
            to_recipients=[Mailbox(email_address=email.strip()) for email in to_email_list]
        )

        if attachments:
            for filepath in attachments:
                with open(filepath, 'rb') as f:
                    filename = os.path.basename(filepath)
                    attachment = FileAttachment(name=filename, content=f.read())
                    message.attach(attachment)

        message.send()
        print(f"Email sent to: {', '.join(to_email_list)}.")
    except Exception as e:
        print(f"Error sending email: {e}")

# ----------------------------------------------------
# 3. DATA EXTRACTION (USING SESSION)
# ----------------------------------------------------

def read_data(session, all_raw_fan_webids, quartile_pair, interval, attribute_id_map, base_url):
    start_time, end_time = quartile_pair[0], quartile_pair[1]
    print(f"Processing from {start_time} to {end_time}")
    
    selected_fields = "Items.Path;Items.Items.Timestamp;Items.Items.Value"
    bulk_request = {}
    for idx, (tag_name, webid) in enumerate(all_raw_fan_webids.items(), start=1):
        if webid is None or webid == "" or webid == "null":
            continue
        bulk_request[f"request_{idx}"] = {
            "method": "GET",
            "resource": f"{base_url}/streamsets/{webid}/interpolated?startTime={start_time}&endTime={end_time}&interval={interval}&selectedFields={selected_fields}"
        }
    try:
        data_response = session.post(
            f"{base_url}/batch",
            json=bulk_request,
            timeout=3000
        )
        archive_rows = []
        batch_data = json_lib.loads(data_response.content)
        for requests_name, request_data in batch_data.items():
            items = request_data.get("Content", {}).get("Items", [])
            for item in items:
                path_key = item['Path'] 
                for val_entry in item.get("Items", []):
                    val = val_entry['Value']
                    if isinstance(val, dict): 
                        val=None 
                    archive_rows.append({
                        'lookup_key': path_key,
                        'timestamp': val_entry['Timestamp'],
                        'value': val
                    })

        df = pd.DataFrame(archive_rows)
        df['attribute_id'] = df['lookup_key'].map(attribute_id_map)
        df = df.drop(columns=['lookup_key'])
        return df
        
    except Exception as e:
        print(f"Error processing {e}")
        raise Exception

# ----------------------------------------------------
# 4. POPULATE DATA 
# ----------------------------------------------------

def populate_data(df, db, table_name='archive'):
    required_cols = ['attribute_id', 'timestamp', 'value']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"    ✗ Missing required columns: {missing_cols}")
        return
    
    df['timestamp'] = pd.to_datetime(df['timestamp'], format="ISO8601") + pd.Timedelta(hours=7)
    df['value'] = df['value'].apply(lambda x: 1 if x is True else 0 if x is False else x)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df[df['attribute_id'].notna()]
    
    if len(df) == 0:
        print(f"    ℹ No valid rows to insert")
        raise Exception("No valid rows to insert after cleaning.")
    
    initial_len = len(df)
    df = df.drop_duplicates(subset=['attribute_id', 'timestamp'], keep='first')
    final_len = len(df)
    if initial_len > final_len:
        print(f"    ℹ Removed {initial_len - final_len} duplicate rows")
    
    df = df[required_cols]
    df['attribute_id'] = pd.to_numeric(df['attribute_id'], errors='coerce')
    df['attribute_id'] = df['attribute_id'].astype('int64')

    print(f"    → Inserting {len(df)} rows into {table_name}...")
    conn = db.raw_connection()
    cur = None
    try:
        cur = conn.cursor()
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
        output.seek(0)
        columns = tuple(required_cols)
        cur.copy_from(output, table_name, null="\\N", columns=columns)
        conn.commit()
        print(f"    ✓ Successfully inserted {len(df)} rows into {table_name}")
    except Exception as e:
        print(f"    ✗ Error during population: {e}")
        conn.rollback()
        import traceback
        traceback.print_exc()
    finally:
        if cur is not None:
            try: cur.close()
            except Exception: pass
        if conn is not None:
            try: conn.close()
            except Exception: pass

# ----------------------------------------------------
# 5. THREAD WORKER (WITH CLOSURE & EMAIL)
# ----------------------------------------------------

def process_database_thread(db_section, webids_file, thread_name, base_url):
    print(f"\n[{thread_name}] Starting thread for database: {db_section}")
    db = None
    session = requests.Session()
    
    try:
        if thread_name=="Thread-1-MD1" or thread_name=="Thread-2-MD1":
            pi_section="MONGDUONG1"
        else:
            pi_section="VINHTAN2"

        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        credential_filepath = os.path.join(project_root, 'credentials', 'db_credentials.ini')
        if not os.path.exists(credential_filepath):
            credential_filepath = os.path.join(project_root, 'config', 'db_credentials.ini')
            
        db, USERNAME, PASSWORD = pgconnect(credential_filepath, section=db_section, pi_section=pi_section)
        session.auth = HttpNtlmAuth(USERNAME, PASSWORD)
        session.verify = False

        with open(webids_file, 'r', encoding='utf-8') as f:
            all_raw_fan_webids = json.load(f)
        
        with db.connect() as conn:
            last_ts = str(conn.execute(text("SELECT max(timestamp) FROM archive")).scalar())

        start_time = str(pd.to_datetime(last_ts, format="ISO8601") + pd.Timedelta(minutes=1))
        now = datetime.now().replace(microsecond=0, second=0)
        
        map_paths = {
            "Thread-1-MD1": "data\\mong_duong\\attribute_mapping_Early_Warning_System_MD1.json",
            "Thread-2-MD1": "data\\mong_duong\\attribute_mapping_MD1.json",
            "Thread-3-VT2": "data\\vinh_tan\\attribute_mapping_Early_Warning_System_VT2.json",
            "Thread-4-VT2": "data\\vinh_tan\\attribute_mapping_VT2.json"
        }
        with open(map_paths[thread_name], 'r', encoding='utf-8') as f:
            attribute_id_map = json.load(f)

        duration_minutes = (now - pd.to_datetime(start_time)).total_seconds() / 60
        expected_rows = int(duration_minutes * 5000)

        try:
            final_dfs = read_data(session, all_raw_fan_webids, [start_time, "*"], "1m", attribute_id_map, base_url)
            if final_dfs is not None and not final_dfs.empty:
                if len(final_dfs) < expected_rows:
                    raise Exception(f"Data density too low: Got {len(final_dfs)}, expected ~{expected_rows}.")
                populate_data(final_dfs, db, table_name='archive')
        except Exception as e:
            if now == pd.to_datetime(start_time, format="ISO8601"):
                print("No new data.")
                return

            print(f"[{thread_name}] Triggering Recovery Logic: {e}")
            
            # --- EMAIL CODE ---
            email_val = 'YOUR_EMAIL_ADDRESS'
            to_email = ['YOUR_RECIPIENT_EMAIL']
            creds_obj = Credentials(username='YOUR_EMAIL_USERNAME', password='YOUR_EMAIL_PASSWORD')
            config_obj = Configuration(server='mail.eps.genco3.vn', credentials=creds_obj)
            account_obj = Account(primary_smtp_address=email_val, config=config_obj, autodiscover=False, access_type=DELEGATE)
            # send_email(account_obj, to_email, "Lambda Function Error Alert", f"An error occurred in {thread_name}:\n\n{e}")

            start_dt = pd.to_datetime(start_time, format="ISO8601")
            now_dt = pd.to_datetime(now, format="ISO8601")
            time_diff_hours = int((now_dt - start_dt).total_seconds() / 3600)

            for i in range(time_diff_hours + 1):
                q_start = start_dt + pd.Timedelta(hours=i)
                q_end = q_start + pd.Timedelta(minutes=59)
                if q_start >= now_dt: break
                if q_end > now_dt: q_end = now_dt

                try:
                    block_df = read_data(session, all_raw_fan_webids, [str(q_start), str(q_end)], "1m", attribute_id_map, base_url)
                    populate_data(block_df, db, table_name='archive')
                except Exception as be:
                    print(f"Error in block {i+1}: {be}")

    finally:
        session.close()
        if db: db.dispose()
        print(f"[{thread_name}] Resource closure complete.")

# ----------------------------------------------------
# 6. MAIN EXECUTION
# ----------------------------------------------------

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    configs = [
        ('Early Warning System MD1', os.path.join(project_root, 'data', 'mong_duong', 'Early_Warning_System_MD1_pi_tree_cache_selected_webids.json'), 'Thread-1-MD1', 'https://YOUR_MD_PI_WEBAPI_URL/piwebapi'),
        ('MONGDUONG1', os.path.join(project_root, 'data', 'mong_duong', 'MD1_pi_tree_cache_selected_webids.json'), 'Thread-2-MD1', 'https://YOUR_MD_PI_WEBAPI_URL/piwebapi'),
        ('Early Warning System VT2', os.path.join(project_root, 'data', 'vinh_tan', 'Early_Warning_System_VT2_pi_tree_cache_selected_webids.json'), 'Thread-3-VT2', 'https://YOUR_VT_PI_WEBAPI_URL/piwebapi'),
        ('VINHTAN2', os.path.join(project_root, 'data', 'vinh_tan', 'VT2_pi_tree_cache_selected_webids.json'), 'Thread-4-VT2', 'https://YOUR_VT_PI_WEBAPI_URL/piwebapi')
    ]
    valid_configs = [c for c in configs if os.path.exists(c[1])]
    with ThreadPoolExecutor(max_workers=len(valid_configs)) as executor:
        futures = {executor.submit(process_database_thread, *cfg): cfg[2] for cfg in valid_configs}
        for future in as_completed(futures):
            try: future.result()
            except Exception as e: print(f"Thread failed: {e}")

if __name__ == "__main__":
    main()