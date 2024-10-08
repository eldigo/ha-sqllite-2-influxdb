import sqlite3
import json
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
import logging
import os

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# SQLite database file path (update this to the correct path)
sqlite_db = os.getenv("SQLITE_DB") # Use environment variable for SQLITE_DB

# InfluxDB v2 connection details (update with your settings)
influx_url = os.getenv("INFLUXDB_URL")  # Use environment variable for INFLUXDB_URL
influx_token = os.getenv("INFLUXDB_TOKEN")  # Use environment variable for INFLUXDB_TOKEN
influx_org = os.getenv("INFLUXDB_ORG")  # Use environment variable for "orginization"
influx_bucket = os.getenv("INFLUXDB_BUCKET")  # Use environment variable for "bucket"

# Batch size for processing rows
BATCH_SIZE = 10000

try:
    # Connect to SQLite
    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()
    logging.info("Successfully connected to SQLite")
except sqlite3.Error as e:
    logging.error(f"SQLite error: {e}")
    exit(1)

try:
    # Connect to InfluxDB (v2)
    client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    query_api = client.query_api()
    logging.info("Successfully connected to InfluxDB")
except Exception as e:
    logging.error(f"InfluxDB connection error: {e}")
    exit(1)

# Function to query the oldest timestamp from InfluxDB
def get_oldest_influx_timestamp():
    try:
        query_string = f'''
        from(bucket: "{influx_bucket}")
          |> range(start: 0)
          |> filter(fn: (r) => r["_measurement"] == "units")
          |> sort(columns: ["_time"], desc: false)
          |> limit(n: 1)
        '''
        result = query_api.query(org=influx_org, query=query_string)
        if result and len(result) > 0:
            return result[0].records[0].get_time().isoformat()
    except Exception as e:
        logging.error(f"Error querying InfluxDB for the oldest timestamp: {e}")
    return None

# Fetch the oldest timestamp from InfluxDB
oldest_influx_timestamp = get_oldest_influx_timestamp()

# Debug: Print the oldest timestamp fetched from InfluxDB
print(f"Oldest InfluxDB timestamp: {oldest_influx_timestamp}")

# Check if the timestamp is None (i.e., no data in InfluxDB)
if oldest_influx_timestamp:
    try:
        # Format the timestamp to match SQLite's format (removing 'Z' from ISO format)
        dt_obj = datetime.fromisoformat(oldest_influx_timestamp.replace('Z', ''))
        formatted_timestamp = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
        print(f"Formatted timestamp for SQLite: {formatted_timestamp}")
    except ValueError as e:
        logging.error(f"Error parsing timestamp: {e}")
        exit(1)
else:
    print("No entries found in InfluxDB. Proceeding without timestamp filter.")
    formatted_timestamp = None

# Adjusted SQL query for Home Assistant's new database structure (2023+)
sqlite_query = """
SELECT s.state, sm.entity_id, s.last_updated_ts, sa.shared_attrs
FROM states s
LEFT JOIN state_attributes sa ON sa.attributes_id = s.attributes_id
JOIN states_meta sm ON sm.metadata_id = s.metadata_id
"""

# Add a timestamp filter if the oldest timestamp exists in InfluxDB
if formatted_timestamp:
    sqlite_query += f' AND s.last_updated_ts < "{formatted_timestamp}"'

sqlite_query += " ORDER BY s.last_updated_ts ASC"

# Debug: Print the final query
print(f"Final SQLite query: {sqlite_query}")

# Function to process rows in batches and write to InfluxDB
def batch_insert_to_influx(rows):
    points = []
    for row in rows:
        state, entity_id, last_updated_ts, shared_attrs = row

        # Parse the attributes JSON
        try:
            attributes_json = json.loads(shared_attrs)
            friendly_name = attributes_json.get('friendly_name', 'Unknown')
            unit_of_measurement = attributes_json.get('unit_of_measurement', 'units')
        except (TypeError, json.JSONDecodeError) as e:
            logging.warning(f"Failed to parse attributes for {entity_id}: {e}")
            friendly_name = 'Unknown'
            unit_of_measurement = 'units'

        # Extract domain and entity_id without domain
        domain, _, entity_id_short = entity_id.partition('.')

        # Create InfluxDB point
        try:
            # Convert last_updated_ts from seconds to datetime
            last_updated_dt = datetime.fromtimestamp(float(last_updated_ts))

            point = Point(unit_of_measurement)
            point.tag("source", "HA")
            point.tag("domain", domain)
            point.tag("entity_id", entity_id_short)
            point.tag("friendly_name", friendly_name)
            point.time(last_updated_dt)

            # Add all fields from shared_attrs to the point, ensuring consistent types
            for key, value in attributes_json.items():
                # Avoid field type conflicts by skipping conflicting fields
                if key in ["id", "id_str"]:
                    logging.warning(f"Skipping field '{key}' due to potential type conflicts.")
                    continue

                if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
                    point.field(key, float(value))
                else:
                    point.field(key, str(value))

            points.append(point)

        except ValueError as e:
            logging.warning(f"Error preparing InfluxDB point for entity {entity_id}: {e}")

    # Batch write to InfluxDB
    if points:
        try:
            write_api.write(bucket=influx_bucket, org=influx_org, record=points)
            logging.info(f"Successfully wrote {len(points)} points to InfluxDB")
        except Exception as e:
            logging.error(f"Error writing points to InfluxDB: {e}")
    else:
        logging.info("No points to write in this batch.")

# Fetch rows from SQLite in batches
try:
    cursor.execute(sqlite_query)
    rows_fetched = 0
    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        batch_insert_to_influx(rows)
        rows_fetched += len(rows)
        logging.info(f"Processed {rows_fetched} rows so far.")
except sqlite3.Error as e:
    logging.error(f"SQLite query error: {e}")
    conn.close()
    exit(1)

# Close connections
try:
    conn.close()
    client.close()
    logging.info("Closed connections to SQLite and InfluxDB")
except Exception as e:
    logging.error(f"Error closing connections: {e}")

print("Data export complete.")