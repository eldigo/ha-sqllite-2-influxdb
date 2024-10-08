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

# Function to connect to SQLite
def connect_to_sqlite(db_path):
    try:
        conn = sqlite3.connect(db_path)
        logging.info("Successfully connected to SQLite")
        return conn
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
        exit(1)

# Function to connect to InfluxDB
def connect_to_influxdb(url, token, org):
    try:
        client = InfluxDBClient(url=url, token=token, org=org)
        logging.info("Successfully connected to InfluxDB")
        return client
    except Exception as e:
        logging.error(f"InfluxDB connection error: {e}")
        exit(1)

# Function to query the oldest timestamp from InfluxDB
def get_oldest_influx_timestamp(query_api, bucket):
    try:
        query_string = f'''
        from(bucket: "{bucket}")
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

# Function to format timestamp
def format_timestamp(iso_timestamp):
    try:
        dt_obj = datetime.fromisoformat(iso_timestamp.replace('Z', ''))
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        logging.error(f"Error parsing timestamp: {e}")
        exit(1)

# Function to create SQLite query
def create_sqlite_query(formatted_timestamp):
    sqlite_query = """
    SELECT s.state, sm.entity_id, s.last_updated_ts, sa.shared_attrs
    FROM states s
    LEFT JOIN state_attributes sa ON sa.attributes_id = s.attributes_id
    JOIN states_meta sm ON sm.metadata_id = s.metadata_id
    """
    if formatted_timestamp:
        sqlite_query += f' AND s.last_updated_ts < "{formatted_timestamp}"'
    return sqlite_query + " ORDER BY s.last_updated_ts ASC"

# Function to process attributes JSON
def parse_attributes(shared_attrs):
    try:
        return json.loads(shared_attrs)
    except (TypeError, json.JSONDecodeError) as e:
        logging.warning(f"Failed to parse attributes: {e}")
        return {}

# Function to batch insert to InfluxDB
def batch_insert_to_influx(write_api, rows):
    points = []
    for row in rows:
        state, entity_id, last_updated_ts, shared_attrs = row
        attributes_json = parse_attributes(shared_attrs)

        friendly_name = attributes_json.get('friendly_name', 'Unknown')
        unit_of_measurement = attributes_json.get('unit_of_measurement', 'units')

        # Extract domain and entity_id without domain
        domain, _, entity_id_short = entity_id.partition('.')

        # Create InfluxDB point
        try:
            last_updated_dt = datetime.fromtimestamp(float(last_updated_ts))
            point = Point(unit_of_measurement).tag("source", "HA").tag("domain", domain)\
                .tag("entity_id", entity_id_short).tag("friendly_name", friendly_name).time(last_updated_dt)

            for key, value in attributes_json.items():
                if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
                    point.field(key, float(value))
                else:
                    point.field(key, str(value))

            points.append(point)

        except ValueError as e:
            logging.warning(f"Error preparing InfluxDB point for entity {entity_id}: {e}")

    if points:
        try:
            write_api.write(bucket=influx_bucket, org=influx_org, record=points)
            logging.info(f"Successfully wrote {len(points)} points to InfluxDB")
        except Exception as e:
            logging.error(f"Error writing points to InfluxDB: {e}")
    else:
        logging.info("No points to write in this batch.")

# Main execution flow
def main():
    conn = connect_to_sqlite(sqlite_db)
    client = connect_to_influxdb(influx_url, influx_token, influx_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    query_api = client.query_api()

    oldest_influx_timestamp = get_oldest_influx_timestamp(query_api, influx_bucket)
    logging.info(f"Oldest InfluxDB timestamp: {oldest_influx_timestamp}")

    formatted_timestamp = format_timestamp(oldest_influx_timestamp) if oldest_influx_timestamp else None

    sqlite_query = create_sqlite_query(formatted_timestamp)
    logging.info(f"Final SQLite query: {sqlite_query}")

    try:
        cursor = conn.cursor()
        cursor.execute(sqlite_query)

        rows_fetched = 0
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            batch_insert_to_influx(write_api, rows)
            rows_fetched += len(rows)
            logging.info(f"Processed {rows_fetched} rows so far.")
    except sqlite3.Error as e:
        logging.error(f"SQLite query error: {e}")
    finally:
        cursor.close()
        conn.close()
        client.close()
        logging.info("Closed connections to SQLite and InfluxDB")

    logging.info("Import to InfluxDB complete.")

if __name__ == "__main__":
    main()
