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

# Retrieve configuration from environment variables
sqlite_db = os.getenv("SQLITE_DB")
influx_url = os.getenv("INFLUXDB_URL")
influx_token = os.getenv("INFLUXDB_TOKEN")
influx_org = os.getenv("INFLUXDB_ORG")
influx_bucket = os.getenv("INFLUXDB_BUCKET")

# Validate environment variables
required_env_vars = [sqlite_db, influx_url, influx_token, influx_org, influx_bucket]
if any(v is None for v in required_env_vars):
    logging.error("One or more required environment variables are not set.")
    exit(1)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10000))

def connect_to_sqlite(db_path):
    """Connect to the SQLite database and return the connection and cursor."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        logging.info("Successfully connected to SQLite")
        return conn, cursor
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
        exit(1)

def connect_to_influxdb(url, token, org):
    """Connect to InfluxDB and return the client and APIs."""
    try:
        client = InfluxDBClient(url=url, token=token, org=org)
        logging.info("Successfully connected to InfluxDB")
        return client.write_api(write_options=SYNCHRONOUS), client.query_api()
    except Exception as e:
        logging.error(f"InfluxDB connection error: {e}")
        exit(1)

def get_oldest_influx_timestamp(query_api):
    """Query InfluxDB for the oldest timestamp."""
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

def format_timestamp(oldest_timestamp):
    """Format the timestamp for SQLite."""
    try:
        dt_obj = datetime.fromisoformat(oldest_timestamp.replace('Z', ''))
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        logging.error(f"Error parsing timestamp: {e}")
        exit(1)

def build_sqlite_query(formatted_timestamp):
    """Build the SQLite query with an optional timestamp filter."""
    base_query = """
    SELECT s.state, sm.entity_id, s.last_updated_ts, sa.shared_attrs
    FROM states s
    LEFT JOIN state_attributes sa ON sa.attributes_id = s.attributes_id
    JOIN states_meta sm ON sm.metadata_id = s.metadata_id
    """
    if formatted_timestamp:
        return f"{base_query} WHERE s.last_updated_ts < '{formatted_timestamp}' ORDER BY s.last_updated_ts ASC"
    return f"{base_query} ORDER BY s.last_updated_ts ASC"

def parse_attributes(shared_attrs):
    """Parse the shared attributes JSON."""
    try:
        return json.loads(shared_attrs)
    except (TypeError, json.JSONDecodeError) as e:
        logging.warning(f"Failed to parse attributes: {e}")
        return {}

def batch_insert_to_influx(write_api, rows):
    """Process rows in batches and write them to InfluxDB."""
    points = []
    for row in rows:
        state, entity_id, last_updated_ts, shared_attrs = row
        attributes_json = parse_attributes(shared_attrs)

        friendly_name = attributes_json.get('friendly_name', 'Unknown')
        unit_of_measurement = attributes_json.get('unit_of_measurement', 'default_measurement')
        domain, _, entity_id_short = entity_id.partition('.')

        if not unit_of_measurement:
            unit_of_measurement = 'default_measurement'

        try:
            last_updated_dt = datetime.fromtimestamp(float(last_updated_ts))
            point = Point(unit_of_measurement).tag("source", "HA").tag("domain", domain)
            point.tag("entity_id", entity_id_short).tag("friendly_name", friendly_name).time(last_updated_dt)

            # Insert state as either state_float or state_str
            if isinstance(state, (int, float)) or (isinstance(state, str) and state.replace('.', '', 1).isdigit()):
                point.field("state_float", float(state))
            else:
                point.field("state_str", str(state))

            for key, value in attributes_json.items():
                # Avoid field type conflicts by ensuring consistent types
                if key in ["id", "id_str"]:
                    continue

                # Handle type conflicts by renaming fields with inconsistent types
                try:
                    if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
                        value = float(value)
                        point.field(key, value)
                    else:
                        point.field(f"{key}_str", str(value))
                except Exception as e:
                    logging.warning(f"Skipping field '{key}' for entity '{entity_id}' due to type conflict: {e}")

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

def main():
    """Main execution flow."""
    conn, cursor = connect_to_sqlite(sqlite_db)
    write_api, query_api = connect_to_influxdb(influx_url, influx_token, influx_org)

    oldest_influx_timestamp = get_oldest_influx_timestamp(query_api)
    logging.info(f"Oldest InfluxDB timestamp: {oldest_influx_timestamp}")

    formatted_timestamp = format_timestamp(oldest_influx_timestamp) if oldest_influx_timestamp else None
    sqlite_query = build_sqlite_query(formatted_timestamp)
    logging.info(f"Final SQLite query: {sqlite_query}")

    try:
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
        write_api.client.close()
        logging.info("Closed connections to SQLite and InfluxDB")

    logging.info("Data export complete.")

if __name__ == "__main__":
    main()
