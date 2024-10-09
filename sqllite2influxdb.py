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
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
logging_level = logging.DEBUG if DEBUG_MODE else logging.INFO
logging.basicConfig(level=logging_level, format='%(asctime)s - %(levelname)s - %(message)s')

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
    try:
        # Connect to SQLite database and return connection and cursor
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        logging.info("Successfully connected to SQLite")
        return conn, cursor
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
        exit(1)

def connect_to_influxdb(url, token, org):
    try:
        # Connect to InfluxDB and return the client write and query APIs
        client = InfluxDBClient(url=url, token=token, org=org)
        logging.info("Successfully connected to InfluxDB")
        return client.write_api(write_options=SYNCHRONOUS), client.query_api()
    except Exception as e:
        logging.error(f"InfluxDB connection error: {e}")
        exit(1)

def get_oldest_influx_timestamp(query_api):
    try:
        # Query InfluxDB for the oldest timestamp in the specified bucket
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
    try:
        # Convert ISO format timestamp to a string format compatible with SQLite
        dt_obj = datetime.fromisoformat(oldest_timestamp.replace('Z', ''))
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        logging.error(f"Error parsing timestamp: {e}")
        exit(1)

def build_sqlite_query(formatted_timestamp):
    # Build the SQLite query with an optional timestamp filter
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
    try:
        # Parse the shared attributes JSON
        return json.loads(shared_attrs)
    except (TypeError, json.JSONDecodeError) as e:
        logging.warning(f"Failed to parse attributes: {e}")
        return {}

def batch_insert_to_influx(write_api, rows):
    # Process rows in batches and write them to InfluxDB
    for row in rows:
        state, entity_id, last_updated_ts, shared_attrs = row
        if state in ["unknown", "unavailable"]:
            continue
        domain, _, entity_id_short = entity_id.partition('.')
        attributes_json = parse_attributes(shared_attrs)

        friendly_name = attributes_json.get('friendly_name', entity_id_short)
        unit_of_measurement = attributes_json.get('unit_of_measurement', 'default_measurement')

        try:
            # Create a point to write to InfluxDB
            last_updated_dt = datetime.fromtimestamp(float(last_updated_ts))
            point = Point(unit_of_measurement).tag("source", "HA").tag("domain", domain)
            point.tag("entity_id", entity_id_short).tag("friendly_name", friendly_name).time(last_updated_dt)

            # Add the state value as either a numerical value or a string
            if isinstance(state, (int, float)) or (isinstance(state, str) and state.replace('.', '', 1).isdigit()):
                point.field("value", float(state))
            else:
                point.field("state", str(state))

            # Add additional attributes as fields, ensuring correct type
            for key, value in attributes_json.items():
                if key in ["id", "id_str", "update_available"] or value is None:
                    continue
                try:
                    if key in ["temperature", "humidity"]:
                        point.field(key, float(value))
                    elif isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
                        point.field(key, float(value))
                    else:
                        point.field(f"{key}", str(value))
                except Exception as e:
                    logging.warning(f"Skipping field '{key}' for entity '{entity_id}' with value '{value}' due to type conflict: {e}")

            # Write the point to InfluxDB
            try:
                write_api.write(bucket=influx_bucket, org=influx_org, record=point)
                # logging.info(f"Successfully wrote point to InfluxDB: {point}")
            except Exception as e:
                logging.error(f"Error writing point to InfluxDB: {e}. Point: {point}")

        except ValueError as e:
            logging.warning(f"Error preparing InfluxDB point for entity {entity_id}: {e}")

def main():
    # Main execution flow
    conn, cursor = connect_to_sqlite(sqlite_db)
    write_api, query_api = connect_to_influxdb(influx_url, influx_token, influx_org)

    oldest_influx_timestamp = get_oldest_influx_timestamp(query_api)
    logging.info(f"Oldest InfluxDB timestamp: {oldest_influx_timestamp}")

    formatted_timestamp = format_timestamp(oldest_influx_timestamp) if oldest_influx_timestamp else None
    sqlite_query = build_sqlite_query(formatted_timestamp)
    logging.info(f"Final SQLite query: {sqlite_query}")

    try:
        # Execute the SQLite query and process rows in batches
        cursor.execute(sqlite_query)
        total_rows = cursor.fetchall()
        logging.info(f"Processing {len(total_rows)} rows.")
        rows_fetched = 0
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            batch_insert_to_influx(write_api, rows)
            rows_fetched += len(rows)
            if DEBUG_MODE:
                logging.info(f"Processed {rows_fetched} rows so far.")
    except sqlite3.Error as e:
        logging.error(f"SQLite query error: {e}")
    finally:
        cursor.close()
        conn.close()
        write_api.close()
        logging.info("Closed connections to SQLite and InfluxDB")

    logging.info("Data export complete.")

if __name__ == "__main__":
    main()
