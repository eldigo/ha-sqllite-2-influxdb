# Home Assistant SQLite to InfluxDB Script

This script transfers historical data from a Home Assistant SQLite database to InfluxDB.
It retrieves the earliest records from the InfluxDB bucket and extracts the states, attributes, and friendly names from the Home Assistant database for records prior to that.
Created using ChatGPT and tested with Home Assistant Core 2024.10.1 and InfluxDB v2.7.10.
Follow the steps below to set up the environment and run the script.

## Prerequisites

- Python 3.6 or higher
- A SQLite database file you wish to import data from
- An InfluxDB instance running and accessible

## Installation

### Step 1: Clone the Repository

Clone the repository or download the script files to your local machine.

```bash
git clone https://github.com/eldigo/ha-sqllite-2-influxdb
cd ha-sqllite-2-influxdb
```

### Step 2: Create a Virtual Environment

Create a Python virtual environment to isolate the project dependencies.

```bash
python3 -m venv myenv
```

### Step 3: Activate the Virtual Environment

Activate the virtual environment:

```bash
source myenv/bin/activate
```

### Step 4: Install Requirements

Install the required packages using the `requirements.txt` file provided.

```bash
pip install -r requirements.txt
```

### Step 5: Configure Environment Variables

Copy the `.env.example` file to a new file named `.env` and fill in the required values. You can use the following command:

```bash
cp .env.example .env
```

Open the `.env` file in a text editor and provide the necessary configurations for your InfluxDB connection.

```plaintext
INFLUXDB_URL=http://localhost:8086
INFLUXDB_TOKEN=your_token
INFLUXDB_ORG=your_organization
INFLUXDB_BUCKET=your_bucket
SQLITE_DB_PATH=/path/to/your/sqlite.db
DEBUG_MODE=false
```

When DEBUG_MODE is true. The script will insert into Influx point by point

## Usage

Run the script using the following command:

```bash
python3 sqllite2influxdb.py
```

Make sure that your SQLite database file is correctly specified in the `.env` file, and that your InfluxDB instance is running and accessible.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
