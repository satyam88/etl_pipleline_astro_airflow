# Import the DAG class (represents a workflow in Airflow)
from airflow import DAG

# HttpHook is used to call REST APIs using Airflow Connections
from airflow.providers.http.hooks.http import HttpHook

# PostgresHook is used to connect to PostgreSQL using Airflow Connections
from airflow.providers.postgres.hooks.postgres import PostgresHook

# @task decorator allows us to define tasks using Python functions (TaskFlow API)
from airflow.decorators import task

# Utility function to set start_date relative to today
from datetime import datetime, timedelta


# -----------------------------
# Step 1: Define configuration variables
# -----------------------------

# Latitude and longitude for the desired location (London in this case)
LATITUDE = '32.239632'
LONGITUDE = '77.188713'

# Airflow Connection IDs (must be created in Airflow UI -> Admin -> Connections)
POSTGRES_CONN_ID = 'weather_db'   # For PostgreSQL database
API_CONN_ID = 'open_meteo_api'           # For Open-Meteo API

# Default arguments applied to the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1)
}

# -----------------------------
# Step 2: Define the DAG (Workflow)
# -----------------------------
# dag_id: Unique name of the DAG in Airflow UI
# schedule: How often the DAG runs (@daily = once per day)
# catchup=False: Do not run missed historical jobs
# -----------------------------

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    schedule='@daily',     # In Airflow 2.7+, use "schedule" instead of "schedule_interval"
    catchup=False,
    tags=["weather", "etl"]
) as dag:

    # -----------------------------
    # Step 3: Extract Task
    # -----------------------------
    @task()
    def extract_weather_data():
        """
        Extract weather data from Open-Meteo API using Airflow HTTP Connection.
        This is the 'E' (Extract) step of ETL.
        """

        # Create an HTTP Hook using the connection defined in Airflow
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        # Build the API endpoint
        # Example full URL:
        # https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        # Make the API call via the HTTP Hook
        response = http_hook.run(endpoint)

        # Check if the API call was successful
        if response.status_code == 200:
            # Return JSON response (this will be passed to the next task)
            return response.json()
        else:
            # If API fails, raise an exception so Airflow marks task as failed
            raise Exception(f"Failed to fetch weather data: {response.status_code}")


    # -----------------------------
    # Step 4: Transform Task
    # -----------------------------
    @task()
    def transform_weather_data(weather_data):
        """
        Transform the extracted weather data.
        This is the 'T' (Transform) step of ETL.
        """

        # Extract only the fields we care about
        current_weather = weather_data['current_weather']

        # Create a clean, structured dictionary
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }

        # Return transformed data to be used by the Load task
        return transformed_data


    # -----------------------------
    # Step 5: Load Task
    # -----------------------------
    @task()
    def load_weather_data(transformed_data):
        """
        Load transformed data into PostgreSQL.
        This is the 'L' (Load) step of ETL.
        """

        # Create a Postgres Hook using Airflow connection
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Get a database connection and cursor
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it does not exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        # Commit the transaction and close cursor
        conn.commit()
        cursor.close()


    # -----------------------------
    # Step 6: Define the ETL Workflow (Task Dependencies)
    # -----------------------------
    # Extract -> Transform -> Load
    # -----------------------------

    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
