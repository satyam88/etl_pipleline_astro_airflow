from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import json


## Define the DAG
with DAG(
    dag_id='nasa_apod_snowflake',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:
    
    ## Step 1: Create the table if it doesn't exist in Snowflake
    @task
    def create_table():
        snowflake_hook = SnowflakeHook(snowflake_conn_id="my_snowflake_connection")
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS AIRFLOW_DB.PUBLIC.apod_data (
            id INT AUTOINCREMENT PRIMARY KEY,
            title VARCHAR(255),
            explanation STRING,
            url STRING,
            date DATE,
            media_type VARCHAR(50),
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        
        snowflake_hook.run(create_table_query)


    ## Step 2: Extract the NASA API Data (APOD)
    extract_apod = HttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',
        endpoint='planetary/apod?api_key={{ conn.nasa_api.extra_dejson.api_key }}',
        method='GET',
        response_filter=lambda response: response.json(),
    )

    
    ## Step 3: Transform the data
    @task
    def transform_apod_data(response):
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data


    ## Step 4: Load the data into Snowflake
    @task
    def load_data_to_snowflake(apod_data):
        snowflake_hook = SnowflakeHook(snowflake_conn_id='my_snowflake_connection')
        
        insert_query = """
        INSERT INTO AIRFLOW_DB.PUBLIC.apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        
        snowflake_hook.run(
            insert_query,
            parameters=(
                apod_data['title'],
                apod_data['explanation'],
                apod_data['url'],
                apod_data['date'],
                apod_data['media_type']
            )
        )


    ## Step 5: Define the task dependencies
    table_created = create_table()
    api_response = extract_apod.output
    transformed_data = transform_apod_data(api_response)
    load_task = load_data_to_snowflake(transformed_data)

    table_created >> extract_apod
