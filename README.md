# etl_pipleline_astro_airflow


USE ROLE SYSADMIN;

-- 1. Create a warehouse for compute
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- 2. Create database for Airflow
CREATE DATABASE IF NOT EXISTS AIRFLOW_DB;

-- 3. Use the database
USE DATABASE AIRFLOW_DB;

-- 4. Create schema
CREATE SCHEMA IF NOT EXISTS PUBLIC;

-- 5. (Optional) Verify objects
SHOW WAREHOUSES;
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE AIRFLOW_DB;

SELECT 
  CURRENT_ACCOUNT()            AS account_locator,
  CURRENT_ORGANIZATION_NAME()  AS org_name,
  CURRENT_ACCOUNT_NAME()       AS account_name,
  CURRENT_REGION()             AS region,
  SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO() AS platform_info;


SELECT *
FROM AIRFLOW_DB.PUBLIC.apod_data
ORDER BY created_at DESC;