USE DATABASE SNOWFLAKE_SAMPLE_DATA;

DESC TABLE TPCDS_SF100TCL.STORE_SALES;

SELECT COUNT(*) 
FROM TPCDS_SF100TCL.STORE_SALES;

USE WAREHOUSE COMPUTE_WH;

USE DATABASE SNOWFLAKE_LEARNING_DB;

CREATE SCHEMA IF NOT EXISTS BRONZE;

CREATE OR REPLACE TABLE BRONZE.STORE_SALES_RAW AS
SELECT*
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.STORE_SALES
LIMIT 50000000;

select count(*) from bronze.STORE_SALES_RAW;

select count(*)
from snowflake_sample_data.tpcds_sf100tcl.store_sales;

desc table bronze.STORE_SALES_RAW;

select 
  count(*) as total_rows,
  count(ss_sales_price) as non_null_sales_price
from bronze.STORE_SALES_RAW;

select min(ss_sold_date_sk), max(ss_sold_date_sk)
from bronze.STORE_SALES_RAW;