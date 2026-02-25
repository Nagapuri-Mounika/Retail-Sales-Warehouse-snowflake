use warehouse compute_wh;

use database snowflake_learning_db;

create schema if not exists SILVER;

create or replace table silver.store_sales_clean as
select
      ss.ss_sold_date_sk,
      ss.ss_store_sk,
      ss.ss_customer_sk,
      ss.ss_quantity,
      ss.ss_sales_price
from bronze.store_sales_raw ss 
join snowflake_sample_data.tpcds_sf100tcl.date_dim d
     on ss.ss_sold_date_sk=d.d_date_sk
where d.d_year between 1998 and 1999;


CREATE OR REPLACE TABLE BRONZE.STORE_SALES_RAW AS
SELECT ss.*
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.STORE_SALES ss
JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM d
     ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_year BETWEEN 1998 AND 1999
LIMIT 50000000;

desc table silver.store_sales_clean;

select MIN(ss_sales_price), MAX(ss_sales_price)
from silver.store_sales_clean;


select count(*) from bronze.store_sales_raw;

select count(*) from silver.store_sales_clean;

select count(*) 
from silver.store_sales_clean
where ss_sales_price is null;

select count(*) 
from silver.store_sales_clean
where ss_quantity<=0;

