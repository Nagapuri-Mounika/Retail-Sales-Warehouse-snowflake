use warehouse compute_wh;

use database snowflake_learning_db;

create schema if not exists gold;

create or replace TABLE gold.sales_by_state AS
select
    d.state,
    sum(f.sales_price) AS total_sales
from silver.fact_sales f
join silver.customer_dim d
    on f.customer_sk= d.customer_sk
group by d.state;
    
SELECT*FROM SILVER.CUSTOMER_DIM limit 1;

CREATE OR REPLACE TABLE GOLD.SALES_BY_YEAR AS
 SELECT 
   YEAR(transaction_date) AS sales_year,
   SUM(sales_price) AS total_sales
FROM SILVER.FACT_SALES
GROUP BY sales_year;

CREATE OR REPLACE TABLE GOLD.MONTHLY_SALES AS
SELECT 
   DATE_TRUNC('month',transaction_date) AS sales_month,
   SUM(sales_price) AS total_sales
FROM SILVER.FACT_SALES
GROUP BY sales_month;

--Top 10 stores by sales
CREATE OR REPLACE TABLE GOLD.TOP_sTORES AS
SELECT
  store_id,
  SUM(sales_price) AS total_sales
FROM SILVER.FACT_SALES
GROUP BY store_id
ORDER BY total_sales DESC
LIMIT 10;


--performance validation
SELECT COUNT(*) FROM GOLD.SALES_BY_STATE;

SELECT COUNT(*) FROM GOLD.SALES_BY_YEAR;
SELECT COUNT(*) FROM GOLD.MONTHLY_SALES;
SELECT COUNT(*) FROM GOLD.TOP_STORES;

SELECT * FROM SILVER.FACT_SALES LIMIT 5;

SELECT SUM(sales_price) FROM SILVER.FACT_SALES;

SELECT SUM(total_sales) FROM GOLD.SALES_BY_STATE;

SELECT SUM(total_sales) FROM GOLD.SALES_BY_YEAR;