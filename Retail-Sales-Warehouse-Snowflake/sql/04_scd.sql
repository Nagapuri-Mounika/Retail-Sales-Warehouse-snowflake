desc table snowflake_sample_data.tpcds_sf100tcl.customer;

desc table snowflake_sample_data.tpcds_sf100tcl.customer_address;

use warehouse compute_wh;

use database snowflake_learning_db;

create or replace table silver.customer_dim(
    customer_sk number autoincrement,
    customer_id number,
    state string,
    start_date date,
    end_date date,
    is_current string
);

--initial load

insert into silver.customer_dim
(customer_id,state,start_date,end_date,is_current)
select 
    c.c_customer_sk,
    ca.ca_state,
    current_date,
    null,
    'Y'
from snowflake_sample_data.tpcds_sf100tcl.customer c
join snowflake_sample_data.tpcds_sf100tcl.customer_address ca
    on c.c_current_addr_sk=ca.ca_address_sk;

select count(*) from silver.customer_dim;

select *from silver.customer_dim limit 5;


--create a small stage

create or replace table silver.customer_stage as
select customer_id,state
from silver.customer_dim
where is_current='Y';

--manually update one row

update silver.customer_stage
set state= 'TX'
where customer_id=76852945;

select *from silver.customer_stage where customer_id=76852945;

merge into SILVER.CUSTOMER_DIM target
using silver.customer_stage source 
on target.customer_id=source.customer_id
and target.is_current='Y'
when matched and target.state <> source.state then
update set
      target.end_date=current_date,
      target.is_current='N'

when not matched then
insert(customer_id,state,start_date,end_date,is_current)
values(source.customer_id,source.state,current_date,null,'Y');

--VALIDATE SCD BEHAVIOUR

SELECT *
FROM SILVER.CUSTOMER_DIM
WHERE CUSTOMER_ID=53962927
ORDER BY START_DATE;

SELECT COUNT(DISTINCT customer_id)
FROM silver.customer_dim;

