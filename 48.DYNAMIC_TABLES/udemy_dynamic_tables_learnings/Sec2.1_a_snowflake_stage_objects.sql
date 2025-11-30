CREATE OR REPLACE DATABASE DEMO_DB;

-- CHANGE CONTEXT: 
use role sysadmin;
create or replace database demo_db; 
use schema demo_db.public;
use warehouse compute_wh;

-- SNOWFLAKE OFFERS 3 TYPES OF STAGE OBJECTS:
    -- exteral stage object
    -- internal stage objects
    -- table stage objects
    -- user stage objects

-- CREATING AN EXTERNAL STAGE: 

create or replace stage my_ext_stage
url = 's3://snowflake-workshop-lab/weather-nyc'
comment = 'an external stage with aws/s3 object storage';


-- CREATING AN INTERNAL STAGE:
create or replace stage my_int_stage
comment = 'an internal stage';

-- DESCRIBE STAGE OBJECT:
desc stage my_ext_stage;
desc stage my_int_stage;

--- LIST STAGES:
show stages;

-- LIST ALL FILES INSIDE A STAGE: 

list @my_ext_stage;


-- TABLE STAGE:
create or replace transient table demo_db.public.customer (
    cust_key number,
    name text,
    address text,
    nation_name text,
    phone text,
    acct_bal number,
    mkt_segment text
);

list @%customer; -- No records yest

// Ingested one file of EXCEL:

list @%customer; 

// NAMED USER STAGE: 
/*

used snowsql
-----------------------------------------------+
ajaymohanty#COMPUTE_WH@DEMO_DB.PUBLIC>put file:///C:\Users\Ronu\Downloads\customer-india-100-rows.csv @~/csv/custome;
                                      r/india/;
+-----------------------------+--------------------------------+-------------+-------------+--------------------+--------------------+----------+---------+
| source                      | target                         | source_size | target_size | source_compression | target_compression | status   | message |
|-----------------------------+--------------------------------+-------------+-------------+--------------------+--------------------+----------+---------|
| customer-india-100-rows.csv | customer-india-100-rows.csv.gz |        9126 |        4432 | NONE               | GZIP               | UPLOADED |         |
+-----------------------------+--------------------------------+-------------+-------------+--------------------+--------------------+----------+---------+
1 Row(s) produced. Time Elapsed: 1.063s
ajaymohanty#COMPUTE_WH@DEMO_DB.PUBLIC>list @~/csv/customer/india;
+---------------------------------------------------+------+----------------------------------+-------------------------------+
| name                                              | size | md5                              | last_modified                 |
|---------------------------------------------------+------+----------------------------------+-------------------------------|
| csv/customer/india/customer-india-100-rows.csv.gz | 4432 | d05512b65b2320f53072dbbe5614e460 | Sun, 30 Nov 2025 11:46:45 GMT |
+---------------------------------------------------+------+----------------------------------+-------------------------------+
1 Row(s) produced. Time Elapsed: 0.184s

*/

list @~/csv/custome;

/* name	                                            size	                    md5	               last_modified
csv/customer/india/customer-india-100-rows.csv.gz	4432	d05512b65b2320f53072dbbe5614e460	Sun, 30 Nov 2025 11:46:45 GMT

*/ -- if you dont mention, files are generally  compressed hence this.gz

/*
ajaymohanty#COMPUTE_WH@DEMO_DB.PUBLIC>put file:///C:\Users\Ronu\Downloads\customer-india-100-rows.csv @%customer/cus
                                      csv/customer/india;
+-----------------------------+--------------------------------+-------------+-------------+--------------------+--------------------+----------+---------+
| source                      | target                         | source_size | target_size | source_compression | target_compression | status   | message |
|-----------------------------+--------------------------------+-------------+-------------+--------------------+--------------------+----------+---------|
| customer-india-100-rows.csv | customer-india-100-rows.csv.gz |        9126 |        4432 | NONE               | GZIP               | UPLOADED |         |
+-----------------------------+--------------------------------+-------------+-------------+--------------------+--------------------+----------+---------+
1 Row(s) produced. Time Elapsed: 1.090s

Also Ingested into the table stage @%customer/cus csv/customer/india; here the first customer is teh name of teh table, then followed by further folders or partitions.
*/

list @%customer;

/*name	                                                 size	         md5	                                last_modified
cuscsv/customer/india/customer-india-100-rows.csv.gz	4432	c1c099b714015a32a4f70687b4aa221b	Sun, 30 Nov 2025 11:53:31 GMT */ 



