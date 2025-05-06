CREATE OR REPLACE DATABASE time_travel;




// WHERE CAN YOU SEE THE RETENTION PERIOD

SHOW TABLES IN SCHEMA DB_ECOM.SC_ECOMM; -- Check for retention_time
SHOW TABLES IN DATABASE AWS_INTEGRATION;

SHOW TABLES LIKE '%json%' -- Check for retention_time

// How to get Meta data of a Table:

-- Way 1:

USE DATABASE MYDB;
USE SCHEMA PUBLIC;

SELECT *
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'CUSTOMER_CLUSTER'
  AND TABLE_SCHEMA = 'PUBLIC'
  AND TABLE_CATALOG = 'MYDB';

-- WAY 2:
SHOW TABLES like '%customer_cluster%';



// Table Creation 

Create or replace table sample_table
(
    a int,
    b varchar
)
CLUSTER BY (a)
DATA_RETENTION_TIME_IN_DAYS = 1; -- Note it can be any thing between 1 and 90 




// ALTERING

ALTER TABLE sample_table SET DATA_RETENTION_TIME_IN_DAYS = 8;



// QUERYING HISTORICAL DATA

-- first update some records and then delete some



SELECT * FROM PRACTISE.PUBLIC.SALES_DATA;

SELECT * FROM PRACTISE.PUBLIC.USER_PURCHASES; -- 

-- CASE 1: update some data in the table:

UPDATE PRACTISE.PUBLIC.USER_PURCHASES SET product = 'Samsung_Laptop'
WHERE user_ID = 101 and purchase_date = '2024-01-05'; -- updated.


-- Case 2: Delte some data from the table

DELETE FROM PRACTISE.PUBLIC.USER_PURCHASES WHERE user_id  = 104 and purchase_date = '2024-02-15' and product = 'Chair'; -- deleted 1 row

SELECT CURRENT_TIMESTAMP; -- 2025-05-05 14:56:46.037 -0700




// QUerying the details (history) 

SELECT * FROM PRACTISE.PUBLIC.USER_PURCHASES; -- 8 records user_id = 104 and purchase_date = '2024-02-15' missing


// Retrieve Data using before statement
SELECT * FROM PRACTISE.PUBLIC.USER_PURCHASES BEFORE(statement => '01bc2743-3201-97a5-000d-1a9a0002ea02') -- [resent


// Retrieve Data using Timestamp

-- if you see above case 1 there was an update.


SELECT * FROM PRACTISE.PUBLIC.USER_PURCHASES AT (timestamp => '2025-05-05 14:52:46.037 -0700'::timestamp) -- thus by this time update was not made.



// creation of table:


INSERT INTO PRACTISE.PUBLIC.USER_PURCHASES  
SELECT * FROM PRACTISE.PUBLIC.USER_PURCHASES BEFORE (STATEMENT => '01bc2743-3201-97a5-000d-1a9a0002ea02') WHERE user_id = 104
AND purchase_date = '2024-02-15' and product = 'Chair'



// Check

Select * from PRACTISE.PUBLIC.USER_PURCHASES;



// RESTORING TABLEs:

DROP TABLE PRACTISE.PUBLIC.USER_PURCHASES;
UNDROP TABLE PRACTISE.PUBLIC.USER_PURCHASES;


SELECT * FROM PRACTISE.PUBLIC.USER_PURCHASES; -- working


show schemas in database database_name;
drop schema schema_name;
undrop schema schema_name;
show schemas in database database_name;
show tables in schema schema_name;
drop table table_name;
undrop table_name; 


// Time Travel Cost:

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS; -- Look for Active_bytes (talks about teh memory)
-- if deleted and within retention time limit - will reflect in time_travel_bytes Active_bytes become Zero
-- if retention zone crosses and goes into Fail_safe Zone - will reflect in Failsafe_bytes.














