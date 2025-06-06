CREATE OR REPLACE DATABASE mydb;

//Create a table with no cluster keys
CREATE OR REPLACE TABLE PUBLIC.CUSTOMER_NONCLUSTER 
(
	C_CUSTKEY NUMBER(38,0),
	C_NAME VARCHAR(25),
	C_ADDRESS VARCHAR(40),
	C_NATIONKEY NUMBER(38,0),
	C_PHONE VARCHAR(15),
	C_ACCTBAL NUMBER(12,2),
	C_MKTSEGMENT VARCHAR(10),
	C_COMMENT VARCHAR(117)
);

// Insert data into above non-clustered table
INSERT INTO PUBLIC.CUSTOMER_NONCLUSTER
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;



//Create a table with cluster key
CREATE OR REPLACE TABLE PUBLIC.CUSTOMER_CLUSTER
(
	C_CUSTKEY NUMBER(38,0),
	C_NAME VARCHAR(25),
	C_ADDRESS VARCHAR(40),
	C_NATIONKEY NUMBER(38,0),
	C_PHONE VARCHAR(15),
	C_ACCTBAL NUMBER(12,2),
	C_MKTSEGMENT VARCHAR(10),
	C_COMMENT VARCHAR(117)
	
) CLUSTER  BY (C_NATIONKEY);


// Insert data into above clustered table
INSERT INTO PUBLIC.CUSTOMER_CLUSTER
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;



// Observe time taken and no.of partitions scanned
SELECT * FROM PUBLIC.CUSTOMER_NONCLUSTER WHERE C_NATIONKEY=2; -- 13 secs
SELECT * FROM PUBLIC.CUSTOMER_CLUSTER WHERE C_NATIONKEY=2; -- 6.6 seconds



---------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE PUBLIC.ORDERS_NONCLUSTER
AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS;-- Created the table directly without create table statement.


CREATE OR REPLACE TABLE PUBLIC.ORDERS_CLUSTER
AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS;


// Add Cluster key to the table

ALTER TABLE  PUBLIC.ORDERS_CLUSTER CLUSTER BY (YEAR (O_ORDERDATE));


// Observe time taken and no.of partitions scanned

SELECT * FROM PUBLIC.ORDERS_NONCLUSTER WHERE YEAR(O_ORDERDATE) = 1995; -- 2min 20 secs, partition scanned 2426
SELECT * FROM PUBLIC.ORDERS_CLUSTER WHERE YEAR(O_ORDERDATE) = 1995; --  2minsa 23 secs Partition scanned 373


// Alter Table to add multiple cluster keys

ALTER TABLE PUBLIC.ORDERS_CLUSTER CLUSTER BY (YEAR(O_ORDERDATE),O_ORDERPRIORITY);


// Observe time taken and no.of partitions scanned

SELECT * FROM  PUBLIC.ORDERS_NONCLUSTER WHERE YEAR(O_ORDERDATE) = 1996 AND  O_ORDERPRIORITY = '1-URGENT'; -- 36 seconds 1019 partitions scanned;
SELECT * FROM PUBLIC.ORDERS_CLUSTER WHERE YEAR(O_ORDERDATE) = 1996 and O_ORDERPRIORITY = '1-URGENT'; -- 30 seconds 85 Partitions scanned.


// To Turn-off results cache

ALTER SESSION SET USE_CACHED_RESULT = FALSE;


//To look at clustering information

SELECT SYSTEM$CLUSTERING_INFORMATION('ORDERS_CLUSTER') -- ORDERS_CLUSTER is the clustered table name
























