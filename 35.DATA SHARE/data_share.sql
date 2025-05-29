-- ==========================================================================================================================================================================================================================================
// Data Sharing to Other Snowflake Users 
-- ==========================================================================================================================================================================================================================================

// Create a Database
CREATE DATABASE CUST_DB;

// Create schemas
CREATE SCHEMA CUST_TBLS;
CREATE SCHEMA CUST_VIEWS;


// Create some tables in tbls schema
CREATE TABLE CUST_DB.CUST_TBLS.CUSTOMER
AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;


CREATE TABLE CUST_DB.CUST_TBLS.ORDERS
AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;



// Create a view in views schema

CREATE OR REPLACE VIEW CUST_VIEWS.VW_CUST
AS
SELECT CST.C_CUSTKEY, CST.C_NAME, CST.C_ADDRESS, CST.C_PHONE FROM 
SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER CST
INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.NATION NTN
ON CST.C_NATIONKEY = NTN.N_NATIONKEY
WHERE NTN.N_NAME='BRAZIL';


// Create a secure view in views schema
CREATE OR REPLACE SECURE VIEW CUST_VIEWS.SEC_VW_CUST
AS
SELECT CST.C_CUSTKEY, C_NAME,C_ADDRESS, C_PHONE 
FROM CUST_DB.CUST_TBLS.CUSTOMER CST;

// Create a  mat view in views schema
CREATE MATERIALIZED VIEW CUST_DB.CUST_VIEWS.MAT_VW_ORDERS
AS
SELECT * FROM CUST_DB.CUST_TBLS.CUSTOMER;


// Create a secure mat view in views schema
CREATE SECURE MATERIALIZED VIEW CUST_DB.CUST_VIEWS.SEC_MAT_VW_ORDERS
AS
SELECT * FROM CUST_DB.CUST_TBLS.CUSTOMER;


-- ===================================================================================================================================================================================================================================================================

// Create a share object
-- we can create and manage share objects in two ways
-- 1. By using sql queries 2. By using share tabs in UI

DROP SHARE CUST_DATA_SHARE;
CREATE OR REPLACE SHARE CUST_DATA_SHARE WITH SECURE_OBJECTS_ONLY = FALSE; -- because this would allow both secured and not secured views to be shared.
-- without it only secured views  can be shared. 
-- Not secured views upon sharing would show problems. 



// Grant access to share object

GRANT USAGE ON DATABASE CUST_DB TO SHARE CUST_DATA_SHARE;
GRANT USAGE ON SCHEMA CUST_DB.CUST_TBLS TO SHARE CUST_DATA_SHARE;
GRANT SELECT ON TABLE CUST_DB.CUST_TBLS.CUSTOMER TO SHARE CUST_DATA_SHARE;
GRANT SELECT ON TABLE CUST_DB.CUST_TBLS.ORDERS TO SHARE CUST_DATA_SHARE;


GRANT USAGE ON SCHEMA CUST_DB.CUST_VIEWS TO SHARE CUST_DATA_SHARE; 
GRANT SELECT ON VIEW CUST_DB.CUST_VIEWS.VW_CUST TO SHARE CUST_DATA_SHARE;  -- wont work because non secured views cant be share.
GRANT SELECT ON VIEW CUST_DB.CUST_VIEWS.SEC_VW_CUST TO SHARE CUST_DATA_SHARE; -- done successfully.
GRANT SELECT ON VIEW CUST_DB.CUST_VIEWS.MAT_VW_ORDERS TO SHARE CUST_DATA_SHARE;-- done

-- materialized views : have simply precomputed data, and desnt involve refefrences to other views, hence no logic is shared 
-- Not secured views : are mostly dynamic, everytime the data change, and come up with new rows 
-- for instance select * from table_a where country ilike 'a%' -- means star with a lets say as of now you have only australia.
-- tomorrow you might have a different country azerbaizan , so the end user will have access to all
-- but when you do materialized view it is just a subset of the main set which is already shared, for eg, select * from table where country ilike 'A%' is a subset of table. thus, has not much scope of security loss.

GRANT SELECT ON VIEW CUST_DB.CUST_VIEWS.SEC_MAT_VW_ORDERS TO SHARE CUST_DATA_SHARE; -- done. 

-- Remeber: sharing allowed : 
-- secured View, Materialized View
-- Table.

// How to see share objects

SHOW SHARES; -- look in my case 
/*
kind	name
INBOUND	ACCOUNT_USAGE -- provided 
INBOUND	SAMPLE_DATA -- provided by Snowflake
OUTBOUND	CUST_DATA_SHARE -- we created
*/ 

// How to see the grants of a share object

SHOW GRANTS TO SHARE CUST_DATA_SHARE; -- you will find privileges.

// ADDING the other ACCOUNT to the Share.

ALTER SHARE CUST_DATA_SHARE ADD ACCOUNT  account_number;

select current_account(); -- KQ59974
select current_region(); -- AWS_AP_SOUTHEAST_1


// Sharing all Tables in a Schema 
GRANT SELECT ON ALL TABLES IN SCHEMA CUST_DB.CUST_TBLS  TO SHARE CUST_DATA_SHARE;

// Sharing all Tables in a Database
GRANT SELECT ON ALL TABLES IN DATABASE CUST_DB TO SHARE CUST_DATA_SHARE;


--====================================================================================================================================================================================================================================================================
// Consumer side database setup
--====================================================================================================================================================================================================================================================================

SHOW SHARES;  -- the sharing should have been inbound (becasue from other)

DESC SHARE SHARE_NAME;

--  Create a database to consume the shared data

CREATE OR REPLACE DATABASE CUST_DB_SHARED FROM SHARE share_name_that_is_shared; 

-- Select:
SELECT * FROM CUST_DB_SHARED;





--====================================================================================================================================================================================================================================================================
-- Data Sharing to Non-Snowflake Users 
--====================================================================================================================================================================================================================================================================

CREATE MANAGED ACCOUNT dummy_managed_account
ADMIN_NAME = 'Ajaymohanty2024'
ADMIN_PASSWORD = 'Ajaymohanty@2024'
TYPE = READER; -- created

SHOW MANAGED ACCOUNTS;
-- DB15933 (account  Locator)
-- https://rpfiddy-dummy_managed_account.snowflakecomputing.com -- account_url
-- https://rpfiddy-ur24744.snowflakecomputing.com

SHOW SHARES;

ALTER SHARE CUST_DATA_SHARE ADD ACCOUNT = DB15933; -- adding account

select current_account();-- KQ59974; 


-- open the link to get into  the reader account:

SHOW SHARES; -- RPFIDDY.CT97193
-- share name -- CUST_DATA_SHARE

CREATE DATABASE shared_db_check FROM SHARE RPFIDDY.CT97193.CUST_DATA_SHARE;

-- then You can access the db objects.


