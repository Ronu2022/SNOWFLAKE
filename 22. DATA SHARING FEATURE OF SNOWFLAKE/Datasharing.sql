/* STEP 1 CREATION OF SHARE */

CREATE OR REPLACE SHARE share_name; 

/* STEP 2 ADDITION OF OBJECTS LIKE TABLES OR VIEWS */

GRANT USAGE ON DATABASE "REVENUE_HISTORY"  TO SHARE share_name;
GRANT USAGE ON SCHEMA "REVENUE_HISTORY"."RAW_SCHEMA" TO SHARE share_name;
GRANT SELECT ON TABLE "REVENUE_HISTORY"."RAW_SCHEMA"."REVENUE_HISTORY_DATES" TO SHARE share_name; 

/* STEP 3 ASSIGN THE SHARE TO A SPECIFIC ACCOUNT */

ALTER SHARE share_name ADD ACCOUNTS (consumer_account _locator);

/* STEP 4 CONSUMER ACCESING THE SHARE*/

CREATE OR REPLACE new_database FROM SHARE provide_account_locator.share_name; 

-- note: 
 -- the cost is null for the consumer, because he is only querying it, the data still is stored in our db.
 -- conumer can't join the shared views or tables, unless it is materialised or copied in the his db. 
 -- share can happen only for permanent db not temporary ones. 
