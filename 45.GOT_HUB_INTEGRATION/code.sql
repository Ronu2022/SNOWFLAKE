-- Create a dedicated database and schema for this exercise
CREATE DATABASE IF NOT EXISTS dev_payments;
CREATE SCHEMA  IF NOT EXISTS dev_payments.raw;
CREATE SCHEMA  IF NOT EXISTS dev_payments.mart;

SET DB_NAME = 'dev_payments';
SET sh_stage = 'STAGING';
SET Sh_raw = 'raw';
SET sh_mart = 'mart';




USE DATABASE IDENTIFIER($DB_NAME);
USE SCHEMA IDENTIFIER($Sh_raw);

// storage integration:

CREATE OR REPLACE STORAGE INTEGRATION s3_int_mindtree
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::481665128591:role/alerts_sf_project_role'
STORAGE_ALLOWED_LOCATIONS = 
(
  's3://mindtreedev/customers/',
  's3://mindtreepreprod/customers/',
  's3://mindtreeprod/customers/',
  's3://mindtreedev/payments/'
  
)
COMMENT = 'This is s3_int for all environments';


// CREATE FILE FORMAT:

CREATE OR REPLACE FILE FORMAT dev_payments.raw.ff
TYPE  = 'CSV'
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY ='"'
EMPTY_FIELD_AS_NULL = TRUE
DATE_FORMAT = 'YYYY-MM-DD'
SKIP_HEADER = 1;





// CREATION OF STAGE:

CREATE OR REPLACE STAGE dev_payments.raw.stg_raw
URL = 's3://mindtreedev/payments/'
STORAGE_INTEGRATION = s3_int_mindtree
FILE_FORMAT  = dev_payments.raw.ff;


// LIST STAGE:

LIST @dev_payments.raw.stg_raw; -- no records for the time being.


// CREATION OF TABLE SCHEMA:


CREATE OR REPLACE TABLE dev_payments.raw.payments_raw
(
    payment_id   STRING,
    user_id      STRING,
    amount       NUMBER(10,2),
    currency     STRING,
    payment_ts   TIMESTAMP_NTZ,
    file_name VARCHAR,
    file_row_number INT,
    INGESTION_TS TIMESTAMP_NTZ 
    --DEFAULT current_timestamp
    
);

// STREAM CREATION 

CREATE OR REPLACE STREAM dev_payments.raw.PAYMENT_RAW_STREAM
ON TABLE dev_payments.raw.payments_raw;

// PIPE CREATION:

CREATE OR REPLACE PIPE s3_pipe
AUTO_INGEST = TRUE
AS
COPY INTO dev_payments.raw.payments_raw
FROM 
(
    SELECT $1,$2,$3,$4,$5,METADATA$FILENAME,METADATA$FILE_ROW_NUMBER,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    FROM @dev_payments.raw.stg_raw
)
FILE_FORMAT = dev_payments.raw.ff
PATTERN = '.*payments.*';


DESC PIPE s3_pipe;
-- arn:aws:sqs:ap-south-1:555111849042:sf-snowpipe-AIDAYCPZ6TRJCCYNM6EIW-s1HvU6yhiOt2VUbXT6BA3g
-- arn:aws:sqs:ap-south-1:555111849042:sf-snowpipe-AIDAYCPZ6TRJCCYNM6EIW-s1HvU6yhiOt2VUbXT6BA3g
-- arn:aws:sqs:ap-south-1:555111849042:sf-snowpipe-AIDAYCPZ6TRJCCYNM6EIW-s1HvU6yhiOt2VUbXT6BA3g
-- arn:aws:sqs:ap-south-1:555111849042:sf-snowpipe-AIDAYCPZ6TRJCCYNM6EIW-s1HvU6yhiOt2VUbXT6BA3g


list @dev_payments.raw.stg_raw; -- 0


select * from dev_payments.raw.payments_raw; -- no records

/* Inserting Payements File */

list @dev_payments.raw.stg_raw;
/* name	size	md5	last_modified
s3://mindtreedev/payments/payments.csv	259	8ead634701349e404ac19dfaec26214a	Mon, 15 Sep 2025 18:05:39 GMT */

select * from dev_payments.raw.payments_raw;
/* 
PAYMENT_ID	USER_ID	AMOUNT	CURRENCY	PAYMENT_TS	FILE_NAME	FILE_ROW_NUMBER	INGESTION_TS
P001	U1001	250.50	USD	2025-09-15 10:15:00.000	payments/payments.csv	1	2025-09-15 11:06:03.847
P002	U1002	99.99	EUR	2025-09-15 11:30:00.000	payments/payments.csv	2	2025-09-15 11:06:03.847
P003	U1003	500.00	INR	2025-09-15 12:45:00.000	payments/payments.csv	3	2025-09-15 11:06:03.847
P004	U1004	75.25	USD	2025-09-15 13:00:00.000	payments/payments.csv	4	2025-09-15 11:06:03.847
P005	U1005	1200.00	GBP	2025-09-15 14:10:00.000	payments/payments.csv	5	2025-09-15 11:06:03.847
*/ --> ALL OKAY!!



/* after inserting Payements 2 file with updates  */ 

/* payment_id,user_id,amount,currency,payment_ts
P001,U2001,290.50,USD,2025-09-15 10:15:00*/ -- updated record

list @dev_payments.raw.stg_raw;

/* name	size	md5	last_modified
s3://mindtreedev/payments/payments.csv	259	8ead634701349e404ac19dfaec26214a	Mon, 15 Sep 2025 18:05:39 GMT
s3://mindtreedev/payments/payments_2.csv	88	cdd7f5813dcbe50d38860e0dab198cb9	Mon, 15 Sep 2025 18:09:12 GMT
*/

Select * from dev_payments.raw.payments_raw;
-- 2 records for P001 => 0kay-> let's observe in staging table

/*
PAYMENT_ID	USER_ID	AMOUNT	CURRENCY	PAYMENT_TS	FILE_NAME	FILE_ROW_NUMBER	INGESTION_TS
P001	U2001	290.50	USD	2025-09-15 10:15:00.000	payments/payments_2.csv	1	2025-09-15 11:06:03.847
P001	U1001	250.50	USD	2025-09-15 10:15:00.000	payments/payments.csv	1	2025-09-15 11:06:03.847
P002	U1002	99.99	EUR	2025-09-15 11:30:00.000	payments/payments.csv	2	2025-09-15 11:06:03.847
P003	U1003	500.00	INR	2025-09-15 12:45:00.000	payments/payments.csv	3	2025-09-15 11:06:03.847
P004	U1004	75.25	USD	2025-09-15 13:00:00.000	payments/payments.csv	4	2025-09-15 11:06:03.847
P005	U1005	1200.00	GBP	2025-09-15 14:10:00.000	payments/payments.csv	5	2025-09-15 11:06:03.847
*/

So we have 2 records for p001 one to be depricated
there other fresh

*/

-- Ingested on emore file that is Payments_3 :

list @dev_payments.raw.stg_raw;

/* name	size	md5	last_modified
s3://mindtreedev/payments/payments.csv	259	8ead634701349e404ac19dfaec26214a	Mon, 15 Sep 2025 15:00:51 GMT
s3://mindtreedev/payments/payments_2.csv	88	cdd7f5813dcbe50d38860e0dab198cb9	Mon, 15 Sep 2025 15:02:17 GMT
s3://mindtreedev/payments/payments_3.csv	259	b1e482375cb6bfd1faab42529239c314	Mon, 15 Sep 2025 17:48:21 GMT
*/ 


select * from dev_payments.raw.payments_raw;

/*


PAYMENT_ID	USER_ID	AMOUNT	CURRENCY	PAYMENT_TS	FILE_NAME	FILE_ROW_NUMBER	INGESTION_TS
P001	U2001	290.50	USD	2025-09-15 10:15:00.000	payments/payments_2.csv	1	2025-09-15 08:01:11.305
P001	U1001	270.50	USD	2025-09-15 10:15:00.000	payments/payments_3.csv	1	2025-09-15 10:48:36.152
P001	U1001	250.50	USD	2025-09-15 10:15:00.000	payments/payments.csv	1	2025-09-15 08:01:11.305 -- changed Value
P002	U1002	99.99	EUR	2025-09-15 11:30:00.000	payments/payments_3.csv	2	2025-09-15 10:48:36.152 -- -- changed Value
P002	U1002	99.99	EUR	2025-09-15 11:30:00.000	payments/payments.csv	2	2025-09-15 08:01:11.305
P003	U1003	510.00	INR	2025-09-15 12:45:00.000	payments/payments_3.csv	3	2025-09-15 10:48:36.152 -- Changed Value
P003	U1003	500.00	INR	2025-09-15 12:45:00.000	payments/payments.csv	3	2025-09-15 08:01:11.305
P004	U1004	75.25	USD	2025-09-15 13:00:00.000	payments/payments.csv	4	2025-09-15 08:01:11.305
P005	U1005	1200.00	GBP	2025-09-15 14:10:00.000	payments/payments.csv	5	2025-09-15 08:01:11.305
P006	U1006	150.00	USD	2025-09-15 15:20:00.000	payments/payments_3.csv	4	2025-09-15 10:48:36.152 -- New Value
P007	U1007	200.00	CAD	2025-09-15 16:00:00.000	payments/payments_3.csv	5	2025-09-15 10:48:36.152 -- New Value
*/
-- Will check in the staging layer how things go 


---------------------------------------------------------------------------------------------------------------------------------------------
// STAGING LAYER:
--------------------------------------------------------------------------------------------------------------------------------------------

USE SCHEMA DEV_PAYMENTS.STAGING;
CREATE SCHEMA IF NOT EXISTS DEV_PAYMENTS.STAGING;


CREATE OR REPLACE TABLE DEV_PAYMENTS.STAGING.PAYMENTS_STAGING
(
    payment_id   STRING,
    user_id      STRING,
    amount       NUMBER(10,2),
    currency     STRING,
    payment_ts   TIMESTAMP_NTZ,
    file_name VARCHAR,
    file_row_number INT,

    EFFECTIVE_START_DATE TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP),
    EFFECTIVE_END_DATE TIMESTAMP_NTZ DEFAULT NULL,
    IS_CURRENT BOOLEAN DEFAULT TRUE,

    
    INGESTION_TS TIMESTAMP_NTZ  DEFAULT TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    
);


SELECT * FROM dev_payments.raw.PAYMENT_RAW_STREAM;


SELECT * FROM DEV_PAYMENTS.STAGING.PAYMENTS_STAGING;

// CREATE PROCEDURE:

CREATE OR REPLACE PROCEDURE  DEV_PAYMENTS.STAGING.PROC_PAYMENTS_STAGING()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN

    CREATE OR REPLACE TEMPORARY TABLE DEV_PAYMENTS.STAGING.TEMP_PAYMENTS_STAGING_STREAM
    AS
    SELECT * FROM dev_payments.raw.PAYMENT_RAW_STREAM;

   UPDATE DEV_PAYMENTS.STAGING.PAYMENTS_STAGING  T
   SET T.IS_CURRENT = FALSE, 
       T.EFFECTIVE_END_DATE = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
   FROM DEV_PAYMENTS.STAGING.TEMP_PAYMENTS_STAGING_STREAM  S 
   WHERE 
    T.payment_id = S.payment_id
    AND T.IS_CURRENT = TRUE
    AND
    (
        T.user_id <> S.user_id
        or T.amount <> S.amount
        or T.currency <> S.currency
        or T.payment_ts <> s.payment_ts
       
    );

    INSERT INTO DEV_PAYMENTS.STAGING.PAYMENTS_STAGING (payment_id,user_id,amount,currency,payment_ts,file_name,file_row_number,
     IS_CURRENT, INGESTION_TS
    )
    SELECT 
        PAYMENT_ID,USER_ID,AMOUNT,CURRENCY,PAYMENT_TS,FILE_NAME,FILE_ROW_NUMBER, TRUE, TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
        FROM DEV_PAYMENTS.STAGING.TEMP_PAYMENTS_STAGING_STREAM as S
        WHERE NOT EXISTS
        (
            SELECT 1 FROM DEV_PAYMENTS.STAGING.PAYMENTS_STAGING as T
            WHERE T.PAYMENT_ID = S.PAYMENT_ID
            AND T.USER_ID = S.USER_ID
            AND T.AMOUNT = S.AMOUNT
            AND T.CURRENCY = S.CURRENCY
            AND T.PAYMENT_TS  = S.PAYMENT_TS
        );

    Return 'SUccess';
    END;
    $$;
    

    
   
// TASK:

CREATE OR REPLACE TASK DEV_PAYMENTS.STAGING.TASK_RUN
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON * * * * * UTC'
AS
CALL DEV_PAYMENTS.STAGING.PROC_PAYMENTS_STAGING();

ALTER TASK DEV_PAYMENTS.STAGING.TASK_RUN SUSPEND; 
ALTER TASK DEV_PAYMENTS.STAGING.TASK_RUN RESUME;  




 
SELECT * FROM DEV_PAYMENTS.STAGING.PAYMENTS_STAGING;
TRUNCATE TABLE DEV_PAYMENTS.STAGING.PAYMENTS_STAGING;
/*
o/p- just 1st Payment File 

PAYMENT_ID	USER_ID	AMOUNT	CURRENCY	PAYMENT_TS	FILE_NAME	FILE_ROW_NUMBER	EFFECTIVE_START_DATE	EFFECTIVE_END_DATE	IS_CURRENT	INGESTION_TS
P001	U1001	250.50	USD	2025-09-15 10:15:00.000	payments/payments.csv	1	2025-09-15 11:07:25.034		TRUE	2025-09-15 11:07:25.034
P002	U1002	99.99	EUR	2025-09-15 11:30:00.000	payments/payments.csv	2	2025-09-15 11:07:25.034		TRUE	2025-09-15 11:07:25.034
P003	U1003	500.00	INR	2025-09-15 12:45:00.000	payments/payments.csv	3	2025-09-15 11:07:25.034		TRUE	2025-09-15 11:07:25.034
P004	U1004	75.25	USD	2025-09-15 13:00:00.000	payments/payments.csv	4	2025-09-15 11:07:25.034		TRUE	2025-09-15 11:07:25.034
P005	U1005	1200.00	GBP	2025-09-15 14:10:00.000	payments/payments.csv	5	2025-09-15 11:07:25.034		TRUE	2025-09-15 11:07:25.034
*/

-- 2nd File payments_2 inserted 

-- o/p in raw was:
/*
PAYMENT_ID	USER_ID	AMOUNT	CURRENCY	PAYMENT_TS	FILE_NAME	FILE_ROW_NUMBER	INGESTION_TS
P001	U2001	290.50	USD	2025-09-15 10:15:00.000	payments/payments_2.csv	1	2025-09-15 11:06:03.847
P001	U1001	250.50	USD	2025-09-15 10:15:00.000	payments/payments.csv	1	2025-09-15 11:06:03.847
P002	U1002	99.99	EUR	2025-09-15 11:30:00.000	payments/payments.csv	2	2025-09-15 11:06:03.847
P003	U1003	500.00	INR	2025-09-15 12:45:00.000	payments/payments.csv	3	2025-09-15 11:06:03.847
P004	U1004	75.25	USD	2025-09-15 13:00:00.000	payments/payments.csv	4	2025-09-15 11:06:03.847
P005	U1005	1200.00	GBP	2025-09-15 14:10:00.000	payments/payments.csv	5	2025-09-15 11:06:03.847 */ 

SELECT * FROM DEV_PAYMENTS.STAGING.PAYMENTS_STAGING;
-- With first file Payments uploaded records populated in the Staging Table
-- Now, Uploaded another file named Payments2 to see how things are operating
-- became successfull.
-- 2 records for P001 one old depricated, the other New.
-- So working fine.

/* PAYMENT_ID	USER_ID	AMOUNT	CURRENCY	PAYMENT_TS	FILE_NAME	FILE_ROW_NUMBER	EFFECTIVE_START_DATE	EFFECTIVE_END_DATE	IS_CURRENT	INGESTION_TS
P001	U1001	250.50	USD	2025-09-15 10:15:00.000	payments/payments.csv	1	2025-09-15 11:07:25.034	2025-09-15 11:19:35.892	FALSE	2025-09-15 11:07:25.034
P001	U2001	290.50	USD	2025-09-15 10:15:00.000	payments/payments_2.csv	1	2025-09-15 11:19:36.336		TRUE	2025-09-15 11:19:36.336
P002	U1002	99.99	EUR	2025-09-15 11:30:00.000	payments/payments.csv	2	2025-09-15 11:07:25.034		TRUE	2025-09-15 11:07:25.034
P003	U1003	500.00	INR	2025-09-15 12:45:00.000	payments/payments.csv	3	2025-09-15 11:07:25.034		TRUE	2025-09-15 11:07:25.034
P004	U1004	75.25	USD	2025-09-15 13:00:00.000	payments/payments.csv	4	2025-09-15 11:07:25.034		TRUE	2025-09-15 11:07:25.034
P005	U1005	1200.00	GBP	2025-09-15 14:10:00.000	payments/payments.csv	5	2025-09-15 11:07:25.034		TRUE	2025-09-15 11:07:25.034
*/







-- WHEN NEXT FILE PAYMENT_3 UPDATED:
/*
payment_id,user_id,amount,currency,payment_ts
P001,U1001,270.50,USD,2025-09-15 10:15:00   -- UPDATED AMOUNT for P001
P002,U1002,99.99,EUR,2025-09-15 11:30:00    -- SAME as before
P003,U1003,510.00,INR,2025-09-15 12:45:00   -- UPDATED AMOUNT for P003
P006,U1006,150.00,USD,2025-09-15 15:20:00   -- NEW RECORD
P007,U1007,200.00,CAD,2025-09-15 16:00:00   -- NEW RECORD
*/

-- Let's upload teh file in the bucket and then see the records



-- Fil3 3 was inserted 
-- this was the records in raw schema :

/*


PAYMENT_ID	USER_ID	AMOUNT	CURRENCY	PAYMENT_TS	FILE_NAME	FILE_ROW_NUMBER	INGESTION_TS
P001	U2001	290.50	USD	2025-09-15 10:15:00.000	payments/payments_2.csv	1	2025-09-15 08:01:11.305
P001	U1001	270.50	USD	2025-09-15 10:15:00.000	payments/payments_3.csv	1	2025-09-15 10:48:36.152
P001	U1001	250.50	USD	2025-09-15 10:15:00.000	payments/payments.csv	1	2025-09-15 08:01:11.305 -- changed Value
P002	U1002	99.99	EUR	2025-09-15 11:30:00.000	payments/payments_3.csv	2	2025-09-15 10:48:36.152 -- -- changed Value
P002	U1002	99.99	EUR	2025-09-15 11:30:00.000	payments/payments.csv	2	2025-09-15 08:01:11.305
P003	U1003	510.00	INR	2025-09-15 12:45:00.000	payments/payments_3.csv	3	2025-09-15 10:48:36.152 -- Changed Value
P003	U1003	500.00	INR	2025-09-15 12:45:00.000	payments/payments.csv	3	2025-09-15 08:01:11.305
P004	U1004	75.25	USD	2025-09-15 13:00:00.000	payments/payments.csv	4	2025-09-15 08:01:11.305
P005	U1005	1200.00	GBP	2025-09-15 14:10:00.000	payments/payments.csv	5	2025-09-15 08:01:11.305
P006	U1006	150.00	USD	2025-09-15 15:20:00.000	payments/payments_3.csv	4	2025-09-15 10:48:36.152 -- New Value
P007	U1007	200.00	CAD	2025-09-15 16:00:00.000	payments/payments_3.csv	5	2025-09-15 10:48:36.152 -- New Value
*/


-- let's check

SELECT * FROM DEV_PAYMENTS.STAGING.PAYMENTS_STAGING;






/* Transformation */

SHOW PARAMETERS LIKE 'Timezone';

CREATE OR REPLACE VIEW DEV_PAYMENTS.STAGING.PAYMENTS_STAGING_CLEAN
AS
SELECT 
    PAYMENT_ID,
    USER_ID,
    COALESCE(AMOUNT,0) AS AMOUNT,
    UPPER(CURRENCY) AS CURRENCY,
    CONVERT_TIMEZONE('America/Los_Angeles','Asia/Kolkata',PAYMENT_TS) as PAYMENT_TS_INDIA,
    FILE_NAME,
    FILE_ROW_NUMBER,
    CONVERT_TIMEZONE('America/Los_Angeles','Asia/Kolkata',EFFECTIVE_START_DATE) as EFFECTIVE_START_DATE_INDIA,
    CONVERT_TIMEZONE('America/Los_Angeles','Asia/Kolkata',EFFECTIVE_END_DATE) as EFFECTIVE_END_DATE_INDIA,
    IS_CURRENT,
    CONVERT_TIMEZONE('America/Los_Angeles','Asia/Kolkata',INGESTION_TS) as INGESTION_TS_INDIA
FROM DEV_PAYMENTS.STAGING.PAYMENTS_STAGING
WHERE IS_CURRENT = TRUE
ORDER BY PAYMENT_ID ASC;




SELECT * FROM DEV_PAYMENTS.STAGING.PAYMENTS_STAGING_CLEAN;




-----------------------------------------------------------------------------------------------------------------------------------------------
// MART (PRESENTATION LAYER):
-----------------------------------------------------------------------------------------------------------------------------------------------
USE SCHEMA dev_payments.mart;

CREATE OR REPLACE TABLE dev_payments.mart.dim_currency
(
    CURR_ID INT PRIMARY KEY IDENTITY(1,1),
    CURRENCY VARCHAR,
    INSERT_TS  TIMESTAMP DEFAULT TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
) COMMENT = 'THIS IS CURRENCY DIMENSION TABLE';


INSERT INTO dev_payments.mart.dim_currency(CURRENCY)
SELECT DISTINCT CURRENCY FROM DEV_PAYMENTS.STAGING.PAYMENTS_STAGING_CLEAN;

SELECT * FROM dev_payments.mart.dim_currency;

TRUNCATE dev_payments.mart.dim_currency;


-- PROCEDURE TO UPDATE THE CURRENCY DIM TABLE:

CREATE OR REPLACE PROCEDURE dev_payments.mart.PROC_DIM_CURRENCY_UPDATE()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE 
    rows_inserted INT;
BEGIN
    INSERT INTO dev_payments.mart.dim_currency(CURRENCY)
    SELECT DISTINCT S.CURRENCY FROM DEV_PAYMENTS.STAGING.PAYMENTS_STAGING_CLEAN  S
    WHERE NOT EXISTS
    (
        SELECT 1 FROM dev_payments.mart.dim_currency  T
        WHERE S.CURRENCY = T.CURRENCY
    );

    rows_inserted  := SQLROWCOUNT;
    RETURN 'Inserted ' || rows_inserted || ' New Currencies';
END;
$$;


-- TASK TO CALL THE THINGS:
CREATE OR REPLACE TASK dev_payments.mart.TASK_RUN_CURRENCY_DIM_PROC
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON * * * * * UTC'
AS
CALL dev_payments.mart.PROC_DIM_CURRENCY_UPDATE();


ALTER TASK  dev_payments.mart.TASK_RUN_CURRENCY_DIM_PROC SUSPEND;
ALTER TASK  dev_payments.mart.TASK_RUN_CURRENCY_DIM_PROC RESUME;


-- FACT TABLE: 

CREATE OR REPLACE TABLE dev_payments.mart.FACT_PAYMENT
(
    
    PAYMENT_ID VARCHAR,
    USER_ID VARCHAR,
    AMOUNT DECIMAL,
    CURRENCY_ID INT,
    PAYMENT_TS_INDIA TIMESTAMP
    
) COMMENT = 'THIS IS FACT_PAYMENT_TABLE';


// CHECK FOR PROCEDURE:

CREATE OR REPLACE PROCEDURE dev_payments.mart.PROC_FACT_PAYMENT()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    DECLARE row_count_no INT;
BEGIN
    INSERT INTO dev_payments.mart.FACT_PAYMENT (PAYMENT_ID,USER_ID,AMOUNT,CURRENCY_ID,PAYMENT_TS_INDIA)

    SELECT 
        S.PAYMENT_ID,
        S.USER_ID,
        S.AMOUNT,
        C.CURR_ID,
        S.PAYMENT_TS_INDIA
    FROM DEV_PAYMENTS.STAGING.PAYMENTS_STAGING_CLEAN AS S
    JOIN dev_payments.mart.dim_currency AS  C
    ON S.CURRENCY = C.CURRENCY
    WHERE NOT EXISTS
    (
        SELECT 1 FROM dev_payments.mart.FACT_PAYMENT as f
        WHERE f.PAYMENT_ID = s.PAYMENT_ID
        AND f.USER_ID = s.USER_ID
        AND f.AMOUNT = s.AMOUNT
        AND f.PAYMENT_TS_INDIA = s.PAYMENT_TS_INDIA
        
    );

    row_count_no := SQLROWCOUNT;

    RETURN 'Total rows inserted into dev_payments.mart.FACT_PAYMENT ' || '= ' || row_count_no;
END;
$$;

// TASK CREATION TO CALL TEH PROCEUDRE: 


CREATE OR REPLACE TASK dev_payments.mart.TASK_RUN_PAYEMENTS_FACT_PROC
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON * * * * * UTC'
AS
CALL dev_payments.mart.PROC_FACT_PAYMENT();

ALTER TASK dev_payments.mart.TASK_RUN_PAYEMENTS_FACT_PROC RESUME;
        
        
SELECT * FROM dev_payments.mart.FACT_PAYMENT;  
TRUNCATE dev_payments.mart.FACT_PAYMENT;

/* PAYMENT_ID	USER_ID	AMOUNT	CURRENCY_ID	PAYMENT_TS_INDIA
P005	U1005	1200	1	2025-09-16 02:40:00.000
P001	U2001	291	2	2025-09-15 22:45:00.000
P004	U1004	75	2	2025-09-16 01:30:00.000
P003	U1003	500	3	2025-09-16 01:15:00.000
P002	U1002	100	4	2025-09-16 00:00:00.000
*/


