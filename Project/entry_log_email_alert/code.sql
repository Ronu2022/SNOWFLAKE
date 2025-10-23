/* There are 4 Correct files and 1 defective files:

-- customer_20251023
-- customer_20251024
-- customer_20251025
-- customer_20251026
-- customer_20251027
-- customer_20251028 (This is defective file)

*/




USE DATABASE LEARNINGS;
CREATE OR REPLACE SCHEMA LEARNINGS.PROJECT_WORKS;

// STORAGE INTEGRATION:

CREATE OR REPLACE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = s3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = "arn:aws:iam::481665128591:role/pipeline_project_role"
STORAGE_ALLOWED_LOCATIONS = ("s3://pipelinebucketludul/csv/");

// DESC STORAGE INTEGRATION:

DESC INTEGRATION s3_int;

-- STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::959976692893:user/5wh91000-s
-- STORAGE_AWS_ROLE_ARN: arn:aws:iam::481665128591:role/pipeline_project_role
/* Policy update:

{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Principal": {
				"AWS": "arn:aws:iam::959976692893:user/5wh91000-s"
			},
			"Action": "sts:AssumeRole",
			"Condition": {}
		}
	]
}
*/

// FILE FORMAT:

CREATE OR REPLACE FILE FORMAT csv_format_pipeline
TYPE = 'CSV'
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
NULL_IF = ('NULL','null')
EMPTY_FIELD_AS_NULL = TRUE
TRIM_SPACE = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE;


// CREATION OF STAGE:

CREATE OR REPLACE STAGE stage_pipeline_csv
URL = 's3://pipelinebucketludul/csv/'
STORAGE_INTEGRATION = s3_int
FILE_FORMAT = csv_format_pipeline;

// LISTING STAGE:

LIST @stage_pipeline_csv; -- No files yet.

// TBALE CREATION:
CREATE OR REPLACE TABLE LEARNINGS.PROJECT_WORKS.customer_data 
(
  CustomerID STRING,
  Name STRING,
  Segment STRING,
  Region STRING,
  CreatedDate DATE,
  file_name VARCHAR,
  row_num INT
);


// CREATE PIPE:

CREATE OR REPLACE PIPE pipe_customer_data
AUTO_INGEST = TRUE
AS
COPY INTO LEARNINGS.PROJECT_WORKS.customer_data
FROM 
(
    SELECT 
        $1,$2,$3,$4,$5,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
        FROM @stage_pipeline_csv
) FILE_FORMAT = csv_format_pipeline
ON_ERROR = CONTINUE
PATTERN = '.*customer.*';

// DESC PIPE :

DESC PIPE pipe_customer_data;
-- notification_channel : arn:aws:sqs:ap-south-1:959976692893:sf-snowpipe-AIDA57AYR3SOXAG3KAVML-9BmnyoXbuXZcxk9Hcanprg
--                        arn:aws:sqs:ap-south-1:959976692893:sf-snowpipe-AIDA57AYR3SOXAG3KAVML-9BmnyoXbuXZcxk9Hcanprg

// CHEK PIPE STATUS:

SELECT SYSTEM$PIPE_STATUS('pipe_customer_data'); -- lastforwardedmessage, lastforwarded filepath -> all okay


// UPLOADED FILE 1:

LIST @stage_pipeline_csv; -- UPLOADED.

// CHECK THE TABLE: 

SELECT * FROM LEARNINGS.PROJECT_WORKS.customer_data; -- ALL DATA has come.




// CHECK COPY_HISTORY: 

SELECT * FROM TABLE
(
    INFORMATION_SCHEMA.COPY_HISTORY
    (
        TABLE_NAME => 'PROJECT_WORKS.customer_data',
        START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP)
    )
);

// CREATING THE LOG TABLE:

CREATE OR REPLACE TABLE LEARNINGS.PROJECT_WORKS.PIPELINE_PROJECT_LOG_TABLE
(
    LAST_LOAD_TIME TIMESTAMP,   
    FILE_NAME VARCHAR,
    TABLE_CATALOG_NAME VARCHAR,
    TABLE_SCHEMA_NAME VARCHAR,
    TABLE_NAME VARCHAR,
    STATUS VARCHAR,
    ROW_COUNT INT,
    ROW_PARSED INT,
    ERROR_COUNT INT,
    FIRST_ERROR_MESSAGE VARCHAR,
    FIRST_ERROR_LINE_NUMBER INT,
    FIRST_ERROR_COLUMN_NAME VARCHAR,
    ALERT_SENT BOOLEAN DEFAULT FALSE
);




// CREATE STREAM ON LOG TABLE:

    CREATE OR REPLACE STREAM LEARNINGS.PROJECT_WORKS.PIPELINE_PROJECT_LOG_STREAM
    ON TABLE LEARNINGS.PROJECT_WORKS.PIPELINE_PROJECT_LOG_TABLE;


SELECT * FROM LEARNINGS.PROJECT_WORKS.PIPELINE_PROJECT_LOG_STREAM;


// CREATE ALERT INTEGRATION 


CREATE NOTIFICATION INTEGRATION email_alerts_int
TYPE  = EMAIL
ENABLED = TRUE
ALLOWED_RECIPIENTS = ('luduldbt@gmail.com'); -- note: you can only add those reciepients who are user in this snowflake account


// PROCEDURE:

CREATE OR REPLACE PROCEDURE  LEARNINGS.PROJECT_WORKS.sp_monitor_pipeline_ingestion()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE 
    V_NEW_ROWS INT;
    V_ALERT_BODY STRING; 

BEGIN
    -- Step 1: Create temp table with last 1 hour of COPY_HISTORY
    CREATE OR REPLACE TEMPORARY TABLE TMP_COPY_HISTORY AS
    SELECT * FROM 
    SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
    WHERE
        TABLE_NAME = 'CUSTOMER_DATA'
      AND   LAST_LOAD_TIME > DATEADD(HOUR,-2, CURRENT_TIMESTAMP())
    ;

    -- Step 2: Delete old error records if the same file now succeeded

    DELETE FROM LEARNINGS.PROJECT_WORKS.PIPELINE_PROJECT_LOG_TABLE
    WHERE FILE_NAME IN
        ( 
        SELECT DISTINCT FILE_NAME FROM  TMP_COPY_HISTORY WHERE ROW_COUNT = ROW_PARSED 
        )
        AND TABLE_NAME = 'CUSTOMER_DATA'; -- successful load

    -- Step 3: Insert only new error records:

    INSERT INTO LEARNINGS.PROJECT_WORKS.PIPELINE_PROJECT_LOG_TABLE (
        LAST_LOAD_TIME,
        FILE_NAME,
        TABLE_CATALOG_NAME,
        TABLE_SCHEMA_NAME,
        TABLE_NAME,
        STATUS,
        ROW_COUNT,
        ROW_PARSED,
        ERROR_COUNT,
        FIRST_ERROR_MESSAGE,
        FIRST_ERROR_LINE_NUMBER,
        FIRST_ERROR_COLUMN_NAME,
        ALERT_SENT
    )
    SELECT 
        LAST_LOAD_TIME,
        FILE_NAME,
        TABLE_CATALOG_NAME,
        TABLE_SCHEMA_NAME,
        TABLE_NAME,
        STATUS,
        ROW_COUNT,
        ROW_PARSED,
        ERROR_COUNT,
        FIRST_ERROR_MESSAGE,
        FIRST_ERROR_LINE_NUMBER,
        FIRST_ERROR_COLUMN_NAME,
        FALSE
    FROM TMP_COPY_HISTORY
    WHERE ROW_COUNT <> ROW_PARSED
    AND FILE_NAME NOT IN 
    (SELECT FILE_NAME FROM LEARNINGS.PROJECT_WORKS.PIPELINE_PROJECT_LOG_TABLE WHERE TABLE_NAME = 'CUSTOMER_DATA');
    
    -- -- Step 4: Temp Stream Table:
    CREATE OR REPLACE TEMPORARY TABLE TMP_STREAM_TABLE AS
    SELECT * FROM LEARNINGS.PROJECT_WORKS.PIPELINE_PROJECT_LOG_STREAM;

    --   Step 5: Check stream for new inserts:
    SELECT   COUNT(*) INTO :V_NEW_ROWS
    FROM TMP_STREAM_TABLE WHERE METADATA$ISUPDATE = FALSE
    AND METADATA$ACTION = 'INSERT';

    -- Step 6: If new rows exist, send a single email alert
    IF (V_NEW_ROWS  > 0 ) THEN 
       CALL SYSTEM$SEND_EMAIL
       (
          'email_alerts_int', -- iNTEGRATION NAME
          'luduldbt@gmail.com', -- Recipient list
          '❌NEW ENTRY IN ERROR_LOG', -- Subject
          'If you receive this, your integration is fully active.', -- MESSAGE BODY
          'text/plain' -- MIME type (optional)
    );
    end if; 

    Return 'Monitoring Complete';
    END; 
    $$;



    
// TASK:

CREATE OR REPLACE TASK LEARNINGS.PROJECT_WORKS.task_monitor_ingestion
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON */10 * * * * UTC'
AS
CALL LEARNINGS.PROJECT_WORKS.sp_monitor_pipeline_ingestion();


// ALTER TASK RESUME:

ALTER TASK LEARNINGS.PROJECT_WORKS.task_monitor_ingestion RESUME;
ALTER TASK LEARNINGS.PROJECT_WORKS.task_monitor_ingestion suspend;

SHOW TASKS;

    
-- Uploading a new file: 

LIST @stage_pipeline_csv; -- 1 file yet


LIST @stage_pipeline_csv;    -- s3://pipelinebucketludul/csv/customer_20251024.txt (NEW FILE UPLOADED);

-- Check tables: 

SELECT * FROM LEARNINGS.PROJECT_WORKS.customer_data; -- Data ingested

-- Check for the copy load: 
SELECT * FROM TABLE
(
    INFORMATION_SCHEMA.COPY_HISTORY
    (
        TABLE_NAME => 'PROJECT_WORKS.customer_data',
        START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP)
    )
); -- Okay!!

-- Lets check the log.
select * from LEARNINGS.PROJECT_WORKS.PIPELINE_PROJECT_LOG_TABLE; --  No records because there was no error.

-- check the stream: 
SELECT * FROM LEARNINGS.PROJECT_WORKS.PIPELINE_PROJECT_LOG_STREAM; -- No records off course.




-- Added 3 more files (correct Files):
-- Check List: 

LIST @stage_pipeline_csv;

--s3://pipelinebucketludul/csv/customer_20251025.txt
--s3://pipelinebucketludul/csv/customer_20251026.txt
-- s3://pipelinebucketludul/csv/customer_20251027.txt

-- Check Table: 


SELECT * FROM LEARNINGS.PROJECT_WORKS.customer_data; -- all ingested

-- Check for the copy load: 
SELECT * FROM TABLE
(
    INFORMATION_SCHEMA.COPY_HISTORY
    (
        TABLE_NAME => 'PROJECT_WORKS.customer_data',
        START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP)
    )
); -- Okay!! -- total 4 files 




-- Lets check the log.
select * from LEARNINGS.PROJECT_WORKS.PIPELINE_PROJECT_LOG_TABLE; --  No records because there was no error.

-- check the stream: 
SELECT * FROM LEARNINGS.PROJECT_WORKS.PIPELINE_PROJECT_LOG_STREAM; -- No records off course.




-- nOW INSERTED THE DEFFECTIVE FILE: 
-- cALLED TEH PROCEDURE -wE DRECIEVED THE MAIL => WORKING FINE.




