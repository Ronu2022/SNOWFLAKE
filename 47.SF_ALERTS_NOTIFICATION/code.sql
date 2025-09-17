-- HAve a situation
-- File uploaded on sf
-- at times the correct file comes snow pipe works well
-- when there is a wrong file (lets say a file with more columns) -> You want that to be not loaded and marked as an error and an alert need to be sent
-- ALSO MAINTAIN AN ERROR LOG
-- FILE 1 : Payments FIle (1st Load) -> Works Fine
--> FILE 2: With incorrect Column Count -> ERROR FILE (NEEDS HANDLING)\\


CREATE OR REPLACE DATABASE DEMO_DB;
use schema demo_db.public;
use role accountadmin; -- without permission event notification wont be set



// STORAGE INTEGRATION:

CREATE OR REPLACE STORAGE INTEGRATION s3_int_mindtree_email_alert
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::481665128591:role/trial_email_alerts_ingestion'
STORAGE_ALLOWED_LOCATIONS = 
(
  's3://mindtreedev/customers/',
  's3://mindtreepreprod/customers/',
  's3://mindtreeprod/customers/',
  's3://mindtreedev/payments/',
  's3://emailaltertsrunning/csv/'
  
)
COMMENT = 'This is s3_int for all environments';


// DESC STORAGE INTEGRATION:
desc storage integration s3_int_mindtree_email_alert;

-- arn:aws:iam::555111849042:user/v6c71000-s
-- 
-- arn:aws:iam::555111849042:user/v6c71000-s\
-- arn:aws:iam::555111849042:user/v6c71000-s



// CREATE FILE FORMAT:

CREATE OR REPLACE FILE FORMAT ff
TYPE  = 'CSV'
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY ='"'
EMPTY_FIELD_AS_NULL = TRUE
DATE_FORMAT = 'YYYY-MM-DD'
SKIP_HEADER = 1
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE;



// CREATION OF STAGE:

CREATE OR REPLACE STAGE stg_name
URL = 's3://emailaltertsrunning/csv/'
STORAGE_INTEGRATION = s3_int_mindtree_email_alert
FILE_FORMAT  = ff;


// LIST STAGE:

LIST @stg_name; -- no records for the time being.


// CREATION OF TABLE SCHEMA:


CREATE OR REPLACE TABLE payments_table
(
    payment_id   STRING,
    user_id      STRING,
    amount       NUMBER(10,2),
    currency     STRING,
    payment_ts   TIMESTAMP_NTZ
  
    
);
    
// STREAM CREATION 

CREATE OR REPLACE STREAM PAYMENT_TABLE_STREAM
ON TABLE payments_table;

// PIPE CREATION:

CREATE OR REPLACE PIPE s3_pipe_name
AUTO_INGEST = TRUE
AS
COPY INTO payments_table
FROM @stg_name
FILE_FORMAT = ff
PATTERN = '.*payments.*';


DESC PIPE s3_pipe_name;
-- arn:aws:sqs:ap-south-1:555111849042:sf-snowpipe-AIDAYCPZ6TRJCCYNM6EIW-s1HvU6yhiOt2VUbXT6BA3g
-- arn:aws:sqs:ap-south-1:555111849042:sf-snowpipe-AIDAYCPZ6TRJCCYNM6EIW-s1HvU6yhiOt2VUbXT6BA3g
-- arn:aws:sqs:ap-south-1:555111849042:sf-snowpipe-AIDAYCPZ6TRJCCYNM6EIW-s1HvU6yhiOt2VUbXT6BA3g

-- Ingested a file a correct one: Payements_file

LIST @stg_name;
select * from payments_table;

select * from table (information_schema.validate_pipe_load
(
    pipe_name => 's3_pipe_name',
    start_time => dateadd(hour,-2,current_timestamp) -- no records because there were no files in it
));
-- columns in validate
/*
    ERROR
    FILE
    LINE
    CHARACTER
    BYTE_OFFSET
    CATEGORY
    CODE
    SQL_STATE
    COLUMN_NAME
    ROW_NUMBER
    ROW_START_LINE
    REJECTED_RECORD
*/


// LOG TABLE:

CREATE OR REPLACE TRANSIENT TABLE ingestion_error_log 
(
    file_name string,
    line_number int,
    rejected_record string,
    error_message string,
    error_category string,
    error_code string,
    error_time timestamp default current_timestamp()
);


// PROCEDURE:

CREATE OR REPLACE PROCEDURE capture_pipe_errors()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO  ingestion_error_log(file_name, line_number, rejected_record, error_message, error_category, error_code)
    SELECT v.FILE,V.LINE,V.REJECTED_RECORD,V.ERROR,V.CATEGORY,CODE
    FROM TABLE(INFORMATION_SCHEMA.VALIDATE_PIPE_LOAD(

    PIPE_NAME => 's3_pipe_name',
    START_TIME => DATEADD(MINUTE,-5,CURRENT_TIMESTAMP)
    )) AS V
    LEFT JOIN ingestion_error_log AS el
    ON 
        V.FILE = el.file_name
        AND V.LINE = el.line_number
        AND V.REJECTED_RECORD = el.rejected_record
        AND V.ERROR = el.error_message
        AND V.CATEGORY = el.error_category
        AND V.CODE = el.error_code
    WHERE el.file_name IS NULL;

    return 'Error Log Updated';
END;
$$;


    
// TASK:

CREATE OR REPLACE TASK task_capture_pipe_errors
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '5 minute'
AS
CALL capture_pipe_errors();
    

// RESUMING TASK:

ALTER TASK task_capture_pipe_errors RESUME; 


// NOTIFICATION INTEGRATION:

create or replace notification integration MY_EMAIL_NOTIFICATION
  type = email
  enabled = true
  allowed_recipients = ('ludul4@outlook.com'); --> Note this is the name of the account's email
  --> else it would give not allowed email

  // Test:
call system$send_email(
  'MY_EMAIL_NOTIFICATION',                 -- integration name
  'ludul4@outlook.com',           -- recipient(s)
  'Snowpipe Test Alert',                   -- subject
  'This is a test email from Snowflake',   -- body
  'text/plain'                             -- optional mime_type
); --> Worked Now we shoudl create the alert




// CREATE THE EMAIL ALERT:

CREATE OR REPLACE ALERT ALERT_PIPE_ERRORS
WAREHOUSE = COMPUTE_WH
SCHEDULE = '6 MINUTE'
IF(EXISTS
    (
        SELECT 1 FROM ingestion_error_log 
        WHERE error_time > DATEADD(MINUTE,-6, CURRENT_TIMESTAMP)
    )

)
THEN 
    CALL SYSTEM$SEND_EMAIL
    (
        'MY_EMAIL_NOTIFICATION', -- -- integration name
        'ludul4@outlook.com', -- RECIPIENT
        'Snowpipe Test Alert', -- SUBJECT
        'New Entry in the Error Log',-- BODY
        'text/plain'              -- optional mime_type
    );


// RESUMING THE TASK:
ALTER ALERT ALERT_PIPE_ERRORS RESUME;



-- Now uploaded a wrong file with error in column count
-- i,e payments_4

// VALIDATION OF PIPE_LOAD

LIST @stg_name;
select * from payments_table;
select * from table (information_schema.validate_pipe_load
(
    pipe_name => 's3_pipe_name',
    start_time => dateadd(hour,-2,current_timestamp)
));



select * from ingestion_error_log;


// VALIDATION OF ALERT HISTORY;
select *
from table(
  information_schema.alert_history(
    scheduled_time_range_start => dateadd(hour,-2,current_timestamp()),
    scheduled_time_range_end   => current_timestamp(),
    alert_name => 'ALERT_PIPE_ERRORS'
  )
);

--  MAIL WAS SENT


--> NOTIFICATION INTEGRATION
--> ALTERT

