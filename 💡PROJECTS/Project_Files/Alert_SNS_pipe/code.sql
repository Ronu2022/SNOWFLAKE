CREATE OR REPLACE DATABASE PROJECTS;

CREATE SCHEMA PROJECTS.RAW;
CREATE SCHEMA PROJECTS.FILE_FORMATS;
USE SCHEMA PROJECTS.RAW;

// ROLE CREATION: 
-- alerts_sf_project_role


// STORAGE INTEGRATION

CREATE OR REPLACE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::481665128591:role/alerts_sf_project_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://ordersalert/csv/');

// DES:

DESC STORAGE INTEGRATION  s3_int;
-- arn:aws:iam::959976692893:user/5wh91000-s (STORAGE_AWS_IAM_USER_ARN)
-- STORAGE_AWS_ROLE_ARN:  arn:aws:iam::481665128591:role/alerts_sf_project_role
-- STORAGE_AWS_EXTERNAL_ID:HU28388_SFCRole=6_T6wIxcw6JwBwkC/qrZv4LL+oqXE=
// EDIT TRUST POLICY
/*
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::959976692893:user/5wh91000-s"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "HU28388_SFCRole=6_T6wIxcw6JwBwkC/qrZv4LL+oqXE="
                }
            }
        }
    ]
}

*/




// CREATION OF INTEGRATION PRIVILIGES:



// GO FOR SUBSCRIPTION CREATION: 

--> Sign in to the Amazon Simple Notification Service (SNS) console
--> In the left navigation pane select “Topics” → Click “Create topic”.
--> give Name snowpipe_alerts_topic: arn:aws:sns:ap-south-1:123456789012:snowpipe_alerts_topic
--> Would ask for Topic Name : snowpipe_alerts_topic
--> Type : Keep “Standard” selected
-- > rest keep as it is 
-- > Create subscription
--> copy ARN : arn:aws:sns:ap-south-1:481665128591:snowpipe_alerts_topic
--> Then SNS console -> topics -> chose it -> Open -> protocol -Email
-- > endpoint -> ronu.mondeep@example.com
--> Create subscription

CREATE OR REPLACE NOTIFICATION INTEGRATION PIPE_ALERT_INT
TYPE = QUEUE
ENABLED = TRUE
DIRECTION = OUTBOUND
NOTIFICATION_PROVIDER = AWS_SNS
AWS_SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:481665128591:snowpipe_alerts_topic'
AWS_SNS_ROLE_ARN = 'arn:aws:iam::481665128591:role/snowflake_sns_alerts_role'; 




// DESC THE ABOVE NOTIFICATION INTEGRATION
DESC NOTIFICATION INTEGRATION PIPE_ALERT_INT;
-- AWS_SNS_ROLE_ARN: arn:aws:iam::481665128591:role/snowflake_sns_alerts_role
-- SF_AWS_IAM_USER_ARN: arn:aws:iam::959976692893:user/5wh91000-s
-- SF_AWS_EXTERNAL_ID: HU28388_SFCRole=6_tC0wAdhtXW+W/BGsufXu9YV/Lkk=


// GO TO THE NEW ROLE CREATED FOR THIS: snowflake_sns_alerts_role -> Trust relationship -> Edit
-- We’ll now link your SNS topic with your Snowflake pipe
/*

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::959976692893:user/5wh91000-s"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "HU28388_SFCRole=6_tC0wAdhtXW+W/BGsufXu9YV/Lkk="
                }
            }
        }
    ]
}

step- 2: then attach sns access policy to the topic: 
Sns topic-> access policy -> edit json
{
  "Version": "2012-10-17",
  "Id": "__default_policy_ID",
  "Statement": [
    {
      "Sid": "AllowS3Publish",
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "SNS:Publish",
      "Resource": "arn:aws:sns:ap-south-1:481665128591:snowpipe_alerts_topic",
      "Condition": {
        "ArnLike": {
          "aws:SourceArn": "arn:aws:s3:::ordersalert"
        }
      }
    },
    {
      "Sid": "AllowOwnerFullAccess",
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": [
        "SNS:GetTopicAttributes",
        "SNS:SetTopicAttributes",
        "SNS:AddPermission",
        "SNS:RemovePermission",
        "SNS:DeleteTopic",
        "SNS:Subscribe",
        "SNS:ListSubscriptionsByTopic",
        "SNS:Publish"
      ],
      "Resource": "arn:aws:sns:ap-south-1:481665128591:snowpipe_alerts_topic",
      "Condition": {
        "StringEquals": {
          "AWS:SourceOwner": "481665128591"
        }
      }
    }
  ]
}



*/




// CREATION OF FILE FORMAT: 

CREATE OR REPLACE FILE FORMAT PROJECTS.FILE_FORMATS.CSV_FORMAT
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
FIELD_DELIMITER = ','
NULL_IF = ('NULL','null','Null','')
SKIP_HEADER = 1
EMPTY_FIELD_AS_NULL = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
REPLACE_INVALID_CHARACTERS = FALSE
ESCAPE_UNENCLOSED_FIELD = NONE
TRIM_SPACE = TRUE
DATE_FORMAT = 'YYYY-MM-DD'
TIME_FORMAT = 'HH24:MI:SS';





// CREATION OF EXTERNAL STAGE: 

CREATE OR REPLACE STAGE PROJECTS.RAW.s3_orders_stage
URL = 's3://ordersalert/csv/'
STORAGE_INTEGRATION = s3_int
FILE_FORMAT = PROJECTS.FILE_FORMATS.CSV_FORMAT
COMMENT = 'Stage for Orders CSV files from ordersalert bucket';


// VERIFY: 
LIST @PROJECTS.RAW.s3_orders_stage;  -- 0


// CREATE TABLE: 

CREATE OR REPLACE TABLE PROJECTS.RAW.ORDERS_RAW 
(
    ORDER_ID        STRING         NOT NULL,
    CUSTOMER_ID     STRING         NOT NULL,
    ORDER_DATE      DATE           NOT NULL,
    SHIP_DATE       DATE           NOT NULL,
    SHIP_MODE       STRING,
    SALES           NUMBER(10,2),
    QUANTITY        NUMBER(10,0),
    PROFIT          NUMBER(10,2),
    REGION          STRING,
    FILE_NAME       STRING,          -- auto-filled from METADATA$FILENAME
    LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


// PIPE:

CREATE OR REPLACE PIPE PROJECTS.RAW.PIPE_ORDERS_RAW
AUTO_INGEST = TRUE
AWS_SNS_TOPIC = 'arn:aws:sns:ap-south-1:481665128591:snowpipe_alerts_topic'
AS
COPY INTO PROJECTS.RAW.ORDERS_RAW
(
     ORDER_ID,
     CUSTOMER_ID,
     ORDER_DATE,
     SHIP_DATE,
     SHIP_MODE,
     SALES,
     QUANTITY,
     PROFIT,
     REGION,
     FILE_NAME
)
FROM (
    SELECT 
        t.$1,
        t.$2,
        TO_DATE(t.$3,'YYYY-MM-DD'),
        TO_DATE(t.$4,'YYYY-MM-DD'),
        t.$5,
        TO_NUMBER(t.$6,10,2),
        TO_NUMBER(t.$7,10,0),
        TO_NUMBER(t.$8,10,2),
        t.$9,
        METADATA$FILENAME
    FROM @PROJECTS.RAW.s3_orders_stage (FILE_FORMAT => PROJECTS.FILE_FORMATS.CSV_FORMAT) t
);


-- to create topic:
    -- go to AWS console
    -- check for create
    -- Access policy 
















// DESC PIPE:

DESC PIPE PROJECTS.RAW.PIPE_ORDERS_RAW;
-- arn:aws:sqs:ap-south-1:959976692893:sf-snowpipe-AIDA57AYR3SOXAG3KAVML-9BmnyoXbuXZcxk9Hcanprg (NOTIFICATION CHANNEL)
--arn:aws:sns:ap-south-1:481665128591:snowpipe_alerts_topic
-- arn:aws:sns:ap-south-1:481665128591:snowpipe_alerts_topic

/* go to the event notification: 
events-name : alerts_orders

Prefix - optional -> csv/
Object creation -> All object create events
s3:ObjectCreated:*

Destination -> SNS topic
Specify SNS topic -> Choose from your SNS topics

SNS topic -> Choose from the topic: snoflake_alerts_topic
*/

// TESTING: 

LIST @PROJECTS.RAW.s3_orders_stage; -- 0 
SELECT * FROM PROJECTS.RAW.ORDERS_RAW; -- 0 Records

-- uploaded : orders_data_2025_10_31.txt
LIST @PROJECTS.RAW.s3_orders_stage; 
SELECT * FROM PROJECTS.RAW.ORDERS_RAW; -- 5 records


-- CHeck STatus

select file_name,
stage_location,
row_count,
row_parsed,
FIRST_ERROR_MESSAGE,
FIRST_ERROR_LINE_NUMBER,
STATUS,
TABLE_CATALOG_NAME,
TABLE_SCHEMA_NAME,
TABLE_NAME
from 
table(
information_schema.copy_history
(
    table_name => 'PROJECTS.RAW.ORDERS_RAW',
    start_time => dateadd('hour',-1,current_timestamp())
)
); -- shows success


-- uploaded: orders_data_2025_11_01.txt
LIST @PROJECTS.RAW.s3_orders_stage;  -- 2 records
SELECT * FROM PROJECTS.RAW.ORDERS_RAW; -- 10 Records
-- CHeck STatus

select file_name,
stage_location,
row_count,
row_parsed,
FIRST_ERROR_MESSAGE,
FIRST_ERROR_LINE_NUMBER,
STATUS,
TABLE_CATALOG_NAME,
TABLE_SCHEMA_NAME,
TABLE_NAME
from 
table(
information_schema.copy_history
(
    table_name => 'PROJECTS.RAW.ORDERS_RAW',
    start_time => dateadd('hour',-1,current_timestamp())
)
); -- shows success


-- orders_data_2025_11_02: uploaded (bad Record)

LIST @PROJECTS.RAW.s3_orders_stage; -- 3 records

SELECT * FROM PROJECTS.RAW.ORDERS_RAW; -- same 10 records 

-- CHeck STatus

select file_name,
stage_location,
row_count,
row_parsed,
FIRST_ERROR_MESSAGE,
FIRST_ERROR_LINE_NUMBER,
STATUS,
TABLE_CATALOG_NAME,
TABLE_SCHEMA_NAME,
TABLE_NAME
from 
table(
information_schema.copy_history
(
    table_name => 'PROJECTS.RAW.ORDERS_RAW',
    start_time => dateadd('hour',-1,current_timestamp())
)
); 


-----------------------------------------------------------------------------------------------------------
// MONITORING
-----------------------------------------------------------------------------------------------------------



CREATE OR REPLACE SCHEMA PROJECTS.MONITOR;

// PIPE_MONITORING_LOG_TABLE:

CREATE OR REPLACE TABLE PROJECTS.MONITOR.PIPE_MONITOR_LOG (
    TABLE_CATALOG_NAME STRING,
    TABLE_SCHEMA_NAME STRING,
    TABLE_NAME STRING,
    FILE_NAME STRING,
    STAGE_LOCATION STRING,
    LAST_LOAD_TIME TIMESTAMP_NTZ,
    ROW_COUNT NUMBER,
    ROW_PARSED NUMBER,
    FIRST_ERROR_MESSAGE STRING,
    FIRST_ERROR_LINE_NUMBER NUMBER,
    STATUS STRING,
    SNAPSHOT_TS TIMESTAMP_NTZ
);






// STREAM CREATION: 

CREATE OR REPLACE STREAM PROJECTS.MONITOR.STREAM_PIPE_MONITOR_LOG
ON TABLE PROJECTS.MONITOR.PIPE_MONITOR_LOG;



// TASK TO UPDATE THE PROJECTS.MONITOR.PIPE_MONITOR_LOG  TABLE :

CREATE OR REPLACE TASK PROJECTS.MONITOR.TASK_REFRESH_PIPE_MONITOR
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
AS
INSERT INTO PROJECTS.MONITOR.PIPE_MONITOR_LOG 
SELECT
src.TABLE_CATALOG_NAME,
src.TABLE_SCHEMA_NAME,
src.TABLE_NAME,
src.FILE_NAME,
src.STAGE_LOCATION,
src.LAST_LOAD_TIME,
src.ROW_COUNT,
src.ROW_PARSED,
src.FIRST_ERROR_MESSAGE,
src.FIRST_ERROR_LINE_NUMBER,
src.STATUS,
current_timestamp() AS SNAPSHOT_TS
FROM TABLE
(
    INFORMATION_SCHEMA.COPY_HISTORY
    (
        TABLE_NAME => 'PROJECTS.RAW.ORDERS_RAW',
        START_TIME => DATEADD(MINUTE, -10, CURRENT_TIMESTAMP())
    )
) as src
WHERE NOT EXISTS
(
    SELECT 1 FROM PROJECTS.MONITOR.PIPE_MONITOR_LOG as t
    where t.FILE_NAME = src.FILE_NAME
    and t.LAST_LOAD_TIME = src.LAST_LOAD_TIME   
    
);

ALTER TASK PROJECTS.MONITOR.TASK_REFRESH_PIPE_MONITOR RESUME;
ALTER TASK PROJECTS.MONITOR.TASK_REFRESH_PIPE_MONITOR SUSPEND;

Select * from PROJECTS.MONITOR.PIPE_MONITOR_LOG;

// EMAIL NOTIFICATION: 

CREATE OR REPLACE NOTIFICATION INTEGRATION EMAIL_INT
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('luduldbt@gmail.com');


-- Test:
CALL SYSTEM$SEND_EMAIL(
  'EMAIL_INT',
  'luduldbt@gmail.com',
  '✅ Snowflake Email Test',
  'This is a test email from Snowflake notification integration.'
);

-- SYSTEM$SEND_EMAIL
--TRUE


// PROCEDURE:

create or replace procedure PROJECTS.MONITOR.SP_SEND_PIPE_ALERTS()
returns string
language sql
AS
$$
declare
    V_ALERT_COUNT INT;
    V_ALERT_BODY  STRING;
begin
    -- Step 1: Get failed loads from the monitoring stream

    create or replace temp table TMP_FAILED as
    select * from PROJECTS.MONITOR.STREAM_PIPE_MONITOR_LOG
    where UPPER(STATUS) != 'LOADED';

    select count(*)  into :V_ALERT_COUNT from TMP_FAILED;

     -- Step 2: If no failures, exit quietly
     if (V_ALERT_COUNT > 0) then 

      -- Step 3: Build formatted alert text
        select listagg(
              ' FILE NAME: ' || FILE_NAME || '| STATUS: ' || STATUS || '|  Rows Parsed: ' || ROW_PARSED ||  ' | Error: ' || 
              NVL(FIRST_ERROR_MESSAGE, 'Unknown')  ||  '| Line: ' || NVL(TO_VARCHAR(FIRST_ERROR_LINE_NUMBER), 'N/A') || ' | Load Time: ' ||
              TO_VARCHAR(LAST_LOAD_TIME) || '\n',
              '\n'
              ) into :V_ALERT_BODY
              from TMP_FAILED;

       -- Step 4: Send Email

       CALL SYSTEM$SEND_EMAIL(
       'EMAIL_INT',
       'luduldbt@gmail.com',
       '🚨 Snowpipe Load Failure Detected',
       :V_ALERT_BODY
       );

       RETURN '❌ Alert sent for failed loads: ' || :V_ALERT_COUNT;
       ELSE
        RETURN '✅ No failed loads found. No email sent.';
        END IF;
    END;
    $$ ;

    

CALL PROJECTS.MONITOR.SP_SEND_PIPE_ALERTS();


// TSK TO RUN THE PROCEDURE:

CREATE OR REPLACE TASK PROJECTS.MONITOR.TASK_SEND_PIPE_ALERTS
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 0/10 * * * UTC'
AS
CALL PROJECTS.MONITOR.SP_SEND_PIPE_ALERTS();

ALTER TASK PROJECTS.MONITOR.TASK_SEND_PIPE_ALERTS SUSPEND;




---------------------------------------------------------------------------------------------------------------------------------------------------
//STAGING:
------------------------------------------------------------------------------------------------------------------------------------
// CREATION OF CLEAN TABLE: 
CREATE OR REPLACE SCHEMA PROJECTS.STAGING;
CREATE OR REPLACE TRANSIENT TABLE PROJECTS.STAGING.ORDERS_STG (
    ORDER_ID        STRING        NOT NULL,
    CUSTOMER_ID     STRING        NOT NULL,
    ORDER_DATE      DATE          NOT NULL,
    SHIP_DATE       DATE          NOT NULL,
    SHIP_MODE       STRING,
    SALES           NUMBER(10,2),
    QUANTITY        NUMBER(10,0),
    PROFIT          NUMBER(10,2),
    REGION          STRING,
    FILE_NAME       STRING,
    LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

// TASK TO UPDATE THE STAGING TABLE - DEDUPLICATION AND GETTING THE LATEST RECORD

CREATE OR REPLACE TASK PROJECTS.STAGING.TASK_REFRESH_ORDERS_STG
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
AS
MERGE INTO PROJECTS.STAGING.ORDERS_STG AS tgt
USING
(
    SELECT ORDER_ID,
    CUSTOMER_ID,
    TO_DATE(ORDER_DATE) AS ORDER_DATE,
    TO_DATE(SHIP_DATE) AS SHIP_DATE,
    TRIM(SHIP_MODE) AS SHIP_MODE,
    TRY_TO_NUMBER(SALES,10,2) AS SALES,
    TRY_TO_NUMBER(QUANTITY,10,0) AS QUANTITY,
    TRY_TO_NUMBER(PROFIT,10,2) AS PROFIT,
    INITCAP(TRIM(REGION)) AS REGION,
    FILE_NAME AS FILE_NAME,
    LOAD_TS AS LOAD_TS
    FROM PROJECTS.RAW.ORDERS_RAW
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY LOAD_TS DESC) = 1 
    
) AS SRC

ON tgt.ORDER_ID = src.ORDER_ID
WHEN MATCHED THEN UPDATE SET
    tgt.CUSTOMER_ID = src.CUSTOMER_ID,
    tgt.ORDER_DATE = src.ORDER_DATE,
    tgt.SHIP_DATE = src.SHIP_DATE,
    tgt.SHIP_MODE = src.SHIP_MODE,
    tgt.SALES  = src.SALES,
    tgt.QUANTITY = src.QUANTITY,
    tgt.PROFIT = src.PROFIT,
    tgt.REGION  = src.REGION,
    tgt.FILE_NAME = src.FILE_NAME,
    tgt.LOAD_TS = src.LOAD_TS

WHEN NOT MATCHED THEN 
INSERT 
(
    ORDER_ID,CUSTOMER_ID,ORDER_DATE,SHIP_DATE,SHIP_MODE,SALES,QUANTITY,PROFIT,REGION,FILE_NAME,LOAD_TS
)
VALUES
(
    src.ORDER_ID,src.CUSTOMER_ID,src.ORDER_DATE,src.SHIP_DATE,src.SHIP_MODE,src.SALES,src.quantity,src.PROFIT,
    src.REGION,src.FILE_NAME,src.LOAD_TS
);

// RESUMING THE TASK:
ALTER TASK PROJECTS.STAGING.TASK_REFRESH_ORDERS_STG RESUME;
ALTER TASK PROJECTS.STAGING.TASK_REFRESH_ORDERS_STG SUSPEND;
SELECT * FROM PROJECTS.RAW.ORDERS_RAW;
SELECT * FROM PROJECTS.STAGING.ORDERS_STG;
------------------------------------------------------------------------------------------------------------------------------------
// PRESENTATION/ MART LAYER:
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE SCHEMA PROJECTS.MART;

/*
For the current project (ORDERS data), we’ll design:

Table	                      Type	                           Purpose
DIM_CUSTOMER	           Dimension	                       Stores customer info, keeps history (SCD2)
DIM_REGION	                Dimension	                       Stores region details (small static table)
FACT_ORDERS	                Fact	                           Stores order transactions linked to dimensions

*/

CREATE OR REPLACE TABLE PROJECTS.MART.DIM_CUSTOMER 
(
    CUSTOMER_ID       STRING       NOT NULL,
    --CUSTOMER_NAME     STRING,
    REGION            STRING,
    EFFECTIVE_FROM    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EFFECTIVE_TO      TIMESTAMP_NTZ DEFAULT NULL,
    IS_CURRENT        BOOLEAN DEFAULT TRUE,
    FILE_NAME         STRING,
    LOAD_TS           TIMESTAMP_NTZ
);
SELECT * FROM PROJECTS.STAGING.ORDERS_STG;


// CREATE TASK:
CREATE OR REPLACE TASK PROJECTS.MART.TASK_REFRESH_DIM_CUSTOMER 
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
AS
MERGE INTO
PROJECTS.MART.DIM_CUSTOMER AS tgt
USING
(
    SELECT CUSTOMER_ID,
    REGION,
    FILE_NAME,
    LOAD_TS
    FROM PROJECTS.STAGING.ORDERS_STG 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY LOAD_TS DESC) = 1
    
) as src
on tgt.CUSTOMER_ID = src.CUSTOMER_ID
and tgt.IS_CURRENT = TRUE

WHEN MATCHED AND (tgt.REGION <> src.REGION)
THEN UPDATE 
SET
tgt.IS_CURRENT = FALSE,
tgt.EFFECTIVE_TO = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN 
INSERT 
(CUSTOMER_ID, REGION, FILE_NAME, LOAD_TS,
    EFFECTIVE_FROM, IS_CURRENT, EFFECTIVE_TO
)
VALUES
(
    src.CUSTOMER_ID, src.REGION, src.FILE_NAME,src.LOAD_TS, CURRENT_TIMESTAMP(),TRUE, NULL
);

ALTER TASK PROJECTS.MART.TASK_REFRESH_DIM_CUSTOMER  RESUME;
ALTER TASK PROJECTS.MART.TASK_REFRESH_DIM_CUSTOMER  SUSPEND; 

