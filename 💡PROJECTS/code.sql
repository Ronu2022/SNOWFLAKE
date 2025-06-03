// STEP - 1 - DATABASE/SCHEMA CREATION:

CREATE OR REPLACE DATABASE SP_PROJECT;
CREATE OR REPLACE SCHEMA staging_schema;

// STEP - 2 - BUCKET CREATION:
-- Springloandata -> Application and Demographics.

// Step- 3- Storage Integration:
create or replace storage integration s3_int
type = external_stage
storage_provider = s3
enabled = True
storage_aws_role_arn = 'arn:aws:iam::481665128591:role/loan_application_pipeline_role'
storage_allowed_locations = ('s3://springloandata/application/','s3://springloandata/demographics/')
comment = 'This is a storage integration creation'; 

// Step 4 : Desc Storage Interagton:

DESC STORAGE INTEGRATION s3_int;
-- arn:aws:iam::937025013744:user/1nn11000-s (to get the IAM_USER_ARN)
-- arn:aws:iam::937025013744:user/1nn11000-s
// Step 5: Changing Trust Policy.

-- Pasted the above arn there.

// Step 6: FILE FORMAT

CREATE OR REPLACE FILE FORMAT file_format_trial
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
EMPTY_FIELD_AS_NULL = TRUE;

//  Step 7: STAGING

-- for application:

CREATE OR REPLACE STAGE spring_pipe_stage
URL = 's3://springloandata/application/'
STORAGE_INTEGRATION = s3_int
FILE_FORMAT = file_format_trial;

LIST @spring_pipe_stage;

-- for demograohics:
CREATE OR REPLACE STAGE spring_pipe_stage_demographics
URL = 's3://springloandata/demographics/'
STORAGE_INTEGRATION = s3_int
FILE_FORMAT = file_format_trial;

LIST @spring_pipe_stage_demographics;


// Step 8: Creation of Table:

-- loa Application
Create or replace Table loan_application
(
    application_id VARCHAR,
    customer_id VARCHAR,
    loan_amount FLOAT,
    application_date DATE,
    status VARCHAR,
    file_name STRING,
    INGESTION_TIME TIMESTAMP_NTZ
    
);



-- Demographics:

CREATE OR REPLACE TABLE cx_demographics
(
    customer_id VARCHAR,
    age INT,
    region VARCHAR,
    credit_score INT,
    income FLOAT,
    file_name STRING,
    INGESTION_TIME TIMESTAMP_NTZ

);


// SNOWPIPE:

-- for Loan application

CREATE OR REPLACE PIPE PIPE_LOAN 
AUTO_INGEST = TRUE
AS COPY INTO loan_application (application_id,customer_id,loan_amount,application_date,status,file_name,INGESTION_TIME)
FROM
(
    SELECT $1, $2,$3, $4,$5,
    METADATA$FILENAME as file_name,
    METADATA$START_SCAN_TIME AS ingestion_time 
    FROM @spring_pipe_stage
)
PATTERN = '.*loan.*';


-- for demographics:

CREATE OR REPLACE PIPE PIPE_DEMOGRAPHICS
AUTO_INGEST = TRUE
AS COPY INTO cx_demographics(customer_id,age,region,credit_score,income,file_name,INGESTION_TIME)
FROM
    (
        SELECT $1,$2,$3,$4,$5,
        METADATA$FILENAME as file_name,
        METADATA$START_SCAN_TIME as ingestion_time
        FROM @spring_pipe_stage_demographics
    )
PATTERN = '.*demographics.*';

// Describing PIPES:
-- for Loan:
DESC PIPE PIPE_LOAN;
-- arn:aws:sqs:ap-south-1:937025013744:sf-snowpipe-AIDA5UKYLA7YGW3Y34CR6-sC7NZfjbnBm9GpaBbAlfdw  (Notification_Channel)
-- arn:aws:sqs:ap-south-1:937025013744:sf-snowpipe-AIDA5UKYLA7YGW3Y34CR6-sC7NZfjbnBm9GpaBbAlfdw
-- arn:aws:sqs:ap-south-1:937025013744:sf-snowpipe-AIDA5UKYLA7YGW3Y34CR6-sC7NZfjbnBm9GpaBbAlfdw
-- for demographics:
DESC PIPE PIPE_DEMOGRAPHICS;
-- arn:aws:sqs:ap-south-1:937025013744:sf-snowpipe-AIDA5UKYLA7YGW3Y34CR6-sC7NZfjbnBm9GpaBbAlfdw (Notification Channel)

// Creation of Event Notification:
-- for Loan:
    -- notification_loan(name)
-- for demographics:

// INGEST :

-- uploaded 1 into Loan Folder.
-- Let's cehck:
LIST @spring_pipe_stage;-- there
SELECT * FROM loan_application; -- there 

-- uploaded 1 into the Demographics folder:
-- Let's check:
LIST @spring_pipe_stage_demographics; -- There
SELECT * FROM cx_demographics;


// All ingestions done:


// Trouble Shooting:
SHOW PIPES;

SELECT SYSTEM$PIPE_STATUS('PIPE_DEMOGRAPHICS');
SELECT SYSTEM$PIPE_STATUS('PIPE_LOAN');

SELECT * FROM TABLE(VALIDATE_PIPE_LOAD
(
    PIPE_NAME => 'PIPE_DEMOGRAPHICS',
    START_TIME => DATEADD(hour, -1, current_timestamp())
));


// Now Delete:

-- Let's say you got an update to delte all records which were ingested from a specific file: 
-- it would be easy now

delete  from loan_application where FILE_NAME = 'application/loan_applications_3.csv';
delete from cx_demographics WHERE FILE_NAME = 'demographics/customer_demographics_2.csv';

-- Thus adding file_name column and ingestion column becomes extremely crucial.



