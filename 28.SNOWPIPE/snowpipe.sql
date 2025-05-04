USE DATABASE AWS_INTEGRATION;
USE SCHEMA AWS_INTEGRATION.AWSINTEGRATIONJANA;

// STORAGE INTEGRATION (you can Create a new one or you can update the earlier one)
-- but in creating a new storage integration you will have to create a new role and policies
-- IAM -> Role _> Create Role -> Policy -> Search for the policy -> Create Policy -> Copy ARN  TO be used in STorage Integration Creation -> once created -> Desc Integration storage_integration_name -> Copy  the STORAGE_AWS_IAM_USER_ARN  -> edit trust policy -> past the Arn.

CREATE OR REPLACE STORAGE INTEGRATION s3_int_awsintegrationjana_snowpiping
TYPE = external_stage
STORAGE_PROVIDER = s3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::481665128591:role/S3fullAccess'
STORAGE_ALLOWED_LOCATIONS = ('s3://awsintegrationjana/csv/','s3://awsintegrationjana/json/', 's3://awsintegrationjana/pipe/csv/')
COMMENT = 'Integration With s3 Bucket for Folder awsintegrationjana';



-- previous one 
CREATE OR REPLACE STORAGE INTEGRATION s3_int_awsintegrationjana
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = s3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::481665128591:role/S3fullAccess'
STORAGE_ALLOWED_LOCATIONS = ('s3://awsintegrationjana/csv/','s3://awsintegrationjana/json/')
COMMENT = 'Integration With s3 Bucket for Folder awsintegrationjana';


// Altering the created one

ALTER STORAGE INTEGRATION s3_int_awsintegrationjana
SET STORAGE_ALLOWED_LOCATIONS = ('s3://awsintegrationjana/csv/','s3://awsintegrationjana/json/', 's3://awsintegrationjana/pipe/csv/');


// Creating File Format

CREATE OR REPLACE FILE FORMAT  AWS_INTEGRATION.AWSINTEGRATIONJANA.S3_INT_AWSINTEGRATIONJANA_FILE_FORMAT_SNOWPIPE_CSV
TYPE = 'csv'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
EMPTY_FIELD_AS_NULL = TRUE;


// STAGING

CREATE OR REPLACE STAGE AWS_INTEGRATION.AWSINTEGRATIONJANA.S3_INT_SNOWPIPE_STAGE
URL = 's3://awsintegrationjana/pipe/csv/'
STORAGE_INTEGRATION = s3_int_awsintegrationjana
FILE_FORMAT = AWS_INTEGRATION.AWSINTEGRATIONJANA.S3_INT_AWSINTEGRATIONJANA_FILE_FORMAT_SNOWPIPE_CSV;


LIST @AWS_INTEGRATION.AWSINTEGRATIONJANA.S3_INT_SNOWPIPE_STAGE;


// Create a table to load these files


CREATE OR REPLACE TABLE AWS_INTEGRATION.AWSINTEGRATIONJANA.snow_pipe_emp_data 
(
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  location STRING,
  department STRING
);


// Schema for Snowpipe:

CREATE OR REPLACE SCHEMA AWS_INTEGRATION.PIPES;



// CREATE THE SNOWPIPE

CREATE OR REPLACE PIPE AWS_INTEGRATION.PIPES.EMPLOYEE_PIPE 
AUTO_INGEST = TRUE
AS
COPY INTO AWS_INTEGRATION.AWSINTEGRATIONJANA.snow_pipe_emp_data
FROM @AWS_INTEGRATION.AWSINTEGRATIONJANA.S3_INT_SNOWPIPE_STAGE
PATTERN = '.*employee.*';


// DESC Pipe

DESC PIPE AWS_INTEGRATION.PIPES.EMPLOYEE_PIPE;
-- Check for Notification Channel : arn:aws:sqs:ap-south-1:077380691495:sf-snowpipe-AIDAREBB7UYT7MVPOGYP7-Z5W3ICfSzjFJu_9rGTKkXA



SELECT * FROM AWS_INTEGRATION.AWSINTEGRATIONJANA.snow_pipe_emp_data; -- 0 Records.


// EVENT NOTIFICATION CREATION ON AWS:

-- s3 -> Bucket -> Enter the bucket -> Properties -> Event Notification

-- Details Asked:
-- Event Name : snowpipe_employee
-- Prefix - optional: pipe/csv (I had created folders awsintegrationjana/pipe/csv)
    -- means whenever the file is put into this location, it is detected.
-- Event types(chose the type):
    -- Object creation (because I wish whenever there are uploads , it should notify)
    -- With that , the following got ticked or included:   
        -- Put
        -- Post
        -- Copy
        --Multipart upload completed
-- Destination:
    -- SQS Queue.
    -- Specify SQS queue:
        -- Enter SQS queue ARN : Paste the notification_channel you had obtained by doing DESC PIPE AWS_INTEGRATION.PIPES.EMPLOYEE_PIPE;
            -- arn:aws:sqs:ap-south-1:077380691495:sf-snowpipe-AIDAREBB7UYT7MVPOGYP7-Z5W3ICfSzjFJu_9rGTKkXA
-- Create Notification



// Check for Records:
SELECT * FROM AWS_INTEGRATION.AWSINTEGRATIONJANA.snow_pipe_emp_data; -- 0 records

// Let's Upload a file with 100 Records:

SELECT * FROM AWS_INTEGRATION.AWSINTEGRATIONJANA.snow_pipe_emp_data; -- 100 Rows ingested
