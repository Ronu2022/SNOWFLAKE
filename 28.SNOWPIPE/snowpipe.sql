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


  ----------------------------------------------------------------------------------------------------------------------------------------------------------------


  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
/* TROUBLE SHOOTING SNOWPIPES */
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------


// Step 1: CHECKING THE SNOWPIPE STATUS

select SYSTEM$PIPE_STATUS('AWS_INTEGRATION.PIPES.EMPLOYEE_PIPE');

-- Check for:
-- "lastReceivedMessageTimestamp" : This gives you when was the event notification recieved by the Snowpipe
-- "lastForwardedMessageTimestamp"  This gives you when  did the snowpipe start working that is copying the entries into the Table.
-- If both show near about same time => smooth.
-- Let's assume file was uploaded at 10 AM, but here the last recieved message was of 8 AM => there could be possibility of mismatch wrt the 
    -- notification set up -> Check for the nOtification ARN
    -- Recall: Creation of Snowpipe -> Desc Snowpipe_name -> Notification_Channel -> Copy and check it with the event creation Notification 
      -- channel ARN that was set up in AWS if they are matching or not.
--  for Lastforwarded Message : If there is a considerable Lag then the issue is with the File Path
    -- Recall: Storage Integration Creation -> storage_aws_role_arn -> check if it matches to that of the role and Policies Trust relationship 
    -- if not change it. 


// STEP 2: CHECKING FOR COPY HISTORY

-- let's say Step 1 is okay
-- Then Check for Copy History : to see of there are any errors in the Files.


SELECT * FROM TABLE
(INFORMATION_SCHEMA.COPY_HISTORY
    (
        TABLE_NAME => 'AWS_INTEGRATION.AWSINTEGRATIONJANA.snow_pipe_emp_data',
        START_TIME => DATEADD(DAY,-1, CURRENT_TIMESTAMP)
    )
) -- Here you can check for the Error Specific to any Specific File if any

-- Let's say here you got to know there is an error in file employee_2.csv
-- you might go to step 3 to check what all are the error logs.
-- go to the source team to get the correct file.
-- once done, you will have to copy them Manually once, from next time onwards if the pattern remains same, it will work automatically.
-- Don't RUN!!

LOAD INTO table_name 
FROM @stage_name
FILE = ('employee_2.csv');



// STEP 3:  CHECKING FOR VALIDATE DATA FILES


SELECT * FROM TABLE
(INFORMATION_SCHEMA.VALIDATE_PIPE_LOAD
        (
            PIPE_NAME => 'AWS_INTEGRATION.PIPES.EMPLOYEE_PIPE',
            START_TIME => DATEADD(DAY,-1, CURRENT_TIMESTAMP)
        )
) --- NO rows because the loading was successfull, if it wasn't it would have been reflecting the rows that are error prone.


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* PIPES SPECIFIC COMMANDS*/


DESC PIPE AWS_INTEGRATION.PIPES.EMPLOYEE_PIPE; -- Check for the Definition (Code)
-- or for the notification_channel That was used to create notification.

SHOW PIPES;

SHOW PIPES LIKE '%employee%';

SHOW PIPES IN DATABASE AWS_INTEGRATION;

SHOW PIPES IN SCHEMA AWS_INTEGRATION.PIPES;

SHOW PIPES LIKE '%employee%' IN DATABASE AWS_INTEGRATION;


-- Let's say there was one snowpipe already created

Create or replace pipe snowpipe_dummy
auto_ingest = True
As
copy into table_a
from @stage_name
pattern = '*employee.*';

-- now you wish to replace the table_a with table_b

-- Step 1: Pause the Existing Snopipe

ALTER PIPE AWS_INTEGRATION.PIPES.EMPLOYEE_PIPE SET PIPE_EXECUTION_PAUSED = true;

-- Step 2: Confirm the Status

SELECT SYSTEM$PIPE_STATUS ('AWS_INTEGRATION.PIPES.EMPLOYEE_PIPE') -- Check for Execution: Paused.


-- Step 3: CREATE OR REPLACE the existing snowpipe and replace the old table_name with new if you wish to keep the same name

Create or replace pipe snowpipe_dummy
auto_ingest = True
As
copy into table_b
from @stage_name
pattern = '*employee.*';


-- else create a new pipe

-- Step 4: after the replacement all copy history the old ones get truncated.
i.e

SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY
    (
        TABLE_NAME => 'AWS_INTEGRATION.AWSINTEGRATIONJANA.snow_pipe_emp_data',
        START_TIME =>  DATEADD(DAY,-1, CURRENT_TIMESTAMP)
    )
) -- this would be null becuase existing ones gets replaced.

-- STep 5; Resume the old pipe or keep suspended.

ALTER PIPE AWS_INTEGRATION.PIPES.EMPLOYEE_PIPE SET PIPE_EXECUTION_PAUSED = FALSE; 

SELECT SYSTEM$PIPE_STATUS ('AWS_INTEGRATION.PIPES.EMPLOYEE_PIPE');

  
