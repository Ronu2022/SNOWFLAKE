// Step 1: S3

-- Search s3
-- Create Folders
-- Upload Files.

// Step 2: Roles

-- IAM (Search)
-- Roles 
-- Create Role (Fill details)

// Step 3: Role Policy:

-- Select the created Role
-- fill details
-- Create role
-- agter creation => Select the role policy and Copythe ARN and keep it handy
-- also observe the Trust ploicy


// Step 5: Creation of Storage Integration


CREATE OR REPLACE DATABASE AWS_INTEGRATION;

CREATE OR REPLACE SCHEMA awsintegrationjana;


//Creation of storage integration object:

CREATE OR REPLACE STORAGE INTEGRATION s3_int_awsintegrationjana
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = s3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::481665128591:role/S3fullAccess'
STORAGE_ALLOWED_LOCATIONS = ('s3://awsintegrationjana/csv/','s3://awsintegrationjana/json/')
COMMENT = 'Integration With s3 Bucket for Folder awsintegrationjana';


// ARN -- Amazon Resource Names
// S3 -- Simple Storage Service
// IAM -- Integrated Account Management



// STEP - 6: GET THE ID to update it on AWS

DESC INTEGRATION  s3_int_awsintegrationjana;

-- Select STORAGE_AWS_IAM_USER_ARN and Copy the Property Value


// Step 7: Updating the Role Policy

-- go to roles the created policy
-- Check for Update Trust Policy and the ARN Replace with this



// Step 8: Creation of File Object

CREATE OR REPLACE FILE FORMAT s3_int_awsintegrationjana_file_format
type = 'csv'
field_delimiter = '|'
skip_header = 1
empty_field_as_null = TRUE;


CREATE OR REPLACE FILE FORMAT s3_int_awsintegrationjana_file_format_parse_header
type = 'csv'
field_delimiter = '|'
parse_header = TRUE;


//Step 9:  Creation of Stage Object



CREATE OR REPLACE STAGE s3_int_awsintegrationjana_stage
URL = 's3://awsintegrationjana/csv/'
STORAGE_INTEGRATION = s3_int_awsintegrationjana
FILE_FORMAT = AWS_INTEGRATION.AWSINTEGRATIONJANA.S3_INT_AWSINTEGRATIONJANA_FILE_FORMAT;


-- If you wish to access ine of the file.
SELECT 
$1,
$2,
$3
FROM @AWS_INTEGRATION.AWSINTEGRATIONJANA.S3_INT_AWSINTEGRATIONJANA_STAGE/customer_data_2020.csv
(file_format => s3_int_awsintegrationjana_file_format);


LIST @AWS_INTEGRATION.AWSINTEGRATIONJANA.S3_INT_AWSINTEGRATIONJANA_STAGE;



// Step 10:  Now we have to create a table to copy the data into 


CREATE OR replace TABLE AWS_INTEGRATION.AWSINTEGRATIONJANA.Customer_Table 
(
    customerid NUMBER,
    custname STRING,
    email STRING,
    city STRING,
    state STRING,
    DOB DATE
);



// Step 11:  Use Copy command to load the files

COPY INTO AWS_INTEGRATION.AWSINTEGRATIONJANA.Customer_Table 
FROM @AWS_INTEGRATION.AWSINTEGRATIONJANA.S3_INT_AWSINTEGRATIONJANA_STAGE
PATTERN = '.*customer.*';


SELECT * FROM AWS_INTEGRATION.AWSINTEGRATIONJANA.Customer_Table;



-- Note: Just to confirm what are the available column Names, 
-- You can use the INFER_SCHEMA (before that you have to create a file format with parse_header)


SELECT * FROM TABLE
(
    INFER_SCHEMA
    (
        LOCATION => '@AWS_INTEGRATION.AWSINTEGRATIONJANA.S3_INT_AWSINTEGRATIONJANA_STAGE/customer_data_2020.csv',
        file_format => 'AWS_INTEGRATION.AWSINTEGRATIONJANA.S3_INT_AWSINTEGRATIONJANA_FILE_FORMAT_PARSE_HEADER'
        --FILES => 'customer_data_2020.csv'
    )
); -- 5 columns





LIST @AWS_INTEGRATION.AWSINTEGRATIONJANA.S3_INT_AWSINTEGRATIONJANA_STAGE


Select * from table
(
    INFER_SCHEMA
    (
        location => '@AWS_INTEGRATION.AWSINTEGRATIONJANA.S3_INT_AWSINTEGRATIONJANA_STAGE/customer_data_2020.csv',
        file_format => 'AWS_INTEGRATION.AWSINTEGRATIONJANA.S3_INT_AWSINTEGRATIONJANA_FILE_FORMAT_PARSE_HEADER'
    )
)








