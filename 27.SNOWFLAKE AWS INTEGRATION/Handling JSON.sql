// STORAGE INTEGRATION ( REMAINS SAME AS THE BEFORE)

CREATE OR REPLACE STORAGE INTEGRATION s3_int_awsintegrationjana
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = s3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::481665128591:role/S3fullAccess'
STORAGE_ALLOWED_LOCATIONS = ('s3://awsintegrationjana/csv/','s3://awsintegrationjana/json/')
COMMENT = 'Integration With s3 Bucket for Folder awsintegrationjana';



// DESC

DESC INTEGRATION  s3_int_awsintegrationjana;

// CREATION OF FILE FORMAT

USE DATABASE AWS_INTEGRATION;
USE SCHEMA AWS_INTEGRATION.AWSINTEGRATIONJANA;

CREATE OR REPLACE FILE FORMAT  AWS_INTEGRATION.AWSINTEGRATIONJANA.s3_int_awsintegrationjana_file_format_JSON
TYPE = 'json';



// CREATION OF STAGE

CREATE OR REPLACE STAGE AWS_INTEGRATION.AWSINTEGRATIONJANA.s3_int_awsintegrationjana_stage_json
URL = 's3://awsintegrationjana/json/'
STORAGE_INTEGRATION = s3_int_awsintegrationjana
FILE_FORMAT = AWS_INTEGRATION.AWSINTEGRATIONJANA.s3_int_awsintegrationjana_file_format_JSON;


// LIST THE STAGE:

LIST @AWS_INTEGRATION.AWSINTEGRATIONJANA.s3_int_awsintegrationjana_stage_json;


// CREATION OF TABLE

CREATE OR REPLACE TABLE AWS_INTEGRATION.AWSINTEGRATIONJANA.json_table
(
    raw_data VARIANT
);


// LIST THE STAGE:

LIST @AWS_INTEGRATION.AWSINTEGRATIONJANA.s3_int_awsintegrationjana_stage_json;


// CHECKING IF THE DATA IS LOADED OR NOT

SELECT $1
FROM @AWS_INTEGRATION.AWSINTEGRATIONJANA.s3_int_awsintegrationjana_stage_json/4Wheelers.json
(FILE_FORMAT => AWS_INTEGRATION.AWSINTEGRATIONJANA.s3_int_awsintegrationjana_file_format_JSON);


// COPYING THE DATA USING COPY COMAND

COPY INTO AWS_INTEGRATION.AWSINTEGRATIONJANA.json_table
FROM @AWS_INTEGRATION.AWSINTEGRATIONJANA.s3_int_awsintegrationjana_stage_json/4Wheelers.json
FILE_FORMAT = AWS_INTEGRATION.AWSINTEGRATIONJANA.s3_int_awsintegrationjana_file_format_JSON


// CHecking:

SELECT * FROM AWS_INTEGRATION.AWSINTEGRATIONJANA.json_table;


--Extracting single column


SELECT 
    RAW_DATA:"Car Model"::STRING as car_model,
    RAW_DATA:"Car Model Year"::STRING AS year,
    RAW_DATA:"car make"::STRING AS Car_product,
    RAW_DATA:"id"::STRING AS ID,
    RAW_DATA:"first_name"::STRING AS first_name,
    RAW_DATA:"last_name"::STRING AS last_name
FROM AWS_INTEGRATION.AWSINTEGRATIONJANA.json_table;


