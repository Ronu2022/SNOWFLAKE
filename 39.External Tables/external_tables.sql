// EXTERNAL TABLES:

-- allow you to query the files stores in external stage like a regular table => without moving files to snowflake tables.
-- external tables access the files stored in external stage area such as Amazon s3, GCP buckt or Azure blob stopragfe.
-- External Tables styore mnetadata abvout these datra files such as value(complete record) the file name and file row number.
-- They are read-only therefore no DML operations can be performed on them.
-- we can use external tables for query and join operations.
-- Views and materialized views can be created against external tables.
-- querying data from external tables is likely slower than querying database tables.
-- Advantage of external Table is you can analyze the data without storing it in snwoflake.


// EXTERNAL TABNLE STORES THE FOLLOWING METADATA:

-- Value - a variant type column that represents a single row in the external file.

-- METADATA$FILENAME: A pseudocolumn that identifies the name of each staged data file included in the external table including
-- its path in the stage.

-- METADATA$FILE_ROW_NUMBER: Shwos the row number of each staged data file. 



// CREATION OF EXTERNAL TABLES:

-- Create FILE  FORMAT object.
-- create STAGE object referring to cloud storage location.
-- create EXTERNAL Table.

-- NOTE: if therfe are multiple files of same pattern, we can just create one External Table and it can store the details of all similar files.



// EXAMPLE:

/*
  CREATE OR REPLACE ECTERNAL TABLE table_name
  (ID INT AS (VALUE:C1::INT),
  NAME VHARCHAR(20) AS (VALUE:C2::VARCHAR),
  DEPT INT AS (VALUE:C3::INT)
  )
  WITH LOCATION = @MYSTAGE
  FILE_FORMAT = MYFORMAT_NAME;
*/




--------------------------------------------------------------------------------------------------------------------------------------

// REFRESXHING EXTYERNAL TABLES:

-- External Tables will be refreshed automatically.
-- The refresh operation syunchornizes the metdata with the latest set of associated files in exterenal staghe and path, i/.e:
    -- new files in thge path are added to the table metadata.
    -- changes to files on the path are updated in the table metadata.
    -- files no longer in the path are removed from the table metadata.
    

-----------------------------------------------------------------------------------------------------------------------------------

CREATE DATABASE EXTERNAL_TABLES;


CREATE SCHEMA EXT_STAGES;

CREATE SCHEMA FILE_FORMATS;

CREATE SCHEMA EXT_TABLES;


// FILE FORMAT:

CREATE OR REPLACE FILE FORMAT EXTERNAL_TABLES.FILE_FORMATS.CSV_FILEFORMAT
TYPE = 'CSV'
FIELD_DELIMITER = '|'
SKIP_HEADER = 1
EMPTY_FIELD_AS_NULL = TRUE;


// CREATE STAGE OBJECT WITH INTEGRATION OBJECT 

CREATE OR REPLACE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::481665128591:role/unloading_role' 
-- unloading_role is just the name of the role => don't get confused
STORAGE_ALLOWED_LOCATIONS = ('s3://awsintegrationjana/csv/')
COMMENT = 'Storage Integration';

// DESC Storage integration:

DESC STORAGE INTEGRATION s3_int; -- STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::663590917978:user/hl231000-s
 -- COPY PASTE IN THE TRUST RELATIONSHIP AREA OF THE ROLE.

// STAGE CREATION:

CREATE OR REPLACE STAGE EXTERNAL_TABLES.EXT_STAGES.MYS3_STAGE
URL = 's3://awsintegrationjana/csv/'
STORAGE_INTEGRATION = s3_int
FILE_FORMAT = EXTERNAL_TABLES.FILE_FORMATS.CSV_FILEFORMAT;

// list:

LIST @EXTERNAL_TABLES.EXT_STAGES.MYS3_STAGE;

// ACCESSING:

SELECT t.$1 as id,t.$2 as name, t.$3  as email_id FROM 
@EXTERNAL_TABLES.EXT_STAGES.MYS3_STAGE/customer_data_2020.csv 
(file_format => EXTERNAL_TABLES.FILE_FORMATS.CSV_FILEFORMAT) AS t;


select t.* from @EXTERNAL_TABLES.EXT_STAGES.MYS3_STAGE/customer_data_2020.csv 
(file_format => EXTERNAL_TABLES.FILE_FORMATS.CSV_FILEFORMAT) as t; -- will not workl you have to select columns


// CREATING EXTERNAL_TABLE:

CREATE OR REPLACE EXTERNAL TABLE EXTERNAL_TABLES.EXT_TABLES


 
CREATE OR REPLACE EXTERNAL TABLE EXTERNAL_TABLES.EXT_TABLES.CX_TABLE
(
    cust_id NUMBER as (VALUE:c1::NUMBER),
    cx_name VARCHAR AS (VALUE:c2::VARCHAR),
    cx_email VARCHAR as (VALUE:c3::VARCHAR),
    cx_city VARCHAR AS (VALUE:c4::VARCHAR),
    cx_state VARCHAR AS (VALUE:c5::VARCHAR),
    cx_DOB DATE AS (VALUE:c6::DATE)
)
WITH 
LOCATION = @EXTERNAL_TABLES.EXT_STAGES.MYS3_STAGE/customer_data_2020.csv 
FILE_FORMAT = EXTERNAL_TABLES.FILE_FORMATS.CSV_FILEFORMAT;


SELECT * FROM EXTERNAL_TABLES.EXT_TABLES.CX_TABLE;

ALTER EXTERNAL TABLE EXTERNAL_TABLES.EXT_TABLES.CX_TABLE REFRESH;


-- no results yet:



CREATE OR REPLACE EXTERNAL TABLE EXTERNAL_TABLES.EXT_TABLES.CX_TABLE
(
    cust_id NUMBER as (VALUE:c1::NUMBER),
    cx_name VARCHAR AS (VALUE:c2::VARCHAR),
    cx_email VARCHAR as (VALUE:c3::VARCHAR),
    cx_city VARCHAR AS (VALUE:c4::VARCHAR),
    cx_state VARCHAR AS (VALUE:c5::VARCHAR),
    cx_DOB DATE AS (VALUE:c6::DATE)
)
WITH 
LOCATION = @EXTERNAL_TABLES.EXT_STAGES.MYS3_STAGE
PATTERN = '.*customer_data_2020.csv.*'
FILE_FORMAT = EXTERNAL_TABLES.FILE_FORMATS.CSV_FILEFORMAT;


SELECT * FROM EXTERNAL_TABLES.EXT_TABLES.CX_TABLE;


// TO SEE EXTERNAL TABLE:

DESC EXTERNAL TABLE EXTERNAL_TABLES.EXT_TABLES.CX_TABLE TYPE = 'COLUMN';
DESC EXTERNAL TABLE EXTERNAL_TABLES.EXT_TABLES.CX_TABLE TYPE = 'STAGE';


SELECT * FROM EXTERNAL_TABLES.EXT_TABLES.CX_TABLE SAMPLE(10);
SELECT * FROM EXTERNAL_TABLES.EXT_TABLES.CX_TABLE SAMPLE BERNOULLI(10);
SELECT * FROM EXTERNAL_TABLES.EXT_TABLES.CX_TABLE SAMPLE SYSTEM(10);

SELECT * FROM EXTERNAL_TABLES.EXT_TABLES.CX_TABLE SAMPLE SYSTEM (10) SEED (111);


// TO SEE THE FILE NAME IT IS REFFERING To

SELECT DISTINCT METADATA$FILENAME FROM EXTERNAL_TABLES.EXT_TABLES.CX_TABLE; --csv/customer_data_2020.csv

SELECT DISTINCT METADATA$FILENAME FROM EXTERNAL_TABLES;
; 


// You Cran create views (both secure as well as materialized):

create or replace secure  view viw_name as
select t1.cx_f, count(distinct t2.oder_id)
from externaL_table t1
left join oexternal_table t2 on t1.cx_id = t2.cx_id
group by t1.cx_f 
having count(distinct t2.oder_id) > 2; 



SELECT METADATA$FILENAME, t.* FROM EXTERNAL_TABLES.EXT_TABLES.CX_TABLE t; -- displays everything metadata$filename, value

-- Note: value is by default created once an external table is created 
-- but for thje Metadata$filename we need to hgave it in the  table ; 











