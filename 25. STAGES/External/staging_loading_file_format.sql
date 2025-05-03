USE DATABASE MYDB;

// Publicly accessible staging area 

CREATE OR REPLACE STAGE MYDB.external_stages.sample_stage
URL = 's3://bucketsnowflakes3';




// Description of external stage

DESCRIBE STAGE MYDB.external_stages.sample_stage;


// List files in stage
LIST @external_stages.sample_stage;

// Creating ORDERS table

CREATE OR REPLACE TABLE MYDB.PUBLIC.ORDERS 
(
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
);

SELECT * FROM MYDB.PUBLIC.ORDERS; -- 0 records

// LOAD using COPY COMMAND

COPY INTO MYDB.PUBLIC.ORDERS
FROM @MYDB.external_stages.sample_stage
file_format = (type = 'csv', field_delimiter = ',', skip_header = 1)
files = ('OrderDetails.csv');



// Copy command with pattern for file names

SELECT * FROM MYDB.PUBLIC.ORDERS; -- 1500+ Records

// Let's truncate
TRUNCATE MYDB.PUBLIC.ORDERS; -- 0 Records

// LOAD using COPY COMMAND and PATTERN 


COPY INTO MYDB.PUBLIC.ORDERS
FROM @MYDB.external_stages.sample_stage
file_format = (type = 'csv', field_delimiter = ',', skip_header = 1)
pattern = '.*Order.*';

SELECT * FROM MYDB.PUBLIC.ORDERS; -- 1500 records


=====================================================================================================================================================================
-- File Formats
=====================================================================================================================================================================


// Creating schema to keep file formats

CREATE OR REPLACE SCHEMA file_formats_schema;

// Creating file format object

CREATE OR REPLACE FILE FORMAT MYDB.file_formats_schema.csv_file_format;

// See properties of file format object
DESCRIBE FILE FORMAT  MYDB.file_formats_schema.csv_file_format;

// Creating table

CREATE OR REPLACE TABLE MYDB.PUBLIC.ORDERS_EX 
(
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
);

// Using file format object in Copy command     

COPY INTO MYDB.PUBLIC.ORDERS_EX 
FROM @MYDB.external_stages.sample_stage
file_format = (FORMAT_NAME = MYDB.file_formats_schema.csv_file_format)
FILES = ('OrderDetails.csv'); -- note this throws error becuase skip_header in the file_format property is missing.
-- we need to add that property because in the table amount is Int, when we have it with the header, it becomes char, thus throws error



// Altering file format object

ALTER FILE FORMAT MYDB.file_formats_schema.csv_file_format
SET SKIP_HEADER = 1, FIELD_DELIMITER = ',';

DESCRIBE FILE FORMAT MYDB.file_formats_schema.csv_file_format; -- set


// Again  Using file format object in Copy command     

COPY INTO MYDB.PUBLIC.ORDERS_EX 
FROM @MYDB.external_stages.sample_stage
file_format = (FORMAT_NAME = MYDB.file_formats_schema.csv_file_format)
FILES = ('OrderDetails.csv');

SELECT * FROM MYDB.PUBLIC.ORDERS_EX; -- all inserted







