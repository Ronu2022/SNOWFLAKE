

USE DATABASE MYDB;
USE SCHEMA MYDB.INTERNAL_STAGES;

// creation of Stage (Named Stage):
CREATE OR REPLACE STAGE demo_infer_schema_internal_stage;

// LISTING:

LIST @MYDB.INTERNAL_STAGES.DEMO_INFER_SCHEMA_INTERNAL_STAGE; -- Files Uploaded Successfully
-- We could have done that one by one using snowsql as well
-- PUT file://file_path @my_db.schema.list_name

DROP FILE FORMAT file_format_csv_comma_infer_schema;

// Creating a File Format For CSV files:
CREATE OR REPLACE FILE FORMAT dummy_file_format
TYPE = 'csv'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;


// Accessing some columns
SELECT $1, $2
FROM @demo_infer_schema_internal_stage/csv/customer_data_2021.csv
(FILE_FORMAT => dummy_file_format);



-- Note Accessing all columns isn't feasible, what if the file had 20 columns in it ?
-- You can't just do trial and error.










// FILE FORMATS:

-- Csv with delimiter ',' default is comma hence didnt specify.
CREATE OR REPLACE FILE FORMAT MYDB.INTERNAL_STAGES.csv_format
TYPE= 'csv'
PARSE_HEADER = TRUE;

-- with delijiter = | for CSV
CREATE OR REPLACE FILE FORMAT MYDB.INTERNAL_STAGES.csv_format_pipe
TYPE= 'CSV' 
PARSE_HEADER = TRUE
FIELD_DELIMITER = '|';

-- with JSON format.
CREATE OR REPLACE FILE FORMAT MYDB.INTERNAL_STAGES.JSON_FORMAT
TYPE = 'json';


---------------------------------------------------------------------------------------------------------------------------------------------------------------
-- csv with filed delim as ,

-- way 1:
SELECT * FROM TABLE
(
    INFER_SCHEMA
    (  
        LOCATION => '@MYDB.INTERNAL_STAGES.DEMO_INFER_SCHEMA_INTERNAL_STAGE', FILE_FORMAT => 'MYDB.INTERNAL_STAGES.csv_format',
        FILES => 'csv/customer_data_2021.csv'
    )
); -- Note perhaps the delimiter is mismatched hence everything comes in one row.


-- way 2

SELECT * FROM TABLE
(
    INFER_SCHEMA
    (  
        LOCATION => '@demo_infer_schema_internal_stage/csv/customer_data_2021.csv', FILE_FORMAT => 'MYDB.INTERNAL_STAGES.csv_format'
    )
);



-- so from here you can check the details of the columns



-- ALSO, HERE FILE_FORMAT MUST BE DEFINED BEFORE.
-- REMEMBER:  for select statements, you would need skip_header in the file_format, but to check the metadata, you would need parse_header in the file format creation.

SELECT * FROM TABLE
(
    INFER_SCHEMA
    (  
        LOCATION => '@demo_infer_schema_internal_stage/csv/customer_data_2021.csv', FILE_FORMAT => (type = 'csv', field_delimiter = ',',parse_header = TRUE)
    )
);



LIST @MYDB.INTERNAL_STAGES.DEMO_INFER_SCHEMA_INTERNAL_STAGE;

SELECT * FROM TABLE
(
    INFER_SCHEMA
        (
            LOCATION => '@demo_infer_schema_internal_stage/csv/employee_data.csv'
            ,FILE_FORMAT => 'MYDB.INTERNAL_STAGES.csv_format'
        )
) -- delimiter matches hence each column_name has a row for it.
-------------------------------------------------------------------------------------------------------------------------------------------------------------


-- csv with filed delim as |

LIST @MYDB.INTERNAL_STAGES.DEMO_INFER_SCHEMA_INTERNAL_STAGE;

SELECT * FROM TABLE
(
    INFER_SCHEMA
        (
            LOCATION => '@demo_infer_schema_internal_stage/csv/customer_data_2021.csv'
            ,FILE_FORMAT => 'MYDB.INTERNAL_STAGES.csv_format_pipe'
        )
)


-------------------------------------------------------------------------------------------------------------------------------------------------------------


-- csv with filed delim as ,

LIST @MYDB.INTERNAL_STAGES.DEMO_INFER_SCHEMA_INTERNAL_STAGE;

SELECT * FROM TABLE
(
    infer_schema
    (
        location => '@demo_infer_schema_internal_stage/csv/customers_details.csv',
        file_format => 'MYDB.INTERNAL_STAGES.csv_format'
    )
)

-------------------------------------------------------------------------------------------------------------------------------------------------------------

-- -- csv with filed delim as ,
LIST @MYDB.INTERNAL_STAGES.DEMO_INFER_SCHEMA_INTERNAL_STAGE;

SELECT * FROM TABLE
(
    infer_schema
    (
        location => '@demo_infer_schema_internal_stage/csv/business-employment-data-sep-2024-quarter.csv',
        file_format => 'MYDB.INTERNAL_STAGES.csv_format'
    )
)

-------------------------------------------------------------------------------------------------------------------------------------------------------------


-- create table with extracted metadata

CREATE OR REPLACE TABLE MYDB.INTERNAL_STAGES.METADATA_TEMPLATE_TABLE USING TEMPLATE
(
    SELECT * FROM TABLE
    (
        INFER_SCHEMA
        (
            LOCATION => '@demo_infer_schema_internal_stage/csv/',
            file_format => 'MYDB.INTERNAL_STAGES.csv_format',
            files => 'business-employment-data-sep-2024-quarter.csv'
        )
    )
); -- this won't work




 -- WORKED

SELECT * FROM MYDB.INTERNAL_STAGES.METADATA_TEMPLATE_TABLE;
LIST @demo_infer_schema_internal_stage/csv/business-employment-data-sep-2024-quarter.csv;

SELECT $1, $2, $3, $4, $5,$6 
From @demo_infer_schema_internal_stage/csv/business-employment-data-sep-2024-quarter.csv
 (FILE_FORMAT => dummy_file_format) -- Has arecords



 // Inserting the Metadata into a table


 CREATE OR REPLACE TABLE meta_data_table 
(
    json_data ARRAY
);

// Inserting the data

INSERT INTO meta_data_table

SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM
(

SELECT * FROM  TABLE 
(
    INFER_SCHEMA
    (
        LOCATION => '@demo_infer_schema_internal_stage/csv/business-employment-data-sep-2024-quarter.csv',
        FILE_FORMAT => 'MYDB.INTERNAL_STAGES.csv_format'
    )
)
)


-- This wont work.
CREATE OR REPLACE TABLE abc USING Template
(
select array_agg(object_construct(*)) as json_data from
(
 SELECT * 
FROM TABLE (
  INFER_SCHEMA
  (
    LOCATION => '@demo_infer_schema_internal_stage/csv/business-employment-data-sep-2024-quarter.csv',
    FILE_FORMAT => 'MYDB.INTERNAL_STAGES.csv_format'
  )
)
)
) -- MARK: TEMPLATE doesnt work with arrray_agg and object_construct





-----------------------------------------------------------------------------------------------------------------------------------------------------------

/* ARRAY_AGG */

-- is an aggregate function, used to agg multiple rows of data into a single array.

-- Create the employees table
CREATE OR REPLACE TABLE employees_array_agg_object_construct 
(
    id INT,
    name STRING,
    department STRING
);

-- Insert some sample records into the employees table
INSERT INTO employees_array_agg_object_construct (id, name, department) 
VALUES 
(1, 'Alice', 'HR'),
(2, 'Bob', 'IT'),
(3, 'Charlie', 'Finance');

SELECT ARRAY_AGG(name) as name_array from employees_array_agg_object_construct; -- converts into a list

SELECT OBJECT_CONSTRUCT('ID',id, 'Name',name, 'department',department) as employee_construct
from employees_array_agg_object_construct; -- Creates a Json type Object


Select object_construct(*) from employees_array_agg_object_construct; 
-- Creates an object for each row, so you get a separate object per row.Example: 3 rows → 3 individual objects.
Select array_agg(object_construct(*)) from employees_array_agg_object_construct;  
-- Aggregates all the objects from each row into a single array.Example: 3 rows → 1 array containing 3 objects.



---------------------------------------------------------------------------------------------------------------------------------------------------------


-- Getting Json Files:

LIST @MYDB.INTERNAL_STAGES.DEMO_INFER_SCHEMA_INTERNAL_STAGE;

// Selcting using INFER_SCHEMA
SELECT * FROM TABLE
(
    INFER_SCHEMA
    (
        LOCATION => '@demo_infer_schema_internal_stage/json/socialmedia.json',
        FILE_FORMAT => 'MYDB.INTERNAL_STAGES.JSON_FORMAT'
    )
)



CREATE OR REPLACE TABLE social_media_array_agg_object_construct
(
    details ARRAY
);


-- iNSERTION
INSERT INTO social_media_array_agg_object_construct

SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
FROM
(
    SELECT * FROM TABLE
        (
            INFER_SCHEMA
            (
                LOCATION => '@demo_infer_schema_internal_stage/json/socialmedia.json',
                FILE_FORMAT => 'MYDB.INTERNAL_STAGES.JSON_FORMAT'
            )
        )
)



select * from social_media_array_agg_object_construct;


---------------------------------------------------------------------------------------------------------------------------------------------------------


-- bUT LET'S ASSUME IN A FILE THERE ARE 100 COLUMNS:
-- YOU WANT TO CREATE A TEMPLATE WITHOUT RECORDS 



// Creating the table:

CREATE OR REPLACE TABLE MYDB.INTERNAL_STAGES.TEMPLATE_TABLE USING TEMPLATE

(
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE
    (
      INFER_SCHEMA
      (
        LOCATION => '@demo_infer_schema_internal_stage/csv/business-employment-data-sep-2024-quarter.csv',
        FILE_FORMAT => 'MYDB.INTERNAL_STAGES.csv_format'
      )
    )    
)


-- Checking the entries:
SELECT * FROM MYDB.INTERNAL_STAGES.TEMPLATE_TABLE;



















