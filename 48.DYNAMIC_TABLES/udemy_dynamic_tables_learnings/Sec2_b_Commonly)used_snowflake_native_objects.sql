DEMO_DB.PUBLIC.CSV_DOUBLE_Q_FFDEMO_DB.PUBLIC.CSV_DOUBLE_Q_FF-- File Formats 
    -- Snowflake supports 6 different type of file formats
    -- csv (delimited file formats) (tsv etc)
    -- JSON
    -- Parquet
    -- Avro
    -- ORC
    -- XML

use role sysadmin;
use database demo_db;
use schema public;


// CSV FILE FORMAT:
create or replace file format demo_db.public.csv_simple_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ',' 
    record_delimiter = '\n' 
    skip_header = 1;


-- with double quote  
create or replace file format demo_db.public.csv_double_q_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ',' 
    record_delimiter = '\n' 
    skip_header = 1 
    field_optionally_enclosed_by = '\042' 
    trim_space = false 
    error_on_column_count_mismatch = true;


-- with single quote
create or replace file format demo_db.public.csv_single_q_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ',' 
    record_delimiter = '\n' 
    skip_header = 1 
    field_optionally_enclosed_by = '\047' 
    trim_space = false 
    error_on_column_count_mismatch = true;



-- other types
create or replace file format demo_db.public.json_ff 
    type = 'JSON' ;


create or replace file format demo_db.public.parquet_ff 
    type = 'Parquet' ;


create or replace file format demo_db.public.avro_ff 
    type = 'AVRO' ;

create or replace file format demo_db.public.orc_ff 
type = 'ORC' ;


-- desc file formats
desc file format demo_db.public.csv_single_q_ff;


    
// LAST WS, we loaded some customer data;

list @DEMO_DB.PUBLIC.MY_INT_STAGE;

select t.$1, t.$2, t.$3, t.$4, t.$5,t.$6,t.$7
from @DEMO_DB.PUBLIC.MY_INT_STAGE/customer/india/csv/
(file_format => demo_db.public.csv_simple_ff )t;

select t.$1, t.$2, t.$3, t.$4, t.$5,t.$6,t.$7
from @DEMO_DB.PUBLIC.MY_INT_STAGE/customer/india/csv/
(file_format => demo_db.public.csv_double_q_ff  )t;

select t.$1, t.$2
from @stage_name/csv
(file_format => )t; 

