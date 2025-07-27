

-- CREATION OF  DATABASES/SCHEMA:


CREATE DATABASE IF NOT EXISTS mydb;
CREATE SCHEMA IF NOT EXISTS mydb.external_stages;
CREATE SCHEMA IF NOT EXISTS mydb.staging;
CREATE SCHEMA IF NOT EXISTS mydb.control;
CREATE SCHEMA IF NOT EXISTS mydb.storage_schema;
-----------------------------------------------------------------------------------------------------------------------------------------------------------

-- Storage Integration

CREATE OR REPLACE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = s3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::481665128591:role/proc_load_role_class'
STORAGE_ALLOWED_LOCATIONS =(
    's3://procedureautomationjana/csv/',
    's3://procedureautomationjana/files/',
    's3://procedureautomationjana/json/',
    's3://procedureautomationjana/pipes/csv/'
    )
comment = 'This is the storage integration giving full access';

----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CREATION OF CONTROL TABLE WITH DATA LOAD DETAILS:

CREATE OR REPLACE TABLE mydb.control.copy_ctrl
(
    stage_table_name string,
    schema_name string,
    database_name string,
    storage_int string,
    storage_loc string,
    files_typ string,
    files_pattern string,
    field_delim string,
    on_error string,
    skip_header int,
    force boolean,
    truncate_cols boolean,
    is_active boolean
);



--------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CREATION OF TARGET TABLES:

-- CUSTOMER TABLE:
CREATE OR REPLACE TABLE mydb.staging.customer_data 
(
    customerid NUMBER,
    custname STRING,
    email STRING,
    city STRING,
    state STRING,
    DOB DATE
);


-- PETS TABLE:

CREATE OR REPLACE TABLE mydb.staging.pets_data_raw 
(raw_file variant);



-- EMP TABLE:
CREATE OR REPLACE TABLE mydb.staging.emp_data 
(
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  location STRING,
  department STRING
);

-------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- INSERTION INTO CONTROL TABLE:

SELECT * FROM mydb.control.copy_ctrl; -- 0 records.
TRUNCATE TABLE  mydb.control.copy_ctrl; -- 0 records

select  * from mydb.control.copy_ctrl;

INSERT INTO mydb.control.copy_ctrl VALUES
('customer_data','staging','mydb','s3_int','s3://procedureautomationjana/csv/','CSV','.*customer.*','|','CONTINUE',1,True,True, True),
('pets_data_raw','staging','mydb','s3_int','s3://procedureautomationjana/json/','json','.*pets_data.*',Null,'CONTINUE',NULL,True, True,True),
('emp_data','staging','mydb','s3_int','s3://procedureautomationjana/pipes/csv/','csv','.*sp_employee.*',',','CONTINUE',1,True, True, True);

SELECT * FROM mydb.control.copy_ctrl;

select * from table(result_scan(last_query_id()));

select files_pattern, replace(files_pattern,'.*','') as files_changed from mydb.control.copy_ctrl;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE mydb.staging.automate_cop()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE as caller as 
DECLARE
    curs CURSOR FOR SELECT * FROM  mydb.control.copy_ctrl WHERE is_active= True;
    tbl String;
    sch string; 
    db string; 
    st_int string; 
    st_loc string; 
    file_typ string; 
    file_pat string;
    fld_dlm string;
    skp_hdr string; 
    forc string;
    on_err string;
    trun_col string; 
    cnt integer;
    ret string;
    file_format1 string; 
    copy_stmt string;
    create_stage_stmt string;
    fp string;

    begin
        ret := '';
    for rec in curs
    do
        tbl := rec.stage_table_name;
        ch := rec.SCHEMA_NAME;
        db := rec.DATABASE_NAME;
        st_int := rec.storage_int;
        st_loc := rec.storage_loc;
        file_typ := rec.files_typ;
        file_pat := rec.files_pattern;
        fld_dlm := rec.field_delim;
        skp_hdr := rec.skip_header;
        forc := rec.force;
        on_error := rec.on_error;
        trun_col := rec.truncate_cols;

    if(:file_typ = 'csv') then 
        file_format1 := '(type='||:file_typ||' skip_header='||:skp_hdr||' field_delimiter=\''||:fld_dlm||'\' empty_field_as_null = TRUE)';
    else
        file_format1 :=  '(type='||:file_typ||')';
    end if; 

    create_stage_stmt := 
    'create or replace temporary stage mydb.external_stages.s3_stage' ||
    ' url = ''' || :st_loc || ''' ' ||
    'storage_integration = ''' || :st_int || ''' ' ||
    'file_format = ''' || :file_format1 || ''' ';


    execute immediate create_stage_stmt; 

    fp := replace(:file_pat,'.*','');

    LIST @mydb.external_stages.s3_stage;

    SELECT COUNT(1) INTO cnt FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (:cnt > 0) then 
    copy_stmt := 'COPY INTO '||:db||'.'||:sch||'.'||:tbl || '
		FROM @mydb.external_stages.s3_stage
		pattern = \'' || :file_pat || '\' 
		ON_ERROR = ' || :on_err  || '
		FORCE = ' || :forc  || '
		TRUNCATECOLUMNS = ' || :trun_col ;
    execute immediate copy_stmt;
    ret := :ret || :fp || 'Format files completed successfully.'
    else
        ret := ret|| :fp || 'successful not prtesent'
    end if; 
end for; 
end; 

call  mydb.staging.automate_cop;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Automation:


-- Automate this process to run every day
CREATE OR REPLACE TASK staging.task_automate_data_load
SCHEDULE = 'USING CRON 0 7 * * * UTC'
AS
call mydb.staging.sp_automate_data_copy()
;

-- Process different files at diff timings
CREATE OR REPLACE PROCEDURE mydb.staging.sp_automate_data_copy("table_name" varchar)

curs cursor for SELECT * FROM mydb.control.copy_ctrl WHERE is_active = True and stage_table_name=:table_name;

-- every day at 8 am
CREATE OR REPLACE TASK staging.task_automate_data_load
SCHEDULE = 'USING CRON 0 8 * * * UTC'
AS
call mydb.staging.sp_automate_data_copy('customer_data')
;

-- every day at 10 am and 10 pm
CREATE OR REPLACE TASK staging.task_automate_data_load
SCHEDULE = 'USING CRON 0 10,22 * * * UTC'
AS
call mydb.staging.sp_automate_data_copy('emp_data')
;

-- every monday at 6.30 am
CREATE OR REPLACE TASK staging.task_automate_data_load
SCHEDULE = 'USING CRON 30 6 * * 1 UTC'
AS
call mydb.staging.sp_automate_data_copy('pets_data_raw')
;

---------------------------------------------------------------------------------------------------------------------------------------------------------
        
    
