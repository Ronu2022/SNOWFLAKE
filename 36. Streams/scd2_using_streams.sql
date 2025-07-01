CREATE OR REPLACE SEQUENCE seq
START = 1
INCREMENT = 1;



CREATE OR REPLACE  TABLE MYOWN_DB.STAGE_TBLS.STG_EMP_2 -- created the table in the staging schema
(     
      EMPID INT,
      EMPNAME VARCHAR(30),
      SALARY FLOAT,
      AGE INT,
      DEPT VARCHAR(10),
      LOCATION VARCHAR(20)
);



CREATE OR REPLACE STREAM MYOWN_DB.MYSTREAMS.STREAM_EMPL_2 ON TABLE MYOWN_DB.STAGE_TBLS.STG_EMP_2;

-- INSERTING RECORDS INTO STAGING TABLE:

INSERT INTO MYOWN_DB.STAGE_TBLS.STG_EMP_2 VALUES
(1, 'Amar', 80000, 35, 'SALES', 'Bangalore'),
(2, 'Bharath', 45000, 26, 'SALES', 'Hyderabad'),
(3, 'Charan', 76000, 34, 'TECHNOLOGY', 'Chennai'),
(4, 'Divya', 52000, 28, 'HR', 'Hyderabad'),
(5, 'Gopal', 24500, 22, 'TECHNOLOGY', 'Bangalore'),
(6, 'Haritha', 42000, 27, 'HR', 'Chennai');

SELECT * FROM MYOWN_DB.STAGE_TBLS.STG_EMP_2; -- 6 records.

SELECT * FROM MYOWN_DB.MYSTREAMS.STREAM_EMPL_2; -- 6 records in the stream


-- Target Table:
CREATE OR REPLACE TABLE MYOWN_DB.INTG_TBLS.TARGET_TABLE
(
    id INT DEFAULT seq.nextval,
    empID INT,
    empname VARCHAR,
    salary INT,
    AGE INT,
    dept VARCHAR,
    location varchar,
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP_NTZ,
    valid_to TIMESTAMP_NTZ DEFAULT NULL
          
);

-- 1st insert:

INSERT INTO MYOWN_DB.INTG_TBLS.TARGET_TABLE(empID,empname,salary,AGE,dept,location,valid_from)
SELECT EMPID,EMPNAME,SALARY,AGE,DEPT,LOCATION,CURRENT_TIMESTAMP
FROM MYOWN_DB.MYSTREAMS.STREAM_EMPL_2 WHERE METADATA$ACTION = 'INSERT'
AND METADATA$ISUPDATE = FALSE;

SELECT * FROM MYOWN_DB.INTG_TBLS.TARGET_TABLE; -- 6 records

SELECT * FROM MYOWN_DB.MYSTREAMS.STREAM_EMPL_2; -- all consumed hence no more records


-- Update so records in source:
Select * from MYOWN_DB.STAGE_TBLS.STG_EMP_2;
UPDATE MYOWN_DB.STAGE_TBLS.STG_EMP_2 SET empname = 'Ronu' WHERE EMPID = 1;

Select * from MYOWN_DB.STAGE_TBLS.STG_EMP_2; -- updated
SELECT * FROM MYOWN_DB.MYSTREAMS.STREAM_EMPL_2; -- captured in stream s(2 records one for insert and one for delete)
-- 1	Ronu	80000	35	SALES	Bangalore	INSERT  TRUE
-- 1	Amar	80000	35	SALES	Bangalore	DELETE  TRUE




MERGE INTO  MYOWN_DB.INTG_TBLS.TARGET_TABLE t
USING MYOWN_DB.MYSTREAMS.STREAM_EMPL_2 s ON t.EMPID = s.EMPID AND t.is_active = TRUE

WHEN MATCHED
AND s.METADATA$ACTION = 'DELETE' AND s.METADATA$ISUPDATE = FALSE 
THEN UPDATE SET
    t.is_active = False,
    t.valid_to = CURRENT_TIMESTAMP

WHEN MATCHED
AND s.METADATA$ACTION = 'INSERT' AND s.METADATA$ISUPDATE = TRUE 
THEN UPDATE SET
    t.is_active = FALSE,
    t.valid_to = CURRENT_TIMESTAMP


WHEN NOT MATCHED
AND s.METADATA$ACTION = 'INSERT'
THEN INSERT 
(
    EMPID,EMPNAME,SALARY,AGE,DEPT,LOCATION,
    VALID_FROM
) 
VALUES
(s.empid,s.empname,s.salary,s.age,s.dept,s.location,current_timestamp
)

 SELECT * FROM MYOWN_DB.INTG_TBLS.TARGET_TABLE;   -- observe amar got updated but we had a new row requirement for Ronu That didnt happen.
 -- merge in snwoflake doesnt allow for SCd2 implementation 
 -- in scd - we require two things, update the old, and insert the new so for each update there would one 
    -- olde valie be updated and the new record with the new vlue gets inserted
-- snowflake'ss merge allows one change not two

-- Thus we would require insert and update for the same
-- if we do insert and update many times, we cant help, because streams's records can be consumed only once:
-- Thus, what we can do infact is 
-- we create a temp table where we ingest the records from streams.
    -- first update the table when there is a match with emp_id, and metadtaaction = insert and isupdate = True
    -- is_active in the target table = True => for those records just set is_active = False


Create or replace procedure update_target_table()
returns string
language sql
as
$$
BEGIN

create or replace temporary table temp_stream_records as
SELECT * FROM MYOWN_DB.MYSTREAMS.STREAM_EMPL_2;

UPDATE MYOWN_DB.INTG_TBLS.TARGET_TABLE t
SET IS_ACTIVE = FALSE, VALID_TO = CURRENT_TIMESTAMP
FROM temp_stream_records s 
WHERE t.empid = s.empid
and t.is_active = TRUE
and s.METADATA$ACTION = 'DELETE' AND s.METADATA$ISUPDATE = TRUE;

insert into MYOWN_DB.INTG_TBLS.TARGET_TABLE(EMPID,EMPNAME,SALARY,AGE,DEPT,LOCATION,VALID_FROM)
select empid,empname,salary,age,dept,location,CURRENT_TIMESTAMP FROM temp_stream_records 
where METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = TRUE;


insert into MYOWN_DB.INTG_TBLS.TARGET_TABLE(EMPID,EMPNAME,SALARY,AGE,DEPT,LOCATION,VALID_FROM)
select empid,empname,salary,age,dept,location,CURRENT_TIMESTAMP FROM temp_stream_records 
where METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE;

Return 'Target table successfully updated from stream.';
END;
$$;

call update_target_table();

select * from  MYOWN_DB.INTG_TBLS.TARGET_TABLE;
