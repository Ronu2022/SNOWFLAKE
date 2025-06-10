-- ========
-- Streams 
--========

CREATE OR REPLACE DATABASE MYOWN_DB;

// SCHEMA FOR STREAMS:

CREATE OR REPLACE SCHEMA mystreams;

//SCHEMA FOR SOURCE TABLE +> STAGING TABLES;

CREATE OR REPLACE SCHEMA STAGE_TBLS;

// SCHEMA FOR TARGET TABLES: 

CREATE OR REPLACE SCHEMA INTG_TBLS;


// CREATION OF A SAMPLE TABLE => STAGE TABLE IN THE STAGE SCHEMA.


CREATE OR REPLACE  TABLE STAGE_TBLS.STG_EMPL -- created the table in the staging schema
(     
      EMPID INT,
      EMPNAME VARCHAR(30),
      SALARY FLOAT,
      AGE INT,
      DEPT VARCHAR(10),
      LOCATION VARCHAR(20)
);


// STREAM CREATION ON THE SOURCE TABLE

CREATE STREAM MYSTREAMS.STREAM_EMPL ON TABLE STAGE_TBLS.STG_EMPL;

SHOW STREAMS IN SCHEMA mystreams;


SELECT * FROM mystreams.STREAM_EMPL; -- currently no records because the table on which it was applied has no insertions yet.

// CREATION OF TARGET TABLE:


CREATE TABLE INTG_TBLS.EMPL
( EMPID INT,
  EMPNAME VARCHAR(30),
  SALARY FLOAT,
  AGE INT,
  DEPT VARCHAR(15),
  LOCATION VARCHAR(20),
  INSRT_DT DATE,
  LST_UPDT_DT DATE DEFAULT NULL -- deliberately kept null -> such that whenever there is a change in the source table, this column will capture the date there i.e current date.
);


-- =============
// ONLY INSERTS INTO THE SATGING/SOURCE TABLE
-- =============
SELECT * FROM STAGE_TBLS.STG_EMPL; -- 0 records


INSERT INTO STAGE_TBLS.STG_EMPL VALUES
(1, 'Amar', 80000, 35, 'SALES', 'Bangalore'),
(2, 'Bharath', 45000, 26, 'SALES', 'Hyderabad'),
(3, 'Charan', 76000, 34, 'TECHNOLOGY', 'Chennai'),
(4, 'Divya', 52000, 28, 'HR', 'Hyderabad'),
(5, 'Gopal', 24500, 22, 'TECHNOLOGY', 'Bangalore'),
(6, 'Haritha', 42000, 27, 'HR', 'Chennai');


SELECT * FROM STAGE_TBLS.STG_EMPL; -- 6records. ``


// CHECK THE STREAM OBJECT:

SELECT * FROM MYSTREAMS.STREAM_EMPL;             

--  METADATA$ACTION; METADATA$ISUPDATE; METADATA$ROW_ID; ALL 3 are showing up.
-- BUT the moment these info are used to populate the target table, it gets truncated or refreshed, and gets populated with fresh set of records.



// CONSUME THE STREAM OBJECT AND LOAD THE CONTENTS INTO A TARGET TABLE:

SELECT * FROM MYSTREAMS.STREAM_EMPL;    -- there were entries of the inserted records.

SELECT * FROM INTG_TBLS.EMPL; -- No Records

INSERT INTO INTG_TBLS.EMPL
(EMPID,EMPNAME,SALARY,AGE,DEPT,LOCATION,INSRT_DT)

SELECT EMPID,
       EMPNAME, 
       SALARY,
       AGE,
       DEPT,
       LOCATION,
      CURRENT_DATE
FROM MYSTREAMS.STREAM_EMPL WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE; 



SELECT * FROM MYSTREAMS.STREAM_EMPL;  -- observe the stream is truncated now.


-- ==================================================================================================================================
-- Only Updates: LET'S UPDATE SOME RECORDS IN THE STAGING TABLE:
-- ==================================================================================================================================


SELECT * FROM STAGE_TBLS.STG_EMPL;

// Update 2 records in stage table:

UPDATE STAGE_TBLS.STG_EMPL SET SALARY=49000 WHERE EMPID=2;

UPDATE STAGE_TBLS.STG_EMPL SET LOCATION='Pune' WHERE EMPID=5;


-- Check the stage Table:

SELECT * FROM STAGE_TBLS.STG_EMPL; -- updated.

-- Check stream Object:

SELECT * FROM MYSTREAMS.STREAM_EMPL; 
-- observe for each update there are two records one with METADATA$ACTION = DELETE and METADATA$ISUPDATE = TRUE
-- the other METADATA$ACTION = INSERT and METADATA$ISUPDATE = TRUE => beause update =>deletion followed by update (logically)


// Consume stream object and merge into final table => for Consuming UPDATES we use MERGE

SELECT * FROM MYSTREAMS.STREAM_EMPL; -- 4 4record (2 for each update)
SELECT * FROM INTG_TBLS.EMPL; -- old records.

-- MERGE OPERATION:

MERGE INTO INTG_TBLS.EMPL E 
USING MYSTREAMS.STREAM_EMPL S  ON E.EMPID = S.EMPID
WHEN MATCHED
    AND S.METADATA$ACTION = 'INSERT'
    AND S.METADATA$ISUPDATE  = TRUE
THEN UPDATE
    SET E.EMPNAME = S.EMPNAME,
        E.SALARY = S.SALARY,
        E.AGE = S.AGE,
        E.DEPT = S.DEPT,
        E.LOCATION = S.LOCATION,
        E.LST_UPDT_DT = CURRENT_DATE;


-- observe the stream:
SELECT * FROM MYSTREAMS.STREAM_EMPL;  -- 0 records - refreshed post consumption.


-- =================================================================================================================================================================================
-- Only Deletes
--==================================================================================================================================================================================  SELECT * FROM STAGE_TBLS.STG_EMPL;

SELECT * FROM STAGE_TBLS.STG_EMPL; -- 6 records.

// Delete 2 records from stage table:

DELETE FROM STAGE_TBLS.STG_EMPL WHERE EMPID IN (3,4); -- 2 records deleted

// check the source/staging table post delete:
SELECT * FROM STAGE_TBLS.STG_EMPL; -- 4 records.

// check Stage object:

SELECT * FROM MYSTREAMS.STREAM_EMPL; -- METADATA$ACTION = DELETE, METADATA$ISUPDATE = FALSE.

// CONSUME FROM STREAM -> MERGE
SELECT * FROM MYSTREAMS.STREAM_EMPL; -- 2 reecords
SELECT * FROM INTG_TBLS.EMPL; -- old records (6)

MERGE INTO INTG_TBLS.EMPL E
USING MYSTREAMS.STREAM_EMPL S ON E.EMPNAME = S.EMPNAME
WHEN MATCHED 
    AND S.METADATA$ACTION = 'DELETE'
    AND S.METADATA$ISUPDATE = FALSE
THEN DELETE; 

// Check the stream object:
    SELECT * FROM MYSTREAMS.STREAM_EMPL; -- 0 records, truncated,refreshed.

// Check for the updates in the target table post delete:

SELECT * FROM  INTG_TBLS.EMPL; -- 4 records



-- ================================================================================================================================================================================
-- All changes at a time
-- =====================================================================================================================================================================================
SELECT * FROM STAGE_TBLS.STG_EMPL; -- Stage table -- 4 records:

// Insert:
INSERT INTO STAGE_TBLS.STG_EMPL VALUES
(7, 'Janaki', 61000, 29, 'SALES', 'Pune'),
(8, 'Kamal', 92000, 33, 'TECHNOLOGY', 'Bangalore');

SELECT * FROM STAGE_TBLS.STG_EMPL; -- 6 records


//Update:

UPDATE STAGE_TBLS.STG_EMPL SET SALARY=85000, LOCATION='Hyderabad' 
WHERE EMPID=1;


// Delete one existing record:

DELETE FROM STAGE_TBLS.STG_EMPL WHERE EMPID in (6); 

-- 2 insert 
-- 1 update
-- 1 delete 


SELECT * FROM STAGE_TBLS.STG_EMPL; -- 5 Records.

SELECT * FROM MYSTREAMS.STREAM_EMPL; -- 5 records (2 for insert, 2 for update(for each update there would be 2, insert and delete, 1 for delete))


// CONSUMING ALLL: 

SELECT * FROM INTG_TBLS.EMPL; -- Target Table
SELECT * FROM MYSTREAMS.STREAM_EMPL; -- 5 records

MERGE INTO INTG_TBLS.EMPL E
USING MYSTREAMS.STREAM_EMPL S ON E.EMPID = S.EMPID
WHEN MATCHED 
AND  
     METADATA$ACTION = 'DELETE'
     AND METADATA$ISUPDATE = FALSE
THEN DELETE

WHEN MATCHED
AND
     METADATA$ACTION  = 'INSERT'
     AND METADATA$ISUPDATE = TRUE
THEN UPDATE
        SET E.EMPNAME = S.EMPNAME,
            E.SALARY = S.SALARY,
            E.AGE = S.AGE,
            E.DEPT = S.DEPT,
            E.LOCATION = S.LOCATION,
            E.LST_UPDT_DT = CURRENT_DATE
WHEN NOT MATCHED
    AND E.METADATA$ACTION = 'INSERT'
    AND E.METADATA$ISUPDATE = FALSE
THEN INSERT (EMPID, EMPNAME,SALARY,AGE,DEPT,LOCATION,INSRT_DT)
VALUES (S.EMPID,S.EMPNAME,S.SALARY,S.AGE,S.DEPT,S.LOCATION,CURRENT_DATE);


// View final target table data
SELECT * FROM INTG_TBLS.EMPL;


--==================================================================================================================================================================================
-- Streams with Tasks
--==================================================================================================================================================================================
CREATE OR REPLACE TASK MYTASKS.TASK_EMPL_DATA_LOAD
    WAREHOUSE = MYOWN_WH
    SCHEDULE = '5 MINUTES'
    WHEN SYSTEM$STREAM_HAS_DATA('MYSTREAMS.STREAM_EMPL')
AS 
MERGE INTO INTG_TBLS.EMPL T
USING MYSTREAMS.STREAM_EMPL S
	ON T.EMPID = S.EMPID
WHEN MATCHED
    AND S.METADATA$ACTION ='DELETE' 
    AND S.METADATA$ISUPDATE = 'FALSE'
    THEN DELETE                   
WHEN MATCHED
    AND S.METADATA$ACTION ='INSERT' 
    AND S.METADATA$ISUPDATE  = 'TRUE'       
    THEN UPDATE 
    SET T.EMPNAME = S.EMPNAME,
		T.SALARY = S.SALARY,
		T.AGE = S.AGE,
		T.DEPT = S.DEPT,
		T.LOCATION = S.LOCATION,
		T.LST_UPDT_DT = CURRENT_DATE
WHEN NOT MATCHED
    AND S.METADATA$ACTION ='INSERT'
	AND S.METADATA$ISUPDATE  = 'FALSE'
    THEN INSERT( EMPID, EMPNAME, SALARY, AGE, DEPT, LOCATION, INSRT_DT, LST_UPDT_DT)
	VALUES(S.EMPID, S.EMPNAME, S.SALARY, S.AGE, S.DEPT, S.LOCATION, CURRENT_DATE, NULL)
;

// Start the task
ALTER TASK MYTASKS.TASK_EMPL_DATA_LOAD RESUME;


SELECT * FROM STAGE_TBLS.STG_EMPL;

// Insert 1 new record
INSERT INTO STAGE_TBLS.STG_EMPL VALUES
(9, 'Latha', 47000, 25, 'HR', 'Chennai');

// Update existing record
UPDATE STAGE_TBLS.STG_EMPL 
SET SALARY=67000 WHERE EMPID=7;

// Delete one existing record
DELETE FROM STAGE_TBLS.STG_EMPL WHERE EMPID in (8);


// Observe Stream object now
SELECT * FROM MYSTREAMS.STREAM_EMPL;

// Check the data after 5 mins
SELECT * FROM INTG_TBLS.EMPL;

// Observe Stream object now
SELECT * FROM MYSTREAMS.STREAM_EMPL;




