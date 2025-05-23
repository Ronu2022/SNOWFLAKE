/* USING A NAMED INTERNAL STAGE */ 


-- Create database
CREATE OR REPLACE DATABASE FINTECH_DB;

-- Create schema
CREATE OR REPLACE SCHEMA FINTECH_DB.LOAN_DATA;

-- Create loan_applications table
CREATE OR REPLACE TABLE FINTECH_DB.LOAN_DATA.loan_applications (
    loan_id VARCHAR(50),
    email VARCHAR(100),
    application_date DATE,
    loan_amount NUMBER(10,2)
);

-- Verify table structure
DESCRIBE TABLE FINTECH_DB.LOAN_DATA.loan_applications;

select current_account(); -- SC02668
select current_region(); -- AWS_AP_SOUTHEAST_1


// Stage Creation

CREATE OR REPLACE STAGE FINTECH_DB.LOAN_DATA.loan_application_stage
FILE_FORMAT = (type = 'csv', field_delimiter = ',', skip_header = 1, field_optionally_enclosed_by = '"');

-- Used Snow Sql to upload:
-- PUT file://C:\Users\Ronu\Desktop\Notes\Uploads\loan_applications.csv @loan_application_stage;

// Checking the List:
LIST @FINTECH_DB.LOAN_DATA.loan_application_stage;


select $1,$2, $3 from @FINTECH_DB.LOAN_DATA.loan_application_stage; -- mark no header


// Copying into the table 

COPY INTO FINTECH_DB.LOAN_DATA.loan_applications
FROM @FINTECH_DB.LOAN_DATA.loan_application_stage
file_format = (type = 'csv',field_delimiter = ',', skip_header = 1, field_optionally_enclosed_by = '"')
ON_ERROR = CONTINUE; 

SELECT * FROM FINTECH_DB.LOAN_DATA.loan_applications; -- 5 records

// Task: 

-- Create an error Log Table (Captures the Test Time stamp, Test Name, Success?Failure, no of invalid emails)
-- This must be always having the fresh count (Thus, it must be truncated first and the updated with fresh records everytime).
-- This is a or Table.
-- I would also need a table which would also have historical records that is it must have the date and the error count. 









// Log Tables Creation:  -- this is a daily record, since there is a task, it will always have one record since procedure satrts with truncate and then insert 

CREATE OR REPLACE TABLE FINTECH_DB.LOAN_DATA.test_log (
    test_name VARCHAR(100) DEFAULT 'Test:Email format Check!',
    test_status VARCHAR(10),
    test_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message VARCHAR(500),
    error_count INT
);


-- ERROR SQL: 

INSERT INTO FINTECH_DB.LOAN_DATA.test_log(test_status,error_message,error_count) 
SELECT 
  CASE 
    WHEN COUNT(*) = COUNT(CASE 
                            WHEN email ILIKE '%@%.%' 
                                 AND LEN(email) - LEN(REPLACE(email, '.', '')) BETWEEN 1 AND 2 
                            THEN 1 
                          END) 
    THEN 'Success' 
    ELSE 'Failure' 
  END AS test_status,

  CASE 
    WHEN COUNT(*) <> COUNT(CASE 
                             WHEN email ILIKE '%@%.%' 
                                  AND LEN(email) - LEN(REPLACE(email, '.', '')) BETWEEN 1 AND 2 
                             THEN 1 
                           END)
    THEN CONCAT(
      'Invalid Email Counts: ', 
      COUNT(*) - COUNT(CASE 
                         WHEN email ILIKE '%@%.%' 
                              AND LEN(email) - LEN(REPLACE(email, '.', '')) BETWEEN 1 AND 2 
                         THEN 1 
                       END)
    )
    ELSE NULL 
  END AS invalid_email_counts

FROM FINTECH_DB.LOAN_DATA.loan_applications;


// Historical Table:

Create or replace table Tableau_error_log
(
    date_mark date,
    error_count INT
);




// PROCEDURE

-- 1st truncating the error log TABLE
-- inserting the test result success or failure => if no of records = no of valid emails +> Success else Failure.
-- if Failure what is the message to be delivered.
-- count(*) - count(valid emails) = no of errors.
-- Delete all records from current date from the historical table for the current date(here Tableau_error_log )
-- Insert all records from the error log table where date = current date => this would ensure every time it is updated with recenet record and only one record per day.

CREATE OR REPLACE PROCEDURE proc_email_test_log()
RETURNS STRING
LANGUAGE SQL AS
$$
BEGIN
    TRUNCATE FINTECH_DB.LOAN_DATA.test_log;

    INSERT INTO FINTECH_DB.LOAN_DATA.test_log(test_status,error_message,error_count) 
SELECT 
  CASE 
    WHEN COUNT(*) = COUNT(CASE 
                            WHEN email ILIKE '%@%.%' 
                                 AND LEN(email) - LEN(REPLACE(email, '.', '')) BETWEEN 1 AND 2 
                            THEN 1 
                          END) 
    THEN 'Success' 
    ELSE 'Failure' 
  END AS test_status,

  CASE 
    WHEN COUNT(*) <> COUNT(CASE 
                             WHEN email ILIKE '%@%.%' AND LEN(email) - LEN(REPLACE(email, '.', '')) BETWEEN 1 AND 2 THEN 1 END)
    THEN CONCAT(
      'Invalid Email Counts: ', 
      COUNT(*) - COUNT(CASE 
                         WHEN email ILIKE '%@%.%' AND LEN(email) - LEN(REPLACE(email, '.', '')) BETWEEN 1 AND 2 
                         THEN 1 END)
    )
    ELSE NULL 
  END AS invalid_email_counts,

  COUNT(*) - COUNT(CASE 
                            WHEN email ILIKE '%@%.%' 
                                 AND LEN(email) - LEN(REPLACE(email, '.', '')) BETWEEN 1 AND 2 
                            THEN 1 
                          END) as error_count

FROM FINTECH_DB.LOAN_DATA.loan_applications;

DELETE  FROM Tableau_error_log WHERE date_mark = CURRENT_DATE;

INSERT INTO Tableau_error_log (date_mark,error_count) 
SELECT DATE(test_timestamp) , error_count FROM FINTECH_DB.LOAN_DATA.test_log WHERE DATE(test_timestamp) = CURRENT_DATE;

RETURN 'TESTING EXECUTED';

END;
$$; 


// TASK:


CREATE OR REPLACE TASK task_email_test_log
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON * * * * * UTC'
AS CALL proc_email_test_log();


// RESUMING TASK

ALTER TASK task_email_test_log RESUME; 

// SUSPEMDING TASK:

ALTER TASK task_email_test_log SUSPEND;


Call proc_email_test_log(); -- just did to check one time, else it would have ran at the scheduled time



Select * FROM FINTECH_DB.LOAN_DATA.test_log; -- results as expected
Select * from Tableau_error_log; -- results as expected



