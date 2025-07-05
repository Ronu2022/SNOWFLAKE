CREATE OR REPLACE DATABASE PROJECT;
CREATE OR REPLACE SCHEMA SPRING_FINANCIALS;
CREATE OR REPLACE SCHEMA STAGING;
CREATE OR REPLACE SCHEMA MY_VIEWS;
-- Table 1: customers
CREATE OR REPLACE TABLE PROJECT.staging.customers 
(
    customer_id NUMBER(10),
    -- PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    province VARCHAR(2),
    credit_score NUMBER(3),
    --CHECK (credit_score BETWEEN 300 AND 900),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Table 2: loan_applications
CREATE OR REPLACE TABLE  PROJECT.staging.loan_applications (
    application_id NUMBER(10),
    --PRIMARY KEY,
    customer_id NUMBER(10),
    -- REFERENCES customers(customer_id),
    loan_type VARCHAR(20) ,
    -- CHECK (loan_type IN ('PERSONAL', 'EVERGREEN', 'MORTGAGE')),
    loan_amount NUMBER(10, 2),
    --CHECK,
    -- (loan_amount BETWEEN 300 AND 35000),
    interest_rate NUMBER(5, 2),
    -- CHECK (interest_rate BETWEEN 9.99 AND 34.95),
    application_status VARCHAR(20),
    -- CHECK (application_status IN ('PENDING', 'APPROVED', 'DECLINED')),
    application_date TIMESTAMP_NTZ NOT NULL,
    approval_date TIMESTAMP_NTZ
);

-- Table 3: payment_transactions
CREATE OR REPLACE TABLE  PROJECT.staging.payment_transactions (
    transaction_id NUMBER(10), 
    --PRIMARY KEY,
    application_id NUMBER(10),
    --REFERENCES loan_applications(application_id),
    customer_id NUMBER(10),
    -- REFERENCES customers(customer_id),
    payment_amount NUMBER(10, 2) NOT NULL,
    payment_date TIMESTAMP_NTZ NOT NULL,
    payment_status VARCHAR(20),
    -- CHECK (payment_status IN ('ON_TIME', 'LATE', 'MISSED')),
    reported_to_bureau BOOLEAN DEFAULT FALSE
);

-- Table 4: credit_program_enrollments
CREATE OR REPLACE TABLE  PROJECT.staging.credit_program_enrollments (
    enrollment_id NUMBER(10),
    --PRIMARY KEY,
    customer_id NUMBER(10),
    --REFERENCES customers(customer_id),
    program_name VARCHAR(50),
    --CHECK (program_name IN ('THE_FOUNDATION', 'BLOOM')),
    enrollment_date TIMESTAMP_NTZ NOT NULL,
    monthly_payment NUMBER(10, 2) NOT NULL,
    total_payments_made NUMBER(3) DEFAULT 0,
    program_status VARCHAR(20)
    --CHECK (program_status IN ('ACTIVE', 'COMPLETED', 'CANCELLED'))
);

-- Table 5: risk_profiles
CREATE OR REPLACE TABLE PROJECT.staging.risk_profiles 
(
    risk_id NUMBER(10),
    --PRIMARY KEY,
    customer_id NUMBER(10),
    -- REFERENCES customers(customer_id),
    risk_score NUMBER(3),
    -- CHECK (risk_score BETWEEN 1 AND 100),
    credit_bureau VARCHAR(20),
    --CHECK (credit_bureau IN ('EQUIFAX', 'TRANSUNION')),
    last_updated TIMESTAMP_NTZ NOT NULL
);


-- Insert into customers
INSERT INTO PROJECT.staging.customers (customer_id, first_name, last_name, email, province, credit_score, created_at) VALUES
(1, 'John', 'Smith', 'john.smith@email.com', 'ON', 620, '2025-01-01 10:00:00'),
(2, 'Emma', 'Wilson', 'emma.wilson@email.com', 'BC', 550, '2025-01-02 12:00:00'),
(3, 'Liam', 'Brown', 'liam.brown@email.com', 'AB', 700, '2025-01-03 14:00:00'),
(4, 'Olivia', 'Davis', 'olivia.davis@email.com', 'MB', 480, '2025-01-04 09:00:00'),
(5, 'Noah', 'Taylor', 'noah.taylor@email.com', 'NS', 650, '2025-01-05 11:00:00'),
(6, 'Ava', 'Moore', 'ava.moore@email.com', 'SK', 590, '2025-01-06 13:00:00'),
(7, 'William', 'Lee', 'william.lee@email.com', 'NL', 720, '2025-01-07 15:00:00'),
(8, 'Sophia', 'Clark', 'sophia.clark@email.com', 'ON', 510, '2025-01-08 08:00:00'),
(9, 'James', 'Harris', 'james.harris@email.com', 'BC', 670, '2025-01-09 10:00:00'),
(10, 'Isabella', 'Martin', 'isabella.martin@email.com', 'AB', 600, '2025-01-10 12:00:00');

-- Insert into loan_applications
INSERT INTO PROJECT.staging.loan_applications (application_id, customer_id, loan_type, loan_amount, interest_rate, application_status, application_date, approval_date) VALUES
(1, 1, 'PERSONAL', 5000.00, 15.99, 'APPROVED', '2025-02-01 09:00:00', '2025-02-01 14:00:00'),
(2, 2, 'EVERGREEN', 1500.00, 18.99, 'APPROVED', '2025-02-02 10:00:00', '2025-02-02 15:00:00'),
(3, 3, 'MORTGAGE', 25000.00, 9.99, 'PENDING', '2025-02-03 11:00:00', NULL),
(4, 4, 'PERSONAL', 10000.00, 29.99, 'DECLINED', '2025-02-04 12:00:00', NULL),
(5, 5, 'PERSONAL', 8000.00, 12.99, 'APPROVED', '2025-02-05 13:00:00', '2025-02-05 16:00:00'),
(6, 6, 'EVERGREEN', 1500.00, 18.99, 'APPROVED', '2025-02-06 14:00:00', '2025-02-06 17:00:00'),
(7, 7, 'PERSONAL', 12000.00, 14.99, 'APPROVED', '2025-02-07 15:00:00', '2025-02-07 18:00:00'),
(8, 8, 'PERSONAL', 3000.00, 34.95, 'DECLINED', '2025-02-08 16:00:00', NULL),
(9, 9, 'MORTGAGE', 30000.00, 10.99, 'APPROVED', '2025-02-09 17:00:00', '2025-02-09 20:00:00'),
(10, 10, 'PERSONAL', 6000.00, 16.99, 'APPROVED', '2025-02-10 18:00:00', '2025-02-10 21:00:00');

-- Insert into payment_transactions
INSERT INTO PROJECT.staging.payment_transactions (transaction_id, application_id, customer_id, payment_amount, payment_date, payment_status, reported_to_bureau) VALUES
(1, 1, 1, 200.00, '2025-03-01 08:00:00', 'ON_TIME', TRUE),
(2, 1, 1, 200.00, '2025-03-15 08:00:00', 'ON_TIME', TRUE),
(3, 2, 2, 100.00, '2025-03-02 09:00:00', 'LATE', FALSE),
(4, 2, 2, 100.00, '2025-03-16 09:00:00', 'MISSED', FALSE),
(5, 5, 5, 300.00, '2025-03-03 10:00:00', 'ON_TIME', TRUE),
(6, 5, 5, 300.00, '2025-03-17 10:00:00', 'ON_TIME', TRUE),
(7, 6, 6, 100.00, '2025-03-04 11:00:00', 'ON_TIME', TRUE),
(8, 7, 7, 400.00, '2025-03-05 12:00:00', 'LATE', FALSE),
(9, 9, 9, 1000.00, '2025-03-06 13:00:00', 'ON_TIME', TRUE),
(10, 10, 10, 250.00, '2025-03-07 14:00:00', 'ON_TIME', TRUE);

-- Insert into credit_program_enrollments
INSERT INTO PROJECT.staging.credit_program_enrollments (enrollment_id, customer_id, program_name, enrollment_date, monthly_payment, total_payments_made, program_status) VALUES
(1, 1, 'THE_FOUNDATION', '2025-01-15 09:00:00', 60.00, 2, 'ACTIVE'),
(2, 2, 'THE_FOUNDATION', '2025-01-16 10:00:00', 60.00, 1, 'ACTIVE'),
(3, 3, 'BLOOM', '2025-01-17 11:00:00', 50.00, 3, 'ACTIVE'),
(4, 4, 'THE_FOUNDATION', '2025-01-18 12:00:00', 60.00, 0, 'CANCELLED'),
(5, 5, 'THE_FOUNDATION', '2025-01-19 13:00:00', 60.00, 2, 'ACTIVE'),
(6, 6, 'THE_FOUNDATION', '2025-01-20 14:00:00', 60.00, 1, 'ACTIVE'),
(7, 7, 'BLOOM', '2025-01-21 15:00:00', 50.00, 4, 'COMPLETED'),
(8, 8, 'THE_FOUNDATION', '2025-01-22 16:00:00', 60.00, 0, 'CANCELLED'),
(9, 9, 'THE_FOUNDATION', '2025-01-23 17:00:00', 60.00, 2, 'ACTIVE'),
(10, 10, 'BLOOM', '2025-01-24 18:00:00', 50.00, 3, 'ACTIVE');

-- Insert into risk_profiles
INSERT INTO PROJECT.staging.risk_profiles (risk_id, customer_id, risk_score, credit_bureau, last_updated) VALUES
(1, 1, 75, 'EQUIFAX', '2025-02-01 09:00:00'),
(2, 2, 60, 'TRANSUNION', '2025-02-02 10:00:00'),
(3, 3, 85, 'EQUIFAX', '2025-02-03 11:00:00'),
(4, 4, 45, 'TRANSUNION', '2025-02-04 12:00:00'),
(5, 5, 80, 'EQUIFAX', '2025-02-05 13:00:00'),
(6, 6, 65, 'TRANSUNION', '2025-02-06 14:00:00'),
(7, 7, 90, 'EQUIFAX', '2025-02-07 15:00:00'),
(8, 8, 50, 'TRANSUNION', '2025-02-08 16:00:00'),
(9, 9, 82, 'EQUIFAX', '2025-02-09 17:00:00'),
(10, 10, 70, 'TRANSUNION', '2025-02-10 18:00:00');



-- QUERIES GIVEN: 

// Loan Approval Rate by Province


SELECT c.province, la.loan_type, COUNT(*) AS total_applications, 
       SUM(CASE WHEN la.application_status = 'APPROVED' THEN 1 ELSE 0 END) AS approved_applications,
       (SUM(CASE WHEN la.application_status = 'APPROVED' THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS approval_rate
FROM PROJECT.staging.customers c
JOIN PROJECT.staging.loan_applications la ON c.customer_id = la.customer_id
GROUP BY c.province, la.loan_type; 



describe table  PROJECT.staging.customers;

SELECT * FROM PROJECT.staging.customers; -- CLUSTER BY province, credit_score because,
-- select * from PROJECT.staging.customers where provice = 'ON' will prune most of the data and when we add that with credit_score
-- select * from PROJECT.staging.customers where province = 'ON' and credit_score between 620 and 700 gets you prune most of teh queries.
-- PLUS IN TEH QUERY =>  province → used in GROUP BY




SELECT * FROM PROJECT.staging.loan_applications; -- Here we can go for Loan_type/ Application_status
-- filter/prune out un necessary records.
-- secondly appllication_status is used in the group by. 
-- application_id, customer_id => they are ostly unique wont get much help by making them clustering columns.
-- but usually in time based filtering we generally go for date column => like select * from  PROJECT.staging.loan_applications where date between d1 an d2;
-- but we see sattus wise filtering too
-- so the most feasible options would be cluster by (loan_type, application_status, application_date;


 
//Payment Performance for Credit Bureau Reporting:


SELECT c.customer_id, c.first_name, c.last_name, pt.payment_status, COUNT(*) AS payment_count
FROM PROJECT.staging.customers c
JOIN PROJECT.staging.payment_transactions pt ON c.customer_id = pt.customer_id
WHERE pt.reported_to_bureau = FALSE
GROUP BY c.customer_id, c.first_name, c.last_name, pt.payment_status;
-- CUSTOMERS - CUSTOMER_ID(JOIN) REPORTED_TO_BUEREAU - FALSE

SELECT * FROM PROJECT.staging.payment_transactions;-- rEPORTED TO_BUREAU (USED IN WHER CLAUSE)
-- here payment_status _> low cardinality can be used to get on time payments and missed payment counts in a month or year,
-- reorted to beaureau -- Yes or no -> yes low cardinlity 
-- Payment date -- mid cardinality --> yes
-- Transactiuon id, application_id, customer_id --> wont make much difference here. 
-- so cluster by (payment_date, pyment_status, reported_to_buerau)


// Credit Program Progress by Customer:


SELECT cpe.customer_id, c.first_name, c.last_name, cpe.program_name, cpe.total_payments_made, cpe.program_status
FROM PROJECT.staging.credit_program_enrollments cpe
JOIN PROJECT.staging.customers c ON cpe.customer_id = c.customer_id
WHERE cpe.program_status = 'ACTIVE'
ORDER BY cpe.total_payments_made DESC; -- 199 MS

-- CREDIT_PROGRAM_ENROLLMENTS - CUSTOMER_Id , PROGRAM_status  (ACTIVE)

SELECT  * FROM PROJECT.staging.credit_program_enrollments;-- PROGRAM_STATUS
    
--- again: 
    -- programme_name -- low cardinality --> allowed
    -- enrollment date-- mid - allowed
    -- programme_status -- low cardinality -- alloweed
    -- hence cluster by (enrollment_date) -- allowed
    -- Cluster by (enrollment_date, program_name, program_status)


SELECT * FROM PROJECT.STAGING.RISK_PROFILES;

-- credit buerau -- low cardinality 
-- risk_score - mid cardinality 
-- Last _updated - mid cardinality
-- cluster by (last_updated, credit_bureau, risk_score)



-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

// Optimize a Query:

-- Select one of the three provided sample analytics queries (or write a similar one).
-- Optimize it using Snowflake features like clustering keys, materialized views, or caching.
-- Justify how your optimization improves performance (e.g., reduces query cost or execution time).



-- QUERY 1: 
// Loan Approval Rate by Province


SELECT c.province, la.loan_type, COUNT(*) AS total_applications, 
       SUM(CASE WHEN la.application_status = 'APPROVED' THEN 1 ELSE 0 END) AS approved_applications,
       (SUM(CASE WHEN la.application_status = 'APPROVED' THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS approval_rate
FROM PROJECT.staging.customers c
JOIN PROJECT.staging.loan_applications la ON c.customer_id = la.customer_id
GROUP BY c.province, la.loan_type; -- 156 MS



-- OPTION 1: WE CAN MAKE A MATERIALIZED VIEW WITH JUST APPRIOVED: 
CREATE OR REPLACE MATERIALIZED VIEW  PROJECT.MY_VIEWS.APPROVED_APPLICATION_STATUS AS
SELECT * FROM PROJECT.staging.loan_applications WHERE APPLICATION_STATUS = 'APPROVED'; -- BUT THIS WONT MAKE SENSE -- i MEAN IF THATS THE CASE WHAT IF I WOULD REQUIRE DATA FOR 

-- UPDATED:
SELECT c.province, la.loan_type, COUNT(*) AS total_applications, 
       SUM(CASE WHEN la.application_status = 'APPROVED' THEN 1 ELSE 0 END) AS approved_applications,
       (SUM(CASE WHEN la.application_status = 'APPROVED' THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS approval_rate
FROM PROJECT.staging.customers c
JOIN  PROJECT.MY_VIEWS.APPROVED_APPLICATION_STATUS la ON c.customer_id = la.customer_id
GROUP BY c.province, la.loan_type;  -- tHIS ISNT SCALABLE => BECAUSE WHAT IF TEHRE IS A REQUIREMENT FOR NOT APPROVED, OR PENDING ? WHAT IF THERE IS A REQUIREMENT OF OTHER STAGES => BECOMES DIFFICULT


-- OPTION 2: 
create or replace materialized view view_name as
select c.province, la.loan_type, COUNT(*) AS total_applications,
    sum(case when la.application_status = 'APPROVED'  then 1 else 0) as approved_application,
    sum(case when la.application_status = 'Pending' then 1 else 0) as pending_count,
    sum(case when la.appication_status = 'Rejected' then 1 else 0) as rejected_count -- could be anyy other stages.
from PROJECT.staging.customers c
JOIN PROJECT.staging.loan_applications la ON c.customer_id = la.customer_id
GROUP BY c.province, la.loan_type;  -- This too is a biot manual 

-- Option 3: 

Create or replace materialized view view_name as
select 
    c.province, la.loan_type, la.application_status, count(*) as total_applications
from PROJECT.staging.customers c
JOIN PROJECT.staging.loan_applications la ON c.customer_id = la.customer_id
GROUP BY c.province, la.loan_type, la.application_status;  -- tjhi si more robust and in teh dashboard level yuou can filter out based on province,loan_type ,application_status.

-- so option 3  should be the best bet!


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
//Query 2 
//Payment Performance for Credit Bureau Reporting:


SELECT c.customer_id, c.first_name, c.last_name, pt.payment_status, COUNT(*) AS payment_count
FROM PROJECT.staging.customers c
JOIN PROJECT.staging.payment_transactions pt ON c.customer_id = pt.customer_id
--WHERE pt.reported_to_bureau = FALSE
GROUP BY c.customer_id, c.first_name, c.last_name, pt.payment_status;

-- option 1:
create or repace materialized view_name as
select  pt.reported_to_bureau,pt.payment_status, COUNT(DISTINCT pt.customer_id) as payment_counts
FROM PROJECT.staging.payment_transactions pt
WHERE pt.reported_to_bureau = FALSE
GROUP BY pt.reported_to_bureau,pt.payment_status; -- this way we have the flexibility of taking the count by reported_to beaureau aggregate.

-- oPtion 2:

create or replace materialized_view view_name as 

SELECT c.customer_id, c.first_name, c.last_name, pt.payment_status,  pt.reported_to_bureau, COUNT(*) AS payment_count
FROM PROJECT.staging.customers c
JOIN PROJECT.staging.payment_transactions pt ON c.customer_id = pt.customer_id
--WHERE pt.reported_to_bureau = FALSE
GROUP BY c.customer_id, c.first_name, c.last_name, pt.payment_status,pt.reported_to_bureau; -- This seems to be the best  becasue it is calable and can be filtered based on all three
-- first_name, -- Last_name, -- Payment_status, reeported to beareau amd can be filtered at  teh dashboard elvel too. 


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// Task -3
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// Plan for Large-Scale Data Loading:
    -- Describe how you’d load millions of records (e.g., COPY INTO from S3, Snowpipe for streaming).
    -- Mention tools or configurations for efficiency.

    --- if thats there in s3 bucket will use continious dat aloading using snowpipe.
    -- Tables would be created based on the clustering keuys such taht if the data becomes messy latter it can be reclustered. 
        -- PROJECT.staging.customers; -- CLUSTER BY province, credit_score
        -- PROJECT.staging.loan_applications;-- cluster by (loan_type, application_status, application_date)
        -- PROJECT.staging.payment_transactions; -- cluster by (payment_date, pyment_status, reported_to_buerau)
        -- PROJECT.staging.credit_program_enrollments; -- cluster by (enrollment_date, program_name, program_status)
        -- PROJECT.STAGING.RISK_PROFILES; -- cluster by (last_updated, credit_bureau, risk_score)
    -- STorage Integration Creation
    Create or replace storage integration s3_int
    type = external
    storage_provider = s3
    enabled = true
    storage_aws_role_arn = ''
    storage_allowed_locations= ('','')
    comment = "This is Storage Location";

    --- Desc:
    DESC STORAGE INTEGRATION s3_int -- get the arn and edit trust policy in s3 Roles 

    -- FILE FORMAT 
    CREATE OR REPLACE FILE FORMAT format_name
    TYPE = 'CSV'
    FIELD_DELIMITER = '|'
    COMPRESSION = 'GZIP'
    EMPTY_FIELD_AS_NULL = TRUE;

    -- List : 

    CREATE OR REPLACE STAGE stage_name
    URL = ''
    STORAGE_INTEGRAION = s3_int
    FILE_FORMAT = format_name;

    -- LKisting stage:
    LIST @stage_name; 

    -- verify : 

    SELECT t.$1 as column_1,
    t.$2 as column_2
    FROM @list_name/file_name.csv
    (FILE_FORMAT => format_name) as t

    -- Copy into tables:
    copy into db_name.schema_name.table_name
    from @stage_name
    on_error = 'CONTINUE'; 

    -- create or replace pipr: 
    create or replace pipe pipe_name 
    as copy into db_name.schema_name.table_name
    from @stage_name
    ON_ERROR = 'CONTINUE' -- the bad rows gets logged into a separate table. 

    
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    // TASK 4:Handle Data Quality:
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    -- Propose checks or processes to catch issues like invalid credit scores (outside 300-900) or missing payments.

    //Option 1: 
    -- Creating a staging table and only load those which has the desired_scores
    -- and do that periodically using a task which runs and that calls a proicedure to import the data from the staging layer using merge.

    -- Lets say taking customer table as an eg


    // Table Creation :

    CREATE OR REPLACE TABLE PROJECT.staging.customers -- Staging_table
(
    customer_id NUMBER(10),
    -- PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    province VARCHAR(2),
    credit_score NUMBER(3),
    --CHECK (credit_score BETWEEN 300 AND 900),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

// TARGET TABLE: 

    CREATE OR REPLACE TABLE PROJECT.staging.customers_target -- Staging_table
(
    customer_id NUMBER(10),
    -- PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    province VARCHAR(2),
    credit_score NUMBER(3),
    --CHECK (credit_score BETWEEN 300 AND 900),
    created_at TIMESTAMP_NTZ,
    --DEFAULT CURRENT_TIMESTAMP()
    -- last_update TIMESTAMP_NTZ DEFAULT NULL
);



-- copy command that would copy data into the staging table 

    create or replace pipe pipe_name 
    as copy into PROJECT.staging.customers
    from @stage_name
    ON_ERROR = 'CONTINUE'; 

-- creating streams:

CREATE STREAM STREAM_NAME ON TABLE PROJECT.staging.customers
APPEND_ONLY = FALSE;


// PROCEDURE_NAME

CREATE OR REPLACE PROCEDURE PROC_NAME()
RETURNS STRING
LANGUAGE SQL AS
$$
BEGIN

CREATE OR REPLACE TEMPORARY TABLE streams_temp_table AS 
SELECT * FROM STREAM_NAME;

MERGE INTO PROJECT.staging.customers_target as t
USING streams_temp_table as s on t.CUSTOMER_ID = s.CUSTOMER_ID

WHEN MATCHED 
    AND s.METADATA$ACTION = 'DELETE'
    AND s.METADATA$ISUPDATE = FALSE
THEN DELETE

WHEN MATCHED
    AND   s.METADATA$ACTION = 'INSERT'
    AND   s.METADATA$ISUPDATE = TRUE
THEN UPDATE
    SET t.CUSTOMER_ID = s.CUSTOMER_ID,
        t.FIRST_NAME = s.FIRST_NAME,
        t.LAST_NAME = s.LAST_NAME,
        t.EMAIL = s.EMAIL,
        t.PROVINCE = s.PROVINCE,
        t.CREDIT_SCORE = s.CREDIT_SCORE,
        t.CREATED_AT = s.CREATED_AT
WHEN NOT MATCHED
    AND   s.METADATA$ACTION = 'INSERT'
    AND   s.METADATA$ISUPDATE = FALSE
THEN INSERT(CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,PROVINCE,CREDIT_SCORE,CREATED_AT)     
VAlUES (s.CUSTOMER_ID,s.FIRST_NAME,s.LAST_NAME,s.EMAIL,s.PROVINCE,s.CREDIT_SCORE,s.CREATED_AT);

RETURN 'MERGE COMPLETE'; 

END;
$$; 

// TASK:
CREATE OR REPLACE TASK TASK_NAME
WAREHOUSE = WARE_HOUSE_NAME
SCHEDULE = 'using CRON  25 18 * * * UTC'
AS 
CALL PROC_NAME(); 



-- SAME GOES FOR CREDIT SCORE
-- GET REOCRD S INTO STAGING TABLE AND ONLY FILTER RECORDS THOSE QUALIFY AN DINSERT INTO TARGET TABLE 
--  THE REMAIONING MOVE INTO A LOG TABLE AS ERROR RECORDS. 


    

    

    

    
    
    
    
    
