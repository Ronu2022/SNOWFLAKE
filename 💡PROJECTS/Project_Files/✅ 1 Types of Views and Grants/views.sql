
-- Create database and schema
CREATE DATABASE SPRING_FINANCIAL_DB;
USE DATABASE SPRING_FINANCIAL_DB;
CREATE SCHEMA ANALYTICS;
USE DATABASE SPRING_FINANCIAL_DB;

-- Create CUSTOMERS table
CREATE OR REPLACE TABLE SPRING_FINANCIAL_DB.ANALYTICS.CUSTOMERS (
    customer_id STRING,
    customer_name STRING,
    customer_email STRING,
    customer_phone STRING,
    customer_address STRING,
    region_id STRING,
    credit_score INTEGER
);

-- Insert synthetic customer data
INSERT INTO SPRING_FINANCIAL_DB.ANALYTICS.CUSTOMERS
VALUES
    ('C001', 'John Smith', 'john.smith@email.com', '604-555-1234', '123 Main St, Vancouver, BC', 'BC', 650),
    ('C002', 'Maria Gonzalez', 'maria.g@email.com', '416-555-5678', '456 Queen St, Toronto, ON', 'ON', 580),
    ('C003', 'Li Wei', 'li.wei@email.com', '403-555-9012', '789 1st Ave, Calgary, AB', 'AB', 720),
    ('C004', 'Emma Brown', NULL, '604-555-3456', '321 Oak St, Vancouver, BC', 'BC', 500),
    ('C005', 'Ahmed Khan', 'ahmed.k@email.com', '780-555-7890', '654 2nd St, Edmonton, AB', 'AB', 620),
    ('C006', 'Sophie Tremblay', 'sophie.t@email.com', NULL, '987 Pine Rd, Halifax, NS', 'NS', 550);

-- Create LOANS table
CREATE OR REPLACE TABLE SPRING_FINANCIAL_DB.ANALYTICS.LOANS (
    loan_id STRING,
    customer_id STRING,
    loan_type STRING,
    loan_amount FLOAT,
    interest_rate FLOAT,
    loan_status STRING,
    application_date DATE,
    approval_date DATE
);

-- Insert synthetic loan data
INSERT INTO SPRING_FINANCIAL_DB.ANALYTICS.LOANS
VALUES
    ('L001', 'C001', 'Personal', 10000.00, 12.99, 'Active', '2025-01-10', '2025-01-11'),
    ('L002', 'C002', 'Foundation', 0.00, 0.00, 'Active', '2025-02-01', '2025-02-01'),
    ('L003', 'C003', 'Evergreen', 1500.00, 18.99, 'Active', '2025-03-15', '2025-03-16'),
    ('L004', 'C004', 'Personal', 25000.00, 22.50, 'Pending', '2025-04-05', NULL),
    ('L005', 'C005', 'Personal', 5000.00, 15.99, 'Active', '2025-04-10', '2025-04-11'),
    ('L006', 'C006', 'Foundation', 0.00, 0.00, 'Active', '2025-05-01', '2025-05-01');

-- Create REGIONS table
CREATE OR REPLACE TABLE SPRING_FINANCIAL_DB.ANALYTICS.REGIONS (
    region_id STRING,
    region_name STRING,
    country STRING
);

-- Insert synthetic region data
INSERT INTO SPRING_FINANCIAL_DB.ANALYTICS.REGIONS
VALUES
    ('BC', 'British Columbia', 'Canada'),
    ('ON', 'Ontario', 'Canada'),
    ('AB', 'Alberta', 'Canada'),
    ('NS', 'Nova Scotia', 'Canada');


-- Select STatements:

Select * from SPRING_FINANCIAL_DB.ANALYTICS.CUSTOMERS;
Select * from SPRING_FINANCIAL_DB.ANALYTICS.LOANS;
select * from SPRING_FINANCIAL_DB.ANALYTICS.REGIONS;


// TASKS:
/*
Task 1: Create a Regular View for Customer Contact Details
Requirement: 
The marketing team needs contact details (name, email, phone, address) for customers in British Columbia for a loan promotion campaign.
*/



CREATE OR REPLACE VIEW SPRING_FINANCIAL_DB.ANALYTICS.W_BC_CUSTOMERS AS

SELECT c.CUSTOMER_NAME,c.CUSTOMER_EMAIL,c.CUSTOMER_PHONE,c.CUSTOMER_ADDRESS
FROM SPRING_FINANCIAL_DB.ANALYTICS.CUSTOMERS as c
LEFT JOIN SPRING_FINANCIAL_DB.ANALYTICS.REGIONS as R on c.REGION_ID = R.REGION_ID
WHERE R.REGION_NAME = 'British Columbia';


-- Check for the query Profile
Select * from  SPRING_FINANCIAL_DB.ANALYTICS.W_BC_CUSTOMERS;  

-- turn off the cache: 
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
-- Re do the things;
Select * from  SPRING_FINANCIAL_DB.ANALYTICS.W_BC_CUSTOMERS;   -- reruns from scratch 410 ms.
select * from SPRING_FINANCIAL_DB.ANALYTICS.W_BC_CUSTOMERS; -- runs from the results.even if it is turned off, becasue the warehouse cache is still active => Local Disk Cache

-- Turn it on:
ALTER SESSION SET USE_CACHED_RESULT = TRUE;


-- Creating role for the Marketing Team

CREATE OR REPLACE  ROLE marketing_team_role; 
-- but observe from the drop down the role is not being seen why ? 


SHOW USERS;
SELECT CURRENT_USER();-- AJAYMOHANTY2024

SHOW GRANTS TO USER AJAYMOHANTY2024; -- observe the role is not granted.

-- Grant the role to the the user
GRANT ROLE marketing_team_role TO USER AJAYMOHANTY2024; -- now 


-- Grant privileges:

GRANT USAGE ON DATABASE SPRING_FINANCIAL_DB TO ROLE marketing_team_role;
GRANT USAGE ON SCHEMA SPRING_FINANCIAL_DB.ANALYTICS TO ROLE marketing_team_role;
GRANT SELECT ON VIEW SPRING_FINANCIAL_DB.ANALYTICS.W_BC_CUSTOMERS TO ROLE marketing_team_role;


/*
Task 2: Create a Secure View for Loan Details
Requirement: The analytics team needs loan details (loan ID, customer name, loan amount, interest rate) for Canadian customers, 
but sensitive data (customer name) must be masked for non-admin roles to comply with privacy regulations.
*/

-- Creation of Masking Policy 

CREATE OR REPLACE MASKING POLICY cust_masking_policy
AS(val STRING)
RETURNS STRING -> 
CASE WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN val
ELSE '*** MASK ***'
END; 

-- Adding the Masking Policy to the Column:

ALTER VIEW SPRING_FINANCIAL_DB.ANALYTICS.CUSTOMERS 
MODIFY COLUMN CUSTOMER_NAME SET MASKING POLICY cust_masking_policy; -- said, plociy existing on the column

SHOW MASKING POLICIES;

-- To unset :

ALTER VIEW SPRING_FINANCIAL_DB.ANALYTICS.CUSTOMERS 
MODIFY COLUMN CUSTOMER_NAME UNSET  MASKING POLICY; -- becasue in one column you can have one masking policy hence no name required

-- Let's do it again: 
ALTER VIEW SPRING_FINANCIAL_DB.ANALYTICS.CUSTOMERS 
MODIFY COLUMN CUSTOMER_NAME SET MASKING POLICY cust_masking_policy; -- done. 



-- Creating role for Analytics Team: 

CREATE or replace ROLE analytics_role; 

-- Granting role to user; 
SHOW USERS;
SHOW GRANTS;
SELECT CURRENT_USER(); --AJAYMOHANTY2024
SHOW GRANTS TO USER AJAYMOHANTY2024;

GRANT ROLE analytics_role TO USER AJAYMOHANTY2024;

-- Creation of Secured View: 

CREATE OR REPLACE SECURE VIEW SPRING_FINANCIAL_DB.ANALYTICS.SEC_VW_LOAN_DETAILS AS
SELECT 
    l.LOAN_ID,
    c.Customer_Name,
    l.loan_amount,
    l.interest_rate
FROM SPRING_FINANCIAL_DB.ANALYTICS.LOANS AS l 
LEFT JOIN SPRING_FINANCIAL_DB.ANALYTICS.CUSTOMERS AS c ON L.CUSTOMER_ID = c.CUSTOMER_ID;


-- Check: 
Select * from SEC_VW_LOAN_DETAILS; -- view is created;

-- check the views: 

show views; -- look the text definition is visible, for all the viwes secured and not secured

-- Let's grant access to the other role:

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYTICS_ROLE;
GRANT SELECT ON VIEW  SPRING_FINANCIAL_DB.ANALYTICS.SEC_VW_LOAN_DETAILS TO ROLE ANALYTICS_ROLE;


-- Let's grant access to Public role.
GRANT USAGE ON DATABASE SPRING_FINANCIAL_DB TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE PUBLIC;
GRANT SELECT ON VIEW   SPRING_FINANCIAL_DB.ANALYTICS.SEC_VW_LOAN_DETAILS TO ROLE PUBLIC;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE PUBLIC;


-- change the role analytics_role to from the dropdown: 

SHOW VIEWS; -- Look here the View definition for the secured view is not seen.

-- Let's run the view:
select * from SPRING_FINANCIAL_DB.ANALYTICS.SEC_VW_LOAN_DETAILS; -- look the customer_name is masked. 


-- Check the details on the role Public switch the role to Public

SELECT * FROM SPRING_FINANCIAL_DB.ANALYTICS.SEC_VW_LOAN_DETAILS; -- Masked

-- Change the role back to ACCOunt ADMIN: 

/*
Task 3: Create a Materialized View for High-Priority Loans
Requirement: 
The risk team frequently queries loans with interest rates >20% for monitoring high-risk accounts. A materialized view is needed to improve query performance.
*/

-- Observe Performance -> thus and they are talking about interest rate.

-- thus interest_rate is frequently used; 
Select * from SPRING_FINANCIAL_DB.ANALYTICS.LOANS;
ALTER TABLE SPRING_FINANCIAL_DB.ANALYTICS.LOANS  CLUSTER BY (INTEREST_RATE); -- Set the cluster key
-- nkw, materialised view woiuld make sense , because they query most frequently on the interest rate

CREATE OR REPLACE MATERIALIZED VIEW SPRING_FINANCIAL_DB.ANALYTICS.MAT_VW_HIGH_RISK_LOANS AS 
SELECT loan_id,
    customer_id,
    loan_amount,
    interest_rate,
    loan_status
FROM SPRING_FINANCIAL_DB.ANALYTICS.LOANS
WHERE INTEREST_RATE > 20.00; 

-- Grant access to PUBLIC

GRANT SELECT ON VIEW SPRING_FINANCIAL_DB.ANALYTICS.MAT_VW_HIGH_RISK_LOANS  TO ROLE PUBLIC; -- Granted

-- Query the view
SELECT * FROM SPRING_FINANCIAL_DB.ANALYTICS.MAT_VW_HIGH_RISK_LOANS;-- 185ms -- from scratch

-- Insert Some records:
INSERT INTO SPRING_FINANCIAL_DB.ANALYTICS.LOANS
VALUES ('L007', 'C001', 'Personal', 15000.00, 25.00, 'Active', '2025-05-20', '2025-05-21');

-- Wait for automatic refresh (Snowflake schedules refreshes)
-- Check refresh history;

Select * from SPRING_FINANCIAL_DB.ANALYTICS.MAT_VW_HIGH_RISK_LOANS; -- took 188ms

SELECT * 
FROM TABLE(INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY())
WHERE TABLE_NAME = 'MAT_VW_HIGH_RISK_LOANS';


-- refresh history
select * from table(information_schema.materialized_view_refresh_history());





