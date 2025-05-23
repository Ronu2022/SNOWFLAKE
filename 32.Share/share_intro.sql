-- Create a secure view to mask PII and aggregate loan data
CREATE OR REPLACE SECURE VIEW database_dummy.schema_dummy.loan_performance_view AS
SELECT 
    loan_id,
    MASK(customer_ssn, '****-****-####') AS masked_ssn,
    payment_status,
    delinquency_days,
    IFF(delinquency_days > 30, 'Delinquent', 'Current') AS delinquency_flag
FROM database_dummy.schema_dummy.loan_data
WHERE payment_date >= DATEADD(month, -12, CURRENT_DATE);







-- Create a Data Share
CREATE OR REPLACE SHARE share_loan_performance;




select current_account(); --SC02668
SELECT CURRENT_REGION(); -- AWS_AP_SOUTHEAST_1

-- account loactor becomes: SC02668.AWS_AP_SOUTHEAST_1



-- Grant usage on database and schema
GRANT USAGE ON DATABASE database_dummy TO SHARE share_loan_performance;
GRANT USAGE ON SCHEMA database_dummy.schema_dummy TO SHARE share_loan_performance;





-- Grant select on the secure view
GRANT SELECT ON database_dummy.schema_dummy.loan_performance_view TO SHARE share_loan_performance;

-- Add credit bureau's Snowflake account to the share
ALTER SHARE share_loan_performance ADD ACCOUNTS = 'cx_account_locator';





select current_account();
select current_region(); 

-- account_locator = current_caaount.current_region

-- Test script to validate shared data
CREATE OR REPLACE TABLE database_dummy.schema_dummy.test_log (
    test_name VARCHAR,
    test_status VARCHAR,
    test_timestamp TIMESTAMP,
    error_message VARCHAR
);

INSERT INTO database_dummy.schema_dummy.test_log
SELECT 
    'Row Count Check' AS test_name,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS test_status,
    CURRENT_TIMESTAMP AS test_timestamp,
    IFF(COUNT(*) = 0, 'No rows found in shared view', NULL) AS error_message
FROM database_dummy.schema_dummy.loan_performance_view;

-- Consumer side: Create a database from the share
CREATE DATABASE credit_bureau_db FROM SHARE provider_account_locator.share_loan_performance;
