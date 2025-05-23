// 🧭 Let’s Start: Module 1 — Introduction to Data Testing
// ✅ What Is Data Testing?
--Data testing is the process of checking whether the data in your system is:

--Accurate (is it correct?)
-- Complete (is anything missing?)
-- Consistent (does it follow expected patterns?)
-- Valid (does it conform to rules and types?)
-- You test:
    -- Raw data (after ingestion)
    -- Transformed data (after SQL logic or joins)
    -- Final outputs (used for analytics)\

// 📘 Module 2: Basic SQL Test Scripts

-- Row count checks
-- Null checks
-- Uniqueness tests
-- Referential integrity tests
-- Value range and pattern checks

/*📦 Step 1: Create Sample Tables and Insert Data*/


CREATE OR REPLACE DATABASE testing_db;

CREATE OR REPLACE TABLE customers 
(
    customer_id INT,
    name STRING,
    email STRING,
    created_at DATE
);

INSERT INTO customers (customer_id, name, email, created_at) VALUES
(1, 'Alice', 'alice@example.com', '2024-01-01'),
(2, 'Bob', NULL, '2024-02-15'),
(3, 'Charlie', 'charlie@example.com', '2024-03-10'),
(4, 'Alice', 'alice@example.com', '2024-01-01'), -- Duplicate record
(5, NULL, 'eve@example.com', '2024-05-01');

CREATE OR REPLACE TABLE orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10,2)
);

INSERT INTO orders (order_id, customer_id, order_date, amount) VALUES
(101, 1, '2024-04-01', 100.00),
(102, 2, '2024-04-02', 200.00),
(103, 99, '2024-04-03', 150.00), -- Invalid foreign key (good for testing)
(104, 3, '2024-04-04', NULL);     -- Null amount (also good for testing)




// ✅ Test #1: Row Count Validation

/*🎯 Goal:
Check whether the number of records in each table matches what we expect.

Table	Expected Rows
customers	5
orders	4
*/


Select 'customers' as table_name,
count(*) as total_records from customers
union 
select 'orders' as table_name,
count(*) as total_records from orders

-- 🎯 What This Test Tells Us:

    --If count is as expected → ✅ ingestion is correct.
    -- If count is lower → 🔴 something is missing.
    -- If count is higher → ⚠️ duplicates or extra rows.



CREATE OR REPLACE TABLE customers (
    customer_id INT,
    full_name STRING,
    email STRING,
    kyc_verified BOOLEAN,
    created_at DATE
);

INSERT INTO customers (customer_id, full_name, email, kyc_verified, created_at) VALUES
(1001, 'Ravi Kumar', 'ravi.kumar@springfin.com', TRUE, '2023-11-12'),
(1002, 'Sonal Mehta', NULL, TRUE, '2023-11-15'),
(1003, 'John Doe', 'john.doe@springfin.com', FALSE, '2023-12-01'),
(1004, NULL, 'no.name@springfin.com', TRUE, '2023-12-02'),
(1005, 'Priya Rathi', 'priya.rathi@springfin.com', TRUE, '2023-11-20');


CREATE OR REPLACE TABLE loan_applications (
    application_id INT,
    customer_id INT,
    loan_amount DECIMAL(10, 2),
    status STRING,
    application_date DATE
);

INSERT INTO loan_applications (application_id, customer_id, loan_amount, status, application_date) VALUES
(2001, 1001, 500000.00, 'APPROVED', '2024-01-01'),
(2002, 1002, 200000.00, 'PENDING', '2024-01-03'),
(2003, 9999, 300000.00, 'REJECTED', '2024-01-05'),  -- Invalid customer ID
(2004, 1003, NULL, 'PENDING', '2024-01-07'),         -- Missing loan amount
(2005, 1005, 250000.00, 'APPROVED', '2024-01-10');

/* 🏢 Scenario Context: Spring Financials – Loan Origination System
You work in the Data Engineering team at Spring Financials.
Your pipeline ingests and processes data from a Loan Origination System into Snowflake. 
One key requirement from the Risk and Compliance team is to ensure data quality before loan metrics are reported. */

/* Interview */ 
-- Then we’ll move to our next test with this real-world context:
-- “We ran a routine check to identify any loan applications missing the loan amount (which must never be null as it affects exposure calculations).”


// ✅ Test #2: Null Check — Critical Field Validation

/*🏢 Real Scenario at Spring Financials
As part of the monthly data validation process before risk aggregation, 
you are tasked with checking for missing loan amounts in the loan_applications table.
The Risk and Compliance team has flagged that reports should never include applications with NULL loan_amount, as it affects the exposure model.*/

-- 🧪 SQL: Null Check on loan_amount

select * from loan_applications where loan_amount is null; 

-- 🛠️ Action Steps (If Error Found), If this happened in real life:
    -- Flag the issue to the data source team or application owner.
    -- Hold back the loan_applications table from loading to reporting.
    -- Apply a data quality rule to prevent NULLs in critical columns going forward.
    -- Optionally: log these rows into a data quality exceptions table.

/* Interview */

-- 💬 How to Talk About It in an Interview
    -- “At Spring Financials, I built SQL-based data validation checks in Snowflake to catch critical data quality issues.
    --  For instance, we identified that a few loan applications were loaded with NULL loan amounts, which would have impacted our risk exposure calculations. 
    -- I flagged it upstream and built a reusable test script to prevent this in the future.”

    -- Now we saw the presence of Null, what's next? we can divert the same i.e the error into a separate table, that captures for the tracking.
    -- Delete the error from main table.
    -- do this automatically, with less manual intervention.


-- creation of  a log table 
CREATE OR REPLACE TABLE data_quality_issues_a 
(
    application_id INT,
    customer_id INT,
    loan_amount DECIMAL(10, 2),
    status STRING,
    application_date DATE,
    issue_desc STRING
);



CREATE OR REPLACE PROCEDURE clean_loan_amount_nulls()
RETURNS STRING
LANGUAGE SQL AS
$$
BEGIN 
 INSERT INTO data_quality_issues_a SELECT application_id, 
        customer_id,
        loan_amount,
        status,
        application_date, 'Missing Loan Amount' as issue_desc
        FROM loan_applications WHERE loan_amount IS NULL;
DELETE FROM loan_applications WHERE loan_amount IS NULL; 
RETURN 'Cleaning Completed';
END; 
$$; 

--  Create Task to Run This Procedure Automatically

CREATE OR REPLACE TASK task_name
WAREHOUSE = ware_house_name
SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS CALL clean_loan_amount_nulls() ;

-- Resuming 
ALTER TASK task_name RESUME; 



// ✅ Test #3:  Uniqueness Check


 -- Recreate the table with duplicates intentionally
CREATE OR REPLACE TABLE loan_applications (
    application_id INT,
    customer_id INT,
    loan_amount DECIMAL(10, 2),
    status STRING,
    application_date DATE
);

INSERT INTO loan_applications (application_id, customer_id, loan_amount, status, application_date) VALUES
(2001, 1001, 500000.00, 'APPROVED', '2024-01-01'),
(2002, 1002, 200000.00, 'PENDING', '2024-01-03'),
(2003, 1003, 300000.00, 'REJECTED', '2024-01-05'),
(2003, 1003, 300000.00, 'REJECTED', '2024-01-05'),  -- Duplicate
(2004, 1004, 400000.00, 'PENDING', '2024-01-07'),
(2005, 1005, 250000.00, 'APPROVED', '2024-01-10'),
(2005, 1005, 250000.00, 'APPROVED', '2024-01-10');  -- Duplicate
 
 truncate loan_applications;
 select * from loan_applications;

-- 🧠 Challenge for You:
-- Write a SQL query to:
    -- Find which application_ids appear more than once.
    -- Show how many times each duplicate appears.
    -- List some details (like customer_id or status) if helpful.

Select * from loan_applications;

-- i
Select distinct application_id from (select application_id, count(*) as count_no from loan_applications group by application_id) where count_no > 1

-- ii and iii
select application_id,customer_id, count_no from
(
  select application_id, customer_id, count(*) as count_no from loan_applications group by application_id, customer_id
) where count_no > 1;




-- last question could be done this way : since we required only application_id in group by 
SELECT 
    application_id,
    COUNT(*) AS duplicate_count,
    ARRAY_AGG(TO_VARCHAR(customer_id)) AS customers_involved,
    ARRAY_AGG(status) AS statuses
FROM loan_applications
GROUP BY application_id
HAVING COUNT(*) > 1;

-- observe the diff between array_agg and listagg(column, '|') within group (order by asc, desc) as efg 
select 
    application_id,
    count(*) as duplicate_count,
    array_agg(to_varchar(customer_id)) as customers_involved,
    listagg(customer_id,'|') within group (order by application_date asc) as list_agg
from loan_applications
group by application_id




-- log_table 
CREATE OR REPLACE TABLE log_table 
(
    application_id INT,
    issue_desc STRING,
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
-- procedure

create or replace procedure procedure_name ()
returns string 
language sql as
$$
begin
    truncate log_table ; -- first removing all records 

    insert into log_table(application_id, issue_desc)-- inserting the duplicate ids
    select application_id , 
    'duplicate_id' as duplicate_entry from (select application_id, count(*) from loan_applications group by application_id having Count(*) > 1 );
    
return 'completed';
end;
$$; 

-- task
create or replace task task_name
warehouse = warehouse_name
schedule = 'USING CRON 0 2 * * * UTC'
AS call procedure_name(); 


/* 🧠 How to Explain in an Interview
“At Spring Financials, I implemented a Snowflake stored procedure to scan for duplicate loan application IDs in our ingestion tables. It logged any violations to a log_table with an issue type. This procedure was scheduled daily via a Snowflake Task. This approach helped us flag and trace pipeline-level data duplication issues without manual intervention.”*/ 



// ✅ Test #4: Referential Integrity Check


-- 🧠 Concept (Simple words):
    -- Ensures that foreign keys in one table (like customer_id in loan_applications) actually exist in the parent table (like customers).
    -- If loan_applications.customer_id = 9999 but no customer with ID 9999 exists in the customers table → that’s a referential integrity issue.


-- Customer master table
CREATE OR REPLACE TABLE customers
(
    customer_id INT,
    full_name STRING
);

INSERT INTO customers (customer_id, full_name) VALUES
(1001, 'John Doe'),
(1002, 'Jane Smith'),
(1003, 'Raj Patel');

-- Loan applications table
CREATE OR REPLACE TABLE loan_applications 
(
    application_id INT,
    customer_id INT,
    loan_amount DECIMAL(10, 2),
    status STRING,
    application_date DATE
);

INSERT INTO loan_applications (application_id, customer_id, loan_amount, status, application_date) VALUES
(3001, 1001, 500000.00, 'APPROVED', '2024-01-01'),
(3002, 1002, 200000.00, 'PENDING', '2024-01-03'),
(3003, 1009, 300000.00, 'REJECTED', '2024-01-05'),  -- ⚠️ Invalid customer
(3004, 9999, 400000.00, 'PENDING', '2024-01-07'),   -- ⚠️ Invalid customer
(3005, 1003, 250000.00, 'APPROVED', '2024-01-10');



-- 🕵️ Find all rows in loan_applications where customer_id does NOT exist in customers table.
        -- expand it into a logging procedure + task

select * from loan_applications where customer_id not in (select distinct customer_id from customers); 


-- LogTable: 

create or replace log_table
(
    application_id INT,
    customer_id INT,
    status VARCHAR DEFAULT 'duplicate',
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Stored Procedure: 

    CREATE OR REPLACE PROCEDURE dupplicate_entries()
    RETURNS STRING 
    LANGUAGE SQL
    $$
    BEGIN 
        TRUNCATE log_table; 
    
        INSERT INTO log_table(application_id,customer_id) 
        SELECT application_id, customer_id FROM loan_applications WHERE customer_id NOT IN ( SELECT DISTINCT customer_id FROM customers);
    
        RETURN 'Completed';
    END; 
    $$; 


-- Task: 

    CREATE OR REPLACE TASK task_name
    WAREHOUSE = warehouse_name
    SCHEDULE = 'USING CRON 0 2 * * * UTC'
    AS CALL dupplicate_entries();



/* 🧠 How to Talk About This in Interviews

    “To ensure referential integrity between loan applications and customer master data, I built an automated check in Snowflake.
    A stored procedure identifies invalid customer_ids and logs them into a QA table with timestamps.
    A scheduled Snowflake Task runs this daily, helping us prevent broken joins in dashboards or downstream logic.”

*/


// ✅ Test #5: Referential Integrity Check

/*
    💼 Spring Financials Use Case:
        You’re working with the Loan Processing & Credit Risk team.
        They’ve reported that some applications are slipping through with illogical loan values, such as:
            -- Negative amounts
            -- Extremely high values not allowed by policy
            -- Missing interest rates
            -- Invalid loan types
*/



-- Loan products master
CREATE OR REPLACE TABLE loan_products (
    product_type STRING,
    max_allowed_amount DECIMAL(10, 2),
    interest_rate DECIMAL(5, 2)
);

INSERT INTO loan_products VALUES
('HOME_LOAN', 1000000.00, 7.5),
('PERSONAL_LOAN', 500000.00, 12.0),
('AUTO_LOAN', 300000.00, 9.0);

-- Loan applications with edge cases
CREATE OR REPLACE TABLE loan_applications (
    application_id INT,
    customer_id INT,
    product_type STRING,
    loan_amount DECIMAL(10, 2),
    application_date DATE
);

INSERT INTO loan_applications VALUES
(4001, 1001, 'HOME_LOAN', 950000.00, '2024-01-01'),
(4002, 1002, 'PERSONAL_LOAN', -200000.00, '2024-01-05'),   -- ❌ Negative amount
(4003, 1003, 'AUTO_LOAN', 450000.00, '2024-01-10'),        -- ❌ Over max allowed
(4004, 1004, 'BUSINESS_LOAN', 700000.00, '2024-01-12'),    -- ❌ Unknown product
(4005, 1005, 'HOME_LOAN', 850000.00, '2024-01-15');        -- ✅ Valid


/*
🎯 Your Task
    Write a SQL query that identifies bad records from loan_applications where any of the following rules are broken:
        Business Rules:
            - loan_amount is negative ❌
            - loan_amount exceeds the max allowed for that product ❌
            - product_type in loan_applications does not exist in loan_products ❌

*/

with cte as
(
    select la.application_id, la.customer_id,la.product_type,lp.product_type as matching_product_type, lp.max_allowed_amount,la.loan_amount,lp.interest_rate,la.application_date
    from loan_applications as la
    left join loan_products as lp on la.product_type  = lp.product_type
) 
SELECT 

application_id, customer_id, product_type,

case when LOAN_AMOUNT < 0 THEN 'Loan_Amount'
     when  LOAN_AMOUNT > MAX_ALLOWED_AMOUNT THEN 'Loan_Amount'
     when MATCHING_PRODUCT_TYPE IS NULL THEN 'Product_Type'
     END AS discrepency,

case when LOAN_AMOUNT < 0 THEN CONCAT(LOAN_AMOUNT || ' Loan amount is negative.')
     when LOAN_AMOUNT > MAX_ALLOWED_AMOUNT THEN CONCAT(LOAN_AMOUNT || ' is greater than the max_allowed_amount ' || MAX_ALLOWED_AMOUNT)
     when MATCHING_PRODUCT_TYPE IS NULL THEN CONCAT(product_type || ' doesnt exists in the product list of loan_products. ')
     END AS discrepancy_desc

from cte where 
LOAN_AMOUNT < 0 or 
LOAN_AMOUNT > MAX_ALLOWED_AMOUNT or
MATCHING_PRODUCT_TYPE IS NULL;




-- Log Table: 

CREATE OR REPLACE TABLE dq_loan_issues (
    application_id INT,
    customer_id INT,
    product_type STRING,
    discrepancy STRING,
    discrepancy_desc STRING,
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Stored Procedure;

CREATE OR REPLACE PROCEDURE log_business_rule_violations()
RETURNS STRING 
LANGUAGE SQL AS
$$
BEGIN 

    TRUNCATE dq_loan_issues;

        INSERT INTO dq_loan_issues(application_id,customer_id,product_type,discrepancy,discrepancy_desc) 
 
         with cte as
            (
                select la.application_id, la.customer_id,la.product_type,lp.product_type as matching_product_type, lp.max_allowed_amount,la.loan_amount,lp.interest_rate,la.application_date
                from loan_applications as la
                left join loan_products as lp on la.product_type  = lp.product_type
            ) 
            SELECT 
            
            application_id, customer_id, product_type,
            
            case when LOAN_AMOUNT < 0 THEN 'Loan_Amount'
                 when  LOAN_AMOUNT > MAX_ALLOWED_AMOUNT THEN 'Loan_Amount'
                 when MATCHING_PRODUCT_TYPE IS NULL THEN 'Product_Type'
                 END AS discrepency,
            
            case when LOAN_AMOUNT < 0 THEN CONCAT(LOAN_AMOUNT || ' Loan amount is negative.')
                 when LOAN_AMOUNT > MAX_ALLOWED_AMOUNT THEN CONCAT(LOAN_AMOUNT || ' is greater than the max_allowed_amount ' || MAX_ALLOWED_AMOUNT)
                 when MATCHING_PRODUCT_TYPE IS NULL THEN CONCAT(product_type || ' doesnt exists in the product list of loan_products. ')
                 END AS discrepancy_desc
            
            from cte where 
            LOAN_AMOUNT < 0 or 
            LOAN_AMOUNT > MAX_ALLOWED_AMOUNT or
            MATCHING_PRODUCT_TYPE IS NULL;

    RETURN 'Business Rule DQ Check Completed';
END;
$$; 



-- Task: 
CREATE OR REPLACE TASK task_name
WAREHOUSE = compute_wh
SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS CALL log_business_rule_violations();

-- Resuming Task: 
ALTER TASK task_name RESUME;


SHOW TASKS LIKE '%task%';


-- Didnt check the task, just called the procedure to check the entries.
CALL log_business_rule_violations();


select * from dq_loan_issues; -- all done.


/*
🔍 Interview Tip: How to Present This Like a Pro
“At Spring Financials, I built data quality checks to enforce business logic — such as loan amount thresholds and valid product types. I created a stored procedure that logs all violations to a centralized QA table. Then I scheduled this using a Snowflake task to run daily. This not only improved data trust in reporting, but also flagged issues upstream.”
*/


-- 🧪 Test #6 – Date Validation

/*💼 Spring Financials Use Case:
The Compliance team flagged that some loan applications have invalid dates, such as:
    -- Dates in the future
    -- Dates before the year 2000 (policy came into effect only after Y2K)
    -- These cause issues in downstream financial reports and compliance audits.
    */






-- Create the master loan products table
CREATE OR REPLACE TABLE loan_products
(
    product_type STRING,
    max_allowed_amount DECIMAL(10, 2),
    interest_rate DECIMAL(5, 2)
);

-- Insert valid loan products
INSERT INTO loan_products VALUES
('HOME_LOAN', 1000000.00, 7.5),
('PERSONAL_LOAN', 500000.00, 12.0),
('AUTO_LOAN', 300000.00, 9.0);

-- Create loan applications table
CREATE OR REPLACE TABLE loan_applications 
(
    application_id INT,
    customer_id INT,
    product_type STRING,
    loan_amount DECIMAL(10, 2),
    application_date DATE
);

-- Insert original test data (including business rule edge cases)
INSERT INTO loan_applications VALUES
(4001, 1001, 'HOME_LOAN', 950000.00, '2024-01-01'),         -- ✅ Valid
(4002, 1002, 'PERSONAL_LOAN', -200000.00, '2024-01-05'),    -- ❌ Negative amount
(4003, 1003, 'AUTO_LOAN', 450000.00, '2024-01-10'),         -- ❌ Over max allowed
(4004, 1004, 'BUSINESS_LOAN', 700000.00, '2024-01-12'),     -- ❌ Unknown product
(4005, 1005, 'HOME_LOAN', 850000.00, '2024-01-15'),         -- ✅ Valid

-- Add new test data for Date Validation
(4006, 1006, 'AUTO_LOAN', 250000.00, '1999-12-31'),         -- ❌ Too old
(4007, 1007, 'HOME_LOAN', 750000.00, CURRENT_DATE + 5),     -- ❌ Future date
(4008, 1008, 'PERSONAL_LOAN', 200000.00, CURRENT_DATE);     -- ✅ Valid


/*🎯 Your Task
Write a SQL query to identify invalid application_date values from loan_applications:
Validation Rules:
application_date must be on or after '2000-01-01'
application_date must not be in the future  */


Select * from loan_applications where application_date <= DATE '2000-01-01' or application_date > current_date;

-- Log table:
CREATE or replace TABLE  log_table
(
    log_date DATE DEFAULT CURRENT_DATE,
    log_time TIME DEFAULT CURRENT_TIME,
    application_id INT,
    customer_id INT,
    product_type VARCHAR,
    loan_amount FLOAT,
    application_date DATE,
    descrepency_description STRING
);

    
   

-- Procedure

create or replace procedure procedure_name()
returns string
language sql as
$$
begin
    truncate log_table; 

    insert into log_table(application_id,customer_id,product_type,loan_amount,application_date,descrepency_description)
    
    with cte as 
    (
    select application_id,customer_id,product_type,loan_amount,application_date,
    case when application_date <= '2000-01-01' THEN CONCAT(application_date || ' : this application date is less than the min date 2000-01-01.')
         when application_date > current_date THEN CONCAT(application_date || ' : the application date cant be a future date!')
         end as descrepency_description
    from loan_applications where application_date <= '2000-01-01' or application_date > current_date
    ) select * from cte;

    return 'Logging discrepency is completed';
end; 
$$; 


-- Task: 

create or replace task task_name
warehouse = COMPUTE_WH
schedule = 'USING CRON 0 2 * * * UTC'
AS CALL procedure_name();


-- Resuming Task: 

ALTER TASK task_name RESUME; 

ALTER TASK task_name SUSPEND; 


-- 🧪 Test #7 – Cross-Table Consistency

    -- 💼 Spring Financials Use Case:
        -- You're working with the Payments Audit Team. They discovered that some loan applications marked as APPROVED never got a corresponding payment entry.
        -- This breaks both compliance and financial reporting.



-- Drop and recreate loan_applications with status column
CREATE OR REPLACE TABLE loan_applications 
(
    application_id INT,
    customer_id INT,
    product_type STRING,
    loan_amount DECIMAL(10, 2),
    application_date DATE,
    application_status STRING
);

-- Insert records with different statuses
INSERT INTO loan_applications VALUES
(5001, 2001, 'HOME_LOAN', 900000.00, '2024-02-01', 'APPROVED'),   -- ✅ Should have payment
(5002, 2002, 'AUTO_LOAN', 250000.00, '2024-02-02', 'PENDING'),    -- ❌ Pending, no payment needed
(5003, 2003, 'PERSONAL_LOAN', 150000.00, '2024-02-03', 'APPROVED'), -- ❌ Approved but no payment
(5004, 2004, 'AUTO_LOAN', 280000.00, '2024-02-04', 'REJECTED');   -- ❌ Rejected, no payment expected

-- Create the payments table
CREATE OR REPLACE TABLE loan_payments
(
    payment_id INT,
    application_id INT,
    paid_amount DECIMAL(10, 2),
    payment_date DATE
);

-- Insert payment only for one approved loan
INSERT INTO loan_payments VALUES
(9001, 5001, 900000.00, '2024-02-05');



select 
    la.application_id,la.customer_id, la.product_type, la.loan_amount, la.application_date, la.application_status
from loan_applications as la
left join loan_payments as lp on la.application_id = lp.application_id
WHERE la.application_status = 'APPROVED' and lp.application_id IS NULL;




