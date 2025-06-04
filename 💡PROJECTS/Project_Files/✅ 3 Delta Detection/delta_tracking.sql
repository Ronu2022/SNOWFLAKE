cREATE DATABASE DELTA_DETECTION; 
CREATE SCHEMA DELATA_DEETECTION_SCHEMA;


// STAGING_TABLE:


CREATE OR REPLACE TABLE loan_staging
(
    application_id VARCHAR,
    customer_id VARCHAR,
    loan_amount FLOAT,
    application_date DATE,
    status VARCHAR,
    DATE_OF_ENTRY DATE DEFAULT CURRENT_DATE
);


-- MMICKING ENTRY OF DATE.BECAUSE IN REAL LIFE YOU WILL HAVE DATA IN FILES.
-- Simulate data for today (e.g., June 3, 2025)
INSERT INTO loan_staging(application_id,customer_id,loan_amount,application_date,status) VALUES
('APP001', 'CUST001', 5000, '2025-06-03', 'APPROVED'),
('APP002', 'CUST002', 10000, '2025-06-03', 'PENDING'),
('APP003', 'CUST003', 7500, '2025-06-03', 'REJECTED');

selecT * FROM loan_staging;


// Inse4rting it to a table. 

CREATE OR REPLACE TABLE loan_snapshot_current (
    application_id VARCHAR,
    customer_id VARCHAR,
    loan_amount FLOAT,
    application_date DATE,
    status VARCHAR,
    load_date DATE -- date the snapshot was loaded
);



-- procedurfe to insert the current days data:
CREATE OR REPLACE PROCEDURE pro_loan_snapshot_current()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE loan_snapshot_current;
    INSERT INTO loan_snapshot_current 
    SELECT * FROM loan_staging;
    
    RETURN 'Current day data inserted into the table';
END
$$;

// task to be run at 4 20 PM every day:
create or replace task task_current_table
warehouse = COMPUTE_WH
schedule = 'USING CRON 20 16 * * * UTC'
as call pro_loan_snapshot_current();


CREATE OR REPLACE TABLE loan_snapshot_yesterday (
    application_id VARCHAR,
    customer_id VARCHAR,
    loan_amount FLOAT,
    application_date DATE,
    status VARCHAR,
    load_date DATE -- date the snapshot was loaded
);

// Procedure for inserting
create or replace procedure proc_prev_day()
returns string
language sql AS
$$
BEGIN 
    TRUNCATE loan_snapshot_yesterday;
    INSERT INTO loan_snapshot_yesterday SELECT * FROM loan_snapshot_current where 
    load_date = current_date - 1; 

    RETURN 'Yesterdays record populated';

END
$$;


// Task runs at 1 AM because the current dat inserts at 4 16 becaus ethe staging table gets populated by 3:30 PM

create or replace task task_prev_table
warehouse  = COMPUTE_WH
SCHEDULE = 'USING CRON 10 1 * * * UTC'
AS CALL proc_prev_day();


-- Now let's say you carried out your analysis at 7 PM every day

by 7 pm you shall have current records in current table and previous days record in prevc table

-- getting all those new loan_ids:

select application_id, customer_id FROM loan_snapshot_current
EXCEPT 
SELECT  application_id, customer_id FROM loan_snapshot_yesterday;


--  getting all the updated loanids: 

select t1.application_id, t2.customer_id, t1.status as current_status,
t2.status as previous status
from loan_snapshot_current as t1
inner join loan_snapshot_yesterday as t2 on t1.application_id = t2.application_id and t1.customer_id = t2.customer_id;



