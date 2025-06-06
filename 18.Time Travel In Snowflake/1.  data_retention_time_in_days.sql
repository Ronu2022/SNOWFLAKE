
// TIME TRAVEL IN SNOWFLAKE

    
/*
What is TIME TRAVEL and how it is used in SNOWFLAKE?
-- Snowflake Time Travel enables accessing historical data (i.e. data that has been changed or deleted) at any point within a defined period. 
-- It serves as a powerful tool for performing the following tasks:
    - Restoring data-related objects (tables, schemas, and databases) that may have been accidentally or intentionally deleted.
    - Duplicating and backing up data from key points in the past.
    - Analyzing data usage/manipulation over specified periods of time.
*/

-- SET DATA_RETENTION_TIME_IN_DAYS PROPERTY FOR TIME TRAVEL

create or replace table employees
(
    employee_id number,
    salary number,
    manager_id number
)
data_retention_time_in_days=90; -- wont work unless it is an enterprise ediition
-- for non enterprise edition it is 1 day

                     
SHOW TABLES;


create or replace table employees_test(employee_id number,
                     salary number,
                     manager_id number)
                     data_retention_time_in_days=95;
                     
alter table employees set data_retention_time_in_days=30; -- altering the data_retention_time_in_days
