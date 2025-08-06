CREATE DATABASE INTERVIEW_QUESTIONS;
CREATE OR REPLACE SCHEMA QUESTIONS;


CREATE OR REPLACE TABLE employee
(
    emp_id INT,
    company STRING,   -- Snowflake uses STRING for text columns instead of VARCHAR
    salary INT
);



INSERT INTO employee (emp_id, company, salary) 
VALUES 
    (1, 'A', 2341),
    (2, 'A', 341),
    (3, 'A', 15),
    (4, 'A', 15314),
    (5, 'A', 451),
    (6, 'A', 513),
    (7, 'B', 15),
    (8, 'B', 13),
    (9, 'B', 1154),
    (10, 'B', 1345),
    (11, 'B', 1221),
    (12, 'B', 234),
    (13, 'C', 2345),
    (14, 'C', 2645),
    (15, 'C', 2645),
    (16, 'C', 2652),
    (17, 'C', 65);



select * from employee;

-- GET ME THE MEDIAN WITH INBUILT FUNCTION: 
SELECT 
    company,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary
FROM employee
GROUP BY company;


-- GET ME THE MEDIAN WITHOUT INBUILT FUNCTION:

-- check:
select *,
cnt* 1.0/2 as lower_range,
(cnt * 1.0 )/2 + 1 as upper_range
from 
(
    select *, 
    row_number() over (partition by company order by salary asc) as rn,
    count(1) over (partition by company) as cnt
    from employee

);

    


-- check thsi:
select *,
cnt* 1.0/2 as lower_range,
(cnt * 1.0 )/2 + 1 as upper_range
from 
(
    select *, 
    row_number() over (partition by company order by salary asc) as rn,
    count(1) over (partition by company) as cnt
    from employee

) where rn between lower_range and upper_range;

----------------------------------------------------------------------------------------------------------------------------------------------------------------

-- FINAL:

with cte as
(
            select *,
        cnt* 1.0/2 as lower_range,
        (cnt * 1.0 )/2 + 1 as upper_range
        from 
        (
            select *, 
            row_number() over (partition by company order by salary asc) as rn,
            count(1) over (partition by company) as cnt
            from employee
        
        ) where rn between lower_range and upper_range
)
select company, 
avg(salary) as median_salary
from cte
group by company
order by company; 

