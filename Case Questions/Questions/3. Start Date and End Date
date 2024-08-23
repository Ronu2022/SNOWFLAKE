CREATE OR REPLACE  TABLE tasks 
(
date_value date,
state varchar(10)
) COMMENT = 'This is Tasks table';

INSERT INTO  tasks  VALUES 
('2019-01-01','success'),
('2019-01-02','success'),
('2019-01-03','success'),
('2019-01-04','fail'),
('2019-01-05','fail'),
('2019-01-06','success');


SELECT * FROM tasks;


/* 
format required 
We would need the O/p In the following format:

Start_date   end_date  state

for e.g

start_date: 2019-01-01
end_date : 2019-01-03
state : success


Start_date: 2019-01-04
Start_date: 2019-01-05
State: fail

start_date: 2019-01-06
end_date : 2019-01-06
state: Success

*/

-------------------------------------------------------------------------


WITH numbered_rows AS
    (
        SELECT date_value,
               state, 
               ROW_NUMBER() OVER (ORDER BY date_value) AS row_num
        FROM tasks
    ),
status_change AS
    (
        SELECT date_value,
               state, 
               ROW_NUMBER() OVER (ORDER BY date_value) AS row_num,
               LAG(state) OVER (ORDER BY row_num) AS previous_state
        FROM numbered_rows
    ),
groups AS
    (
        SELECT date_value,
               state, 
               row_num,
               previous_state,
               CASE WHEN state = previous_state THEN 0 ELSE 1 END AS new_group
        FROM status_change
               
    ),
group_identifier AS
    (
        SELECT 
            date_value,
            state,
            row_num, 
            previous_state,
            new_group,
            SUM(new_group) OVER (ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING  AND CURRENT ROW) AS group_num
        FROM groups
        
    ),
grouped_dates AS
    (
        SELECT 
            group_num,
            state,
            MIN(date_value) AS start_date,
            MAX(date_value) AS end_date
        FROM group_identifier
        GROUP BY 1,2

    )SELECT start_date, end_date, state FROM grouped_dates;



----------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Second Way. 


SELECT * FROM tasks;

WiTH Grouped_cte AS
(
SELECT *, ROW_NUMBER() OVER (PARTITION BY state ORDER BY date_value) AS row_num,
DATEADD('day',- 1 * ROW_NUMBER() OVER (PARTITION BY state ORDER BY date_value),date_value ) AS group_date
FROM tasks
ORDER BY date_value
) SELECT MIN(date_value) AS start_date,
         MAX(date_value) AS MAX_date,
         state
  FROM GROUPED_CTE 
  GROUP BY state,group_date
         



