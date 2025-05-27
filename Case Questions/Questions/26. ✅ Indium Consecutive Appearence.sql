
CREATE OR REPLACE TABLE LOGS
(
    id INT IDENTITY(1,1),
    num Int
);

insert into logs(num)
values
(1),
(1),
(1),
(2),
(2),
(2),
(1),
(2),
(2);


select * from logs;

// Find all numbers that appear at least 3 times consecutively;


with grouping_cte as
(
    select 
        id, num,
        row_number() over(order by id asc) as rn,
        row_number() over(partition by num order by id asc) as par,
         row_number() over(order by id asc) - row_number() over(partition by num order by id asc) as grouping_tag
    from logs order by id asc
),
consecutive_counts_cte as
(
    select 
    num, grouping_tag,
    count(*) as streak_length from grouping_cte
    group by num, grouping_tag
)

select num from consecutive_counts_cte where streak_length >= 3;


// WAY 2: 

# Write your MySQL query statement below
select distinct num as ConsecutiveNums 
from
(
    select id, num, 
    lag(num) over (order by id asc) as prev_num,
    lead(num) over(order by id) as next_num,
    lead(num,2) over (order by id asc) as third_number
    from logs
) as alias_name where (NUM = Next_NUM) AND (NUM = THIRD_NUMBER) AND (NEXT_NUM = THIRD_NUMBER);

